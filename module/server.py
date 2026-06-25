"""Multi-device REST server.

Routes are device-namespaced under /{node_id}/ (e.g. /!3f172791/nodes).
Server-level routes are flat (/status, /devices, /bridge_config, /events).
No static files — dashboard is a separate service.
CORS enabled for all origins.
"""
import asyncio
import logging
import subprocess
import time
from contextlib import asynccontextmanager

from bleak import BleakScanner
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body, Query
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from core import bridge_config as _bcfg
from core.bridge_config import update_ble_device
from core.claude_daemon import run as _claude_daemon_run
from core.methods import METHODS, get_nodes
from core.mcp_server import mount_mcp
from core.sections import CONFIG_SECTIONS, MODULE_CONFIG_SECTIONS, REBOOT_SECTIONS, SECTION_META
from core.schema import get_section_schema, get_channel_schema, get_owner_schema, get_fixed_position_schema
from .device_manager import DeviceManager
from .help import HELP_TEXT

logger = logging.getLogger(__name__)


def _err(code: int, message: str, status: int = 400):
    return JSONResponse({"error": {"code": code, "message": message}}, status_code=status)


def create_app(dm: DeviceManager) -> FastAPI:
    _mcp_mgr: list = []  # one-element list so the closure can see the final value

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        daemon_task = asyncio.create_task(_claude_daemon_run(), name="claude-daemon")
        async with _mcp_mgr[0].run():
            yield
        daemon_task.cancel()

    app = FastAPI(title="mesh-rest-bridge-multi", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=500)

    # -- helper: resolve node_id to bridge or 404 ----------------------------

    def _bridge(node_id: str):
        b = dm.get(node_id)
        if b is None:
            raise HTTPException(404, f"Unknown device: {node_id}")
        return b

    async def _call(node_id: str, method_name: str, params: dict):
        bridge = _bridge(node_id)
        fn = METHODS.get(method_name)
        if not fn:
            raise HTTPException(404, f"Method not found: {method_name}")
        try:
            return await fn(bridge, params)
        except KeyError as e:
            raise HTTPException(400, f"Missing/invalid param: {e}")
        except TimeoutError as e:
            raise HTTPException(504, str(e))
        except RuntimeError as e:
            raise HTTPException(503, str(e))
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Method %s failed", method_name)
            raise HTTPException(500, str(e))

    # =========================================================================
    # Server-level routes
    # =========================================================================

    _mcp_mgr.append(mount_mcp(app, dm))

    @app.get("/help", response_class=PlainTextResponse)
    async def help_text():
        return HELP_TEXT

    @app.post("/reload")
    async def reload_config():
        return await dm.reload_config()

    @app.get("/status")
    async def server_status():
        return {
            "server": "mesh-rest-bridge-multi",
            "devices": dm.list_devices(),
        }

    @app.get("/devices")
    async def list_devices():
        return {"devices": dm.list_devices()}

    @app.post("/devices")
    async def add_device(body: dict = Body(...)):
        address = (body.get("address") or "").strip()
        pin = (body.get("pin") or "").strip()
        if not address:
            raise HTTPException(400, "address required")
        tcp_port = body.get("tcp_port") or None
        if tcp_port:
            tcp_port = int(tcp_port)
        if body.get("persist", True):
            cfg = _bcfg.load()
            devices = cfg.get("ble_devices") or []
            addrs = [d.get("address", "").upper() for d in devices]
            if address.upper() not in addrs:
                entry = {"address": address, "pin": pin}
                if tcp_port:
                    entry["tcp_port"] = tcp_port
                devices.append(entry)
                cfg["ble_devices"] = devices
                _bcfg.save(cfg)
        key = await dm.connect(address, pin=pin, tcp_port=tcp_port)
        return {"connecting": True, "key": key, "address": address, "tcp_port": tcp_port}

    @app.delete("/devices/{node_id:path}")
    async def remove_device(node_id: str):
        asyncio.create_task(dm.disconnect(node_id))
        return {"disconnecting": True, "node_id": node_id}

    @app.post("/devices/{node_id}/retry")
    async def retry_device(node_id: str):
        """Reset reconnect backoff and trigger an immediate reconnect attempt."""
        bridge = _bridge(node_id)
        if bridge.ble:
            bridge.ble.reconnect_attempts = 0
        if bridge.ble_state not in ("reconnecting",):
            asyncio.create_task(bridge._on_disconnected())
        return {"retrying": True, "node_id": node_id}

    # -- OTA firmware management -----------------------------------------------

    _releases_cache: dict = {"data": None, "ts": 0.0}
    _RELEASES_TTL = 3600.0

    @app.get("/ota/firmware")
    async def ota_list_firmware(node_id: str = Query(...)):
        """List firmware files available for this device's hw_model.

        Returns files from {ota.dir}/{hw_model}/ filtered by protocol extension.
        """
        import time
        from pathlib import Path
        from core.bridge_config import load as _load_cfg
        from core.ota_esp32 import is_nrf52

        ota_dir = _load_cfg().get("ota", {}).get("dir", "")
        if not ota_dir:
            return {"files": [], "hw_model": None, "dir": None, "configured": False}

        bridge = dm.get(node_id)
        hw_model = (bridge.state.metadata.get("hw_model") or "") if bridge else ""
        if not hw_model:
            return {"files": [], "hw_model": None, "dir": None, "configured": True, "error": "hw_model not available yet"}

        hw_dir = Path(ota_dir) / hw_model
        ext = ".zip" if is_nrf52(hw_model) else ".bin"

        if not hw_dir.exists():
            return {"files": [], "hw_model": hw_model, "dir": str(hw_dir), "configured": True}

        files = sorted(
            [{"name": f.name, "size": f.stat().st_size, "mtime": int(f.stat().st_mtime)}
             for f in hw_dir.iterdir() if f.is_file() and f.suffix == ext],
            key=lambda x: x["mtime"], reverse=True,
        )
        return {"files": files, "hw_model": hw_model, "dir": str(hw_dir), "configured": True}

    @app.get("/ota/releases")
    async def ota_releases():
        """Return Meshtastic firmware releases from GitHub, cached 1 h."""
        import time
        import httpx

        now = time.monotonic()
        if _releases_cache["data"] and now - _releases_cache["ts"] < _RELEASES_TTL:
            return _releases_cache["data"]

        url = "https://api.github.com/repos/meshtastic/firmware/releases?per_page=15"
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers={
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "mesh-gw/1.0",
            })
            r.raise_for_status()
            raw = r.json()

        releases = [
            {
                "tag":        rel["tag_name"],
                "name":       rel["name"],
                "published":  rel["published_at"],
                "prerelease": rel["prerelease"],
                "assets": [
                    {"name": a["name"], "url": a["browser_download_url"], "size": a["size"]}
                    for a in rel["assets"]
                    if a["name"].endswith((".bin", ".zip")) and "ota" not in a["name"].lower()
                ],
            }
            for rel in raw
        ]
        result = {"releases": releases}
        _releases_cache["data"] = result
        _releases_cache["ts"] = now
        return result

    @app.post("/ota/firmware/download")
    async def ota_download_firmware(body: dict = Body(...)):
        """Download a firmware asset from GitHub into {ota.dir}/{hw_model}/."""
        from pathlib import Path
        from core.bridge_config import load as _load_cfg

        node_id  = body.get("node_id")
        url      = body.get("url")
        filename = body.get("filename")

        if not node_id or not url or not filename:
            raise HTTPException(400, "node_id, url, and filename are required")

        ota_dir = _load_cfg().get("ota", {}).get("dir", "")
        if not ota_dir:
            raise HTTPException(400, "ota.dir not configured in bridge_config.yaml")

        bridge   = dm.get(node_id)
        hw_model = (bridge.state.metadata.get("hw_model") or "") if bridge else ""
        if not hw_model:
            raise HTTPException(400, "hw_model not available — device still syncing?")

        hw_dir = Path(ota_dir) / hw_model
        hw_dir.mkdir(parents=True, exist_ok=True)
        dest = hw_dir / filename

        await dm._broadcast({"type": "ota_download_start", "device": node_id, "filename": filename, "hw_model": hw_model})

        async def _run():
            import httpx
            try:
                async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
                    async with client.stream("GET", url, headers={"User-Agent": "mesh-gw/1.0"}) as resp:
                        resp.raise_for_status()
                        total = int(resp.headers.get("content-length", 0))
                        done  = 0
                        last_pct = -1
                        with open(dest, "wb") as f:
                            async for chunk in resp.aiter_bytes(65536):
                                f.write(chunk)
                                done += len(chunk)
                                if total:
                                    pct = round(done / total * 100)
                                    if pct != last_pct:
                                        last_pct = pct
                                        await dm._broadcast({"type": "ota_download_progress", "device": node_id, "data": {"pct": pct, "done": done, "total": total}})
                await dm._broadcast({"type": "ota_download_complete", "device": node_id, "filename": filename, "size": done})
            except Exception as e:
                logger.exception("OTA download failed for %s", node_id)
                if dest.exists():
                    dest.unlink(missing_ok=True)
                await dm._broadcast({"type": "ota_download_error", "device": node_id, "data": {"error": str(e)}})

        asyncio.create_task(_run())
        return {"started": True, "filename": filename, "dest": str(dest)}

    @app.post("/ota")
    async def ota_update(body: dict = Body(...)):
        """Trigger a BLE OTA firmware update.

        Routes automatically by hw_model:
          nRF52 devices (RAK4631 etc.) → Nordic Legacy DFU (.zip)
          ESP32 devices                → esp32-unified-ota (.bin)

        Body: { "node_id": "!xxxx", "ble_addr": "AA:BB:CC:DD:EE:FF", "firmware": "<filename or full path>" }
        If firmware is a bare filename (no path separator), it is resolved via ota.dir/hw_model/.
        Streams progress via /events WS as ota_start / ota_progress / ota_complete / ota_error.
        Returns immediately with {"started": true}.
        """
        from pathlib import Path
        from core.bridge_config import load as _load_cfg
        from core.ota_esp32 import is_nrf52

        ble_addr = (body.get("ble_addr") or body.get("ble_address") or "").strip() or None
        fw_path  = body.get("firmware")
        node_id  = body.get("node_id") or ble_addr

        if not ble_addr or not fw_path:
            raise HTTPException(400, "ble_addr and firmware are required")

        # Auto-resolve bare filename via ota.dir/hw_model/
        if "/" not in fw_path and "\\" not in fw_path:
            from core.bridge_config import load as _load_cfg
            _pre = dm.get(node_id) or (dm.get_by_ble(ble_addr) if hasattr(dm, "get_by_ble") else None)
            _hw  = (_pre.state.metadata.get("hw_model") or "") if _pre else ""
            _dir = _load_cfg().get("ota", {}).get("dir", "")
            if _dir and _hw:
                fw_path = str(Path(_dir) / _hw / fw_path)

        if not Path(fw_path).is_file():
            raise HTTPException(400, f"firmware file not found: {fw_path}")

        bridge = dm.get_by_ble(ble_addr) if hasattr(dm, "get_by_ble") else None
        if not bridge:
            raise HTTPException(400, f"no connected bridge for BLE address {ble_addr} — device must be connected")
        hw_model = bridge.state.metadata.get("hw_model") or ""
        use_nrf  = is_nrf52(hw_model)
        protocol = "nrf52-dfu" if use_nrf else "esp32-unified-ota"
        logger.info("OTA %s: hw_model=%r (from device) → %s", node_id, hw_model, protocol)

        await dm._broadcast({
            "type": "ota_start",
            "ble_addr": ble_addr,
            "device": node_id,
            "firmware": Path(fw_path).name,
            "protocol": protocol,
        })

        async def _run():
            def _progress(pct, total, done):
                asyncio.create_task(dm._broadcast({
                    "type": "ota_progress",
                    "device": node_id,
                    "ble_addr": ble_addr,
                    "data": {"pct": pct},
                }))

            try:
                if use_nrf:
                    from core.ota import ota_update as _do_ota, DfuError as _OtaError
                else:
                    from core.ota_esp32 import ota_update as _do_ota, Esp32OtaError as _OtaError

                result = await _do_ota(bridge, node_id, fw_path, ble_addr=ble_addr, progress_cb=_progress)
                await dm._broadcast({"type": "ota_complete", "device": node_id, "ble_addr": ble_addr, "data": result})
            except Exception as e:
                logger.exception("OTA failed for %s", ble_addr)
                await dm._broadcast({"type": "ota_error", "device": node_id, "ble_addr": ble_addr, "data": {"error": str(e)}})

        asyncio.create_task(_run())
        return {"started": True, "ble_addr": ble_addr, "node_id": node_id, "firmware": fw_path, "protocol": protocol}

    @app.patch("/ble_devices/{address}")
    async def patch_ble_device(address: str, body: dict = Body(...)):
        """Update persisted settings for a BLE device (auto_connect etc.)."""
        allowed = {"auto_connect", "tcp_port"}
        fields = {k: v for k, v in body.items() if k in allowed}
        if "tcp_port" in fields and fields["tcp_port"] is not None:
            fields["tcp_port"] = int(fields["tcp_port"])
        if not fields:
            raise HTTPException(400, f"No recognised fields. Allowed: {sorted(allowed)}")
        return update_ble_device(address, fields)

    # -- MQTT publisher --------------------------------------------------------

    @app.get("/bridge_config")
    async def get_bridge_config():
        cfg = _bcfg.load()
        return {k: v for k, v in cfg.items() if k != "ble_devices"}

    @app.put("/bridge_config")
    async def put_bridge_config(body: dict = Body(...)):
        cfg = _bcfg.load()
        body.pop("ble_devices", None)
        for k, v in body.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k] = _bcfg._deep_merge(cfg[k], v)
            else:
                cfg[k] = v
        saved = _bcfg.save(cfg)
        return {k: v for k, v in saved.items() if k != "ble_devices"}

    @app.get("/mqtt_publish")
    async def get_mqtt_publish():
        return _bcfg.load().get("mqtt_publish", {})

    @app.put("/mqtt_publish")
    async def put_mqtt_publish(body: dict = Body(...)):
        cfg = _bcfg.load()
        cfg["mqtt_publish"] = _bcfg._deep_merge(cfg.get("mqtt_publish", {}), body)
        _bcfg.save(cfg)
        pub = dm.get_mqtt_publisher()
        if pub:
            enabled = cfg["mqtt_publish"].get("enabled", True)
            if not enabled:
                await dm.stop_mqtt_publisher()
        else:
            if cfg["mqtt_publish"].get("enabled"):
                dm.start_mqtt_publisher(cfg["mqtt_publish"])
        return cfg["mqtt_publish"]

    @app.get("/mqtt_publish/status")
    async def mqtt_publish_status():
        pub = dm.get_mqtt_publisher()
        if not pub:
            return {"running": False}
        return {"running": True, "connected": pub.connected}

    @app.get("/nodes")
    async def all_nodes_aggregated(
        max_age: int = 0, max_hops: int = 99,
        named_only: bool = False, has_position: bool = False,
        hide_mqtt: bool = False, has_signal: bool = False,
        has_telemetry: bool = False,
        node_roles: List[str] = Query(default=[]),
    ):
        """Merged node list from all connected bridges."""
        params = {
            "max_age": max_age, "max_hops": max_hops,
            "named_only": named_only, "has_position": has_position,
            "hide_mqtt": hide_mqtt, "has_signal": has_signal,
            "has_telemetry": has_telemetry, "node_roles": node_roles,
        }
        merged: dict = {}
        for bridge in dm._devices.values():
            data = await get_nodes(bridge, params)
            for k, v in (data.get("nodes") or {}).items():
                if k not in merged or (v.get("last_heard") or 0) > (merged[k].get("last_heard") or 0):
                    merged[k] = v
        return {"total": len(merged), "count": len(merged), "nodes": merged}

    def _bluez_status(address: str) -> dict:
        """Return paired/trusted state from BlueZ for a given address."""
        try:
            out = subprocess.run(
                ["bluetoothctl", "info", address],
                capture_output=True, text=True, timeout=5,
            ).stdout
            return {"paired": "Paired: yes" in out, "trusted": "Trusted: yes" in out}
        except Exception:
            return {"paired": False, "trusted": False}

    @app.get("/ble/scan")
    async def ble_scan():
        try:
            MESHTASTIC_SVC = "6ba1b218-15a8-461f-9fa8-5dcae273eafd"
            found = await BleakScanner.discover(timeout=5.0, return_adv=True)
            result = []
            for addr, (dev, adv) in found.items():
                uuids = [str(u).lower() for u in (adv.service_uuids or [])]
                is_mesh = MESHTASTIC_SVC in uuids or any(
                    k in (dev.name or "").lower() for k in ("meshtastic", "ta2r", "ta2m"))
                status = _bluez_status(addr)
                result.append({
                    "name": dev.name or "Unknown",
                    "address": addr,
                    "rssi": adv.rssi if adv.rssi is not None else -100,
                    "meshtastic": is_mesh,
                    "paired": status["paired"],
                    "trusted": status["trusted"],
                })
            result = [r for r in result if r["meshtastic"]]
            result.sort(key=lambda x: -x["rssi"])
            return {"devices": result}
        except Exception as e:
            raise HTTPException(500, f"Scan failed: {e}")

    @app.delete("/ble/known/{address}")
    async def ble_remove(address: str):
        """Remove a device from BlueZ bonding database."""
        address = address.upper()
        try:
            subprocess.run(
                ["bluetoothctl", "remove", address],
                capture_output=True, text=True, timeout=10,
            )
        except Exception as e:
            raise HTTPException(500, f"Remove failed: {e}")
        return {"removed": True, "address": address}

    # -- BLE pairing for dynamic-PIN devices ------------------------------------

    @app.post("/ble/pair")
    async def ble_pair(body: dict = Body(...)):
        """Start connection to a dynamic-PIN device. The pairing process pauses
        at the passkey prompt. Watch the device screen and call POST /ble/passkey
        with the PIN shown."""
        address = (body.get("address") or "").strip()
        if not address:
            raise HTTPException(400, "address required")
        tcp_port = body.get("tcp_port") or None
        if tcp_port:
            tcp_port = int(tcp_port)
        key = await dm.pair_device(address, tcp_port=tcp_port)
        return {
            "connecting": True,
            "key": key,
            "address": address,
            "tcp_port": tcp_port,
            "hint": "Watch device screen for PIN, then POST /ble/passkey",
        }

    @app.post("/ble/passkey")
    async def ble_passkey(body: dict = Body(...)):
        """Supply the PIN shown on the device screen to complete pairing."""
        address = (body.get("address") or "").strip()
        passkey = str(body.get("passkey") or "").strip()
        if not address or not passkey:
            raise HTTPException(400, "address and passkey required")
        try:
            dm.resolve_passkey(address, passkey)
        except ValueError as e:
            raise HTTPException(404, str(e))
        return {"accepted": True, "address": address}

    # -- Unified WebSocket: all devices, events tagged with device ID ----------

    @app.websocket("/events")
    async def ws_all(websocket: WebSocket):
        device_filter = websocket.query_params.get("device", "")
        await websocket.accept()
        # Push current snapshot for every active device — no HTTP needed by subscribers
        for node_id, bridge in dm._devices.items():
            if device_filter and node_id != device_filter:
                continue
            snapshot = bridge.current_snapshot()
            snapshot["device"] = node_id
            await websocket.send_json(snapshot)
        for bridge in dm._devices.values():
            for event in bridge.state.get_cached_messages():
                if device_filter and event.get("device") != device_filter:
                    continue
                await websocket.send_json(event)
        q = dm.subscribe()
        try:
            while True:
                event = await q.get()
                if device_filter and event.get("device") != device_filter:
                    continue
                await websocket.send_json(event)
        except WebSocketDisconnect:
            pass
        finally:
            dm.unsubscribe(q)

    # Schema meta (device-independent) — must be registered before /{node_id}/
    # routes to avoid /{node_id}/range_test shadowing /schema/range_test etc.
    @app.get("/sections")
    async def get_sections():
        return {
            "config": list(CONFIG_SECTIONS),
            "module_config": list(MODULE_CONFIG_SECTIONS),
            "meta": SECTION_META,
        }

    @app.get("/schema/channel")
    async def schema_channel():
        return get_channel_schema()

    @app.get("/schema/owner")
    async def schema_owner():
        return get_owner_schema()

    @app.get("/schema/fixed_position")
    async def schema_fixed_position():
        return get_fixed_position_schema()

    @app.get("/schema/{section}")
    async def schema_section(section: str):
        try:
            return get_section_schema(section)
        except KeyError as e:
            raise HTTPException(404, str(e))

    # =========================================================================
    # Device-namespaced routes  — prefix /{node_id}/
    # node_id is the full '!3f172791' string (the '!' is part of the path)
    # =========================================================================

    @app.get("/{node_id}/status")
    async def device_status(node_id: str):
        return await _call(node_id, "get_status", {})

    @app.get("/{node_id}/info")
    async def device_info(node_id: str):
        return await _call(node_id, "get_info", {})

    @app.get("/{node_id}/nodes")
    async def device_nodes(
        node_id: str,
        max_age: int = 0,
        max_hops: int = 99,
        named_only: bool = False,
        has_position: bool = False,
        hide_mqtt: bool = False,
        has_signal: bool = False,
        has_telemetry: bool = False,
        node_roles: List[str] = Query(default=[]),
    ):
        params = {
            "max_age": max_age, "max_hops": max_hops,
            "named_only": named_only, "has_position": has_position,
            "hide_mqtt": hide_mqtt, "has_signal": has_signal,
            "has_telemetry": has_telemetry, "node_roles": node_roles,
        }
        return await _call(node_id, "get_nodes", params)

    @app.get("/{node_id}/nodes/{num}")
    async def device_node(node_id: str, num: int):
        return await _call(node_id, "get_nodes", {"num": num})

    @app.get("/{node_id}/channels")
    async def device_channels(node_id: str):
        return await _call(node_id, "get_channels", {})

    @app.get("/{node_id}/channels/{index}")
    async def device_channel_live(node_id: str, index: int):
        return await _call(node_id, "get_channel_live", {"index": index})

    @app.put("/{node_id}/channels/{index}")
    async def device_set_channel(node_id: str, index: int, body: dict = Body(...)):
        params = {"index": index}
        if "settings" in body:
            params["settings"] = body["settings"]
        if "role" in body:
            params["role"] = body["role"]
        return await _call(node_id, "set_channel", params)

    @app.get("/{node_id}/config")
    async def device_config(node_id: str):
        return await _call(node_id, "get_config", {})

    @app.get("/{node_id}/config/{section}")
    async def device_config_section(node_id: str, section: str):
        return await _call(node_id, "get_config_live", {"section": section})

    @app.put("/{node_id}/config/{section}")
    async def device_set_config(node_id: str, section: str, body: dict = Body(...)):
        return await _call(node_id, "set_config", {"section": section, "values": body})

    @app.get("/{node_id}/owner")
    async def device_owner(node_id: str):
        return await _call(node_id, "get_owner_live", {})

    @app.put("/{node_id}/owner")
    async def device_set_owner(node_id: str, body: dict = Body(...)):
        return await _call(node_id, "set_owner", body)

    @app.get("/{node_id}/fixed_position")
    async def device_fixed_position(node_id: str):
        return await _call(node_id, "get_fixed_position", {})

    @app.put("/{node_id}/fixed_position")
    async def device_set_fixed_position(node_id: str, body: dict = Body(...)):
        return await _call(node_id, "set_fixed_position", body)

    @app.delete("/{node_id}/fixed_position")
    async def device_delete_fixed_position(node_id: str):
        return await _call(node_id, "remove_fixed_position", {})

    @app.post("/{node_id}/messages")
    async def device_send_text(node_id: str, body: dict = Body(...)):
        return await _call(node_id, "send_text", body)

    @app.post("/{node_id}/admin")
    async def device_admin(node_id: str, body: dict = Body(...)):
        return await _call(node_id, "admin", body)

    @app.post("/{node_id}/traceroute")
    async def device_traceroute(node_id: str, body: dict = Body(...)):
        return await _call(node_id, "traceroute", body)

    @app.post("/{node_id}/rpc")
    async def device_rpc(node_id: str, body: dict = Body(...)):
        bridge = _bridge(node_id)
        fn = METHODS.get(body.get("method"))
        if not fn:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": body.get("id"),
                 "error": {"code": -32601, "message": f"Method not found: {body.get('method')}"}},
                status_code=404,
            )
        try:
            result = await fn(bridge, body.get("params") or {})
            return {"jsonrpc": "2.0", "id": body.get("id"), "result": result}
        except Exception as e:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": body.get("id"),
                 "error": {"code": -32603, "message": str(e)}},
                status_code=500,
            )

    @app.get("/{node_id}/radio_backup")
    async def device_radio_backup(node_id: str):
        """Return all cached config sections and channels as a backup snapshot."""
        bridge = _bridge(node_id)
        return {
            "version": 1,
            "node_id": node_id,
            "ts": int(time.time()),
            "config": bridge.state.config,
            "module_config": bridge.state.module_config,
            "channels": bridge.state.channels,
        }

    @app.post("/{node_id}/radio_restore")
    async def device_radio_restore(node_id: str, body: dict = Body(...)):
        """Write all config sections and channels from a backup, then reboot once."""
        bridge = _bridge(node_id)
        config = body.get("config") or {}
        module_config = body.get("module_config") or {}
        channels = body.get("channels") or []

        n_sections = len([v for v in config.values() if v]) + len([v for v in module_config.values() if v])

        async def send():
            for section, values in config.items():
                if section not in CONFIG_SECTIONS or not isinstance(values, dict) or not values:
                    continue
                await bridge.send_admin({"set_config": {section: values}}, want_response=False)
            for section, values in module_config.items():
                if section not in MODULE_CONFIG_SECTIONS or not isinstance(values, dict) or not values:
                    continue
                await bridge.send_admin({"set_module_config": {section: values}}, want_response=False)
            for ch in channels:
                if not isinstance(ch, dict) or ch.get("index") is None:
                    continue
                channel = {"index": int(ch["index"])}
                if "settings" in ch:
                    channel["settings"] = ch["settings"]
                if "role" in ch:
                    channel["role"] = ch["role"]
                await bridge.send_admin({"set_channel": channel}, want_response=False)

        result = await bridge.write_and_reboot(send)
        return {**result, "sections": n_sections, "channels": len(channels)}

    @app.get("/{node_id}/range_test")
    async def device_range_test(node_id: str):
        bridge = _bridge(node_id)
        return {"log": list(bridge.state.range_test_log), "count": len(bridge.state.range_test_log)}

    @app.delete("/{node_id}/range_test")
    async def device_clear_range_test(node_id: str):
        bridge = _bridge(node_id)
        bridge.state.range_test_log.clear()
        return {"cleared": True}

    # Per-device WebSocket — typed state-machine event stream
    @app.websocket("/{node_id}/events")
    async def ws_device(node_id: str, websocket: WebSocket):
        bridge = _bridge(node_id)
        await websocket.accept()
        # Snapshot: immediate current state so subscriber is never blind on connect
        snapshot = bridge.current_snapshot()
        snapshot["device"] = node_id
        await websocket.send_json(snapshot)
        # Replay recent cached messages (text packets)
        for event in bridge.state.get_cached_messages():
            await websocket.send_json(event)
        q = bridge.state.subscribe()
        try:
            while True:
                event = await q.get()
                await websocket.send_json(event)
        except WebSocketDisconnect:
            pass
        finally:
            bridge.state.unsubscribe(q)

    return app

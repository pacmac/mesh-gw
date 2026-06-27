"""Multi-device REST/WS server.

Routes are device-namespaced under /{node_id}/ (e.g. /!3f172791/nodes).
Server-level routes are flat (/status, /devices, /bridge_config, /events).
No static files — dashboard is a separate service.
CORS enabled for all origins.

Source of truth: docs/BLE-SPEC.md § "module/server.py — HTTP surface only"
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from contextlib import asynccontextmanager
from typing import List

from bleak import BleakClient, BleakScanner
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, Body, Query, Form, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from core import bridge_config as _bcfg
from core.ble_device import _remove_bluez_bond
from core.app_router import AppRouter
from core.bridge_config import update_ble_device
from core.config import load as _load_config
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
    _mcp_mgr: list = []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Load config and start all auto-connect devices
        device_configs, ble_cfg, ota_cfg = _load_config()
        app.state.ota_cfg = ota_cfg
        await dm.reconcile(device_configs, ble_cfg, ota_cfg)
        logger.info("Started %d BLE device(s)", len(dm.all()))

        # Start event drain loop — pulls from shared BleDevice queue, fans out to WS subscribers
        drain_task = asyncio.create_task(_drain_loop(dm), name="event-drain")

        # Start AppRouter — decodes all packets and emits typed events back to dm.queue
        app_router = AppRouter(dm, dm.queue)
        app.state.app_router = app_router
        await app_router.start()

        async with _mcp_mgr[0].run():
            yield

        await app_router.stop()

        drain_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await drain_task

        logger.info("Shutting down all device connections…")
        try:
            await asyncio.wait_for(dm.stop_all(), timeout=15.0)
        except asyncio.TimeoutError:
            logger.warning("Device shutdown timed out")

    app = FastAPI(title="mesh-gw", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=500)

    _mcp_mgr.append(mount_mcp(app, dm))

    # -- helpers --------------------------------------------------------------

    def _device(addr_or_node_id: str):
        dev = dm.get(addr_or_node_id)
        if dev is None:
            raise HTTPException(404, f"Unknown device: {addr_or_node_id}")
        return dev

    async def _call(addr_or_node_id: str, method_name: str, params: dict):
        dev = _device(addr_or_node_id)
        fn = METHODS.get(method_name)
        if not fn:
            raise HTTPException(404, f"Method not found: {method_name}")
        try:
            return await fn(dev, params)
        except NotImplementedError as e:
            raise HTTPException(503, f"Not yet implemented: {e}")
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

    @app.get("/help", response_class=PlainTextResponse)
    async def help_text():
        return HELP_TEXT

    @app.post("/reload")
    async def reload_config():
        return await dm.reload_config()

    @app.get("/status")
    async def server_status():
        return {
            "server": "mesh-gw",
            "devices": dm.list_devices(),
        }

    @app.get("/devices")
    async def list_devices():
        return {
            "devices": [
                {
                    **dev.snapshot,
                    "addr": dev.addr,
                    "node_id": dev.node_id,
                    "state": dev.state,
                }
                for dev in dm.all()
            ]
        }

    @app.post("/devices")
    async def add_device(body: dict = Body(...)):
        """Add a device to config and start connecting."""
        address = (body.get("address") or "").strip().upper()
        pin = str(body.get("pin") or "")
        if not address:
            raise HTTPException(400, "address required")
        tcp_port = body.get("tcp_port") or None
        if tcp_port is not None:
            tcp_port = int(tcp_port)
        display_name = str(body.get("display_name") or "")
        auto_connect = bool(body.get("auto_connect", True))

        if body.get("persist", True):
            cfg = _bcfg.load()
            devices = cfg.get("ble_devices") or []
            addrs = [d.get("address", "").upper() for d in devices]
            if address not in addrs:
                entry: dict = {"address": address, "pin": pin, "auto_connect": auto_connect}
                if tcp_port is not None:
                    entry["tcp_port"] = tcp_port
                if display_name:
                    entry["display_name"] = display_name
                devices.append(entry)
                cfg["ble_devices"] = devices
                _bcfg.save(cfg)
            else:
                # Device already in config — update pin if it changed
                for d in devices:
                    if d.get("address", "").upper() == address:
                        if pin != d.get("pin", ""):
                            d["pin"] = pin
                            cfg["ble_devices"] = devices
                            _bcfg.save(cfg)
                        break

        # Reconcile picks up config changes; then explicitly start if auto_connect
        # (reconcile only calls start() for new addresses, not existing OFFLINE devices)
        await dm.reload_config()
        if auto_connect:
            dev = dm.get_by_ble(address)
            if dev is not None:
                await dev.start()
        return {"connecting": auto_connect, "address": address, "tcp_port": tcp_port}

    @app.post("/devices/{addr}/connect")
    async def connect_device(addr: str):
        """Start connection on an existing device (if it has auto_connect=false)."""
        dev = dm.get_by_ble(addr)
        if dev is None:
            raise HTTPException(404, f"Unknown BLE address: {addr}")
        await dev.start()
        return {"started": True, "addr": dev.addr, "state": dev.state}

    @app.delete("/devices/{addr}")
    async def remove_device(addr: str):
        """Stop and remove a device. Removes from config if persist=true."""
        dev = dm.get(addr)
        if dev is None:
            raise HTTPException(404, f"Unknown device: {addr}")
        ble_addr = dev.addr
        await dev.stop()
        dm.remove(ble_addr)
        return {"stopped": True, "addr": ble_addr}

    @app.patch("/ble_devices/{address}")
    async def patch_ble_device(address: str, body: dict = Body(...)):
        allowed = {"auto_connect", "tcp_port"}
        fields = {k: v for k, v in body.items() if k in allowed}
        if "tcp_port" in fields and fields["tcp_port"] is not None:
            fields["tcp_port"] = int(fields["tcp_port"])
        if not fields:
            raise HTTPException(400, f"No recognised fields. Allowed: {sorted(allowed)}")
        return update_ble_device(address, fields)

    @app.delete("/ble/known/{address}")
    async def ble_remove(address: str):
        """Disconnect, remove from config, and remove BlueZ bond."""
        address = address.upper()
        dev = dm.get_by_ble(address)
        if dev:
            await dev.stop()
            dm.remove(address)
        cfg = _bcfg.load()
        cfg["ble_devices"] = [
            d for d in cfg.get("ble_devices", [])
            if d.get("address", "").upper() != address
        ]
        cfg["known_ble_addresses"] = [
            a for a in cfg.get("known_ble_addresses", [])
            if a.upper() != address
        ]
        _bcfg.save(cfg)
        await _remove_bluez_bond(address)
        return {"removed": True, "address": address}

    # -- OTA ------------------------------------------------------------------

    _releases_cache: dict = {"data": None, "ts": 0.0}
    _RELEASES_TTL = 3600.0

    def _hw_dir(request, hw_model: str):
        """Resolve firmware directory for a hw_model using typed OtaConfig."""
        return request.app.state.ota_cfg.firmware_dir(hw_model)

    @app.get("/ota/firmware")
    async def ota_list_firmware(request: Request, node_id: str = Query(...)):
        from pathlib import Path
        dev = dm.get(node_id)
        hw_model = (dev.data.hw_model or "") if dev else ""
        if not hw_model:
            return {"files": [], "hw_model": None, "error": "hw_model not available yet"}
        hw_dir = _hw_dir(request, hw_model)
        if not hw_dir.exists():
            return {"files": [], "hw_model": hw_model, "dir": str(hw_dir)}
        import re as _re
        _SKIP = {"mt-esp32c3-ota.bin"}
        def _fw_entry(f):
            m = _re.search(r"(\d+\.\d+\.\d+)", f.name)
            version = m.group(1) if m else None
            prepared = bool(version and (hw_dir / version / "nvs_ota_hash.bin").exists())
            ota_ready = f.suffix == ".bin"
            return {"name": f.name, "size": f.stat().st_size, "mtime": int(f.stat().st_mtime),
                    "ota_ready": ota_ready, "version": version, "prepared": prepared}
        files = sorted(
            [_fw_entry(f) for f in hw_dir.iterdir()
             if f.is_file() and f.suffix in {".zip", ".bin"}
             and not f.name.endswith(".factory.bin")
             and not f.name.startswith("littlefs-")
             and not f.name.startswith("nvs_")
             and f.name not in _SKIP],
            key=lambda x: x["mtime"], reverse=True,
        )
        return {"files": files, "hw_model": hw_model, "dir": str(hw_dir)}

    @app.delete("/ota/firmware")
    async def ota_delete_firmware(request: Request, node_id: str = Query(...), filename: str = Query(...)):
        dev = dm.get(node_id)
        hw_model = (dev.data.hw_model or "") if dev else ""
        if not hw_model:
            raise HTTPException(400, "hw_model not available")
        hw_dir = _hw_dir(request, hw_model).resolve()
        target = (hw_dir / filename).resolve()
        if not str(target).startswith(str(hw_dir)):
            raise HTTPException(400, "Invalid filename")
        if not target.exists():
            raise HTTPException(404, "File not found")
        target.unlink()
        return {"deleted": filename}

    @app.get("/ota/releases")
    async def ota_releases():
        import httpx
        now = time.monotonic()
        if _releases_cache["data"] and now - _releases_cache["ts"] < _RELEASES_TTL:
            return _releases_cache["data"]
        url = "https://api.github.com/repos/meshtastic/firmware/releases?per_page=15"
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers={"Accept": "application/vnd.github.v3+json", "User-Agent": "mesh-gw/1.0"})
            r.raise_for_status()
            raw = r.json()
        releases = [
            {"tag": rel["tag_name"], "name": rel["name"], "published": rel["published_at"],
             "prerelease": rel["prerelease"],
             "assets": [{"name": a["name"], "url": a["browser_download_url"], "size": a["size"]}
                        for a in rel["assets"]
                        if a["name"].endswith((".bin", ".zip")) and "ota" not in a["name"].lower()]}
            for rel in raw
        ]
        result = {"releases": releases}
        _releases_cache["data"] = result
        _releases_cache["ts"] = now
        return result

    @app.post("/ota/firmware/upload")
    async def ota_upload_firmware(request: Request, node_id: str = Form(...), file: UploadFile = File(...)):
        dev = dm.get(node_id)
        hw_model = (dev.data.hw_model or "") if dev else ""
        if not hw_model:
            raise HTTPException(400, "hw_model not available — device still syncing?")
        hw_dir = _hw_dir(request, hw_model)
        hw_dir.mkdir(parents=True, exist_ok=True)
        dest = hw_dir / file.filename
        contents = await file.read()
        dest.write_bytes(contents)
        return {"filename": file.filename, "size": len(contents), "hw_model": hw_model, "dir": str(hw_dir)}

    @app.post("/ota/firmware/prepare")
    async def ota_prepare_firmware(request: Request, body: dict = Body(...)):
        """Create versioned folder for a firmware file: copy fw + generate NVS + copy bleota.

        Body: { "node_id": "...", "filename": "firmware-xxx-2.7.26.xxx.bin" }
        Optionally: { "bleota_bin": "/absolute/path/to/mt-esp32c3-ota.bin" }
        """
        from pathlib import Path
        from core.nvs_image import prepare_fw_version
        node_id  = body.get("node_id")
        filename = body.get("filename")
        if not node_id or not filename:
            raise HTTPException(400, "node_id and filename are required")
        dev = dm.get(node_id)
        hw_model = (dev.data.hw_model or "") if dev else ""
        if not hw_model:
            raise HTTPException(400, "hw_model not available — device still syncing?")
        hw_dir = _hw_dir(request, hw_model)
        bleota_override = body.get("bleota_bin")
        bleota = Path(bleota_override) if bleota_override else None
        try:
            result = prepare_fw_version(hw_dir, filename, bleota)
        except (FileNotFoundError, ValueError) as e:
            raise HTTPException(400, str(e))
        return result

    @app.post("/ota/firmware/download")
    async def ota_download_firmware(request: Request, body: dict = Body(...)):
        node_id  = body.get("node_id")
        url      = body.get("url")
        filename = body.get("filename")
        if not node_id or not url or not filename:
            raise HTTPException(400, "node_id, url, and filename are required")
        dev = dm.get(node_id)
        hw_model = (dev.data.hw_model or "") if dev else ""
        if not hw_model:
            raise HTTPException(400, "hw_model not available — device still syncing?")
        hw_dir = _hw_dir(request, hw_model)
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
                        done = 0
                        last_pct = -1
                        with open(dest, "wb") as f:
                            async for chunk in resp.aiter_bytes(65536):
                                f.write(chunk)
                                done += len(chunk)
                                if total:
                                    pct = round(done / total * 100)
                                    if pct != last_pct:
                                        last_pct = pct
                                        await dm._broadcast({"type": "ota_download_progress", "device": node_id,
                                                            "data": {"pct": pct, "done": done, "total": total}})
                await dm._broadcast({"type": "ota_download_complete", "device": node_id, "filename": filename, "size": done})
            except Exception as e:
                logger.exception("OTA download failed for %s", node_id)
                if dest.exists():
                    dest.unlink(missing_ok=True)
                await dm._broadcast({"type": "ota_download_error", "device": node_id, "data": {"error": str(e)}})

        asyncio.create_task(_run())
        return {"started": True, "filename": filename, "dest": str(dest)}

    @app.post("/ota")
    async def ota_update(request: Request, body: dict = Body(...)):
        ble_addr = (body.get("ble_addr") or body.get("ble_address") or "").strip() or None
        fw_name  = body.get("firmware")
        node_id  = body.get("node_id") or ble_addr
        if not fw_name:
            raise HTTPException(400, "firmware is required")
        dev = dm.get(node_id) if node_id else None
        if not dev and ble_addr:
            dev = dm.get_by_ble(ble_addr)
        if not dev:
            raise HTTPException(404, f"Device not found: {node_id or ble_addr}")
        hw_model = dev.data.hw_model or (body.get("hw_model") or "").strip()
        if not hw_model:
            raise HTTPException(400, "hw_model not available — pass hw_model in body or wait for device sync")
        fw_path = _hw_dir(request, hw_model) / fw_name
        if not fw_path.is_file():
            raise HTTPException(400, f"firmware file not found: {fw_path}")
        if dev.state in ("OFFLINE", "OTA_BOOTLOADER_STUCK"):
            await dev.trigger_ota_stuck(str(fw_path))
        else:
            await dev.trigger_ota(str(fw_path))
        return {"started": True, "addr": dev.addr, "firmware": fw_name, "hw_model": hw_model}

    # -- BLE scan -------------------------------------------------------------

    async def _bluez_device_info() -> dict[str, dict]:
        result = {}
        try:
            from dbus_fast.aio import MessageBus
            from dbus_fast import BusType
            bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            introspection = await bus.introspect("org.bluez", "/")
            proxy = bus.get_proxy_object("org.bluez", "/", introspection)
            mgr = proxy.get_interface("org.freedesktop.DBus.ObjectManager")
            objects = await mgr.call_get_managed_objects()
            def _unwrap(v):
                return v.value if hasattr(v, "value") else v
            for _path, interfaces in objects.items():
                dev = interfaces.get("org.bluez.Device1", {})
                addr = _unwrap(dev.get("Address", ""))
                if not addr:
                    continue
                connected = bool(_unwrap(dev.get("Connected", False)))
                raw_uuids = _unwrap(dev.get("UUIDs", []))
                uuids = [_unwrap(u).lower() for u in (raw_uuids or [])]
                result[addr.upper()] = {"connected": connected, "uuids": uuids}
            bus.disconnect()
        except Exception as e:
            logger.debug("BlueZ D-Bus query failed: %s", e)
        return result

    @app.get("/ble/scan")
    async def ble_scan():
        MESHTASTIC_SVC = "6ba1b218-15a8-461f-9fa8-5dcae273eafd"
        BLEOTA_SVC = "4fafc201-1fb5-459e-8fcc-c5c9c331914b"

        bluez_info = await _bluez_device_info()

        # Release stale connections
        active_addrs = {
            dev.addr for dev in dm.all()
            if dev.state in ("CONNECTING", "SYNCING", "READY", "RECONNECTING")
        }
        stale = [addr for addr, info in bluez_info.items()
                 if info["connected"] and addr not in active_addrs]
        if stale:
            for addr in stale:
                try:
                    client = BleakClient(addr)
                    await asyncio.wait_for(client.disconnect(), timeout=3.0)
                    logger.info("Scan pre-flight: released stale connection %s", addr)
                except Exception as e:
                    logger.debug("Stale disconnect %s: %s", addr, e)
            await asyncio.sleep(1.5)

        try:
            found = await BleakScanner.discover(timeout=10.0, return_adv=True)
        except Exception as e:
            if "inprogress" in str(e).lower():
                raise HTTPException(503, "Adapter busy — retry in a few seconds")
            raise HTTPException(500, f"Scan failed: {e}")

        _cfg = _bcfg.load()
        configured = {d["address"].upper() for d in _cfg.get("ble_devices", [])}
        known = {a.upper() for a in _cfg.get("known_ble_addresses", [])}
        result = []
        for addr, (dev, adv) in found.items():
            adv_uuids = [str(u).lower() for u in (adv.service_uuids or [])]
            cached_uuids = bluez_info.get(addr.upper(), {}).get("uuids", [])
            all_uuids = set(adv_uuids) | set(cached_uuids)
            is_mesh = (MESHTASTIC_SVC in all_uuids or BLEOTA_SVC in all_uuids
                       or "meshtastic" in (dev.name or "").lower()
                       or addr.upper() in configured or addr.upper() in known)
            if not is_mesh:
                continue
            result.append({
                "name": dev.name or "Unknown",
                "address": addr,
                "rssi": adv.rssi if adv.rssi is not None else -100,
                "meshtastic": is_mesh,
                "paired": False,   # dbus lookup only — no bluetoothctl
                "trusted": False,
            })

        result.sort(key=lambda x: -x["rssi"])
        return {"devices": result}

    # -- MQTT publisher -------------------------------------------------------

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
        return cfg["mqtt_publish"]

    @app.get("/mqtt_publish/status")
    async def mqtt_publish_status():
        return {"running": False, "note": "MQTT publisher not yet wired in step 5"}

    # -- Aggregated node list --------------------------------------------------

    @app.get("/nodes")
    async def all_nodes_aggregated(
        max_age: int = 0, max_hops: int = 99,
        named_only: bool = False, has_position: bool = False,
        hide_mqtt: bool = False, has_signal: bool = False,
        has_telemetry: bool = False,
        node_roles: List[str] = Query(default=[]),
    ):
        params = {
            "max_age": max_age, "max_hops": max_hops,
            "named_only": named_only, "has_position": has_position,
            "hide_mqtt": hide_mqtt, "has_signal": has_signal,
            "has_telemetry": has_telemetry, "node_roles": node_roles,
        }
        merged: dict = {}
        router = getattr(app.state, "app_router", None)
        for dev in dm.all():
            live_nodes = router.get_nodes(dev.addr) if router else None
            data = await get_nodes(dev, params, nodes_override=live_nodes)
            for k, v in (data.get("nodes") or {}).items():
                if k not in merged or (v.get("last_heard") or 0) > (merged[k].get("last_heard") or 0):
                    merged[k] = v
        return {"total": len(merged), "count": len(merged), "nodes": merged}

    # -- Schema meta ----------------------------------------------------------

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

    # -- All-device WebSocket (spec § "Event queue pattern") ------------------

    @app.websocket("/events")
    async def ws_all(websocket: WebSocket):
        device_filter = websocket.query_params.get("device", "")
        # ?subscribe=topic1,topic2 or ?subscribe=* (None = all)
        sub_param = websocket.query_params.get("subscribe")
        if sub_param and sub_param != "*":
            topics: set[str] | None = {t.strip() for t in sub_param.split(",") if t.strip()}
        else:
            topics = None  # wildcard — send everything

        await websocket.accept()

        # Always send device_snapshot on connect regardless of topic filter
        snapshots = []
        for dev in dm.all():
            snap = dev.snapshot
            if device_filter and snap["addr"] != device_filter.upper():
                continue
            snapshots.append(snap)
        if snapshots:
            await websocket.send_json({"type": "device_snapshot", "devices": snapshots})

        q = dm.subscribe()
        # topics is wrapped in a list so the inner tasks can mutate it
        topic_box: list[set[str] | None] = [topics]
        stop_event = asyncio.Event()

        async def _send_loop():
            while not stop_event.is_set():
                try:
                    event = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if device_filter and event.get("addr", "").upper() != device_filter.upper():
                    continue
                t = topic_box[0]
                if t is not None and event.get("type") not in t:
                    continue
                try:
                    await websocket.send_json(event)
                except Exception:
                    stop_event.set()
                    return

        async def _recv_loop():
            while not stop_event.is_set():
                try:
                    raw = await websocket.receive_text()
                    import json as _json
                    msg = _json.loads(raw)
                    if msg.get("type") == "subscribe":
                        new_topics = msg.get("topics")
                        if new_topics == "*" or new_topics is None:
                            topic_box[0] = None
                        elif isinstance(new_topics, list):
                            topic_box[0] = set(new_topics)
                except WebSocketDisconnect:
                    stop_event.set()
                    return
                except Exception:
                    pass

        send_task = asyncio.create_task(_send_loop())
        recv_task = asyncio.create_task(_recv_loop())
        try:
            await asyncio.gather(send_task, recv_task)
        except Exception:
            pass
        finally:
            stop_event.set()
            send_task.cancel()
            recv_task.cancel()
            dm.unsubscribe(q)

    # =========================================================================
    # Device-namespaced routes  /{node_id}/...
    # node_id = BLE addr (AA:BB:CC:DD:EE:FF) or !hex node_id
    # =========================================================================

    @app.get("/{node_id}/status")
    async def device_status(node_id: str):
        dev = _device(node_id)
        return {
            "addr": dev.addr,
            "node_id": dev.node_id,
            "state": dev.state,
            **dev.snapshot,
        }

    @app.get("/{node_id}/info")
    async def device_info(node_id: str):
        return await _call(node_id, "get_info", {})

    @app.get("/{node_id}/nodes")
    async def device_nodes(
        node_id: str,
        max_age: int = 0, max_hops: int = 99,
        named_only: bool = False, has_position: bool = False,
        hide_mqtt: bool = False, has_signal: bool = False,
        has_telemetry: bool = False,
        node_roles: List[str] = Query(default=[]),
    ):
        dev = _device(node_id)
        router = getattr(app.state, "app_router", None)
        live_nodes = router.get_nodes(dev.addr) if router else None
        return await get_nodes(dev, {
            "max_age": max_age, "max_hops": max_hops,
            "named_only": named_only, "has_position": has_position,
            "hide_mqtt": hide_mqtt, "has_signal": has_signal,
            "has_telemetry": has_telemetry, "node_roles": node_roles,
        }, nodes_override=live_nodes)

    @app.get("/{node_id}/nodes/{num}")
    async def device_node(node_id: str, num: int):
        dev = _device(node_id)
        router = getattr(app.state, "app_router", None)
        live_nodes = router.get_nodes(dev.addr) if router else None
        return await get_nodes(dev, {"num": num}, nodes_override=live_nodes)

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

    @app.get("/{node_id}/messages")
    async def device_get_messages(node_id: str, since_id: str = Query(default=None)):
        return await _call(node_id, "get_messages", {"since_id": since_id})

    @app.post("/{node_id}/messages")
    async def device_send_text(node_id: str, body: dict = Body(...)):
        return await _call(node_id, "send_text", body)

    @app.post("/{node_id}/admin")
    async def device_admin(node_id: str, body: dict = Body(...)):
        return await _call(node_id, "admin", body)

    @app.post("/{node_id}/purge_nodedb")
    async def device_purge_nodedb(node_id: str):
        return await _call(node_id, "purge_nodedb", {})

    @app.post("/{node_id}/traceroute")
    async def device_traceroute(node_id: str, body: dict = Body(...)):
        return await _call(node_id, "traceroute", body)

    @app.post("/{node_id}/rpc")
    async def device_rpc(node_id: str, body: dict = Body(...)):
        dev = _device(node_id)
        fn = METHODS.get(body.get("method"))
        if not fn:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": body.get("id"),
                 "error": {"code": -32601, "message": f"Method not found: {body.get('method')}"}},
                status_code=404,
            )
        try:
            result = await fn(dev, body.get("params") or {})
            return {"jsonrpc": "2.0", "id": body.get("id"), "result": result}
        except Exception as e:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": body.get("id"),
                 "error": {"code": -32603, "message": str(e)}},
                status_code=500,
            )

    @app.get("/{node_id}/radio_backup")
    async def device_radio_backup(node_id: str):
        dev = _device(node_id)
        return {
            "version": 1,
            "node_id": node_id,
            "ts": int(time.time()),
            "config": dev.config,
            "module_config": dev.module_config,
            "channels": dev.channels,
        }

    @app.get("/{node_id}/range_test")
    async def device_range_test(node_id: str):
        dev = _device(node_id)
        msgs = [m for m in dev.messages if m.get("type") == "range_test"]
        return {"log": msgs, "count": len(msgs)}

    return app


async def _drain_loop(dm: DeviceManager) -> None:
    """Drain the shared BleDevice event queue and fan out to all WS subscribers."""
    while True:
        event = await dm.queue.get()
        for q in list(dm._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning(
                    "WS subscriber queue full — dropping %s event", event.get("type")
                )


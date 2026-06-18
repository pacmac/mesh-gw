"""Multi-device REST server.

Routes are device-namespaced under /{node_id}/ (e.g. /!3f172791/nodes).
Server-level routes are flat (/status, /devices, /bridge_config, /events).
No static files — dashboard is a separate service.
CORS enabled for all origins.
"""
import asyncio
import logging

from bleak import BleakScanner
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body, Query
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from core import bridge_config as _bcfg
from core.methods import METHODS, get_nodes
from core.mcp_server import mount_mcp
from core.sections import CONFIG_SECTIONS, MODULE_CONFIG_SECTIONS
from core.schema import get_section_schema, get_channel_schema, get_owner_schema, get_fixed_position_schema
from .device_manager import DeviceManager
from .help import HELP_TEXT

logger = logging.getLogger(__name__)


def _err(code: int, message: str, status: int = 400):
    return JSONResponse({"error": {"code": code, "message": message}}, status_code=status)


def create_app(dm: DeviceManager) -> FastAPI:
    app = FastAPI(title="mesh-rest-bridge-multi")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

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

    mount_mcp(app, dm)

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
                result.append({
                    "name": dev.name or "Unknown",
                    "address": addr,
                    "rssi": adv.rssi if adv.rssi is not None else -100,
                    "meshtastic": is_mesh,
                })
            result = [r for r in result if r["meshtastic"]]
            result.sort(key=lambda x: -x["rssi"])
            return {"devices": result}
        except Exception as e:
            raise HTTPException(500, f"Scan failed: {e}")

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
        return {"config": list(CONFIG_SECTIONS), "module_config": list(MODULE_CONFIG_SECTIONS)}

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

    @app.get("/{node_id}/range_test")
    async def device_range_test(node_id: str):
        bridge = _bridge(node_id)
        return {"log": list(bridge.state.range_test_log), "count": len(bridge.state.range_test_log)}

    @app.delete("/{node_id}/range_test")
    async def device_clear_range_test(node_id: str):
        bridge = _bridge(node_id)
        bridge.state.range_test_log.clear()
        return {"cleared": True}

    # Per-device WebSocket
    @app.websocket("/{node_id}/events")
    async def ws_device(node_id: str, websocket: WebSocket):
        bridge = _bridge(node_id)
        await websocket.accept()
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

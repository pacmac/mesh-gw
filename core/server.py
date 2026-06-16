"""HTTP surface: JSON-RPC 2.0 (/rpc, agent/MCP-friendly), a structured
REST API over the same method registry, and a websocket event stream
(/ws)."""
import asyncio
import logging
import os

from bleak import BleakScanner
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .bridge import MeshBridge
from .methods import METHODS
from .sections import CONFIG_SECTIONS, MODULE_CONFIG_SECTIONS
from .schema import get_section_schema, get_channel_schema, get_owner_schema, get_fixed_position_schema
from . import bridge_config as _bcfg

logger = logging.getLogger(__name__)




def _error_response(req_id, code, message, status):
    return JSONResponse(
        {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}},
        status_code=status,
    )


def create_app(bridge: MeshBridge) -> FastAPI:
    app = FastAPI(title="mesh-rest-bridge")

    async def call(method_name: str, params: dict, req_id=None):
        fn = METHODS.get(method_name)
        if not fn:
            return _error_response(req_id, -32601, f"Method not found: {method_name}", 404)
        try:
            result = await fn(bridge, params)
            if req_id is None:
                return result
            return {"jsonrpc": "2.0", "id": req_id, "result": result}
        except KeyError as e:
            return _error_response(req_id, -32602, f"Missing/invalid param: {e}", 400)
        except TimeoutError as e:
            return _error_response(req_id, -32000, str(e), 504)
        except RuntimeError as e:
            return _error_response(req_id, -32001, str(e), 503)
        except Exception as e:
            logger.exception("Method %s failed", method_name)
            return _error_response(req_id, -32603, str(e), 500)

    # -- JSON-RPC 2.0 -----------------------------------------------------
    @app.post("/rpc")
    async def rpc(body: dict):
        return await call(body.get("method"), body.get("params") or {}, body.get("id"))

    # -- REST: reads --------------------------------------------------------
    @app.get("/info")
    async def get_info():
        return await call("get_info", {})

    @app.get("/status")
    async def get_status():
        return await call("get_status", {})

    @app.get("/nodes")
    async def get_nodes():
        return await call("get_nodes", {})

    @app.get("/nodes/{num}")
    async def get_node(num: int):
        return await call("get_nodes", {"num": num})

    @app.get("/channels")
    async def get_channels():
        return await call("get_channels", {})

    @app.get("/channels/{index}")
    async def get_channel_live(index: int):
        return await call("get_channel_live", {"index": index})

    @app.put("/channels/{index}")
    async def set_channel(index: int, body: dict = Body(...)):
        params = {"index": index}
        if "settings" in body:
            params["settings"] = body["settings"]
        if "role" in body:
            params["role"] = body["role"]
        return await call("set_channel", params)

    @app.get("/bridge_config")
    async def get_bridge_config():
        return await call("get_bridge_config", {})

    @app.put("/bridge_config")
    async def put_bridge_config(body: dict = Body(...)):
        return await call("set_bridge_config", body)

    @app.get("/config")
    async def get_config():
        return await call("get_config", {})

    @app.get("/config/{section}")
    async def get_config_section(section: str):
        return await call("get_config_live", {"section": section})

    @app.put("/config/{section}")
    async def put_config_section(section: str, body: dict = Body(...)):
        return await call("set_config", {"section": section, "values": body})

    @app.get("/owner")
    async def get_owner():
        return await call("get_owner_live", {})

    @app.put("/owner")
    async def put_owner(body: dict = Body(...)):
        return await call("set_owner", body)

    @app.get("/fixed_position")
    async def get_fixed_position():
        return await call("get_fixed_position", {})

    @app.put("/fixed_position")
    async def put_fixed_position(body: dict = Body(...)):
        return await call("set_fixed_position", body)

    @app.delete("/fixed_position")
    async def delete_fixed_position():
        return await call("remove_fixed_position", {})

    # -- REST: writes ---------------------------------------------------------
    @app.post("/messages")
    async def post_message(body: dict = Body(...)):
        return await call("send_text", body)

    @app.post("/admin")
    async def post_admin(body: dict = Body(...)):
        return await call("admin", body)

    @app.post("/yagi/point")
    async def post_yagi_point(body: dict = Body(...)):
        return await call("yagi_point", body)

    @app.get("/range_test")
    async def get_range_test():
        return {"log": list(bridge.state.range_test_log), "count": len(bridge.state.range_test_log)}

    @app.delete("/range_test")
    async def clear_range_test():
        bridge.state.range_test_log.clear()
        return {"cleared": True}

    # -- meta -----------------------------------------------------------------
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
            return _error_response(None, -32602, str(e), 404)

    # -- BLE management -------------------------------------------------------

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

    @app.post("/ble/connect")
    async def ble_connect_endpoint(body: dict = Body(...)):
        address = (body.get("address") or "").strip()
        pin     = (body.get("pin")     or "").strip()
        if not address:
            raise HTTPException(400, "address required")
        # Persist address and pin only when auto_connect is requested
        if body.get("auto_connect", True):
            cfg = _bcfg.load()
            cfg["ble"]["address"] = address
            cfg["ble"]["pin"] = pin
            _bcfg.save(cfg)
        else:
            cfg = _bcfg.load()
            cfg["ble"]["address"] = None
            cfg["ble"]["pin"] = ""
            _bcfg.save(cfg)
        asyncio.create_task(bridge.connect_to(address, pin=pin))
        return {"connecting": True, "address": address}

    @app.post("/ble/disconnect")
    async def ble_disconnect_clear_endpoint():
        cfg = _bcfg.load()
        cfg["ble"]["address"] = None
        _bcfg.save(cfg)
        asyncio.create_task(bridge.disconnect_ble())
        return {"disconnecting": True}

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        queue = bridge.state.subscribe()
        try:
            while True:
                event = await queue.get()
                await websocket.send_json(event)
        except WebSocketDisconnect:
            pass
        finally:
            bridge.state.unsubscribe(queue)

    # -- dashboard (static SPA) ------------------------------------------------
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="dashboard")

    return app

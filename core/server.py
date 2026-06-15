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
from .schema import get_section_schema, get_channel_schema, get_owner_schema

logger = logging.getLogger(__name__)


async def _do_ble_connect(bridge, address: str):
    """Background task: connect to BLE device, retry up to 3 times."""
    for attempt in range(1, 4):
        try:
            await bridge.connect_to(address)
            logger.info(f"Dashboard-initiated BLE connect succeeded: {address}")
            return
        except Exception as e:
            logger.warning(f"BLE connect attempt {attempt}/3 failed: {e}")
            if attempt < 3:
                await asyncio.sleep(5)
    logger.error(f"Dashboard-initiated BLE connect gave up after 3 attempts: {address}")


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

    # -- REST: writes ---------------------------------------------------------
    @app.post("/messages")
    async def post_message(body: dict = Body(...)):
        return await call("send_text", body)

    @app.post("/admin")
    async def post_admin(body: dict = Body(...)):
        return await call("admin", body)

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
            devices = await BleakScanner.discover(timeout=5.0)
            MESHTASTIC_SVC = "6ba1b218-15a8-461f-9fa8-5dcae273eafd"
            result = []
            for d in devices:
                uuids = [str(u).lower() for u in (d.metadata.get("uuids") or [])]
                is_mesh = MESHTASTIC_SVC in uuids or (d.name and any(
                    k in (d.name or "").lower() for k in ("meshtastic", "ta2r", "ta2m")))
                result.append({
                    "name": d.name or "Unknown",
                    "address": d.address,
                    "rssi": d.rssi or -100,
                    "meshtastic": is_mesh,
                })
            result.sort(key=lambda x: (not x["meshtastic"], -x["rssi"]))
            return {"devices": result}
        except Exception as e:
            raise HTTPException(500, f"Scan failed: {e}")

    @app.post("/ble/connect")
    async def ble_connect_endpoint(body: dict = Body(...)):
        address = (body.get("address") or "").strip()
        if not address:
            raise HTTPException(400, "address required")
        asyncio.create_task(_do_ble_connect(bridge, address))
        return {"connecting": True, "address": address}

    @app.post("/ble/disconnect")
    async def ble_disconnect_endpoint():
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

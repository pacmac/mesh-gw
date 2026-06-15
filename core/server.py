"""HTTP surface: JSON-RPC 2.0 (/rpc, agent/MCP-friendly), a structured
REST API over the same method registry, and a websocket event stream
(/ws)."""
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import JSONResponse

from .bridge import MeshBridge
from .methods import METHODS
from .sections import CONFIG_SECTIONS, MODULE_CONFIG_SECTIONS

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

    return app

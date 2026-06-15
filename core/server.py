"""HTTP surface: JSON-RPC 2.0 (/rpc, agent/MCP-friendly), REST read
shortcuts (/nodes, /info, ...), and a websocket event stream (/ws)."""
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from .bridge import MeshBridge
from .methods import METHODS

logger = logging.getLogger(__name__)


def create_app(bridge: MeshBridge) -> FastAPI:
    app = FastAPI(title="mesh-rest-bridge")

    @app.post("/rpc")
    async def rpc(body: dict):
        req_id = body.get("id")
        method_name = body.get("method")
        params = body.get("params") or {}

        fn = METHODS.get(method_name)
        if not fn:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": req_id,
                 "error": {"code": -32601, "message": f"Method not found: {method_name}"}},
                status_code=404,
            )

        try:
            result = await fn(bridge, params)
            return {"jsonrpc": "2.0", "id": req_id, "result": result}
        except KeyError as e:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": req_id,
                 "error": {"code": -32602, "message": f"Missing param: {e}"}},
                status_code=400,
            )
        except TimeoutError as e:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32000, "message": str(e)}},
                status_code=504,
            )
        except RuntimeError as e:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32001, "message": str(e)}},
                status_code=503,
            )
        except Exception as e:
            logger.exception("RPC method %s failed", method_name)
            return JSONResponse(
                {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32603, "message": str(e)}},
                status_code=500,
            )

    # REST shortcuts -- thin GET wrappers around the same registry for
    # read-only methods, handy for curl/dashboards.
    for name in ("get_info", "get_nodes", "get_channels", "get_config", "get_status"):
        path = "/" + name[len("get_"):]

        def make_handler(method_name):
            async def handler():
                return await METHODS[method_name](bridge, {})
            return handler

        app.get(path)(make_handler(name))

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

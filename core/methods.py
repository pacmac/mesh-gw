"""Method registry shared by the JSON-RPC endpoint, REST wrappers, and
(eventually) an MCP tool list -- one place defines what the bridge can do.

Each method is `async def fn(bridge: MeshBridge, params: dict) -> dict`.
"""
from .bridge import MeshBridge

METHODS = {}


def method(name):
    def deco(fn):
        METHODS[name] = fn
        return fn
    return deco


@method("get_info")
async def get_info(bridge: MeshBridge, params: dict):
    return {"my_info": bridge.state.my_info, "metadata": bridge.state.metadata}


@method("get_nodes")
async def get_nodes(bridge: MeshBridge, params: dict):
    return {"nodes": bridge.state.nodes}


@method("get_channels")
async def get_channels(bridge: MeshBridge, params: dict):
    return {"channels": bridge.state.channels}


@method("get_config")
async def get_config(bridge: MeshBridge, params: dict):
    return {"config": bridge.state.config, "module_config": bridge.state.module_config}


@method("get_status")
async def get_status(bridge: MeshBridge, params: dict):
    stats = bridge.state
    return {
        "ble_connected": bridge.ble.client.is_connected if bridge.ble.client else False,
        "config_complete": stats.config_complete,
        "node_count": len(stats.nodes),
    }


@method("send_text")
async def send_text(bridge: MeshBridge, params: dict):
    return await bridge.send_text(
        text=params["text"],
        to=int(params.get("to", 0xFFFFFFFF)),
        channel=int(params.get("channel", 0)),
    )


@method("admin")
async def admin(bridge: MeshBridge, params: dict):
    """Generic AdminMessage passthrough. params: {message, to?, want_response?}"""
    return await bridge.send_admin(
        message=params["message"],
        to=params.get("to"),
        want_response=params.get("want_response", True),
    )


@method("get_radio_config")
async def get_radio_config(bridge: MeshBridge, params: dict):
    """Convenience wrapper: admin get_config_request for a named section,
    e.g. params={"section": "LORA_CONFIG"}"""
    return await bridge.send_admin({"get_config_request": params["section"]})


@method("set_owner")
async def set_owner(bridge: MeshBridge, params: dict):
    """params: {long_name?, short_name?, is_licensed?}"""
    owner = {k: v for k, v in params.items() if k in ("long_name", "short_name", "is_licensed")}
    return await bridge.send_admin({"set_owner": owner})

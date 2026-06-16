"""Method registry shared by the JSON-RPC endpoint, REST wrappers, and
(eventually) an MCP tool list -- one place defines what the bridge can do.

Each method is `async def fn(bridge: MeshBridge, params: dict) -> dict`.
"""
from . import bridge_config
from .bridge import MeshBridge
from .sections import CONFIG_SECTIONS, MODULE_CONFIG_SECTIONS, config_kind

METHODS = {}


def method(name):
    def deco(fn):
        METHODS[name] = fn
        return fn
    return deco


# -- read-only, served from cached state (populated on connect + live) -----

@method("get_info")
async def get_info(bridge: MeshBridge, params: dict):
    return {"my_info": bridge.state.my_info, "metadata": bridge.state.metadata}


@method("get_nodes")
async def get_nodes(bridge: MeshBridge, params: dict):
    if "num" in params:
        node = bridge.state.nodes.get(str(params["num"]))
        if node is None:
            raise KeyError(f"unknown node: {params['num']}")
        return {"node": node}
    return {"nodes": bridge.state.nodes}


@method("get_channels")
async def get_channels(bridge: MeshBridge, params: dict):
    return {"channels": bridge.state.channels}


@method("get_config")
async def get_config(bridge: MeshBridge, params: dict):
    return {"config": bridge.state.config, "module_config": bridge.state.module_config}


@method("get_bridge_config")
async def get_bridge_config(bridge: MeshBridge, params: dict):
    """Bridge-side settings (radar UI defaults, MQTT topic conventions) --
    persisted in core/bridge_config.yaml, separate from the radio's own
    Config/ModuleConfig."""
    return bridge_config.load()


@method("set_bridge_config")
async def set_bridge_config(bridge: MeshBridge, params: dict):
    """params: a (partial) bridge config dict, deep-merged onto defaults
    and the existing file, then persisted."""
    current = bridge_config.load()
    merged = bridge_config._deep_merge(current, params)
    return bridge_config.save(merged)


@method("get_status")
async def get_status(bridge: MeshBridge, params: dict):
    state = bridge.state
    ble_connected = bool(bridge.ble and bridge.ble.client and bridge.ble.client.is_connected)
    return {
        "ble_connected": ble_connected,
        "ble_address": bridge.ble_address,
        "ble_state": bridge.ble_state,
        "ble_error": bridge.ble_error,
        "config_complete": state.config_complete,
        "node_count": len(state.nodes),
        "mqtt_proxy_connected": bool(bridge.mqtt_proxy and bridge.mqtt_proxy.connected),
        "last_rx_snr": state.last_rx_snr,
        "last_rx_rssi": state.last_rx_rssi,
    }


# -- live admin reads (round-trip to the radio) ------------------------------

@method("get_config_live")
async def get_config_live(bridge: MeshBridge, params: dict):
    """Live admin fetch of a config or module_config section by name,
    e.g. params={"section": "lora"} or {"section": "mqtt"}."""
    section = params["section"]
    kind = config_kind(section)
    if kind == "config":
        resp = await bridge.send_admin({"get_config_request": CONFIG_SECTIONS[section]})
        return resp.get("get_config_response", {})
    resp = await bridge.send_admin({"get_module_config_request": MODULE_CONFIG_SECTIONS[section]})
    return resp.get("get_module_config_response", {})


@method("get_channel_live")
async def get_channel_live(bridge: MeshBridge, params: dict):
    resp = await bridge.send_admin({"get_channel_request": int(params["index"]) + 1})
    return resp.get("get_channel_response", {})


@method("get_owner_live")
async def get_owner_live(bridge: MeshBridge, params: dict):
    resp = await bridge.send_admin({"get_owner_request": True})
    return resp.get("get_owner_response", {})


# -- writes -------------------------------------------------------------------

# These admin messages are fire-and-forget in the Meshtastic protocol --
# the device applies them but never sends a reply, so we'd otherwise
# always time out waiting for one.

@method("set_config")
async def set_config(bridge: MeshBridge, params: dict):
    """params: {"section": "lora", "values": {...}}"""
    section = params["section"]
    kind = config_kind(section)
    key = "set_config" if kind == "config" else "set_module_config"
    return await bridge.send_admin({key: {section: params["values"]}}, want_response=False)


@method("set_channel")
async def set_channel(bridge: MeshBridge, params: dict):
    """params: {"index": int, "settings": {...}, "role": "..."?}"""
    channel = {"index": int(params["index"])}
    if "settings" in params:
        channel["settings"] = params["settings"]
    if "role" in params:
        channel["role"] = params["role"]
    return await bridge.send_admin({"set_channel": channel}, want_response=False)


@method("set_owner")
async def set_owner(bridge: MeshBridge, params: dict):
    """params: {long_name?, short_name?, is_licensed?}"""
    owner = {k: v for k, v in params.items() if k in ("long_name", "short_name", "is_licensed")}
    return await bridge.send_admin({"set_owner": owner}, want_response=False)


@method("get_fixed_position")
async def get_fixed_position(bridge: MeshBridge, params: dict):
    """Returns the device's own last-known position, which reflects the
    fixed position once Config.PositionConfig.fixed_position is set."""
    num = bridge.my_node_num
    node = bridge.state.nodes.get(str(num), {}) if num is not None else {}
    return {"position": node.get("position", {})}


@method("set_fixed_position")
async def set_fixed_position(bridge: MeshBridge, params: dict):
    """params: {latitude_i, longitude_i, altitude?}"""
    position = {k: v for k, v in params.items() if k in ("latitude_i", "longitude_i", "altitude")}
    return await bridge.send_admin({"set_fixed_position": position}, want_response=False)


@method("remove_fixed_position")
async def remove_fixed_position(bridge: MeshBridge, params: dict):
    return await bridge.send_admin({"remove_fixed_position": True}, want_response=False)


@method("send_text")
async def send_text(bridge: MeshBridge, params: dict):
    return await bridge.send_text(
        text=params["text"],
        to=int(params.get("to", 0xFFFFFFFF)),
        channel=int(params.get("channel", 0)),
    )


@method("yagi_point")
async def yagi_point(bridge: MeshBridge, params: dict):
    """params: {az, node?, name?}. Publishes the target azimuth for the Yagi
    rotator over MQTT (full rotator integration is a future task -- this just
    gets the command onto the broker in a stable shape)."""
    if not bridge.mqtt_proxy or not bridge.mqtt_proxy.connected:
        raise RuntimeError("MQTT proxy not connected")
    import json
    payload = json.dumps({
        "az": float(params["az"]),
        "node": params.get("node"),
        "name": params.get("name", ""),
    })
    bridge.mqtt_proxy.publish("yagi/point", payload.encode(), retained=False)
    return {"published": True}


@method("admin")
async def admin(bridge: MeshBridge, params: dict):
    """Generic AdminMessage passthrough. params: {message, to?, want_response?}"""
    return await bridge.send_admin(
        message=params["message"],
        to=params.get("to"),
        want_response=params.get("want_response", True),
    )

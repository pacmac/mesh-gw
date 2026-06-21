"""Method registry shared by the JSON-RPC endpoint, REST wrappers, and
(eventually) an MCP tool list -- one place defines what the bridge can do.

Each method is `async def fn(bridge: MeshBridge, params: dict) -> dict`.
"""
from fastapi import HTTPException

from .bridge import MeshBridge
from .sections import CONFIG_SECTIONS, MODULE_CONFIG_SECTIONS, config_kind

METHODS = {}


def method(name):
    def deco(fn):
        METHODS[name] = fn
        return fn
    return deco


# -- read-only, served from cached state (populated on connect + live) -----

@method("get_messages")
async def get_messages(bridge: MeshBridge, params: dict):
    """Return cached received text messages, newest last. Optional since_id to skip already-seen."""
    msgs = bridge.state.get_cached_messages()
    since_id = params.get("since_id")
    if since_id:
        ids = [m.get("id") for m in msgs]
        if since_id in ids:
            msgs = msgs[ids.index(since_id) + 1:]
    return {"messages": msgs, "count": len(msgs)}


@method("wait_for_message")
async def wait_for_message(bridge: MeshBridge, params: dict):
    """Block until a text message arrives (long-poll). Returns immediately when one comes in."""
    import asyncio, base64
    timeout = min(int(params.get("timeout", 55)), 55)
    from_node = params.get("from")  # optional: only match this sender (!hex or numeric)

    q = bridge.state.subscribe()
    try:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return {"arrived": False, "message": None}
            try:
                event = await asyncio.wait_for(q.get(), timeout=remaining)
            except asyncio.TimeoutError:
                return {"arrived": False, "message": None}

            pkt = event.get("data", {}).get("packet", {})
            decoded = pkt.get("decoded", {})
            if decoded.get("portnum") != "TEXT_MESSAGE_APP":
                continue

            sender_num = pkt.get("from")
            if from_node:
                # match by !hex string or numeric
                if from_node.startswith("!"):
                    if sender_num != int(from_node[1:], 16):
                        continue
                elif str(sender_num) != str(from_node):
                    continue

            text = base64.b64decode(decoded["payload"]).decode("utf-8", errors="replace")
            return {
                "arrived": True,
                "message": {
                    "id": pkt.get("id"),
                    "from": f"!{sender_num:08x}" if sender_num else None,
                    "text": text,
                    "rx_time": pkt.get("rx_time"),
                    "rssi": pkt.get("rx_rssi"),
                    "snr": pkt.get("rx_snr"),
                }
            }
    finally:
        bridge.state.unsubscribe(q)


@method("get_info")
async def get_info(bridge: MeshBridge, params: dict):
    return {"my_info": bridge.state.my_info, "metadata": bridge.state.metadata}


@method("get_nodes")
async def get_nodes(bridge: MeshBridge, params: dict):
    import time
    if "num" in params:
        node = bridge.state.nodes.get(str(params["num"]))
        if node is None:
            raise KeyError(f"unknown node: {params['num']}")
        return {"node": node}

    all_nodes = bridge.state.nodes
    total = len(all_nodes)

    max_age      = int(params.get("max_age", 0))
    max_hops     = int(params.get("max_hops", 99))
    named_only   = bool(params.get("named_only", False))
    has_position = bool(params.get("has_position", False))
    hide_mqtt    = bool(params.get("hide_mqtt", False))
    has_signal   = bool(params.get("has_signal", False))
    has_telemetry= bool(params.get("has_telemetry", False))
    node_roles   = params.get("node_roles") or []
    if isinstance(node_roles, str):
        node_roles = [node_roles]

    no_filter = (max_age == 0 and max_hops == 99 and not named_only
                 and not has_position and not hide_mqtt
                 and not has_signal and not has_telemetry and not node_roles)
    if no_filter:
        return {"total": total, "count": total, "filter": {}, "nodes": all_nodes}

    now = int(time.time())
    active_filters = {}
    if max_age:      active_filters["max_age"] = max_age
    if max_hops < 99: active_filters["max_hops"] = max_hops
    if named_only:   active_filters["named_only"] = True
    if has_position: active_filters["has_position"] = True
    if hide_mqtt:    active_filters["hide_mqtt"] = True
    if has_signal:   active_filters["has_signal"] = True
    if has_telemetry:active_filters["has_telemetry"] = True
    if node_roles:   active_filters["node_roles"] = node_roles

    filtered = {}
    for key, node in all_nodes.items():
        if max_age and (now - (node.get("last_heard") or 0)) > max_age:
            continue
        if max_hops < 99 and (node.get("hops") or 0) > max_hops:
            continue
        if named_only and not (node.get("user") or {}).get("long_name"):
            continue
        if has_position and not node.get("position"):
            continue
        if hide_mqtt and node.get("via_mqtt") is True:
            continue
        if has_signal and node.get("snr") is None and node.get("rssi") is None:
            continue
        if has_telemetry and not node.get("device_metrics"):
            continue
        if node_roles:
            effective_role = (node.get("user") or {}).get("role") or "CLIENT"
            if effective_role not in node_roles:
                continue
        filtered[key] = node

    return {"total": total, "count": len(filtered), "filter": active_filters, "nodes": filtered}


@method("get_channels")
async def get_channels(bridge: MeshBridge, params: dict):
    return {"channels": bridge.state.channels}


@method("get_config")
async def get_config(bridge: MeshBridge, params: dict):
    return {"config": bridge.state.config, "module_config": bridge.state.module_config}


@method("get_status")
async def get_status(bridge: MeshBridge, params: dict):
    state = bridge.state
    ble_connected = bool(bridge.ble and bridge.ble.client and bridge.ble.client.is_connected)
    return {
        "ble_connected": ble_connected,
        "ble_address": bridge.ble_address,
        "ble_state": bridge.ble_state,
        "ble_error": bridge.ble_error,
        "ble_rssi": (ble_rssi := bridge.ble.get_rssi() if bridge.ble else None),
        "ble_rssi_pct": max(0, min(100, round((ble_rssi + 100) / 60 * 100))) if ble_rssi is not None else None,
        "config_complete": state.config_complete,
        "node_count": len(state.nodes),
        "my_node_num": bridge.my_node_num,
        "has_my_info": bool(state.my_info),
        "has_mqtt_config": state.mqtt_config_ready.is_set(),
        "last_rx_snr": state.last_rx_snr,
        "last_rx_rssi": state.last_rx_rssi,
        "mqtt_proxy_connected": bridge._mqtt_proxy is not None and not bridge._mqtt_proxy._stopped,
        "ready": bridge.ble_state == "ready" and state.config_complete,
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

_FORCED_VALUES: dict[str, dict] = {
    "range_test": {"enabled": True},
}


_PASSWORD_FIELDS: dict[str, set[str]] = {
    "mqtt":    {"password"},
    "network": {"wifi_psk"},
    "bluetooth": {"fixed_pin"},  # 0 is default; skip if not explicitly set
}

def _merge_config(cached: dict, submitted: dict) -> dict:
    """Deep-merge submitted values onto cached state.

    null submitted values keep the cached value — the form sent the key
    (satisfying key validation) but the user left the field blank/masked.
    Non-null values override. Nested dicts are merged recursively.
    """
    result = dict(cached)
    for k, v in submitted.items():
        if v is None:
            pass  # keep cached
        elif isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _merge_config(result[k], v)
        else:
            result[k] = v
    return result


def _validate_config_keys(bridge: MeshBridge, section: str, values: dict):
    """Reject submissions where any key from the cached section is absent.

    set_module_config replaces the entire section on the radio, so a partial
    submission silently wipes every missing field. All keys must be present;
    null values are acceptable, but absent keys are not.

    Exemptions:
    - _HIDDEN_FIELDS: never shown in the form, server manages them
    - _FORCED_VALUES: always set server-side regardless of submission
    - _PASSWORD_FIELDS: masked in the form; absence means "don't change"
    """
    from .schema import _HIDDEN_FIELDS
    kind = config_kind(section)
    cached = (bridge.state.module_config if kind == "module_config" else bridge.state.config).get(section, {})
    if not cached:
        return  # no cached state yet — radio not synced, can't validate
    exempt = (
        _HIDDEN_FIELDS.get(section, set())
        | set(_FORCED_VALUES.get(section, {}).keys())
        | _PASSWORD_FIELDS.get(section, set())
    )
    required = {k for k in cached if k not in exempt}
    missing = required - values.keys()
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required fields for section '{section}': {sorted(missing)}")


async def _write_and_verify(bridge: MeshBridge, send_fn, timeout: int = 55) -> dict:
    """Global closed-loop for every config write: send, reboot, wait for reconnect.

    Broadcasts config_save_start so the UI can show reconnecting state, then waits
    for the device to come back online with a full config sync before returning.
    Raises RuntimeError on timeout so the caller's asyncOp shows an error toast.
    """
    await bridge.state._broadcast({"type": "config_save_start"})
    await send_fn()
    await asyncio.sleep(0.3)
    try:
        await bridge.send_admin({"reboot": True}, want_response=False)
    except Exception:
        pass

    loop = asyncio.get_event_loop()

    # Wait up to 12s for disconnect to begin
    deadline = loop.time() + 12
    while loop.time() < deadline and bridge.ble_state == "ready":
        await asyncio.sleep(0.5)

    # Wait for full reconnect + config sync
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if bridge.ble_state == "ready" and bridge.state.config_complete:
            return {"verified": True}
        await asyncio.sleep(1)

    raise RuntimeError(f"Device did not reconnect within {timeout}s — config may not have applied")


@method("set_config")
async def set_config(bridge: MeshBridge, params: dict):
    """params: {"section": "lora", "values": {...}}"""
    section = params["section"]
    submitted = params["values"]
    kind = config_kind(section)
    _validate_config_keys(bridge, section, submitted)
    cached = (bridge.state.module_config if kind == "module_config" else bridge.state.config).get(section, {})
    values = _merge_config(cached, submitted)
    values.update(_FORCED_VALUES.get(section, {}))
    for field in _PASSWORD_FIELDS.get(section, set()):
        if values.get(field) == "" or values.get(field) is None:
            values.pop(field, None)
    key = "set_config" if kind == "config" else "set_module_config"

    async def send():
        await bridge.send_admin({key: {section: values}}, want_response=False)

    return await _write_and_verify(bridge, send)


@method("set_channel")
async def set_channel(bridge: MeshBridge, params: dict):
    """params: {"index": int, "settings": {...}, "role": "..."?}"""
    channel = {"index": int(params["index"])}
    if "settings" in params:
        channel["settings"] = params["settings"]
    if "role" in params:
        channel["role"] = params["role"]

    async def send():
        await bridge.send_admin({"set_channel": channel}, want_response=False)

    return await _write_and_verify(bridge, send)


@method("set_owner")
async def set_owner(bridge: MeshBridge, params: dict):
    """params: {long_name?, short_name?, is_licensed?}. No reboot needed."""
    owner = {k: v for k, v in params.items() if k in ("long_name", "short_name", "is_licensed")}
    await bridge.send_admin({"set_owner": owner}, want_response=False)
    return {"verified": True}


@method("get_fixed_position")
async def get_fixed_position(bridge: MeshBridge, params: dict):
    num = bridge.my_node_num
    node = bridge.state.nodes.get(str(num), {}) if num is not None else {}
    return {"position": node.get("position", {})}


@method("set_fixed_position")
async def set_fixed_position(bridge: MeshBridge, params: dict):
    """params: {latitude_i, longitude_i, altitude?}"""
    position = {k: v for k, v in params.items() if k in ("latitude_i", "longitude_i", "altitude")}

    async def send():
        await bridge.send_admin({"set_fixed_position": position}, want_response=False)

    return await _write_and_verify(bridge, send)


@method("remove_fixed_position")
async def remove_fixed_position(bridge: MeshBridge, params: dict):
    async def send():
        await bridge.send_admin({"remove_fixed_position": True}, want_response=False)

    return await _write_and_verify(bridge, send)


def _parse_node_num(value, default=0xFFFFFFFF) -> int:
    """Accept decimal int, '!hex' node ID string, or plain hex string."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if s.startswith("!"):
        return int(s[1:], 16)
    try:
        return int(s)
    except ValueError:
        return int(s, 16)


@method("send_text")
async def send_text(bridge: MeshBridge, params: dict):
    return await bridge.send_text(
        text=params["text"],
        to=_parse_node_num(params.get("to")),
        channel=int(params.get("channel", 0)),
        reply_id=int(params["reply_id"]) if params.get("reply_id") else None,
    )


@method("admin")
async def admin(bridge: MeshBridge, params: dict):
    """Generic AdminMessage passthrough. params: {message, to?, want_response?}"""
    return await bridge.send_admin(
        message=params["message"],
        to=params.get("to"),
        want_response=params.get("want_response", True),
    )


@method("traceroute")
async def traceroute(bridge: MeshBridge, params: dict):
    """Send a traceroute request. params: {to: node_num}. Response arrives as WS TRACEROUTE_APP packet."""
    return await bridge.send_traceroute(to=int(params["to"]))

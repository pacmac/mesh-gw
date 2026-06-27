"""Method registry shared by the JSON-RPC endpoint, REST wrappers, and
(eventually) an MCP tool list -- one place defines what the bridge can do.

Each method is `async def fn(bridge, params: dict) -> dict`.
"""
import asyncio
import logging

from fastapi import HTTPException

from .sections import CONFIG_SECTIONS, MODULE_CONFIG_SECTIONS, REBOOT_SECTIONS, SECTION_META, config_kind

logger = logging.getLogger(__name__)

METHODS = {}


def method(name):
    def deco(fn):
        METHODS[name] = fn
        return fn
    return deco


# -- read-only, served from cached state (populated on connect + live) -----

@method("get_messages")
async def get_messages(bridge, params: dict):
    """Return cached received text messages, newest last. Optional since_id to skip already-seen."""
    msgs = list(bridge.messages)
    since_id = params.get("since_id")
    if since_id:
        ids = [m.get("id") for m in msgs]
        if since_id in ids:
            msgs = msgs[ids.index(since_id) + 1:]
    return {"messages": msgs, "count": len(msgs)}


@method("wait_for_message")
async def wait_for_message(bridge, params: dict):
    """Block until a text message arrives (long-poll)."""
    raise NotImplementedError("wait_for_message not yet wired to shared event queue")


@method("get_info")
async def get_info(bridge, params: dict):
    return {"my_info": bridge.my_info, "metadata": bridge.metadata}


@method("get_nodes")
async def get_nodes(bridge, params: dict):
    import time
    if "num" in params:
        node = bridge.nodes.get(str(params["num"]))
        if node is None:
            raise KeyError(f"unknown node: {params['num']}")
        return {"node": node}

    all_nodes = bridge.nodes
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
async def get_channels(bridge, params: dict):
    return {"channels": bridge.channels}


@method("get_config")
async def get_config(bridge, params: dict):
    return {"config": bridge.config, "module_config": bridge.module_config}


@method("get_status")
async def get_status(bridge, params: dict):
    return {
        "addr": bridge.addr,
        "node_id": bridge.node_id,
        "state": bridge.state,
        **bridge.snapshot,
    }


# -- live admin reads (round-trip to the radio) ------------------------------

@method("get_config_live")
async def get_config_live(bridge, params: dict):
    """Live admin fetch — falls back to cached state if send_admin not yet implemented."""
    section = params["section"]
    if not hasattr(bridge, "send_admin"):
        # Return cached state with spec metadata injected
        kind = config_kind(section)
        cached = (bridge.module_config if kind == "module_config" else bridge.config).get(section, {})
        return {section: {**cached, **SECTION_META.get(section, {})}}
    kind = config_kind(section)
    if kind == "config":
        resp = await bridge.send_admin({"get_config_request": CONFIG_SECTIONS[section]})
        data = resp.get("get_config_response", {})
    else:
        resp = await bridge.send_admin({"get_module_config_request": MODULE_CONFIG_SECTIONS[section]})
        data = resp.get("get_module_config_response", {})
    inner = data.get(section, {})
    if isinstance(inner, dict):
        data = {**data, section: {**inner, **SECTION_META.get(section, {})}}
    return data


@method("get_channel_live")
async def get_channel_live(bridge, params: dict):
    if not hasattr(bridge, "send_admin"):
        idx = int(params["index"])
        channels = bridge.channels
        ch = channels[idx] if 0 <= idx < len(channels) else {}
        return ch
    resp = await bridge.send_admin({"get_channel_request": int(params["index"]) + 1})
    return resp.get("get_channel_response", {})


@method("get_owner_live")
async def get_owner_live(bridge, params: dict):
    if not hasattr(bridge, "send_admin"):
        d = bridge.data
        return {
            "short_name": d.short_name,
            "long_name": d.long_name,
            **SECTION_META.get("owner", {}),
        }
    resp = await bridge.send_admin({"get_owner_request": True})
    data = resp.get("get_owner_response", {})
    return {**data, **SECTION_META.get("owner", {})}


# -- writes -------------------------------------------------------------------

# These admin messages are fire-and-forget in the Meshtastic protocol --
# the device applies them but never sends a reply, so we'd otherwise
# always time out waiting for one.

_FORCED_VALUES: dict[str, dict] = {}


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


def _validate_config_keys(bridge, section: str, values: dict):
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
    cached = (bridge.module_config if kind == "module_config" else bridge.config).get(section, {})
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


async def _write_direct(bridge, send_fn) -> dict:
    """Write config and return. No reboot — change takes effect live."""
    if not hasattr(bridge, "send_admin"):
        raise NotImplementedError("Admin writes not yet implemented (step 5+)")
    if bridge.state != "READY":
        raise RuntimeError(f"Device not connected (state={bridge.state})")
    try:
        await asyncio.wait_for(send_fn(), timeout=5)
    except asyncio.TimeoutError:
        raise RuntimeError("Write timed out — BLE too slow")
    except Exception as e:
        raise RuntimeError(f"Write failed — {e}")
    return {"verified": True}


@method("set_config")
async def set_config(bridge, params: dict):
    """params: {"section": "lora", "values": {...}}

    __ prefixed keys (e.g. __reboot) are metadata injected by get_config_live
    for UI/pipeline use. Strip them here so they are never sent to the radio.
    """
    section = params["section"]
    # Strip __ metadata fields — they are UI hints, not radio config.
    submitted = {k: v for k, v in params["values"].items() if not k.startswith("__")}
    kind = config_kind(section)
    _validate_config_keys(bridge, section, submitted)
    cached = (bridge.module_config if kind == "module_config" else bridge.config).get(section, {})
    values = _merge_config(cached, submitted)
    values.update(_FORCED_VALUES.get(section, {}))
    for field in _PASSWORD_FIELDS.get(section, set()):
        if values.get(field) == "" or values.get(field) is None:
            values.pop(field, None)
    key = "set_config" if kind == "config" else "set_module_config"

    async def send():
        await bridge.send_admin({key: {section: values}}, want_response=False)

    if section in REBOOT_SECTIONS:
        return await bridge.write_and_reboot(send)
    return await _write_direct(bridge, send)


@method("set_channel")
async def set_channel(bridge, params: dict):
    """params: {"index": int, "settings": {...}, "role": "..."?}"""
    channel = {"index": int(params["index"])}
    if "settings" in params:
        channel["settings"] = params["settings"]
    if "role" in params:
        channel["role"] = params["role"]

    async def send():
        await bridge.send_admin({"set_channel": channel}, want_response=False)

    return await _write_direct(bridge, send)


_OWNER_FIELDS = {"long_name", "short_name", "is_licensed", "role", "is_unmessagable"}

@method("set_owner")
async def set_owner(bridge, params: dict):
    """params: {long_name?, short_name?, is_licensed?, role?, is_unmessagable?}

    role lives in DeviceConfig (not User) — route it through set_config("device")
    so it actually persists across reboots. Other owner fields go via setOwner.
    __ metadata keys are ignored.
    """
    owner = {k: v for k, v in params.items() if k in _OWNER_FIELDS}
    role = owner.pop("role", None)

    # Write non-role owner fields first if any
    result = {"verified": True}
    if owner:
        async def send_owner():
            await bridge.send_admin({"set_owner": owner}, want_response=False)
        result = await _write_direct(bridge, send_owner)

    if role is not None:
        device_cfg = bridge.config.get("device", {})
        if not device_cfg:
            raise RuntimeError("Device config not synced — cannot safely update role")
        return await set_config(bridge, {"section": "device", "values": {**device_cfg, "role": role}})

    return result

    return {"verified": True}


@method("get_fixed_position")
async def get_fixed_position(bridge, params: dict):
    num = bridge._own_node_num
    node = bridge.nodes.get(str(num), {}) if num is not None else {}
    return {"position": node.get("position", {})}


@method("set_fixed_position")
async def set_fixed_position(bridge, params: dict):
    """params: {latitude_i, longitude_i, altitude?}"""
    if not hasattr(bridge, "send_admin"):
        raise NotImplementedError("set_fixed_position not yet implemented (step 5+)")
    position = {k: v for k, v in params.items() if k in ("latitude_i", "longitude_i", "altitude")}

    async def send():
        await bridge.send_admin({"set_fixed_position": position}, want_response=False)

    return await _write_direct(bridge, send)


@method("remove_fixed_position")
async def remove_fixed_position(bridge, params: dict):
    if not hasattr(bridge, "send_admin"):
        raise NotImplementedError("remove_fixed_position not yet implemented (step 5+)")

    async def send():
        await bridge.send_admin({"remove_fixed_position": True}, want_response=False)

    return await _write_direct(bridge, send)


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
async def send_text(bridge, params: dict):
    if not hasattr(bridge, "send_text_message"):
        raise NotImplementedError("send_text not yet implemented (BleDevice admin methods are step 5+)")
    return await bridge.send_text_message(
        text=params["text"],
        to=_parse_node_num(params.get("to")),
        channel=int(params.get("channel", 0)),
        reply_id=int(params["reply_id"]) if params.get("reply_id") else None,
    )


@method("admin")
async def admin(bridge, params: dict):
    """Generic AdminMessage passthrough. params: {message, to?, want_response?}"""
    if not hasattr(bridge, "send_admin"):
        raise NotImplementedError("admin not yet implemented (step 5+)")
    return await bridge.send_admin(
        message=params["message"],
        to=params.get("to"),
        want_response=params.get("want_response", True),
    )


@method("traceroute")
async def traceroute(bridge, params: dict):
    """Send a traceroute request. params: {to: node_num}. Response arrives as WS TRACEROUTE_APP packet."""
    if not hasattr(bridge, "send_traceroute"):
        raise NotImplementedError("traceroute not yet implemented (step 5+)")
    return await bridge.send_traceroute(to=int(params["to"]))

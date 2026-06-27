"""Bridge connection config — BLE device addresses persisted across restarts."""
import logging
import os

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "bridge_config.yaml")

# Default OTA dir used when ota.dir is empty — auto-created on first use.
DEFAULT_OTA_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "ota_firmware"))

_BLE_DEVICE_DEFAULTS = {
    "auto_connect": True,   # connect automatically on bridge startup
    "tcp_port":    None,    # Meshtastic TCP port (4403 convention); None = disabled
}

DEFAULTS = {
    "ble_devices": [],  # list of {address, tcp_port?, auto_connect?, hw_model?, lora_region?}
    # Per-logger level overrides. Set a logger name to DEBUG/INFO/WARNING/ERROR.
    # Example: {"core.state": "DEBUG", "core.ble_handler": "WARNING"}
    "logging": {},
    "message_cache": {
        "enabled":          False,
        "max_messages":     100,
        "max_age_seconds":  86400,
    },
    "mqtt_publish": {
        "enabled":              False,
        "broker":               "localhost",
        "port":                 1883,
        "username":             "",
        "password":             "",
        "use_tls":              False,
        "topic_prefix":         "mesh",
        "ha_discovery":         False,
        "ha_discovery_prefix":  "homeassistant",
    },
    "ota": {
        "dir": "",      # root directory; firmware stored in {dir}/{hw_model}/
    },
    "claude_chat": {
        "enabled":          False,
        "trigger_word":     "@claude",
        "system_prompt":    "You are Claude, accessible via Meshtastic radio. Keep replies concise — this is a low-bandwidth radio link.",
        "max_history":      20,
        "max_reply_length": 200,
        "whitelist":        [],           # !hex node IDs allowed; empty = my_nodes only
        "my_nodes":         [],           # own node IDs (!hex) — always allowed
    },
}


def _deep_merge(base: dict, overrides: dict) -> dict:
    result = dict(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load() -> dict:
    try:
        with open(CONFIG_PATH) as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        data = {}
    except Exception as e:
        logger.warning(f"Failed to read {CONFIG_PATH}: {e}")
        data = {}
    return _deep_merge(DEFAULTS, data)


def save(data: dict) -> dict:
    merged = _deep_merge(DEFAULTS, data)
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(merged, f, default_flow_style=False, sort_keys=False)
    return merged


def get_ble_device(address: str) -> dict:
    """Return the persisted entry for a BLE address, merged with defaults."""
    devices = load().get("ble_devices") or []
    for d in devices:
        if d.get("address", "").upper() == address.upper():
            return {**_BLE_DEVICE_DEFAULTS, **d}
    return {**_BLE_DEVICE_DEFAULTS, "address": address.upper()}


def update_ble_device(address: str, fields: dict) -> dict:
    """Persist field updates for a BLE device entry. Creates entry if absent."""
    cfg = load()
    devices = cfg.get("ble_devices") or []
    addr_upper = address.upper()
    for entry in devices:
        if entry.get("address", "").upper() == addr_upper:
            entry.update(fields)
            break
    else:
        devices.append({"address": addr_upper, **_BLE_DEVICE_DEFAULTS, **fields})
    cfg["ble_devices"] = devices
    save(cfg)
    return get_ble_device(addr_upper)

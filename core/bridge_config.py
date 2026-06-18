"""Bridge connection config — BLE device addresses/pins persisted across restarts."""
import logging
import os

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "bridge_config.yaml")

DEFAULTS = {
    "ble_devices": [],  # list of {address, pin, tcp_port?} to auto-connect on startup
    # Fallback MQTT credentials — used when the radio's firmware redacts the
    # password from its config response (common in Meshtastic firmware).
    "mqtt_credentials": {
        "password": "",
    },
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

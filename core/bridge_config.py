"""Bridge-side configuration: small persisted YAML file for settings that
live in the bridge itself (not on the radio), e.g. radar UI defaults and
MQTT topic conventions used for the ESP32-compatible nodeinfo cache.

Distinct from core/methods.py's get_config/set_config, which round-trip
the radio's own protobuf Config/ModuleConfig.
"""
import logging
import os

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "bridge_config.yaml")

DEFAULTS = {
    "ble": {
        "address": None,
        "pin": "",
    },
    "radar": {
        "max_range_km": 100,
        "crosshair_default": True,
        "heatmap_max_age_sec": 3600,
    },
    "rotator": {
        "enabled": False,
        "driver": "v4_ws",
        "ws_url": "ws://192.168.10.186:81",
        "beam_width_deg": 35,
        "mode": 0,               # 0=passive 1=active 2=scan 3=track
        "aim_accumulate_sec": 3, # collect packets this long before picking a winner
        "aim_dwell_sec": 15,     # stay at target; keep collecting for next move during this window
        "aim_deadband_deg": 5,   # minimum az change to trigger a physical move
    },
    "devices": {
        # BLE address → role tag. Roles: "yagi", "omni" (future multi-radio use).
        # e.g. "AA:BB:CC:DD:EE:FF": {"role": "yagi"}
    },
    "mqtt_topics": {
        # Retained per-node position cache, compatible with the v3 ESP32
        # rotator firmware: <nodeinfo_root>/nodeinfo/<nodeID>
        "nodeinfo_root": "uk",
    },
    "antenna": {
        "rx": {
            "type": "DL6WU 5-element Yagi",
            "gain_dbi": 9.5,
            "cable_loss_db": 0.0,
        },
        "reference": {
            "type": "Omni collinear",
            "gain_dbi": 12.0,
            "cable_loss_db": 0.0,
        },
        "remote_default": {
            "type": "Stock whip",
            "gain_dbi": 2.0,
            "tx_power_dbm": 22.0,
        },
    },
    "node_display": {
        "default_max_age_sec": 1800,   # 30 minutes
        "default_hide_mqtt": True,
    },
    "node_filter": {
        # Active node filter — shared by the dashboard display and the rotator.
        # Saved here so both sides always agree on which nodes are visible/targetable.
        "max_age":      1800,
        "max_hops":     99,
        "named_only":   False,
        "has_position": False,
        "hide_mqtt":    True,
        "has_signal":   False,
        "has_telemetry": False,
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

"""Maps REST-friendly section names to AdminMessage Config/ModuleConfig
enum values, so callers never need to know the protobuf enum names.

__ metadata convention
----------------------
SECTION_META is the single source of truth for all __ metadata fields.
Do not define or inject __ fields anywhere else in the codebase.

__ fields travel with config payloads between bridge, Node.js, and UI:
  - They describe the section, not radio settings.
  - They are never written to the radio (stripped in set_config).
  - The frontend renders them as badges/hints, never as editable inputs.
  - Node.js strips __ keys before forwarding any write to the bridge.

To add metadata for a section: edit SECTION_META below. Nothing else changes.
To add a new __ field type: add it to SECTION_META, handle it in the frontend.

Current __ fields:
  __reboot  bool    True if writing this section triggers a device reboot.
  __notes   list    Advisory strings shown in the section header in the UI.
"""

CONFIG_SECTIONS = {
    "device": "DEVICE_CONFIG",
    "position": "POSITION_CONFIG",
    "power": "POWER_CONFIG",
    "network": "NETWORK_CONFIG",
    "display": "DISPLAY_CONFIG",
    "lora": "LORA_CONFIG",
    "bluetooth": "BLUETOOTH_CONFIG",
    "security": "SECURITY_CONFIG",
}

MODULE_CONFIG_SECTIONS = {
    "mqtt": "MQTT_CONFIG",
    "serial": "SERIAL_CONFIG",
    "external_notification": "EXTNOTIF_CONFIG",
    "store_forward": "STOREFORWARD_CONFIG",
    "range_test": "RANGETEST_CONFIG",
    "telemetry": "TELEMETRY_CONFIG",
    "canned_message": "CANNEDMSG_CONFIG",
    "audio": "AUDIO_CONFIG",
    "remote_hardware": "REMOTEHARDWARE_CONFIG",
    "neighbor_info": "NEIGHBORINFO_CONFIG",
    "ambient_lighting": "AMBIENTLIGHTING_CONFIG",
    "detection_sensor": "DETECTIONSENSOR_CONFIG",
    "paxcounter": "PAXCOUNTER_CONFIG",
}

# SSOT for all __ metadata. Edit here only — never in methods.py or elsewhere.
SECTION_META: dict[str, dict] = {
    "lora": {
        "__reboot": True,
        "__notes": ["Changes require a device reboot — BLE reconnects automatically."],
    },
    "bluetooth": {
        "__reboot": True,
    },
    "device": {
        "__reboot": True,
    },
    "network": {
        "__reboot": True,
    },
    "security": {
        "__reboot": True,
    },
    "power": {
        "__reboot": True,
    },
    "range_test": {
        "__reboot": False,
        "__notes": ["Set 'sender' to a non-zero interval (seconds) to transmit range test packets. Leave at 0 (unset) to receive only. hop_limit=0 so packets do not relay — direct RF only."],
    },
    "owner": {
        "__reboot": True,
        "__notes": ["Changing role requires a device reboot — BLE reconnects automatically."],
    },
}

# Derived from SECTION_META — never maintained separately.
REBOOT_SECTIONS = frozenset(k for k, v in SECTION_META.items() if v.get("__reboot"))


def config_kind(section: str) -> str:
    """Returns 'config', 'module_config', or raises KeyError."""
    if section in CONFIG_SECTIONS:
        return "config"
    if section in MODULE_CONFIG_SECTIONS:
        return "module_config"
    raise KeyError(f"unknown config section: {section}")

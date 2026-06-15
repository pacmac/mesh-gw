"""Maps REST-friendly section names to AdminMessage Config/ModuleConfig
enum values, so callers never need to know the protobuf enum names."""

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


def config_kind(section: str) -> str:
    """Returns 'config', 'module_config', or raises KeyError."""
    if section in CONFIG_SECTIONS:
        return "config"
    if section in MODULE_CONFIG_SECTIONS:
        return "module_config"
    raise KeyError(f"unknown config section: {section}")

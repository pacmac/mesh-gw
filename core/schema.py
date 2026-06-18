"""Introspects protobuf Config/ModuleConfig descriptors so the frontend
can render config forms dynamically -- adding a field to the protobuf
makes it show up in the UI automatically, no frontend changes needed."""
from meshtastic import config_pb2
from meshtastic.protobuf import module_config_pb2
from google.protobuf.descriptor import FieldDescriptor

from .sections import CONFIG_SECTIONS, MODULE_CONFIG_SECTIONS

_TYPE_MAP = {
    FieldDescriptor.TYPE_BOOL: "bool",
    FieldDescriptor.TYPE_STRING: "string",
    FieldDescriptor.TYPE_BYTES: "bytes",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_DOUBLE: "float",
    FieldDescriptor.TYPE_ENUM: "enum",
}
_INT_TYPES = {
    FieldDescriptor.TYPE_INT32, FieldDescriptor.TYPE_INT64,
    FieldDescriptor.TYPE_UINT32, FieldDescriptor.TYPE_UINT64,
    FieldDescriptor.TYPE_SINT32, FieldDescriptor.TYPE_SINT64,
    FieldDescriptor.TYPE_FIXED32, FieldDescriptor.TYPE_FIXED64,
    FieldDescriptor.TYPE_SFIXED32, FieldDescriptor.TYPE_SFIXED64,
}


def _field_schema(f: FieldDescriptor, annotation_path: str = "") -> dict:
    if f.type == FieldDescriptor.TYPE_MESSAGE:
        sub_path = f"{annotation_path}.{f.name}" if annotation_path else f.name
        return {
            "name": f.name,
            "type": "object",
            "fields": [_field_schema(sub, sub_path) for sub in f.message_type.fields],
        }

    kind = _TYPE_MAP.get(f.type, "int" if f.type in _INT_TYPES else "string")
    field = {"name": f.name, "type": kind}
    if f.is_repeated:
        field["repeated"] = True
    if kind == "enum":
        field["options"] = [v.name for v in f.enum_type.values]
    full_path = f"{annotation_path}.{f.name}" if annotation_path else f.name
    ann = _FIELD_ANNOTATIONS.get(full_path)
    if ann:
        field.update(ann)
    return field


def section_message_type(section: str):
    if section in CONFIG_SECTIONS:
        return config_pb2.Config.DESCRIPTOR.fields_by_name[section].message_type
    if section in MODULE_CONFIG_SECTIONS:
        return module_config_pb2.ModuleConfig.DESCRIPTOR.fields_by_name[section].message_type
    raise KeyError(f"unknown config section: {section}")


_HIDDEN_FIELDS: dict[str, set[str]] = {
    "range_test": {"enabled"},  # always kept True server-side, no user-facing toggle needed
}

# Annotations for fields that need UI hints. Keys are "section.field" or
# "section.parent_field.child_field" for nested messages.
# unit: display unit suffix; min: minimum valid value; hint: extra tooltip text.
_FIELD_ANNOTATIONS: dict[str, dict] = {
    "device.node_info_broadcast_secs":                          {"unit": "seconds", "min": 0, "hint": "0 = use default (900 s)"},
    "position.position_broadcast_secs":                         {"unit": "seconds", "min": 0, "hint": "0 = smart broadcast only"},
    "position.gps_update_interval":                             {"unit": "seconds", "min": 0, "hint": "0 = GPS always on"},
    "position.gps_attempt_time":                                {"unit": "seconds", "min": 0},
    "position.broadcast_smart_minimum_interval_secs":           {"unit": "seconds", "min": 0},
    "power.on_battery_shutdown_after_secs":                     {"unit": "seconds", "min": 0, "hint": "0 = never"},
    "power.wait_bluetooth_secs":                                {"unit": "seconds", "min": 0, "hint": "0 = never sleep BT"},
    "power.sds_secs":                                           {"unit": "seconds", "min": 0, "hint": "Super-deep sleep duration; 0 = disabled"},
    "power.ls_secs":                                            {"unit": "seconds", "min": 0, "hint": "Light-sleep duration; 0 = disabled"},
    "power.min_wake_secs":                                      {"unit": "seconds", "min": 0},
    "display.screen_on_secs":                                   {"unit": "seconds", "min": 0, "hint": "0 = always on"},
    "display.auto_screen_carousel_secs":                        {"unit": "seconds", "min": 0, "hint": "0 = disabled"},
    "mqtt.map_report_settings.publish_interval_secs":           {"unit": "seconds", "min": 0},
    "serial.timeout":                                           {"unit": "seconds", "min": 0},
    "external_notification.nag_timeout":                        {"unit": "seconds", "min": 0, "hint": "0 = no repeat"},
    "telemetry.device_update_interval":                         {"unit": "seconds", "min": 0, "hint": "0 = use default (900 s)"},
    "telemetry.environment_update_interval":                    {"unit": "seconds", "min": 0, "hint": "0 = use default (900 s)"},
    "telemetry.air_quality_interval":                           {"unit": "seconds", "min": 0, "hint": "0 = use default (900 s)"},
    "telemetry.power_update_interval":                          {"unit": "seconds", "min": 0, "hint": "0 = use default (900 s)"},
    "telemetry.health_update_interval":                         {"unit": "seconds", "min": 0, "hint": "0 = use default (900 s)"},
    "neighbor_info.update_interval":                            {"unit": "seconds", "min": 0},
    "detection_sensor.minimum_broadcast_secs":                  {"unit": "seconds", "min": 0},
    "detection_sensor.state_broadcast_secs":                    {"unit": "seconds", "min": 0},
    "paxcounter.paxcounter_update_interval":                    {"unit": "seconds", "min": 0},
}


def get_section_schema(section: str) -> dict:
    msg_type = section_message_type(section)
    hidden = _HIDDEN_FIELDS.get(section, set())
    fields = [_field_schema(f, section) for f in msg_type.fields if f.name not in hidden]
    return {"section": section, "fields": fields}


_CHANNEL_LABEL_OVERRIDES = {
    "uplink_enabled": "Radio → Broker",
    "downlink_enabled": "Broker → Radio",
}

def get_channel_schema() -> dict:
    from meshtastic import channel_pb2
    msg_type = channel_pb2.ChannelSettings.DESCRIPTOR
    role_enum = channel_pb2.Channel.DESCRIPTOR.fields_by_name["role"].enum_type
    fields = []
    for f in msg_type.fields:
        s = _field_schema(f)
        if f.name in _CHANNEL_LABEL_OVERRIDES:
            s["label"] = _CHANNEL_LABEL_OVERRIDES[f.name]
        fields.append(s)
    fields.append({"name": "role", "type": "enum", "options": [v.name for v in role_enum.values]})
    return {"section": "channel", "fields": fields}


def get_owner_schema() -> dict:
    from meshtastic import mesh_pb2
    msg_type = mesh_pb2.User.DESCRIPTOR
    return {"section": "owner", "fields": [_field_schema(f) for f in msg_type.fields]}


def get_fixed_position_schema() -> dict:
    """The fixed-position lat/lon/alt live in mesh_pb2.Position, set via
    AdminMessage.set_fixed_position -- not part of Config.PositionConfig
    (which only has the fixed_position enable flag)."""
    from meshtastic import mesh_pb2
    msg_type = mesh_pb2.Position.DESCRIPTOR
    names = ["latitude_i", "longitude_i", "altitude"]
    return {
        "section": "fixed_position",
        "fields": [_field_schema(msg_type.fields_by_name[name]) for name in names],
    }

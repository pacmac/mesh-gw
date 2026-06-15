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


def _field_schema(f: FieldDescriptor) -> dict:
    if f.type == FieldDescriptor.TYPE_MESSAGE:
        return {
            "name": f.name,
            "type": "object",
            "fields": [_field_schema(sub) for sub in f.message_type.fields],
        }

    kind = _TYPE_MAP.get(f.type, "int" if f.type in _INT_TYPES else "string")
    field = {"name": f.name, "type": kind}
    if f.is_repeated:
        field["repeated"] = True
    if kind == "enum":
        field["options"] = [v.name for v in f.enum_type.values]
    return field


def section_message_type(section: str):
    if section in CONFIG_SECTIONS:
        return config_pb2.Config.DESCRIPTOR.fields_by_name[section].message_type
    if section in MODULE_CONFIG_SECTIONS:
        return module_config_pb2.ModuleConfig.DESCRIPTOR.fields_by_name[section].message_type
    raise KeyError(f"unknown config section: {section}")


def get_section_schema(section: str) -> dict:
    msg_type = section_message_type(section)
    return {"section": section, "fields": [_field_schema(f) for f in msg_type.fields]}


def get_channel_schema() -> dict:
    from meshtastic import channel_pb2
    msg_type = channel_pb2.ChannelSettings.DESCRIPTOR
    role_enum = channel_pb2.Channel.DESCRIPTOR.fields_by_name["role"].enum_type
    return {
        "section": "channel",
        "fields": [_field_schema(f) for f in msg_type.fields] + [
            {"name": "role", "type": "enum", "options": [v.name for v in role_enum.values]},
        ],
    }


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

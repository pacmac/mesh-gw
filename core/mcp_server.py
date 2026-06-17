"""MCP (Model Context Protocol) server — exposes bridge METHODS as tools.

Mounts two routes on the FastAPI app:
  GET  /mcp/sse       — SSE stream (client connects here first)
  POST /mcp/messages  — client POSTs requests here

Each METHODS entry becomes an MCP tool.  Device-level tools require a
`node_id` parameter (e.g. '!3f172791').  Server-level tools operate on the
DeviceManager directly.

Usage from an MCP client (e.g. Claude Desktop):
  {
    "mcpServers": {
      "mesh-gw": {
        "url": "http://<host>:8001/mcp/sse"
      }
    }
  }
"""
import json
import logging
from typing import TYPE_CHECKING, Any

import anyio
from mcp.server import Server
from mcp.server.sse import SseServerTransport
import mcp.types as types
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from .methods import METHODS

if TYPE_CHECKING:
    from module.device_manager import DeviceManager

logger = logging.getLogger(__name__)


# ── JSON schema helpers ────────────────────────────────────────────────────

def _str(description: str) -> dict:
    return {"type": "string", "description": description}


def _int(description: str, default: int | None = None) -> dict:
    s: dict[str, Any] = {"type": "integer", "description": description}
    if default is not None:
        s["default"] = default
    return s


def _bool(description: str, default: bool = False) -> dict:
    return {"type": "boolean", "description": description, "default": default}


# ── Tool definitions ───────────────────────────────────────────────────────

# Server-level tools (no node_id needed)
_SERVER_TOOLS: list[types.Tool] = [
    types.Tool(
        name="list_devices",
        description="List all connected BLE bridge devices with their status.",
        inputSchema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="connect_device",
        description="Connect a new BLE device by address.",
        inputSchema={
            "type": "object",
            "required": ["address"],
            "properties": {
                "address": _str("BLE MAC address, e.g. 'AA:BB:CC:DD:EE:FF'"),
                "pin":     _str("BLE PIN if required (leave empty if not needed)"),
            },
        },
    ),
    types.Tool(
        name="disconnect_device",
        description="Disconnect a device by node_id or BLE address.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {
                "node_id": _str("Node ID (!hex) or BLE address of the device to disconnect"),
            },
        },
    ),
]

# Device-level tools: mirror METHODS registry, each requires node_id
_DEVICE_TOOLS: list[types.Tool] = [
    types.Tool(
        name="get_info",
        description="Get my_info and radio metadata for a connected bridge device.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {"node_id": _str("Bridge node ID, e.g. '!3f172791'")},
        },
    ),
    types.Tool(
        name="get_nodes",
        description="Get the mesh node list seen by a bridge device.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {
                "node_id":      _str("Bridge node ID"),
                "max_age":      _int("Only include nodes heard within N seconds (0 = no limit)", 0),
                "max_hops":     _int("Maximum hop count filter (99 = no limit)", 99),
                "named_only":   _bool("Only include nodes with a long_name"),
                "has_position": _bool("Only include nodes with GPS position"),
                "hide_mqtt":    _bool("Exclude MQTT-bridged nodes"),
                "has_signal":   _bool("Only include nodes with SNR/RSSI data"),
                "has_telemetry":_bool("Only include nodes with telemetry data"),
            },
        },
    ),
    types.Tool(
        name="get_status",
        description="Get connection and config status for a bridge device.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {"node_id": _str("Bridge node ID")},
        },
    ),
    types.Tool(
        name="get_channels",
        description="Get cached channel list for a bridge device.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {"node_id": _str("Bridge node ID")},
        },
    ),
    types.Tool(
        name="get_config",
        description="Get cached full config for a bridge device.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {"node_id": _str("Bridge node ID")},
        },
    ),
    types.Tool(
        name="get_config_live",
        description="Fetch a specific config section live from the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id", "section"],
            "properties": {
                "node_id": _str("Bridge node ID"),
                "section": _str("Config section name (e.g. 'lora', 'bluetooth', 'position')"),
            },
        },
    ),
    types.Tool(
        name="get_owner_live",
        description="Fetch the owner (long_name, short_name, id) live from the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {"node_id": _str("Bridge node ID")},
        },
    ),
    types.Tool(
        name="get_channel_live",
        description="Fetch a specific channel config live from the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id", "index"],
            "properties": {
                "node_id": _str("Bridge node ID"),
                "index":   _int("Channel index (0–7)"),
            },
        },
    ),
    types.Tool(
        name="get_fixed_position",
        description="Get the fixed GPS position set on the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {"node_id": _str("Bridge node ID")},
        },
    ),
    types.Tool(
        name="set_fixed_position",
        description="Set a fixed GPS position on the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id", "latitude", "longitude"],
            "properties": {
                "node_id":   _str("Bridge node ID"),
                "latitude":  {"type": "number", "description": "Latitude in decimal degrees"},
                "longitude": {"type": "number", "description": "Longitude in decimal degrees"},
                "altitude":  _int("Altitude in metres", 0),
            },
        },
    ),
    types.Tool(
        name="remove_fixed_position",
        description="Remove the fixed GPS position from the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {"node_id": _str("Bridge node ID")},
        },
    ),
    types.Tool(
        name="send_text",
        description="Send a text message via a bridge radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id", "text"],
            "properties": {
                "node_id": _str("Bridge node ID to send from"),
                "text":    _str("Message text to send"),
                "to":      _str("Destination node ID (!hex) or 'broadcast' (default)"),
                "channel": _int("Channel index (default 0)", 0),
            },
        },
    ),
    types.Tool(
        name="set_config",
        description="Write a config section to the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id", "section", "values"],
            "properties": {
                "node_id": _str("Bridge node ID"),
                "section": _str("Config section name"),
                "values":  {"type": "object", "description": "Key/value pairs to set"},
            },
        },
    ),
    types.Tool(
        name="set_owner",
        description="Set the owner (long_name, short_name) on the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id"],
            "properties": {
                "node_id":    _str("Bridge node ID"),
                "long_name":  _str("Long name"),
                "short_name": _str("Short name (max 4 chars)"),
            },
        },
    ),
    types.Tool(
        name="set_channel",
        description="Write channel config to the radio.",
        inputSchema={
            "type": "object",
            "required": ["node_id", "index"],
            "properties": {
                "node_id":  _str("Bridge node ID"),
                "index":    _int("Channel index (0–7)"),
                "role":     _str("Channel role: PRIMARY, SECONDARY, or DISABLED"),
                "settings": {"type": "object", "description": "Channel settings fields"},
            },
        },
    ),
]

ALL_TOOLS = _SERVER_TOOLS + _DEVICE_TOOLS
_TOOL_INDEX = {t.name: t for t in ALL_TOOLS}
_DEVICE_TOOL_NAMES = {t.name for t in _DEVICE_TOOLS}


# ── MCP server factory ─────────────────────────────────────────────────────

def create_mcp_server(dm: "DeviceManager") -> Server:
    server = Server("mesh-gw-bridge")

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return ALL_TOOLS

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
        try:
            result = await _dispatch(name, arguments, dm)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
        except Exception as e:
            return [types.TextContent(type="text", text=json.dumps({"error": str(e)}))]

    return server


async def _dispatch(name: str, args: dict, dm: "DeviceManager") -> dict:
    # Server-level tools
    if name == "list_devices":
        return {"devices": dm.list_devices()}

    if name == "connect_device":
        address = (args.get("address") or "").strip()
        if not address:
            raise ValueError("address is required")
        pin = (args.get("pin") or "").strip()
        key = await dm.connect(address, pin=pin)
        return {"connecting": True, "key": key, "address": address}

    if name == "disconnect_device":
        node_id = (args.get("node_id") or "").strip()
        if not node_id:
            raise ValueError("node_id is required")
        await dm.disconnect(node_id)
        return {"disconnected": True, "node_id": node_id}

    # Device-level tools
    if name not in _DEVICE_TOOL_NAMES:
        raise ValueError(f"Unknown tool: {name}")

    node_id = (args.get("node_id") or "").strip()
    if not node_id:
        raise ValueError("node_id is required")

    bridge = dm.get(node_id)
    if bridge is None:
        raise ValueError(f"Unknown device: {node_id}")

    fn = METHODS.get(name)
    if fn is None:
        raise ValueError(f"No method registered for tool: {name}")

    params = {k: v for k, v in args.items() if k != "node_id"}
    return await fn(bridge, params)


# ── FastAPI route mounts ───────────────────────────────────────────────────

def mount_mcp(app, dm: "DeviceManager", path_prefix: str = "/mcp"):
    """Mount MCP SSE transport routes onto a FastAPI app.

    Adds:
      GET  {path_prefix}/sse       — SSE endpoint (client opens this)
      POST {path_prefix}/messages  — POST endpoint (client sends requests here)
    """
    mcp_server = create_mcp_server(dm)
    sse = SseServerTransport(f"{path_prefix}/messages")

    async def handle_sse(request: Request):
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await mcp_server.run(
                streams[0], streams[1],
                mcp_server.create_initialization_options(),
            )

    async def handle_post(request: Request):
        await sse.handle_post_message(request.scope, request.receive, request._send)

    from fastapi.routing import APIRoute
    app.add_api_route(f"{path_prefix}/sse",      handle_sse,  methods=["GET"],  include_in_schema=False)
    app.add_api_route(f"{path_prefix}/messages", handle_post, methods=["POST"], include_in_schema=False)

    logger.info("MCP server mounted at %s/sse", path_prefix)

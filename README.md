# mesh-gw — Meshtastic Multi-Device BLE Bridge

[![License: PolyForm Noncommercial](https://img.shields.io/badge/license-PolyForm%20NC-blue)](LICENSE)

A pure BLE-to-JSON bridge for Meshtastic radios. Connects to N radios simultaneously over BLE and exposes a unified JSON REST API, WebSocket event stream, MCP tool server, and Meshtastic TCP gateway. Consumers (dashboard servers, logging tools, automation, AI agents) never see protobuf.

Multi-device BLE bridging is the core feature — most existing Meshtastic bridges connect to one device at a time.

The BLE connection handling (`core/ble_handler.py`, `core/stats.py`) is adapted from [Yeraze/meshtastic-ble-bridge](https://github.com/Yeraze/meshtastic-ble-bridge).

## Scope

This bridge:
- Connects to Meshtastic radios over BLE (multi-device)
- Streams decoded `FromRadio` packets as JSON over WebSocket
- Accepts outbound packets (send text, admin messages)
- Proxies MQTT traffic on behalf of radios configured for `proxy_to_client_enabled`
- Publishes mesh events to an external MQTT broker (optional)
- Provides filtered node queries
- Exposes all methods as MCP tools over streamable HTTP transport
- Caches recent text messages and replays them to new WebSocket clients (optional)
- Bridges each BLE device to a Meshtastic TCP gateway port (optional)
- Runs a background Claude AI daemon that responds to `@claude` trigger words over the mesh

It does **not** contain: dashboard UI, rotator logic, radar, node history, or any consuming-application logic. Those belong in a separate dashboard server.

## API

### Server-level (all devices)

| Endpoint | Description |
|---|---|
| `GET /help` | API reference |
| `GET /status` | Server status and device list |
| `GET /devices` | Connected device list |
| `POST /devices` | Connect a new BLE device `{address, pin?, tcp_port?}` |
| `DELETE /devices/{node_id}` | Disconnect a device |
| `PATCH /ble_devices/{address}` | Update per-device config fields (`auto_connect`, `tcp_port`) |
| `POST /reload` | Reload `bridge_config.yaml` without restarting (also triggered by `SIGHUP`) |
| `GET /nodes` | Merged node list across all bridges (query params below) |
| `GET /ble/scan` | Scan for nearby Meshtastic BLE devices |
| `POST /ble/pair` | Start dynamic-PIN pairing |
| `POST /ble/passkey` | Supply PIN for dynamic-PIN pairing |
| `GET /mqtt_publish` | Get MQTT publisher config |
| `PUT /mqtt_publish` | Update MQTT publisher config |
| `GET /mqtt_publish/status` | MQTT publisher connection status |
| `WS /events` | Unified event stream from all devices (tagged with `device`). Replays cached text messages on connect if `message_cache.enabled`. |
| `GET /sections` | Available config section names |
| `GET /schema/{section}` | JSON schema for a config section |

### Per-device (prefix `/{node_id}/`)

| Endpoint | Description |
|---|---|
| `GET /status` | BLE state, node count, MQTT status |
| `GET /info` | my_info + device metadata |
| `GET /nodes` | NodeDB with optional filters |
| `GET /nodes/{num}` | Single node |
| `GET /channels` | Channel list |
| `GET /channels/{index}` | Single channel (live admin read) |
| `PUT /channels/{index}` | Update a channel |
| `GET /config` | Cached config + module_config |
| `GET /config/{section}` | Live admin read of a config section |
| `PUT /config/{section}` | Write a config section |
| `GET /owner` | Device owner (live) |
| `PUT /owner` | Set device owner |
| `GET /fixed_position` | Fixed position |
| `PUT /fixed_position` | Set fixed position |
| `DELETE /fixed_position` | Remove fixed position |
| `POST /messages` | Send text `{text, to?, channel?}` |
| `GET /messages` | Recent cached text messages |
| `POST /admin` | Generic AdminMessage passthrough |
| `POST /rpc` | JSON-RPC 2.0 method call |
| `GET /range_test` | Range test log |
| `DELETE /range_test` | Clear range test log |
| `WS /events` | Per-device event stream. Replays cached text messages on connect if `message_cache.enabled`. |

### OTA firmware update

| Endpoint | Description |
|---|---|
| `POST /ota` | Trigger BLE OTA firmware update (nRF52 or ESP32) |

Body: `{ "ble_addr": "AA:BB:CC:DD:EE:FF", "firmware": "/path/to/firmware", "node_id": "!aabbccdd" }`

- `ble_addr` and `firmware` are required; `node_id` is optional (used only to label WS events)
- Returns `{"started": true, "protocol": "<protocol>"}` immediately — the update runs as a background task
- Does **not** require the bridge to already be connected to the target device over BLE; it opens its own direct BLE connection for DFU
- **Protocol is auto-detected** from the device's `hw_model`:
  - **nRF52 devices** (RAK4631, T-Echo, etc.) — Nordic Secure DFU over BLE via [recrof/nrf_dfu_py](https://github.com/recrof/nrf_dfu_py); firmware must be a `.zip` DFU package
  - **ESP32 devices** (Heltec, T-Beam, etc.) — `esp32-unified-ota` GATT protocol; firmware must be a `.bin` file
- Progress is streamed to all `/events` WebSocket subscribers as `ota_start` → `ota_progress` → `ota_complete` or `ota_error`

```json
{"type": "ota_start",    "ble_addr": "AA:BB:CC:DD:EE:FF", "device": "!aabbccdd", "firmware": "firmware.zip", "protocol": "nrf52-dfu"}
{"type": "ota_progress", "ble_addr": "AA:BB:CC:DD:EE:FF", "device": "!aabbccdd", "data": {"pct": 42}}
{"type": "ota_complete", "ble_addr": "AA:BB:CC:DD:EE:FF", "device": "!aabbccdd", "data": {...}}
{"type": "ota_error",    "ble_addr": "AA:BB:CC:DD:EE:FF", "device": "!aabbccdd", "data": {"error": "..."}}
```

### Node filter query params

All `/nodes` endpoints accept:

| Param | Default | Description |
|---|---|---|
| `max_age` | 0 (off) | Max seconds since last heard |
| `max_hops` | 99 | Max hop count |
| `named_only` | false | Only nodes with a long_name |
| `has_position` | false | Only nodes with position |
| `hide_mqtt` | false | Exclude MQTT-sourced nodes |
| `has_signal` | false | Only nodes with SNR/RSSI |
| `has_telemetry` | false | Only nodes with device_metrics |
| `node_roles` | [] (all) | Filter by role strings e.g. `ROUTER`, `CLIENT` |

## MCP Server

The bridge exposes all methods as [MCP](https://modelcontextprotocol.io) tools over **streamable HTTP** transport (stateless per-request, no dropped connections):

| Endpoint | Description |
|---|---|
| `POST /mcp` | Streamable HTTP MCP endpoint |

**Tools available:** `list_devices`, `connect_device`, `disconnect_device`, `get_info`, `get_nodes`, `get_status`, `get_channels`, `get_config`, `get_config_live`, `get_owner_live`, `get_channel_live`, `get_fixed_position`, `set_fixed_position`, `remove_fixed_position`, `send_text`, `set_config`, `set_owner`, `set_channel`, `get_messages`, `wait_for_message`

`wait_for_message` long-polls the bridge event queue and returns the next `TEXT_MESSAGE_APP` packet — useful for interactive chat loops and event-driven agents.

Configure in Claude Code (`/etc/claude-code/managed-mcp.json` or `~/.claude/managed-mcp.json`):
```json
{
  "mcpServers": {
    "mesh-gw": {
      "type": "http",
      "url": "http://<host>:8001/mcp"
    }
  }
}
```

## Interactive Mesh Chat (`/mt-chat` skill)

The `/mt-chat` Claude Code skill enables an interactive chat loop over the mesh using MCP tools:

1. Invoke `/mt-chat` in Claude Code
2. Claude uses `wait_for_message` to receive incoming texts and `send_text` to reply — all directly from the Claude Code session, no extra API account needed.
3. Replies go only to the message sender (never broadcast to the mesh).

## Claude AI Daemon

`core/claude_daemon.py` runs as a background task inside the bridge server. It watches all incoming mesh messages for a configurable trigger word (default: `@claude`) from trusted node IDs, calls `claude -p` (Claude Code CLI non-interactively), and sends the reply back as a direct message.

- No separate Anthropic API account or API key required — uses the local Claude Code CLI credentials.
- Replies are per-sender, with conversation history kept per sender.
- Trigger word, system prompt, trusted nodes, and reply length are all configurable in `bridge_config.yaml`.

```yaml
claude_chat:
  enabled: true
  trigger_word: "@claude"
  system_prompt: "You are Claude, accessible via Meshtastic radio. Keep replies concise — this is a low-bandwidth radio link."
  max_history: 20
  max_reply_length: 200
  whitelist: ""          # comma-separated !hex node IDs; empty = my_nodes only
  my_nodes: "!aabbccdd"  # your own node IDs (always allowed to trigger)
```

## TCP Gateway

Each device entry in `ble_devices` can have a `tcp_port`. The bridge opens a TCP server on that port implementing the standard **Meshtastic StreamAPI** framing (`0x94 0xc3` magic + 2-byte length). This makes each radio accessible to:

- Meshtastic CLI: `meshtastic --host <host>`
- Meshtastic Android/iOS app: add TCP connection in app settings
- Any other Meshtastic TCP-capable client

The TCP gateway and REST/WebSocket API operate **concurrently on the same radio** — a Meshtastic app can be connected on the TCP port while the dashboard, MCP tools, and Claude daemon all continue to operate via the REST/WS API. Packets received over BLE are forwarded to all TCP clients and all WS subscribers simultaneously.

Different radios get different ports (e.g., 4403, 4404). The TCP port is configurable per-device in the dashboard or directly in `bridge_config.yaml`.

## Configuration

All settings live in `core/bridge_config.yaml`:

```yaml
ble_devices:
  - address: AA:BB:CC:DD:EE:FF
    pin: ""
    auto_connect: true       # connect automatically on startup
    tcp_port: 4403           # optional: Meshtastic TCP gateway port
  - address: 11:22:33:44:55:66
    pin: "123456"
    auto_connect: false
    tcp_port: 4404

message_cache:
  enabled: false            # replay recent text messages to new WS clients
  max_messages: 100         # ring buffer size
  max_age_seconds: 86400    # discard messages older than this on replay

mqtt_publish:
  enabled: false            # publish mesh events to an external MQTT broker
  broker: localhost
  port: 1883
  username: ""
  password: ""
  use_tls: false
  topic_prefix: mesh
  ha_discovery: false       # publish Home Assistant discovery payloads
  ha_discovery_prefix: homeassistant

claude_chat:
  enabled: false            # background Claude AI daemon
  trigger_word: "@claude"
  system_prompt: "..."
  max_history: 20
  max_reply_length: 200
  whitelist: ""             # comma-separated !hex IDs; empty = my_nodes only
  my_nodes: ""              # your own node IDs
```

Config changes can be applied without restarting:

```bash
systemctl reload mesh-gw          # sends SIGHUP
# or
curl -X POST http://localhost:8001/reload
```

BLE connections are preserved across a reload.

## Running

```bash
pip install -r requirements.txt
python -m module.main AA:BB:CC:DD:EE:FF 11:22:33:44:55:66 --http-port 8001
```

## MQTT Proxy

If a connected radio has `moduleConfig.mqtt.enabled` and `proxy_to_client_enabled` set, the bridge automatically connects to the radio's configured broker and relays MQTT traffic (`mqttClientProxyMessage`) bidirectionally. No bridge configuration needed — broker address, credentials, and root topic all come from the radio's own config.

## MQTT Publisher

Separately from the MQTT proxy, the bridge can publish decoded mesh events to any MQTT broker. Configure under `mqtt_publish` in `bridge_config.yaml`. Events are published per-device and per-portnum. Set `ha_discovery: true` to publish Home Assistant discovery payloads for automatic entity creation.

## WebSocket Events

Events on `/events` (and `/{node_id}/events`) are JSON objects:

| Type | Description |
|---|---|
| `packet` | Raw decoded `FromRadio` packet |
| `node_info` | Node added or updated in NodeDB |
| `node_update` | Emitted after every mesh packet — use instead of REST polling for live node state |
| `status` | BLE connection state change (`ble_state`, `mqtt_proxy`, etc.) |
| `tilt_update` | LIS3DH tilt telemetry decoded from `PRIVATE_APP` (portnum 256) packets |
| `ota_start` | OTA flash started — includes `ble_addr`, `device`, `firmware` filename |
| `ota_progress` | OTA progress — `data.pct` is 0–100 |
| `ota_complete` | OTA finished successfully |
| `ota_error` | OTA failed — `data.error` contains the reason |

```json
{"type": "packet",      "data": {...},                          "device": "!aabbccdd"}
{"type": "node_info",   "data": {...},                          "device": "!aabbccdd"}
{"type": "node_update", "data": {...},                          "device": "!aabbccdd"}
{"type": "status",      "data": {"ble_state": "ready", ...},   "device": "!aabbccdd"}
{"type": "tilt_update", "data": {"pitch": 1.2, "roll": -0.4, "x": 0.02, "y": -0.01, "z": 0.98}, "device": "!aabbccdd"}
{"type": "ota_start",   "ble_addr": "AA:BB:CC:DD:EE:FF", "device": "!aabbccdd", "firmware": "firmware.zip"}
{"type": "ota_progress","ble_addr": "AA:BB:CC:DD:EE:FF", "device": "!aabbccdd", "data": {"pct": 42}}
{"type": "ota_complete","ble_addr": "AA:BB:CC:DD:EE:FF", "device": "!aabbccdd", "data": {...}}
{"type": "ota_error",   "ble_addr": "AA:BB:CC:DD:EE:FF", "device": "!aabbccdd", "data": {"error": "..."}}
```

If `message_cache.enabled`, replayed messages include `"_replay": true` so clients can distinguish them from live events.

## Architecture

```
[Meshtastic Radio] <--BLE--> [core/ble_handler.py]
                                      |
                              [core/bridge.py]
                              [core/state.py]
                                      |
          +-----------+--------------+-----------+-----------+
          |           |              |           |           |
  [module/server.py]  |   [core/mcp_server.py]  |  [core/mqtt_publisher.py]
  FastAPI, port 8001  |   MCP streamable HTTP   |   external MQTT broker
          |           |                          |
   REST/WS clients    |              [core/mqtt_proxy.py]
  (dashboard, apps)   |              radio MQTT proxy
                      |
              [core/tcp_gateway.py]       [core/claude_daemon.py]
              Meshtastic TCP bridge       @claude AI daemon
              (per-device port)           (claude -p via WS events)

[nRF52 Device] <--BLE (DFU)--> [core/ota.py]
                                POST /ota → background task
                                streams ota_progress via /events WS
```

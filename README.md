# mesh-gw — Meshtastic Multi-Device BLE Bridge

A pure BLE-to-JSON bridge for Meshtastic radios. Connects to N radios simultaneously over BLE and exposes a unified JSON REST API, WebSocket event stream, and MCP tool server. Consumers (dashboard servers, logging tools, automation, AI agents) never see protobuf.

Multi-device BLE bridging is the core feature — most existing Meshtastic bridges connect to one device at a time.

The BLE connection handling (`core/ble_handler.py`, `core/stats.py`) is adapted from [Yeraze/meshtastic-ble-bridge](https://github.com/Yeraze/meshtastic-ble-bridge).

## Scope

This bridge is intentionally minimal. It:
- Connects to Meshtastic radios over BLE
- Streams decoded `FromRadio` packets as JSON over WebSocket
- Accepts outbound packets (send text, admin messages)
- Proxies MQTT traffic on behalf of radios configured for `proxy_to_client_enabled`
- Publishes mesh events to an external MQTT broker (optional)
- Provides filtered node queries
- Exposes all methods as MCP tools over SSE transport
- Caches recent text messages and replays them to new WebSocket clients (optional)

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
| `POST /admin` | Generic AdminMessage passthrough |
| `POST /rpc` | JSON-RPC 2.0 method call |
| `GET /range_test` | Range test log |
| `DELETE /range_test` | Clear range test log |
| `WS /events` | Per-device event stream. Replays cached text messages on connect if `message_cache.enabled`. |

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

The bridge exposes all methods as [MCP](https://modelcontextprotocol.io) tools over SSE transport:

| Endpoint | Description |
|---|---|
| `GET /mcp/sse` | SSE stream — connect your MCP client here |
| `POST /mcp/messages` | MCP message endpoint |

**Tools available:** `list_devices`, `connect_device`, `disconnect_device`, `get_info`, `get_nodes`, `get_status`, `get_channels`, `get_config`, `get_config_live`, `get_owner_live`, `get_channel_live`, `get_fixed_position`, `set_fixed_position`, `remove_fixed_position`, `send_text`, `set_config`, `set_owner`, `set_channel`

Configure in Claude Code (`/etc/claude-code/managed-mcp.json`):
```json
{
  "mcpServers": {
    "mesh-gw": {
      "type": "sse",
      "url": "http://<host>:8001/mcp/sse"
    }
  }
}
```

## TCP Gateway

If a device entry in `ble_devices` has a `tcp_port`, the bridge opens a TCP server on that port implementing the standard Meshtastic serial/TCP framing protocol. This allows the Meshtastic app, CLI (`meshtastic --host`), and other tools to connect to the radio without BLE.

## Configuration

All settings live in `core/bridge_config.yaml`:

```yaml
ble_devices:
  - address: AA:BB:CC:DD:EE:FF
    pin: ""
    tcp_port: 4403          # optional: expose Meshtastic TCP gateway on this port
  - address: 11:22:33:44:55:66
    pin: "123456"

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
```

Config changes can be applied without restarting:

```bash
systemctl reload mesh-gw          # sends SIGHUP
# or
curl -X POST http://localhost:8001/reload
```

BLE connections are preserved across a reload. Addresses can also be passed as command-line arguments at startup.

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

```json
{"type": "packet", "data": {...}, "device": "!3f172791"}
{"type": "node_info", "data": {...}, "device": "!3f172791"}
```

If `message_cache.enabled`, replayed messages include `"_replay": true` so clients can distinguish them from live events.

## Architecture

```
[Meshtastic Radio] <--BLE--> [core/ble_handler.py]
                                      |
                              [core/bridge.py]
                              [core/state.py]
                                      |
                    +-----------------+-----------------+
                    |                 |                 |
             [module/server.py]  [core/mcp_server.py]  [core/mqtt_publisher.py]
             FastAPI, port 8001   MCP SSE /mcp/sse      external MQTT broker
                    |
         +----------+----------+
         |                     |
    REST clients          WS subscribers
  (dashboard server)   (dashboard, loggers, AI)
```


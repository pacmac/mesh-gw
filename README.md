# mesh-rest-bridge

A Meshtastic BLE-to-JSON bridge: connects to a Meshtastic radio over BLE
and exposes it as JSON-RPC 2.0, plain REST, and a WebSocket event stream.
Clients never see protobuf.

The BLE connection handling (`core/ble_handler.py`, `core/stats.py`) is
adapted from [Yeraze/meshtastic-ble-bridge](https://github.com/Yeraze/meshtastic-ble-bridge),
which proxies raw protobuf frames over TCP. This project replaces that
TCP/protobuf surface with JSON, using `google.protobuf.json_format` to
convert protobuf messages (`meshtastic` package) to/from plain dicts.

## API

- `POST /rpc` -- JSON-RPC 2.0 (`{"jsonrpc":"2.0","method":"get_nodes","id":1}`).
  Same method registry (`core/methods.py`) is intended to back an MCP
  tool list later, so agents can call this directly.
- `GET /info`, `/nodes`, `/channels`, `/config`, `/status` -- REST
  shortcuts for the read-only RPC methods.
- `WS /ws` -- stream of decoded FromRadio events as JSON.

### Methods

| method | params | description |
|---|---|---|
| `get_info` | - | my_info + device metadata |
| `get_nodes` | - | NodeDB |
| `get_channels` | - | channel list |
| `get_config` | - | last-seen config/module_config sections |
| `get_status` | - | BLE connection state, node count |
| `send_text` | `text`, `to?`, `channel?` | send a text message |
| `admin` | `message`, `to?`, `want_response?` | generic AdminMessage passthrough (same JSON shape as `meshtastic --export-config`) |
| `get_radio_config` | `section` | admin `get_config_request` for a named section (e.g. `LORA_CONFIG`) |
| `set_owner` | `long_name?`, `short_name?`, `is_licensed?` | set device owner/name |

## Running

```
pip install -r requirements.txt
python -m cli.main <BLE_MAC_ADDRESS> --http-port 8000
```

## Docker

```
docker build -t mesh-rest-bridge .
docker run --rm --net=host --privileged \
  -e BLE_ADDRESS=E9:B0:3F:17:27:91 \
  mesh-rest-bridge E9:B0:3F:17:27:91 --http-port 8000
```

Needs a host with a Bluetooth adapter (passed through to the
container/VM) within range of the radio.

## Status

Early scaffold. Read side (NodeDB, info, channels, config) and basic
admin/text-send are wired up. Not yet deployed.

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
| `get_bridge_config` | - | bridge-side settings (radar UI defaults, MQTT topic conventions) -- `core/bridge_config.yaml` |
| `set_bridge_config` | partial config dict | deep-merge + persist bridge-side settings |

## MQTT topic map

The bridge connects to the same broker (`mqtt.peter-c.net`) as the v3 ESP32
rotator/virtual-compass, and must stay compatible with its topic
conventions. Two independent topic trees are in play:

### Device-stored (radio's own MQTT module config)

The connected radio's `moduleConfig.mqtt` (Device Config tab) sets the
**gateway root**, e.g. `yagi/uk/msh/EU_868`. The bridge's `MqttProxy`
(`core/mqtt_proxy.py`) subscribes to `<gateway_root>/#` and relays
`mqttClientProxyMessage` traffic between the radio and the broker
(`/2/json/...`, `/2/e/...`, etc). This is the standard Meshtastic MQTT
module behaviour and isn't bridge-specific.

### Bridge-managed: ESP32-compatible nodeinfo cache

Configured via the **Bridge Config** tab / `GET|PUT /bridge_config`
(`core/bridge_config.yaml`, `mqtt_topics.nodeinfo_root`, default `uk`).
This is a *separate* topic tree from the gateway root above -- it's the
v3 ESP32 rotator's retained per-node position cache convention
(`/usr/share/pac/dev/pio/projects/mt-yagi/v3/spec.md`):

```
<nodeinfo_root>/nodeinfo/<nodeID>   # retained, JSON:
                                     # {mac, ln, sn, lat, lon, az, km, id, alt}
```

- On MQTT connect, the bridge subscribes to `<nodeinfo_root>/nodeinfo/#`
  and seeds `position`/`user` for any node it hasn't heard fresher data
  for yet (`bridge._on_nodeinfo_cache`).
- The first time the bridge hears a position for a node with no existing
  cache entry (via the `/2/json/` gateway feed), it publishes a retained
  doc in this same format (`bridge._maybe_publish_nodeinfo_cache`), so the
  ESP32 rotator/virtual-compass (which subscribes to `uk/nodeinfo/#`)
  benefit too.

### Critical: outbound sendtext "from" routing (YAGI/OMNI)

Not yet used by this bridge (`send_text` is BLE-direct), but **any future
MQTT-published `sendtext`/ping feature must respect this**, per
`v3/src/main_mqtt.cpp:128-163`:

A Meshtastic radio **silently drops** any MQTT downlink message whose JSON
`"from"` equals its own node ID. To make a message appear to come from
radio A, publish to the topic keyed by radio B's MAC:

```
<gateway_root>/mqtt/<MAC>   # MAC of the OTHER radio (cross-route)
{"from": <numeric ID>, "to": <numeric ID, 4294967295=broadcast>,
 "channel": 0, "type": "sendtext", "payload": "<text>",
 "want_ack": true}          // DMs only
```

`from`/`to` must be numeric node IDs, never MAC strings.

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

# mesh-gw — Meshtastic Multi-Device BLE Bridge

A pure BLE-to-JSON bridge for Meshtastic radios. Connects to N radios simultaneously over BLE and exposes a unified JSON REST API and WebSocket event stream. Consumers (dashboard servers, logging tools, automation) never see protobuf.

Multi-device BLE bridging is the core feature — most existing Meshtastic bridges connect to one device at a time.

The BLE connection handling (`core/ble_handler.py`, `core/stats.py`) is adapted from [Yeraze/meshtastic-ble-bridge](https://github.com/Yeraze/meshtastic-ble-bridge).

## Scope

This bridge is intentionally minimal. It:
- Connects to Meshtastic radios over BLE
- Streams decoded `FromRadio` packets as JSON over WebSocket
- Accepts outbound packets (send text, admin messages)
- Proxies MQTT traffic on behalf of radios configured for `proxy_to_client_enabled`
- Provides filtered node queries

It does **not** contain: dashboard UI, rotator logic, radar, node history, message storage, or any consuming-application logic. Those belong in a separate dashboard server.

## API

### Server-level (all devices)

| Endpoint | Description |
|---|---|
| `GET /status` | Server status and device list |
| `GET /devices` | Connected device list |
| `POST /devices` | Connect a new BLE device `{address, pin?, tcp_port?}` |
| `DELETE /devices/{node_id}` | Disconnect a device |
| `GET /nodes` | Merged node list across all bridges (query params below) |
| `GET /ble/scan` | Scan for nearby Meshtastic BLE devices |
| `POST /ble/pair` | Start dynamic-PIN pairing |
| `POST /ble/passkey` | Supply PIN for dynamic-PIN pairing |
| `WS /events` | Unified event stream from all devices (tagged with `device`) |
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
| `WS /events` | Per-device event stream |

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

## Configuration

BLE device addresses are persisted in `core/bridge_config.yaml` and auto-connected on startup:

```yaml
ble_devices:
  - address: AA:BB:CC:DD:EE:FF
    pin: ""
  - address: 11:22:33:44:55:66
    pin: ""
    tcp_port: 4403
```

Addresses can also be passed as command-line arguments.

## Running

```bash
pip install -r requirements.txt
python -m module.main AA:BB:CC:DD:EE:FF 11:22:33:44:55:66 --http-port 8001
```

## MQTT Proxy

If a connected radio has `moduleConfig.mqtt.enabled` and `proxy_to_client_enabled` set, the bridge automatically connects to the radio's configured broker and relays MQTT traffic (`mqttClientProxyMessage`) bidirectionally. No bridge configuration needed — broker address, credentials, and root topic all come from the radio's own config.

## Architecture

```
[Meshtastic Radio] <--BLE--> [core/ble_handler.py]
                                      |
                              [core/bridge.py]
                              [core/state.py]
                                      |
                              [module/server.py]  (FastAPI, port 8001)
                                      |
                    +-----------------+-----------------+
                    |                                   |
             REST clients                        WS subscribers
          (dashboard server)                (dashboard server, loggers)
```

## Reference

The `archive/v1/` directory contains the previous monolithic version (bridge + rotator + dashboard logic combined) as a reference for porting logic to the dashboard server.

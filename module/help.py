HELP_TEXT = """\
mesh-gw  —  Meshtastic BLE-to-JSON bridge  (multi-device)
==========================================================

NODE ID FORMAT
  Devices are addressed by their Meshtastic node ID: !{hex}
  e.g.  !3f172791
  Until my_info arrives the temporary key is  ble:{ADDR}  (upper-case MAC).

SERVER ROUTES
  GET  /help                  This text
  GET  /status                Server status + all device summaries
  GET  /devices               List connected devices
  POST /devices               Connect a device
                                body: {"address": "AA:BB:CC:DD:EE:FF", "pin": "", "tcp_port": 4403}
  DELETE /devices/{node_id}   Disconnect a device

  GET  /bridge_config         Read bridge config (YAML-backed)
  PUT  /bridge_config         Merge-update bridge config

  GET  /ble/scan              Scan for nearby Meshtastic BLE devices (5 s)
  POST /ble/pair              Start dynamic-PIN pairing (see PAIRING FLOW below)
                                body: {"address": "AA:BB:CC:DD:EE:FF", "tcp_port": 4403}
  POST /ble/passkey           Supply PIN shown on device screen
                                body: {"address": "AA:BB:CC:DD:EE:FF", "passkey": "335024"}

  WS   /events                Unified event stream — all devices, tagged {"device": "!..."}

  GET  /sections              List config section names
  GET  /schema/{section}      Protobuf-derived JSON schema for a config section
  GET  /schema/channel        Channel schema
  GET  /schema/owner          Owner schema
  GET  /schema/fixed_position Fixed position schema

  GET  /docs                  Interactive Swagger UI (full API reference)
  GET  /openapi.json          OpenAPI schema

DEVICE ROUTES  (prefix /{node_id}/)
  GET  /{node_id}/status              BLE state, RSSI, packet stats
  GET  /{node_id}/info                my_info + metadata
  GET  /{node_id}/nodes               All heard nodes
  GET  /{node_id}/nodes/{num}         Single node by node_num
  GET  /{node_id}/channels            Cached channel list
  GET  /{node_id}/channels/{index}    Live channel fetch from radio
  PUT  /{node_id}/channels/{index}    Update channel  (body: {settings, role})
  GET  /{node_id}/config              Cached full config + module_config
  GET  /{node_id}/config/{section}    Live section fetch from radio
  PUT  /{node_id}/config/{section}    Update config section  (body: field dict)
  GET  /{node_id}/owner               Live owner fetch
  PUT  /{node_id}/owner               Update owner  (body: {long_name, short_name, is_licensed})
  GET  /{node_id}/fixed_position      Device's own last-known position
  PUT  /{node_id}/fixed_position      Set fixed position  (body: {latitude_i, longitude_i, altitude})
  DELETE /{node_id}/fixed_position    Remove fixed position
  POST /{node_id}/messages            Send text message  (body: {text, to?, channel?})
  POST /{node_id}/admin               Generic AdminMessage passthrough
  POST /{node_id}/rpc                 JSON-RPC 2.0 method call
  GET  /{node_id}/range_test          Range test packet log
  DELETE /{node_id}/range_test        Clear range test log
  WS   /{node_id}/events              Per-device event stream

PAIRING FLOW (dynamic PIN devices)
  1. POST /ble/pair      {"address": "F4:28:..."}
                           -> server starts pairing, pauses at passkey prompt
  2. Watch device screen for the 6-digit PIN
  3. POST /ble/passkey   {"address": "F4:28:...", "passkey": "335024"}
                           -> pairing completes, connection proceeds normally
  For devices with a fixed PIN (e.g. 123456) use POST /devices with "pin" instead.

TCP GATEWAY
  Each device can expose a Meshtastic TCP protocol port (4-byte length prefix + protobuf).
  Compatible with Meshtastic clients that support TCP connections (same protocol as USB serial).
  Set tcp_port in bridge_config.yaml under ble_devices, or pass it to POST /devices.

QUICK EXAMPLES
  curl http://localhost:8001/status
  curl http://localhost:8001/ble/scan
  curl http://localhost:8001/!3f172791/nodes | python3 -m json.tool
  curl -X POST http://localhost:8001/devices -H 'Content-Type: application/json' \\
       -d '{"address":"E9:B0:3F:17:27:91","tcp_port":4403}'
"""

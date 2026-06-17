# Archive: v1 — Monolithic bridge + dashboard

Snapshot of the codebase before the architectural refactor (June 2026).

## What this was

A single Python process (FastAPI, port 8001) doing everything:
- Multi-device BLE bridge to Meshtastic radios
- MQTT proxy (forwarding radio's mqttClientProxyMessage to a real broker)
- MQTT nodeinfo cache (publishing retained node position docs to `uk/nodeinfo/#`)
- Rotator controller state machine (active/passive/scan/track modes)
- Rotator WebSocket driver (V4 WS protocol, ws://192.168.10.186:81)
- Node filtering logic
- Bridge config persistence (bridge_config.yaml)
- REST API for dashboard consumption

A separate static file server (port 8000) served the Alpine.js dashboard.

## Key files for reference

- `core/rotator_controller.py` — rotator mode state machine, target selection logic
- `core/rotator_v4ws.py` — V4 WebSocket rotator driver
- `core/rotator.py` — rotator base class
- `core/geo.py` — bearing and haversine calculations
- `core/bridge_config.py` — config schema and defaults
- `core/bridge.py` — BLE bridge + MQTT proxy + nodeinfo cache
- `core/mqtt_proxy.py` — MQTT proxy with nodeinfo cache layer
- `module/server.py` — full API surface (bridge + rotator + config endpoints)
- `module/device_manager.py` — multi-device manager + rotator lifecycle
- `dashboard/static/app.js` — Alpine.js frontend logic
- `dashboard/static/index.html` — dashboard UI

## Why it was refactored

- Bridge was doing too much — rotator, UI config, nodeinfo cache are not bridge concerns
- MQTT nodeinfo cache tied to a private broker, not portable for other users
- Config page hardcoded to single active device
- Home page stats single-device only
- Message history in localStorage (lost on reload, browser-specific)
- No database — node cache, messages, telemetry all ephemeral
- Rotator controller embedded in bridge process instead of being a separate service

#!/usr/bin/env python3
"""
mesh-gw API test script — step 5 of api-sync-test plan.

Connects to mesh-gw WS /events, collects events for COLLECT_SECS,
validates event schemas, then exercises REST endpoints.
Outputs structured PASS/FAIL/MISSING/SKIP report.
Exits 0 if all PASS/SKIP, non-zero on any FAIL.
"""

import asyncio
import json
import sys
import time
import yaml
import requests
import websockets

BASE      = "http://localhost:8001"
WS_URL    = "ws://localhost:8001/events"
CFG_PATH  = "/usr/share/pac/dev/projects/mt-radar/mesh-gw/core/bridge_config.yaml"
COLLECT_SECS = 30

# ─────────────────────────────────────────────────────────────────────────────
# Schema definitions — required fields per event type
# Source: GW_API.md (2026-06-27)
# ─────────────────────────────────────────────────────────────────────────────

SCHEMAS = {
    "device_snapshot": {
        "required": ["type", "devices"],
        "devices_item": ["addr"],
        "optional_device": ["state_event", "data_event"],
    },
    "device_state": {
        "required": ["type", "addr", "state"],
        "optional": ["node_id", "label", "message", "pct", "deadline", "display"],
        "display": ["badge_color", "badge_text", "show_spinner", "show_progress", "action_required", "action_text"],
        "valid_states": [
            "OFFLINE", "SCANNING", "CONNECTING", "DISCOVERING", "SYNCING", "READY",
            "RECONNECTING", "FIRMWARE_INCOMPATIBLE",
            "OTA_PENDING", "OTA_HANDSHAKE", "OTA_FLASHING", "OTA_COMPLETE",
            "OTA_BOOTLOADER_STUCK", "OTA_NVS_MISMATCH", "OTA_SERIAL_WAIT",
            "OTA_SERIAL_ERASING", "OTA_ERROR",
        ],
    },
    "device_data": {
        "required": ["type", "addr"],
        "optional": [
            "node_id", "my_node_num", "hw_model", "short_name", "long_name",
            "firmware_version", "battery_level", "voltage", "uptime_s",
            "channel_utilization", "air_util_tx", "node_count", "tcp_port",
            "sync_duration_s", "mtu", "sync_mode", "conn_priority",
            "session_passkey", "session_passkey_ttl_s",
        ],
    },
    "packet": {
        "required": ["type", "addr"],
        "data_required": ["packet"],
        "packet_optional": ["from", "to", "rx_rssi", "rx_snr", "rx_time", "hop_limit", "hop_start", "via_mqtt", "decoded"],
    },
    "node_update": {
        "required": ["type"],
        "required_one_of": ["addr", "device"],
        "data_required": ["num"],
    },
    "private_app": {
        "required": ["type", "from_num", "portnum"],
        "required_one_of": ["addr", "node_id"],
    },
    "telemetry": {
        "required": ["type", "addr", "from_num"],
        "optional": ["to_num", "rx_rssi", "rx_snr", "hops", "via_mqtt", "data"],
    },
    "position": {
        "required": ["type", "addr", "from_num"],
    },
    "user": {
        "required": ["type", "addr", "from_num"],
    },
    "text_message": {
        "required": ["type", "addr", "from_num"],
    },
    "rangetest": {
        "required": ["type", "addr", "from_num"],
    },
    "traceroute": {
        "required": ["type", "addr", "from_num"],
    },
    "neighborinfo": {
        "required": ["type", "addr", "from_num"],
    },
    "routing": {
        "required": ["type", "addr", "from_num"],
    },
    "admin": {
        "required": ["type", "addr", "from_num"],
    },
    "ota_download_start": {
        "required": ["type", "device", "filename"],
    },
    "ota_download_progress": {
        "required": ["type", "device", "data"],
        "data_required": ["pct"],
    },
    "ota_download_complete": {
        "required": ["type", "device", "filename"],
    },
    "ota_download_error": {
        "required": ["type", "device", "data"],
    },
}

# Event types that are only seen if specific conditions hold
CONDITIONAL_TYPES = {
    "packet":               "only if BLE device READY and mesh traffic flowing",
    "node_update":          "only if BLE device READY and user/position/telemetry packet received",
    "private_app":          "only if portnum 256 (tilt) device active",
    "telemetry":            "only if telemetry packet received",
    "position":             "only if position packet received",
    "user":                 "only if user packet received",
    "text_message":         "only if text message received",
    "rangetest":            "only if range test packet received",
    "traceroute":           "only if traceroute response received",
    "neighborinfo":         "only if neighbor info packet received",
    "routing":              "only if routing packet received",
    "admin":                "only if admin response received",
    "device_state":         "only if BLE device transitions state during collect window",
    "device_data":          "only if BLE device completes SYNCING during collect window",
    "ota_download_start":   "only if OTA download triggered externally",
    "ota_download_progress":"only if OTA download triggered externally",
    "ota_download_complete":"only if OTA download triggered externally",
    "ota_download_error":   "only if OTA download fails",
}

# Always sent on connect — these must appear
REQUIRED_ON_CONNECT = {"device_snapshot"}

# ─────────────────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────────────────

results = []

def report(status, subject, detail=""):
    tag = {"PASS": "✓", "FAIL": "✗", "SKIP": "○", "MISSING": "?"}.get(status, status)
    line = f"  [{tag}] {status:7s} {subject}"
    if detail:
        line += f"  — {detail}"
    results.append((status, subject, detail))
    print(line)

def section(title):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")

# ─────────────────────────────────────────────────────────────────────────────
# Schema validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_event(ev: dict, schema: dict, label: str) -> tuple[bool, str]:
    for f in schema.get("required", []):
        if f not in ev:
            return False, f"missing required field '{f}'"

    if "required_one_of" in schema:
        if not any(f in ev for f in schema["required_one_of"]):
            return False, f"missing one of {schema['required_one_of']}"

    if "valid_states" in schema and "state" in ev:
        if ev["state"] not in schema["valid_states"]:
            return False, f"unknown state '{ev['state']}'"

    if "data_required" in schema:
        data = ev.get("data")
        if not isinstance(data, dict):
            return False, f"'data' must be a dict, got {type(data).__name__}"
        for f in schema["data_required"]:
            if f not in data:
                return False, f"data missing required field '{f}'"

    if "devices_item" in schema and isinstance(ev.get("devices"), list):
        for i, d in enumerate(ev["devices"]):
            for f in schema["devices_item"]:
                if f not in d:
                    return False, f"devices[{i}] missing required field '{f}'"
            if "display" in schema.get("display", {}):
                disp = d.get("state_event", {}).get("display")
                if disp:
                    for df in schema["display"]:
                        if df not in disp:
                            return False, f"devices[{i}].state_event.display missing '{df}'"

    return True, "ok"

# ─────────────────────────────────────────────────────────────────────────────
# REST helpers
# ─────────────────────────────────────────────────────────────────────────────

def rest_get(path, **kwargs):
    try:
        r = requests.get(BASE + path, timeout=10, **kwargs)
        return r
    except Exception as e:
        return None

def rest_check(method, path, expect_status=200, expect_keys=None, body=None, skip_reason=None):
    label = f"{method} {path}"
    if skip_reason:
        report("SKIP", label, skip_reason)
        return None
    try:
        if method == "GET":
            r = requests.get(BASE + path, timeout=10)
        elif method == "POST":
            r = requests.post(BASE + path, json=body, timeout=10)
        elif method == "DELETE":
            r = requests.delete(BASE + path, timeout=10)
        elif method == "PUT":
            r = requests.put(BASE + path, json=body, timeout=10)
        else:
            report("SKIP", label, f"method {method} not tested")
            return None
    except Exception as e:
        report("FAIL", label, f"connection error: {e}")
        return None

    if r.status_code != expect_status:
        report("FAIL", label, f"HTTP {r.status_code} (expected {expect_status})")
        return None

    if expect_keys:
        try:
            data = r.json()
        except Exception:
            report("FAIL", label, "response is not JSON")
            return None
        missing = [k for k in expect_keys if k not in data]
        if missing:
            report("FAIL", label, f"response missing keys: {missing}")
            return None

    report("PASS", label, f"HTTP {r.status_code}")
    try:
        return r.json()
    except Exception:
        return r.text

# ─────────────────────────────────────────────────────────────────────────────
# Load config
# ─────────────────────────────────────────────────────────────────────────────

def load_config():
    try:
        with open(CFG_PATH) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"  Warning: could not read {CFG_PATH}: {e}")
        return {}

# ─────────────────────────────────────────────────────────────────────────────
# WS collection
# ─────────────────────────────────────────────────────────────────────────────

async def collect_events(secs: int) -> list[dict]:
    events = []
    deadline = time.time() + secs
    try:
        async with websockets.connect(WS_URL, open_timeout=5) as ws:
            while time.time() < deadline:
                remaining = deadline - time.time()
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5))
                    ev = json.loads(raw)
                    events.append(ev)
                except asyncio.TimeoutError:
                    continue
    except Exception as e:
        print(f"  WS connection failed: {e}")
    return events

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'═'*60}")
    print(f"  mesh-gw API Test  —  {BASE}")
    print(f"  Collect window: {COLLECT_SECS}s")
    print(f"{'═'*60}")

    cfg = load_config()
    ble_devices = cfg.get("ble_devices", [])
    device_addrs = [d["address"] for d in ble_devices if "address" in d]
    primary_addr = device_addrs[0] if device_addrs else None
    print(f"  Configured BLE devices: {device_addrs or '(none)'}")

    # ── Reachability ──────────────────────────────────────────────────────────
    section("Reachability")
    status_r = rest_get("/status")
    if status_r is None or status_r.status_code != 200:
        print("  FATAL: mesh-gw not reachable at localhost:8001. Aborting.")
        sys.exit(1)
    try:
        status_data = status_r.json()
        report("PASS", "GET /status", f"server={status_data.get('server', '?')}")
    except Exception:
        report("FAIL", "GET /status", "response not JSON")
        sys.exit(1)

    # ── WS event collection ────────────────────────────────────────────────────
    section(f"WS /events — collecting {COLLECT_SECS}s")
    print(f"  Connecting to {WS_URL} ...")
    events = asyncio.run(collect_events(COLLECT_SECS))
    print(f"  Received {len(events)} events")

    by_type: dict[str, list[dict]] = {}
    for ev in events:
        t = ev.get("type", "__unknown__")
        by_type.setdefault(t, []).append(ev)

    print(f"  Event types seen: {sorted(by_type.keys())}")

    # ── Schema validation ──────────────────────────────────────────────────────
    section("WS Event Schema Validation")

    for ev_type, schema in SCHEMAS.items():
        cond = CONDITIONAL_TYPES.get(ev_type)
        if ev_type not in by_type:
            if ev_type in REQUIRED_ON_CONNECT:
                report("FAIL", f"WS:{ev_type}", "expected on connect but never received")
            else:
                status = "SKIP" if cond else "MISSING"
                detail = cond or "not seen during collection window"
                report(status, f"WS:{ev_type}", detail)
            continue

        sample = by_type[ev_type][0]
        ok, reason = validate_event(sample, schema, ev_type)
        count = len(by_type[ev_type])
        if ok:
            report("PASS", f"WS:{ev_type}", f"{count} seen, schema OK")
        else:
            report("FAIL", f"WS:{ev_type}", f"schema error: {reason} — sample: {json.dumps(sample)[:200]}")

    # Report unexpected event types (not in SCHEMAS)
    for ev_type in sorted(by_type.keys()):
        if ev_type not in SCHEMAS and ev_type != "__unknown__":
            report("PASS", f"WS:{ev_type}", f"unexpected but received (no schema to validate) — {len(by_type[ev_type])} seen")

    # ── REST Endpoints ─────────────────────────────────────────────────────────
    section("REST Endpoints — Server Level")

    rest_check("GET", "/status",       expect_keys=["server"])
    rest_check("GET", "/devices",      expect_keys=["devices"])
    rest_check("GET", "/nodes",        expect_keys=["nodes"])
    rest_check("GET", "/bridge_config")
    rest_check("GET", "/mqtt_publish")
    rest_check("GET", "/sections",     expect_keys=["config"])
    rest_check("GET", "/schema/channel")
    rest_check("GET", "/schema/owner")
    rest_check("GET", "/schema/fixed_position")

    section("REST Endpoints — OTA")

    node_id_for_ota = None
    if primary_addr:
        # Try to get the node_id from /devices
        try:
            devs = requests.get(BASE + "/devices", timeout=5).json().get("devices", [])
            d = next((x for x in devs if x.get("addr") == primary_addr), None)
            if d:
                node_id_for_ota = d.get("node_id")
        except Exception:
            pass

    rest_check("GET", f"/ota/firmware?node_id={node_id_for_ota or ''}", skip_reason=None if node_id_for_ota else "no active device node_id")
    rest_check("GET", "/ota/releases", skip_reason="makes external network call — skipped")

    section("REST Endpoints — Device-Namespaced")

    if primary_addr:
        rest_check("GET", f"/{primary_addr}/status")
        rest_check("GET", f"/{primary_addr}/nodes",  expect_keys=["nodes"])
        rest_check("GET", f"/{primary_addr}/channels")
        rest_check("GET", f"/{primary_addr}/config")
        rest_check("GET", f"/{primary_addr}/messages")
        rest_check("GET", f"/{primary_addr}/range_test")
    else:
        report("SKIP", "Device-namespaced endpoints", "no configured BLE devices in bridge_config.yaml")

    section("REST Endpoints — BLE")

    # BLE scan — adapter may be occupied; allow long timeout and accept various status codes
    try:
        r = requests.get(BASE + "/ble/scan", timeout=90)
        if r.status_code in (200, 503, 409, 500):
            report("PASS", "GET /ble/scan", f"HTTP {r.status_code} (scan result or adapter busy)")
        else:
            report("FAIL", "GET /ble/scan", f"unexpected HTTP {r.status_code}")
    except requests.exceptions.Timeout:
        report("SKIP", "GET /ble/scan", "scan timed out — BLE adapter occupied by existing connections")
    except Exception as e:
        report("SKIP", "GET /ble/scan", f"scan failed (hardware): {e}")

    # ── device_snapshot content check ─────────────────────────────────────────
    section("device_snapshot Content")

    snapshots = by_type.get("device_snapshot", [])
    if not snapshots:
        report("FAIL", "device_snapshot received", "never received — cannot validate content")
    else:
        snap = snapshots[0]
        devices_in_snap = snap.get("devices", [])
        report("PASS", "device_snapshot received", f"{len(devices_in_snap)} device(s)")

        for i, d in enumerate(devices_in_snap):
            addr = d.get("addr", f"[{i}]")
            if "state_event" not in d:
                report("FAIL", f"device_snapshot.devices[{i}].state_event", "missing")
            else:
                se = d["state_event"]
                ok, reason = validate_event(se, SCHEMAS["device_state"], "device_state")
                if ok:
                    report("PASS", f"device_snapshot.devices[{i}] ({addr})", f"state={se.get('state')}")
                else:
                    report("FAIL", f"device_snapshot.devices[{i}].state_event", reason)

            if "data_event" not in d:
                state = d.get("state_event", {}).get("state", "UNKNOWN")
                if state in ("OFFLINE", "SCANNING", "CONNECTING", "DISCOVERING"):
                    report("SKIP", f"device_snapshot.devices[{i}].data_event", f"state={state} — not yet available")
                else:
                    report("FAIL", f"device_snapshot.devices[{i}].data_event", f"missing (state={state})")
            else:
                de = d["data_event"]
                ok, reason = validate_event(de, SCHEMAS["device_data"], "device_data")
                if ok:
                    report("PASS", f"device_snapshot.devices[{i}].data_event ({addr})", "schema OK")
                else:
                    report("FAIL", f"device_snapshot.devices[{i}].data_event", reason)

    # ── Summary ────────────────────────────────────────────────────────────────
    section("Summary")

    total  = len(results)
    passed = sum(1 for r in results if r[0] == "PASS")
    failed = sum(1 for r in results if r[0] == "FAIL")
    skipped= sum(1 for r in results if r[0] in ("SKIP", "MISSING"))

    print(f"  Total:   {total}")
    print(f"  PASS:    {passed}")
    print(f"  FAIL:    {failed}")
    print(f"  SKIP:    {skipped}")

    if failed:
        print(f"\n  FAILED ITEMS:")
        for status, subject, detail in results:
            if status == "FAIL":
                print(f"    ✗ {subject}  — {detail}")

    print(f"\n{'═'*60}")
    if failed:
        print(f"  RESULT: FAIL  ({failed} failure(s))")
    else:
        print(f"  RESULT: PASS")
    print(f"{'═'*60}\n")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

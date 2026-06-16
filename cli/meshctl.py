#!/usr/bin/env python3
"""meshctl — thin CLI wrapper for the mesh-rest-bridge multi-device server.

Usage:
    python -m cli.meshctl <command> [args]

Environment:
    MESHCTL_URL   Base URL of the server  (default: http://localhost:8000)

Commands:
    status                          Server health + connected devices
    devices                         List connected devices
    scan                            BLE scan for nearby Meshtastic devices
    connect <ble_addr> [--pin PIN]  Connect a BLE device
    disconnect <node_id>            Disconnect a device
    nodes <node_id>                 Node table for a device
    info <node_id>                  my_info + metadata for a device
    config <node_id> [section]      Radio config (all sections or one)
    set-config <node_id> <section> key=value ...
                                    Update a radio config section
    channels <node_id>              Channel list for a device
    bridge-config                   Show bridge_config.yaml
    set-bridge-config key=value ... Update bridge config (dot-notation keys)
"""
import argparse
import json
import os
import sys
import textwrap

import httpx

BASE_URL = os.environ.get("MESHCTL_URL", "http://localhost:8000").rstrip("/")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(path: str) -> dict:
    r = httpx.get(f"{BASE_URL}{path}", timeout=15)
    r.raise_for_status()
    return r.json()


def _post(path: str, data: dict) -> dict:
    r = httpx.post(f"{BASE_URL}{path}", json=data, timeout=15)
    r.raise_for_status()
    return r.json()


def _put(path: str, data: dict) -> dict:
    r = httpx.put(f"{BASE_URL}{path}", json=data, timeout=15)
    r.raise_for_status()
    return r.json()


def _delete(path: str) -> dict:
    r = httpx.delete(f"{BASE_URL}{path}", timeout=15)
    r.raise_for_status()
    return r.json()


def _print_json(data):
    print(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _device_table(devices: list):
    if not devices:
        print("  (none)")
        return
    w_id, w_ble, w_state = 14, 20, 14
    header = f"  {'NODE ID':<{w_id}}  {'BLE ADDRESS':<{w_ble}}  {'STATE':<{w_state}}  NODES"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for d in devices:
        nid = str(d.get("node_id", ""))
        ble = str(d.get("ble_address") or "")
        state = str(d.get("ble_state", ""))
        nodes = str(d.get("node_count", ""))
        err = d.get("ble_error")
        line = f"  {nid:<{w_id}}  {ble:<{w_ble}}  {state:<{w_state}}  {nodes}"
        print(line)
        if err:
            print(f"  {'':>{w_id}}  error: {err}")


def _node_table(nodes: dict):
    if not nodes:
        print("  (no nodes)")
        return
    rows = sorted(nodes.values(), key=lambda n: n.get("last_heard", 0), reverse=True)
    w_id, w_sn, w_ln = 12, 6, 24
    print(f"  {'NODE ID':<{w_id}}  {'SN':<{w_sn}}  {'LONG NAME':<{w_ln}}  RSSI  SNR   HOPS  AGO")
    print("  " + "-" * 80)
    import time
    now = time.time()
    for n in rows:
        num = n.get("num", 0)
        node_id = f"!{num:x}" if isinstance(num, int) else str(num)
        user = n.get("user", {})
        sn = user.get("short_name", "")[:w_sn]
        ln = user.get("long_name", "")[:w_ln]
        rssi = n.get("rssi", "")
        snr = n.get("snr", "")
        hops = n.get("hops", "")
        lh = n.get("last_heard")
        ago = f"{int(now - lh)}s" if lh else ""
        print(f"  {node_id:<{w_id}}  {sn:<{w_sn}}  {ln:<{w_ln}}  {str(rssi):<5} {str(snr):<5} {str(hops):<5} {ago}")


def _parse_kv(pairs: list[str]) -> dict:
    """Parse ['key=val', 'a.b=1'] into nested dict {'key': 'val', 'a': {'b': 1}}."""
    result = {}
    for pair in pairs:
        if "=" not in pair:
            print(f"  Warning: ignoring invalid key=value pair: {pair!r}", file=sys.stderr)
            continue
        k, v = pair.split("=", 1)
        # Try to parse as JSON value (numbers, booleans, null, strings)
        try:
            v_parsed = json.loads(v)
        except json.JSONDecodeError:
            v_parsed = v  # keep as string
        # Support dot-notation: a.b.c=1 -> {a: {b: {c: 1}}}
        parts = k.split(".")
        node = result
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = v_parsed
    return result


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_status(_args):
    data = _get("/status")
    print(f"Server: {data.get('server', 'mesh-rest-bridge')}")
    print(f"URL:    {BASE_URL}")
    print(f"\nDevices ({len(data.get('devices', []))}):")
    _device_table(data.get("devices", []))


def cmd_devices(_args):
    data = _get("/devices")
    print(f"Connected devices ({len(data.get('devices', []))}):")
    _device_table(data.get("devices", []))


def cmd_scan(_args):
    print("Scanning for Meshtastic BLE devices (5s)…")
    data = _get("/ble/scan")
    devs = data.get("devices", [])
    if not devs:
        print("  (none found)")
        return
    print(f"\n{'ADDRESS':<20}  {'NAME':<24}  RSSI")
    print("-" * 52)
    for d in devs:
        print(f"  {d['address']:<20}  {d['name']:<24}  {d['rssi']}")


def cmd_connect(args):
    data = _post("/devices", {"address": args.address, "pin": args.pin or "", "persist": not args.no_persist})
    print(f"Connecting: {data}")


def cmd_disconnect(args):
    data = _delete(f"/devices/{args.node_id}")
    print(f"Disconnecting: {data}")


def cmd_nodes(args):
    data = _get(f"/{args.node_id}/nodes")
    nodes = data.get("nodes", {})
    print(f"Nodes for {args.node_id} ({len(nodes)} total):")
    _node_table(nodes)


def cmd_info(args):
    _print_json(_get(f"/{args.node_id}/info"))


def cmd_config(args):
    if args.section:
        _print_json(_get(f"/{args.node_id}/config/{args.section}"))
    else:
        _print_json(_get(f"/{args.node_id}/config"))


def cmd_set_config(args):
    body = _parse_kv(args.kv)
    result = _put(f"/{args.node_id}/config/{args.section}", body)
    _print_json(result)


def cmd_channels(args):
    _print_json(_get(f"/{args.node_id}/channels"))


def cmd_bridge_config(_args):
    _print_json(_get("/bridge_config"))


def cmd_set_bridge_config(args):
    body = _parse_kv(args.kv)
    result = _put("/bridge_config", body)
    _print_json(result)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="meshctl",
        description=textwrap.dedent(__doc__),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--url", default=None, help="Override MESHCTL_URL")
    sub = p.add_subparsers(dest="command", metavar="command")

    sub.add_parser("status", help="Server health + connected devices")
    sub.add_parser("devices", help="List connected devices")
    sub.add_parser("scan", help="BLE scan for nearby Meshtastic devices")

    c = sub.add_parser("connect", help="Connect a BLE device")
    c.add_argument("address", help="BLE MAC address")
    c.add_argument("--pin", default="", help="BLE PIN")
    c.add_argument("--no-persist", action="store_true", help="Don't save to bridge_config")

    d = sub.add_parser("disconnect", help="Disconnect a device")
    d.add_argument("node_id", help="Node ID (!hex) or BLE address")

    n = sub.add_parser("nodes", help="Node table for a device")
    n.add_argument("node_id", help="Node ID (!hex)")

    i = sub.add_parser("info", help="my_info + metadata for a device")
    i.add_argument("node_id", help="Node ID (!hex)")

    cfg = sub.add_parser("config", help="Radio config for a device")
    cfg.add_argument("node_id", help="Node ID (!hex)")
    cfg.add_argument("section", nargs="?", help="Config section name (optional)")

    sc = sub.add_parser("set-config", help="Update a radio config section")
    sc.add_argument("node_id", help="Node ID (!hex)")
    sc.add_argument("section", help="Config section name")
    sc.add_argument("kv", nargs="+", metavar="key=value")

    ch = sub.add_parser("channels", help="Channel list for a device")
    ch.add_argument("node_id", help="Node ID (!hex)")

    sub.add_parser("bridge-config", help="Show bridge_config.yaml")

    sbc = sub.add_parser("set-bridge-config", help="Update bridge config")
    sbc.add_argument("kv", nargs="+", metavar="key=value")

    return p


COMMANDS = {
    "status": cmd_status,
    "devices": cmd_devices,
    "scan": cmd_scan,
    "connect": cmd_connect,
    "disconnect": cmd_disconnect,
    "nodes": cmd_nodes,
    "info": cmd_info,
    "config": cmd_config,
    "set-config": cmd_set_config,
    "channels": cmd_channels,
    "bridge-config": cmd_bridge_config,
    "set-bridge-config": cmd_set_bridge_config,
}


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.url:
        global BASE_URL
        BASE_URL = args.url.rstrip("/")

    if not args.command:
        parser.print_help()
        sys.exit(1)

    fn = COMMANDS.get(args.command)
    if not fn:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)

    try:
        fn(args)
    except httpx.ConnectError:
        print(f"Error: cannot connect to {BASE_URL}", file=sys.stderr)
        sys.exit(2)
    except httpx.HTTPStatusError as e:
        print(f"Error: {e.response.status_code} {e.response.text}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

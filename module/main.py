"""mesh-rest-bridge multi-device server entry point.

Usage:
    python -m module.main [BLE_ADDRESS ...] [--http-port 8000] [--verbose]
    python -m module.main AA:BB:CC:DD:EE:FF 11:22:33:44:55:66 --http-port 8000

BLE addresses can also be stored in bridge_config.yaml under 'ble_devices':
    ble_devices:
      - address: AA:BB:CC:DD:EE:FF
        pin: ""
      - address: 11:22:33:44:55:66
        pin: ""

Command-line addresses are connected in addition to any persisted ones.
"""
import argparse
import asyncio
import logging
import os
import signal
import sys

import uvicorn

# Allow running from the project root without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core import bridge_config as _bcfg
from module.device_manager import DeviceManager
from module.server import create_app

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


async def run(addresses: list[tuple[str, str]], http_port: int):
    """addresses: list of (ble_address, pin) tuples."""
    dm = DeviceManager()
    app = create_app(dm)

    config = uvicorn.Config(app, host="0.0.0.0", port=http_port, log_level="info")
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Shutdown signal received")
        server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    async def connect_all():
        if not addresses:
            logger.info("No BLE addresses configured — use POST /devices to connect")
            return
        for addr, pin in addresses:
            try:
                key = await dm.connect(addr, pin=pin)
                logger.info("Initiated connection: %s -> %s", addr, key)
            except Exception as e:
                logger.error("Failed to start connection to %s: %s", addr, e)

    connect_task = asyncio.create_task(connect_all())

    try:
        await server.serve()
    finally:
        connect_task.cancel()
        logger.info("Shutting down all device connections…")
        try:
            await asyncio.wait_for(dm.stop_all(), timeout=15.0)
        except asyncio.TimeoutError:
            logger.warning("Device shutdown timed out")
        logger.info("Shutdown complete")


def main():
    parser = argparse.ArgumentParser(description="Meshtastic multi-device BLE-to-JSON bridge")
    parser.add_argument(
        "addresses", nargs="*",
        help="BLE MAC address(es) of Meshtastic device(s) to connect at startup",
    )
    parser.add_argument("--http-port", type=int, default=8000)
    parser.add_argument("--pin", default="", help="BLE PIN (applied to all command-line addresses)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger.info("mesh-rest-bridge-multi starting on port %d", args.http_port)

    # Collect addresses: persisted config + command-line
    cfg = _bcfg.load()
    persisted = cfg.get("ble_devices") or []
    # Legacy single-device config
    if not persisted and cfg.get("ble", {}).get("address"):
        persisted = [{"address": cfg["ble"]["address"], "pin": cfg["ble"].get("pin", "")}]

    addresses: list[tuple[str, str]] = []
    seen: set[str] = set()

    for entry in persisted:
        addr = (entry.get("address") or "").strip().upper()
        if addr and addr not in seen:
            addresses.append((addr, entry.get("pin", "") or ""))
            seen.add(addr)

    for addr in args.addresses:
        addr = addr.strip().upper()
        if addr and addr not in seen:
            addresses.append((addr, args.pin))
            seen.add(addr)

    if addresses:
        for addr, _ in addresses:
            logger.info("Will connect to: %s", addr)
    else:
        logger.info("No BLE addresses configured — use POST /devices to connect")

    try:
        asyncio.run(run(addresses, args.http_port))
    except KeyboardInterrupt:
        logger.info("Exiting…")


if __name__ == "__main__":
    main()

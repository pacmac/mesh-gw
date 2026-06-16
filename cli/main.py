#!/usr/bin/env python3
"""mesh-rest-bridge entry point.

Connects to a Meshtastic device via BLE and exposes its NodeDB,
config, and admin functions as JSON over HTTP/WebSocket/JSON-RPC --
no protobuf knowledge required by clients.

Usage:
    python -m cli.main <BLE_ADDRESS> [--http-port 8000] [--verbose]
"""
import argparse
import asyncio
import logging
import os
import signal
import sys

import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.bridge import MeshBridge
from core.server import create_app
from core import __version__
from core import bridge_config as _bcfg

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


async def run(address: str | None, http_port: int, ble_pin: str = ""):
    bridge = MeshBridge(address, ble_pin=ble_pin)

    app = create_app(bridge)
    config = uvicorn.Config(app, host="0.0.0.0", port=http_port, log_level="info")
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Received shutdown signal")
        server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    async def ble_connect_loop():
        """Connect to BLE in the background if address was provided at startup.
        If no address given, the dashboard drives connect/disconnect instead."""
        if not bridge.ble_address:
            logger.info("No BLE address configured — waiting for dashboard to connect")
            return
        while not server.should_exit:
            try:
                async with bridge._reconnect_lock:
                    await bridge.start()
                return  # Connected — _on_disconnected handles future drops
            except Exception as e:
                if server.should_exit:
                    return
                logger.warning(f"BLE connect failed: {e}, retrying in 15s")
                try:
                    await asyncio.wait_for(bridge.stop(), timeout=5.0)
                except Exception:
                    pass
                await asyncio.sleep(15)

    ble_task = asyncio.create_task(ble_connect_loop())

    try:
        await server.serve()
    finally:
        ble_task.cancel()
        try:
            await ble_task
        except (asyncio.CancelledError, Exception):
            pass
        logger.info("Disconnecting from BLE device...")
        try:
            await asyncio.wait_for(bridge.stop(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("BLE disconnect timed out")
        logger.info("Shutdown complete")


def main():
    parser = argparse.ArgumentParser(description="Meshtastic BLE-to-JSON bridge")
    parser.add_argument(
        "address", nargs="?", default=None,
        help="BLE MAC address of Meshtastic device (optional — can connect via dashboard)"
    )
    parser.add_argument("--http-port", type=int, default=8000)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger.info(f"mesh-rest-bridge v{__version__}")

    # Use address from arg, or fall back to the one persisted by /ble/connect
    address = args.address
    pin = ""
    if not address:
        stored_ble = _bcfg.load().get("ble", {})
        if stored_ble.get("address"):
            address = stored_ble["address"]
            pin = stored_ble.get("pin") or ""
            logger.info(f"Auto-connecting to persisted BLE address: {address}")

    try:
        asyncio.run(run(address, args.http_port, ble_pin=pin))
    except KeyboardInterrupt:
        logger.info("Exiting...")


if __name__ == "__main__":
    main()

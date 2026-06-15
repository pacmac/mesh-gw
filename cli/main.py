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

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


async def run(address: str, http_port: int):
    bridge = MeshBridge(address)
    await bridge.start()

    app = create_app(bridge)
    config = uvicorn.Config(app, host="0.0.0.0", port=http_port, log_level="info")
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Received shutdown signal")
        server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    try:
        await server.serve()
    finally:
        logger.info("Disconnecting from BLE device...")
        try:
            await asyncio.wait_for(bridge.stop(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("BLE disconnect timed out")
        logger.info("Shutdown complete")


def main():
    parser = argparse.ArgumentParser(description="Meshtastic BLE-to-JSON bridge")
    parser.add_argument("address", help="BLE MAC address of Meshtastic device")
    parser.add_argument("--http-port", type=int, default=8000)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger.info(f"mesh-rest-bridge v{__version__}")

    try:
        asyncio.run(run(args.address, args.http_port))
    except KeyboardInterrupt:
        logger.info("Exiting...")


if __name__ == "__main__":
    main()

"""mesh-gw server entry point.

Usage:
    python -m module.main [--http-port 8001] [--verbose]

BLE device list is loaded from core/bridge_config.yaml under 'ble_devices'.
All auto-connect devices are started during server startup (lifespan).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from module.device_manager import DeviceManager
from module.server import create_app

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool, log_config: dict | None = None):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    for logger_name, level_str in (log_config or {}).items():
        level = getattr(logging, str(level_str).upper(), None)
        if level is not None:
            logging.getLogger(logger_name).setLevel(level)


async def run(http_port: int) -> None:
    queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
    dm = DeviceManager(queue)
    app = create_app(dm)

    config = uvicorn.Config(app, host="0.0.0.0", port=http_port, log_level="info")
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()

    def _handle_signal():
        logger.info("Shutdown signal received")
        server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    loop.add_signal_handler(
        signal.SIGHUP,
        lambda: asyncio.create_task(dm.reload_config()),
    )

    await server.serve()


def main():
    parser = argparse.ArgumentParser(description="Meshtastic BLE gateway")
    parser.add_argument("--http-port", type=int, default=8000)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    # Load just for log config at startup — reconcile() in lifespan handles everything else
    try:
        from core import bridge_config as _bcfg
        cfg = _bcfg.load()
        log_config = cfg.get("logging", {})
    except Exception:
        log_config = {}

    setup_logging(args.verbose, log_config=log_config)
    logger.info("mesh-gw starting on port %d", args.http_port)

    try:
        asyncio.run(run(args.http_port))
    except KeyboardInterrupt:
        logger.info("Exiting…")


if __name__ == "__main__":
    main()

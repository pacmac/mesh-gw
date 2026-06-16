"""v4_ws rotator driver — targets the v4 ESP32 rotator firmware WebSocket API.

Protocol: JSON over WS on port 81. Commands: {"action": "move2az", "args": [az]}
Events: {"evt": "status", "az": ..., "busy": ..., ...}
"""
import asyncio
import json
import logging

import websockets

from .rotator import RotatorBase

logger = logging.getLogger(__name__)

_RECONNECT_DELAY = 5.0


class V4WsRotator(RotatorBase):
    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self._connected = False
        self._status: dict = {}
        self._ws = None
        self._task: asyncio.Task | None = None

    # -- RotatorBase ----------------------------------------------------------

    def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()

    async def move(self, az: float):
        await self._send({"action": "move2az", "args": [az]})

    async def set_mode(self, mode: int):
        await self._send({"action": "mode", "args": [mode]})

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def status(self) -> dict:
        return self._status

    # -- internals ------------------------------------------------------------

    async def _send(self, msg: dict):
        if not self._ws:
            raise RuntimeError("Rotator not connected")
        await self._ws.send(json.dumps(msg))

    async def _run(self):
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self._ws = ws
                    self._connected = True
                    logger.info(f"Rotator connected: {self.ws_url}")
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if "az" in data or data.get("evt") == "status":
                            self._status = data
                            if self.on_status:
                                await self.on_status(data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Rotator WS error ({self.ws_url}): {e}")
            finally:
                self._ws = None
                self._connected = False
            await asyncio.sleep(_RECONNECT_DELAY)

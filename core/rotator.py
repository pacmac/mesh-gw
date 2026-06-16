"""Persistent async WebSocket client for the v4 rotator firmware.

Reconnects automatically on disconnect. Caches the latest status dict and
broadcasts it to registered callbacks so the dashboard WS feed stays live.
"""
import asyncio
import json
import logging

import websockets

logger = logging.getLogger(__name__)

_RECONNECT_DELAY = 5.0


class RotatorClient:
    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self.status: dict = {}
        self.connected: bool = False
        self.on_status = None          # async callback(status_dict)
        self._ws = None
        self._task: asyncio.Task | None = None

    # -- lifecycle ------------------------------------------------------------

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

    # -- commands -------------------------------------------------------------

    async def move(self, az: float):
        await self._send({"action": "move2az", "args": [az]})

    async def seek(self, az: float):
        await self._send({"action": "seek2az", "args": [az]})

    async def set_mode(self, mode: int):
        await self._send({"action": "mode", "args": [mode]})

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
                    self.connected = True
                    logger.info(f"Rotator connected: {self.ws_url}")
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if data.get("evt") == "status" or "az" in data:
                            self.status = data
                            if self.on_status:
                                await self.on_status(data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Rotator WS error: {e}")
            finally:
                self._ws = None
                self.connected = False
            await asyncio.sleep(_RECONNECT_DELAY)

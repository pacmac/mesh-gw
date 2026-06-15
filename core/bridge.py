"""Orchestrates the BLE link and the decoded mesh state, and builds
outgoing ToRadio messages from plain dicts (no protobuf in callers)."""
import asyncio
import logging
import random

from google.protobuf import json_format
from meshtastic import mesh_pb2, admin_pb2, portnums_pb2

from .ble_handler import BLEHandler
from .stats import StatsCollector
from .state import MeshState

logger = logging.getLogger(__name__)

BROADCAST_NUM = 0xFFFFFFFF
ADMIN_REPLY_TIMEOUT = 10.0


def _random_id() -> int:
    return random.randint(1, 2**32 - 1)


class MeshBridge:
    def __init__(self, ble_address: str):
        self.ble_address = ble_address
        self.stats = StatsCollector()
        self.state = MeshState()
        self.ble = BLEHandler(ble_address, self.stats)

        self.ble.on_packet_received = self._on_packet
        self.ble.on_disconnected = self._on_disconnected

    async def start(self):
        await self.ble.connect()
        await self._request_config()

    async def stop(self):
        await self.ble.disconnect()

    async def _on_packet(self, data: bytes):
        await self.state.handle_from_radio_bytes(data)

    async def _on_disconnected(self):
        for attempt in range(1, self.ble.MAX_RECONNECT_ATTEMPTS + 1):
            if await self.ble.attempt_reconnection():
                logger.info("Reconnected, re-requesting config")
                await self._request_config()
                return
        logger.error("Giving up reconnecting to BLE device")

    async def _request_config(self):
        to_radio = mesh_pb2.ToRadio()
        to_radio.want_config_id = _random_id()
        await self.ble.send(to_radio.SerializeToString())

    @property
    def my_node_num(self):
        return self.state.my_info.get("my_node_num")

    # -- outgoing helpers, JSON in / JSON out -------------------------------

    async def send_text(self, text: str, to: int = BROADCAST_NUM, channel: int = 0):
        data = mesh_pb2.Data()
        data.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
        data.payload = text.encode("utf-8")

        packet = mesh_pb2.MeshPacket()
        packet.id = _random_id()
        packet.to = to
        packet.channel = channel
        packet.decoded.CopyFrom(data)
        packet.want_ack = False

        to_radio = mesh_pb2.ToRadio()
        to_radio.packet.CopyFrom(packet)
        await self.ble.send(to_radio.SerializeToString())
        return {"sent": True, "id": packet.id}

    async def send_admin(self, message: dict, to: int = None, want_response: bool = True):
        """Send an AdminMessage built from a plain dict (same shape as
        protobuf json_format / `meshtastic --export-config` JSON).

        Example message: {"set_owner": {"long_name": "..."}}
        Example message: {"get_config_request": "LORA_CONFIG"}
        """
        admin = admin_pb2.AdminMessage()
        json_format.ParseDict(message, admin)

        data = mesh_pb2.Data()
        data.portnum = portnums_pb2.PortNum.ADMIN_APP
        data.payload = admin.SerializeToString()
        data.want_response = want_response

        packet = mesh_pb2.MeshPacket()
        packet.id = _random_id()
        packet.to = to if to is not None else (self.my_node_num or BROADCAST_NUM)
        packet.decoded.CopyFrom(data)
        packet.want_ack = False

        to_radio = mesh_pb2.ToRadio()
        to_radio.packet.CopyFrom(packet)

        if want_response:
            fut = self.state.await_reply(packet.id)
            await self.ble.send(to_radio.SerializeToString())
            try:
                return await asyncio.wait_for(fut, timeout=ADMIN_REPLY_TIMEOUT)
            except asyncio.TimeoutError:
                self.state.cancel_wait(packet.id)
                raise TimeoutError("No admin reply received")

        await self.ble.send(to_radio.SerializeToString())
        return {"sent": True, "id": packet.id}

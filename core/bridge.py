"""Orchestrates the BLE link and the decoded mesh state, and builds
outgoing ToRadio messages from plain dicts (no protobuf in callers)."""
import asyncio
import logging
import random

from google.protobuf import json_format
from meshtastic import mesh_pb2, admin_pb2, portnums_pb2

from .ble_handler import BLEHandler
from .mqtt_proxy import MqttProxy
from .stats import StatsCollector
from .state import MeshState

logger = logging.getLogger(__name__)

BROADCAST_NUM = 0xFFFFFFFF
ADMIN_REPLY_TIMEOUT = 10.0


def _random_id() -> int:
    return random.randint(1, 2**32 - 1)


class MeshBridge:
    def __init__(self, ble_address: str | None = None):
        self.ble_address: str | None = ble_address
        self.stats = StatsCollector()
        self.state = MeshState()
        self.ble: BLEHandler | None = None
        self.mqtt_proxy: MqttProxy | None = None
        self._reconnect_lock = asyncio.Lock()
        self._user_disconnect = False  # set True when user explicitly disconnects

        self.state.on_mqtt_proxy_from_radio = self._on_mqtt_proxy_from_radio
        if ble_address:
            self._init_ble(ble_address)

    def _init_ble(self, address: str):
        self.ble = BLEHandler(address, self.stats)
        self.ble.on_packet_received = self._on_packet
        self.ble.on_disconnected = self._on_disconnected

    async def start(self):
        if not self.ble:
            raise RuntimeError("No BLE device configured")
        logger.info(f"Connecting to BLE device: {self.ble_address}")
        await self.ble.connect()
        logger.info("BLE connected, requesting config…")
        await self._request_config()

    async def stop(self):
        """Full shutdown — stop MQTT proxy and disconnect BLE."""
        self._user_disconnect = True
        if self.mqtt_proxy:
            self.mqtt_proxy.stop()
            self.mqtt_proxy = None
        if self.ble:
            await self._ble_disconnect_safe()

    async def _ble_disconnect_safe(self):
        try:
            await asyncio.wait_for(self.ble.disconnect(), timeout=8.0)
        except Exception as e:
            logger.warning(f"BLE disconnect warning: {e}")

    # -- dashboard-driven connect/disconnect -----------------------------------

    async def connect_to(self, address: str):
        """Connect to a specific BLE device, called from dashboard."""
        self._user_disconnect = False
        async with self._reconnect_lock:
            if self.ble:
                await self._ble_disconnect_safe()
            self.ble_address = address
            self._init_ble(address)
            self.state.config_complete = False
            self.state.nodes.clear()
        await self.start()

    async def disconnect_ble(self):
        """Disconnect BLE and clear address — called from dashboard."""
        self._user_disconnect = True
        self.ble_address = None
        if self.mqtt_proxy:
            self.mqtt_proxy.stop()
            self.mqtt_proxy = None
        if self.ble:
            await self._ble_disconnect_safe()
            self.ble = None
        self.state.config_complete = False

    async def _on_packet(self, data: bytes):
        await self.state.handle_from_radio_bytes(data)
        if self.state.config_complete and not self.mqtt_proxy:
            self._maybe_start_mqtt_proxy()

    async def _on_disconnected(self):
        if self._user_disconnect:
            return
        if not self.ble_address:
            return
        if self._reconnect_lock.locked():
            logger.debug("Reconnect already in progress, skipping duplicate _on_disconnected")
            return
        async with self._reconnect_lock:
            if self._user_disconnect:
                return
            for attempt in range(1, self.ble.MAX_RECONNECT_ATTEMPTS + 1):
                if await self.ble.attempt_reconnection():
                    logger.info("Reconnected, re-requesting config")
                    try:
                        await self._request_config()
                    except Exception as e:
                        logger.warning(f"Config request failed after reconnect: {e}")
                    return
            logger.error("Giving up reconnecting to BLE device")

    async def _request_config(self):
        to_radio = mesh_pb2.ToRadio()
        to_radio.want_config_id = _random_id()
        await self.ble.send(to_radio.SerializeToString())
        logger.debug("want_config sent")

    @property
    def my_node_num(self):
        return self.state.my_info.get("my_node_num")

    # -- MQTT proxy -----------------------------------------------------------

    def _maybe_start_mqtt_proxy(self):
        cfg = self.state.module_config.get("mqtt", {})
        if not (cfg.get("enabled") and cfg.get("proxy_to_client_enabled")):
            return
        try:
            self.mqtt_proxy = MqttProxy(
                address=cfg["address"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                root=cfg.get("root", "msh"),
                use_tls=cfg.get("tls_enabled", False),
                on_downlink=self._on_mqtt_downlink,
            )
            self.mqtt_proxy.on_mqtt_node_update = self._on_mqtt_node_update
            logger.info(f"MQTT proxy started -> {cfg['address']} root={cfg.get('root', 'msh')}")
        except Exception as e:
            logger.error(f"Failed to start MQTT proxy: {e}")

    async def _on_mqtt_proxy_from_radio(self, msg):
        if not self.mqtt_proxy:
            return
        payload = msg.data if msg.data else msg.text.encode("utf-8")
        self.mqtt_proxy.publish(msg.topic, payload, retained=msg.retained)

    async def _on_mqtt_downlink(self, topic: str, payload: bytes):
        msg = mesh_pb2.MqttClientProxyMessage()
        msg.topic = topic
        msg.data = payload
        to_radio = mesh_pb2.ToRadio()
        to_radio.mqttClientProxyMessage.CopyFrom(msg)
        try:
            await self.ble.send(to_radio.SerializeToString())
        except RuntimeError as e:
            logger.debug(f"Dropping MQTT downlink, BLE not ready: {e}")

    def _on_mqtt_node_update(self, node: dict):
        """Called from paho thread — sync merge into state then schedule WS broadcast."""
        self.state._merge_node_info(node)
        asyncio.run_coroutine_threadsafe(
            self.state._broadcast({"type": "mqtt_node", "data": node}),
            asyncio.get_event_loop(),
        )

    # -- outgoing helpers, JSON in / JSON out -------------------------------

    async def send_text(self, text: str, to: int = BROADCAST_NUM, channel: int = 0):
        if not self.ble:
            raise RuntimeError("BLE not connected")
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
        if not self.ble:
            raise RuntimeError("BLE not connected")
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

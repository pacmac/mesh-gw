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
from .tcp_gateway import TcpGateway

logger = logging.getLogger(__name__)

BROADCAST_NUM = 0xFFFFFFFF
ADMIN_REPLY_TIMEOUT = 10.0


def _random_id() -> int:
    return random.randint(1, 2**32 - 1)


class MeshBridge:
    def __init__(self, ble_address: str | None = None, ble_pin: str = "", tcp_port: int | None = None):
        self.ble_address: str | None = ble_address
        self.ble_pin: str = ble_pin
        self.stats = StatsCollector()
        self.state = MeshState()
        self.ble: BLEHandler | None = None
        self.mqtt_proxy: MqttProxy | None = None
        self.tcp_gateway: TcpGateway | None = TcpGateway(tcp_port, on_to_radio=self._tcp_to_radio) if tcp_port else None
        self._reconnect_lock = asyncio.Lock()
        self._user_disconnect = False
        self.ble_state: str = "idle"   # idle|connecting|syncing|active|reconnecting|error
        self.ble_error: str | None = None

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
        if self.tcp_gateway:
            await self.tcp_gateway.start()
        self.ble_state = "connecting"
        logger.info(f"Connecting to BLE device: {self.ble_address}")
        try:
            await self.ble.connect(pin=self.ble_pin)
        except Exception as e:
            self.ble_state = "error"
            self.ble_error = str(e)
            raise
        self.ble_state = "syncing"
        logger.info("BLE connected, requesting config…")
        await self._request_config()

    async def stop(self):
        """Full shutdown — stop TCP gateway, MQTT proxy, and disconnect BLE."""
        self._user_disconnect = True
        if self.tcp_gateway:
            await self.tcp_gateway.stop()
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

    async def connect_to(self, address: str, pin: str = "", passkey_future=None):
        """Connect to BLE device — runs as a background task from the endpoint.

        passkey_future: asyncio.Future that resolves with the PIN shown on the
        device screen. Set by POST /ble/pair for dynamic-PIN devices.
        """
        self._user_disconnect = False
        self.ble_state = "connecting"
        self.ble_error = None
        try:
            async with self._reconnect_lock:
                if self.ble:
                    await self._ble_disconnect_safe()
                self.ble_address = address
                self.ble_pin = pin
                self.state.config_complete = False
                self.state.nodes.clear()
                logger.info(f"Connecting to BLE device: {address}")
                for attempt in range(1, 4):
                    self._init_ble(address)
                    if passkey_future:
                        self.ble.passkey_future = passkey_future
                    try:
                        await self.ble.connect(pin=pin)
                        break
                    except Exception as e:
                        logger.warning(f"Connect attempt {attempt}/3 failed: {e}")
                        if attempt == 3:
                            raise
                        await asyncio.sleep(5)
            if self.tcp_gateway and not self.tcp_gateway._server:
                await self.tcp_gateway.start()
            self.ble_state = "syncing"
            logger.info("BLE connected, requesting config…")
            asyncio.create_task(self._safe_request_config())
        except Exception as e:
            logger.error(f"BLE connect_to failed: {e}")
            self.ble_state = "error"
            self.ble_error = str(e)
            self.ble_address = None
            self.ble = None

    async def _safe_request_config(self):
        """Request config with timeout; retries once if TORADIO write is slow."""
        await asyncio.sleep(2.0)
        for attempt in range(1, 4):
            try:
                await asyncio.wait_for(self._request_config(), timeout=15.0)
                logger.info(f"want_config sent OK (attempt {attempt}/3) — waiting for FROMRADIO data")
                return
            except asyncio.TimeoutError:
                logger.warning(f"Config request timed out (attempt {attempt}/3)")
            except Exception as e:
                logger.warning(f"Config request failed (attempt {attempt}/3): {e!r}")
            if attempt < 3:
                await asyncio.sleep(5)
        logger.error("All config request attempts failed — stuck in syncing")

    async def disconnect_ble(self):
        """Disconnect BLE and clear address."""
        self._user_disconnect = True
        self.ble_address = None
        self.ble_state = "idle"
        self.ble_error = None
        if self.mqtt_proxy:
            self.mqtt_proxy.stop()
            self.mqtt_proxy = None
        if self.ble:
            await self._ble_disconnect_safe()
            self.ble = None
        self.state.config_complete = False

    async def _on_packet(self, data: bytes):
        if self.tcp_gateway:
            self.tcp_gateway.broadcast(data)
        await self.state.handle_from_radio_bytes(data)
        if self.state.config_complete and self.ble_state == "syncing":
            self.ble_state = "active"
            asyncio.create_task(self._start_mqtt_proxy_live())

    async def _tcp_to_radio(self, payload: bytes):
        """Forward a ToRadio packet received from a TCP client to the BLE radio."""
        if not self.ble:
            raise RuntimeError("BLE not connected")
        await self.ble.send(payload)

    async def _on_disconnected(self):
        if self._user_disconnect:
            return
        if not self.ble_address:
            return
        if self._reconnect_lock.locked():
            logger.debug("Reconnect already in progress, skipping duplicate _on_disconnected")
            return
        self.ble_state = "reconnecting"
        async with self._reconnect_lock:
            if self._user_disconnect:
                return
            while not self._user_disconnect:
                # Reset counter when backoff ceiling is reached so we keep retrying at max delay
                if self.ble.reconnect_attempts >= self.ble.MAX_RECONNECT_ATTEMPTS:
                    logger.info("Reconnect cycle exhausted, resetting and continuing at max interval")
                    self.ble.reconnect_attempts = 0
                if await self.ble.attempt_reconnection():
                    logger.info("Reconnected, re-requesting config")
                    self.ble_state = "syncing"
                    self.state.config_complete = False
                    asyncio.create_task(self._safe_request_config())
                    return
            logger.info("Reconnection stopped — user disconnect")

    async def _request_config(self):
        to_radio = mesh_pb2.ToRadio()
        to_radio.want_config_id = _random_id()
        await self.ble.send(to_radio.SerializeToString())
        logger.debug("want_config sent")

    @property
    def my_node_num(self):
        return self.state.my_info.get("my_node_num")

    # -- MQTT proxy -----------------------------------------------------------

    async def _start_mqtt_proxy_live(self):
        """After config_complete, query the radio directly for its MQTT module config
        (admin round-trip returns the real password, unlike the masked sync data), then
        start the proxy with the correct credentials."""
        try:
            from .sections import MODULE_CONFIG_SECTIONS
            resp = await asyncio.wait_for(
                self.send_admin({"get_module_config_request": MODULE_CONFIG_SECTIONS["mqtt"]}),
                timeout=10.0,
            )
            live = resp.get("get_module_config_response", {}).get("mqtt", {})
            if live:
                self.state.module_config["mqtt"] = live
                pwd_len = len(live.get("password", ""))
                logger.info(f"Live MQTT config fetched: user={live.get('username')!r} pwd_len={pwd_len} proxy={live.get('proxy_to_client_enabled')}")
        except Exception as e:
            logger.warning(f"Could not fetch live MQTT config via admin: {e}")
        if self.mqtt_proxy:
            self.mqtt_proxy.stop()
            self.mqtt_proxy = None
        self._maybe_start_mqtt_proxy()

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

    async def restart_mqtt_proxy(self):
        """Re-read live MQTT module config from the radio, then restart the proxy."""
        if self.mqtt_proxy:
            self.mqtt_proxy.stop()
            self.mqtt_proxy = None
        # Refresh module config from the radio via live admin round-trip so we
        # pick up any password changes made after the initial BLE sync.
        try:
            from .sections import MODULE_CONFIG_SECTIONS
            resp = await asyncio.wait_for(
                self.send_admin({"get_module_config_request": MODULE_CONFIG_SECTIONS["mqtt"]}),
                timeout=10.0,
            )
            live = resp.get("get_module_config_response", {}).get("mqtt", {})
            if live:
                self.state.module_config["mqtt"] = live
                logger.info("Refreshed MQTT module config from radio for proxy restart")
        except Exception as e:
            logger.warning(f"Could not refresh live MQTT config: {e}")
        self._maybe_start_mqtt_proxy()

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
        existing = self.state.nodes.get(str(node["num"]), {})
        if existing.get("via_mqtt") is not False:
            node["via_mqtt"] = True
        self.state._merge_node_info(node)
        asyncio.run_coroutine_threadsafe(
            self.state._broadcast({"type": "mqtt_node", "data": node}),
            self.mqtt_proxy.loop,
        )

    # -- outgoing helpers, JSON in / JSON out -------------------------------

    async def send_text(self, text: str, to: int = BROADCAST_NUM, channel: int = 0, reply_id: int = None):
        if not self.ble:
            raise RuntimeError("BLE not connected")
        data = mesh_pb2.Data()
        data.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
        data.payload = text.encode("utf-8")
        if reply_id:
            data.reply_id = reply_id

        hop_limit = self.state.config.get("lora", {}).get("hop_limit", 3)
        is_dm = to != BROADCAST_NUM

        packet = mesh_pb2.MeshPacket()
        packet.id = _random_id()
        packet.to = to
        packet.channel = channel
        packet.decoded.CopyFrom(data)
        packet.hop_limit = hop_limit
        packet.want_ack = is_dm

        to_radio = mesh_pb2.ToRadio()
        to_radio.packet.CopyFrom(packet)
        self.state.suppress_packet_id(packet.id)
        await self.ble.send(to_radio.SerializeToString())
        return {"sent": True, "id": packet.id}

    async def send_admin(self, message: dict, to: int = None, want_response: bool = True):
        if not self.ble:
            raise RuntimeError("BLE not connected")
        """Send an AdminMessage built from a plain dict."""
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

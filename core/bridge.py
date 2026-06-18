"""Orchestrates the BLE link and the decoded mesh state, and builds
outgoing ToRadio messages from plain dicts (no protobuf in callers)."""
import asyncio
import logging
import random
import time

from google.protobuf import json_format
from meshtastic import mesh_pb2, admin_pb2, portnums_pb2

from .ble_handler import BLEHandler
from .claude_chat import ClaudeChat
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
        self.tcp_gateway: TcpGateway | None = TcpGateway(tcp_port, on_to_radio=self._tcp_to_radio) if tcp_port else None
        self._reconnect_lock = asyncio.Lock()
        self._user_disconnect = False
        self.ble_state: str = "idle"   # idle|connecting|syncing|ready|reconnecting|error
        self.ble_error: str | None = None
        self._mqtt_proxy: MqttProxy | None = None
        self._mqtt_proxy_task: asyncio.Task | None = None
        self._claude_chat = ClaudeChat(self)
        self._claude_chat.start()

        if ble_address:
            self._init_ble(ble_address)

    def _init_ble(self, address: str):
        self.ble = BLEHandler(address, self.stats)
        self.ble.on_packet_received = self._on_packet
        self.ble.on_disconnected = self._on_disconnected

    # -- state machine event emitter -------------------------------------------

    def _ble_rssi_fields(self) -> dict:
        ble_rssi = self.ble.get_rssi() if self.ble else None
        return {
            "ble_rssi": ble_rssi,
            "ble_rssi_pct": max(0, min(100, round((ble_rssi + 100) / 60 * 100))) if ble_rssi is not None else None,
        }

    async def _emit(self, event_type: str, **data):
        """Broadcast a typed state-machine event."""
        await self.state._broadcast({
            "type": event_type,
            "ts": int(time.time()),
            "ble_state": self.ble_state,
            **data,
        })

    def _emit_task(self, event_type: str, **data):
        """Schedule _emit as a task — safe to call from sync contexts."""
        try:
            asyncio.get_running_loop().create_task(self._emit(event_type, **data))
        except RuntimeError:
            pass

    def current_snapshot(self) -> dict:
        """Current state — sent to new WS subscribers immediately on connect."""
        ble_rssi = self.ble.get_rssi() if self.ble else None
        return {
            "type": "snapshot",
            "ts": int(time.time()),
            "ble_state": self.ble_state,
            "ble_address": self.ble_address,
            "ble_error": self.ble_error,
            "ble_rssi": ble_rssi,
            "ble_rssi_pct": max(0, min(100, round((ble_rssi + 100) / 60 * 100))) if ble_rssi is not None else None,
            "config_complete": self.state.config_complete,
            "node_count": len(self.state.nodes),
            "my_node_num": self.my_node_num,
            "mqtt_proxy": self._mqtt_proxy is not None and not self._mqtt_proxy._stopped,
            "last_rx_snr": self.state.last_rx_snr,
            "last_rx_rssi": self.state.last_rx_rssi,
            "has_my_info": bool(self.state.my_info),
            "has_mqtt_config": self.state.mqtt_config_ready.is_set(),
        }

    async def start(self):
        if not self.ble:
            raise RuntimeError("No BLE device configured")
        if self.tcp_gateway:
            await self.tcp_gateway.start()
        self.ble_state = "connecting"
        self._emit_task("connecting", address=self.ble_address)
        logger.info(f"Connecting to BLE device: {self.ble_address}")
        try:
            await self.ble.connect(pin=self.ble_pin)
        except Exception as e:
            self.ble_state = "error"
            self.ble_error = str(e)
            self._emit_task("error", message=str(e))
            raise
        self.ble_state = "syncing"
        self._emit_task("syncing")
        logger.info("BLE connected, requesting config…")
        await self._request_config()

    async def stop(self):
        """Full shutdown — stop TCP gateway and disconnect BLE."""
        self._user_disconnect = True
        self._cancel_mqtt_proxy()
        if self.tcp_gateway:
            await self.tcp_gateway.stop()
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
        self._emit_task("connecting", address=address)
        try:
            async with self._reconnect_lock:
                if self.ble:
                    await self._ble_disconnect_safe()
                self.ble_address = address
                self.ble_pin = pin
                self.state.config_complete = False
                self.state.nodes.clear()
                self.state.my_info_ready.clear()
                self.state.mqtt_config_ready.clear()
                self.state.config_complete_event.clear()
                self._cancel_mqtt_proxy()
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
            self._emit_task("syncing")
            logger.info("BLE connected, requesting config…")
            asyncio.create_task(self._safe_request_config())
            self._mqtt_proxy_task = asyncio.create_task(self._start_mqtt_proxy())
        except Exception as e:
            logger.error(f"BLE connect_to failed: {e}")
            self.ble_state = "error"
            self.ble_error = str(e)
            self.ble_address = None
            self.ble = None
            self._emit_task("error", message=str(e))

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
        self._emit_task("idle")
        if self.ble:
            await self._ble_disconnect_safe()
            self.ble = None
        self.state.config_complete = False
        self.state.my_info_ready.clear()
        self.state.mqtt_config_ready.clear()
        self.state.config_complete_event.clear()
        self._cancel_mqtt_proxy()

    async def _on_packet(self, data: bytes):
        if self.tcp_gateway:
            self.tcp_gateway.broadcast(data)
        await self.state.handle_from_radio_bytes(data)
        if self.state.config_complete and self.ble_state == "syncing":
            self.ble_state = "ready"
            ble_rssi = self.ble.get_rssi() if self.ble else None
            self._emit_task(
                "ready",
                ble_address=self.ble_address,
                ble_rssi=ble_rssi,
                ble_rssi_pct=max(0, min(100, round((ble_rssi + 100) / 60 * 100))) if ble_rssi is not None else None,
                config_complete=True,
                node_count=len(self.state.nodes),
                my_node_num=self.my_node_num,
                mqtt_proxy=self._mqtt_proxy is not None and not self._mqtt_proxy._stopped,
                has_my_info=bool(self.state.my_info),
                has_mqtt_config=self.state.mqtt_config_ready.is_set(),
            )

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
        self._emit_task("reconnecting")
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
                    self._emit_task("syncing")
                    self.state.config_complete = False
                    self.state.my_info_ready.clear()
                    self.state.mqtt_config_ready.clear()
                    self.state.config_complete_event.clear()
                    self._cancel_mqtt_proxy()
                    asyncio.create_task(self._safe_request_config())
                    self._mqtt_proxy_task = asyncio.create_task(self._start_mqtt_proxy())
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

    def _cancel_mqtt_proxy(self):
        """Cancel the startup task and stop any running proxy. Synchronous — safe to call anywhere."""
        was_running = self._mqtt_proxy is not None
        if self._mqtt_proxy_task and not self._mqtt_proxy_task.done():
            self._mqtt_proxy_task.cancel()
        self._mqtt_proxy_task = None
        if self._mqtt_proxy:
            self._mqtt_proxy.stop()
            self._mqtt_proxy = None
        self.state.on_mqtt_proxy_message = None
        if was_running:
            self._emit_task("mqtt_proxy_down")

    async def _start_mqtt_proxy(self):
        """Wait for required state then start the MQTT proxy. Runs as a background task."""
        try:
            await self.state.my_info_ready.wait()
            self._emit_task("sync_progress", has_my_info=True, has_mqtt_config=False,
                            config_complete=self.state.config_complete,
                            node_count=len(self.state.nodes))
            await self.state.mqtt_config_ready.wait()
            self._emit_task("sync_progress", has_my_info=True, has_mqtt_config=True,
                            config_complete=self.state.config_complete,
                            node_count=len(self.state.nodes))
        except asyncio.CancelledError:
            logger.debug("MqttProxy startup cancelled")
            raise

        cfg = self.state.module_config.get("mqtt", {})
        if not cfg.get("enabled") or not cfg.get("proxy_to_client_enabled"):
            logger.info("MqttProxy: not starting (enabled=%s proxy_to_client_enabled=%s)",
                        cfg.get("enabled"), cfg.get("proxy_to_client_enabled"))
            return

        num = self.state.my_info.get("my_node_num")
        if not num:
            logger.error("MqttProxy: my_node_num not available, cannot start")
            return
        client_id = f"!{num:x}"

        loop = asyncio.get_running_loop()
        proxy = MqttProxy(cfg, client_id, loop, self._on_mqtt_downlink)
        self._mqtt_proxy = proxy
        self.state.on_mqtt_proxy_message = proxy.publish
        asyncio.create_task(self._emit("mqtt_proxy_up"))

        await loop.run_in_executor(None, proxy.start)

    async def _on_mqtt_downlink(self, topic: str, payload: bytes, retain: bool):
        """Wrap a broker downlink in ToRadio.mqtt_message and forward to radio via BLE."""
        if not self.ble:
            logger.debug("MqttProxy downlink dropped: BLE not connected")
            return
        try:
            to_radio = mesh_pb2.ToRadio()
            to_radio.mqtt_message.topic = topic
            to_radio.mqtt_message.data = payload
            to_radio.mqtt_message.retained = retain
            await self.ble.send(to_radio.SerializeToString())
            logger.debug("MqttProxy downlink forwarded to radio: topic=%s len=%d", topic, len(payload))
        except Exception as e:
            logger.warning("MqttProxy downlink forward error: %s", e)

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

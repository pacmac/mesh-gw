"""Orchestrates the BLE link and the decoded mesh state, and builds
outgoing ToRadio messages from plain dicts (no protobuf in callers)."""
import asyncio
import logging
import random
import subprocess
import time

from google.protobuf import json_format
from meshtastic import mesh_pb2, admin_pb2, portnums_pb2

from .ble_handler import BLEHandler
from .mqtt_proxy import MqttProxy
from .stats import StatsCollector
from .state import MeshState
from .tcp_gateway import TcpGateway

logger = logging.getLogger(__name__)


_adapter_state_cache: dict = {}
_adapter_state_ts: float = 0.0
_ADAPTER_CACHE_TTL = 30.0  # seconds

def _query_adapter_state() -> dict:
    """Read BlueZ adapter state from bluetoothctl show. Cached for 30s."""
    global _adapter_state_cache, _adapter_state_ts
    now = time.monotonic()
    if _adapter_state_cache and (now - _adapter_state_ts) < _ADAPTER_CACHE_TTL:
        return _adapter_state_cache
    try:
        out = subprocess.run(
            ["bluetoothctl", "show"],
            capture_output=True, text=True, timeout=5,
        ).stdout
        if not out or "No default controller" in out:
            result = {"adapter_state": "missing", "adapter_name": None, "adapter_discovering": False}
        else:
            powered = "Powered: yes" in out
            discovering = "Discovering: yes" in out
            name = None
            for line in out.splitlines():
                if line.strip().startswith("Name:"):
                    name = line.split(":", 1)[1].strip()
                    break
            result = {
                "adapter_state": "up" if powered else "down",
                "adapter_discovering": discovering,
                "adapter_name": name,
            }
    except FileNotFoundError:
        result = {"adapter_state": "missing", "adapter_name": None, "adapter_discovering": False}
    except Exception:
        result = {"adapter_state": "unknown", "adapter_name": None, "adapter_discovering": False}
    _adapter_state_cache = result
    _adapter_state_ts = now
    return result

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
        self.ble_state: str = "idle"   # idle|connecting|syncing|ready|reconnecting|failed|error
        self.ble_error: str | None = None
        self._mqtt_proxy: MqttProxy | None = None
        self._mqtt_proxy_task: asyncio.Task | None = None
        self._reboot_waiter: asyncio.Future | None = None  # set during write_and_reboot

        # Connection stability tracking
        self.reconnect_count: int = 0          # unexpected disconnects in this session
        self.last_disconnect_ts: float | None = None
        self.last_disconnect_rssi: int | None = None
        self.reboot_reason: str | None = None  # config_save | unexpected | user_disconnect

        # Error history — last 10 errors with timestamps
        self._error_history: list[dict] = []   # [{ts, message}, ...]

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

    def _record_error(self, message: str):
        """Append to error history (capped at 10 entries)."""
        self._error_history.append({"ts": time.time(), "message": message})
        if len(self._error_history) > 10:
            self._error_history = self._error_history[-10:]

    def _ble_link_fields(self) -> dict:
        """BLE link-layer + stability fields included in every state event."""
        ble_rssi = self.ble.get_rssi() if self.ble else None
        return {
            "ble_rssi": ble_rssi,
            "ble_rssi_pct": max(0, min(100, round((ble_rssi + 100) / 60 * 100))) if ble_rssi is not None else None,
            "is_found":             self.ble.is_found      if self.ble else False,
            "is_paired":            self.ble.is_paired     if self.ble else False,
            "is_trusted":           self.ble.is_trusted    if self.ble else False,
            "mtu_size":             self.ble.mtu_size      if self.ble else None,
            "pin_required":         self.ble.pin_required  if self.ble else False,
            "auth_failed":          self.ble.auth_failed   if self.ble else False,
            "reconnect_count":      self.reconnect_count,
            "last_disconnect_ts":   self.last_disconnect_ts,
            "last_disconnect_rssi": self.last_disconnect_rssi,
            "reboot_reason":        self.reboot_reason,
            "error_history":        list(self._error_history),
        }

    async def _emit(self, event_type: str, **data):
        """Broadcast a typed state-machine event — always includes BLE link fields."""
        await self.state._broadcast({
            "type": event_type,
            "ts": int(time.time()),
            "ble_state": self.ble_state,
            **self._ble_link_fields(),
            **_query_adapter_state(),
            **data,  # caller kwargs override link fields if explicitly set
        })

    def _emit_task(self, event_type: str, **data):
        """Schedule _emit as a task — safe to call from sync contexts."""
        try:
            asyncio.get_running_loop().create_task(self._emit(event_type, **data))
        except RuntimeError:
            pass

    def current_snapshot(self) -> dict:
        """Current state — sent to new WS subscribers immediately on connect."""
        local = self.state.nodes.get(str(self.my_node_num), {}).get("user", {}) if self.my_node_num else {}
        hw_model = self.state.metadata.get("hw_model") or None
        from .ota_esp32 import is_nrf52
        gw = self.tcp_gateway
        return {
            "type": "snapshot",
            "ts": int(time.time()),
            "ble_state": self.ble_state,
            "ble_address": self.ble_address,
            "ble_error": self.ble_error,
            **self._ble_link_fields(),
            **_query_adapter_state(),
            "config_complete": self.state.config_complete,
            "node_count": len(self.state.nodes),
            "my_node_num": self.my_node_num,
            "mqtt_proxy": self._mqtt_proxy is not None and not self._mqtt_proxy._stopped,
            "last_rx_snr": self.state.last_rx_snr,
            "last_rx_rssi": self.state.last_rx_rssi,
            "has_my_info": bool(self.state.my_info),
            "has_mqtt_config": self.state.mqtt_config_ready.is_set(),
            # Device identity — so subscribers never need HTTP /devices
            "short_name": local.get("short_name", ""),
            "long_name": local.get("long_name", ""),
            "hw_model": hw_model,
            "firmware_version": self.state.metadata.get("firmware_version") or None,
            "ota_protocol": "nrf52-dfu" if is_nrf52(hw_model or "") else "esp32-unified-ota",
            "tcp_port": gw.port if gw else None,
            "tcp_clients": gw.client_count if gw else 0,
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
            self._record_error(str(e))
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

    def _resolve_reboot_waiter(self, success: bool):
        """Resolve the write_and_reboot future if one is waiting."""
        if self._reboot_waiter and not self._reboot_waiter.done():
            try:
                self._reboot_waiter.set_result(success)
            except asyncio.InvalidStateError:
                pass

    async def write_and_reboot(self, send_fn) -> dict:
        """Write config packets, trigger reboot if needed, wait for reconnect.

        The bridge owns this entire lifecycle — callers just supply the send
        function and await the result. No external event coordination needed.
        """
        if self.ble_state != "ready":
            raise RuntimeError(f"Device not connected (ble_state={self.ble_state})")
        if self._reboot_waiter is not None:
            raise RuntimeError("A save operation is already in progress")

        self._reboot_waiter = asyncio.get_running_loop().create_future()

        try:
            # Write packets — GATT error here means device disconnected mid-write (reboot started)
            try:
                await asyncio.wait_for(send_fn(), timeout=10)
            except asyncio.TimeoutError:
                raise RuntimeError("Write timed out — BLE too slow")
            except Exception:
                # Yield once so _on_disconnected callback task can run and update ble_state
                await asyncio.sleep(0)
                if self.ble_state not in ("reconnecting", "syncing", "ready"):
                    raise RuntimeError("Write failed — device not rebooting")

            # Trigger reboot only if the device hasn't already started disconnecting
            if self.ble_state == "ready":
                try:
                    await self.send_admin({"reboot_seconds": 2}, want_response=False)
                except Exception:
                    await asyncio.sleep(0)
                    if self.ble_state not in ("reconnecting", "syncing"):
                        raise RuntimeError("Reboot command failed")

            # Wait for _on_disconnected to resolve us — no fixed timeout shorter than actual reconnect
            try:
                await asyncio.wait_for(asyncio.shield(self._reboot_waiter), timeout=120)
            except asyncio.TimeoutError:
                raise RuntimeError("Device didn't reconnect within 2 minutes — check device power")

            if not self._reboot_waiter.result():
                raise RuntimeError(f"Device failed to reconnect after max attempts")

            logger.info("write_and_reboot: reconnected OK")
            return {"verified": True}

        finally:
            self._reboot_waiter = None

    async def connect_to(self, address: str, pin: str = "", passkey_future=None):
        """Connect to BLE device — runs as a background task from the endpoint.

        passkey_future: asyncio.Future that resolves with the PIN shown on the
        device screen. Set by POST /ble/pair for dynamic-PIN devices.
        """
        self._user_disconnect = False
        self.ble_state = "connecting"
        self.ble_error = None
        self.reconnect_count = 0
        self.last_disconnect_ts = None
        self.last_disconnect_rssi = None
        self.reboot_reason = None
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
                self.state.disconnected_event.clear()
                self.state.reconnected_event.clear()
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
            self._record_error(str(e))
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
        self._resolve_reboot_waiter(False)
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
        self.state.disconnected_event.clear()
        self.state.reconnected_event.clear()
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
                my_info=self.state.my_info,
                metadata=self.state.metadata,
                config=self.state.config,
                module_config=self.state.module_config,
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

        self.state.disconnected_event.set()
        self.last_disconnect_ts = time.time()
        self.last_disconnect_rssi = self.ble.get_rssi() if self.ble else None
        self.reboot_reason = "config_save" if self._reboot_waiter is not None else "unexpected"
        self.reconnect_count += 1
        self.ble_state = "reconnecting"
        self._emit_task("reconnecting", reboot_reason=self.reboot_reason)

        async with self._reconnect_lock:
            if self._user_disconnect:
                return
            while not self._user_disconnect:
                if await self.ble.attempt_reconnection():
                    logger.info("Reconnected, re-requesting config")
                    self.ble_state = "syncing"
                    self.state.reconnected_event.set()
                    self._emit_task("syncing")
                    self.state.config_complete = False
                    self.state.config_complete_event.clear()
                    self.state.my_info_ready.clear()
                    self.state.mqtt_config_ready.clear()
                    self._cancel_mqtt_proxy()
                    asyncio.create_task(self._safe_request_config())
                    self._mqtt_proxy_task = asyncio.create_task(self._start_mqtt_proxy())
                    self._resolve_reboot_waiter(True)
                    return
                # attempt failed — loop continues with capped backoff delay
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
        # Do NOT suppress the echo — the radio echoes our TX packet back on FROMRADIO
        # as confirmation it was queued for RF transmission. The frontend dedup code
        # absorbs the echo (updates the local TX entry) rather than creating a duplicate.
        await self.ble.send(to_radio.SerializeToString())
        return {"sent": True, "id": packet.id}

    async def send_traceroute(self, to: int):
        """Send a traceroute request to a node. Response arrives as a TRACEROUTE_APP packet."""
        if not self.ble:
            raise RuntimeError("BLE not connected")
        route = mesh_pb2.RouteDiscovery()
        data = mesh_pb2.Data()
        data.portnum = portnums_pb2.PortNum.TRACEROUTE_APP
        data.payload = route.SerializeToString()
        data.want_response = True

        hop_limit = self.state.config.get("lora", {}).get("hop_limit", 3)
        packet = mesh_pb2.MeshPacket()
        packet.id = _random_id()
        packet.to = to
        packet.decoded.CopyFrom(data)
        packet.hop_limit = hop_limit
        packet.want_ack = False

        to_radio = mesh_pb2.ToRadio()
        to_radio.packet.CopyFrom(packet)
        self.state.suppress_packet_id(packet.id)
        await self.ble.send(to_radio.SerializeToString())
        return {"sent": True, "id": packet.id, "to": to}

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

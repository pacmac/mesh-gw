"""Orchestrates the BLE link and the decoded mesh state, and builds
outgoing ToRadio messages from plain dicts (no protobuf in callers)."""
import asyncio
import json
import logging
import random

from google.protobuf import json_format
from meshtastic import mesh_pb2, admin_pb2, portnums_pb2

from . import bridge_config, geo
from .ble_handler import BLEHandler
from .mqtt_proxy import MqttProxy
from .rotator import RotatorBase, load_rotator
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
        self.rotator: RotatorBase | None = self._init_rotator()

        # node IDs known to have a retained <nodeinfo_root>/nodeinfo/<id>
        # cache entry (seeded from retained docs, grown as we publish new ones)
        self._nodeinfo_cached_ids: set[str] = set()

        self.state.on_mqtt_proxy_from_radio = self._on_mqtt_proxy_from_radio
        if ble_address:
            self._init_ble(ble_address)

    def _init_rotator(self) -> RotatorBase | None:
        cfg = bridge_config.load().get("rotator", {})
        if not cfg.get("enabled"):
            return None
        try:
            r = load_rotator(cfg)
            r.on_status = self._on_rotator_status
            return r
        except Exception as e:
            logger.error(f"Failed to load rotator driver: {e}")
            return None

    async def _on_rotator_status(self, status: dict):
        await self.state._broadcast({"type": "rotator", "data": status})

    def _init_ble(self, address: str):
        self.ble = BLEHandler(address, self.stats)
        self.ble.on_packet_received = self._on_packet
        self.ble.on_disconnected = self._on_disconnected

    async def start(self):
        if not self.ble:
            raise RuntimeError("No BLE device configured")
        if self.tcp_gateway:
            await self.tcp_gateway.start()
        if self.rotator:
            self.rotator.start()
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
        """Full shutdown — stop TCP gateway, MQTT proxy, rotator, and disconnect BLE."""
        self._user_disconnect = True
        if self.tcp_gateway:
            await self.tcp_gateway.stop()
        if self.mqtt_proxy:
            self.mqtt_proxy.stop()
            self.mqtt_proxy = None
        if self.rotator:
            await self.rotator.stop()
        if self.ble:
            await self._ble_disconnect_safe()

    async def _ble_disconnect_safe(self):
        try:
            await asyncio.wait_for(self.ble.disconnect(), timeout=8.0)
        except Exception as e:
            logger.warning(f"BLE disconnect warning: {e}")

    # -- dashboard-driven connect/disconnect -----------------------------------

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
        # Wait for BLE stack to fully stabilise after connect before writing
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
        """Disconnect BLE and clear address — called from dashboard."""
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
        if self.state.config_complete:
            if self.ble_state == "syncing":
                self.ble_state = "active"
            if not self.mqtt_proxy:
                self._maybe_start_mqtt_proxy()

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
            for attempt in range(1, self.ble.MAX_RECONNECT_ATTEMPTS + 1):
                if await self.ble.attempt_reconnection():
                    logger.info("Reconnected, re-requesting config")
                    self.ble_state = "syncing"
                    self.state.config_complete = False
                    asyncio.create_task(self._safe_request_config())
                    return
            logger.error("Giving up reconnecting to BLE device")
            self.ble_state = "error"
            self.ble_error = "Lost connection — could not reconnect after max attempts"

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
        nodeinfo_root = bridge_config.load()["mqtt_topics"]["nodeinfo_root"]
        try:
            self.mqtt_proxy = MqttProxy(
                address=cfg["address"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                root=cfg.get("root", "msh"),
                use_tls=cfg.get("tls_enabled", False),
                on_downlink=self._on_mqtt_downlink,
                nodeinfo_root=nodeinfo_root,
            )
            self.mqtt_proxy.on_mqtt_node_update = self._on_mqtt_node_update
            self.mqtt_proxy.on_nodeinfo_cache = self._on_nodeinfo_cache
            logger.info(f"MQTT proxy started -> {cfg['address']} root={cfg.get('root', 'msh')} "
                        f"nodeinfo_root={nodeinfo_root!r}")
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
        # Mark as MQTT-sourced unless the BLE/RF path already cleared the flag
        existing = self.state.nodes.get(str(node["num"]), {})
        if existing.get("via_mqtt") is not False:
            node["via_mqtt"] = True
        self.state._merge_node_info(node)
        self._maybe_publish_nodeinfo_cache(str(node["num"]))
        asyncio.run_coroutine_threadsafe(
            self.state._broadcast({"type": "mqtt_node", "data": node}),
            self.mqtt_proxy.loop,
        )

    def _on_nodeinfo_cache(self, node_id: str, doc: dict):
        """Called from paho thread for retained <nodeinfo_root>/nodeinfo/<id>
        docs (v3 ESP32-compatible cache, see core/bridge_config.py). Seeds
        position/az/km for nodes we haven't heard fresher data from yet."""
        self._nodeinfo_cached_ids.add(node_id)
        existing = self.state.nodes.get(node_id, {})
        node: dict = {"num": int(node_id)} if node_id.lstrip("-").isdigit() else {}
        if not existing.get("position") and doc.get("lat") is not None and doc.get("lon") is not None:
            node["position"] = {
                "latitude_i": int(doc["lat"] * 1e7),
                "longitude_i": int(doc["lon"] * 1e7),
            }
            if doc.get("alt") is not None:
                node["position"]["altitude"] = doc["alt"]
        if not existing.get("user") and (doc.get("ln") or doc.get("sn")):
            node["user"] = {"long_name": doc.get("ln", ""), "short_name": doc.get("sn", "")}
        if not node or "num" not in node:
            return
        existing = self.state.nodes.get(node_id, {})
        if existing.get("via_mqtt") is not False:
            node["via_mqtt"] = True
        self.state._merge_node_info(node)
        asyncio.run_coroutine_threadsafe(
            self.state._broadcast({"type": "mqtt_node", "data": node}),
            self.mqtt_proxy.loop,
        )

    def _home_pos(self):
        """(lat, lon) of this bridge's own node, or None if unknown --
        mirrors updateHomePos() in core/static/app.js."""
        num = self.my_node_num
        if num is None:
            return None
        pos = self.state.nodes.get(str(num), {}).get("position")
        if not pos or not pos.get("latitude_i") or not pos.get("longitude_i"):
            return None
        return pos["latitude_i"] / 1e7, pos["longitude_i"] / 1e7

    def _maybe_publish_nodeinfo_cache(self, node_id: str):
        """If we've heard a position for a node with no existing
        <nodeinfo_root>/nodeinfo/<id> cache entry, publish one (retained)
        in the v3-compatible format so the ESP32 rotator/virtual-compass
        benefit too."""
        if node_id in self._nodeinfo_cached_ids:
            return
        if not self.mqtt_proxy or not self.mqtt_proxy.connected or not self.mqtt_proxy.nodeinfo_root:
            return
        node = self.state.nodes.get(node_id, {})
        pos = node.get("position")
        if not pos or not pos.get("latitude_i") or not pos.get("longitude_i"):
            return
        home = self._home_pos()
        lat, lon = pos["latitude_i"] / 1e7, pos["longitude_i"] / 1e7
        user = node.get("user", {})
        doc = {
            "mac": user.get("id", ""),
            "ln": user.get("long_name", ""),
            "sn": user.get("short_name", ""),
            "lat": lat,
            "lon": lon,
            "az": round(geo.bearing_deg(*home, lat, lon), 1) if home else 0,
            "km": round(geo.haversine_km(*home, lat, lon), 2) if home else 0,
            "id": node_id,
            "alt": pos.get("altitude", 0),
        }
        topic = f"{self.mqtt_proxy.nodeinfo_root}/nodeinfo/{node_id}"
        self.mqtt_proxy.publish(topic, json.dumps(doc).encode(), retained=True)
        self._nodeinfo_cached_ids.add(node_id)

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
        self.state.suppress_packet_id(packet.id)
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

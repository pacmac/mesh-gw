"""Decodes FromRadio protobuf bytes into a JSON-friendly mesh state.

Clients never see protobuf: everything exposed via state/methods is
plain dict/JSON (built with google.protobuf.json_format), and writes
accept the same JSON shape back.
"""
import asyncio
import logging
import time
from collections import deque

from google.protobuf import json_format
from meshtastic import mesh_pb2, admin_pb2, portnums_pb2, telemetry_pb2
from . import bridge_config as _bcfg

logger = logging.getLogger(__name__)


def _to_dict(msg):
    return json_format.MessageToDict(msg, preserving_proto_field_name=True)


class MeshState:
    """Holds the decoded view of the mesh as seen via the connected radio."""

    def __init__(self):
        self.my_info = {}
        self.metadata = {}
        self.nodes = {}          # node_num (str) -> dict
        self.channels = {}        # index (str) -> dict
        self.config = {}          # by config section name
        self.module_config = {}   # by module config section name
        self.config_complete = False
        self.range_test_log = []  # {ts, from_num, rssi, snr, hops, seq}, newest last, max 500

        # request_id -> asyncio.Future, resolved when a matching ADMIN_APP
        # reply (Data.reply_id == request_id) is decoded
        self._pending = {}

        # async queues for websocket subscribers
        self._subscribers = set()

        # called with fr.mqttClientProxyMessage when the radio asks us to
        # publish onto its configured MQTT broker (proxy_to_client_enabled)
        self.on_mqtt_proxy_from_radio = None

        # most recently received mesh-packet signal metrics (any portnum)
        self.last_rx_snr: float | None = None
        self.last_rx_rssi: int | None = None

        # packet IDs we sent ourselves; the radio echoes them back and we
        # suppress that echo from the WS feed (clients already showed it)
        self._suppress_packet_ids: set[int] = set()

        # set by DeviceManager once my_info reveals the real node_id
        self.device_id: str | None = None

        _cache_cfg = _bcfg.load().get("message_cache", {})
        self._cache_enabled: bool = bool(_cache_cfg.get("enabled", False))
        self._cache_max_age: int = int(_cache_cfg.get("max_age_seconds", 86400))
        self._message_cache: deque = deque(maxlen=int(_cache_cfg.get("max_messages", 100)))

    def suppress_packet_id(self, packet_id: int):
        """Register a sent packet ID so its TX echo is not re-broadcast on WS."""
        self._suppress_packet_ids.add(packet_id)
        if len(self._suppress_packet_ids) > 100:
            self._suppress_packet_ids.pop()

    # -- subscriber management ------------------------------------------------
    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=100)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        self._subscribers.discard(q)

    def get_cached_messages(self) -> list[dict]:
        """Return cached text messages within max_age, oldest first, tagged for replay."""
        if not self._cache_enabled:
            return []
        cutoff = time.time() - self._cache_max_age
        return [{**ev, "_replay": True} for ts, ev in self._message_cache if ts >= cutoff]

    def reload_cache_config(self, cfg: dict):
        self._cache_enabled = bool(cfg.get("enabled", False))
        self._cache_max_age = int(cfg.get("max_age_seconds", 86400))
        new_maxlen = int(cfg.get("max_messages", 100))
        if new_maxlen != self._message_cache.maxlen:
            self._message_cache = deque(self._message_cache, maxlen=new_maxlen)

    async def _broadcast(self, event: dict):
        if self.device_id and "device" not in event:
            event = {**event, "device": self.device_id}
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("Dropping event for slow websocket subscriber")

    # -- pending admin replies -------------------------------------------------
    def await_reply(self, request_id: int) -> asyncio.Future:
        fut = asyncio.get_event_loop().create_future()
        self._pending[request_id] = fut
        return fut

    def cancel_wait(self, request_id: int):
        self._pending.pop(request_id, None)

    # -- main decode entrypoint ------------------------------------------------
    async def handle_from_radio_bytes(self, data: bytes):
        fr = mesh_pb2.FromRadio()
        try:
            fr.ParseFromString(data)
        except Exception as e:
            logger.warning(f"Failed to parse FromRadio: {e}")
            return

        which = fr.WhichOneof("payload_variant")

        if which == "my_info":
            self.my_info = _to_dict(fr.my_info)
        elif which == "metadata":
            self.metadata = _to_dict(fr.metadata)
        elif which == "node_info":
            node = _to_dict(fr.node_info)
            # node_info records are the startup NodeDB dump only — live NODEINFO
            # broadcasts arrive as mesh packets (NODEINFO_APP portnum).  Tag as
            # MQTT-sourced so the Hide MQTT filter catches nodes the Yagi never
            # heard directly on RF; a subsequent real RF packet clears the flag.
            num_key = str(node.get("num", ""))
            if num_key and self.nodes.get(num_key, {}).get("via_mqtt") is not False:
                node["via_mqtt"] = True
            self._merge_node_info(node)
        elif which == "channel":
            ch = _to_dict(fr.channel)
            self.channels[str(fr.channel.index)] = ch
        elif which == "config":
            self._merge_config(fr.config)
        elif which == "moduleConfig":
            self._merge_module_config(fr.moduleConfig)
        elif which == "config_complete_id":
            self.config_complete = True
        elif which == "mqttClientProxyMessage":
            if self.on_mqtt_proxy_from_radio:
                await self.on_mqtt_proxy_from_radio(fr.mqttClientProxyMessage)
        suppress_broadcast = False
        if which == "packet":
            suppress_broadcast = await self._handle_mesh_packet(fr.packet)

        # Suppress startup NodeDB dump from WS — clients fetch nodedb via
        # GET /nodes. After config_complete, live NODEINFO arrives as packets.
        if which == "node_info" and not self.config_complete:
            return

        if not suppress_broadcast:
            event = {"type": which, "data": _to_dict(fr)}
            await self._broadcast(event)
            if (which == "packet"
                    and self._cache_enabled
                    and fr.packet.decoded.portnum == portnums_pb2.PortNum.TEXT_MESSAGE_APP):
                tagged = {**event, "device": self.device_id}
                self._message_cache.append((int(time.time()), tagged))

    def _merge_config(self, config_msg):
        which = config_msg.WhichOneof("payload_variant")
        if which:
            self.config[which] = _to_dict(getattr(config_msg, which))

    def _merge_module_config(self, mc_msg):
        which = mc_msg.WhichOneof("payload_variant")
        if which:
            self.module_config[which] = _to_dict(getattr(mc_msg, which))

    def _merge_node_info(self, node: dict):
        num = node.get("num")
        if num is None:
            return
        key = str(num)
        existing = self.nodes.setdefault(key, {})
        existing.update(node)

    async def _handle_mesh_packet(self, pkt) -> bool:
        """Returns True if the WS broadcast for this packet should be suppressed."""
        # Suppress TX echo: we sent this packet, the radio echoed it back
        if pkt.id in self._suppress_packet_ids:
            self._suppress_packet_ids.discard(pkt.id)
            return True

        # Admin replies are correlated to a pending request via request_id
        if pkt.decoded.portnum == portnums_pb2.PortNum.ADMIN_APP:
            req_id = pkt.decoded.reply_id or pkt.decoded.request_id
            fut = self._pending.pop(req_id, None)
            if fut and not fut.done():
                admin = admin_pb2.AdminMessage()
                try:
                    admin.ParseFromString(pkt.decoded.payload)
                    fut.set_result(_to_dict(admin))
                except Exception as e:
                    fut.set_exception(e)
            return

        pkt_from = getattr(pkt, "from")
        node = self.nodes.setdefault(str(pkt_from), {"num": pkt_from})

        if pkt.decoded.portnum == portnums_pb2.PortNum.POSITION_APP:
            pos = mesh_pb2.Position()
            try:
                pos.ParseFromString(pkt.decoded.payload)
            except Exception:
                return
            if pos.HasField("latitude_i") and pos.HasField("longitude_i"):
                node["position"] = _to_dict(pos)

        elif pkt.decoded.portnum == portnums_pb2.PortNum.NODEINFO_APP:
            user = mesh_pb2.User()
            try:
                user.ParseFromString(pkt.decoded.payload)
            except Exception:
                return
            node["user"] = _to_dict(user)

        elif pkt.decoded.portnum == portnums_pb2.PortNum.TELEMETRY_APP:
            tel = telemetry_pb2.Telemetry()
            try:
                tel.ParseFromString(pkt.decoded.payload)
            except Exception:
                return
            which = tel.WhichOneof("variant")
            if which:
                node[which] = _to_dict(getattr(tel, which))

        elif pkt.decoded.portnum == portnums_pb2.PortNum.RANGE_TEST_APP:
            try:
                seq_text = pkt.decoded.payload.decode("utf-8", errors="replace")
            except Exception:
                seq_text = ""
            entry = {
                "ts": int(time.time()),
                "from_num": pkt_from,
                "rssi": pkt.rx_rssi if pkt.rx_rssi else None,
                "snr": round(pkt.rx_snr, 1) if pkt.rx_snr else None,
                "hops": max(0, pkt.hop_start - pkt.hop_limit) if pkt.hop_start else 0,
                "seq": seq_text,
            }
            self.range_test_log.append(entry)
            if len(self.range_test_log) > 500:
                self.range_test_log = self.range_test_log[-500:]

        if pkt.rx_snr:
            node["snr"] = pkt.rx_snr
            self.last_rx_snr = pkt.rx_snr
        if pkt.rx_rssi:
            node["rssi"] = pkt.rx_rssi
            self.last_rx_rssi = pkt.rx_rssi
        if pkt.hop_start:
            node["hops"] = max(0, pkt.hop_start - pkt.hop_limit)
        # Track whether this node has ever been heard via RF (not just MQTT)
        if pkt.via_mqtt:
            node.setdefault("via_mqtt", True)
        else:
            node["via_mqtt"] = False   # RF packet — clear the flag
        node["last_heard"] = int(time.time())
        return False

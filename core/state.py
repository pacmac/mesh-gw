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
import struct
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

        # Session passkey returned by the device in admin responses (fw >= 2.7.18).
        # Must be echoed back in sensitive admin sends (OTA, factory-reset, etc.).
        self.session_passkey: bytes = b""

        # async queues for websocket subscribers
        self._subscribers = set()


        # most recently received mesh-packet signal metrics (any portnum)
        self.last_rx_snr: float | None = None
        self.last_rx_rssi: int | None = None

        # packet IDs we sent ourselves; the radio echoes them back and we
        # suppress that echo from the WS feed (clients already showed it)
        self._suppress_packet_ids: set[int] = set()

        # set by DeviceManager once my_info reveals the real node_id
        self.device_id: str | None = None

        # set by bridge when MqttProxy is active; called with (topic, payload, retain)
        self.on_mqtt_proxy_message = None

        # one-shot events fired progressively during BLE config dump
        self.my_info_ready = asyncio.Event()       # my_info received — device_id derivable
        self.mqtt_config_ready = asyncio.Event()   # moduleConfig.mqtt received — broker config available
        self.config_complete_event = asyncio.Event()  # config_complete_id received — full dump done

        # fence counter — incremented on every config_complete_id; lets _write_and_verify
        # detect a post-reboot dump without the asyncio.Event clear/wait race
        self.config_complete_fence: int = 0

        # phase events for _write_and_verify — set/cleared by bridge.py
        self.disconnected_event = asyncio.Event()  # set on BLE drop, cleared before write
        self.reconnected_event = asyncio.Event()   # set on BLE reconnect (syncing), cleared before write

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
            self.my_info_ready.set()
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
            if "mqtt" in self.module_config:
                self.mqtt_config_ready.set()
        elif which == "config_complete_id":
            self.config_complete = True
            self.config_complete_fence += 1
            self.config_complete_event.set()
        elif which == "mqttClientProxyMessage":
            msg = fr.mqttClientProxyMessage
            payload = msg.data if msg.data else msg.text.encode()
            payload_type = "text" if msg.text else "binary"
            logger.info(f"mqttClientProxyMessage: topic={msg.topic} len={len(payload)} type={payload_type} retained={msg.retained}")
            if self.on_mqtt_proxy_message:
                self.on_mqtt_proxy_message(msg.topic, payload, msg.retained)
            if msg.data:
                try:
                    from meshtastic import mqtt_pb2
                    env = mqtt_pb2.ServiceEnvelope()
                    env.ParseFromString(msg.data)
                    pkt = env.packet
                    logger.debug(f"  ServiceEnvelope: channel={env.channel_id} gateway={env.gateway_id} portnum={pkt.decoded.portnum}")
                    if pkt.decoded.portnum == portnums_pb2.PortNum.RANGE_TEST_APP:
                        pkt_from = getattr(pkt, "from")
                        try:
                            seq_text = pkt.decoded.payload.decode("utf-8", errors="replace")
                        except Exception:
                            seq_text = ""
                        entry = {
                            "ts":       int(time.time()),
                            "from_num": pkt_from,
                            "rssi":     pkt.rx_rssi if pkt.rx_rssi else None,
                            "snr":      round(pkt.rx_snr, 1) if pkt.rx_snr else None,
                            "hops":     max(0, pkt.hop_start - pkt.hop_limit) if pkt.hop_start else 0,
                            "seq":      seq_text or None,
                            "via_mqtt": True,
                        }
                        self.range_test_log.append(entry)
                        if len(self.range_test_log) > 500:
                            self.range_test_log = self.range_test_log[-500:]
                        await self._broadcast({"type": "range_test_entry", "data": entry})
                        logger.info(f"range_test_entry (MQTT proxy): from=!{pkt_from:x} seq={seq_text}")
                except Exception as e:
                    logger.debug(f"  (ServiceEnvelope decode failed: {e})")
        suppress_broadcast = False
        if which == "packet":
            suppress_broadcast = await self._handle_mesh_packet(fr.packet)

        # Suppress startup NodeDB dump from WS — clients fetch nodedb via
        # GET /nodes. After config_complete, live NODEINFO arrives as packets.
        if which == "node_info" and not self.config_complete:
            return

        if not suppress_broadcast:
            event = {"type": which, "data": _to_dict(fr)}
            # Inject decoded sub-objects that protobuf serialises as opaque payload bytes.
            # _to_dict() leaves packet.decoded.payload as base64 — we add the structured
            # form alongside it so consumers don't need to re-parse protobuf.
            if which == "packet":
                portnum = fr.packet.decoded.portnum
                pkt_from = getattr(fr.packet, "from")
                node = self.nodes.get(str(pkt_from), {})
                pkt_dict = event["data"].get("packet", {})
                dec_dict = pkt_dict.get("decoded", {})
                if portnum == portnums_pb2.PortNum.POSITION_APP and "position" in node:
                    dec_dict["position"] = node["position"]
                elif portnum in (portnums_pb2.PortNum.NODEINFO_APP, portnums_pb2.PortNum.TEXT_MESSAGE_APP) and "user" in node:
                    dec_dict["user"] = node["user"]
                elif portnum == portnums_pb2.PortNum.TRACEROUTE_APP:
                    try:
                        rd = mesh_pb2.RouteDiscovery()
                        rd.ParseFromString(fr.packet.decoded.payload)
                        dec_dict["route_discovery"] = {
                            "route":       list(rd.route),
                            "route_back":  list(rd.route_back),
                            "snr_towards": list(rd.snr_towards),
                            "snr_back":    list(rd.snr_back),
                        }
                    except Exception:
                        pass
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

        # ROUTING_APP — mesh ACK/NAK for a DM we sent (want_ack=True)
        if pkt.decoded.portnum == portnums_pb2.PortNum.ROUTING_APP:
            try:
                routing = mesh_pb2.Routing()
                routing.ParseFromString(pkt.decoded.payload)
                error_reason = routing.error_reason
                # request_id on the routing packet points back to the original TX packet
                orig_id = pkt.decoded.request_id
                if orig_id:
                    await self._broadcast({
                        "type": "routing_ack",
                        "packet_id": orig_id,
                        "error_reason": error_reason,
                        "error_name": mesh_pb2.Routing.ErrorReason.Name(error_reason),
                        "from_num": getattr(pkt, "from"),
                    })
            except Exception as e:
                logger.debug(f"ROUTING_APP parse error: {e}")
            return True  # suppress raw packet broadcast

        # Admin replies are correlated to a pending request via request_id
        if pkt.decoded.portnum == portnums_pb2.PortNum.ADMIN_APP:
            req_id = pkt.decoded.reply_id or pkt.decoded.request_id
            fut = self._pending.pop(req_id, None)
            if fut and not fut.done():
                admin = admin_pb2.AdminMessage()
                try:
                    admin.ParseFromString(pkt.decoded.payload)
                    if admin.session_passkey:
                        self.session_passkey = bytes(admin.session_passkey)
                        logger.debug("admin session passkey captured (%d bytes)", len(self.session_passkey))
                    fut.set_result(_to_dict(admin))
                except Exception as e:
                    fut.set_exception(e)
            return

        pkt_from = getattr(pkt, "from")
        logger.debug(f"PKT portnum={pkt.decoded.portnum} from={hex(pkt_from & 0xffffffff)} hop_start={pkt.hop_start} hop_limit={pkt.hop_limit} payload_len={len(pkt.decoded.payload)}")
        if pkt.decoded.portnum == portnums_pb2.PortNum.RANGE_TEST_APP:
            logger.info(f"RANGE_TEST_APP: device={self.device_id} from=!{pkt_from:x} rssi={pkt.rx_rssi} snr={pkt.rx_snr}")
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
                own_num = self.my_info.get("my_node_num")
                if own_num and pkt_from == own_num:
                    await self._broadcast({"type": "telemetry_update", "from_num": pkt_from, "variant": which, "data": node[which]})

        elif pkt.decoded.portnum == 256:  # PRIVATE_APP — tilt telemetry from LIS3DH
            payload = bytes(pkt.decoded.payload)
            if len(payload) == 20:
                roll, pitch, ax, ay, az = struct.unpack('<fffff', payload)
                tilt = {"roll": round(roll, 2), "pitch": round(pitch, 2),
                        "x": round(ax, 4), "y": round(ay, 4), "z": round(az, 4)}
                node["tilt"] = tilt
                await self._broadcast({"type": "tilt_update", "from_num": pkt_from, "data": tilt})

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
                "seq": seq_text or None,
            }
            self.range_test_log.append(entry)
            if len(self.range_test_log) > 500:
                self.range_test_log = self.range_test_log[-500:]
            await self._broadcast({"type": "range_test_entry", "data": entry})

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
        await self._broadcast({"type": "node_update", "data": dict(node)})
        return False

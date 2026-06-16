"""Bridges FromRadio/ToRadio mqttClientProxyMessage to a real MQTT broker.

When a Meshtastic device has moduleConfig.mqtt.enabled and
proxy_to_client_enabled set (typical for radios without their own
internet, e.g. BLE-only), it expects its connected client (normally a
phone app) to relay MQTT traffic on its behalf:
  - device -> broker: FromRadio.mqttClientProxyMessage{topic, data}
  - broker -> device: ToRadio.mqttClientProxyMessage{topic, data}
This module plays that role using paho-mqtt.
"""
import asyncio
import json
import logging

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MqttProxy:
    def __init__(self, address: str, username: str, password: str,
                 root: str, use_tls: bool, on_downlink, nodeinfo_root: str = ""):
        """on_downlink: async callback(topic: str, payload: bytes), scheduled
        on the asyncio loop running when this is constructed.

        nodeinfo_root: if set, also subscribe to
        "<nodeinfo_root>/nodeinfo/#" -- the v3 ESP32-compatible retained
        per-node position cache (see core/bridge_config.py)."""
        self.root = root
        self.nodeinfo_root = nodeinfo_root
        self.on_downlink = on_downlink
        self.on_mqtt_node_update = None   # callback(node_dict) for /json/ parsed nodes
        self.on_nodeinfo_cache = None     # callback(node_id: str, doc: dict) for retained nodeinfo cache docs
        self.loop = asyncio.get_event_loop()
        self.connected = False

        host, _, port_s = address.partition(":")
        port = int(port_s) if port_s else (8883 if use_tls else 1883)

        self.client = mqtt.Client()
        if username:
            self.client.username_pw_set(username, password)
        if use_tls:
            self.client.tls_set()

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        self.client.connect_async(host, port, keepalive=60)
        self.client.loop_start()

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            topic = f"{self.root}/#"
            client.subscribe(topic)
            logger.info(f"MQTT proxy connected to broker, subscribed {topic}")
            if self.nodeinfo_root:
                cache_topic = f"{self.nodeinfo_root}/nodeinfo/#"
                client.subscribe(cache_topic)
                logger.info(f"Subscribed to nodeinfo cache {cache_topic}")
        else:
            logger.error(f"MQTT proxy connect failed: rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        logger.warning(f"MQTT proxy disconnected from broker: rc={rc}")

    def _on_message(self, client, userdata, msg):
        logger.info(f"MQTT downlink <- {msg.topic} ({len(msg.payload)} bytes)")
        cache_prefix = f"{self.nodeinfo_root}/nodeinfo/"
        if self.nodeinfo_root and msg.topic.startswith(cache_prefix):
            node_id = msg.topic[len(cache_prefix):]
            if node_id and self.on_nodeinfo_cache:
                self._try_parse_nodeinfo_cache(node_id, msg.payload)
            return
        if "/2/json/" in msg.topic and self.on_mqtt_node_update:
            self._try_parse_json_node(msg.payload)
        asyncio.run_coroutine_threadsafe(self.on_downlink(msg.topic, msg.payload), self.loop)

    def _try_parse_nodeinfo_cache(self, node_id: str, payload: bytes):
        if not payload:
            return  # empty retained message = topic cleared
        try:
            doc = json.loads(payload)
        except Exception:
            return
        self.on_nodeinfo_cache(node_id, doc)

    def _try_parse_json_node(self, payload: bytes):
        try:
            data = json.loads(payload)
        except Exception:
            return
        pkt_type = data.get("type")
        pkt_from = data.get("from")
        if not pkt_from or pkt_type not in ("nodeinfo", "position"):
            return
        node: dict = {"num": pkt_from}
        if pkt_type == "nodeinfo":
            p = data.get("payload", {})
            node["user"] = {
                "id": p.get("id", ""),
                "long_name": p.get("longname") or p.get("long_name", ""),
                "short_name": p.get("shortname") or p.get("short_name", ""),
                "hw_model": str(p.get("hardware", "")),
            }
        else:  # position
            p = data.get("payload", {})
            # Meshtastic JSON uses float lat/lon or integer latitude_i/longitude_i (1e-7)
            lat = p.get("latitude") or (p.get("latitude_i", 0) / 1e7)
            lon = p.get("longitude") or (p.get("longitude_i", 0) / 1e7)
            if not (lat and lon):
                return
            pos = {"latitude_i": int(lat * 1e7), "longitude_i": int(lon * 1e7)}
            if p.get("altitude"):
                pos["altitude"] = p["altitude"]
            node["position"] = pos
        # rx signal if present
        if data.get("rxSnr"):
            node["snr"] = data["rxSnr"]
        if data.get("rxRssi"):
            node["rssi"] = data["rxRssi"]
        logger.debug(f"MQTT /json/ node update: num={pkt_from} type={pkt_type}")
        self.on_mqtt_node_update(node)

    def publish(self, topic: str, payload: bytes, retained: bool = False):
        logger.info(f"MQTT uplink -> {topic} ({len(payload)} bytes)")
        self.client.publish(topic, payload, retain=retained)

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

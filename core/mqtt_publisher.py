"""MQTT publisher: streams decoded bridge events to an MQTT broker and
accepts control commands back.

Topic structure (prefix configurable, default "mesh"):

  Published:
    {prefix}/bridge/online              retained bool — bridge liveness (LWT)
    {prefix}/bridge/status              retained JSON — device list + counts
    {prefix}/{node_id}/nodeinfo         JSON — long_name, short_name, hw_model, role
    {prefix}/{node_id}/position         JSON — latitude, longitude, altitude
    {prefix}/{node_id}/telemetry        JSON — battery, voltage, uptime, snr, rssi,
                                               channel_utilization, temperature, humidity, ...
    {prefix}/{node_id}/message          JSON — text, to, channel, timestamp

  Subscribed (commands):
    {prefix}/bridge/cmd/connect         JSON {address, pin?}
    {prefix}/bridge/cmd/disconnect      JSON {node_id}
    {prefix}/bridge/cmd/send            JSON {node_id, text, to?, channel?}
    {prefix}/bridge/cmd/status          any  — trigger status publish

  Optional HA MQTT discovery (ha_discovery: true):
    {ha_prefix}/sensor/{object_id}/config     retained JSON — sensor entities
    {ha_prefix}/device_tracker/{id}/config    retained JSON — GPS tracker
"""
import asyncio
import base64
import json
import logging
import time
from typing import TYPE_CHECKING

import paho.mqtt.client as mqtt

if TYPE_CHECKING:
    from module.device_manager import DeviceManager

logger = logging.getLogger(__name__)


def _node_key(num: int) -> str:
    return f"!{num:x}"


class MqttPublisher:
    def __init__(self, cfg: dict, dm: "DeviceManager"):
        self._cfg = cfg
        self._dm = dm
        self._prefix = cfg.get("topic_prefix", "mesh").rstrip("/")
        self._ha_discovery = cfg.get("ha_discovery", False)
        self._ha_prefix = cfg.get("ha_discovery_prefix", "homeassistant")
        self._loop = asyncio.get_event_loop()
        self._task: asyncio.Task | None = None
        self._ha_published: set[int] = set()  # node nums with discovery configs published
        self.connected = False

        address = cfg.get("broker", "localhost")
        host, _, port_s = address.partition(":")
        port = int(port_s) if port_s else int(cfg.get("port", 1883))

        self.client = mqtt.Client(client_id="mesh-gw-bridge")
        username = cfg.get("username", "")
        if username:
            self.client.username_pw_set(username, cfg.get("password", ""))
        if cfg.get("use_tls", False):
            self.client.tls_set()

        self.client.will_set(
            f"{self._prefix}/bridge/online", "false", retain=True,
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        self.client.connect_async(host, port, keepalive=60)
        self.client.loop_start()

    def start(self):
        self._task = asyncio.create_task(self._run(), name="mqtt-publisher")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._pub(f"{self._prefix}/bridge/online", "false", retained=True)
        self.client.loop_stop()
        self.client.disconnect()

    # -- paho callbacks (run in paho thread) -----------------------------------

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            logger.error("MQTT publisher connect failed: rc=%d", rc)
            return
        self.connected = True
        cmd_topic = f"{self._prefix}/bridge/cmd/#"
        client.subscribe(cmd_topic)
        logger.info("MQTT publisher connected, subscribed to %s", cmd_topic)
        asyncio.run_coroutine_threadsafe(self._publish_status(), self._loop)

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        logger.warning("MQTT publisher disconnected: rc=%d", rc)

    def _on_message(self, client, userdata, msg):
        cmd_prefix = f"{self._prefix}/bridge/cmd/"
        if not msg.topic.startswith(cmd_prefix):
            return
        cmd = msg.topic[len(cmd_prefix):]
        try:
            payload = json.loads(msg.payload) if msg.payload else {}
        except Exception:
            payload = {}
        asyncio.run_coroutine_threadsafe(self._dispatch_cmd(cmd, payload), self._loop)

    # -- command dispatch (asyncio thread) -------------------------------------

    async def _dispatch_cmd(self, cmd: str, payload: dict):
        try:
            if cmd == "connect":
                address = (payload.get("address") or "").strip()
                if address:
                    await self._dm.connect(address, pin=payload.get("pin", ""))
                    logger.info("MQTT cmd: connecting %s", address)
            elif cmd == "disconnect":
                node_id = (payload.get("node_id") or "").strip()
                if node_id:
                    await self._dm.disconnect(node_id)
                    logger.info("MQTT cmd: disconnecting %s", node_id)
            elif cmd == "send":
                node_id = (payload.get("node_id") or "").strip()
                text = (payload.get("text") or "").strip()
                bridge = self._dm.get(node_id) if node_id else None
                if bridge and text:
                    to = int(payload.get("to", 0xFFFFFFFF))
                    channel = int(payload.get("channel", 0))
                    await bridge.send_text(text, to=to, channel=channel)
                    logger.info("MQTT cmd: sent text via %s", node_id)
            elif cmd == "status":
                await self._publish_status()
            else:
                logger.warning("Unknown MQTT command: %s", cmd)
        except Exception as e:
            logger.error("MQTT command '%s' failed: %s", cmd, e)

    # -- event loop ------------------------------------------------------------

    async def _run(self):
        q = self._dm.subscribe()
        try:
            while True:
                event = await q.get()
                if self.connected:
                    await self._handle_event(event)
        except asyncio.CancelledError:
            pass
        finally:
            self._dm.unsubscribe(q)

    async def _handle_event(self, event: dict):
        ev_type = event.get("type")
        if ev_type == "packet":
            await self._handle_packet(event)
        elif ev_type == "my_info":
            await self._publish_status()

    async def _handle_packet(self, event: dict):
        pkt = (event.get("data") or {}).get("packet", {})
        decoded = pkt.get("decoded", {})
        portnum = decoded.get("portnum", "")
        from_num = pkt.get("from")
        if not from_num:
            return

        node_key = _node_key(from_num)
        snr = pkt.get("rx_snr")
        rssi = pkt.get("rx_rssi")

        if portnum == "NODEINFO_APP":
            user = decoded.get("user", {})
            self._pub(f"{self._prefix}/{node_key}/nodeinfo", json.dumps({
                "long_name":  user.get("long_name", ""),
                "short_name": user.get("short_name", ""),
                "hw_model":   user.get("hw_model", ""),
                "role":       user.get("role", "CLIENT"),
                "id":         user.get("id", node_key),
            }))
            if self._ha_discovery:
                self._publish_ha_discovery(from_num, user)

        elif portnum == "POSITION_APP":
            pos = decoded.get("position", {})
            lat_i = pos.get("latitude_i")
            lon_i = pos.get("longitude_i")
            if lat_i is not None and lon_i is not None:
                self._pub(f"{self._prefix}/{node_key}/position", json.dumps({
                    "latitude":  lat_i / 1e7,
                    "longitude": lon_i / 1e7,
                    "altitude":  pos.get("altitude"),
                }))

        elif portnum == "TELEMETRY_APP":
            tel = decoded.get("telemetry", {})
            dm = tel.get("device_metrics", {})
            em = tel.get("environment_metrics", {})
            payload: dict = {}
            if dm:
                payload.update({k: v for k, v in {
                    "battery_level":        dm.get("battery_level"),
                    "voltage":              dm.get("voltage"),
                    "uptime_seconds":       dm.get("uptime_seconds"),
                    "channel_utilization":  dm.get("channel_utilization"),
                    "air_util_tx":          dm.get("air_util_tx"),
                }.items() if v is not None})
            if em:
                payload.update({k: v for k, v in {
                    "temperature":          em.get("temperature"),
                    "relative_humidity":    em.get("relative_humidity"),
                    "barometric_pressure":  em.get("barometric_pressure"),
                }.items() if v is not None})
            if snr is not None:
                payload["snr"] = snr
            if rssi is not None:
                payload["rssi"] = rssi
            if payload:
                self._pub(f"{self._prefix}/{node_key}/telemetry", json.dumps(payload))

        elif portnum == "TEXT_MESSAGE_APP":
            raw = decoded.get("payload", "")
            try:
                text = base64.b64decode(raw).decode("utf-8")
            except Exception:
                text = str(raw)
            to_num = pkt.get("to", 0xFFFFFFFF)
            self._pub(f"{self._prefix}/{node_key}/message", json.dumps({
                "from":      node_key,
                "from_num":  from_num,
                "to":        _node_key(to_num) if to_num != 0xFFFFFFFF else "broadcast",
                "to_num":    to_num,
                "channel":   pkt.get("channel", 0),
                "text":      text,
                "timestamp": int(time.time()),
            }))

    # -- status and discovery --------------------------------------------------

    async def _publish_status(self):
        devices = self._dm.list_devices()
        self._pub(f"{self._prefix}/bridge/online", "true", retained=True)
        self._pub(f"{self._prefix}/bridge/status", json.dumps({
            "online":       True,
            "device_count": len([d for d in devices if d.get("ble_state") == "active"]),
            "devices": [{
                "node_id":   d["node_id"],
                "ble_state": d["ble_state"],
                "node_count": d.get("node_count", 0),
            } for d in devices],
            "timestamp": int(time.time()),
        }), retained=True)

    def _publish_ha_discovery(self, node_num: int, user: dict):
        if node_num in self._ha_published:
            return
        node_key = _node_key(node_num)
        long_name  = user.get("long_name") or user.get("short_name") or node_key
        short_name = user.get("short_name") or node_key
        prefix = self._prefix
        ha     = self._ha_prefix

        device = {
            "identifiers":    [f"meshtastic_{node_num}"],
            "name":           long_name,
            "model":          user.get("hw_model", "Meshtastic"),
            "manufacturer":   "Meshtastic",
            "via_device":     "mesh-gw-bridge",
        }

        entities = [
            ("sensor", f"mesh_{node_num}_battery", {
                "name": f"{short_name} Battery",
                "state_topic": f"{prefix}/{node_key}/telemetry",
                "value_template": "{{ value_json.battery_level }}",
                "unit_of_measurement": "%",
                "device_class": "battery",
                "state_class": "measurement",
            }),
            ("sensor", f"mesh_{node_num}_voltage", {
                "name": f"{short_name} Voltage",
                "state_topic": f"{prefix}/{node_key}/telemetry",
                "value_template": "{{ value_json.voltage }}",
                "unit_of_measurement": "V",
                "device_class": "voltage",
                "state_class": "measurement",
            }),
            ("sensor", f"mesh_{node_num}_uptime", {
                "name": f"{short_name} Uptime",
                "state_topic": f"{prefix}/{node_key}/telemetry",
                "value_template": "{{ value_json.uptime_seconds }}",
                "unit_of_measurement": "s",
                "device_class": "duration",
                "state_class": "total_increasing",
            }),
            ("sensor", f"mesh_{node_num}_chan_util", {
                "name": f"{short_name} Channel Util",
                "state_topic": f"{prefix}/{node_key}/telemetry",
                "value_template": "{{ value_json.channel_utilization }}",
                "unit_of_measurement": "%",
                "state_class": "measurement",
            }),
            ("sensor", f"mesh_{node_num}_snr", {
                "name": f"{short_name} SNR",
                "state_topic": f"{prefix}/{node_key}/telemetry",
                "value_template": "{{ value_json.snr }}",
                "unit_of_measurement": "dB",
                "state_class": "measurement",
                "enabled_by_default": False,
            }),
            ("sensor", f"mesh_{node_num}_temperature", {
                "name": f"{short_name} Temperature",
                "state_topic": f"{prefix}/{node_key}/telemetry",
                "value_template": "{{ value_json.temperature }}",
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "enabled_by_default": False,
            }),
            ("sensor", f"mesh_{node_num}_humidity", {
                "name": f"{short_name} Humidity",
                "state_topic": f"{prefix}/{node_key}/telemetry",
                "value_template": "{{ value_json.relative_humidity }}",
                "unit_of_measurement": "%",
                "device_class": "humidity",
                "state_class": "measurement",
                "enabled_by_default": False,
            }),
            ("device_tracker", f"mesh_{node_num}", {
                "name": f"{short_name} Location",
                "json_attributes_topic": f"{prefix}/{node_key}/position",
                "source_type": "gps",
            }),
        ]

        for component, object_id, cfg in entities:
            self.client.publish(
                f"{ha}/{component}/{object_id}/config",
                json.dumps({**cfg, "unique_id": object_id, "device": device}),
                retain=True,
            )

        self._ha_published.add(node_num)
        logger.info("HA discovery published for %s (%s)", long_name, node_key)

    # -- internal --------------------------------------------------------------

    def _pub(self, topic: str, payload: str, retained: bool = False):
        if self.connected:
            self.client.publish(topic, payload, retain=retained)

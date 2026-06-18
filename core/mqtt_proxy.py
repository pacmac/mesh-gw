"""Per-device MQTT proxy — bridges one radio's mqttClientProxyMessage uplinks to
a broker and forwards broker downlinks back to the radio via ToRadio.mqtt_message.

Each MeshBridge owns one MqttProxy instance. Client ID is the radio's node ID
(e.g. !3f172791) — same as the phone app uses, ensuring only one proxy is active
at a time per radio.
"""
import asyncio
import logging
import ssl

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

_RC_CODES = {
    1: "unacceptable protocol version",
    2: "identifier rejected",
    3: "server unavailable",
    4: "bad username or password",
    5: "not authorised",
}


class MqttProxy:
    def __init__(self, cfg: dict, client_id: str, loop: asyncio.AbstractEventLoop, on_downlink):
        """
        cfg:         radio's module_config['mqtt'] dict
        client_id:   radio node ID, e.g. !3f172791 — never empty
        loop:        running asyncio event loop (for scheduling BLE sends from paho thread)
        on_downlink: async callable(topic, payload, retain) — called for each broker→radio message
        """
        self._cfg = cfg
        self._client_id = client_id
        self._loop = loop
        self._on_downlink = on_downlink
        self._client: mqtt.Client | None = None
        self._stopped = False
        self._root = cfg.get("root") or "msh"

    def start(self):
        """Blocking connect + start background network thread. Run in executor."""
        cfg = self._cfg
        address = cfg.get("address") or ""
        tls_enabled = bool(cfg.get("tls_enabled", False))
        port = int(cfg.get("port") or 0) or (8883 if tls_enabled else 1883)
        username = cfg.get("username") or ""
        password = cfg.get("password") or ""

        if not address:
            logger.error("MqttProxy: no broker address configured")
            return

        client = mqtt.Client(client_id=self._client_id, clean_session=True)

        if username:
            client.username_pw_set(username, password or None)

        if tls_enabled:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            client.tls_set_context(ctx)
            client.tls_insecure_set(True)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        self._client = client
        logger.info("MqttProxy connecting: broker=%s:%d client_id=%s tls=%s",
                    address, port, self._client_id, tls_enabled)
        try:
            client.connect(address, port, keepalive=60)
        except Exception as e:
            logger.error("MqttProxy connect failed: %s", e)
            self._client = None
            return

        client.loop_start()

    def stop(self):
        """Stop the paho network thread and disconnect. Run in executor to avoid blocking asyncio."""
        self._stopped = True
        client = self._client
        self._client = None
        if client:
            try:
                client.disconnect()
                client.loop_stop()
            except Exception:
                pass
        logger.info("MqttProxy stopped")

    def publish(self, topic: str, payload: bytes, retain: bool = False):
        """Publish a radio uplink to the broker. Thread-safe — called from asyncio loop."""
        if not self._client or self._stopped:
            return
        result = self._client.publish(topic, payload, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning("MqttProxy publish error: rc=%d topic=%s", result.rc, topic)
        else:
            logger.info("MqttProxy radio→broker: topic=%s len=%d", topic, len(payload))

    # -- paho callbacks (run in paho's network thread) ------------------------

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("MqttProxy connected (client_id=%s)", self._client_id)
            client.subscribe(f"{self._root}/#")
            logger.info("MqttProxy subscribed: %s/#", self._root)
        else:
            logger.error("MqttProxy connection refused: rc=%d (%s)",
                         rc, _RC_CODES.get(rc, "unknown"))

    def _on_disconnect(self, client, userdata, rc):
        if rc == 0:
            logger.info("MqttProxy disconnected cleanly")
        else:
            logger.warning("MqttProxy unexpected disconnect: rc=%d", rc)

    def _on_message(self, client, userdata, msg):
        if self._stopped:
            return
        logger.info("MqttProxy broker→radio: topic=%s len=%d", msg.topic, len(msg.payload))
        asyncio.run_coroutine_threadsafe(
            self._on_downlink(msg.topic, bytes(msg.payload), bool(msg.retain)),
            self._loop,
        )

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
import logging

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MqttProxy:
    def __init__(self, address: str, username: str, password: str,
                 root: str, use_tls: bool, on_downlink):
        """on_downlink: async callback(topic: str, payload: bytes), scheduled
        on the asyncio loop running when this is constructed."""
        self.root = root
        self.on_downlink = on_downlink
        self.loop = asyncio.get_event_loop()

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
            topic = f"{self.root}/#"
            client.subscribe(topic)
            logger.info(f"MQTT proxy connected to broker, subscribed {topic}")
        else:
            logger.error(f"MQTT proxy connect failed: rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        logger.warning(f"MQTT proxy disconnected from broker: rc={rc}")

    def _on_message(self, client, userdata, msg):
        asyncio.run_coroutine_threadsafe(self.on_downlink(msg.topic, msg.payload), self.loop)

    def publish(self, topic: str, payload: bytes, retained: bool = False):
        self.client.publish(topic, payload, retain=retained)

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

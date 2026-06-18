"""Manages N simultaneous BLE+Mesh bridge connections.

Each connection is a MeshBridge keyed by its node ID ('!3f172791'
format) once my_info has arrived from the radio. Before that it is
held under a temporary 'ble:<ADDRESS>' key.
"""
import asyncio
import logging
from typing import Optional

from core import bridge_config as _bcfg
from core.bridge import MeshBridge

logger = logging.getLogger(__name__)


class DeviceManager:
    def __init__(self):
        # node_id -> MeshBridge (after my_info received)
        self._devices: dict[str, MeshBridge] = {}
        # BLE address (upper) -> node_id reverse lookup
        self._by_ble: dict[str, str] = {}
        # temp_key -> MeshBridge (before node_id known)
        self._pending: dict[str, MeshBridge] = {}
        # per-device watcher tasks
        self._watchers: dict[str, asyncio.Task] = {}
        # unified WS subscriber queues
        self._subscribers: set[asyncio.Queue] = set()
        # pending passkey futures for dynamic-PIN pairing: addr(upper) -> Future[str]
        self._passkey_futures: dict[str, asyncio.Future] = {}
        # optional MQTT publisher
        self._mqtt_publisher = None

    # -- device lifecycle -------------------------------------------------------

    async def connect(self, ble_address: str, pin: str = "", passkey_future=None, tcp_port: int | None = None) -> str:
        """Start a BLE connection. Returns temp key ('ble:<ADDR>') immediately;
        the device is re-keyed to '!{node_id}' once my_info arrives."""
        addr = ble_address.upper()
        if addr in self._by_ble:
            node_id = self._by_ble[addr]
            logger.info("Already connected to %s as %s", addr, node_id)
            return node_id

        temp_key = f"ble:{addr}"
        if temp_key in self._pending:
            logger.info("Already connecting to %s", addr)
            return temp_key

        bridge = MeshBridge(ble_address, ble_pin=pin, tcp_port=tcp_port)
        self._pending[temp_key] = bridge

        task = asyncio.create_task(
            self._watch_bridge(temp_key, addr, bridge),
            name=f"watch-{temp_key}",
        )
        self._watchers[temp_key] = task

        asyncio.create_task(bridge.connect_to(ble_address, pin=pin, passkey_future=passkey_future))
        logger.info("Connecting to %s (temp key: %s)", addr, temp_key)
        return temp_key

    async def pair_device(self, ble_address: str, tcp_port: int | None = None) -> str:
        """Initiate connection for a dynamic-PIN device. The pairing process
        will pause at the passkey prompt — call resolve_passkey() with the PIN
        shown on the device screen to complete it. Returns the temp key."""
        addr = ble_address.upper()
        # Cancel any stale future for this address
        old = self._passkey_futures.pop(addr, None)
        if old and not old.done():
            old.cancel()
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._passkey_futures[addr] = future
        return await self.connect(ble_address, passkey_future=future, tcp_port=tcp_port)

    def resolve_passkey(self, ble_address: str, passkey: str):
        """Supply the PIN seen on the device screen to complete pairing."""
        addr = ble_address.upper()
        future = self._passkey_futures.pop(addr, None)
        if future is None or future.done():
            raise ValueError(f"No pending passkey request for {ble_address}")
        future.set_result(passkey)

    async def disconnect(self, node_id: str):
        """Disconnect and remove a device by node_id or temp key."""
        bridge = self._devices.pop(node_id, None)
        if bridge is not None:
            ble_addr = (bridge.ble_address or "").upper()
            self._by_ble.pop(ble_addr, None)
            self._cancel_watcher(node_id)
        else:
            # Try pending (could be passed a temp key or a BLE address)
            temp_key = node_id if node_id.startswith("ble:") else f"ble:{node_id.upper()}"
            bridge = self._pending.pop(temp_key, None)
            if bridge is None:
                logger.warning("disconnect: unknown device %s", node_id)
                return
            self._cancel_watcher(temp_key)

        try:
            await asyncio.wait_for(bridge.stop(), timeout=8.0)
        except Exception as e:
            logger.warning("Error stopping bridge for %s: %s", node_id, e)
        logger.info("Disconnected %s", node_id)

    def _cancel_watcher(self, key: str):
        task = self._watchers.pop(key, None)
        if task and not task.done():
            task.cancel()

    def get(self, node_id: str) -> Optional[MeshBridge]:
        """Look up a bridge by node_id ('!3f172791')."""
        return self._devices.get(node_id)

    def start_mqtt_publisher(self, cfg: dict):
        from core.mqtt_publisher import MqttPublisher
        self._mqtt_publisher = MqttPublisher(cfg, self)
        self._mqtt_publisher.start()
        return self._mqtt_publisher

    def get_mqtt_publisher(self):
        return self._mqtt_publisher

    async def stop_mqtt_publisher(self):
        if self._mqtt_publisher:
            await self._mqtt_publisher.stop()
            self._mqtt_publisher = None

    def get_by_ble(self, ble_address: str) -> Optional[MeshBridge]:
        node_id = self._by_ble.get(ble_address.upper())
        return self._devices.get(node_id) if node_id else None

    @property
    def bridge_node_nums(self) -> set:
        """Node nums of all connected bridge radios — used to exclude own devices."""
        return {b.my_node_num for b in self._devices.values() if b.my_node_num}

    def list_devices(self) -> list[dict]:
        result = []
        for node_id, bridge in self._devices.items():
            gw = bridge.tcp_gateway
            rssi = bridge.ble.get_rssi() if bridge.ble else None
            local = bridge.state.nodes.get(str(bridge.my_node_num), {}).get("user", {}) if bridge.my_node_num else {}
            ble_cfg = _bcfg.get_ble_device(bridge.ble_address) if bridge.ble_address else {}
            result.append({
                "node_id": node_id,
                "short_name": local.get("short_name", ""),
                "long_name": local.get("long_name", ""),
                "ble_address": bridge.ble_address,
                "ble_state": bridge.ble_state,
                "ble_error": bridge.ble_error,
                "ble_rssi": rssi,
                "ble_rssi_pct": max(0, min(100, round((rssi + 100) / 60 * 100))) if rssi is not None else None,
                "node_count": len(bridge.state.nodes),
                "config_complete": bridge.state.config_complete,
                "tcp_port": gw.port if gw else None,
                "tcp_clients": gw.client_count if gw else 0,
                "auto_connect": ble_cfg.get("auto_connect", True),
            })
        for temp_key, bridge in self._pending.items():
            gw = bridge.tcp_gateway
            result.append({
                "node_id": temp_key,
                "ble_address": bridge.ble_address,
                "ble_state": bridge.ble_state,
                "ble_error": bridge.ble_error,
                "node_count": 0,
                "config_complete": False,
                "tcp_port": gw.port if gw else None,
                "tcp_clients": 0,
            })
        return result

    async def reload_config(self):
        """Re-read bridge_config.yaml and apply changes without restarting BLE."""
        from core import bridge_config as _bcfg
        cfg = _bcfg.load()
        logger.info("Reloading config")

        cache_cfg = cfg.get("message_cache", {})
        for bridge in list(self._devices.values()) + list(self._pending.values()):
            bridge.state.reload_cache_config(cache_cfg)

        mqtt_cfg = cfg.get("mqtt_publish", {})
        pub = self.get_mqtt_publisher()
        if mqtt_cfg.get("enabled"):
            if pub:
                await self.stop_mqtt_publisher()
            self.start_mqtt_publisher(mqtt_cfg)
        else:
            if pub:
                await self.stop_mqtt_publisher()

        logger.info("Config reloaded")
        return {"reloaded": True, "message_cache": cache_cfg, "mqtt_enabled": bool(mqtt_cfg.get("enabled"))}

    async def stop_all(self):
        await self.stop_mqtt_publisher()
        for task in list(self._watchers.values()):
            if not task.done():
                task.cancel()
        coros = [
            asyncio.wait_for(b.stop(), timeout=8.0)
            for b in list(self._devices.values()) + list(self._pending.values())
        ]
        await asyncio.gather(*coros, return_exceptions=True)
        self._devices.clear()
        self._pending.clear()
        self._by_ble.clear()
        self._watchers.clear()

    # -- internal bridge watcher ------------------------------------------------

    async def _watch_bridge(self, temp_key: str, ble_address: str, bridge: MeshBridge):
        """Subscribe to a bridge's event stream, re-tag each event with device
        ID, and re-broadcast to all unified WS subscribers. Re-keys the bridge
        from its temporary BLE key to the real '!{node_id}' on first my_info."""
        q = bridge.state.subscribe()
        node_id = temp_key
        try:
            while True:
                event = await q.get()
                # First my_info: alias temp key -> real node_id
                if node_id == temp_key and event.get("type") == "my_info":
                    num = bridge.state.my_info.get("my_node_num")
                    if num:
                        node_id = f"!{num:x}"
                        self._pending.pop(temp_key, None)
                        self._devices[node_id] = bridge
                        self._by_ble[ble_address] = node_id
                        # Move watcher registration to real key
                        task = self._watchers.pop(temp_key, None)
                        if task:
                            self._watchers[node_id] = task
                        bridge.state.device_id = node_id
                        logger.info("Bridge aliased: %s -> %s", temp_key, node_id)
                await self._broadcast({**event, "device": node_id})
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Watcher for %s crashed: %s", node_id, e)
        finally:
            bridge.state.unsubscribe(q)

    # -- unified WS subscriber management --------------------------------------

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        self._subscribers.discard(q)

    async def _broadcast(self, event: dict):
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("Dropping event for slow unified WS subscriber")

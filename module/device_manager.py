"""Thin device registry — owns the BleDevice lifecycle.

Source of truth: docs/BLE-SPEC.md § "core/device_manager.py — thin registry only"

No state beyond _devices. No background tasks. reconcile() is the single
place where the device list changes.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from core.ble_device import BleDevice
from core.config import BleConfig, BleDeviceConfig, OtaConfig, load as _load_config

logger = logging.getLogger(__name__)


class DeviceManager:
    def __init__(self, queue: asyncio.Queue) -> None:
        self._queue = queue
        self._devices: dict[str, BleDevice] = {}
        self._ble_cfg: Optional[BleConfig] = None
        self._ota_cfg: Optional[OtaConfig] = None

        # WS subscriber queues — drained by server broadcast loop
        self._subscribers: set[asyncio.Queue] = set()

    # -- Registry interface ------------------------------------------------

    def add(self, device: BleDevice) -> None:
        self._devices[device.addr] = device

    def remove(self, addr: str) -> Optional[BleDevice]:
        return self._devices.pop(addr.upper(), None)

    def get(self, addr_or_node_id: str) -> Optional[BleDevice]:
        if not addr_or_node_id:
            return None
        # Try BLE address lookup first
        dev = self._devices.get(addr_or_node_id.upper())
        if dev:
            return dev
        # Fall back to node_id (!hexstring)
        return self.get_by_node_id(addr_or_node_id)

    def get_by_node_id(self, node_id: str) -> Optional[BleDevice]:
        for dev in self._devices.values():
            if dev.node_id == node_id:
                return dev
        return None

    def get_by_ble(self, addr: str) -> Optional[BleDevice]:
        return self._devices.get(addr.upper())

    def all(self) -> list[BleDevice]:
        return list(self._devices.values())

    def list_devices(self) -> list[dict]:
        result = []
        for dev in self._devices.values():
            d = dev.data
            result.append({
                "addr": dev.addr,
                "node_id": dev.node_id,
                "state": dev.state,
                "short_name": d.short_name,
                "long_name": d.long_name,
                "hw_model": d.hw_model,
                "firmware_version": d.firmware_version,
                "battery_level": d.battery_level,
                "voltage": d.voltage,
                "tcp_port": d.tcp_port,
                "node_count": d.node_count,
            })
        return result

    @property
    def queue(self) -> asyncio.Queue:
        return self._queue

    # -- Lifecycle ---------------------------------------------------------

    async def reconcile(
        self,
        new_configs: list[BleDeviceConfig],
        ble_cfg: BleConfig,
        ota_cfg: OtaConfig,
    ) -> None:
        """Diff new_configs against running _devices; create, update, or stop."""
        self._ble_cfg = ble_cfg
        self._ota_cfg = ota_cfg

        new_by_addr: dict[str, BleDeviceConfig] = {
            cfg.address.upper(): cfg for cfg in new_configs
        }
        current = set(self._devices.keys())
        new_addrs = set(new_by_addr.keys())

        # Stop removed
        for addr in current - new_addrs:
            dev = self._devices.pop(addr)
            logger.info("reconcile: stopping %s (removed from config)", addr)
            await dev.stop()

        # Update kept in-place — BLE connection preserved
        for addr in current & new_addrs:
            logger.debug("reconcile: updating config for %s", addr)
            await self._devices[addr].update_config(new_by_addr[addr])

        # Start new
        for addr in new_addrs - current:
            cfg = new_by_addr[addr]
            logger.info("reconcile: adding %s (auto_connect=%s)", addr, cfg.auto_connect)
            dev = BleDevice(cfg.address, cfg, ble_cfg, ota_cfg, self._queue)
            self._devices[addr] = dev
            if cfg.auto_connect:
                await dev.start()

    async def stop_all(self) -> None:
        for dev in list(self._devices.values()):
            await dev.stop()
        self._devices.clear()

    async def reload_config(self) -> dict:
        new_device_configs, ble_cfg, ota_cfg = _load_config()
        old_count = len(self._devices)
        await self.reconcile(new_device_configs, ble_cfg, ota_cfg)
        return {
            "reloaded": True,
            "device_count": len(self._devices),
            "was": old_count,
        }

    # -- WS subscriber fan-out --------------------------------------------
    # The server drain loop writes to _queue; these subscriber queues let
    # WS connections each get their own copy of every event.

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    async def _broadcast(self, event: dict) -> None:
        """Fan-out a synthetic event (not from the BleDevice queue) to all WS subscribers."""
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

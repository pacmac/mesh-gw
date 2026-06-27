"""bleota GATT wire protocol for esp32-unified-ota v1.0.1.

BleDevice owns all BLE management (scanning, connecting, disconnecting,
state transitions, event emission). OtaSession handles only the bleota-
specific GATT conversation: VERSION handshake, OTA command, chunk writes,
and REBOOT confirmation.

Source of truth: docs/BLE-SPEC.md § "OTA Protocol Detail"
"""
from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from bleak import BleakClient

from .config import OtaConfig

logger = logging.getLogger(__name__)

# bleota GATT UUIDs — same constants as ble_device.py (imported to avoid duplication)
from .ble_device import OTA_WRITE_UUID, OTA_TX_UUID, OTA_FLASHING


class OtaSession:
    """Executes the bleota GATT handshake and firmware flash (ESP32 devices).

    OtaSession is the SSOT for all ESP32 bleota OTA data: firmware file format,
    protocol constants, and config defaults. UI reads FIRMWARE_EXT / FIRMWARE_DESC
    from this class — never from hw_model strings or device config.

    Called by BleDevice._run_ota_flow() with an already-connected BleakClient
    for the bleota GATT service. Returns one of:
      "ok"          — flash complete; device rebooting to Meshtastic
      "nvs_mismatch"— device rejected hash; NVS erase needed
      "error:<msg>" — unexpected failure
    """

    FIRMWARE_EXT  = ".bin"
    FIRMWARE_DESC = "ESP32 OTA firmware (.bin)"

    def __init__(self, *, fw_bytes: bytes, fw_hash: bytes, ota_cfg: OtaConfig) -> None:
        self._fw_bytes = fw_bytes
        self._fw_hash = fw_hash
        self._ota_cfg = ota_cfg
        self._notify_queue: asyncio.Queue = asyncio.Queue()

    def _on_notify(self, _sender, data: bytes) -> None:
        self._notify_queue.put_nowait(data)

    async def _read_response(self, timeout_s: float) -> str:
        """Read one notify response line, stripping trailing whitespace."""
        raw = await asyncio.wait_for(self._notify_queue.get(), timeout=timeout_s)
        return raw.decode(errors="replace").strip()

    async def run(
        self,
        client: BleakClient,
        *,
        on_progress: Callable[[int], None],
        on_transition: Callable[..., None],
    ) -> str:
        """Execute handshake + flash using the already-connected bleota client."""
        await client.start_notify(OTA_TX_UUID, self._on_notify)
        try:
            return await self._handshake_and_flash(client, on_progress, on_transition)
        finally:
            try:
                await asyncio.wait_for(client.stop_notify(OTA_TX_UUID), timeout=3.0)
            except Exception:
                pass

    async def _handshake_and_flash(
        self,
        client: BleakClient,
        on_progress: Callable[[int], None],
        on_transition: Callable[..., None],
    ) -> str:
        timeout = self._ota_cfg.handshake_timeout_s

        # ── VERSION ──────────────────────────────────────────────────
        await client.write_gatt_char(OTA_WRITE_UUID, b"VERSION\n", response=False)
        resp = await self._read_response(timeout)
        logger.debug("bleota VERSION response: %r", resp)
        if not resp.startswith("OK"):
            return f"error:VERSION unexpected: {resp!r}"

        # ── OTA <size> <sha256> ───────────────────────────────────────
        size = len(self._fw_bytes)
        sha256_hex = self._fw_hash.hex()
        cmd = f"OTA {size} {sha256_hex}\n".encode()
        await client.write_gatt_char(OTA_WRITE_UUID, cmd, response=False)
        # Bootloader sends ERASING (async, while erasing) then OK when ready.
        # Loop until we see a terminal response.
        erase_timeout = max(timeout, 60.0)
        while True:
            resp = await self._read_response(erase_timeout)
            logger.debug("bleota OTA response: %r", resp)
            if resp == "ERASING":
                logger.debug("bleota: partition erase in progress…")
                continue
            break
        if resp.startswith("ERR Hash Rejected"):
            return "nvs_mismatch"
        if not resp.startswith("OK"):
            return f"error:OTA unexpected: {resp!r}"

        # ── Flash chunks (BLE: wait for ACK after each chunk) ────────
        on_transition(OTA_FLASHING)
        chunk_size = self._ota_cfg.chunk_size
        total = len(self._fw_bytes)
        offsets = range(0, total, chunk_size)
        n_chunks = len(offsets)
        sent = 0
        last_pct = -1
        last_emit = asyncio.get_running_loop().time()

        for i, offset in enumerate(offsets):
            chunk = self._fw_bytes[offset : offset + chunk_size]
            is_last = (i == n_chunks - 1)
            await client.write_gatt_char(OTA_WRITE_UUID, chunk, response=False)
            sent += len(chunk)

            # BLE requires ACK per chunk; last chunk gets OK (or ERR) from endOta()
            ack = await self._read_response(30.0)
            if is_last:
                if ack.startswith("ERR"):
                    return f"error:flash verify: {ack}"
                if not ack.startswith("OK"):
                    logger.warning("bleota final response unexpected: %r", ack)
            else:
                if ack != "ACK":
                    if ack.startswith("ERR"):
                        return f"error:chunk ack: {ack}"
                    logger.warning("bleota expected ACK, got %r", ack)

            pct = int(sent * 100 / total)
            now = asyncio.get_running_loop().time()
            if pct >= last_pct + 5 or now - last_emit >= 2.0:
                on_progress(pct)
                last_pct = pct
                last_emit = now

        # Device reboots automatically 2 s after sending final OK — no REBOOT needed
        logger.info("OTA flash complete — %d bytes sent", total)
        return "ok"

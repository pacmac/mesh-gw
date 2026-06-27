"""Nordic Legacy DFU wire protocol for nRF52840 Meshtastic devices.

Protocol logic extracted from the proven dfu_lib.py implementation.
All BLE management (scanning, connecting, MTU negotiation, disconnecting,
state transitions, event emission) stays in BleDevice — this module handles
only the Nordic Legacy DFU GATT conversation once BleDevice provides a
connected BleakClient for the DFU bootloader service.

Source of truth: docs/BLE-SPEC.md § "Nordic Legacy DFU Protocol Detail"
"""
from __future__ import annotations

import asyncio
import json
import logging
import struct
import zipfile
from typing import Callable, Optional

from bleak import BleakClient

from .config import OtaConfig
from .ble_device import DFU_CONTROL_POINT_UUID, OTA_FLASHING

logger = logging.getLogger(__name__)

DFU_PACKET_UUID = "00001532-1212-efde-1523-785feabcd123"

# Op codes
_OP_START_DFU             = 0x01
_OP_INIT_DFU_PARAMS       = 0x02
_OP_RECEIVE_FW_IMAGE      = 0x03
_OP_VALIDATE              = 0x04
_OP_ACTIVATE_AND_RESET    = 0x05
_OP_RESET                 = 0x06
_OP_PRN_REQ               = 0x08
_OP_RESPONSE_CODE         = 0x10
_OP_PRN_NOTIF             = 0x11

UPLOAD_MODE_SOFTDEVICE    = 0x01
UPLOAD_MODE_BOOTLOADER    = 0x02
UPLOAD_MODE_SD_BL         = 0x03
UPLOAD_MODE_APPLICATION   = 0x04


class NordicDfuSession:
    """Executes the Nordic Legacy DFU handshake and firmware flash (nRF52840 devices).

    NordicDfuSession is the SSOT for all Nordic OTA data: firmware file format,
    protocol constants, and config defaults. UI reads FIRMWARE_EXT / FIRMWARE_DESC
    from this class — never from hw_model strings or device config.

    Called by BleDevice._run_ota_flow() with an already-connected BleakClient
    for the Nordic DFU GATT service. The caller is responsible for all BLE
    connection lifecycle. Returns one of:
      "ok"          — DFU complete; device rebooting to application
      "error:<msg>" — unexpected failure
    """

    FIRMWARE_EXT  = ".zip"
    FIRMWARE_DESC = "Nordic BLE OTA package (.zip)"

    def __init__(self, *, zip_path: str, ota_cfg: OtaConfig) -> None:
        self._zip_path = zip_path
        self._ota_cfg = ota_cfg
        self._response_queue: asyncio.Queue = asyncio.Queue()
        self._prn_event = asyncio.Event()
        self._reset_in_progress = False
        # Populated by parse_zip()
        self.bin_data: bytes = b""
        self.dat_data: bytes = b""
        self.upload_mode: int = UPLOAD_MODE_APPLICATION
        self.sd_size: int = 0
        self.bl_size: int = 0
        self.app_size: int = 0

    def parse_zip(self) -> None:
        """Parse the DFU zip and populate bin_data, dat_data, sizes, upload_mode.
        Raises on missing or malformed file.
        """
        with zipfile.ZipFile(self._zip_path, "r") as z:
            names = z.namelist()
            if "manifest.json" in names:
                with z.open("manifest.json") as f:
                    manifest = json.load(f).get("manifest", {})

                if "softdevice_bootloader" in manifest:
                    info = manifest["softdevice_bootloader"]
                    self.bin_data = z.read(info["bin_file"])
                    self.dat_data = z.read(info["dat_file"])
                    self.sd_size = info["sd_size"]
                    self.bl_size = info["bl_size"]
                    self.upload_mode = UPLOAD_MODE_SD_BL
                elif "bootloader" in manifest:
                    info = manifest["bootloader"]
                    self.bin_data = z.read(info["bin_file"])
                    self.dat_data = z.read(info["dat_file"])
                    self.bl_size = len(self.bin_data)
                    self.upload_mode = UPLOAD_MODE_BOOTLOADER
                elif "softdevice" in manifest:
                    info = manifest["softdevice"]
                    self.bin_data = z.read(info["bin_file"])
                    self.dat_data = z.read(info["dat_file"])
                    self.sd_size = len(self.bin_data)
                    self.upload_mode = UPLOAD_MODE_SOFTDEVICE
                elif "application" in manifest:
                    info = manifest["application"]
                    self.bin_data = z.read(info["bin_file"])
                    self.dat_data = z.read(info["dat_file"])
                    self.app_size = len(self.bin_data)
                    self.upload_mode = UPLOAD_MODE_APPLICATION
                else:
                    raise ValueError("manifest.json has no recognised firmware entry")
            else:
                # Legacy: no manifest — infer from filenames
                app_bin = next((n for n in names if n.endswith(".bin")), None)
                app_dat = next((n for n in names if n.endswith(".dat")), None)
                if not app_bin:
                    raise ValueError("No .bin found in zip and no manifest.json")
                self.bin_data = z.read(app_bin)
                self.dat_data = z.read(app_dat) if app_dat else b""
                self.app_size = len(self.bin_data)
                self.upload_mode = UPLOAD_MODE_APPLICATION

    def _on_notify(self, _sender, data: bytes) -> None:
        op = data[0]
        if op == _OP_RESPONSE_CODE:
            self._response_queue.put_nowait((data[1], data[2]))  # (request_op, status)
        elif op == _OP_PRN_NOTIF:
            self._prn_event.set()

    async def _wait_response(self, expected_op: int, timeout_s: float = 30.0) -> int:
        """Wait for a response to expected_op. Returns status (1 = success)."""
        request_op, status = await asyncio.wait_for(self._response_queue.get(), timeout=timeout_s)
        if request_op != expected_op:
            raise RuntimeError(f"unexpected response op {request_op:#04x}, want {expected_op:#04x}")
        if status != 1:
            raise RuntimeError(f"DFU op {expected_op:#04x} failed, status={status}")
        return status

    async def run(
        self,
        client: BleakClient,
        *,
        on_progress: Callable[[int], None],
        on_transition: Callable[..., None],
    ) -> str:
        """Execute Nordic Legacy DFU on the already-connected DFU client."""
        await client.start_notify(DFU_CONTROL_POINT_UUID, self._on_notify)
        try:
            await self._dfu_sequence(client, on_progress, on_transition)
            return "ok"
        except Exception as e:
            if self._reset_in_progress:
                # Device disconnects mid-write during ACTIVATE_AND_RESET — that's success
                logger.info("Nordic DFU: device disconnected during reset — success")
                return "ok"
            logger.error("Nordic DFU failed: %s", e)
            return f"error:{e}"
        finally:
            try:
                await asyncio.wait_for(
                    client.stop_notify(DFU_CONTROL_POINT_UUID), timeout=3.0
                )
            except Exception:
                pass

    async def _dfu_sequence(
        self,
        client: BleakClient,
        on_progress: Callable[[int], None],
        on_transition: Callable[..., None],
    ) -> None:
        timeout = self._ota_cfg.handshake_timeout_s
        prn = self._ota_cfg.nordic_prn

        # Clear any stale responses
        while not self._response_queue.empty():
            self._response_queue.get_nowait()

        # ── START DFU ─────────────────────────────────────────────────
        await client.write_gatt_char(
            DFU_CONTROL_POINT_UUID,
            bytes([_OP_START_DFU, self.upload_mode]),
            response=True,
        )
        if self._ota_cfg.nordic_packet_delay_ms:
            await asyncio.sleep(self._ota_cfg.nordic_packet_delay_ms / 1000.0)

        size_payload = struct.pack("<III", self.sd_size, self.bl_size, self.app_size)
        logger.debug("Nordic DFU: sizes SD=%d BL=%d App=%d", self.sd_size, self.bl_size, self.app_size)
        await client.write_gatt_char(DFU_PACKET_UUID, size_payload, response=False)

        await self._wait_response(_OP_START_DFU, timeout_s=60.0)

        # ── INIT PACKET ───────────────────────────────────────────────
        await client.write_gatt_char(
            DFU_CONTROL_POINT_UUID, bytes([_OP_INIT_DFU_PARAMS, 0x00]), response=True
        )
        await client.write_gatt_char(DFU_PACKET_UUID, self.dat_data, response=False)
        await client.write_gatt_char(
            DFU_CONTROL_POINT_UUID, bytes([_OP_INIT_DFU_PARAMS, 0x01]), response=True
        )
        await self._wait_response(_OP_INIT_DFU_PARAMS, timeout_s=timeout)

        # ── PRN CONFIG ────────────────────────────────────────────────
        if prn > 0:
            prn_payload = bytes([_OP_PRN_REQ]) + struct.pack("<H", prn)
            await client.write_gatt_char(DFU_CONTROL_POINT_UUID, prn_payload, response=True)

        # ── RECEIVE FIRMWARE IMAGE ────────────────────────────────────
        await client.write_gatt_char(
            DFU_CONTROL_POINT_UUID, bytes([_OP_RECEIVE_FW_IMAGE]), response=True
        )

        on_transition(OTA_FLASHING)
        await self._stream_firmware(client, on_progress, prn)

        # ── VALIDATE ──────────────────────────────────────────────────
        flash_timeout = max(60.0, len(self.bin_data) / 50_000)
        await self._wait_response(_OP_RECEIVE_FW_IMAGE, timeout_s=flash_timeout)

        await client.write_gatt_char(
            DFU_CONTROL_POINT_UUID, bytes([_OP_VALIDATE]), response=True
        )
        await self._wait_response(_OP_VALIDATE, timeout_s=timeout)

        # ── ACTIVATE AND RESET ────────────────────────────────────────
        self._reset_in_progress = True
        await client.write_gatt_char(
            DFU_CONTROL_POINT_UUID, bytes([_OP_ACTIVATE_AND_RESET]), response=True
        )
        logger.info("Nordic DFU: activate sent — device rebooting")

    async def _stream_firmware(
        self,
        client: BleakClient,
        on_progress: Callable[[int], None],
        prn: int,
    ) -> None:
        mtu = client.mtu_size if client.mtu_size else 23
        chunk_size = min(mtu - 3, 244)
        if chunk_size < 20:
            chunk_size = 20
        logger.debug("Nordic DFU: streaming %d bytes, chunk=%d PRN=%d",
                     len(self.bin_data), chunk_size, prn)

        total = len(self.bin_data)
        sent = 0
        packets_since_prn = 0
        prn_timeout = max(0.8, prn * 0.08) if prn > 0 else 0
        last_pct = -1

        for offset in range(0, total, chunk_size):
            chunk = self.bin_data[offset : offset + chunk_size]
            await client.write_gatt_char(DFU_PACKET_UUID, chunk, response=False)
            sent += len(chunk)
            packets_since_prn += 1

            pct = int(sent * 100 / total)
            if pct > last_pct:
                on_progress(pct)
                last_pct = pct

            if prn > 0 and packets_since_prn >= prn:
                self._prn_event.clear()
                try:
                    await asyncio.wait_for(self._prn_event.wait(), timeout=prn_timeout)
                except asyncio.TimeoutError:
                    logger.debug("Nordic DFU: PRN timeout, continuing")
                packets_since_prn = 0

        on_progress(100)

"""BLE OTA firmware update for ESP32 Meshtastic devices using esp32-unified-ota.

Protocol:
  1. Compute SHA-256 of .bin file
  2. Send AdminMessage(ota_request={reboot_ota_mode, ota_hash}) → device reboots into OTA bootloader
  3. Connect to OTA GATT service, handshake, stream binary in 256-byte chunks with ACK flow control
  4. Device verifies hash, reboots into new firmware
"""

import asyncio
import hashlib
import logging
from pathlib import Path

from bleak import BleakClient, BleakScanner

logger = logging.getLogger(__name__)

# esp32-unified-ota GATT UUIDs
OTA_SERVICE_UUID = "4fafc201-1fb5-459e-8fcc-c5c9c331914b"
OTA_WRITE_UUID   = "62ec0272-3ec5-11eb-b378-0242ac130005"  # client → device (commands + binary)
OTA_TX_UUID      = "62ec0272-3ec5-11eb-b378-0242ac130003"  # device → client (responses, notify)

CHUNK_SIZE       = 256   # bytes per BLE write
SCAN_TIMEOUT     = 30.0  # seconds to wait for OTA bootloader to advertise
REBOOT_WAIT      = 6.0   # seconds to wait after AdminMessage before scanning

NRF52_MODELS = {
    "RAK4631", "NRF52840", "NRF52_DK", "TECHO",
    "TECHO_V0", "TECHO_V1", "TECHO_V2", "PPR1",
}


def is_nrf52(hw_model: str) -> bool:
    return hw_model.upper().replace("-", "_") in NRF52_MODELS


class Esp32OtaError(Exception):
    pass


async def _send_reboot_ota(bridge, fw_hash: bytes) -> None:
    """Send AdminMessage asking device to store hash and reboot into OTA bootloader."""
    await bridge.send_admin(
        {"ota_request": {"reboot_ota_mode": True, "ota_hash": fw_hash}},
        want_response=False,
    )


async def _find_ota_device(ble_addr: str) -> str:
    """Scan for OTA bootloader advertising on the same BLE address."""
    logger.info("Scanning for OTA bootloader at %s (timeout=%ss)…", ble_addr, SCAN_TIMEOUT)
    deadline = asyncio.get_event_loop().time() + SCAN_TIMEOUT
    while asyncio.get_event_loop().time() < deadline:
        devices = await BleakScanner.discover(timeout=3.0, return_adv=False)
        for d in devices:
            if d.address.upper() == ble_addr.upper():
                logger.info("OTA bootloader found: %s (%s)", d.name, d.address)
                return d.address
        logger.debug("OTA bootloader not seen yet, retrying…")
    raise Esp32OtaError(f"OTA bootloader did not appear at {ble_addr} within {SCAN_TIMEOUT}s")


async def ota_update(bridge, device_label: str, bin_path: str,
                     ble_addr: str | None = None,
                     progress_cb=None) -> dict:
    """Full ESP32 OTA: reboot device into bootloader, stream firmware, return result."""
    if not ble_addr:
        raise Esp32OtaError("ble_addr is required")

    fw = Path(bin_path).read_bytes()
    fw_hash = hashlib.sha256(fw).digest()
    fw_hash_hex = fw_hash.hex()
    fw_size = len(fw)
    logger.info("Firmware: %s  size=%d  sha256=%s", Path(bin_path).name, fw_size, fw_hash_hex)

    # Step 1 — tell device to reboot into OTA bootloader
    if bridge is not None:
        logger.info("Sending ota_request AdminMessage to %s", device_label)
        await _send_reboot_ota(bridge, fw_hash)
        # Disconnect bridge so it doesn't hold the BLE slot
        bridge._user_disconnect = True
        try:
            await bridge.disconnect_ble()
        except Exception as e:
            logger.warning("disconnect_ble: %s (continuing)", e)
        logger.info("Waiting %.1fs for device to reboot into OTA bootloader…", REBOOT_WAIT)
        await asyncio.sleep(REBOOT_WAIT)
    else:
        logger.warning("No bridge — skipping AdminMessage, device must already be in OTA mode")

    # Step 2 — find and connect to OTA GATT service
    await _find_ota_device(ble_addr)

    logger.info("Connecting to OTA GATT service at %s…", ble_addr)
    async with BleakClient(ble_addr, timeout=15.0) as client:
        rx_queue: asyncio.Queue[str] = asyncio.Queue()

        def _on_notify(_, data: bytearray):
            text = data.decode(errors="replace").strip()
            logger.debug("[OTA rx] %r", text)
            rx_queue.put_nowait(text)

        await client.start_notify(OTA_TX_UUID, _on_notify)

        async def _recv(timeout: float = 30.0) -> str:
            return await asyncio.wait_for(rx_queue.get(), timeout=timeout)

        async def _send(text: str):
            await client.write_gatt_char(OTA_WRITE_UUID, text.encode(), response=True)

        # Step 3 — handshake
        await _send("VERSION\n")
        ver_resp = await _recv()
        if not ver_resp.startswith("OK"):
            raise Esp32OtaError(f"VERSION rejected: {ver_resp}")
        logger.info("OTA bootloader: %s", ver_resp)

        await _send(f"OTA {fw_size} {fw_hash_hex}\n")

        # Device may send one or more ERASING lines before OK
        while True:
            resp = await _recv(timeout=60.0)
            if resp.startswith("OK"):
                break
            if resp.startswith("ERR"):
                raise Esp32OtaError(f"OTA rejected: {resp}")
            logger.info("[OTA] %s", resp)  # ERASING etc.

        # Step 4 — stream binary in chunks with ACK flow control
        logger.info("Streaming %d bytes in %d-byte chunks…", fw_size, CHUNK_SIZE)
        offset = 0
        total_chunks = (fw_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        chunk_num = 0

        while offset < fw_size:
            chunk = fw[offset:offset + CHUNK_SIZE]
            await client.write_gatt_char(OTA_WRITE_UUID, chunk, response=False)
            ack = await _recv(timeout=30.0)
            if not ack.startswith("ACK"):
                raise Esp32OtaError(f"Unexpected response during stream: {ack!r}")
            offset += len(chunk)
            chunk_num += 1
            pct = round(offset / fw_size * 100)
            if progress_cb:
                progress_cb(pct, fw_size, offset)
            if chunk_num % 20 == 0 or offset >= fw_size:
                logger.info("OTA progress: %d%% (%d/%d bytes)", pct, offset, fw_size)

        # Step 5 — wait for final verification result
        final = await _recv(timeout=30.0)
        if final.startswith("ERR"):
            raise Esp32OtaError(f"Firmware verification failed: {final}")
        logger.info("OTA complete: %s — device rebooting", final)

    if bridge is not None:
        logger.info("OTA done — reconnecting bridge to %s", ble_addr)
        asyncio.create_task(bridge.connect_to(ble_addr, pin=getattr(bridge, "ble_pin", "")))

    return {
        "ok": True,
        "ble_addr": ble_addr,
        "label": device_label,
        "firmware": Path(bin_path).name,
        "sha256": fw_hash_hex,
        "size": fw_size,
    }

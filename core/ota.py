"""BLE OTA firmware update for nRF52 devices using the legacy Nordic DFU protocol.

Flow:
  1. Bridge sends admin `enter_dfu_mode_request` to the target device.
  2. Device disconnects, reboots into the Adafruit bootloader (advertises as DFU target).
  3. We scan for the DFU advertisement, connect, and run the DFU protocol.
  4. Device validates, flashes, and reboots into the new firmware.
"""

import asyncio
import json
import logging
import struct
import zipfile
from pathlib import Path

from bleak import BleakClient, BleakScanner

logger = logging.getLogger(__name__)

# Legacy Nordic DFU service / characteristic UUIDs (Adafruit nRF52 bootloader)
_SVC  = "00001530-1212-efde-1523-785feabcd123"
_CTRL = "00001531-1212-efde-1523-785feabcd123"   # Control Point (notify + write)
_PKT  = "00001532-1212-efde-1523-785feabcd123"   # Packet Data (write no response)

# DFU opcodes
_OP_START_DFU        = 0x01
_OP_INIT_DFU         = 0x02
_OP_RECV_FW          = 0x03
_OP_VALIDATE         = 0x04
_OP_ACTIVATE         = 0x05
_OP_RESET            = 0x06
_OP_REQ_PRN          = 0x08   # packet receipt notification
_OP_RESPONSE         = 0x10
_OP_PRN_NOTIF        = 0x11

_PRN_INTERVAL = 10            # send a notification every N data packets
_CHUNK_SIZE   = 20            # BLE MTU payload

_DFU_NAMES = {"DfuTarg", "DFUTarg", "Adafruit Bluefruit LE", "RAK4631"}


class DfuError(Exception):
    pass


def _parse_ota_zip(zip_path: str):
    """Extract init packet (.dat) and firmware (.bin) from an adafruit-nrfutil OTA zip."""
    with zipfile.ZipFile(zip_path) as z:
        manifest = json.loads(z.read("manifest.json"))
        app = manifest["manifest"]["application"]
        dat = z.read(app["dat_file"])
        fw  = z.read(app["bin_file"])
    return dat, fw


async def _scan_for_dfu(orig_addr: str, timeout: float = 30.0):
    """
    Scan for an nRF52 DFU bootloader advertisement.

    The bootloader usually increments the BLE address by 1, or it may keep
    the same address. We match by service UUID or by device name.
    """
    logger.info("Scanning for DFU target (timeout=%.0fs)…", timeout)
    # Normalise address: uppercase, colon-separated
    orig_norm = orig_addr.upper().replace("-", ":")

    # Derive the expected DFU address (last byte + 1, wrapping)
    try:
        parts = orig_norm.split(":")
        last = (int(parts[-1], 16) + 1) & 0xFF
        dfu_addr = ":".join(parts[:-1] + [f"{last:02X}"])
    except Exception:
        dfu_addr = None

    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        devices = await BleakScanner.discover(timeout=3.0, return_adv=True)
        for addr, (dev, adv) in devices.items():
            addr_norm = addr.upper().replace("-", ":")
            svc_match  = _SVC.upper() in [str(u).upper() for u in (adv.service_uuids or [])]
            name_match = (dev.name or "") in _DFU_NAMES
            addr_match = addr_norm in (orig_norm, dfu_addr)
            if svc_match or (name_match and addr_match):
                logger.info("Found DFU target: %s (%s)", addr, dev.name)
                return addr
        logger.debug("DFU target not found yet, retrying…")

    raise DfuError(f"DFU target not found within {timeout}s")


async def _dfu_flash(address: str, dat: bytes, fw: bytes,
                     progress_cb=None) -> None:
    """Run the legacy Nordic DFU protocol over BLE."""

    loop = asyncio.get_event_loop()
    ctrl_event = asyncio.Event()
    prn_event  = asyncio.Event()
    last_notif = bytearray()

    def _on_notify(_handle, data: bytearray):
        nonlocal last_notif
        last_notif = bytes(data)
        if data[0] == _OP_RESPONSE:
            ctrl_event.set()
        elif data[0] == _OP_PRN_NOTIF:
            prn_event.set()

    async def _ctrl(opcode: int, payload: bytes = b""):
        ctrl_event.clear()
        await client.write_gatt_char(_CTRL, bytes([opcode]) + payload)

    async def _wait_resp(op: int, timeout: float = 30.0):
        await asyncio.wait_for(ctrl_event.wait(), timeout)
        r = last_notif
        if r[0] != _OP_RESPONSE or r[1] != op or r[2] != 0x01:
            raise DfuError(f"Bad response for op 0x{op:02X}: {r.hex()}")

    async def _send_chunks(data: bytes, prn: int):
        for i in range(0, len(data), _CHUNK_SIZE):
            chunk = data[i:i + _CHUNK_SIZE]
            await client.write_gatt_char(_PKT, chunk, response=False)
            pkt_num = (i // _CHUNK_SIZE) + 1
            if prn and pkt_num % prn == 0:
                prn_event.clear()
                await asyncio.wait_for(prn_event.wait(), timeout=10.0)
            if progress_cb and i % (50 * _CHUNK_SIZE) == 0:
                pct = min(100, int(i * 100 / len(data)))
                progress_cb(pct, len(data), i)

    async with BleakClient(address) as client:
        logger.info("Connected to DFU target %s", address)

        await client.start_notify(_CTRL, _on_notify)

        # 1. Start DFU — application image only
        app_size  = len(fw)
        size_pkt  = struct.pack("<III", 0, 0, app_size)   # SD=0, BL=0, APP=size
        await _ctrl(_OP_START_DFU, bytes([0x04]))          # mode = application
        await client.write_gatt_char(_PKT, size_pkt)
        await _wait_resp(_OP_START_DFU, timeout=10.0)
        logger.info("Start DFU OK (app_size=%d)", app_size)

        # 2. Send init packet (.dat)
        await _ctrl(_OP_INIT_DFU, bytes([0x00]))           # init start
        for i in range(0, len(dat), _CHUNK_SIZE):
            await client.write_gatt_char(_PKT, dat[i:i + _CHUNK_SIZE], response=False)
        await _ctrl(_OP_INIT_DFU, bytes([0x01]))           # init end
        await _wait_resp(_OP_INIT_DFU, timeout=60.0)
        logger.info("Init packet OK")

        # 3. Request packet receipt notification every N packets
        await _ctrl(_OP_REQ_PRN, struct.pack("<H", _PRN_INTERVAL))

        # 4. Receive firmware
        await _ctrl(_OP_RECV_FW)
        logger.info("Sending firmware (%d bytes)…", app_size)
        await _send_chunks(fw, _PRN_INTERVAL)
        await _wait_resp(_OP_RECV_FW, timeout=120.0)
        logger.info("Firmware transfer complete")

        if progress_cb:
            progress_cb(90, app_size, app_size)

        # 5. Validate
        await _ctrl(_OP_VALIDATE)
        await _wait_resp(_OP_VALIDATE, timeout=30.0)
        logger.info("Firmware validated OK")

        # 6. Activate — device reboots into new firmware
        await _ctrl(_OP_ACTIVATE)
        logger.info("Activate sent — device will reboot")

        if progress_cb:
            progress_cb(100, app_size, app_size)


async def ota_update(bridge, device_node_id: str, zip_path: str,
                     progress_cb=None) -> dict:
    """
    Full OTA update sequence:
      1. Resolve BLE address from bridge device list.
      2. Disconnect bridge from device.
      3. Send enter_dfu admin message via temporary meshtastic BLE connection.
      4. Scan for DFU bootloader advertisement.
      5. Flash firmware over BLE DFU.
      6. Bridge will auto-reconnect when the device reboots.

    Returns {"ok": True} or raises on failure.
    """
    from meshtastic.ble_interface import BLEInterface
    from meshtastic.node import Node
    from . import bridge_config as _bcfg

    # Resolve BLE address
    ble_addr = None
    for dev in (_bcfg.get("ble_devices") or []):
        if dev.get("node_id") == device_node_id or dev.get("ble_addr") == device_node_id:
            ble_addr = dev.get("ble_addr")
            break
    if not ble_addr:
        raise DfuError(f"No BLE address found for device {device_node_id!r}")

    dat, fw = _parse_ota_zip(zip_path)
    logger.info("OTA zip loaded: dat=%d fw=%d bytes", len(dat), len(fw))

    # Disconnect bridge BLE connection so we can claim the device
    logger.info("Disconnecting bridge from %s for DFU", device_node_id)
    try:
        if bridge is not None:
            await bridge.disconnect_ble()
        await asyncio.sleep(2.0)
    except Exception as e:
        logger.warning("disconnect_ble error (continuing): %s", e)

    # Connect with meshtastic library and send enter_dfu
    logger.info("Sending enter_dfu_mode to %s via %s", device_node_id, ble_addr)
    try:
        iface = BLEInterface(ble_addr, noProto=True)
        node = Node(iface, iface.myInfo, noProto=True)
        node.enterDFUMode()
        await asyncio.sleep(1.0)
        iface.close()
    except Exception as e:
        logger.warning("enter_dfu send failed (%s) — device may already be in DFU mode", e)

    # Give device time to reboot into bootloader
    await asyncio.sleep(5.0)

    # Scan for DFU advertisement
    dfu_addr = await _scan_for_dfu(ble_addr, timeout=30.0)

    # Flash
    await _dfu_flash(dfu_addr, dat, fw, progress_cb=progress_cb)

    # Bridge will auto-reconnect via its normal reconnect loop
    return {"ok": True, "device": device_node_id, "firmware": Path(zip_path).name}

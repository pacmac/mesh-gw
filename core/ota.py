"""BLE OTA firmware update for nRF52 devices using Nordic Legacy DFU.

Delegates to dfu_cli.py (recrof/nrf_dfu_py) which handles:
  - jump_to_bootloader (BLE write to DFU ctrl in Meshtastic app)
  - scan for DFU bootloader
  - perform_update with 3 retries
"""

import asyncio
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


class DfuError(Exception):
    pass


async def _prepare_dfu(bridge, device_node_id: str, ble_addr: str) -> None:
    """Disconnect bridge so the BLE adapter is free for dfu_cli.py."""
    if bridge is not None:
        bridge._user_disconnect = True
        logger.info("Disconnecting bridge for DFU")
        try:
            await bridge.disconnect_ble()
        except Exception as e:
            logger.warning("disconnect_ble error (continuing): %s", e)
        await asyncio.sleep(2.0)


async def ota_update(bridge, device_node_id: str, zip_path: str,
                     ble_addr: str | None = None,
                     progress_cb=None) -> dict:
    """Full OTA: disconnect bridge, run dfu_cli.py, reconnect bridge."""
    from . import bridge_config as _bcfg

    if not ble_addr and bridge is not None:
        ble_addr = getattr(bridge, "ble_address", None)
    if not ble_addr:
        cfg = _bcfg.load()
        for dev in (cfg.get("ble_devices") or []):
            if dev.get("node_id") == device_node_id:
                ble_addr = dev.get("address")
                break
    if not ble_addr:
        raise DfuError(f"No BLE address found for device {device_node_id!r}")

    await _prepare_dfu(bridge, device_node_id, ble_addr)

    dfu_cli = Path(__file__).parent / "dfu_cli.py"
    cmd = [
        sys.executable, str(dfu_cli),
        "--adapter", "hci0",
        "--wait",
        "--verbose",
        "--retry", "3",
        zip_path,
        ble_addr,
    ]
    logger.info("Running: %s", " ".join(cmd))

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    async def _stream_output():
        pct = 0
        async for line in proc.stdout:
            text = line.decode(errors="replace").rstrip()
            logger.info("[dfu] %s", text)
            if "Uploading:" in text:
                try:
                    pct = int(text.split("Uploading:")[1].strip().rstrip("%"))
                    if progress_cb:
                        progress_cb(pct, 0, 0)
                except ValueError:
                    pass

    await _stream_output()
    rc = await proc.wait()

    if rc != 0:
        raise DfuError(f"dfu_cli.py exited with code {rc}")

    if bridge is not None:
        logger.info("Reflashing complete — reconnecting bridge to %s", ble_addr)
        asyncio.create_task(bridge.connect_to(ble_addr, pin=getattr(bridge, "ble_pin", "")))

    return {"ok": True, "device": device_node_id, "firmware": Path(zip_path).name}

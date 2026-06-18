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
    """Disconnect bridge only if it's connected to the OTA target device."""
    if bridge is not None and getattr(bridge, "ble_address", None) == ble_addr:
        bridge._user_disconnect = True
        logger.info("Disconnecting bridge from %s for DFU", ble_addr)
        try:
            await bridge.disconnect_ble()
        except Exception as e:
            logger.warning("disconnect_ble error (continuing): %s", e)
        await asyncio.sleep(2.0)
    else:
        logger.info("Bridge not on OTA target — dfu_cli.py will connect to %s independently", ble_addr)


async def ota_update(bridge, device_label: str, zip_path: str,
                     ble_addr: str | None = None,
                     progress_cb=None) -> dict:
    """Full OTA: disconnect bridge if needed, run dfu_cli.py, return result."""
    if not ble_addr:
        raise DfuError("ble_addr is required")

    await _prepare_dfu(bridge, device_label, ble_addr)

    dfu_cli = Path(__file__).parent / "dfu_cli.py"
    cmd = [
        sys.executable, "-u", str(dfu_cli),
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
        buf = b""
        while True:
            chunk = await proc.stdout.read(256)
            if not chunk:
                break
            buf += chunk
            # cli_progress_handler uses \r (no \n until 100%) — split on both
            parts = buf.replace(b"\r", b"\n").split(b"\n")
            buf = parts[-1]
            for part in parts[:-1]:
                text = part.decode(errors="replace").strip()
                if not text:
                    continue
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

    return {"ok": True, "ble_addr": ble_addr, "label": device_label, "firmware": Path(zip_path).name}

"""BLE connection handling for Meshtastic devices"""
import asyncio
import logging
import subprocess
import time
from typing import Optional, Callable
from bleak import BleakClient, BleakScanner
from .stats import StatsCollector

logger = logging.getLogger(__name__)

# Meshtastic BLE UUIDs
MESHTASTIC_SERVICE_UUID = "6ba1b218-15a8-461f-9fa8-5dcae273eafd"
TORADIO_UUID = "f75c76d2-129e-4dad-a1dd-7866124401e7"  # Write to device
FROMRADIO_UUID = "2c55e69e-4993-11ed-b878-0242ac120002"  # Read from device

# Meshtastic devices use a fixed BLE pairing passkey (not shown on screen)
DEFAULT_BLE_PIN = "123456"


class BLEHandler:
    """Handles BLE connectivity and communication with Meshtastic device"""

    # Reconnection constants
    MAX_RECONNECT_ATTEMPTS = 10  # Increased from 5 - Meshtastic devices can take 2+ minutes to reboot
    INITIAL_RECONNECT_DELAY = 2.0  # seconds
    MAX_RECONNECT_DELAY = 60.0  # seconds
    RECONNECT_BACKOFF_FACTOR = 2.0

    def __init__(self, ble_address: str, stats: StatsCollector):
        self.ble_address = ble_address
        self.stats = stats

        self.client: Optional[BleakClient] = None
        self.poll_task: Optional[asyncio.Task] = None
        self.running = False

        # Reconnection state
        self.reconnect_attempts = 0
        self.services_ready = False  # True only after service discovery completes
        self.disconnection_event = asyncio.Event()

        # Callbacks
        self.on_packet_received: Optional[Callable] = None
        self.on_disconnected: Optional[Callable] = None

        # Set to an asyncio.Future before connecting to a dynamic-PIN device.
        # _bluetoothctl_pair will await it for the passkey instead of using the
        # fixed pin — the future is resolved by POST /ble/passkey.
        self.passkey_future: Optional[asyncio.Future] = None

        # Packet deduplication
        self.last_packet_hash: Optional[int] = None
        self.last_packet_time: float = 0

        # RSSI captured at last connect (device stops advertising once connected
        # so this is the best reading we can get without raw HCI access)
        self.last_scan_rssi: Optional[int] = None

        # BlueZ link-layer state — updated by query_bluez_state() before each connect
        self.is_found:   bool = False  # device is in BlueZ device cache (recently seen)
        self.is_paired:  bool = False  # bonded in BlueZ
        self.is_trusted: bool = False  # trusted in BlueZ (auto-reconnect allowed)
        self.mtu_size:   Optional[int] = None  # negotiated ATT MTU after connect

        # Track initial vs reconnect
        self._initial_connect = True

    async def _ensure_paired(self, pin: str = ""):
        """Pair if needed, then untrust so BlueZ won't auto-reconnect before bleak gets GATT."""
        pin = pin or DEFAULT_BLE_PIN
        try:
            out = subprocess.run(
                ["bluetoothctl", "info", self.ble_address],
                capture_output=True, text=True, timeout=5,
            ).stdout
            paired = "Paired: yes" in out

            if not paired:
                await self._discover_device()
                logger.info(f"Pairing {self.ble_address} (PIN {pin})…")
                await self._bluetoothctl_pair(pin)
                # Drop bluetoothctl's GATT hold so device re-advertises for bleak
                subprocess.run(
                    ["bluetoothctl", "disconnect", self.ble_address],
                    capture_output=True, timeout=5,
                )
                await asyncio.sleep(1)

            # Always untrust — BlueZ must not auto-reconnect before bleak gets GATT
            subprocess.run(
                ["bluetoothctl", "untrust", self.ble_address],
                capture_output=True, timeout=5,
            )
        except FileNotFoundError:
            logger.warning("bluetoothctl not found — skipping pairing check")
        except RuntimeError:
            raise
        except Exception as e:
            logger.warning(f"Pairing check error (will attempt connect anyway): {e}")

    async def _bluetoothctl_pair(self, pin: str, _retry: bool = True):
        """
        Run `bluetoothctl pair <addr>`, answering the passkey prompt when it
        appears (it shows up only after the BLE link is established, several
        seconds after `pair` is issued — so we read stdout incrementally
        rather than piping all input up front).
        """
        proc = await asyncio.create_subprocess_exec(
            "bluetoothctl",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        async def _interact():
            proc.stdin.write(f"pair {self.ble_address}\n".encode())
            await proc.stdin.drain()

            while True:
                line = await proc.stdout.readline()
                if not line:
                    return False
                text = line.decode(errors="ignore")
                logger.debug(f"bluetoothctl: {text.strip()}")
                lower = text.lower()

                if "enter passkey" in lower or "request passkey" in lower:
                    if self.passkey_future and not self.passkey_future.done():
                        logger.info("Passkey required — waiting for POST /ble/passkey (60s)…")
                        try:
                            resolved_pin = await asyncio.wait_for(
                                asyncio.shield(self.passkey_future), timeout=60.0
                            )
                        except asyncio.TimeoutError:
                            proc.kill()
                            await proc.wait()
                            raise RuntimeError("Passkey timeout — user did not supply PIN within 60s")
                        proc.stdin.write(f"{resolved_pin}\n".encode())
                    else:
                        proc.stdin.write(f"{pin}\n".encode())
                    await proc.stdin.drain()
                elif "confirm passkey" in lower or "(yes/no)" in lower:
                    proc.stdin.write(b"yes\n")
                    await proc.stdin.drain()
                elif "pairing successful" in lower or "alreadyexists" in lower:
                    return "ok"
                elif "org.bluez.error.inprogress" in lower:
                    return "inprogress"
                elif "failed" in lower:
                    raise RuntimeError(f"Pairing failed: {text.strip()}")

        try:
            result = await asyncio.wait_for(_interact(), timeout=30.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise RuntimeError("Pairing timed out — confirm on device screen if required")
        finally:
            if proc.returncode is None:
                try:
                    proc.stdin.write(b"quit\n")
                    await proc.stdin.drain()
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    proc.kill()

        if result == "inprogress":
            if not _retry:
                raise RuntimeError("Pairing failed: org.bluez.Error.InProgress")
            logger.warning("bluetoothctl pair hit InProgress — recovering adapter and retrying")
            await self._recover_bluez_discovery()
            await self._bluetoothctl_pair(pin, _retry=False)
        elif result is False:
            raise RuntimeError("bluetoothctl exited before pairing completed")

    async def _discover_device(self, timeout: float = 10.0, _retry: bool = True):
        """
        Ensure the target address is in BlueZ's device cache — `bluetoothctl
        pair` fails with "not available" otherwise, and bleak's scanner does
        not reliably populate this cache.
        """
        devices = subprocess.run(
            ["bluetoothctl", "devices"], capture_output=True, text=True, timeout=5,
        ).stdout
        if self.ble_address.upper() in devices.upper():
            return

        logger.info(f"Device {self.ble_address} not yet known to BlueZ — scanning…")
        proc = await asyncio.create_subprocess_exec(
            "bluetoothctl",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        found = False
        inprogress = False
        try:
            proc.stdin.write(b"scan on\n")
            await proc.stdin.drain()

            loop = asyncio.get_event_loop()
            deadline = loop.time() + timeout
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    line = await asyncio.wait_for(proc.stdout.readline(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                if not line:
                    break
                text = line.decode(errors="ignore")
                if self.ble_address.upper() in text.upper():
                    found = True
                    break
                if "org.bluez.error.inprogress" in text.lower():
                    inprogress = True
                    break
        finally:
            try:
                proc.stdin.write(b"scan off\nquit\n")
                await proc.stdin.drain()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                proc.kill()

        if inprogress:
            if not _retry:
                raise RuntimeError("Discovery failed: org.bluez.Error.InProgress")
            await self._recover_bluez_discovery()
            await self._discover_device(timeout=timeout, _retry=False)
            return

        if not found:
            raise RuntimeError(
                f"Device {self.ble_address} not found during discovery — "
                "ensure it's powered on and in range"
            )

    async def _recover_bluez_discovery(self):
        """Clear a stuck BlueZ adapter (org.bluez.Error.InProgress)."""
        logger.warning("Recovering BlueZ adapter from stuck discovery state…")
        subprocess.run(["bluetoothctl", "scan", "off"], capture_output=True, timeout=5)
        await asyncio.sleep(1)

        show = subprocess.run(
            ["bluetoothctl", "show"], capture_output=True, text=True, timeout=5
        ).stdout
        if "Discovering: yes" in show:
            logger.warning("Adapter still discovering — restarting bluetooth service")
            subprocess.run(["systemctl", "restart", "bluetooth"], capture_output=True, timeout=15)
            for _ in range(10):
                await asyncio.sleep(1)
                show = subprocess.run(
                    ["bluetoothctl", "show"], capture_output=True, text=True, timeout=5
                ).stdout
                if "Discovering: no" in show:
                    break

    def query_bluez_state(self):
        """Read is_found/is_paired/is_trusted from bluetoothctl info (cached on self)."""
        try:
            out = subprocess.run(
                ["bluetoothctl", "info", self.ble_address],
                capture_output=True, text=True, timeout=5,
            ).stdout
            self.is_found   = bool(out) and "not available" not in out.lower()
            self.is_paired  = "Paired: yes" in out
            self.is_trusted = "Trusted: yes" in out
        except Exception:
            pass

    async def connect(self, pin: str = ""):
        """Connect to BLE device.

        Pre-flight disconnect + untrust prevents BlueZ from auto-reconnecting
        before bleak can acquire the GATT connection — the root cause of
        "failed to discover services" on every second connect attempt.
        No BleakScanner.discover() calls here; they trigger the same race.
        """
        logger.info(f"Connecting to BLE device: {self.ble_address}")
        try:
            # Snapshot BlueZ state before attempting — surfaced in state events
            self.query_bluez_state()

            # Pre-flight: drop any stale BlueZ GATT hold and untrust the device
            # so BlueZ won't auto-reconnect during BleakClient.connect()
            try:
                subprocess.run(["bluetoothctl", "disconnect", self.ble_address],
                               capture_output=True, timeout=5)
                subprocess.run(["bluetoothctl", "untrust", self.ble_address],
                               capture_output=True, timeout=5)
                await asyncio.sleep(1)
            except Exception:
                pass

            await self._ensure_paired(pin)
            # Refresh after pairing may have changed state
            self.query_bluez_state()

            logger.info(f"Connecting to {self.ble_address} via bleak (timeout=20s)...")
            self.client = BleakClient(
                self.ble_address, timeout=20.0,
                disconnected_callback=self._on_ble_disconnect,
            )
            try:
                await self.client.connect()
            except asyncio.TimeoutError:
                raise RuntimeError("Connection timeout — device may still be rebooting")

            if not self.client.is_connected:
                raise RuntimeError("Failed to establish BLE connection")

            # Wait for service discovery (shorter on reconnect to fail fast)
            max_wait = 20 if self._initial_connect else 10
            waited = 0.0
            while waited < max_wait:
                try:
                    services = self.client.services
                    if services and any(
                        str(s.uuid).lower() == MESHTASTIC_SERVICE_UUID.lower()
                        for s in services
                    ):
                        self.services_ready = True
                        logger.debug(f"Service discovery complete ({waited:.1f}s)")
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)
                waited += 0.5
            else:
                self.services_ready = False
                error_msg = f"Service discovery timed out after {max_wait}s"
                if not self._initial_connect:
                    raise RuntimeError(error_msg)
                logger.warning(f"⚠️  {error_msg}")

            self.reconnect_attempts = 0
            self.disconnection_event.clear()
            self.is_found = True
            try:
                self.mtu_size = self.client.mtu_size
            except Exception:
                self.mtu_size = None
            await self.stats.on_ble_connected(self.ble_address)
            logger.info(f"✅ Connected to BLE device: {self.ble_address} MTU={self.mtu_size}")

            if self._initial_connect:
                self.running = True
                self.poll_task = asyncio.create_task(self._poll_from_radio())
                self._initial_connect = False

        except Exception as e:
            logger.error(f"❌ Failed to connect to BLE device: {e}")
            raise

    def _on_ble_disconnect(self, client: BleakClient):
        """Callback when BLE device disconnects"""
        logger.warning(f"⚠️  BLE device disconnected: {self.ble_address}")
        self.services_ready = False  # Services no longer available
        self.disconnection_event.set()

        # Notify bridge
        if self.on_disconnected:
            asyncio.create_task(self.on_disconnected())

    async def _poll_from_radio(self):
        """
        Poll FromRadio characteristic for incoming packets.
        Monitors connection health and triggers reconnection on disconnect.
        """
        logger.info("FROMRADIO polling loop started")
        _poll_count = 0
        _data_count = 0

        while self.running:
            try:
                # Check if still connected
                if not self.client or not self.client.is_connected:
                    logger.warning("⚠️  BLE connection lost during polling")

                    # Wait for bridge's disconnect handler to complete reconnection
                    # Don't call attempt_reconnection() ourselves - let the callback handle it
                    logger.debug("⏸️  Waiting for reconnection to complete...")
                    max_wait = 600  # Allow time for all 10 reconnection attempts (up to 10 minutes)
                    waited = 0

                    while waited < max_wait and self.running:
                        # Check if we're back online
                        if self.client and self.client.is_connected:
                            logger.info("✅ Reconnection completed, resuming polling")
                            break

                        # Still disconnected, wait a bit more
                        await asyncio.sleep(1)
                        waited += 1

                    # Check final state
                    if not self.client or not self.client.is_connected:
                        logger.error("💀 Reconnection failed after waiting, exiting polling loop")
                        self.running = False
                        break

                    # Reconnection succeeded, continue polling
                    continue

                # Read from FromRadio characteristic
                try:
                    data = await self.client.read_gatt_char(FROMRADIO_UUID)
                    _poll_count += 1

                    if _poll_count % 100 == 0:
                        logger.info(f"FROMRADIO poll #{_poll_count}: {_data_count} packets received so far")

                    if data and len(data) > 0:
                        _data_count += 1
                        packet_hash = hash(bytes(data))
                        current_time = time.time()

                        if (packet_hash == self.last_packet_hash and
                            (current_time - self.last_packet_time) < 0.1):
                            logger.info(f"FROMRADIO: skipping duplicate {len(data)}b")
                        else:
                            self.last_packet_hash = packet_hash
                            self.last_packet_time = current_time

                            logger.info(f"FROMRADIO: got {len(data)} bytes (packet #{_data_count})")
                            await self.stats.on_packet_from_ble(len(data))

                            # Notify callback
                            if self.on_packet_received:
                                await self.on_packet_received(bytes(data))

                except Exception as read_err:
                    if "not connected" in str(read_err).lower():
                        logger.warning("FROMRADIO: disconnection detected during read")
                        self.disconnection_event.set()
                        continue
                    elif "CharacteristicNotFound" in type(read_err).__name__ or "not found" in str(read_err).lower():
                        _dfu_err_count = getattr(self, "_dfu_err_count", 0) + 1
                        self._dfu_err_count = _dfu_err_count
                        if _dfu_err_count == 1:
                            logger.warning("FROMRADIO characteristic missing — device may be in DFU bootloader")
                        if _dfu_err_count == 20:
                            # Device is stuck in DFU bootloader; send SYSTEM_RESET to reboot it.
                            # Nordic bootloader requires CCCD enabled (start_notify) before
                            # accepting any control-point writes.
                            _DFU_CTRL = "00001531-1212-efde-1523-785feabcd123"
                            _OP_RESET = 0x06
                            logger.warning("Device stuck in DFU bootloader — sending SYSTEM_RESET to recover")
                            try:
                                await self.client.start_notify(_DFU_CTRL, lambda *_: None)
                                await asyncio.sleep(1.0)  # CCCD wr_auth=1 needs time to propagate
                                await self.client.write_gatt_char(_DFU_CTRL, bytes([_OP_RESET]), response=True)
                                logger.info("DFU SYSTEM_RESET sent — device will reboot to Meshtastic")
                            except Exception as rst_err:
                                logger.warning("DFU SYSTEM_RESET failed: %s", rst_err)
                            # Force-disconnect so bridge re-connects after device reboots
                            self.disconnection_event.set()
                    else:
                        logger.warning(f"FROMRADIO read error: {read_err!r}")
                        self._dfu_err_count = 0

                await asyncio.sleep(0.1)  # 100ms polling interval

            except asyncio.CancelledError:
                logger.debug("Polling task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(1)

        logger.debug("Polling loop ended")

    async def attempt_reconnection(self) -> bool:
        """Single reconnection attempt with exponential backoff delay.

        Returns True on success, False on failure (individual or max exceeded).
        Caller checks reconnect_attempts vs MAX_RECONNECT_ATTEMPTS to distinguish
        "keep trying" from "give up."
        """
        if self.reconnect_attempts >= self.MAX_RECONNECT_ATTEMPTS:
            logger.error(
                f"Maximum reconnection attempts ({self.MAX_RECONNECT_ATTEMPTS}) exceeded."
            )
            return False

        self.reconnect_attempts += 1
        delay = min(
            self.INITIAL_RECONNECT_DELAY * (
                self.RECONNECT_BACKOFF_FACTOR ** (self.reconnect_attempts - 1)
            ),
            self.MAX_RECONNECT_DELAY,
        )
        logger.info(
            "Reconnect attempt %d/%d in %.1fs...",
            self.reconnect_attempts, self.MAX_RECONNECT_ATTEMPTS, delay,
        )
        await self.stats.on_reconnect_attempt()
        await asyncio.sleep(delay)

        if self.client:
            try:
                if self.client.is_connected:
                    await self.client.disconnect()
            except Exception as e:
                logger.debug(f"Error cleaning up old client: {e}")
            self.client = None
            await asyncio.sleep(1.0)

        try:
            await self.connect()
            await self.stats.on_reconnect_success()
            self.reconnect_attempts = 0
            return True
        except Exception as e:
            logger.warning(
                "Reconnect attempt %d/%d failed: %s",
                self.reconnect_attempts, self.MAX_RECONNECT_ATTEMPTS, e,
            )
            return False

    async def send(self, packet_bytes: bytes):
        """
        Send packet to BLE device via ToRadio characteristic.

        Args:
            packet_bytes: Raw protobuf bytes to send

        Raises:
            RuntimeError: If not connected or reconnecting
        """
        if not self.client or not self.client.is_connected:
            logger.warning("⚠️  Cannot send to BLE - not connected")
            raise RuntimeError("BLE client not connected")

        if not self.services_ready:
            logger.debug("⏸️  Skipping send - BLE services not ready")
            raise RuntimeError("BLE services not ready, please retry")

        try:
            logger.info(f"BLE send: writing {len(packet_bytes)} bytes to TORADIO")

            await self.client.write_gatt_char(TORADIO_UUID, packet_bytes)
            await self.stats.on_packet_to_ble(len(packet_bytes))

            logger.info(f"BLE send: TORADIO write OK ({len(packet_bytes)} bytes)")

        except Exception as e:
            error_msg = str(e)

            # Handle characteristic not found (services not ready yet)
            if "characteristic" in error_msg.lower() and "not found" in error_msg.lower():
                logger.warning("⚠️  BLE characteristics not ready, triggering reconnection")
                self.disconnection_event.set()
                raise RuntimeError("BLE services not ready, reconnecting")

            logger.error(f"Failed to send to BLE: {e}")

            # Check if error indicates disconnection
            if "not connected" in error_msg.lower() or "disconnected" in error_msg.lower():
                logger.warning("⚠️  Detected disconnection during send")
                self.disconnection_event.set()

            raise

    async def disconnect(self):
        """Disconnect from BLE device"""
        self.running = False

        # Cancel polling task
        if self.poll_task:
            self.poll_task.cancel()
            try:
                await self.poll_task
            except asyncio.CancelledError:
                pass

        # Disconnect client
        if self.client and self.client.is_connected:
            try:
                await self.client.disconnect()
                logger.info("✅ Disconnected from BLE device")
            except Exception as e:
                logger.warning(f"Error during BLE disconnect: {e}")

        # Flush BlueZ's internal GATT state — without this, BlueZ can retain
        # "Connected: yes" for the device even after bleak's disconnect returns,
        # preventing the radio from re-advertising on next startup.
        try:
            subprocess.run(
                ["bluetoothctl", "disconnect", self.ble_address],
                capture_output=True, timeout=5,
            )
        except Exception:
            pass

        await self.stats.on_ble_disconnected()

        # Reset state for next connection
        self._initial_connect = True
        self.reconnect_attempts = 0
        self.services_ready = False

    def get_rssi(self) -> int | None:
        """Return last known BLE RSSI (dBm). Checks bluetoothctl info first
        (available when device is advertising/not connected), then falls back
        to the RSSI captured at connect time."""
        try:
            out = subprocess.run(
                ["bluetoothctl", "info", self.ble_address],
                capture_output=True, text=True, timeout=3,
            ).stdout
            for line in out.splitlines():
                if "RSSI:" in line:
                    part = line.split("(")[-1].rstrip(")")
                    return int(part)
        except Exception:
            pass
        return self.last_scan_rssi

    async def scan_devices(self) -> list:
        """
        Scan for nearby Meshtastic BLE devices.

        Returns:
            List of discovered Meshtastic devices
        """
        logger.info("Scanning for Meshtastic devices...")

        try:
            devices = await BleakScanner.discover(timeout=10.0)

            meshtastic_devices = []
            for device in devices:
                # Check by name
                if device.name and ("meshtastic" in device.name.lower() or
                                   "ble" in device.name.lower()):
                    meshtastic_devices.append(device)
                    logger.info(f"  Found: {device.name} ({device.address})")
                # Check by UUID
                elif device.metadata.get("uuids"):
                    if MESHTASTIC_SERVICE_UUID.lower() in [
                        u.lower() for u in device.metadata.get("uuids", [])
                    ]:
                        meshtastic_devices.append(device)
                        logger.info(f"  Found: {device.name or 'Unknown'} ({device.address})")

            if not meshtastic_devices:
                logger.warning("No Meshtastic devices found")
                logger.info("All devices found:")
                for device in devices:
                    logger.info(f"  {device.name or 'Unknown'} ({device.address})")

            return meshtastic_devices

        except Exception as e:
            logger.error(f"Scan failed: {e}")
            return []

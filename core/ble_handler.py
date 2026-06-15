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
        self.is_reconnecting = False
        self.services_ready = False  # True only after service discovery completes
        self.disconnection_event = asyncio.Event()
        self.reconnect_lock = asyncio.Lock()  # Prevent concurrent reconnection

        # Callbacks
        self.on_packet_received: Optional[Callable] = None
        self.on_disconnected: Optional[Callable] = None

        # Packet deduplication
        self.last_packet_hash: Optional[int] = None
        self.last_packet_time: float = 0

        # Track initial vs reconnect
        self._initial_connect = True

    async def _ensure_paired(self, pin: str = ""):
        """Check BlueZ pairing/trust state and pair if needed."""
        pin = pin or DEFAULT_BLE_PIN
        try:
            info = subprocess.run(
                ["bluetoothctl", "info", self.ble_address],
                capture_output=True, text=True, timeout=5,
            )
            out = info.stdout
            paired  = "Paired: yes"  in out
            trusted = "Trusted: yes" in out

            if paired and trusted:
                logger.debug(f"Device {self.ble_address} already paired and trusted")
                return

            if not paired:
                await self._discover_device()
                logger.info(f"Pairing {self.ble_address} (PIN {pin})…")
                await self._bluetoothctl_pair(pin)

            # Trust so BlueZ allows reconnects without prompting
            logger.info(f"Trusting device {self.ble_address}…")
            subprocess.run(
                ["bluetoothctl", "trust", self.ble_address],
                capture_output=True, timeout=5,
            )

            if not paired:
                # bluetoothctl keeps the GATT connection from the pairing
                # process open, so the device stops advertising and bleak's
                # scan-based connect() can't find it. Drop that connection so
                # the device starts advertising again.
                logger.debug(f"Releasing bluetoothctl connection to {self.ble_address}…")
                subprocess.run(
                    ["bluetoothctl", "disconnect", self.ble_address],
                    capture_output=True, timeout=5,
                )
                await asyncio.sleep(1)
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

    async def connect(self, pin: str = ""):
        """Connect to BLE device"""
        logger.info(f"Connecting to BLE device: {self.ble_address}")

        try:
            await self._ensure_paired(pin)

            # On reconnect after drop: scan to confirm device is back before connecting
            if not self._initial_connect:
                logger.info("Reconnect: scanning for device (10s)...")
                try:
                    devices = await BleakScanner.discover(timeout=10.0, return_adv=True)
                    if not any(a.upper() == self.ble_address.upper() for a in devices):
                        raise RuntimeError("Device not found in scan — may still be rebooting")
                    logger.info("Device found in reconnect scan")
                except RuntimeError:
                    raise
                except Exception as e:
                    logger.warning(f"Reconnect scan error: {e}")

            # Connect directly by address. BlueZ knows the device from the dashboard
            # /ble/scan that the user ran just before clicking Connect.
            logger.info(f"Connecting to {self.ble_address} (timeout=20s)...")
            self.client = BleakClient(
                self.ble_address, timeout=20.0,
                disconnected_callback=self._on_ble_disconnect,
            )
            try:
                timeout = None if self._initial_connect else 15.0
                if timeout:
                    await asyncio.wait_for(self.client.connect(), timeout=timeout)
                else:
                    await self.client.connect()
            except asyncio.TimeoutError:
                raise RuntimeError("Connection timeout — device may still be rebooting")

            if not self.client.is_connected:
                raise RuntimeError("Failed to establish BLE connection")

            # Wait for service discovery
            logger.debug("Waiting for service discovery...")
            # Use shorter timeout during reconnection to fail fast
            max_wait = 10 if not self._initial_connect else 20
            wait_interval = 0.5
            waited = 0

            while waited < max_wait:
                try:
                    services = self.client.services
                    if services and any(
                        str(s.uuid).lower() == MESHTASTIC_SERVICE_UUID.lower()
                        for s in services
                    ):
                        logger.debug(f"Service discovery complete ({waited:.1f}s)")
                        self.services_ready = True
                        break
                except Exception:
                    pass

                await asyncio.sleep(wait_interval)
                waited += wait_interval
            else:
                error_msg = f"Service discovery timed out after {max_wait}s"
                logger.warning(f"⚠️  {error_msg}")
                self.services_ready = False
                # During reconnection, fail fast so next attempt can try
                if not self._initial_connect:
                    raise RuntimeError(error_msg)

            # Reset reconnection state
            self.reconnect_attempts = 0
            self.is_reconnecting = False
            self.disconnection_event.clear()

            # Update stats
            await self.stats.on_ble_connected(self.ble_address)

            logger.info(f"✅ Connected to BLE device: {self.ble_address}")

            # Start polling task ONLY on initial connect, not on reconnect
            # (reconnect happens within the existing polling loop)
            if self._initial_connect:
                self.running = True
                self.poll_task = asyncio.create_task(self._poll_from_radio())
                logger.debug(f"✅ Started polling FromRadio characteristic")
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
                    else:
                        logger.warning(f"FROMRADIO read error: {read_err!r}")

                await asyncio.sleep(0.1)  # 100ms polling interval

            except asyncio.CancelledError:
                logger.debug("Polling task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(1)

        logger.debug("Polling loop ended")

    async def attempt_reconnection(self) -> bool:
        """
        Attempt to reconnect with exponential backoff.

        Returns:
            True if reconnected successfully, False if max attempts exceeded
        """
        # Quick check before acquiring lock to prevent redundant attempts
        if self.is_reconnecting:
            logger.debug("⏸️  Reconnection already in progress, waiting for it to complete...")
            # Wait for the other reconnection to finish
            # Max 600s to allow for all 10 reconnection attempts with exponential backoff
            max_wait = 600
            waited = 0
            while self.is_reconnecting and waited < max_wait:
                await asyncio.sleep(0.5)
                waited += 0.5

            if self.is_reconnecting:
                logger.warning(f"⚠️  Reconnection still in progress after {max_wait}s")
                return False

            # Reconnection finished, check result
            result = self.client and self.client.is_connected
            logger.debug(f"✅ Waited for reconnection to complete: {'success' if result else 'failed'}")
            return result

        async with self.reconnect_lock:
            # Check again after acquiring lock
            if self.is_reconnecting:
                logger.debug("⏸️  Reconnection already in progress (after lock)")
                return self.client and self.client.is_connected

            if self.reconnect_attempts >= self.MAX_RECONNECT_ATTEMPTS:
                logger.error(
                    f"💀 Maximum reconnection attempts ({self.MAX_RECONNECT_ATTEMPTS}) "
                    f"exceeded. Giving up."
                )
                return False

            self.is_reconnecting = True
            self.reconnect_attempts += 1

            # Calculate delay with exponential backoff
            delay = min(
                self.INITIAL_RECONNECT_DELAY * (
                    self.RECONNECT_BACKOFF_FACTOR ** (self.reconnect_attempts - 1)
                ),
                self.MAX_RECONNECT_DELAY
            )

            logger.info(
                f"🔄 Reconnection attempt {self.reconnect_attempts}/"
                f"{self.MAX_RECONNECT_ATTEMPTS} in {delay:.1f}s..."
            )

            await self.stats.on_reconnect_attempt()
            await asyncio.sleep(delay)

            try:
                # Fully clean up old client before creating new one
                if self.client:
                    logger.debug("Cleaning up old BLE client...")
                    try:
                        if self.client.is_connected:
                            await self.client.disconnect()
                            logger.debug("Disconnected old client")
                    except Exception as e:
                        logger.debug(f"Error disconnecting old client: {e}")

                    # Release the old client object
                    self.client = None

                    # Give Windows time to release BLE resources
                    await asyncio.sleep(1.0)
                    logger.debug("Released old client resources")

                # Reconnect with fresh client
                await self.connect()

                logger.info("✅ Reconnection successful")
                await self.stats.on_reconnect_success()
                self.reconnect_attempts = 0
                self.is_reconnecting = False
                return True

            except Exception as e:
                logger.error(f"❌ Reconnection attempt failed: {e}")
                self.is_reconnecting = False
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

        # Don't send during reconnection - characteristics may not be ready
        if self.is_reconnecting:
            logger.debug("⏸️  Skipping send during reconnection")
            raise RuntimeError("BLE client reconnecting, please retry")

        # Don't send if services aren't ready (after connect but before service discovery)
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

        await self.stats.on_ble_disconnected()

        # Reset state for next connection
        self._initial_connect = True
        self.reconnect_attempts = 0
        self.is_reconnecting = False
        self.services_ready = False

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

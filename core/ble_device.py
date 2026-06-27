"""BLE device state machine.

Single module that owns the entire BLE lifecycle for one device:
  OFFLINE → SCANNING → CONNECTING → DISCOVERING → SYNCING → READY → RECONNECTING

Source of truth: docs/BLE-SPEC.md
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from dataclasses import dataclass
from typing import Optional

from bleak import BleakClient, BleakScanner
from bleak.exc import BleakError, BleakDeviceNotFoundError
from google.protobuf import json_format
from meshtastic.protobuf import mesh_pb2

from .config import BleConfig, BleDeviceConfig, OtaConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Protocol constants  (BLE-SPEC.md § "UUIDs and Protocol Constants")
# ---------------------------------------------------------------------------

MESHTASTIC_SERVICE_UUID = "6ba1b218-15a8-461f-9fa8-5dcae273eafd"
TORADIO_UUID            = "f75c76d2-129e-4dad-a1dd-7866124401e7"
FROMRADIO_UUID          = "2c55e69e-4993-11ed-b878-0242ac120002"  # unchanged across all firmware
FROMNUM_UUID            = "ed9da18c-a800-4f66-a670-aa7547e34453"  # firmware >= 2.3
LOGRADIO_UUID           = "5a3d6e49-06e6-4423-9944-e9de8cdf9547"

OTA_SERVICE_UUID = "4fafc201-1fb5-459e-8fcc-c5c9c331914b"
OTA_WRITE_UUID   = "62ec0272-3ec5-11eb-b378-0242ac130005"  # client → device
OTA_TX_UUID      = "62ec0272-3ec5-11eb-b378-0242ac130003"  # device → client (notify)

# Nordic Legacy DFU service (nRF52840 devices: RAK4631, T-Echo, etc.)
DFU_SERVICE_UUID        = "00001530-1212-efde-1523-785feabcd123"
DFU_CONTROL_POINT_UUID  = "00001531-1212-efde-1523-785feabcd123"

PORTNUM_ADMIN_APP = 6   # portnums_pb2.PortNum.ADMIN_APP

# Detected during DISCOVERING to identify firmware < 2.3. Never used for I/O.
# FROMRADIO UUID never changed. Firmware version is detected via FROMNUM.
_OLD_FROMNUM_UUID = "ed9da18c-a800-11e8-98d0-529269fb1459"  # firmware < 2.3

# Firmware sync nonce constants (PhoneAPI.h).  Tell firmware what to dump on want_config.
_NONCE_CONFIG_ONLY = 69420  # SPECIAL_NONCE_ONLY_CONFIG — skip nodedb, dump config/channels only
_NONCE_NODES_ONLY  = 69421  # SPECIAL_NONCE_ONLY_NODES  — skip config, dump nodedb only

# ---------------------------------------------------------------------------
# FSM — state constants
# ---------------------------------------------------------------------------

OFFLINE                  = "OFFLINE"
SCANNING              = "SCANNING"
CONNECTING            = "CONNECTING"
DISCOVERING           = "DISCOVERING"
SYNCING               = "SYNCING"
READY                 = "READY"
RECONNECTING          = "RECONNECTING"
FIRMWARE_INCOMPATIBLE = "FIRMWARE_INCOMPATIBLE"
OTA_PENDING           = "OTA_PENDING"
OTA_HANDSHAKE         = "OTA_HANDSHAKE"
OTA_FLASHING          = "OTA_FLASHING"
OTA_COMPLETE          = "OTA_COMPLETE"
OTA_BOOTLOADER_STUCK  = "OTA_BOOTLOADER_STUCK"
OTA_NVS_MISMATCH      = "OTA_NVS_MISMATCH"
OTA_SERIAL_WAIT       = "OTA_SERIAL_WAIT"
OTA_SERIAL_ERASING    = "OTA_SERIAL_ERASING"
OTA_ERROR             = "OTA_ERROR"
REGION_UNSET          = "REGION_UNSET"

# Legal transition table (BLE-SPEC.md § "Legal transitions")
_LEGAL: dict[str, frozenset[str]] = {
    OFFLINE:                  frozenset({SCANNING}),
    SCANNING:              frozenset({CONNECTING, OFFLINE}),
    CONNECTING:            frozenset({DISCOVERING, SCANNING, OFFLINE}),
    DISCOVERING:           frozenset({SYNCING, OTA_BOOTLOADER_STUCK, FIRMWARE_INCOMPATIBLE, CONNECTING, OFFLINE}),
    SYNCING:               frozenset({READY, REGION_UNSET, CONNECTING, OFFLINE}),
    READY:                 frozenset({RECONNECTING, OTA_PENDING, OFFLINE}),
    RECONNECTING:          frozenset({DISCOVERING, OFFLINE}),
    FIRMWARE_INCOMPATIBLE: frozenset({OFFLINE}),
    OTA_PENDING:           frozenset({OTA_HANDSHAKE, OTA_ERROR}),
    OTA_HANDSHAKE:         frozenset({OTA_FLASHING, OTA_NVS_MISMATCH, OTA_ERROR}),
    OTA_FLASHING:          frozenset({OTA_COMPLETE, OTA_ERROR}),
    OTA_COMPLETE:          frozenset({OFFLINE}),
    OTA_BOOTLOADER_STUCK:  frozenset({OFFLINE}),
    OTA_NVS_MISMATCH:      frozenset({OTA_SERIAL_WAIT, OFFLINE}),
    OTA_SERIAL_WAIT:       frozenset({OTA_SERIAL_ERASING, OTA_ERROR}),
    OTA_SERIAL_ERASING:    frozenset({OFFLINE, OTA_ERROR}),
    OTA_ERROR:             frozenset({OFFLINE}),
    REGION_UNSET:          frozenset({OFFLINE}),
}

# States that cannot reach OFFLINE in a single transition — must route through OTA_ERROR first.
_NEEDS_OTA_ERROR_TO_OFFLINE: frozenset[str] = frozenset({
    OTA_PENDING, OTA_HANDSHAKE, OTA_FLASHING, OTA_SERIAL_WAIT,
})

# Display hints — (label, badge_color, badge_text, show_spinner, show_progress, action_required)
# BLE-SPEC.md § "State → display hints (implementation table)"
_DISPLAY: dict[str, tuple] = {
    OFFLINE:                  ("Offline",               "muted",   "offline",      False, False, False),
    SCANNING:              ("Connecting…",        "info",    "connecting",   True,  False, False),
    CONNECTING:            ("Connecting…",       "info",    "connecting",   True,  False, False),
    DISCOVERING:           ("Discovering…",      "info",    "discovering",  True,  False, False),
    SYNCING:               ("Syncing…",          "info",    "syncing",      True,  False, False),
    READY:                 ("Connected",              "success", "ready",        False, False, False),
    RECONNECTING:          ("Reconnecting…",     "warning", "reconnecting", True,  False, False),
    FIRMWARE_INCOMPATIBLE: ("Firmware too old",       "error",   "incompatible", False, False, False),
    OTA_PENDING:           ("OTA — rebooting…",   "info", "ota",       True,  False, False),
    OTA_HANDSHAKE:         ("OTA — connecting…",  "info", "ota",       True,  False, False),
    OTA_FLASHING:          ("Flashing firmware",      "info",    "ota",          False, True,  False),
    OTA_COMPLETE:          ("Flash complete",          "success", "done",         False, False, False),
    OTA_BOOTLOADER_STUCK:  ("Bootloader — rebooting", "warning", "bootloader", True, False, False),
    OTA_NVS_MISMATCH:      ("OTA — recovering…",  "warning", "ota",   True,  False, False),
    OTA_SERIAL_WAIT:       ("Action required",         "warning", "action",       False, False, True),
    OTA_SERIAL_ERASING:    ("Erasing NVS…",      "warning", "erasing",      False, True,  False),
    OTA_ERROR:             ("OTA failed",              "error",   "error",        False, False, False),
    REGION_UNSET:          ("LoRa region not set",     "warning", "config",       False, False, True),
}


class InvalidTransition(Exception):
    pass


# Module-level lock: only one BleDevice may hold the BLE scanner at a time.
# BlueZ/bleak raises org.bluez.Error.InProgress if two scans start simultaneously.
_scan_lock: asyncio.Lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# DeviceData — mutable record populated during SYNCING, updated on telemetry
# ---------------------------------------------------------------------------

@dataclass
class DeviceData:
    node_id: Optional[str] = None
    my_node_num: Optional[int] = None
    hw_model: Optional[str] = None
    short_name: Optional[str] = None
    long_name: Optional[str] = None
    firmware_version: Optional[str] = None
    battery_level: Optional[int] = None
    voltage: Optional[float] = None
    uptime_s: Optional[int] = None
    channel_utilization: Optional[float] = None
    air_util_tx: Optional[float] = None
    node_count: int = 0
    tcp_port: Optional[int] = None
    sync_duration_s: Optional[float] = None
    mtu: Optional[int] = None
    sync_mode: Optional[str] = None       # "full" | "config_only" | "nodes_only"
    conn_priority: Optional[str] = None   # "high" | "balanced" | "disabled" (current level)
    session_passkey: Optional[str] = None          # hex; required for admin SET / OTA commands
    session_passkey_refreshed_at: Optional[float] = None  # monotonic timestamp of last refresh

    _PASSKEY_TTL_S: int = 300                       # device expires passkey after 300s

    def session_passkey_ttl_s(self) -> Optional[int]:
        """Remaining TTL in seconds, or None if passkey not yet received."""
        if self.session_passkey is None or self.session_passkey_refreshed_at is None:
            return None
        remaining = self._PASSKEY_TTL_S - (time.monotonic() - self.session_passkey_refreshed_at)
        return max(0, int(remaining))

    def as_event(self, addr: str) -> dict:
        return {
            "type": "device_data",
            "addr": addr,
            "node_id": self.node_id,
            "my_node_num": self.my_node_num,
            "hw_model": self.hw_model,
            "short_name": self.short_name,
            "long_name": self.long_name,
            "firmware_version": self.firmware_version,
            "battery_level": self.battery_level,
            "voltage": self.voltage,
            "uptime_s": self.uptime_s,
            "channel_utilization": self.channel_utilization,
            "air_util_tx": self.air_util_tx,
            "node_count": self.node_count,
            "tcp_port": self.tcp_port,
            "sync_duration_s": self.sync_duration_s,
            "mtu": self.mtu,
            "sync_mode": self.sync_mode,
            "conn_priority": self.conn_priority,
            "session_passkey": self.session_passkey,
            "session_passkey_ttl_s": self.session_passkey_ttl_s(),
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _safe_disconnect(client: BleakClient) -> None:
    with contextlib.suppress(Exception):
        await client.disconnect()


def _proto_to_dict(msg) -> dict:
    return json_format.MessageToDict(msg, preserving_proto_field_name=True)


# ---------------------------------------------------------------------------
# BleDevice
# ---------------------------------------------------------------------------

class BleDevice:
    """Owns the full BLE lifecycle for one device. Thread-safe via asyncio.

    All state changes go through _transition() — the only method that may
    write self._state. stop() is the single exception: it force-sets OFFLINE
    as a hard shutdown from any state.
    """

    def __init__(
        self,
        addr: str,
        pin: str,
        cfg: BleDeviceConfig,
        ble_cfg: BleConfig,
        ota_cfg: OtaConfig,
        queue: asyncio.Queue,
    ) -> None:
        self._addr = addr.upper()
        self._pin = pin
        self._cfg = cfg
        self._ble_cfg = ble_cfg
        self._ota_cfg = ota_cfg
        self._queue = queue

        self._state: str = OFFLINE
        self._data: DeviceData = DeviceData(tcp_port=cfg.tcp_port)
        self._last_state_event: dict = {}

        # BLE resources — set during _connection_loop
        self._client: Optional[BleakClient] = None
        self._conn_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None

        # Sync state — reset per connection cycle
        self._own_node_num: Optional[int] = None
        self._want_config_id: Optional[int] = None
        self._sync_complete: asyncio.Event = asyncio.Event()
        self._fromnum_event: asyncio.Event = asyncio.Event()
        self._lora_region_unset: bool = False

        # OTA (step 7)
        self._ota: Optional[object] = None
        self._ota_event: asyncio.Event = asyncio.Event()
        self._pending_ota: Optional[str] = None

        # Connection priority downgrade timer
        self._priority_downgrade_task: Optional[asyncio.Task] = None

        # Admin session passkey refresh task
        self._passkey_task: Optional[asyncio.Task] = None

        # Sync-time node buffer — populated during SYNCING, used for READY seed emit.
        # Live node cache lives in AppRouter after the BLE/AppRouter split.
        self._sync_nodes: dict[str, dict] = {}
        self._channels: list[dict] = []
        self._config: dict = {}
        self._module_config: dict = {}
        self._my_info: dict = {}
        self._metadata: dict = {}
        self._messages: list[dict] = []

        # Optional TCP gateway (started in start() if tcp_port set)
        self._tcp_gateway = None

        # Captured event loop — set in start(), used by _on_fromnum() callback
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # FSM core
    # ------------------------------------------------------------------

    def _transition(
        self,
        new_state: str,
        *,
        message: str = "",
        pct: Optional[int] = None,
        deadline: Optional[int] = None,
        action_text: str = "",
    ) -> None:
        """Validate, set state, build device_state event, enqueue.

        Raises InvalidTransition for any move not in the legal set.
        Never cancels tasks — that is the caller's responsibility.
        """
        if new_state not in _LEGAL.get(self._state, frozenset()):
            raise InvalidTransition(
                f"{self._addr}: {self._state} → {new_state} is not a legal transition"
            )
        self._state = new_state
        label, badge_color, badge_text, show_spinner, show_progress, action_required = _DISPLAY[new_state]

        event: dict = {
            "type": "device_state",
            "addr": self._addr,
            "node_id": self._data.node_id,
            "state": new_state,
            "label": label,
            "message": message,
            "pct": pct,
            "deadline": deadline,
            "display": {
                "badge_color": badge_color,
                "badge_text": badge_text,
                "show_spinner": show_spinner,
                "show_progress": show_progress,
                "action_required": action_required,
                "action_text": action_text if action_required else "",
            },
        }
        self._last_state_event = event
        self._enqueue(event)
        logger.debug("%s → %s  msg=%r", self._addr, new_state, message)

    def _enqueue(self, event: dict) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning(
                "%s: event queue full — dropping %s(%s)",
                self._addr, event.get("type"), event.get("state", ""),
            )

    # ------------------------------------------------------------------
    # Public commands
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Begin the connection loop. Idempotent — safe to call twice."""
        if self._conn_task and not self._conn_task.done():
            return
        self._loop = asyncio.get_running_loop()

        if self._cfg.tcp_port and self._tcp_gateway is None:
            from .tcp_gateway import TcpGateway
            self._tcp_gateway = TcpGateway(
                port=self._cfg.tcp_port,
                on_to_radio=self.send_toradio,
            )
            await self._tcp_gateway.start()

        self._conn_task = asyncio.get_running_loop().create_task(
            self._connection_loop(), name=f"conn:{self._addr}"
        )

    async def stop(self) -> None:
        """Hard shutdown from any state. Cancels all tasks, disconnects BLE."""
        if self._priority_downgrade_task and not self._priority_downgrade_task.done():
            self._priority_downgrade_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._priority_downgrade_task
            self._priority_downgrade_task = None

        if self._passkey_task and not self._passkey_task.done():
            self._passkey_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._passkey_task
            self._passkey_task = None

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._poll_task
            self._poll_task = None

        if self._conn_task and not self._conn_task.done():
            self._conn_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._conn_task
            self._conn_task = None

        if self._tcp_gateway is not None:
            with contextlib.suppress(Exception):
                await self._tcp_gateway.stop()
            self._tcp_gateway = None

        if self._client is not None:
            await _safe_disconnect(self._client)
            self._client = None

        if self._state != OFFLINE:
            self._state = OFFLINE
            self._data = DeviceData(tcp_port=self._cfg.tcp_port)
            self._own_node_num = None
            label, badge_color, badge_text, show_spinner, show_progress, action_required = _DISPLAY[OFFLINE]
            event: dict = {
                "type": "device_state",
                "addr": self._addr,
                "node_id": None,
                "state": OFFLINE,
                "label": label,
                "message": "",
                "pct": None,
                "deadline": None,
                "display": {
                    "badge_color": badge_color,
                    "badge_text": badge_text,
                    "show_spinner": show_spinner,
                    "show_progress": show_progress,
                    "action_required": action_required,
                    "action_text": "",
                },
            }
            self._last_state_event = event
            self._enqueue(event)

    async def trigger_ota(self, fw_path: str) -> None:
        if self._state != READY:
            raise RuntimeError(f"trigger_ota called in state {self._state}")
        self._pending_ota = fw_path
        self._ota_event.set()

    async def send_toradio(self, data: bytes) -> None:
        """Write to TORADIO. Silently dropped if not READY. (spec § send_toradio Safety)"""
        if self._state != READY or self._client is None:
            logger.warning(
                "%s: send_toradio: not ready (state=%s), dropping", self._addr, self._state
            )
            return
        await self._client.write_gatt_char(TORADIO_UUID, data, response=True)

    async def send_admin(self, message: dict, to: Optional[int] = None, want_response: bool = False) -> None:
        """Send an arbitrary AdminMessage to the device.

        message: dict mapping AdminMessage field name to value (e.g. {"nodedb_reset": True}).
        Injects session_passkey automatically if one is available.
        """
        from meshtastic.protobuf import admin_pb2
        if self._own_node_num is None:
            raise RuntimeError(f"{self._addr}: own_node_num not yet known — device not fully synced")
        if self._state != READY:
            raise RuntimeError(f"{self._addr}: send_admin called in state {self._state}")

        admin = json_format.ParseDict(message, admin_pb2.AdminMessage())
        if self._data.session_passkey:
            admin.session_passkey = bytes.fromhex(self._data.session_passkey)

        inner = mesh_pb2.MeshPacket()
        inner.to = to if to is not None else self._own_node_num
        inner.decoded.portnum = PORTNUM_ADMIN_APP
        inner.decoded.payload = admin.SerializeToString()
        inner.decoded.want_response = want_response

        toradio = mesh_pb2.ToRadio()
        toradio.packet.CopyFrom(inner)
        await self.send_toradio(toradio.SerializeToString())
        logger.debug("%s: send_admin sent: %s", self._addr, list(message.keys()))

    async def purge_nodedb(self) -> dict:
        """Reset the device node database and wait for the reboot cycle to complete.

        Sends nodedb_reset AdminMessage (with session passkey). The firmware calls
        disableBluetooth() then reboots after 7s — the gateway sees RECONNECTING
        (not OFFLINE) as the BLE link drops. Polls for not-READY (≤15 s) then for
        READY again (≤45 s). Returns {"node_count": N} from the post-reboot sync.
        Raises RuntimeError or TimeoutError on failure.
        """
        if self._state != READY:
            raise RuntimeError(f"{self._addr}: purge_nodedb called in state {self._state}")

        logger.info("%s: purge_nodedb — sending nodedb_reset", self._addr)
        await self.send_admin({"nodedb_reset": True}, want_response=False)

        # Firmware calls disableBluetooth() → BLE drops → gateway transitions to
        # RECONNECTING. Wait for device to leave READY state.
        deadline = asyncio.get_event_loop().time() + 15
        while self._state == READY:
            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(f"{self._addr}: device did not leave READY within 15s after nodedb_reset")
            await asyncio.sleep(0.5)
        logger.info("%s: purge_nodedb — device left READY (state=%s), waiting for recovery", self._addr, self._state)

        # Wait for reconnect and READY.
        deadline = asyncio.get_event_loop().time() + 45
        while self._state != READY:
            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(f"{self._addr}: device did not reach READY within 45s after nodedb_reset")
            await asyncio.sleep(1)

        # node_count from sync data reflects device's view after reboot (own node only).
        node_count = self._data.node_count
        logger.info("%s: purge_nodedb — complete, node_count=%d", self._addr, node_count)
        return {"node_count": node_count}

    async def update_config(self, cfg: BleDeviceConfig) -> None:
        """Hot-reload per-device config. Preserves the BLE connection."""
        self._cfg = cfg
        self._pin = cfg.pin
        self._data.tcp_port = cfg.tcp_port

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def addr(self) -> str:
        return self._addr

    @property
    def state(self) -> str:
        return self._state

    @property
    def node_id(self) -> Optional[str]:
        return self._data.node_id

    @property
    def data(self) -> DeviceData:
        return self._data

    @property
    def nodes(self) -> dict:
        return self._sync_nodes

    @property
    def channels(self) -> list:
        return self._channels

    @property
    def config(self) -> dict:
        return self._config

    @property
    def module_config(self) -> dict:
        return self._module_config

    @property
    def my_info(self) -> dict:
        return self._my_info

    @property
    def metadata(self) -> dict:
        return self._metadata

    @property
    def messages(self) -> list:
        return self._messages

    @property
    def snapshot(self) -> dict:
        """Full device_state + device_data as one dict for WS connect burst."""
        return {
            "addr": self._addr,
            "state_event": self._last_state_event,
            "data_event": self._data.as_event(self._addr),
        }

    # ------------------------------------------------------------------
    # Connection loop
    # ------------------------------------------------------------------

    def _reset_session_data(self) -> None:
        """Clear per-session DeviceData fields before each new connection attempt.

        Ensures a failed or OTA-bootloader session never leaves stale MTU /
        priority / passkey values visible in the /devices snapshot.
        """
        self._data.mtu = None
        self._data.conn_priority = None
        self._data.sync_mode = None
        self._data.sync_duration_s = None
        self._data.session_passkey = None
        self._data.session_passkey_refreshed_at = None

    async def _run_one_session(self, client: BleakClient) -> str:
        """Run one connected session and clean up unconditionally.

        Wraps _connected_session() with the poll-task teardown and client
        disconnect so the caller never has to repeat those in every code path.
        Returns the same string as _connected_session():
          "idle" | "reconnecting" | "firmware_incompatible" | "scan_immediately"
        """
        self._client = client
        try:
            return await self._connected_session(client)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s: unexpected error in connected session (state=%s)",
                             self._addr, self._state)
            # Route through OTA_ERROR for states that cannot reach OFFLINE in one step.
            if self._state in _NEEDS_OTA_ERROR_TO_OFFLINE:
                self._transition(OTA_ERROR, message="unexpected exception")
            if self._state != OFFLINE:
                self._transition(OFFLINE)
            return "idle"
        finally:
            if self._poll_task and not self._poll_task.done():
                self._poll_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._poll_task
            self._poll_task = None
            await _safe_disconnect(client)
            self._client = None

    async def _do_reconnect(self) -> Optional[BleakClient]:
        """RECONNECTING phase: attempt to reconnect with exponential backoff.

        Returns a connected BleakClient on success, or None when
        reconnect_max_retries are exhausted (transitions to OFFLINE on exhaustion).
        Caller must ensure _run_one_session() (or equivalent) is used so the
        returned client is always disconnected in a finally block.
        """
        delay = 1.0
        for attempt in range(1, self._ble_cfg.reconnect_max_retries + 1):
            await asyncio.sleep(delay)
            delay = min(delay * 2.0, self._ble_cfg.reconnect_backoff_max_s)
            logger.info(
                "%s: reconnect attempt %d/%d (next backoff %.0fs)",
                self._addr, attempt, self._ble_cfg.reconnect_max_retries, delay,
            )
            client = BleakClient(self._addr, timeout=self._ble_cfg.reconnect_timeout_s)
            try:
                await client.connect()
                logger.info("%s: reconnected on attempt %d", self._addr, attempt)
                return client
            except (BleakError, BleakDeviceNotFoundError, asyncio.TimeoutError, OSError) as e:
                await _safe_disconnect(client)
                logger.warning("%s: reconnect %d/%d failed: %s",
                               self._addr, attempt, self._ble_cfg.reconnect_max_retries, e)

        logger.warning("%s: all %d reconnect attempts exhausted → OFFLINE",
                       self._addr, self._ble_cfg.reconnect_max_retries)
        self._transition(OFFLINE)
        return None

    async def _get_conn_handle(self) -> Optional[str]:
        """Return the HCI connection handle for self._addr (e.g. '0x0010') via hcitool con."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "hcitool", "con",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await proc.communicate()
            addr_lower = self._addr.lower()
            for line in stdout.decode().splitlines():
                if addr_lower in line.lower():
                    parts = line.split()
                    for i, p in enumerate(parts):
                        if p == "handle" and i + 1 < len(parts):
                            return hex(int(parts[i + 1]))
        except Exception as e:
            logger.warning("%s: _get_conn_handle failed: %s", self._addr, e)
        return None

    async def _request_conn_priority(self, level: str) -> None:
        """Issue HCI LE Connection Update — equivalent to Android requestConnectionPriority.

        level='high'     → 7.5–11.25ms interval (Android CONNECTION_PRIORITY_HIGH)
        level='balanced' → 30–50ms interval (Android CONNECTION_PRIORITY_BALANCED)
        """
        if not self._ble_cfg.conn_priority_enabled:
            return
        handle = await self._get_conn_handle()
        if handle is None:
            logger.warning("%s: conn handle not found; skipping priority update", self._addr)
            return
        if level == "high":
            min_i, max_i = self._ble_cfg.conn_interval_high_min, self._ble_cfg.conn_interval_high_max
        else:
            min_i, max_i = self._ble_cfg.conn_interval_balanced_min, self._ble_cfg.conn_interval_balanced_max
        proc = await asyncio.create_subprocess_exec(
            "hcitool", "lecup",
            f"--handle={handle}",
            f"--min={min_i}",
            f"--max={max_i}",
            f"--latency={self._ble_cfg.conn_interval_latency}",
            f"--timeout={self._ble_cfg.conn_interval_timeout}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning("%s: hcitool lecup failed: %s", self._addr, stderr.decode().strip())
        else:
            logger.info("%s: BLE conn priority → %s (interval %d–%d × 1.25ms)",
                        self._addr, level, min_i, max_i)
            self._data.conn_priority = level

    async def _schedule_priority_downgrade(self) -> None:
        """Background task: downgrade to BALANCED after conn_priority_downgrade_s seconds."""
        await asyncio.sleep(self._ble_cfg.conn_priority_downgrade_s)
        await self._request_conn_priority("balanced")

    async def _send_admin_get_owner(self, client: BleakClient) -> None:
        """Send get_owner_request to self to trigger an AdminMessage response with session_passkey."""
        from meshtastic.protobuf import admin_pb2
        if self._own_node_num is None:
            logger.debug("%s: _send_admin_get_owner skipped (own_node_num not set)", self._addr)
            return
        logger.debug("%s: sending get_owner_request to %08x", self._addr, self._own_node_num)
        admin = admin_pb2.AdminMessage()
        admin.get_owner_request = True

        inner = mesh_pb2.MeshPacket()
        inner.to = self._own_node_num
        inner.decoded.portnum = PORTNUM_ADMIN_APP
        inner.decoded.payload = admin.SerializeToString()
        inner.decoded.want_response = True

        toradio = mesh_pb2.ToRadio()
        toradio.packet.CopyFrom(inner)
        await client.write_gatt_char(TORADIO_UUID, toradio.SerializeToString(), response=True)
        logger.debug("%s: get_owner_request sent", self._addr)

    async def _passkey_refresh_loop(self, client: BleakClient) -> None:
        """Background task: fetch session_passkey immediately on READY, then refresh every
        admin_passkey_refresh_s seconds (key expires after 300s on device)."""
        while self._state == READY:
            await self._send_admin_get_owner(client)
            await asyncio.sleep(self._ble_cfg.admin_passkey_refresh_s)

    async def _connection_loop(self) -> None:
        """Outer connection lifecycle. Runs until CancelledError (stop() called)."""
        try:
            await self._connection_loop_inner()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s: _connection_loop crashed in state %s", self._addr, self._state)
            if self._state in _NEEDS_OTA_ERROR_TO_OFFLINE:
                self._transition(OTA_ERROR, message="crash recovery")
            if self._state != OFFLINE:
                self._transition(OFFLINE)

    async def _connection_loop_inner(self) -> None:
        self._transition(SCANNING)
        connect_attempts = 0

        while True:
            # ── SCANNING: discover device before connecting ────────────
            try:
                found = await BleakScanner.find_device_by_address(
                    self._addr, timeout=12.0,
                )
            except Exception as e:
                logger.warning("%s: discovery failed: %s", self._addr, e)
                found = None
            if found is None:
                logger.warning("%s: device not found — not advertising", self._addr)
                self._transition(OFFLINE)
                if not self._cfg.auto_connect:
                    return
                await asyncio.sleep(self._ble_cfg.scan_pause_s)
                self._transition(SCANNING)
                connect_attempts = 0
                continue

            # ── CONNECTING ────────────────────────────────────────────
            self._reset_session_data()
            self._transition(CONNECTING)
            client = BleakClient(self._addr, timeout=self._ble_cfg.connect_timeout_s)
            try:
                await client.connect()
            except (BleakError, BleakDeviceNotFoundError, asyncio.TimeoutError, OSError) as e:
                await _safe_disconnect(client)
                connect_attempts += 1
                logger.warning(
                    "%s: connect failed (%d/%d): %s",
                    self._addr, connect_attempts, self._ble_cfg.connect_max_retries, e,
                )
                if connect_attempts >= self._ble_cfg.connect_max_retries:
                    self._transition(OFFLINE)
                    if not self._cfg.auto_connect:
                        return
                    await asyncio.sleep(self._ble_cfg.scan_pause_s)
                    self._transition(SCANNING)
                    connect_attempts = 0
                else:
                    await asyncio.sleep(self._ble_cfg.connect_retry_delay_s)
                    self._transition(SCANNING)
                continue

            connect_attempts = 0

            session_done = await self._run_one_session(client)

            # RECONNECTING: retry connection with exponential backoff before
            # falling back to the full SCANNING cycle.
            while session_done == "reconnecting":
                new_client = await self._do_reconnect()
                if new_client is None:
                    session_done = "idle"
                    break
                self._reset_session_data()
                session_done = await self._run_one_session(new_client)

            if session_done == "firmware_incompatible":
                return  # stuck; must stop() + start() after firmware update

            # Every session exit path must transition to OFFLINE before returning.
            # If this assertion fires, there is a bug in _connected_session or
            # _run_ota_flow — fix it there, not here.
            if self._state != OFFLINE:
                raise InvalidTransition(
                    f"{self._addr}: session returned {session_done!r} but state is {self._state!r}"
                )

            if not self._cfg.auto_connect:
                return

            if session_done == "scan_immediately":
                # OTA or bootloader event — device is rebooting or just completed.
                # Skip scan_pause_s and reconnect immediately.
                self._transition(SCANNING)
                connect_attempts = 0
                continue
            elif session_done == "idle":
                pass  # wait scan_pause_s before next scan
            else:
                raise ValueError(
                    f"{self._addr}: unhandled session_done value: {session_done!r}"
                )

            await asyncio.sleep(self._ble_cfg.scan_pause_s)
            self._transition(SCANNING)

    async def _scan_once(self, timeout_s: float = 30.0) -> bool:
        """Scan for self._addr. Returns True if found within timeout_s.

        NOT used in the normal connection loop or OTA flow — scanning is user-triggered
        only (POST /scan from UI). Retained for future user-initiated device discovery.
        Uses the module-level _scan_lock so only one BleakScanner runs at a time.
        """
        found = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _cb(device, _adv):
            if device.address.upper() == self._addr:
                loop.call_soon_threadsafe(found.set)

        async with _scan_lock:
            scanner = BleakScanner(detection_callback=_cb)
            await scanner.start()
            try:
                await asyncio.wait_for(found.wait(), timeout=timeout_s)
                return True
            except asyncio.TimeoutError:
                return False
            finally:
                with contextlib.suppress(Exception):
                    await scanner.stop()

    def _emit_progress(self, pct: int) -> None:
        """Emit an OTA_FLASHING progress event without a state transition.
        Only valid while self._state == OTA_FLASHING.
        """
        if self._state != OTA_FLASHING:
            return
        label, badge_color, badge_text, show_spinner, show_progress, action_required = _DISPLAY[OTA_FLASHING]
        event: dict = {
            "type": "device_state",
            "addr": self._addr,
            "node_id": self._data.node_id,
            "state": OTA_FLASHING,
            "label": label,
            "message": "",
            "pct": pct,
            "deadline": None,
            "display": {
                "badge_color": badge_color,
                "badge_text": badge_text,
                "show_spinner": show_spinner,
                "show_progress": show_progress,
                "action_required": action_required,
                "action_text": "",
            },
        }
        self._last_state_event = event
        self._enqueue(event)

    async def _send_nordic_dfu_trigger(self, client: BleakClient) -> None:
        """Write ENTER_BOOTLOADER to DFU_CONTROL_POINT on the existing Meshtastic
        connection (nRF52 exposes this in app-mode). Device disconnects and reboots
        into Nordic Legacy DFU bootloader.
        """
        payload = bytes([0x01, 0x04])  # OP_CODE_ENTER_BOOTLOADER, UPLOAD_MODE_APPLICATION
        try:
            await client.write_gatt_char(DFU_CONTROL_POINT_UUID, payload, response=True)
        except Exception:
            # Device often disconnects mid-write as it's rebooting — that's fine
            pass
        logger.info("%s: Nordic DFU trigger sent", self._addr)

    async def _send_ota_request(self, client: BleakClient, fw_hash: bytes) -> None:
        """Send AdminMessage{ota_request} to trigger device reboot into bleota.
        Requires the active Meshtastic BleakClient (TORADIO write).
        """
        from meshtastic.protobuf import admin_pb2
        admin = admin_pb2.AdminMessage()
        admin.ota_request.reboot_ota_mode = 1
        admin.ota_request.ota_hash = fw_hash
        if self._data.session_passkey:
            admin.session_passkey = bytes.fromhex(self._data.session_passkey)

        inner = mesh_pb2.MeshPacket()
        inner.to = 0xFFFFFFFF  # broadcast; device accepts ota_request from any sender
        inner.decoded.portnum = PORTNUM_ADMIN_APP
        inner.decoded.payload = admin.SerializeToString()

        toradio = mesh_pb2.ToRadio()
        toradio.packet.CopyFrom(inner)
        await client.write_gatt_char(TORADIO_UUID, toradio.SerializeToString(), response=True)
        logger.info("%s: ota_request sent (hash=%s)", self._addr, fw_hash.hex()[:16] + "…")

    async def _wait_ota_disconnect(self, client: BleakClient, timeout_s: float) -> None:
        """Poll client.is_connected until False (device rebooted) or timeout."""
        deadline = time.monotonic() + timeout_s
        while client.is_connected:
            if time.monotonic() >= deadline:
                raise asyncio.TimeoutError("OTA pending timeout — device did not disconnect")
            await asyncio.sleep(0.2)
        logger.info("%s: OTA disconnect detected", self._addr)

    async def _run_ota_flow(self, client: BleakClient, fw_path: str) -> str:
        """Full OTA flow from OTA_PENDING through OTA_COMPLETE (or error).

        BleDevice owns all BLE management (scan, connect, disconnect, transitions).
        Dispatches to OtaSession (.bin — ESP32 bleota) or NordicDfuSession (.zip — nRF52840).
        Returns a session_done string for _connection_loop_inner().
        """
        is_nordic = fw_path.lower().endswith(".zip")
        if is_nordic:
            from .nordic_dfu_session import NordicDfuSession
        else:
            from .ota_session import OtaSession
            import hashlib

        # ── validate firmware file ─────────────────────────────────────
        if is_nordic:
            try:
                session = NordicDfuSession(zip_path=fw_path, ota_cfg=self._ota_cfg)
                session.parse_zip()
            except Exception as e:
                logger.error("%s: cannot parse firmware zip %s: %s", self._addr, fw_path, e)
                self._transition(OTA_ERROR, message=f"firmware file error: {e}")
                self._transition(OFFLINE)
                return "idle"
            logger.info("%s: Nordic DFU firmware %s — bin=%d dat=%d bytes mode=0x%02x",
                        self._addr, fw_path, len(session.bin_data), len(session.dat_data),
                        session.upload_mode)
        else:
            try:
                fw_bytes = open(fw_path, "rb").read()
            except OSError as e:
                logger.error("%s: cannot read firmware file %s: %s", self._addr, fw_path, e)
                self._transition(OTA_ERROR, message=f"firmware file error: {e}")
                self._transition(OFFLINE)
                return "idle"
            fw_hash = hashlib.sha256(fw_bytes).digest()
            logger.info("%s: ESP32 OTA firmware %s — %d bytes, sha256=%s",
                        self._addr, fw_path, len(fw_bytes), fw_hash.hex()[:16] + "…")
            session = OtaSession(fw_bytes=fw_bytes, fw_hash=fw_hash, ota_cfg=self._ota_cfg)

        # ── OTA_PENDING: trigger reboot into bootloader ────────────────
        self._transition(OTA_PENDING)
        try:
            if is_nordic:
                await self._send_nordic_dfu_trigger(client)
            else:
                await self._send_ota_request(client, fw_hash)
            await self._wait_ota_disconnect(client, self._ota_cfg.pending_timeout_s)
        except asyncio.TimeoutError:
            logger.warning("%s: OTA pending timeout", self._addr)
            self._transition(OTA_ERROR, message="device did not reboot")
            self._transition(OFFLINE)
            return "idle"
        except Exception as e:
            logger.warning("%s: OTA pending error: %s", self._addr, e)
            self._transition(OTA_ERROR, message=str(e))
            self._transition(OFFLINE)
            return "idle"

        # ── OTA_HANDSHAKE: direct connect to bootloader ────────────────
        # Device address is unchanged in bootloader mode. BlueZ cache hit is
        # guaranteed — device was connected moments ago. No scan needed.
        self._transition(OTA_HANDSHAKE)
        logger.debug("%s: waiting %.1fs for bootloader to initialize",
                     self._addr, self._ota_cfg.bootloader_init_wait_s)
        await asyncio.sleep(self._ota_cfg.bootloader_init_wait_s)
        ota_client = BleakClient(self._addr, timeout=self._ota_cfg.bootloader_connect_timeout_s)
        try:
            await ota_client.connect()
        except Exception as e:
            logger.warning("%s: bootloader connect failed: %s", self._addr, e)
            await _safe_disconnect(ota_client)
            self._transition(OTA_ERROR, message=f"bootloader connect: {e}")
            self._transition(OFFLINE)
            return "idle"

        try:
            await ota_client._backend._acquire_mtu()
            logger.debug("%s: DFU MTU = %d", self._addr, ota_client.mtu_size)
        except Exception as e:
            logger.warning("%s: DFU _acquire_mtu() failed (%s), using default", self._addr, e)

        # ── OTA_FLASHING: wire protocol ───────────────────────────────
        try:
            result = await session.run(
                ota_client,
                on_progress=self._emit_progress,
                on_transition=self._transition,
            )
        except Exception as e:
            logger.exception("%s: OTA session error: %s", self._addr, e)
            result = "error"
        finally:
            await _safe_disconnect(ota_client)

        if result == "ok":
            self._transition(OTA_COMPLETE)
            self._transition(OFFLINE)
            return "scan_immediately"  # OTA_COMPLETE: device is alive and rebooting to Meshtastic

        if result == "nvs_mismatch":
            # nvs_mismatch only occurs on ESP32 bleota (Nordic DFU has no equivalent)
            self._transition(OTA_NVS_MISMATCH)
            logger.info("%s: NVS mismatch — attempting direct connect fast-path", self._addr)
            # Direct connect: device address unchanged. Fast-path almost always fails because
            # Meshtastic checkForOtaRequest() runs before BLE advertising starts and the
            # device cycles back to bleota before we can connect.
            fp_client = BleakClient(self._addr, timeout=self._ble_cfg.connect_timeout_s)
            try:
                await fp_client.connect()
                await self._send_ota_request(fp_client, fw_hash)  # noqa: F821 — only reachable for ESP32
                await _safe_disconnect(fp_client)
                logger.info("%s: NVS fast-path: ota_request re-sent", self._addr)
                self._transition(OFFLINE)
                return "scan_immediately"
            except Exception as e:
                logger.warning("%s: NVS fast-path failed (expected): %s", self._addr, e)
                await _safe_disconnect(fp_client)

            # Serial NVS erase — user must connect serial adapter
            _deadline_ms = int((time.monotonic() + self._ota_cfg.nvs_serial_wait_s) * 1000)
            self._transition(OTA_SERIAL_WAIT,
                message="Hold PRG and press RST on the device to enter ROM bootloader",
                action_text="Hold PRG and press RST on the device",
                deadline=_deadline_ms)
            try:
                await asyncio.wait_for(
                    self._serial_erase_nvs(),
                    timeout=self._ota_cfg.nvs_serial_wait_s,
                )
                self._transition(OFFLINE)
                return "idle"
            except asyncio.TimeoutError:
                logger.warning("%s: serial NVS erase timed out", self._addr)
            except Exception as e:
                logger.warning("%s: serial NVS erase error: %s", self._addr, e)
            self._transition(OTA_ERROR, message="NVS erase failed")
            self._transition(OFFLINE)
            return "idle"

        # Generic error
        self._transition(OTA_ERROR, message=result if isinstance(result, str) else "unknown")
        self._transition(OFFLINE)
        return "idle"

    async def _serial_erase_nvs(self) -> None:
        """Poll for serial device, then run esptool to erase the NVS partition.
        Raises asyncio.TimeoutError (handled by caller) if no serial device appears.
        OTA_SERIAL_WAIT → OTA_SERIAL_ERASING transition managed here.
        """
        import glob
        esptool = self._ota_cfg.resolved_esptool_bin()
        port: Optional[str] = None

        # Poll for serial device (user must connect USB-serial adapter)
        deadline = time.monotonic() + self._ota_cfg.nvs_serial_wait_s
        while time.monotonic() < deadline:
            ports = sorted(glob.glob("/dev/ttyUSB*") + glob.glob("/dev/ttyACM*"))
            if ports:
                port = ports[0]
                logger.info("%s: serial device found: %s", self._addr, port)
                break
            remaining_s = int(deadline - time.monotonic())
            self._transition(OTA_SERIAL_WAIT,
                message=f"Waiting for serial device — {remaining_s}s remaining",
                action_text="Hold PRG and press RST on the device",
                deadline=_deadline_ms)
            await asyncio.sleep(self._ota_cfg.nvs_serial_poll_s)

        if port is None:
            raise asyncio.TimeoutError("no serial device appeared")

        # Probe with chip-id to confirm device is in ROM bootloader before erasing
        probe_cmd = [
            esptool, "--chip", "esp32c3",
            "--port", port, "--baud", str(self._ota_cfg.nvs_serial_baud),
            "chip_id",
        ]
        logger.info("%s: probing serial device: %s", self._addr, " ".join(probe_cmd))
        probe = await asyncio.create_subprocess_exec(
            *probe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            probe_out, _ = await asyncio.wait_for(
                probe.communicate(),
                timeout=self._ota_cfg.nvs_serial_poll_s * 4,
            )
        except asyncio.TimeoutError:
            probe.kill()
            raise RuntimeError("chip-id probe timed out — device not in ROM bootloader")
        if probe.returncode != 0:
            raise RuntimeError(
                f"chip-id probe failed (rc={probe.returncode}) — device not in ROM bootloader: "
                f"{probe_out.decode()[-200:]}"
            )
        logger.info("%s: chip-id confirmed ROM bootloader", self._addr)

        self._transition(OTA_SERIAL_ERASING)
        cmd = [
            esptool, "--chip", "esp32c3",
            "--port", port, "--baud", str(self._ota_cfg.nvs_serial_baud),
            "--before", "no-reset", "--after", "hard-reset",
            "erase_region", self._ota_cfg.nvs_offset, self._ota_cfg.nvs_size,
        ]
        logger.info("%s: erasing NVS: %s", self._addr, " ".join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(),
                timeout=self._ota_cfg.nvs_serial_erase_timeout_s,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"esptool exited {proc.returncode}: {stdout.decode()[-200:]}")
            logger.info("%s: NVS erase complete", self._addr)
        except asyncio.TimeoutError:
            proc.kill()
            raise RuntimeError("esptool timeout")

    async def _connected_session(self, client: BleakClient) -> str:
        """Drives DISCOVERING → SYNCING → READY. Returns "idle" or "firmware_incompatible"."""

        # ── DISCOVERING ───────────────────────────────────────────────
        self._transition(DISCOVERING)
        discover_attempts = 0

        while True:
            # bleak >= 0.20: services populated during connect(), accessed via client.services
            try:
                services = client.services
                if services is None:
                    raise BleakError("services not populated after connect")
            except (BleakError, BleakDeviceNotFoundError, OSError) as e:
                discover_attempts += 1
                logger.warning("%s: discovery failed (%d/%d): %s",
                               self._addr, discover_attempts, self._ble_cfg.discover_max_retries, e)
                if discover_attempts >= self._ble_cfg.discover_max_retries:
                    self._transition(OFFLINE)
                    return "idle"
                await asyncio.sleep(self._ble_cfg.connect_retry_delay_s)
                self._transition(CONNECTING)
                try:
                    await client.connect()
                    self._transition(DISCOVERING)
                except Exception:
                    self._transition(OFFLINE)
                    return "idle"
                continue

            svc_uuids = {s.uuid.lower() for s in services}
            char_uuids = {c.uuid.lower() for s in services for c in s.characteristics}

            if OTA_SERVICE_UUID.lower() in svc_uuids:
                # Do not send REBOOT\n — without a completed OTA it reboots back into bleota.
                logger.warning("%s: ESP32 bleota detected with no active OTA session", self._addr)
                self._transition(OTA_BOOTLOADER_STUCK, message="ESP32 stuck in bleota — OTA required")
                self._transition(OFFLINE)
                return "idle"

            if (DFU_SERVICE_UUID.lower() in svc_uuids
                    and MESHTASTIC_SERVICE_UUID.lower() not in svc_uuids):
                # Do not send 0x06 RESET — GPREGRET is preserved through soft reset.
                # Power cycle clears GPREGRET and returns to Meshtastic.
                logger.warning("%s: Nordic DFU detected with no active OTA session", self._addr)
                self._transition(OTA_BOOTLOADER_STUCK, message="Nordic DFU stuck — power cycle or OTA required")
                self._transition(OFFLINE)
                return "idle"

            if MESHTASTIC_SERVICE_UUID.lower() not in svc_uuids:
                discover_attempts += 1
                if discover_attempts >= self._ble_cfg.discover_max_retries:
                    self._transition(OFFLINE)
                    return "idle"
                await asyncio.sleep(self._ble_cfg.connect_retry_delay_s)
                self._transition(CONNECTING)
                try:
                    await client.connect()
                    self._transition(DISCOVERING)
                except Exception:
                    self._transition(OFFLINE)
                    return "idle"
                continue

            # Meshtastic SVC found — check FROMNUM to confirm firmware >= 2.3.
            # FROMRADIO UUID is unchanged across firmware versions; only FROMNUM differs.
            logger.debug("%s: discovered char UUIDs: %s", self._addr, sorted(char_uuids))

            if FROMNUM_UUID.lower() in char_uuids:
                break  # firmware >= 2.3 confirmed — proceed

            if _OLD_FROMNUM_UUID.lower() in char_uuids:
                msg = (
                    f"Firmware < 2.3 detected on {self._addr} — old FROMNUM UUID present, "
                    f"new one absent. Update firmware to >= 2.3."
                )
                logger.warning(msg)
                self._transition(FIRMWARE_INCOMPATIBLE, message=msg)
                return "firmware_incompatible"

            # Meshtastic SVC present but FROMNUM absent — transient gap, retry
            discover_attempts += 1
            if discover_attempts >= self._ble_cfg.discover_max_retries:
                self._transition(OFFLINE)
                return "idle"
            await asyncio.sleep(self._ble_cfg.connect_retry_delay_s)
            self._transition(CONNECTING)
            try:
                await client.connect()
                self._transition(DISCOVERING)
            except Exception:
                self._transition(OFFLINE)
                return "idle"
            continue

        # Now confirmed Meshtastic — read MTU and request HIGH connection priority.
        # These are only meaningful for Meshtastic sessions; OTA bootloader sessions
        # never reach this point and therefore never pollute DeviceData.
        try:
            await client._backend._acquire_mtu()
            self._data.mtu = client.mtu_size
            logger.debug("%s: MTU = %d", self._addr, self._data.mtu)
        except Exception as e:
            logger.warning("%s: _acquire_mtu() failed (%s), MTU unknown", self._addr, e)

        if self._ble_cfg.conn_priority_enabled:
            await self._request_conn_priority("high")
            await asyncio.sleep(self._ble_cfg.conn_priority_update_wait_s)
        else:
            self._data.conn_priority = "disabled"

        # ── SYNCING ───────────────────────────────────────────────────
        self._transition(SYNCING)
        self._fromnum_event.clear()
        self._sync_complete.clear()
        sync_attempts = 0
        _sync_start = time.monotonic()

        # Select nonce based on sync_mode (spec § "want_config")
        mode = self._ble_cfg.sync_mode
        self._data.sync_mode = mode
        if mode == "config_only":
            self._want_config_id = _NONCE_CONFIG_ONLY
        elif mode == "nodes_only":
            self._want_config_id = _NONCE_NODES_ONLY
        else:
            self._want_config_id = random.randint(1, 69419)  # full dump; avoid special values

        while True:
            self._sync_complete.clear()
            try:
                await asyncio.wait_for(
                    self._run_sync(client),
                    timeout=self._ble_cfg.sync_timeout_s,
                )
                break  # sync succeeded
            except asyncio.TimeoutError:
                sync_attempts += 1
                logger.warning("%s: sync timeout (%d/%d)",
                               self._addr, sync_attempts, self._ble_cfg.sync_max_retries)
                if sync_attempts >= self._ble_cfg.sync_max_retries:
                    self._transition(OFFLINE)
                    return "idle"
                # Reconnect and retry SYNCING
                self._transition(CONNECTING)
                await _safe_disconnect(client)
                try:
                    await client.connect()
                    self._transition(SYNCING)
                except Exception:
                    self._transition(OFFLINE)
                    return "idle"
            except (BleakError, BleakDeviceNotFoundError) as e:
                sync_attempts += 1
                logger.warning("%s: sync BLE error (%d/%d): %s",
                               self._addr, sync_attempts, self._ble_cfg.sync_max_retries, e)
                if sync_attempts >= self._ble_cfg.sync_max_retries:
                    self._transition(OFFLINE)
                    return "idle"
                self._transition(CONNECTING)
                await _safe_disconnect(client)
                try:
                    await client.connect()
                    self._transition(SYNCING)
                except Exception:
                    self._transition(OFFLINE)
                    return "idle"

        # ── REGION_UNSET check ─────────────────────────────────────────
        if self._lora_region_unset:
            self._lora_region_unset = False
            auto_region = self._cfg.lora_region.strip().upper()
            self._data.sync_duration_s = round(time.monotonic() - _sync_start, 1)
            if auto_region:
                logger.info("%s: LoRa region unset — auto-configuring %s", self._addr, auto_region)
                try:
                    from meshtastic.protobuf import admin_pb2, config_pb2
                    region_num = config_pb2.Config.LoRaConfig.RegionCode.Value(auto_region)
                    admin = admin_pb2.AdminMessage()
                    admin.set_config.lora.region = region_num
                    inner = mesh_pb2.MeshPacket()
                    inner.to = self._own_node_num
                    inner.decoded.portnum = 68  # PORTNUM_ADMIN_APP
                    inner.decoded.payload = admin.SerializeToString()
                    tr = mesh_pb2.ToRadio()
                    tr.packet.CopyFrom(inner)
                    await asyncio.wait_for(
                        client.write_gatt_char(TORADIO_UUID, tr.SerializeToString(), response=True),
                        timeout=5.0,
                    )
                    logger.info("%s: LoRa region %s written — device will reboot", self._addr, auto_region)
                except Exception as e:
                    logger.warning("%s: failed to write LoRa region: %s", self._addr, e)
            else:
                logger.warning(
                    "%s: LoRa region unset — add lora_region: EU_868 (or your region) "
                    "to ble_devices entry in bridge_config.yaml",
                    self._addr,
                )
            self._transition(REGION_UNSET)
            await _safe_disconnect(client)
            self._transition(OFFLINE)
            return "scan_immediately"

        # Subscribe to FROMNUM now that config sync is complete
        try:
            await client.start_notify(FROMNUM_UUID, self._on_fromnum)
        except (BleakError, BleakDeviceNotFoundError) as e:
            logger.warning("%s: start_notify failed: %s", self._addr, e)
            self._transition(OFFLINE)
            return "idle"

        # ── READY ─────────────────────────────────────────────────────
        self._data.sync_duration_s = round(time.monotonic() - _sync_start, 1)
        logger.info("%s: sync complete — %d nodes in %.1fs (MTU %s, mode=%s)",
                    self._addr, self._data.node_count, self._data.sync_duration_s,
                    self._data.mtu or "?", self._ble_cfg.sync_mode)
        self._transition(READY)
        self._enqueue(self._data.as_event(self._addr))
        # Seed own-device node into node-list so nodeSelf populates in the browser
        own_key = str(self._own_node_num)
        if own_key in self._sync_nodes:
            self._enqueue({
                "type": "node_update",
                "device": self._addr,
                "addr": self._addr,
                "data": dict(self._sync_nodes[own_key]),
            })

        # Schedule priority downgrade to BALANCED after conn_priority_downgrade_s (matches Android)
        loop = asyncio.get_running_loop()
        self._priority_downgrade_task = loop.create_task(
            self._schedule_priority_downgrade(), name=f"prio:{self._addr}"
        )

        # Immediately fetch session passkey (needed for admin SET / OTA), refresh every 240s
        self._passkey_task = loop.create_task(
            self._passkey_refresh_loop(client), name=f"passkey:{self._addr}"
        )

        self._poll_task = loop.create_task(
            self._notify_loop(client), name=f"notify:{self._addr}"
        )

        # Wait for poll loop end (disconnect) or OTA trigger
        ota_waiter = loop.create_task(
            self._ota_event.wait(), name=f"ota_wait:{self._addr}"
        )
        try:
            done, _ = await asyncio.wait(
                {self._poll_task, ota_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            ota_waiter.cancel()
            raise
        finally:
            # Cancel priority downgrade and passkey refresh whenever we leave READY
            if self._priority_downgrade_task and not self._priority_downgrade_task.done():
                self._priority_downgrade_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._priority_downgrade_task
            self._priority_downgrade_task = None

            if self._passkey_task and not self._passkey_task.done():
                self._passkey_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._passkey_task
            self._passkey_task = None

        if ota_waiter in done:
            self._ota_event.clear()
            if not self._poll_task.done():
                self._poll_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._poll_task
            self._poll_task = None
            ota_waiter.cancel()

            fw_path = self._pending_ota
            self._pending_ota = None
            return await self._run_ota_flow(client, fw_path)
        else:
            # Poll loop ended — device disconnected
            ota_waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await ota_waiter

            try:
                self._poll_task.result()
            except (asyncio.CancelledError, Exception) as e:
                logger.warning("%s: disconnected: %s", self._addr, e)
            self._poll_task = None

            self._transition(RECONNECTING)
            return "reconnecting"

    # ------------------------------------------------------------------
    # Sync loop — drives FROMRADIO reads during SYNCING
    # ------------------------------------------------------------------

    async def _run_sync(self, client: BleakClient) -> None:
        """Send want_config and read FROMRADIO until config_complete received.

        Subscribe to FROMNUM before sending want_config — NimBLE devices (ESP32-C3)
        only push FROMRADIO data once they detect an active FROMNUM subscriber.
        The subscription is torn down in the finally block so the post-sync code
        can re-subscribe for the notify loop.
        """
        # iOS subscribes FROMRADIO + FROMNUM + LOGRADIO during characteristic discovery
        # before sending want_config. NimBLE devices (ESP32-C3) require all three CCCDs
        # written before arming the TORADIO write handler — without FROMRADIO subscribed,
        # the ATT Write Request to TORADIO never receives a Write Response.
        self._fromnum_event.clear()
        logger.debug("%s: sync — subscribing FROMRADIO + FROMNUM + LOGRADIO", self._addr)
        for _uuid, _desc in (
            (FROMRADIO_UUID, "FROMRADIO"),
            (FROMNUM_UUID,   "FROMNUM"),
            (LOGRADIO_UUID,  "LOGRADIO"),
        ):
            try:
                if _uuid == FROMNUM_UUID:
                    await client.start_notify(_uuid, self._on_fromnum)
                else:
                    # FROMRADIO notify data handled via the drain read loop below;
                    # LOGRADIO is subscribed to match iOS connection sequence.
                    _cb = self._on_fromnum if _uuid == FROMNUM_UUID else (lambda s, d: None)
                    await client.start_notify(_uuid, _cb)
                logger.debug("%s: sync — subscribed %s", self._addr, _desc)
            except Exception as e:
                logger.debug("%s: sync — %s notify not supported: %s", self._addr, _desc, e)
        try:
            pkt = mesh_pb2.ToRadio()
            pkt.want_config_id = self._want_config_id
            logger.debug("%s: sync — sending want_config_id=%d", self._addr, self._want_config_id)
            await client.write_gatt_char(TORADIO_UUID, pkt.SerializeToString(), response=True)
            logger.debug("%s: sync — want_config GATT write ACKed", self._addr)
            # Trigger initial drain immediately (handles fast-responding devices like RAK4631)
            self._fromnum_event.set()
            _read_count = 0
            while not self._sync_complete.is_set():
                await self._fromnum_event.wait()
                self._fromnum_event.clear()
                logger.debug("%s: sync — FROMNUM fired, draining FROMRADIO", self._addr)
                while True:
                    raw = await client.read_gatt_char(FROMRADIO_UUID)
                    if not raw:
                        logger.debug("%s: sync — FROMRADIO empty after %d packets", self._addr, _read_count)
                        break
                    _read_count += 1
                    logger.debug("%s: sync — FROMRADIO pkt #%d len=%d bytes=%s",
                                 self._addr, _read_count, len(raw), raw[:8].hex())
                    await self._decode_fromradio(bytes(raw))
                    if self._sync_complete.is_set():
                        return
        finally:
            logger.debug("%s: sync — stop_notify all", self._addr)
            for _uuid in (FROMRADIO_UUID, FROMNUM_UUID, LOGRADIO_UUID):
                with contextlib.suppress(Exception):
                    await client.stop_notify(_uuid)

    # ------------------------------------------------------------------
    # Notify loop — FROMNUM notification-driven FROMRADIO reads while READY
    # ------------------------------------------------------------------

    def _on_fromnum(self, _sender, _data) -> None:
        """FROMNUM notify callback — called by bleak when device has a new packet."""
        self._loop.call_soon_threadsafe(self._fromnum_event.set)

    async def _notify_loop(self, client: BleakClient) -> None:
        """Runs while READY. Exits via BleakError on disconnect, or CancelledError."""
        while self._state == READY:
            try:
                await asyncio.wait_for(self._fromnum_event.wait(), timeout=self._ble_cfg.notify_idle_timeout_s)
            except asyncio.TimeoutError:
                # Quiet mesh — check connection health
                if not client.is_connected:
                    raise BleakError(f"{self._addr}: connection lost")
                continue

            self._fromnum_event.clear()
            while True:
                raw = await client.read_gatt_char(FROMRADIO_UUID)
                if not raw:
                    break
                await self._decode_fromradio(bytes(raw))

    # ------------------------------------------------------------------
    # Protobuf decode
    # ------------------------------------------------------------------

    async def _decode_fromradio(self, raw: bytes) -> None:
        """Decode one FromRadio message; update state and emit events."""
        fr = mesh_pb2.FromRadio()
        try:
            fr.ParseFromString(raw)
        except Exception as e:
            logger.warning("%s: failed to parse FromRadio: %s", self._addr, e)
            return

        # TCP gateway gets raw bytes directly (before JSON encoding)
        if self._tcp_gateway is not None:
            self._tcp_gateway.broadcast(raw)

        which = fr.WhichOneof("payload_variant")
        logger.debug("%s: FROMRADIO which=%s", self._addr, which)

        if which == "my_info":
            num = fr.my_info.my_node_num
            self._own_node_num = num
            self._data.node_id = f"!{num:08x}"
            self._data.my_node_num = num
            self._my_info = _proto_to_dict(fr.my_info)
            logger.debug("%s: my_info node_num=!%08x", self._addr, num)

        elif which == "node_info":
            ni = fr.node_info
            node_dict = _proto_to_dict(ni)
            key = str(ni.num)
            self._sync_nodes[key] = node_dict
            self._data.node_count = len(self._sync_nodes)
            logger.debug("%s: node_info num=!%08x short=%s",
                         self._addr, ni.num, ni.user.short_name if ni.HasField("user") else "?")

            # Populate DeviceData from own node
            if ni.num == self._own_node_num and ni.HasField("user"):
                u = ni.user
                if u.short_name:
                    self._data.short_name = u.short_name
                if u.long_name:
                    self._data.long_name = u.long_name
                if u.hw_model:
                    from meshtastic.protobuf import mesh_pb2 as _m
                    hw_name = _m.HardwareModel.Name(u.hw_model)
                    self._data.hw_model = hw_name if hw_name else str(u.hw_model)

            # Update telemetry from device_metrics in node_info
            if ni.num == self._own_node_num and ni.HasField("device_metrics"):
                dm = ni.device_metrics
                if dm.battery_level:
                    self._data.battery_level = dm.battery_level
                if dm.voltage:
                    self._data.voltage = round(dm.voltage, 2)
                if dm.channel_utilization:
                    self._data.channel_utilization = round(dm.channel_utilization, 2)
                if dm.air_util_tx:
                    self._data.air_util_tx = round(dm.air_util_tx, 2)
                if dm.uptime_seconds:
                    self._data.uptime_s = dm.uptime_seconds

        elif which == "config":
            section = fr.config.WhichOneof("payload_variant")
            if section:
                self._config[section] = _proto_to_dict(getattr(fr.config, section))
                if section == "lora":
                    region = self._config["lora"].get("region", 0)
                    logger.debug("%s: config lora region=%s(%s)",
                                 self._addr, region, self._config["lora"])
                    if region == 0:
                        self._lora_region_unset = True
                        logger.debug("%s: config lora region UNSET — will go REGION_UNSET after sync", self._addr)
                else:
                    logger.debug("%s: config section=%s", self._addr, section)

        elif which == "moduleConfig":
            section = fr.moduleConfig.WhichOneof("payload_variant")
            if section:
                self._module_config[section] = _proto_to_dict(getattr(fr.moduleConfig, section))
            logger.debug("%s: moduleConfig section=%s", self._addr, section)

        elif which == "channel":
            ch = _proto_to_dict(fr.channel)
            idx = fr.channel.index
            while len(self._channels) <= idx:
                self._channels.append({})
            self._channels[idx] = ch
            logger.debug("%s: channel idx=%d", self._addr, idx)

        elif which == "metadata":
            self._metadata = _proto_to_dict(fr.metadata)
            if fr.metadata.firmware_version:
                self._data.firmware_version = fr.metadata.firmware_version
            if not self._data.hw_model and fr.metadata.hw_model:
                from meshtastic.protobuf import mesh_pb2 as _m
                hw_name = _m.HardwareModel.Name(fr.metadata.hw_model)
                self._data.hw_model = hw_name if hw_name else str(fr.metadata.hw_model)
            logger.debug("%s: metadata fw=%s hw=%s", self._addr,
                         fr.metadata.firmware_version, fr.metadata.hw_model)

        elif which == "config_complete_id":
            logger.debug("%s: config_complete_id=%d want=%d match=%s",
                         self._addr, fr.config_complete_id, self._want_config_id or 0,
                         fr.config_complete_id == self._want_config_id)
            if fr.config_complete_id == self._want_config_id:
                self._sync_complete.set()

        elif which == "packet":
            logger.debug("%s: packet portnum=%d payload_len=%d",
                         self._addr, fr.packet.decoded.portnum,
                         len(fr.packet.decoded.payload))
            # Only emit as events after we're in READY (not during SYNCING).
            # Payload stays as raw bytes — AppRouter handles all decoding and routing.
            if self._state == READY:
                pkt_dict = _proto_to_dict(fr.packet)
                self._enqueue({
                    "type": "packet",
                    "addr": self._addr,
                    "device": self._addr,
                    "node_id": self._data.node_id,
                    "data": {"packet": pkt_dict},
                })

        elif which == "rebooted":
            logger.info("%s: device rebooted", self._addr)

        elif which is None:
            logger.debug("%s: FROMRADIO — empty/unknown payload (raw len=%d)", self._addr, len(raw))

        else:
            logger.debug("%s: FROMRADIO — unhandled which=%s", self._addr, which)

    def set_session_passkey(self, passkey: bytes) -> None:
        """Called by AppRouter when it decodes an ADMIN_APP response containing a session_passkey."""
        self._data.session_passkey = passkey.hex()
        self._data.session_passkey_refreshed_at = time.monotonic()
        logger.debug("%s: session_passkey refreshed by AppRouter", self._addr)

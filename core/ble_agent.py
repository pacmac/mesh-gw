"""BlueZ pairing agent — org.bluez.Agent1 via dbus_fast.

Handles PasskeyEntry and JustWorks pairing for BLE devices.
PIN is sourced from node-dash GET /device-config/{mac} → ble_pin field.

dbus_fast is a mandatory bleak dependency — no new packages needed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import urllib.request
from typing import Callable, Awaitable, Optional

from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType
from dbus_fast.errors import DBusError
from dbus_fast.service import ServiceInterface, method

logger = logging.getLogger(__name__)

AGENT_PATH = "/org/mt_radar/pairing_agent"
# KeyboardDisplay: can both display and enter passkeys → handles PasskeyEntry + NumericComparison
AGENT_CAPABILITY = "KeyboardDisplay"


def _device_path_to_mac(device_path: str) -> str:
    """Extract MAC from BlueZ device path e.g. /org/bluez/hci0/dev_F4_12_FA_39_F7_B6."""
    return device_path.split("/dev_")[-1].replace("_", ":")


class _PairingAgent(ServiceInterface):
    def __init__(self, pin_provider: Callable[[str], Awaitable[Optional[str]]]) -> None:
        super().__init__("org.bluez.Agent1")
        self._pin_provider = pin_provider
        self._pending_pair_addr: Optional[str] = None

    # dbus_fast infers output signature from return annotation string.
    # Void methods must NOT annotate return type (it would be parsed as NoneType → error).

    @method()
    def Release(self):
        logger.debug("agent: Release")

    @method()
    def Cancel(self):
        logger.debug("agent: Cancel for %s", self._pending_pair_addr)
        self._pending_pair_addr = None

    @method()
    async def RequestPasskey(self, device: "o") -> "u":
        """Called by BlueZ for PasskeyEntry (device shows passkey, we enter it)."""
        mac = _device_path_to_mac(device)
        self._pending_pair_addr = mac
        logger.info("agent: RequestPasskey for %s", mac)
        pin = await self._pin_provider(mac)
        if pin is None:
            logger.warning("agent: no PIN stored for %s — rejecting", mac)
            raise DBusError("org.bluez.Error.Rejected", f"No PIN configured for {mac}")
        try:
            passkey = int(pin)
        except ValueError:
            raise DBusError("org.bluez.Error.Rejected", f"PIN is not numeric: {pin!r}")
        logger.info("agent: supplying passkey for %s", mac)
        return passkey

    @method()
    async def DisplayPasskey(self, device: "o", passkey: "u", entered: "q"):
        """BlueZ shows us a passkey the device is displaying (NumericComparison / DisplayYesNo)."""
        mac = _device_path_to_mac(device)
        logger.info("agent: DisplayPasskey %s  passkey=%06d  entered=%d", mac, passkey, entered)

    @method()
    async def RequestConfirmation(self, device: "o", passkey: "u"):
        """BlueZ asks us to confirm the passkey matches what the device shows (NumericComparison)."""
        mac = _device_path_to_mac(device)
        logger.info("agent: RequestConfirmation %s  passkey=%06d — auto-confirming", mac, passkey)

    @method()
    async def RequestAuthorization(self, device: "o"):
        logger.info("agent: RequestAuthorization %s — auto-accepting", _device_path_to_mac(device))

    @method()
    async def AuthorizeService(self, device: "o", uuid: "s"):
        logger.info("agent: AuthorizeService %s uuid=%s — auto-accepting",
                    _device_path_to_mac(device), uuid)

    @method()
    async def DisplayPinCode(self, device: "o", pincode: "s"):
        logger.info("agent: DisplayPinCode %s  pin=%s", _device_path_to_mac(device), pincode)


def _fetch_pin_from_node_dash(mac: str, node_dash_url: str) -> Optional[str]:
    """Synchronous PIN fetch — run in asyncio.to_thread to stay non-blocking."""
    try:
        url = f"{node_dash_url.rstrip('/')}/device-config/{mac}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            return data.get("ble_pin") or None
    except Exception as e:
        logger.debug("agent: PIN fetch failed for %s: %s", mac, e)
        return None


async def setup_pairing_agent(node_dash_url: str) -> MessageBus:
    """Register the BlueZ pairing agent. Returns the bus (keep alive for the process lifetime).

    Call once on startup. The returned bus must not be closed while BLE devices are active.
    """
    async def pin_provider(mac: str) -> Optional[str]:
        return await asyncio.to_thread(_fetch_pin_from_node_dash, mac, node_dash_url)

    agent = _PairingAgent(pin_provider)

    bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
    bus.export(AGENT_PATH, agent)

    introspection = await bus.introspect("org.bluez", "/org/bluez")
    proxy = bus.get_proxy_object("org.bluez", "/org/bluez", introspection)
    agent_mgr = proxy.get_interface("org.bluez.AgentManager1")

    await agent_mgr.call_register_agent(AGENT_PATH, AGENT_CAPABILITY)
    await agent_mgr.call_request_default_agent(AGENT_PATH)

    logger.info("BlueZ pairing agent registered at %s (capability=%s)", AGENT_PATH, AGENT_CAPABILITY)
    return bus

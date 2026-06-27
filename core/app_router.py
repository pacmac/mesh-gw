"""Universal Meshtastic packet router.

Reads raw `packet` events from the DeviceManager event stream, decodes every
payload using the meshtastic protocols registry, and emits typed events to
out_queue.

Hard boundary: this module never touches BLE. ble_device.py never decodes
packet content. See docs/APP_ROUTER.md.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import struct
import time
from typing import TYPE_CHECKING

from google.protobuf.json_format import MessageToDict
from meshtastic import protocols

if TYPE_CHECKING:
    from module.device_manager import DeviceManager

logger = logging.getLogger(__name__)

PORTNUM_ADMIN  = 6
PORTNUM_TILT   = 256   # PRIVATE_APP — 5×little-endian float32 (roll, pitch, x, y, z)


class AppRouter:
    """Decodes every packet from every BLE device and emits typed WS events.

    Input:  dm.subscribe() — copy of all DeviceManager events
    Output: out_queue      — typed events consumed by the server broadcast loop
    """

    def __init__(self, device_manager: DeviceManager, out_queue: asyncio.Queue) -> None:
        self._dm = device_manager
        self._out = out_queue
        self._input: asyncio.Queue | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._input = self._dm.subscribe()
        self._task = asyncio.get_running_loop().create_task(
            self._run(), name="app-router"
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            import contextlib
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._input is not None:
            self._dm.unsubscribe(self._input)
            self._input = None

    async def _run(self) -> None:
        while True:
            event = await self._input.get()
            if event.get("type") != "packet":
                continue
            try:
                await self._route(event)
            except Exception as e:
                logger.warning("app_router: unhandled error routing packet: %s", e)

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    async def _route(self, event: dict) -> None:
        addr     = event.get("addr", "")
        node_id  = event.get("node_id")
        pkt      = event.get("data", {}).get("packet", {})
        decoded  = pkt.get("decoded", {})
        portnum  = decoded.get("portnum")
        payload_b64 = decoded.get("payload", "")

        if portnum is None:
            return

        # Decode raw payload bytes
        try:
            raw_bytes = base64.b64decode(payload_b64) if payload_b64 else b""
        except Exception:
            raw_bytes = b""

        # Shared packet-level fields for typed events
        from_num  = pkt.get("from")
        to_num    = pkt.get("to")
        rx_rssi   = pkt.get("rx_rssi")
        rx_snr    = pkt.get("rx_snr")
        hop_start = pkt.get("hop_start", 0)
        hop_limit = pkt.get("hop_limit", 0)
        hops      = max(0, hop_start - hop_limit) if hop_start else 0
        via_mqtt  = bool(pkt.get("via_mqtt", False))

        def _typed(event_type: str, data: dict) -> dict:
            return {
                "type":     event_type,
                "addr":     addr,
                "node_id":  node_id,
                "from_num": from_num,
                "to_num":   to_num,
                "rx_rssi":  rx_rssi,
                "rx_snr":   rx_snr,
                "hops":     hops,
                "via_mqtt": via_mqtt,
                "data":     data,
            }

        # Attempt registry decode
        handler = protocols.get(portnum)
        sub_decoded: dict | None = None

        if handler and handler.protobufFactory:
            try:
                pb = handler.protobufFactory()
                pb.ParseFromString(raw_bytes)
                sub_decoded = MessageToDict(pb, preserving_proto_field_name=True)
            except Exception as e:
                logger.debug("app_router: decode failed portnum=%d: %s", portnum, e)

        elif handler:
            # Handler registered but no protobuf factory — text payload
            try:
                sub_decoded = {"text": raw_bytes.decode("utf-8", errors="replace")}
            except Exception:
                pass

        # Re-broadcast packet event with decoded sub-message merged in
        if sub_decoded is not None:
            merged_decoded = dict(decoded)
            handler_name = handler.name if handler else "private_app"
            merged_decoded[handler_name] = sub_decoded
            merged_pkt = dict(pkt)
            merged_pkt["decoded"] = merged_decoded
            self._emit({
                "type":    "packet",
                "addr":    addr,
                "device":  addr,
                "node_id": node_id,
                "data":    {"packet": merged_pkt},
            })
        else:
            # Pass through original packet unchanged
            self._emit(event)

        # Typed app event
        if sub_decoded is not None and handler:
            self._emit(_typed(handler.name, sub_decoded))

            # ADMIN_APP — extract session_passkey and feed back to BleDevice
            if portnum == PORTNUM_ADMIN:
                self._handle_admin(addr, sub_decoded)

        elif not handler:
            # Unregistered portnum — emit private_app with raw payload
            self._emit({
                "type":        "private_app",
                "addr":        addr,
                "node_id":     node_id,
                "from_num":    from_num,
                "portnum":     portnum,
                "payload_b64": payload_b64,
            })

            # Tilt special-case: portnum 256, 5×little-endian float32
            if portnum == PORTNUM_TILT and len(raw_bytes) >= 20:
                roll, pitch, x, y, z = struct.unpack_from("<5f", raw_bytes)
                logger.debug(
                    "app_router: tilt addr=%s roll=%.1f pitch=%.1f x=%.3f y=%.3f z=%.3f",
                    addr, roll, pitch, x, y, z,
                )
                self._emit(_typed("tilt", {
                    "roll":  round(roll, 2),
                    "pitch": round(pitch, 2),
                    "x":     round(x, 3),
                    "y":     round(y, 3),
                    "z":     round(z, 3),
                }))

    def _handle_admin(self, addr: str, decoded: dict) -> None:
        passkey_b64 = decoded.get("session_passkey")
        if not passkey_b64:
            return
        dev = self._dm.get_by_ble(addr)
        if dev is None:
            return
        try:
            passkey_bytes = base64.b64decode(passkey_b64)
            dev.set_session_passkey(passkey_bytes)
        except Exception as e:
            logger.warning("app_router: session_passkey decode error: %s", e)

    def _emit(self, event: dict) -> None:
        try:
            self._out.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("app_router: out_queue full — dropping %s", event.get("type"))

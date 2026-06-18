"""Meshtastic TCP gateway.

Bridges a single BLE-connected MeshBridge to the standard Meshtastic TCP
protocol (same as serial/USB, same as what Android app / meshtastic --host
uses). Port 4403 is the Meshtastic convention; each device gets its own port.

Wire protocol (Meshtastic StreamAPI framing):
  Each packet (both directions):
    [0x94][0xc3][size_high][size_low][protobuf bytes...]
  On connect, client sends wakeup bytes (repeated 0xc3) before first packet.

Usage:
    gw = TcpGateway(port=4403, on_to_radio=bridge._tcp_to_radio)
    await gw.start()
    gw.broadcast(raw_fromradio_bytes)   # called by bridge._on_packet()
    await gw.stop()
"""
import asyncio
import logging
import struct

logger = logging.getLogger(__name__)

MAGIC = b'\x94\xc3'
WAKEUP = 0xc3
MAX_PAYLOAD = 512 * 1024


def _frame(raw_bytes: bytes) -> bytes:
    return MAGIC + struct.pack(">H", len(raw_bytes)) + raw_bytes


class TcpGateway:
    def __init__(self, port: int, on_to_radio=None):
        self.port = port
        self.on_to_radio = on_to_radio

        self._server: asyncio.Server | None = None
        self._clients: set[asyncio.StreamWriter] = set()

    async def start(self):
        try:
            self._server = await asyncio.start_server(
                self._handle_client, "0.0.0.0", self.port,
                reuse_address=True,
            )
            logger.info("TCP gateway listening on port %d", self.port)
        except OSError as e:
            logger.error("TCP gateway failed to bind port %d: %s", self.port, e)
            raise

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for writer in list(self._clients):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self._clients.clear()
        logger.info("TCP gateway on port %d stopped", self.port)

    def broadcast(self, raw_bytes: bytes):
        """Fan-out a FromRadio packet to all connected TCP clients."""
        if not self._clients:
            return
        frame = _frame(raw_bytes)
        dead = set()
        for writer in list(self._clients):
            try:
                writer.write(frame)
            except Exception:
                dead.add(writer)
        self._clients -= dead

    @property
    def client_count(self) -> int:
        return len(self._clients)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        logger.info("TCP client connected: %s (port %d)", peer, self.port)
        self._clients.add(writer)
        try:
            while True:
                # Skip wakeup bytes (0xc3) until we see the 0x94 magic start
                b = await reader.readexactly(1)
                if b == b'\xc3':
                    continue
                if b != b'\x94':
                    logger.warning("TCP client %s: unexpected byte 0x%02x", peer, b[0])
                    break

                # Read second magic byte
                b2 = await reader.readexactly(1)
                if b2 != b'\xc3':
                    logger.warning("TCP client %s: bad magic byte2 0x%02x", peer, b2[0])
                    break

                # 2-byte big-endian length
                size_bytes = await reader.readexactly(2)
                length = struct.unpack(">H", size_bytes)[0]
                if length == 0 or length > MAX_PAYLOAD:
                    logger.warning("TCP client %s: invalid length %d", peer, length)
                    break

                payload = await reader.readexactly(length)
                if self.on_to_radio:
                    try:
                        await self.on_to_radio(payload)
                    except Exception as e:
                        logger.warning("on_to_radio error for %s: %s", peer, e)

        except asyncio.IncompleteReadError:
            pass
        except Exception as e:
            logger.warning("TCP client %s error: %s", peer, e)
        finally:
            self._clients.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("TCP client disconnected: %s (port %d)", peer, self.port)

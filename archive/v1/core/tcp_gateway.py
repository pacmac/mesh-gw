"""Meshtastic TCP gateway.

Bridges a single BLE-connected MeshBridge to the standard Meshtastic TCP
protocol (same as serial/USB, same as what Android app / meshtastic --host
uses). Port 4403 is the Meshtastic convention; each device gets its own port.

Wire protocol (identical to Meshtastic serial framing over TCP):
  FromRadio (radio → client): 4-byte big-endian length + protobuf bytes
  ToRadio   (client → radio): 4-byte big-endian length + protobuf bytes

Usage:
    gw = TcpGateway(port=4403, on_to_radio=bridge.ble.send)
    await gw.start()
    ...
    gw.broadcast(raw_fromradio_bytes)   # called by bridge._on_packet()
    ...
    await gw.stop()
"""
import asyncio
import logging
import struct

logger = logging.getLogger(__name__)


class TcpGateway:
    def __init__(self, port: int, on_to_radio=None):
        self.port = port
        self.on_to_radio = on_to_radio   # async callable(bytes) — sends to BLE TORADIO

        self._server: asyncio.Server | None = None
        self._clients: set[asyncio.StreamWriter] = set()

    async def start(self):
        self._server = await asyncio.start_server(
            self._handle_client, "0.0.0.0", self.port
        )
        logger.info("TCP gateway listening on port %d", self.port)

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
        frame = struct.pack(">I", len(raw_bytes)) + raw_bytes
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
                # Read 4-byte big-endian length prefix
                header = await reader.readexactly(4)
                length = struct.unpack(">I", header)[0]
                if length == 0 or length > 512 * 1024:
                    logger.warning("TCP client %s sent invalid length %d — closing", peer, length)
                    break
                payload = await reader.readexactly(length)
                if self.on_to_radio:
                    try:
                        await self.on_to_radio(payload)
                    except Exception as e:
                        logger.warning("on_to_radio error for TCP client %s: %s", peer, e)
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

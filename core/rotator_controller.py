"""Rotator mode state machine.

Subscribes to the DeviceManager's unified event bus and drives the
rotator in ACTIVE mode: accumulates RF packets, picks a winner (most
heard), moves the antenna, then dwells at that bearing while already
collecting candidates for the next move — so the transition is seamless.
"""
import asyncio
import logging
import math
import time

logger = logging.getLogger(__name__)

MODE_PASSIVE = 0
MODE_ACTIVE  = 1
MODE_SCAN    = 2
MODE_TRACK   = 3


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def _angle_diff(a: float, b: float) -> float:
    d = (a - b + 360) % 360
    return d if d <= 180 else d - 360


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


class RotatorController:
    """Server-side rotator mode state machine."""

    def __init__(self, dm, broadcast_fn):
        self._dm = dm
        self._broadcast = broadcast_fn
        self._task: asyncio.Task | None = None
        self._mode: int = int(self._cfg().get("mode", MODE_PASSIVE))
        # _next_move = 0 means "start a fresh accumulation window on next packet".
        # During accumulation it is set to now + aim_accumulate_sec.
        # During dwell it is set to now + aim_dwell_sec.
        # Either way: if now < _next_move, keep collecting; if now >= _next_move, evaluate.
        self._next_move: float = 0.0
        self._candidates: dict[int, int] = {}  # node_num -> packet count
        self._point_target: int | None = None

    # -- lifecycle ----------------------------------------------------------------

    def start(self):
        self._task = asyncio.create_task(self._run(), name="rotator-controller")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # -- public API ---------------------------------------------------------------

    @property
    def mode(self) -> int:
        return self._mode

    @mode.setter
    def mode(self, value: int):
        self._mode = int(value)
        self._candidates.clear()
        self._next_move = 0.0
        from core import bridge_config
        cfg = bridge_config.load()
        cfg.setdefault("rotator", {})["mode"] = self._mode
        bridge_config.save(cfg)
        logger.info("Rotator mode -> %d", self._mode)

    @property
    def point_target(self) -> int | None:
        return self._point_target

    @property
    def state(self) -> dict:
        dwell_remaining = max(0.0, self._next_move - time.time()) if self._next_move else 0.0
        return {
            "mode": self._mode,
            "point_target": self._point_target,
            "dwell_remaining": round(dwell_remaining, 1),
        }

    # -- event loop ---------------------------------------------------------------

    async def _run(self):
        q = self._dm.subscribe()
        try:
            while True:
                ev = await q.get()
                if self._mode == MODE_ACTIVE:
                    await self._handle_active(ev)
        except asyncio.CancelledError:
            pass
        finally:
            self._dm.unsubscribe(q)

    async def _handle_active(self, ev: dict):
        if ev.get("type") != "packet":
            return
        pkt = (ev.get("data") or {}).get("packet") or {}
        from_num = pkt.get("from")
        if not from_num:
            return

        home = self._home_pos()
        if not home:
            return
        node_pos = self._node_pos(from_num)
        if not node_pos:
            return  # no position known for this node — can't aim at it
        if _haversine_km(home["lat"], home["lon"], node_pos["lat"], node_pos["lon"]) < 0.1:
            return  # node is co-located with home (e.g. OMNI radio at same site) — skip

        now = time.time()
        cfg = self._cfg()

        # Always accumulate during both the accumulation window and the dwell —
        # this is what lets us pick the best next target without stopping to listen.
        self._candidates[from_num] = self._candidates.get(from_num, 0) + 1

        # First packet after reset — open an accumulation window before deciding
        if self._next_move == 0.0:
            self._next_move = now + cfg.get("aim_accumulate_sec", 3)
            return

        # Still inside the window (accumulating or dwelling) — keep collecting
        if now < self._next_move:
            return

        # Window expired: evaluate, move, then start a new dwell window
        winner_num = max(self._candidates, key=lambda n: self._candidates[n])
        self._candidates.clear()
        # Set dwell immediately so packets that arrive during the move are counted
        # toward the *next* target evaluation.
        self._next_move = now + cfg.get("aim_dwell_sec", 15)

        winner_pos = self._node_pos(winner_num)
        if not winner_pos:
            return

        az = _bearing(home["lat"], home["lon"], winner_pos["lat"], winner_pos["lon"])

        # Deadband: skip the physical move if already pointing close enough
        rot = self._dm.get_rotator()
        if rot:
            cur_az = rot.status.get("az")
            if cur_az is not None and abs(_angle_diff(az, cur_az)) < cfg.get("aim_deadband_deg", 5):
                self._point_target = winner_num
                await self._broadcast({
                    "type": "rotator",
                    "data": {"az": cur_az, "point_target": winner_num, "mode": MODE_ACTIVE},
                })
                return

        logger.info("Active: !%x az=%.1f° dwell=%.0fs", winner_num, az, cfg.get("aim_dwell_sec", 15))
        if rot and rot.connected:
            try:
                await rot.move(az)
            except Exception as e:
                logger.warning("Rotator move failed: %s", e)

        self._point_target = winner_num
        await self._broadcast({
            "type": "rotator",
            "data": {"az": az, "point_target": winner_num, "mode": MODE_ACTIVE},
        })

    # -- helpers ------------------------------------------------------------------

    def _cfg(self) -> dict:
        from core import bridge_config
        return bridge_config.load().get("rotator", {})

    def _home_pos(self) -> dict | None:
        """Position of the YAGI gateway's own radio (the bearing origin)."""
        for bridge in self._dm._devices.values():
            if getattr(bridge, "rotator", None) is not None and bridge.my_node_num:
                pos = bridge.state.nodes.get(str(bridge.my_node_num), {}).get("position", {})
                lat_i = pos.get("latitude_i")
                lon_i = pos.get("longitude_i")
                if lat_i is not None and lon_i is not None:
                    return {"lat": lat_i / 1e7, "lon": lon_i / 1e7}
        return None

    def _node_pos(self, node_num: int) -> dict | None:
        """Most recent known position of node_num across all connected bridges."""
        for bridge in self._dm._devices.values():
            pos = bridge.state.nodes.get(str(node_num), {}).get("position", {})
            lat_i = pos.get("latitude_i")
            lon_i = pos.get("longitude_i")
            if lat_i is not None and lon_i is not None:
                return {"lat": lat_i / 1e7, "lon": lon_i / 1e7}
        return None

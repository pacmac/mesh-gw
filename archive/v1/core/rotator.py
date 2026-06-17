"""Rotator driver interface and factory.

To add a new driver: subclass RotatorBase, implement all abstract methods,
then register the name in load_rotator().
"""
from abc import ABC, abstractmethod


class RotatorBase(ABC):
    """Common interface all rotator drivers must implement."""

    on_status = None    # async callable(status_dict) — set by bridge

    @abstractmethod
    def start(self): ...

    @abstractmethod
    async def stop(self): ...

    @abstractmethod
    async def move(self, az: float): ...

    @abstractmethod
    async def set_mode(self, mode: int): ...

    @property
    @abstractmethod
    def connected(self) -> bool: ...

    @property
    @abstractmethod
    def status(self) -> dict: ...


def load_rotator(cfg: dict) -> RotatorBase:
    """Instantiate the driver named in cfg['driver'] (default 'v4_ws')."""
    driver = cfg.get("driver", "v4_ws")
    if driver == "v4_ws":
        from .rotator_v4ws import V4WsRotator
        return V4WsRotator(cfg["ws_url"])
    raise ValueError(f"Unknown rotator driver: {driver!r}")

"""Typed configuration dataclasses for the BLE subsystem.

All config is loaded once at startup into these dataclasses. No raw dict key
lookups appear inside BleDevice or OtaSession — every field access is typed.

Source of truth: docs/BLE-SPEC.md § "Configuration SSOT"
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

_CONFIG_FILE = Path(__file__).parent / "bridge_config.yaml"


@dataclass
class BleConfig:
    scan_timeout_s: float = 30.0
    scan_pause_s: float = 300.0
    connect_timeout_s: float = 15.0
    connect_max_retries: int = 5
    connect_retry_delay_s: float = 5.0
    discover_timeout_s: float = 10.0
    discover_max_retries: int = 3
    stale_release_wait_s: float = 2.0
    sync_mode: str = "config_only"
    sync_timeout_s: float = 30.0
    sync_max_retries: int = 3
    notify_idle_timeout_s: float = 5.0
    reconnect_timeout_s: float = 15.0
    reconnect_max_retries: int = 10
    reconnect_backoff_max_s: float = 60.0
    conn_priority_enabled: bool = True
    conn_interval_high_min: int = 6
    conn_interval_high_max: int = 9
    conn_interval_balanced_min: int = 24
    conn_interval_balanced_max: int = 40
    conn_interval_latency: int = 0
    conn_interval_timeout: int = 50
    conn_priority_update_wait_s: float = 1.0
    conn_priority_downgrade_s: float = 30.0
    admin_passkey_refresh_s: float = 240.0


@dataclass
class OtaConfig:
    dir: str = ""
    pending_timeout_s: float = 30.0
    bootloader_init_wait_s: float = 2.0
    bootloader_connect_timeout_s: float = 30.0
    handshake_timeout_s: float = 15.0
    chunk_size: int = 512
    chunk_delay_ms: int = 0
    nvs_serial_wait_s: float = 120.0
    nvs_serial_poll_s: float = 2.0
    nvs_serial_erase_timeout_s: float = 30.0
    nvs_serial_baud: int = 460800
    nvs_offset: str = "0x9000"
    nvs_size: str = "0x5000"
    esptool_bin: str = ""
    bootloader_reboot_max_retries: int = 3
    bootloader_reboot_retry_delay_s: float = 5.0
    nordic_prn: int = 10                    # packet receipt notification interval (0 = disabled)
    nordic_packet_delay_ms: int = 0         # per-chunk delay for slow hosts

    def firmware_dir(self, hw_model: str) -> Path:
        """Resolve ota.dir / hw_model. Relative paths anchor to the config file's directory.
        hw_model is the exact string reported by the device (e.g. "RAK4631", "HELTEC_HT62").
        """
        raw = Path(self.dir) if self.dir else Path(".")
        if not raw.is_absolute():
            raw = (_CONFIG_FILE.parent / raw).resolve()
        return raw / hw_model

    def resolved_esptool_bin(self) -> str:
        if self.esptool_bin:
            return self.esptool_bin
        return str(Path(sys.executable).parent / "esptool")


@dataclass
class BleDeviceConfig:
    address: str = ""
    auto_connect: bool = True
    tcp_port: Optional[int] = None
    flags: list[str] = field(default_factory=list)
    hw_model: str = ""   # e.g. "HELTEC_HT62"; used for rescue OTA when no sync has completed
    lora_region: str = ""  # e.g. "EU_868"; auto-sent when region is unset after sync


def _ble_config_from_dict(raw: dict) -> BleConfig:
    defaults = BleConfig()
    return BleConfig(
        scan_timeout_s=float(raw.get("scan_timeout_s", defaults.scan_timeout_s)),
        scan_pause_s=float(raw.get("scan_pause_s", defaults.scan_pause_s)),
        connect_timeout_s=float(raw.get("connect_timeout_s", defaults.connect_timeout_s)),
        connect_max_retries=int(raw.get("connect_max_retries", defaults.connect_max_retries)),
        connect_retry_delay_s=float(raw.get("connect_retry_delay_s", defaults.connect_retry_delay_s)),
        discover_timeout_s=float(raw.get("discover_timeout_s", defaults.discover_timeout_s)),
        discover_max_retries=int(raw.get("discover_max_retries", defaults.discover_max_retries)),
        stale_release_wait_s=float(raw.get("stale_release_wait_s", defaults.stale_release_wait_s)),
        sync_mode=str(raw.get("sync_mode", defaults.sync_mode)),
        sync_timeout_s=float(raw.get("sync_timeout_s", defaults.sync_timeout_s)),
        sync_max_retries=int(raw.get("sync_max_retries", defaults.sync_max_retries)),
        notify_idle_timeout_s=float(raw.get("notify_idle_timeout_s", defaults.notify_idle_timeout_s)),
        reconnect_timeout_s=float(raw.get("reconnect_timeout_s", defaults.reconnect_timeout_s)),
        reconnect_max_retries=int(raw.get("reconnect_max_retries", defaults.reconnect_max_retries)),
        reconnect_backoff_max_s=float(raw.get("reconnect_backoff_max_s", defaults.reconnect_backoff_max_s)),
        conn_priority_enabled=bool(raw.get("conn_priority_enabled", defaults.conn_priority_enabled)),
        conn_interval_high_min=int(raw.get("conn_interval_high_min", defaults.conn_interval_high_min)),
        conn_interval_high_max=int(raw.get("conn_interval_high_max", defaults.conn_interval_high_max)),
        conn_interval_balanced_min=int(raw.get("conn_interval_balanced_min", defaults.conn_interval_balanced_min)),
        conn_interval_balanced_max=int(raw.get("conn_interval_balanced_max", defaults.conn_interval_balanced_max)),
        conn_interval_latency=int(raw.get("conn_interval_latency", defaults.conn_interval_latency)),
        conn_interval_timeout=int(raw.get("conn_interval_timeout", defaults.conn_interval_timeout)),
        conn_priority_update_wait_s=float(raw.get("conn_priority_update_wait_s", defaults.conn_priority_update_wait_s)),
        conn_priority_downgrade_s=float(raw.get("conn_priority_downgrade_s", defaults.conn_priority_downgrade_s)),
        admin_passkey_refresh_s=float(raw.get("admin_passkey_refresh_s", defaults.admin_passkey_refresh_s)),
    )


def _ota_config_from_dict(raw: dict) -> OtaConfig:
    defaults = OtaConfig()
    return OtaConfig(
        dir=str(raw.get("dir", defaults.dir)),
        pending_timeout_s=float(raw.get("pending_timeout_s", defaults.pending_timeout_s)),
        bootloader_init_wait_s=float(raw.get("bootloader_init_wait_s", defaults.bootloader_init_wait_s)),
        bootloader_connect_timeout_s=float(raw.get("bootloader_connect_timeout_s", defaults.bootloader_connect_timeout_s)),
        handshake_timeout_s=float(raw.get("handshake_timeout_s", defaults.handshake_timeout_s)),
        chunk_size=int(raw.get("chunk_size", defaults.chunk_size)),
        chunk_delay_ms=int(raw.get("chunk_delay_ms", defaults.chunk_delay_ms)),
        nvs_serial_wait_s=float(raw.get("nvs_serial_wait_s", defaults.nvs_serial_wait_s)),
        nvs_serial_poll_s=float(raw.get("nvs_serial_poll_s", defaults.nvs_serial_poll_s)),
        nvs_serial_erase_timeout_s=float(raw.get("nvs_serial_erase_timeout_s", defaults.nvs_serial_erase_timeout_s)),
        nvs_serial_baud=int(raw.get("nvs_serial_baud", defaults.nvs_serial_baud)),
        nvs_offset=str(raw.get("nvs_offset", defaults.nvs_offset)),
        nvs_size=str(raw.get("nvs_size", defaults.nvs_size)),
        esptool_bin=str(raw.get("esptool_bin", defaults.esptool_bin)),
        bootloader_reboot_max_retries=int(raw.get("bootloader_reboot_max_retries", defaults.bootloader_reboot_max_retries)),
        bootloader_reboot_retry_delay_s=float(raw.get("bootloader_reboot_retry_delay_s", defaults.bootloader_reboot_retry_delay_s)),
        nordic_prn=int(raw.get("nordic_prn", defaults.nordic_prn)),
        nordic_packet_delay_ms=int(raw.get("nordic_packet_delay_ms", defaults.nordic_packet_delay_ms)),
    )


def _device_config_from_dict(raw: dict) -> BleDeviceConfig:
    addr = str(raw.get("address", "")).upper().strip()
    return BleDeviceConfig(
        address=addr,
        auto_connect=bool(raw.get("auto_connect", True)),
        tcp_port=int(raw["tcp_port"]) if raw.get("tcp_port") is not None else None,
        flags=list(raw.get("flags", [])),
        hw_model=str(raw.get("hw_model", "")),
        lora_region=str(raw.get("lora_region", "")),
    )


def load(path: Path | str = _CONFIG_FILE) -> tuple[list[BleDeviceConfig], BleConfig, OtaConfig]:
    """Load bridge_config.yaml and return typed config objects.

    Missing `ble:` or `ota:` sections get spec defaults. Unknown keys are
    silently ignored so old config files remain forward-compatible.

    Returns:
        (device_configs, ble_cfg, ota_cfg)
    """
    try:
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
    except FileNotFoundError:
        raw = {}

    device_configs = [
        _device_config_from_dict(d)
        for d in (raw.get("ble_devices") or [])
        if d.get("address")
    ]

    ble_cfg = _ble_config_from_dict(raw.get("ble") or {})
    ota_cfg = _ota_config_from_dict(raw.get("ota") or {})

    return device_configs, ble_cfg, ota_cfg

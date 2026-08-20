"""Systemd control for one mode's IB Gateway + trading engine services,
used only by the dashboard's Disconnect/Reconnect control — lets you stop
the bot's IBKR session on purpose to free it up for a manual TWS/IBKR
Mobile login, then bring it back afterward, without needing SSH.

start/stop go through `sudo systemctl ...`: the `tradingbot` user (which
runs the dashboard) has a narrowly scoped, passwordless sudo rule for
exactly these 8 commands and nothing else (see
deploy/sudoers-tradingbot) — the dashboard itself is never granted a
general shell or broader systemctl access. Status queries (`is-active`)
don't need sudo — systemd allows any local user to read unit state.
"""
import socket
import subprocess

from src import db, mode_config

GATEWAY_UNIT = {"paper": "ibgateway-paper.service", "live": "ibgateway-live.service"}
ENGINE_UNIT = {"paper": "trading-bot-paper.service", "live": "trading-bot-live.service"}
SYSTEMCTL_TIMEOUT = 20


class GatewayControlError(RuntimeError):
    """Raised when a start/stop actually fails — most likely because
    deploy/sudoers-tradingbot hasn't been installed on this server yet."""


def _systemctl(*args: str, use_sudo: bool) -> subprocess.CompletedProcess:
    # sudo -n (non-interactive): fail immediately with a clear stderr
    # message if the sudoers rule isn't installed, instead of hanging on
    # a password prompt that can never be answered here.
    cmd = (["sudo", "-n"] if use_sudo else []) + ["systemctl", *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=SYSTEMCTL_TIMEOUT)


def _is_active(unit: str) -> bool:
    return _systemctl("is-active", unit, use_sudo=False).stdout.strip() == "active"


def _run_privileged(*args: str):
    result = _systemctl(*args, use_sudo=True)
    if result.returncode != 0:
        raise GatewayControlError(
            (result.stderr or result.stdout or f"systemctl {' '.join(args)} failed").strip()
        )


def _port_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def status(mode: str, env: dict) -> dict:
    # This module only controls the admin's fixed systemd units so far —
    # real per-account Gateway processes are a separate, not-yet-built
    # piece of infrastructure (dynamic systemd units + IBC config
    # provisioning), so it always checks the admin's own Gateway.
    return {
        "gateway_active": _is_active(GATEWAY_UNIT[mode]),
        "engine_active": _is_active(ENGINE_UNIT[mode]),
        "port_listening": _port_listening(mode_config.ibkr_port(env, db.get_default_account_id(), mode)),
    }


def disconnect(mode: str):
    """Stops the engine first, then the Gateway — so the engine never sees
    a Gateway disappear out from under it mid-cycle."""
    _run_privileged("stop", ENGINE_UNIT[mode])
    _run_privileged("stop", GATEWAY_UNIT[mode])


def reconnect_gateway(mode: str):
    """Starts only the Gateway. IBKR may require a fresh 2FA approval on
    the phone before it actually logs in — poll status() and only call
    resume_engine() once port_listening is true."""
    _run_privileged("start", GATEWAY_UNIT[mode])


def resume_engine(mode: str):
    _run_privileged("start", ENGINE_UNIT[mode])

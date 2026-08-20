"""Low-level systemd control shared by web/gateway_control.py (the admin's
fixed Gateway/engine units) and src/gateway_provisioning.py (every other
account's instantiated units).

start/stop go through `sudo systemctl ...`: the `tradingbot` user (which
runs the dashboard) has a narrowly scoped, passwordless sudo rule for
exactly these unit patterns and nothing else (see
deploy/sudoers-tradingbot) — the dashboard itself is never granted a
general shell or broader systemctl access. Status queries (`is-active`)
don't need sudo — systemd allows any local user to read unit state.
"""
import socket
import subprocess

SYSTEMCTL_TIMEOUT = 20


class SystemctlError(RuntimeError):
    """Raised when a start/stop actually fails — most likely because
    deploy/sudoers-tradingbot hasn't been installed (or updated) on this
    server yet."""


def systemctl(*args: str, use_sudo: bool) -> subprocess.CompletedProcess:
    # sudo -n (non-interactive): fail immediately with a clear stderr
    # message if the sudoers rule isn't installed, instead of hanging on
    # a password prompt that can never be answered here.
    cmd = (["sudo", "-n"] if use_sudo else []) + ["systemctl", *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=SYSTEMCTL_TIMEOUT)


def is_active(unit: str) -> bool:
    return systemctl("is-active", unit, use_sudo=False).stdout.strip() == "active"


def run_privileged(*args: str):
    result = systemctl(*args, use_sudo=True)
    if result.returncode != 0:
        raise SystemctlError(
            (result.stderr or result.stdout or f"systemctl {' '.join(args)} failed").strip()
        )


def port_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex(("127.0.0.1", port)) == 0

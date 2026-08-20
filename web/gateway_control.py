"""Systemd control for the ADMIN's fixed IB Gateway + trading engine
services, used only by the dashboard's Disconnect/Reconnect control — lets
you stop the bot's IBKR session on purpose to free it up for a manual TWS/
IBKR Mobile login, then bring it back afterward, without needing SSH.

Every other account's own Gateway/engine (instantiated systemd units) is
controlled by src/gateway_provisioning.py instead — this module only ever
touches the admin's original, fixed unit names.
"""
from src import db, mode_config, systemd_util

GATEWAY_UNIT = {"paper": "ibgateway-paper.service", "live": "ibgateway-live.service"}
ENGINE_UNIT = {"paper": "trading-bot-paper.service", "live": "trading-bot-live.service"}

# Kept as an alias so existing callers catching this name keep working.
GatewayControlError = systemd_util.SystemctlError


def status(mode: str, env: dict) -> dict:
    # This module only controls the admin's fixed systemd units so far —
    # real per-account Gateway processes go through gateway_provisioning.py
    # instead, so this always checks the admin's own Gateway.
    return {
        "gateway_active": systemd_util.is_active(GATEWAY_UNIT[mode]),
        "engine_active": systemd_util.is_active(ENGINE_UNIT[mode]),
        "port_listening": systemd_util.port_listening(mode_config.ibkr_port(env, db.get_default_account_id(), mode)),
    }


def disconnect(mode: str):
    """Stops the engine first, then the Gateway — so the engine never sees
    a Gateway disappear out from under it mid-cycle."""
    systemd_util.run_privileged("stop", ENGINE_UNIT[mode])
    systemd_util.run_privileged("stop", GATEWAY_UNIT[mode])


def reconnect_gateway(mode: str):
    """Starts only the Gateway. IBKR may require a fresh 2FA approval on
    the phone before it actually logs in — poll status() and only call
    resume_engine() once port_listening is true."""
    systemd_util.run_privileged("start", GATEWAY_UNIT[mode])


def resume_engine(mode: str):
    systemd_util.run_privileged("start", ENGINE_UNIT[mode])

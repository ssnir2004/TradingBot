#!/usr/bin/env bash
# Launches IB Gateway headlessly via IBC + Xvfb (Gateway needs a display
# even when nobody's watching it). Called by deploy/ibgateway.service.
#
# IBC's own gatewaystart.sh (this IBC release, 3.24.x) does NOT read
# environment variables — it has a block of `VAR=value` assignments at the
# top of the file that you're meant to edit directly. To keep that
# configuration in version control instead of a manual one-off edit on the
# server, this script patches those lines with sed every time it runs
# (idempotent — safe to run repeatedly). Adjust the values below to match
# your install if they differ from the defaults in DEPLOY.md.
set -euo pipefail

IBC_PATH="/opt/ibc"
TWS_PATH="/opt/ibgateway"
TWS_MAJOR_VRSN="1045"          # from `IB Gateway 10.45` — see DEPLOY.md
TRADING_MODE="paper"
IBC_INI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config.ini"
GATEWAYSTART="$IBC_PATH/gatewaystart.sh"

if [ ! -f "$IBC_INI" ]; then
    echo "Missing $IBC_INI — copy config.ini.example to config.ini and fill in your credentials first." >&2
    exit 1
fi

if [ ! -f "$GATEWAYSTART" ]; then
    echo "$GATEWAYSTART not found — check your IBC install path (IBC_PATH=$IBC_PATH)." >&2
    exit 1
fi

sed -i \
    -e "s|^TWS_MAJOR_VRSN=.*|TWS_MAJOR_VRSN=${TWS_MAJOR_VRSN}|" \
    -e "s|^IBC_INI=.*|IBC_INI=${IBC_INI}|" \
    -e "s|^TRADING_MODE=.*|TRADING_MODE=${TRADING_MODE}|" \
    -e "s|^IBC_PATH=.*|IBC_PATH=${IBC_PATH}|" \
    -e "s|^TWS_PATH=.*|TWS_PATH=${TWS_PATH}|" \
    "$GATEWAYSTART"

exec xvfb-run --auto-servernum --server-args="-screen 0 1024x768x24" \
    "$GATEWAYSTART" -inline

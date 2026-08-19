#!/usr/bin/env bash
# Launches IB Gateway headlessly via IBC + Xvfb (Gateway needs a display
# even when nobody's watching it). Adjust TWS_PATH/IBC_PATH below to match
# wherever you installed IB Gateway and IBC on this machine — see
# ../../DEPLOY.md for the install steps. Called by deploy/ibgateway.service.
set -euo pipefail

export TWS_PATH="/opt/ibgateway"
export IBC_PATH="/opt/ibc"
export IBC_INI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config.ini"
export TRADING_MODE="paper"

if [ ! -f "$IBC_INI" ]; then
    echo "Missing $IBC_INI — copy config.ini.example to config.ini and fill in your credentials first." >&2
    exit 1
fi

# IBC's own gatewaystart.sh script name/flags have changed across releases —
# check IBC_PATH/README.md for the exact invocation on the version you
# installed if this doesn't work as-is.
exec xvfb-run --auto-servernum --server-args="-screen 0 1024x768x24" \
    "$IBC_PATH/scripts/gatewaystart.sh"

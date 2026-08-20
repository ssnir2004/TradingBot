#!/usr/bin/env bash
# Launches ONE mode's IB Gateway headlessly via IBC + Xvfb (Gateway needs a
# display even when nobody's watching it). Called by
# deploy/ibgateway-paper.service / deploy/ibgateway-live.service (the
# admin's own Gateway, $1=mode only) and by the instantiated
# deploy/ibgateway-paper@.service / deploy/ibgateway-live@.service (one per
# other account, $1=mode $2=account_id — %i in the unit file).
#
# Paper and live run as two separate, simultaneous IB Gateway processes,
# and every account (admin or not) is its own separate pair on top of that
# — each needs its own settings dir (so login sessions never clobber each
# other), its own log dir, and its own copy of IBC's gatewaystart.sh (IBC's
# gatewaystart.sh does not read environment variables — it has a block of
# `VAR=value` assignments at the top of the file that's meant to be edited
# directly; instances patching the SAME physical file at once would race
# each other, so each one gets its own copy instead).
set -euo pipefail

MODE="${1:-}"
ACCOUNT_ID="${2:-}"   # empty = the admin's original single-account setup
if [ "$MODE" != "paper" ] && [ "$MODE" != "live" ]; then
    echo "Usage: $0 <paper|live> [account_id]" >&2
    exit 1
fi

IBC_PATH="/opt/ibc"
TWS_PATH="/opt/ibgateway"
TWS_MAJOR_VRSN="1045"          # from `IB Gateway 10.45` — see DEPLOY.md
IBC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -n "$ACCOUNT_ID" ]; then
    SUFFIX="${ACCOUNT_ID}-${MODE}"
else
    SUFFIX="${MODE}"
fi
IBC_INI="${IBC_DIR}/config-${SUFFIX}.ini"
TWS_SETTINGS_PATH="/home/tradingbot/ibgateway-settings-${SUFFIX}"
LOG_PATH="/home/tradingbot/ibc-logs-${SUFFIX}"
GATEWAYSTART_TEMPLATE="$IBC_PATH/gatewaystart.sh"
GATEWAYSTART="$IBC_PATH/gatewaystart-${SUFFIX}.sh"

if [ ! -f "$IBC_INI" ]; then
    if [ -n "$ACCOUNT_ID" ]; then
        echo "Missing $IBC_INI — this account hasn't saved IBKR credentials and clicked Connect from the dashboard yet." >&2
    else
        echo "Missing $IBC_INI — copy config-${MODE}.ini.example to config-${MODE}.ini and fill in your credentials first." >&2
    fi
    exit 1
fi

if [ ! -f "$GATEWAYSTART_TEMPLATE" ]; then
    echo "$GATEWAYSTART_TEMPLATE not found — check your IBC install path (IBC_PATH=$IBC_PATH)." >&2
    exit 1
fi

mkdir -p "$TWS_SETTINGS_PATH" "$LOG_PATH"
cp "$GATEWAYSTART_TEMPLATE" "$GATEWAYSTART"

sed -i \
    -e "s|^TWS_MAJOR_VRSN=.*|TWS_MAJOR_VRSN=${TWS_MAJOR_VRSN}|" \
    -e "s|^IBC_INI=.*|IBC_INI=${IBC_INI}|" \
    -e "s|^TRADING_MODE=.*|TRADING_MODE=${MODE}|" \
    -e "s|^IBC_PATH=.*|IBC_PATH=${IBC_PATH}|" \
    -e "s|^TWS_PATH=.*|TWS_PATH=${TWS_PATH}|" \
    -e "s|^TWS_SETTINGS_PATH=.*|TWS_SETTINGS_PATH=${TWS_SETTINGS_PATH}|" \
    -e "s|^LOG_PATH=.*|LOG_PATH=${LOG_PATH}|" \
    "$GATEWAYSTART"

# Keep IBC's own config-<mode>.ini in sync too, so both files agree.
sed -i -e "s|^TradingMode=.*|TradingMode=${MODE}|" "$IBC_INI"

exec xvfb-run --auto-servernum --server-args="-screen 0 1024x768x24" \
    "$GATEWAYSTART" -inline

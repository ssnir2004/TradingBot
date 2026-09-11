"""Turn a finalized Signal into: a row in momentum_signals, a decision_log
entry (so it shows up in the existing Decision Center), and a Telegram
message. Phase 1's only output - nothing here places an order.
"""
import logging
import os

from src import db
from src.notify import notify
from momentum import store
from momentum.signals import Signal

log = logging.getLogger("momentum.alert")

# Set to "1" to persist + log signals but suppress the Telegram push -
# used by --once smoke tests so a detector tripping on stale EOD data
# doesn't ping the phone.
_DRYRUN = os.environ.get("MOMENTUM_ALERT_DRYRUN") == "1"


def _fmt(sig: Signal) -> str:
    f = sig.features
    lines = [
        f"{sig.strategy} · {sig.symbol}  ${sig.price:.2f}",
        f"entry {sig.entry_ref:.2f} → stop {sig.stop_price:.2f} "
        f"(max {sig.max_loss_price:.2f})",
        f"scale1 {sig.first_scale_price:.2f} · scale2 {sig.second_scale_price:.2f}",
        f"RVOL {f.get('rvol', float('nan')):.1f}× · +{f.get('change_from_open_pct', float('nan')):.0f}% "
        f"from open · float {f.get('float_shares', 0)/1e6:.1f}M",
    ]
    if sig.shares_hint:
        lines.append(f"size hint {sig.shares_hint} sh (~${sig.shares_hint*sig.entry_ref:,.0f})")
    if sig.conviction == "low":
        lines.append("⚠ low conviction (MA-pullback weakness)")
    if sig.above_preferred_range:
        lines.append("⚠ above $10 preferred range — 5m signals only")
    return "\n".join(lines)


def emit(sig: Signal, account_id: int, mode: str = "alert") -> int:
    """Persist + notify. Returns the momentum_signals row id."""
    row = sig.to_store_row(mode=mode)
    sig_id = store.record_signal(row)

    try:
        db.log_decision(
            account_id, "live", "momentum_signal",
            strategy=sig.strategy, symbol=sig.symbol, price=sig.price,
            entry_ref=sig.entry_ref, stop=sig.stop_price,
            conviction=sig.conviction, signal_id=sig_id, features=sig.features,
        )
    except Exception:                       # decision_log must never break the loop
        log.exception("log_decision failed for %s %s", sig.strategy, sig.symbol)

    if _DRYRUN:
        log.info("[dryrun] would notify:\n%s", _fmt(sig))
    else:
        try:
            notify(f"Momentum {sig.strategy}: {sig.symbol}", _fmt(sig),
                   priority="high" if sig.conviction != "low" else "default")
        except Exception:
            log.exception("notify failed for %s %s", sig.strategy, sig.symbol)

    log.info("SIGNAL #%s %s %s entry=%.2f stop=%.2f", sig_id, sig.strategy, sig.symbol,
             sig.entry_ref, sig.stop_price)
    return sig_id

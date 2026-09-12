"""One momentum scan cycle: TradingView scan -> shortlist -> bars ->
G2/G6 gates -> four detectors -> alert. Called on a timer by run_momentum.py.

Phase 1: alert-only. `mode` is always "alert" here; the wiring for a
future "live" mode (place orders through src.ibkr_client) is intentionally
left as the single branch in _handle_signal.
"""
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo

import pandas as pd

from src import db
from momentum import bars, scanner, store
from momentum import features as F
from momentum.config import load_config
from momentum.alert import emit
from momentum.signals import Signal, cooldown_ok
from momentum.strategies import DetectContext, enabled_detectors

log = logging.getLogger("momentum.loop")
ET = ZoneInfo("America/New_York")


def _parse_hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def in_entry_window(cfg: dict, now: datetime | None = None) -> bool:
    now = now or datetime.now(ET)
    if now.weekday() >= 5:
        return False
    s = cfg["screener"]
    start, end = (_parse_hhmm(x) for x in s["entry_window_et"])
    if s["premarket_enabled"]:
        start = time(4, 0)
    if s["afternoon_enabled"]:
        end = time(15, 55)
    return start <= now.time() <= end


def _equity(account_id: int) -> float | None:
    raw = db.get_account_info(account_id, "live").get("net_liquidation", "")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _log_rejection(account_id: int, symbol: str, reason: str, **extra) -> None:
    """A shortlisted candidate (already cleared the G1/G3 screener, see
    momentum_candidates' own reject_reason for that earlier stage) that
    didn't become a signal - and WHY, one level deeper than the screener
    reject_reason. Before this, a candidate could sit shortlisted for an
    entire session (confirmed live: MKDW, 2026-09-11, passed_screener for
    over an hour straight, ~80 evaluations) with zero persisted trace of
    which of the four gates below actually blocked it every single time.
    Called only for shortlisted candidates (a handful per cycle, not the
    full scanned universe), so volume stays bounded - same reasoning
    alert.emit's own log_decision call already accepts. Never allowed to
    break the scan loop, same as alert.emit's own try/except."""
    try:
        db.log_decision(account_id, "live", "momentum_rejected", symbol=symbol, reason=reason, **extra)
    except Exception:
        log.exception("log_decision failed for rejection %s %s", symbol, reason)


def _closed_session_5m(symbol: str) -> pd.DataFrame | None:
    df = bars.get_intraday(symbol, "5m", archive=True)
    if df is None or df.empty:
        return None
    session = bars.session_bars(df)
    if session is None or len(session) < 2:
        return None
    # drop the still-forming last candle
    now = datetime.now(ET)
    if (now - session.index[-1].to_pydatetime()).total_seconds() < 300:
        session = session.iloc[:-1]
    return session if len(session) >= 2 else None


def run_scan_cycle(account_id: int, *, force: bool = False) -> dict:
    cfg = load_config()
    scan_iso = datetime.now(ET).isoformat(timespec="seconds")

    if not force and not cfg.get("enabled", True):
        return {"scan_iso": scan_iso, "skipped": "disabled (dashboard kill switch)"}
    if not force and not in_entry_window(cfg):
        return {"scan_iso": scan_iso, "skipped": "outside entry window"}

    cands = scanner.scan(cfg)
    store.record_candidates(scan_iso, [c.as_row() for c in cands])
    if not cands:
        return {"scan_iso": scan_iso, "candidates": 0, "signals": 0}

    shortlist = [c for c in cands if c.passed_screener][: cfg["screener"]["max_shortlist"]]
    detectors = enabled_detectors(cfg)
    equity = _equity(account_id)
    summary = {"scan_iso": scan_iso, "candidates": len(cands),
               "shortlist": len(shortlist), "signals": 0, "fired": []}

    for c in shortlist:
        try:
            n = _evaluate_candidate(c, cfg, detectors, account_id, equity)
            summary["signals"] += n
            if n:
                summary["fired"].append(c.symbol)
        except Exception:
            log.exception("candidate %s failed", c.symbol)
    return summary


def _evaluate_candidate(c: scanner.Candidate, cfg: dict, detectors, account_id: int,
                        equity: float | None) -> int:
    session = _closed_session_5m(c.symbol)
    if session is None:
        _log_rejection(account_id, c.symbol, "no_bar_data")
        return 0

    # G2 - per-minute volume spike (informational gate; annotate only in v1)
    minute = bars.get_intraday(c.symbol, "1m", archive=True)
    minute_session = bars.session_bars(minute) if minute is not None else None
    spiked, spike_ratio = F.volume_spike(
        minute_session, cfg["screener"]["volume_spike_lookback_min"],
        cfg["screener"]["volume_spike_mult"],
    ) if minute_session is not None else (False, float("nan"))

    # G6 - overhead resistance on the daily chart
    daily = bars.get_daily(c.symbol)
    mode = cfg["screener"]["resistance_filter_mode"]
    clear, ceiling = F.overhead_resistance(
        daily, c.price, cfg["screener"]["resistance_lookback_days"],
        cfg["screener"]["resistance_headroom_pct"],
    )
    if mode == "reject" and not clear:
        _log_rejection(account_id, c.symbol, "resistance_blocked", price=c.price, ceiling=_r(ceiling))
        return 0

    # guard against a scan price that's badly out of step with the bars
    # (stale after-hours quote, a halt, or a bad tick) - during the entry
    # window the two should track within a few percent.
    last_close = float(session["Close"].iloc[-1])
    if last_close > 0 and not (0.94 <= c.price / last_close <= 1.20):
        log.info("skip %s: scan price %.2f vs last 5m close %.2f (out of band)",
                 c.symbol, c.price, last_close)
        _log_rejection(account_id, c.symbol, "price_out_of_band", scan_price=c.price, last_5m_close=last_close)
        return 0

    hod = F.session_hod(session)
    ctx = DetectContext(cfg=cfg, candidate=c, price=c.price,
                        session_5m=session, session_1m=minute_session,
                        daily=daily, hod=hod)

    fired = 0
    no_match: list[str] = []
    suppressed: list[dict] = []
    for det in detectors:
        sig: Signal | None = det.evaluate(ctx)
        if sig is None:
            no_match.append(det.key)
            continue
        sig.features.update({
            "volume_spike": bool(spiked), "volume_spike_ratio": _r(spike_ratio),
            "overhead_clear": bool(clear),
            "overhead_ceiling": (round(ceiling, 4) if ceiling else None),
        })
        # honour a detector-supplied structural stop (Strategy D pivot_low)
        stop_override = sig.features.get("stop_override")
        sig.finalize(cfg, equity)
        if stop_override:
            sig.stop_price = float(stop_override)

        ok, why = cooldown_ok(sig, cfg)
        if not ok:
            log.info("suppressed: %s", why)
            suppressed.append({"strategy": det.key, "why": why})
            continue
        emit(sig, account_id, mode="alert")
        fired += 1

        # Phase 3: real order placement - gated on BOTH the master live
        # kill switch and this specific strategy being approved for it.
        # Exceptions here must never break the scan/alert loop (an alert
        # has already gone out above regardless of what happens next) -
        # see momentum.live's own module docstring for the isolation and
        # safety design.
        if cfg["live"]["enabled"] and sig.strategy in cfg["live_strategies"]:
            try:
                from momentum import live
                live.place_entry(sig, cfg)
            except Exception:
                log.exception("live.place_entry failed for %s %s", sig.strategy, sig.symbol)

    if fired == 0:
        # Every enabled detector either found no pattern match at all, or
        # matched but was suppressed by cooldown - the two are reported
        # separately (no_pattern_match still carries which detectors were
        # actually tried) since a run of cooldown suppressions on an
        # otherwise-qualifying setup is a materially different situation
        # from the pattern simply never forming.
        if suppressed:
            _log_rejection(account_id, c.symbol, "cooldown_suppressed", suppressed=suppressed)
        else:
            _log_rejection(account_id, c.symbol, "no_pattern_match", detectors_tried=no_match)
    return fired


def _r(x) -> float | None:
    try:
        return None if x != x else round(float(x), 2)
    except (TypeError, ValueError):
        return None

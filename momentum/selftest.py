"""Offline sanity checks for the momentum suite - synthetic bars that
should trip each detector, plus a live scanner ping. Not a full test
suite; enough to catch an import error, a shape mismatch, or a detector
that never fires / always fires.

    python -m momentum.selftest            # synthetic detectors + config
    python -m momentum.selftest --live     # also hit TradingView + yfinance
"""
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from momentum import features as F
from momentum import scanner, store
from momentum.config import load_config
from momentum.scanner import Candidate
from momentum.signals import Signal
from momentum.strategies import DetectContext, ALL_DETECTORS

ET = ZoneInfo("America/New_York")


def _mk_session(closes, opens=None, highs=None, lows=None, vols=None) -> pd.DataFrame:
    n = len(closes)
    opens = opens or [closes[max(0, i - 1)] for i in range(n)]
    highs = highs or [max(o, c) + 0.02 for o, c in zip(opens, closes)]
    lows = lows or [min(o, c) - 0.02 for o, c in zip(opens, closes)]
    vols = vols or [100000] * n
    start = datetime.now(ET).replace(hour=9, minute=30, second=0, microsecond=0)
    idx = [start + timedelta(minutes=5 * i) for i in range(n)]
    return pd.DataFrame(
        {"Open": opens, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
        index=pd.DatetimeIndex(idx),
    )


def _ctx(cfg, session, price) -> DetectContext:
    c = Candidate(symbol="TEST", exchange="NASDAQ", price=price, change_from_open_pct=25.0,
                  rvol=40.0, float_shares=8_000_000, volume=5e6, avg_volume_10d=1e5,
                  premarket_pct=5.0, passed_screener=True)
    return DetectContext(cfg=cfg, candidate=c, price=price, session_5m=session,
                         session_1m=None, daily=None, hod=F.session_hod(session))


def check_detectors(cfg) -> list[str]:
    out = []

    # A: run-up to just under 5.00, then break it
    s = _mk_session([4.55, 4.70, 4.82, 4.90, 4.97])
    ctx = _ctx(cfg, s, price=5.04)
    sig = ALL_DETECTORS["A"].evaluate(ctx)
    out.append(f"A WholeHalfDollarBreak: {'FIRED entry=%.2f' % sig.entry_ref if sig else 'no signal'}")

    # B: 4 green impulse, 2 red flat-top flag, green break candle
    s = _mk_session(
        closes=[3.10, 3.30, 3.55, 3.80, 3.72, 3.70, 3.95],
        opens= [3.00, 3.12, 3.32, 3.56, 3.79, 3.71, 3.71],
        highs= [3.12, 3.32, 3.57, 3.83, 3.84, 3.83, 3.99],
        lows=  [2.98, 3.10, 3.30, 3.54, 3.68, 3.66, 3.70],
    )
    ctx = _ctx(cfg, s, price=3.97)
    sig = ALL_DETECTORS["B"].evaluate(ctx)
    out.append(f"B BullFlagFlatTop: {'FIRED entry=%.2f' % sig.entry_ref if sig else 'no signal'}")

    # C: impulse to 3.90, then a long flat drift at ~3.72 (below the impulse
    # high) that taps a rising 9ema, then a green new-high candle
    s = _mk_session(
        closes=[3.05, 3.35, 3.62, 3.86, 3.74, 3.71, 3.73, 3.70, 3.72, 3.90],
        opens= [3.00, 3.06, 3.36, 3.63, 3.85, 3.72, 3.70, 3.72, 3.69, 3.72],
        highs= [3.08, 3.38, 3.65, 3.90, 3.88, 3.75, 3.76, 3.74, 3.75, 3.93],
        lows=  [2.98, 3.04, 3.34, 3.60, 3.70, 3.66, 3.67, 3.66, 3.67, 3.70],
    )
    ctx = _ctx(cfg, s, price=3.92)
    sig = ALL_DETECTORS["C"].evaluate(ctx)
    out.append(f"C MAPullback9ema: {'FIRED entry=%.2f' % sig.entry_ref if sig else 'no signal'}")

    # D: swing-low(1) ~2.72 @ idx4, pivot swing-high(2) ~3.32 @ idx6,
    # higher swing-low(3) ~2.90 @ idx9, then break the pivot(4)
    s = _mk_session(
        closes=[3.06, 3.10, 3.05, 2.88, 2.80, 3.12, 3.28, 3.14, 3.02, 2.98, 3.18, 3.32, 3.42],
        opens= [3.02, 3.06, 3.10, 3.04, 2.87, 2.96, 3.13, 3.29, 3.13, 3.03, 3.06, 3.19, 3.30],
        highs= [3.10, 3.12, 3.14, 3.05, 2.92, 3.18, 3.32, 3.30, 3.12, 3.05, 3.22, 3.35, 3.45],
        lows=  [3.00, 3.05, 3.02, 2.85, 2.72, 2.95, 3.15, 3.10, 2.98, 2.90, 3.05, 3.20, 3.28],
    )
    ctx = _ctx(cfg, s, price=3.35)
    sig = ALL_DETECTORS["D"].evaluate(ctx)
    out.append(f"D Setup1234: {'FIRED entry=%.2f' % sig.entry_ref if sig else 'no signal'}")

    # negatives: a flat nothing session should fire nothing
    flat = _mk_session([3.00] * 10)
    fired = [k for k, d in ALL_DETECTORS.items() if d.evaluate(_ctx(cfg, flat, 3.00))]
    out.append(f"flat session fires: {fired or 'none (good)'}")
    return out


def check_finalize(cfg) -> str:
    sig = Signal(strategy="A", symbol="TEST", price=5.05, entry_ref=5.00)
    sig.finalize(cfg, equity_usd=12000.0)
    return (f"finalize: stop={sig.stop_price} max={sig.max_loss_price} "
            f"scale1={sig.first_scale_price} scale2={sig.second_scale_price} "
            f"shares_hint={sig.shares_hint}")


def main():
    live = "--live" in sys.argv
    cfg = load_config()
    print("config keys:", sorted(cfg.keys()))
    print(check_finalize(cfg))
    for line in check_detectors(cfg):
        print(" ", line)

    if live:
        store.init_momentum_db()
        cands = scanner.scan(cfg)
        passed = [c for c in cands if c.passed_screener]
        print(f"\nlive scan: {len(cands)} rows, {len(passed)} passed screener")
        for c in sorted(cands, key=lambda x: -x.change_from_open_pct)[:12]:
            print(f"  {c.symbol:6} {c.exchange:5} ${c.price:6.2f} +{c.change_from_open_pct:6.1f}% "
                  f"rvol={c.rvol:8.1f} float={c.float_shares/1e6:6.1f}M pass={c.passed_screener} "
                  f"{c.reject_reason or ''}")


if __name__ == "__main__":
    main()

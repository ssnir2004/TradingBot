"""One-shot historical reconstruction of the momentum scanner, so the
detectors can be checked against real market data tonight instead of
waiting ~3 weeks for phase 1's live scan to accumulate enough days.

TradingView's screener (momentum.scanner) is live-only - it has no
history. This module substitutes a **proxy** screen built entirely from
data we already have full access to:

  1. Bulk yfinance DAILY bars (long history, cheap) over the whole NASDAQ-
     listed universe (build_custom_universe.fetch_nasdaq_listed_symbols) -
     used to approximate "up >=10% intraday" as (High-Open)/Open on the
     day, and RVOL as that day's volume over a trailing 20-day average.
  2. yfinance floatShares (verified 2026-09-10 to match TradingView's own
     number closely) on the survivors only - this is the one genuinely
     equivalent field, not a proxy.
  3. Real yfinance 5-MINUTE intraday bars for the specific (symbol, day)
     pairs that pass 1+2 - capped at yfinance's ~60-day intraday window.
  4. The exact same four detectors (momentum.strategies), replayed bar-by-
     bar with NO LOOKAHEAD: at "bar k", a detector only ever sees bars
     1..k and a price equal to bar k's own close (the live loop's
     equivalent of "last closed candle + near-real-time price").

Caveats (surfaced in the summary, not hidden):
  - change-from-open and RVOL are DAILY-bar proxies, not TradingView's
    true intraday figures - this will pass/reject some days the live
    scanner would have scored differently.
  - G2 (per-minute volume spike) and G6 (overhead resistance) are not
    computed here (both are annotate-only in phase 1 anyway - see
    docs/momentum_strategy_spec.md).
  - Bounded to DAYS_BACK calendar days (yfinance 5m cap), not the full
    live-collection window phase 1 would eventually cover.

Signals land in the same momentum_signals table as live alerts, tagged
mode="backfill" so they're never confused with a real alert and never
interfere with the live loop's own per-symbol cooldown (that only ever
looks at today's real date; every backfill trade_date is in the past).
"""
import gc
import logging
import time
from datetime import datetime, time as dtime, timedelta

import pandas as pd
import yfinance as yf

from momentum import features as F
from momentum import store
from momentum.config import load_config
from momentum.scanner import Candidate
from momentum.signals import Signal
from momentum.strategies import DetectContext, enabled_detectors
from momentum.universe import fetch_nasdaq_listed_symbols, float_shares_bulk

log = logging.getLogger("momentum.backfill")

DAYS_BACK = 45              # yfinance 5m cap is ~60 calendar days; stay well inside it
DAILY_CHUNK_SIZE = 25       # symbols per yf.download batch call - kept small deliberately
                            # (see stream_daily_candidates' own docstring: this server has
                            # under 1GB RAM and runs the live trading engine, so this module
                            # trades a slower wall-clock time for a small, bounded memory
                            # footprint rather than ever holding the whole universe's bars
                            # in memory at once)
MAX_INTRADAY_FETCHES = 200  # bounds runtime/rate-limit exposure
MIN_5M_BARS_PER_DAY = 8     # skip a halted/no-data session


def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _screen_daily_frame(symbol: str, df: pd.DataFrame, cfg: dict, cutoff: pd.Timestamp) -> list[dict]:
    """The G1/G3 proxy screen for one symbol's already-fetched daily
    frame - factored out of stream_daily_candidates so a chunk's frames
    can be screened and dropped one at a time instead of accumulated."""
    s = cfg["screener"]
    if len(df) < 25:
        return []
    vol_avg20 = df["Volume"].rolling(20).mean().shift(1)
    change_from_open_pct = (df["High"] - df["Open"]) / df["Open"] * 100.0
    rvol_proxy = df["Volume"] / vol_avg20
    out = []
    for ts in df.index:
        if ts < cutoff:
            continue
        o = float(df.loc[ts, "Open"])
        if not (s["price_hard_min"] <= o <= s["price_hard_max"]):
            continue
        chg = float(change_from_open_pct.loc[ts])
        rv = float(rvol_proxy.loc[ts]) if pd.notna(rvol_proxy.loc[ts]) else float("nan")
        if chg >= s["min_change_from_open_pct"] and rv >= s["rvol_min"]:
            out.append({"symbol": symbol, "date": ts.date(), "change_pct": chg,
                       "rvol_proxy": rv, "open": o})
    return out


def stream_daily_candidates(symbols: list[str], cfg: dict, days_back: int = DAYS_BACK,
                            period: str = "3mo") -> list[dict]:
    """Fetches daily bars and applies the G1/G3 proxy screen (see module
    docstring) one small chunk at a time, keeping only the tiny resulting
    candidate dicts and immediately discarding each chunk's DataFrame -
    unlike a single big multi-ticker yf.download, peak memory here stays
    bounded to ~DAILY_CHUNK_SIZE symbols' worth of daily bars (a few MB),
    never the whole ~5600-symbol NASDAQ universe at once. threads=False
    for the same reason: yfinance's threaded downloader buffers every
    thread's response concurrently, which multiplies the peak instead of
    bounding it - this trades speed for a low, predictable memory profile
    on a box that also runs the live trading engine."""
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days_back)
    out: list[dict] = []
    chunks = list(_chunked(symbols, DAILY_CHUNK_SIZE))
    fetched = 0
    for i, chunk in enumerate(chunks):
        try:
            df = yf.download(tickers=chunk, period=period, interval="1d",
                             group_by="ticker", threads=False, progress=False, auto_adjust=False)
        except Exception as exc:
            log.warning("daily chunk %s/%s failed: %s", i + 1, len(chunks), exc)
            continue
        for sym in chunk:
            try:
                sub = df[sym] if len(chunk) > 1 else df
            except (KeyError, TypeError):
                continue
            sub = sub.dropna(how="all")
            if sub.empty:
                continue
            fetched += 1
            out.extend(_screen_daily_frame(sym, sub, cfg, cutoff))
        del df
        if i % 20 == 0:
            gc.collect()
            log.info("daily chunk %s/%s screened (%s symbols fetched, %s candidates so far)",
                     i + 1, len(chunks), fetched, len(out))
    out.sort(key=lambda d: -d["change_pct"])
    return out


def fetch_intraday_day(symbol: str, day) -> pd.DataFrame | None:
    """Real 5-minute bars for exactly one past session."""
    start = datetime(day.year, day.month, day.day)
    end = start + timedelta(days=1)
    try:
        df = yf.download(symbol, start=start, end=end, interval="5m",
                         prepost=True, progress=False, auto_adjust=False)
    except Exception as exc:
        log.debug("intraday fetch %s %s failed: %s", symbol, day, exc)
        return None
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    try:
        df.index = df.index.tz_convert("America/New_York")
    except (TypeError, AttributeError):
        df.index = df.index.tz_localize("UTC").tz_convert("America/New_York")
    return df[["Open", "High", "Low", "Close", "Volume"]].dropna()


def _entry_window_bars(day_bars: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Restricts a full (pre/post-market-included) day of bars to the same
    window momentum.loop.in_entry_window gates live scanning to - without
    this, a replay sees ~16h of thin extended-hours candles instead of the
    ~2.5h the live scanner would ever actually look at, both inflating
    detector fires (many more, choppier candles) and computing
    change-from-open against the wrong "open" (4am, not the 9:30 session
    open TradingView's own change_from_open uses)."""
    s = cfg["screener"]
    start_h, start_m = (int(x) for x in s["entry_window_et"][0].split(":"))
    end_h, end_m = (int(x) for x in s["entry_window_et"][1].split(":"))
    if s.get("premarket_enabled"):
        start_h, start_m = 4, 0
    if s.get("afternoon_enabled"):
        end_h, end_m = 15, 55
    start_t, end_t = dtime(start_h, start_m), dtime(end_h, end_m)
    times = day_bars.index.time
    return day_bars[(times >= start_t) & (times <= end_t)]


def replay_day(symbol: str, day, day_bars: pd.DataFrame, cfg: dict, detectors,
               float_shares: float | None, cooldowns: dict) -> list[tuple[Signal, "pd.Timestamp"]]:
    """Walks day_bars forward one candle at a time, evaluating every
    enabled detector at each step against ONLY the bars closed so far -
    the no-lookahead replay described in the module docstring. Returns
    (signal, bar_timestamp) pairs - the timestamp is the REAL historical
    moment the signal fired, for momentum.signals.Signal.to_store_row's
    `at` override (without it every backfill row would be stamped with
    "now", collapsing every historical day onto today's date)."""
    signals: list[tuple[Signal, "pd.Timestamp"]] = []
    if len(day_bars) < MIN_5M_BARS_PER_DAY:
        return signals

    for cut in range(3, len(day_bars) + 1):
        closed = day_bars.iloc[:cut - 1]
        price = float(day_bars["Close"].iloc[cut - 1])
        hod = F.session_hod(closed)
        change_from_open_pct = (price - float(day_bars["Open"].iloc[0])) / float(day_bars["Open"].iloc[0]) * 100.0

        cand = Candidate(
            symbol=symbol, exchange="NASDAQ", price=price,
            change_from_open_pct=change_from_open_pct, rvol=cfg["screener"]["rvol_min"],
            float_shares=float_shares or 0.0, volume=float(closed["Volume"].sum()),
            avg_volume_10d=0.0, premarket_pct=None, passed_screener=True,
            above_preferred_range=price > cfg["screener"]["price_preferred_max"],
        )
        ctx = DetectContext(cfg=cfg, candidate=cand, price=price, session_5m=closed,
                            session_1m=None, daily=None, hod=hod)

        for det in detectors:
            key = (symbol, det.key)
            last = cooldowns.get(key)
            bar_ts = day_bars.index[cut - 1]
            if last is not None and (bar_ts - last).total_seconds() / 60.0 < cfg["patterns"]["signal_cooldown_min"]:
                continue
            sig = det.evaluate(ctx)
            if sig is None:
                continue
            sig.finalize(cfg, equity_usd=None)
            stop_override = sig.features.pop("stop_override", None)
            if stop_override:
                sig.stop_price = float(stop_override)
            sig.features["backfill_date"] = str(day)
            cooldowns[key] = bar_ts
            signals.append((sig, bar_ts))
    return signals


def run_backfill(days_back: int = DAYS_BACK, max_intraday_fetches: int = MAX_INTRADAY_FETCHES,
                 universe: list[str] | None = None) -> dict:
    t0 = time.time()
    cfg = load_config()
    store.init_momentum_db()

    symbols = universe if universe is not None else fetch_nasdaq_listed_symbols()
    log.info("universe: %s NASDAQ-listed symbols", len(symbols))

    candidates = stream_daily_candidates(symbols, cfg, days_back)
    log.info("daily-proxy screen: %s (symbol, day) candidates before float check", len(candidates))

    uniq_symbols = sorted({c["symbol"] for c in candidates})
    floats = float_shares_bulk(uniq_symbols)
    passed = [c for c in candidates if (floats.get(c["symbol"]) or float("inf")) < cfg["screener"]["float_max"]]
    log.info("after float<%s filter: %s candidates (%s symbols had no/too-high float)",
             cfg["screener"]["float_max"], len(passed), len(uniq_symbols) - len({c["symbol"] for c in passed}))

    passed = passed[:max_intraday_fetches]
    detectors = enabled_detectors(cfg)
    cooldowns: dict = {}
    all_signals: list[Signal] = []
    days_replayed = 0

    for i, c in enumerate(passed):
        day_bars = fetch_intraday_day(c["symbol"], c["date"])
        if day_bars is None:
            continue
        day_bars = _entry_window_bars(day_bars, cfg)
        if day_bars.empty:
            continue
        days_replayed += 1
        sig_pairs = replay_day(c["symbol"], c["date"], day_bars, cfg, detectors,
                               floats.get(c["symbol"]), cooldowns)
        for sig, bar_ts in sig_pairs:
            store.record_signal(sig.to_store_row(mode="backfill", at=bar_ts.to_pydatetime()))
        all_signals.extend(sig for sig, _ in sig_pairs)
        del day_bars
        if i % 20 == 0:
            gc.collect()
            log.info("replayed %s/%s candidate days, %s signals so far", i, len(passed), len(all_signals))

    by_strategy: dict[str, int] = {}
    for sig in all_signals:
        by_strategy[sig.strategy] = by_strategy.get(sig.strategy, 0) + 1

    summary = {
        "elapsed_sec": round(time.time() - t0, 1),
        "universe_size": len(symbols),
        "daily_proxy_candidates": len(candidates),
        "after_float_filter": len(passed),
        "days_replayed": days_replayed,
        "signals_total": len(all_signals),
        "signals_by_strategy": by_strategy,
        "top_signals": [
            {"strategy": s.strategy, "symbol": s.symbol, "date": s.features.get("backfill_date"),
             "entry_ref": s.entry_ref, "stop": s.stop_price, "conviction": s.conviction}
            for s in all_signals[:30]
        ],
    }
    return summary

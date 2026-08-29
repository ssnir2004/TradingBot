"""Shared "run these strategies, build their results dict" logic used by
both run_backtest.py (spawned as a local subprocess by web/app.py) and
backtest_worker.py (a remote worker polling the dashboard over HTTP,
see docs/worker.md) - kept in one place so the two callers can never
quietly drift apart, the same reason cycle._evaluate_filters_from_bars is
shared between the live bot and backtest_engine.py rather than each
having its own copy.
"""
from datetime import date

import pandas as pd

from src import backtest_data, backtest_engine, es_filter, perf, trade_diagnostics


def _load_es_intraday(start_date: date, end_date: date) -> pd.DataFrame | None:
    """Same cache-read-and-window pattern every simulate_* function
    already applies to a regular symbol's own intraday bars (see
    backtest_engine.simulate_strategy's own window_start/window_end) -
    only called when a strategy's rules actually carry "es_vwap_filter"
    (see run_one_strategy below), so a backtest with no ES-gated
    strategies never even touches the ES cache. None if ES has no cached
    bars at all yet (see fetch_es_backtest_data.py) or none fall in this
    date range - _es_filter_pass already treats that as "not evaluable",
    same as any other missing-data case, not an error."""
    es_bars = backtest_data.load_cached_bars(es_filter.ES_SYMBOL, es_filter.ES_BAR_SIZE)
    if es_bars is None or es_bars.empty:
        return None
    window_start = pd.Timestamp(start_date, tz=es_bars.index.tz) - pd.Timedelta(days=backtest_engine.INTRADAY_LOOKBACK_DAYS)
    window_end = pd.Timestamp(end_date, tz=es_bars.index.tz) + pd.Timedelta(days=1)
    windowed = es_bars[(es_bars.index >= window_start) & (es_bars.index < window_end)]
    return windowed if not windowed.empty else None


def run_one_strategy(
    strategy_name: str, direction: str, rules: dict, symbols: list[str],
    start_date: date, end_date: date,
    portfolio_value: float, max_risk_pct: float, max_trades_per_day: int,
    commission_per_trade: float,
) -> dict:
    # ORB strategies (see src/orb.py) carry an "opening_range" key, Touch &
    # Turn strategies (see src/touch_turn.py) an "opening_candle" key -
    # both replayed by their own dedicated simulator (genuinely different
    # engines - no D1-D3 daily bias, fixed-target exits instead of
    # breakeven/trailing, and for Touch & Turn a resting-limit-order fill
    # model instead of an instant-on-signal one), not just a different
    # rules_json for the same one.
    if "opening_range" in rules:
        simulate = backtest_engine.simulate_orb_strategy
    elif "opening_candle" in rules:
        simulate = backtest_engine.simulate_touch_turn_strategy
    else:
        simulate = backtest_engine.simulate_strategy
    es_intraday = _load_es_intraday(start_date, end_date) if rules.get("es_vwap_filter") else None
    sim = simulate(
        rules, direction, symbols, start_date, end_date,
        portfolio_value, max_risk_pct, max_trades_per_day,
        commission_per_trade=commission_per_trade, es_intraday=es_intraday,
    )
    pairs = perf.pair_trades(sim["trades"])
    aggregate = perf.aggregate(pairs)
    # full_report enriches each pair with mfe_usd/mfe_r/mae_usd/mae_r/
    # final_r/capture_pct (see src/trade_diagnostics.py) - stored as THIS
    # backtest's own "pairs" going forward, so the per-trade table and PDF
    # export get those columns for free with no separate lookup. A pair
    # missing mfe_price/mae_price (predates backtest_engine.py tracking
    # excursion at all) gets None for all of them rather than a fabricated
    # number - see trade_diagnostics.enrich's own docstring.
    report = trade_diagnostics.full_report(pairs)
    return {
        "strategy_name": strategy_name,
        "direction": direction,
        "pairs": report["pairs"],
        "aggregate": aggregate,
        # Same {"label", "count", "is_loss"} shape every existing reader
        # already expects, now with a "pct" alongside each bucket's count.
        "histogram": report["r_distribution"],
        "diagnostics": {
            "summary": report["summary"],
            "exit_quality": report["exit_quality"],
            "entry_vs_exit": report["entry_vs_exit"],
        },
        # None unless this strategy's rules actually carried
        # "es_vwap_filter" AND ES's own cached bars were available for
        # this date range - see trade_diagnostics.es_filter_report.
        "es_filter": report["es_filter"],
        "skipped_symbols": sim["skipped_symbols"],
        "filter_stats": sim["filter_stats"],
    }


def run_backtest_params(params: dict, strategies: dict) -> dict:
    """strategies is {str(strategy_id): {"name": ..., "direction": ...,
    "rules": {...}}} - already resolved by the caller (run_backtest.py
    reads them from the strategies table directly; backtest_worker.py
    gets them pre-resolved in the claim response, since a remote worker
    has no direct DB access of its own). Returns the same
    {strategy_id_str: {...} | {"error": ...}} shape db.finish_backtest
    expects for results_json."""
    start_date = date.fromisoformat(params["start_date"])
    end_date = date.fromisoformat(params["end_date"])
    symbols = params["symbols"]
    results = {}
    for strategy_id in params["strategy_ids"]:
        key = str(strategy_id)
        strategy = strategies.get(key)
        if not strategy:
            results[key] = {"error": "Strategy not found"}
            continue
        results[key] = run_one_strategy(
            strategy["name"], strategy["direction"], strategy["rules"], symbols,
            start_date, end_date,
            params["portfolio_value"], params["max_risk_pct"], params["max_trades_per_day"],
            params.get("commission_per_trade", 0.0),
        )
    return results

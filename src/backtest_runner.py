"""Shared "run these strategies, build their results dict" logic used by
both run_backtest.py (spawned as a local subprocess by web/app.py) and
backtest_worker.py (a remote worker polling the dashboard over HTTP,
see docs/worker.md) - kept in one place so the two callers can never
quietly drift apart, the same reason cycle._evaluate_filters_from_bars is
shared between the live bot and backtest_engine.py rather than each
having its own copy.
"""
from datetime import date

from src import backtest_engine, perf


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
    sim = simulate(
        rules, direction, symbols, start_date, end_date,
        portfolio_value, max_risk_pct, max_trades_per_day,
        commission_per_trade=commission_per_trade,
    )
    pairs = perf.pair_trades(sim["trades"])
    aggregate = perf.aggregate(pairs)
    r_values = perf.compute_r_multiples(pairs)
    histogram = [{"label": l, "count": c, "is_loss": loss} for l, c, loss in perf.histogram(r_values)]
    return {
        "strategy_name": strategy_name,
        "direction": direction,
        "pairs": pairs,
        "aggregate": aggregate,
        "histogram": histogram,
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

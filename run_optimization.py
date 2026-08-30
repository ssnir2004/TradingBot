"""Runs one ORB Long v4.3 Parameter Lab optimization sweep (see
web/app.py's POST /api/optimizations, web/templates/optimization.html) as
an isolated subprocess, spawned by the dashboard - same "a memory-heavy
run must never risk taking the dashboard itself down" reasoning as
run_backtest.py, which this deliberately parallels file-for-file rather
than reusing (see the Critical Architecture Rule this feature was built
under: never touch the existing Backtest screen's own execution path).

Every parameter combination in the sweep is run SEQUENTIALLY inside this
ONE subprocess, not one subprocess per combination - a wide sweep (many
hard_stop_R x trailing_activation_R values) would otherwise repeat the
exact concurrent-subprocess memory pressure that motivated chunking the
New Backtest page's own multi-day runs down to 5-day groups. Each combo
still gets a fully independent, correct backtest_runner.run_one_strategy
call (real chronological simulation, entry to exit, same as any other
strategy) - "sequential" here only means the OS-process boundary, not
anything about how one combo's trades are processed.

hard_stop_R/trailing_trigger_R are already fully runtime-configurable via
rules_json (that's literally how v4.1 and v4.2 differ - see backtest_
engine.py's "no_stop_delayed_trail" management_style and its opt-in
exit_cfg["hard_stop_R"]), so this script needs ZERO backtest_engine.py
changes - it just deep-copies the v4.3 base strategy's own rules_json
once per combination and overrides those two exit_cfg values before
calling run_one_strategy, exactly like a human hand-editing rules.json
between runs would.
"""
import argparse
import copy
import json
from datetime import date
from pathlib import Path

from analyze_v5_ablation import _stats
from src import backtest_runner, db

PROJECT_DIR = Path(__file__).resolve().parent

# Maps the dashboard's own objective dropdown values to _stats()'s keys -
# every one of these is a "higher is better" metric, so max() with no
# extra direction handling picks the right winner for all of them.
OBJECTIVE_KEYS = {
    "net_pnl": "net_pnl_usd",
    "profit_factor": "profit_factor",
    "expectancy": "expectancy_usd",
    "avg_r": "avg_r",
    "win_rate": "win_rate_pct",
}


def _objective_value(stats: dict, objective: str) -> float:
    """profit_factor can be the string "inf" (no losing trades) or "n/a"
    (no trades at all) rather than a float - see src/perf.py's own
    aggregate(). Mapped to real +inf/-inf so max() across combos still
    picks correctly without special-casing the comparison itself; every
    other objective key is already a plain float or None (no trades),
    which -inf covers the same way."""
    value = stats.get(OBJECTIVE_KEYS[objective])
    if value == "inf":
        return float("inf")
    if value is None or value == "n/a":
        return float("-inf")
    return float(value)


def run_optimization(optimization_id: int):
    record = db.get_optimization(optimization_id)
    if record is None:
        print(f"optimization {optimization_id}: not found")
        return
    params = record["params"]
    db.start_optimization(optimization_id)
    try:
        base_strategy = db.get_strategy(params["base_strategy_id"])
        if base_strategy is None:
            raise RuntimeError(f"base strategy {params['base_strategy_id']} not found")
        base_rules = json.loads(base_strategy["rules_json"])
        direction = base_strategy["direction"]
        symbols = params["symbols"]
        start_date = date.fromisoformat(params["start_date"])
        end_date = date.fromisoformat(params["end_date"])
        objective = params.get("objective", "net_pnl")

        combos = []
        for hard_stop_r in params["hard_stop_values"]:
            for trailing_activation_r in params["trailing_activation_values"]:
                rules = copy.deepcopy(base_rules)
                rules["exit"]["hard_stop_R"] = hard_stop_r
                rules["exit"]["trailing_trigger_R"] = trailing_activation_r
                label = f"ORB Long v4.3 (hard_stop={hard_stop_r}R, trailing={trailing_activation_r}R)"
                result = backtest_runner.run_one_strategy(
                    label, direction, rules, symbols, start_date, end_date,
                    params["portfolio_value"], params["max_risk_pct"],
                    params["max_trades_per_day"], params["commission_per_trade"],
                )
                combos.append({
                    "hard_stop_r": hard_stop_r,
                    "trailing_activation_r": trailing_activation_r,
                    "stats": _stats(result["pairs"]),
                })
                print(f"optimization {optimization_id}: {label} -> "
                      f"{combos[-1]['stats']['total_trades']} trade(s)")

        best = max(combos, key=lambda c: _objective_value(c["stats"], objective)) if combos else None
        db.finish_optimization(optimization_id, {"combos": combos, "best": best, "objective": objective})
        print(f"optimization {optimization_id}: done ({len(combos)} combination(s))")
    except Exception as exc:  # noqa: BLE001 - a bad run must record failure, not crash silently
        db.fail_optimization(optimization_id, f"{type(exc).__name__}: {exc}")
        print(f"optimization {optimization_id}: failed - {type(exc).__name__}: {exc}")


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--optimization-id", type=int, required=True)
    args = parser.parse_args()
    run_optimization(args.optimization_id)


if __name__ == "__main__":
    main()

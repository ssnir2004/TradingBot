"""Runs one ORB Long v4.3 Parameter Lab optimization sweep (see
web/app.py's POST /api/optimizations, web/templates/optimization.html).

Two ways a sweep's combinations actually get computed, matching the New
Backtest page's own local/remote split:

  - LOCAL (main()/run_optimization() below): spawned as its own isolated
    subprocess by the dashboard - same "a memory-heavy run must never
    risk taking the dashboard itself down" reasoning as run_backtest.py.
    Every combination runs SEQUENTIALLY inside this ONE subprocess, not
    one subprocess per combination - a wide sweep would otherwise repeat
    the exact concurrent-subprocess memory pressure that motivated
    chunking the New Backtest page's own multi-day runs. Each combo
    still gets a fully independent, correct backtest_runner.run_one_
    strategy call (real chronological simulation, entry to exit) -
    "sequential" here only means the OS-process boundary.

  - REMOTE (aggregate_from_children() below, called periodically by
    web/app.py's _aggregate_optimizations_loop - no subprocess of this
    script runs at all in this path): each combination is dispatched as
    its own ordinary REMOTE backtest row (see db.create_backtest's own
    optimization_id param), reusing the EXISTING worker claim/result
    protocol (backtest_worker.py, docs/worker.md) completely unchanged -
    a worker has no idea it's running an Optimization Lab combination
    rather than a normal backtest. Once every one of a sweep's child
    backtests reaches a terminal state, aggregate_from_children pulls
    their results back together into the same combos/best shape the
    local path produces.

Either way, hard_stop_R/trailing_trigger_R are already fully runtime-
configurable via rules_json (that's literally how v4.1 and v4.2 differ -
see backtest_engine.py's "no_stop_delayed_trail" management_style and its
opt-in exit_cfg["hard_stop_R"]), so NEITHER path needed any backtest_
engine.py changes - a combination is just the v4.3 base strategy's own
rules_json with those two exit_cfg values overridden (locally via a
plain deep-copy; remotely via src/backtest_runner.run_backtest_params's
own new "rules_override" param, applied identically for a local or
remote ordinary backtest too - unused, so unaffected, unless something
sets it), exactly like a human hand-editing rules.json between runs
would produce.
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


def _finalize(optimization_id: int, combos: list[dict], objective: str, failed_note: str | None = None):
    """Shared by both the local and remote paths: picks "best" and writes
    the terminal result - db.finish_optimization when at least one combo
    actually produced stats, db.fail_optimization when none did (e.g.
    every remote combination itself failed - `failed_note` names why)."""
    if not combos:
        db.fail_optimization(optimization_id, failed_note or "No combinations produced a result")
        print(f"optimization {optimization_id}: failed - no combinations produced a result")
        return
    best = max(combos, key=lambda c: _objective_value(c["stats"], objective))
    db.finish_optimization(optimization_id, {"combos": combos, "best": best, "objective": objective})
    print(f"optimization {optimization_id}: done ({len(combos)} combination(s))")


def aggregate_from_children(optimization_id: int) -> bool:
    """Called periodically by web/app.py's _aggregate_optimizations_loop
    for a REMOTE-mode sweep (see this module's own docstring) - checks
    whether every child backtest db.create_backtest tagged with this
    optimization_id has reached a terminal state ('done'/'failed'), and
    if so, pulls their results together into the same combos/best shape
    run_optimization() below produces for a local sweep, then finishes
    (or fails) the optimization row itself.

    Returns True if this call actually finalized the optimization (so
    the caller's loop can stop polling it), False if it's still waiting
    on at least one child (nothing else happens in that case - the next
    periodic tick checks again)."""
    record = db.get_optimization(optimization_id)
    if record is None or record["status"] not in ("pending", "running"):
        return False
    children = db.list_optimization_child_backtests(optimization_id)
    if not children or any(c["status"] not in ("done", "failed") for c in children):
        return False  # still waiting on at least one combination (or none dispatched yet)

    base_strategy_key = str(record["params"]["base_strategy_id"])
    combos = []
    failed_count = 0
    for child in children:
        combo_params = child["params"]
        hard_stop_r = combo_params.get("hard_stop_r")
        trailing_activation_r = combo_params.get("trailing_activation_r")
        strategy_result = (child["results"] or {}).get(base_strategy_key) if child["status"] == "done" else None
        if not strategy_result or "pairs" not in strategy_result:
            failed_count += 1
            continue
        combos.append({
            "hard_stop_r": hard_stop_r,
            "trailing_activation_r": trailing_activation_r,
            "stats": _stats(strategy_result["pairs"]),
        })
    note = f"{failed_count} of {len(children)} combination(s) failed and none of the rest succeeded" if failed_count else None
    _finalize(optimization_id, combos, record["params"].get("objective", "net_pnl"), failed_note=note)
    return True


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

        _finalize(optimization_id, combos, objective)
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

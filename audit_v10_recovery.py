"""ORB Long V10 "Dynamic Recovery" audit across MANY already-finished
backtests, pooled into one combined report - the same need audit_v6_
risk_event.py's own --backtest-id pooling addresses for V8/V9: a real
deployment produces backtests as weekly chunks (one row per week), so a
full multi-month V10 audit needs many ids pooled together, and V4.2's
own comparison pairs don't have to come from the SAME backtest rows as
V10's (unlike the dashboard's own single-backtest "Dynamic Recovery
Report" card) - useful when you already have a batch of v4.2-only
backtests from earlier and a separate batch of V8+V9+V10 backtests, and
don't want to re-run everything together just to get one combined
comparison.

Read-only: never modifies the DB, never runs any simulation - only
reads what a remote worker (or the server itself) already computed and
stored (see src/db.get_backtest). Do NOT run the local V10 simulation
directly on the small production server - see docs/worker.md.

Usage:
    # Find candidate backtest ids first:
    python3 audit_v10_recovery.py --list

    # Pool v4.2's own pairs from one set of backtests, V10's own from another:
    python3 audit_v10_recovery.py --v42-backtest-id 870,871,...,903 --v10-backtest-id 982,983,...,986
"""
import argparse
import re
from pathlib import Path

from src import db
from src import v10_recovery_report as v10r

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = "v10_recovery_report.xlsx"

V42_PATTERN = r"v4\.2"
V10_PATTERN = r"\bv10\b"


def _find_strategy_id_in_results(results: dict, pattern: str) -> str | None:
    """strategy_id (str) of the first result in one multi-strategy
    backtest's own `results` dict whose strategy_name matches `pattern` -
    same whole-word, case-insensitive convention audit_v6_risk_event.py's
    own _find_strategy_id_in_results/backtest.html's own
    _findStrategyIdByPattern already use."""
    needle = re.compile(pattern, re.IGNORECASE)
    for sid, r in results.items():
        if isinstance(r, dict) and not r.get("error") and needle.search(r.get("strategy_name") or ""):
            return sid
    return None


def _pool_pairs(backtest_ids: list[int], pattern: str, label_hint: str) -> tuple[list[dict], str]:
    """Pools one strategy's own pairs (matched by name `pattern`) across
    multiple already-finished backtests, with the SAME exact (start_date,
    end_date) dedup analyze_strategy.pool_strategy_pairs/audit_v6_risk_
    event's own _load_pairs_from_backtests already use (newest created_at
    wins for a repeated range)."""
    latest_by_range: dict[tuple, dict] = {}
    label = None
    for bid in backtest_ids:
        backtest = db.get_backtest(bid)
        if backtest is None:
            raise SystemExit(f"No backtest with id {bid}.")
        if backtest["status"] != "done":
            raise SystemExit(f"Backtest {bid} is not done yet (status={backtest['status']}).")
        results = backtest["results"] or {}
        sid = _find_strategy_id_in_results(results, pattern)
        if sid is None:
            available = [r.get("strategy_name") for r in results.values() if isinstance(r, dict)]
            raise SystemExit(f"Backtest {bid} has no {label_hint} result. Strategies in this backtest: {available}")
        result = results[sid]
        label = label or result.get("strategy_name", label_hint)
        params = backtest["params"] or {}
        date_key = (params.get("start_date"), params.get("end_date"))
        existing = latest_by_range.get(date_key)
        if existing is None or backtest["created_at"] > existing["created_at"]:
            latest_by_range[date_key] = {"created_at": backtest["created_at"], "pairs": result.get("pairs") or []}

    pooled_pairs = []
    for date_key in sorted(latest_by_range):
        pooled_pairs.extend(latest_by_range[date_key]["pairs"])
    return pooled_pairs, (label or label_hint)


def list_candidates(account_id: int) -> None:
    """--list: every 'done' backtest carrying a v4.2 and/or V10 result,
    tagged with which, so you can build the two --...-backtest-id lists
    without guessing ids."""
    summaries = [b for b in db.list_backtests(account_id, limit=300) if b["status"] == "done"]
    if not summaries:
        print("No 'done' backtests found for this account.")
        return
    print(f"{'ID':>6}  {'Date range':<23}  {'Contains'}")
    found_any = False
    for summary in summaries:
        backtest = db.get_backtest(summary["id"])
        results = backtest["results"] or {}
        names = [r.get("strategy_name") for r in results.values() if isinstance(r, dict) and not r.get("error")]
        has_v42 = any(re.search(V42_PATTERN, n or "", re.IGNORECASE) for n in names)
        has_v10 = any(re.search(V10_PATTERN, n or "", re.IGNORECASE) for n in names)
        if not (has_v42 or has_v10):
            continue
        found_any = True
        tags = "+".join(t for t, present in (("V4.2", has_v42), ("V10", has_v10)) if present)
        params = backtest["params"] or {}
        date_range = f"{params.get('start_date')} to {params.get('end_date')}"
        print(f"{summary['id']:>6}  {date_range:<23}  {tags}")
    if not found_any:
        print("No 'done' backtest in this account's history carries a v4.2 or V10 result yet.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true",
                         help="List every 'done' backtest carrying a v4.2 and/or V10 result and exit.")
    parser.add_argument("--v42-backtest-id",
                         help="Comma-separated backtest ids whose own ORB Long v4.2 result to pool as the baseline.")
    parser.add_argument("--v10-backtest-id",
                         help="Comma-separated backtest ids whose own ORB Long V10 result to pool as the variant.")
    parser.add_argument("--account-id", type=int, default=None)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Path to write the .xlsx report to")
    args = parser.parse_args()

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    if args.list:
        list_candidates(account_id)
        return

    if not args.v42_backtest_id or not args.v10_backtest_id:
        raise SystemExit("Both --v42-backtest-id and --v10-backtest-id are required (each a comma-separated list of backtest ids) - or use --list to find candidates.")

    v42_ids = [int(x) for x in args.v42_backtest_id.split(",") if x.strip()]
    v10_ids = [int(x) for x in args.v10_backtest_id.split(",") if x.strip()]

    print(f"Pooling ORB Long v4.2 from {len(v42_ids)} backtest(s), V10 from {len(v10_ids)} backtest(s) (no simulation runs here)...")
    baseline_pairs, baseline_label = _pool_pairs(v42_ids, V42_PATTERN, "ORB Long v4.2")
    variant_pairs, variant_label = _pool_pairs(v10_ids, V10_PATTERN, "ORB Long V10")
    print(f"{baseline_label}: {len(baseline_pairs)} trade(s) pooled. {variant_label}: {len(variant_pairs)} trade(s) pooled.")

    report = v10r.build_v10_recovery_report_from_pairs(baseline_pairs, variant_pairs, baseline_label, variant_label)

    print(f"\nEntry Parity: {report['entry_parity']}")
    if not report["entry_parity"]["parity_ok"]:
        print("\nWARNING: Entry Parity FAILED - the exported report will not claim V10 outperformed/underperformed v4.2 "
              "until this is resolved (see the Entry Parity Check sheet for the exact mismatches).")
    print(f"\nV10 State Summary: {report['state_summary']}")
    print(f"\nReconciliation: {report['reconciliation']}")

    scope_label = f"{variant_label} vs {baseline_label} | pooled across {len(v42_ids)} + {len(v10_ids)} backtest(s)"
    xlsx_bytes = v10r.export_v10_recovery_report_xlsx(report, scope_label)
    with open(args.output, "wb") as f:
        f.write(xlsx_bytes)
    print(f"\nWritten: {args.output}")


if __name__ == "__main__":
    main()

"""Phase 2: replay every backfilled signal through real historical bars
using the Shared Exit Engine, and print a PnL report. See
momentum/backtest.py's own docstring for the simulation rules and
caveats. Not scheduled - run by hand after a momentum.backfill pass.

    python run_momentum_backtest.py
"""
import json
import logging
import sys

from momentum.backtest import run_backtest_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])


def _pct(x):
    return f"{x}%" if x is not None else "n/a"


def main():
    report = run_backtest_report()

    print("\n" + "=" * 70)
    print("MOMENTUM BACKTEST REPORT (phase 2 - simulated over backfilled signals)")
    print("=" * 70)

    o = report["overall"]
    print(f"\nOVERALL: {o.get('n', 0)} signals simulated"
          f" ({report['no_data_count']} skipped - no bar data)")
    if o.get("n"):
        print(f"  win rate      : {_pct(o['win_rate_pct'])} ({o['wins']}W / {o['losses']}L / {o['scratches']}scr)")
        print(f"  avg R         : {o['avg_r']}")
        print(f"  total R       : {o['total_r']}")
        print(f"  profit factor : {o['profit_factor']}")
        print(f"  nominal $ P&L : ${o['total_dollars_nominal']:,.2f}  (${12000:,.0f} account, 0.5%/trade risk)")
        print(f"  exit reasons  : {o['exit_reasons']}")

    print("\nBY STRATEGY")
    for strat, s in report["by_strategy"].items():
        if not s.get("n"):
            print(f"  {strat}: no data")
            continue
        print(f"  {strat}: n={s['n']:3d}  win={_pct(s['win_rate_pct']):>6}  "
              f"avgR={s['avg_r']:+.2f}  totalR={s['total_r']:+.1f}  "
              f"PF={s['profit_factor']}  ${s['total_dollars_nominal']:+,.0f}")

    print("\nWORST 10")
    for r in report["worst_10"]:
        print(f"  {r['strategy']} {r['symbol']:6} {r['trade_date']}  R={r['r_multiple']:+.2f}  {r['exit_reason']}")

    print("\nBEST 10")
    for r in report["best_10"]:
        print(f"  {r['strategy']} {r['symbol']:6} {r['trade_date']}  R={r['r_multiple']:+.2f}  {r['exit_reason']}")

    with open("momentum_backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("\nfull detail written to momentum_backtest_report.json")


if __name__ == "__main__":
    main()

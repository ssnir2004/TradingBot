"""Phase 2: replay every backfilled signal through real historical bars
using the Shared Exit Engine, and print a PnL report - gross (optimistic,
no execution costs) alongside net (realistic: slippage + the broker's own
commission schedule, both configurable in momentum.config). See
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

    print("\n" + "=" * 78)
    print("MOMENTUM BACKTEST REPORT (phase 2 - simulated over backfilled signals)")
    print("=" * 78)

    o = report["overall"]
    print(f"\nOVERALL: {o.get('n', 0)} signals simulated"
          f" ({report['no_data_count']} skipped - no bar data)")
    if o.get("n"):
        print(f"  win rate            : {_pct(o['win_rate_pct'])} "
              f"({o['wins']}W / {o['losses']}L / {o['scratches']}scr)  [net of costs]")
        print(f"  avg R   gross->net  : {o['avg_r_gross']:+.3f}  ->  {o['avg_r_net']:+.3f}")
        print(f"  total R gross->net  : {o['total_r_gross']:+.1f}  ->  {o['total_r_net']:+.1f}")
        print(f"  profit factor g->n  : {o['profit_factor_gross']}  ->  {o['profit_factor_net']}")
        print(f"  nominal $ gross->net: ${o['total_dollars_gross']:+,.2f}  ->  ${o['total_dollars_net']:+,.2f}"
              f"  (${12000:,.0f} account, 0.5%/trade risk)")
        print(f"  total commissions   : ${o['total_commission']:,.2f}")
        print(f"  exit reasons        : {o['exit_reasons']}")

    print("\nBY STRATEGY (net of slippage + commissions)")
    for strat, s in report["by_strategy"].items():
        if not s.get("n"):
            print(f"  {strat}: no data")
            continue
        print(f"  {strat}: n={s['n']:3d}  win={_pct(s['win_rate_pct']):>6}  "
              f"avgR(net)={s['avg_r_net']:+.2f}  totalR(net)={s['total_r_net']:+.1f}  "
              f"PF(net)={s['profit_factor_net']}  ${s['total_dollars_net']:+,.0f}  "
              f"(gross was ${s['total_dollars_gross']:+,.0f})")

    print("\nWORST 10 (by net R)")
    for r in report["worst_10"]:
        print(f"  {r['strategy']} {r['symbol']:6} {r['trade_date']}  "
              f"R(net)={r['r_multiple_net']:+.2f} (gross {r['r_multiple_gross']:+.2f})  {r['exit_reason']}")

    print("\nBEST 10 (by net R)")
    for r in report["best_10"]:
        print(f"  {r['strategy']} {r['symbol']:6} {r['trade_date']}  "
              f"R(net)={r['r_multiple_net']:+.2f} (gross {r['r_multiple_gross']:+.2f})  {r['exit_reason']}")

    with open("momentum_backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("\nfull detail written to momentum_backtest_report.json")


if __name__ == "__main__":
    main()

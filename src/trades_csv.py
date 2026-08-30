"""Renders a strategy's pooled backtest trade log as a downloadable .csv -
the direct feed for analyze_entry_metrics.py's own statistical analysis
(and for anyone who'd rather load the raw data into their own tool than
Excel). Same column set as trades_xlsx.py's own "Trades" sheet (existing
per-trade columns, then every src/entry_metrics.py field) - kept as a
parallel, independently-maintained exporter rather than sharing row-
building code with trades_xlsx.py, same convention as trades_pdf.py/
trades_xlsx.py already following their own separate paths.
"""
import csv
from io import StringIO

from src import entry_metrics, perf
from src.trades_pdf import EXIT_REASON_LABELS


def build_trades_csv(pairs: list[dict]) -> str:
    header = [
        "symbol", "side", "model", "entry_price", "stop", "risk_usd", "exit_price", "size",
        "mfe_usd", "mfe_r", "mae_usd", "mae_r", "pnl_usd", "final_r", "capture_pct", "commission_usd",
        "exit_reason", "profit_lock_activated", "profit_lock_activated_at_r",
        "trail_activated", "trail_activated_at_r", "entry_time_iso", "exit_time_iso",
    ] + entry_metrics.ENTRY_METRICS_KEYS

    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    for p in pairs:
        entry_time = p["sell_time"] if p["side"] == "short" else p["buy_time"]
        exit_time = p["buy_time"] if p["side"] == "short" else p["sell_time"]
        risk = perf.initial_risk_per_share(p)
        risk_usd = risk * p["size"] if risk is not None else None
        row = [
            p["symbol"], p["side"], p.get("model"),
            p["open_price"], p.get("initial_stop"), risk_usd, p["close_price"], p["size"],
            p.get("mfe_usd"), p.get("mfe_r"), p.get("mae_usd"), p.get("mae_r"), p["pnl_usd"],
            p.get("final_r"), p.get("capture_pct"), p.get("commission_usd"),
            EXIT_REASON_LABELS.get(p.get("exit_reason"), p.get("exit_reason") or ""),
            p.get("profit_lock_activated"), p.get("profit_lock_activated_at_r"),
            p.get("trail_activated"), p.get("trail_activated_at_r"),
            entry_time, exit_time,
        ] + [p.get(k) for k in entry_metrics.ENTRY_METRICS_KEYS]
        writer.writerow(["" if v is None else v for v in row])
    return buf.getvalue()

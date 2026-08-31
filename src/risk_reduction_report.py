"""ORB Long V8/V9 "Dynamic Risk Reduction" comparison report - a PASSIVE,
read-only report over an ALREADY-FINISHED multi-strategy backtest's own
results (see db.get_backtest), never touching any strategy/entry/exit/
backtest logic itself. Matches trades across a baseline strategy (ORB Long
v4.2) and one or more variant strategies (V8/V9) by (symbol, entry
timestamp) - valid because every variant's own rules_json is a byte-for-
byte copy of the baseline's entry logic/position sizing (see src.db's own
EXTRA_STRATEGY_PRESETS comment for V8/V9), so the SAME (symbol, entry
timestamp) pair always identifies the SAME trade across every strategy in
one multi-strategy run - only the exit/stop management can ever differ.

Same "never silently drop, always account for every trade" discipline as
src.telemetry_engine.py throughout - a variant trade with no baseline
match (or vice versa) is reported separately (unmatched_*), never
silently excluded from a count without saying so.
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from src import perf


def _round(value, digits=4):
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(f) or np.isinf(f):
        return None
    return round(f, digits)


def _pairs_by_key(pairs: list[dict]) -> dict[tuple, dict]:
    """(symbol, entry timestamp) -> pair - the open leg's own timestamp
    (buy_time for a long, matching this whole feature's long-only scope)
    is the SAME real bar timestamp across every strategy sharing the same
    entry logic, so this is an exact, not fuzzy, match key."""
    return {(p["symbol"], p["buy_time"]): p for p in pairs}


def _core_metrics(pairs: list[dict]) -> dict:
    """"Core Metrics" section - Total Trades/Winners/Losers/Win Rate/
    Profit Factor/Gross P&L/Net P&L (all from perf.aggregate, the same
    numbers the /backtest page's own results table already shows for
    this strategy) plus Average/Median Final R, Expectancy ($ per trade,
    net-of-commission), and Max Drawdown (perf.compute_max_drawdown)."""
    agg = perf.aggregate(pairs)
    final_rs = [p["final_r"] for p in pairs if p.get("final_r") is not None]
    expectancy = (agg["net_pnl_usd"] / agg["total_trades"]) if agg["total_trades"] else None
    return {
        **agg,
        "avg_final_r": _round(sum(final_rs) / len(final_rs), 3) if final_rs else None,
        "median_final_r": _round(float(np.median(final_rs)), 3) if final_rs else None,
        "expectancy_usd": _round(expectancy, 2),
        "max_drawdown_usd": perf.compute_max_drawdown(pairs),
    }


def _outcome_label(pair: dict) -> str:
    """Winner/Loser purely off final_r sign (None-safe) - used for "later
    became a winner/loser" in the Risk Event Metrics section, independent
    of exit_reason (a trailing_stop exit can still be a loser if MFE never
    reached breakeven before trailing kicked in, etc.)."""
    final_r = pair.get("final_r")
    if final_r is None:
        return "unknown"
    return "winner" if final_r > 0 else "loser"


def _risk_event_metrics(variant_pairs: list[dict]) -> dict:
    """"Risk Event Metrics" section, computed over ONE variant's own
    trades - every field the spec asks for, all derived from the risk_
    event_*/hard_stop_tightened fields src.backtest_engine.py already
    stamps onto each closed trade (see its own _dynamic_risk_reduction_
    check)."""
    total = len(variant_pairs)
    triggered = [p for p in variant_pairs if p.get("risk_event_triggered")]
    hit_adjusted_stop = [p for p in triggered if p.get("hard_stop_tightened") and p.get("exit_reason") == "hard_stop"]
    activated_trailing = [p for p in triggered if p.get("trail_activated")]
    became_winner = [p for p in triggered if _outcome_label(p) == "winner"]
    became_loser = [p for p in triggered if _outcome_label(p) == "loser"]
    return {
        "total_trades": total,
        "risk_events": len(triggered),
        "risk_event_pct": _round(len(triggered) / total * 100, 1) if total else None,
        "later_hit_adjusted_stop": len(hit_adjusted_stop),
        "later_activated_trailing": len(activated_trailing),
        "later_became_winner": len(became_winner),
        "later_became_loser": len(became_loser),
    }


def _hard_stop_impact(variant_pairs: list[dict], baseline_by_key: dict[tuple, dict]) -> dict:
    """"Hard Stop Impact" section - Trades Saved (the tightened stop fired
    AND the matched baseline trade's own Delta R was positive, i.e.
    exiting at the tightened level really was better than what the
    baseline actually did) vs Winners Lost (the tightened stop fired but
    the MATCHED BASELINE trade went on to become a real winner - the
    tightening cut off a recovery the baseline captured). Both require a
    matched baseline trade to judge "vs what" - a triggered/tightened
    trade with no baseline match is reported separately, never silently
    folded into either bucket."""
    saved, winners_lost, unmatched = [], [], []
    for p in variant_pairs:
        if not (p.get("risk_event_triggered") and p.get("hard_stop_tightened") and p.get("exit_reason") == "hard_stop"):
            continue
        key = (p["symbol"], p["buy_time"])
        baseline = baseline_by_key.get(key)
        if baseline is None or baseline.get("final_r") is None or p.get("final_r") is None:
            unmatched.append(p)
            continue
        delta_r = p["final_r"] - baseline["final_r"]
        if _outcome_label(baseline) == "winner":
            winners_lost.append({"symbol": p["symbol"], "entry_time": p["buy_time"], "delta_r": _round(delta_r, 3)})
        elif delta_r > 0:
            saved.append({"symbol": p["symbol"], "entry_time": p["buy_time"], "delta_r": _round(delta_r, 3)})
    return {
        "trades_saved": len(saved), "trades_saved_detail": saved,
        "winners_lost": len(winners_lost), "winners_lost_detail": winners_lost,
        "unmatched_or_incomplete": len(unmatched),
    }


def _delta_analysis(variant_pairs: list[dict], baseline_by_key: dict[tuple, dict]) -> dict:
    """"Delta Analysis" - Delta R = Version Result - V4.2 Result, for
    every trade this variant shares with the baseline (matched by symbol+
    entry timestamp). unmatched_trades counts variant trades with no
    baseline counterpart (should be 0 for V8/V9 vs v4.2, since they share
    identical entry logic - a non-zero count here is itself a useful
    diagnostic that something about the "same entries" assumption broke,
    e.g. mismatched date ranges/symbol universes between runs)."""
    deltas = []
    unmatched = 0
    for p in variant_pairs:
        if p.get("final_r") is None:
            continue
        key = (p["symbol"], p["buy_time"])
        baseline = baseline_by_key.get(key)
        if baseline is None or baseline.get("final_r") is None:
            unmatched += 1
            continue
        deltas.append(p["final_r"] - baseline["final_r"])
    if not deltas:
        return {"matched_trades": 0, "unmatched_trades": unmatched, "total_delta_r": None, "avg_delta_r": None, "median_delta_r": None}
    return {
        "matched_trades": len(deltas), "unmatched_trades": unmatched,
        "total_delta_r": _round(sum(deltas), 3),
        "avg_delta_r": _round(sum(deltas) / len(deltas), 3),
        "median_delta_r": _round(float(np.median(deltas)), 3),
    }


def build_risk_reduction_report(
    results_by_strategy: dict, strategy_labels: dict, baseline_id: str, variant_ids: list[str],
) -> dict:
    """The full comparison report. `results_by_strategy` is {strategy_id_
    str: {"pairs": [...], ...}} - a subset of one already-finished multi-
    strategy backtest's own results_json (see db.get_backtest). `baseline_
    id` should be the ORB Long v4.2 strategy_id (str), `variant_ids` the
    V8/V9 strategy_id(s) (str) - all as they appear as keys of `results_
    by_strategy`. `strategy_labels` maps every id (baseline + variants) to
    its display name, for the report's own headers/exports.

    Returns:
      {"baseline": {"strategy_id", "label", "core_metrics", "trade_count"},
       "variants": [{"strategy_id", "label", "core_metrics", "risk_event_metrics",
                     "hard_stop_impact", "delta_analysis", "risk_event_trades"}, ...],
       "winner": {...} | None  (only when exactly 2 variants are compared)}

    Every section handles a missing/empty strategy result gracefully
    (empty pairs list) rather than raising - a variant strategy that
    genuinely produced zero trades in this scope is a valid, reportable
    outcome, not an error."""
    baseline_result = results_by_strategy.get(baseline_id) or {}
    baseline_pairs = baseline_result.get("pairs") or []
    baseline_by_key = _pairs_by_key(baseline_pairs)

    baseline_section = {
        "strategy_id": baseline_id, "label": strategy_labels.get(baseline_id, f"Strategy #{baseline_id}"),
        "core_metrics": _core_metrics(baseline_pairs), "trade_count": len(baseline_pairs),
    }

    variants_section = []
    for vid in variant_ids:
        variant_result = results_by_strategy.get(vid) or {}
        variant_pairs = variant_result.get("pairs") or []
        rows = []
        for p in variant_pairs:
            if not p.get("risk_event_triggered"):
                continue
            key = (p["symbol"], p["buy_time"])
            row = {
                "symbol": p["symbol"], "entry_date": (p["buy_time"] or "")[:10], "entry_time": p["buy_time"],
                "10m_current_r": _round(p.get("risk_event_10m_current_r"), 3),
                "10m_rsi_delta": _round(p.get("risk_event_10m_rsi_delta"), 2),
                "10m_mfe_r": _round(p.get("risk_event_10m_mfe_r"), 3),
                "original_v42_result": _round((baseline_by_key.get(key) or {}).get("final_r"), 3),
            }
            for other_vid in variant_ids:
                other_pairs = results_by_strategy.get(other_vid, {}).get("pairs") or []
                other_by_key = _pairs_by_key(other_pairs)
                row[f"{strategy_labels.get(other_vid, other_vid)}_result"] = _round((other_by_key.get(key) or {}).get("final_r"), 3)
            rows.append(row)

        variants_section.append({
            "strategy_id": vid, "label": strategy_labels.get(vid, f"Strategy #{vid}"),
            "core_metrics": _core_metrics(variant_pairs), "trade_count": len(variant_pairs),
            "risk_event_metrics": _risk_event_metrics(variant_pairs),
            "hard_stop_impact": _hard_stop_impact(variant_pairs, baseline_by_key),
            "delta_analysis": _delta_analysis(variant_pairs, baseline_by_key),
            "risk_event_trades": rows,
        })

    winner = None
    if len(variants_section) == 2:
        a, b = variants_section
        am, bm = a["core_metrics"], b["core_metrics"]

        def _pf(m):
            pf = m.get("profit_factor")
            return pf if isinstance(pf, (int, float)) else (float("inf") if pf == "inf" else None)

        winner = {
            "better_net_pnl": a["label"] if (am["net_pnl_usd"] or 0) > (bm["net_pnl_usd"] or 0) else b["label"],
            "better_profit_factor": a["label"] if (_pf(am) or -1) > (_pf(bm) or -1) else b["label"],
            "better_drawdown": a["label"] if (am["max_drawdown_usd"] or 0) < (bm["max_drawdown_usd"] or 0) else b["label"],
            "better_expectancy": a["label"] if (am.get("expectancy_usd") or -1e18) > (bm.get("expectancy_usd") or -1e18) else b["label"],
        }

    return {"baseline": baseline_section, "variants": variants_section, "winner": winner}


def export_risk_reduction_report_xlsx(report: dict, scope_label: str) -> bytes:
    """Downloadable multi-sheet .xlsx - Summary (core metrics for
    baseline + every variant, side by side, plus the V8-vs-V9 Winner
    verdict when present), one "Risk Events" sheet per variant (risk_
    event_metrics + hard_stop_impact + delta_analysis as key/value rows),
    and one "Trades" sheet per variant (the full risk_event_trades table).
    Same styling convention as src.telemetry_engine's own export_rule_
    matrix_xlsx/export_rule_evaluation_xlsx."""
    from io import BytesIO

    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter

    header_fill = PatternFill(start_color="EEF1F5", end_color="EEF1F5", fill_type="solid")
    header_font = Font(bold=True)

    def autosize(ws, ncols):
        for col in range(1, ncols + 1):
            letter = get_column_letter(col)
            width = max((len(str(c.value)) for c in ws[letter] if c.value is not None), default=8)
            ws.column_dimensions[letter].width = min(max(width + 2, 8), 40)

    def write_table(ws, rows: list[dict], columns: list[str] | None = None):
        if not rows:
            ws.append(["No data"])
            return
        cols = columns or list(rows[0].keys())
        ws.append(cols)
        for cell in ws[ws.max_row]:
            cell.font, cell.fill = header_font, header_fill
        for row in rows:
            ws.append([row.get(c) for c in cols])
        ws.freeze_panes = "A2"
        autosize(ws, len(cols))

    wb = Workbook()
    summary_ws = wb.active
    summary_ws.title = "Summary"
    summary_ws.append(["ORB Long V8/V9 - Dynamic Risk Reduction Report"])
    summary_ws["A1"].font = Font(bold=True, size=14)
    summary_ws.append([f"Scope: {scope_label} | Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}"])
    summary_ws.append([])

    core_cols = [
        "total_trades", "wins", "losses", "win_rate_pct", "gross_pnl_usd", "net_pnl_usd",
        "avg_final_r", "median_final_r", "expectancy_usd", "profit_factor", "max_drawdown_usd",
    ]
    core_rows = [{"strategy": report["baseline"]["label"], **{c: report["baseline"]["core_metrics"].get(c) for c in core_cols}}]
    for v in report["variants"]:
        core_rows.append({"strategy": v["label"], **{c: v["core_metrics"].get(c) for c in core_cols}})
    write_table(summary_ws, core_rows, columns=["strategy"] + core_cols)
    summary_ws.append([])

    if report.get("winner"):
        summary_ws.append(["V8 vs V9 Winner"])
        summary_ws[summary_ws.max_row][0].font = Font(bold=True)
        for label, key in [
            ("Better Net P&L", "better_net_pnl"), ("Better Profit Factor", "better_profit_factor"),
            ("Better Max Drawdown", "better_drawdown"), ("Better Expectancy", "better_expectancy"),
        ]:
            summary_ws.append([label, report["winner"][key]])
    autosize(summary_ws, len(core_cols) + 1)

    for v in report["variants"]:
        re_ws = wb.create_sheet(f"{v['label'][:20]} Risk Events"[:31])
        rem, hsi, da = v["risk_event_metrics"], v["hard_stop_impact"], v["delta_analysis"]
        rows = [
            {"metric": "Total Trades", "value": rem["total_trades"]},
            {"metric": "Risk Events", "value": rem["risk_events"]},
            {"metric": "Risk Event %", "value": rem["risk_event_pct"]},
            {"metric": "Later Hit Adjusted Stop", "value": rem["later_hit_adjusted_stop"]},
            {"metric": "Later Activated Trailing", "value": rem["later_activated_trailing"]},
            {"metric": "Later Became Winner", "value": rem["later_became_winner"]},
            {"metric": "Later Became Loser", "value": rem["later_became_loser"]},
            {"metric": "Trades Saved", "value": hsi["trades_saved"]},
            {"metric": "Winners Lost", "value": hsi["winners_lost"]},
            {"metric": "Hard-Stop-Impact Unmatched/Incomplete", "value": hsi["unmatched_or_incomplete"]},
            {"metric": "Delta Analysis - Matched Trades", "value": da["matched_trades"]},
            {"metric": "Delta Analysis - Unmatched Trades", "value": da["unmatched_trades"]},
            {"metric": "Total Delta R", "value": da["total_delta_r"]},
            {"metric": "Average Delta R", "value": da["avg_delta_r"]},
            {"metric": "Median Delta R", "value": da["median_delta_r"]},
        ]
        write_table(re_ws, rows, columns=["metric", "value"])

        trades_ws = wb.create_sheet(f"{v['label'][:22]} Trades"[:31])
        write_table(trades_ws, v["risk_event_trades"])

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()

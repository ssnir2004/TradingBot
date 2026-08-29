"""Renders a strategy's pooled backtest trade log (see
perf.pooled_trades_for_strategy) as a downloadable PDF - a full,
trade-by-trade blotter for offline review, since the dashboard's own
Strategy Report card only ever shows a scrollable in-page table capped
to whatever a single browser session cares to render.

Timestamps in a pair (buy_time/sell_time) are already tz-aware ISO
strings in America/New_York (see fetch_backtest_data.py's tz_convert(ET)
on every bar it fetches) - formatted here by just slicing the wall-clock
part off the ISO string, no further tz conversion needed.
"""
from datetime import datetime
from io import BytesIO

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from src import perf

# Mirrors backtest.html's own EXIT_REASON_LABELS - "partial_profit" omitted,
# since the exit stage it named no longer exists (see cycle._breakeven_
# decision's docstring) and no current code path can produce it.
EXIT_REASON_LABELS = {
    "stop_loss": "Stop loss",
    "trailing_stop": "Trailing stop",
    "eod_close": "End of day",
    "target": "Fixed target (ORB)",
}


def _fmt_money(value) -> str:
    if value is None:
        return "-"
    return f"${value:,.2f}"


def _fmt_dt(iso: str | None) -> str:
    if not iso:
        return "-"
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return iso
    return dt.strftime("%Y-%m-%d %H:%M ET")


def _fmt_r(value) -> str:
    return f"{value:.2f}" if value is not None else "-"


def _fmt_pct(value) -> str:
    return f"{value:.1f}%" if value is not None else "-"


def build_trades_pdf(strategy_name: str, direction: str, backtests_included: int,
                      aggregate_stats: dict, pairs: list[dict], diagnostics: dict | None = None) -> bytes:
    """`pairs` must already be enriched (see trade_diagnostics.enrich_all)
    - mfe_usd/mfe_r/mae_usd/mae_r/final_r/capture_pct read straight off
    each pair here, "-" wherever a pair predates that field (see
    trade_diagnostics' own docstring on why some backtest pairs won't
    have it). `diagnostics` is trade_diagnostics.full_report(pairs)'s own
    {"summary", "entry_vs_exit"} - optional (None renders the PDF without
    that section, e.g. if every pair turned out non-diagnosable)."""
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=landscape(A4),
        title=f"{strategy_name} - Backtest Trade Log",
        leftMargin=12 * mm, rightMargin=12 * mm, topMargin=12 * mm, bottomMargin=12 * mm,
    )
    styles = getSampleStyleSheet()
    story = [
        Paragraph(f"{strategy_name} — Backtest Trade Log", styles["Title"]),
        Paragraph(
            f"Direction: {direction or '-'} · {backtests_included} backtest(s) pooled · "
            f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            styles["Normal"],
        ),
        Spacer(1, 6 * mm),
    ]

    a = aggregate_stats
    summary_rows = [
        ["Total trades", str(a["total_trades"]), "Win rate", f"{a['win_rate_pct']}%"],
        ["Wins / Losses", f"{a['wins']} / {a['losses']}", "Profit factor", str(a["profit_factor"])],
        ["Gross P&L", _fmt_money(a["gross_pnl_usd"]), "Commission", _fmt_money(a["total_commission_usd"])],
        ["Net P&L", _fmt_money(a["net_pnl_usd"]), "Avg winner / loser",
         f"{_fmt_money(a['avg_winner'])} / {_fmt_money(a['avg_loser'])}"],
    ]
    summary_table = Table(summary_rows, colWidths=[35 * mm, 35 * mm, 35 * mm, 45 * mm])
    summary_table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 6 * mm))

    if diagnostics and diagnostics["summary"]["diagnosable_trades"]:
        s = diagnostics["summary"]
        ev = diagnostics["entry_vs_exit"]
        story.append(Paragraph("Entry vs Exit Analysis", styles["Heading3"]))
        diag_rows = [
            ["Avg MFE ($ / R)", f"{_fmt_money(s['avg_mfe_usd'])} / {_fmt_r(s['avg_mfe_r'])}",
             "Avg MAE ($ / R)", f"{_fmt_money(s['avg_mae_usd'])} / {_fmt_r(s['avg_mae_r'])}"],
            ["Avg / Median Final R", f"{_fmt_r(s['avg_final_r'])} / {_fmt_r(s['median_final_r'])}",
             "Best / Worst Final R", f"{_fmt_r(s['best_final_r'])} / {_fmt_r(s['worst_final_r'])}"],
            ["Avg / Median Capture %", f"{_fmt_pct(s['avg_capture_pct'])} / {_fmt_pct(s['median_capture_pct'])}",
             "Reached +1R / +2R / +3R", f"{_fmt_pct(ev['pct_reaching_1r'])} / {_fmt_pct(ev['pct_reaching_2r'])} / {_fmt_pct(ev['pct_reaching_3r'])}"],
        ]
        diag_table = Table(diag_rows, colWidths=[40 * mm, 45 * mm, 40 * mm, 55 * mm])
        diag_table.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8.5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ]))
        story.append(diag_table)
        for note in ev["notes"]:
            story.append(Paragraph(f"• {note}", styles["Normal"]))
        story.append(Spacer(1, 6 * mm))

    # PNL_COL/FINAL_R_COL index into `header` below - used after the loop to
    # color those two columns green/red per row without hardcoding the
    # index twice and risking the two silently drifting apart.
    header = ["#", "Symbol", "Side", "Entry $", "Stop", "Risk $", "Exit $", "Size",
              "MFE $", "MFE R", "MAE $", "MAE R", "P&L $", "Final R", "Capture %",
              "Comm $", "Exit Reason", "Entry → Exit (ET)"]
    PNL_COL = header.index("P&L $")
    FINAL_R_COL = header.index("Final R")
    rows = [header]
    for i, p in enumerate(pairs, start=1):
        entry_time = p["sell_time"] if p["side"] == "short" else p["buy_time"]
        exit_time = p["buy_time"] if p["side"] == "short" else p["sell_time"]
        risk = perf.initial_risk_per_share(p)
        risk_usd = risk * p["size"] if risk is not None else None
        rows.append([
            str(i), p["symbol"], p["side"], _fmt_money(p["open_price"]),
            _fmt_money(p.get("initial_stop")), _fmt_money(risk_usd), _fmt_money(p["close_price"]),
            str(p["size"]), _fmt_money(p.get("mfe_usd")), _fmt_r(p.get("mfe_r")),
            _fmt_money(p.get("mae_usd")), _fmt_r(p.get("mae_r")), _fmt_money(p["pnl_usd"]),
            _fmt_r(p.get("final_r")), _fmt_pct(p.get("capture_pct")), _fmt_money(p.get("commission_usd")),
            EXIT_REASON_LABELS.get(p.get("exit_reason"), p.get("exit_reason") or "-"),
            f"{_fmt_dt(entry_time)} → {_fmt_dt(exit_time)}",
        ])

    trades_table = Table(rows, repeatRows=1)
    style = [
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 6.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.lightgrey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eef1f5")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
    ]
    for i, p in enumerate(pairs, start=1):
        color = colors.HexColor("#1a7f37") if p["pnl_usd"] > 0 else (
            colors.HexColor("#c0392b") if p["pnl_usd"] < 0 else colors.black)
        style.append(("TEXTCOLOR", (PNL_COL, i), (PNL_COL, i), color))
        style.append(("TEXTCOLOR", (FINAL_R_COL, i), (FINAL_R_COL, i), color))
    trades_table.setStyle(TableStyle(style))
    story.append(trades_table)

    doc.build(story)
    return buf.getvalue()

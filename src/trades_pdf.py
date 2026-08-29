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


def _r_multiple(pair: dict) -> float | None:
    """Real R-multiple off this trade's own initial_stop - unlike perf.
    compute_r_multiples' 1%-of-entry synthetic fallback (needed there only
    because a closed LIVE position doesn't retain its stop), every backtest
    pair already carries the actual initial_stop it was opened with."""
    initial_stop = pair.get("initial_stop")
    if initial_stop is None:
        return None
    risk_per_share = abs(pair["open_price"] - initial_stop)
    if risk_per_share <= 0:
        return None
    move = (
        (pair["open_price"] - pair["close_price"])
        if pair["side"] == "short"
        else (pair["close_price"] - pair["open_price"])
    )
    return move / risk_per_share


def build_trades_pdf(strategy_name: str, direction: str, backtests_included: int,
                      aggregate_stats: dict, pairs: list[dict]) -> bytes:
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
    story.append(Spacer(1, 8 * mm))

    header = ["#", "Symbol", "Side", "Entry Time", "Entry $", "Initial Stop", "Exit Time", "Exit $",
              "Size", "Exit Reason", "R", "P&L $", "P&L %", "Commission"]
    rows = [header]
    for i, p in enumerate(pairs, start=1):
        entry_time = p["sell_time"] if p["side"] == "short" else p["buy_time"]
        exit_time = p["buy_time"] if p["side"] == "short" else p["sell_time"]
        r = _r_multiple(p)
        rows.append([
            str(i), p["symbol"], p["side"], _fmt_dt(entry_time), _fmt_money(p["open_price"]),
            _fmt_money(p.get("initial_stop")), _fmt_dt(exit_time), _fmt_money(p["close_price"]),
            str(p["size"]), EXIT_REASON_LABELS.get(p.get("exit_reason"), p.get("exit_reason") or "-"),
            f"{r:.2f}" if r is not None else "-", _fmt_money(p["pnl_usd"]),
            f"{p['pnl_pct']:.2f}%", _fmt_money(p.get("commission_usd")),
        ])

    trades_table = Table(rows, repeatRows=1)
    style = [
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.lightgrey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eef1f5")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
    ]
    for i, p in enumerate(pairs, start=1):
        color = colors.HexColor("#1a7f37") if p["pnl_usd"] > 0 else (
            colors.HexColor("#c0392b") if p["pnl_usd"] < 0 else colors.black)
        style.append(("TEXTCOLOR", (11, i), (11, i), color))
    trades_table.setStyle(TableStyle(style))
    story.append(trades_table)

    doc.build(story)
    return buf.getvalue()

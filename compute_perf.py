"""Daily performance summary: pairs today's BUY/SELL rows from trades.csv
FIFO-style, computes win rate / P&L / profit factor, sends a Telegram
summary, and (re)writes dashboard/index.html with an R-multiple histogram,
open positions, and the last 20 closed trades. Best run once after the
close (Task Scheduler runs it at 16:05 ET). Never modifies trades.csv.
"""
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yfinance as yf

from src.notify import notify

PROJECT_DIR = Path(__file__).resolve().parent
TRADES_CSV = PROJECT_DIR / "trades.csv"
STATE_PATH = PROJECT_DIR / "open_positions.json"
SAFETY_LOG_PATH = PROJECT_DIR / "safety-check-log.json"
DASHBOARD_DIR = PROJECT_DIR / "dashboard"
DASHBOARD_PATH = DASHBOARD_DIR / "index.html"
ET = ZoneInfo("America/New_York")

R_BUCKETS = [
    ("<= -2R", lambda r: r <= -2, True),
    ("-2R to -1R", lambda r: -2 < r <= -1, True),
    ("-1R to 0R", lambda r: -1 < r <= 0, True),
    ("0R to +1R", lambda r: 0 < r <= 1, False),
    ("+1R to +2R", lambda r: 1 < r <= 2, False),
    ("+2R to +3R", lambda r: 2 < r <= 3, False),
    ("> +3R", lambda r: r > 3, False),
]


def _read_trades(today_only: bool = True) -> list[dict]:
    if not TRADES_CSV.exists():
        return []
    today = datetime.now(ET).strftime("%Y-%m-%d")
    rows = []
    with open(TRADES_CSV, newline="") as f:
        for row in csv.DictReader(f):
            if today_only and not row["timestamp_iso"].startswith(today):
                continue
            rows.append(row)
    return rows


def _pair_trades(rows: list[dict]) -> list[dict]:
    """FIFO-pair BUY rows with SELL rows per symbol."""
    open_buys = defaultdict(list)
    pairs = []
    for row in sorted(rows, key=lambda r: r["timestamp_iso"]):
        symbol = row["symbol"]
        if row["side"] == "BUY":
            open_buys[symbol].append(row)
        elif row["side"] == "SELL" and open_buys[symbol]:
            buy_row = open_buys[symbol].pop(0)
            buy_price = float(buy_row["fill_price"] or 0)
            sell_price = float(row["fill_price"] or 0)
            size = min(int(buy_row["size"]), int(row["size"]))
            pnl_usd = (sell_price - buy_price) * size
            pnl_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price else 0.0
            try:
                hold_minutes = (
                    datetime.fromisoformat(row["timestamp_iso"])
                    - datetime.fromisoformat(buy_row["timestamp_iso"])
                ).total_seconds() / 60
            except ValueError:
                hold_minutes = None
            pairs.append({
                "symbol": symbol,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "size": size,
                "pnl_usd": pnl_usd,
                "pnl_pct": pnl_pct,
                "hold_minutes": hold_minutes,
                "buy_time": buy_row["timestamp_iso"],
                "sell_time": row["timestamp_iso"],
            })
    return pairs


def _aggregate(pairs: list[dict]) -> dict:
    if not pairs:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate_pct": 0.0,
            "gross_pnl_usd": 0.0, "largest_winner": None, "largest_loser": None,
            "avg_winner": 0.0, "avg_loser": 0.0, "profit_factor": "n/a",
        }

    wins = [p for p in pairs if p["pnl_usd"] > 0]
    losses = [p for p in pairs if p["pnl_usd"] <= 0]
    gross_pnl = sum(p["pnl_usd"] for p in pairs)
    sum_wins = sum(p["pnl_usd"] for p in wins)
    sum_losses = sum(p["pnl_usd"] for p in losses)

    largest_winner = max(pairs, key=lambda p: p["pnl_usd"]) if pairs else None
    largest_loser = min(pairs, key=lambda p: p["pnl_usd"]) if pairs else None

    if not losses:
        profit_factor = "inf"
    elif sum_losses == 0:
        profit_factor = "inf"
    else:
        profit_factor = round(sum_wins / abs(sum_losses), 2)

    return {
        "total_trades": len(pairs),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(pairs) * 100, 1),
        "gross_pnl_usd": round(gross_pnl, 2),
        "largest_winner": {"symbol": largest_winner["symbol"], "pnl_usd": round(largest_winner["pnl_usd"], 2)} if largest_winner else None,
        "largest_loser": {"symbol": largest_loser["symbol"], "pnl_usd": round(largest_loser["pnl_usd"], 2)} if largest_loser else None,
        "avg_winner": round(sum_wins / len(wins), 2) if wins else 0.0,
        "avg_loser": round(sum_losses / len(losses), 2) if losses else 0.0,
        "profit_factor": profit_factor,
    }


def _load_state_stops() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        positions = json.loads(STATE_PATH.read_text())
    except json.JSONDecodeError:
        return {}
    return {p["symbol"]: p.get("initial_stop") for p in positions}


def _compute_r_multiples(pairs: list[dict], stops: dict) -> list[float]:
    r_values = []
    for p in pairs:
        stop = stops.get(p["symbol"])
        if stop is None:
            stop = p["buy_price"] * 0.99  # fallback proxy: 1% initial risk
        risk_per_share = p["buy_price"] - stop
        if risk_per_share <= 0:
            continue
        r_values.append((p["sell_price"] - p["buy_price"]) / risk_per_share)
    return r_values


def _histogram(r_values: list[float]) -> list[tuple]:
    counts = []
    for label, predicate, is_loss in R_BUCKETS:
        counts.append((label, sum(1 for r in r_values if predicate(r)), is_loss))
    return counts


def _last_price(symbol: str) -> float | None:
    try:
        bars = yf.Ticker(symbol.replace(" ", "-")).history(period="1d", interval="5m")
        if bars.empty:
            return None
        return float(bars["Close"].iloc[-1])
    except Exception:
        return None


def _open_positions_display() -> list[dict]:
    if not STATE_PATH.exists():
        return []
    try:
        positions = json.loads(STATE_PATH.read_text())
    except json.JSONDecodeError:
        return []

    display = []
    for p in positions:
        price = _last_price(p["symbol"])
        risk_per_share = p["entry_price"] - p["initial_stop"]
        unrealized_r = ((price - p["entry_price"]) / risk_per_share) if price and risk_per_share > 0 else None
        display.append({
            "symbol": p["symbol"], "qty": p["qty"], "entry": p["entry_price"],
            "stop": p.get("stop_price", p["initial_stop"]),
            "unrealized_r": unrealized_r,
        })
    return display


def _last_cycle_status() -> str:
    if not SAFETY_LOG_PATH.exists():
        return "no cycle data yet"
    try:
        with open(SAFETY_LOG_PATH, "rb") as f:
            lines = f.read().splitlines()
        if not lines:
            return "no cycle data yet"
        last = json.loads(lines[-1].decode("utf-8"))
        return f"{last.get('timestamp_iso', '?')} ({last.get('event', '?')})"
    except (json.JSONDecodeError, IndexError):
        return "no cycle data yet"


def _render_dashboard(aggregates: dict, r_values: list[float], open_positions: list[dict],
                       closed_pairs: list[dict], last_cycle: str):
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    histogram = _histogram(r_values)
    max_count = max((c for _, c, _ in histogram), default=0) or 1

    hist_rows = "".join(
        f'<div class="d-flex align-items-center mb-1">'
        f'<div style="width:110px" class="small">{label}</div>'
        f'<div class="flex-grow-1 bg-light rounded overflow-hidden" style="height:18px;">'
        f'<div style="width:{(count / max_count) * 100:.0f}%; height:100%; '
        f'background-color:{"#dc3545" if is_loss else "#28a745"};"></div>'
        f'</div><div class="small ms-2">{count}</div></div>'
        for label, count, is_loss in histogram
    )

    open_rows = "".join(
        f"<tr><td>{p['symbol']}</td><td>{p['qty']}</td><td>${p['entry']:.2f}</td>"
        f"<td>${p['stop']:.2f}</td><td>{'%.2fR' % p['unrealized_r'] if p['unrealized_r'] is not None else '-'}</td></tr>"
        for p in open_positions
    ) or '<tr><td colspan="5" class="text-center text-muted">No open positions</td></tr>'

    closed_rows = ""
    for p in sorted(closed_pairs, key=lambda x: x["sell_time"], reverse=True)[:20]:
        color = "text-success" if p["pnl_usd"] > 0 else "text-danger"
        closed_rows += (
            f"<tr><td>{p['symbol']}</td><td>${p['buy_price']:.2f}</td><td>${p['sell_price']:.2f}</td>"
            f"<td class=\"{color}\">${p['pnl_usd']:+.2f}</td><td>{p['sell_time']}</td></tr>"
        )
    if not closed_rows:
        closed_rows = '<tr><td colspan="5" class="text-center text-muted">No closed trades</td></tr>'

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TradingBot Dashboard</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container py-4">
  <div class="d-flex justify-content-between align-items-center mb-4 p-3 bg-white rounded shadow-sm">
    <div><span class="badge bg-success">Bot Status: ACTIVE</span></div>
    <div class="small text-muted">Last cycle: {last_cycle}</div>
  </div>

  <div class="row g-3">
    <div class="col-md-6">
      <div class="card h-100">
        <div class="card-header">Today's P&amp;L Summary</div>
        <div class="card-body">
          <p>Trades: <strong>{aggregates['total_trades']}</strong>
             ({aggregates['wins']}W / {aggregates['losses']}L,
             {aggregates['win_rate_pct']}%)</p>
          <p>Gross P&amp;L: <strong>${aggregates['gross_pnl_usd']:+.2f}</strong></p>
          <p>Profit factor: <strong>{aggregates['profit_factor']}</strong></p>
          <p>Avg winner: ${aggregates['avg_winner']:+.2f} | Avg loser: ${aggregates['avg_loser']:+.2f}</p>
        </div>
      </div>
    </div>
    <div class="col-md-6">
      <div class="card h-100">
        <div class="card-header">R-Multiple Histogram</div>
        <div class="card-body">{hist_rows or '<p class="text-muted">No closed trades</p>'}</div>
      </div>
    </div>
    <div class="col-md-6">
      <div class="card h-100">
        <div class="card-header">Open Positions</div>
        <div class="card-body p-0">
          <table class="table table-sm mb-0">
            <thead><tr><th>Symbol</th><th>Qty</th><th>Entry</th><th>Stop</th><th>Unrealized R</th></tr></thead>
            <tbody>{open_rows}</tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col-md-6">
      <div class="card h-100">
        <div class="card-header">Recent Closed Trades</div>
        <div class="card-body p-0">
          <table class="table table-sm mb-0">
            <thead><tr><th>Symbol</th><th>Buy</th><th>Sell</th><th>P&amp;L</th><th>Closed</th></tr></thead>
            <tbody>{closed_rows}</tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</div>
</body>
</html>
"""
    DASHBOARD_PATH.write_text(html)


def main():
    rows = _read_trades(today_only=True)
    pairs = _pair_trades(rows)
    aggregates = _aggregate(pairs)

    print(json.dumps(aggregates, default=str))

    if aggregates["total_trades"] == 0:
        body = "No closed trades today."
    else:
        body = (
            f"Trades: {aggregates['total_trades']} "
            f"({aggregates['wins']}W / {aggregates['losses']}L, {aggregates['win_rate_pct']}%)\n"
            f"P&L: ${aggregates['gross_pnl_usd']:+.2f}\n"
            f"Best: {aggregates['largest_winner']['symbol']} ${aggregates['largest_winner']['pnl_usd']:+.2f}\n"
            f"Worst: {aggregates['largest_loser']['symbol']} ${aggregates['largest_loser']['pnl_usd']:+.2f}\n"
            f"PF: {aggregates['profit_factor']}"
        )
    notify(f"Daily Summary {datetime.now(ET).strftime('%Y-%m-%d')}", body, "default")

    stops = _load_state_stops()
    r_values = _compute_r_multiples(pairs, stops)
    open_positions = _open_positions_display()
    last_cycle = _last_cycle_status()
    _render_dashboard(aggregates, r_values, open_positions, pairs, last_cycle)


if __name__ == "__main__":
    main()

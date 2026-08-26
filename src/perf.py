"""Trade pairing and performance aggregation, shared by daily_summary.py
(the scheduled Telegram summary) and the dashboard API (live views). Reads
from the trades/positions tables in src/db.py.
"""
from collections import defaultdict
from datetime import datetime

from src import db

R_BUCKETS = [
    ("<= -2R", lambda r: r <= -2, True),
    ("-2R to -1R", lambda r: -2 < r <= -1, True),
    ("-1R to 0R", lambda r: -1 < r <= 0, True),
    ("0R to +1R", lambda r: 0 < r <= 1, False),
    ("+1R to +2R", lambda r: 1 < r <= 2, False),
    ("+2R to +3R", lambda r: 2 < r <= 3, False),
    ("> +3R", lambda r: r > 3, False),
]


def pair_trades(rows: list[dict]) -> list[dict]:
    """FIFO-pairs opening and closing trades per symbol. A symbol is only
    ever long XOR short at a time — cycle.py's entry_scan for either
    direction checks all currently-held symbols regardless of side before
    entering — so whichever action opens a symbol's position (BUY or SELL)
    determines that pair's side: BUY-then-SELL is a long, SELL-then-BUY is
    a short. pnl_usd = (sell_price - buy_price) * size holds unchanged for
    both — a short's SELL is its (higher, ideally) opening price and its
    BUY is its (lower, ideally) closing price, so the same subtraction
    still yields the profit.

    A single open fill can be closed across SEVERAL opposite-side fills
    (a partial-profit take followed by a later final close is the normal
    case here, since every strategy preset configures one) - each pending
    open row tracks its own unfilled remaining size, and a closing row
    walks through pending open rows (oldest first) consuming from each
    until its own size is exhausted, producing one pair per matched slice.
    An earlier version of this function popped the WHOLE open row on the
    first opposite-side match regardless of quantity - correct only when
    a position closes in exactly one fill, but for a partial-then-final
    close it silently left the final closing fill unmatched (dropped from
    every pair, along with its own P&L and commission) once the entry was
    already fully consumed by the smaller partial fill. A trade closed via
    2+ fills is correctly split into that many pairs now, not 1."""
    open_trades = defaultdict(list)  # symbol -> pending open rows, each carrying its own "_remaining" unfilled size
    pairs = []
    for row in sorted(rows, key=lambda r: (r["timestamp_iso"], r.get("id", 0))):
        symbol = row["symbol"]
        pending = open_trades[symbol]
        remaining = int(row["size"])
        while remaining > 0 and pending and pending[0]["side"] != row["side"]:
            open_row = pending[0]
            matched = min(open_row["_remaining"], remaining)
            side = "long" if open_row["side"] == "BUY" else "short"
            open_price = float(open_row["fill_price"] or 0)
            close_price = float(row["fill_price"] or 0)
            buy_price, sell_price = (open_price, close_price) if side == "long" else (close_price, open_price)
            pnl_usd = (sell_price - buy_price) * matched
            move_pct = ((close_price - open_price) / open_price * 100) if open_price else 0.0
            pnl_pct = move_pct if side == "long" else -move_pct
            try:
                hold_minutes = (
                    datetime.fromisoformat(row["timestamp_iso"])
                    - datetime.fromisoformat(open_row["timestamp_iso"])
                ).total_seconds() / 60
            except ValueError:
                hold_minutes = None
            # The open leg's own commission was paid once, at entry - only
            # attribute it to the slice that finally exhausts open_row's
            # remaining size, so splitting one entry across several closing
            # pairs doesn't charge that same commission again on each one.
            open_commission = float(open_row.get("commission") or 0) if matched == open_row["_remaining"] else 0.0
            pairs.append({
                "symbol": symbol,
                "side": side,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "open_price": open_price,
                "close_price": close_price,
                "size": matched,
                "pnl_usd": pnl_usd,
                "pnl_pct": pnl_pct,
                "hold_minutes": hold_minutes,
                "buy_time": open_row["timestamp_iso"] if side == "long" else row["timestamp_iso"],
                "sell_time": row["timestamp_iso"] if side == "long" else open_row["timestamp_iso"],
                "exit_reason": row.get("exit_reason"),
                "initial_stop": open_row.get("initial_stop"),
                "commission_usd": open_commission + float(row.get("commission") or 0),
            })
            open_row["_remaining"] -= matched
            remaining -= matched
            if open_row["_remaining"] <= 0:
                pending.pop(0)
        if remaining > 0:
            leftover = dict(row)
            leftover["_remaining"] = remaining
            pending.append(leftover)
    return pairs


def aggregate(pairs: list[dict]) -> dict:
    if not pairs:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate_pct": 0.0,
            "gross_pnl_usd": 0.0, "total_commission_usd": 0.0, "net_pnl_usd": 0.0,
            "largest_winner": None, "largest_loser": None,
            "avg_winner": 0.0, "avg_loser": 0.0, "profit_factor": "n/a",
        }

    wins = [p for p in pairs if p["pnl_usd"] > 0]
    losses = [p for p in pairs if p["pnl_usd"] <= 0]
    gross_pnl = sum(p["pnl_usd"] for p in pairs)
    sum_wins = sum(p["pnl_usd"] for p in wins)
    sum_losses = sum(p["pnl_usd"] for p in losses)
    # win/loss classification and profit_factor stay GROSS-based (a live/
    # paper trade row never carries a "commission" field at all, so this
    # is unchanged there) - total_commission_usd/net_pnl_usd are additive,
    # answering "is this actually worth it after real transaction costs"
    # without redefining what every existing caller already reads.
    total_commission = sum(p.get("commission_usd", 0) for p in pairs)

    largest_winner = max(pairs, key=lambda p: p["pnl_usd"])
    largest_loser = min(pairs, key=lambda p: p["pnl_usd"])

    profit_factor = "inf" if (not losses or sum_losses == 0) else round(sum_wins / abs(sum_losses), 2)

    return {
        "total_trades": len(pairs),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(pairs) * 100, 1),
        "gross_pnl_usd": round(gross_pnl, 2),
        "total_commission_usd": round(total_commission, 2),
        "net_pnl_usd": round(gross_pnl - total_commission, 2),
        "largest_winner": {"symbol": largest_winner["symbol"], "pnl_usd": round(largest_winner["pnl_usd"], 2)},
        "largest_loser": {"symbol": largest_loser["symbol"], "pnl_usd": round(largest_loser["pnl_usd"], 2)},
        "avg_winner": round(sum_wins / len(wins), 2) if wins else 0.0,
        "avg_loser": round(sum_losses / len(losses), 2) if losses else 0.0,
        "profit_factor": profit_factor,
    }


def compute_r_multiples(pairs: list[dict]) -> list[float]:
    # closed positions won't be in the open table anymore, so this is
    # always the 1% fallback proxy, mirrored for a short's stop (above
    # entry) vs a long's (below entry).
    r_values = []
    for p in pairs:
        open_price = p["open_price"]
        if p["side"] == "short":
            stop = open_price * 1.01
            risk_per_share = stop - open_price
            move = open_price - p["close_price"]
        else:
            stop = open_price * 0.99
            risk_per_share = open_price - stop
            move = p["close_price"] - open_price
        if risk_per_share <= 0:
            continue
        r_values.append(move / risk_per_share)
    return r_values


def histogram(r_values: list[float]) -> list[tuple]:
    return [(label, sum(1 for r in r_values if predicate(r)), is_loss)
            for label, predicate, is_loss in R_BUCKETS]


def today_summary(account_id: int, mode: str) -> dict:
    rows = db.get_trades(account_id, mode, limit=5000, today_only=True)
    pairs = pair_trades(rows)
    return aggregate(pairs)


# A pooled sample below this many trades is flagged "small sample" in the
# dashboard's Strategy Report card - not a rigorous significance test, just
# a rough tripwire against reading a real signal into a handful of trades
# (most of this dashboard's own backtests so far are single trading days).
STRATEGY_REPORT_LOW_SAMPLE_TRADES = 30


def strategy_report(backtests: list[dict]) -> list[dict]:
    """Pools every strategy's trade pairs across every 'done' backtest that
    included it, then re-aggregates over the pooled set with the same
    aggregate()/compute_r_multiples()/histogram() every single backtest's
    own card already uses - NOT an average of each run's own win_rate_pct/
    profit_factor, which would weight a lone-trade day the same as a
    7-trade day. `backtests` is db.list_done_backtest_results' shape:
    [{"id", "created_at", "params", "results"}, ...], oldest first.

    Deduped by (strategy_id, start_date, end_date), keeping only the
    newest-created run for an exact date range - re-running the same range
    for the same strategy (e.g. right after a bug fix - this has already
    happened more than once against this exact dashboard) would otherwise
    silently pool a stale pre-fix result alongside its corrected re-run,
    double-counting those trades AND averaging in the wrong numbers.
    Doesn't attempt to resolve PARTIALLY-overlapping ranges (e.g. one run
    covering Aug 1-5 and another covering just Aug 3) - every backtest run
    against this dashboard so far has been single-day, so exact-range
    dedup already covers the real usage pattern; a partial-overlap
    resolver would be unvalidated complexity for a case that hasn't
    actually come up yet.

    Returns one entry per strategy_id that appears in at least one 'done'
    result, busiest (most pooled trades) first."""
    latest_by_key = {}  # (strategy_id, start_date, end_date) -> {"created_at", "result"}
    for bt in backtests:
        date_key = (bt["params"].get("start_date"), bt["params"].get("end_date"))
        for strategy_id, result in bt["results"].items():
            if not isinstance(result, dict) or "aggregate" not in result:
                continue  # {"error": "..."} entries - strategy deleted/not found at run time
            key = (strategy_id, date_key)
            existing = latest_by_key.get(key)
            if existing is None or bt["created_at"] > existing["created_at"]:
                latest_by_key[key] = {"created_at": bt["created_at"], "result": result}

    by_strategy = defaultdict(list)
    for (strategy_id, _date_key), entry in latest_by_key.items():
        by_strategy[strategy_id].append(entry["result"])

    report = []
    for strategy_id, results in by_strategy.items():
        pooled_pairs = [pair for r in results for pair in r["pairs"]]
        pooled_filter_stats = defaultdict(int)
        for r in results:
            for cond, count in (r.get("filter_stats") or {}).items():
                pooled_filter_stats[cond] += count
        r_values = compute_r_multiples(pooled_pairs)
        agg = aggregate(pooled_pairs)
        report.append({
            "strategy_id": strategy_id,
            "strategy_name": results[0].get("strategy_name") or f"Strategy {strategy_id}",
            "direction": results[0].get("direction"),
            "backtests_included": len(results),
            "aggregate": agg,
            "low_sample": agg["total_trades"] < STRATEGY_REPORT_LOW_SAMPLE_TRADES,
            "histogram": [{"label": l, "count": c, "is_loss": loss} for l, c, loss in histogram(r_values)],
            "filter_stats": dict(pooled_filter_stats),
        })
    report.sort(key=lambda entry: entry["aggregate"]["total_trades"], reverse=True)
    return report

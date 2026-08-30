"""Trade pairing and performance aggregation, shared by daily_summary.py
(the scheduled Telegram summary) and the dashboard API (live views). Reads
from the trades/positions tables in src/db.py.
"""
from collections import defaultdict
from datetime import datetime

from src import db, entry_metrics

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

    A single open fill can be closed across SEVERAL opposite-side fills -
    a position opened before the partial-profit exit stage was removed can
    still have historical trades shaped that way, and a manual/broker-side
    partial close is always possible regardless of strategy config - each
    pending open row tracks its own unfilled remaining size, and a closing
    row walks through pending open rows (oldest first) consuming from each
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
                # None for every non-ORB trade (predates this field) - only
                # ever set by src/backtest_engine.simulate_orb_strategy's
                # entry rows, to "breakout"|"retest" (see orb.evaluate_orb_
                # entry). Off the OPEN leg, same reasoning as initial_stop
                # just above - the model that triggered ENTRY, not exit.
                "model": open_row.get("model"),
                # None unless the strategy opted into src/es_filter.py's
                # gate (rules["es_vwap_filter"]) AND ES's own bars were
                # supplied to the backtest run - see backtest_engine.py's
                # _es_filter_pass. Off the OPEN leg, same reasoning as
                # initial_stop/model above - the gate is evaluated at
                # entry, not exit.
                "es_filter_pass": open_row.get("es_filter_pass"),
                "commission_usd": open_commission + float(row.get("commission") or 0),
                # Both None for a live/paper pair (predates these fields,
                # and there's no intrabar price-path history to derive them
                # from after the fact) - only ever set by backtest_engine.py,
                # which tracks the actual best/worst price seen while the
                # position was open (see its own _update_excursion). Off the
                # CLOSE leg (`row`), not the open leg - unlike initial_stop/
                # model, this can only be known once the position has
                # actually finished its life, not at entry time.
                "mfe_price": row.get("mfe_price"),
                "mae_price": row.get("mae_price"),
                # None for every trade that predates this (any non-ORB
                # pair, or an ORB pair whose exit_reason isn't one of
                # "initial_stop_loss"/"profit_lock_stop"/"staged_trailing_
                # stop") - only ever set by simulate_orb_strategy's
                # staged_trail branch, off the CLOSE leg (same reasoning
                # as mfe_price/mae_price above - the lifecycle snapshot at
                # actual exit time, not entry). See src/trade_diagnostics.
                # py's own exit_reason_breakdown for how these feed the
                # "Exit Reason Breakdown" report.
                "profit_lock_activated": row.get("profit_lock_activated"),
                "profit_lock_activated_at_r": row.get("profit_lock_activated_at_r"),
                "trail_activated": row.get("trail_activated"),
                "trail_activated_at_r": row.get("trail_activated_at_r"),
                # Point-in-time market/stock/setup context captured at
                # entry (see src/entry_metrics.py) - off the OPEN leg
                # (open_row), not the close leg, since these are all
                # entry-time snapshots, the opposite convention from mfe_
                # price/profit_lock_activated above. None for every trade
                # that predates this (any non-ORB pair, or an ORB pair
                # from before this feature shipped).
                **{k: open_row.get(k) for k in entry_metrics.ENTRY_METRICS_KEYS},
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


def initial_risk_per_share(pair: dict) -> float | None:
    """abs(entry - real stop) for a pair that carries one - every backtest
    pair does (pair_trades passes initial_stop straight through from the
    open leg's trade row, see backtest_engine.py). A LIVE/paper pair falls
    back to a synthetic "assume the stop was 1% away" proxy instead, since
    a closed live position's real stop isn't retained anywhere once
    db.remove_position deletes it - this is the best information left by
    the time pair_trades ever sees that trade. None only if even the
    proxy can't be computed (a zero/negative open_price)."""
    open_price = pair["open_price"]
    initial_stop = pair.get("initial_stop")
    risk = abs(open_price - initial_stop) if initial_stop is not None else open_price * 0.01
    return risk if risk > 0 else None


def r_multiple(pair: dict) -> float | None:
    """Signed R-multiple for one pair's actual close - shared by
    compute_r_multiples (batch, over live+backtest pairs alike) and
    src/trade_diagnostics.py's per-trade MFE_R/MAE_R/FinalR (backtest
    only, since those depend on fields only backtest_engine.py sets).
    None if initial_risk_per_share can't be computed."""
    risk = initial_risk_per_share(pair)
    if risk is None:
        return None
    move = (pair["open_price"] - pair["close_price"]) if pair["side"] == "short" else (pair["close_price"] - pair["open_price"])
    return move / risk


def _exit_time(pair: dict) -> str:
    # short's exit is its buy leg (SELL opens, BUY closes); long's is its
    # sell leg - same convention already documented in pair_trades/
    # backtest.html's renderTrades ("entryTime"/"exitTime" split).
    return pair["buy_time"] if pair["side"] == "short" else pair["sell_time"]


def compute_max_drawdown(pairs: list[dict]) -> float:
    """Largest peak-to-trough decline in the cumulative net-of-commission
    equity curve, walking closed pairs in chronological EXIT order (the
    order equity actually changed, not entry order or trade_id order) -
    0.0 for an empty set or one that only ever rose. Dollar terms, not a
    percentage - matching aggregate()'s own net_pnl_usd, since pair_trades
    never carries a portfolio_value to express a % drawdown against."""
    if not pairs:
        return 0.0
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in sorted(pairs, key=_exit_time):
        equity += p["pnl_usd"] - float(p.get("commission_usd") or 0)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return round(max_dd, 2)


def compute_r_multiples(pairs: list[dict]) -> list[float]:
    """Before this used the 1%-proxy stop unconditionally, even for a
    backtest pair that had its own real stop sitting right there in the
    same dict - off by however far the real stop actually was from 1%,
    which for a wide swing-based stop (backtest_engine's own default) is
    often several multiples wide, silently sorting trades into the wrong R
    bucket entirely (see the dashboard's own "Initial Stop" trade column,
    which was already showing the real number the histogram wasn't
    using). r_multiple() above now prefers that real stop whenever a pair
    has one."""
    return [r for r in (r_multiple(p) for p in pairs) if r is not None]


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


def _latest_results_by_strategy(backtests: list[dict]) -> dict:
    """Shared dedup step behind strategy_report and pooled_trades_for_
    strategy - keeps only the newest-created 'done' result per (strategy_id,
    start_date, end_date), see strategy_report's own docstring for the full
    rationale. Returns {strategy_id: [result, ...]}, each result carrying
    its own "pairs"/"aggregate"/"filter_stats"/"strategy_name"/"direction"."""
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
    return by_strategy


def pooled_trades_for_strategy(backtests: list[dict], strategy_id) -> dict | None:
    """Same pooling/dedup as strategy_report, narrowed to a single strategy
    and returning the actual trade pairs (not just the aggregated stats) -
    for a full trade-by-trade export (see web/app.py's trades-PDF endpoint
    and src/trades_pdf.py). `strategy_id` is matched as a string, since a
    parsed results_json's top-level keys always are. Returns None if this
    strategy has no 'done' backtest result in `backtests` at all."""
    by_strategy = _latest_results_by_strategy(backtests)
    results = by_strategy.get(str(strategy_id))
    if not results:
        return None
    pooled_pairs = [pair for r in results for pair in r["pairs"]]
    pooled_pairs.sort(key=lambda p: p["buy_time"] if p["side"] == "long" else p["sell_time"])
    return {
        "strategy_name": results[0].get("strategy_name") or f"Strategy {strategy_id}",
        "direction": results[0].get("direction"),
        "backtests_included": len(results),
        "pairs": pooled_pairs,
        "aggregate": aggregate(pooled_pairs),
    }


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
    by_strategy = _latest_results_by_strategy(backtests)

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

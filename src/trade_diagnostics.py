"""Advanced per-trade and per-strategy diagnostics: MFE (Maximum
Favorable Excursion), MAE (Maximum Adverse Excursion), real R-multiple,
and "capture %" (how much of a trade's own best available move its exit
actually kept) - plus the summary stats, R distribution, and exit-quality
counts built on top of them.

BACKTEST-ONLY: every function here depends on mfe_price/mae_price, which
only backtest_engine.py's simulators ever set (see its own
_update_excursion, called every tick a position is open, tracking the
best/worst price actually seen from the real intrabar High/Low). A
live/paper pair never carries them - there's no retained intrabar price
history for a position once it's closed - so enrich() below returns None
for every added field on a pair missing them, same convention
perf.pair_trades already uses for initial_stop/model on a pair that
predates those fields, rather than fabricating a number from data that
was never there.
"""
from statistics import mean, median

from src import perf

# Same 7 buckets perf.R_BUCKETS already defines for the existing
# histogram() - reused here (not redefined) so "R Distribution" can never
# quietly drift out of sync with the buckets the rest of the dashboard
# already shows.
R_BUCKETS = perf.R_BUCKETS


def enrich(pair: dict) -> dict:
    """A NEW dict (pair's own fields, plus mfe_usd/mfe_r/mae_usd/mae_r/
    final_r/capture_pct) - never mutates `pair` itself. mfe_usd/mae_usd
    are TOTAL position dollars (per-share move * size), matching how
    pnl_usd/commission_usd are already reported elsewhere in this
    codebase; mfe_r/mae_r/final_r stay size-independent per-share ratios,
    the same convention perf.r_multiple already uses."""
    out = dict(pair)
    risk = perf.initial_risk_per_share(pair)
    mfe_price = pair.get("mfe_price")
    mae_price = pair.get("mae_price")
    if risk is None or mfe_price is None or mae_price is None:
        out.update(mfe_usd=None, mfe_r=None, mae_usd=None, mae_r=None, final_r=None, capture_pct=None)
        return out

    open_price = pair["open_price"]
    size = pair["size"]
    if pair["side"] == "short":
        mfe_per_share = open_price - mfe_price
        mae_per_share = mae_price - open_price
    else:
        mfe_per_share = mfe_price - open_price
        mae_per_share = open_price - mae_price

    mfe_usd = mfe_per_share * size
    mae_usd = mae_per_share * size
    final_r = perf.r_multiple(pair)
    out.update(
        mfe_usd=mfe_usd, mfe_r=mfe_per_share / risk,
        mae_usd=mae_usd, mae_r=mae_per_share / risk,
        final_r=final_r,
        capture_pct=(pair["pnl_usd"] / mfe_usd * 100) if mfe_usd > 0 else 0.0,
    )
    return out


def enrich_all(pairs: list[dict]) -> list[dict]:
    return [enrich(p) for p in pairs]


def _diagnosable(enriched: list[dict]) -> list[dict]:
    return [p for p in enriched if p["final_r"] is not None]


def summarize(enriched: list[dict]) -> dict:
    """Section 5 of the spec: strategy-level MFE/MAE/FinalR/Capture%
    summary stats. Every average/median/best/worst is computed only over
    diagnosable trades (see _diagnosable) - a pooled report mixing
    pre-feature and post-feature backtest runs shouldn't silently treat a
    missing MFE as zero. `count`/`total` distinguish "N/A, no diagnosable
    trades in this set" from "0, trades exist but all had e.g. no
    favorable move at all"."""
    d = _diagnosable(enriched)
    if not d:
        return {
            "diagnosable_trades": 0, "total_trades": len(enriched),
            "avg_mfe_usd": None, "avg_mfe_r": None, "avg_mae_usd": None, "avg_mae_r": None,
            "avg_final_r": None, "median_final_r": None, "best_final_r": None, "worst_final_r": None,
            "avg_capture_pct": None, "median_capture_pct": None,
        }
    final_rs = [p["final_r"] for p in d]
    captures = [p["capture_pct"] for p in d]
    return {
        "diagnosable_trades": len(d), "total_trades": len(enriched),
        "avg_mfe_usd": round(mean(p["mfe_usd"] for p in d), 2),
        "avg_mfe_r": round(mean(p["mfe_r"] for p in d), 3),
        "avg_mae_usd": round(mean(p["mae_usd"] for p in d), 2),
        "avg_mae_r": round(mean(p["mae_r"] for p in d), 3),
        "avg_final_r": round(mean(final_rs), 3),
        "median_final_r": round(median(final_rs), 3),
        "best_final_r": round(max(final_rs), 3),
        "worst_final_r": round(min(final_rs), 3),
        "avg_capture_pct": round(mean(captures), 1),
        "median_capture_pct": round(median(captures), 1),
    }


def r_distribution(enriched: list[dict]) -> list[dict]:
    """Section 6: same buckets as perf.histogram(), but keyed off each
    trade's real final_r (already computed via perf.r_multiple in
    enrich()) and carrying a percentage-of-diagnosable-trades alongside
    the raw count, since a bare count doesn't say whether 3 trades is
    "most of them" or "a rounding error"."""
    d = _diagnosable(enriched)
    total = len(d)
    out = []
    for label, predicate, is_loss in R_BUCKETS:
        count = sum(1 for p in d if predicate(p["final_r"]))
        out.append({
            "label": label, "count": count, "is_loss": is_loss,
            "pct": round(count / total * 100, 1) if total else 0.0,
        })
    return out


def exit_quality(enriched: list[dict]) -> dict:
    """Section 7: how often a trade reached +1R/+2R/+3R AT ANY POINT
    (mfe_r >= threshold - MFE is by definition the best R a trade ever
    saw, so this is exactly what "reached" means) versus where it
    actually closed (final_r) - the gap between the two is entries doing
    their job but exits giving profit back. Every count's percentage is
    of ALL diagnosable trades (not just the "reached" subset), so e.g.
    "38% reached +2R but closed below +1R" reads as a standalone number
    without needing the reached-count alongside it to make sense."""
    d = _diagnosable(enriched)
    total = len(d)

    def pct(n):
        return round(n / total * 100, 1) if total else 0.0

    reached_1r = sum(1 for p in d if p["mfe_r"] >= 1)
    reached_2r = sum(1 for p in d if p["mfe_r"] >= 2)
    reached_3r = sum(1 for p in d if p["mfe_r"] >= 3)
    gave_back_1r = sum(1 for p in d if p["mfe_r"] >= 1 and p["final_r"] < 0)
    gave_back_2r = sum(1 for p in d if p["mfe_r"] >= 2 and p["final_r"] < 1)
    gave_back_3r = sum(1 for p in d if p["mfe_r"] >= 3 and p["final_r"] < 1)
    return {
        "diagnosable_trades": total,
        "reached_1r": {"count": reached_1r, "pct": pct(reached_1r)},
        "reached_2r": {"count": reached_2r, "pct": pct(reached_2r)},
        "reached_3r": {"count": reached_3r, "pct": pct(reached_3r)},
        "reached_1r_closed_below_0r": {"count": gave_back_1r, "pct": pct(gave_back_1r)},
        "reached_2r_closed_below_1r": {"count": gave_back_2r, "pct": pct(gave_back_2r)},
        "reached_3r_closed_below_1r": {"count": gave_back_3r, "pct": pct(gave_back_3r)},
    }


# Thresholds behind the auto-interpretation below - the spec's own wording
# ("much larger", "very small", "many") is qualitative, so these numbers
# are a documented, reasonable reading of it, not a discovered constant:
#   - captured less than half the average favorable move available
#   - MAE rarely used even 30% of the stop before the trade worked out
#   - among trades that reached +2R, at least a third gave it back below +1R
_CAPTURE_INEFFICIENT_RATIO = 0.5
_MAE_TOO_SMALL_R = 0.3
_GIVEBACK_FRACTION_FLAG = 0.3


def entry_vs_exit_analysis(enriched: list[dict]) -> dict:
    """Section 9: "ENTRY VS EXIT ANALYSIS" - the headline numbers plus a
    short auto-generated interpretation per the spec's three rules. Each
    rule is independent (a strategy can trip more than one, or none)."""
    d = _diagnosable(enriched)
    summary = summarize(enriched)
    quality = exit_quality(enriched)
    notes = []

    if d and summary["avg_mfe_r"] and summary["avg_mfe_r"] > 0 \
            and summary["avg_final_r"] < summary["avg_mfe_r"] * _CAPTURE_INEFFICIENT_RATIO:
        notes.append(
            "Avg MFE (R) is much larger than Avg FinalR - entries may be effective but exits are giving back profit."
        )
    if d and mean(p["mae_r"] for p in d) < _MAE_TOO_SMALL_R:
        notes.append("Avg MAE (R) is very small relative to stop size - stops may be too wide.")
    reached_2r = quality["reached_2r"]["count"]
    if reached_2r and quality["reached_2r_closed_below_1r"]["count"] / reached_2r >= _GIVEBACK_FRACTION_FLAG:
        notes.append(
            "Many trades that reached +2R closed back below +1R - investigate trailing stop and breakeven logic."
        )

    return {
        "avg_mfe_r": summary["avg_mfe_r"],
        "avg_final_r": summary["avg_final_r"],
        "avg_capture_pct": summary["avg_capture_pct"],
        "pct_reaching_1r": quality["reached_1r"]["pct"],
        "pct_reaching_2r": quality["reached_2r"]["pct"],
        "pct_reaching_3r": quality["reached_3r"]["pct"],
        "notes": notes,
    }


def es_filter_report(pairs: list[dict]) -> dict | None:
    """Before/after stats for src/es_filter.py's ES-VWAP gate (see
    backtest_engine.py's _es_filter_pass) - None if this strategy never
    actually carried the gate in this run (no pair has an es_filter_pass
    other than None), so a caller can tell "not applicable" apart from
    "applied, rejected zero trades". "after" keeps every pair whose
    es_filter_pass is True OR None (not evaluable for that specific
    trade - e.g. ES data was missing that day even though the strategy
    is gated) - only a pair explicitly tagged False was actually
    rejected, matching backtest_engine._es_filter_pass's own fail-open
    convention."""
    if not any(p.get("es_filter_pass") is not None for p in pairs):
        return None
    after = [p for p in pairs if p.get("es_filter_pass") is not False]
    rejected_count = sum(1 for p in pairs if p.get("es_filter_pass") is False)

    def _stats(subset: list[dict]) -> dict:
        agg = perf.aggregate(subset)
        r_values = perf.compute_r_multiples(subset)
        return {
            "total_trades": agg["total_trades"], "win_rate_pct": agg["win_rate_pct"],
            "profit_factor": agg["profit_factor"], "net_profit_usd": agg["net_pnl_usd"],
            "avg_r": round(mean(r_values), 3) if r_values else None,
            "max_drawdown_usd": perf.compute_max_drawdown(subset),
        }

    return {
        "total_trades": len(pairs),
        "rejected_count": rejected_count,
        "rejected_pct": round(rejected_count / len(pairs) * 100, 1) if pairs else 0.0,
        "before": _stats(pairs),
        "after": _stats(after),
    }


def full_report(pairs: list[dict]) -> dict:
    """Everything above, run once over one pooled/single-backtest pair
    list - the one call site (backtest_runner.py / web/app.py's Strategy
    Report) needs."""
    enriched = enrich_all(pairs)
    return {
        "pairs": enriched,
        "summary": summarize(enriched),
        "r_distribution": r_distribution(enriched),
        "exit_quality": exit_quality(enriched),
        "entry_vs_exit": entry_vs_exit_analysis(enriched),
        "es_filter": es_filter_report(enriched),
    }

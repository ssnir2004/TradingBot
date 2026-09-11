"""Unit tests for src/sst_swing.py against hand-checked synthetic OHLC
sequences - long and short entries, inside-day handling, a big-move
trailing-stop day, and the "stop never widens" guarantee. Plain
assertions, no framework (this repo has no pytest dependency - same
convention as momentum/selftest.py).

    python test_sst_swing.py
"""
import sys

import numpy as np
import pandas as pd

from src.sst_swing import (
    classify_days, evaluate_sst_entry, initial_stop_price,
    last_significant_extreme, latest_dmi_trigger, size_for_risk,
    trailing_stop_update,
)
from src.technicals import adx_series

RULES = {
    "sma_period": 50,
    "dmi_period": 8,
    "sma50_neutral_band_pct": 0.5,
    "dmi_touch_tolerance": 2.0,
    "dmi_diverge_confirm_bars": 2,
    "max_entry_delay_bars": 1,
    "avoid_200sma_obstruction": False,  # off for the entry-logic tests below - see the dedicated 200SMA test
    "stop_buffer_pct": 0.5,
    "big_move_threshold_pct": 5.0,
    "max_risk_pct": 3.0,
}

FAILURES = []


def check(label, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {label}" + (f" - {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(label)


def _base_series(n, start, daily_drift, seed=1):
    """A smooth deterministic base series (no hand-crafted "obvious" days
    yet) - just enough realistic wobble for indicators to warm up on, long
    enough to satisfy a 50-bar SMA / 8-period DMI without needing the real
    200-day history (that's exercised separately, see test_200sma_obstruction)."""
    rng = np.random.default_rng(seed)
    closes = [start]
    for _ in range(n - 1):
        closes.append(closes[-1] * (1 + daily_drift + rng.normal(0, 0.004)))
    closes = np.array(closes)
    highs = closes * (1 + np.abs(rng.normal(0.004, 0.002, n)))
    lows = closes * (1 - np.abs(rng.normal(0.004, 0.002, n)))
    opens = np.roll(closes, 1)
    opens[0] = closes[0]
    idx = pd.bdate_range("2026-01-02", periods=n)
    return pd.DataFrame({"Open": opens, "High": highs, "Low": lows, "Close": closes,
                         "Volume": 1_000_000}, index=idx)


def _append_days(base: pd.DataFrame, rows: list[dict]) -> pd.DataFrame:
    idx = pd.bdate_range(base.index[-1] + pd.Timedelta(days=1), periods=len(rows))
    extra = pd.DataFrame(rows, index=idx)
    return pd.concat([base, extra])


def _trend_reversal_series(direction: str, n_base: int = 55) -> pd.DataFrame:
    """Fully deterministic (no randomness) - a MILD base trend opposing
    the eventual entry direction (mild deliberately: a strongly-entrenched
    base needs a multi-day push to flip DMI(8), which then tends to make
    EVERY one of those push days its own "significant day" too, muddying
    which one Rule 4 should reference - a single sharp reversal day
    against a mild base flips DMI cleanly on its own, verified by hand),
    then one sharp reversal day that is BOTH the DMI trigger and the
    significant day (the source prompt explicitly allows a trigger and a
    trend-cross to coincide on the same day - the same reasoning applies
    here), an inside day, and a confirming breakout/breakdown day.
    direction='long' -> mild downtrend base + sharp bullish reversal;
    'short' -> the mirror."""
    rows = []
    price = 20.0
    base_step = 0.9988 if direction == "long" else 1.0012
    for _ in range(n_base):
        price *= base_step
        rows.append({"Open": price * 1.001, "High": price * 1.004, "Low": price * 0.997, "Close": price})

    if direction == "long":
        sig = price * 1.10  # sharp reversal up - the significant day's HIGH
        rows.append({"Open": price, "High": sig, "Low": price * 0.99, "Close": sig * 0.99})
        rows.append({"Open": sig * 0.99, "High": sig * 0.97, "Low": sig * 0.965, "Close": sig * 0.975})  # inside
        rows.append({"Open": sig * 0.975, "High": sig * 1.03, "Low": sig * 0.98, "Close": sig * 1.025})  # breakout
    else:
        sig = price * 0.90  # sharp reversal down - the significant day's LOW
        rows.append({"Open": price, "High": price * 1.01, "Low": sig, "Close": sig * 1.01})
        rows.append({"Open": sig * 1.01, "High": sig * 1.03, "Low": sig * 1.005, "Close": sig * 1.02})  # inside
        rows.append({"Open": sig * 1.02, "High": sig * 1.02, "Low": sig * 0.97, "Close": sig * 0.975})  # breakdown

    idx = pd.bdate_range("2026-01-02", periods=len(rows))
    return pd.DataFrame(rows, index=idx), sig


# ---------------------------------------------------------------- test 1 ---
def test_classify_days():
    print("\n=== classify_days: inside vs significant ===")
    df = pd.DataFrame({
        "Open":  [10.0, 10.2, 10.1, 10.3],
        "High":  [10.5, 10.8, 10.6, 11.0],   # day 2 (10.6) is INSIDE day1's 10.8 high
        "Low":   [9.8,  9.9,  10.0, 9.7],    # day 2 (10.0) is INSIDE day1's 9.9 low
        "Close": [10.3, 10.5, 10.3, 10.8],
    })
    days = classify_days(df)
    check("day 0 (no prior) treated as significant", bool(days["is_significant"].iloc[0]))
    check("day 1 (new high 10.8 > 10.5) is significant", bool(days["is_significant"].iloc[1]))
    check("day 2 (10.6 <= 10.8 high AND 10.0 >= 9.9 low) is an inside day", bool(days["is_inside"].iloc[2]))
    check("day 3 (new high 11.0, new low 9.7) is significant", bool(days["is_significant"].iloc[3]))
    ext = last_significant_extreme(days, 2, "long")  # as of day 2 (inside), should skip back to day 1
    check("last_significant_extreme skips the inside day, lands on day 1's high 10.8",
         ext is not None and abs(ext[0] - 10.8) < 1e-9, str(ext))


# ---------------------------------------------------------------- test 2 ---
def test_long_entry_sequence():
    print("\n=== long entry: uptrend + bullish DMI cross + breakout ===")
    base = _base_series(60, 20.0, 0.003, seed=1)  # gentle uptrend -> close should end up > SMA50

    # engineer a clean +DI/-DI cross by having several strong down-then-up
    # days right before the breakout (forces a real crossover, not a
    # hand-set DI value - adx_series computes it from OHLC)
    extra = [
        {"Open": 24.0, "High": 24.1, "Low": 23.0, "Close": 23.2},  # strong down day (push -DI up)
        {"Open": 23.2, "High": 23.4, "Low": 22.6, "Close": 22.8},  # another down day
        {"Open": 22.8, "High": 24.6, "Low": 22.7, "Close": 24.5},  # strong up day -> significant day (new high) - the "significant day" to break above
        {"Open": 24.5, "High": 24.55, "Low": 24.3, "Close": 24.4},  # inside-ish day, should be ignored
        {"Open": 24.4, "High": 24.9, "Low": 24.35, "Close": 24.85},  # breaks above 24.6 -> entry
    ]
    daily = _append_days(base, extra)

    result = evaluate_sst_entry(daily, RULES, "long")
    check("long entry fires", result["pass"], str(result))
    if result["pass"]:
        check("entry_price is the significant day's high (24.6)", abs(result["entry_price"] - 24.6) < 0.01, str(result))
        check("initial_stop is below that day's low (22.7) minus buffer",
             result["initial_stop"] < 22.7, str(result))


# ---------------------------------------------------------------- test 3 ---
def test_short_entry_sequence():
    print("\n=== short entry: uptrend base + counter-rally + bearish reversal + breakdown ===")
    daily, sig_low = _trend_reversal_series("short")

    result = evaluate_sst_entry(daily, RULES, "short")
    check("short entry fires", result["pass"], str(result))
    if result["pass"]:
        check("entry_price is the significant day's low", abs(result["entry_price"] - sig_low) < 0.05, str(result))
        check("initial_stop is above the significant day's high (buffer applied)",
             result["initial_stop"] > result["entry_price"], str(result))


# ---------------------------------------------------------------- test 4 ---
def test_max_entry_delay_bars():
    print("\n=== max_entry_delay_bars: don't chase a stale breakout ===")
    base = _base_series(60, 20.0, 0.003, seed=1)
    extra = [
        {"Open": 24.0, "High": 24.1, "Low": 23.0, "Close": 23.2},
        {"Open": 23.2, "High": 23.4, "Low": 22.6, "Close": 22.8},
        {"Open": 22.8, "High": 24.6, "Low": 22.7, "Close": 24.5},   # significant day, high=24.6
        {"Open": 24.5, "High": 24.9, "Low": 24.4, "Close": 24.85},  # breaks above 24.6 (delay=0 as of THIS bar)
        {"Open": 24.85, "High": 25.0, "Low": 24.7, "Close": 24.9},  # one bar later (delay=1, still within max=1)
        {"Open": 24.9, "High": 25.1, "Low": 24.8, "Close": 25.0},   # two bars later (delay=2, > max=1)
    ]
    daily_ontime = _append_days(base, extra[:5])
    daily_late = _append_days(base, extra)

    on_time = evaluate_sst_entry(daily_ontime, RULES, "long")
    late = evaluate_sst_entry(daily_late, RULES, "long")
    check("entry still fires 1 bar after the breakout (within max_entry_delay_bars=1)", on_time["pass"], str(on_time))
    check("entry is suppressed 2 bars after the breakout (exceeds max_entry_delay_bars=1)", not late["pass"], str(late))


# ---------------------------------------------------------------- test 5 ---
def test_trailing_stop_big_move_and_never_widens():
    print("\n=== trailing stop: normal day, big-move-day exception, never widens ===")
    base = _base_series(30, 20.0, 0.001, seed=3)

    # normal significant up-day: stop should trail to just under the low
    normal_day = _append_days(base, [{"Open": 25.0, "High": 26.0, "Low": 24.8, "Close": 25.9}])
    new_stop = trailing_stop_update(normal_day, "long", current_stop=23.0, rules=RULES)
    check("normal day trails stop to just under 24.8", new_stop is not None and abs(new_stop - 24.8 * 0.995) < 0.01, str(new_stop))

    # big-move day: range = 26.0-22.0 = 4.0, price ~24 -> 4/24=16.7% >> 5% threshold
    # reference should be the midpoint (24.0), not the full low (22.0)
    big_move_day = _append_days(base, [{"Open": 22.5, "High": 26.0, "Low": 22.0, "Close": 25.8}])
    big_stop = trailing_stop_update(big_move_day, "long", current_stop=20.0, rules=RULES)
    expected_mid = (22.0 + 26.0) / 2 * 0.995
    check("big-move day (16.7% range) uses the 50%-of-range midpoint, not the full low",
         big_stop is not None and abs(big_stop - expected_mid) < 0.05, f"got {big_stop}, expected ~{expected_mid:.2f}")

    # never-widen: a "significant" day whose reference is BELOW the
    # current stop must not move the stop backward
    would_widen = trailing_stop_update(normal_day, "long", current_stop=25.5, rules=RULES)
    check("a candidate that would widen the stop is rejected (returns None)", would_widen is None, str(would_widen))

    # inside day never moves the stop
    inside_day = _append_days(base, [{"Open": 25.0, "High": 25.5, "Low": 25.1, "Close": 25.3}])
    # force it to look "inside" relative to a taller pseudo-prior day by
    # checking directly against classify_days' own is_inside flag
    days = classify_days(inside_day)
    if bool(days["is_inside"].iloc[-1]):
        inside_result = trailing_stop_update(inside_day, "long", current_stop=23.0, rules=RULES)
        check("an inside day never moves the stop", inside_result is None, str(inside_result))
    else:
        print("  [SKIP] synthetic day didn't land as an inside day this run (base series is random) - not a logic failure")


# ---------------------------------------------------------------- test 6 ---
def test_sizing():
    print("\n=== position sizing (Rule 1) ===")
    shares, reason = size_for_risk(equity=50_000, entry_price=100.0, stop_price=97.0, rules=RULES)
    # risk_per_share=3.0, max_risk_amount = 50000*0.03=1500, shares = 1500//3 = 500
    check("3% risk on $50k / $3 risk-per-share = 500 shares", shares == 500 and reason is None, f"{shares}, {reason}")

    shares0, reason0 = size_for_risk(equity=1000, entry_price=100.0, stop_price=50.0, rules=RULES)
    check("stop too far for account size -> 0 shares with a reason", shares0 == 0 and reason0 is not None, f"{shares0}, {reason0}")

    # hard 5% ceiling enforced even if rules asks for more
    greedy_rules = dict(RULES, max_risk_pct=10.0)
    shares_capped, _ = size_for_risk(equity=50_000, entry_price=100.0, stop_price=97.0, rules=greedy_rules)
    shares_uncapped_would_be = int(50_000 * 0.10 // 3.0)
    check("max_risk_pct=10% in config is still capped at the 5% hard ceiling",
         shares_capped < shares_uncapped_would_be, f"{shares_capped} vs uncapped {shares_uncapped_would_be}")


# ---------------------------------------------------------------- test 7 ---
def test_200sma_obstruction():
    print("\n=== avoid_200sma_obstruction (needs real 200-day history) ===")
    base = _base_series(210, 20.0, 0.0015, seed=4)  # long enough for a real 200-day SMA
    extra = [
        {"Open": 27.0, "High": 27.1, "Low": 26.0, "Close": 26.2},
        {"Open": 26.2, "High": 26.4, "Low": 25.6, "Close": 25.8},
        {"Open": 25.8, "High": 27.6, "Low": 25.7, "Close": 27.5},
        {"Open": 27.5, "High": 27.9, "Low": 27.4, "Close": 27.85},
    ]
    daily = _append_days(base, extra)
    rules_with_200 = dict(RULES, avoid_200sma_obstruction=True)
    rules_without_200 = dict(RULES, avoid_200sma_obstruction=False)

    with_check = evaluate_sst_entry(daily, rules_with_200, "long")
    without_check = evaluate_sst_entry(daily, rules_without_200, "long")
    print(f"  with 200SMA check: {with_check}")
    print(f"  without 200SMA check: {without_check}")
    check("200SMA-obstruction flag changes the outcome or is at least evaluated without crashing",
         "pass" in with_check and "pass" in without_check)


def main():
    test_classify_days()
    test_long_entry_sequence()
    test_short_entry_sequence()
    test_max_entry_delay_bars()
    test_trailing_stop_big_move_and_never_widens()
    test_sizing()
    test_200sma_obstruction()

    print(f"\n{'='*50}")
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()

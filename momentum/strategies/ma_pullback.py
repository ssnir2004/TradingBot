"""Strategy C - MAPullback9ema (G18).

When a Bull Flag (Strategy B) takes too long to resolve and instead drifts
sideways into the 9 EMA. Lower conviction than a clean flag - the sideways
action already shows some weakness - so every signal is tagged
conviction="low" (never dropped: G18's explicit note).

Precondition : a REAL recent impulse (>= min_impulse_pct over the last
               impulse_lookback_candles), then a TIGHT sideways stretch of
               >= sideways_min_candles (its own high-low range capped at
               sideways_max_range_pct of price - this is what "stalled",
               not "still moving", means) that did NOT break out, and the
               most recent candle tapped the 9 EMA (its low within
               tap_tol_cents of it).
Fire (a)     : the first closed candle to make a new high after the tap.
Fire (b)     : if a prior candle already made a new high but faked out
               (closed back below the flag top), the break of the flag top
               per G14 - i.e. this degrades to a flat-top break.
Entry ref    : the new-high candle's high (a), or the flag top (b).

Tightened 2026-09-10: the original version scoped "the impulse" to
EVERYTHING before the sideways window (any prior bar) and never checked
the sideways window's own range, so a normal intraday grind - any 4
candles that simply didn't make a new high - satisfied it constantly
(347/488 signals, 71%, in a same-day backfill). Both gates below exist to
make "stalled flag" mean something narrower than "recent 20 minutes".
"""
from __future__ import annotations

from momentum import features as F
from momentum.signals import Signal
from momentum.strategies.base import DetectContext, Detector


class MAPullback9ema(Detector):
    key = "C"
    name = "MAPullback9ema"

    def evaluate(self, ctx: DetectContext) -> Signal | None:
        sc = self._strat_cfg(ctx)
        p = ctx.cfg["patterns"]
        s = ctx.session_5m
        need = sc["sideways_min_candles"] + sc["impulse_lookback_candles"] + 1
        if len(s) < need:
            return None

        ema9 = F.ema_last(s, sc["ma_period"], sc["ma_price"])
        tap_tol = sc["tap_tol_cents"] / 100.0
        last = s.iloc[-1]
        prev = s.iloc[-2]

        # sideways stretch = the last N candles, required to actually be a
        # tight stall (own range capped at sideways_max_range_pct) that did
        # not make a decisive new high until now
        sideways = s.iloc[-(sc["sideways_min_candles"] + 1):-1]
        flag_top = float(sideways["High"].max())
        flag_low = float(sideways["Low"].min())
        mid_price = float(sideways["Close"].iloc[-1])
        if mid_price <= 0 or (flag_top - flag_low) / mid_price * 100.0 > sc["sideways_max_range_pct"]:
            return None  # still moving, not stalled - Strategy B's territory, not this one's

        # impulse = a BOUNDED, recent lookback right before the stall - not
        # "anything before it" - required to have actually run up by
        # min_impulse_pct (a real move to stall out of, not just noise)
        impulse_start = max(0, len(s) - sc["sideways_min_candles"] - 1 - sc["impulse_lookback_candles"])
        impulse = s.iloc[impulse_start: len(s) - sc["sideways_min_candles"] - 1]
        if impulse.empty:
            return None
        impulse_open = float(impulse["Open"].iloc[0])
        impulse_high = float(impulse["High"].max())
        if impulse_open <= 0 or (impulse_high - impulse_open) / impulse_open * 100.0 < sc["min_impulse_pct"]:
            return None
        if flag_top > impulse_high * 1.001:   # it already broke out - that's Strategy B's job
            return None

        # tap: the MOST RECENT (last sideways) candle's low came within
        # tap_tol of the 9 EMA - not "sometime in the last N candles"
        tapped = float(sideways["Low"].iloc[-1]) <= ema9 + tap_tol
        if not tapped:
            return None

        # (a) first new high after the tap
        fresh = p["fresh_break_max_cents"] / 100.0
        made_new_high = float(last["High"]) > float(prev["High"]) and F.is_green(last)
        if (made_new_high and float(last["High"]) >= flag_top - 1e-9
                and ctx.price >= float(last["High"]) - fresh):
            return self._mk_signal(
                ctx, entry_ref=float(last["High"]), conviction=sc["conviction"],
                features={"variant": "new_high_after_tap", "ema9": round(ema9, 4),
                          "flag_top": round(flag_top, 4)},
            )

        # (b) faked-out earlier -> flat-top break
        faked = bool((sideways["High"] >= flag_top - 1e-9).any()
                     and float(prev["Close"]) < flag_top)
        if faked and F.broke_level(ctx.price, flag_top, p["break_confirm_mode"],
                                   p["break_buffer_cents"], float(last["Close"])):
            return self._mk_signal(
                ctx, entry_ref=flag_top, conviction=sc["conviction"],
                features={"variant": "flat_top_break_after_fakeout", "ema9": round(ema9, 4),
                          "flag_top": round(flag_top, 4)},
            )
        return None

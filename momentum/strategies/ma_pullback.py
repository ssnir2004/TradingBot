"""Strategy C - MAPullback9ema (G18).

When a Bull Flag (Strategy B) takes too long to resolve and instead drifts
sideways into the 9 EMA. Lower conviction than a clean flag - the sideways
action already shows some weakness - so every signal is tagged
conviction="low" (never dropped: G18's explicit note).

Precondition : an impulse, then a sideways stretch of >= sideways_min_candles
               that did NOT break out, and price has come down to tap the
               9 EMA (a candle low within tap_tol_cents of it, or wicking it).
Fire (a)     : the first closed candle to make a new high after the tap.
Fire (b)     : if a prior candle already made a new high but faked out
               (closed back below the flag top), the break of the flag top
               per G14 - i.e. this degrades to a flat-top break.
Entry ref    : the new-high candle's high (a), or the flag top (b).
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
        need = sc["sideways_min_candles"] + 3
        if len(s) < need:
            return None

        ema9 = F.ema_last(s, sc["ma_period"], sc["ma_price"])
        tap_tol = sc["tap_tol_cents"] / 100.0
        last = s.iloc[-1]
        prev = s.iloc[-2]

        # sideways stretch = the last N candles held a tight range and did
        # not make a decisive new high until now
        sideways = s.iloc[-(sc["sideways_min_candles"] + 1):-1]
        flag_top = float(sideways["High"].max())
        base_before = s.iloc[:-(sc["sideways_min_candles"] + 1)]
        if base_before.empty:
            return None
        impulse_high = float(base_before["High"].max())
        if flag_top > impulse_high * 1.001:   # it already broke out - that's Strategy B's job
            return None

        # tap: a recent candle's low came within tap_tol of the 9 EMA
        recent = s.iloc[-(sc["sideways_min_candles"] + 1):]
        tapped = bool((recent["Low"] <= ema9 + tap_tol).any())
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

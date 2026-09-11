"""Strategy D - Setup1234 / pivot break (G19).

Pattern, using fractal swing points on the entry timeframe:
  point 1 : a swing low  (the first pullback)
  point 2 : the next swing high (the small move higher) - its high is the PIVOT
  point 3 : a higher swing low (second pullback; must be above point 1 when
            require_higher_low is set)
  point 4 : price breaks the pivot (point 2's high)

Fire  : the pivot breaks per G14, with the 1-2-3 structure formed within
        the last max_base_candles bars.
Entry ref = the pivot level.
Stop   : inherits the shared 10c stop (stop_mode="shared"); "pivot_low"
         would place it under point 3 instead (not used in v1).
"""
from __future__ import annotations

from momentum import features as F
from momentum.signals import Signal
from momentum.strategies.base import DetectContext, Detector


class Setup1234(Detector):
    key = "D"
    name = "Setup1234"

    def evaluate(self, ctx: DetectContext) -> Signal | None:
        sc = self._strat_cfg(ctx)
        p = ctx.cfg["patterns"]
        fractal = sc["swing_fractal_bars"]
        s = ctx.session_5m
        if len(s) < sc["max_base_candles"]:
            base = s
        else:
            base = s.iloc[-sc["max_base_candles"]:]
        if len(base) < 4 * fractal + 2:
            return None

        lows = F.swing_lows(base, fractal)
        highs = F.swing_highs(base, fractal)
        if len(lows) < 2 or not highs:
            return None

        p1 = lows[-2]
        p3 = lows[-1]
        # the pivot swing-high must sit between p1 and p3
        between = [h for h in highs if p1 < h < p3]
        if not between:
            return None
        p2 = between[-1]

        p1_low = float(base["Low"].iloc[p1])
        p3_low = float(base["Low"].iloc[p3])
        pivot = float(base["High"].iloc[p2])

        if sc["require_higher_low"] and not (p3_low > p1_low):
            return None
        if not (pivot > p1_low and pivot > p3_low):
            return None

        last_close = float(s["Close"].iloc[-1])
        # price must not have already run well past the pivot (stale break)
        if ctx.price - pivot > p["fresh_break_max_cents"] / 100.0:
            return None
        if not F.broke_level(ctx.price, pivot, p["break_confirm_mode"],
                             p["break_buffer_cents"], last_close):
            return None

        entry_ref = pivot
        sig = self._mk_signal(
            ctx, entry_ref=entry_ref,
            features={
                "point1_low": round(p1_low, 4),
                "point2_pivot": round(pivot, 4),
                "point3_low": round(p3_low, 4),
                "base_candles": len(base),
            },
        )
        if sc["stop_mode"] == "pivot_low":
            sig.features["stop_override"] = round(p3_low, 4)
        return sig

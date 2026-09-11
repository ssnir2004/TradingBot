"""Strategy A - WholeHalfDollarBreak (G16).

A surging low-float stock approaches a half- or whole-dollar level
(e.g. 4.90 -> 5.00, 5.30 -> 5.50). Momentum traders' resting orders cluster
at those round numbers, so a clean break through one tends to run.

Arm  : last price is within arm_distance_cents *below* a whole/half level,
       and that level sits within require_within_hod_pct of the session HOD
       (so we're breaking near the highs, not some level mid-range).
Fire : the level breaks per G14 (buffer_cents through it, or a 5m close
       above it).
"""
from __future__ import annotations

from momentum import features as F
from momentum.signals import Signal
from momentum.strategies.base import DetectContext, Detector


class WholeHalfDollarBreak(Detector):
    key = "A"
    name = "WholeHalfDollarBreak"

    def evaluate(self, ctx: DetectContext) -> Signal | None:
        sc = self._strat_cfg(ctx)
        p = ctx.cfg["patterns"]
        price = ctx.price
        arm = sc["arm_distance_cents"] / 100.0
        if len(ctx.session_5m) < 2:
            return None
        last_close = float(ctx.session_5m["Close"].iloc[-1])
        prior_close = float(ctx.session_5m["Close"].iloc[-2])
        recent_low = float(ctx.session_5m["Low"].iloc[-4:].min())

        # levels straddling the current price, closest first
        for kind, level in sorted(F.round_levels_near(price, sc["levels"], arm + 0.10),
                                  key=lambda kv: abs(kv[1] - price)):
            # the level must be one price is breaking UPWARD through: price
            # was below it very recently, and is now at/above it.
            was_below = recent_low < level or prior_close < level
            if not was_below or price < level:
                continue
            # only a *fresh* break - price not already far above the level
            if price - level > arm:
                continue
            # break must be near the highs (G16)
            if ctx.hod > 0 and abs(level - ctx.hod) / ctx.hod * 100.0 > sc["require_within_hod_pct"]:
                continue
            # G14 confirmation
            if not F.broke_level(price, level, p["break_confirm_mode"],
                                 p["break_buffer_cents"], last_close):
                continue
            return self._mk_signal(
                ctx, entry_ref=level,
                features={"level_kind": kind, "level": level,
                          "dist_to_hod_pct": (abs(level - ctx.hod) / ctx.hod * 100.0) if ctx.hod else None},
            )
        return None

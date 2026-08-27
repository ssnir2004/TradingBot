# ORB — Opening Range Breakout (הגדרת אסטרטגיה, שלב 1)

מקור: תמלול סרטון YouTube ([bITIVwysCzM](https://www.youtube.com/watch?v=bITIVwysCzM)) — "This 1 Minute
Scalping Strategy Works Everyday". זהו **מסמך הגדרה בלבד** — אין כאן עדיין שינוי לקוד. המטרה: לסגור
מפרט מדויק (rules_json) לפני מעבר לשלב 2 (מימוש ב-`cycle.py` + backtest).

שתי אסטרטגיות תאומות, בדיוק כמו הזוגות הקיימים (`Long Breakout Conservative` / `Short Breakdown
Conservative`): **ORB Long** ו-**ORB Short**, מראה-הפוכה זו של זו.

## הרעיון (מהסרטון)

לא סוחרים את טווח היום המלא — רק את הנר הראשון של פתיחת ה-New York session (9:30 ET), הנקרא
"opening range". שלושה שלבים מכניים, בלי daily bias ובלי אינדיקטורים מסובכים:

1. סמן High/Low של נר ה-**15 דקות** הראשון מ-9:30 ET → זה ה-OR (Opening Range).
2. חכה לסגירת נר **5 דקות** מעל ה-OR High (לונג) / מתחת ל-OR Low (שורט) — אישור.
3. רד ל-**1 דקה** וחפש כניסה להמשך התנועה.

שלושה מודלים לכניסה: **Breakout** (עם gap/displacement), **Retest** (המועדף על המרצה), **Reversal**
(כשה-OR נכשל וה-price חוזר ושובר לכיוון ההפוך).

## החלטות שנסגרו איתך

| נושא | החלטה |
|---|---|
| מודלי כניסה | שלושתם: Breakout + Retest + Reversal |
| מסגרות זמן | OR על 15m, אישור על 5m, כניסה על 1m |
| מחיר מינימלי | $3 (כמו הבסיס הקיים) |
| ATR% | סף שונה למדרגת מחיר (לא סף אחיד) |
| יקום | S&P 500 בלבד |
| Market Cap | ≥ $1B |
| RVOL | ≥ 2.0 |

## מדרגות ATR% (הצעה — טעונה אישור)

`ATR% = ATR(14) יומי / מחיר נוכחי * 100`. במקום סף אחיד, סף יורד ככל שהמניה יקרה יותר (מניה יקרה
זזה פחות באחוזים בממוצע, אז דורשים ממנה פחות כדי להיחשב "בתנועה"):

| טווח מחיר | ATR% מינימלי |
|---|---|
| $3 – $20 | 4.0% |
| $20 – $50 | 3.0% |
| $50 – $100 | 2.0% |
| מעל $100 | 1.5% |

אלו מספרים התחלתיים סבירים, לא מבוססי backtest — לשנות בקלות לפני/אחרי הרצת בדיקות.

## מפרט מלא — ORB Long

```json
{
  "strategy_name": "ORB Long (Opening Range Breakout)",
  "direction": "long_only",

  "opening_range": {
    "or_timeframe": "15m",
    "confirm_timeframe": "5m",
    "entry_timeframe": "1m",
    "session": "new_york",
    "session_open_et": "09:30"
  },

  "universe_filters": {
    "index": "S&P 500",
    "min_price_usd": 3.0,
    "min_market_cap_usd": 1000000000
  },

  "volatility_filters": {
    "V1_rvol_min": 2.0,
    "V1_rvol_lookback_days": 14,
    "V2_atr_period": 14,
    "V2_atr_pct_tiers": [
      {"price_min": 3.0,   "price_max": 20.0,  "atr_pct_min": 4.0},
      {"price_min": 20.0,  "price_max": 50.0,  "atr_pct_min": 3.0},
      {"price_min": 50.0,  "price_max": 100.0, "atr_pct_min": 2.0},
      {"price_min": 100.0, "price_max": null,  "atr_pct_min": 1.5}
    ]
  },

  "entry_models": {
    "breakout": {
      "enabled": true,
      "trigger": "5m_close_above_or_high",
      "confirmation": "1m_bullish_displacement_gap",
      "stop_rule": "below_gap_candle_low",
      "target_rr": 2.0
    },
    "retest": {
      "enabled": true,
      "trigger": "5m_close_above_or_high",
      "confirmation": "1m_retest_of_or_high_holds",
      "stop_rule": "below_retest_swing_low",
      "target_rr": 2.0
    },
    "reversal": {
      "enabled": true,
      "trigger": "5m_close_below_or_low_then_1m_reclaim_above_or_high",
      "confirmation": "1m_retest_of_order_block_holds",
      "stop_rule": "below_order_block_low",
      "target": "high_of_day"
    }
  },

  "time_filter": {
    "earliest_entry_et": "09:50",
    "latest_entry_et": "11:30",
    "force_close_et": "15:51"
  },

  "exit": {
    "management_style": "fixed_target_no_trail",
    "note": "אין breakeven/trailing כמו באסטרטגיות הקיימות - יעד קבוע R:R לפי entry_models, יציאה מלאה ביעד או בסטופ"
  },

  "risk": {
    "max_risk_per_trade_pct": 1.0,
    "max_position_size_pct_of_portfolio": 10,
    "max_concurrent_positions": 5
  }
}
```

## מפרט מלא — ORB Short (מראה הפוכה)

```json
{
  "strategy_name": "ORB Short (Opening Range Breakdown)",
  "direction": "short_only",

  "opening_range": {
    "or_timeframe": "15m",
    "confirm_timeframe": "5m",
    "entry_timeframe": "1m",
    "session": "new_york",
    "session_open_et": "09:30"
  },

  "universe_filters": {
    "index": "S&P 500",
    "min_price_usd": 3.0,
    "min_market_cap_usd": 1000000000
  },

  "volatility_filters": {
    "V1_rvol_min": 2.0,
    "V1_rvol_lookback_days": 14,
    "V2_atr_period": 14,
    "V2_atr_pct_tiers": [
      {"price_min": 3.0,   "price_max": 20.0,  "atr_pct_min": 4.0},
      {"price_min": 20.0,  "price_max": 50.0,  "atr_pct_min": 3.0},
      {"price_min": 50.0,  "price_max": 100.0, "atr_pct_min": 2.0},
      {"price_min": 100.0, "price_max": null,  "atr_pct_min": 1.5}
    ]
  },

  "entry_models": {
    "breakout": {
      "enabled": true,
      "trigger": "5m_close_below_or_low",
      "confirmation": "1m_bearish_displacement_gap",
      "stop_rule": "above_gap_candle_high",
      "target_rr": 2.0
    },
    "retest": {
      "enabled": true,
      "trigger": "5m_close_below_or_low",
      "confirmation": "1m_retest_of_or_low_holds",
      "stop_rule": "above_retest_swing_high",
      "target_rr": 2.0
    },
    "reversal": {
      "enabled": true,
      "trigger": "5m_close_above_or_high_then_1m_reclaim_below_or_low",
      "confirmation": "1m_retest_of_order_block_holds",
      "stop_rule": "above_order_block_high",
      "target": "low_of_day"
    }
  },

  "time_filter": {
    "earliest_entry_et": "09:50",
    "latest_entry_et": "11:30",
    "force_close_et": "15:51"
  },

  "exit": {
    "management_style": "fixed_target_no_trail",
    "note": "אין breakeven/trailing כמו באסטרטגיות הקיימות - יעד קבוע R:R לפי entry_models, יציאה מלאה ביעד או בסטופ"
  },

  "risk": {
    "max_risk_per_trade_pct": 1.0,
    "max_position_size_pct_of_portfolio": 10,
    "max_concurrent_positions": 5
  }
}
```

## הערות הנדסיות לשלב 2 (מימוש — עדיין לא בוצע)

מנוע ה-filters הקיים ב-`cycle.py` (D1-D3 / I1-I3) בנוי סביב **מסגרת זמן אחת** (`trade_timeframe`)
ובדיקת daily bias מהיום הקודם. ORB שונה מהותית:

1. **Multi-timeframe אמיתי** — צריך לעקוב אחרי 3 מסגרות זמן במקביל (15m ל-OR, 5m לאישור, 1m
   לכניסה) באותו יום מסחר, לא רק filter בודד על bar אחד. זה מנגנון חדש, לא הרחבה של D1-D3/I1-I3.
2. **Displacement/gap detection** — זיהוי "bullish/bearish gap" בין נרות 1 דקה (המרצה קורא לזה
   displacement) צריך היגיון חדש, לא קיים היום בקוד.
3. **Retest detection** — זיהוי חזרה (pullback) לרמת ה-OR שנפרצה ואישור החזקה שלה.
4. **Order block / reversal detection** — המורכב מבין השלושה: זיהוי swing high/low, break of
   structure, ו"order block" (הנר האחרון בכיוון ההפוך לפני התנועה) — טעון הגדרה מדויקת יותר גם
   מבחינה אלגוריתמית לפני מימוש.
5. **RVOL + ATR% כבר קיימים בקוד** (`I3_rvol_min` וכו') — אלה ניתנים לשימוש חוזר. שדה `min_market_cap_usd`
   גם כבר קיים (בפריסט `Long Breakout NASDAQ Beta`). ATR% מדורג לפי מדרגת מחיר הוא שדה חדש.
6. **מודל היציאה שונה מכל האסטרטגיות הקיימות** — קבוע R:R (target_rr) בלי breakeven/trailing,
   לעומת המנגנון הקיים (partial + breakeven + trailing stop). זה גם דורש קוד יציאה נפרד.

כשתאשר את המפרט (או תבקש שינויים במספרים/במודלים), אפשר לעבור לשלב 2: מימוש ב-`cycle.py` +
backtest על נתונים היסטוריים לפני העלאה ל-paper/live.

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
3. חפש כניסה להמשך התנועה **על אותה מסגרת 5 דקות** (במקור בסרטון: ירידה ל-1 דקה לכניסה — הוחלף
   בפשרה בגלל זמינות נתונים, ראה החלטות למטה).

שני מודלים לכניסה: **Breakout** (עם gap/displacement) ו-**Retest** (המועדף על המרצה). Reversal
הוסר מהיקף שלב 1 (ראה החלטות למטה) — אפשר להוסיף בעתיד כשלב נפרד.

## החלטות שנסגרו איתך (סופי)

| נושא | החלטה |
|---|---|
| מודלי כניסה | **Breakout + Retest בלבד** — Reversal ירד מהיקף שלב 1 |
| מסגרות זמן | OR על 15m, אישור **וגם כניסה** על 5m (לא 1m — ראה "נתוני 1m" למטה) |
| מחיר מינימלי | $3 (כמו הבסיס הקיים) |
| ATR% | מדרגות לפי מחיר, כמוצע למטה — **אושר** |
| יקום | S&P 500 בלבד |
| Market Cap | ≥ $1B |
| RVOL | ≥ 2.0 |
| חלון כניסות | 09:50–11:30 ET (שעתיים ראשונות) — **אושר** |
| נתוני 1m | **אין** cache ל-1 דקה (התשתית בנויה על 5m בלבד) — הוחלט על **פשרה**: הכניסה בפועל תהיה על סגירת נר 5 דקות במקום 1 דקה כמו בסרטון המקורי, כדי להישאר תואמים לתשתית הקיימת בלי לבנות fetch/cache חדש |

## מדרגות ATR% (אושר)

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
    "entry_timeframe": "5m",
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
      "confirmation": "5m_bullish_displacement_gap",
      "stop_rule": "below_gap_candle_low",
      "target_rr": 2.0
    },
    "retest": {
      "enabled": true,
      "trigger": "5m_close_above_or_high",
      "confirmation": "5m_retest_of_or_high_holds",
      "stop_rule": "below_retest_swing_low",
      "target_rr": 2.0
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
    "entry_timeframe": "5m",
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
      "confirmation": "5m_bearish_displacement_gap",
      "stop_rule": "above_gap_candle_high",
      "target_rr": 2.0
    },
    "retest": {
      "enabled": true,
      "trigger": "5m_close_below_or_low",
      "confirmation": "5m_retest_of_or_low_holds",
      "stop_rule": "above_retest_swing_high",
      "target_rr": 2.0
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

1. **Multi-timeframe** — צריך לעקוב אחרי 2 מסגרות זמן במקביל (15m ל-OR, 5m לאישור+כניסה) באותו
   יום מסחר, לא רק filter בודד על bar אחד. הודות לפשרה על 1m (ראה טבלת ההחלטות למעלה), כל הנתונים
   הנדרשים כבר קיימים ב-cache הקיים (`BAR_SIZE = "5 mins"` ב-`fetch_backtest_data.py` /
   `src/backtest_engine.py`) — 15m נבנה מצירוף שלושה נרות 5m, אין צורך בשום fetch/cache חדש. זה
   עדיין מנגנון חדש (לא הרחבה ישירה של D1-D3/I1-I3), אבל בלי חסם נתונים.
2. **Displacement/gap detection** — זיהוי "bullish/bearish gap" בין נרות 5 דקות (המרצה קורא לזה
   displacement) צריך היגיון חדש, לא קיים היום בקוד.
3. **Retest detection** — זיהוי חזרה (pullback) לרמת ה-OR שנפרצה ואישור החזקה שלה, על נרות 5 דקות.
4. **RVOL + ATR% כבר קיימים בקוד** (`I3_rvol_min` וכו') — אלה ניתנים לשימוש חוזר. שדה `min_market_cap_usd`
   גם כבר קיים (בפריסט `Long Breakout NASDAQ Beta`). ATR% מדורג לפי מדרגת מחיר הוא שדה חדש.
5. **מודל היציאה שונה מכל האסטרטגיות הקיימות** — קבוע R:R (target_rr) בלי breakeven/trailing,
   לעומת המנגנון הקיים (partial + breakeven + trailing stop). זה גם דורש קוד יציאה נפרד.
6. **פשרת 1m→5m משנה את דיוק הכניסה בפועל** — כניסה "על סגירת 5 דקות" תמיד תהיה מרוחקת יותר
   מהרמה שנפרצה מאשר כניסה על 1 דקה (פחות דיוק, R:R בפועל מעט שונה מהמתוכנן). שווה לזכור את זה
   כשמנתחים תוצאות backtest מול הציפיות מהסרטון המקורי.

כשתאשר את המפרט (או תבקש שינויים במספרים/במודלים), אפשר לעבור לשלב 2: מימוש ב-`cycle.py` +
backtest על נתונים היסטוריים לפני העלאה ל-paper/live.

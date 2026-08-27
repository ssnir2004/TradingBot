# ORB — Opening Range Breakout

מקור: תמלול סרטון YouTube ([bITIVwysCzM](https://www.youtube.com/watch?v=bITIVwysCzM)) — "This 1 Minute
Scalping Strategy Works Everyday".

**שלב 1 (הגדרה) ✅ ושלב 2 (מימוש) ✅ הושלמו.** ORB Long/ORB Short רשומות ב-`EXTRA_STRATEGY_PRESETS`
(`src/db.py`), עם מנוע הערכה משלהן ב-`src/orb.py` (מחובר גם ל-`cycle.py` החי וגם ל-`src/
backtest_engine.simulate_orb_strategy` לבקטסט) - לא פעילות אצל אף חשבון כברירת מחדל (דורש הפעלה
ידנית מהדשבורד, וגם הקלדת אישור בגלל דירוג `aggressive`). ראו "מה מומש בשלב 2" בתחתית הקובץ.

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

## מה מומש בשלב 2

- **`src/orb.py`** (חדש) — כל הלוגיקה הטהורה: `compute_opening_range` (15m מ-3 נרות 5m),
  `evaluate_orb_entry` (הפילטרים + שני מודלי הכניסה, מקביל ל-`cycle._evaluate_filters_from_bars`
  אבל למנוע ORB עצמאי — בלי daily_filters/D1-D3 בכלל), `fixed_target_decision` (יציאה ביעד קבוע).
  עצמאי לגמרי מ-`cycle.py` (אין import הפוך) כדי למנוע circular import ולשמור על יכולת בדיקה
  מבודדת. יש כפילות מכוונת וקטנה מול `cycle._compute_atr`/ה-RVOL של I3 (במקום לשנות קוד production
  קיים כדי לחלוק אותם).
- **`cycle.py`**: `entry_scan` מפנה ל-`_evaluate_orb_entry` (wrapper חי, אותה צורת fetch כמו
  `_evaluate_entry_filters`) כל אימת ש-`"opening_range" in rules`; ה-stop נלקח ישירות מהאיתות עצמו
  (לא מ-`INITIAL_STOP_RULES`). `manage_position` מקבל branch חדש: `exit.management_style ==
  "fixed_target_no_trail"` מדלג לגמרי על breakeven/trailing ובודק רק אם המחיר הגיע ליעד
  (`orb.fixed_target_decision`) — אם כן, סוגר את כל הפוזיציה במחיר שוק (אותו מנגנון fallback
  ל-delayed-fill כמו `force_close_all`). ה-stop ה"רגיל" עדיין מטופל ע"י ההזמנה האמיתית שהוצבה
  אצל הברוקר בכניסה, כמו בכל אסטרטגיה אחרת.
- **`src/db.py`**: `ORB Long (Opening Range Breakout)` ו-`ORB Short (Opening Range Breakdown)`
  נוספו ל-`EXTRA_STRATEGY_PRESETS`, דירוג `aggressive` (דורש הקלדת אישור, לא פעילות כברירת מחדל).
  טבלת `positions` קיבלה עמודה חדשה `target_price` (migration + עדכון `upsert_position`) לשמירת
  היעד הקבוע לכל פוזיציית ORB.
- **`src/custom_universes.py` + `build_custom_universe.py`**: יקום חדש `sp500_marketcap_1b`
  (S&P 500 מ-`src/sp500_tickers.py`, סינון Market Cap בלבד — בלי דרישת beta/דירוג אנליסטים כמו
  `ixic_large_beta_buy`). `build_custom_universe.py` הוכלל כך שכל יקום שומר את פרמטרי הסינון
  המוגדרים-לו-עצמו (`default_min_market_cap/beta/recommendation_mean` ב-`CUSTOM_UNIVERSES`) במקום
  שכולם ישתמשו באותם ברירות מחדל גלובליות; `run_service.py`'s השבועי שכבר רץ על כל יקום נשאר ללא
  שינוי במבנה, רק מעביר `None` כדי לתת לכל יקום להשתמש בברירת המחדל שלו.
- **`src/backtest_engine.py`**: `simulate_orb_strategy` חדש — לולאת יום/בר עצמאית (לא לוגיקה
  משותפת עם `simulate_strategy` הקיים, כדי לא לגעת בקוד ה-production העובד): כניסות דרך
  `orb.evaluate_orb_entry`, יציאות stop-או-target (בלי state machine של breakeven). `filter_stats`
  משתמש במפתחות אבחוניים משלו (`or_formed`/`confirmed`/`volatility_ok`) במקום D1-I3.
  `src/backtest_runner.py` מפנה לפי `"opening_range" in rules`. `web/templates/backtest.html`
  עודכן להציג את מפתחות ה-filter_stats בצורה דינמית (לא רשימת D1-I3 קשיחה) ותווית exit_reason
  חדשה ("target").
- **נבדק**: יחידה (opening range, breakout/retest לונג ושורט, דחיית volatility, fixed target
  decision) + בקטסט סינתטי מקצה-לקצה (entry breakout → יציאה ביעד, דרך `perf.pair_trades`/
  `aggregate`) + migration מלא של ה-DB על בסיס נקי. **לא נבדק**: הרצה אמיתית מול IBKR/yfinance
  חיים (paper trading) — זה השלב הבא לפני כל שיקול LIVE.

## מה עוד פתוח (לא נפתר, מתועד בכוונה)

- **פשרת 1m→5m** עדיין קיימת ומתועדת בקוד עצמו (`src/orb.py`'s docstring) — כניסה על סגירת 5
  דקות פחות מדויקת מהסרטון המקורי (שרוצה כניסה על 1 דקה), וה-R:R בפועל עלול להיות שונה מהמתוכנן.
- **מגבלת התזמון בין live לבקטסט** (ראו `src/orb.py`'s docstring "KNOWN LIMITATION") — מודל
  ה-breakout דורש להיות מוערך בדיוק על נר האישור עצמו; live מריץ tick בזמן אמת אחרי שהנר נסגר,
  בעוד שהבקטסט מבקר את הנר בדיוק בזמן ה-label שלו — מקרה קצה נדיר (הנר הראשון האפשרי לאישור, 9:45)
  עלול "להיחסם" ב-`earliest_entry_et` בבקטסט אך לא ב-live.
- **הבקטסט לא מחיל את `custom_universe` של האסטרטגיה** — מגבלה קיימת מראש בפרויקט (גם
  `Long Breakout NASDAQ Beta` לא מוגבל ל-universe שלו בבקטסט), לא ספציפית ל-ORB, ולא תוקנה כאן.
- **אין paper/live run אמיתי עדיין** — השלב הבא, לפני כל שיקול הפעלה.

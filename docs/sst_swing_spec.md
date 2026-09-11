# SST Swing — Daily-Chart Swing Strategy

מקור: פרומפט שהוגדר במלואו ע"י המשתמש (SMA50 + DMI(8) + significant/inside days + trailing stop +
watchlist screener נפרד) — לא סרטון/קורס כמו ORB/Momentum. שש חוקים מדויקים: 4 כניסה, 2 סיכון/יציאה.
"Implement them precisely as specified — do not 'improve' or add extra filters unless I ask."

**הושלם במלואו ונדחף ל-`origin`.** לא פעילה אצל אף חשבון כברירת מחדל — כמו כל אסטרטגיה אחרת,
דורשת יצירה + הפעלה ידנית מהדשבורד (`strategy_run.run_mode` מתחיל ב-`"off"`). שירות ה-trading-bot-
live עדיין **לא הופעל מחדש** מאז השינויים ב-`run_service.py` — עד שיופעל מחדש, ה-jobs החדשים
(`sst_daily_scan`, `sst_watchlist_scan`) לא רצים בפועל, למרות שהקוד כבר בשרת. ראו "מה עוד פתוח" בתחתית.

## הרעיון (מהמקור)

לא אינטראדיי — פוזיציה מוחזקת ימים-שבועות, מוערכת מחדש פעם ביום בלבד, בסגירת נר יומי:

1. **מגמה (Rule 2):** `close > SMA50` → לונג בלבד; `close < SMA50` → שורט בלבד; קרוב מדי ל-SMA50 →
   בלי כניסה בכלל.
2. **"יום משמעותי" / "יום פנימי":** יום משמעותי מרחיב את הטווח של היום הקודם (high או low חדשים);
   יום פנימי לא — **מתעלמים מימים פנימיים לגמרי** בכל לוגיקת ה-trigger/stop, כאילו לא היו.
3. **טריגר DMI (Rule 3):** `+DI`/`-DI` (תקופה 8, **לא** 14 הרגיל) חוצים זה את זה, או מתכנסים לנגיעה
   ואז מתפצלים שוב. רק טריגרים בכיוון המגמה נחשבים.
4. **אישור מחיר (Rule 4, הכניסה בפועל):** אחרי טריגר DMI תקף, המחיר צריך לפרוץ את השיא/שפל של היום
   המשמעותי האחרון (בלי לרדוף פריצה ישנה מדי — `max_entry_delay_bars`, ברירת מחדל 1).
5. **סטופ ראשוני (Rule 5):** מתחת/מעל היום המשמעותי ששימש לאישור הכניסה, + `stop_buffer_pct` (0.5%
   ברירת מחדל).
6. **Trailing stop (Rule 6):** כל יום משמעותי חדש לטובת העסקה מזיז את הסטופ. חריגת יום-מהלך-גדול
   (`big_move_threshold_pct`, 5% ברירת מחדל): באותו יום, הרף הוא אמצע הטווח במקום הקצה המלא. אכיפה
   קשיחה בקוד: הסטופ **לעולם לא מתרחב**, רק מתהדק.

בנוסף: מודול סינון-יקום **נפרד** (לא חלק מלוגיקת הכניסה/יציאה) — רץ שבועית, בונה watchlist לפי noise
score, step-regularity, קורלציה למדד ייחוס (SPY), רצפת נזילות, רצפת מחיר.

## G-SST-1 עד G-SST-7 — כל ה-GAPs שסגרנו איתך

| # | נושא | החלטה |
|---|---|---|
| G-SST-1 | דיוק DMI trigger ("touch ואז diverge") | `dmi_touch_tolerance: 2.0` (נקודות ±DI), `dmi_diverge_confirm_bars: 2` |
| G-SST-2 | "המחיר בעצם ב-SMA50" | `sma50_neutral_band_pct: 0.5` — טווח ±0.5% מה-SMA50 = בלי כניסה |
| G-SST-3 | נקודת ייחוס ל-`max_entry_delay_bars` | סופרים מיום **הפריצה בפועל** (לא מיום ה-DMI trigger) |
| G-SST-4 | סינון היקום — חלונות וספים | ראו טבלה נפרדת למטה |
| G-SST-5 | מרקר ה-dispatch | `"strategy_type": "sst_swing"` **מפורש** (לא ניחוש-לפי-צורה כמו ORB/Touch&Turn/Breakout) |
| G-SST-6 | תזמון ההערכה | Job נפרד, יומי ~09:35 ET (לא כל דקה כמו המחזור הרגיל) — `sst_swing_live.py` |
| G-SST-7 | לוג סיגנלים שלא נלקחו | דרך `db.log_decision` הקיים, לא טבלה נפרדת |

### G-SST-4 — פרטי סינון היקום

| פרמטר | ערך |
|---|---|
| `noise_lookback_days` | 60 |
| `noise_max_score` | אין סף קשיח — **דירוג בלבד** |
| `correlation_lookback_days` | 60 |
| `correlation_pass_max` | 0.3 (עובר) |
| `correlation_reject_min` | 0.5 (נפסל אוטומטית — בלי שורה בכלל) |
| בין 0.3–0.5 | `status = "review"` — דורש בדיקה ידנית, לא pass/fail אוטומטי |
| `step_regularity_lookback_days` | 60 |
| `step_regularity_min_ratio` | אין סף — דירוג בלבד |
| `liquidity_min_avg_dollar_volume` | $5,000,000/יום — **רף קשיח** |
| `price_floor_usd` | $10 — **רף קשיח** |

## פריסת הקבצים (כולם נבנו, נבדקו, נדחפו)

| קובץ | תפקיד |
|---|---|
| `src/sst_swing.py` | הליבה הטהורה — `classify_days`, `latest_dmi_trigger`, `evaluate_sst_entry`, `initial_stop_price`, `trailing_stop_update`, `size_for_risk`. אין fetch/IBKR/DB בכלל — אותו חוזה כמו `src/orb.py`. |
| `test_sst_swing.py` | בדיקות יחידה (סגנון `momentum/selftest.py`, לא pytest) — סדרות OHLC סינתטיות, לונג ושורט, `max_entry_delay_bars`, trailing עם big-move + never-widen, sizing, חסימת 200SMA. |
| `sst_watchlist_scan.py` | סינון היקום (שורש, מקביל ל-`build_custom_universe.py`) — chunked `yf.download` (25 סימבולים, בטוח-זיכרון כמו `momentum/backfill.py`), כותב ל-`sst_watchlist`. |
| `sst_swing_live.py` | ה-driver היומי (שורש, מקביל ל-`refresh_account.py`) — מחבר ל-IBKR על `SST_SWING_CLIENT_ID` (26), קורא ל-`cycle.sst_entry_scan`/`cycle.sst_manage_positions` וכו'. |
| `cycle.py` | `_fetch_sst_daily_bars`, `sst_entry_scan`/`virtual_sst_entry_scan`, `sst_manage_positions`/`sst_manage_virtual_positions`, `_is_swing_hold` + שינוי `force_close_all`, guard ב-`entry_scan`/`virtual_entry_scan`, ענף `"sst_swing_trail"` ב-`_manage_position_core`. |
| `src/db.py` | טבלת `sst_watchlist` חדשה + `replace_sst_watchlist`/`get_sst_watchlist`. **לא** נוספה עמודת `no_eod_close` כפי שתוכנן במקור — התגלה ש-`positions.strategy_id` + `cycle._rules_for_position` הקיימים כבר פותרים את זה (ראו למטה). |
| `src/backtest_engine.py` | `simulate_sst_swing_strategy` — לולאת replay יומית, multi-day-hold (הראשונה מסוגה בקובץ — כל שאר הסימולטורים סוגרים תוך יום). |
| `src/backtest_runner.py` | dispatch לפי `rules["strategy_type"] == "sst_swing"`. |
| `run_service.py` | `sst_daily_scan` (יומי, 09:35 ET, per-account) + `sst_watchlist_scan` (שבועי, יום א' 08:30 ET, admin-only). |
| `web/app.py`, `web/templates/sst_watchlist.html`, `_nav.html` | מסך דשבורד read-only לצפייה ב-watchlist. |

## שינוי עיצוב באמצע הבנייה: `force_close_all`

התוכנית המקורית שאושרה תכננה עמודת DB חדשה `no_eod_close`, נחתמת ברגע הכניסה. באמצע הבנייה התגלה
ש-`positions.strategy_id` (קיים כבר, migration ישנה) + `cycle._rules_for_position` (מחפש את ה-
`rules_json` של האסטרטגיה של הפוזיציה) כבר פותרים בדיוק את אותה בעיה — בלי migration חדשה בכלל.
המנגנון בפועל: `_is_swing_hold(pos)` ב-`cycle.py` בודק `rules.get("no_eod_force_close")` על
האסטרטגיה של הפוזיציה. **לא מתאפס אוטומטית** כמו `hold_overnight` (טוגל ידני חד-פעמי) — נשאר בתוקף
כל עוד הפוזיציה פתוחה, כי זו תכונה קבועה של האסטרטגיה, לא בקשה יומית.

## מה נבדק בפועל (בלי לגעת ב-IBKR)

- **`src/sst_swing.py`**: 7 בדיקות יחידה, כולן עוברות (2 באגים אמיתיים נמצאו ותוקנו בדרך).
- **`sst_watchlist_scan.py`**: הרצה אמיתית על 5 סימבולים אמיתיים (AAPL/ABBV/ABT/A/ABNB) — כל 4 השדות
  בטווח הגיוני, ABBV נפל נכון ל-`review` ב-`|corr|=0.32`.
- **`cycle._fetch_sst_daily_bars`**: מאמת שהיום הנוכחי (בר עדיין נבנה) נחתך נכון.
- **`cycle.sst_entry_scan`/`evaluate_sst_entry`**: זרימה מקצה-לקצה מול בארים יומיים אמיתיים
  (AAPL/NVDA/KO) — תוצאות נקיות, בלי exceptions.
- **`simulate_sst_swing_strategy`**: בקטסט אמיתי, 6 סימבולים אמיתיים, 2024–2025, לונג ושורט:
  - לונג: 94 עסקאות, win rate 46.8%, profit factor 2.08, net P&L +$84,042. עסקת NVDA: כניסה
    2024-01-08 ב-$49.46, יציאה 2024-01-23 ב-$58.67 (+18.6%, 15 ימי החזקה) — תואם את העלייה האמיתית
    של NVDA בינואר 2024.
  - שורט: 62 עסקאות, win rate 40.3%, profit factor 1.2.
  - `filter_stats` (funnel: trend → dmi_trigger → price_confirmation → 200sma_obstruction) —
    יורד מונוטונית, והמספר הסופי תואם בדיוק את מספר העסקאות.
- **`web/app.py`/`sst_watchlist.html`**: ה-app נטען נקי עם שני ה-routes החדשים רשומים; הטמפלט
  מרונדר מקצה-לקצה דרך סביבת Jinja האמיתית (כולל `_nav.html`/`_theme.html`).
- **`run_service.py`**: מיובא נקי אחרי הוספת שני ה-jobs. **לא הופעל מחדש בפועל.**

## מה עוד פתוח (לא נפתר בכוונה / ממתין להחלטה שלך)

- **השירות החי (`trading-bot-live.service`) לא הופעל מחדש** — הקוד בשרת, אבל `run_service.py`'s
  scheduler עדיין רץ עם הגרסה הישנה (בלי `sst_daily_scan`/`sst_watchlist_scan`) עד שיופעל מחדש.
  לפי המשמעת שקבענו בסשן הזה: לוודא שאין פוזיציות S&P פתוחות לפני כל restart.
- **אין paper/live run אמיתי מול IBKR עדיין** — כל מה שנבדק הוא קוד טהור + נתוני שוק אמיתיים
  (yfinance), בלי חיבור IBKR בפועל. זה השלב הבא לפני כל שיקול הפעלה.
- **צריך ליצור את אסטרטגיית ה-SST Swing בפועל** מהדשבורד (מסך Strategies) — שום rules_json לא
  נכתב ל-`strategies` table עדיין; זה מה שיקבע את `dmi_touch_tolerance`/`stop_buffer_pct`/וכו' בפועל
  (הברירות מחדל שסוכמו ב-G-SST-1..4 הן המלצה, לא נחתמו עדיין ב-DB).
- **`sst_watchlist` ריק עד ההרצה השבועית הראשונה** (יום א' 08:30 ET) — או שאפשר להריץ ידנית
  (`python sst_watchlist_scan.py`) לפני זה כדי שיהיה למה להצביע ב-entry scan הראשון.

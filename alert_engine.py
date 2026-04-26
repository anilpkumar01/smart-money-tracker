# ============================================================
# alert_engine.py — 3 alert types, Windows 11 toast + DB log
# ============================================================
# Called automatically after each stock is scraped.
# Also called manually via "Check Alerts" button in app.
#
# Alert types:
#   1. value_zone    — price drops into buy zone
#   2. fii_selling_4q — stock completes 4th consecutive FII selling quarter
#   3. score_60      — smart money score crosses 60 (grade B→A)
# ============================================================

import logging
import platform
import pandas as pd
from database import (
    save_alert, alert_already_fired, get_all_stocks, get_unacknowledged_alerts
)

log = logging.getLogger(__name__)


# ── Toast notification ────────────────────────────────────────────────────────

def _send_toast(title: str, message: str, duration: int = 8):
    """
    Send a Windows 11 toast notification.
    Falls back to console log on non-Windows or if win10toast not installed.
    """
    if platform.system() != "Windows":
        log.info(f"[ALERT] {title}: {message}")
        return

    try:
        from win10toast import ToastNotifier
        toaster = ToastNotifier()
        toaster.show_toast(
            title,
            message,
            icon_path=None,
            duration=duration,
            threaded=True,   # non-blocking
        )
    except ImportError:
        log.info(f"[ALERT — no toast available] {title}: {message}")
    except Exception as e:
        log.warning(f"Toast failed ({e}) — {title}: {message}")


# ── Alert check functions ─────────────────────────────────────────────────────

def check_value_zone_alert(row: pd.Series) -> bool:
    """
    Fires when: current price ≤ buy zone high (stock entered value zone)
    Deduplicates: won't re-fire for same stock within 48 hours.
    """
    ticker      = row.get("ticker", "")
    price       = row.get("price", 0)
    buy_high    = row.get("buy_zone_high", 0)
    buy_low     = row.get("buy_zone_low", 0)
    fair_val    = row.get("fair_value", 0)
    score       = row.get("smart_money_score", 0)
    name        = row.get("name", ticker)
    signal      = str(row.get("value_signal", ""))

    if price <= 0 or buy_high <= 0:
        return False
    if "BUY" not in signal.upper():
        return False
    if alert_already_fired(ticker, "value_zone", within_hours=48):
        return False

    discount = round((fair_val - price) / fair_val * 100, 1) if fair_val > 0 else 0
    message = (
        f"₹{price:.0f} entered value zone ₹{buy_low:.0f}–₹{buy_high:.0f}  "
        f"({discount:+.1f}% vs fair value ₹{fair_val:.0f})  "
        f"Score: {score:.0f}/100"
    )

    save_alert(ticker, name, "value_zone", message, price, score)
    _send_toast(
        f"💰 Value Zone: {name}",
        message,
        duration=10,
    )
    log.info(f"[Alert fired] value_zone — {ticker}")
    return True


def check_fii_selling_alert(row: pd.Series) -> bool:
    """
    Fires when: FII has been selling for 4+ consecutive quarters.
    Deduplicates: won't re-fire for same stock within 90 days
    (one quarter = ~90 days — only alert on new FII selling streaks).
    """
    ticker        = row.get("ticker", "")
    fii_selling   = bool(row.get("fii_selling_4q", False))
    fii_pct       = row.get("fii_pct", 0)
    fii_chg       = row.get("fii_trend_pct", 0)
    dii_buying    = bool(row.get("dii_buying_4q", False))
    score         = row.get("smart_money_score", 0)
    name          = row.get("name", ticker)
    price         = row.get("price", 0)

    if not fii_selling:
        return False
    if alert_already_fired(ticker, "fii_selling_4q", within_hours=24 * 90):
        return False

    dii_note = "✅ DII absorbing" if dii_buying else "⚠️ DII not buying"
    message = (
        f"FII stake fell {fii_chg:+.1f}% over 4 quarters → {fii_pct:.1f}% now  "
        f"{dii_note}  Score: {score:.0f}/100  ₹{price:.0f}"
    )

    save_alert(ticker, name, "fii_selling_4q", message, price, score)
    _send_toast(
        f"📡 FII Selling Signal: {name}",
        message,
        duration=10,
    )
    log.info(f"[Alert fired] fii_selling_4q — {ticker}")
    return True


def check_score_60_alert(row: pd.Series, prev_score: float = 0) -> bool:
    """
    Fires when: smart_money_score crosses 60 from below.
    Requires passing in the previous score so we only alert on the crossover,
    not on every scrape where score > 60.
    Deduplicates: 7 days.
    """
    ticker  = row.get("ticker", "")
    score   = row.get("smart_money_score", 0)
    grade   = row.get("grade", "")
    name    = row.get("name", ticker)
    price   = row.get("price", 0)
    signal  = row.get("value_signal", "")

    # Only fire on crossover
    if not (prev_score < 60 <= score):
        return False
    if alert_already_fired(ticker, "score_60", within_hours=24 * 7):
        return False

    message = (
        f"Score just crossed 60 → {score:.0f}/100 (Grade: {grade})  "
        f"Signal: {signal}  ₹{price:.0f}"
    )

    save_alert(ticker, name, "score_60", message, price, score)
    _send_toast(
        f"🏆 Score Alert (A-Grade): {name}",
        message,
        duration=10,
    )
    log.info(f"[Alert fired] score_60 — {ticker}")
    return True


# ── Run all checks across full dataset ───────────────────────────────────────

def run_all_alerts(df: pd.DataFrame | None = None,
                   prev_scores: dict | None = None) -> int:
    """
    Runs all 3 alert types across the full stock universe.
    Call this after each batch scrape completes.

    Args:
        df: DataFrame of scored stocks. If None, loads from DB.
        prev_scores: dict of {ticker: old_score} for crossover detection.

    Returns:
        Number of alerts fired.
    """
    if df is None:
        df = get_all_stocks()

    if df.empty:
        return 0

    total_fired = 0

    for _, row in df.iterrows():
        ticker     = row.get("ticker", "")
        prev_score = (prev_scores or {}).get(ticker, 0)

        fired = (
            check_value_zone_alert(row)
            + check_fii_selling_alert(row)
            + check_score_60_alert(row, prev_score)
        )
        total_fired += fired

    if total_fired:
        log.info(f"Alert run complete — {total_fired} alert(s) fired.")
    return total_fired


# ── Unread count (for app badge) ─────────────────────────────────────────────

def get_unread_count() -> int:
    try:
        return len(get_unacknowledged_alerts())
    except Exception:
        return 0

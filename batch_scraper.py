# ============================================================
# batch_scraper.py — v2
# Fixed: column name sanitisation (52W_Low → low_52w etc.)
# ============================================================

import time, random, threading, logging
from datetime import datetime
import pandas as pd

from config import SECTOR_PE
from database import (
    init_db, get_pending_tickers, upsert_stock,
    start_scrape, finish_scrape, set_scrape_progress,
    mark_scrape_status
)
from data_pipeline import (
    get_price_data, _scrape_screener,
    _compute_fii_dii_flags, _compute_value_zones
)
from scoring import calculate_score

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

_scrape_thread: threading.Thread | None = None
_stop_event = threading.Event()


def is_running() -> bool:
    return _scrape_thread is not None and _scrape_thread.is_alive()


def stop_scrape():
    _stop_event.set()
    log.info("Stop signal sent.")


# ── Key rename map: pipeline output → SQLite schema columns ──────────────────
# data_pipeline.py uses CamelCase / mixed keys.
# database.py schema uses lowercase_underscore.
# This map bridges the two so upsert_stock() never gets an invalid column name.
KEY_MAP = {
    # Price
    "Price":             "price",
    "Price_1M_Ret":      "price_1m_ret",
    "Price_3M_Ret":      "price_3m_ret",
    "Price_6M_Ret":      "price_6m_ret",
    "52W_Low":           "low_52w",        # was producing "52w_low" — invalid SQL
    "52W_High":          "high_52w",       # same
    "Pct_Above_52W_Low": "pct_above_52w_low",
    "Price_Trend":       "price_trend",
    "Volume_Trend":      "volume_trend",
    # Fundamentals
    "PE":                "pe",
    "PB":                "pb",
    "RoE":               "roe",
    "RoCE":              "roce",
    "DE":                "de",
    "Revenue_Growth":    "revenue_growth",
    "Market_Cap_Cr":     "market_cap_cr",
    "Screener_URL":      "screener_url",
    # Shareholding
    "FII_Pct":           "fii_pct",
    "DII_Pct":           "dii_pct",
    "Promoter_Pct":      "promoter_pct",
    "FII_Q1":            "fii_q1",
    "FII_Q2":            "fii_q2",
    "FII_Q3":            "fii_q3",
    "FII_Q4":            "fii_q4",
    "DII_Q1":            "dii_q1",
    "DII_Q2":            "dii_q2",
    "DII_Q3":            "dii_q3",
    "DII_Q4":            "dii_q4",
    # Derived flags
    "FII_Selling_4Q":    "fii_selling_4q",
    "DII_Buying_4Q":     "dii_buying_4q",
    "FII_Trend_Pct":     "fii_trend_pct",
    "DII_Trend_Pct":     "dii_trend_pct",
    "FII_Label":         "fii_label",
    "DII_Label":         "dii_label",
    # Valuation
    "Fair_Value":        "fair_value",
    "Buy_Zone_Low":      "buy_zone_low",
    "Buy_Zone_High":     "buy_zone_high",
    "Strong_Buy_Below":  "strong_buy_below",
    "Value_Signal":      "value_signal",
    "Valuation_Methods": "valuation_methods",
    # Scores
    "Smart_Money_Score": "smart_money_score",
    "Grade":             "grade",
    "Score_FII":         "score_fii",
    "Score_DII":         "score_dii",
    "Score_PE":          "score_pe",
    "Score_PB":          "score_pb",
    "Score_RoE":         "score_roe",
    "Score_RoCE":        "score_roce",
    "Score_Debt":        "score_debt",
    "Score_RevGrowth":   "score_revgrowth",
    "Score_Promoter":    "score_promoter",
    "Score_52W":         "score_52w",
    # Identity
    "Ticker":            "ticker",
    "Name":              "name",
    "Sector":            "sector",
    "Sector_PE":         "sector_pe",
    "Last_Updated":      "last_updated",
}

# Columns that exist in our schema (whitelist)
SCHEMA_COLS = set(KEY_MAP.values()) | {
    "ticker","name","sector","sector_pe","last_updated","scrape_status"
}


def _normalise_row(raw: dict) -> dict:
    """
    Rename keys from pipeline format to schema format.
    Drop any key not in the schema to avoid SQL errors.
    """
    out = {}
    for k, v in raw.items():
        mapped = KEY_MAP.get(k)
        if mapped:
            out[mapped] = v
        else:
            # Generic fallback: lowercase + underscore, but skip
            # keys that start with a digit (like 52w_* before mapping)
            generic = k.lower().replace(" ", "_")
            if generic[0].isalpha() and generic in SCHEMA_COLS:
                out[generic] = v
            # silently drop anything else — no bad SQL column names
    return out


# ── Core scrape for one stock ─────────────────────────────────────────────────

def _scrape_one(meta: dict) -> dict | None:
    ticker      = meta["ticker"]
    sector      = meta.get("sector", "Miscellaneous")
    screener_id = meta.get("screener_id", ticker.replace(".NS", ""))

    price_data = get_price_data(ticker)
    if not price_data:
        log.warning(f"  [{ticker}] No price data.")
        return None

    pb_hint = float(meta.get("pb", 0.0) or 0.0)
    fund_data = {}
    for attempt in range(3):
        try:
            fund_data = _scrape_screener(screener_id, pb_hint=pb_hint)
            break
        except Exception as e:
            if attempt < 2:
                wait = 5 * (attempt + 1)
                log.warning(f"  [{ticker}] Retry in {wait}s ({e})")
                time.sleep(wait)
            else:
                log.error(f"  [{ticker}] All retries failed.")

    # Build raw row (pipeline keys)
    raw = {
        "Ticker":       ticker,
        "Name":         meta.get("name", ticker),
        "Sector":       sector,
        "Sector_PE":    SECTOR_PE.get(sector, 25.0),
        "Last_Updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    raw.update(price_data)
    raw.update(fund_data)
    raw.update(_compute_fii_dii_flags(raw))
    raw.update(_compute_value_zones(raw, sector))

    # Score (needs a DataFrame)
    df_single  = pd.DataFrame([raw])
    df_scored  = calculate_score(df_single)
    scored_raw = df_scored.iloc[0].to_dict()

    # Normalise keys to schema format — this is the key fix
    db_row = _normalise_row(scored_raw)
    db_row["scrape_status"] = "done"

    return db_row


# ── Background thread ─────────────────────────────────────────────────────────

def _run_batch(tickers_meta: list[dict]):
    global _scrape_thread
    total = len(tickers_meta)
    start_scrape(total)
    log.info(f"Batch started: {total} stocks.")
    completed = 0

    for i, meta in enumerate(tickers_meta):
        if _stop_event.is_set():
            log.info("Stopped by user.")
            break

        ticker = meta["ticker"]
        log.info(f"[{i+1}/{total}] {ticker} — {meta.get('name','')}")

        try:
            mark_scrape_status(ticker, "scraping")
            result = _scrape_one(meta)
            if result:
                upsert_stock(result)
                completed += 1
                log.info(f"  ✓  Score={result.get('smart_money_score',0):.0f}  "
                         f"Signal={result.get('value_signal','—')}")
            else:
                mark_scrape_status(ticker, "failed")
        except Exception as e:
            log.error(f"  [{ticker}] Error: {e}")
            mark_scrape_status(ticker, "failed")

        set_scrape_progress(total, completed, running=1)
        time.sleep(random.uniform(2.0, 4.0))

    finish_scrape()
    set_scrape_progress(total, completed, running=0)
    log.info(f"Done. {completed}/{total} scraped.")
    _scrape_thread = None


def start_batch_scrape(force: bool = False) -> str:
    global _scrape_thread, _stop_event

    if is_running():
        return "⚙️ Scraper already running."

    pending = get_pending_tickers()

    if not pending and not force:
        return "✅ All stocks up to date."

    if force:
        from database import get_universe
        pending = get_universe().to_dict(orient="records")

    if not pending:
        return "⚠️ No stocks in universe. Run universe_builder.py first."

    _stop_event.clear()
    _scrape_thread = threading.Thread(
        target=_run_batch, args=(pending,), daemon=True, name="BatchScraper"
    )
    _scrape_thread.start()
    return f"🚀 Scraping started — {len(pending)} stocks queued."


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    pending = get_pending_tickers()

    if not pending:
        print("Nothing to scrape. Run universe_builder.py first.")
    else:
        print(f"Scraping {len(pending)} stocks. Ctrl+C to stop.\n")
        total, completed = len(pending), 0
        start_scrape(total)

        for i, meta in enumerate(pending):
            ticker = meta["ticker"]
            print(f"[{i+1}/{total}] {ticker} — {meta.get('name','')}")
            try:
                result = _scrape_one(meta)
                if result:
                    upsert_stock(result)
                    completed += 1
                    print(f"  ✓  Score={result.get('smart_money_score',0):.0f}  "
                          f"Signal={result.get('value_signal','—')}")
                else:
                    print("  ✗  No data")
            except KeyboardInterrupt:
                print("\nStopped.")
                break
            except Exception as e:
                print(f"  ✗  {e}")

            set_scrape_progress(total, completed, running=1)
            time.sleep(random.uniform(2.0, 4.0))

        finish_scrape()
        print(f"\nDone: {completed}/{total} scraped.")

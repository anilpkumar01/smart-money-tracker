# ============================================================
# database.py — SQLite wrapper
# Replaces the fragile CSV approach.
# Supports concurrent reads while batch scraping is writing.
# ============================================================

import sqlite3
import pandas as pd
import threading
from datetime import datetime
from pathlib import Path

DB_PATH   = Path("data/smart_money.db")
ALERT_DB  = Path("data/alerts.db")

# Thread-local connections — safe for concurrent access
_local = threading.local()


def _get_conn(db_path: Path) -> sqlite3.Connection:
    """Return a thread-local connection to the given DB."""
    attr = f"conn_{db_path.stem}"
    if not hasattr(_local, attr) or getattr(_local, attr) is None:
        conn = sqlite3.connect(str(db_path), check_same_thread=False,
                               timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")   # allows concurrent reads
        conn.execute("PRAGMA synchronous=NORMAL")
        setattr(_local, attr, conn)
    return getattr(_local, attr)


# ── Schema ────────────────────────────────────────────────────────────────────

STOCKS_DDL = """
CREATE TABLE IF NOT EXISTS stocks (
    ticker              TEXT PRIMARY KEY,
    name                TEXT,
    sector              TEXT,
    sector_pe           REAL,
    last_updated        TEXT,
    -- Price
    price               REAL,
    price_1m_ret        REAL,
    price_3m_ret        REAL,
    price_6m_ret        REAL,
    low_52w             REAL,
    high_52w            REAL,
    pct_above_52w_low   REAL,
    price_trend         TEXT,
    volume_trend        TEXT,
    -- Fundamentals
    pe                  REAL,
    pb                  REAL,
    roe                 REAL,
    roce                REAL,
    de                  REAL,
    revenue_growth      REAL,
    market_cap_cr       REAL,
    screener_url        TEXT,
    -- Shareholding
    fii_pct             REAL,
    dii_pct             REAL,
    promoter_pct        REAL,
    fii_q1              REAL,  fii_q2 REAL, fii_q3 REAL, fii_q4 REAL,
    dii_q1              REAL,  dii_q2 REAL, dii_q3 REAL, dii_q4 REAL,
    -- Derived flags
    fii_selling_4q      INTEGER,
    dii_buying_4q       INTEGER,
    fii_trend_pct       REAL,
    dii_trend_pct       REAL,
    fii_label           TEXT,
    dii_label           TEXT,
    -- Valuation
    fair_value          REAL,
    buy_zone_low        REAL,
    buy_zone_high       REAL,
    strong_buy_below    REAL,
    value_signal        TEXT,
    valuation_methods   TEXT,
    -- Scores
    smart_money_score   REAL,
    grade               TEXT,
    score_fii           REAL,
    score_dii           REAL,
    score_pe            REAL,
    score_pb            REAL,
    score_roe           REAL,
    score_roce          REAL,
    score_debt          REAL,
    score_revgrowth     REAL,
    score_promoter      REAL,
    score_52w           REAL,
    -- Scrape status
    scrape_status       TEXT DEFAULT 'pending'
);
"""

UNIVERSE_DDL = """
CREATE TABLE IF NOT EXISTS universe (
    ticker          TEXT PRIMARY KEY,
    name            TEXT,
    sector          TEXT,
    screener_id     TEXT,
    market_cap_cr   REAL,
    pe              REAL,
    pb              REAL,
    roe             REAL,
    added_on        TEXT,
    active          INTEGER DEFAULT 1
);
"""

ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT,
    name            TEXT,
    alert_type      TEXT,   -- 'value_zone' | 'fii_selling_4q' | 'score_60'
    message         TEXT,
    price           REAL,
    score           REAL,
    fired_at        TEXT,
    acknowledged    INTEGER DEFAULT 0
);
"""

SCRAPE_PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS scrape_progress (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    total       INTEGER DEFAULT 0,
    completed   INTEGER DEFAULT 0,
    running     INTEGER DEFAULT 0,
    started_at  TEXT,
    updated_at  TEXT
);
"""


def init_db():
    """Create all tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    ALERT_DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(STOCKS_DDL)
        conn.execute(UNIVERSE_DDL)
        conn.execute(SCRAPE_PROGRESS_DDL)
        conn.execute("""
            INSERT OR IGNORE INTO scrape_progress (id, total, completed, running)
            VALUES (1, 0, 0, 0)
        """)
        conn.commit()

    with sqlite3.connect(str(ALERT_DB)) as conn:
        conn.execute(ALERTS_DDL)
        conn.commit()


# ── Stocks table ──────────────────────────────────────────────────────────────

def upsert_stock(row: dict):
    """Insert or replace a fully-scored stock row."""
    conn = _get_conn(DB_PATH)
    cols  = ", ".join(row.keys())
    placeholders = ", ".join(["?"] * len(row))
    sql = f"INSERT OR REPLACE INTO stocks ({cols}) VALUES ({placeholders})"
    conn.execute(sql, list(row.values()))
    conn.commit()


def get_all_stocks() -> pd.DataFrame:
    """Return all stocks as a DataFrame, sorted by score desc."""
    conn = _get_conn(DB_PATH)
    return pd.read_sql(
        "SELECT * FROM stocks WHERE scrape_status='done' ORDER BY smart_money_score DESC",
        conn
    )


def get_stock(ticker: str) -> pd.Series | None:
    conn = _get_conn(DB_PATH)
    df = pd.read_sql(
        "SELECT * FROM stocks WHERE ticker=?", conn, params=(ticker,)
    )
    return df.iloc[0] if not df.empty else None


def get_stocks_count() -> dict:
    conn = _get_conn(DB_PATH)
    cur = conn.execute(
        "SELECT scrape_status, COUNT(*) as n FROM stocks GROUP BY scrape_status"
    )
    return {row["scrape_status"]: row["n"] for row in cur.fetchall()}


# ── Universe table ────────────────────────────────────────────────────────────

def upsert_universe(rows: list[dict]):
    """Bulk-insert universe stocks."""
    if not rows:
        return
    conn = _get_conn(DB_PATH)
    for row in rows:
        cols  = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        conn.execute(
            f"INSERT OR IGNORE INTO universe ({cols}) VALUES ({placeholders})",
            list(row.values())
        )
    conn.commit()


def get_universe() -> pd.DataFrame:
    conn = _get_conn(DB_PATH)
    return pd.read_sql(
        "SELECT * FROM universe WHERE active=1 ORDER BY market_cap_cr DESC",
        conn
    )


def get_pending_tickers() -> list[dict]:
    """Return tickers that haven't been scraped yet or are stale (>24h)."""
    conn = _get_conn(DB_PATH)
    cur = conn.execute("""
        SELECT u.ticker, u.name, u.sector, u.screener_id, u.market_cap_cr
        FROM universe u
        LEFT JOIN stocks s ON u.ticker = s.ticker
        WHERE u.active = 1
          AND (
            s.ticker IS NULL
            OR s.scrape_status != 'done'
            OR datetime(s.last_updated) < datetime('now', '-24 hours')
          )
        ORDER BY u.market_cap_cr DESC
    """)
    return [dict(r) for r in cur.fetchall()]


def mark_scrape_status(ticker: str, status: str):
    conn = _get_conn(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO stocks (ticker, scrape_status) VALUES (?, ?)"
        " ON CONFLICT(ticker) DO UPDATE SET scrape_status=excluded.scrape_status",
        (ticker, status)
    )
    conn.commit()


# ── Scrape progress ───────────────────────────────────────────────────────────

def set_scrape_progress(total: int, completed: int, running: int):
    conn = _get_conn(DB_PATH)
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE scrape_progress
        SET total=?, completed=?, running=?, updated_at=?
        WHERE id=1
    """, (total, completed, running, now))
    conn.commit()


def get_scrape_progress() -> dict:
    conn = _get_conn(DB_PATH)
    cur = conn.execute("SELECT * FROM scrape_progress WHERE id=1")
    row = cur.fetchone()
    if row:
        return dict(row)
    return {"total": 0, "completed": 0, "running": 0}


def start_scrape(total: int):
    conn = _get_conn(DB_PATH)
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE scrape_progress
        SET total=?, completed=0, running=1, started_at=?, updated_at=?
        WHERE id=1
    """, (total, now, now))
    conn.commit()


def finish_scrape():
    conn = _get_conn(DB_PATH)
    conn.execute(
        "UPDATE scrape_progress SET running=0, updated_at=? WHERE id=1",
        (datetime.now().isoformat(),)
    )
    conn.commit()


# ── Alerts table ──────────────────────────────────────────────────────────────

def save_alert(ticker: str, name: str, alert_type: str,
               message: str, price: float, score: float):
    with sqlite3.connect(str(ALERT_DB)) as conn:
        conn.execute("""
            INSERT INTO alerts (ticker, name, alert_type, message, price, score, fired_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (ticker, name, alert_type, message, price, score,
              datetime.now().isoformat()))
        conn.commit()


def get_alerts(limit: int = 100) -> pd.DataFrame:
    with sqlite3.connect(str(ALERT_DB)) as conn:
        return pd.read_sql(
            "SELECT * FROM alerts ORDER BY fired_at DESC LIMIT ?",
            conn, params=(limit,)
        )


def get_unacknowledged_alerts() -> pd.DataFrame:
    with sqlite3.connect(str(ALERT_DB)) as conn:
        return pd.read_sql(
            "SELECT * FROM alerts WHERE acknowledged=0 ORDER BY fired_at DESC",
            conn
        )


def acknowledge_alerts():
    with sqlite3.connect(str(ALERT_DB)) as conn:
        conn.execute("UPDATE alerts SET acknowledged=1")
        conn.commit()


def alert_already_fired(ticker: str, alert_type: str,
                        within_hours: int = 24) -> bool:
    """Prevent duplicate alerts firing repeatedly."""
    with sqlite3.connect(str(ALERT_DB)) as conn:
        cur = conn.execute("""
            SELECT COUNT(*) FROM alerts
            WHERE ticker=? AND alert_type=?
              AND datetime(fired_at) > datetime('now', ? || ' hours')
        """, (ticker, alert_type, f"-{within_hours}"))
        return cur.fetchone()[0] > 0

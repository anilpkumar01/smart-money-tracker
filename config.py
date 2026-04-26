# ============================================================
# config.py — Central configuration for Smart Money Tracker
# ============================================================

# --- Stocks to track (NSE tickers for yfinance) ---
# Format: "NSE_SYMBOL.NS"  |  Add or remove freely
WATCHLIST = {
    "HDFCBANK.NS":   {"name": "HDFC Bank",            "sector": "Banking",          "screener_id": "HDFCBANK"},
    "ICICIBANK.NS":  {"name": "ICICI Bank",            "sector": "Banking",          "screener_id": "ICICIBANK"},
    "CMSINFO.NS":    {"name": "CMS Info Systems",      "sector": "Miscellaneous",    "screener_id": "CMSINFO"},
    "PVRINOX.NS":    {"name": "PVR INOX",              "sector": "Media",            "screener_id": "PVRINOX"},
    "CROMPTON.NS":   {"name": "Crompton Greaves CE",   "sector": "Consumer Durables","screener_id": "CROMPTON"},
    "FIVESTAR.NS":   {"name": "Five Star Business Fin","sector": "Finance",          "screener_id": "FIVESTAR"},
    "PNBHOUSING.NS": {"name": "PNB Housing Finance",   "sector": "Finance",          "screener_id": "PNBHOUSING"},
    "AAVAS.NS":      {"name": "Aavas Financiers",      "sector": "Finance",          "screener_id": "AAVAS"},
    "APTUS.NS":      {"name": "Aptus Value Housing",   "sector": "Finance",          "screener_id": "APTUS"},
    "MPHASIS.NS":    {"name": "Mphasis",               "sector": "IT",               "screener_id": "MPHASIS"},
    "NEWGEN.NS":     {"name": "Newgen Software",       "sector": "IT",               "screener_id": "NEWGEN"},
    "LEMONTREE.NS":  {"name": "Lemon Tree Hotels",     "sector": "Hotels",           "screener_id": "LEMONTREE"},
    "AARTIIND.NS":   {"name": "Aarti Industries",      "sector": "Chemicals",        "screener_id": "AARTIIND"},
    "COALINDIA.NS":  {"name": "Coal India",            "sector": "Mining",           "screener_id": "COALINDIA"},
    "ITC.NS":        {"name": "ITC",                   "sector": "FMCG",             "screener_id": "ITC"},
}

# --- Sector average P/E benchmarks (update quarterly) ---
SECTOR_PE = {
    "Banking":          12.0,
    "Finance":          18.0,
    "IT":               28.0,
    "FMCG":             45.0,
    "Consumer Durables":38.0,
    "Chemicals":        30.0,
    "Media":            35.0,
    "Hotels":           30.0,
    "Mining":           10.0,
    "Miscellaneous":    20.0,
}

# --- Scoring weights (must sum to 100) ---
# Tuned for value investing: fundamentals > momentum > AI signal
SCORE_WEIGHTS = {
    "fii_selling":         10,   # FII selling = opportunity (contrarian)
    "dii_buying":          10,   # DIIs accumulating = domestic confidence
    "pe_discount":         15,   # PE below sector average
    "pb_below_2":          10,   # PB ratio < 2 (value zone)
    "roe_above_15":        15,   # Quality check — compounding ability
    "roce_above_15":       10,   # Capital efficiency
    "low_debt":            10,   # D/E < 1 (balance sheet safety)
    "revenue_growth":      10,   # Revenue growing YoY
    "promoter_confidence":  5,   # Promoter holding stable or rising
    "price_vs_52w_low":     5,   # Closer to 52w low = margin of safety
}

# --- Value buy zone parameters ---
# Based on P/E mean reversion logic
VALUE_ZONE_PE_DISCOUNT   = 0.80   # Buy zone = 80% of fair value (20% margin of safety)
VALUE_ZONE_STRONG_MARGIN = 0.70   # Strong buy = 70% of fair value (30% MoS)

# --- Screener.in scraping ---
SCREENER_BASE_URL = "https://www.screener.in/company/{screener_id}/consolidated/"
SCREENER_HEADERS  = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
SCRAPE_DELAY_SECONDS = 2.5   # Be polite to screener.in

# --- Ollama ---
OLLAMA_URL   = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.1"    # Change to "mistral" or "phi3" if you prefer

# --- File paths ---
DATA_CSV      = "data/smart_money_data.csv"
CACHE_CSV     = "data/screener_cache.csv"

# --- App display ---
APP_TITLE     = "📊 Smart Money Tracker — Value Edition"
REFRESH_HOURS = 24    # How old cached data can be before forced refresh

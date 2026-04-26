# Smart Money Tracker — Value Edition

A Streamlit app for value investors tracking FII/DII institutional activity,
valuation zones, and AI-powered insights via Ollama.

---

## Project Structure

```
smart_money_tracker/
├── app.py              ← Streamlit UI (5 tabs)
├── config.py           ← Watchlist, weights, API settings  ← EDIT THIS
├── data_pipeline.py    ← yfinance prices + screener.in scraping
├── scoring.py          ← 10-factor weighted score engine (0–100)
├── ai_engine.py        ← Ollama LLM integration
├── utils.py            ← Plotly charts + formatters
├── data/               ← Auto-created, stores cached CSV
└── requirements.txt
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Install & start Ollama (for AI features)
```bash
# Install from https://ollama.com
ollama pull llama3.1
ollama run llama3.1   # leave this running in a terminal
```

### 3. Fetch data (first time)
```bash
python data_pipeline.py
```
This scrapes screener.in for each stock — takes ~1 min due to polite rate limiting.

### 4. Run the app
```bash
streamlit run app.py
```

---

## Customising Your Watchlist

Edit `config.py`:
```python
WATCHLIST = {
    "TICKER.NS": {
        "name": "Company Name",
        "sector": "Banking",        # must match SECTOR_PE keys
        "screener_id": "TICKERSYMBOL"   # as it appears in screener.in URL
    },
}
```

To find the `screener_id`: go to `https://www.screener.in/company/SYMBOL/`
and use the exact symbol from the URL.

---

## Score Weights (config.py)

Tuned for value investing — adjust to your style:
```python
SCORE_WEIGHTS = {
    "fii_selling":          10,   # Contrarian signal
    "dii_buying":           10,   # Domestic confidence
    "pe_discount":          15,   # Cheap vs sector
    "pb_below_2":           10,   # Asset value safety
    "roe_above_15":         15,   # Business quality
    "roce_above_15":        10,   # Capital efficiency
    "low_debt":             10,   # Balance sheet safety
    "revenue_growth":       10,   # Business momentum
    "promoter_confidence":   5,   # Management alignment
    "price_vs_52w_low":      5,   # Technical margin of safety
}
```

---

## Data Refresh

Data is cached for 24 hours (configurable via `REFRESH_HOURS` in config.py).
To force refresh: click **Refresh All Data** in the sidebar, or run:
```bash
python data_pipeline.py
```

---

## Disclaimer

This tool is for **research and educational purposes only**.
It is NOT investment advice. Always verify data from official
BSE/NSE/SEBI filings. Consult a SEBI-registered financial advisor
before making investment decisions.

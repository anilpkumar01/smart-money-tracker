# ============================================================
# ai_engine.py — Ollama LLM integration
# Provides: per-stock insights, portfolio-level summary,
#           natural language screener queries
# ============================================================

import requests
import pandas as pd
from config import OLLAMA_URL, OLLAMA_MODEL


def _call_ollama(prompt: str, max_tokens: int = 400) -> str:
    """Low-level call to local Ollama API."""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model":  OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_tokens, "temperature": 0.3},
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("response", "No response from model.").strip()
    except requests.exceptions.ConnectionError:
        return (
            "⚠️ Ollama is not running. Start it with: `ollama run llama3.1` "
            "then restart the app."
        )
    except Exception as e:
        return f"⚠️ LLM error: {e}"


# ── Stock-level insight ──────────────────────────────────────────────────────

def generate_stock_insight(row: pd.Series) -> str:
    """
    Generates a concise value-investing insight for a single stock.
    Feeds the LLM real numbers and asks for structured output.
    """
    fii_change  = row.get("FII_Trend_Pct", 0)
    dii_change  = row.get("DII_Trend_Pct", 0)
    val_signal  = row.get("Value_Signal", "N/A")
    fair_val    = row.get("Fair_Value", 0)
    price       = row.get("Price", 0)
    discount    = round((fair_val - price) / fair_val * 100, 1) if fair_val > 0 else 0

    prompt = f"""
You are a value investing analyst focused on Indian equities with a 1–3 year horizon.
Analyse this stock and give a structured, concise insight.

Stock: {row.get("Name")} ({row.get("Ticker")})
Sector: {row.get("Sector")}
Current Price: ₹{price}
Fair Value Estimate: ₹{fair_val} ({discount:+.1f}% vs current price)
Value Signal: {val_signal}

Fundamentals:
- P/E: {row.get("PE")}x  (Sector avg: {row.get("Sector_PE")}x)
- P/B: {row.get("PB")}x
- RoE: {row.get("RoE")}%
- RoCE: {row.get("RoCE")}%
- D/E: {row.get("DE")}x
- Revenue Growth (YoY): {row.get("Revenue_Growth")}%

Smart Money:
- FII holding change over 4 quarters: {fii_change:+.1f}%  ({row.get("FII_Label")})
- DII holding change over 4 quarters: {dii_change:+.1f}%  ({row.get("DII_Label")})
- Current FII %: {row.get("FII_Pct")}%
- Promoter %: {row.get("Promoter_Pct")}%

Composite Smart Money Score: {row.get("Smart_Money_Score")}/100  (Grade: {row.get("Grade")})
Value Buy Zone: ₹{row.get("Buy_Zone_Low")} – ₹{row.get("Buy_Zone_High")}
Strong Buy Below: ₹{row.get("Strong_Buy_Below")}

Respond in exactly this format:
THESIS: [1 sentence on why or why not this is a value opportunity]
OPPORTUNITY: [1 sentence on what needs to happen for the thesis to play out]
RISK: [1 sentence on the biggest risk to avoid]
ACTION: [BUY NOW / BUY ON DIP / WATCH / AVOID] at [price range]
"""
    return _call_ollama(prompt, max_tokens=300)


# ── Portfolio-level summary ──────────────────────────────────────────────────

def generate_portfolio_summary(df: pd.DataFrame) -> str:
    """
    Gives a macro summary of the watchlist — best ideas, themes, risks.
    """
    top5 = df.nlargest(5, "Smart_Money_Score")[
        ["Name", "Sector", "Smart_Money_Score", "Value_Signal", "Price",
         "Fair_Value", "FII_Label", "DII_Label"]
    ].to_dict(orient="records")

    bottom3 = df.nsmallest(3, "Smart_Money_Score")[["Name", "Grade"]].to_dict(orient="records")

    prompt = f"""
You are a value investing portfolio manager focused on Indian equities.
Horizon: 1–3 years. Style: Buy quality at a discount. Avoid value traps.

Top 5 stocks by Smart Money Score:
{top5}

Bottom 3 stocks (weakest scores):
{bottom3}

In 4–5 sentences:
1. What is the strongest value opportunity right now and why?
2. What sector theme is the smart money signal pointing to?
3. What is the biggest risk across this watchlist?
4. One contrarian idea worth watching?
"""
    return _call_ollama(prompt, max_tokens=400)


# ── Natural language screener ────────────────────────────────────────────────

def natural_language_query(df: pd.DataFrame, user_query: str) -> str:
    """
    Interprets a free-text query and filters/ranks the watchlist accordingly.
    Returns LLM explanation + the filtered tickers.
    """
    # Give the LLM a structured summary of the data (not raw rows)
    summary = df[[
        "Name", "Sector", "Price", "PE", "PB", "RoE", "RoCE", "DE",
        "Revenue_Growth", "FII_Selling_4Q", "DII_Buying_4Q",
        "Smart_Money_Score", "Grade", "Value_Signal",
        "Buy_Zone_Low", "Buy_Zone_High", "Fair_Value"
    ]].to_dict(orient="records")

    prompt = f"""
You are a stock screener AI for Indian equities. You have data on {len(df)} stocks.

Here is the data:
{summary}

User query: "{user_query}"

Instructions:
- Identify which stocks match the query criteria.
- List matching stock names and tickers.
- Briefly explain why each matches.
- If no stocks match, say so clearly.
- Be concise. Max 200 words.
"""
    return _call_ollama(prompt, max_tokens=400)


# ── Ollama health check ──────────────────────────────────────────────────────

def check_ollama_status() -> tuple[bool, str]:
    """Returns (is_running: bool, message: str)."""
    try:
        resp = requests.get("http://localhost:11434/api/tags", timeout=5)
        if resp.status_code == 200:
            models = [m["name"] for m in resp.json().get("models", [])]
            return True, f"✅ Ollama running. Models: {', '.join(models) or 'none loaded'}"
        return False, "⚠️ Ollama responded but returned an error."
    except Exception:
        return False, "❌ Ollama not running. Start with: `ollama run llama3.1`"

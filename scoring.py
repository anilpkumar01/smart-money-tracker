# ============================================================
# scoring.py — Value investing score engine
# Each factor is scored 0–10, then weighted per config.py
# Final score is 0–100 (higher = more attractive for value investor)
# ============================================================

import pandas as pd
from config import SCORE_WEIGHTS


# ── Individual factor scorers ─────────────────────────────────────────────────

def _score_fii_selling(row: pd.Series) -> int:
    """
    Contrarian value signal:
    FII selling consistently = potential mispricing by foreign funds.
    - Selling 4Q consistently + >3% drop  → 10
    - Selling 4Q consistently             → 7
    - Stable                              → 4
    - Buying aggressively                 → 2 (already priced in)
    """
    if row.get("FII_Selling_4Q", False) and abs(row.get("FII_Trend_Pct", 0)) > 3:
        return 10
    elif row.get("FII_Selling_4Q", False):
        return 7
    elif abs(row.get("FII_Trend_Pct", 0)) < 1:
        return 4  # Stable
    else:
        return 2  # FII buying hard = less contrarian value


def _score_dii_buying(row: pd.Series) -> int:
    """
    Domestic institutions absorbing FII supply = quality check.
    - Buying consistently 4Q + >2% gain   → 10
    - Buying consistently 4Q              → 7
    - Stable                              → 5
    - Reducing                            → 2
    """
    if row.get("DII_Buying_4Q", False) and row.get("DII_Trend_Pct", 0) > 2:
        return 10
    elif row.get("DII_Buying_4Q", False):
        return 7
    elif row.get("DII_Trend_Pct", 0) >= 0:
        return 5
    else:
        return 2


def _score_pe_discount(row: pd.Series) -> int:
    """
    P/E relative to sector average.
    - >40% below sector PE               → 10
    - 20–40% below                       → 8
    - 0–20% below                        → 6
    - At sector PE                       → 4
    - Above sector PE                    → 1
    - PE = 0 (loss-making)               → 0
    """
    pe = row.get("PE", 0)
    sector_pe = row.get("Sector_PE", 25)
    if pe <= 0 or sector_pe <= 0:
        return 0
    ratio = pe / sector_pe
    if ratio < 0.60:
        return 10
    elif ratio < 0.80:
        return 8
    elif ratio < 1.00:
        return 6
    elif ratio < 1.20:
        return 4
    else:
        return 1


def _score_pb(row: pd.Series) -> int:
    """
    P/B ratio — lower is better for value investors.
    - P/B < 1 (below book)               → 10
    - 1 ≤ P/B < 1.5                     → 8
    - 1.5 ≤ P/B < 2                     → 6
    - 2 ≤ P/B < 3                       → 4
    - P/B ≥ 3                           → 2
    - P/B = 0 (missing)                  → 3
    """
    pb = row.get("PB", 0)
    if pb <= 0:
        return 3
    elif pb < 1.0:
        return 10
    elif pb < 1.5:
        return 8
    elif pb < 2.0:
        return 6
    elif pb < 3.0:
        return 4
    else:
        return 2


def _score_roe(row: pd.Series) -> int:
    """
    Return on Equity — quality of the business.
    - RoE > 25%                          → 10
    - 20–25%                             → 8
    - 15–20%                             → 6
    - 10–15%                             → 4
    - < 10%                              → 1
    """
    roe = row.get("RoE", 0)
    if roe > 25:
        return 10
    elif roe > 20:
        return 8
    elif roe > 15:
        return 6
    elif roe > 10:
        return 4
    else:
        return 1


def _score_roce(row: pd.Series) -> int:
    """
    Return on Capital Employed — capital efficiency.
    - RoCE > 25%                         → 10
    - 20–25%                             → 8
    - 15–20%                             → 6
    - 10–15%                             → 4
    - < 10%                              → 1
    """
    roce = row.get("RoCE", 0)
    if roce > 25:
        return 10
    elif roce > 20:
        return 8
    elif roce > 15:
        return 6
    elif roce > 10:
        return 4
    else:
        return 1


def _score_debt(row: pd.Series) -> int:
    """
    Debt/Equity — balance sheet safety.
    For NBFCs/HFCs, higher D/E is normal — adjusted scoring.
    - D/E < 0.1 (debt-free)              → 10
    - 0.1–0.5                            → 8
    - 0.5–1.0                            → 6
    - 1.0–2.0                            → 4
    - 2.0–4.0 (OK for financials)        → 3
    - > 4.0                              → 1
    """
    de = row.get("DE", 0)
    sector = row.get("Sector", "")
    is_financial = sector in ("Banking", "Finance")

    if de < 0.1:
        return 10
    elif de < 0.5:
        return 8
    elif de < 1.0:
        return 6
    elif de < 2.0:
        return 4
    elif de < 4.0:
        return 3 if is_financial else 2  # Forgive financials more
    else:
        return 1


def _score_revenue_growth(row: pd.Series) -> int:
    """
    Revenue growth YoY — business momentum.
    - > 20% growth                       → 10
    - 12–20%                             → 8
    - 8–12%                              → 6
    - 4–8%                               → 4
    - 0–4%                               → 2
    - Negative                           → 0
    """
    g = row.get("Revenue_Growth", 0)
    if g > 20:
        return 10
    elif g > 12:
        return 8
    elif g > 8:
        return 6
    elif g > 4:
        return 4
    elif g >= 0:
        return 2
    else:
        return 0


def _score_promoter(row: pd.Series) -> int:
    """
    Promoter holding stability — management skin in the game.
    Note: we don't have Q4 data in all cases, so we use current pct as proxy.
    - > 50% promoter holding             → 10
    - 35–50%                             → 7
    - 20–35%                             → 5
    - < 20% or 0 (missing)              → 3
    """
    prom = row.get("Promoter_Pct", 0)
    if prom > 50:
        return 10
    elif prom > 35:
        return 7
    elif prom > 20:
        return 5
    else:
        return 3


def _score_price_vs_52w_low(row: pd.Series) -> int:
    """
    Margin of safety proxy: how far is stock from 52-week low?
    Value investors prefer stocks closer to their lows.
    - Within 10% of 52W low              → 10
    - 10–25% above                       → 8
    - 25–40% above                       → 6
    - 40–60% above                       → 4
    - > 60% above 52W low                → 2
    """
    pct_above = row.get("Pct_Above_52W_Low", 50)
    if pct_above < 10:
        return 10
    elif pct_above < 25:
        return 8
    elif pct_above < 40:
        return 6
    elif pct_above < 60:
        return 4
    else:
        return 2


# ── Master scoring function ──────────────────────────────────────────────────

def calculate_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies all factor scorers and computes a weighted composite score (0–100).
    Adds individual factor columns for transparency.
    """
    df = df.copy()

    # Individual factor scores (0–10)
    df["Score_FII"]      = df.apply(_score_fii_selling,      axis=1)
    df["Score_DII"]      = df.apply(_score_dii_buying,        axis=1)
    df["Score_PE"]       = df.apply(_score_pe_discount,       axis=1)
    df["Score_PB"]       = df.apply(_score_pb,                axis=1)
    df["Score_RoE"]      = df.apply(_score_roe,               axis=1)
    df["Score_RoCE"]     = df.apply(_score_roce,              axis=1)
    df["Score_Debt"]     = df.apply(_score_debt,              axis=1)
    df["Score_RevGrowth"]= df.apply(_score_revenue_growth,    axis=1)
    df["Score_Promoter"] = df.apply(_score_promoter,          axis=1)
    df["Score_52W"]      = df.apply(_score_price_vs_52w_low,  axis=1)

    # Weighted composite (0–100)
    w = SCORE_WEIGHTS
    df["Smart_Money_Score"] = (
        df["Score_FII"]       * w["fii_selling"]       / 10 +
        df["Score_DII"]       * w["dii_buying"]         / 10 +
        df["Score_PE"]        * w["pe_discount"]        / 10 +
        df["Score_PB"]        * w["pb_below_2"]         / 10 +
        df["Score_RoE"]       * w["roe_above_15"]       / 10 +
        df["Score_RoCE"]      * w["roce_above_15"]      / 10 +
        df["Score_Debt"]      * w["low_debt"]           / 10 +
        df["Score_RevGrowth"] * w["revenue_growth"]     / 10 +
        df["Score_Promoter"]  * w["promoter_confidence"]/ 10 +
        df["Score_52W"]       * w["price_vs_52w_low"]   / 10
    ).round(1)

    # Grade label
    def _grade(score):
        if score >= 75:   return "A+ (Strong Buy)"
        elif score >= 60: return "A  (Buy)"
        elif score >= 45: return "B  (Watch)"
        elif score >= 30: return "C  (Hold)"
        else:             return "D  (Avoid)"

    df["Grade"] = df["Smart_Money_Score"].apply(_grade)

    return df.sort_values("Smart_Money_Score", ascending=False).reset_index(drop=True)

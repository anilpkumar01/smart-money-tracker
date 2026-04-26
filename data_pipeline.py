# ============================================================
# data_pipeline.py  — v3
# Fixes:
#   1. PB scraping: screener.in labels it "Book Value" in the
#      ratio list, not "Price to Book". Updated all selectors.
#   2. Fallback: if PB still 0 after scraping, derive from
#      the screener export CSV value we already have in universe.
#   3. Key names with 52W now map cleanly (handled in batch_scraper).
# ============================================================

import time, os, re, requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from config import (
    WATCHLIST, SECTOR_PE, SCREENER_BASE_URL, SCREENER_HEADERS,
    SCRAPE_DELAY_SECONDS, DATA_CSV, CACHE_CSV, REFRESH_HOURS
)


def _parse_num(text):
    if not text: return 0.0
    text = str(text).strip().replace(",","").replace("%","") \
                    .replace("₹","").replace("Cr","").strip()
    if " - " in text:
        parts = text.split(" - ")
        try: return sum(float(p) for p in parts) / len(parts)
        except: return 0.0
    try: return float(text)
    except: return 0.0


def _cache_is_fresh():
    if not os.path.exists(CACHE_CSV): return False
    age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(CACHE_CSV))
    return age < timedelta(hours=REFRESH_HOURS)


# ── Price data ────────────────────────────────────────────────────────────────

def get_price_data(ticker):
    try:
        hist = yf.Ticker(ticker).history(period="1y")
        if hist.empty: return {}
        close  = hist["Close"]
        price  = float(close.iloc[-1])
        p1m    = float(close.iloc[-22])  if len(close)>22  else price
        p3m    = float(close.iloc[-66])  if len(close)>66  else price
        p6m    = float(close.iloc[-132]) if len(close)>132 else price
        lo52   = float(close.min())
        hi52   = float(close.max())
        r1m    = (price-p1m)/p1m*100
        r3m    = (price-p3m)/p3m*100
        r6m    = (price-p6m)/p6m*100
        trend  = "Rising" if r3m>10 else ("Falling" if r3m<-10 else "Stable")
        pct_lo = (price-lo52)/lo52*100
        v20    = float(hist["Volume"].iloc[-20:].mean()) if len(hist)>=20 else 0
        v60    = float(hist["Volume"].iloc[-60:].mean()) if len(hist)>=60 else 0
        return {
            "Price":round(price,2), "Price_1M_Ret":round(r1m,2),
            "Price_3M_Ret":round(r3m,2), "Price_6M_Ret":round(r6m,2),
            "52W_Low":round(lo52,2), "52W_High":round(hi52,2),
            "Pct_Above_52W_Low":round(pct_lo,2),
            "Price_Trend":trend,
            "Volume_Trend":"Rising" if v20>v60*1.15 else "Normal",
        }
    except Exception as e:
        print(f"  [yfinance] {ticker}: {e}")
        return {}


# ── Screener.in scraping — v3 ─────────────────────────────────────────────────

def _scrape_screener(screener_id, pb_hint: float = 0.0):
    """
    pb_hint: pass the CMP/BV value from the universe CSV as a fallback
             so PB is never 0 even if screener page changes layout.
    """
    url = SCREENER_BASE_URL.format(screener_id=screener_id)
    result = {
        "PE":0.0, "PB":pb_hint,          # pre-seed with CSV value
        "RoE":0.0, "RoCE":0.0, "DE":0.0,
        "Revenue_Growth":0.0,
        "FII_Pct":0.0, "DII_Pct":0.0, "Promoter_Pct":0.0,
        "FII_Q1":0.0, "FII_Q2":0.0, "FII_Q3":0.0, "FII_Q4":0.0,
        "DII_Q1":0.0, "DII_Q2":0.0, "DII_Q3":0.0, "DII_Q4":0.0,
        "Market_Cap_Cr":0.0, "Screener_URL":url,
    }

    try:
        resp = requests.get(url, headers=SCREENER_HEADERS, timeout=15)
        if resp.status_code == 429:
            print(f"  [screener] Rate limited — waiting 30s")
            time.sleep(30)
            resp = requests.get(url, headers=SCREENER_HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"  [screener] HTTP {resp.status_code} for {screener_id}")
            return result

        soup = BeautifulSoup(resp.text, "lxml")

        # ── Key ratios ────────────────────────────────────────────────────────
        ratio_items = []
        ratios_ul = soup.find("ul", id="top-ratios")
        if ratios_ul:
            ratio_items = ratios_ul.find_all("li")
        if not ratio_items:
            rdiv = soup.find("div", class_=re.compile(r"company-ratios|ratios"))
            if rdiv:
                ratio_items = rdiv.find_all("li")

        for li in ratio_items:
            label_tag = li.find("span", class_="name") or li.find("span")
            value_tag = (li.find("span", class_="nowrap")
                         or li.find("span", class_="number")
                         or li.find("b"))
            if not label_tag or not value_tag:
                continue
            label = label_tag.get_text(strip=True).lower()
            value = _parse_num(value_tag.get_text(strip=True))
            if value == 0.0:
                continue  # skip empty fields entirely

            # P/E
            if any(x in label for x in ["stock p/e","p/e ratio","price to earning"]):
                result["PE"] = value

            # P/B — screener.in shows this as "Book Value" in the ratio list
            # The value shown IS the Price-to-Book ratio (CMP / Book Value per share)
            # Labels seen in the wild:
            #   "Book Value"        → this is BVPS, NOT PB — skip
            #   "Price to Book"     → PB ✓
            #   "P/B"               → PB ✓
            #   "CMP / BV"          → PB ✓  (screener export column name)
            elif any(x in label for x in ["price to book","cmp / bv","p/b ratio"]):
                result["PB"] = value
            elif label == "p/b":
                result["PB"] = value
            # "Book Value" alone is BVPS (rupees), not the ratio — skip it

            elif any(x in label for x in ["roe","return on equity","return on net worth"]):
                result["RoE"] = value
            elif any(x in label for x in ["roce","return on capital"]):
                result["RoCE"] = value
            elif any(x in label for x in ["debt to equity","d/e","debt / eq"]):
                result["DE"] = value
            elif "market cap" in label:
                result["Market_Cap_Cr"] = value

        # ── PB page-wide fallback scan ────────────────────────────────────────
        # Only try if pb_hint wasn't available and scrape also returned 0
        if result["PB"] == 0.0:
            for tag in soup.find_all(["td","th","span","b","li"]):
                txt = tag.get_text(strip=True).lower()
                if txt in ("price to book","p/b value","price/book","p/b ratio","cmp/bv"):
                    sib = tag.find_next_sibling() or tag.parent.find_next_sibling()
                    if sib:
                        val = _parse_num(sib.get_text(strip=True))
                        if 0 < val < 300:
                            result["PB"] = val
                            break

        # ── Revenue growth ────────────────────────────────────────────────────
        pl = soup.find("section", id="profit-loss")
        if pl:
            for row in pl.find_all("tr"):
                cells = row.find_all("td")
                if not cells: continue
                rl = cells[0].get_text(strip=True).lower()
                if "sales" in rl and "growth" not in rl:
                    nums = [_parse_num(c.get_text(strip=True))
                            for c in cells[1:] if c.get_text(strip=True)]
                    nums = [n for n in nums if n > 0]
                    if len(nums) >= 2:
                        result["Revenue_Growth"] = round(
                            (nums[-1]-nums[-2])/nums[-2]*100, 2)
                    break

        # ── Shareholding — catches both FII and FPI labels ────────────────────
        sh = soup.find("section", id="shareholding")
        if sh:
            tbl = sh.find("table")
            if tbl:
                for row in tbl.find_all("tr"):
                    cells = row.find_all("td")
                    if not cells: continue
                    label = cells[0].get_text(strip=True).lower()
                    vals  = [_parse_num(c.get_text(strip=True))
                             for c in cells[1:] if c.get_text(strip=True)]

                    if "promoters" in label:
                        result["Promoter_Pct"] = vals[0] if vals else 0.0
                    elif any(x in label for x in
                             ["fii","fpi","foreign inst","foreign portfolio"]):
                        result["FII_Pct"] = vals[0] if vals else 0.0
                        for i, k in enumerate(
                                ["FII_Q1","FII_Q2","FII_Q3","FII_Q4"]):
                            result[k] = vals[i] if i < len(vals) else 0.0
                    elif any(x in label for x in
                             ["dii","domestic inst","mutual fund"]):
                        result["DII_Pct"] = vals[0] if vals else 0.0
                        for i, k in enumerate(
                                ["DII_Q1","DII_Q2","DII_Q3","DII_Q4"]):
                            result[k] = vals[i] if i < len(vals) else 0.0

    except Exception as e:
        print(f"  [screener] Error {screener_id}: {e}")

    # Log remaining zeros
    zeros = [k for k in ("PE","PB","RoE","FII_Pct","Promoter_Pct")
             if result[k] == 0.0]
    if zeros:
        print(f"  [screener] Still zero for {screener_id}: {zeros}")

    return result


# ── FII/DII flags ─────────────────────────────────────────────────────────────

def _compute_fii_dii_flags(row):
    q  = [row.get("FII_Q1",0),row.get("FII_Q2",0),
          row.get("FII_Q3",0),row.get("FII_Q4",0)]
    dq = [row.get("DII_Q1",0),row.get("DII_Q2",0),
          row.get("DII_Q3",0),row.get("DII_Q4",0)]
    has_fii = any(v>0 for v in q)
    has_dii = any(v>0 for v in dq)

    if has_fii:
        fii_selling   = all(q[i]<=q[i+1] for i in range(3)) and q[0]<q[3]
        fii_trend_pct = round(q[0]-q[3], 2)
        fii_label     = (f"Selling ({fii_trend_pct:+.1f}% over 4Q)"
                         if fii_selling else
                         (f"Buying ({fii_trend_pct:+.1f}% over 4Q)"
                          if fii_trend_pct>0 else "Stable"))
    else:
        fii_selling, fii_trend_pct, fii_label = False, 0.0, "No data"

    if has_dii:
        dii_buying    = all(dq[i]>=dq[i+1] for i in range(3)) and dq[0]>dq[3]
        dii_trend_pct = round(dq[0]-dq[3], 2)
        dii_label     = (f"Accumulating ({dii_trend_pct:+.1f}% over 4Q)"
                         if dii_buying else "Stable/Reducing")
    else:
        dii_buying, dii_trend_pct, dii_label = False, 0.0, "No data"

    return {
        "FII_Selling_4Q":fii_selling, "DII_Buying_4Q":dii_buying,
        "FII_Trend_Pct":fii_trend_pct, "DII_Trend_Pct":dii_trend_pct,
        "FII_Label":fii_label, "DII_Label":dii_label,
    }


# ── Value zones ───────────────────────────────────────────────────────────────

def _compute_value_zones(row, sector):
    price     = row.get("Price", 0)
    pe        = row.get("PE", 0)
    pb        = row.get("PB", 0)
    sector_pe = SECTOR_PE.get(sector, 25.0)
    estimates = []

    if pe > 0 and price > 0:
        fair = (price/pe) * sector_pe
        if fair > 0:
            estimates.append(("PE Mean Reversion", round(fair,2)))

    if pb > 0 and price > 0:
        fair = (price/pb) * min(2.5, sector_pe/10)
        if fair > 0:
            estimates.append(("PB Mean Reversion", round(fair,2)))

    lo52 = row.get("52W_Low", price*0.75)
    if lo52 > 0:
        estimates.append(("52W Low Buffer", round(lo52*1.15, 2)))

    if not estimates:
        return {"Fair_Value":0,"Buy_Zone_Low":0,"Buy_Zone_High":0,
                "Strong_Buy_Below":0,"Value_Signal":"No Data",
                "Valuation_Methods":"Insufficient data"}

    vals     = sorted(e[1] for e in estimates)
    fair_val = vals[len(vals)//2]
    bz_high  = round(fair_val*0.90, 2)
    bz_low   = round(fair_val*0.80, 2)
    sb       = round(fair_val*0.70, 2)

    if   price <= 0:        sig = "No Data"
    elif price <= sb:       sig = "STRONG BUY"
    elif price <= bz_low:   sig = "BUY"
    elif price <= bz_high:  sig = "WATCH"
    elif price <= fair_val: sig = "FAIR VALUE"
    else:
        sig = f"OVERVALUED +{round((price-fair_val)/fair_val*100,1)}%"

    return {
        "Fair_Value":fair_val, "Buy_Zone_Low":bz_low,
        "Buy_Zone_High":bz_high, "Strong_Buy_Below":sb,
        "Value_Signal":sig,
        "Valuation_Methods":" | ".join(f"{m}: ₹{v}" for m,v in estimates),
    }


# ── Legacy CSV-mode build (for small 15-stock watchlist) ─────────────────────

def build_dataset(force_refresh=False):
    os.makedirs("data", exist_ok=True)
    if not force_refresh and _cache_is_fresh():
        print("Loading from cache...")
        return pd.read_csv(DATA_CSV)

    print(f"Fetching {len(WATCHLIST)} stocks...")
    rows = []
    for ticker, meta in WATCHLIST.items():
        print(f"  [{ticker}]")
        pd_ = get_price_data(ticker)
        if not pd_: continue
        fd = _scrape_screener(meta["screener_id"])
        time.sleep(SCRAPE_DELAY_SECONDS)
        row = {
            "Ticker":ticker, "Name":meta["name"], "Sector":meta["sector"],
            "Sector_PE":SECTOR_PE.get(meta["sector"],25.0),
            "Last_Updated":datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        row.update(pd_); row.update(fd)
        row.update(_compute_fii_dii_flags(row))
        row.update(_compute_value_zones(row, meta["sector"]))
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(DATA_CSV, index=False)
    df.to_csv(CACHE_CSV, index=False)
    return df


if __name__ == "__main__":
    df = build_dataset(force_refresh=True)
    print(df[["Name","Price","PE","PB","RoE","FII_Label","Value_Signal"]].to_string())

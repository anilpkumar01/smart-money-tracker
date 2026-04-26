# ============================================================
# app.py — v2: SQLite, batch scraping progress, Alerts tab
# ============================================================

import streamlit as st
import pandas as pd
import os
from datetime import datetime
from pathlib import Path

from data_pipeline  import build_dataset
from scoring        import calculate_score
from ai_engine      import (generate_stock_insight, generate_portfolio_summary,
                             natural_language_query, check_ollama_status)
from utils          import (plot_score_radar, plot_value_zone,
                             plot_score_leaderboard, plot_fii_dii_trend,
                             format_signal_badge, style_score)
from config         import APP_TITLE, DATA_CSV, SCORE_WEIGHTS
from database       import (init_db, get_all_stocks, get_scrape_progress,
                             get_universe, get_alerts, acknowledge_alerts,
                             get_unacknowledged_alerts)
from batch_scraper  import start_batch_scrape, is_running, stop_scrape
from alert_engine   import run_all_alerts, get_unread_count

# ── Init ──────────────────────────────────────────────────────────────────────
init_db()

st.set_page_config(
    page_title="Smart Money Tracker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
div[data-testid="stMetricValue"] { font-size: 1.5rem; }
.stDataFrame { font-size: 0.84rem; }
.alert-badge {
    display:inline-block;padding:2px 8px;border-radius:10px;
    background:#FFEBEE;color:#B71C1C;font-size:11px;font-weight:600;
}
</style>
""", unsafe_allow_html=True)


# ── Data loading — prefers SQLite, falls back to CSV ─────────────────────────

def _load_data() -> pd.DataFrame:
    """Load from SQLite if available, else CSV."""
    try:
        df = get_all_stocks()
        if not df.empty:
            # Re-normalise column names to match scoring output
            # Map SQLite lowercase cols → CamelCase expected by scoring/utils
            COL_MAP = {
                "ticker":"Ticker","name":"Name","sector":"Sector",
                "sector_pe":"Sector_PE","last_updated":"Last_Updated",
                "price":"Price","price_1m_ret":"Price_1M_Ret",
                "price_3m_ret":"Price_3M_Ret","price_6m_ret":"Price_6M_Ret",
                "low_52w":"52W_Low","high_52w":"52W_High",
                "pct_above_52w_low":"Pct_Above_52W_Low",
                "price_trend":"Price_Trend","volume_trend":"Volume_Trend",
                "pe":"PE","pb":"PB","roe":"RoE","roce":"RoCE",
                "de":"DE","revenue_growth":"Revenue_Growth",
                "market_cap_cr":"Market_Cap_Cr","screener_url":"Screener_URL",
                "fii_pct":"FII_Pct","dii_pct":"DII_Pct",
                "promoter_pct":"Promoter_Pct",
                "fii_q1":"FII_Q1","fii_q2":"FII_Q2",
                "fii_q3":"FII_Q3","fii_q4":"FII_Q4",
                "dii_q1":"DII_Q1","dii_q2":"DII_Q2",
                "dii_q3":"DII_Q3","dii_q4":"DII_Q4",
                "fii_selling_4q":"FII_Selling_4Q",
                "dii_buying_4q":"DII_Buying_4Q",
                "fii_trend_pct":"FII_Trend_Pct",
                "dii_trend_pct":"DII_Trend_Pct",
                "fii_label":"FII_Label","dii_label":"DII_Label",
                "fair_value":"Fair_Value","buy_zone_low":"Buy_Zone_Low",
                "buy_zone_high":"Buy_Zone_High",
                "strong_buy_below":"Strong_Buy_Below",
                "value_signal":"Value_Signal",
                "valuation_methods":"Valuation_Methods",
                "smart_money_score":"Smart_Money_Score","grade":"Grade",
                "score_fii":"Score_FII","score_dii":"Score_DII",
                "score_pe":"Score_PE","score_pb":"Score_PB",
                "score_roe":"Score_RoE","score_roce":"Score_RoCE",
                "score_debt":"Score_Debt","score_revgrowth":"Score_RevGrowth",
                "score_promoter":"Score_Promoter","score_52w":"Score_52W",
                "scrape_status":"Scrape_Status",
            }
            df.columns = [COL_MAP.get(c, c) for c in df.columns]
            return df
    except Exception:
        pass
    # Fallback to CSV
    if os.path.exists(DATA_CSV):
        raw = pd.read_csv(DATA_CSV)
        return calculate_score(raw)
    return pd.DataFrame()


@st.cache_data(ttl=120)   # re-check every 2 min so live progress shows
def load_and_score() -> pd.DataFrame:
    df = _load_data()
    if df.empty:
        return df
    # Scoring columns may already be present from DB; recalc if missing
    if "Smart_Money_Score" not in df.columns:
        df = calculate_score(df)
    return df.sort_values("Smart_Money_Score", ascending=False).reset_index(drop=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Controls")

    # Ollama status
    ollama_ok, ollama_msg = check_ollama_status()
    st.caption(ollama_msg)
    st.divider()

    # ── Scraping controls ─────────────────────────────────────────────────────
    st.subheader("📡 Data Refresh")

    progress = get_scrape_progress()
    is_scraping = bool(progress.get("running", 0))
    total     = int(progress.get("total", 0))
    completed = int(progress.get("completed", 0))

    if is_scraping and total > 0:
        pct = completed / total
        st.progress(pct, text=f"Scraping {completed}/{total} stocks ({pct*100:.0f}%)")

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("🔄 Refresh New",
                     disabled=is_scraping,
                     width='stretch',
                     help="Scrapes only stocks not updated in last 24h"):
            msg = start_batch_scrape(force=False)
            st.toast(msg)
            st.cache_data.clear()

    with col_b:
        if st.button("🔁 Force All",
                     disabled=is_scraping,
                     width='stretch',
                     help="Re-scrapes every stock in the universe"):
            msg = start_batch_scrape(force=True)
            st.toast(msg)
            st.cache_data.clear()

    if is_scraping:
        if st.button("⏹ Stop Scrape", width='stretch'):
            stop_scrape()
            st.toast("Stop signal sent.")

    # Legacy: small watchlist refresh
    if st.button("⚡ Quick Refresh (watchlist only)",
                 width='stretch',
                 help="Only refreshes the 15 stocks in config.py WATCHLIST"):
        st.cache_data.clear()
        build_dataset(force_refresh=True)
        st.rerun()

    # Universe info
    try:
        uni_df = get_universe()
        st.caption(f"Universe: {len(uni_df)} stocks loaded")
    except Exception:
        st.caption("Universe: not set up yet")
        if st.button("ℹ️ How to load universe"):
            st.info("Run: python universe_builder.py\nSee README for details.")

    st.divider()

    # ── Filters ───────────────────────────────────────────────────────────────
    st.subheader("🔍 Filters")
    min_score   = st.slider("Min Score", 0, 100, 40, step=5)
    fii_selling = st.checkbox("FII Selling 4Q+", value=False)
    dii_buying  = st.checkbox("DII Buying 4Q+",  value=False)
    signals     = st.multiselect("Value Signal", options=[
                     "STRONG BUY","BUY","WATCH","FAIR VALUE","OVERVALUED"],
                  default=[])
    st.divider()

    with st.expander("📐 Score Weights"):
        for factor, weight in SCORE_WEIGHTS.items():
            st.caption(f"{factor.replace('_',' ').title()}: **{weight}%**")


# ── Load data ─────────────────────────────────────────────────────────────────
df = load_and_score()

if df.empty:
    st.title(APP_TITLE)
    st.warning(
        "No data yet. Click **Quick Refresh** in the sidebar to fetch the "
        "15-stock watchlist, or run `python universe_builder.py` then "
        "**Refresh New** to load the full universe."
    )
    st.stop()

# Populate sector filter
with st.sidebar:
    all_sectors = sorted(df["Sector"].unique().tolist()) if "Sector" in df.columns else []
    sectors = st.multiselect("Sectors", options=all_sectors,
                              default=all_sectors, key="sector_filter")

# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = df.copy()
if "Smart_Money_Score" in filtered.columns:
    filtered = filtered[filtered["Smart_Money_Score"] >= min_score]
if "Sector" in filtered.columns and sectors:
    filtered = filtered[filtered["Sector"].isin(sectors)]
if signals and "Value_Signal" in filtered.columns:
    filtered = filtered[filtered["Value_Signal"].str.contains(
        "|".join(signals), na=False)]
if fii_selling and "FII_Selling_4Q" in filtered.columns:
    filtered = filtered[filtered["FII_Selling_4Q"].astype(bool)]
if dii_buying and "DII_Buying_4Q" in filtered.columns:
    filtered = filtered[filtered["DII_Buying_4Q"].astype(bool)]


# ── Alert badge in title ──────────────────────────────────────────────────────
unread = get_unread_count()
badge  = f' <span class="alert-badge">🔔 {unread} alerts</span>' if unread else ""
st.markdown(f"<h1 style='margin-bottom:4px'>{APP_TITLE}{badge}</h1>",
            unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏆 Leaderboard",
    "💰 Value Zones",
    "📡 Smart Money Map",
    "🔬 Deep Dive",
    "🤖 AI Assistant",
    f"🔔 Alerts {'('+str(unread)+')' if unread else ''}",
])


# ══════ TAB 1 — LEADERBOARD ══════════════════════════════════════════════════
with tab1:
    st.subheader("Smart Money Score Leaderboard")

    buy_count = len(filtered[filtered.get("Value_Signal", pd.Series(dtype=str))
                   .str.contains("BUY", na=False)]) if "Value_Signal" in filtered.columns else 0
    avg_score = filtered["Smart_Money_Score"].mean() if "Smart_Money_Score" in filtered.columns else 0
    fii_ct    = filtered["FII_Selling_4Q"].astype(bool).sum() if "FII_Selling_4Q" in filtered.columns else 0
    dii_ct    = filtered["DII_Buying_4Q"].astype(bool).sum()  if "DII_Buying_4Q"  in filtered.columns else 0

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Stocks Showing",       str(len(filtered)))
    c2.metric("Avg Smart Score",       f"{avg_score:.1f}/100")
    c3.metric("In Value Zone (BUY)",   str(buy_count))
    c4.metric("FII Selling / DII Buying", f"{int(fii_ct)} / {int(dii_ct)}")

    if not filtered.empty:
        st.plotly_chart(plot_score_leaderboard(filtered), width='stretch')

    display_cols = [c for c in [
        "Name","Sector","Price","Smart_Money_Score","Grade",
        "PE","PB","RoE","DE","FII_Label","DII_Label",
        "Value_Signal","Buy_Zone_Low","Buy_Zone_High"
    ] if c in filtered.columns]

    def _color_signal(val):
        if "STRONG BUY" in str(val): return "background-color:#E8F5E9;color:#1B5E20;font-weight:bold"
        if "BUY"        in str(val): return "background-color:#F1F8E9;color:#2E7D32"
        if "WATCH"      in str(val): return "background-color:#FFF9C4;color:#7F4F00"
        if "OVERVALUED" in str(val): return "background-color:#FFEBEE;color:#B71C1C"
        return ""

    fmt = {}
    if "Price"          in display_cols: fmt["Price"]          = "₹{:.0f}"
    if "Buy_Zone_Low"   in display_cols: fmt["Buy_Zone_Low"]   = "₹{:.0f}"
    if "Buy_Zone_High"  in display_cols: fmt["Buy_Zone_High"]  = "₹{:.0f}"
    if "PE"             in display_cols: fmt["PE"]             = "{:.1f}x"
    if "PB"             in display_cols: fmt["PB"]             = "{:.1f}x"
    if "RoE"            in display_cols: fmt["RoE"]            = "{:.1f}%"
    if "DE"             in display_cols: fmt["DE"]             = "{:.2f}x"
    if "Smart_Money_Score" in display_cols: fmt["Smart_Money_Score"] = "{:.0f}"

    styled = (
        filtered[display_cols]
        .rename(columns={"Smart_Money_Score":"Score","Buy_Zone_Low":"Buy Low ₹",
                         "Buy_Zone_High":"Buy High ₹","Value_Signal":"Signal",
                         "FII_Label":"FII Trend","DII_Label":"DII Trend"})
        .style
        .map(_color_signal, subset=["Signal"] if "Signal" in
                  [c for c in ["Score","Buy Low ₹","Buy High ₹","Signal","FII Trend","DII Trend"]] else [])
        .format(fmt)
        .background_gradient(subset=["Score"], cmap="RdYlGn", vmin=0, vmax=100)
    )
    st.dataframe(styled, width='stretch', height=420)


# ══════ TAB 2 — VALUE ZONES ══════════════════════════════════════════════════
with tab2:
    st.subheader("Current Price vs Value Buy Zones")
    st.caption("Green band = 10–20% discount to fair value. Darker green = 30%+ discount (strong buy).")
    if not filtered.empty:
        st.plotly_chart(plot_value_zone(filtered), width='stretch')

    st.subheader("Stocks Currently in Value Zone")
    if "Value_Signal" in filtered.columns:
        value_df = filtered[
            filtered["Value_Signal"].str.contains("BUY|STRONG", na=False, regex=True)
        ]
        if value_df.empty:
            st.info("No stocks in value zone with current filters.")
        else:
            for _, row in value_df.iterrows():
                fv = row.get("Fair_Value",0) or row.get("fair_value",0)
                pr = row.get("Price",0)
                disc = (fv-pr)/fv*100 if fv>0 else 0
                with st.expander(
                    f"{format_signal_badge(row.get('Value_Signal',''))}  "
                    f"**{row.get('Name','')}** — ₹{pr:.0f} "
                    f"({disc:+.1f}% vs fair value ₹{fv:.0f})"
                ):
                    cc1,cc2,cc3,cc4 = st.columns(4)
                    cc1.metric("Current",     f"₹{pr:.0f}")
                    cc2.metric("Fair Value",  f"₹{fv:.0f}")
                    cc3.metric("Buy Zone",    f"₹{row.get('Buy_Zone_Low',0):.0f}–₹{row.get('Buy_Zone_High',0):.0f}")
                    cc4.metric("Strong Buy",  f"₹{row.get('Strong_Buy_Below',0):.0f}")
                    st.caption(row.get("Valuation_Methods",""))


# ══════ TAB 3 — SMART MONEY MAP ══════════════════════════════════════════════
with tab3:
    st.subheader("FII vs DII Quadrant")
    st.caption("Top-left = FII selling + DII buying = classic contrarian signal.")
    if not filtered.empty:
        st.plotly_chart(plot_fii_dii_trend(filtered), width='stretch')

    sh_cols = [c for c in ["Name","Promoter_Pct","FII_Pct","DII_Pct",
               "FII_Trend_Pct","DII_Trend_Pct","FII_Selling_4Q","DII_Buying_4Q"]
               if c in filtered.columns]
    if sh_cols:
        sh = filtered[sh_cols].copy()
        for col in ["FII_Selling_4Q","DII_Buying_4Q"]:
            if col in sh.columns:
                sh[col] = sh[col].map({True:"✅ Yes",False:"—",1:"✅ Yes",0:"—"})
        st.dataframe(sh, width='stretch')


# ══════ TAB 4 — DEEP DIVE ════════════════════════════════════════════════════
with tab4:
    st.subheader("Individual Stock Deep Dive")
    name_list = filtered["Name"].tolist() if "Name" in filtered.columns else []
    if not name_list:
        st.info("No stocks to display.")
    else:
        selected = st.selectbox("Select stock", options=name_list)
        row = filtered[filtered["Name"]==selected].iloc[0]

        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Price",      f"₹{row.get('Price',0):.0f}",
                  delta=f"{row.get('Price_3M_Ret',0):+.1f}% (3M)")
        c2.metric("Score",      f"{row.get('Smart_Money_Score',0):.0f}/100")
        c3.metric("Signal",     format_signal_badge(row.get("Value_Signal","")))
        c4.metric("Fair Value", f"₹{row.get('Fair_Value',0):.0f}")
        c5.metric("Grade",      row.get("Grade","—"))

        left,right = st.columns(2)
        with left:
            score_cols = [c for c in row.index if "Score_" in c]
            if score_cols:
                st.plotly_chart(plot_score_radar(row), width='stretch')
        with right:
            st.markdown("#### Fundamentals")
            for k,v in {
                "P/E":   f"{row.get('PE',0):.1f}x  (Sector: {row.get('Sector_PE',0):.1f}x)",
                "P/B":   f"{row.get('PB',0):.2f}x",
                "RoE":   f"{row.get('RoE',0):.1f}%",
                "RoCE":  f"{row.get('RoCE',0):.1f}%",
                "D/E":   f"{row.get('DE',0):.2f}x",
                "Rev Growth": f"{row.get('Revenue_Growth',0):.1f}%",
                "Mkt Cap":    f"₹{row.get('Market_Cap_Cr',0):,.0f} Cr",
            }.items():
                a,b = st.columns([2,3])
                a.caption(k); b.write(v)

            st.markdown("#### Smart Money")
            for k,v in {
                "FII":   f"{row.get('FII_Pct',0):.1f}% → {row.get('FII_Label','')}",
                "DII":   f"{row.get('DII_Pct',0):.1f}% → {row.get('DII_Label','')}",
                "Promoter": f"{row.get('Promoter_Pct',0):.1f}%",
                "1M / 3M / 6M": (f"{row.get('Price_1M_Ret',0):+.1f}% / "
                                  f"{row.get('Price_3M_Ret',0):+.1f}% / "
                                  f"{row.get('Price_6M_Ret',0):+.1f}%"),
                "52W Range": f"₹{row.get('52W_Low',0):.0f} – ₹{row.get('52W_High',0):.0f}",
            }.items():
                a,b = st.columns([2,3])
                a.caption(k); b.write(v)

        st.markdown("#### Value Buy Zone")
        vz1,vz2,vz3,vz4 = st.columns(4)
        vz1.metric("Current Price",   f"₹{row.get('Price',0):.0f}")
        vz2.metric("Fair Value",      f"₹{row.get('Fair_Value',0):.0f}")
        vz3.metric("Value Buy Zone",  f"₹{row.get('Buy_Zone_Low',0):.0f}–₹{row.get('Buy_Zone_High',0):.0f}")
        vz4.metric("Strong Buy Below",f"₹{row.get('Strong_Buy_Below',0):.0f}")
        st.caption(row.get("Valuation_Methods",""))

        url = row.get("Screener_URL","")
        if url:
            st.link_button("🔗 Open on Screener.in", url)


# ══════ TAB 5 — AI ASSISTANT ═════════════════════════════════════════════════
with tab5:
    st.subheader("AI Assistant (Ollama)")
    if not ollama_ok:
        st.warning(ollama_msg)
        st.code("ollama run llama3.1", language="bash")

    st.markdown("### Portfolio Summary")
    if st.button("Generate Summary", disabled=not ollama_ok):
        with st.spinner("Analysing..."):
            st.markdown(generate_portfolio_summary(filtered))

    st.divider()
    st.markdown("### Stock Insight")
    sel_ai = st.selectbox("Pick stock", options=filtered["Name"].tolist() if "Name" in filtered.columns else [], key="ai_sel")
    if st.button("Generate Insight", disabled=not ollama_ok):
        ai_row = filtered[filtered["Name"]==sel_ai].iloc[0]
        with st.spinner(f"Analysing {sel_ai}..."):
            st.markdown(f"```\n{generate_stock_insight(ai_row)}\n```")

    st.divider()
    st.markdown("### Natural Language Screener")
    st.caption("Try: 'high RoE stocks where FII is selling' / 'undervalued financials'")
    q = st.text_area("Your question:", height=80)
    if st.button("Run", disabled=not ollama_ok):
        with st.spinner("Running..."):
            st.markdown(natural_language_query(filtered, q))


# ══════ TAB 6 — ALERTS ═══════════════════════════════════════════════════════
with tab6:
    st.subheader("Alert Centre")

    col_left, col_right = st.columns([3,1])
    with col_left:
        if st.button("🔔 Check Alerts Now"):
            with st.spinner("Running alert checks..."):
                n = run_all_alerts(filtered)
            st.success(f"{n} alert(s) fired." if n else "No new alerts.")
            st.cache_data.clear()
    with col_right:
        if st.button("✅ Mark All Read"):
            acknowledge_alerts()
            st.rerun()

    st.divider()

    # Unread alerts (highlighted)
    unread_df = get_unacknowledged_alerts()
    if not unread_df.empty:
        st.markdown(f"#### 🔴 Unread ({len(unread_df)})")
        for _, a in unread_df.iterrows():
            alert_type = a.get("alert_type","")
            icon = {"value_zone":"💰","fii_selling_4q":"📡","score_60":"🏆"}.get(alert_type,"🔔")
            with st.expander(f"{icon} **{a.get('name','')}** — {alert_type.replace('_',' ').title()}  ·  {str(a.get('fired_at',''))[:16]}"):
                st.write(a.get("message",""))
                cc1,cc2 = st.columns(2)
                cc1.metric("Price at alert", f"₹{a.get('price',0):.0f}")
                cc2.metric("Score at alert", f"{a.get('score',0):.0f}/100")

    # Full alert history
    st.markdown("#### Alert History")
    hist_df = get_alerts(limit=200)
    if hist_df.empty:
        st.info("No alerts yet. Click **Check Alerts Now** after data is loaded.")
    else:
        st.dataframe(
            hist_df[["fired_at","name","alert_type","message","price","score","acknowledged"]]
            .rename(columns={"fired_at":"When","name":"Stock","alert_type":"Type",
                             "message":"Details","price":"Price ₹","score":"Score",
                             "acknowledged":"Read?"})
            .assign(**{"Read?": lambda d: d["Read?"].map({0:"❌",1:"✅"})}),
            width='stretch',
            height=400,
        )

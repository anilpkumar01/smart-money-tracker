# ============================================================
# utils.py — Plotly chart builders and display helpers
# ============================================================

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


# ── Colour map ────────────────────────────────────────────────────────────────

SIGNAL_COLORS = {
    "STRONG BUY":  "#1B5E20",
    "BUY":         "#388E3C",
    "WATCH":       "#F9A825",
    "FAIR VALUE":  "#1565C0",
}

def _signal_color(signal: str) -> str:
    for key, color in SIGNAL_COLORS.items():
        if key in str(signal):
            return color
    return "#C62828"  # red = overvalued


# ── Score radar chart ─────────────────────────────────────────────────────────

def plot_score_radar(row: pd.Series) -> go.Figure:
    """Radar chart showing all 10 factor scores for a single stock."""
    factors = [
        "FII Contrarian", "DII Accumulation", "PE Discount",
        "PB Ratio", "RoE Quality", "RoCE Efficiency",
        "Debt Safety", "Rev Growth", "Promoter", "52W Safety"
    ]
    score_cols = [
        "Score_FII", "Score_DII", "Score_PE", "Score_PB",
        "Score_RoE", "Score_RoCE", "Score_Debt",
        "Score_RevGrowth", "Score_Promoter", "Score_52W"
    ]
    scores = [row.get(c, 0) for c in score_cols]

    fig = go.Figure(go.Scatterpolar(
        r=scores + [scores[0]],  # close the polygon
        theta=factors + [factors[0]],
        fill="toself",
        fillcolor="rgba(33, 150, 243, 0.2)",
        line=dict(color="#1565C0", width=2),
        name=row.get("Name", ""),
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 10],
                            tickfont=dict(size=9)),
            angularaxis=dict(tickfont=dict(size=10)),
        ),
        showlegend=False,
        title=dict(text=f"Factor Breakdown — {row.get('Name', '')}", x=0.5),
        margin=dict(t=60, b=20, l=40, r=40),
        height=350,
    )
    return fig


# ── Value zone price chart ────────────────────────────────────────────────────

def plot_value_zone(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart showing current price vs value buy zone
    for each stock in the watchlist.
    """
    df = df.copy().sort_values("Smart_Money_Score", ascending=True)

    fig = go.Figure()

    # Strong buy zone (shaded band)
    fig.add_trace(go.Bar(
        y=df["Name"],
        x=df["Strong_Buy_Below"],
        name="Strong Buy Below",
        orientation="h",
        marker_color="rgba(27, 94, 32, 0.3)",
        showlegend=True,
    ))

    # Value buy zone (Buy_Zone_Low → Buy_Zone_High)
    fig.add_trace(go.Bar(
        y=df["Name"],
        x=df["Buy_Zone_High"] - df["Buy_Zone_Low"],
        base=df["Buy_Zone_Low"],
        name="Value Buy Zone",
        orientation="h",
        marker_color="rgba(56, 142, 60, 0.5)",
        showlegend=True,
    ))

    # Fair value line
    fig.add_trace(go.Scatter(
        y=df["Name"],
        x=df["Fair_Value"],
        mode="markers",
        marker=dict(symbol="line-ns", size=12, color="#1565C0",
                    line=dict(width=2, color="#1565C0")),
        name="Fair Value",
    ))

    # Current price dot
    colors = [_signal_color(s) for s in df["Value_Signal"]]
    fig.add_trace(go.Scatter(
        y=df["Name"],
        x=df["Price"],
        mode="markers+text",
        marker=dict(size=10, color=colors),
        text=["₹" + str(p) for p in df["Price"]],
        textposition="middle right",
        textfont=dict(size=9),
        name="Current Price",
    ))

    fig.update_layout(
        barmode="overlay",
        title="Current Price vs Value Buy Zones",
        xaxis_title="Price (₹)",
        yaxis_title="",
        height=max(350, len(df) * 40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        margin=dict(l=160, r=80, t=80, b=40),
    )
    return fig


# ── Score leaderboard bar ─────────────────────────────────────────────────────

def plot_score_leaderboard(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar chart of Smart Money Scores with grade colour coding."""
    grade_colors = {
        "A+": "#1B5E20", "A ": "#388E3C",
        "B ": "#F9A825", "C ": "#E65100", "D ": "#B71C1C"
    }

    bar_colors = []
    for grade in df["Grade"]:
        matched = "#888"
        for key, color in grade_colors.items():
            if key.strip() in grade:
                matched = color
                break
        bar_colors.append(matched)

    fig = go.Figure(go.Bar(
        x=df["Smart_Money_Score"],
        y=df["Name"],
        orientation="h",
        marker_color=bar_colors,
        text=df["Grade"],
        textposition="outside",
        textfont=dict(size=10),
    ))
    fig.add_vline(x=60, line_dash="dash", line_color="#1565C0",
                  annotation_text="Buy threshold (60)", annotation_position="top right")
    fig.update_layout(
        title="Smart Money Score Leaderboard",
        xaxis=dict(title="Score (0–100)", range=[0, 110]),
        yaxis=dict(title=""),
        height=max(350, len(df) * 40),
        margin=dict(l=160, r=100, t=60, b=40),
    )
    return fig


# ── FII/DII trend chart ───────────────────────────────────────────────────────

def plot_fii_dii_trend(df: pd.DataFrame) -> go.Figure:
    """
    Scatter plot: FII 4Q trend % (x) vs DII 4Q trend % (y)
    Robust: falls back to zeros if columns missing (e.g. first load).
    """
    # Safe column access — fill missing with 0 so chart still renders
    fii_x     = df["FII_Trend_Pct"]     if "FII_Trend_Pct"     in df.columns else pd.Series([0]*len(df))
    dii_y     = df["DII_Trend_Pct"]     if "DII_Trend_Pct"     in df.columns else pd.Series([0]*len(df))
    names     = df["Name"]              if "Name"              in df.columns else pd.Series([""]*len(df))
    scores    = df["Smart_Money_Score"] if "Smart_Money_Score" in df.columns else pd.Series([50]*len(df))

    fig = go.Figure()

    # Quadrant shading
    fig.add_shape(type="rect", x0=-20, x1=0, y0=0, y1=20,
                  fillcolor="rgba(56,142,60,0.08)", line_width=0)

    # Scatter
    fig.add_trace(go.Scatter(
        x=fii_x,
        y=dii_y,
        mode="markers+text",
        text=names,
        textposition="top center",
        textfont=dict(size=9),
        marker=dict(
            size=scores / 5 + 8,
            color=scores,
            colorscale="RdYlGn",
            colorbar=dict(title="Score"),
            line=dict(width=1, color="white"),
            showscale=True,
        ),
        hovertemplate=(
            "<b>%{text}</b><br>"
            "FII 4Q Change: %{x:.1f}%<br>"
            "DII 4Q Change: %{y:.1f}%<br>"
            "<extra></extra>"
        ),
    ))

    # Axis lines
    fig.add_hline(y=0, line_color="grey", line_width=1)
    fig.add_vline(x=0, line_color="grey", line_width=1)

    # Quadrant labels
    fig.add_annotation(x=-10, y=8,  text="🟢 Smart Money Signal",
                        showarrow=False, font=dict(color="#388E3C", size=11))
    fig.add_annotation(x=8,   y=8,  text="🔵 Both Buying (expensive?)",
                        showarrow=False, font=dict(color="#1565C0", size=11))
    fig.add_annotation(x=-10, y=-8, text="🔴 Both Reducing (caution)",
                        showarrow=False, font=dict(color="#B71C1C", size=11))

    fig.update_layout(
        title="FII vs DII 4-Quarter Trend (Smart Money Quadrant)",
        xaxis=dict(title="FII 4Q Change (%) — Negative = Selling", zeroline=False),
        yaxis=dict(title="DII 4Q Change (%) — Positive = Buying", zeroline=False),
        height=480,
        margin=dict(t=60, b=60, l=60, r=40),
    )
    return fig


# ── Formatting helpers ────────────────────────────────────────────────────────

def format_signal_badge(signal: str) -> str:
    """Returns an HTML badge for the value signal."""
    color_map = {
        "STRONG BUY":  ("✅", "#1B5E20", "#E8F5E9"),
        "BUY":         ("🟢", "#2E7D32", "#F1F8E9"),
        "WATCH":       ("🟡", "#7F4F00", "#FFF9C4"),
        "FAIR VALUE":  ("🔵", "#0D47A1", "#E3F2FD"),
        "OVERVALUED":  ("🔴", "#B71C1C", "#FFEBEE"),
    }
    for key, (icon, fg, bg) in color_map.items():
        if key in str(signal).upper():
            return f"{icon} {signal}"
    return signal


def style_score(score: float) -> str:
    """Colour-codes a score value as a string."""
    if score >= 75: return f"🟢 {score}"
    if score >= 60: return f"🟡 {score}"
    if score >= 45: return f"🟠 {score}"
    return f"🔴 {score}"

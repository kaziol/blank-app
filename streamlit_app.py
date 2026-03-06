# dashboard.py

import json
import time

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv

load_dotenv()

from db.database import Database

st.set_page_config(
    page_title="Trade Engine",
    layout="wide",
    page_icon="🧠",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stSidebar"] { background: #0f1117; }
    .metric-card {
        background: #1a1d27;
        border: 1px solid #2d3142;
        border-radius: 8px;
        padding: 1rem 1.2rem;
    }
    .idea-long  { color: #00d4aa; font-weight: 700; }
    .idea-short { color: #ff4b6e; font-weight: 700; }
    .tier-high   { background: #1a3a2a; color: #00d4aa; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; }
    .tier-medium { background: #2a2a1a; color: #f0c040; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; }
    .tier-low    { background: #2a1a1a; color: #ff6b6b; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; }

    /* ── TA Watchlist styles ── */
    .ta-card {
        background: #1a1d27;
        border: 1px solid #2d3142;
        border-radius: 10px;
        padding: 1rem 1.4rem;
        margin-bottom: 0.5rem;
    }
    .ta-card-squeeze { border-color: #f0c040; box-shadow: 0 0 10px rgba(240,192,64,0.12); }
    .ta-card-long    { border-left: 3px solid #00d4aa; }
    .ta-card-short   { border-left: 3px solid #ff4b6e; }
    .badge {
        display: inline-block;
        padding: 0.15rem 0.55rem;
        border-radius: 5px;
        font-size: 0.72rem;
        font-weight: 700;
    }
    .b-long     { background:#0d2b1d; color:#00d4aa; border:1px solid #166534; }
    .b-short    { background:#2b0d0d; color:#ff4b6e; border:1px solid #7f1d1d; }
    .b-neutral  { background:#1e1f2b; color:#8b8fa8; border:1px solid #334155; }
    .b-squeeze  { background:#2b2200; color:#f0c040; border:1px solid #92400e; }
    .b-walkup   { background:#0d2b1d; color:#00d4aa; border:1px solid #166534; }
    .b-walkdn   { background:#2b0d0d; color:#ff4b6e; border:1px solid #7f1d1d; }
    .b-high     { background:#0d2b1d; color:#00d4aa; border:1px solid #166534; }
    .b-medium   { background:#2b2200; color:#f0c040; border:1px solid #92400e; }
    .b-low      { background:#2b0d0d; color:#ff4b6e; border:1px solid #7f1d1d; }
    .b-conflict { background:#1e0d2b; color:#a78bfa; border:1px solid #6d28d9; }
    .b-vstrong-up   { background:#0d2b1d; color:#00d4aa; border:2px solid #166534; }
    .b-strong-up    { background:#0d2b1d; color:#00d4aa; border:1px solid #166534; }
    .b-weak-up      { background:#2b2200; color:#f0c040; border:1px solid #92400e; }
    .b-ranging      { background:#1e1f2b; color:#8b8fa8; border:1px solid #334155; }
    .b-weak-dn      { background:#2b1500; color:#f97316; border:1px solid #7c2d12; }
    .b-strong-dn    { background:#2b0d0d; color:#ff4b6e; border:1px solid #7f1d1d; }
    .b-vstrong-dn   { background:#2b0d0d; color:#ff4b6e; border:2px solid #7f1d1d; }
    .lbl  { font-size:0.68rem; color:#8b8fa8; text-transform:uppercase; letter-spacing:0.05em; }
    .val  { font-size:0.9rem; color:#f1f5f9; font-weight:600; }
    .muted { color:#8b8fa8; font-size:0.78rem; }
    a.fv-link { color:#4f8bff; text-decoration:none; font-size:0.78rem; font-weight:600; }
    a.fv-link:hover { text-decoration:underline; }
    .ta-div { border-top:1px solid #2d3142; margin:0.5rem 0; }
</style>
""", unsafe_allow_html=True)

db = Database.get()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🧠 Trade Engine")
    st.caption("Supply Chain Signal Intelligence")
    st.divider()

    min_conf   = st.slider("Min confidence", 0, 100, 50)
    _lb_all  = st.checkbox("All time", value=False, key="lb_all")
    if _lb_all:
        hours_back = None
    else:
        hours_back = st.number_input(
            "Lookback (hours)", min_value=1, max_value=8760,
            value=48, step=1, key="lb_hours",
        )
    directions = st.multiselect("Direction", ["LONG", "SHORT"],
                                default=["LONG", "SHORT"])
    sectors    = st.multiselect("Sector filter", [
        "Energy", "Materials", "Industrials", "Consumer Discretionary",
        "Consumer Staples", "Health Care", "Financials", "Information Technology",
        "Communication Services", "Utilities", "Real Estate", "Commodities",
    ])

    st.divider()

    if st.button("▶ Run pipeline now", width="stretch", type="primary"):
        import asyncio
        from pipeline import run_pipeline
        with st.spinner("Running pipeline..."):
            asyncio.run(run_pipeline())
        st.success("Done!")
        st.rerun()

    st.divider()
    st.caption("🔄 Auto-refresh every 15 min")
    if st.button("🔄 Refresh data", width="stretch"):
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
raw_ideas = db.get_recent_ideas(
    hours=hours_back,          # None = all time
    min_confidence=min_conf,
    direction=directions,
)
ideas = pd.DataFrame(raw_ideas) if raw_ideas else pd.DataFrame()

if sectors and not ideas.empty and "sector" in ideas.columns:
    ideas = ideas[ideas["sector"].isin(sectors)]

if ideas.empty:
    st.info("No ideas match your filters. Try lowering confidence or extending the lookback window.")
    st.stop()

# ── Tab layout ────────────────────────────────────────────────────────────────
tab_ideas, tab_ledger, tab_signals, tab_ta_watch, tab_tokens, tab_health = st.tabs([
    "💡 Ideas", "📊 Ledger", "📡 Signals", "📈 TA Watchlist", "🔢 Token Usage", "🩺 Pipeline Health"
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — IDEAS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_ideas:
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Ideas",    len(ideas))
    c2.metric("Avg Confidence", f"{ideas['confidence'].mean():.0f}")
    c3.metric("Long",           int((ideas["direction"] == "LONG").sum()))
    c4.metric("Short",          int((ideas["direction"] == "SHORT").sum()))
    c5.metric("High Conv.",     int((ideas["size_tier"] == "HIGH").sum()))
    avg_conf_long  = ideas[ideas["direction"] == "LONG"]["confidence"].mean()
    avg_conf_short = ideas[ideas["direction"] == "SHORT"]["confidence"].mean()
    c6.metric("L/S Conf Gap",
              f"{abs(avg_conf_long - avg_conf_short):.0f}",
              help="Confidence gap between long and short ideas.")

    st.divider()

    st.subheader("Trade Ideas")

    # ── Keyword search ────────────────────────────────────────────────────────
    _kw = st.text_input(
        "🔍 Search ticker / thesis",
        placeholder="e.g. NVDA  or  supply chain  or  semiconductor",
        key="ideas_search",
    ).strip().lower()

    if _kw:
        _mask = pd.Series([False] * len(ideas), index=ideas.index)
        if "ticker" in ideas.columns:
            _mask |= ideas["ticker"].str.lower().str.contains(_kw, na=False)
        if "thesis" in ideas.columns:
            _mask |= ideas["thesis"].str.lower().str.contains(_kw, na=False)
        ideas = ideas[_mask]
        if ideas.empty:
            st.warning(f"No ideas match **{_kw}**.")

    display_cols = [c for c in [
        "generated_at", "ticker", "direction", "sector", "confidence",
        "size_tier", "time_horizon", "thesis", "pricing_risk",
        "contrarian_flag", "feed_topic",
    ] if c in ideas.columns]

    st.dataframe(
        ideas[display_cols].sort_values("confidence", ascending=False),
        width="stretch",
        hide_index=True,
        column_config={
            "confidence":      st.column_config.ProgressColumn("Conf", min_value=0, max_value=100),
            "generated_at":    st.column_config.DatetimeColumn("Generated", format="MMM D, HH:mm"),
            "contrarian_flag": st.column_config.CheckboxColumn("Contrarian"),
            "thesis":          st.column_config.TextColumn("Thesis", width="large"),
            "pricing_risk":    st.column_config.TextColumn("Pricing Risk", width="medium"),
        },
    )

    if "chain_map" in ideas.columns:
        st.divider()
        st.subheader("Supply Chain Analysis")
        selected_ticker = st.selectbox("View chain map for idea", ideas["ticker"].unique())
        selected = ideas[ideas["ticker"] == selected_ticker].iloc[0]
        chain = selected.get("chain_map") or {}
        if chain:
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown("**Event Summary**")
                st.info(chain.get("event_summary", "—"))
                st.markdown("**Squeeze Targets**")
                for t in chain.get("squeeze_targets", []):
                    st.markdown(
                        f"- **{t.get('company')}** — {t.get('reason', '')} "
                        f"*(Dependence: {t.get('dependence_level')}, Pricing power: {t.get('pricing_power')})*"
                    )
            with col_b:
                st.markdown("**Downstream Customers**")
                for d in chain.get("downstream", []):
                    impact_icon = "🔴" if d.get("impact") == "NEGATIVE" else "🟢"
                    st.markdown(f"{impact_icon} **{d.get('company')}** — {d.get('mechanism', '')}")
                st.markdown("**Geo/Regulatory Note**")
                st.warning(chain.get("geo_regulatory_note") or "None identified")
        else:
            st.caption("No chain map available for this idea.")

    st.divider()
    ta_ideas = ideas[ideas["ta_score"].notna()] if "ta_score" in ideas.columns else pd.DataFrame()

    if not ta_ideas.empty:
        st.subheader("📊 Technical Analysis")
        ta_cols = [c for c in [
            "ticker", "direction", "confidence",
            "ta_score", "ta_confidence", "ta_bias",
            "ta_entry", "ta_stop_loss", "ta_rr", "ta_summary",
        ] if c in ta_ideas.columns]

        def _conf_icon(v):
            return {"HighConfidence": "🟢", "MediumConfidence": "🟡",
                    "LowConfidence": "🔴", "ConflictingSignals": "⚡"}.get(v, "⚪")

        ta_display = ta_ideas[ta_cols].copy()
        if "ta_confidence" in ta_display.columns:
            ta_display["ta_confidence"] = ta_display["ta_confidence"].apply(
                lambda v: _conf_icon(v) + " " + (v or "") if v else ""
            )

        st.dataframe(
            ta_display.sort_values("ta_score", ascending=False),
            width="stretch", hide_index=True,
            column_config={
                "ticker":        st.column_config.TextColumn("Ticker"),
                "direction":     st.column_config.TextColumn("Dir"),
                "confidence":    st.column_config.ProgressColumn("Idea Conf", min_value=0, max_value=100),
                "ta_score":      st.column_config.ProgressColumn("TA Score",  min_value=0, max_value=100),
                "ta_confidence": st.column_config.TextColumn("TA Signal"),
                "ta_bias":       st.column_config.TextColumn("Bias"),
                "ta_entry":      st.column_config.NumberColumn("Entry $",  format="$%.2f"),
                "ta_stop_loss":  st.column_config.NumberColumn("Stop $",   format="$%.2f"),
                "ta_rr":         st.column_config.NumberColumn("R/R",      format="%.1fx"),
                "ta_summary":    st.column_config.TextColumn("Summary",    width="large"),
            },
        )

        st.divider()
        st.subheader("Technical Analysis Detail")
        selected_ta = st.selectbox(
            "Select idea to inspect",
            options=ta_ideas["ticker"].tolist(),
            format_func=lambda t: f"{t}  —  TA score: {ta_ideas[ta_ideas['ticker']==t]['ta_score'].iloc[0]:.0f}/100",
            key="ta_select",
        )
        row  = ta_ideas[ta_ideas["ticker"] == selected_ta].iloc[0]
        full = row.get("ta_full_result") or {}
        if isinstance(full, str):
            try: full = json.loads(full)
            except: full = {}

        if full:
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("TA Score",   f"{full.get('score', 0):.0f}/100")
            m2.metric("Confidence", full.get("overallConfidence","—").replace("Confidence","").replace("Signals","Signals"))
            ts = full.get("tradeSetup", {})
            m3.metric("Bias",       ts.get("bias","—"))
            m4.metric("R/R Ratio",  f"{ts.get('riskRewardRatio',0):.1f}x" if ts.get("riskRewardRatio") else "—")
            bd = full.get("scoreBreakdown", {})
            m5.metric("Pattern Q",  f"{bd.get('patternQuality',0):.0f}/25")

            col_left, col_right = st.columns(2)
            with col_left:
                st.markdown("**Score Breakdown**")
                breakdown_data = {
                    "Dimension": ["Trend Alignment","Volume Confirmation","Pattern Quality","Risk/Reward"],
                    "Score":     [bd.get("trendAlignment",0), bd.get("volumeConfirmation",0),
                                  bd.get("patternQuality",0), bd.get("riskReward",0)],
                }
                fig_bd = go.Figure(go.Bar(
                    x=breakdown_data["Score"], y=breakdown_data["Dimension"], orientation="h",
                    marker_color=["#00d4aa" if s >= 18 else "#f0c040" if s >= 12 else "#ff4b6e"
                                  for s in breakdown_data["Score"]],
                    text=[f"{s}/25" for s in breakdown_data["Score"]], textposition="outside",
                ))
                fig_bd.update_layout(template="plotly_dark", height=180,
                                     xaxis=dict(range=[0,25]), margin=dict(t=10,b=10,l=10,r=40))
                st.plotly_chart(fig_bd, width="stretch")
                st.markdown("**Momentum**")
                mom  = full.get("momentum", {})
                rsi  = mom.get("rsi",  {})
                macd = mom.get("macd", {})
                mc1, mc2 = st.columns(2)
                mc1.metric("RSI",  f"{rsi.get('value',0):.1f}", rsi.get("signal",""))
                mc2.metric("MACD", f"{macd.get('value',0):.4f}", macd.get("crossover",""))
                mas = full.get("movingAverages", {})
                if mas:
                    st.markdown("**Moving Averages**")
                    for key in ["MA20","MA60","MA250"]:
                        ma = mas.get(key)
                        if ma:
                            st.caption(f"{key} ({ma.get('type','SMA')}): **${ma.get('value',0):.2f}**")
                    cross = mas.get("crossoverSignal","NoCrossover")
                    if cross != "NoCrossover":
                        icon = "🌟" if cross == "GoldenCross" else "💀"
                        st.warning(f"{icon} {cross}")
            with col_right:
                st.markdown("**Trade Setup**")
                setup_rows = []
                if ts.get("entryPrice"):        setup_rows.append(("Entry",        f"${ts['entryPrice']:.2f}"))
                if ts.get("stopLoss"):          setup_rows.append(("Stop Loss",    f"${ts['stopLoss']:.2f}"))
                if ts.get("invalidationPrice"): setup_rows.append(("Invalidation", f"${ts['invalidationPrice']:.2f}"))
                for t in ts.get("targets", []):
                    setup_rows.append((t.get("label","Target"), f"${t.get('price',0):.2f}"))
                for label, val in setup_rows:
                    st.caption(f"{label}: **{val}**")
                if ts.get("entryCondition"):
                    st.info(f"📋 {ts['entryCondition']}")
                fib = full.get("fibonacciRetracement", {})
                if fib.get("levels"):
                    st.markdown("**Fibonacci Levels**")
                    price_val = ts.get("entryPrice", 0)
                    for lvl in fib.get("levels", []):
                        ratio = lvl.get("ratio", 0)
                        fp    = lvl.get("price", 0)
                        marker = " ◀ current" if price_val and abs(fp - price_val) / max(price_val,1) < 0.01 else ""
                        st.caption(f"{ratio:.3f}  →  **${fp:.2f}**{marker}")

            patterns = full.get("patternData", [])
            if patterns:
                st.markdown("**Detected Patterns**")
                for p in patterns:
                    trend_icon = {"Bullish":"🟢","Bearish":"🔴","Consolidation":"🟡"}.get(p.get("trendDirection"),"⚪")
                    signal_map = {"LongEntry":"📈 Long Entry","ShortEntry":"📉 Short Entry",
                                  "StrongBuy":"🚀 Strong Buy","StrongSell":"💥 Strong Sell","NoAction":"➡️ No Action"}
                    sig_label = signal_map.get(p.get("signalType"), p.get("signalType",""))
                    with st.expander(f"{trend_icon} {p.get('pattern','—')}  [{p.get('interval','')}]  "
                                     f"— {p.get('trendStrength','')} / {sig_label}"):
                        st.caption(f"Confidence: {p.get('AnalysisConfidence','—')}")
                        st.caption(f"S/R State:  {p.get('SupportResistanceState','—')}")
                        if p.get("details"): st.json(p["details"])
            st.markdown("**Summary**")
            st.success(full.get("summary","No summary available."))
        else:
            st.caption("Full TA result not available for this idea.")
    else:
        if "ta_score" in ideas.columns:
            st.info("No technical analysis data yet.")

    st.divider()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("Confidence Distribution")
        fig = px.histogram(ideas, x="confidence", color="direction", nbins=20,
                           color_discrete_map={"LONG":"#00d4aa","SHORT":"#ff4b6e"}, template="plotly_dark")
        fig.update_layout(bargap=0.1, showlegend=True)
        st.plotly_chart(fig, width="stretch")
    with col2:
        if "sector" in ideas.columns:
            st.subheader("Ideas by Sector")
            sector_counts = ideas.groupby(["sector","direction"]).size().reset_index(name="count")
            fig2 = px.bar(sector_counts, x="sector", y="count", color="direction",
                          color_discrete_map={"LONG":"#00d4aa","SHORT":"#ff4b6e"}, template="plotly_dark")
            fig2.update_layout(xaxis_tickangle=-30)
            st.plotly_chart(fig2, width="stretch")
    with col3:
        st.subheader("Size Tier Breakdown")
        tier_counts = ideas["size_tier"].value_counts().reset_index()
        tier_counts.columns = ["tier","count"]
        fig3 = px.pie(tier_counts, values="count", names="tier", color="tier",
                      color_discrete_map={"HIGH":"#00d4aa","MEDIUM":"#f0c040","LOW":"#ff6b6b"},
                      template="plotly_dark", hole=0.4)
        st.plotly_chart(fig3, width="stretch")

    if "contrarian_flag" in ideas.columns:
        contrarian = ideas[ideas["contrarian_flag"] == True]
        if not contrarian.empty:
            st.divider()
            st.subheader(f"⚡ Contrarian Ideas ({len(contrarian)})")
            st.caption("These ideas go against consensus — higher risk, potentially higher reward.")
            for _, row in contrarian.iterrows():
                with st.expander(f"{row['direction']} {row['ticker']} — conf {row['confidence']}"):
                    st.write(row.get("thesis",""))
                    if "invalidation" in row:
                        st.caption(f"Invalidation: {row['invalidation']}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — LEDGER
# ═══════════════════════════════════════════════════════════════════════════════
with tab_ledger:
    st.subheader("Paper Trading Ledger")
    st.caption("Simulated P&L tracking — one virtual position per idea, direction-adjusted.")
    try:
        _last_snap = db._execute("""
            SELECT MAX(fetched_at) AS last_fetch FROM price_snapshots
            WHERE fetch_reason = 'outcome_tracking'
        """, fetch=True)
        if _last_snap and _last_snap[0]["last_fetch"]:
            from datetime import datetime, timezone
            _last = _last_snap[0]["last_fetch"]
            if hasattr(_last, "tzinfo") and _last.tzinfo is None:
                _last = _last.replace(tzinfo=timezone.utc)
            _age_mins = int((datetime.now(timezone.utc) - _last).total_seconds() / 60)
            _age_str  = f"{_age_mins}m ago" if _age_mins < 60 else f"{_age_mins//60}h {_age_mins%60}m ago"
            st.caption(f"{'⚠️' if _age_mins > 60 else '🟢'} Prices last updated: {_age_str}")
    except Exception:
        pass

    NOTIONAL = {"HIGH": 10_000, "MEDIUM": 5_000, "LOW": 2_000}
    _col_refresh, _col_status = st.columns([1, 4])
    with _col_refresh:
        _refresh_clicked = st.button("🔄 Refresh Prices", width="stretch")
    if _refresh_clicked:
        with st.spinner("Fetching latest prices for open positions..."):
            try:
                from paper_trading import run_mtm_update
                _mtm = run_mtm_update()
                _updated = _mtm.get("updated", 0)
                _closed  = _mtm.get("closed_sl",0) + _mtm.get("closed_tp",0) + _mtm.get("closed_exp",0)
                _col_status.success(f"✓ Updated {_updated} prices" + (f", closed {_closed} positions" if _closed else ""))
            except Exception as _e:
                _col_status.error(f"Price refresh failed: {_e}")

    try:
        rows = db._execute("""
            SELECT idea_id, ticker, direction, confidence, size_tier, time_horizon,
                   thesis, generated_at, entry_price, current_price, age_days, outcome_return_pct
            FROM v_idea_outcomes ORDER BY generated_at DESC
        """, fetch=True)
        positions = [dict(r) for r in rows] if rows else []
    except Exception as e:
        st.warning(f"Could not load ledger data: {e}")
        st.caption("Make sure price_and_llm_messages.sql has been run to create v_idea_outcomes.")
        positions = []

    if not positions:
        st.info("No positions yet.")
        st.stop()

    for p in positions:
        tier     = p.get("size_tier") or "LOW"
        notional = NOTIONAL.get(tier, 2_000)
        p["notional"] = notional
        ep  = p.get("entry_price")
        cp  = p.get("current_price")
        ret = p.get("outcome_return_pct")
        if ret is not None:
            p["return_pct"] = float(ret)
            p["pnl_usd"]    = notional * float(ret) / 100
            p["status"]     = "CLOSED" if p.get("age_days", 0) > 0 else "OPEN"
        elif ep and cp:
            raw     = (float(cp) - float(ep)) / float(ep)
            ret_adj = raw if p["direction"] == "LONG" else -raw
            p["return_pct"] = ret_adj * 100
            p["pnl_usd"]    = notional * ret_adj
            p["status"]     = "OPEN"
        else:
            p["return_pct"] = None
            p["pnl_usd"]    = None
            p["status"]     = "OPEN"

    open_pos     = [p for p in positions if p["status"] == "OPEN"   and p["return_pct"] is not None]
    closed_pos   = [p for p in positions if p["status"] == "CLOSED" and p["return_pct"] is not None]
    all_with_pnl = [p for p in positions if p["return_pct"] is not None]

    c1,c2,c3,c4,c5 = st.columns(5)
    total_pnl = sum(p["pnl_usd"] for p in all_with_pnl)
    winners   = [p for p in closed_pos if p["return_pct"] > 0]
    win_rate  = len(winners) / len(closed_pos) if closed_pos else None
    open_pnl  = sum(p["pnl_usd"] for p in open_pos)
    avg_conf  = sum(p["confidence"] for p in all_with_pnl) / len(all_with_pnl) if all_with_pnl else 0
    c1.metric("Total P&L",    f"${total_pnl:+,.0f}", delta_color="normal" if total_pnl >= 0 else "inverse")
    c2.metric("Open P&L",     f"${open_pnl:+,.0f}",  delta_color="normal" if open_pnl  >= 0 else "inverse")
    c3.metric("Open / Total", f"{len(open_pos)} / {len(all_with_pnl)}")
    c4.metric("Win Rate",     f"{win_rate*100:.0f}%" if win_rate is not None else "—")
    c5.metric("Avg Confidence", f"{avg_conf:.0f}")
    st.divider()

    if open_pos:
        st.subheader(f"Open Positions ({len(open_pos)})")
        open_df = pd.DataFrame(open_pos)[["ticker","direction","confidence","size_tier",
            "entry_price","current_price","return_pct","pnl_usd","age_days","time_horizon","thesis"]].copy()
        open_df["return_pct"] = open_df["return_pct"].round(2)
        open_df["pnl_usd"]    = open_df["pnl_usd"].round(0)
        st.dataframe(open_df.sort_values("return_pct", ascending=False), width="stretch", hide_index=True,
            column_config={
                "confidence":    st.column_config.ProgressColumn("Conf", min_value=0, max_value=100),
                "entry_price":   st.column_config.NumberColumn("Entry",   format="$%.2f"),
                "current_price": st.column_config.NumberColumn("Now",     format="$%.2f"),
                "return_pct":    st.column_config.NumberColumn("Return %",format="%.1f%%"),
                "pnl_usd":       st.column_config.NumberColumn("P&L",     format="$%.0f"),
                "age_days":      st.column_config.NumberColumn("Days"),
                "thesis":        st.column_config.TextColumn("Thesis", width="large"),
            })

    st.divider()
    st.subheader("Cumulative P&L")
    chart_data = sorted(all_with_pnl, key=lambda x: x.get("generated_at") or "")
    if chart_data:
        running_pnl, chart_rows = 0, []
        for p in chart_data:
            running_pnl += p["pnl_usd"]
            chart_rows.append({"date": p.get("generated_at"), "pnl": round(running_pnl,2), "ticker": p["ticker"]})
        chart_df = pd.DataFrame(chart_rows)
        fig_cum  = px.line(chart_df, x="date", y="pnl", template="plotly_dark")
        fig_cum.update_traces(line_color="#00d4aa", line_width=2)
        fig_cum.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
        st.plotly_chart(fig_cum, width="stretch")

    st.divider()
    col_cal, col_dir = st.columns(2)
    with col_cal:
        st.subheader("Confidence Calibration")
        if closed_pos:
            buckets: dict = {}
            for p in closed_pos:
                b = f"{(p['confidence']//10)*10}–{(p['confidence']//10)*10+9}"
                if b not in buckets:
                    buckets[b] = {"total":0,"wins":0,"mid":(p['confidence']//10)*10+5}
                buckets[b]["total"] += 1
                if p["return_pct"] > 0: buckets[b]["wins"] += 1
            cal_rows = []
            for b, v in sorted(buckets.items()):
                actual = v["wins"]/v["total"]*100 if v["total"] else 0
                cal_rows.append({"Confidence":b,"n":v["total"],"Actual Win %":round(actual,1),"Ideal %":v["mid"],"Δ":round(actual-v["mid"],1)})
            st.dataframe(pd.DataFrame(cal_rows), width="stretch", hide_index=True)
        else:
            st.info("No closed positions yet.")
    with col_dir:
        st.subheader("P&L by Direction & Tier")
        if closed_pos:
            dir_rows = []
            for direction in ("LONG","SHORT"):
                dp = [p for p in closed_pos if p["direction"]==direction]
                if not dp: continue
                wins = [p for p in dp if p["return_pct"]>0]
                dir_rows.append({"Direction":direction,"n":len(dp),"Win Rate":f"{len(wins)/len(dp)*100:.0f}%",
                                  "Avg Return":f"{sum(p['return_pct'] for p in dp)/len(dp):+.1f}%",
                                  "Total P&L":f"${sum(p['pnl_usd'] for p in dp):+,.0f}"})
            if dir_rows: st.dataframe(pd.DataFrame(dir_rows), width="stretch", hide_index=True)
        else:
            st.info("P&L breakdown available once positions close.")

    if closed_pos:
        st.divider()
        st.subheader(f"Closed Positions ({len(closed_pos)})")
        closed_df = pd.DataFrame(closed_pos)[["ticker","direction","confidence","return_pct","pnl_usd","age_days","thesis"]].copy()
        closed_df["return_pct"] = closed_df["return_pct"].round(2)
        closed_df["pnl_usd"]    = closed_df["pnl_usd"].round(0)
        st.dataframe(closed_df.sort_values("pnl_usd",ascending=False), width="stretch", hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — SIGNALS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_signals:
    st.subheader("Recent Signals")
    try:
        raw_signals = db.get_signals_summary()
        if raw_signals:
            if isinstance(raw_signals, dict):
                sig_df = pd.DataFrame([raw_signals])
            elif isinstance(raw_signals, list):
                if len(raw_signals) == 0:
                    sig_df = pd.DataFrame()
                elif isinstance(raw_signals[0], dict):
                    sig_df = pd.DataFrame(raw_signals)
                else:
                    try: sig_df = pd.DataFrame([r._asdict() for r in raw_signals])
                    except: sig_df = pd.DataFrame([dict(r) for r in raw_signals])
            else:
                sig_df = pd.DataFrame()
            if not sig_df.empty:
                s1, s2, s3, s4 = st.columns(4)
                total_col     = next((c for c in ["total","count","signal_count"] if c in sig_df.columns), None)
                relevance_col = next((c for c in ["avg_relevance","relevance_score"] if c in sig_df.columns), None)
                s1.metric("Total Signals", int(sig_df[total_col].sum()) if total_col else len(sig_df))
                s2.metric("Avg Relevance", f"{sig_df[relevance_col].mean():.1f}" if relevance_col else "—")
                if "status" in sig_df.columns and total_col:
                    s3.metric("Enriched", int(sig_df[sig_df["status"]=="enriched"][total_col].sum()))
                if "source" in sig_df.columns:
                    s4.metric("Sources", sig_df["source"].nunique())
                st.dataframe(sig_df, width="stretch", hide_index=True)
    except Exception as e:
        st.warning(f"Could not load signal summary: {e}")

    if "source" in ideas.columns or "feed_topic" in ideas.columns:
        st.divider()
        st.subheader("Signal Sources → Ideas")
        source_col = "feed_topic" if "feed_topic" in ideas.columns else "source"
        source_counts = ideas[source_col].value_counts().reset_index()
        source_counts.columns = [source_col, "ideas"]
        fig_s = px.bar(source_counts, x=source_col, y="ideas", template="plotly_dark",
                       color="ideas", color_continuous_scale="teal")
        st.plotly_chart(fig_s, width="stretch")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — TA WATCHLIST
# ═══════════════════════════════════════════════════════════════════════════════
with tab_ta_watch:
    from datetime import timezone

    FINVIZ_URL = "https://finviz.com/quote.ashx?t={ticker}"

    TREND_META = {
        "VeryStrongUptrend":   ("b-vstrong-up", "🔥 V.Strong ↑"),
        "StrongUptrend":       ("b-strong-up",  "Strong ↑"),
        "WeakUptrend":         ("b-weak-up",    "Weak ↑"),
        "Ranging":             ("b-ranging",    "Ranging"),
        "WeakDowntrend":       ("b-weak-dn",    "Weak ↓"),
        "StrongDowntrend":     ("b-strong-dn",  "Strong ↓"),
        "VeryStrongDowntrend": ("b-vstrong-dn", "🔥 V.Strong ↓"),
    }

    BB_SIG_LABEL = {
        "SqueezeBreakoutLong":       "⚡ Squeeze → Long",
        "SqueezeBreakoutShort":      "⚡ Squeeze → Short",
        "MeanReversionLong":         "↩ Mean Rev Long",
        "MeanReversionShort":        "↩ Mean Rev Short",
        "MomentumContinuationLong":  "→ Momentum Long",
        "MomentumContinuationShort": "→ Momentum Short",
        "Neutral":                   "Neutral",
    }

    CONF_META = {
        "HighConfidence":     ("b-high",    "High"),
        "MediumConfidence":   ("b-medium",  "Medium"),
        "LowConfidence":      ("b-low",     "Low"),
        "ConflictingSignals": ("b-conflict","⚡ Conflict"),
    }

    SORT_OPTIONS = {
        "RSI (momentum)":       "rsi_value",
        "Overall score":        "score",
        "ADX (trend strength)": "adx_value",
        "MACD line":            "macd_value",
        "Volume ratio":         "volume_ratio",
        "Risk / Reward":        "risk_reward_ratio",
        "BB %B":                "bb_pct_b",
    }

    def _b(text, cls):
        return f'<span class="badge {cls}">{text}</span>'

    def _bias_badge(bias):
        b = (bias or "").capitalize()
        # Neutral is an explicit state alongside Long/Short
        return _b(b or "—", {"Long": "b-long", "Short": "b-short", "Neutral": "b-neutral"}.get(b, "b-neutral"))

    def _trend_badge(label):
        cls, text = TREND_META.get(label or "", ("b-ranging","—"))
        return _b(text, cls)

    def _conf_badge(conf):
        cls, text = CONF_META.get(conf or "", ("b-neutral", conf or "—"))
        return _b(text, cls)

    def _bb_badges(row):
        out = []
        if row.get("bb_squeeze"):      out.append(_b("⚡ Squeeze","b-squeeze"))
        if row.get("bb_walking_up"):   out.append(_b("↑ Walk Up","b-walkup"))
        if row.get("bb_walking_down"): out.append(_b("↓ Walk Dn","b-walkdn"))
        if row.get("bb_bullish_div"):  out.append(_b("↗ BullDiv","b-high"))
        if row.get("bb_bearish_div"):  out.append(_b("↘ BearDiv","b-low"))
        return " ".join(out) if out else '<span class="muted">—</span>'

    def _adx_icon(d):
        return {"Rising":"↗","Falling":"↘","Flat":"→"}.get(d or "","")

    def _rsi_col(v):
        if v is None: return "#8b8fa8"
        return "#ff4b6e" if v >= 70 else "#00d4aa" if v <= 30 else "#f1f5f9"

    def _score_col(v):
        if v is None: return "#8b8fa8"
        return "#00d4aa" if v >= 65 else "#f0c040" if v >= 40 else "#ff4b6e"

    def _card_cls(row):
        bias = (row.get("bias") or "").lower()
        if row.get("bb_squeeze"): return "ta-card ta-card-squeeze"
        if bias == "long":        return "ta-card ta-card-long"
        if bias == "short":       return "ta-card ta-card-short"
        return "ta-card"

    def _fmtp(v):
        try: return f"${float(v):,.2f}" if v is not None else "—"
        except: return "—"

    def _fmt(v, d=2):
        try: return f"{float(v):.{d}f}" if v is not None else "—"
        except: return "—"

    def _ts(ts):
        if ts is None: return "—"
        if hasattr(ts, "strftime"): return ts.strftime("%Y-%m-%d %H:%M")
        return str(ts)[:16]

    def _wl_get():
        try:
            rows = db._execute(
                "SELECT ticker, name, sector, added_at FROM ta_watchlist "
                "WHERE is_active = TRUE ORDER BY ticker", fetch=True
            )
            return [dict(r) for r in rows] if rows else []
        except Exception:
            return []

    def _wl_add(ticker, name="", sector=""):
        db._execute(
            """INSERT INTO ta_watchlist (ticker, name, sector) VALUES (%s,%s,%s)
               ON CONFLICT (ticker) DO UPDATE
                   SET is_active=TRUE, name=EXCLUDED.name, sector=EXCLUDED.sector""",
            (ticker.upper(), name, sector),
        )

    def _wl_remove(ticker):
        db._execute("UPDATE ta_watchlist SET is_active=FALSE WHERE ticker=%s",
                    (ticker.upper(),))

    ALLOWED_SORT = {"rsi_value","score","adx_value","macd_value",
                    "volume_ratio","risk_reward_ratio","bb_pct_b"}

    def _get_analyses(sort_col="rsi_value"):
        col = sort_col if sort_col in ALLOWED_SORT else "rsi_value"
        try:
            rows = db._execute("""
                SELECT DISTINCT ON (ta.ticker)
                    ta.ticker,
                    COALESCE(w.name,'')   AS name,
                    COALESCE(w.sector,'') AS sector,
                    ta.score, ta.overall_confidence,
                    ta.trend_alignment, ta.volume_confirmation,
                    ta.pattern_quality,  ta.risk_reward,
                    ta.bias, ta.entry_price, ta.stop_loss,
                    ta.invalidation_price, ta.risk_reward_ratio,
                    ta.rsi_value,  ta.rsi_signal,
                    ta.macd_value, ta.macd_signal_line,
                    ta.macd_histogram, ta.macd_crossover,
                    ta.ma20, ta.ma50, ta.ma60, ta.ma200, ta.ma250, ta.ma_crossover,
                    ta.current_volume, ta.avg_volume_20, ta.volume_ratio, ta.volume_signal,
                    ta.bb_upper, ta.bb_middle, ta.bb_lower,
                    ta.bb_pct_b, ta.bb_bandwidth, ta.bb_position,
                    ta.bb_squeeze, ta.bb_walking_up, ta.bb_walking_down,
                    ta.bb_bullish_div, ta.bb_bearish_div,
                    ta.bb_signal, ta.bb_interpretation,
                    ta.adx_value, ta.adx_plus_di, ta.adx_minus_di,
                    ta.adx_di_cross, ta.adx_trend, ta.adx_direction,
                    ta.summary, ta.full_result, ta.analysed_at
                FROM technical_analysis ta
                JOIN ta_watchlist w ON w.ticker = ta.ticker
                WHERE w.is_active = TRUE
                ORDER BY ta.ticker, ta.analysed_at DESC
            """, fetch=True)
            data = [dict(r) for r in rows] if rows else []
            asc = (col == "bb_bandwidth")
            data.sort(key=lambda x: float(x.get(col) or 0), reverse=not asc)
            return data
        except Exception as e:
            st.warning(f"Could not load TA analyses: {e}")
            return []

    def _get_history(ticker):
        try:
            rows = db._execute("""
                SELECT analysed_at, score, rsi_value, macd_value, bias,
                       overall_confidence, volume_ratio,
                       bb_pct_b, bb_bandwidth, bb_squeeze, bb_position,
                       adx_value, adx_trend, adx_direction
                FROM technical_analysis
                WHERE ticker = %s
                ORDER BY analysed_at DESC LIMIT 30
            """, fetch=True, params=(ticker.upper(),))
            return [dict(r) for r in rows] if rows else []
        except Exception:
            return []

    def _run_ta(ticker, direction="NEUTRAL"):
        from analyzer.technical_analysis import run_watchlist_analysis
        return run_watchlist_analysis(
            ticker    = ticker,
            direction = direction,
            run_id    = None,
        )

    def _squeeze_banner(analyses):
        sq = [r for r in analyses if r.get("bb_squeeze")]
        if not sq:
            return
        links = "  ".join(
            f'<a class="fv-link" href="{FINVIZ_URL.format(ticker=r["ticker"])}" target="_blank">'
            f'{r["ticker"]}</a>' for r in sq
        )
        st.markdown(
            f'<div style="background:#2b2200;border:1px solid #92400e;border-radius:8px;'
            f'padding:0.6rem 1rem;margin-bottom:1rem;">'
            f'⚡ <strong style="color:#f0c040;">Active Squeezes</strong> &nbsp;—&nbsp; {links}'
            f'</div>', unsafe_allow_html=True,
        )

    def _detail_panel(row):
        full = row.get("full_result") or {}
        if isinstance(full, str):
            try: full = json.loads(full)
            except: full = {}

        dt1, dt2, dt3, dt4, dt5, dt6 = st.tabs(
            ["📐 Setup", "📊 Bollinger", "📉 ADX / Trend", "🔎 Patterns", "📈 History", "{ } JSON"]
        )

        with dt1:
            ts = full.get("tradeSetup", {})
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Entry",        _fmtp(ts.get("entryPrice")))
            c2.metric("Stop Loss",    _fmtp(ts.get("stopLoss")))
            c3.metric("Invalidation", _fmtp(ts.get("invalidationPrice")))
            c4.metric("R/R",          f"{_fmt(ts.get('riskRewardRatio'))}×")
            if ts.get("entryCondition"):
                st.caption(f"📌 *{ts['entryCondition']}*")
            for t in ts.get("targets", []):
                st.markdown(f"- **{_fmtp(t.get('price'))}** — {t.get('label','')}")

        with dt2:
            bb = full.get("bollingerBands", {})
            if bb:
                c1,c2,c3,c4,c5 = st.columns(5)
                c1.metric("Upper",     _fmtp(bb.get("upper")))
                c2.metric("Middle",    _fmtp(bb.get("middle")))
                c3.metric("Lower",     _fmtp(bb.get("lower")))
                c4.metric("%B",        _fmt(bb.get("pct_b"),3))
                c5.metric("Bandwidth", _fmt(bb.get("bandwidth"),4))
                flags = []
                if bb.get("squeeze"):                  flags.append("⚡ **Squeeze active**")
                if bb.get("walkingBand") == "Up":      flags.append("↑ Walking upper band")
                if bb.get("walkingBand") == "Down":    flags.append("↓ Walking lower band")
                if bb.get("divergence") == "Bullish":  flags.append("↗ Bullish %B divergence")
                if bb.get("divergence") == "Bearish":  flags.append("↘ Bearish %B divergence")
                for f in flags: st.markdown(f)
                st.markdown(f"**Signal:** {BB_SIG_LABEL.get(bb.get('signal',''), bb.get('signal','') or '—')}")
                if bb.get("interpretation"): st.info(bb["interpretation"])
            else:
                st.caption("No Bollinger Band data.")

        with dt3:
            adx_val  = row.get("adx_value")
            plus_di  = row.get("adx_plus_di")
            minus_di = row.get("adx_minus_di")
            trend    = row.get("adx_trend") or "—"
            adx_dir  = row.get("adx_direction") or "—"
            di_cross = row.get("adx_di_cross") or "None"

            st.markdown("**ADX Trend Strength** *(deterministic — no LLM)*")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("ADX(14)",  _fmt(adx_val,1), f"{_adx_icon(adx_dir)} {adx_dir}")
            c2.metric("+DI",      _fmt(plus_di,1))
            c3.metric("−DI",      _fmt(minus_di,1))
            c4.metric("DI Cross", di_cross)

            col_map = {
                "VeryStrongUptrend":"#00d4aa","StrongUptrend":"#00d4aa","WeakUptrend":"#f0c040",
                "Ranging":"#8b8fa8",
                "WeakDowntrend":"#f97316","StrongDowntrend":"#ff4b6e","VeryStrongDowntrend":"#ff4b6e",
            }
            st.markdown(
                f'<div style="margin:0.8rem 0;font-size:1.1rem;font-weight:700;'
                f'color:{col_map.get(trend,"#8b8fa8")};">Trend: {trend}</div>',
                unsafe_allow_html=True,
            )

            try: adx_num = float(adx_val) if adx_val else 0
            except: adx_num = 0
            if adx_num < 20:
                st.caption("📊 ADX < 20: market is **ranging** — trend-following setups unreliable")
            elif adx_num < 25:
                st.caption("📊 ADX 20–25: **weak trend forming** — wait for confirmation before entering")
            elif adx_num < 40:
                st.caption("📊 ADX 25–40: **strong trend** — trend-following setups have highest probability")
            else:
                st.caption("📊 ADX 40+: **very strong trend** — watch for exhaustion / mean reversion risk")

            if di_cross != "None":
                icon = "🟢" if di_cross == "BullishCross" else "🔴"
                st.warning(f"{icon} DI Crossover: **{di_cross}** — potential trend change signal")

        with dt4:
            patterns = full.get("patternData", [])
            if not patterns:
                st.caption("No patterns identified.")
            else:
                from datetime import datetime as _dt
                rows_p = []
                for p in patterns:
                    ts_p = p.get("detectedAt")
                    rows_p.append({
                        "Pattern":    p.get("pattern","—"),
                        "Signal":     p.get("signalType","—"),
                        "Trend":      p.get("trendDirection","—"),
                        "Strength":   p.get("trendStrength","—"),
                        "S/R State":  p.get("SupportResistanceState","—"),
                        "Confidence": p.get("AnalysisConfidence","—"),
                        "Detected":   _dt.fromtimestamp(ts_p, tz=timezone.utc).strftime("%Y-%m-%d") if ts_p else "—",
                    })
                st.dataframe(pd.DataFrame(rows_p), width="stretch", hide_index=True)

        with dt5:
            history = _get_history(row["ticker"])
            if not history:
                st.caption("No historical data yet.")
            else:
                df_h = pd.DataFrame(history)
                df_h["analysed_at"] = pd.to_datetime(df_h["analysed_at"]).dt.strftime("%Y-%m-%d %H:%M")
                df_h = df_h.rename(columns={
                    "analysed_at":"Date","score":"Score","rsi_value":"RSI",
                    "macd_value":"MACD","bias":"Bias","overall_confidence":"Conf",
                    "bb_pct_b":"%B","bb_bandwidth":"BB Width",
                    "bb_squeeze":"Squeeze","bb_position":"BB Pos",
                    "adx_value":"ADX","adx_trend":"Trend","adx_direction":"ADX Dir",
                })
                cols = ["Date","Score","RSI","MACD","Bias","Conf",
                        "ADX","Trend","ADX Dir","%B","BB Width","Squeeze"]
                st.dataframe(df_h[[c for c in cols if c in df_h.columns]],
                             width="stretch", hide_index=True)

        with dt6:
            st.json(full, expanded=False)

    # ── Watchlist management ───────────────────────────────────────────────────
    with st.expander("➕ Manage Watchlist"):
        wl_c1, wl_c2 = st.columns(2)
        with wl_c1:
            st.markdown("**Add ticker**")
            with st.form("wl_add", clear_on_submit=True):
                wl_t = st.text_input("Ticker", placeholder="NVDA")
                wl_n = st.text_input("Name (optional)")
                wl_s = st.text_input("Sector (optional)")
                if st.form_submit_button("Add") and wl_t.strip():
                    try:
                        _wl_add(wl_t.strip(), wl_n.strip(), wl_s.strip())
                        st.success(f"{wl_t.upper()} added!")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
        with wl_c2:
            st.markdown("**Remove ticker**")
            _wl = _wl_get()
            if _wl:
                rm_t = st.selectbox("Select", [r["ticker"] for r in _wl],
                                    key="wl_rm_sel", label_visibility="collapsed")
                if st.button("Remove", key="wl_rm_btn"):
                    _wl_remove(rm_t)
                    st.rerun()
            else:
                st.caption("No tickers yet.")

    # ── Controls row ───────────────────────────────────────────────────────────
    ctrl1, ctrl2, ctrl3, ctrl4, _ = st.columns([2,1,1,1,2])
    sort_label = ctrl1.selectbox("Sort by", list(SORT_OPTIONS.keys()), index=1, label_visibility="collapsed")
    sort_dir   = ctrl2.radio("Order", ["↓ Desc","↑ Asc"], horizontal=True, label_visibility="collapsed")
    sort_col   = SORT_OPTIONS[sort_label]
    if ctrl3.button("↺ Refresh", key="ta_ctrl_refresh"):
        st.rerun()

    _wl = _wl_get()
    if _wl:
        with ctrl4.popover("⚙️ Run Analysis"):
            if st.button("🔄 Refresh ALL", width="stretch", type="primary", key="ta_run_all"):
                prog = st.progress(0, text="Starting…")
                for i, r in enumerate(_wl):
                    prog.progress(i / len(_wl), text=f"Analysing {r['ticker']}…")
                    _run_ta(r["ticker"])
                prog.progress(1.0, text="Done!")
                time.sleep(0.3)
                st.rerun()
            st.divider()
            run_t = st.selectbox("Ticker", [r["ticker"] for r in _wl], key="ta_run_sel")
            # NEUTRAL added alongside LONG / SHORT
            run_d = st.radio("Direction", ["LONG", "SHORT", "NEUTRAL"], horizontal=True, key="ta_run_dir")
            if st.button("Run", key="ta_run_one", width="stretch"):
                with st.spinner(f"Analysing {run_t}…"):
                    res = _run_ta(run_t, run_d)
                st.success("Done!") if res else st.error("Failed — check logs.")
                if res: st.rerun()
            # Show when this ticker was last analysed
            _all_analyses = _get_analyses()
            _last_ta = next((r for r in _all_analyses if r["ticker"] == run_t), None)
            if _last_ta and _last_ta.get("analysed_at"):
                st.caption(f"Last analysed: {_ts(_last_ta['analysed_at'])}")

    analyses = _get_analyses(sort_col)
    if sort_dir == "↑ Asc":
        analyses = list(reversed(analyses))

    if not analyses:
        st.info("No TA data yet. Add tickers via 'Manage Watchlist' above, then run an analysis.")
        st.stop()

    _squeeze_banner(analyses)

    # ── TA keyword search ─────────────────────────────────────────────────────
    _ta_kw = st.text_input(
        "🔍 Search ticker / summary",
        placeholder="e.g. AAPL  or  breakout  or  squeeze",
        key="ta_search",
    ).strip().lower()
    if _ta_kw:
        analyses = [
            r for r in analyses
            if _ta_kw in (r.get("ticker") or "").lower()
            or _ta_kw in (r.get("summary") or "").lower()
            or _ta_kw in (r.get("name") or "").lower()
            or _ta_kw in (r.get("sector") or "").lower()
        ]

    # ── KPI row ────────────────────────────────────────────────────────────────
    total     = len(analyses)
    longs     = sum(1 for r in analyses if (r.get("bias") or "").lower() == "long")
    shorts    = sum(1 for r in analyses if (r.get("bias") or "").lower() == "short")
    neutrals  = sum(1 for r in analyses if (r.get("bias") or "").lower() == "neutral")
    squeezes  = sum(1 for r in analyses if r.get("bb_squeeze"))
    strong_up = sum(1 for r in analyses if "StrongUptrend" in (r.get("adx_trend") or ""))
    strong_dn = sum(1 for r in analyses if "StrongDowntrend" in (r.get("adx_trend") or ""))
    avg_adx   = sum(float(r.get("adx_value") or 0) for r in analyses) / max(total,1)

    k1,k2,k3,k4,k5,k6,k7,k8 = st.columns(8)
    k1.metric("Tracked",       total)
    k2.metric("🟢 Long",       longs)
    k3.metric("🔴 Short",      shorts)
    k4.metric("⚪ Neutral",    neutrals)
    k5.metric("⚡ Squeezes",   squeezes)
    k6.metric("↑ Strong Up",   strong_up)
    k7.metric("↓ Strong Down", strong_dn)
    k8.metric("Avg ADX",       f"{avg_adx:.1f}")

    st.divider()

    # ── Ticker cards ───────────────────────────────────────────────────────────
    for row in analyses:
        ticker    = row["ticker"]
        bias      = row.get("bias") or "Neutral"
        conf      = row.get("overall_confidence") or ""
        rsi       = row.get("rsi_value")
        score     = row.get("score")
        adx_val   = row.get("adx_value")
        adx_trend = row.get("adx_trend") or ""
        adx_dir   = row.get("adx_direction") or ""
        ma50      = row.get("ma50") or row.get("ma60")
        ma200     = row.get("ma200") or row.get("ma250")
        bb_sig    = BB_SIG_LABEL.get(row.get("bb_signal",""), row.get("bb_signal","") or "")
        bb_interp = row.get("bb_interpretation") or ""
        _aat      = row.get("analysed_at")

        with st.container():
            st.markdown(f'<div class="{_card_cls(row)}">', unsafe_allow_html=True)

            h1,h2,h3,h4,h5,h6,h7,h8 = st.columns([2,1,2,1,1,2,1,1])

            h1.markdown(
                f"<span style='font-size:1.1rem;font-weight:700;color:#f1f5f9;'>{ticker}</span> "
                f'<a class="fv-link" href="{FINVIZ_URL.format(ticker=ticker)}" target="_blank">📊 Finviz →</a><br>'
                f"<span class='muted'>{row.get('name','')}"
                f"{'  ·  ' + row.get('sector','') if row.get('sector') else ''}</span>",
                unsafe_allow_html=True,
            )
            h2.markdown(f"<span class='lbl'>Bias</span><br>{_bias_badge(bias)}", unsafe_allow_html=True)
            h3.markdown(
                f"<span class='lbl'>Trend (ADX)</span><br>"
                f"{_trend_badge(adx_trend)} "
                f"<span class='muted'>{_adx_icon(adx_dir)} {_fmt(adx_val,1)}</span>",
                unsafe_allow_html=True,
            )
            h4.markdown(
                f"<span class='lbl'>RSI(14)</span><br>"
                f"<span style='font-size:1rem;font-weight:700;color:{_rsi_col(rsi)};'>"
                f"{'—' if rsi is None else f'{rsi:.1f}'}</span>",
                unsafe_allow_html=True,
            )
            h5.markdown(
                f"<span class='lbl'>Score</span><br>"
                f"<span style='font-size:1rem;font-weight:700;color:{_score_col(score)};'>"
                f"{'—' if score is None else f'{score:.0f}'}/100</span>",
                unsafe_allow_html=True,
            )
            h6.markdown(f"<span class='lbl'>BB Signals</span><br>{_bb_badges(row)}", unsafe_allow_html=True)
            h7.markdown(f"<span class='lbl'>Confidence</span><br>{_conf_badge(conf)}", unsafe_allow_html=True)
            # Analysed timestamp — full datetime in tooltip, short form displayed
            h8.markdown(
                f"<span class='lbl'>Analysed</span><br>"
                f"<span class='muted' title='{_aat or ''}'>{_ts(_aat)}</span>",
                unsafe_allow_html=True,
            )

            st.markdown('<div class="ta-div"></div>', unsafe_allow_html=True)
            s1,s2,s3,s4,s5,s6,s7,s8 = st.columns(8)
            s1.markdown(f"<span class='lbl'>+DI / -DI</span><br><span class='val'>{_fmt(row.get('adx_plus_di'),1)} / {_fmt(row.get('adx_minus_di'),1)}</span>", unsafe_allow_html=True)
            s2.markdown(f"<span class='lbl'>DI Cross</span><br><span class='val'>{row.get('adx_di_cross') or '—'}</span>", unsafe_allow_html=True)
            s3.markdown(f"<span class='lbl'>MACD Cross</span><br><span class='val'>{row.get('macd_crossover') or '—'}</span>", unsafe_allow_html=True)
            s4.markdown(f"<span class='lbl'>MA Cross</span><br><span class='val'>{row.get('ma_crossover') or '—'}</span>", unsafe_allow_html=True)
            s5.markdown(f"<span class='lbl'>SMA50</span><br><span class='val'>{_fmtp(ma50)}</span>", unsafe_allow_html=True)
            s6.markdown(f"<span class='lbl'>SMA200</span><br><span class='val'>{_fmtp(ma200)}</span>", unsafe_allow_html=True)
            s7.markdown(f"<span class='lbl'>%B</span><br><span class='val'>{_fmt(row.get('bb_pct_b'),3)}</span>", unsafe_allow_html=True)
            s8.markdown(f"<span class='lbl'>Vol Ratio</span><br><span class='val'>{_fmt(row.get('volume_ratio'))}×</span>", unsafe_allow_html=True)

            if bb_sig or bb_interp:
                st.markdown(
                    f'<div style="margin-top:0.4rem;font-size:0.8rem;">'
                    f'{"<strong>" + bb_sig + "</strong> &nbsp;—&nbsp; " if bb_sig else ""}'
                    f'<span class="muted">{bb_interp}</span></div>',
                    unsafe_allow_html=True,
                )

            if row.get("summary"):
                st.markdown(
                    f'<div style="font-size:0.82rem;color:#8b8fa8;margin-top:0.25rem;">'
                    f'{row["summary"]}</div>', unsafe_allow_html=True,
                )

            with st.expander("🔍 Full analysis"):
                _detail_panel(row)

            st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — TOKEN USAGE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_tokens:
    st.subheader("LLM Usage Tracking")
    try:
        daily_usage   = db.query("SELECT * FROM v_daily_token_usage ORDER BY day DESC LIMIT 30")
        monthly_usage = db.query("SELECT * FROM v_monthly_usage ORDER BY month DESC LIMIT 3")
        if daily_usage:
            daily_df = pd.DataFrame(daily_usage)
            if monthly_usage:
                month_df  = pd.DataFrame(monthly_usage)
                cur_month = month_df.iloc[0]
                m1,m2,m3,m4 = st.columns(4)
                m1.metric("Requests this month", int(cur_month.get("total_requests",0)))
                pct = cur_month.get("pct_of_monthly_request_limit",0)
                m2.metric("% of free quota used", f"{pct:.1f}%")
                m3.metric("Total tokens",  int(cur_month.get("total_tokens",0)))
                m4.metric("Est. cost USD", f"${float(cur_month.get('total_cost_usd',0)):.4f}")
                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number", value=float(pct),
                    title={"text":"Monthly Request Quota Used"},
                    gauge={"axis":{"range":[0,100]},"bar":{"color":"#00d4aa"},
                           "steps":[{"range":[0,60],"color":"#1a2a1a"},{"range":[60,85],"color":"#2a2a1a"},{"range":[85,100],"color":"#2a1a1a"}],
                           "threshold":{"line":{"color":"#ff4b6e","width":3},"thickness":0.75,"value":90}},
                    number={"suffix":"%"},
                ))
                fig_gauge.update_layout(template="plotly_dark", height=250, margin=dict(t=40,b=10,l=20,r=20))
                st.plotly_chart(fig_gauge, width="stretch")
            st.divider()
            col_t1, col_t2 = st.columns(2)
            with col_t1:
                st.subheader("Daily Requests")
                if "requests_stage1" in daily_df.columns:
                    fig_req = px.bar(daily_df.sort_values("day"), x="day",
                                     y=["requests_stage1","requests_stage2"], template="plotly_dark",
                                     color_discrete_map={"requests_stage1":"#00d4aa","requests_stage2":"#4b8bff"},
                                     barmode="stack")
                    fig_req.add_hline(y=250, line_dash="dash", line_color="#ff4b6e", annotation_text="250 RPD limit")
                    st.plotly_chart(fig_req, width="stretch")
            with col_t2:
                st.subheader("Tokens per Idea")
                if "tokens_per_idea" in daily_df.columns:
                    fig_tpi = px.line(daily_df.sort_values("day"), x="day", y="tokens_per_idea",
                                      template="plotly_dark", markers=True, color_discrete_sequence=["#f0c040"])
                    st.plotly_chart(fig_tpi, width="stretch")
            st.divider()
            st.dataframe(daily_df[[c for c in ["day","runs","total_requests","tokens_stage1",
                "tokens_stage2","total_tokens","ideas_generated","tokens_per_idea","total_cost_usd"]
                if c in daily_df.columns]], width="stretch", hide_index=True)
        else:
            st.info("No token data yet.")
    except Exception as e:
        st.warning(f"Token usage data unavailable: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — PIPELINE HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
with tab_health:
    st.subheader("Pipeline Health (last 7 days)")
    try:
        raw_health = db.get_pipeline_health(days=7)
        health = pd.DataFrame(raw_health) if raw_health else pd.DataFrame()
        if not health.empty:
            h1,h2,h3,h4 = st.columns(4)
            if "total_runs"     in health.columns: h1.metric("Total Runs",  int(health["total_runs"].sum()))
            if "total_ideas"    in health.columns: h2.metric("Total Ideas", int(health["total_ideas"].sum()))
            if "failures"       in health.columns: h3.metric("Failed Runs", int(health["failures"].sum()))
            if "avg_duration_s" in health.columns: h4.metric("Avg Duration",f"{health['avg_duration_s'].mean():.0f}s")
            st.divider()
            if "failures" in health.columns and "total_runs" in health.columns:
                hp = health.copy()
                hp["successes"] = hp["total_runs"] - hp.get("failures",0)
                fig_h = px.bar(hp, x="day" if "day" in hp.columns else hp.index,
                               y=["successes","failures"], template="plotly_dark",
                               color_discrete_map={"successes":"#00d4aa","failures":"#ff4b6e"},
                               barmode="stack", title="Pipeline Runs — Success vs Failure")
                st.plotly_chart(fig_h, width="stretch")
            st.dataframe(health, width="stretch", hide_index=True)
        else:
            st.info("No pipeline runs recorded yet.")
    except Exception as e:
        st.warning(f"Could not load pipeline health: {e}")

    st.divider()
    st.subheader("Stuck Signals")
    try:
        stuck = db.query("SELECT * FROM v_pending_signals LIMIT 20")
        if stuck:
            st.warning(f"{len(stuck)} signals appear stuck in the pipeline.")
            st.dataframe(pd.DataFrame(stuck), width="stretch", hide_index=True)
        else:
            st.success("No stuck signals — pipeline is healthy.")
    except Exception:
        st.caption("Run the schema.sql to enable stuck signal detection.")
"""
Prism - Streamlit Command Center
===================================
The visual brain of Prism. 4 tabs:
  1. 📊 Live Dashboard     — Pass/Hold/Block stats at a glance
  2. ⚠️  Review Queue      — HITL: pending human decisions
  3. 📋 Audit Ledger       — Full history with human overrides
  4. 📝 Contracts          — Manage plain-English data contracts
"""

import sys
sys.path.insert(0, ".")

import streamlit as st
import pandas as pd
from datetime import datetime

from core.ledger import get_dashboard_stats, get_recent_decisions, get_contracts
from core.hitl import approve_decision, reject_decision, get_review_queue, get_accountability_report

# ─── Page Config ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Prism — Data Clearinghouse",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Styles ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main { background: #0d1117; color: #e6edf3; }
    .block-container { padding: 1.5rem 2rem; }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 1rem 1.5rem;
    }

    /* PASS / HOLD / BLOCK badge styles */
    .badge-pass  { background:#1a4731; color:#3fb950; padding:3px 10px; border-radius:12px; font-weight:600; font-size:0.8rem; }
    .badge-hold  { background:#3d2f00; color:#d29922; padding:3px 10px; border-radius:12px; font-weight:600; font-size:0.8rem; }
    .badge-block { background:#3d1a1a; color:#f85149; padding:3px 10px; border-radius:12px; font-weight:600; font-size:0.8rem; }
    .badge-approved { background:#1a4731; color:#3fb950; padding:3px 10px; border-radius:12px; font-weight:600; font-size:0.8rem; }
    .badge-rejected { background:#3d1a1a; color:#f85149; padding:3px 10px; border-radius:12px; font-weight:600; font-size:0.8rem; }
    .badge-pending  { background:#2d2d2d; color:#8b949e; padding:3px 10px; border-radius:12px; font-weight:600; font-size:0.8rem; }

    /* Section headers */
    h1, h2, h3 { color: #e6edf3 !important; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { background: #161b22; border-radius: 8px; padding: 4px; }
    .stTabs [data-baseweb="tab"] { color: #8b949e; border-radius: 6px; }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { background: #21262d; color: #e6edf3; }

    /* Buttons */
    div.stButton > button {
        background: #21262d;
        color: #e6edf3;
        border: 1px solid #30363d;
        border-radius: 8px;
        font-weight: 500;
    }
    div.stButton > button:hover { background: #30363d; border-color: #58a6ff; color: #58a6ff; }

    /* Tables */
    .stDataFrame { background: #161b22; }
    [data-testid="stDataFrame"] div { background: #161b22 !important; }

    /* Input fields */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        background: #161b22;
        border: 1px solid #30363d;
        color: #e6edf3;
        border-radius: 8px;
    }

    /* Info boxes */
    .info-box {
        background: #161b22;
        border: 1px solid #30363d;
        border-left: 3px solid #58a6ff;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 8px 0;
    }
    .violation-box {
        background: #1c0f0f;
        border: 1px solid #f85149;
        border-left: 3px solid #f85149;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 8px 0;
    }
    .fix-box {
        background: #0f1c10;
        border: 1px solid #3fb950;
        border-left: 3px solid #3fb950;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 8px 0;
    }
</style>
""", unsafe_allow_html=True)


# ─── Header ────────────────────────────────────────────────────────────
st.markdown("""
<div style="display:flex; align-items:center; gap:12px; margin-bottom:8px;">
    <span style="font-size:2rem;">🔷</span>
    <div>
        <h1 style="margin:0; font-size:1.8rem; font-weight:700;">PRISM</h1>
        <p style="margin:0; color:#8b949e; font-size:0.85rem;">Semantic Data Clearinghouse — Command Center</p>
    </div>
    <div style="margin-left:auto; color:#8b949e; font-size:0.8rem;">
        Last refreshed: {ts}
    </div>
</div>
""".format(ts=datetime.now().strftime("%H:%M:%S")), unsafe_allow_html=True)

st.divider()

# ─── Stats Bar ─────────────────────────────────────────────────────────
stats = get_dashboard_stats()

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Events", stats["total_events"])
c2.metric("✅ Passed", stats["total_pass"],
          delta=f"{round(stats['total_pass']/max(stats['total_events'],1)*100)}%")
c3.metric("⚠️ Held", stats["total_hold"])
c4.metric("❌ Blocked", stats["total_block"])
c5.metric("🕐 Pending Review", stats["pending_review"],
          delta="Needs action" if stats["pending_review"] > 0 else "Clear",
          delta_color="inverse")
c6.metric("🧑 Human Overrides", stats["human_overrides"])

st.divider()

# ─── Tabs ──────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Live Dashboard",
    f"⚠️ Review Queue ({stats['pending_review']})",
    "📋 Audit Ledger",
    "🧑 Accountability",
    "📝 Contracts",
])


# ══════════════════════════════════════════════════
#  TAB 1: LIVE DASHBOARD
# ══════════════════════════════════════════════════
with tab1:
    st.subheader("Recent Decisions")

    decisions = get_recent_decisions(limit=20)
    if not decisions:
        st.info("No decisions yet. Run `python demo/pipeline.py` to generate data.")
    else:
        for d in decisions:
            ai_dec = d["ai_decision"]
            color_map = {"PASS": "badge-pass", "HOLD": "badge-hold", "BLOCK": "badge-block"}
            badge = f'<span class="{color_map.get(ai_dec, "")}">{"✅" if ai_dec=="PASS" else "⚠️" if ai_dec=="HOLD" else "❌"} {ai_dec}</span>'

            override_html = ""
            if d.get("human_decision"):
                hd = d["human_decision"]
                h_badge = f'<span class="badge-{"approved" if hd=="APPROVED" else "rejected"}">' \
                          f'{"✅" if hd=="APPROVED" else "❌"} Human: {hd}</span>'
                override_html = f' &nbsp;→&nbsp; {h_badge} by <b>{d.get("human_name","?")}</b>'

            ts = str(d["timestamp"])[:19] if d["timestamp"] else "—"
            conf_pct = round((d.get("ai_confidence") or 0) * 100)
            drift = d.get("fingerprint_delta") or 0

            with st.expander(
                f'{ts} | {d["pipeline_name"]} → {d["data_asset"]} | {ai_dec}',
                expanded=False
            ):
                st.markdown(
                    f'<div class="info-box">'
                    f'{badge}{override_html}<br/>'
                    f'<small>'
                    f'Confidence: <b>{conf_pct}%</b> &nbsp;|&nbsp; '
                    f'Drift Score: <b>{drift:.3f}</b> &nbsp;|&nbsp; '
                    f'Rows: <b>{d.get("rows_affected", "?")} </b>&nbsp;|&nbsp; '
                    f'Snapshot Served: <b>{"Yes" if d.get("snapshot_used") else "No"}</b>'
                    f'</small></div>',
                    unsafe_allow_html=True,
                )

                if d.get("ai_reason"):
                    st.markdown(f'**AI Reason:** {d["ai_reason"]}')

                if d.get("ai_fix_suggestion"):
                    st.markdown(
                        f'<div class="fix-box">🔧 <b>Suggested Fix:</b><br/>{d["ai_fix_suggestion"]}</div>',
                        unsafe_allow_html=True,
                    )

                if d.get("human_note"):
                    st.markdown(f'**Human Note:** _{d["human_note"]}_')

                st.caption(f'Event ID: `{d["event_id"]}`')


# ══════════════════════════════════════════════════
#  TAB 2: REVIEW QUEUE (HITL)
# ══════════════════════════════════════════════════
with tab2:
    st.subheader("⚠️ Pending Human Review")
    st.markdown(
        '<div class="info-box">These events were flagged by AI (HOLD or BLOCK) and '
        'are waiting for a data steward to approve or reject them. '
        'Every decision here is permanently logged.</div>',
        unsafe_allow_html=True
    )

    queue = get_review_queue()
    if not queue:
        st.success("🎉 Review queue is clear! All flagged events have been reviewed.")
    else:
        for item in queue:
            ai_dec = item["ai_decision"]
            color = "#d29922" if ai_dec == "HOLD" else "#f85149"
            icon = "⚠️" if ai_dec == "HOLD" else "❌"

            with st.expander(
                f'{icon} {ai_dec} — {item["data_asset"]} | Pipeline: {item["pipeline_name"]}',
                expanded=True
            ):
                st.markdown(f'**AI Decision:** `{ai_dec}` (confidence: {round((item.get("ai_confidence") or 0)*100)}%)')
                st.markdown(f'**AI Reason:** {item.get("ai_reason", "—")}')

                if item.get("ai_fix_suggestion"):
                    st.markdown(
                        f'<div class="fix-box">🔧 <b>Suggested Fix:</b><br/>{item["ai_fix_suggestion"]}</div>',
                        unsafe_allow_html=True
                    )

                st.markdown("---")
                st.markdown("**Your Decision:**")

                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("###### ✅ Approve (Override AI)")
                    with st.form(key=f"approve_{item['event_id']}"):
                        name_a = st.text_input("Your Name", key=f"name_a_{item['event_id']}", placeholder="e.g. Sumit Singh")
                        email_a = st.text_input("Your Email", key=f"email_a_{item['event_id']}", placeholder="you@company.com")
                        note_a = st.text_area("Why are you approving this?", key=f"note_a_{item['event_id']}", height=80)
                        if st.form_submit_button("✅ Approve & Let Data Through", use_container_width=True):
                            if name_a and email_a:
                                result = approve_decision(item["event_id"], name_a, email_a, note_a)
                                st.success(result["message"])
                                st.rerun()
                            else:
                                st.error("Please enter your name and email.")

                with col_b:
                    st.markdown("###### ❌ Reject (Confirm AI was Right)")
                    with st.form(key=f"reject_{item['event_id']}"):
                        name_r = st.text_input("Your Name", key=f"name_r_{item['event_id']}", placeholder="e.g. Sumit Singh")
                        email_r = st.text_input("Your Email", key=f"email_r_{item['event_id']}", placeholder="you@company.com")
                        note_r = st.text_area("Why are you rejecting?", key=f"note_r_{item['event_id']}", height=80)
                        if st.form_submit_button("❌ Reject & Keep Data Blocked", use_container_width=True):
                            if name_r and email_r:
                                result = reject_decision(item["event_id"], name_r, email_r, note_r)
                                st.success(result["message"])
                                st.rerun()
                            else:
                                st.error("Please enter your name and email.")

                st.caption(f'Event ID: `{item["event_id"]}`')


# ══════════════════════════════════════════════════
#  TAB 3: FULL AUDIT LEDGER
# ══════════════════════════════════════════════════
with tab3:
    st.subheader("📋 Full Audit Ledger")
    st.caption("Complete, immutable record of every AI decision — with human overrides merged.")

    all_decisions = get_recent_decisions(limit=100)
    if not all_decisions:
        st.info("No events yet. Run the demo pipeline first.")
    else:
        rows = []
        for d in all_decisions:
            ai_dec = d["ai_decision"]
            h_dec = d.get("human_decision") or "—"
            rows.append({
                "Time": str(d["timestamp"])[:19] if d["timestamp"] else "—",
                "Pipeline": d["pipeline_name"],
                "Asset": d["data_asset"],
                "AI Decision": ai_dec,
                "Confidence": f"{round((d.get('ai_confidence') or 0)*100)}%",
                "Drift": f"{(d.get('fingerprint_delta') or 0):.3f}",
                "Human Decision": h_dec,
                "Reviewed By": d.get("human_name") or "—",
                "Event ID": d["event_id"][:8] + "...",
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════
#  TAB 4: ACCOUNTABILITY REPORT
# ══════════════════════════════════════════════════
with tab4:
    st.subheader("🧑 Accountability Report")
    st.markdown(
        '<div class="info-box">Every human override, who made it, when, and why. '
        'This is your compliance and audit trail.</div>',
        unsafe_allow_html=True,
    )

    report = get_accountability_report()
    if not report:
        st.info("No human overrides yet. Once reviewers act on the queue, their decisions appear here.")
    else:
        rows = []
        for r in report:
            rows.append({
                "Time": str(r["timestamp"])[:19] if r["timestamp"] else "—",
                "Person": f'{r["human_name"]} ({r["human_email"]})',
                "Action": r["human_decision"],
                "Asset": r["data_asset"],
                "Original AI Call": r["original_ai_decision"],
                "Note": (r.get("human_note") or "—")[:80],
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════
#  TAB 5: CONTRACT MANAGEMENT
# ══════════════════════════════════════════════════
with tab5:
    st.subheader("📝 Data Contracts")
    st.markdown(
        '<div class="info-box">Define what your data <em>means</em> in plain English. '
        'Prism compiles this into executable rules.</div>',
        unsafe_allow_html=True,
    )

    st.markdown("#### Add / Update a Contract")

    with st.form("new_contract"):
        asset = st.text_input("Data Asset Name", placeholder="e.g. fct_monthly_revenue")
        author = st.text_input("Your Name / Email", placeholder="e.g. sumit@company.com")
        contract_text = st.text_area(
            "Plain-English Contract",
            height=120,
            placeholder=(
                "Revenue is always in USD. Values must be non-negative. "
                "Represents monthly recurring revenue from active paid subscribers only. "
                "Should not grow by more than 30% week-over-week."
            )
        )
        submitted = st.form_submit_button("💾 Save & Compile Contract", use_container_width=True)
        if submitted:
            if asset and contract_text and author:
                from core.contracts import create_contract
                with st.spinner("Compiling contract with AI..."):
                    result = create_contract(asset, contract_text, author)
                st.success(f"✅ Contract registered for `{asset}`")
                with st.expander("View compiled rules"):
                    import json
                    st.json(result["compiled"])
                st.rerun()
            else:
                st.error("Please fill all fields.")

    st.markdown("#### Existing Contracts")
    contracts = get_contracts()
    if not contracts:
        st.info("No contracts yet. Add one above.")
    else:
        for c in contracts:
            with st.expander(f"📋 {c['data_asset']} — by {c['created_by']}"):
                st.markdown(f"**Contract:** {c['plain_english']}")
                st.caption(f"Last updated: {str(c['updated_at'])[:19]}")
                import json
                try:
                    rules = json.loads(c.get("compiled_rules") or "{}")
                    if rules:
                        st.json(rules)
                except Exception:
                    pass

# ─── Footer ────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    '<center><small style="color:#484f58;">🔷 PRISM — Semantic Data Clearinghouse &nbsp;|&nbsp; '
    'Built to be acquired &nbsp;|&nbsp; github.com/prism-data</small></center>',
    unsafe_allow_html=True,
)

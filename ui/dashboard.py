"""
Prism - Streamlit Command Center
===================================
The visual brain of Prism. 4 tabs:
  1. Dashboard         — High-level telemetry
  2. Review Queue      — Pending HITL decisions
  3. Audit Ledger      — Immutable decision log
  4. Accountability    — Human override tracking
  5. Contracts         — Data contract definitions
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
    page_title="Prism Command Center",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ─── AUTHENTICATION ───────────────────────────────────────────────────
# [TESTING PHASE] Authentication bypassed for local development/demos
if "authenticated" not in st.session_state:
    st.session_state.authenticated = True
    st.session_state.user_name = "Admin (Test Mode)"
    st.session_state.user_email = "admin@prism.local"

# if not st.session_state.authenticated:
#     st.markdown("<style>body, .main { background: var(--background-color); color: var(--text-color); font-family: 'Inter', sans-serif; }</style>", unsafe_allow_html=True)
#     st.markdown("<center><h1 style='margin-top:100px; font-weight: 500;'>PRISM</h1><p style='color: var(--text-color); opacity: 0.7;'>Identity & Access Management</p></center>", unsafe_allow_html=True)
#     
#     col1, col2, col3 = st.columns([1,1,1])
#     with col2:
#         with st.form("login"):
#             st.markdown("<span style='color: var(--text-color); opacity: 0.7; font-size: 0.9em;'>Secure access required to view ledger.</span>", unsafe_allow_html=True)
#             name = st.text_input("Full Name")
#             email = st.text_input("Corporate Email")
#             secret = st.text_input("IAM Secret", type="password")
#             if st.form_submit_button("Authenticate", use_container_width=True):
#                 if name and email and secret == "admin":
#                     st.session_state.authenticated = True
#                     st.session_state.user_name = name
#                     st.session_state.user_email = email
#                     st.rerun()
#                 else:
#                     st.error("Invalid credentials or unauthorized identity.")
#     st.stop()

# ─── Styles ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    /* Core Backgrounds */
    .main { background: var(--background-color); color: var(--text-color); }
    .block-container { padding: 2rem; max-width: 1200px; margin: 0 auto; }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 6px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
    }
    
    [data-testid="stMetricValue"] {
        font-family: 'SF Mono', Consolas, monospace;
        font-weight: 500;
        color: var(--text-color);
    }

    /* Status Badges */
    .badge-pass { background: rgba(63, 185, 80, 0.15); color: #3fb950; border: 1px solid rgba(63, 185, 80, 0.3); padding: 2px 8px; border-radius: 4px; font-weight: 500; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; }
    .badge-hold { background: rgba(210, 153, 34, 0.15); color: #d29922; border: 1px solid rgba(210, 153, 34, 0.3); padding: 2px 8px; border-radius: 4px; font-weight: 500; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; }
    .badge-block { background: rgba(248, 81, 73, 0.15); color: #f85149; border: 1px solid rgba(248, 81, 73, 0.3); padding: 2px 8px; border-radius: 4px; font-weight: 500; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; }

    /* Section headers */
    h1, h2, h3 { color: var(--text-color) !important; font-weight: 500 !important; letter-spacing: -0.5px; }
    h4, h5, h6 { color: var(--text-color) !important; font-weight: 500 !important; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { background: transparent; border-bottom: 1px solid rgba(128, 128, 128, 0.2); gap: 2rem; padding-bottom: 0px; }
    .stTabs [data-baseweb="tab"] { color: var(--text-color); opacity: 0.7; border-radius: 0; padding: 10px 0; border-bottom: 2px solid transparent; }
    .stTabs [data-baseweb="tab"]:hover { color: var(--text-color); }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { background: transparent; color: var(--text-color); border-bottom-color: var(--primary-color); font-weight: 500; }

    /* Buttons */
    div.stButton > button {
        background: var(--secondary-background-color);
        color: var(--text-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 6px;
        font-weight: 500;
        font-size: 0.85rem;
        transition: all 0.2s ease;
    }
    div.stButton > button:hover { background: #30363d; border-color: var(--text-color); opacity: 0.7; color: var(--text-color); }
    
    /* Primary buttons */
    .st-emotion-cache-1n76uvr p { font-weight: 500; }

    /* Tables */
    .stDataFrame { border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 6px; overflow: hidden; }
    [data-testid="stDataFrame"] div { background: #0d1117 !important; }

    /* Data expanders */
    [data-testid="stExpander"] {
        background: var(--background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 6px;
    }
    [data-testid="stExpander"] summary {
        background: var(--secondary-background-color);
        padding: 0.75rem 1rem;
        color: var(--text-color);
        font-family: 'SF Mono', Consolas, monospace;
        font-size: 0.85rem;
    }

    /* Input fields */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        background: var(--background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        color: var(--text-color);
        border-radius: 6px;
        font-size: 0.9rem;
    }
    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: var(--primary-color);
        box-shadow: inset 0 0 0 1px var(--primary-color);
    }

    /* Enterprise Info boxes */
    .info-box {
        background: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-left: 3px solid #58a6ff;
        border-radius: 4px;
        padding: 12px 16px;
        margin: 12px 0;
        font-size: 0.85rem;
        color: var(--text-color); opacity: 0.7;
    }
    .violation-box {
        background: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-left: 3px solid #f85149;
        border-radius: 4px;
        padding: 12px 16px;
        margin: 12px 0;
        font-size: 0.85rem;
        color: var(--text-color);
    }
    .fix-box {
        background: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-left: 3px solid #3fb950;
        border-radius: 4px;
        padding: 12px 16px;
        margin: 12px 0;
        font-size: 0.85rem;
        color: var(--text-color);
        font-family: 'SF Mono', Consolas, monospace;
    }
    
    hr { border-color: rgba(128, 128, 128, 0.2); margin: 2rem 0; }
</style>
""", unsafe_allow_html=True)


# ─── Header ────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="display:flex; align-items:center; gap:16px; margin-bottom:12px;">
    <div>
        <h1 style="margin:0; font-size:1.6rem; font-weight:600; letter-spacing: 1px;">PRISM</h1>
        <p style="margin:0; color:var(--text-color); opacity:0.7; font-size:0.8rem; text-transform: uppercase; letter-spacing: 0.5px;">Semantic Data Clearinghouse</p>
    </div>
    <div style="margin-left:auto; text-align:right; color:var(--text-color); opacity:0.7; font-size:0.75rem; font-family: 'SF Mono', Consolas, monospace;">
        Identity: <span style="color:var(--text-color);">{st.session_state.user_name}</span><br/>
        System Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}
    </div>
</div>
""", unsafe_allow_html=True)

st.divider()

# ─── Stats Bar ─────────────────────────────────────────────────────────
stats = get_dashboard_stats()

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Ingestion Events", stats["total_events"])
c2.metric("Passed", stats["total_pass"],
          delta=f"{round(stats['total_pass']/max(stats['total_events'],1)*100)}%")
c3.metric("Held", stats["total_hold"])
c4.metric("Blocked", stats["total_block"])
c5.metric("Pending Review", stats["pending_review"],
          delta="Requires Action" if stats["pending_review"] > 0 else "Clear",
          delta_color="inverse")
c6.metric("Human Overrides", stats["human_overrides"])

st.divider()

# ─── Tabs ──────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "LIVE DASHBOARD",
    f"REVIEW QUEUE ({stats['pending_review']})",
    "AUDIT LEDGER",
    "ACCOUNTABILITY",
    "CONTRACTS",
])


# ══════════════════════════════════════════════════
#  TAB 1: LIVE DASHBOARD
# ══════════════════════════════════════════════════
with tab1:
    st.markdown("<h3 style='font-size: 1.1rem; margin-top: 1rem;'>Recent Executions</h3>", unsafe_allow_html=True)

    decisions = get_recent_decisions(limit=20)
    if not decisions:
        st.info("No data pipelines have executed through Prism yet.")
    else:
        for d in decisions:
            ai_dec = d["ai_decision"]
            color_map = {"PASS": "badge-pass", "HOLD": "badge-hold", "BLOCK": "badge-block"}
            badge = f'<span class="{color_map.get(ai_dec, "")}">{ai_dec}</span>'

            override_html = ""
            if d.get("human_decision"):
                hd = d["human_decision"]
                h_class = "badge-pass" if hd == "APPROVED" else "badge-block"
                override_html = f' &nbsp;→&nbsp; <span class="{h_class}">OVERRIDDEN: {hd}</span> <span style="color:var(--text-color); opacity:0.7; font-size:0.75rem;">by {d.get("human_name","?")}</span>'

            ts = str(d["timestamp"])[:19] if d["timestamp"] else "—"
            conf_pct = round((d.get("ai_confidence") or 0) * 100)
            drift = d.get("fingerprint_delta") or 0

            with st.expander(
                f'{ts}  │  {d["pipeline_name"]}  →  {d["data_asset"]}  │  {ai_dec}',
                expanded=False
            ):
                st.markdown(
                    f'<div style="margin-bottom: 1rem;">'
                    f'{badge}{override_html}<br/>'
                    f'<div style="color:var(--text-color); opacity:0.7; font-family:\'SF Mono\', Consolas, monospace; font-size:0.75rem; margin-top:8px;">'
                    f'Confidence Model: {conf_pct}% &nbsp;│&nbsp; '
                    f'Drift Coefficient: {drift:.3f} &nbsp;│&nbsp; '
                    f'Rows Processed: {d.get("rows_affected", "?")} &nbsp;│&nbsp; '
                    f'Snapshot Served: {"Yes" if d.get("snapshot_used") else "No"}'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )

                if d.get("ai_reason"):
                    st.markdown(f'<div style="font-size: 0.9rem; color: var(--text-color);"><b>Diagnostic Reason:</b> {d["ai_reason"]}</div>', unsafe_allow_html=True)

                if d.get("ai_fix_suggestion"):
                    st.markdown(
                        f'<div class="fix-box"><b>Suggested Mitigation:</b><br/>{d["ai_fix_suggestion"]}</div>',
                        unsafe_allow_html=True,
                    )

                if d.get("human_note"):
                    st.markdown(f'<div style="font-size: 0.85rem; color: var(--text-color); opacity: 0.7; margin-top: 8px;"><b>Auditor Note:</b> <i>{d["human_note"]}</i></div>', unsafe_allow_html=True)

                st.markdown(f'<div style="font-family:\'SF Mono\', Consolas, monospace; font-size:0.7rem; color:var(--text-color); opacity:0.5; margin-top: 1rem;">Trace ID: {d["event_id"]}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════
#  TAB 2: REVIEW QUEUE (HITL)
# ══════════════════════════════════════════════════
with tab2:
    st.markdown("<h3 style='font-size: 1.1rem; margin-top: 1rem;'>Pending Human Review</h3>", unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">Events flagged with HOLD or BLOCK status require manual steward arbitration. '
        'All interventions are permanently recorded in the immutable ledger.</div>',
        unsafe_allow_html=True
    )

    queue = get_review_queue()
    if not queue:
        st.success("Review queue is currently empty. All flagged events have been resolved.")
    else:
        for item in queue:
            ai_dec = item["ai_decision"]
            badge_class = "badge-hold" if ai_dec == "HOLD" else "badge-block"

            with st.expander(
                f'{ai_dec}  │  {item["data_asset"]}  │  Pipeline: {item["pipeline_name"]}',
                expanded=True
            ):
                st.markdown(f'<span class="{badge_class}">{ai_dec}</span> <span style="font-family:\'SF Mono\', Consolas, monospace; font-size:0.8rem; color:var(--text-color); opacity:0.7; margin-left:8px;">Model Confidence: {round((item.get("ai_confidence") or 0)*100)}%</span>', unsafe_allow_html=True)
                st.markdown(f'<div style="margin-top: 12px; font-size: 0.9rem; color: var(--text-color);"><b>Reason:</b> {item.get("ai_reason", "—")}</div>', unsafe_allow_html=True)

                if item.get("ai_fix_suggestion"):
                    st.markdown(
                        f'<div class="fix-box"><b>Suggested Mitigation:</b><br/>{item["ai_fix_suggestion"]}</div>',
                        unsafe_allow_html=True
                    )

                st.markdown("<hr style='margin: 1rem 0; border-color: rgba(128, 128, 128, 0.2);' />", unsafe_allow_html=True)
                st.markdown("<div style='font-size: 0.85rem; color: var(--text-color); opacity: 0.7; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.5px;'>Arbitration Required</div>", unsafe_allow_html=True)

                col_a, col_b = st.columns(2)
                with col_a:
                    with st.form(key=f"approve_{item['event_id']}"):
                        st.markdown(f"<div style='font-family:\'SF Mono\', Consolas, monospace; font-size:0.75rem; color:var(--text-color); margin-bottom: 8px;'>Authorized By: {st.session_state.user_name} ({st.session_state.user_email})</div>", unsafe_allow_html=True)
                        note_a = st.text_area("Justification for Override", key=f"note_a_{item['event_id']}", height=80, placeholder="Explain why this data is safe to ingest...")
                        if st.form_submit_button("Approve & Unblock Data", use_container_width=True):
                            result = approve_decision(item["event_id"], st.session_state.user_name, st.session_state.user_email, note_a)
                            st.success(result["message"])
                            st.rerun()

                with col_b:
                    with st.form(key=f"reject_{item['event_id']}"):
                        st.markdown(f"<div style='font-family:\'SF Mono\', Consolas, monospace; font-size:0.75rem; color:var(--text-color); margin-bottom: 8px;'>Authorized By: {st.session_state.user_name} ({st.session_state.user_email})</div>", unsafe_allow_html=True)
                        note_r = st.text_area("Justification for Rejection", key=f"note_r_{item['event_id']}", height=80, placeholder="Explain why the AI model's block is correct...")
                        if st.form_submit_button("Reject & Maintain Block", use_container_width=True):
                            result = reject_decision(item["event_id"], st.session_state.user_name, st.session_state.user_email, note_r)
                            st.success(result["message"])
                            st.rerun()

                st.markdown(f'<div style="font-family:\'SF Mono\', Consolas, monospace; font-size:0.7rem; color:var(--text-color); opacity:0.5; margin-top: 1rem;">Trace ID: {item["event_id"]}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════
#  TAB 3: FULL AUDIT LEDGER
# ══════════════════════════════════════════════════
with tab3:
    st.markdown("<h3 style='font-size: 1.1rem; margin-top: 1rem;'>Immutable Audit Record</h3>", unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">Comprehensive log of all automated decisions and subsequent manual interventions.</div>',
        unsafe_allow_html=True
    )

    all_decisions = get_recent_decisions(limit=250)
    if not all_decisions:
        st.info("The ledger is currently empty.")
    else:
        rows = []
        for d in all_decisions:
            ai_dec = d["ai_decision"]
            h_dec = d.get("human_decision") or "—"
            rows.append({
                "Timestamp (UTC)": str(d["timestamp"])[:19] if d["timestamp"] else "—",
                "Ingestion Pipeline": d["pipeline_name"],
                "Data Asset Target": d["data_asset"],
                "Automated Decision": ai_dec,
                "Model Conf.": f"{round((d.get('ai_confidence') or 0)*100)}%",
                "Semantic Drift": f"{(d.get('fingerprint_delta') or 0):.3f}",
                "Human Override": h_dec,
                "Auditor": d.get("human_name") or "—",
                "Trace ID": d["event_id"][:8] + "...",
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════
#  TAB 4: ACCOUNTABILITY REPORT
# ══════════════════════════════════════════════════
with tab4:
    st.markdown("<h3 style='font-size: 1.1rem; margin-top: 1rem;'>Human Intervention Tracking</h3>", unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">Isolated view of manual overrides for compliance and operational auditing purposes.</div>',
        unsafe_allow_html=True,
    )

    report = get_accountability_report()
    if not report:
        st.info("No manual overrides have been recorded.")
    else:
        rows = []
        for r in report:
            rows.append({
                "Timestamp (UTC)": str(r["timestamp"])[:19] if r["timestamp"] else "—",
                "Authorized Auditor": f'{r["human_name"]} ({r["human_email"]})',
                "Arbitration": r["human_decision"],
                "Target Asset": r["data_asset"],
                "Original Model Status": r["original_ai_decision"],
                "Justification Note": (r.get("human_note") or "—")[:100],
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════
#  TAB 5: CONTRACT MANAGEMENT
# ══════════════════════════════════════════════════
with tab5:
    st.markdown("<h3 style='font-size: 1.1rem; margin-top: 1rem;'>Semantic Data Contracts</h3>", unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">Define semantic constraints in plain English. The Prism compilation engine will translate these into executable invariants.</div>',
        unsafe_allow_html=True,
    )

    st.markdown("<h4 style='font-size: 0.95rem; margin-top: 1.5rem; color: #8b949e !important; text-transform: uppercase;'>Register New Sequence</h4>", unsafe_allow_html=True)

    with st.form("new_contract"):
        asset = st.text_input("Target Asset Identifier", placeholder="e.g., fct_monthly_revenue")
        author = f"{st.session_state.user_name} ({st.session_state.user_email})"
        st.markdown(f"<div style='font-family:\'SF Mono\', Consolas, monospace; font-size:0.75rem; color:var(--text-color); opacity:0.7; margin-bottom: 12px;'>Authoring Entity: {author}</div>", unsafe_allow_html=True)
        contract_text = st.text_area(
            "Semantic Constraints (Plain English)",
            height=120,
            placeholder=(
                "Revenue must be recorded in USD and cannot be negative. "
                "The metric represents strictly active monthly subscribers. "
                "Week-over-week volatility should not exceed 30%."
            )
        )
        col1, col2, col3 = st.columns([1,2,1])
        with col2:
            submitted = st.form_submit_button("Compile & Register Contract", use_container_width=True)
            
        if submitted:
            if asset and contract_text and author:
                from core.contracts import create_contract
                with st.spinner("Compiling semantic constraints..."):
                    result = create_contract(asset, contract_text, author)
                st.success(f"Successfully compiled invariants for '{asset}'")
                with st.expander("View Compiled Invariants Structure"):
                    import json
                    st.json(result["compiled"])
                st.rerun()
            else:
                st.error("Please supply both target identifier and constraint text.")

    st.markdown("<h4 style='font-size: 0.95rem; margin-top: 2rem; color: #8b949e !important; text-transform: uppercase;'>Active Contracts</h4>", unsafe_allow_html=True)
    contracts = get_contracts()
    if not contracts:
        st.info("No semantic contracts registered in the active environment.")
    else:
        for c in contracts:
            with st.expander(f"CONTRACT: {c['data_asset']}  │  Author: {c['created_by']}"):
                st.markdown(f"<div style='font-size: 0.9rem; color: var(--text-color); margin-bottom: 1rem;'>{c['plain_english']}</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='font-family:\'SF Mono\', Consolas, monospace; font-size:0.75rem; color:var(--text-color); opacity:0.7;'>Last Compilation: {str(c['updated_at'])[:19]}</div>", unsafe_allow_html=True)
                import json
                try:
                    rules = json.loads(c.get("compiled_rules") or "{}")
                    if rules:
                        st.json(rules)
                except Exception:
                    pass

# ─── Footer ────────────────────────────────────────────────────────────
st.markdown("<hr style='margin: 3rem 0 1rem 0; border-color: rgba(128, 128, 128, 0.2);' />", unsafe_allow_html=True)
st.markdown(
    '<center><span style="color:var(--text-color); opacity:0.5; font-size: 0.8rem; letter-spacing: 0.5px;">PRISM SEMANTIC DATA CLEARINGHOUSE &nbsp;│&nbsp; '
    'ENTERPRISE INFRASTRUCTURE &nbsp;│&nbsp; GITHUB.COM/PRISM-DATA</span></center>',
    unsafe_allow_html=True,
)

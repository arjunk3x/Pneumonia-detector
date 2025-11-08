# =========================
# Stylish per-row "View" actions
# =========================

# ---- once per page: light CSS theme for rows, pills & buttons
st.markdown("""
<style>
/* card row */
.action-card {
  padding: 12px 16px;
  border: 1px solid #ececec;
  border-radius: 14px;
  background: linear-gradient(180deg,#ffffff 0%, #fafafa 100%);
  box-shadow: 0 1px 2px rgba(0,0,0,.05);
}
/* small meta text */
.action-meta { font-size: 13px; color: #6b7280; }
/* id/title */
.action-id { font-size: 16px; font-weight: 700; }
/* pills */
.pill { display:inline-block; padding: 2px 10px; border-radius: 999px;
        font-size: 12px; font-weight: 600; margin-right: 6px; }
.pill.ok     { background:#e8f5e9; color:#1b5e20; }
.pill.warn   { background:#fff8e1; color:#7b5e00; }
.pill.danger { background:#ffebee; color:#b71c1c; }
/* pretty buttons */
.stButton>button {
  border-radius: 12px; padding: 8px 14px; font-weight: 600;
  border: 1px solid #e5e7eb; background: white;
  box-shadow: 0 1px 2px rgba(0,0,0,.04); transition: all .15s ease;
}
.stButton>button:hover {
  transform: translateY(-1px);
  box-shadow: 0 8px 18px rgba(0,0,0,.08);
}
</style>
""", unsafe_allow_html=True)

st.markdown("#### Actions")

def _pill_class(txt: str) -> str:
    """Map text to a pill color class."""
    if not txt: return "ok"
    t = str(txt).lower()
    if any(k in t for k in ["high", "critical", "reject", "blocked", "risk 3"]): return "danger"
    if any(k in t for k in ["medium", "review", "pending", "risk 2"]):           return "warn"
    return "ok"  # low / approved / risk 1

for _, r in filtered_df.reset_index(drop=True).iterrows():
    sid   = str(r["Sanction_ID"])
    stage = str(r["Stage"])
    stat  = str(r["Status in Stage"])
    val   = r["Value"]
    risk  = str(r["Risk Level"])

    risk_cls = _pill_class(risk)
    stat_cls = _pill_class(stat)

    # Two columns: card text | button
    c1, c2 = st.columns([6, 1], vertical_alignment="center")

    with c1:
        st.markdown(
            f"""
            <div class="action-card">
              <div class="action-id">🔎 {sid}</div>
              <div class="action-meta">
                Stage: <b>{stage}</b> &nbsp;•&nbsp;
                Status: <span class="pill {stat_cls}">{stat}</span> &nbsp;•&nbsp;
                Value: <b>{val:,.0f}</b> &nbsp;•&nbsp;
                Risk: <span class="pill {risk_cls}">{risk}</span>
              </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c2:
        if st.button("View →", key=f"view_{sid}"):
            st.session_state["selected_sanction_id"] = sid
            # path must match how you registered the page
            st.switch_page("app_pages/Feedback_Page.py")

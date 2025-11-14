# ----------- PASTEL UI CSS (wrapper + filters + table headers) -----------
st.markdown(
    """
    <style>

    /* ------------------------------
        SECTION WRAPPER
    ------------------------------*/
    .pending-wrapper {
        background: #f8fafc;
        border-radius: 18px;
        padding: 20px 22px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 12px rgba(15, 23, 42, 0.05);
        margin-bottom: 20px;
    }

    .section-title {
        font-size: 1.35rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        color: #0f172a;
        margin-bottom: 4px;
        text-transform: uppercase;
    }

    .section-subtitle {
        font-size: 0.9rem;
        color: #6b7280;
        margin-bottom: 18px;
    }


    /* ------------------------------
        FILTERS – Pastel input boxes
    ------------------------------*/
    .filter-container {
        background: #eef2ff;      /* soft pastel lavender */
        padding: 14px 16px;
        border-radius: 14px;
        border: 1px solid #e0e7ff;
        box-shadow: 0 3px 8px rgba(99, 102, 241, 0.12);
        margin-bottom: 16px;
    }

    .filter-help {
        font-size: 0.82rem;
        font-weight: 600;
        color: #6366f1;
        margin-bottom: 10px;
        letter-spacing: 0.02em;
    }

    /* Style text inputs / multiselects */
    .stTextInput > div > div > input,
    .stMultiSelect div[data-baseweb="select"] > div {
        background-color: #ffffff !important;
        border-radius: 10px !important;
        border: 1px solid #d4d4ff !important;
        box-shadow: inset 0 1px 3px rgba(99, 102, 241, 0.15);
    }

    .stTextInput > div > div > input:hover,
    .stMultiSelect div[data-baseweb="select"] > div:hover {
        border: 1px solid #a5b4fc !important;
        box-shadow: 0 0 0 3px rgba(165, 180, 252, 0.35);
    }

    /* Widget labels */
    label[data-testid="stWidgetLabel"] > div {
        font-weight: 600;
        color: #4338ca !important;
    }


    /* ------------------------------
        DATA TABLE HEADER STYLING
    ------------------------------*/
    [data-testid="stDataFrame"] thead tr th,
    [data-testid="stDataEditor"] thead tr th {
        background: #e0f2fe !important;             /* pastel sky blue */
        color: #0c4a6e !important;
        font-weight: 700 !important;
        border-bottom: 2px solid #bae6fd !important;
        font-size: 0.92rem !important;
        padding-top: 8px !important;
        padding-bottom: 8px !important;
        border-radius: 6px !important;
    }

    /* Table container */
    [data-testid="stDataFrame"], [data-testid="stDataEditor"] {
        background-color: #f0f9ff !important;
        border-radius: 14px;
        border: 1px solid #bae6fd;
        box-shadow: 0 3px 10px rgba(56, 189, 248, 0.25);
        padding: 6px;
    }

    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- SECTION WRAPPER ----------
with st.container():
    st.markdown('<div class="pending-wrapper">', unsafe_allow_html=True)

    # Title + subtitle
    st.markdown(
        f"""
        <div class="section-title">Pending in {current_role}</div>
        <div class="section-subtitle">
            Sanctions currently at your stage. Use the filters below to focus on specific IDs, statuses, or stages.
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------- BUILD DISPLAY DF ----------
    # risk_series from full df; default to "Medium"
    risk_series = df.get("Risk Level", pd.Series(["Medium"] * len(df)))

    risk_txt = (
        pending_df.index.to_series()
        .map(lambda i: risk_series.iloc[i] if i in risk_series.index else "Medium")
        .fillna("Medium")
        .astype(str)
    )

    def risk_badge(v: str) -> str:
        s = str(v).strip().lower()
        if s == "high":
            return "🔴 High"
        if s == "low":
            return "🟢 Low"
        return "🟠 Medium"

    display_df = pd.DataFrame(
        {
            "Sanction_ID": pending_df["Sanction_ID"].astype(str),
            "Value": pending_df["Value"],
            "Stage": current_role,
            "Status in Stage": pending_df[status_col].fillna("Pending").astype(str),
            "Risk Level": risk_txt.map(risk_badge),
        }
    )

    # ---------- FILTERS (inside pastel container) ----------
    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    st.markdown('<div class="filter-help">FILTERS</div>', unsafe_allow_html=True)

    colA, colB, colC = st.columns(3)

    with colA:
        search_id = st.text_input("Search by Sanction_ID", "")

    with colB:
        selected_status = st.multiselect(
            "Filter by Status",
            options=sorted(display_df["Status in Stage"].dropna().unique()),
        )

    with colC:
        selected_stage = st.multiselect(
            "Filter by Stage",
            options=sorted(display_df["Stage"].dropna().unique()),
            default=[current_role] if current_role in display_df["Stage"].unique() else [],
        )

    st.markdown("</div>", unsafe_allow_html=True)  # close filter-container

    # ---------- APPLY FILTERS ----------
    filtered_df = display_df.copy()
    if search_id:
        filtered_df = filtered_df[
            filtered_df["Sanction_ID"].str.contains(search_id, case=False)
        ]
    if selected_status:
        filtered_df = filtered_df[
            filtered_df["Status in Stage"].isin(selected_status)
        ]
    if selected_stage:
        filtered_df = filtered_df[filtered_df["Stage"].isin(selected_stage)]

    # ---------- PASTEL DATA EDITOR ----------
    st.data_editor(
        filtered_df[["Sanction_ID", "Value", "Stage", "Status in Stage", "Risk Level"]],
        hide_index=True,
        disabled=True,
        use_container_width=True,
        column_config={
            "Sanction_ID": st.column_config.TextColumn("Sanction ID"),
            "Value": st.column_config.NumberColumn("Value"),
            "Stage": st.column_config.TextColumn("Stage"),
            "Status in Stage": st.column_config.TextColumn("Status in Stage"),
            "Risk Level": st.column_config.TextColumn("Risk Level"),
        },
    )

    st.markdown("</div>", unsafe_allow_html=True)  # close pending-wrapper

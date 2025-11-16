def kpi_card(title, value, bg="#0A1A44", badge_bg="#0EA5E9", badge_color="#FFFFFF"):
    st.markdown(
        f"""
        <div style="
            background:{bg};
            border-radius:14px;
            padding:20px 22px;
            text-align:center;
            color:white;
            border:1px solid rgba(255,255,255,0.15);
            box-shadow:0 4px 10px rgba(0,0,0,0.15);
        ">
            
            <div style="
                font-size:36px;
                font-weight:800;
                margin-bottom:6px;
            ">
                {value}
            </div>

            <span style="
                display:inline-block;
                padding:6px 14px;
                border-radius:999px;
                background:{badge_bg};
                color:{badge_color};
                font-weight:700;
                font-size:14px;
            ">
                {title}
            </span>

        </div>
        """,
        unsafe_allow_html=True,
    )



with c1:
    kpi_card("Pending", len(pending_df),
             bg="#0A1A44",          # hero dark blue
             badge_bg="#1E3A8A",    # deep blue accent
             badge_color="#FFFFFF")

with c2:
    kpi_card("Approved", len(approved_df),
             bg="#0A1A44",
             badge_bg="#16A34A",     # green success
             badge_color="#FFFFFF")

with c3:
    kpi_card("Awaiting Others", len(awaiting_df),
             bg="#0A1A44",
             badge_bg="#F59E0B",     # orange warning
             badge_color="#FFFFFF")

with c4:
    kpi_card("Total Items", len(df),
             bg="#0A1A44",
             badge_bg="#0EA5E9",     # aqua highlight
             badge_color="#FFFFFF")

def create_card(title, value, subtext="", bg_color="#E6F4FF", size="lg"):
    # size presets
    sizes = {
        "sm": {"pad": 14, "title": 16, "value": 28, "sub": 12, "radius": 10},
        "md": {"pad": 18, "title": 18, "value": 36, "sub": 13, "radius": 12},
        "lg": {"pad": 22, "title": 20, "value": 44, "sub": 14, "radius": 14},
        "xl": {"pad": 26, "title": 22, "value": 52, "sub": 16, "radius": 16},
    }
    s = sizes.get(size, sizes["lg"])

    st.markdown(
        f"""
        <div style="
            background:{bg_color};
            border:1px solid #E5E7EB;
            border-radius:{s['radius']}px;
            padding:{s['pad']}px;
            margin:10px 4px;
            text-align:center;
            box-shadow:0 2px 6px rgba(0,0,0,0.06);
        ">
            <div style="font-size:{s['value']}px; font-weight:700; margin:0;">{value}</div>
            <div style="color:#374151; font-size:{s['title']}px; font-weight:600; margin-top:6px;">
                {title}
            </div>
            <div style="color:#6B7280; font-size:{s['sub']}px; margin-top:4px;">
                {subtext}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )



c1, c2, c3, c4 = st.columns([1.3, 1.3, 1.3, 1.3])
with c1:
    create_card("Pending Approvals", len(pending_df), "", "#E6F4FF", size="xl")
with c2:
    create_card("Sanctions to Review", len(to_review_df), "", "#FFF4E5", size="xl")
with c3:
    create_card(f"Approved by {current_role}", len(approved_df), "", "#E7F8E6", size="xl")
with c4:
    create_card("Awaiting Others", len(awaiting_df), "", "#FFE8E8", size="xl")



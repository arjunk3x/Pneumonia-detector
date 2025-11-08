def create_card(
    title, value, subtext=None,
    bg_color="#E6F4FF", size="lg",
    title_as_badge=True, badge_bg="#1D4ED8", badge_color="#ffffff"
):
    # --- size presets ---
    sizes = {
        "sm": {"pad": 14, "title": 14, "value": 22, "sub": 12, "radius": 10, "badge_py": 4, "badge_px": 10},
        "md": {"pad": 18, "title": 16, "value": 30, "sub": 13, "radius": 12, "badge_py": 5, "badge_px": 12},
        "lg": {"pad": 22, "title": 18, "value": 36, "sub": 14, "radius": 14, "badge_py": 6, "badge_px": 14},
        "xl": {"pad": 26, "title": 20, "value": 40, "sub": 16, "radius": 16, "badge_py": 7, "badge_px": 16},
    }
    s = sizes.get(size, sizes["lg"])

    # --- title badge style ---
    if title_as_badge:
        title_html = (
            f'<span style="display:inline-block;'
            f' padding:{s["badge_py"]}px {s["badge_px"]}px;'
            f' border-radius:999px;'
            f' background:{badge_bg};'
            f' color:{badge_color};'
            f' font-size:{s["title"]}px;'
            f' font-weight:700;'
            f' letter-spacing:.3px;">{title}</span>'
        )
    else:
        title_html = (
            f'<div style="color:#374151; font-size:{s["title"]}px; font-weight:600; margin-top:6px;">'
            f'{title}</div>'
        )

    # --- optional subtext ---
    sub_html = (
        f'<div style="color:#6B7280; font-size:{s["sub"]}px; margin-top:6px;">{subtext}</div>'
        if subtext else ""
    )

    # --- card layout ---
    import streamlit as st
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
            <div style="margin-top:10px;">{title_html}</div>
            {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


c1, c2, c3, c4 = st.columns([1.3, 1.3, 1.3, 1.3])

with c1:
    create_card(
        "Pending Approvals", len(pending_df),
        subtext="+12% this week",
        bg_color="#E6F4FF", size="xl",
        badge_bg="#1D4ED8", badge_color="#FFFFFF"
    )

with c2:
    create_card(
        "Sanctions to Review", len(to_review_df),
        subtext="+5% last month",
        bg_color="#FFF4E5", size="xl",
        badge_bg="#CA8A04", badge_color="#1F2937"
    )

with c3:
    create_card(
        f"Approved by {current_role}", len(approved_df),
        subtext="Improved by 0.5 day",
        bg_color="#E7F8E6", size="xl",
        badge_bg="#16A34A", badge_color="#FFFFFF"
    )

with c4:
    create_card(
        "Awaiting Others", len(awaiting_df),
        subtext="2 new this quarter",
        bg_color="#FFE8E8", size="xl",
        badge_bg="#DC2626", badge_color="#FFFFFF"
    )



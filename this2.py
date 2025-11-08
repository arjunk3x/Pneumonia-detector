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
    </div>   <!-- ✅ only one closing tag -->
    """,
    unsafe_allow_html=True,
)

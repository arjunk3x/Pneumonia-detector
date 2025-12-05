import base64
from pathlib import Path

import pandas as pd
import streamlit as st

# --------------------------------------------------------------------
# RIGHT: ATTACHMENTS PANEL – SANCTION DOCUMENT VIEW
# --------------------------------------------------------------------
with right:
    # Section title
    st.markdown(
        "<h3 style='font-weight:700;'>Attachments</h3>",
        unsafe_allow_html=True,
    )

    # ----------------- READ ATTACHMENTS CELL -----------------
    atts_raw = s_row.get("Attachments", None)

    # sensible default filename even if no attachment stored
    main_name = f"sanction_report_{sid}.pdf"
    has_attachment_name = False

    if atts_raw is not None:
        txt = str(atts_raw).strip()
        # guard against NaN / None / blank
        if txt and txt.lower() not in ("nan", "none"):
            items = [a.strip() for a in txt.replace(";", ",").split(",") if a.strip()]
            if items:
                main_name = items[0]          # e.g. "SanctionDocument.pdf"
                has_attachment_name = True

    if not has_attachment_name:
        st.info("No original attachment name stored – using generated sanction PDF.")

    pdf_name = Path(main_name)

    # ----------------- HEADER: ICON + TITLE -----------------
    # (Assumes you already have CSS for these classes in your app-wide styles.
    #  If not, you can easily swap to inline styles.)
    st.markdown(
        """
        <div class="attachments-panel-header">
            <div class="attachments-panel-icon">PDF</div>
            <div class="attachments-panel-title">Sanction Document View</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ----------------- PREVIEW IMAGE -----------------
    preview_path = Path("assets/Preview_image.png")

    if preview_path.exists():
        st.image(str(preview_path), use_container_width=True)
    else:
        # nice styled "no preview" card
        st.markdown(
            """
            <div style="
                height:230px;
                border-radius:10px;
                border:1px solid #e5e7eb;
                background:linear-gradient(180deg,#ffffff 0%,#f1f5f9 100%);
                display:flex;
                align-items:center;
                justify-content:center;
                color:#6b7280;
                font-size:0.9rem;
            ">
                Document preview not available
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ----------------- CAPTION TEXT -----------------
    st.markdown(
        """
        <div style="
            margin-top:0.6rem;
            color:#6b7280;
            font-size:0.85rem;
            font-family:Inter, system-ui, sans-serif;
        ">
            The official sanction document is available for viewing and download.
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ----------------- LOAD REAL PDF BYTES -----------------
    try:
        # First try to load the real sanction report PDF (if it exists)
        real_pdf_path = Path(f"sanction_database/sanction_report_{sid}.pdf")

        if real_pdf_path.exists():
            pdf_path = real_pdf_path
        else:
            # Fall back to preview template (your current static PDF)
            pdf_path = Path("assets/SanctionTemplate.pdf")

        file_bytes = pdf_path.read_bytes()

    except Exception:
        # Final fallback so the app doesn't crash
        file_bytes = main_name.encode("utf-8")

    # Encode PDF bytes as base64 for HTML download link
    b64 = base64.b64encode(file_bytes).decode("utf-8")
    download_href = f"data:application/pdf;base64,{b64}"

    # ----------------- RIGHT-ALIGNED STYLED PDF BUTTON -----------------
    button_html = f"""
    <div style="display:flex; justify-content:flex-end; margin-top:1.1rem;">
        <a href="{download_href}" download="{main_name}" style="text-decoration:none;">
            <button style="
                background-color:#A3ACF3;
                color:black;
                border:none;
                border-radius:999px;
                padding:0.55rem 1.7rem;
                font-size:0.92rem;
                font-weight:600;
                font-family:Inter, system-ui, sans-serif;
                box-shadow:0 2px 4px rgba(0,0,0,0.15);
                cursor:pointer;
                display:flex;
                align-items:center;
                gap:0.4rem;
            ">
                <span>⬇</span>
                <span>Download Sanction PDF</span>
            </button>
        </a>
    </div>
    """

    st.markdown(button_html, unsafe_allow_html=True)

    st.divider()

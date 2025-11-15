/* ===== Attachments – Sanction Document View Card ===== */

.attachments-panel {
    margin-top: 0.5rem;
    background: #ffffff;
    border-radius: 16px;
    box-shadow: 0 1px 4px rgba(15, 23, 42, 0.10);
    padding: 1rem 1.25rem 1.1rem;
}

.attachments-panel-header {
    display: flex;
    align-items: center;
    gap: 0.65rem;
    margin-bottom: 0.75rem;
}

.attachments-panel-icon {
    width: 32px;
    height: 32px;
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1rem;
    font-weight: 600;
    background: #E74C3C;        /* red PDF-style icon */
    color: #ffffff;
}

.attachments-panel-title {
    font-size: 1.05rem;
    font-weight: 600;
}

.attachments-panel-body {
    background: #F8FAFC;        /* light grey/blue content area */
    border-radius: 12px;
    padding: 0.85rem;
    text-align: center;
    margin-bottom: 0.75rem;
}

/* Placeholder for preview text/image caption */
.attachments-panel-caption {
    margin-top: 0.55rem;
    font-size: 0.85rem;
    color: #6b7280;
}

/* Full-width download button inside the card */
.download-pdf-btn button {
    width: 100% !important;
    background-color: #A3ACF3 !important;   /* pastel periwinkle */
    color: #ffffff !important;
    border-radius: 999px !important;
    border: none !important;
    padding: 0.6rem 1.4rem !important;
    font-weight: 600 !important;
    font-size: 0.95rem !important;
    cursor: pointer !important;
}



# ---------- RIGHT: ATTACHMENTS – Sanction Document View ----------
# ---------- RIGHT: ATTACHMENTS – Sanction Document View ----------
with right:
    st.markdown(
        '<h3 style="font-weight:700; margin-bottom:0.5rem;">Attachments</h3>',
        unsafe_allow_html=True,
    )

    atts = s_row.get("Attachments", "")

    if pd.isna(atts) or str(atts).strip() == "":
        st.info("No attachments uploaded.")
    else:
        # Parse the attachments list
        items = [a.strip() for a in str(atts).replace(";", ",").split(",") if a.strip()]
        main_name = items[0]                                  # use first document

        # Start the panel
        st.markdown(
            f"""
                <div class="attachments-panel">
                    <div class="attachments-panel-header">
                        <div class="attachments-panel-icon">PDF</div>
                        <div class="attachments-panel-title">Sanction Document View</div>
                    </div>
            """,
            unsafe_allow_html=True,
        )

        # --- PDF PREVIEW (optional but recommended) ---
        # Path where preview images would exist (PNG/JPG)
        preview_path = f"assets/previews/{main_name.replace('.pdf', '.png')}"

        if os.path.exists(preview_path):
            st.image(preview_path, use_column_width=True)
        else:
            # Placeholder box
            st.markdown(
                """
                <div class="attachments-panel-body">
                    <div style="height:220px; border-radius:8px; border:1px solid #e5e7eb; 
                                background:linear-gradient(180deg,#ffffff 0%,#f1f5f9 100%);
                                display:flex; align-items:center; justify-content:center;
                                font-size:0.85rem; color:#6b7280;">
                        Document preview not available
                    </div>
                """,
                unsafe_allow_html=True,
            )

        # Caption
        st.markdown(
            """
            <div class="attachments-panel-caption">
                The official sanction document is available for viewing and download.
            </div>
            </div> <!-- end attachments-panel-body -->
            """,
            unsafe_allow_html=True,
        )

        # --- REAL PDF DOWNLOAD ---
        st.markdown('<div class="download-pdf-btn">', unsafe_allow_html=True)

        try:
            pdf_path = Path("assets/attachments") / main_name
            file_bytes = pdf_path.read_bytes()
        except Exception:
            file_bytes = main_name.encode("utf-8")  # fallback

        st.download_button(
            label="Download Sanction PDF",
            data=file_bytes,
            file_name=main_name,
            key=f"download_{sid}",
        )

        # Close wrapper
        st.markdown("</div></div>", unsafe_allow_html=True)



st.markdown("""
<style>

.attachments-panel {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 1.2rem 1.4rem;
    margin-top: 0.5rem;
    box-shadow: 0 1px 2px rgba(0,0,0,0.06);
}

/* Header of the attachments card */
.attachments-panel-header {
    display: flex;
    align-items: center;
    gap: 0.60rem;
    margin-bottom: 0.8rem;
}

/* PDF icon */
.attachments-panel-icon {
    background-color: #ef4444;
    color: white;
    padding: 0.35rem 0.55rem;
    border-radius: 6px;
    font-weight: 700;
    font-size: 0.75rem;
    font-family: 'Inter', sans-serif;
}

/* Title */
.attachments-panel-title {
    font-size: 1.05rem;
    font-weight: 700;
    color: #374151;
    font-family: 'Inter', sans-serif;
}

/* Caption under preview */
.attachments-panel-caption {
    margin-top: 0.60rem;
    color: #6b7280;
    font-size: 0.85rem;
    font-family: 'Inter', sans-serif;
}

/* Footer: aligns the PDF button to the right */
.attachments-panel-footer {
    display: flex;
    justify-content: flex-end;
    margin-top: 1.2rem;
}

/* Styled PDF button wrapper */
.pdf-download-btn button {
    background-color: #A3ACF3 !important;           /* pastel periwinkle */
    color: white !important;
    border-radius: 999px !important;                /* pill button */
    border: none !important;
    padding: 0.55rem 1.6rem !important;
    font-size: 0.92rem !important;
    font-weight: 600 !important;
    font-family: 'Inter', sans-serif !important;
    box-shadow: 0 2px 4px rgba(0,0,0,0.15);
    cursor: pointer !important;
    transition: 0.12s ease;
}

/* Hover effect */
.pdf-download-btn button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 10px rgba(0,0,0,0.18);
}

</style>
""", unsafe_allow_html=True)


# ---------- RIGHT: ATTACHMENTS – Sanction Document View ----------
# -------------------- ATTACHMENTS PANEL --------------------
with right:
    st.markdown("<h3 style='font-weight:700;'>Attachments</h3>", unsafe_allow_html=True)

    atts = s_row.get("Attachments", "")

    if pd.isna(atts) or str(atts).strip() == "":
        st.info("No attachments uploaded.")
    else:
        # Use first item only
        items = [a.strip() for a in str(atts).replace(";", ",").split(",") if a.strip()]
        main_name = items[0]              # e.g. "SanctionDocument.pdf"
        pdf_name = Path(main_name)

        st.markdown('<div class="attachments-panel">', unsafe_allow_html=True)

        # ---- Header with icon + title ----
        st.markdown(
            """
            <div class="attachments-panel-header">
                <div class="attachments-panel-icon">PDF</div>
                <div class="attachments-panel-title">Sanction Document View</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # ---- PREVIEW IMAGE (OPTIONAL) ----
        preview_path = Path("assets/previews") / (pdf_name.stem + ".png")

        if preview_path.exists():
            st.image(str(preview_path), use_container_width=True)
        else:
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
                    font-size:0.9rem;">
                    Document preview not available
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ---- Caption ----
        st.markdown(
            """
            <div class="attachments-panel-caption">
                The official sanction document is available for viewing and download.
            </div>
            """,
            unsafe_allow_html=True,
        )

        # ---- PDF DOWNLOAD BUTTON (RIGHT ALIGNED) ----
        st.markdown('<div class="attachments-panel-footer pdf-download-btn">', unsafe_allow_html=True)

        # Try loading PDF
        try:
            pdf_path = Path("assets/attachments") / main_name
            file_bytes = pdf_path.read_bytes()
        except:
            file_bytes = main_name.encode("utf-8")

        st.download_button(
            label="📄 Download Sanction PDF",
            data=file_bytes,
            file_name=main_name,
            key=f"download_pdf_{sid}",
        )

        st.markdown("</div></div>", unsafe_allow_html=True)




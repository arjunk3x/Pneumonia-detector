# -------------------- ATTACHMENTS PANEL --------------------
with right:
    st.markdown("<h3 style='font-weight:700;'>Attachments</h3>", unsafe_allow_html=True)

    atts = s_row.get("Attachments", "")

    if pd.isna(atts) or str(atts).strip() == "":
        st.info("No attachments uploaded.")
    else:
        # Use first attachment as the main sanction PDF
        items = [a.strip() for a in str(atts).replace(";", ",").split(",") if a.strip()]
        main_name = items[0]              # e.g. "SanctionDocument.pdf"
        pdf_name = Path(main_name)

        # Open the outer panel
        st.markdown(
            """
            <div style="
                background:#ffffff;
                border:1px solid #e5e7eb;
                border-radius:12px;
                padding:1.2rem 1.4rem;
                margin-top:0.5rem;
                box-shadow:0 1px 2px rgba(0,0,0,0.06);
            ">
            """,
            unsafe_allow_html=True,
        )

        # ---- Header: icon + title ----
        st.markdown(
            """
            <div style="display:flex; align-items:center; gap:0.6rem; margin-bottom:0.8rem;">
                <div style="
                    background:#ef4444;
                    color:#ffffff;
                    padding:0.35rem 0.55rem;
                    border-radius:6px;
                    font-weight:700;
                    font-size:0.75rem;
                    font-family:Inter, system-ui, sans-serif;
                ">
                    PDF
                </div>
                <div style="
                    font-size:1.05rem;
                    font-weight:700;
                    color:#374151;
                    font-family:Inter, system-ui, sans-serif;
                ">
                    Sanction Document View
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # ---- PREVIEW IMAGE ----
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
                    font-size:0.9rem;
                ">
                    Document preview not available
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ---- Caption ----
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

        # ---- LOAD REAL PDF BYTES ----
        try:
            pdf_path = Path("assets/attachments") / main_name
            file_bytes = pdf_path.read_bytes()
        except Exception:
            file_bytes = main_name.encode("utf-8")  # fallback so the app doesn't crash

        # Encode PDF bytes as base64 for HTML download link
        b64 = base64.b64encode(file_bytes).decode()
        download_href = f"data:application/pdf;base64,{b64}"

        # ---- RIGHT-ALIGNED STYLED PDF BUTTON ----
        button_html = f"""
        <div style="display:flex; justify-content:flex-end; margin-top:1.1rem;">
            <a href="{download_href}" download="{main_name}" style="text-decoration:none;">
                <button style="
                    background-color:#A3ACF3;
                    color:#ffffff;
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
                    <span>📄</span>
                    <span>Download Sanction PDF</span>
                </button>
            </a>
        </div>
        """

        st.markdown(button_html, unsafe_allow_html=True)

        # Close outer panel div
        st.markdown("</div>", unsafe_allow_html=True)\














        st.markdown(
            """
            <div class="attachments-panel-header">
                <div class="attachments-panel-icon">PDF</div>
                <div class="attachments-panel-title">Sanction Document View</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


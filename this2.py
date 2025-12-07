# --------- RIGHT: ATTACHMENTS – Sanction Document View ----------
with right:
    st.markdown(
        "<h3 style='font-weight:700;'>Attachments</h3>",
        unsafe_allow_html=True,
    )

    # ---------- PATH HELPERS ----------
    BASE_DIR = Path(__file__).resolve().parent.parent  # project root
    SANCTION_DB_DIR = BASE_DIR / "sanction_database"
    ASSETS_DIR = BASE_DIR / "assets"

    # ---------- FIND LATEST VERSIONED PDF IN sanction_database ----------
    def get_latest_sanction_pdf(sid_value) -> Path | None:
        """
        Look for files like sanction_report_{sid}_v1.pdf, v2, v3...
        and return the one with the highest numeric version.
        """
        pattern = f"sanction_report_{sid_value}_v*.pdf"
        latest_path = None
        latest_version = -1

        if not SANCTION_DB_DIR.exists():
            return None

        for pdf_path in SANCTION_DB_DIR.glob(pattern):
            # extract version number from the filename
            m = re.search(r"_v(\d+)\.pdf$", pdf_path.name)
            if not m:
                continue
            v = int(m.group(1))
            if v > latest_version:
                latest_version = v
                latest_path = pdf_path

        return latest_path

    # ---------- CHOOSE PDF SOURCE ----------
    # 1) Try database (latest version)
    latest_pdf_path = get_latest_sanction_pdf(sid)

    if latest_pdf_path is not None:
        pdf_path = latest_pdf_path
        main_name = latest_pdf_path.name              # for download filename
        source_label = f"Latest uploaded sanction report (v from {latest_pdf_path.name})"
    else:
        # 2) Fallback: static template in assets
        pdf_path = ASSETS_DIR / "SanctionTemplate.pdf"
        main_name = f"sanction_report_{sid}_v1.pdf"   # or any default name you like
        source_label = "Template sanction report (no uploaded versions found)"

    # ---------- OPTIONAL: INFO LINE SO YOU SEE WHAT IT PICKED ----------
    st.caption(f"📄 Using PDF: `{pdf_path.name}` from `{pdf_path.parent.name}` ({source_label})")

    # ---------- PREVIEW IMAGE (your existing PNG card / styling) ----------
    assets_preview_path = ASSETS_DIR / "Preview_image.png"

    if assets_preview_path.exists():
        try:
            st.markdown("**Preview Image:**")
            st.image(str(assets_preview_path), use_container_width=True, caption="Document Preview")
        except Exception as e:
            st.warning(f"Could not display preview image: {e}")
    else:
        st.markdown("**Preview Image:**")
        st.markdown(
            """
            <div style="
                height:200px;
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

    # ---------- PDF PREVIEW (iframe) ----------
    try:
        st.markdown("**PDF Preview:**")

        if pdf_path.exists():
            with open(pdf_path, "rb") as pdf_file:
                base64_pdf = base64.b64encode(pdf_file.read()).decode("utf-8")

            pdf_display = f"""
            <iframe src="data:application/pdf;base64,{base64_pdf}"
                    width="100%" height="400"
                    type="application/pdf"
                    style="border-radius:10px; border:1px solid #e5e7eb;">
                <p>Your browser does not support PDFs.
                   <a href="data:application/pdf;base64,{base64_pdf}" download="{main_name}">
                       Download the PDF instead.
                   </a>
                </p>
            </iframe>
            """
            st.markdown(pdf_display, unsafe_allow_html=True)
        else:
            # If even the fallback doesn't exist
            st.markdown(
                """
                <div style="
                    height:200px;
                    border-radius:10px;
                    border:1px solid #e5e7eb;
                    background:linear-gradient(180deg,#ffffff 0%,#f1f5f9 100%);
                    display:flex;
                    align-items:center;
                    justify-content:center;
                    color:#6b7280;
                    font-size:0.9rem;
                ">
                    PDF preview not available (file not found)
                </div>
                """,
                unsafe_allow_html=True,
            )
    except Exception as e:
        st.warning(f"Could not display PDF preview: {e}")

    # ---------- LOAD BYTES FOR DOWNLOAD BUTTON ----------
    try:
        file_bytes = pdf_path.read_bytes()
    except Exception:
        # last-ditch fallback so app doesn't crash
        file_bytes = main_name.encode("utf-8")

    b64 = base64.b64encode(file_bytes).decode("utf-8")
    download_href = f"data:application/pdf;base64,{b64}"

    # ---------- STYLED DOWNLOAD BUTTON (your style) ----------
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
                transition: all 0.2s ease;
            ">
                <span>⬇</span>
                <span>Download Sanction PDF</span>
            </button>
        </a>
    </div>
    """
    st.markdown(button_html, unsafe_allow_html=True)

SECTION_TREE = {
    "1. Initial Details": {
        "1.1 Investment Details": [
            "Investment Title",
            "PRJ Number",
            "Investment Type",
            "Value (£)",
        ],
        "1.2 Investment Team": [
            "Sponsor / Exec Owner",
            "CIDO",
            "PLD",
            "Finance",
            "PM",
            "Regulatory Funding",
            "Delivery Vehicle",
            "Enterprise Architecture",
            "Data & AI",
        ],
        "1.3 Records of Approval": [
            "Digital Guild",
            "ET CIDO / DV DIDM",
            "Business Unit DIDM (ETIDM / SI Governance)",
        ],
    },
    "2. Executive Summary": {
        "2.1 Problem Statement": [
            "The problem we want to address",
            "What is required to close the gap (business need)",
            "Aim of this investment",
        ],
        "2.2 Investment Objective": [
            "To achieve X, this investment is seeking £_ funding",
        ],
        "2.3 Investment Structure": [
            "Is this paper part of multiple submissions?",
            "Description of submissions",
        ],
        "2.4 Risk Overview": [
            "Key risks (high-level description per risk)",
        ],
        "2.5 Benefit Overview": [
            "Key benefits (high-level description per benefit)",
        ],
        "2.6 Cost Overview": [
            "CapEx",
            "Project OpEx",
            "Net Sanction Value (NSV)",
            "Risk / Contingency (%)",
            "Gross Sanction Value (GSV)",
            "iRTB",
        ],
        "2.7 Regulatory Funding Allocation": [
            "Regulatory Allowance (Code)",
            "Cost (£)",
        ],
        "2.8 Approvals": [
            "Approval statement",
            "Decision",
            "E-Signature",
            "Name",
            "Role",
            "Date",
        ],
    },
    "3. Investment Description": {
        "3.1 In Scope": [
            "In-scope items",
        ],
        "3.2 Out of Scope": [
            "Out-of-scope items",
        ],
        "3.3 Non-Functional Requirements": [
            "Non-functional requirements",
        ],
        "3.4 Optioneering": [
            "Option",
            "Decision (Y/N)",
            "Rationale for decision",
        ],
        "3.5 Milestones": [
            "Milestone",
            "Criteria",
            "Date",
        ],
        "3.6 Solution Roadmap": [
            "Delivery of features / capabilities until end of investment",
            "Delivery across multiple PIs",
            "Key milestones",
            "Critical dependencies with other programmes",
        ],
        "3.7 Project Setup": [
            "Governance, controls, and delivery approach",
        ],
    },
    "4. Strategic Case": {
        "4.1 Strategic Alignment": [
            "Strategic alignment",
        ],
        "4.2 Benefits": [
            "Benefit ID",
            "Benefit description",
            "Financial or non-financial",
            "Value if financial (£)",
        ],
        "4.3 Risks": [
            "Risk",
            "Impact",
            "Mitigation",
            "Owner",
        ],
        "4.4 Dependencies": [
            "Dependency",
            "Initiative",
            "Timing of dependency",
            "Owner of dependency",
        ],
    },
    "5. Financial Case": {
        "5.1 Cost Breakdown": [
            "Cost category",
            "Project OpEx / CapEx",
            "Net Sanction Value (NSV) (£)",
            "Contingency (£)",
            "Gross Sanction Value (GSV) (£)",
            "Total row",
            "Total sanction value (£)",
        ],
        "5.2 Yearly Breakdown": [
            "Regulatory allowance (Code)",
            "FY26 CapEx",
            "FY26 OpEx",
            "FY27 CapEx",
            "FY27 OpEx",
            "FY28 CapEx",
            "FY28 OpEx",
            "FY29 CapEx",
            "FY29 OpEx",
            "Yearly CapEx / OpEx (£)",
            "Yearly total (£)",
            "Total (£)",
        ],
        "5.3 Finance Business Partner Judgement": [
            "CapEx / Project OpEx classification comments",
        ],
        "5.4 Funding Allowance Lines": [
            "Funding allowance line",
            "Status (Approved / Not approved with Strategic Portfolio)",
        ],
        "5.5 Funding Status": [
            "Part of existing regulatory funding",
            "Part of a re-opener",
            "Business / self-funded",
        ],
    },
    "6. Appendix": {
        "6. Appendix": [
            "Supporting material (if applicable)",
        ],
    },
}


with st.form(f"form_{current_stage}"):

    # 1. Decision
    decision = st.radio(
        "**Choose Your Action [Approve/Reject/Request changes]:**",
        ["Approve ✓", "Reject ✗", "Request changes ✎"],
        index=0,
        disabled=not role_can_act,
    )

    # 2. Rating
    rating_stars = st.selectbox(
        "**Rating (optional):**",
        ["⭐⭐⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐", "⭐⭐", "⭐", "-"],
        index=0,
        disabled=not role_can_act,
    )
    rating = rating_stars.count("⭐")

    # 3. Assigned To + Decision Time
    col1, col2 = st.columns(2)
    with col1:
        assigned_to = st.text_input(
            "**Assign to [Email/Name]:**",
            disabled=not role_can_act,
        )
    with col2:
        when = st.text_input(
            "**Decision time:**",
            value=_now_iso(),
            help="Auto-filled, editable.",
            disabled=not role_can_act,
        )

    # 3b. DOCUMENT LOCATION (Heading → Section → Subsection)
    st.markdown("#### Where in the document is this feedback about?")

    # Top-level heading
    heading_options = list(SECTION_TREE.keys())
    selected_heading = st.selectbox(
        "Heading",
        heading_options,
        disabled=not role_can_act,
    )

    # Section (depends on heading)
    section_options = list(SECTION_TREE[selected_heading].keys())
    selected_section = st.selectbox(
        "Section",
        section_options,
        disabled=not role_can_act,
    )

    # Subsection (optional, depends on section)
    subsection_list = SECTION_TREE[selected_heading][selected_section]
    subsection_options = ["(Whole section)"] + subsection_list
    selected_subsection = st.selectbox(
        "Sub-section / field (optional)",
        subsection_options,
        index=0,
        disabled=not role_can_act,
    )

    # Build a single path string (used in CSV)
    if selected_subsection == "(Whole section)":
        section_path = f"{selected_heading} > {selected_section}"
    else:
        section_path = f"{selected_heading} > {selected_section} > {selected_subsection}"

    # 4. Comments
    comment = st.text_area(
        "**Comments / Rationale**",
        placeholder="Add remarks for documentation.",
        disabled=not role_can_act,
    )

    # 5. Buttons (as you already have)
    ...







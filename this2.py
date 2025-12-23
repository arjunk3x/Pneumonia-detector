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


# =========================================================
# DECISION FORM (includes document hierarchy selector)
# =========================================================
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

    # --- HEADING LEVEL ---
    heading_options = list(SECTION_TREE.keys())
    selected_heading = st.selectbox(
        "Heading",
        heading_options,
        key="fb_heading",
        disabled=not role_can_act,
    )

    # --- SECTION LEVEL (depends on heading) ---
    section_options = list(SECTION_TREE[selected_heading].keys())
    selected_section = st.selectbox(
        "Section",
        section_options,
        key="fb_section",
        disabled=not role_can_act,
    )

    # --- SUBSECTION LEVEL (depends on section) ---
    subsection_list = SECTION_TREE[selected_heading][selected_section]
    subsection_options = ["(Whole section)"] + subsection_list

    selected_subsection = st.selectbox(
        "Sub-section / field (optional)",
        subsection_options,
        index=0,
        key="fb_subsection",
        disabled=not role_can_act,
    )

    # Build a single path string to save in feedback.csv
    if selected_subsection == "(Whole section)":
        section_path = f"{selected_heading} > {selected_section}"
    else:
        section_path = (
            f"{selected_heading} > {selected_section} > {selected_subsection}"
        )

    # Optional: show what they’re commenting on
    st.caption(f"Commenting on: `{section_path}`")

    # 4. Comments
    comment = st.text_area(
        "**Comments / Rationale**",
        placeholder="Add remarks for documentation.",
        disabled=not role_can_act,
    )

    # 5. Buttons (right side)
    spacer, col_buttons = st.columns([0.77, 0.23])
    b_reset, b_submit = col_buttons.columns([0.47, 0.53])

    with b_reset:
        cancel = st.form_submit_button("Reset form", disabled=not role_can_act)

    with b_submit:
        submitted = st.form_submit_button(
            "Submit decision", disabled=not role_can_act
        )







"""
GITHUB COPILOT – MANDATE APPROVER DASHBOARD + FEEDBACK LOGIC

Goal
====
Create a Mandate Approver flow, reusing the pattern from the existing
Sanction Approver Dashboard, but **simplified to two parallel approvers**:
James and Tim.

We want:
- A new Mandate Approver Dashboard (MandateApproverDashboard.py).
- A small extension on the existing Feedback_Page.py to handle Mandate
  decisions (Approve / Reject / Request changes) and update a separate
  Mandate approver tracker CSV.
- No SQL – only CSV files.

Data sources
============
1) Mandate_view.csv
   - This is the Mandate list, similar to Sanctions_view.
   - Must contain at least:
        Mandate_ID
        Value    (or similar – whatever is needed for cards)
        Risk     (if we want risk pills)
        Stage or other metadata (optional for cards)
   - We'll use Mandate_ID as the primary key.

2) Mandate_approver_tracker.csv
   - New CSV for tracking Mandate approvals by James and Tim.
   - Column structure (Copilot: create if file does not exist):

        Mandate_ID            (string)
        Current Stage         (string)  # e.g. "MandateApproval", "MandateApproved"
        Overall_status        (string)  # "Pending", "In progress", "Approved", "Rejected"

        james_status          (string)  # "Pending", "Approved", "Rejected", "Request Changes"
        james_flag            (bool)    # True if James still has this Mandate in Actions
        james_assigned_to     (string)
        james_decision_at     (string ISO timestamp)

        tim_status            (string)
        tim_flag              (bool)
        tim_assigned_to       (string)
        tim_decision_at       (string)

        last_comment          (string)

   - For a **new** Mandate_ID row, initialise:

        {
            "Mandate_ID": mid,
            "Current Stage": "MandateApproval",
            "Overall_status": "Pending",

            "james_status": "Pending",
            "james_flag": True,
            "james_assigned_to": "",
            "james_decision_at": "",

            "tim_status": "Pending",
            "tim_flag": True,
            "tim_assigned_to": "",
            "tim_decision_at": "",

            "last_comment": "",
        }

   - That is what makes an item appear in **both** James and Tim's Actions lists.

Global helpers (MandateApproverDashboard.py)
============================================
At the top of MandateApproverDashboard.py, add:

1) CSV helpers, or reuse existing ones from the sanctions code:

    from pathlib import Path
    import pandas as pd
    import streamlit as st

    MANDATE_VIEW_PATH = Path("Mandate_view.csv")
    MANDATE_TRACKER_PATH = Path("Mandate_approver_tracker.csv")

    def _read_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    def _write_csv(df: pd.DataFrame, path: Path) -> None:
        df.to_csv(path, index=False)

2) Stage metadata for the two Mandate approvers:

    MANDATE_STAGE_KEYS = {
        "James": {
            "status": "james_status",
            "flag": "james_flag",
            "assigned_to": "james_assigned_to",
            "decision_at": "james_decision_at",
        },
        "Tim": {
            "status": "tim_status",
            "flag": "tim_flag",
            "assigned_to": "tim_assigned_to",
            "decision_at": "tim_decision_at",
        },
    }

3) Ensure the tracker has all required columns:

    def _ensure_mandate_tracker_columns(df: pd.DataFrame) -> pd.DataFrame:
        base_cols = [
            "Mandate_ID",
            "Current Stage",
            "Overall_status",
            "last_comment",
        ]
        for col in base_cols:
            if col not in df.columns:
                df[col] = ""

        for meta in MANDATE_STAGE_KEYS.values():
            for col in meta.values():  # status / flag / assigned_to / decision_at
                if col not in df.columns:
                    if col.endswith("_flag"):
                        df[col] = True
                    else:
                        df[col] = ""

        return df

4) Function to know which Mandate role the user is acting as.
   For now, implement as a sidebar selectbox. Later this can be wired to login:

    def _current_mandate_role() -> str:
        return st.sidebar.selectbox("Acting as Mandate approver:", ["James", "Tim"])

Main dashboard logic – MandateApproverDashboard.py
==================================================
Inside the main `run()` (or equivalent) function of MandateApproverDashboard:

1) Load CSVs and ensure tracker columns:

    mandates_df = _read_csv(MANDATE_VIEW_PATH)
    tracker_df  = _read_csv(MANDATE_TRACKER_PATH)
    tracker_df  = _ensure_mandate_tracker_columns(tracker_df)

2) Ensure every Mandate_ID in Mandate_view has a row in the tracker:

    if not mandates_df.empty:
        existing_ids = set(tracker_df["Mandate_ID"].astype(str)) if not tracker_df.empty else set()
        new_rows = []

        for mid in mandates_df["Mandate_ID"].astype(str).tolist():
            if mid not in existing_ids:
                new_rows.append({
                    "Mandate_ID": mid,
                    "Current Stage": "MandateApproval",
                    "Overall_status": "Pending",
                    "james_status": "Pending",
                    "james_flag": True,
                    "james_assigned_to": "",
                    "james_decision_at": "",
                    "tim_status": "Pending",
                    "tim_flag": True,
                    "tim_assigned_to": "",
                    "tim_decision_at": "",
                    "last_comment": "",
                })

        if new_rows:
            tracker_df = pd.concat([tracker_df, pd.DataFrame(new_rows)], ignore_index=True)
            tracker_df = _ensure_mandate_tracker_columns(tracker_df)
            _write_csv(tracker_df, MANDATE_TRACKER_PATH)

3) Determine current approver role and its metadata:

    current_role = _current_mandate_role()          # "James" or "Tim"
    meta         = MANDATE_STAGE_KEYS[current_role]
    status_col   = meta["status"]
    flag_col     = meta["flag"]

4) Compute the list of Mandates that this approver should see under "Actions":

    if tracker_df.empty or mandates_df.empty:
        actions_df = pd.DataFrame()
    else:
        mask_actions = (
            (tracker_df[flag_col] == True) &
            (~tracker_df[status_col].isin(["Approved", "Rejected"]))
        )
        active_ids = tracker_df.loc[mask_actions, "Mandate_ID"].astype(str)
        actions_df = mandates_df[
            mandates_df["Mandate_ID"].astype(str).isin(active_ids)
        ].copy()

5) Render a section exactly like the Sanction Actions UI, but using `actions_df`
   and Mandate fields:

    st.markdown(
        f'''
        <div class="section-title" style="font-size: 28px; font-weight: bold;">
            Mandate Actions for {current_role}
        </div>
        ''',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="actions-divider"></div>', unsafe_allow_html=True)

    for _, r in actions_df.reset_index(drop=True).iterrows():
        # Reuse your existing card HTML: header, value pill, risk pill, etc.
        # IMPORTANT:
        # - Use Mandate_ID instead of Sanction_ID.
        # - The button should set `selected_mandate_id` and navigate to the Mandate feedback page.

        if st.button("View ➜", key=f"mandate_view_{r['Mandate_ID']}"):
            st.session_state["selected_mandate_id"] = str(r["Mandate_ID"])
            st.session_state.navigate_to_mandate_feedback = True
            st.rerun()

No Intake section is needed for mandates; only Actions based on the tracker flags.

Feedback page logic for Mandate approvals
=========================================
Extend Feedback_Page.py (or a new MandateFeedback_Page.py) so that when
James or Tim submits a decision, it updates Mandate_approver_tracker.csv.

High-level behaviour:

- Decisions: "Approve ✓", "Reject ✗", "Request changes ✎"
- Each decision is stored per-approver in:
    james_status / tim_status
    james_flag / tim_flag
    james_assigned_to / tim_assigned_to
    james_decision_at / tim_decision_at
- A Mandate is **fully approved** only when:
    james_status == "Approved" AND tim_status == "Approved"
- Overall_status / Current Stage rules:
    * Initially: Overall_status="Pending", Current Stage="MandateApproval"
    * If one approver approves:
        - Their *_status = "Approved"
        - Their *_flag = False
        - Overall_status = "In progress"
        - Current Stage = "MandateApproval"
    * When both have approved:
        - Overall_status = "Approved"
        - Current Stage = "MandateApproved"
        - Both *_flag = False
    * If either rejects:
        - That approver's *_status = "Rejected"
        - That approver's *_flag = False
        - Overall_status = "Rejected"
        - Current Stage = "MandateApproval"
    * If "Request changes":
        - That approver's *_status = "Request Changes"
        - Keep *_flag = True (so it stays in their Actions, or adapt as needed)
        - Overall_status = "Request Changes"
        - Current Stage = "MandateApproval"

Implementation details in Feedback page (within `if submitted:`)
================================================================
Assume we already know the selected Mandate_ID (`mid`) from session state,
and that we have loaded & ensured tracker columns:

    tracker_df = _read_csv(MANDATE_TRACKER_PATH)
    tracker_df = _ensure_mandate_tracker_columns(tracker_df)

    mask = tracker_df["Mandate_ID"].astype(str) == mid

    current_role = _current_mandate_role()        # "James" or "Tim"
    meta        = MANDATE_STAGE_KEYS[current_role]

    status_col       = meta["status"]
    flag_col         = meta["flag"]
    assigned_to_col  = meta["assigned_to"]
    decision_field   = meta["decision_at"]

    row_before = tracker_df.loc[mask].iloc[0]
    prev_status = str(row_before.get(status_col, ""))

1) Map radio decision to new_status:

    dec_lower = decision.lower()
    if "approve" in dec_lower:
        new_status = "Approved"
    elif "reject" in dec_lower:
        new_status = "Rejected"
    else:
        new_status = "Request Changes"

2) Prevent re-submission if already Rejected, same as you did before:

    if new_status == "Rejected" and prev_status == "Rejected":
        st.warning("This mandate has already been rejected. No further actions can be taken.")
        st.stop()

3) Basic column updates:

    tracker_df.loc[mask, status_col]      = new_status
    tracker_df.loc[mask, assigned_to_col] = assigned_to
    tracker_df.loc[mask, "last_comment"]  = comment

4) Stage progression logic for Mandates:

    if new_status == "Approved":
        # Mark this approver's stage as completed and remove from their Actions
        tracker_df.loc[mask, decision_field] = when
        tracker_df.loc[mask, flag_col]       = False

        row_after = tracker_df.loc[mask].iloc[0]

        both_approved = (
            str(row_after["james_status"]) == "Approved"
            and str(row_after["tim_status"]) == "Approved"
        )

        if both_approved:
            tracker_df.loc[mask, "Overall_status"] = "Approved"
            tracker_df.loc[mask, "Current Stage"]  = "MandateApproved"
        else:
            tracker_df.loc[mask, "Overall_status"] = "In progress"
            tracker_df.loc[mask, "Current Stage"]  = "MandateApproval"

    elif new_status == "Rejected":
        tracker_df.loc[mask, decision_field] = when
        tracker_df.loc[mask, flag_col]       = False
        tracker_df.loc[mask, "Overall_status"] = "Rejected"
        tracker_df.loc[mask, "Current Stage"]  = "MandateApproval"

    else:  # "Request Changes"
        tracker_df.loc[mask, decision_field] = ""
        # Decide whether to keep flag True or False. If True, it stays in Actions.
        tracker_df.loc[mask, flag_col]       = True
        tracker_df.loc[mask, "Overall_status"] = "Request Changes"
        tracker_df.loc[mask, "Current Stage"]  = "MandateApproval"

5) Persist Mandate tracker:

    _write_csv(tracker_df, MANDATE_TRACKER_PATH)

6) Optionally, mirror a simplified status into Mandate_view.csv
   (for list pages / summary dashboards). Only if Mandate_view has those columns:

    mandates_df = _read_csv(MANDATE_VIEW_PATH)
    if "Mandate_ID" in mandates_df.columns:
        ms = mandates_df["Mandate_ID"].astype(str) == mid
        if ms.any():
            mandates_df.loc[ms, "Current Stage"] = tracker_df.loc[mask, "Current Stage"].iloc[0]
            mandates_df.loc[ms, "Status"]        = tracker_df.loc[mask, "Overall_status"].iloc[0]
            _write_csv(mandates_df, MANDATE_VIEW_PATH)

7) Notifications and feedback logging can be copied from the Sanction flow,
   but with "Mandate" wording and Mandate_ID instead of Sanction_ID.

Summary
=======
- Mandate Approver Dashboard shows Actions per-approver (James/Tim) based on
  the Mandate_approver_tracker flags and statuses.
- Both James and Tim see the Mandate initially (flags True + status Pending).
- When one approves, that person’s flag becomes False, but the other still sees it.
- When both have Approved, Overall_status="Approved", Current Stage="MandateApproved",
  and the Mandate disappears from both Actions lists.
- If either Rejects, the Mandate is set to Overall_status="Rejected" and stops moving.

Please implement this behaviour end-to-end using the structures and rules above.
"""








#!/usr/bin/env python3
"""
CRM Query Tools — entry point.
Handles shared sidebar (AWS credentials) and page navigation.
"""

import json
from datetime import datetime, timezone

import boto3
import streamlit as st

st.set_page_config(
    page_title="CRM Query Tools",
    page_icon="🔍",
    layout="wide",
)

# ── Sidebar: AWS credentials (shown on every page) ────────────────────────────

with st.sidebar:
    st.title("🔑 AWS Credentials")
    st.markdown(
        """
Run these two commands in your terminal, then paste the output below:

```bash
aws sso login --profile hook-production-tic
```
```bash
aws configure export-credentials --profile hook-production-tic
```
        """
    )
    creds_json = st.text_area(
        "Paste credentials JSON",
        height=200,
        placeholder='{\n  "AccessKeyId": "ASIA...",\n  "SecretAccessKey": "...",\n  "SessionToken": "...",\n  "Expiration": "..."\n}',
    )
    region = st.text_input("AWS Region", value="eu-west-1")

    if creds_json.strip():
        try:
            raw = json.loads(creds_json)
            expired = False
            if expiry := raw.get("Expiration"):
                try:
                    expiry_dt = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
                    remaining = expiry_dt - datetime.now(timezone.utc)
                    total_secs = int(remaining.total_seconds())
                    if total_secs <= 0:
                        expired = True
                except Exception:
                    pass

            if expired:
                st.session_state.pop("aws_session", None)
                st.error("⛔ Credentials have expired — please re-auth and enter fresh credentials")
            else:
                st.session_state["aws_session"] = boto3.Session(
                    aws_access_key_id=raw["AccessKeyId"],
                    aws_secret_access_key=raw["SecretAccessKey"],
                    aws_session_token=raw.get("SessionToken"),
                    region_name=region,
                )
                st.success("✓ Credentials loaded")
                if expiry:
                    try:
                        hours, rem = divmod(total_secs, 3600)
                        minutes = rem // 60
                        if hours > 0:
                            label = f"⏱ Expires in {hours}h {minutes}m"
                        else:
                            label = f"⏱ Expires in {minutes}m"
                        if total_secs < 1800:
                            st.warning(label)
                        else:
                            st.caption(label)
                    except Exception:
                        st.caption(f"Expires: {expiry}")
        except Exception as exc:
            st.session_state.pop("aws_session", None)
            st.error(f"Invalid JSON: {exc}")

# ── Navigation ────────────────────────────────────────────────────────────────

pg = st.navigation([
    st.Page("pages/CRM_Tools.py", title="CRM Query Tools", icon="🔍"),
    st.Page("pages/Help.py", title="Help & Documentation", icon="❓"),
])
pg.run()

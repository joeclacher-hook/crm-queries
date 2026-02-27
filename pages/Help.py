import streamlit as st

st.set_page_config(page_title="Help — CRM Query Tools", page_icon="❓", layout="wide")

st.title("❓ Help & Documentation")

# ── Getting started ────────────────────────────────────────────────────────────

st.header("Getting started")
st.markdown("""
Every session you need to provide fresh AWS credentials. This takes about 10 seconds.

**Step 1** — Log in to AWS SSO (once per day, or when your session expires):
```bash
aws sso login --profile hook-production-tic
```

**Step 2** — Export your temporary credentials:
```bash
aws configure export-credentials --profile hook-production-tic
```

**Step 3** — Copy the entire JSON block that prints to your terminal and paste it into the
**AWS Credentials** box in the sidebar on the main page.

> Credentials typically last **8–12 hours**. If you get an auth error mid-session, repeat steps 1–2.
""")

st.divider()

# ── Query types ────────────────────────────────────────────────────────────────

st.header("Query types")

col1, col2 = st.columns(2)

with col1:
    st.subheader("🟠 HubSpot")
    st.markdown("""
| Type | What it does |
|------|-------------|
| **count** | Returns the total number of records for the object |
| **list** | Returns ID + default properties (up to your limit) |
| **all** | Returns every property for every record — exports to Excel |
| **shape** | Returns all property names, types, and labels — exports to Excel |
| **search** | Filters records using the JSON filter rules you define |

**Standard objects:** contacts, companies, deals, tickets, line_items, products,
quotes, calls, emails, meetings, notes, tasks, communications

**Search filter operators:**
`EQ`, `NEQ`, `LT`, `LTE`, `GT`, `GTE`, `HAS_PROPERTY`, `NOT_HAS_PROPERTY`,
`CONTAINS_TOKEN`, `NOT_CONTAINS_TOKEN`, `IN`, `NOT_IN`, `BETWEEN`

**Search filter example:**
```json
[
  {"propertyName": "lifecyclestage", "operator": "EQ", "value": "customer"},
  {"propertyName": "email", "operator": "CONTAINS_TOKEN", "value": "@example.com"}
]
```
All filters in the list are ANDed together.
    """)

with col2:
    st.subheader("🔵 Salesforce")
    st.markdown("""
| Type | What it does |
|------|-------------|
| **count** | `SELECT COUNT() FROM Object` — total record count |
| **list** | `SELECT Id, Name FROM Object LIMIT 20` |
| **all** | `SELECT FIELDS(ALL)` — every field, exports to Excel (max 200 records) |
| **shape** | All field names, data types, and labels — exports to Excel |
| **custom** | Write and run any SOQL query you like |

**Custom SOQL examples:**
```sql
SELECT Id, Name, StageName, Amount
FROM Opportunity
WHERE CloseDate = THIS_QUARTER
```
```sql
SELECT Account.Name, COUNT(Id)
FROM Contact
GROUP BY Account.Name
```

> The **all** query type is capped at 200 records by Salesforce's `FIELDS(ALL)` limit.
For larger datasets use **custom** with explicit field names.
    """)

st.divider()

# ── Discover mode ──────────────────────────────────────────────────────────────

st.header("Discover mode")
st.markdown("""
Available on both HubSpot and Salesforce tabs. Instead of querying a specific object,
it lists **all available objects** with their record counts.

- Use the **filter box** to search by object name — e.g. type `asset` to find all asset-related objects
- Leave the filter blank to list everything (can be slow on large orgs — use a filter when possible)
- HubSpot discover shows standard objects + any custom schemas
- Salesforce discover shows all sObjects; non-queryable ones show N/A instead of a count
""")

st.divider()

# ── Rate limits ────────────────────────────────────────────────────────────────

st.header("⚠️ Rate limit protections")
st.markdown("""
The tool has two built-in protections to avoid hammering customer APIs:

**Request timeout — 10 seconds**
Every outbound API call (to HubSpot, Salesforce, or AWS) times out after 10 seconds.
If a call hangs due to a slow response or network issue, it fails cleanly rather than blocking forever.

**Discover mode delay — 100ms between COUNT calls**
When scanning objects in discover mode, the tool waits 100ms between each COUNT query.
This prevents firing hundreds of queries in a burst when a customer has many objects.

**Tips to reduce API load:**
- Always use the **filter box** in discover mode — counting 5 matching objects is much lighter than counting all 300
- Use **count** before **all** to check the size of a dataset before pulling everything
- Keep record limits reasonable — fetching 10,000 records on a wide object will be slow and expensive
""")

st.divider()

# ── Common errors ──────────────────────────────────────────────────────────────

st.header("🔴 Common errors")
st.markdown("""
| Error | Likely cause | Fix |
|-------|-------------|-----|
| `Invalid JSON` | Pasted credentials are malformed or incomplete | Re-run the export command and paste fresh |
| `400 Bad Request` on HubSpot OAuth | Wrong secret path for this tab — e.g. a Salesforce secret path entered in the HubSpot tab | Check the customer name and confirm which tab you're on |
| `instance_url not found` | The Salesforce secret in AWS doesn't contain an instance URL | Check the secret in AWS Secrets Manager |
| `No usable token found` | HubSpot secret doesn't contain a recognised token field | Secret needs one of: `access_token`, `api_key`, `hapikey`, or OAuth fields (`client_id` + `client_secret` + `refresh_token`) |
| `ExpiredTokenException` | Your AWS session has expired | Re-run `aws sso login` then `aws configure export-credentials` |
| `Timeout` | An API call took longer than 10 seconds | Try again — if it keeps happening the customer's API may be having issues |
| `ResourceNotFoundException` | The secret path doesn't exist in AWS Secrets Manager | Double-check the customer name spelling |
""")

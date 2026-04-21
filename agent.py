#!/usr/bin/env python3
"""
Skylark PM Watch Agent
Monitors Basecamp via REST API and posts alerts to Slack #pm-watch
Designed to run as a Google Cloud Run Job.
"""

import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import anthropic
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

ACCOUNT_ID = "4358663"
BC_BASE = f"https://3.basecampapi.com/{ACCOUNT_ID}"
TOKEN_ENDPOINT = "https://launchpad.37signals.com/authorization/token"
USER_AGENT = "Skylark PM Agent (tyler@skylarkav.com)"

SCRIPT_DIR = Path(__file__).parent
STATE_FILE = SCRIPT_DIR / "state.json"
ENV_FILE = SCRIPT_DIR / ".env"


# ── Env / secrets ──────────────────────────────────────────────────────────────

def load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def load_secrets_from_gcp():
    """Pull secrets from Google Secret Manager when running on Cloud Run."""
    try:
        from google.cloud import secretmanager
    except ImportError:
        return

    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "skylark-pm-agents")
    client = secretmanager.SecretManagerServiceClient()

    secret_names = [
        "BC_ACCESS_TOKEN", "BC_REFRESH_TOKEN", "BC_CLIENT_ID",
        "BC_CLIENT_SECRET", "BC_TOKEN_EXPIRES_AT",
        "SLACK_TOKEN", "SLACK_CHANNEL_ID", "ANTHROPIC_API_KEY",
    ]

    for name in secret_names:
        if os.environ.get(name):
            continue
        try:
            path = f"projects/{project}/secrets/{name}/versions/latest"
            resp = client.access_secret_version(request={"name": path})
            os.environ[name] = resp.payload.data.decode("utf-8")
        except Exception as e:
            print(f"Warning: could not load secret {name}: {e}")


# ── State ──────────────────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_run": None}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Basecamp token refresh ─────────────────────────────────────────────────────

def token_needs_refresh():
    expires_at_str = os.environ.get("BC_TOKEN_EXPIRES_AT", "")
    if not expires_at_str:
        return True
    try:
        expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        # Refresh if less than 2 days remaining
        return (expires_at - now).total_seconds() < 172800
    except Exception:
        return True


def refresh_bc_token():
    print("Refreshing Basecamp access token...")
    data = urllib.parse.urlencode({
        "type": "refresh",
        "client_id": os.environ["BC_CLIENT_ID"],
        "client_secret": os.environ["BC_CLIENT_SECRET"],
        "refresh_token": os.environ["BC_REFRESH_TOKEN"],
    }).encode()

    req = urllib.request.Request(
        TOKEN_ENDPOINT, data=data, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as resp:
        tokens = json.loads(resp.read())

    os.environ["BC_ACCESS_TOKEN"] = tokens["access_token"]
    expires_in = tokens.get("expires_in", 1209600)
    now = datetime.now(timezone.utc)
    expires_at = now.replace(second=0, microsecond=0)
    expires_at = expires_at.timestamp() + expires_in
    expires_at_str = datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()
    os.environ["BC_TOKEN_EXPIRES_AT"] = expires_at_str

    # Update secret in GCP if running in cloud
    try:
        from google.cloud import secretmanager
        project = os.environ.get("GOOGLE_CLOUD_PROJECT", "skylark-pm-agents")
        client = secretmanager.SecretManagerServiceClient()
        for secret_name, value in [
            ("BC_ACCESS_TOKEN", tokens["access_token"]),
            ("BC_TOKEN_EXPIRES_AT", expires_at_str),
        ]:
            parent = f"projects/{project}/secrets/{secret_name}"
            client.add_secret_version(
                request={"parent": parent, "payload": {"data": value.encode()}}
            )
        print("Updated tokens in Secret Manager")
    except Exception as e:
        print(f"Note: could not update Secret Manager: {e}")


# ── Basecamp API ───────────────────────────────────────────────────────────────

def bc_get(path, params=None):
    url = f"{BC_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {os.environ['BC_ACCESS_TOKEN']}",
        "User-Agent": USER_AGENT,
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"BC API error {e.code} on {path}")
        return None
    except Exception as e:
        print(f"BC API error on {path}: {e}")
        return None


def fetch_basecamp_data(last_run):
    print("Fetching Basecamp data...")

    # Recent account-wide events
    events_params = {"page": 1}
    if last_run:
        events_params["since"] = last_run
    events = bc_get("/events.json", events_params)

    notifications = bc_get("/notifications.json")
    projects = bc_get("/projects.json")

    active_projects = []
    schedule_entries = []
    labor_todos = []
    schedule_tagged_todos = []

    if projects:
        # Focus on SKY- jobs that are not obviously LOI/closed
        sky_projects = [
            p for p in projects
            if p.get("name", "").startswith("SKY-") and p.get("status") == "active"
        ]

        for proj in sky_projects[:25]:
            proj_id = proj["id"]
            proj_name = proj["name"]
            desc = proj.get("description", "")

            active_projects.append({
                "id": proj_id,
                "name": proj_name,
                "description": desc,
            })

            # Pull todosets to find schedule-tagged and labor todos
            todosets = bc_get(f"/projects/{proj_id}/todosets.json")
            if todosets:
                for tset in todosets[:3]:
                    tset_id = tset.get("id")
                    todolists = bc_get(f"/projects/{proj_id}/todolists.json")
                    if not todolists:
                        continue
                    for tlist in todolists[:10]:
                        tlist_id = tlist.get("id")
                        tlist_name = tlist.get("name", "")
                        todos = bc_get(f"/projects/{proj_id}/todos.json", {"todolist_id": tlist_id, "completed": "false"})
                        if not todos:
                            continue
                        for todo in todos[:20]:
                            title = todo.get("content", "")
                            due = todo.get("due_on")
                            # Collect schedule-tagged todos (phase logic)
                            if any(tag in title for tag in [
                                "[PM-SCHED]", "[ENG-SCHED]", "[PROC-SCHED]",
                                "[SHOP-SCHED]", "[LOG-SCHED]", "[ONS-SCHED]",
                                "[COM-SCHED]", "[FUT-SCHED]"
                            ]):
                                schedule_tagged_todos.append({
                                    "project": proj_name,
                                    "project_id": proj_id,
                                    "title": title,
                                    "due_on": due,
                                    "assignees": [a.get("name") for a in todo.get("assignees", [])],
                                    "app_url": todo.get("app_url"),
                                })
                            # Collect labor todos
                            if "[LABOR]" in title:
                                labor_todos.append({
                                    "project": proj_name,
                                    "title": title,
                                    "due_on": due,
                                    "description": todo.get("description", ""),
                                    "app_url": todo.get("app_url"),
                                })

            # Schedule entries
            dock = proj.get("dock", [])
            sched_tool = next((d for d in dock if d["name"] == "schedule"), None)
            if sched_tool and sched_tool.get("enabled"):
                entries = bc_get(f"/projects/{proj_id}/schedule_entries.json")
                if entries:
                    for e in entries[:5]:
                        e["_project_name"] = proj_name
                    schedule_entries.extend(entries[:5])

    return {
        "recent_events": events,
        "notifications": notifications,
        "active_projects": active_projects[:30],
        "schedule_tagged_todos": schedule_tagged_todos[:60],
        "labor_todos": labor_todos[:30],
        "upcoming_schedule_entries": schedule_entries[:30],
        "last_run": last_run,
    }


# ── Claude analysis ────────────────────────────────────────────────────────────

SKYLARK_SOP_CONTEXT = """
## Skylark AV Operations Standards

### Basecamp Project Structure (Required)
Every SKY- project must have these fields in the Project Description:
- Client Contact: [Name / Title / Email / Phone]
- Job Location: [Address]
- Skylark PM: [Name]
- Engineer: [Name]
- On-Site Lead: [Name]
TBD is acceptable ONLY for LOI-phase (Letter of Intent) projects. Active jobs must have real names.

### Schedule Tag System
Todos in Basecamp use tags to indicate phase. Only todos with these tags and a due_on date activate a phase:
[PM-SCHED] = Project Management milestones
[ENG-SCHED] = Engineering milestones
[PROC-SCHED] = Procurement milestones
[SHOP-SCHED] = Shop/Rack Build milestones
[LOG-SCHED] = Logistics milestones
[ONS-SCHED] = Onsite/Install milestones
[COM-SCHED] = Commissioning milestones
[FUT-SCHED] = Sales/Forecast

CRITICAL: Any incomplete schedule-tagged todo WITHOUT a due date is a "missing_dates" flag — it means the project schedule is broken and needs PM attention.

### Key Milestone Timing (relative to onsite date)
- 25% Design Basis locked [ENG-SCHED]: 16-18 weeks before onsite
- 50% Design Review / Order Ready [ENG-SCHED]: 13 weeks before onsite
- Handoff to Procurement (Long Lead) [ENG-SCHED]: 12 weeks before onsite
- 75% Design Docs [ENG-SCHED]: 7 weeks before onsite
- Handoff to Procurement (Short Lead) [ENG-SCHED]: 6 weeks before onsite
- Cable order [PROC-SCHED]: 4 weeks before onsite
- Rack Build complete [SHOP-SCHED]: 2 weeks before onsite
- Verify Equipment/Materials [LOG-SCHED]: 2 weeks before onsite
- Punch List Walkthrough [PM-SCHED]: 48 hours before end of install
- Client Sign-Off [PM-SCHED]: before pulling off job
- As-Built Package Delivered [ENG-SCHED]: 2 weeks after open
- Internal Post-Mortem [PM-SCHED]: 1 week after open
- Project Closed in Basecamp [PM-SCHED]: 90 days after open

### Labor Scheduling SOP
Each field tech assignment gets a [LABOR] todo with title format: `Name | Role | Status [LABOR]`
The todo description must have flights, hotel, per-diem, and car info filled in.
Missing travel details on a [LABOR] todo for an upcoming trip is a flag.

### Pre-Mobilization Gate
A GO/NO-GO check must happen 14 days AND 7 days before mobilization.
If a project has an [ONS-SCHED] todo coming up in <14 days with no evidence of GO/NO-GO, flag it.

### Communication Rules
- Client communication → "Client Communication" message board only
- Internal updates → "Internal Coordination" message board only
- All scheduling → To-Dos (not messages)
- Decisions/actions from calls must be logged in Basecamp

### Phase Logic (Mission Control)
A project is in closeout when "Client First Open [PM-SCHED]" has passed but "Project Closed in Basecamp [PM-SCHED]" is not complete.
Projects in closeout > 90 days without closure are overdue.
"""

def analyze_with_claude(anthropic_client, data_bundle, last_run):
    since_str = last_run if last_run else "the past hour"

    prompt = f"""You are a PM watch agent for Skylark AV, an AV installation company that designs and installs audio, video, and lighting systems for large churches and venues.

Tyler is the founder/owner. Your job is to review Basecamp data and flag items that need his attention based on Skylark's exact operations standards below.

{SKYLARK_SOP_CONTEXT}

---

Review the data below (activity since {since_str}) and flag these specific issues:

1. **Upset / Frustrated** — tense or frustrated tone in messages, comments, or notifications from clients, PMs, or field staff. Look especially at Client Communication boards.

2. **Missing Dates (Critical)** — any incomplete schedule-tagged todo ([XXXX-SCHED]) with no due_on date. This breaks Mission Control phase logic. Flag each project affected.

3. **SOP Deviation** — examples:
   - Active (non-LOI) project with Engineer = TBD or On-Site Lead = TBD in description
   - [LABOR] todo for an upcoming trip missing flight/hotel/car details
   - Project missing required description fields (Client Contact, Job Location, PM, Engineer, On-Site Lead)
   - Schedule items or decisions that appear to be in wrong message board

4. **Schedule Risk** — examples:
   - [ONS-SCHED] install trip coming up within 14 days with no GO/NO-GO evidence
   - Rack Build [SHOP-SCHED] due date is <2 weeks before onsite but not yet complete
   - Procurement milestones missed relative to onsite date
   - Onsite or commissioning schedule entry with no participants assigned

5. **Communication Gap** — questions in messages or comments with no response for 24+ hours, especially on Client Communication boards.

6. **Closeout Overdue** — project where "Client First Open" has passed but "Project Closed in Basecamp" is still open and it has been > 90 days.

Be specific and actionable. Reference the exact todo title, project name, and timing when relevant.
Do NOT flag LOI-phase projects for TBD fields. Do NOT flag normal routine activity.

Return ONLY a JSON array. Each object:
{{
  "category": "Upset Team Member | Missing Dates | SOP Deviation | Schedule Risk | Communication Gap | Closeout Overdue",
  "severity": "high | medium | low",
  "description": "1-2 sentences. Be specific: name the project, todo, or person involved.",
  "url": "basecamp app_url if available, else null",
  "project": "SKY-XXXX project name"
}}

If nothing needs attention, return [].

--- DATA ---
{json.dumps(data_bundle, indent=2)[:22000]}
"""

    response = anthropic_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        print(f"Could not parse Claude response:\n{text}")
        return []


# ── Slack ──────────────────────────────────────────────────────────────────────

def post_to_slack(slack_client, channel_id, alerts):
    now = datetime.now().strftime("%b %d, %I:%M %p")
    severity_emoji = {"high": ":red_circle:", "medium": ":large_yellow_circle:", "low": ":large_blue_circle:"}

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"PM Watch  —  {now}"}},
        {"type": "divider"},
    ]

    for alert in alerts:
        emoji = severity_emoji.get(alert.get("severity", "low"), ":large_blue_circle:")
        project_line = f"\n_Project: {alert['project']}_" if alert.get("project") else ""
        text = f"{emoji}  *{alert['category']}*{project_line}\n{alert['description']}"
        section = {"type": "section", "text": {"type": "mrkdwn", "text": text}}
        if alert.get("url"):
            section["accessory"] = {
                "type": "button",
                "text": {"type": "plain_text", "text": "Open in Basecamp"},
                "url": alert["url"],
            }
        blocks.append(section)
        blocks.append({"type": "divider"})

    slack_client.chat_postMessage(
        channel=channel_id,
        blocks=blocks,
        text=f"PM Watch: {len(alerts)} alert(s) need your attention",
    )


# ── Core analysis (used by both scheduled job and webhook) ─────────────────────

def run_analysis(on_demand=False):
    """Run a full Basecamp analysis. Returns (alerts, last_run)."""
    load_env()
    load_secrets_from_gcp()

    for var in ["BC_ACCESS_TOKEN", "BC_REFRESH_TOKEN", "BC_CLIENT_ID",
                "BC_CLIENT_SECRET", "SLACK_TOKEN", "SLACK_CHANNEL_ID", "ANTHROPIC_API_KEY"]:
        if not os.environ.get(var):
            raise RuntimeError(f"{var} not set")

    if token_needs_refresh():
        refresh_bc_token()

    state = load_state()
    # On-demand runs look back 24 hours regardless of last scheduled run
    last_run = None if on_demand else state.get("last_run")
    print(f"Running PM Watch... (last run: {last_run or 'never'}, on_demand={on_demand})")

    data_bundle = fetch_basecamp_data(last_run)

    print("Analyzing with Claude...")
    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    alerts = analyze_with_claude(anthropic_client, data_bundle, last_run)
    print(f"Found {len(alerts)} alert(s)")

    if not on_demand:
        state["last_run"] = datetime.now(timezone.utc).isoformat()
        save_state(state)

    return alerts


# ── Main (scheduled Cloud Run Job) ─────────────────────────────────────────────

def main():
    try:
        alerts = run_analysis(on_demand=False)
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    if alerts:
        slack_client = WebClient(token=os.environ["SLACK_TOKEN"])
        try:
            post_to_slack(slack_client, os.environ["SLACK_CHANNEL_ID"], alerts)
            print(f"Posted {len(alerts)} alert(s) to Slack")
        except SlackApiError as e:
            print(f"Slack error: {e.response['error']}")
    else:
        print("All clear — nothing to post")

    print("Done.")


if __name__ == "__main__":
    main()

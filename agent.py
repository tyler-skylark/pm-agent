#!/usr/bin/env python3
"""
Skylark PM Watch Agent
Features: hourly alerts, morning briefing, deep dive, deduplication, stale detection
"""

import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
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

SCHED_TAGS = ["[PM-SCHED]", "[ENG-SCHED]", "[PROC-SCHED]", "[SHOP-SCHED]",
              "[LOG-SCHED]", "[ONS-SCHED]", "[COM-SCHED]", "[FUT-SCHED]"]


# ── Env / secrets ──────────────────────────────────────────────────────────────

def load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def load_secrets_from_gcp():
    try:
        from google.cloud import secretmanager
    except ImportError:
        return
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "skylark-pm-agents")
    client = secretmanager.SecretManagerServiceClient()
    for name in ["BC_ACCESS_TOKEN", "BC_REFRESH_TOKEN", "BC_CLIENT_ID",
                 "BC_CLIENT_SECRET", "BC_TOKEN_EXPIRES_AT",
                 "SLACK_TOKEN", "SLACK_CHANNEL_ID", "ANTHROPIC_API_KEY",
                 "SLACK_SIGNING_SECRET"]:
        if os.environ.get(name):
            continue
        try:
            path = f"projects/{project}/secrets/{name}/versions/latest"
            resp = client.access_secret_version(request={"name": path})
            os.environ[name] = resp.payload.data.decode("utf-8")
        except Exception:
            pass


# ── State ──────────────────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_run": None, "seen_alerts": {}}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Token refresh ──────────────────────────────────────────────────────────────

def token_needs_refresh():
    expires_at_str = os.environ.get("BC_TOKEN_EXPIRES_AT", "")
    if not expires_at_str:
        return True
    try:
        expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        return (expires_at - datetime.now(timezone.utc)).total_seconds() < 172800
    except Exception:
        return True


def refresh_bc_token():
    print("Refreshing Basecamp token...")
    data = urllib.parse.urlencode({
        "type": "refresh",
        "client_id": os.environ["BC_CLIENT_ID"],
        "client_secret": os.environ["BC_CLIENT_SECRET"],
        "refresh_token": os.environ["BC_REFRESH_TOKEN"],
    }).encode()
    req = urllib.request.Request(TOKEN_ENDPOINT, data=data, method="POST",
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req) as resp:
        tokens = json.loads(resp.read())
    os.environ["BC_ACCESS_TOKEN"] = tokens["access_token"]
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=tokens.get("expires_in", 1209600))).isoformat()
    os.environ["BC_TOKEN_EXPIRES_AT"] = expires_at
    try:
        from google.cloud import secretmanager
        project = os.environ.get("GOOGLE_CLOUD_PROJECT", "skylark-pm-agents")
        client = secretmanager.SecretManagerServiceClient()
        for name, value in [("BC_ACCESS_TOKEN", tokens["access_token"]), ("BC_TOKEN_EXPIRES_AT", expires_at)]:
            client.add_secret_version(
                request={"parent": f"projects/{project}/secrets/{name}",
                         "payload": {"data": value.encode()}})
    except Exception:
        pass


# ── Basecamp API ───────────────────────────────────────────────────────────────

def bc_get(path, params=None, retries=2):
    url = path if path.startswith("http") else f"{BC_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {os.environ['BC_ACCESS_TOKEN']}",
            "User-Agent": USER_AGENT,
        })
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read()), resp.headers.get("Link", "")
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** attempt)
                continue
            return None, ""
        except Exception:
            return None, ""
    return None, ""


def bc_get_data(path, params=None):
    """Fetch a single page, return just the data."""
    data, _ = bc_get(path, params)
    return data


def bc_get_all(path, params=None, max_pages=10):
    """Fetch all pages of a paginated BC3 endpoint."""
    import re
    results = []
    url = (path if path.startswith("http") else f"{BC_BASE}{path}")
    if params:
        url += "?" + urllib.parse.urlencode(params)

    for _ in range(max_pages):
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {os.environ['BC_ACCESS_TOKEN']}",
            "User-Agent": USER_AGENT,
        })
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
                link_header = resp.headers.get("Link", "")
        except Exception:
            break

        if isinstance(data, list):
            results.extend(data)
        else:
            return data  # not a list, just return as-is

        # Parse next page from Link header
        next_url = None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                match = re.search(r'<([^>]+)>', part)
                if match:
                    next_url = match.group(1)
                    break
        if not next_url:
            break
        url = next_url

    return results


def get_dock_tool(project, tool_name):
    dock = project.get("dock", [])
    return next((d for d in dock if d["name"] == tool_name and d.get("enabled")), None)


# ── Data fetching ──────────────────────────────────────────────────────────────

def fetch_active_sky_projects(projects):
    return [
        p for p in (projects or [])
        if p.get("name", "").startswith("SKY-") and p.get("status") == "active"
    ]


def fetch_todos_for_project(proj):
    proj_id = proj["id"]
    proj_name = proj["name"]
    todoset_tool = get_dock_tool(proj, "todoset")
    if not todoset_tool:
        return [], []

    todoset_id = todoset_tool["id"]
    todolists = bc_get_data(f"/buckets/{proj_id}/todosets/{todoset_id}/todolists.json")
    if not todolists:
        return [], []

    schedule_todos, labor_todos = [], []
    for tlist in todolists[:12]:
        list_id = tlist["id"]
        todos = bc_get_data(f"/buckets/{proj_id}/todolists/{list_id}/todos.json",
                       {"completed": "false"})
        for todo in (todos or [])[:25]:
            title = todo.get("content", "")
            due = todo.get("due_on")
            assignees = [a.get("name") for a in todo.get("assignees", [])]
            entry = {
                "project": proj_name,
                "project_id": proj_id,
                "title": title,
                "due_on": due,
                "assignees": assignees,
                "app_url": todo.get("app_url"),
                "description": (todo.get("description") or "")[:300],
            }
            if any(tag in title for tag in SCHED_TAGS):
                schedule_todos.append(entry)
            if "[LABOR]" in title:
                labor_todos.append(entry)

    return schedule_todos, labor_todos


def fetch_messages_for_project(proj, since=None):
    proj_id = proj["id"]
    proj_name = proj["name"]
    board_tool = get_dock_tool(proj, "message_board")
    if not board_tool:
        return []

    board_id = board_tool["id"]
    messages = bc_get_data(f"/buckets/{proj_id}/message_boards/{board_id}/messages.json")
    result = []
    for msg in (messages or [])[:8]:
        created = msg.get("created_at", "")
        if since and created < since:
            continue
        content = (msg.get("content") or "")
        # Strip HTML tags roughly
        import re
        content = re.sub(r'<[^>]+>', ' ', content).strip()[:600]
        entry = {
            "project": proj_name,
            "type": "message",
            "board": board_tool.get("title", ""),
            "title": msg.get("subject"),
            "content": content,
            "author": (msg.get("creator") or {}).get("name"),
            "created_at": created,
            "app_url": msg.get("app_url"),
        }
        result.append(entry)

        # Fetch comments on this message
        msg_id = msg.get("id")
        comments = bc_get_data(f"/buckets/{proj_id}/recordings/{msg_id}/comments.json")
        for comment in (comments or [])[:6]:
            c_created = comment.get("created_at", "")
            if since and c_created < since:
                continue
            c_content = re.sub(r'<[^>]+>', ' ', (comment.get("content") or "")).strip()[:300]
            result.append({
                "project": proj_name,
                "type": "comment",
                "parent_title": msg.get("subject"),
                "content": c_content,
                "author": (comment.get("creator") or {}).get("name"),
                "created_at": c_created,
                "app_url": msg.get("app_url"),
            })
    return result


def fetch_basecamp_data(last_run=None, mode="analysis", project_query=None):
    print(f"Fetching Basecamp data (mode={mode})...")

    events_params = {"page": 1}
    if last_run and mode == "analysis":
        events_params["since"] = last_run
    recent_events = bc_get_data("/events.json", events_params) or []

    notifications = bc_get_data("/notifications.json") if mode == "analysis" else []
    projects = bc_get_all("/projects.json") or []
    sky_projects = fetch_active_sky_projects(projects)

    # Deep dive: search all SKY- projects (regardless of status)
    if mode == "deep_dive" and project_query:
        query_upper = project_query.upper()
        all_sky = [p for p in projects if p.get("name", "").upper().startswith("SKY-")]
        matched = [p for p in all_sky if query_upper in p["name"].upper()][:1]
        if not matched:
            return {"error": f"No project found matching {project_query}"}
        sky_projects = matched

    # Stale detection: projects with no events in the feed
    active_project_ids_in_events = set()
    for event in recent_events:
        bucket = event.get("bucket") or {}
        if bucket.get("id"):
            active_project_ids_in_events.add(bucket["id"])

    stale_projects = []
    for proj in sky_projects:
        if "(LOI)" in proj["name"] or "(Design)" in proj["name"]:
            continue
        if proj["id"] not in active_project_ids_in_events:
            stale_projects.append({
                "project": proj["name"],
                "description": proj.get("description", "")[:200],
            })

    # Per-project data
    all_schedule_todos, all_labor_todos, all_messages = [], [], []
    project_summaries = []
    limit = 1 if mode == "deep_dive" else 20

    for proj in sky_projects[:limit]:
        desc = proj.get("description", "")
        project_summaries.append({
            "id": proj["id"],
            "name": proj["name"],
            "description": desc[:300],
        })

        since_filter = last_run if mode == "analysis" else None
        sched_todos, labor_todos = fetch_todos_for_project(proj)
        messages = fetch_messages_for_project(proj, since=since_filter)

        all_schedule_todos.extend(sched_todos)
        all_labor_todos.extend(labor_todos)
        all_messages.extend(messages)

    # Upcoming schedule entries (briefing/deep dive)
    schedule_entries = []
    if mode in ("briefing", "deep_dive"):
        for proj in sky_projects[:limit]:
            sched_tool = get_dock_tool(proj, "schedule")
            if sched_tool:
                entries = bc_get_data(f"/buckets/{proj['id']}/schedules/{sched_tool['id']}/entries.json")
                for e in (entries or [])[:5]:
                    e["_project_name"] = proj["name"]
                schedule_entries.extend((entries or [])[:5])

    return {
        "mode": mode,
        "recent_events": recent_events[:60] if mode == "analysis" else [],
        "notifications": (notifications or [])[:30],
        "project_summaries": project_summaries,
        "schedule_tagged_todos": all_schedule_todos[:80],
        "labor_todos": all_labor_todos[:40],
        "messages_and_comments": all_messages[:80],
        "stale_projects": stale_projects[:20],
        "upcoming_schedule_entries": schedule_entries[:30],
        "last_run": last_run,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }


# ── Alert deduplication ────────────────────────────────────────────────────────

def alert_fingerprint(alert):
    key = f"{alert.get('project','')}-{alert.get('category','')}-{alert.get('description','')[:60]}"
    return hashlib.md5(key.encode()).hexdigest()


def deduplicate_alerts(alerts, state):
    seen = state.get("seen_alerts", {})
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    seen = {k: v for k, v in seen.items() if v > cutoff}

    new_alerts = []
    for alert in alerts:
        fp = alert_fingerprint(alert)
        if fp not in seen:
            new_alerts.append(alert)
            seen[fp] = datetime.now(timezone.utc).isoformat()

    state["seen_alerts"] = seen
    return new_alerts, state


# ── SOP context ────────────────────────────────────────────────────────────────

SKYLARK_SOP_CONTEXT = """
## Skylark AV Operations Standards

### Project Description (Required Fields)
Every active SKY- project must have exactly these 5 fields in its description:
- Client Contact
- Job Location
- Skylark PM
- Engineer
- On-Site Lead
No dates are expected in the description. Dates live in schedule-tagged todos only.
TBD is only acceptable for LOI-phase or Design-only projects.

### Schedule Tag System
[PM-SCHED] [ENG-SCHED] [PROC-SCHED] [SHOP-SCHED] [LOG-SCHED] [ONS-SCHED] [COM-SCHED] [FUT-SCHED]
CRITICAL: Any incomplete schedule-tagged todo WITHOUT a due_on date = missing_dates flag.

### Key Milestone Timing (relative to onsite)
- 25% Design Basis [ENG-SCHED]: 16-18 weeks before onsite
- 50% Design Review / Order Ready [ENG-SCHED]: 13 weeks before onsite
- Handoff to Procurement Long Lead [ENG-SCHED]: 12 weeks before onsite
- 75% Design Docs [ENG-SCHED]: 7 weeks before onsite
- Handoff to Procurement Short Lead [ENG-SCHED]: 6 weeks before onsite
- Cable order [PROC-SCHED]: 4 weeks before onsite
- Rack Build complete [SHOP-SCHED]: 2 weeks before onsite
- Verify Equipment [LOG-SCHED]: 2 weeks before onsite
- Punch List Walkthrough [PM-SCHED]: 48 hours before end of install
- Client Sign-Off [PM-SCHED]: before pulling off job
- As-Built Package [ENG-SCHED]: 2 weeks after open
- Post-Mortem [PM-SCHED]: 1 week after open
- Project Closed [PM-SCHED]: 90 days after open

### Labor Scheduling
[LABOR] todos: format = "Name | Role | Status [LABOR]"
Description must have Flights, Hotel, Per-Diem, Car Rental filled in.
Missing travel info on an upcoming trip = flag.

### Pre-Mobilization Gate
GO/NO-GO check required 14 days AND 7 days before mobilization.
No evidence of GO/NO-GO with [ONS-SCHED] due in <14 days = flag.

### Communication Rules
- Client posts → "Client Communication" board only
- Internal updates → "Internal Coordination" board only
- Decisions/actions from calls must be logged in Basecamp

### Closeout
Project is overdue for closure if "Client First Open [PM-SCHED]" passed >90 days ago
and "Project Closed in Basecamp [PM-SCHED]" is still incomplete.
"""


# ── Claude analysis ────────────────────────────────────────────────────────────

def analyze_with_claude(anthropic_client, data_bundle):
    mode = data_bundle.get("mode", "analysis")
    last_run = data_bundle.get("last_run")
    since_str = last_run or "the past hour"

    if mode == "briefing":
        prompt = f"""You are the PM Watch agent for Skylark AV. Generate a morning briefing for Tyler (founder/owner).

{SKYLARK_SOP_CONTEXT}

Today is {data_bundle['as_of'][:10]}.

Review all active project data below and produce a clear morning briefing in Slack markdown.

Format:
- Start with a one-line summary count (e.g. "12 active jobs — 3 need attention")
- List jobs needing action first (with specific issue)
- Then jobs that are all-clear (just name + current phase)
- End with a "This Week" section: key milestones due in the next 7 days across all jobs

Use :red_circle: for high issues, :large_yellow_circle: for medium, :white_check_mark: for clear.
Be specific — name the todo, the person, the date.

--- DATA ---
{json.dumps(data_bundle, indent=2)[:22000]}
"""
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6", max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        return {"type": "briefing", "text": response.content[0].text.strip()}

    elif mode == "deep_dive":
        project_name = (data_bundle.get("project_summaries") or [{}])[0].get("name", "Unknown")
        prompt = f"""You are the PM Watch agent for Skylark AV. Give Tyler a full status report on {project_name}.

{SKYLARK_SOP_CONTEXT}

As of {data_bundle['as_of'][:10]}.

Cover:
1. Project description fields (PM, Engineer, On-Site Lead, Client Contact)
2. Current active phase (based on incomplete schedule-tagged todos with due dates)
3. Upcoming milestones (next 30 days)
4. Labor/travel status (any [LABOR] todos and their travel details)
5. Recent messages/comments — tone, open questions, anything unresolved
6. Any SOP violations or flags
7. Overall health: GREEN / YELLOW / RED with one-line reason

Use Slack markdown. Be specific and concise.

--- DATA ---
{json.dumps(data_bundle, indent=2)[:22000]}
"""
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6", max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        return {"type": "deep_dive", "project": project_name, "text": response.content[0].text.strip()}

    else:
        # Standard hourly/on-demand alert analysis
        prompt = f"""You are the PM Watch agent for Skylark AV. Review Basecamp activity since {since_str} and flag issues needing Tyler's attention.

{SKYLARK_SOP_CONTEXT}

Flag these issues:
1. **Upset / Frustrated** — tense tone in messages or comments (read the actual content)
2. **Missing Dates** — incomplete [XXXX-SCHED] todo with no due_on date (breaks phase logic)
3. **SOP Deviation** — wrong board, TBD fields on active jobs, [LABOR] missing travel info, no GO/NO-GO before install
4. **Schedule Risk** — milestones overdue relative to onsite dates, install <14 days with no GO/NO-GO
5. **Communication Gap** — client or team question unanswered for 24+ hours
6. **Stale Project** — active-phase project with zero recent Basecamp activity
7. **Closeout Overdue** — Client First Open passed >90 days ago, project not closed

Only flag real issues. Skip LOI/Design-only projects for TBD fields.

Return ONLY a JSON array:
[{{
  "category": "Upset Team Member | Missing Dates | SOP Deviation | Schedule Risk | Communication Gap | Stale Project | Closeout Overdue",
  "severity": "high | medium | low",
  "description": "1-2 sentences. Name the project, todo, person, and timing.",
  "url": "basecamp app_url or null",
  "project": "SKY-XXXX project name or null"
}}]

If nothing needs attention, return [].

--- DATA ---
{json.dumps(data_bundle, indent=2)[:22000]}
"""
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6", max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
        try:
            return {"type": "alerts", "alerts": json.loads(text)}
        except json.JSONDecodeError:
            print(f"Could not parse Claude response:\n{text}")
            return {"type": "alerts", "alerts": []}


# ── Slack posting ──────────────────────────────────────────────────────────────

SEVERITY_EMOJI = {"high": ":red_circle:", "medium": ":large_yellow_circle:", "low": ":large_blue_circle:"}


def post_alerts_to_slack(slack_client, channel_id, alerts, title=None):
    now = datetime.now().strftime("%b %d, %I:%M %p")
    header = title or f"PM Watch  —  {now}"
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
        {"type": "divider"},
    ]
    for alert in alerts:
        emoji = SEVERITY_EMOJI.get(alert.get("severity", "low"), ":large_blue_circle:")
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
        channel=channel_id, blocks=blocks,
        text=f"PM Watch: {len(alerts)} alert(s) need your attention",
    )


def post_freeform_to_slack(slack_client, channel_id, text, fallback="PM Watch update"):
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    slack_client.chat_postMessage(channel=channel_id, blocks=blocks, text=fallback)


# ── Public API (used by webhook and job) ──────────────────────────────────────

def run_analysis(on_demand=False):
    """Hourly or on-demand alert scan. Returns list of new alerts."""
    load_env()
    load_secrets_from_gcp()
    if token_needs_refresh():
        refresh_bc_token()

    state = load_state()
    last_run = None if on_demand else state.get("last_run")

    data = fetch_basecamp_data(last_run=last_run, mode="analysis")
    result = analyze_with_claude(
        anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"]), data
    )
    alerts = result.get("alerts", [])

    # Deduplicate
    alerts, state = deduplicate_alerts(alerts, state)

    if not on_demand:
        state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    return alerts


def run_briefing():
    """Morning briefing — full project health summary."""
    load_env()
    load_secrets_from_gcp()
    if token_needs_refresh():
        refresh_bc_token()

    data = fetch_basecamp_data(mode="briefing")
    result = analyze_with_claude(
        anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"]), data
    )
    return result.get("text", "No briefing generated.")


def run_deep_dive(project_query):
    """Full status report on a specific project."""
    load_env()
    load_secrets_from_gcp()
    if token_needs_refresh():
        refresh_bc_token()

    data = fetch_basecamp_data(mode="deep_dive", project_query=project_query)
    if "error" in data:
        return data["error"]

    result = analyze_with_claude(
        anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"]), data
    )
    return result.get("text", "No data found.")


# ── Main (Cloud Run Job — scheduled hourly) ───────────────────────────────────

def main():
    load_env()
    load_secrets_from_gcp()

    for var in ["BC_ACCESS_TOKEN", "BC_REFRESH_TOKEN", "BC_CLIENT_ID",
                "BC_CLIENT_SECRET", "SLACK_TOKEN", "SLACK_CHANNEL_ID", "ANTHROPIC_API_KEY"]:
        if not os.environ.get(var):
            print(f"ERROR: {var} not set")
            sys.exit(1)

    mode = os.environ.get("RUN_MODE", "analysis")
    slack_client = WebClient(token=os.environ["SLACK_TOKEN"])
    channel_id = os.environ["SLACK_CHANNEL_ID"]

    if mode == "briefing":
        print("Running morning briefing...")
        text = run_briefing()
        post_freeform_to_slack(slack_client, channel_id, text, "Skylark PM Morning Briefing")
        print("Briefing posted.")
    else:
        print("Running hourly analysis...")
        alerts = run_analysis(on_demand=False)
        print(f"Found {len(alerts)} new alert(s)")
        if alerts:
            try:
                post_alerts_to_slack(slack_client, channel_id, alerts)
                print(f"Posted {len(alerts)} alert(s) to Slack")
            except SlackApiError as e:
                print(f"Slack error: {e.response['error']}")
        else:
            print("All clear — nothing to post")
    print("Done.")


if __name__ == "__main__":
    main()

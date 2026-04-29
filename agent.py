#!/usr/bin/env python3
"""
Skylark Rick Stamen PM Agent
Features: hourly alerts, morning briefing, deep dive, deduplication, stale detection
"""

import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_FETCH_WORKERS = 4
TODOLIST_FETCH_WORKERS = 4

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


REQUIRED_CLIENT_VISIBLE_LISTS = {"Onsite Phase", "Commissioning Phase", "Closeout Phase"}


def fetch_todos_for_project(proj):
    import re as _re
    proj_id = proj["id"]
    proj_name = proj["name"]
    todoset_tools = [d for d in proj.get("dock", [])
                     if d.get("name") == "todoset" and d.get("enabled")]
    if not todoset_tools:
        return [], [], [], []

    def _fetch_todoset_lists(ts):
        return ts, bc_get_all(f"/buckets/{proj_id}/todosets/{ts['id']}/todolists.json")

    todolists = []
    with ThreadPoolExecutor(max_workers=TODOLIST_FETCH_WORKERS) as pool:
        for ts, tl in pool.map(_fetch_todoset_lists, todoset_tools):
            for t in (tl or []):
                t["_todoset_title"] = ts.get("title", "")
            todolists.extend(tl or [])
    if not todolists:
        return [], [], [], []

    # Check which lists are client-visible
    client_recordings = bc_get_all(f"/buckets/{proj_id}/client/recordings.json")
    client_visible_ids = {r.get("id") for r in (client_recordings or [])}

    existing_list_names = {tlist.get("name", "") for tlist in todolists}
    client_visibility_issues = []
    for required_name in REQUIRED_CLIENT_VISIBLE_LISTS:
        if required_name not in existing_list_names:
            client_visibility_issues.append({
                "project": proj_name,
                "list": required_name,
                "issue": "missing_list",
            })
        else:
            tlist_obj = next((t for t in todolists if t.get("name") == required_name), None)
            if tlist_obj and tlist_obj.get("id") not in client_visible_ids:
                client_visibility_issues.append({
                    "project": proj_name,
                    "list": required_name,
                    "issue": "not_client_visible",
                    "app_url": tlist_obj.get("app_url"),
                })

    def _fetch_list_todos(tlist):
        lid = tlist["id"]
        open_t = bc_get_all(f"/buckets/{proj_id}/todolists/{lid}/todos.json",
                            {"completed": "false"}) or []
        done_t = bc_get_all(f"/buckets/{proj_id}/todolists/{lid}/todos.json",
                            {"completed": "true"}) or []
        return tlist, open_t, done_t

    schedule_todos, labor_todos, all_todos = [], [], []
    with ThreadPoolExecutor(max_workers=TODOLIST_FETCH_WORKERS) as pool:
        list_results = list(pool.map(_fetch_list_todos, todolists))

    for tlist, open_todos, done_todos in list_results:
        list_name = tlist.get("name", "")
        for todo in open_todos + done_todos:
            title = todo.get("content", "")
            due = todo.get("due_on")
            starts = todo.get("starts_on")
            completed = bool(todo.get("completed"))
            assignees = [a.get("name") for a in todo.get("assignees", [])]
            raw_desc = _re.sub(r'<[^>]+>', ' ', todo.get("description") or "").strip()
            entry = {
                "project": proj_name,
                "project_id": proj_id,
                "todoset": tlist.get("_todoset_title", ""),
                "list": list_name,
                "title": title,
                "starts_on": starts,
                "due_on": due,
                "completed": completed,
                "completed_at": todo.get("completion", {}).get("created_at") if completed else None,
                "created_at": todo.get("created_at"),
                "updated_at": todo.get("updated_at"),
                "comments_count": todo.get("comments_count", 0),
                "assignees": assignees,
                "app_url": todo.get("app_url"),
                "description": raw_desc[:600],
            }
            all_todos.append(entry)
            if any(tag in title for tag in SCHED_TAGS):
                schedule_todos.append(entry)
            if "[LABOR]" in title:
                labor_todos.append(entry)

    return schedule_todos, labor_todos, all_todos, client_visibility_issues


def fetch_messages_for_project(proj):
    import re as _re
    proj_id = proj["id"]
    proj_name = proj["name"]
    board_tools = [d for d in proj.get("dock", [])
                   if d.get("name") == "message_board" and d.get("enabled")]
    if not board_tools:
        return []

    result = []
    for board_tool in board_tools:
        board_id = board_tool["id"]
        board_title = board_tool.get("title", "")
        messages = bc_get_all(f"/buckets/{proj_id}/message_boards/{board_id}/messages.json")
        for msg in (messages or []):
            content = _re.sub(r'<[^>]+>', ' ', (msg.get("content") or "")).strip()[:1500]
            entry = {
                "project": proj_name,
                "type": "message",
                "board": board_title,
                "title": msg.get("subject"),
                "content": content,
                "author": (msg.get("creator") or {}).get("name"),
                "created_at": msg.get("created_at", ""),
                "app_url": msg.get("app_url"),
            }
            result.append(entry)

            msg_id = msg.get("id")
            comments = bc_get_all(f"/buckets/{proj_id}/recordings/{msg_id}/comments.json")
            for comment in (comments or []):
                c_content = _re.sub(r'<[^>]+>', ' ', (comment.get("content") or "")).strip()[:800]
                result.append({
                    "project": proj_name,
                    "type": "comment",
                    "board": board_title,
                    "parent_title": msg.get("subject"),
                    "content": c_content,
                    "author": (comment.get("creator") or {}).get("name"),
                    "created_at": comment.get("created_at", ""),
                    "app_url": msg.get("app_url"),
                })
    return result


def fetch_inbox_forwards_for_project(proj):
    import re as _re
    proj_id = proj["id"]
    proj_name = proj["name"]
    inbox_tool = get_dock_tool(proj, "inbox")
    if not inbox_tool:
        return []

    forwards = bc_get_all(f"/buckets/{proj_id}/inbox_forwards.json")
    result = []
    for fwd in (forwards or []):
        content = _re.sub(r'<[^>]+>', ' ', (fwd.get("content") or "")).strip()[:1500]
        entry = {
            "project": proj_name,
            "type": "email_forward",
            "title": fwd.get("subject") or fwd.get("title"),
            "content": content,
            "from": fwd.get("from"),
            "author": (fwd.get("creator") or {}).get("name"),
            "created_at": fwd.get("created_at", ""),
            "app_url": fwd.get("app_url"),
        }
        result.append(entry)

        fwd_id = fwd.get("id")
        comments = bc_get_all(f"/buckets/{proj_id}/recordings/{fwd_id}/comments.json")
        for comment in (comments or []):
            c_content = _re.sub(r'<[^>]+>', ' ', (comment.get("content") or "")).strip()[:800]
            result.append({
                "project": proj_name,
                "type": "email_forward_comment",
                "parent_title": fwd.get("subject") or fwd.get("title"),
                "content": c_content,
                "author": (comment.get("creator") or {}).get("name"),
                "created_at": comment.get("created_at", ""),
                "app_url": fwd.get("app_url"),
            })
    return result


def fetch_cards_for_project(proj):
    import re as _re
    proj_id = proj["id"]
    proj_name = proj["name"]
    card_tool = get_dock_tool(proj, "kanban_board")
    if not card_tool:
        return []

    # Get card table columns
    table_id = card_tool["id"]
    table = bc_get_data(f"/buckets/{proj_id}/card_tables/{table_id}.json")
    if not table:
        return []

    cards = []
    for column in (table.get("lists") or []):
        col_name = column.get("title", "")
        col_cards = bc_get_all(f"/buckets/{proj_id}/card_tables/lists/{column['id']}/cards.json")
        for card in (col_cards or []):
            desc = _re.sub(r'<[^>]+>', ' ', (card.get("description") or "")).strip()[:800]
            assignees = [a.get("name") for a in (card.get("assignees") or [])]
            cards.append({
                "project": proj_name,
                "column": col_name,
                "title": card.get("title", ""),
                "due_on": card.get("due_on"),
                "assignees": assignees,
                "description": desc,
                "app_url": card.get("app_url"),
            })
    return cards


# ── Google Drive job-folder audit ──────────────────────────────────────────────

DRIVE_JOBS_ROOT_ID = os.environ.get("DRIVE_JOBS_ROOT_ID", "1DiI7KOS6pjiXQclLhepBAiy9q-K2vvEB")

DRIVE_REQUIRED_LAYOUT = {
    "Contract Docs": ["BOM", "Contract Revisions", "Insurance Documents",
                      "Packing Slips", "Purchase Orders", "Signed Contracts"],
    "Engineering": ["Client Supplied Documents", "Equipment Config Files",
                    "Onsite Pictures", "PatchCAD", "PDF", "Pull Sheets",
                    "Renders", "Sketchup", "Skylark DWG", "Soundvision",
                    "Vectorworks", "Vision"],
    "Proposals": ["Archives"],
    "Vendor Docs": [],
}

DRIVE_FOLDER_ALIASES = {
    "Contract Docs": ["Contracts", "Contract Documents"],
    "Engineering": ["Eng", "Engineering Docs"],
    "Proposals": ["Proposal", "Quotes"],
    "Vendor Docs": ["Vendor", "Vendor Documents", "Vendors"],
    "Insurance Documents": ["Insurance Docs", "Insurance", "COI", "Insurance Certificates"],
    "Signed Contracts": ["Signed Documents", "Signed", "Executed Contracts", "Signed Contract"],
    "Purchase Orders": ["POs", "PO", "Purchase Order"],
    "Packing Slips": ["Packing Slip", "Shipping"],
    "Contract Revisions": ["Revisions", "Contract Drafts"],
    "BOM": ["BOMs", "Bill of Materials"],
    "Skylark DWG": ["Skylark Drawings", "DWG"],
    "Onsite Pictures": ["Onsite Photos", "Site Photos", "Jobsite Photos"],
    "Client Supplied Documents": ["Client Docs", "Client Supplied"],
    "Equipment Config Files": ["Config Files", "Configs"],
    "Pull Sheets": ["Pullsheets", "Pull Lists"],
    "Archives": ["Archive", "Old"],
}

FOLDER_MIME = "application/vnd.google-apps.folder"


def _folder_matches(expected, actual_names):
    """Return the matched actual-folder name, or None. Case-insensitive, checks aliases."""
    norm = {n.lower(): n for n in actual_names}
    if expected.lower() in norm:
        return norm[expected.lower()]
    for alias in DRIVE_FOLDER_ALIASES.get(expected, []):
        if alias.lower() in norm:
            return norm[alias.lower()]
    return None


def get_drive_service():
    try:
        from googleapiclient.discovery import build
        import google.auth
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/drive.readonly"])
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"Drive init skipped: {type(e).__name__}: {e}")
        return None


def drive_list_children(svc, folder_id):
    try:
        res = svc.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,name,mimeType,modifiedTime,size)",
            supportsAllDrives=True, includeItemsFromAllDrives=True,
            pageSize=500,
        ).execute()
        return res.get("files") or []
    except Exception as e:
        print(f"Drive list {folder_id} failed: {type(e).__name__}: {e}")
        return []


def drive_find_install_share(svc, sky_id):
    q = f"mimeType='{FOLDER_MIME}' and name contains '{sky_id}' and trashed=false"
    try:
        res = svc.files().list(
            q=q, fields="files(id,name,parents,modifiedTime)",
            supportsAllDrives=True, includeItemsFromAllDrives=True,
            corpora="allDrives", pageSize=20,
        ).execute()
        files = res.get("files") or []
        ranked = sorted(files, key=lambda f: (
            0 if f["name"].upper().startswith(sky_id.upper()) else 1,
            0 if "Install Share" in f["name"] else 1,
            len(f["name"]),
        ))
        return ranked
    except Exception as e:
        print(f"Drive search {sky_id} failed: {type(e).__name__}: {e}")
        return []


def drive_scan_tree(svc, folder_id, max_depth=3, file_limit_per_folder=25, _depth=0):
    """Shallow-to-medium tree scan. Returns dict of {name: {type, modified, size?, children?}}."""
    if _depth >= max_depth:
        return {"_truncated_depth": True}

    children = drive_list_children(svc, folder_id)
    out = {}
    file_count = 0
    for c in children:
        name = c["name"]
        is_folder = c["mimeType"] == FOLDER_MIME
        if not is_folder:
            file_count += 1
            if file_count > file_limit_per_folder:
                out["_more_files_truncated"] = True
                continue
        node = {"type": "folder" if is_folder else "file",
                "modified": (c.get("modifiedTime") or "")[:10]}
        if not is_folder and c.get("size"):
            node["size"] = c["size"]
        if is_folder and _depth + 1 < max_depth:
            node["children"] = drive_scan_tree(svc, c["id"], max_depth,
                                               file_limit_per_folder, _depth + 1)
        out[name] = node
    return out


def audit_drive_folder(svc, proj):
    import re as _re
    m = _re.match(r'(SKY-\d+)', proj.get("name", ""))
    if not m:
        return None
    sky_id = m.group(1)

    hits = drive_find_install_share(svc, sky_id)
    if not hits:
        return {"sky": sky_id, "project": proj["name"],
                "issues": ["no_drive_folder_found"]}

    issues = []
    if len(hits) > 1:
        issues.append(f"multiple_drive_folders_for_sky ({len(hits)})")

    project_folder = hits[0]
    project_folder_id = project_folder["id"]

    children = drive_list_children(svc, project_folder_id)
    child_folder_items = {c["name"]: c for c in children if c["mimeType"] == FOLDER_MIME}
    top_names = list(child_folder_items.keys())

    missing_top, matched_top = [], {}
    for expected in DRIVE_REQUIRED_LAYOUT:
        match = _folder_matches(expected, top_names)
        if match:
            matched_top[expected] = match
        else:
            missing_top.append(expected)

    missing_nested, empty_top = [], []
    for expected, subs in DRIVE_REQUIRED_LAYOUT.items():
        actual_name = matched_top.get(expected)
        if not actual_name:
            continue
        parent = child_folder_items[actual_name]
        nested = drive_list_children(svc, parent["id"])
        if not nested:
            empty_top.append(expected)
            continue
        nested_folder_names = [c["name"] for c in nested if c["mimeType"] == FOLDER_MIME]
        for sub in subs:
            if _folder_matches(sub, nested_folder_names):
                continue
            missing_nested.append(f"{expected}/{sub}")

    days_stale = None
    try:
        mt = project_folder.get("modifiedTime", "")
        if mt:
            dt = datetime.fromisoformat(mt.replace("Z", "+00:00"))
            days_stale = (datetime.now(timezone.utc) - dt).days
    except Exception:
        pass

    tree = drive_scan_tree(svc, project_folder_id, max_depth=3)

    return {
        "sky": sky_id,
        "project": proj["name"],
        "drive_project_folder": project_folder["name"],
        "missing_top_folders": missing_top,
        "missing_nested_folders": missing_nested,
        "empty_top_folders": empty_top,
        "days_since_drive_modified": days_stale,
        "matched_aliases": {k: v for k, v in matched_top.items() if k != v},
        "tree": tree,
        "issues": issues,
        "note": "Use `tree` to reason about real-world naming. Folders / files that don't match the template exactly may still satisfy the intent (e.g. 'Insurance Docs' = 'Insurance Documents', a loose 'Contract SEC Video System.pdf' in Contract Revisions = signed contract on file).",
    }


def audit_drive_for_projects(sky_projects):
    svc = get_drive_service()
    if not svc:
        return []
    out = []
    for proj in sky_projects:
        try:
            r = audit_drive_folder(svc, proj)
            if r:
                out.append(r)
        except Exception as e:
            print(f"Drive audit {proj.get('name')} failed: {type(e).__name__}: {e}")
    return out


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
        if "(Design Contract)" in proj["name"]:
            continue
        if proj["id"] not in active_project_ids_in_events:
            stale_projects.append({
                "project": proj["name"],
                "description": proj.get("description", "")[:200],
            })

    # Per-project data
    all_schedule_todos, all_labor_todos, all_todos = [], [], []
    all_messages, all_cards, all_client_visibility_issues = [], [], []
    project_summaries = []
    projects_to_fetch = sky_projects[:1] if mode == "deep_dive" else sky_projects

    def _fetch_project_bundle(proj):
        sched_t, labor_t, proj_t, vis = fetch_todos_for_project(proj)
        msgs = fetch_messages_for_project(proj)
        emails = fetch_inbox_forwards_for_project(proj)
        cards_p = fetch_cards_for_project(proj)
        sched_entries = []
        sched_tool = get_dock_tool(proj, "schedule")
        if sched_tool:
            sched_entries = bc_get_all(
                f"/buckets/{proj['id']}/schedules/{sched_tool['id']}/entries.json"
            ) or []
            for e in sched_entries:
                e["_project_name"] = proj["name"]
        return proj, sched_t, labor_t, proj_t, vis, msgs, emails, cards_p, sched_entries

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=PROJECT_FETCH_WORKERS) as pool:
        bundles = list(pool.map(_fetch_project_bundle, projects_to_fetch))
    print(f"Fetched {len(bundles)} projects in {(time.perf_counter() - t0):.1f}s")

    schedule_entries = []
    cutoff_msg = cutoff_done = None
    if mode in ("briefing", "analysis"):
        cutoff_msg = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        cutoff_done = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()

    for proj, sched_todos, labor_todos, proj_todos, client_vis_issues, messages, email_forwards, cards, sched_entries in bundles:
        desc = proj.get("description", "")
        is_design_contract = "(Design Contract)" in proj.get("name", "")
        project_summaries.append({
            "id": proj["id"],
            "name": proj["name"],
            "description": desc[:500],
            "type": "Design Contract" if is_design_contract else "Standard Project",
            "app_url": proj.get("app_url"),
        })

        if cutoff_msg:
            messages = [m for m in messages if m.get("created_at", "") >= cutoff_msg]
            email_forwards = [m for m in email_forwards if m.get("created_at", "") >= cutoff_msg]
            proj_todos = [t for t in proj_todos
                          if not t.get("completed")
                          or (t.get("completed_at") or "") >= cutoff_done]

        all_schedule_todos.extend(sched_todos)
        all_labor_todos.extend(labor_todos)
        all_todos.extend(proj_todos)
        all_messages.extend(messages)
        all_messages.extend(email_forwards)
        all_cards.extend(cards)
        schedule_entries.extend(sched_entries)
        if not is_design_contract:
            all_client_visibility_issues.extend(client_vis_issues)

    drive_compliance = audit_drive_for_projects(projects_to_fetch)

    return {
        "mode": mode,
        "recent_events": recent_events[:100] if mode == "analysis" else [],
        "notifications": (notifications or [])[:50],
        "project_summaries": project_summaries,
        "schedule_tagged_todos": all_schedule_todos,
        "labor_todos": all_labor_todos,
        "all_todos": all_todos,
        "messages_and_comments": all_messages,
        "cards": all_cards,
        "client_visibility_issues": all_client_visibility_issues,
        "stale_projects": stale_projects,
        "upcoming_schedule_entries": schedule_entries,
        "drive_compliance": drive_compliance,
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

### Project Types
Skylark has two types of SKY- projects:
- **Design Contract** — pre-execution, design/engineering phase only. Project name includes "(Design Contract)". TBD fields are acceptable. Full SOP compliance (install scheduling, labor, procurement) is NOT expected.
- **Standard Project** — full execution project. All description fields required, no TBDs, full SOP applies.

### Project Description (Required Fields)
Every active SKY- project must have exactly these 5 fields in its description:
- Client Contact
- Job Location
- Skylark PM
- Engineer
- On-Site Lead
No dates are expected in the description. Dates live in schedule-tagged todos only.
TBD is only acceptable for Design Contract projects (not Standard Projects).

### Schedule Tag System
[PM-SCHED] [ENG-SCHED] [PROC-SCHED] [SHOP-SCHED] [LOG-SCHED] [ONS-SCHED] [COM-SCHED] [FUT-SCHED]
CRITICAL: Any incomplete schedule-tagged todo WITHOUT a due_on date = missing_dates flag.

### Date Ranges on Todos (CRITICAL — read this carefully)
Basecamp todos can have a date range, not just a single due date. Each todo has TWO date fields:
- `starts_on` — the FIRST day of the range (null if no range)
- `due_on` — the LAST day (also used for single-date todos when there's no range)

How to interpret:
- If `starts_on` is null → single-date todo, `due_on` is the date.
- If `starts_on` is set → it's a RANGE. The trip/phase BEGINS on `starts_on` and ENDS on `due_on`.

This matters most for `[ONS-SCHED]` install trips. Example: an install todo with `starts_on: 2026-04-25` and `due_on: 2026-05-01` means the install RUNS from April 25 through May 1 — Apr 25 is when crew mobilizes, May 1 is the LAST day onsite. Reporting "install begins 5/1" in this case is wrong; the install ends 5/1.

Always state ranges as "Apr 25 – May 1" or "from 4/25, ends 5/1". Never collapse a range to a single date. When computing "days out" or "begins in N days", base it on `starts_on` if present, otherwise `due_on`.

### Engineering Milestone System
Engineering runs two coordinated tracks: Construction DD/CD and AV Systems.
These tracks are related but distinct. Procurement releases in stages as engineering confidence increases.
Once a package reaches IFC it is in revision-only mode — all changes tracked via Revision IDs + revision clouds.

Key terms:
- DD: design development issue phase for construction coordination
- CD: post-DD coordination phase driven by trades, client changes, or field conditions
- IFC: issued for construction — final released package for execution
- Issue Set: a formally published milestone package
- Revision: a tracked change to an issued package

#### Construction DD/CD Track [ENG-SCHED]
**Milestone 1** — Early GC coordination: weight loads, heat loads, AV spaces identified, total power requirements, "provided by others" callouts, rough coordination mockups. Deliverable: DD/CD Issue Set.
**Milestone 2** — Further GC coordination: circuit Jbox locations, furniture mockups. Deliverable: DD/CD Issue Set.
**Milestone 3** — Construction DD effectively complete. Must include: complete and coherent coordination package, ready for internal review then immediate client review.
  Review sequence: (1) Internal design review → (2) Client review
**Milestone 4** — 100% GC handoff to trades. Standard: baseline issue for construction coordination, design development complete; all future changes must come from coordination feedback, field conditions, or client-driven changes — NOT unfinished engineering work. Deliverable: 100% DD/CD Construction Issue Set.
**CD Phase** — Responds to changes after DD baseline. CD changes are reactions to other trades, not open-ended design. The same 100% DD package may be re-issued to meet GC/architect milestones; those external milestones do not create new internal engineering phases. Revisions must be tracked clearly.

#### AV Systems Track [ENG-SCHED]
**25% AV Systems** — Goal: establish big-picture system layout and surface risk early.
  Must include: file structure, equipment blocks layout, system roughly built by room, big-picture system view established, some lines/cables may begin, Order Detail items represented.
  Internal use: Concept Drawing Comparison Review, PM checkpoint, early discovery of missing gear/scope gaps/system problems.
  Procurement: Release 1 for major component long-lead items.
**50% AV Systems** — Goal: package far enough for engineering coordination and shop planning.
  Must include: wires drawn, rack elevations created, power plan, Custom Panels, overall system layout complete.
  Deliverables: first pass Design Review, package may be given to shop for planning, Custom Panels client signoff.
  Procurement: Release 2 for additional equipment.
**75% AV Systems** — Goal: essentially complete systems design.
  Must include: systems design complete, wire numbers complete, system logic complete, package ready for systems review.
  Review: internal systems design review.
  Procurement: Release 3 for remaining equipment.
**100% AV Systems** — Goal: complete physical implementation package.
  Must include: physical design complete, speaker locations complete, LED wall details complete, TV placement complete, rigging drawings complete, cable pull schedules complete.
  Review: internal review with install team involvement, client review as needed.
  Procurement: Release 4 for rigging and physical infrastructure not previously released.
**IFC (Issued for Construction)** — Final released package for execution. Revision-only mode from this point forward.

#### SOP Violations to Flag [ENG-SCHED]
- Engineering doing new/open-ended design work after Milestone 4 / CD phase = violation (should be reactions only)
- No review sequence at Milestone 3 (internal → client) = flag
- Procurement released before corresponding AV Systems milestone = flag
- IFC package being edited without tracked Revision IDs = flag
- Missing Milestone deliverables (no Issue Set published at M1/M2/M4) = flag

- Punch List Walkthrough [PM-SCHED]: 48 hours before end of install
- Client Sign-Off [PM-SCHED]: before pulling off job
- As-Built Package [ENG-SCHED]: 2 weeks after open
- Post-Mortem [PM-SCHED]: 1 week after open
- Project Closed [PM-SCHED]: 90 days after open

### Labor Scheduling
[LABOR] todos live in the **Labor Scheduling** todoset. Format: "Name | Role | Status [LABOR]".
Description must have Flights, Hotel, Per-Diem, Car Rental filled in.
Missing travel info on an upcoming trip = flag.

EVERY confirmed [ONS-SCHED] onsite trip MUST have corresponding [LABOR] todos documenting who is going onsite for that trip. This is non-negotiable — onsite work without documented labor = SOP violation.

Rule: for each [ONS-SCHED] trip with a real due date, there must be at least one [LABOR] todo in the Labor Scheduling todoset that covers that trip's date range. Tie the labor to the trip by date proximity (the [LABOR] dates should overlap or align with the [ONS-SCHED] trip date).

Flag if:
- A confirmed [ONS-SCHED] trip has zero [LABOR] todos covering it
- A [LABOR] todo is missing the Flights/Hotel/Per-Diem/Car Rental fields when the trip is within 14 days
- A [LABOR] todo exists but has no due date or assignee

### Required Client-Visible Todo Lists (Standard Projects only)
These three todo lists MUST be set to "The client sees this" on every Standard Project:
- Onsite Phase
- Commissioning Phase
- Closeout Phase

The data bundle includes a `client_visibility_issues` array. Each entry has `project`, `list`, and `issue` ("not_client_visible" or "missing_list"). Any entry in this array = flag immediately as "Client Visibility" SOP violation.

### Pre-Mobilization Gate
GO/NO-GO check required 14 days AND 7 days before mobilization.
No evidence of GO/NO-GO with [ONS-SCHED] due in <14 days = flag.

### Required Logistics Todos (Standard Projects with confirmed onsite date)
Projects can have multiple onsite trips. Each confirmed [ONS-SCHED] todo (with a real due date, not TBD) represents one trip and requires its own complete set of three logistics todos.

Rule: count the number of confirmed [ONS-SCHED] todos with due dates. That number = the required count of each of these todos:
1. "Equipment/Materials Arrive Onsite [LOG-SCHED]"
2. "Verify Equipment and Materials [LOG-SCHED]"
3. "Return Trip to Home Shop (Excess Equipment/Materials) [LOG-SCHED]"

Example: 3 onsite trips → must have 3 of each logistics todo, all with due dates.

Flag as "SOP Deviation" if:
- The count of any logistics todo (present with a due date) is less than the number of confirmed [ONS-SCHED] todos
- Any logistics todo is present but missing a due_on date
This check only applies to Standard Projects (not Design Contracts).

### Onsite "Attention Needed" List (Field RFIs)
Inside the **Onsite Tasks** todoset there is a todolist called **"Attention Needed"**. These are field RFIs / problems raised by the install team that need fast resolution.

Rule: any open todo in the "Attention Needed" list whose `updated_at` is more than **24 hours** old (no comments, status change, or assignment update in 24h) = flag as a stale field RFI.

Use `updated_at` (last touched) — not `created_at`. A field RFI created 3 days ago but updated this morning is fine; one created today but untouched for 25 hours is not.

When flagging, include:
- The todo title
- Project (SKY-XXXX)
- Hours since last activity (round to whole hours)
- Assignee, if any (no assignee on a stale field RFI is a worse flag)
- The app_url so Tyler can jump straight to it

Tyler wants these surfaced proactively in any briefing or per-project review — they should never sit without follow-up.

### Onsite Orders Phase (Procurement Mode Switch)
"Onsite Orders" is a procurement phase trigger, not a regular task. It's a single todo (typically a `[PROC-SCHED]` item titled "Onsite Orders" or similar) whose due date is set ~2 weeks before the first onsite/install date. While this todo is open, procurement documents and tracks orders differently — anything ordered during the install window flows through the Onsite Orders process.

Lifecycle:
- Created as part of the project schedule template
- Due date = approximately 14 days before the [ONS-SCHED] install start date
- INTENTIONALLY stays OPEN from its due date through the entire onsite window
- Checked off ONLY after the crew has pulled off the job (onsite work fully complete)

Flag if:
- A Standard Project has an [ONS-SCHED] install starting within 21 days but no Onsite Orders todo exists at all (missing template item).
- An Onsite Orders todo exists but has no due_on date.
- Onsite Orders due_on is more than ~21 days before the [ONS-SCHED] install start (way too early — likely scheduled wrong).
- Onsite Orders is marked complete BEFORE the install team has pulled off (i.e. before the last [ONS-SCHED] day, or before the corresponding Commissioning/Closeout items are done) — that's premature closure and breaks the procurement tracking window.
- Onsite Orders is still open more than 30 days after the install was supposed to end — closeout cleanup needed.

DO NOT flag a still-open Onsite Orders todo during the onsite window as "overdue" — that's the expected state. The whole point is that it stays open while crew is in the field.

### Onsite Trip Companion Items (Standard Projects)
Every confirmed [ONS-SCHED] installation trip should be followed by these companion items, all on the project schedule:
- A **Commissioning** task (typically [COM-SCHED]) — system commissioning after install
- **Client Training** — as needed for the scope (cameras, audio, lighting, control surfaces, etc.)
- A **Punch List Walkthrough** [PM-SCHED] — 48 hours before end of install (per Closeout SOP)

Flag if an [ONS-SCHED] install trip has no following commissioning todo, no training todo (where the scope clearly calls for it), or no punch list walkthrough scheduled.

### Punch List Phase Detection & Client Visibility
A project enters the **Punch List Phase** when there is an [ONS-SCHED] todo whose title contains "Installation (Punch List)" or "Punch List" (e.g. `Installation (Punch List) [ONS-SCHED]`).

When in Punch List Phase:
1. ALL remaining open punch list items in the **Onsite Tasks**, **Engineering Tasks**, and **Commissioning Tasks** todosets must be MOVED to the **Closeout Tasks** todoset (not duplicated — moved).
2. The Closeout list containing these items must be set to client-visible ("The client sees this") so the client can track punch list progress.
3. Items still sitting in Onsite/Engineering/Commissioning during punch list phase = SOP deviation. Flag each one specifically with its title, current todoset, and the action needed ("move to Closeout").

Use the `todoset` field on each todo to determine where it currently lives. If a todo's title or description references a punch-list-style item (touch-ups, corrections, deficiencies, list of items the client called out) and it's still in Onsite/Engineering/Commissioning while the project is in Punch List Phase → flag.

Tyler wants Rick to call these out proactively when reviewing a project that's in the punch list phase.

### Communication Rules
- Client posts → "Client Communication" board only
- Internal updates → "Internal Coordination" board only
- Decisions/actions from calls must be logged in Basecamp

### Closeout
Project is overdue for closure if "Client First Open [PM-SCHED]" passed >90 days ago
and "Project Closed in Basecamp [PM-SCHED]" is still incomplete.

### Google Drive Job Folder Compliance
Every Standard Project should have a Google Drive project folder named `SKY-XXXX ...` (inside the client's folder under Skylark Jobs). Inside that project folder, the template is: `Contract Docs`, `Engineering`, `Proposals`, `Vendor Docs` (each with specific required subfolders).

The data bundle includes `drive_compliance` per project. Each entry has:
- `missing_top_folders` / `missing_nested_folders` — deterministic misses after alias matching
- `empty_top_folders` — folder exists but has no contents
- `matched_aliases` — where an alias was used (e.g. "Insurance Documents" matched "Insurance Docs")
- `tree` — the actual folder + file structure (2-3 levels deep). Use this to reason beyond the template.
- `days_since_drive_modified` — staleness signal

REAL-WORLD RULE: The template is a guide, not a contract. Before flagging something missing, look at `tree` and ask "is there functional evidence this requirement is met?" Examples:
- Template wants `Signed Contracts/` but `tree` shows `Contract Revisions/` contains `Contract SEC Video System.pdf` and a `Signed Documents/` subfolder — that IS the signed contract on file. Don't flag.
- Template wants `Insurance Documents/` but `tree` shows `Insurance Docs/` with a COI PDF inside — same thing. Don't flag.
- Template wants `Onsite Pictures/` but `tree` shows empty `Onsite Photos/` — flag as empty if the project is past the onsite phase.
- Template wants `Signed Contracts/` and NOTHING in `tree` looks like a contract document — flag it, this is a real gap.

Cross-reference against Basecamp phase. Missing a signed contract on a design-phase project is normal. Missing it on a project already in onsite phase is a red flag.
"""


# ── Claude analysis ────────────────────────────────────────────────────────────

def analyze_with_claude(anthropic_client, data_bundle):
    mode = data_bundle.get("mode", "analysis")
    last_run = data_bundle.get("last_run")
    since_str = last_run or "the past hour"
    full_data_json = json.dumps(data_bundle, indent=2)
    DATA_LIMIT = 500000
    truncated = len(full_data_json) > DATA_LIMIT
    data_json = full_data_json[:DATA_LIMIT]
    if truncated:
        data_json += "\n\n[DATA TRUNCATED — original was {full} chars, kept {kept}]".format(
            full=len(full_data_json), kept=DATA_LIMIT)
    print(f"analyze_with_claude: mode={mode} data_chars_full={len(full_data_json)} kept={len(data_json)} truncated={truncated}")

    if mode == "briefing":
        prompt = f"""You are Rick Stamen, the PM agent for Skylark AV. Generate a morning briefing for Tyler (founder/owner).

{SKYLARK_SOP_CONTEXT}

Today is {data_bundle['as_of'][:10]}.

The data below includes EVERY active SKY project with ALL todos (all_todos), ALL messages and comments (messages_and_comments), ALL cards (cards), schedule entries, and labor todos. Use all of it.

Review all active project data below and produce a clear morning briefing in Slack markdown.

Format:
- Start with a one-line summary count (e.g. "12 active jobs — 3 need attention")
- List jobs needing action first (with specific issue)
- Then jobs that are all-clear (just name + current phase)
- End with a "This Week" section: key milestones due in the next 7 days across all jobs

Use :red_circle: for high issues, :large_yellow_circle: for medium, :white_check_mark: for clear.
Be specific — name the todo, the person, the date.

ALWAYS render every project name as a clickable Slack link using the project's `app_url` from `project_summaries`. Format: `<APP_URL|SKY-XXXX>` — angle brackets, URL, pipe, display text. Never write a bare `SKY-XXXX` when an app_url is available. This applies to every mention in every section: headers, bullets, inline references. Tyler should be able to click any project name to jump straight to it.

--- DATA ---
{data_json}
"""
        try:
            response = anthropic_client.messages.create(
                model="claude-opus-4-7", max_tokens=8000,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            print(f"Anthropic error (briefing): {type(e).__name__}: {e}")
            raise
        return {"type": "briefing", "text": response.content[0].text.strip()}

    elif mode == "deep_dive":
        project_name = (data_bundle.get("project_summaries") or [{}])[0].get("name", "Unknown")
        prompt = f"""You are Rick Stamen, the PM agent for Skylark AV. Give Tyler a full status report on {project_name}.

{SKYLARK_SOP_CONTEXT}

As of {data_bundle['as_of'][:10]}.

The data below includes EVERY todo, message, comment, card, and schedule entry for this project. Use all of it — don't skip anything.

Cover:
1. Project description fields (PM, Engineer, On-Site Lead, Client Contact)
2. Current active phase (based on incomplete schedule-tagged todos with due dates)
3. All open todos by list — who owns what, what's overdue, what's missing dates
4. Upcoming milestones (next 30 days)
5. Labor/travel status (any [LABOR] todos and their travel details)
6. Card table status (if present — what's in each column, anything blocked or stale)
7. Full message/comment thread review — tone, open questions, anything unresolved
8. Any SOP violations or engineering milestone flags
9. Overall health: GREEN / YELLOW / RED with one-line reason

Use Slack markdown. Be thorough — Tyler wants the full picture.

Render the project name in the report header as a clickable Slack link using its `app_url` from `project_summaries`. Format: `<APP_URL|SKY-XXXX>`. Same rule for any other project you reference — make every SKY mention clickable.

--- DATA ---
{data_json}
"""
        try:
            response = anthropic_client.messages.create(
                model="claude-opus-4-7", max_tokens=8000,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            print(f"Anthropic error (deep_dive): {type(e).__name__}: {e}")
            raise
        return {"type": "deep_dive", "project": project_name, "text": response.content[0].text.strip()}

    else:
        # Standard hourly/on-demand alert analysis
        prompt = f"""You are Rick Stamen, the PM agent for Skylark AV. Review Basecamp data and flag issues needing Tyler's attention.

{SKYLARK_SOP_CONTEXT}

The data below includes ALL todos (all_todos), ALL messages and comments (messages_and_comments), ALL cards (cards), schedule entries, and labor todos across every active SKY project. Read all of it carefully.

Flag these issues:
1. **Upset / Frustrated** — tense tone in messages or comments (read the actual content)
2. **Missing Dates** — incomplete [XXXX-SCHED] todo with no due_on date (breaks phase logic)
3. **SOP Deviation** — wrong board, TBD fields on active jobs, [LABOR] missing travel info, no GO/NO-GO before install
4. **Schedule Risk** — milestones overdue relative to onsite dates, install <14 days with no GO/NO-GO
5. **Communication Gap** — client or team question unanswered for 24+ hours
6. **Stale Project** — active-phase project with zero recent Basecamp activity
7. **Closeout Overdue** — Client First Open passed >90 days ago, project not closed
8. **Blocked / Stuck** — todo or card sitting in the same state with no activity, assignee missing, or description says waiting on something
9. **Engineering Milestone Violation** — per the two-track milestone system, flag if wrong deliverable at wrong stage
10. **Client Visibility** — any entry in `client_visibility_issues` means a required list (Onsite Phase, Commissioning Phase, or Closeout Phase) is either missing or not set to client-visible

Only flag real issues. Skip Design Contract projects for TBD field violations — those are pre-execution. Standard Projects must have all fields filled.

Return ONLY a JSON array:
[{{
  "category": "Upset Team Member | Missing Dates | SOP Deviation | Schedule Risk | Communication Gap | Stale Project | Closeout Overdue | Client Visibility",
  "severity": "high | medium | low",
  "description": "1-2 sentences. Name the project, todo, person, and timing.",
  "url": "basecamp app_url for the specific todo/message/card, or null",
  "project": "SKY-XXXX project name or null",
  "project_url": "the project's app_url from project_summaries, or null"
}}]

If nothing needs attention, return [].

--- DATA ---
{data_json}
"""
        try:
            response = anthropic_client.messages.create(
                model="claude-opus-4-7", max_tokens=4000,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            print(f"Anthropic error (analysis): {type(e).__name__}: {e}")
            raise
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
    header = title or f"Rick Stamen  —  {now}"
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
        {"type": "divider"},
    ]
    for alert in alerts:
        emoji = SEVERITY_EMOJI.get(alert.get("severity", "low"), ":large_blue_circle:")
        if alert.get("project"):
            if alert.get("project_url"):
                project_line = f"\n_Project: <{alert['project_url']}|{alert['project']}>_"
            else:
                project_line = f"\n_Project: {alert['project']}_"
        else:
            project_line = ""
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
        text=f"Rick Stamen: {len(alerts)} alert(s) need your attention",
    )


def split_for_slack(text, max_size=2900):
    """Split long Slack mrkdwn text without breaking link tokens.

    Slack section blocks cap at 3000 chars. A naive split mid-string can
    cleave `<URL|display>` link syntax in half, leaving raw URLs visible.
    We prefer paragraph/line/word breaks and back off to before any
    unmatched `<` so links stay intact.
    """
    chunks = []
    while len(text) > max_size:
        cut = max_size
        for delim in ("\n\n", "\n", " "):
            idx = text.rfind(delim, 0, max_size)
            if idx >= max_size // 2:
                cut = idx
                break
        head = text[:cut]
        last_open = head.rfind("<")
        last_close = head.rfind(">")
        if last_open > last_close:
            # cursor lands inside a Slack link token — back up to before it
            ws = max(text.rfind(" ", 0, last_open), text.rfind("\n", 0, last_open))
            cut = ws + 1 if ws >= 0 else last_open
        if cut <= 0:
            cut = max_size  # malformed text, fall back to hard split
        chunks.append(text[:cut].rstrip())
        text = text[cut:].lstrip()
    if text.strip():
        chunks.append(text)
    return chunks


def post_freeform_to_slack(slack_client, channel_id, text, fallback="Rick Stamen update"):
    chunks = split_for_slack(text)
    for i, chunk in enumerate(chunks):
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": chunk}}]
        slack_client.chat_postMessage(
            channel=channel_id, blocks=blocks,
            text=fallback if i == 0 else f"{fallback} (cont.)",
        )


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


# ── Main (Cloud Run Job) ───────────────────────────────────────────────────────

def main():
    load_env()
    load_secrets_from_gcp()

    for var in ["BC_ACCESS_TOKEN", "BC_REFRESH_TOKEN", "BC_CLIENT_ID",
                "BC_CLIENT_SECRET", "SLACK_TOKEN", "SLACK_CHANNEL_ID", "ANTHROPIC_API_KEY"]:
        if not os.environ.get(var):
            print(f"ERROR: {var} not set")
            sys.exit(1)

    mode = os.environ.get("RUN_MODE", "analysis")
    on_demand = os.environ.get("ON_DEMAND", "false").lower() == "true"
    channel_id = os.environ["SLACK_CHANNEL_ID"]
    slack_client = WebClient(token=os.environ["SLACK_TOKEN"])

    if mode == "briefing":
        print("Running briefing...")
        text = run_briefing()
        post_freeform_to_slack(slack_client, channel_id, text, "Skylark PM Briefing")
        print("Briefing posted.")

    elif mode == "deep_dive":
        project_query = os.environ.get("PROJECT_QUERY", "")
        print(f"Running deep dive: {project_query}")
        text = run_deep_dive(project_query)
        post_freeform_to_slack(slack_client, channel_id, text, f"Deep Dive: {project_query}")
        print("Deep dive posted.")

    else:
        print(f"Running analysis (on_demand={on_demand})...")
        alerts = run_analysis(on_demand=on_demand)
        print(f"Found {len(alerts)} new alert(s)")
        if alerts:
            try:
                title = "Rick Stamen (on-demand)" if on_demand else "Rick Stamen"
                post_alerts_to_slack(slack_client, channel_id, alerts, title=title)
                print(f"Posted {len(alerts)} alert(s) to Slack")
            except SlackApiError as e:
                print(f"Slack error: {e.response['error']}")
        else:
            if on_demand:
                msg = ":white_check_mark: *All clear* — no issues found across active Skylark projects."
                post_freeform_to_slack(slack_client, channel_id, msg)
            print("All clear — nothing to post")

    print("Done.")


if __name__ == "__main__":
    main()

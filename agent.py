#!/usr/bin/env python3
"""
Skylark Rick Stamen PM Agent
Modes: morning briefing, deep dive, drive audit
"""

import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_FETCH_WORKERS = 4
TODOLIST_FETCH_WORKERS = 4

import anthropic
from slack_sdk import WebClient

ACCOUNT_ID = "4358663"
BC_BASE = f"https://3.basecampapi.com/{ACCOUNT_ID}"
TOKEN_ENDPOINT = "https://launchpad.37signals.com/authorization/token"
USER_AGENT = "Skylark PM Agent (tyler@skylarkav.com)"

SCRIPT_DIR = Path(__file__).parent
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

# 4xx (other than 429) means "the request itself is bad" — won't get better with
# retries. Anything in this set is a transient infrastructure failure and IS
# safe to retry. Critically: silent return on a transient error was masking
# fetch failures in parallel todoset reads, producing phantom "list missing"
# false-positive flags in briefings.
_BC_RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def _bc_request_raw(url, max_attempts=5):
    """Single GET with retry on transient errors. Returns (json_data, link_header).

    Returns (None, "") only after exhausting retries on a transient error, or
    immediately on a hard 4xx (auth, not found, malformed). Logs the failure
    so callers (and Cloud Logging) can see it instead of having it silently
    swallowed.
    """
    backoff = 1.0
    last_err = None
    for attempt in range(1, max_attempts + 1):
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {os.environ['BC_ACCESS_TOKEN']}",
            "User-Agent": USER_AGENT,
        })
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read()), resp.headers.get("Link", "")
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code in _BC_RETRY_STATUSES and attempt < max_attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            print(f"bc fetch FAILED ({last_err}, attempt {attempt}/{max_attempts}): {url}")
            return None, ""
        except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as e:
            last_err = type(e).__name__
            if attempt < max_attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            print(f"bc fetch FAILED ({last_err}, attempt {attempt}/{max_attempts}): {url}")
            return None, ""
    print(f"bc fetch EXHAUSTED ({last_err}): {url}")
    return None, ""


def bc_get(path, params=None):
    url = path if path.startswith("http") else f"{BC_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return _bc_request_raw(url)


def bc_get_data(path, params=None):
    """Fetch a single page, return just the data."""
    data, _ = bc_get(path, params)
    return data


def bc_get_all(path, params=None, max_pages=10):
    """Fetch all pages of a paginated BC3 endpoint.

    Returns a list, or whatever the endpoint returns if it's not a list. On
    transient failure mid-pagination we surface the failure rather than
    returning a half-populated list — partial data is the bug we're guarding
    against.
    """
    import re
    results = []
    url = (path if path.startswith("http") else f"{BC_BASE}{path}")
    if params:
        url += "?" + urllib.parse.urlencode(params)

    for _ in range(max_pages):
        data, link_header = _bc_request_raw(url)
        if data is None:
            # Transient failure even after retries. Returning [] would look
            # identical to "endpoint legitimately empty" downstream and trigger
            # phantom flags. Re-raise so the caller knows to skip this project
            # rather than report incomplete data.
            raise RuntimeError(f"bc_get_all could not complete fetch: {url}")

        if isinstance(data, list):
            results.extend(data)
        else:
            return data  # not a list, just return as-is

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


def classify_project_type(name):
    """Return 'Design Contract', 'Sales', or 'Standard Project'.

    Sales projects are large, pre-contract opportunities still in sales
    engineering. Like Design Contracts, they use the standard project
    template but most SOPs don't apply yet — Rick should ONLY flag
    hanging conversations on them.
    """
    n = name or ""
    if "(Design Contract)" in n:
        return "Design Contract"
    if "(Sales)" in n:
        return "Sales"
    return "Standard Project"


def fetch_todos_for_project(proj):
    """Returns (schedule_todos, labor_todos, all_todos, client_visibility_issues, fetch_incomplete).

    `fetch_incomplete=True` means at least one todoset or todolist read failed
    after all retries. When that's the case the caller should NOT report
    "no labor", "list missing", or "all clear" for this project — the
    underlying data is partial.
    """
    import re as _re
    proj_id = proj["id"]
    proj_name = proj["name"]
    fetch_incomplete = False
    todoset_tools = [d for d in proj.get("dock", [])
                     if d.get("name") == "todoset" and d.get("enabled")]
    if not todoset_tools:
        return [], [], [], [], False

    def _fetch_todoset_lists(ts):
        try:
            return ts, bc_get_all(f"/buckets/{proj_id}/todosets/{ts['id']}/todolists.json"), False
        except RuntimeError as e:
            print(f"todoset fetch failed for project {proj_id} todoset {ts.get('id')}: {e}")
            return ts, [], True

    todolists = []
    with ThreadPoolExecutor(max_workers=TODOLIST_FETCH_WORKERS) as pool:
        for ts, tl, failed in pool.map(_fetch_todoset_lists, todoset_tools):
            if failed:
                fetch_incomplete = True
                continue
            for t in (tl or []):
                t["_todoset_title"] = ts.get("title", "")
            todolists.extend(tl or [])
    if not todolists:
        return [], [], [], [], fetch_incomplete

    # Each todolist carries its own `visible_to_clients` flag — that's the
    # source of truth. (We previously hit /buckets/{id}/client/recordings.json
    # which 404s, causing every required list to false-flag as not-visible.)
    # If fetch_incomplete is already True, skip the visibility check — we
    # might be missing the very todoset that holds the required list.
    existing_list_names = {tlist.get("name", "") for tlist in todolists}
    client_visibility_issues = []
    if not fetch_incomplete:
        for required_name in REQUIRED_CLIENT_VISIBLE_LISTS:
            if required_name not in existing_list_names:
                client_visibility_issues.append({
                    "project": proj_name,
                    "list": required_name,
                    "issue": "missing_list",
                })
            else:
                tlist_obj = next((t for t in todolists if t.get("name") == required_name), None)
                if tlist_obj and not tlist_obj.get("visible_to_clients"):
                    client_visibility_issues.append({
                        "project": proj_name,
                        "list": required_name,
                        "issue": "not_client_visible",
                        "app_url": tlist_obj.get("app_url"),
                    })

    def _fetch_list_todos(tlist):
        lid = tlist["id"]
        try:
            open_t = bc_get_all(f"/buckets/{proj_id}/todolists/{lid}/todos.json",
                                {"completed": "false"}) or []
            done_t = bc_get_all(f"/buckets/{proj_id}/todolists/{lid}/todos.json",
                                {"completed": "true"}) or []
            return tlist, open_t, done_t, False
        except RuntimeError as e:
            print(f"todolist fetch failed for project {proj_id} list {lid}: {e}")
            return tlist, [], [], True

    schedule_todos, labor_todos, all_todos = [], [], []
    with ThreadPoolExecutor(max_workers=TODOLIST_FETCH_WORKERS) as pool:
        list_results = list(pool.map(_fetch_list_todos, todolists))

    for tlist, open_todos, done_todos, list_failed in list_results:
        if list_failed:
            fetch_incomplete = True
            continue
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
                "description": raw_desc[:300],
            }
            all_todos.append(entry)
            if any(tag in title for tag in SCHED_TAGS):
                schedule_todos.append(entry)
            if "[LABOR]" in title:
                # Labor todos keep their full description so booking info
                # (Flights/Hotel/Per-Diem/Car Rental) survives truncation.
                labor_entry = {**entry, "description": raw_desc[:1200]}
                labor_todos.append(labor_entry)

    return schedule_todos, labor_todos, all_todos, client_visibility_issues, fetch_incomplete


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
            content = _re.sub(r'<[^>]+>', ' ', (msg.get("content") or "")).strip()[:600]
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
                c_content = _re.sub(r'<[^>]+>', ' ', (comment.get("content") or "")).strip()[:400]
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
        content = _re.sub(r'<[^>]+>', ' ', (fwd.get("content") or "")).strip()[:600]
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
            c_content = _re.sub(r'<[^>]+>', ' ', (comment.get("content") or "")).strip()[:400]
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


def drive_get_parent_chain(svc, folder_id, max_depth=10):
    """Walk up from folder_id, return list of (id, name) leaf-first."""
    chain = []
    current = folder_id
    for _ in range(max_depth):
        try:
            f = svc.files().get(
                fileId=current, fields="id,name,parents",
                supportsAllDrives=True,
            ).execute()
        except Exception as e:
            print(f"Drive parent walk failed at {current}: {type(e).__name__}: {e}")
            break
        chain.append((f["id"], f.get("name", "?")))
        parents = f.get("parents") or []
        if not parents:
            break
        current = parents[0]
    return chain


DRIVE_JOBS_ROOT_NAMES = ("Skylark Jobs",)
DRIVE_NESTED_PENALTY_TOKENS = (
    "vendor docs", "damaged", "archive", "archived", "old", "backup",
    "install share", "share folder", "shared",
)


def drive_find_project_folder(svc, sky_id):
    """Find the SKY-XXXX project root folder.

    Strategy: search all of Drive for folders whose name contains the SKY-id,
    then for each candidate walk its parent chain. Project root folders sit
    directly under "Skylark Jobs > <Client>" — i.e. the SHALLOWEST candidate
    that's a child of a "Skylark Jobs" ancestor wins. Deep matches (under
    Vendor Docs, Damaged Product, Install Share Folder, etc.) are penalized.
    """
    q = f"mimeType='{FOLDER_MIME}' and name contains '{sky_id}' and trashed=false"
    try:
        res = svc.files().list(
            q=q, fields="files(id,name,parents,modifiedTime)",
            supportsAllDrives=True, includeItemsFromAllDrives=True,
            corpora="allDrives", pageSize=20,
        ).execute()
        files = res.get("files") or []
    except Exception as e:
        print(f"Drive search {sky_id} failed: {type(e).__name__}: {e}")
        return []

    if not files:
        print(f"Drive {sky_id}: 0 candidates")
        return []

    print(f"Drive {sky_id}: {len(files)} candidate(s)")

    under_root, outside_root = [], []
    for f in files:
        chain = drive_get_parent_chain(svc, f["id"])
        f["_chain"] = chain
        path_str = " > ".join(n for _, n in reversed(chain))
        f["_path"] = path_str
        f["_depth"] = max(len(chain) - 1, 0)
        ancestor_ids = {fid for fid, _ in chain}
        ancestor_names_lower = {n.lower() for _, n in chain}
        path_lower = path_str.lower()
        nested_penalty = sum(1 for tok in DRIVE_NESTED_PENALTY_TOKENS if tok in path_lower)
        f["_nested_penalty"] = nested_penalty
        in_jobs_root = (
            DRIVE_JOBS_ROOT_ID in ancestor_ids
            or any(n.lower() in ancestor_names_lower for n in DRIVE_JOBS_ROOT_NAMES)
        )
        bucket = under_root if in_jobs_root else outside_root
        bucket.append(f)
        marker = "ok " if in_jobs_root else "off"
        print(f"  {marker} depth={f['_depth']} pen={nested_penalty} {f['name']!r}  path={path_str}")

    pool = under_root or outside_root
    if not under_root:
        print(f"Drive {sky_id}: no hit under jobs root, falling back to global")

    ranked = sorted(pool, key=lambda f: (
        f.get("_nested_penalty", 0),
        f.get("_depth", 99),
        0 if f["name"].upper().startswith(sky_id.upper()) else 1,
        len(f["name"]),
    ))
    print(f"Drive {sky_id} chose: {ranked[0]['name']!r}  path={ranked[0].get('_path','?')}")
    return ranked


# Backwards-compatible alias (older callers)
drive_find_install_share = drive_find_project_folder


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
    drive_path = project_folder.get("_path") or project_folder["name"]
    drive_url = f"https://drive.google.com/drive/folders/{project_folder_id}"
    chain = project_folder.get("_chain", [])
    chain_ids = {fid for fid, _ in chain}
    chain_names_lower = {n.lower() for _, n in chain}
    under_root = (
        DRIVE_JOBS_ROOT_ID in chain_ids
        or any(n.lower() in chain_names_lower for n in DRIVE_JOBS_ROOT_NAMES)
    )
    if not under_root:
        issues.append("drive_folder_outside_jobs_root")

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
        "drive_path": drive_path,
        "drive_url": drive_url,
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


def fetch_basecamp_data(mode="briefing", project_query=None):
    print(f"Fetching Basecamp data (mode={mode})...")

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

    # Per-project data
    all_schedule_todos, all_labor_todos, all_todos = [], [], []
    all_messages, all_cards, all_client_visibility_issues = [], [], []
    project_summaries = []
    projects_to_fetch = sky_projects[:1] if mode == "deep_dive" else sky_projects

    def _fetch_project_bundle(proj):
        try:
            sched_t, labor_t, proj_t, vis, todos_incomplete = fetch_todos_for_project(proj)
        except Exception as e:
            print(f"_fetch_project_bundle todos failed for {proj['id']}: {type(e).__name__}: {e}")
            sched_t, labor_t, proj_t, vis, todos_incomplete = [], [], [], [], True

        try:
            msgs = fetch_messages_for_project(proj)
        except Exception as e:
            print(f"_fetch_project_bundle messages failed for {proj['id']}: {type(e).__name__}: {e}")
            msgs = []

        try:
            emails = fetch_inbox_forwards_for_project(proj)
        except Exception as e:
            print(f"_fetch_project_bundle inbox failed for {proj['id']}: {type(e).__name__}: {e}")
            emails = []

        try:
            cards_p = fetch_cards_for_project(proj)
        except Exception as e:
            print(f"_fetch_project_bundle cards failed for {proj['id']}: {type(e).__name__}: {e}")
            cards_p = []

        sched_entries = []
        sched_tool = get_dock_tool(proj, "schedule")
        if sched_tool:
            try:
                sched_entries = bc_get_all(
                    f"/buckets/{proj['id']}/schedules/{sched_tool['id']}/entries.json"
                ) or []
            except RuntimeError as e:
                print(f"_fetch_project_bundle schedule entries failed for {proj['id']}: {e}")
                sched_entries = []
            for e in sched_entries:
                e["_project_name"] = proj["name"]
        return proj, sched_t, labor_t, proj_t, vis, msgs, emails, cards_p, sched_entries, todos_incomplete

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=PROJECT_FETCH_WORKERS) as pool:
        bundles = list(pool.map(_fetch_project_bundle, projects_to_fetch))
    print(f"Fetched {len(bundles)} projects in {(time.perf_counter() - t0):.1f}s")

    schedule_entries = []
    incomplete_fetches = []
    cutoff_msg = cutoff_done = None
    if mode == "briefing":
        # 14 days of messages is enough for "open thread" analysis. 30 days
        # was producing 2.9MB bundles that overflowed the 800K char limit.
        cutoff_msg = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
        cutoff_done = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    for proj, sched_todos, labor_todos, proj_todos, client_vis_issues, messages, email_forwards, cards, sched_entries, fetch_incomplete in bundles:
        desc = proj.get("description", "")
        proj_type = classify_project_type(proj.get("name", ""))
        project_summaries.append({
            "id": proj["id"],
            "name": proj["name"],
            "description": desc[:500],
            "type": proj_type,
            "app_url": proj.get("app_url"),
            "fetch_incomplete": fetch_incomplete,
        })
        if fetch_incomplete:
            incomplete_fetches.append({
                "project": proj["name"],
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
        # Visibility flags don't apply to pre-execution projects (Design
        # Contract or Sales) — those use the same template but aren't
        # expected to be SOP-compliant yet.
        if proj_type == "Standard Project":
            all_client_visibility_issues.extend(client_vis_issues)

    drive_compliance = audit_drive_for_projects(projects_to_fetch)

    # IMPORTANT: field order matters. The data bundle gets serialized to JSON
    # and may be truncated at ~800k chars before the LLM sees it. Small,
    # high-signal "anchor" fields go FIRST so they always survive truncation.
    # Bulk content (all_todos, messages_and_comments) goes LAST so it's the
    # first thing dropped when the bundle gets too big.
    return {
        "mode": mode,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "project_summaries": project_summaries,
        "incomplete_fetches": incomplete_fetches,
        "client_visibility_issues": all_client_visibility_issues,
        "labor_todos": all_labor_todos,
        "schedule_tagged_todos": all_schedule_todos,
        "upcoming_schedule_entries": schedule_entries,
        "drive_compliance": drive_compliance,
        "cards": all_cards,
        "all_todos": all_todos,
        "messages_and_comments": all_messages,
    }


# ── SOP context ────────────────────────────────────────────────────────────────

SKYLARK_SOP_CONTEXT = """
## Skylark AV Operations Standards

### Project Types
Skylark has three types of SKY- projects:
- **Design Contract** — pre-execution, design/engineering phase only. Project name includes "(Design Contract)". TBD fields are acceptable. Full SOP compliance (install scheduling, labor, procurement) is NOT expected.
- **Sales** — pre-contract, large opportunity in sales engineering. Project name includes "(Sales)". Uses the standard project template but most SOPs do NOT apply until contract is signed and the (Sales) tag is removed. The ONLY thing to flag on a (Sales) project is *hanging conversations* — a question or request from the client (or internal team) in `messages_and_comments` that has no reply for 3+ days. Do NOT flag missing schedule, missing labor, missing client visibility, missing dates, or any other SOP item on (Sales) projects.
- **Standard Project** — full execution project. All description fields required, no TBDs, full SOP applies.

### Project Description (Required Fields)
Every active SKY- project must have exactly these 5 fields in its description:
- Client Contact
- Job Location
- Skylark PM
- Engineer
- On-Site Lead
No dates are expected in the description. Dates live in schedule-tagged todos only.
TBD is only acceptable for Design Contract and (Sales) projects (not Standard Projects).

### Schedule Tag System
[PM-SCHED] [ENG-SCHED] [PROC-SCHED] [SHOP-SCHED] [LOG-SCHED] [ONS-SCHED] [COM-SCHED] [FUT-SCHED]
CRITICAL: Any incomplete schedule-tagged todo WITHOUT a due_on date = missing_dates flag.

### Date is the source of truth (CRITICAL — read carefully before reporting)
The `due_on` and `starts_on` fields are the source of truth for whether a milestone is scheduled. Title text — including placeholders like `(Trip #)`, `(TBD)`, `(Trip ?)` — does NOT override the date fields.

When a todo has a real `due_on` (a parseable ISO date), the milestone IS scheduled. Report it as scheduled, using the actual dates. If the title also contains a placeholder, raise that as a SEPARATE, smaller flag — never as a reason to say the milestone "isn't confirmed" or "has no date."

Wrong: "No install date confirmed in Basecamp [ONS-SCHED] yet" (when due_on = 2026-06-21 is set, even if title says "Trip #").
Right: "Install scheduled 6/8 – 6/21 [ONS-SCHED]. Trip number placeholder still in title (`Trip #`) — minor cleanup needed."

The same applies to any tagged todo: if the date is real, the date is real. Flag the title hygiene separately, with proportional severity.

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
[LABOR] todos live in the **Labor Scheduling** todoset. Canonical title format: `Name | Role | Status [LABOR]`. Canonical description fields: Flights, Hotel, Per-Diem, Car Rental.

EVERY confirmed [ONS-SCHED] onsite trip MUST have corresponding [LABOR] todos documenting who is going onsite for that trip. This is non-negotiable — onsite work without documented labor = SOP violation.

Rule: for each [ONS-SCHED] trip with a real due date, there must be at least one [LABOR] todo in the Labor Scheduling todoset that covers that trip's date range. Tie the labor to the trip by date proximity (the [LABOR] dates should overlap or align with the [ONS-SCHED] trip date). A todo counts as a [LABOR] todo if its title contains the literal string `[LABOR]` — even if the rest of the title doesn't match the canonical format.

CRITICAL — distinguish ABSENT vs INCOMPLETE labor (Rick's most common misreport):
Never say "no [LABOR] todos exist" when one is actually present, even if it's incomplete. The three states are distinct flags with distinct severity, and you must pick the correct one:

1. **ABSENT** (highest severity, SOP violation):
   No [LABOR] todo with date overlap exists for this trip at all.
   Wording: "No [LABOR] todo exists for the {date range} install trip — SOP violation."

2. **PRESENT-BUT-INCOMPLETE** (flag, not violation):
   A [LABOR] todo with date overlap exists but is missing required fields. Always lead with what IS there before naming what's missing. Specific sub-flags (use the exact wording that fits):
   - "[LABOR] todo present (`{title}`) but no assignee set"
   - "[LABOR] todo present (`{title}`) but description missing booking info (Flights/Hotel/Per-Diem/Car Rental). Trip is {N} days out — needs to be filled in."
   - "[LABOR] todo present (`{title}`) but title doesn't follow `Name | Role | Status [LABOR]` format — minor cleanup."
   - "[LABOR] todo present (`{title}`) but missing due_on or starts_on date."

3. **COMPLETE**: assignee + booking info filled in + dates set. No flag.

Reporting wording — never use these phrases when a [LABOR] todo is actually present:
- ❌ "no labor exists"
- ❌ "no [LABOR] todos for this trip"
- ❌ "labor not documented"

Use those phrases ONLY for state #1 (truly absent).

Triage thresholds for booking info: missing booking info is a flag whenever the trip is within 14 days. Outside 14 days it's a low-priority cleanup note.

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

ALWAYS check Drive when you discuss a project's status. Drive holds the contract, engineering drawings, vendor quotes, and field photos that Basecamp doesn't — a status read with no Drive context is incomplete. If `drive_compliance` is in the bundle, use it. If you're answering interactively and you don't have it, call `get_drive_compliance` for the project first; if that returns `no_drive_folder_found` or looks wrong, call `find_drive_folder` to see every candidate folder and where it sits in Drive — that tells you whether the folder is genuinely missing, mis-named, or living outside the Skylark Jobs root.

The data bundle includes `drive_compliance` per project. Each entry has:
- `drive_project_folder` — name of the resolved project folder
- `drive_path` — full Drive path from the root (e.g. "Skylark Jobs > Lakepointe > SKY-2429 ...")
- `drive_url` — direct link to the project folder
- `missing_top_folders` / `missing_nested_folders` — deterministic misses after alias matching
- `empty_top_folders` — folder exists but has no contents
- `matched_aliases` — where an alias was used (e.g. "Insurance Documents" matched "Insurance Docs")
- `tree` — the actual folder + file structure (2-3 levels deep). Use this to reason beyond the template.
- `days_since_drive_modified` — staleness signal
- `issues` may include `drive_folder_outside_jobs_root` (folder name matches but lives somewhere outside Skylark Jobs — usually means a duplicate/orphan folder)

When linking to the Drive folder for Tyler, render it as a Slack link: `<DRIVE_URL|Drive folder>`.

REAL-WORLD RULE: The template is a guide, not a contract. Before flagging something missing, look at `tree` and ask "is there functional evidence this requirement is met?" Examples:
- Template wants `Signed Contracts/` but `tree` shows `Contract Revisions/` contains `Contract SEC Video System.pdf` and a `Signed Documents/` subfolder — that IS the signed contract on file. Don't flag.
- Template wants `Insurance Documents/` but `tree` shows `Insurance Docs/` with a COI PDF inside — same thing. Don't flag.
- Template wants `Onsite Pictures/` but `tree` shows empty `Onsite Photos/` — flag as empty if the project is past the onsite phase.
- Template wants `Signed Contracts/` and NOTHING in `tree` looks like a contract document — flag it, this is a real gap.

Cross-reference against Basecamp phase. Missing a signed contract on a design-phase project is normal. Missing it on a project already in onsite phase is a red flag.
"""


# ── Claude prompts ────────────────────────────────────────────────────────────

REPORTING_DISCIPLINE = """### Reporting discipline (read before flagging anything)

These rules override anything else in this prompt. Before flagging an issue, the data must literally show it. Do not infer the absence of something from the absence of context — silence is not evidence.

1. **Client visibility — only flag if the array shows it.** A required list is "not client visible" if and only if `client_visibility_issues` contains an entry for that project + list. Empty array for a project = no flag, period. Do not infer client visibility from the project's phase, type, or any other signal. If you say "client can't see Onsite Phase," cite the matching `client_visibility_issues` entry.

2. **Schedule presence lives in TWO places.** A project HAS a schedule if either `schedule_tagged_todos` OR `upcoming_schedule_entries` contains an entry for it. Before saying "no install date" or "no schedule," check BOTH. An [ONS-SCHED] todo with a real `due_on` (or `starts_on`) IS a confirmed install date even if the title contains placeholders like `(Trip #)`.

3. **Read message threads as conversations.** Items in `messages_and_comments` with the same `parent_title` belong to the same thread — original message + replies. A reply that answers the question = resolved. Do NOT flag as "unanswered" if a later comment provides the answer. When summarizing a project's communication state, pair every flagged "open question" with what would resolve it; if a reply already does, drop the flag.

4. **Incomplete fetches are not "all clear."** If a project's `project_summaries` entry has `fetch_incomplete: true`, the underlying data is partial. Do NOT analyze its content as if it were complete. Surface it in a separate "Data fetch incomplete" line so Tyler knows to retry, but never report "no labor" / "no schedule" / "all clear" on a project with `fetch_incomplete: true`.
"""


SLACK_FORMATTING = """### Slack formatting rules (your output goes to Slack as mrkdwn — NOT standard Markdown, NOT HTML)

Your output is posted to Slack. Slack's mrkdwn syntax is its own thing — it is NOT GitHub Markdown and it is NOT HTML. Standard Markdown and HTML render as LITERAL TEXT.

ABSOLUTE PROHIBITIONS — these MUST NOT appear anywhere in your output:
- No HTML. The character sequence `<a ` (opening anchor tag) must NEVER appear. No `<p>`, `<div>`, `<span>`, `<br>`, `<strong>`, `<em>`, etc. The ONLY use of `<` in your output is the start of a Slack link `<URL|text>`.
- No `**double asterisks**` for bold — Slack prints them literally. Use `*single asterisks*`.
- No `[text](URL)` — that's GitHub Markdown and prints literal brackets. Use `<URL|text>`.
- No `# Header` / `## Header` / `### Header` — Slack has no header syntax. Use `*Bold*` on its own line.
- No URL-encoding of the `|` separator inside a Slack link. The pipe is a literal `|` character — never `%7C`.

The CORRECT Slack mrkdwn syntax:

| Element | Right | Wrong |
|---------|-------|-------|
| Bold | `*text*` | `**text**` |
| Italic | `_text_` | `*text*` |
| Strike | `~text~` | `~~text~~` |
| Inline code | `` `text` `` | same |
| Code block | triple backticks | same |
| Link | `<https://example.com|display>` | `[display](url)` or `<a href="url">display</a>` |
| Bullet | `• item` or `- item` | `* item` (renders literal `*`) |
| Quote | `> text` | same |
| Emoji | `:red_circle:` | same |
| Header | `*Section Name*` on its own line | `## Section Name` |

Project links — the rule that matters most: when you reference a SKY project, render it as `<APP_URL|SKY-XXXX>` using the project's `app_url` from `project_summaries`. Concrete example with the correct shape:

  Right: `<https://3.basecamp.com/4358663/projects/46926746|SKY-2647>`
  Wrong: `<a href="https://3.basecamp.com/4358663/projects/46926746|SKY-2647">SKY-2647</a>`
  Wrong: `[SKY-2647](https://3.basecamp.com/4358663/projects/46926746)`
  Wrong: `<https://3.basecamp.com/4358663/projects/46926746%7CSKY-2647>`

Per-line example showing all the rules together:
  Right: `:red_circle: *SKY-2429* — install <https://3.basecamp.com/4358663/buckets/45754447/todos/123|5/19> blocked: missing GO/NO-GO`
  Wrong: `🔴 **SKY-2429** — install [5/19](https://3.basecamp.com/4358663/buckets/45754447/todos/123) blocked: missing GO/NO-GO`

If you ever feel tempted to write `<a href` — STOP. That's HTML. Convert it to `<URL|text>` Slack syntax instead.
"""


def _render_anchor_block(data_bundle):
    """Build a small, must-read summary that lives at the top of the prompt.

    Truncation can drop fields buried in the JSON, and Claude tends to
    hallucinate flags when the data looks "incomplete." Pre-rendering the
    structured anchors as a clean Markdown block above the JSON forces him
    to see them and uses literal data — not pattern matching — to flag.
    """
    vis = data_bundle.get("client_visibility_issues") or []
    incomplete = data_bundle.get("incomplete_fetches") or []
    labor = data_bundle.get("labor_todos") or []
    summaries = data_bundle.get("project_summaries") or []

    # Bucket projects by type so the prompt can scope its rules properly.
    sales_projects = [s for s in summaries if s.get("type") == "Sales"]
    design_projects = [s for s in summaries if s.get("type") == "Design Contract"]
    standard_projects = [s for s in summaries if s.get("type") == "Standard Project"]

    # Group visibility issues by project.
    vis_by_proj = {}
    for v in vis:
        vis_by_proj.setdefault(v.get("project", "?"), []).append(
            f"{v.get('list','?')} ({v.get('issue','?')})"
        )

    # Group labor by project.
    labor_by_proj = {}
    for l in labor:
        labor_by_proj.setdefault(l.get("project", "?"), []).append(
            l.get("title", "?")
        )

    lines = []
    lines.append("## Anchor flags (READ THIS BEFORE ANY VISIBILITY/LABOR JUDGMENT)")
    lines.append("")
    lines.append("These are the ONLY pre-computed flag arrays. If a project name does not appear in `client_visibility_issues` below, its required client-visible lists ARE visible — do NOT flag it. Same for `incomplete_fetches`: only those projects had partial data.")
    lines.append("")
    if vis_by_proj:
        lines.append("### client_visibility_issues (these projects have a real visibility problem)")
        for proj, issues in sorted(vis_by_proj.items()):
            lines.append(f"- {proj}: {', '.join(issues)}")
    else:
        lines.append("### client_visibility_issues: EMPTY")
        lines.append("No project has a client-visibility problem. Do NOT flag any project for visibility.")
    lines.append("")
    if incomplete:
        lines.append("### incomplete_fetches (data partial — surface, don't analyze)")
        for entry in incomplete:
            lines.append(f"- {entry.get('project','?')}")
    else:
        lines.append("### incomplete_fetches: EMPTY")
        lines.append("All project data fetched cleanly.")
    lines.append("")
    if labor_by_proj:
        lines.append(f"### labor_todos: {len(labor)} todo(s) across {len(labor_by_proj)} project(s)")
        for proj, titles in sorted(labor_by_proj.items()):
            lines.append(f"- {proj}: {len(titles)} labor todo(s) — {', '.join(titles[:3])}")
    else:
        lines.append("### labor_todos: NONE found across the account")
    lines.append("")
    lines.append(f"### Project count: {len(summaries)} active — {len(standard_projects)} Standard, {len(design_projects)} Design Contract, {len(sales_projects)} Sales")
    if sales_projects:
        lines.append("")
        lines.append("### Sales projects (apply ONLY the hanging-conversation rule, no other SOP checks)")
        for s in sales_projects:
            lines.append(f"- {s.get('name','?')}")
    if design_projects:
        lines.append("")
        lines.append("### Design Contract projects (pre-execution — TBD fields OK, no install SOP yet)")
        for s in design_projects:
            lines.append(f"- {s.get('name','?')}")
    return "\n".join(lines)


def analyze_with_claude(anthropic_client, data_bundle):
    mode = data_bundle.get("mode", "briefing")
    anchor_block = _render_anchor_block(data_bundle) if mode == "briefing" else ""
    full_data_json = json.dumps(data_bundle, indent=2)
    # ~800k chars is roughly 200k tokens — comfortably inside Opus 4.7's 1M
    # context but leaves room for the prompt itself and the response budget.
    DATA_LIMIT = 800000
    truncated = len(full_data_json) > DATA_LIMIT
    data_json = full_data_json[:DATA_LIMIT]
    if truncated:
        data_json += "\n\n[DATA TRUNCATED — original was {full} chars, kept {kept}. NOTE: the anchor block above is COMPLETE — flag decisions must come from there, not from inferring what's missing here.]".format(
            full=len(full_data_json), kept=DATA_LIMIT)
    print(f"analyze_with_claude: mode={mode} data_chars_full={len(full_data_json)} kept={len(data_json)} truncated={truncated}")

    if mode == "briefing":
        prompt = f"""You are Rick Stamen, the PM agent for Skylark AV. Generate a morning briefing for Tyler (founder/owner).

{SKYLARK_SOP_CONTEXT}

{REPORTING_DISCIPLINE}

{SLACK_FORMATTING}

{anchor_block}

Today is {data_bundle['as_of'][:10]}.

The data below includes EVERY active SKY project with ALL todos (all_todos), ALL messages and comments (messages_and_comments), ALL cards (cards), `upcoming_schedule_entries` (Basecamp Schedule dock — events/calendar entries), labor todos, AND `drive_compliance` (Google Drive job folder audit per project). Use all of it. Drive is a primary data source — not optional context.

Review all active project data and produce the briefing.

Project type rules (apply per-project before flagging):
- **Standard Project** — full SOP applies. All sections below apply.
- **Design Contract** — pre-execution. Skip install/labor/visibility/schedule-tag SOP checks. Flag only: missing description fields, hanging conversations, or stalled engineering with no recent activity.
- **Sales** — pre-contract opportunity in sales engineering. Apply ONLY the hanging-conversation rule below. Do NOT flag missing schedule, missing labor, missing client visibility, missing dates, or any other SOP item. Sales projects don't have an install yet — silence on those is normal.

Hanging conversation rule (applies to ALL three project types): a question or request from the client OR an internal teammate in `messages_and_comments` that has no reply for 3+ days is "hanging." Cite the parent_title and the asker. Skip if a later comment in the same thread answers it.

Structure:
1. *One-line summary* — e.g. `47 active jobs — 5 need attention, 1 fetch incomplete`
2. *Needs attention* — jobs with real issues (each with `:red_circle:` or `:large_yellow_circle:`, specific todo/person/date)
3. *Data fetch incomplete* (only if `incomplete_fetches` is non-empty) — list those projects, do NOT analyze them
4. *Sales watch* (only if any (Sales) projects exist) — for each (Sales) project: `:white_check_mark:` if no hanging conversation, `:large_yellow_circle:` if there is one (cite parent_title, asker, age in days). Nothing else.
5. *All clear* — `:white_check_mark:` per project, name + current phase, one short line each
6. *Drive Health* — projects with missing top-level folders, empty required folders, `no_drive_folder_found`, or `drive_folder_outside_jobs_root`. Use the `tree` to avoid false positives (alias matches, signed contracts in Contract Revisions/Signed Documents, etc.). Render Drive folder as `<DRIVE_URL|Drive>`.
7. *This Week* — milestones due in the next 7 days across all jobs (Standard Projects only — Sales/Design Contract milestones aren't tracked here)

Severity emoji: `:red_circle:` for high, `:large_yellow_circle:` for medium, `:white_check_mark:` for clear.

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

{REPORTING_DISCIPLINE}

{SLACK_FORMATTING}

As of {data_bundle['as_of'][:10]}.

The data below includes EVERY todo, message, comment, card, and schedule entry for this project. Use all of it — don't skip anything.

Cover (each as its own `*Section*` line):
1. Project description fields (PM, Engineer, On-Site Lead, Client Contact)
2. Current active phase (based on incomplete schedule-tagged todos with due dates)
3. All open todos by list — who owns what, what's overdue, what's missing dates
4. Upcoming milestones (next 30 days)
5. Labor/travel status (any [LABOR] todos and their travel details)
6. Card table status (if present — what's in each column, anything blocked or stale)
7. Full message/comment thread review — tone, open questions, anything unresolved
8. Any SOP violations or engineering milestone flags
9. Drive folder review — use `drive_compliance`. Confirm the resolved `drive_path`, link the `drive_url` as `<DRIVE_URL|Drive folder>`, call out missing/empty required folders, stale activity, or "Damaged Product"-style sub-folders that hint at incidents. Use the `tree` to spot real evidence. Cross-reference Drive activity against Basecamp phase.
10. Overall health: `:large_green_circle: GREEN` / `:large_yellow_circle: YELLOW` / `:red_circle: RED` with one-line reason

Be thorough — Tyler wants the full picture.

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

    elif mode == "drive_audit":
        prompt = f"""You are Rick Stamen, the PM agent for Skylark AV. Tyler is asking for a Google Drive audit across every active SKY project.

{SKYLARK_SOP_CONTEXT}

{SLACK_FORMATTING}

As of {data_bundle['as_of'][:10]}.

The data below contains every active SKY project's `drive_compliance` entry. Each has the resolved `drive_path`, `drive_url`, missing folders, empty folders, days-since-modified, and a 3-level `tree` of actual contents.

Produce a report focused ENTIRELY on Drive health. Do NOT discuss Basecamp todos, messages, or schedule — this is Drive-only.

Structure:
1. One-line summary: e.g. `47 jobs audited — 6 need attention`
2. *Critical* — projects with `no_drive_folder_found`, `drive_folder_outside_jobs_root`, or missing phase-required top-level folders
3. *Empty / Stale* — required folders present but empty past their phase, or folders untouched for 30+ days on active jobs
4. *Naming / Layout Issues* — folders not following `Skylark Jobs > Client > SKY-XXXX | Job Name`, or sub-folders the ranker had to filter (Install Share Folder, Damaged Product, etc.)
5. *All Clear* — healthy Drive folders, one short line each

Severity emoji: `:red_circle:` critical, `:large_yellow_circle:` medium, `:white_check_mark:` clear. Render every project as `<APP_URL|SKY-XXXX>`. Render the Drive folder as `<DRIVE_URL|Drive>`.

Apply the REAL-WORLD RULE — use `tree` to avoid false positives. `Insurance Docs/` with a COI satisfies `Insurance Documents/`; a signed contract in `Contract Revisions/Signed Documents/` satisfies `Signed Contracts/`. Don't flag template-name mismatches when functional evidence is there. Cross-reference Basecamp phase from `project_summaries` — missing onsite photos on a design-phase project is normal.

--- DATA ---
{data_json}
"""
        try:
            response = anthropic_client.messages.create(
                model="claude-opus-4-7", max_tokens=8000,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            print(f"Anthropic error (drive_audit): {type(e).__name__}: {e}")
            raise
        return {"type": "drive_audit", "text": response.content[0].text.strip()}

    raise ValueError(f"Unknown analyze_with_claude mode: {mode!r}")


# ── Slack posting ──────────────────────────────────────────────────────────────


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


def post_freeform_to_slack(slack_client, channel_id, text, fallback="Rick Stamen update", thread_ts=None):
    chunks = split_for_slack(text)
    for i, chunk in enumerate(chunks):
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": chunk}}]
        kwargs = {
            "channel": channel_id,
            "blocks": blocks,
            "text": fallback if i == 0 else f"{fallback} (cont.)",
        }
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        slack_client.chat_postMessage(**kwargs)


# ── Public API (used by webhook and job) ──────────────────────────────────────

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


def run_drive_audit():
    """Drive-only audit across every active SKY project."""
    load_env()
    load_secrets_from_gcp()
    if token_needs_refresh():
        refresh_bc_token()

    projects = bc_get_all("/projects.json") or []
    sky = fetch_active_sky_projects(projects)
    project_summaries = [{
        "id": p["id"], "name": p["name"], "app_url": p.get("app_url"),
        "type": classify_project_type(p.get("name", "")),
    } for p in sky]
    drive_compliance = audit_drive_for_projects(sky)

    bundle = {
        "mode": "drive_audit",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "project_summaries": project_summaries,
        "drive_compliance": drive_compliance,
    }
    result = analyze_with_claude(
        anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"]), bundle
    )
    return result.get("text", "Drive audit produced no output.")


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

    mode = os.environ.get("RUN_MODE", "briefing")
    channel_id = os.environ["SLACK_CHANNEL_ID"]
    thread_ts = os.environ.get("SLACK_THREAD_TS") or None
    slack_client = WebClient(token=os.environ["SLACK_TOKEN"])

    if mode == "briefing":
        print("Running briefing...")
        text = run_briefing()
        post_freeform_to_slack(slack_client, channel_id, text, "Skylark PM Briefing", thread_ts=thread_ts)
        print("Briefing posted.")

    elif mode == "deep_dive":
        project_query = os.environ.get("PROJECT_QUERY", "")
        print(f"Running deep dive: {project_query}")
        text = run_deep_dive(project_query)
        post_freeform_to_slack(slack_client, channel_id, text, f"Deep Dive: {project_query}", thread_ts=thread_ts)
        print("Deep dive posted.")

    elif mode == "drive_audit":
        print("Running drive audit across active SKY projects...")
        text = run_drive_audit()
        post_freeform_to_slack(slack_client, channel_id, text, "Drive Audit", thread_ts=thread_ts)
        print("Drive audit posted.")

    else:
        print(f"ERROR: unknown RUN_MODE={mode!r}. Expected briefing | deep_dive | drive_audit.")
        sys.exit(1)

    print("Done.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Rick Stamen conversational chat handler.
Claude tool loop scoped to #pm-watch channel. Read-only tools today;
structure leaves a clear place to add write tools later.
"""

import json
import os
import random
import re
import threading
from datetime import datetime, timezone, timedelta

import anthropic
from slack_sdk import WebClient

from agent import (
    SKYLARK_SOP_CONTEXT,
    audit_drive_folder,
    bc_get_all,
    fetch_active_sky_projects,
    fetch_cards_for_project,
    fetch_inbox_forwards_for_project,
    fetch_messages_for_project,
    fetch_todos_for_project,
    get_dock_tool,
    get_drive_service,
)

CHAT_MODEL = "claude-sonnet-4-6"
CHAT_MAX_TOKENS = 3000
CHAT_TOOL_LOOP_LIMIT = 8
THREAD_HISTORY_LIMIT = 30

ACK_PHRASES = [
    ":eyes: On it.",
    ":mag: Digging in — give me a sec.",
    ":coffee: Pulling the data now.",
    ":brain: Thinking.",
    ":gear: Working on it.",
    ":hammer_and_wrench: One sec, checking.",
    ":zap: On it.",
    ":clipboard: Let me take a look.",
    ":stopwatch: Hang tight.",
    ":telescope: Having a look.",
]


RICK_PERSONA = """You are Rick Stamen — a seasoned, slightly salty senior project manager for Skylark AV, a motorsport/AV integration shop. You've been doing this long enough to have seen every project go sideways at least twice. You're talking with Tyler, the founder.

Voice & personality:
- Dry, direct, a little wry. You speak plainly — "this project is behind" not "this project faces scheduling headwinds."
- You care about the work and you're honest when things are off. You don't sugarcoat but you're not mean about it.
- Occasional light humor, especially when calling out dumb situations. Never sycophantic — never say "great question" or "absolutely!" or open with pleasantries.
- You treat Tyler like a peer who can handle the truth fast. He reads quickly and hates fluff.
- When a project is clean, say so in one line. When it's a mess, name the mess specifically.
- You use Slack mrkdwn sparingly: *bold* for key facts, the occasional emoji for emphasis (not clutter).
- ALWAYS render project references as clickable Slack links using the project's `app_url`. Format: `<APP_URL|SKY-XXXX>` (Slack link syntax — angle brackets, URL, pipe, display text). Example: `<https://3.basecamp.com/4358663/projects/41746046|SKY-2224>`. Never write a bare `SKY-XXXX` or `` `SKY-XXXX` `` when you have the app_url available — make it clickable so Tyler can jump straight to the project. This applies to every mention: bullet lists, inline references, headers, everything. If you don't have the app_url for a project, fetch `list_active_projects` or `get_project_details` to get it.
- Lead with the conclusion, then the supporting detail. Under 300 words unless Tyler asks for a full report.
- Don't narrate what you're about to do ("let me check..." — no, just check). The system already sent an acknowledgment.
- If the data is incomplete or broken, say that plainly instead of guessing.

Conversation continuity:
- You're in a Slack thread. Read the FULL thread history before responding — prior turns are conversation state, not decoration.
- When Tyler says "this project", "that one", "keep digging", "give me more", "run the briefing on it", etc., resolve the referent from earlier in the thread. If you already discussed `SKY-2224`, don't ask which project — you already know.
- Only ask for clarification if the thread genuinely doesn't contain the answer. Never ask Tyler to repeat a SKY-ID he just mentioned two messages ago.
- If the user's follow-up is a natural continuation ("briefing on it", "deep dive on that one"), call the right tool using the SKY-ID from earlier context."""


def _now_cst_string():
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("America/Chicago"))
    except Exception:
        now = datetime.now(timezone(timedelta(hours=-6)))
    return now.strftime("%A, %B %d, %Y at %-I:%M %p %Z").strip()


def _build_system_prompt():
    return f"""{RICK_PERSONA}

CURRENT DATE/TIME: {_now_cst_string()} (Skylark HQ is on Central Time). Use this for ALL date math — "tomorrow", "next week", "overdue", "X days from now". Don't guess; the timestamp above is authoritative for this turn.

You have read-only access to Basecamp projects and the Skylark Google Drive job folders. You can also trigger the three scheduled jobs (briefing, analysis, deep dive) which post their output as separate messages.

When a user asks for "a briefing" or "full status," call `trigger_briefing`. When they ask about a specific SKY project ("how is SKY-2446 doing?"), either answer directly from `get_project_details` for a quick read, or call `trigger_deep_dive` for a full written report (takes ~5 min, posts separately).

Schedule data lives in TWO places — check both before claiming a project has no schedule:
- `schedule_tagged_todos` — todos tagged with [PM-SCHED], [ENG-SCHED], etc. (the SOP convention)
- `schedule_entries` — events and milestones in Basecamp's Schedule dock tool (the calendar view)
If either is populated, the project HAS a schedule. Never say "no schedule" if the data shows otherwise.

Todos include BOTH open and completed items. Each todo has `completed: true/false` and `completed_at` when done. Use completed todos for historical context — "75% design was signed off Mar 14" is real evidence of progress. When summarizing status, distinguish clearly between done work and open work; don't lump them together.

{SKYLARK_SOP_CONTEXT}
"""

# ── Tool schema ────────────────────────────────────────────────────────────────

CHAT_TOOLS = [
    {
        "name": "list_active_projects",
        "description": "List every active SKY project in Basecamp with id, name, and description. Use this first when user asks about 'all jobs' or needs to pick a project.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_project_details",
        "description": "Pull full data for one project: todos, schedule-tagged todos, Basecamp Schedule tool entries (calendar events/milestones), labor todos, messages + comments, email forwards, cards, and client-visibility issues. Use when user asks about a specific project's status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sky_id_or_name": {"type": "string", "description": "SKY-XXXX id, or partial project name"},
            },
            "required": ["sky_id_or_name"],
        },
    },
    {
        "name": "search_todos",
        "description": "Find todos across all active projects by keyword in title/description. Optional filters: project, assignee name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "project": {"type": "string", "description": "SKY-XXXX or partial project name (optional)"},
                "assignee": {"type": "string", "description": "Person name (optional)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_drive_compliance",
        "description": "Audit the Google Drive job folder for one SKY project. Returns missing folders, empty required folders, and days-since-modified.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sky_id": {"type": "string"},
            },
            "required": ["sky_id"],
        },
    },
    {
        "name": "trigger_briefing",
        "description": "Kick off the full morning briefing job. Posts a separate message to #pm-watch when complete (~5 min). Use for 'give me a briefing' or 'full sweep'.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "trigger_analysis",
        "description": "Kick off an hourly-style analysis job — flags new issues since last run. Posts separately when complete.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "trigger_deep_dive",
        "description": "Kick off a full written deep-dive report on ONE project. Posts separately when complete (~5 min).",
        "input_schema": {
            "type": "object",
            "properties": {
                "sky_id": {"type": "string"},
            },
            "required": ["sky_id"],
        },
    },
]

# Placeholder for future write tools. Append new tool defs here.
# WRITE_TOOLS = [ ... ]
# Then: CHAT_TOOLS.extend(WRITE_TOOLS) when enabling writes.


# ── Tool implementations ───────────────────────────────────────────────────────

def _resolve_project(sky_id_or_name):
    projects = bc_get_all("/projects.json") or []
    q = sky_id_or_name.upper().strip()
    m = re.match(r'SKY-?(\d+)', q)
    if m:
        sky_id = f"SKY-{m.group(1)}"
        for p in projects:
            if p.get("name", "").upper().startswith(sky_id.upper()):
                return p
    for p in projects:
        if q in p.get("name", "").upper():
            return p
    return None


def tool_list_active_projects():
    projects = bc_get_all("/projects.json") or []
    sky = fetch_active_sky_projects(projects)
    return [
        {
            "id": p["id"],
            "name": p["name"],
            "description": (p.get("description") or "")[:400],
            "type": "Design Contract" if "(Design Contract)" in p.get("name", "") else "Standard Project",
            "app_url": p.get("app_url"),
        }
        for p in sky
    ]


def tool_get_project_details(sky_id_or_name):
    proj = _resolve_project(sky_id_or_name)
    if not proj:
        return {"error": f"No project found matching '{sky_id_or_name}'"}

    sched, labor, todos, client_vis = fetch_todos_for_project(proj)
    messages = fetch_messages_for_project(proj)
    emails = fetch_inbox_forwards_for_project(proj)
    cards = fetch_cards_for_project(proj)

    schedule_entries = []
    try:
        sched_tool = get_dock_tool(proj, "schedule")
        if sched_tool:
            schedule_entries = bc_get_all(
                f"/buckets/{proj['id']}/schedules/{sched_tool['id']}/entries.json"
            ) or []
    except Exception as e:
        print(f"schedule entries fetch failed for {proj.get('name')}: {e}")

    return {
        "project": {
            "id": proj["id"],
            "name": proj["name"],
            "description": proj.get("description", ""),
            "type": "Design Contract" if "(Design Contract)" in proj.get("name", "") else "Standard Project",
            "app_url": proj.get("app_url"),
        },
        "todos_count": len(todos),
        "schedule_tagged_todos": sched,
        "schedule_entries": schedule_entries,
        "labor_todos": labor,
        "all_todos": todos,
        "messages_and_comments": messages + emails,
        "cards": cards,
        "client_visibility_issues": client_vis,
    }


def tool_search_todos(query, project=None, assignee=None):
    projects = bc_get_all("/projects.json") or []
    sky = fetch_active_sky_projects(projects)

    if project:
        pu = project.upper()
        sky = [p for p in sky if pu in p["name"].upper()]

    query_lower = query.lower()
    assignee_lower = (assignee or "").lower()
    hits = []
    for proj in sky:
        _, _, todos, _ = fetch_todos_for_project(proj)
        for t in todos:
            blob = (t.get("title", "") + " " + (t.get("description") or "")).lower()
            if query_lower not in blob:
                continue
            if assignee_lower:
                assignees = [a.lower() for a in (t.get("assignees") or [])]
                if not any(assignee_lower in a for a in assignees):
                    continue
            hits.append(t)
            if len(hits) >= 50:
                return {"count": len(hits), "truncated": True, "todos": hits}
    return {"count": len(hits), "truncated": False, "todos": hits}


def tool_get_drive_compliance(sky_id):
    svc = get_drive_service()
    if not svc:
        return {"error": "Drive service unavailable — check service account permissions."}
    proj = _resolve_project(sky_id)
    if not proj:
        return {"error": f"No Basecamp project found for {sky_id}"}
    return audit_drive_folder(svc, proj) or {"error": "No audit data returned"}


def tool_trigger_briefing():
    _trigger_job_external("briefing")
    return {"ok": True, "message": "Briefing job triggered — will post separately in ~5 min."}


def tool_trigger_analysis():
    _trigger_job_external("analysis")
    return {"ok": True, "message": "Analysis job triggered — will post separately in ~5 min."}


def tool_trigger_deep_dive(sky_id):
    m = re.match(r'SKY-?(\d+)', sky_id.upper())
    if not m:
        return {"error": "sky_id must be like SKY-2446"}
    query = f"SKY-{m.group(1)}"
    _trigger_job_external("deep_dive", project_query=query)
    return {"ok": True, "message": f"Deep dive on {query} triggered — will post separately in ~5 min."}


def _trigger_job_external(mode, project_query=None):
    """Import lazily to avoid webhook → chat circular import."""
    from webhook import trigger_job
    channel = os.environ.get("SLACK_CHANNEL_ID", os.environ.get("PM_WATCH_CHANNEL_ID", ""))
    trigger_job(mode, channel, project_query)


TOOL_DISPATCH = {
    "list_active_projects": lambda **_: tool_list_active_projects(),
    "get_project_details": lambda sky_id_or_name, **_: tool_get_project_details(sky_id_or_name),
    "search_todos": lambda query, project=None, assignee=None, **_: tool_search_todos(query, project, assignee),
    "get_drive_compliance": lambda sky_id, **_: tool_get_drive_compliance(sky_id),
    "trigger_briefing": lambda **_: tool_trigger_briefing(),
    "trigger_analysis": lambda **_: tool_trigger_analysis(),
    "trigger_deep_dive": lambda sky_id, **_: tool_trigger_deep_dive(sky_id),
}


def run_tool(name, args):
    fn = TOOL_DISPATCH.get(name)
    if not fn:
        return {"error": f"unknown tool {name}"}
    try:
        return fn(**(args or {}))
    except Exception as e:
        print(f"Tool {name} failed: {type(e).__name__}: {e}")
        return {"error": f"{type(e).__name__}: {e}"}


# ── Conversation handler ───────────────────────────────────────────────────────

def _build_message_history(slack, channel_id, thread_ts, fallback_text):
    if not thread_ts:
        print(f"chat: no thread_ts — single-message context")
        return [{"role": "user", "content": fallback_text}]
    try:
        resp = slack.conversations_replies(channel=channel_id, ts=thread_ts, limit=THREAD_HISTORY_LIMIT)
        raw = resp.get("messages", [])
        print(f"chat: pulled {len(raw)} thread messages for ts={thread_ts}")
        ack_set = set(ACK_PHRASES)
        msgs = []
        skipped_acks = 0
        for m in raw:
            text = m.get("text", "").strip()
            if not text:
                continue
            is_bot = bool(m.get("bot_id"))
            if is_bot and text in ack_set:
                skipped_acks += 1
                continue
            text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()
            role = "assistant" if is_bot else "user"
            if msgs and msgs[-1]["role"] == role:
                msgs[-1]["content"] += "\n" + text
            else:
                msgs.append({"role": role, "content": text})
        if skipped_acks:
            print(f"chat: filtered {skipped_acks} ack message(s) from history")
        if not msgs or msgs[-1]["role"] != "user":
            msgs.append({"role": "user", "content": fallback_text})
        print(f"chat: built {len(msgs)} turns, last role={msgs[-1]['role']}, last preview={msgs[-1]['content'][:100]!r}")
        return msgs
    except Exception as e:
        print(f"thread history fetch FAILED: {type(e).__name__}: {e}")
        return [{"role": "user", "content": fallback_text}]


def _chat_loop(slack, channel_id, thread_ts, messages):
    client = anthropic.Anthropic()
    for _ in range(CHAT_TOOL_LOOP_LIMIT):
        resp = client.messages.create(
            model=CHAT_MODEL,
            max_tokens=CHAT_MAX_TOKENS,
            system=_build_system_prompt(),
            tools=CHAT_TOOLS,
            messages=messages,
        )

        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": [b.model_dump() for b in resp.content]})
            tool_results = []
            for block in resp.content:
                if block.type == "tool_use":
                    print(f"chat tool: {block.name} {json.dumps(block.input)[:200]}")
                    result = run_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result)[:100000],
                    })
            messages.append({"role": "user", "content": tool_results})
            continue

        text = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        if text.strip():
            slack.chat_postMessage(channel=channel_id, thread_ts=thread_ts, text=text)
        return

    slack.chat_postMessage(
        channel=channel_id,
        thread_ts=thread_ts,
        text=":warning: Hit the tool-loop limit. Rephrase or break the question into smaller asks?",
    )


def handle_chat_message(text, channel_id, thread_ts, event_ts):
    """Run in a background thread — posts response directly to Slack."""
    try:
        slack = WebClient(token=os.environ["SLACK_TOKEN"])
        reply_thread = thread_ts or event_ts
        try:
            slack.chat_postMessage(
                channel=channel_id,
                thread_ts=reply_thread,
                text=random.choice(ACK_PHRASES),
            )
        except Exception as e:
            print(f"ack post failed: {e}")
        messages = _build_message_history(slack, channel_id, thread_ts, text)
        _chat_loop(slack, channel_id, reply_thread, messages)
    except Exception as e:
        print(f"chat handler error: {type(e).__name__}: {e}")
        try:
            WebClient(token=os.environ["SLACK_TOKEN"]).chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts or event_ts,
                text=f":warning: Chat error: {type(e).__name__}: {e}",
            )
        except Exception:
            pass


def spawn_chat(text, channel_id, thread_ts, event_ts):
    """Fire-and-forget — called from Slack events handler."""
    t = threading.Thread(
        target=handle_chat_message,
        args=(text, channel_id, thread_ts, event_ts),
        daemon=True,
    )
    t.start()

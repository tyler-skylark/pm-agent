#!/usr/bin/env python3
"""
Rick Stamen Slack webhook
Handles /pmwatch slash command and DMs.
Triggers the pm-agent Cloud Run Job for all long-running work.
"""

import hashlib
import hmac
import json
import os
import re
import time
import urllib.request

from flask import Flask, request, jsonify
from slack_sdk import WebClient

from agent import load_env, load_secrets_from_gcp

load_env()
load_secrets_from_gcp()

app = Flask(__name__)

GCP_PROJECT = "skylark-pm-agents"
GCP_REGION = "us-central1"
JOB_NAME = "pm-agent"
PM_WATCH_CHANNEL = os.environ.get("PM_WATCH_CHANNEL_ID", "C0AU5J6SKQS")
_seen_event_ids = set()
_bot_user_id = None
_parent_mention_cache = {}  # thread_ts -> bool (does parent @-mention the bot)


def _get_bot_user_id():
    global _bot_user_id
    if _bot_user_id is None:
        try:
            client = WebClient(token=os.environ["SLACK_TOKEN"])
            _bot_user_id = client.auth_test()["user_id"]
        except Exception as e:
            print(f"bot user id lookup failed: {e}")
            _bot_user_id = ""
    return _bot_user_id


def _parent_mentions_bot(channel_id, thread_ts):
    """True if the parent message of this thread @-mentions the bot."""
    if not thread_ts:
        return False
    cached = _parent_mention_cache.get(thread_ts)
    if cached is not None:
        return cached
    bot_id = _get_bot_user_id()
    if not bot_id:
        return False
    try:
        client = WebClient(token=os.environ["SLACK_TOKEN"])
        resp = client.conversations_replies(channel=channel_id, ts=thread_ts, limit=1)
        msgs = resp.get("messages", [])
        parent_text = (msgs[0].get("text") or "") if msgs else ""
        result = f"<@{bot_id}>" in parent_text
    except Exception as e:
        print(f"parent-mention lookup failed for thread_ts={thread_ts}: {e}")
        result = False
    _parent_mention_cache[thread_ts] = result
    if len(_parent_mention_cache) > 1000:
        _parent_mention_cache.clear()
    return result


def verify_slack_signature(req):
    secret = os.environ.get("SLACK_SIGNING_SECRET", "")
    if not secret:
        return True
    ts = req.headers.get("X-Slack-Request-Timestamp", "")
    if not ts or abs(time.time() - int(ts)) > 300:
        return False
    sig_base = f"v0:{ts}:{req.get_data(as_text=True)}"
    expected = "v0=" + hmac.new(secret.encode(), sig_base.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, req.headers.get("X-Slack-Signature", ""))


def get_gcp_token():
    """Get an access token from the GCP metadata server."""
    meta_req = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
    )
    with urllib.request.urlopen(meta_req, timeout=5) as resp:
        return json.loads(resp.read())["access_token"]


def trigger_job(mode, channel_id, project_query=None):
    """Trigger the pm-agent Cloud Run Job with env overrides — runs independently."""
    try:
        token = get_gcp_token()

        env = [
            {"name": "RUN_MODE", "value": mode},
            {"name": "SLACK_CHANNEL_ID", "value": channel_id},
            {"name": "ON_DEMAND", "value": "true"},
        ]
        if project_query:
            env.append({"name": "PROJECT_QUERY", "value": project_query})

        url = (f"https://run.googleapis.com/v2/projects/{GCP_PROJECT}"
               f"/locations/{GCP_REGION}/jobs/{JOB_NAME}:run")
        payload = json.dumps({
            "overrides": {"containerOverrides": [{"env": env}]}
        }).encode()
        api_req = urllib.request.Request(url, data=payload, method="POST", headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        with urllib.request.urlopen(api_req, timeout=10) as resp:
            result = json.loads(resp.read())
            print(f"Triggered job: mode={mode} channel={channel_id} project={project_query} execution={result.get('name','?').split('/')[-1]}")
    except Exception as e:
        print(f"ERROR triggering job mode={mode}: {e}")


def parse_command(text):
    """
    Parse slash command text. Returns (mode, project_query).
      ""              → ("analysis", None)
      "SKY-2446"      → ("deep_dive", "SKY-2446")
      "status 2446"   → ("deep_dive", "SKY-2446")
      "briefing"      → ("briefing", None)
      "morning"       → ("briefing", None)
    """
    text = (text or "").strip().upper()

    if not text or text in ("CHECK", "RUN", "UPDATE", "STATUS"):
        return "analysis", None

    if text in ("BRIEFING", "MORNING", "SUMMARY", "REPORT"):
        return "briefing", None

    if "DRIVE" in text and not re.search(r'SKY-?\d+', text):
        return "drive_audit", None

    match = re.search(r'SKY-(\d+)', text)
    if not match:
        match = re.search(r'\b(\d{4,})\b', text)
        if match:
            return "deep_dive", f"SKY-{match.group(1)}"
    if match:
        return "deep_dive", f"SKY-{match.group(1)}"

    return "analysis", None


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/slack/command", methods=["POST"])
def slack_command():
    if not verify_slack_signature(request):
        return jsonify({"error": "Invalid signature"}), 403

    channel_id = request.form.get("channel_id")
    user_name = request.form.get("user_name", "Tyler")
    text = request.form.get("text", "")

    mode, project_query = parse_command(text)

    if mode == "deep_dive":
        ack = f":mag: Looking up *{project_query}*... full status report in ~60 seconds."
    elif mode == "briefing":
        ack = ":sunrise: Generating morning briefing... coming up in ~60 seconds."
    elif mode == "drive_audit":
        ack = ":file_folder: Auditing Google Drive across every active project... ~3 min."
    else:
        ack = f":mag: On it, {user_name}. Scanning Basecamp... results in ~60 seconds."

    trigger_job(mode, channel_id, project_query)
    return jsonify({"response_type": "in_channel", "text": ack})


@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json or {}

    if data.get("type") == "url_verification":
        return jsonify({"challenge": data["challenge"]})

    if not verify_slack_signature(request):
        return jsonify({"error": "Invalid signature"}), 403

    if request.headers.get("X-Slack-Retry-Num"):
        return jsonify({"ok": True})

    event_id = data.get("event_id")
    if event_id:
        if event_id in _seen_event_ids:
            return jsonify({"ok": True})
        _seen_event_ids.add(event_id)
        if len(_seen_event_ids) > 1000:
            _seen_event_ids.clear()

    event = data.get("event", {})
    if event.get("bot_id") or event.get("subtype") == "bot_message":
        return jsonify({"ok": True})

    if event.get("type") not in ("message", "app_mention"):
        return jsonify({"ok": True})

    channel_id = event.get("channel")
    text = event.get("text", "")
    thread_ts = event.get("thread_ts")
    event_ts = event.get("ts")
    is_dm = event.get("channel_type") == "im"
    in_pm_watch = channel_id == PM_WATCH_CHANNEL
    is_mention = event.get("type") == "app_mention"
    is_thread_reply = bool(thread_ts) and thread_ts != event_ts
    parent_invoked_bot = (
        is_thread_reply
        and not (in_pm_watch or is_dm or is_mention)
        and _parent_mentions_bot(channel_id, thread_ts)
    )

    if not (in_pm_watch or is_dm or is_mention or parent_invoked_bot):
        return jsonify({"ok": True})

    print(f"chat event: type={event.get('type')} channel={channel_id} thread_ts={thread_ts} event_ts={event_ts} dm={is_dm} mention={is_mention} parent_invoked={parent_invoked_bot}")
    from chat import spawn_chat
    spawn_chat(text, channel_id, thread_ts, event_ts)
    return jsonify({"ok": True})


if __name__ == "__main__":
    load_env()
    load_secrets_from_gcp()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

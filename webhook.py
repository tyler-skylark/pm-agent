#!/usr/bin/env python3
"""
PM Watch Slack webhook
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

app = Flask(__name__)

GCP_PROJECT = "skylark-pm-agents"
GCP_REGION = "us-central1"
JOB_NAME = "pm-agent"


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

    event = data.get("event", {})
    if event.get("type") in ("message", "app_mention") and not event.get("bot_id"):
        text = event.get("text", "")
        channel_id = event.get("channel")
        mode, project_query = parse_command(text)

        slack_client = WebClient(token=os.environ["SLACK_TOKEN"])

        if mode == "deep_dive":
            slack_client.chat_postMessage(channel=channel_id,
                text=f":mag: Looking up *{project_query}*... full status in ~60 seconds.")
            trigger_job(mode, channel_id, project_query)
        elif mode == "briefing":
            slack_client.chat_postMessage(channel=channel_id,
                text=":sunrise: Generating briefing... ~60 seconds.")
            trigger_job(mode, channel_id)
        elif any(w in text.lower() for w in ["check", "status", "update", "run", "watch"]):
            slack_client.chat_postMessage(channel=channel_id,
                text=":mag: Scanning Basecamp... ~60 seconds.")
            trigger_job("analysis", channel_id)

    return jsonify({"ok": True})


if __name__ == "__main__":
    load_env()
    load_secrets_from_gcp()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

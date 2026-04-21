#!/usr/bin/env python3
"""
PM Watch Slack webhook
Handles /pmwatch slash command and DMs.
"""

import hashlib
import hmac
import json
import os
import re
import threading
import time
import urllib.request

from flask import Flask, request, jsonify
from slack_sdk import WebClient

from agent import (
    load_env, load_secrets_from_gcp,
    run_analysis, run_briefing, run_deep_dive,
    post_alerts_to_slack, post_freeform_to_slack,
)

app = Flask(__name__)


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


def post_to_response_url(response_url, text, blocks=None):
    payload = json.dumps({
        "response_type": "in_channel",
        "text": text,
        **({"blocks": blocks} if blocks else {}),
    }).encode()
    req = urllib.request.Request(
        response_url, data=payload, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        print(f"Error posting to response_url: {e}")


def handle_analysis(response_url, channel_id):
    try:
        alerts = run_analysis(on_demand=True)
        slack_client = WebClient(token=os.environ["SLACK_TOKEN"])
        if alerts:
            post_alerts_to_slack(slack_client, channel_id, alerts, title="PM Watch (on-demand)")
            if response_url:
                post_to_response_url(response_url, f"Found {len(alerts)} item(s) — posted above.")
        else:
            msg = ":white_check_mark:  *All clear* — no issues found across active Skylark projects."
            if response_url:
                post_to_response_url(response_url, msg,
                    blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": msg}}])
            else:
                post_freeform_to_slack(slack_client, channel_id, msg)
    except Exception as e:
        err = f":warning: PM Watch error: {e}"
        if response_url:
            post_to_response_url(response_url, err)
        print(f"handle_analysis error: {e}")


def handle_briefing(response_url, channel_id):
    try:
        text = run_briefing()
        slack_client = WebClient(token=os.environ["SLACK_TOKEN"])
        post_freeform_to_slack(slack_client, channel_id, text, "Skylark PM Briefing")
        if response_url:
            post_to_response_url(response_url, "Briefing posted above.")
    except Exception as e:
        if response_url:
            post_to_response_url(response_url, f":warning: Briefing error: {e}")
        print(f"handle_briefing error: {e}")


def handle_deep_dive(response_url, channel_id, project_query):
    try:
        text = run_deep_dive(project_query)
        slack_client = WebClient(token=os.environ["SLACK_TOKEN"])
        post_freeform_to_slack(slack_client, channel_id, text, f"Deep dive: {project_query}")
        if response_url:
            post_to_response_url(response_url, "Deep dive posted above.")
    except Exception as e:
        if response_url:
            post_to_response_url(response_url, f":warning: Deep dive error: {e}")
        print(f"handle_deep_dive error: {e}")


def parse_command(text):
    """
    Parse slash command text. Returns (mode, project_query).
    Examples:
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

    # Match SKY-XXXX explicitly or just digits
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

    response_url = request.form.get("response_url")
    channel_id = request.form.get("channel_id")
    user_name = request.form.get("user_name", "Tyler")
    text = request.form.get("text", "")

    mode, project_query = parse_command(text)

    if mode == "deep_dive":
        ack = f":mag: Looking up *{project_query}*... full status report in ~30 seconds."
        target = handle_deep_dive
        args = (response_url, channel_id, project_query)
    elif mode == "briefing":
        ack = ":sunrise: Generating morning briefing... coming up in ~45 seconds."
        target = handle_briefing
        args = (response_url, channel_id)
    else:
        ack = f":mag: On it, {user_name}. Scanning Basecamp... results in ~30 seconds."
        target = handle_analysis
        args = (response_url, channel_id)

    threading.Thread(target=target, args=args, daemon=True).start()
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
                text=f":mag: Looking up *{project_query}*... full status in ~30 seconds.")
            threading.Thread(target=handle_deep_dive,
                args=(None, channel_id, project_query), daemon=True).start()
        elif mode == "briefing":
            slack_client.chat_postMessage(channel=channel_id,
                text=":sunrise: Generating briefing... ~45 seconds.")
            threading.Thread(target=handle_briefing,
                args=(None, channel_id), daemon=True).start()
        elif any(w in text.lower() for w in ["check", "status", "update", "run", "watch"]):
            slack_client.chat_postMessage(channel=channel_id,
                text=":mag: Scanning Basecamp... ~30 seconds.")
            threading.Thread(target=handle_analysis,
                args=(None, channel_id), daemon=True).start()

    return jsonify({"ok": True})


if __name__ == "__main__":
    load_env()
    load_secrets_from_gcp()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

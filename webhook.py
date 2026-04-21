#!/usr/bin/env python3
"""
PM Watch Slack webhook — handles slash commands and DMs.
Deploy as a Cloud Run Service (always-on HTTP server).
"""

import hashlib
import hmac
import json
import os
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime

from flask import Flask, request, jsonify
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from agent import load_env, load_secrets_from_gcp, run_analysis, post_to_slack

app = Flask(__name__)


def verify_slack_signature(req):
    slack_signing_secret = os.environ.get("SLACK_SIGNING_SECRET", "")
    if not slack_signing_secret:
        return True  # skip verification if not configured

    timestamp = req.headers.get("X-Slack-Request-Timestamp", "")
    if abs(time.time() - int(timestamp)) > 300:
        return False

    sig_basestring = f"v0:{timestamp}:{req.get_data(as_text=True)}"
    expected = "v0=" + hmac.new(
        slack_signing_secret.encode(),
        sig_basestring.encode(),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, req.headers.get("X-Slack-Signature", ""))


def post_to_response_url(response_url, blocks, text):
    payload = json.dumps({
        "response_type": "in_channel",
        "text": text,
        "blocks": blocks,
    }).encode()
    req = urllib.request.Request(
        response_url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read()


def run_and_respond(response_url, channel_id):
    """Run analysis in background thread, post results to Slack."""
    try:
        alerts = run_analysis(on_demand=True)
        slack_client = WebClient(token=os.environ["SLACK_TOKEN"])
        now = datetime.now().strftime("%b %d, %I:%M %p")

        if alerts:
            severity_emoji = {
                "high": ":red_circle:",
                "medium": ":large_yellow_circle:",
                "low": ":large_blue_circle:",
            }
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"PM Watch (on-demand)  —  {now}"}},
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

            summary = f"Found {len(alerts)} item(s) needing attention."
        else:
            blocks = [
                {"type": "section", "text": {"type": "mrkdwn", "text": f":white_check_mark:  *All clear as of {now}*\nNo issues found across active Skylark projects."}},
            ]
            summary = "All clear — no issues found."

        if response_url:
            post_to_response_url(response_url, blocks, summary)
        else:
            slack_client.chat_postMessage(channel=channel_id, blocks=blocks, text=summary)

    except Exception as e:
        error_msg = f":warning: PM Watch error: {e}"
        if response_url:
            post_to_response_url(response_url, [], error_msg)
        print(f"Error in run_and_respond: {e}")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/slack/command", methods=["POST"])
def slack_command():
    """Handles /pmwatch slash command."""
    if not verify_slack_signature(request):
        return jsonify({"error": "Invalid signature"}), 403

    response_url = request.form.get("response_url")
    channel_id = request.form.get("channel_id")
    user_name = request.form.get("user_name", "someone")

    # Kick off analysis in background
    thread = threading.Thread(
        target=run_and_respond,
        args=(response_url, channel_id),
        daemon=True,
    )
    thread.start()

    # Respond within 3s
    return jsonify({
        "response_type": "in_channel",
        "text": f":mag: On it, {user_name}. Checking Basecamp now... results in ~30 seconds.",
    })


@app.route("/slack/events", methods=["POST"])
def slack_events():
    """Handles Slack Events API (DMs to the bot)."""
    data = request.json or {}

    # URL verification challenge
    if data.get("type") == "url_verification":
        return jsonify({"challenge": data["challenge"]})

    if not verify_slack_signature(request):
        return jsonify({"error": "Invalid signature"}), 403

    event = data.get("event", {})
    event_type = event.get("type")

    # Respond to DMs or app mentions
    if event_type in ("message", "app_mention") and not event.get("bot_id"):
        text = event.get("text", "").lower().strip()
        channel_id = event.get("channel")

        trigger_words = ["check", "status", "update", "run", "watch", "pmwatch"]
        if any(w in text for w in trigger_words) or event_type == "app_mention":
            thread = threading.Thread(
                target=run_and_respond,
                args=(None, channel_id),
                daemon=True,
            )
            thread.start()

            slack_client = WebClient(token=os.environ["SLACK_TOKEN"])
            slack_client.chat_postMessage(
                channel=channel_id,
                text=":mag: Checking Basecamp now... results in ~30 seconds.",
            )

    return jsonify({"ok": True})


if __name__ == "__main__":
    load_env()
    load_secrets_from_gcp()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

#!/usr/bin/env python3
"""One-time OAuth setup — run this locally to get Basecamp tokens for Cloud Run."""

import json
import urllib.parse
import urllib.request
import sys
from pathlib import Path

CLIENT_ID = "c1d94075d2dc6d54042ee2f2cf4980f1e59370dc"
CLIENT_SECRET = "b608170317ef0e2cfe3b43308e9ae5454cfc7104"
REDIRECT_URI = "http://localhost"
TOKEN_FILE = Path(__file__).parent / "basecamp_tokens.json"

auth_url = (
    "https://launchpad.37signals.com/authorization/new"
    f"?type=web_server&client_id={CLIENT_ID}&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
)

print("\n=== Basecamp OAuth Setup ===\n")
print("1. Open this URL in your browser:\n")
print(f"   {auth_url}\n")
print("2. Click 'Yes, I'll allow access'")
print("3. Your browser will redirect to localhost (it will fail to load — that's OK)")
print("4. Copy the FULL URL from the address bar and paste it below:\n")

callback_url = input("Paste the redirect URL here: ").strip()

parsed = urllib.parse.urlparse(callback_url)
code = urllib.parse.parse_qs(parsed.query).get("code", [None])[0]

if not code:
    print("ERROR: Could not find 'code' in URL. Make sure you copied the full URL.")
    sys.exit(1)

print(f"\nExchanging code for tokens...")

data = urllib.parse.urlencode({
    "type": "web_server",
    "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI,
    "client_secret": CLIENT_SECRET,
    "code": code,
}).encode()

req = urllib.request.Request(
    "https://launchpad.37signals.com/authorization/token",
    data=data,
    method="POST",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
)

try:
    with urllib.request.urlopen(req) as resp:
        tokens = json.loads(resp.read())
except urllib.error.HTTPError as e:
    print(f"ERROR: {e.code} - {e.read().decode()}")
    sys.exit(1)

tokens["client_id"] = CLIENT_ID
tokens["client_secret"] = CLIENT_SECRET

TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
print(f"\nTokens saved to {TOKEN_FILE}")
print(f"Access token expires: {tokens.get('expires_in', '?')}s from now")
print("\nNext step: run  python3 store_secrets.py")

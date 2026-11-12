"""Admin helper: create a pairing code for a LibreChat user.

    export RELAY_URL=https://relay.example.com        # default: http://localhost:8080
    export RELAY_ADMIN_KEY=...

    python make_code.py --email alice@example.com     # looks the user id up in MongoDB
    python make_code.py --user-id 65f1c0a9e4b0a1b2c3d4e5f6

The relay's `user_id` must equal what LibreChat sends as `{{LIBRECHAT_USER_ID}}`,
which is the user's MongoDB `_id` as a string.

How the email -> id lookup runs is configurable with --lookup-cmd. The default
targets the stock LibreChat docker-compose setup (container `chat-mongodb`,
database `LibreChat`). The command receives a JavaScript snippet as its last
argument and must print the user id on stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys

import httpx

DEFAULT_LOOKUP_CMD = "docker exec chat-mongodb mongosh --quiet LibreChat --eval"
OBJECT_ID_RE = re.compile(r"^[0-9a-f]{24}$")


def lookup_user_id(email: str, lookup_cmd: str) -> str:
    email = email.strip().lower()  # LibreChat stores emails in lower case
    # json.dumps produces a valid JS string literal, so odd characters in the
    # email cannot break out of the snippet. No shell is involved either.
    js = f"const u = db.users.findOne({{email: {json.dumps(email)}}}, {{_id: 1}}); print(u ? String(u._id) : '')"
    try:
        out = subprocess.run([*shlex.split(lookup_cmd), js], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        sys.exit(f"Could not run the lookup command ({exc}).")
    if out.returncode != 0:
        sys.exit(f"Lookup command failed:\n{out.stderr.strip() or out.stdout.strip()}")
    user_id = out.stdout.strip().splitlines()[-1].strip() if out.stdout.strip() else ""
    if not user_id:
        sys.exit(f"No LibreChat user with email {email!r}.")
    if not OBJECT_ID_RE.match(user_id):
        sys.exit(f"Unexpected lookup output (expected a 24-character hex id): {user_id!r}")
    return user_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a pairing code for a LibreChat user")
    who = parser.add_mutually_exclusive_group(required=True)
    who.add_argument("--email", help="LibreChat account email (id is looked up in MongoDB)")
    who.add_argument("--user-id", help="LibreChat user id (MongoDB _id), used as is")
    parser.add_argument("--relay", default=os.environ.get("RELAY_URL", "http://localhost:8080"))
    parser.add_argument("--lookup-cmd", default=DEFAULT_LOOKUP_CMD, help="command used for the email lookup")
    args = parser.parse_args()

    admin_key = os.environ.get("RELAY_ADMIN_KEY")
    if not admin_key:
        sys.exit("Set RELAY_ADMIN_KEY in the environment.")

    user_id = args.user_id or lookup_user_id(args.email, args.lookup_cmd)
    try:
        r = httpx.post(f"{args.relay.rstrip('/')}/admin/pairing-codes",
                       headers={"Authorization": f"Bearer {admin_key}"}, json={"user_id": user_id}, timeout=15)
    except httpx.HTTPError as exc:
        sys.exit(f"Could not reach the relay at {args.relay}: {exc}")
    if r.status_code != 200:
        sys.exit(f"Relay returned HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    print(f"User id:      {user_id}")
    print(f"Pairing code: {data['code']}   (valid for {data['expires_in'] // 60} minutes, single use)")
    print(f"\nOn the user's computer:\n  python agent.py pair --relay {args.relay.rstrip('/')} --code {data['code']}")


if __name__ == "__main__":
    main()

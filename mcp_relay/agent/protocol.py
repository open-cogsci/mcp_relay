"""Constants for the relay <-> agent WebSocket protocol.

Kept in sync with relay.py (deployed separately on the server). Frame shapes:

  relay -> agent: {"type": "open"|"message"|"close", "session": <id>, ...}
  agent -> relay: {"type": "hello"|"message"|"closed", "session": <id>, ...}

The relay is MCP-agnostic: "message" frames carry opaque JSON-RPC dicts.
"""

from __future__ import annotations

import json
import re

MAX_MESSAGE = 32 * 1024 * 1024  # must be >= the relay's uvicorn ws_max_size
FATAL_CLOSE_CODES = {4001, 4400, 4401, 4403}  # replaced / bad hello / unauthorized / revoked
SERVER_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
RESERVED_SERVER_NAMES = {"relay"}  # served by the relay itself; agents may not claim it


def dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))

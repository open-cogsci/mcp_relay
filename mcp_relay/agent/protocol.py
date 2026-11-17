"""Constants for the relay <-> agent WebSocket protocol.

Kept in sync with relay.py (deployed separately on the server). Frame shapes:

  relay -> agent: {"type": "open"|"message"|"close", "session": <id>, ...}
  agent -> relay: {"type": "hello"|"servers"|"message"|"closed", "session": <id>, ...}

The relay is MCP-agnostic: "message" frames carry opaque JSON-RPC dicts.

``hello`` (agent -> relay, once per connection) advertises the initial server
list. ``servers`` (agent -> relay, optional, any time while connected) replaces
that list at runtime, so an agent can enable/disable local MCP servers without
reconnecting. The relay replies to neither frame; an old relay that does not
know ``servers`` simply ignores it (toggles then apply on the next hello).
"""

import json
import re

MAX_MESSAGE = 32 * 1024 * 1024  # must be >= the relay's uvicorn ws_max_size
FATAL_CLOSE_CODES = {4001, 4400, 4401, 4403}  # replaced / bad hello / unauthorized / revoked
SERVER_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
RESERVED_SERVER_NAMES = {"relay"}  # served by the relay itself; agents may not claim it


def validate_server_list(servers: object) -> list[str]:
    """Validate an agent-advertised server list (hello/servers frame).

    Returns the list; raises ValueError on anything malformed, so both the
    agent and the relay can share the exact same rule.
    """
    if (not isinstance(servers, list)
            or not all(isinstance(n, str) and SERVER_NAME_RE.match(n) for n in servers)
            or RESERVED_SERVER_NAMES & set(servers)
            or len(set(servers)) != len(servers)):
        raise ValueError("invalid server list: expected a list of unique valid server "
                         f"names (reserved: {sorted(RESERVED_SERVER_NAMES)})")
    return list(servers)


def dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))

"""Built-in MCP server of the relay itself: self-service pairing from the chat.

Served at ``/mcp/relay``.  Unlike the other ``/mcp/<name>`` endpoints it does not
forward to a desktop agent, so it also works for users who have not paired a
computer yet.  That is the point: it lets a user say "pair my computer" in
LibreChat and get a pairing code back.

Tools
-----
* ``get_pairing_code``  create a one-time code for the calling user
* ``list_devices``      show the calling user's paired computers
* ``revoke_device``     unpair one of the calling user's computers

Identity comes from the ``X-User-Id`` header that LibreChat fills in
(``{{LIBRECHAT_USER_ID}}``) and that ``relay.py`` has already authenticated with
the shared API key.  Tool arguments never contain a user id, so neither the user
nor the model can act on someone else's account.

The server is stateless and tiny: it answers each POST with a plain JSON body
(no session, no SSE) and only implements what a tools-only server needs.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from store import Device, Store

SERVER_NAME = "relay"  # URL path /mcp/relay; agents may not use this name
LATEST_VERSION = "2025-06-18"
SUPPORTED_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")

ERR_INVALID_PARAMS = -32602
ERR_METHOD_NOT_FOUND = -32601

INSTRUCTIONS = (
    "This server manages the connection between the user's own computer and their account. "
    "Use get_pairing_code ONLY when the user explicitly asks to pair or connect a computer, and show "
    "the code and instructions to the user exactly as returned. Never put a pairing code anywhere "
    "except your reply to the user. Use revoke_device ONLY when the user explicitly asks to remove a "
    "specific computer."
)

TOOLS: list[dict[str, Any]] = [
    {
        "name": "get_pairing_code",
        "title": "Get a pairing code",
        "description": (
            "Create a one-time pairing code (valid 10 minutes) that the user types into the desktop "
            "agent on their computer to connect it to their account. Only call this when the user "
            "explicitly asks to pair/connect a computer. Show the returned code and instructions to "
            "the user verbatim."
        ),
        "inputSchema": {"type": "object", "properties": {}},
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False,
                        "openWorldHint": False},
    },
    {
        "name": "list_devices",
        "title": "List paired computers",
        "description": "List the computers paired with the user's account and whether each is online.",
        "inputSchema": {"type": "object", "properties": {}},
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "revoke_device",
        "title": "Unpair a computer",
        "description": (
            "Unpair one of the user's computers and disconnect it. It can only be used again after "
            "pairing anew. Only call this when the user explicitly asked to remove that computer; "
            "get the id from list_devices."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"device_id": {"type": "string", "description": "Device id from list_devices, e.g. dev_1a2b3c4d5e6f"}},
            "required": ["device_id"],
        },
        "annotations": {"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True,
                        "openWorldHint": False},
    },
]


class ToolError(Exception):
    """Reported to the model as a tool result with isError=true."""


def _clean(text: str) -> str:
    """Device names are chosen by whoever ran `agent.py pair`, and this text goes to the model.
    Keep them short and free of line breaks and markup so they cannot smuggle in instructions."""
    return re.sub(r"[^\w .@()+-]", "?", text)[:40]


def _when(ts: float | None) -> str:
    if ts is None:
        return "never"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _error(rpc_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": message}}


class BuiltinMCP:
    def __init__(
        self,
        store: Store,
        hub: Any,  # relay.Hub; only `hub.agents` (user_id -> connection) is used
        revoke: Callable[[Device], Awaitable[None]],
        public_url: str | None,
    ):
        self.store, self.hub, self.revoke, self.public_url = store, hub, revoke, public_url
        self._tools = {
            "get_pairing_code": self._get_pairing_code,
            "list_devices": self._list_devices,
            "revoke_device": self._revoke_device,
        }

    async def handle(self, msg: dict, user_id: str) -> dict | None:
        """Return the JSON-RPC reply, or None if nothing should be sent (HTTP 202)."""
        method = msg.get("method")
        if not isinstance(method, str) or "id" not in msg:
            return None  # a notification, or the client's answer to something: nothing to reply to
        rpc_id = msg["id"]
        params = msg.get("params") or {}
        if not isinstance(params, dict):
            return _error(rpc_id, ERR_INVALID_PARAMS, "params must be an object")

        if method == "initialize":
            requested = params.get("protocolVersion")
            return self._ok(rpc_id, {
                "protocolVersion": requested if requested in SUPPORTED_VERSIONS else LATEST_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "title": "Computer pairing", "version": "1.0.0"},
                "instructions": INSTRUCTIONS,
            })
        if method == "ping":
            return self._ok(rpc_id, {})
        if method == "tools/list":
            return self._ok(rpc_id, {"tools": TOOLS})
        if method == "tools/call":
            name, args = params.get("name"), params.get("arguments") or {}
            fn = self._tools.get(name) if isinstance(name, str) else None
            if fn is None:
                return _error(rpc_id, ERR_INVALID_PARAMS, f"Unknown tool: {str(name)[:60]!r}")
            if not isinstance(args, dict):
                return _error(rpc_id, ERR_INVALID_PARAMS, "arguments must be an object")
            try:
                return self._ok(rpc_id, self._text(await fn(user_id, args)))
            except ToolError as exc:
                return self._ok(rpc_id, self._text(str(exc), is_error=True))
        return _error(rpc_id, ERR_METHOD_NOT_FOUND, f"Method not found: {method[:60]}")

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _ok(rpc_id: Any, result: dict) -> dict:
        return {"jsonrpc": "2.0", "id": rpc_id, "result": result}

    @staticmethod
    def _text(text: str, is_error: bool = False) -> dict:
        return {"content": [{"type": "text", "text": text}], "isError": is_error}

    # -- tools -------------------------------------------------------------

    async def _get_pairing_code(self, user_id: str, args: dict) -> str:
        code, ttl = self.store.create_pairing_code(user_id)  # also voids this user's earlier unused codes
        relay_url = (self.public_url or "<relay URL>").rstrip("/")
        lines = [
            f"Pairing code: {code}",
            f"It is valid for {ttl // 60} minutes and can be used once. Any earlier unused code no longer works.",
            "",
            "On the computer you want to connect:",
            "  1. Enter this code in the desktop agent's pairing box, or run:",
            f"       python agent.py pair --relay {relay_url} --code {code}",
            "  2. Keep the desktop agent running.",
        ]
        if self.store.list_devices(user_id):
            lines += ["", "Note: you already have a paired computer. Only one computer can be connected at a "
                          "time; the agent that was started most recently takes over."]
        return "\n".join(lines)

    async def _list_devices(self, user_id: str, args: dict) -> str:
        devices = sorted(self.store.list_devices(user_id), key=lambda d: d.created_at)
        if not devices:
            return "No computers are paired with this account yet. Use get_pairing_code to pair one."
        conn = self.hub.agents.get(user_id)
        lines = [f"{len(devices)} paired computer(s):"]
        for d in devices:
            online = conn is not None and conn.device_id == d.id
            line = (f"- {_clean(d.name)} (id: {d.id}): {'online' if online else 'offline'}, "
                    f"paired {_when(d.created_at)}, last connected {_when(d.last_seen)}")
            if online and conn.servers:
                line += f", tools available: {', '.join(conn.servers)}"
            lines.append(line)
        return "\n".join(lines)

    async def _revoke_device(self, user_id: str, args: dict) -> str:
        device_id = args.get("device_id")
        if not isinstance(device_id, str) or not device_id.strip():
            raise ToolError("device_id is required. Use list_devices to see the ids.")
        device = self.store.get_device(device_id.strip())
        if device is None or device.user_id != user_id:  # same answer for "missing" and "not yours"
            raise ToolError("No paired computer with that id. Use list_devices to see the ids.")
        await self.revoke(device)
        return (f"Unpaired {_clean(device.name)} ({device.id}). It has been disconnected and must be "
                "paired again before it can be used.")

"""MCP relay: lets a cloud MCP client (e.g. LibreChat) talk to MCP servers that
run on users' desktops, where no inbound connection is possible.

    LibreChat --(streamable HTTP)--> relay <--(outbound WebSocket)-- agent --(stdio)--> MCP server

The relay is deliberately MCP-server-agnostic: it never looks at tools,
resources or prompts.  It only needs to tell JSON-RPC *requests* from
*responses* and *notifications*, so any stdio MCP server works unchanged.

Routing
-------
* LibreChat calls ``POST /mcp/{server}`` with ``Authorization: Bearer <RELAY_API_KEY>``
  and ``X-User-Id: <librechat user id>``.
* Each user has (at most) one connected agent; an agent can expose several
  named servers (``{server}`` in the URL).
* Every MCP *session* (one ``initialize`` handshake) gets its own stdio process
  on the user's machine.  That keeps the relay transparent: no JSON-RPC id
  remapping and no sharing of one stdio process between unrelated clients.

Besides forwarding, the relay serves one MCP server of its own at ``/mcp/relay``
(see ``builtin_mcp.py``): tools to get a pairing code, list devices and revoke
devices, so users can pair their computer from inside the chat.

Scope: implements the classic session-based Streamable HTTP transport
(``initialize`` + ``Mcp-Session-Id``).  See README for limitations.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import logging
import os
import re
import secrets
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse, Response, StreamingResponse

from builtin_mcp import SERVER_NAME as BUILTIN_SERVER, BuiltinMCP
from store import Device, Store

log = logging.getLogger("relay")

MAX_WS_MESSAGE = 32 * 1024 * 1024  # screenshots etc. can be large
SERVER_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
RESERVED_SERVER_NAMES = {BUILTIN_SERVER}  # served by the relay itself; agents cannot claim them

# WebSocket close codes the relay sends to an agent.  The agent treats all of
# them as "do not reconnect automatically".
CLOSE_BAD_HELLO = 4400
CLOSE_UNAUTHORIZED = 4401
CLOSE_REPLACED = 4001
CLOSE_REVOKED = 4403

# JSON-RPC error codes
ERR_PARSE = -32700
ERR_INVALID_REQUEST = -32600
ERR_SERVER = -32000
ERR_TIMEOUT = -32001


# --------------------------------------------------------------------------
# Config / errors / small helpers
# --------------------------------------------------------------------------


@dataclass
class Config:
    api_key: str  # shared secret between LibreChat and the relay
    admin_key: str  # protects /admin/*; keep it away from LibreChat
    db_path: str = "relay.db"
    user_header: str = "x-user-id"
    request_timeout: float = 300.0  # max wait for one tool call
    session_ttl: float = 24 * 3600.0  # idle sessions are closed after this
    max_sessions_per_user: int = 16  # oldest idle session is evicted beyond this
    keepalive: float = 15.0  # SSE comment interval; keeps proxies from timing out
    public_url: str | None = None  # relay URL as seen from users' computers; shown in pairing instructions

    @classmethod
    def from_env(cls) -> "Config":
        missing = [k for k in ("RELAY_API_KEY", "RELAY_ADMIN_KEY") if not os.environ.get(k)]
        if missing:
            raise SystemExit(
                f"Missing environment variable(s): {', '.join(missing)}\n"
                "Generate secrets with:  python -c \"import secrets; print(secrets.token_urlsafe(32))\""
            )
        return cls(
            api_key=os.environ["RELAY_API_KEY"],
            admin_key=os.environ["RELAY_ADMIN_KEY"],
            db_path=os.environ.get("RELAY_DB", "relay.db"),
            user_header=os.environ.get("RELAY_USER_HEADER", "x-user-id").lower(),
            request_timeout=float(os.environ.get("RELAY_REQUEST_TIMEOUT", 300)),
            session_ttl=float(os.environ.get("RELAY_SESSION_TTL", 24 * 3600)),
            max_sessions_per_user=int(os.environ.get("RELAY_MAX_SESSIONS_PER_USER", 16)),
            public_url=os.environ.get("RELAY_PUBLIC_URL") or None,
        )


class RelayError(Exception):
    """Rendered as an HTTP error with a JSON-RPC style body."""

    def __init__(self, status: int, message: str, code: int = ERR_SERVER, rpc_id: Any = None):
        super().__init__(message)
        self.status, self.message, self.code, self.rpc_id = status, message, code, rpc_id


def rpc_error(rpc_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": message}}


def sse(msg: dict) -> str:
    return f"event: message\ndata: {json.dumps(msg, separators=(',', ':'))}\n\n"


def bearer(headers) -> str | None:
    scheme, _, token = headers.get("authorization", "").partition(" ")
    return token.strip() or None if scheme.lower() == "bearer" else None


def key_ok(supplied: str | None, expected: str) -> bool:
    return supplied is not None and hmac.compare_digest(supplied.encode(), expected.encode())


def validate_user_id(value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 128 or not value.isprintable():
        raise RelayError(400, "Missing or invalid user id")
    if "{{" in value:
        # LibreChat didn't substitute its placeholder: refuse rather than route
        # every user to the same bucket.
        raise RelayError(400, f"User id looks like an unresolved placeholder: {value!r}")
    return value.strip()


class FailureLimiter:
    """Tiny in-memory brute-force guard for the public pairing endpoint."""

    def __init__(self, max_failures: int = 10, window: float = 60.0):
        self.max, self.window = max_failures, window
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def blocked(self, key: str) -> bool:
        q, now = self._hits[key], time.monotonic()
        while q and now - q[0] > self.window:
            q.popleft()
        if not q:
            self._hits.pop(key, None)
        return len(q) >= self.max

    def record(self, key: str) -> None:
        self._hits[key].append(time.monotonic())


# --------------------------------------------------------------------------
# Connected agents and MCP sessions
# --------------------------------------------------------------------------


class AgentConn:
    """One connected desktop agent (WebSocket)."""

    def __init__(self, ws: WebSocket, user_id: str, device_id: str, servers: list[str]):
        self.ws, self.user_id, self.device_id, self.servers = ws, user_id, device_id, servers
        self._lock = asyncio.Lock()

    async def send(self, frame: dict) -> None:
        async with self._lock:
            await self.ws.send_text(json.dumps(frame, separators=(",", ":")))


@dataclass
class Session:
    id: str
    user_id: str
    server: str
    agent: AgentConn
    # In-flight client requests: JSON-RPC id -> queue of messages for that POST's SSE stream.
    pending: dict[Any, asyncio.Queue] = field(default_factory=dict)
    # Server-initiated messages with no POST to attach to (served on GET).
    unsolicited: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=100))
    last_used: float = field(default_factory=time.monotonic)
    closed: bool = False


class Hub:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.agents: dict[str, AgentConn] = {}  # user_id -> agent
        self.sessions: dict[str, Session] = {}  # session id -> session

    # -- agent lifecycle ---------------------------------------------------

    async def register(self, conn: AgentConn) -> None:
        old = self.agents.get(conn.user_id)
        self.agents[conn.user_id] = conn
        if old is not None:
            log.info("agent %s replaces %s for user %r", conn.device_id, old.device_id, conn.user_id)
            await self.unregister(old)
            try:
                await old.ws.close(code=CLOSE_REPLACED, reason="Replaced by a newer connection")
            except Exception:
                pass

    async def unregister(self, conn: AgentConn) -> None:
        if self.agents.get(conn.user_id) is conn:
            del self.agents[conn.user_id]
        for s in [s for s in self.sessions.values() if s.agent is conn]:
            await self.close_session(s, "desktop agent disconnected", notify_agent=False)

    async def on_agent_frame(self, conn: AgentConn, frame: Any) -> None:
        if not isinstance(frame, dict):
            return
        s = self.sessions.get(frame.get("session"))
        if s is None or s.agent is not conn:  # unknown, or someone else's session
            return
        kind = frame.get("type")
        if kind == "message" and isinstance(frame.get("data"), dict):
            self._route_to_client(s, frame["data"])
        elif kind == "closed":
            await self.close_session(s, frame.get("reason") or "MCP server process exited", notify_agent=False)

    def _route_to_client(self, s: Session, msg: dict) -> None:
        s.last_used = time.monotonic()
        if "method" not in msg and "id" in msg:  # a response: wake the matching POST
            q = s.pending.get(msg["id"])
            if q is not None:
                q.put_nowait(msg)
            return
        # Server-initiated request/notification (progress, logging, ...).
        # Attach it to the oldest in-flight POST; otherwise park it for GET.
        q = next(iter(s.pending.values()), s.unsolicited)
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            log.warning("dropping server message for session %s (queue full)", s.id)

    # -- session lifecycle -------------------------------------------------

    async def open_session(self, user_id: str, server: str) -> Session:
        agent = self.agents.get(user_id)
        if agent is None:
            raise RelayError(503, "No desktop agent is connected for this user. Start the agent and pair it first.")
        if server not in agent.servers:
            raise RelayError(404, f"The connected agent does not expose a server named {server!r} "
                                  f"(available: {', '.join(agent.servers) or 'none'})")
        mine = [s for s in self.sessions.values() if s.agent is agent]
        while len(mine) >= self.cfg.max_sessions_per_user:
            victim = min(mine, key=lambda s: s.last_used)
            mine.remove(victim)
            await self.close_session(victim, "evicted (too many sessions)")
        s = Session(id=secrets.token_urlsafe(24), user_id=user_id, server=server, agent=agent)
        self.sessions[s.id] = s
        try:
            await agent.send({"type": "open", "session": s.id, "server": server})
        except Exception:
            self.sessions.pop(s.id, None)
            raise RelayError(502, "Desktop agent is unreachable")
        return s

    def find_session(self, sid: str | None, user_id: str, server: str) -> Session:
        if not sid:
            raise RelayError(400, "Missing Mcp-Session-Id header (send 'initialize' first)", ERR_INVALID_REQUEST)
        s = self.sessions.get(sid)
        if s is None or s.closed or s.user_id != user_id or s.server != server:
            raise RelayError(404, "Unknown or expired session; please re-initialize")
        return s

    async def close_session(self, s: Session, reason: str, notify_agent: bool = True) -> None:
        if s.closed:
            return
        s.closed = True
        self.sessions.pop(s.id, None)
        for rpc_id, q in list(s.pending.items()):
            q.put_nowait(rpc_error(rpc_id, ERR_SERVER, f"Session closed: {reason}"))
        try:
            s.unsolicited.put_nowait(None)  # tell a GET stream to finish
        except asyncio.QueueFull:
            pass
        if notify_agent:
            try:
                await s.agent.send({"type": "close", "session": s.id})
            except Exception:
                pass

    async def forward(self, s: Session, msg: dict) -> None:
        s.last_used = time.monotonic()
        if s.closed:
            raise RelayError(404, "Session closed; please re-initialize")
        try:
            await s.agent.send({"type": "message", "session": s.id, "data": msg})
        except Exception:
            await self.close_session(s, "desktop agent unreachable", notify_agent=False)
            raise RelayError(502, "Desktop agent is unreachable")

    async def janitor(self) -> None:
        while True:
            await asyncio.sleep(60)
            now = time.monotonic()
            for s in list(self.sessions.values()):
                if not s.pending and now - s.last_used > self.cfg.session_ttl:
                    log.info("closing idle session %s", s.id)
                    await self.close_session(s, "idle timeout")


# --------------------------------------------------------------------------
# SSE streams
# --------------------------------------------------------------------------


async def response_stream(hub: Hub, s: Session, req_id: Any, q: asyncio.Queue) -> AsyncIterator[str]:
    """SSE body for one POSTed request: yields interim notifications, then the response.

    SSE (rather than a plain JSON body) is used so we can send keepalives while
    a slow desktop action runs, and relay progress notifications.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + hub.cfg.request_timeout
    try:
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                yield sse(rpc_error(req_id, ERR_TIMEOUT, f"Timed out after {hub.cfg.request_timeout:.0f}s"))
                # Per the MCP spec, a sender that times out should cancel the request.
                cancel = {"jsonrpc": "2.0", "method": "notifications/cancelled",
                          "params": {"requestId": req_id, "reason": "relay timeout"}}
                try:
                    await s.agent.send({"type": "message", "session": s.id, "data": cancel})
                except Exception:
                    pass
                return
            try:
                msg = await asyncio.wait_for(q.get(), min(hub.cfg.keepalive, remaining))
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            yield sse(msg)
            if "method" not in msg and msg.get("id") == req_id:
                return
    finally:
        s.pending.pop(req_id, None)


async def unsolicited_stream(hub: Hub, s: Session) -> AsyncIterator[str]:
    """SSE body for GET: server-initiated messages that belong to no request."""
    while not s.closed:
        try:
            msg = await asyncio.wait_for(s.unsolicited.get(), hub.cfg.keepalive)
        except asyncio.TimeoutError:
            yield ": keepalive\n\n"
            continue
        if msg is None:
            return
        yield sse(msg)


# --------------------------------------------------------------------------
# The FastAPI app
# --------------------------------------------------------------------------


def create_app(cfg: Config | None = None) -> FastAPI:
    cfg = cfg or Config.from_env()
    store = Store(cfg.db_path)
    hub = Hub(cfg)
    limiter = FailureLimiter()

    async def revoke_device(device: Device) -> None:
        """Delete a device's credentials and disconnect it if it is online."""
        store.revoke(device.id)
        conn = hub.agents.get(device.user_id)
        if conn is not None and conn.device_id == device.id:
            await hub.unregister(conn)
            try:
                await conn.ws.close(code=CLOSE_REVOKED, reason="Device revoked")
            except Exception:
                pass

    builtin = BuiltinMCP(store, hub, revoke_device, cfg.public_url)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        task = asyncio.create_task(hub.janitor())
        yield
        task.cancel()

    app = FastAPI(title="MCP relay", lifespan=lifespan)
    app.state.hub, app.state.store, app.state.cfg = hub, store, cfg

    @app.exception_handler(RelayError)
    async def relay_error_handler(_: Request, exc: RelayError):
        return JSONResponse(rpc_error(exc.rpc_id, exc.code, exc.message), status_code=exc.status)

    def front_auth(request: Request) -> str:
        """LibreChat -> relay: shared secret + which user this call is for."""
        if not key_ok(bearer(request.headers), cfg.api_key):
            raise RelayError(401, "Invalid or missing API key")
        return validate_user_id(request.headers.get(cfg.user_header))

    def admin_auth(request: Request) -> None:
        if not key_ok(bearer(request.headers), cfg.admin_key):
            raise RelayError(401, "Invalid or missing admin key")

    def check_server_name(server: str) -> None:
        if not SERVER_NAME_RE.match(server):
            raise RelayError(400, "Invalid server name")

    # -- LibreChat-facing MCP endpoints ------------------------------------

    # The relay's own management server.  It must be declared BEFORE the generic
    # /mcp/{server} routes below, because Starlette uses the first route that matches.
    @app.post("/mcp/" + BUILTIN_SERVER)
    async def builtin_post(request: Request):
        user_id = front_auth(request)
        try:
            msg = json.loads(await request.body())
        except ValueError:
            raise RelayError(400, "Parse error", ERR_PARSE)
        if not isinstance(msg, dict):
            raise RelayError(400, "JSON-RPC batches are not supported", ERR_INVALID_REQUEST)
        reply = await builtin.handle(msg, user_id)
        return Response(status_code=202) if reply is None else JSONResponse(reply)

    @app.get("/mcp/" + BUILTIN_SERVER)
    @app.delete("/mcp/" + BUILTIN_SERVER)
    async def builtin_no_stream(request: Request):
        # Stateless server: no session to delete and no server-initiated stream to open.
        front_auth(request)
        return Response(status_code=405, headers={"Allow": "POST"})

    @app.post("/mcp/{server}")
    async def mcp_post(server: str, request: Request):
        user_id = front_auth(request)
        check_server_name(server)
        try:
            msg = json.loads(await request.body())
        except ValueError:
            raise RelayError(400, "Parse error", ERR_PARSE)
        if not isinstance(msg, dict):
            raise RelayError(400, "JSON-RPC batches are not supported", ERR_INVALID_REQUEST)

        has_id, method = "id" in msg, msg.get("method")
        rpc_id = msg.get("id")
        if has_id and not isinstance(rpc_id, (int, str)):
            raise RelayError(400, "Invalid request id", ERR_INVALID_REQUEST)

        if method == "initialize":
            session = await hub.open_session(user_id, server)
        else:
            # Anything else needs a session.  This also answers the newer
            # stateless 'server/discover' probe with a 400, which makes
            # clients fall back to the classic initialize handshake.
            session = hub.find_session(request.headers.get("mcp-session-id"), user_id, server)
        headers = {"Mcp-Session-Id": session.id}

        if method is not None and has_id:  # a request: stream the response back
            if rpc_id in session.pending:
                raise RelayError(400, "Duplicate request id", ERR_INVALID_REQUEST, rpc_id)
            q: asyncio.Queue = asyncio.Queue()
            session.pending[rpc_id] = q
            try:
                await hub.forward(session, msg)
            except RelayError:
                session.pending.pop(rpc_id, None)
                raise
            return StreamingResponse(
                response_stream(hub, session, rpc_id, q),
                media_type="text/event-stream",
                headers={**headers, "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

        # A notification, or the client's answer to a server-initiated request.
        await hub.forward(session, msg)
        return Response(status_code=202, headers=headers)

    @app.get("/mcp/{server}")
    async def mcp_get(server: str, request: Request):
        user_id = front_auth(request)
        check_server_name(server)
        session = hub.find_session(request.headers.get("mcp-session-id"), user_id, server)
        return StreamingResponse(
            unsolicited_stream(hub, session),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.delete("/mcp/{server}")
    async def mcp_delete(server: str, request: Request):
        user_id = front_auth(request)
        check_server_name(server)
        session = hub.find_session(request.headers.get("mcp-session-id"), user_id, server)
        await hub.close_session(session, "terminated by client")
        return Response(status_code=200)

    # -- agent-facing endpoints ---------------------------------------------

    @app.post("/pair/redeem")
    async def pair_redeem(request: Request):
        """Public (rate limited): exchange a pairing code for a device token."""
        ip = request.client.host if request.client else "unknown"
        if limiter.blocked(ip):
            raise RelayError(429, "Too many failed attempts; try again in a minute")
        try:
            body = await request.json()
            code, name = str(body["code"]), str(body.get("device_name") or "unnamed device")
        except Exception:
            raise RelayError(400, "Expected JSON like {\"code\": \"ABCD-1234\"}")
        result = store.redeem_pairing_code(code, name)
        if result is None:
            limiter.record(ip)
            raise RelayError(400, "Invalid or expired pairing code")
        device, token = result
        log.info("paired device %s (%r) to user %r", device.id, device.name, device.user_id)
        return {"device_id": device.id, "device_token": token, "user_id": device.user_id}

    @app.websocket("/agent/connect")
    async def agent_connect(ws: WebSocket):
        token = bearer(ws.headers)
        device = store.authenticate(token) if token else None
        if device is None:
            await ws.close(code=CLOSE_UNAUTHORIZED)  # before accept() => HTTP 403
            return
        await ws.accept()
        try:
            hello = json.loads(await asyncio.wait_for(ws.receive_text(), 10))
            servers = hello["servers"]
            assert hello["type"] == "hello" and isinstance(servers, list)
            assert all(isinstance(n, str) and SERVER_NAME_RE.match(n) for n in servers)
            assert not RESERVED_SERVER_NAMES & set(servers)
        except Exception:
            await ws.close(code=CLOSE_BAD_HELLO,
                           reason="Invalid hello frame: expected a list of server names (reserved: relay)")
            return

        conn = AgentConn(ws, device.user_id, device.id, servers)
        await hub.register(conn)
        store.touch(device.id)
        log.info("agent online: device %s user %r servers %s", device.id, device.user_id, servers)
        try:
            await conn.send({"type": "welcome", "user_id": device.user_id, "device_id": device.id})
            while True:
                await hub.on_agent_frame(conn, json.loads(await ws.receive_text()))
        except Exception as exc:  # WebSocketDisconnect, bad JSON, ...
            log.info("agent offline: device %s (%s)", device.id, type(exc).__name__)
        finally:
            store.touch(device.id)
            await hub.unregister(conn)

    # -- admin endpoints (called by YOUR backend / ops, never by LibreChat) --

    @app.post("/admin/pairing-codes")
    async def admin_create_code(request: Request):
        admin_auth(request)
        try:
            user_id = validate_user_id((await request.json()).get("user_id"))
        except RelayError:
            raise
        except Exception:
            raise RelayError(400, "Expected JSON like {\"user_id\": \"...\"}")
        code, ttl = store.create_pairing_code(user_id)
        return {"code": code, "expires_in": ttl, "user_id": user_id}

    @app.get("/admin/devices")
    async def admin_list_devices(request: Request, user_id: str | None = None):
        admin_auth(request)
        out = []
        for d in store.list_devices(user_id):
            conn = hub.agents.get(d.user_id)
            out.append({**d.__dict__, "online": conn is not None and conn.device_id == d.id,
                        "servers": conn.servers if conn is not None and conn.device_id == d.id else []})
        return out

    @app.delete("/admin/devices/{device_id}")
    async def admin_revoke_device(device_id: str, request: Request):
        admin_auth(request)
        device = store.get_device(device_id)
        if device is None:
            raise RelayError(404, "No such device")
        await revoke_device(device)
        return {"revoked": device_id}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(description="MCP relay server")
    parser.add_argument("--host", default="127.0.0.1", help="bind address (put TLS in front of it!)")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    uvicorn.run(create_app(Config.from_env()), host=args.host, port=args.port, ws_max_size=MAX_WS_MESSAGE)


if __name__ == "__main__":
    main()

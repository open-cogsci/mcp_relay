"""Desktop agent: dials OUT to the relay and bridges it to local stdio MCP servers.

    # 1. one-time pairing: type the code your admin / web UI showed you
    python agent.py pair --relay https://relay.example.com --code K7QM-2XPD

    # 2. run it (keep it running; it reconnects automatically)
    python agent.py run --server hello="python hello_server.py"

``--server NAME=COMMAND`` can be repeated; the NAME becomes the URL path
(``/mcp/NAME``) on the relay.  The command can be *any* stdio MCP server
(``npx -y some-server``, ``uvx ...``, your own script, ...).  The agent starts
one process per MCP session and shuts it down when the session ends.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import random
import re
import shlex
import shutil
import socket
import sys
from pathlib import Path

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

log = logging.getLogger("agent")

MAX_MESSAGE = 32 * 1024 * 1024  # must be >= what the relay accepts; screenshots are big
FATAL_CLOSE_CODES = {4001, 4400, 4401, 4403}  # replaced / bad hello / unauthorized / revoked
DEFAULT_STATE_FILE = Path.home() / ".config" / "mcp-relay-agent" / "credentials.json"


class Fatal(Exception):
    """Reconnecting will not help."""


def dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))


# --------------------------------------------------------------------------
# One local stdio MCP server process (= one MCP session)
# --------------------------------------------------------------------------


class LocalServer:
    def __init__(self, name: str, argv: list[str], session_id: str, send, on_exit):
        self.name, self.argv, self.sid = name, argv, session_id
        self._send, self._on_exit = send, on_exit
        self._stopping = False
        self.proc: asyncio.subprocess.Process | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        # `limit` is the max size of one stdout line = one JSON-RPC message.
        self.proc = await asyncio.create_subprocess_exec(
            *self.argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=MAX_MESSAGE,
        )
        log.info("[%s] started %s (pid %s)", self.sid[:8], self.name, self.proc.pid)
        self._task = asyncio.create_task(self._supervise())

    async def write(self, msg: dict) -> None:
        """Relay -> MCP server (stdio framing: one JSON message per line)."""
        assert self.proc and self.proc.stdin
        try:
            self.proc.stdin.write(dumps(msg).encode() + b"\n")
            await self.proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            log.warning("[%s] server stdin closed", self.sid[:8])

    async def _read_stdout(self) -> None:
        assert self.proc and self.proc.stdout
        while True:
            try:
                line = await self.proc.stdout.readline()
            except ValueError:
                log.error("[%s] dropped an oversized message from the server", self.sid[:8])
                continue
            if not line:
                return
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                log.warning("[%s] non-JSON on stdout (ignored): %.200r", self.sid[:8], line)
                continue
            try:
                await self._send({"type": "message", "session": self.sid, "data": msg})
            except ConnectionClosed:
                return

    async def _read_stderr(self) -> None:
        assert self.proc and self.proc.stderr
        async for line in self.proc.stderr:
            log.info("[%s] %s stderr: %s", self.sid[:8], self.name, line.decode(errors="replace").rstrip())

    async def _supervise(self) -> None:
        assert self.proc
        stderr_task = asyncio.create_task(self._read_stderr())
        await self._read_stdout()
        code = await self.proc.wait()
        await stderr_task
        self._on_exit(self.sid)
        if not self._stopping:
            log.warning("[%s] %s exited with code %s", self.sid[:8], self.name, code)
            with contextlib.suppress(ConnectionClosed):
                await self._send({"type": "closed", "session": self.sid,
                                  "reason": f"MCP server process exited with code {code}"})

    async def stop(self) -> None:
        """Standard stdio shutdown: close stdin, then SIGTERM, then SIGKILL."""
        self._stopping = True
        if not self.proc or self.proc.returncode is not None:
            return
        with contextlib.suppress(Exception):
            self.proc.stdin.close()
        for action in (None, self.proc.terminate, self.proc.kill):
            if action:
                with contextlib.suppress(ProcessLookupError):
                    action()
            try:
                await asyncio.wait_for(self.proc.wait(), 3)
                break
            except asyncio.TimeoutError:
                continue
        if self._task:
            with contextlib.suppress(Exception):
                await self._task
        log.info("[%s] stopped %s", self.sid[:8], self.name)


# --------------------------------------------------------------------------
# Connection to the relay
# --------------------------------------------------------------------------


class Agent:
    def __init__(self, relay_url: str, token: str, servers: dict[str, list[str]]):
        self.ws_url = re.sub(r"^http", "ws", relay_url.rstrip("/"), count=1) + "/agent/connect"
        self.token, self.servers = token, servers
        self.sessions: dict[str, LocalServer] = {}
        self._background: set[asyncio.Task] = set()

    async def run_forever(self) -> None:
        delay = 1.0
        while True:
            try:
                if await self._connect_once():
                    delay = 1.0  # we were connected for a while: start backoff afresh
            except InvalidStatus as exc:
                if exc.response.status_code in (401, 403):
                    raise Fatal("The relay rejected this device's credentials (revoked?). Pair again.")
                log.warning("relay answered HTTP %s", exc.response.status_code)
            except (OSError, ConnectionClosed, asyncio.TimeoutError) as exc:
                log.warning("connection problem: %s", exc or type(exc).__name__)
            wait = delay * random.uniform(0.8, 1.2)
            log.info("reconnecting in %.0fs", wait)
            await asyncio.sleep(wait)
            delay = min(delay * 2, 60.0)

    async def _connect_once(self) -> bool:
        welcomed = False
        log.info("connecting to %s", self.ws_url)
        async with connect(
            self.ws_url,
            additional_headers={"Authorization": f"Bearer {self.token}"},
            max_size=MAX_MESSAGE,
            open_timeout=15,
        ) as ws:
            try:
                await ws.send(dumps({"type": "hello", "servers": list(self.servers)}))
                welcome = json.loads(await asyncio.wait_for(ws.recv(), 10))
                if welcome.get("type") != "welcome":
                    raise ConnectionError(f"unexpected reply from relay: {welcome}")
                welcomed = True
                log.info("connected; paired to user %r; exposing: %s",
                         welcome.get("user_id"), ", ".join(self.servers))
                async for raw in ws:
                    try:
                        await self._handle(ws, json.loads(raw))
                    except Exception:
                        log.exception("error while handling a frame from the relay")
            except ConnectionClosed:
                pass
            finally:
                await self._stop_all()
            if ws.close_code in FATAL_CLOSE_CODES:
                raise Fatal(f"Relay closed the connection permanently ({ws.close_code}: {ws.close_reason}).")
        return welcomed

    async def _handle(self, ws, frame: dict) -> None:
        kind, sid = frame.get("type"), frame.get("session")

        async def send(f: dict) -> None:
            await ws.send(dumps(f))

        if kind == "open":
            name = frame.get("server")
            argv = self.servers.get(name)
            if argv is None:
                await send({"type": "closed", "session": sid, "reason": f"unknown server {name!r}"})
                return
            server = LocalServer(name, argv, sid, send, on_exit=lambda s: self.sessions.pop(s, None))
            try:
                await server.start()  # started before the next frame (the 'initialize') is handled
            except OSError as exc:
                await send({"type": "closed", "session": sid, "reason": f"could not start server: {exc}"})
                return
            self.sessions[sid] = server
        elif kind == "message":
            server = self.sessions.get(sid)
            if server and isinstance(frame.get("data"), dict):
                await server.write(frame["data"])
        elif kind == "close":
            server = self.sessions.pop(sid, None)
            if server:
                task = asyncio.create_task(server.stop())
                self._background.add(task)
                task.add_done_callback(self._background.discard)

    async def _stop_all(self) -> None:
        servers, self.sessions = list(self.sessions.values()), {}
        await asyncio.gather(*(s.stop() for s in servers), return_exceptions=True)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        sys.exit(f"Not paired yet ({path} not found). Run:  python agent.py pair --relay URL --code CODE")


def save_state(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)  # the token is a credential


def cmd_pair(args) -> None:
    relay = args.relay.rstrip("/")
    try:
        r = httpx.post(f"{relay}/pair/redeem", json={"code": args.code, "device_name": args.name}, timeout=15)
    except httpx.HTTPError as exc:
        sys.exit(f"Could not reach the relay at {relay}: {exc}")
    if r.status_code != 200:
        try:
            message = r.json()["error"]["message"]
        except Exception:
            message = r.text[:200]
        sys.exit(f"Pairing failed (HTTP {r.status_code}): {message}")
    data = r.json()
    save_state(args.state_file, {"relay": relay, "device_id": data["device_id"], "device_token": data["device_token"]})
    print(f"Paired as device {data['device_id']} for user {data['user_id']!r}.")
    print(f"Credentials saved to {args.state_file}. Now start the agent with:  python agent.py run --server NAME=COMMAND")


def parse_server_spec(spec: str) -> tuple[str, list[str]]:
    name, sep, command = spec.partition("=")
    if not sep or not name or not command.strip():
        sys.exit(f"Invalid --server {spec!r}; expected NAME=COMMAND")
    if os.name == "nt":  # keep backslashes in Windows paths
        argv = [t.strip('"') for t in shlex.split(command, posix=False)]
    else:
        argv = shlex.split(command)
    if shutil.which(argv[0]) is None:
        sys.exit(f"Command not found for server {name!r}: {argv[0]}")
    return name, argv


def cmd_run(args) -> None:
    state = load_state(args.state_file)
    servers = dict(parse_server_spec(s) for s in args.server)
    agent = Agent(state["relay"], state["device_token"], servers)
    try:
        asyncio.run(agent.run_forever())
    except Fatal as exc:
        sys.exit(f"Stopped: {exc}")
    except KeyboardInterrupt:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Desktop agent for the MCP relay")
    common = argparse.ArgumentParser(add_help=False)  # options accepted after either subcommand
    common.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE, help="where credentials are stored")
    common.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("pair", parents=[common], help="exchange a pairing code for stored credentials")
    p.add_argument("--relay", required=True, help="relay base URL, e.g. https://relay.example.com")
    p.add_argument("--code", required=True, help="pairing code, e.g. K7QM-2XPD")
    p.add_argument("--name", default=socket.gethostname(), help="device name shown to admins")
    p.set_defaults(func=cmd_pair)

    r = sub.add_parser("run", parents=[common], help="connect to the relay and serve local MCP servers")
    r.add_argument("--server", action="append", required=True, metavar="NAME=COMMAND",
                   help="a local stdio MCP server to expose; repeatable")
    r.set_defaults(func=cmd_run)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    args.func(args)


if __name__ == "__main__":
    main()

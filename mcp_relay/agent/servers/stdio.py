"""Stdio subprocess provider: one child process per relay session.

This is the classic MCP deployment: the "server" is any executable that
speaks MCP over stdio (`python my_server.py`, `uvx some-server`,
`node_modules/.bin/mcp-server-filesystem ...`, ...).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import shlex
import shutil

from ..protocol import MAX_MESSAGE, RESERVED_SERVER_NAMES, SERVER_NAME_RE, dumps
from .base import OnExit, SendFrame

log = logging.getLogger("relay-agent")


class LocalServer:
    """One local stdio MCP server process (= one relay session)."""

    def __init__(self, name: str, argv: list[str], session_id: str, send: SendFrame, on_exit: OnExit):
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
            except Exception:
                return  # relay connection is gone

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
            with contextlib.suppress(Exception):
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


class StdioServerProvider:
    """Expose a stdio command under a server name."""

    def __init__(self, name: str, argv: list[str]):
        if not SERVER_NAME_RE.match(name) or name in RESERVED_SERVER_NAMES:
            raise ValueError(f"Invalid or reserved server name: {name!r}")
        self._name, self._argv = name, list(argv)

    @property
    def name(self) -> str:
        return self._name

    @property
    def argv(self) -> list[str]:
        return list(self._argv)

    def create(self, session_id: str, send: SendFrame, on_exit: OnExit) -> LocalServer:
        return LocalServer(self._name, self._argv, session_id, send, on_exit)


def parse_server_spec(spec: str) -> tuple[str, list[str]]:
    """Parse a 'NAME=COMMAND' CLI spec into (name, argv). Raises ValueError."""
    name, sep, command = spec.partition("=")
    if not sep or not name or not command.strip():
        raise ValueError(f"Invalid --server {spec!r}; expected NAME=COMMAND")
    if not SERVER_NAME_RE.match(name) or name in RESERVED_SERVER_NAMES:
        raise ValueError(f"Invalid or reserved server name {name!r}")
    if os.name == "nt":  # keep backslashes in Windows paths
        argv = [t.strip('"') for t in shlex.split(command, posix=False)]
    else:
        argv = shlex.split(command)
    if shutil.which(argv[0]) is None:
        raise ValueError(f"Command not found for server {name!r}: {argv[0]}")
    return name, argv

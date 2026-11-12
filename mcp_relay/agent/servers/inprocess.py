"""In-process MCP server provider.

Runs an `mcp` SDK server *inside this process* on in-memory streams, one
instance per relay session. Use this when the MCP tools must touch objects
that live in your own process (e.g. a Qt application whose tools manipulate
GUI widgets) instead of spawning a subprocess.

The server factory is a zero-argument callable returning an ``MCPServer``
(mcp>=2) / ``FastMCP`` (mcp 1.x) or a low-level ``Server``; use
``servers.mcp_server_cls()`` for a version-agnostic class. Because the
factory is called per session, give each session a fresh server instance.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, Callable

from ..protocol import RESERVED_SERVER_NAMES, SERVER_NAME_RE
from . import _compat
from .base import OnExit, SendFrame

log = logging.getLogger("relay-agent")


class InProcessServerInstance:
    """One in-process MCP server, pumped over anyio memory streams."""

    def __init__(self, server_factory: Callable[[], Any], session_id: str,
                 send: SendFrame, on_exit: OnExit):
        self._factory, self.sid = server_factory, session_id
        self._send, self._on_exit = send, on_exit
        self._stopping = False
        self._to_server_send = None
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        import anyio

        # Plain anyio memory streams satisfy the SDK's ReadStream/WriteStream
        # protocols on both mcp 1.x and 2.x.
        self._to_server_send, to_server_recv = anyio.create_memory_object_stream(100)
        from_server_send, from_server_recv = anyio.create_memory_object_stream(100)
        server = _compat.lowlevel_server(self._factory())
        self._tasks = [
            asyncio.create_task(self._run(server, to_server_recv, from_server_send)),
            asyncio.create_task(self._pump(from_server_recv)),
        ]
        log.info("[%s] in-process MCP server started", self.sid[:8])

    async def _run(self, server, read_stream, write_stream) -> None:
        try:
            await server.run(read_stream, write_stream, server.create_initialization_options())
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[%s] in-process MCP server crashed", self.sid[:8])
        finally:
            if not self._stopping:
                self._on_exit(self.sid)
                with contextlib.suppress(Exception):
                    await self._send({"type": "closed", "session": self.sid,
                                      "reason": "in-process MCP server exited"})

    async def _pump(self, recv) -> None:
        try:
            async for sm in recv:
                if isinstance(sm, BaseException):
                    log.warning("[%s] in-process server stream error: %r", self.sid[:8], sm)
                    continue
                try:
                    await self._send({"type": "message", "session": self.sid,
                                      "data": _compat.message_to_dict(sm)})
                except Exception:
                    return  # relay connection is gone
        except asyncio.CancelledError:
            raise

    async def write(self, msg: dict) -> None:
        """Relay -> in-process server."""
        await self._to_server_send.send(_compat.to_session_message(msg))

    async def stop(self) -> None:
        self._stopping = True
        if self._to_server_send is not None:
            with contextlib.suppress(Exception):
                await self._to_server_send.aclose()  # EOF: lets the server exit gracefully
        try:
            await asyncio.wait_for(asyncio.gather(*self._tasks, return_exceptions=True), 5)
        except asyncio.TimeoutError:
            for t in self._tasks:
                t.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("[%s] in-process MCP server stopped", self.sid[:8])


class InProcessServerProvider:
    """Expose an in-process MCP server under a server name."""

    def __init__(self, name: str, server_factory: Callable[[], Any]):
        if not SERVER_NAME_RE.match(name) or name in RESERVED_SERVER_NAMES:
            raise ValueError(f"Invalid or reserved server name: {name!r}")
        self._name, self._factory = name, server_factory

    @property
    def name(self) -> str:
        return self._name

    def create(self, session_id: str, send: SendFrame, on_exit: OnExit) -> InProcessServerInstance:
        return InProcessServerInstance(self._factory, session_id, send, on_exit)

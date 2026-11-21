"""In-process MCP server provider.

Runs an `mcp` 2.x SDK server *inside this process* on in-memory streams, one
instance per relay session. Use this when the MCP tools must touch objects
that live in your own process (e.g. a Qt application whose tools manipulate
GUI widgets) instead of spawning a subprocess.

The server factory is a zero-argument callable returning an ``MCPServer``
(or a low-level ``Server``). Because the factory is called per session, give
each session a fresh server instance.

Status messages: an ``MCPServer`` may define a ``status_message`` property,
and while a relay session is open, ``InProcessServerProvider.status_message``
reports that property of the newest live server instance, so the agent GUI's
toggle button can show live status in small print. Between sessions no
server instance exists, and the button falls back to the "Click to disable"
default.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import Any

import anyio

from ..protocol import RESERVED_SERVER_NAMES, SERVER_NAME_RE
from .base import OnExit, ProviderBase, SendFrame, session_message_to_dict, to_session_message

log = logging.getLogger("relay-agent")


class InProcessServerInstance:
    """One in-process MCP server, pumped over anyio memory streams.

    When created through the provider it registers its MCPServer with the
    provider on start() and unregisters it on stop(), so the provider's
    status_message can report it (see InProcessServerProvider.status_message).
    """

    def __init__(self, server_factory: Callable[..., Any], session_id: str,
                 send: SendFrame, on_exit: OnExit, args: tuple = (),
                 provider: "InProcessServerProvider | None" = None):
        self._factory, self.sid = server_factory, session_id
        self._send, self._on_exit = send, on_exit
        self._args = tuple(args)
        self._provider = provider
        self._server: Any = None  # the object the factory produced; live while the session runs
        self._stopping = False
        self._to_server_send = None
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        # Plain anyio memory streams satisfy the SDK's read/write stream protocols.
        self._to_server_send, to_server_recv = anyio.create_memory_object_stream(100)
        from_server_send, from_server_recv = anyio.create_memory_object_stream(100)
        server = self._factory(*self._args)
        self._server = server
        if self._provider is not None:
            # Newly live MCPServer: from now on the provider can surface this
            # server's status_message on the GUI toggle button.
            self._provider._register_live_server(server)
        lowlevel = getattr(server, "_lowlevel_server", server)  # MCPServer -> low-level Server
        self._tasks = [
            asyncio.create_task(self._run(lowlevel, to_server_recv, from_server_send)),
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
                                      "data": session_message_to_dict(sm)})
                except Exception:
                    return  # relay connection is gone
        except asyncio.CancelledError:
            raise

    async def write(self, msg: dict) -> None:
        """Relay -> in-process server."""
        await self._to_server_send.send(to_session_message(msg))

    async def stop(self) -> None:
        self._stopping = True
        server, self._server = self._server, None
        if server is not None and self._provider is not None:
            self._provider._unregister_live_server(server)
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


class InProcessServerProvider(ProviderBase):
    """Expose an in-process MCP server under a server name.

    ``server_factory`` is called once per session as ``factory(*args)``, where
    ``args`` is the (possibly empty) tuple returned by ``args_provider`` when
    the provider is *enabled* (see ProviderBase; the result is cached until the
    provider is disabled again) -- e.g. ``make_server(root_dir)``. Zero-argument
    factories keep working when no ``args_provider`` is set.

    ``status_message``: the factory runs once per relay session, so a *live*
    MCPServer exists only while a session is open. Every instance created
    via :meth:`create` therefore registers its server with this provider when
    it starts and unregisters it when it stops; the ``status_message``
    property then reports the newest live server's own ``status_message``
    property when it defines one -- the GUI toggle button shows it and falls
    back to ProviderBase's "Click to disable" between sessions (or when the
    server has no such property).
    """

    def __init__(self, name: str, server_factory: Callable[..., Any], *,
                 display_name: str | None = None, enabled: bool = True,
                 args_provider=None):
        if not SERVER_NAME_RE.match(name) or name in RESERVED_SERVER_NAMES:
            raise ValueError(f"Invalid or reserved server name: {name!r}")
        super().__init__(name, display_name, enabled, args_provider)
        self._factory = server_factory
        self._live_servers: list[Any] = []  # live MCPServer objects; newest last (see status_message)

    # -- live servers / status message -------------------------------------

    def _register_live_server(self, server: Any) -> None:
        """Called by InProcessServerInstance.start() (on the agent's asyncio
        thread) when a new live MCPServer exists."""
        self._live_servers.append(server)

    def _unregister_live_server(self, server: Any) -> None:
        """Called by the instance's stop() when its MCPServer goes away."""
        with contextlib.suppress(ValueError):
            self._live_servers.remove(server)

    @property
    def status_message(self) -> str:
        """The newest live MCPServer's ``status_message`` when it defines one;
        ProviderBase's click-to-enable / click-to-disable otherwise.

        The GUI polls this (on the GUI thread), so an MCPServer-side
        ``status_message`` property must only read thread-safe state.
        """
        if not self._enabled:
            return super().status_message  # "Click to enable"
        # Snapshot before iterating: the agent's asyncio thread registers
        # (start) and unregisters (stop) live servers while the GUI polls.
        for server in reversed(list(self._live_servers)):
            # The provider registers the object the factory returned -- for
            # an MCPServer, respond as the low-level Server would, so absent
            # status_message properties never raise on either kind.
            try:
                message = getattr(server, "status_message", None)
            except Exception:
                log.exception("reading a live in-process server's "
                              "status_message failed")
                continue
            if message is not None:
                return message if isinstance(message, str) else str(message)
        return super().status_message  # "Click to disable": nothing live to ask

    def create(self, session_id: str, send: SendFrame, on_exit: OnExit) -> InProcessServerInstance:
        # Resolved (and cached) when the provider was enabled; this is only a
        # fallback for providers never enabled through the setter.
        args = self.resolve_args()
        if args is None:
            args = ()
        elif isinstance(args, (list, tuple)):
            args = tuple(args)
        else:
            args = (args,)
        # The new instance registers its MCPServer with self when it starts,
        # so status_message can find it (see _register_live_server).
        return InProcessServerInstance(self._factory, session_id, send, on_exit,
                                       args, provider=self)

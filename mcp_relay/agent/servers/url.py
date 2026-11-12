"""URL-based provider: proxy each relay session to a remote streamable-HTTP MCP server.

The remote endpoint can be any MCP server reachable over streamable HTTP
(including another instance of this relay). The SDK's client transport
handles the initialize handshake, Mcp-Session-Id, the GET stream for
server-initiated messages and DELETE-on-close; we only pump JSON-RPC
messages between the relay WebSocket and the transport's stream pair, so
the relay stays completely agnostic.

Task-affinity note (important): ``streamable_http_client`` is an async context
manager wrapping an anyio task group + cancel scopes, and anyio scopes may only
be exited in the task that entered them. So the transport is entered AND exited
inside one dedicated task (``_run``) per session, with an ``asyncio.Queue`` as
the only interface from the outside. Entering it in the relay's WebSocket-loop
task (the previous design) meant that a single remote failure (e.g. connection
refused) cancelled that loop's cancel scope and took down the *whole* agent.
Now a remote failure only ends its own session, reported to the relay with a
``{"type": "closed", ...}`` frame.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from ..protocol import RESERVED_SERVER_NAMES, SERVER_NAME_RE
from . import _compat
from .base import OnExit, SendFrame

log = logging.getLogger("relay-agent")

_START_TIMEOUT = 15.0
_STOP_TIMEOUT = 10.0


class UrlServerInstance:
    """One relay session proxied to a remote streamable-HTTP MCP server."""

    def __init__(self, url: str, headers: dict | None, session_id: str,
                 send: SendFrame, on_exit: OnExit):
        self.url, self.headers, self.sid = url, headers, session_id
        self._send, self._on_exit = send, on_exit
        self._stopping = False
        self._to_remote: asyncio.Queue | None = None  # relay -> remote messages
        self._ready = asyncio.Event()
        self._start_error: str | None = None
        self._task: asyncio.Task | None = None
        self._client = None  # owned httpx2 client, if we had to create one

    def _make_cm(self):
        from mcp.client.streamable_http import streamable_http_client

        if not self.headers:
            return streamable_http_client(self.url)
        try:
            # mcp 1.x accepts headers directly
            return streamable_http_client(self.url, headers=self.headers)
        except TypeError:
            # mcp >= 2.x takes an httpx2 client instead
            import httpx2

            self._client = httpx2.AsyncClient(headers=self.headers)
            return streamable_http_client(self.url, http_client=self._client)

    async def start(self) -> None:
        """Start the dedicated proxy task. The remote is contacted lazily (on the
        first forwarded message), so an unreachable remote does not fail here; it
        fails later and is reported as a ``closed`` frame."""
        self._to_remote = asyncio.Queue()
        self._task = asyncio.create_task(self._run())
        try:
            await asyncio.wait_for(asyncio.shield(self._ready.wait()), _START_TIMEOUT)
        except asyncio.TimeoutError:
            await self.stop()
            raise RuntimeError(f"timed out setting up proxy to {self.url}") from None
        if self._start_error:
            raise RuntimeError(self._start_error)
        log.info("[%s] proxying to %s", self.sid[:8], self.url)

    async def _run(self) -> None:
        """Own the transport CM and both pump directions. Runs in its own task so
        that entering and exiting anyio cancel scopes happens in the same task."""
        error: BaseException | None = None
        try:
            async with self._make_cm() as (read, write):
                self._ready.set()
                pump = asyncio.create_task(self._pump_in(write))
                try:
                    async for sm in read:  # remote -> relay
                        if isinstance(sm, BaseException):
                            log.warning("[%s] remote %s stream error: %r", self.sid[:8], self.url, sm)
                            continue
                        await self._send({"type": "message", "session": self.sid,
                                          "data": _compat.message_to_dict(sm)})
                finally:
                    pump.cancel()
                    # return_exceptions so the pump's cancellation doesn't mask anything
                    await asyncio.gather(pump, return_exceptions=True)
        except asyncio.CancelledError:
            raise  # our own stop(): unwind quietly, finally below skips the closed frame
        except BaseException as exc:  # transport failure (connect refused, HTTP error, ...)
            error = exc
            log.warning("[%s] proxy to %s failed: %r", self.sid[:8], self.url, exc)
        finally:
            if error is not None and not self._ready.is_set():
                self._start_error = f"could not set up proxy to {self.url}: {error}"
            self._ready.set()
            if self._client is not None:
                with contextlib.suppress(BaseException):
                    await asyncio.shield(self._client.aclose())
                self._client = None
            if not self._stopping:
                self._on_exit(self.sid)
                reason = (f"remote MCP server at {self.url} failed: {error}"
                          if error is not None else
                          f"remote MCP server at {self.url} disconnected")
                # Shielded + broad except: this finally also runs while the task
                # is being cancelled, and the 'closed' frame must still go out.
                # Swallowing the new CancelledError here is safe: the original
                # one from the try block keeps propagating after the finally.
                with contextlib.suppress(BaseException):
                    await asyncio.shield(self._send({"type": "closed", "session": self.sid,
                                                     "reason": reason[:300]}))

    async def _pump_in(self, write) -> None:
        """relay -> remote: forward JSON-RPC frames into the transport's write stream."""
        assert self._to_remote is not None
        try:
            while True:
                msg = await self._to_remote.get()
                if msg is None:  # stop sentinel
                    return
                try:
                    sm = _compat.to_session_message(msg)
                except Exception:
                    log.warning("[%s] dropping invalid JSON-RPC frame from relay: %.200r",
                                self.sid[:8], msg)
                    continue
                await write.send(sm)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[%s] relay->remote pump failed", self.sid[:8])

    async def write(self, msg: dict) -> None:
        """Relay -> remote server (requests, notifications and responses alike).
        Never raises: the queue is unbounded and pump errors surface as `closed`."""
        if self._to_remote is not None:
            self._to_remote.put_nowait(msg)

    async def stop(self) -> None:
        """Cancel the proxy task. Safe to call from any task, more than once, and
        even while the surrounding task is being cancelled (shielded)."""
        self._stopping = True
        if self._to_remote is not None:
            with contextlib.suppress(Exception):
                self._to_remote.put_nowait(None)
        task, self._task = self._task, None
        if task is None or task.done():
            return
        task.cancel()  # unwinds the transport CM inside its own task
        try:
            await asyncio.wait_for(asyncio.shield(task), _STOP_TIMEOUT)
        except asyncio.CancelledError:
            if task.cancelled():
                return  # instance task ended as cancelled: expected
            raise  # we ourselves were cancelled; the shielded task finishes in background
        except asyncio.TimeoutError:
            log.warning("[%s] proxy task to %s did not stop within %.0fs", self.sid[:8], self.url, _STOP_TIMEOUT)
        except Exception:
            pass  # transport teardown error: already logged in _run
        log.info("[%s] stopped proxy to %s", self.sid[:8], self.url)


class UrlServerProvider:
    """Expose a remote streamable-HTTP MCP server under a server name."""

    def __init__(self, name: str, url: str, headers: dict | None = None):
        if not SERVER_NAME_RE.match(name) or name in RESERVED_SERVER_NAMES:
            raise ValueError(f"Invalid or reserved server name: {name!r}")
        if not url.startswith(("http://", "https://")):
            raise ValueError(f"url must be http(s)://, got {url!r}")
        self._name, self._url, self._headers = name, url, dict(headers or {})

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return self._url

    def create(self, session_id: str, send: SendFrame, on_exit: OnExit) -> UrlServerInstance:
        return UrlServerInstance(self._url, self._headers, session_id, send, on_exit)

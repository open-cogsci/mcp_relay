"""Provider/instance protocols shared by all local MCP server kinds.

A *provider* is registered under a name in the agent's "hello" frame. For
every relay session ("open" frame) the client asks the provider to *create*
one *instance*; each instance corresponds to exactly one MCP session on the
relay side, so no JSON-RPC id remapping is ever needed.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Protocol

SendFrame = Callable[[dict], Awaitable[None]]  # send one frame to the relay
OnExit = Callable[[str], None]  # called when an instance exits on its own


class LocalServerInstance(Protocol):
    """One live MCP server instance (= one relay session).

    Lifecycle: start() once, write() for every relay "message" frame,
    stop() on relay "close" or on disconnect.
    """

    async def start(self) -> None: ...
    async def write(self, msg: dict) -> None: ...
    async def stop(self) -> None: ...


class ServerProvider(Protocol):
    """Factory for per-session instances, advertised under ``name``."""

    @property
    def name(self) -> str: ...

    def create(self, session_id: str, send: SendFrame, on_exit: OnExit) -> LocalServerInstance: ...

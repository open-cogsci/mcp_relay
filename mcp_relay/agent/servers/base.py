"""Provider/instance protocols shared by all local MCP server kinds, plus the
small helpers that convert between raw JSON-RPC dicts (what the relay
forwards) and the SDK's ``SessionMessage`` objects (what SDK streams carry).

A *provider* is registered under a name in the agent's "hello" frame. For
every relay session ("open" frame) the client asks the provider to *create*
one *instance*; each instance corresponds to exactly one MCP session on the
relay side, so no JSON-RPC id remapping is ever needed.
"""

from collections.abc import Awaitable, Callable
from typing import Protocol

from mcp.shared.message import SessionMessage
from mcp_types import jsonrpc_message_adapter

SendFrame = Callable[[dict], Awaitable[None]]  # send one frame to the relay
OnExit = Callable[[str], None]  # called when an instance exits on its own


def to_session_message(msg: dict) -> SessionMessage:
    """Wrap a raw JSON-RPC dict in a SessionMessage for an SDK stream."""
    return SessionMessage(message=jsonrpc_message_adapter.validate_python(msg))


def session_message_to_dict(sm: SessionMessage) -> dict:
    """Serialize a SessionMessage back to a plain JSON-RPC dict for the relay."""
    return sm.message.model_dump(by_alias=True, mode="json", exclude_none=True)


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

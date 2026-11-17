"""Provider/instance protocols shared by all local MCP server kinds, plus the
small helpers that convert between raw JSON-RPC dicts (what the relay
forwards) and the SDK's ``SessionMessage`` objects (what SDK streams carry).

A *provider* is registered under a name in the agent's "hello" frame. For
every relay session ("open" frame) the client asks the provider to *create*
one *instance*; each instance corresponds to exactly one MCP session on the
relay side, so no JSON-RPC id remapping is ever needed.

Beyond ``name``, providers may carry three optional pieces of metadata that
drive the agent GUI (see ``mcp_relay.agent.widget.RelayWidget``):

* ``display_name`` -- human-readable label for the GUI (the protocol ``name``
  is limited to ``[A-Za-z0-9_.-]{1,64}`` by SERVER_NAME_RE).
* ``enabled`` -- whether the server is currently exposed to the relay. The
  client only advertises enabled providers and sends a "servers" frame when
  the set changes, so this can be toggled at runtime.
* ``args_provider`` -- optional zero-argument callable returning start
  arguments for the server (see :func:`call_args_provider`). Called from the
  agent's asyncio thread when a session is opened, i.e. *not* necessarily
  from the GUI thread.
"""

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from mcp.shared.message import SessionMessage
from mcp_types import jsonrpc_message_adapter

SendFrame = Callable[[dict], Awaitable[None]]  # send one frame to the relay
OnExit = Callable[[str], None]  # called when an instance exits on its own

# Optional metadata; anything registered as a provider may define these.
ArgsProvider = Callable[[], Any]  # returns start arguments for the server


def to_session_message(msg: dict) -> SessionMessage:
    """Wrap a raw JSON-RPC dict in a SessionMessage for an SDK stream."""
    return SessionMessage(message=jsonrpc_message_adapter.validate_python(msg))


def session_message_to_dict(sm: SessionMessage) -> dict:
    """Serialize a SessionMessage back to a plain JSON-RPC dict for the relay."""
    return sm.message.model_dump(by_alias=True, mode="json", exclude_none=True)


def call_args_provider(provider: Any) -> Any:
    """Call a provider's optional ``args_provider`` and return its result.

    Runs wherever the caller runs (the agent's asyncio thread when a session
    opens); a provider whose arguments need GUI interaction must marshal the
    call to the GUI thread itself (the widget provides a helper for that).
    Raises whatever the callable raises -- callers turn that into a failed
    session (a "closed" frame), never a crashed agent.
    """
    fn = getattr(provider, "args_provider", None)
    if fn is None:
        return None
    return fn()


class ProviderBase:
    """Shared implementation of the optional provider metadata.

    Holds ``display_name`` (defaults to the protocol ``name``), the mutable
    ``enabled`` flag (default True) and the optional ``args_provider``
    callable. The three concrete providers inherit from this so the widget
    can treat them uniformly via getattr().
    """

    def __init__(self, name: str, display_name: str | None = None,
                 enabled: bool = True, args_provider: ArgsProvider | None = None):
        self._name = name
        self._display_name = display_name or name
        self._enabled = bool(enabled)
        self._args_provider = args_provider

    @property
    def name(self) -> str:
        """Protocol name: the URL path (/mcp/<name>) on the relay."""
        return self._name

    @property
    def display_name(self) -> str:
        """Human-readable name for the GUI; defaults to ``name``."""
        return self._display_name

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = bool(value)

    @property
    def args_provider(self) -> ArgsProvider | None:
        return self._args_provider


class LocalServerInstance(Protocol):
    """One live MCP server instance (= one relay session).

    Lifecycle: start() once, write() for every relay "message" frame,
    stop() on relay "close" or on disconnect.
    """

    async def start(self) -> None: ...
    async def write(self, msg: dict) -> None: ...
    async def stop(self) -> None: ...


class ServerProvider(Protocol):
    """Factory for per-session instances, advertised under ``name``.

    Optional attributes (checked with getattr, so plain provider objects
    without them keep working): ``display_name: str``, ``enabled: bool`` and
    ``args_provider: ArgsProvider | None``.
    """

    @property
    def name(self) -> str: ...

    def create(self, session_id: str, send: SendFrame, on_exit: OnExit) -> LocalServerInstance: ...

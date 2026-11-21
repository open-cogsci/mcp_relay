"""Provider/instance protocols shared by all local MCP server kinds, plus the
small helpers that convert between raw JSON-RPC dicts (what the relay
forwards) and the SDK's ``SessionMessage`` objects (what SDK streams carry).

A *provider* is registered under a name in the agent's "hello" frame. For
every relay session ("open" frame) the client asks the provider to *create*
one *instance*; each instance corresponds to exactly one MCP session on the
relay side, so no JSON-RPC id remapping is ever needed.

Beyond ``name``, providers may carry optional pieces of metadata that drive
the agent GUI (see ``mcp_relay.agent.widget.RelayWidget``):

* ``display_name`` -- human-readable label for the GUI (the protocol ``name``
  is limited to ``[A-Za-z0-9_.-]{1,64}`` by SERVER_NAME_RE).
* ``enabled`` -- whether the server is currently exposed to the relay. The
  client only advertises enabled providers and sends a "servers" frame when
  the set changes, so this can be toggled at runtime.
* ``args_provider`` -- optional zero-argument callable returning start
  arguments for the server (see :func:`call_args_provider`). It is called
  when the provider is *enabled* -- at widget initialization for servers that
  start enabled, and anew every time a server is enabled afterwards -- and
  the result is cached and reused for every session until the provider is
  disabled again (see :meth:`ProviderBase.resolve_args`).
* ``status_message`` -- read-only property shown in small print inside the
  GUI toggle button, under the ``display_name``: ProviderBase defaults to
  "Click to enable" while disabled and "Click to disable" while enabled, and
  InProcessServerProvider overrides it to forward a live MCPServer's own
  ``status_message`` property while a relay session is open.
"""

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from mcp.shared.message import SessionMessage
from mcp_types import jsonrpc_message_adapter

SendFrame = Callable[[dict], Awaitable[None]]  # send one frame to the relay
OnExit = Callable[[str], None]  # called when an instance exits on its own

# Optional metadata; anything registered as a provider may define these.
ArgsProvider = Callable[[], Any]  # returns start arguments for the server

# Default status_message texts for the GUI toggle buttons (see
# ``ProviderBase.status_message``): while disabled the button tells the user
# what clicking it does; while enabled there is nothing to say -- unless a
# live in-process server's own status_message takes over (see
# InProcessServerProvider.status_message).
STATUS_MESSAGE_ENABLED = "Click to disable"
STATUS_MESSAGE_DISABLED = "Click to enable"


def to_session_message(msg: dict) -> SessionMessage:
    """Wrap a raw JSON-RPC dict in a SessionMessage for an SDK stream."""
    return SessionMessage(message=jsonrpc_message_adapter.validate_python(msg))


def session_message_to_dict(sm: SessionMessage) -> dict:
    """Serialize a SessionMessage back to a plain JSON-RPC dict for the relay."""
    return sm.message.model_dump(by_alias=True, mode="json", exclude_none=True)


def call_args_provider(provider: Any) -> Any:
    """Call a provider's optional ``args_provider`` and return its result.

    Called by :meth:`ProviderBase.resolve_args` when the provider is enabled
    (which may happen on the GUI thread, e.g. from the widget's toggle
    buttons). A provider whose arguments need GUI interaction can simply show
    a Qt dialog; use ``mcp_relay.agent.call_on_gui_thread`` only when the
    callable might run on a non-GUI thread (it runs inline on the GUI
    thread). Raises whatever the callable raises -- the enable then fails and
    the provider stays disabled, and the client turns a late failure into a
    failed session (a "closed" frame), never a crashed agent.
    """
    fn = getattr(provider, "args_provider", None)
    if fn is None:
        return None
    return fn()


_UNRESOLVED = object()  # sentinel: start args not asked yet (None is a valid answer)


class ProviderBase:
    """Shared implementation of the optional provider metadata.

    Holds ``display_name`` (defaults to the protocol ``name``), the mutable
    ``enabled`` flag (default True), the optional ``args_provider`` callable
    and the read-only ``status_message`` property for the GUI toggle button
    (see :attr:`status_message`). The three concrete providers inherit from
    this so the widget can treat them uniformly via getattr().

    Setting ``enabled = True`` asks for the start arguments synchronously in
    the caller's thread (see :meth:`resolve_args`) and may raise -- e.g. when
    the user cancels a dialog; the flag is then left unchanged, so a failed
    enable keeps the server disabled. Setting ``enabled = False`` invalidates
    the cached arguments, so the next enable asks anew.
    """

    def __init__(self, name: str, display_name: str | None = None,
                 enabled: bool = True, args_provider: ArgsProvider | None = None):
        self._name = name
        self._display_name = display_name or name
        self._args_provider = args_provider
        self._resolved_args = _UNRESOLVED
        self._enabled = bool(enabled)  # direct assignment: construction never asks for args

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
        value = bool(value)
        if value:
            # Ask for the start arguments now; each enable asks anew. If this
            # raises, the flag is left unchanged: the server stays disabled.
            self.resolve_args()
            self._enabled = True
        else:
            self._enabled = False
            self.invalidate_args()

    @property
    def args_provider(self) -> ArgsProvider | None:
        return self._args_provider

    @property
    def status_message(self) -> str:
        """A short user-facing status message, shown in small print inside the
        GUI toggle button, under the ``display_name``.

        The default describes the toggle action the user sees: "Click to
        enable" while disabled, "Click to disable" while enabled. Only the
        in-process kind can know more (it may forward a live MCPServer's
        status_message, see InProcessServerProvider). The GUI polls this
        property on a short timer, so overrides must be cheap and only read
        thread-safe state.
        """
        return STATUS_MESSAGE_ENABLED if self._enabled else STATUS_MESSAGE_DISABLED

    def resolve_args(self) -> Any:
        """Return the start arguments, calling ``args_provider`` once if they
        were not resolved yet (or were invalidated by a disable).

        Called automatically when the provider is enabled. ``create()`` also
        calls this as a fallback for providers that were never enabled through
        the setter (e.g. a CLI-built provider constructed with ``enabled=True``),
        in which case the callable runs on the agent thread.
        """
        if self._resolved_args is _UNRESOLVED:
            self._resolved_args = call_args_provider(self)
        return self._resolved_args

    def invalidate_args(self) -> None:
        """Drop the cached start arguments; the next :meth:`resolve_args` (or
        enable) asks the ``args_provider`` anew."""
        self._resolved_args = _UNRESOLVED


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
    without them keep working): ``display_name: str``, ``enabled: bool``,
    ``args_provider: ArgsProvider | None``, ``status_message: str`` (see
    ProviderBase.status_message) and, on ProviderBase, the ``resolve_args()``
    / ``invalidate_args()`` methods used to ask for the start arguments
    when the provider is enabled.
    """

    @property
    def name(self) -> str: ...

    def create(self, session_id: str, send: SendFrame, on_exit: OnExit) -> LocalServerInstance: ...

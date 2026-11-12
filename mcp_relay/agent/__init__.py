"""mcp_relay.agent: the generic desktop side of the MCP relay.

The relay (server.py, running in the cloud) lets a remote AI client (e.g.
LibreChat) use MCP servers that run on a user's desktop. This package is the
desktop side: it dials OUT to the relay over a WebSocket and bridges relay
sessions to local MCP servers of any of three kinds:

* ``StdioServerProvider``     -- a subprocess per session (classic stdio servers)
* ``InProcessServerProvider`` -- an mcp 2.x SDK server inside your own process
* ``UrlServerProvider``       -- a passthrough to a remote streamable-HTTP server

Typical embedding (e.g. in OpenSesame)::

    from mcp_relay.agent import RelayWidget, InProcessServerProvider, MCPServer

    widget = RelayWidget(relay_url="https://relay.example.com",
                         providers={"opensesame": InProcessServerProvider(
                             "opensesame", make_my_mcp_server)},
                         state_file="~/.config/opensesame/relay-credentials.json")

For a non-GUI embedding use ``RelayClient`` directly; ``agent.py`` shows the
minimal CLI usage with stdio servers.

``RelayWidget`` is imported lazily (PEP 562): it needs qtpy + a Qt binding,
while the rest of the package (including the CLI) imports without Qt.
"""

from .client import Fatal, RelayClient
from .credentials import (DEFAULT_STATE_FILE, PairingError, delete_state,
                          load_state, load_state_or_exit, redeem_pairing_code,
                          save_state)
from .protocol import (FATAL_CLOSE_CODES, MAX_MESSAGE, RESERVED_SERVER_NAMES,
                       SERVER_NAME_RE, dumps)
from .servers import (MCPServer, InProcessServerProvider, LocalServer,
                      LocalServerInstance, OnExit, SendFrame, ServerProvider,
                      StdioServerProvider, UrlServerProvider, check_providers,
                      parse_server_spec)

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_STATE_FILE",
    "FATAL_CLOSE_CODES",
    "Fatal",
    "MCPServer",
    "InProcessServerProvider",
    "LocalServer",
    "LocalServerInstance",
    "MAX_MESSAGE",
    "OnExit",
    "PairingError",
    "RESERVED_SERVER_NAMES",
    "RelayClient",
    "RelayWidget",
    "SERVER_NAME_RE",
    "SendFrame",
    "ServerProvider",
    "StdioServerProvider",
    "UrlServerProvider",
    "check_providers",
    "delete_state",
    "dumps",
    "load_state",
    "load_state_or_exit",
    "parse_server_spec",
    "redeem_pairing_code",
    "save_state",
]


def __getattr__(name: str):
    # Lazy so that `import mcp_relay.agent` does not require Qt (qtpy + a Qt
    # binding); only touching RelayWidget imports mcp_relay.agent.widget.
    if name == "RelayWidget":
        from .widget import RelayWidget
        return RelayWidget
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

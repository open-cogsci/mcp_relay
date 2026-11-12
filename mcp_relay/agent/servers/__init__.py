"""Local MCP server backends for the relay agent.

Three kinds are supported, all behind the same ServerProvider protocol:

* StdioServerProvider     -- spawn a subprocess per session (classic stdio MCP server)
* InProcessServerProvider -- run an mcp 2.x SDK server in this process on memory streams
* UrlServerProvider       -- proxy each session to a remote streamable-HTTP MCP server

``MCPServer`` is re-exported for convenience when building in-process servers.
"""

from mcp.server import MCPServer

from ..protocol import RESERVED_SERVER_NAMES, SERVER_NAME_RE
from .base import LocalServerInstance, OnExit, SendFrame, ServerProvider
from .inprocess import InProcessServerProvider
from .stdio import LocalServer, StdioServerProvider, parse_server_spec
from .url import UrlServerProvider


def check_providers(providers: dict) -> dict:
    """Validate the server registry before connecting (mirrors the relay's hello check)."""
    if not providers:
        raise ValueError("At least one server provider is required")
    for name in providers:
        if not SERVER_NAME_RE.match(name):
            raise ValueError(f"Invalid server name {name!r} (must match {SERVER_NAME_RE.pattern})")
        if name in RESERVED_SERVER_NAMES:
            raise ValueError(f"Server name {name!r} is reserved by the relay itself")
    return providers


__all__ = [
    "MCPServer",
    "InProcessServerProvider",
    "LocalServer",
    "LocalServerInstance",
    "OnExit",
    "SendFrame",
    "ServerProvider",
    "StdioServerProvider",
    "UrlServerProvider",
    "check_providers",
    "parse_server_spec",
]

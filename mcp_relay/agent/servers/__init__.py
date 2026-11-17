"""Local MCP server backends for the relay agent.

Three kinds are supported, all behind the same ServerProvider protocol:

* StdioServerProvider     -- spawn a subprocess per session (classic stdio MCP server)
* InProcessServerProvider -- run an mcp 2.x SDK server in this process on memory streams
* UrlServerProvider       -- proxy each session to a remote streamable-HTTP MCP server

``MCPServer`` is re-exported for convenience when building in-process servers.
"""

from mcp.server import MCPServer

from ..protocol import RESERVED_SERVER_NAMES, SERVER_NAME_RE
from .base import (ArgsProvider, LocalServerInstance, OnExit, ProviderBase,
                   SendFrame, ServerProvider, call_args_provider)
from .inprocess import InProcessServerProvider
from .stdio import LocalServer, StdioServerProvider, parse_server_spec
from .url import UrlServerProvider


def check_providers(providers: dict) -> dict:
    """Validate the server registry before connecting (mirrors the relay's hello check)."""
    if not providers:
        raise ValueError("At least one server provider is required")
    for name, provider in providers.items():
        if not SERVER_NAME_RE.match(name):
            raise ValueError(f"Invalid server name {name!r} (must match {SERVER_NAME_RE.pattern})")
        if name in RESERVED_SERVER_NAMES:
            raise ValueError(f"Server name {name!r} is reserved by the relay itself")
        if getattr(provider, "name", name) != name:
            raise ValueError(f"Provider for {name!r} has a different name: "
                             f"{getattr(provider, 'name', None)!r}")
    return providers


def enabled_providers(providers: dict) -> dict:
    """The subset of providers currently enabled (``getattr(p, 'enabled', True)``)."""
    return {name: p for name, p in providers.items() if getattr(p, "enabled", True)}


__all__ = [
    "MCPServer",
    "ArgsProvider",
    "InProcessServerProvider",
    "LocalServer",
    "LocalServerInstance",
    "OnExit",
    "ProviderBase",
    "SendFrame",
    "ServerProvider",
    "StdioServerProvider",
    "UrlServerProvider",
    "call_args_provider",
    "check_providers",
    "enabled_providers",
    "parse_server_spec",
]

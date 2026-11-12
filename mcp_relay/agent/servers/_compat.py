"""Version-compat shims for the mcp SDK (1.x FastMCP vs 2.x MCPServer).

Nothing here is imported at package import time; the heavy ``mcp`` imports
happen lazily inside these functions so the package stays importable in
environments where only the WebSocket client is needed.
"""

from __future__ import annotations


def mcp_server_cls():
    """The ergonomic MCP server class: ``MCPServer`` on mcp>=2, ``FastMCP`` on 1.x."""
    try:
        from mcp.server.mcpserver import MCPServer

        return MCPServer
    except ImportError:
        from mcp.server.fastmcp import FastMCP

        return FastMCP


def lowlevel_server(server):
    """Unwrap MCPServer/FastMCP to the low-level ``Server`` with ``.run(streams, ...)``."""
    return getattr(server, "_lowlevel_server", getattr(server, "_mcp_server", server))


def jsonrpc_adapter():
    """TypeAdapter that parses a JSON-RPC dict into a JSONRPCMessage model."""
    try:
        from mcp_types import jsonrpc_message_adapter

        return jsonrpc_message_adapter
    except ImportError:
        pass
    try:
        from mcp.types import jsonrpc_message_adapter

        return jsonrpc_message_adapter
    except ImportError:
        from pydantic import TypeAdapter

        from mcp.types import JSONRPCMessage

        return TypeAdapter(JSONRPCMessage)


def to_session_message(msg: dict):
    """Wrap a raw JSON-RPC dict in a SessionMessage for an SDK stream."""
    from mcp.shared.message import SessionMessage

    return SessionMessage(message=jsonrpc_adapter().validate_python(msg, by_name=False))


def message_to_dict(session_message) -> dict:
    """Serialize a SessionMessage back to a plain JSON-RPC dict for the relay."""
    return session_message.message.model_dump(by_alias=True, mode="json", exclude_none=True)

"""A minimal MCP server with a single hello-world tool, speaking MCP over stdio.

In a real deployment this would be your server that drives a desktop
application.  It knows nothing about the relay: the agent simply launches it as
a normal stdio MCP server.

Run it standalone to try it:  python hello_server.py
"""

import platform
import socket

try:  # mcp >= 2.0
    from mcp.server.mcpserver import MCPServer
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as MCPServer

server = MCPServer("hello")


@server.tool()
def hello_world(name: str = "world") -> str:
    """Say hello. Reports which machine the tool ran on, which proves the call
    really executed on the user's desktop rather than in the cloud."""
    return f"Hello, {name}! This ran on {socket.gethostname()} ({platform.system()})."


if __name__ == "__main__":
    server.run("stdio")

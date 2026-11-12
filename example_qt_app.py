#!/usr/bin/env python
"""Standalone Qt example for mcp_relay.agent.

Launches one relay agent that exposes three MCP servers, one of each kind:

  hello   in-process dummy MCP server (a hello_world tool), runs inside this process
  files   stdio subprocess: node_modules/.bin/mcp-server-filesystem <fs-root>
  remote  URL passthrough to a streamable-HTTP MCP server

For 'remote' the example self-hosts a demo MCP server over streamable HTTP on a
local port (see start_demo_remote_server), so the example works out of the box.
Use --remote-url to point at any other streamable-HTTP MCP endpoint instead.

Usage:

  1. Run the relay somewhere reachable:  python relay.py --host 0.0.0.0 --port 8080
  2. Get a pairing code (chat tool get_pairing_code on /mcp/relay, or
     POST /admin/pairing-codes with the admin key).
  3. python example_qt_app.py --relay http://localhost:8080 --fs-root .
     Enter the code in the widget and press Connect.

LibreChat (or any MCP client) can then reach all three servers through the
relay at /mcp/hello, /mcp/files and /mcp/remote.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
from pathlib import Path

from mcp_relay.agent import (InProcessServerProvider, RelayWidget,
                             StdioServerProvider, UrlServerProvider,
                             mcp_server_cls)

REPO_DIR = Path(__file__).resolve().parent


def make_hello_server():
    """A dummy MCP server with one tool (fresh instance per session).

    Used both for the in-process 'hello' server and (over HTTP) for the demo
    'remote' server, so you can compare the two paths in LibreChat.
    """
    import platform
    import socket

    server = mcp_server_cls()("hello")

    @server.tool()
    def hello_world(name: str = "world") -> str:
        """Say hello. Reports which machine and process the tool ran in, proving
        the call really executed where expected."""
        return (f"Hello, {name}! This ran in pid {os.getpid()} on "
                f"{socket.gethostname()} ({platform.system()}).")

    return server


def start_demo_remote_server(port: int) -> None:
    """Serve the hello MCP server over streamable HTTP on 127.0.0.1:<port> in a
    daemon thread, for the 'remote' provider to proxy to."""
    import uvicorn

    app = make_hello_server().streamable_http_app(streamable_http_path="/mcp")
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    thread = threading.Thread(target=uvicorn.Server(config).run, daemon=True,
                              name="demo-remote-mcp")
    thread.start()
    print(f"Demo remote MCP server listening on http://127.0.0.1:{port}/mcp")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--relay", default="http://localhost:8080", help="relay base URL")
    p.add_argument("--state-file", default=None,
                   help="credential file (default: ~/.config/mcp-relay-agent/credentials.json)")
    p.add_argument("--fs-root", default=os.getcwd(),
                   help="directory the 'files' stdio server may access")
    p.add_argument("--remote-url", default=None,
                   help="streamable-HTTP MCP endpoint exposed as 'remote' "
                        "(default: self-hosted demo server on --demo-port)")
    p.add_argument("--demo-port", type=int, default=9876,
                   help="port for the self-hosted demo remote server")
    args = p.parse_args()

    remote_url = args.remote_url
    if remote_url is None:
        start_demo_remote_server(args.demo_port)
        remote_url = f"http://127.0.0.1:{args.demo_port}/mcp"

    fs_bin = REPO_DIR / "node_modules" / ".bin" / "mcp-server-filesystem"
    if fs_bin.exists():
        files_argv = [str(fs_bin), os.path.abspath(args.fs_root)]
    else:
        files_argv = ["npx", "-y", "@modelcontextprotocol/server-filesystem",
                      os.path.abspath(args.fs_root)]

    providers = {
        "hello": InProcessServerProvider("hello", make_hello_server),
        "files": StdioServerProvider("files", files_argv),
        "remote": UrlServerProvider("remote", remote_url),
    }

    from qtpy.QtWidgets import QApplication

    app = QApplication(sys.argv)
    kwargs = {}
    if args.state_file:
        kwargs["state_file"] = args.state_file
    # The relay URL comes from the command line; the widget never asks for it.
    widget = RelayWidget(relay_url=args.relay, providers=providers, **kwargs)
    widget.setWindowTitle("MCP relay agent — three server kinds")
    widget.resize(560, 500)

    # Watch the user-facing state machine (disconnected / needs_pairing /
    # connecting / connected / reconnecting / fatal) in the window title.
    def on_state(state: str) -> None:
        widget.setWindowTitle(f"[{state}] MCP relay agent — three server kinds")
        print(f"state -> {state}")

    widget.state_changed.connect(on_state)
    on_state(widget.state)
    widget.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

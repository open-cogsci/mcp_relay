"""Desktop agent CLI: dials OUT to the relay and bridges it to local stdio MCP servers.

    # 1. one-time pairing: type the code your admin / web UI showed you
    python agent.py pair --relay https://relay.example.com --code K7QM-2XPD

    # 2. run it (keep it running; it reconnects automatically)
    python agent.py run --server hello="python hello_server.py"

``--server NAME=COMMAND`` can be repeated; the NAME becomes the URL path
(``/mcp/NAME``) on the relay. The command can be *any* stdio MCP server
(``npx -y some-server``, ``uvx ...``, your own script, ...). The agent starts
one process per MCP session and shuts it down when the session ends.

The generic implementation lives in the ``mcp_relay.agent`` package,
which additionally provides in-process and URL-based servers and a reusable
Qt widget (see example_qt_app.py). This CLI exposes the stdio-subprocess
flavor only.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from mcp_relay.agent import (DEFAULT_STATE_FILE, Fatal, PairingError,
                             RelayClient, StdioServerProvider,
                             load_state_or_exit, parse_server_spec,
                             redeem_pairing_code, save_state)


def cmd_pair(args) -> None:
    try:
        data = redeem_pairing_code(args.relay, args.code, args.name)
    except PairingError as exc:
        sys.exit(str(exc))
    save_state(args.state_file, {"relay": args.relay.rstrip("/"),
                                 "device_id": data["device_id"],
                                 "device_token": data["device_token"]})
    print(f"Paired as device {data['device_id']} for user {data['user_id']!r}.")
    print(f"Credentials saved to {args.state_file}. "
          "Now start the agent with:  python agent.py run --server NAME=COMMAND")


def cmd_run(args) -> None:
    state = load_state_or_exit(args.state_file)
    providers = {}
    for spec in args.server:
        try:
            name, argv = parse_server_spec(spec)
        except ValueError as exc:
            sys.exit(str(exc))
        providers[name] = StdioServerProvider(name, argv)
    client = RelayClient(state["relay"], state["device_token"], providers)
    try:
        asyncio.run(client.run_forever())
    except Fatal as exc:
        sys.exit(f"Stopped: {exc}")
    except KeyboardInterrupt:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Desktop agent for the MCP relay")
    common = argparse.ArgumentParser(add_help=False)  # options accepted after either subcommand
    common.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE,
                        help="where credentials are stored")
    common.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("pair", parents=[common], help="exchange a pairing code for stored credentials")
    p.add_argument("--relay", required=True, help="relay base URL, e.g. https://relay.example.com")
    p.add_argument("--code", required=True, help="pairing code, e.g. K7QM-2XPD")
    p.add_argument("--name", default=_default_device_name(), help="device name shown to admins")
    p.set_defaults(func=cmd_pair)

    r = sub.add_parser("run", parents=[common], help="connect to the relay and serve local MCP servers")
    r.add_argument("--server", action="append", required=True, metavar="NAME=COMMAND",
                   help="a local stdio MCP server to expose; repeatable")
    r.set_defaults(func=cmd_run)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    args.func(args)


def _default_device_name() -> str:
    import socket

    return socket.gethostname()


if __name__ == "__main__":
    main()

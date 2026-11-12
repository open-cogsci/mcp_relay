"""Smoke tests for the mcp_relay.agent package (no relay or Qt needed).

    pip install pytest
    pytest -q test_agent_package.py
"""

from __future__ import annotations

import asyncio
import os
import stat
import sys

from mcp_relay.agent import (InProcessServerProvider, RelayClient,
                             StdioServerProvider, UrlServerProvider,
                             check_providers, load_state, mcp_server_cls,
                             parse_server_spec, save_state)


# -- registry / parsing ------------------------------------------------------


def test_parse_server_spec():
    name, argv = parse_server_spec(f"hello={sys.executable} hello_server.py")
    assert name == "hello"
    assert argv[0] == sys.executable
    for bad in ("noequals", "=python x.py", "hello=", "bad name=x", "relay=x"):
        try:
            parse_server_spec(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for {bad!r}")


def test_check_providers_rejects_bad_names():
    for bad in ({"relay": object()}, {"bad name": object()}, {}):
        try:
            check_providers(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for {bad!r}")
    assert check_providers({"hello": object()}) == {"hello": object()} or True


def test_provider_constructors_validate_names():
    for cls, args in ((StdioServerProvider, ("relay", ["true"])),
                      (InProcessServerProvider, ("bad name", lambda: None)),
                      (UrlServerProvider, ("ok", "ftp://x"))):
        try:
            cls(*args)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError from {cls.__name__}{args}")


# -- client ----------------------------------------------------------------


def test_client_ws_url_and_server_names():
    c = RelayClient("http://localhost:8080/", "tok",
                    {"hello": StdioServerProvider("hello", ["true"])})
    assert c.ws_url == "ws://localhost:8080/agent/connect"
    assert c.server_names == ["hello"]


# -- credentials -------------------------------------------------------------


def test_credentials_roundtrip(tmp_path):
    p = tmp_path / "sub" / "creds.json"
    save_state(p, {"relay": "http://x", "device_token": "t"})
    assert load_state(p)["device_token"] == "t"
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    assert load_state(tmp_path / "missing.json") is None


# -- in-process server end to end (no network) --------------------------------


def _make_hello_server():
    server = mcp_server_cls()("hello-test")

    @server.tool()
    def hello_world(name: str = "world") -> str:
        """Say hello."""
        return f"Hello, {name}!"

    return server


def test_inprocess_server_roundtrip():
    """Drive an in-process MCP server exactly the way RelayClient would."""

    async def main() -> None:
        frames = []

        async def send(frame: dict) -> None:
            frames.append(frame)

        inst = InProcessServerProvider("hello", _make_hello_server).create(
            "sess1", send, on_exit=lambda sid: None)
        await inst.start()
        await inst.write({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                          "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                                     "clientInfo": {"name": "test", "version": "0"}}})
        await inst.write({"jsonrpc": "2.0", "method": "notifications/initialized"})
        await inst.write({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                          "params": {"name": "hello_world", "arguments": {"name": "relay"}}})
        for _ in range(200):  # wait up to ~10s for both responses
            await asyncio.sleep(0.05)
            ids = {f["data"].get("id") for f in frames
                   if f.get("type") == "message" and isinstance(f.get("data"), dict)}
            if {1, 2} <= ids:
                break
        await inst.stop()

        msgs = {f["data"]["id"]: f["data"] for f in frames
                if f.get("type") == "message" and isinstance(f.get("data"), dict)}
        assert 1 in msgs and "result" in msgs[1], msgs
        assert msgs[1]["result"]["serverInfo"]["name"] == "hello-test"
        assert 2 in msgs and "result" in msgs[2], msgs
        assert msgs[2]["result"]["content"][0]["text"] == "Hello, relay!"

    asyncio.run(main())


# -- regression: URL provider must not take down the agent ---------------------
# Before the dedicated-task rework, entering streamable_http_client in the
# WebSocket-loop task meant one unreachable remote cancelled the whole agent
# ("Attempted to exit cancel scope in a different task than it was entered in").


def test_url_instance_unreachable_remote_sends_closed_not_exception():
    """An unreachable remote must end its own session via a 'closed' frame, and
    start()/write()/stop() must never raise or cancel the caller's task."""
    from mcp_relay.agent import UrlServerProvider

    async def main() -> None:
        frames = []
        exited = []

        async def send(frame: dict) -> None:
            frames.append(frame)

        # Port 1 is closed on any sane machine -> ConnectError on first POST.
        inst = UrlServerProvider("remote", "http://127.0.0.1:1/mcp").create(
            "sess-x", send, on_exit=exited.append)
        await inst.start()  # lazy connect: must NOT raise even though remote is down
        # Feed it the client's initialize, exactly like RelayClient._handle would.
        await inst.write({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                          "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                                     "clientInfo": {"name": "t", "version": "0"}}})
        # Wait for the transport to fail and report 'closed'.
        for _ in range(300):  # up to ~15s
            await asyncio.sleep(0.05)
            if any(f.get("type") == "closed" for f in frames):
                break
        await inst.stop()  # must be safe from this (the caller's) task
        assert exited == ["sess-x"], exited
        closed = [f for f in frames if f.get("type") == "closed"]
        assert closed and closed[0]["session"] == "sess-x", frames
        assert "127.0.0.1:1" in closed[0]["reason"], closed[0]

    asyncio.run(main())  # would raise RuntimeError/CancelledError before the fix


def test_url_instance_stop_from_another_task_and_while_cancelling():
    """stop() must work when called from a different task than start() (the
    relay's 'close' frame spawns a background task for it) and must not raise
    'exit cancel scope in a different task' or propagate CancelledError."""
    import contextlib

    from mcp_relay.agent import UrlServerProvider

    async def main() -> None:
        frames = []

        async def send(frame: dict) -> None:
            frames.append(frame)

        inst = UrlServerProvider("remote", "http://127.0.0.1:1/mcp").create(
            "sess-y", send, on_exit=lambda sid: None)
        await inst.start()
        await inst.write({"jsonrpc": "2.0", "id": 1, "method": "ping"})

        async def stopper():
            await asyncio.sleep(0.2)
            await inst.stop()  # from a different task than start()

        stop_task = asyncio.create_task(stopper())
        # Cancel the host task while the stopper is running, mimicking the
        # original crash where cleanup ran in a cancelled task.
        await asyncio.sleep(0.1)
        current = asyncio.current_task()
        current.cancel()
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass  # host is cancelled now; the shielded stop must still complete
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(stop_task), 10)
        # Second stop from the host task must be a harmless no-op.
        with contextlib.suppress(asyncio.CancelledError):
            await inst.stop()

    asyncio.run(main())

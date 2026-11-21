"""Smoke tests for the mcp_relay.agent package (no relay or Qt needed).

    pip install pytest
    pytest -q tests/
"""

import asyncio
import contextlib
import json
import os
import stat
import sys
import threading
import time

import pytest

from mcp_relay.agent import (Fatal, MCPServer, InProcessServerProvider,
                             RelayClient, StdioServerProvider, UrlServerProvider,
                             call_args_provider, check_providers,
                             enabled_providers, load_state, parse_server_spec,
                             save_state, validate_server_list)


# -- registry / parsing ------------------------------------------------------


def test_parse_server_spec():
    name, argv = parse_server_spec(f"hello={sys.executable} hello_server.py")
    assert name == "hello"
    assert argv[0] == sys.executable
    for bad in ("noequals", "=python x.py", "hello=", "bad name=x", "relay=x"):
        with pytest.raises(ValueError):
            parse_server_spec(bad)


def test_check_providers_rejects_bad_names():
    for bad in ({"relay": object()}, {"bad name": object()}, {}):
        with pytest.raises(ValueError):
            check_providers(bad)
    providers = {"hello": object()}
    assert check_providers(providers) is providers
    # A provider whose .name doesn't match its registry key is rejected.
    with pytest.raises(ValueError):
        check_providers({"hello": StdioServerProvider("other", ["true"])})


def test_provider_constructors_validate_names():
    with pytest.raises(ValueError):
        StdioServerProvider("relay", ["true"])
    with pytest.raises(ValueError):
        InProcessServerProvider("bad name", lambda: None)
    with pytest.raises(ValueError):
        UrlServerProvider("ok", "ftp://x")


def test_validate_server_list():
    assert validate_server_list(["a", "b"]) == ["a", "b"]
    assert validate_server_list([]) == []
    for bad in ("x", [1], ["relay"], ["bad name"], ["a", "a"], None):
        with pytest.raises(ValueError):
            validate_server_list(bad)


# -- provider metadata: display_name / enabled / args_provider ----------------


def test_provider_metadata_defaults_and_overrides():
    p = StdioServerProvider("hello", ["true"])
    assert p.name == "hello" and p.display_name == "hello"
    assert p.enabled is True and p.args_provider is None
    assert p.argv == ["true"]

    q = UrlServerProvider("remote", "http://x/mcp", display_name="Remote server",
                          enabled=False, args_provider=lambda: None)
    assert q.display_name == "Remote server"
    assert q.enabled is False
    assert q.url == "http://x/mcp"
    assert callable(q.args_provider)

    r = InProcessServerProvider("inproc", lambda: None, display_name="In process")
    assert r.display_name == "In process" and r.enabled is True

    # enabled is mutable (the widget toggles it at runtime)
    q.enabled = True
    assert q.enabled is True


def test_enabled_providers_filters_by_flag():
    providers = {
        "a": StdioServerProvider("a", ["true"], enabled=True),
        "b": StdioServerProvider("b", ["true"], enabled=False),
        "c": object(),  # plain provider without metadata counts as enabled
    }
    assert list(enabled_providers(providers)) == ["a", "c"]
    providers["b"].enabled = True
    assert list(enabled_providers(providers)) == ["a", "b", "c"]


def test_stdio_args_provider_appends_argv_at_create():
    """create() uses the arguments resolved at enable-time (cached); it only
    calls args_provider itself for providers never enabled via the setter."""
    calls = []

    def args():
        calls.append(1)
        return ["/some/root"]

    p = StdioServerProvider("files", ["mcp-server-filesystem"], args_provider=args)
    assert p.argv == ["mcp-server-filesystem"]  # base argv is unchanged
    # never enabled through the setter -> create() resolves lazily (fallback)
    inst = p.create("sess", send=None, on_exit=lambda s: None)
    assert inst.argv == ["mcp-server-filesystem", "/some/root"]
    assert len(calls) == 1
    # ... and caches: further sessions reuse the args without calling again
    inst2 = p.create("sess2", send=None, on_exit=lambda s: None)
    assert inst2.argv == ["mcp-server-filesystem", "/some/root"]
    assert len(calls) == 1
    # a single string is wrapped; None means no extra args
    p2 = StdioServerProvider("s", ["cmd"], args_provider=lambda: "/root")
    assert p2.create("s1", None, lambda s: None).argv == ["cmd", "/root"]
    p3 = StdioServerProvider("n", ["cmd"], args_provider=lambda: None)
    assert p3.create("s2", None, lambda s: None).argv == ["cmd"]
    # a bad return type raises (the client turns this into a 'closed' frame)
    p4 = StdioServerProvider("bad", ["cmd"], args_provider=lambda: 42)
    with pytest.raises(TypeError):
        p4.create("s3", None, lambda s: None)


def test_args_provider_called_on_each_enable_and_cached_until_disabled():
    """The core of the new semantics: enabling asks for the arguments (anew
    every time), disabling invalidates them, and create() reuses the cache."""
    calls = []

    def args():
        calls.append(len(calls))
        return [f"/root-{len(calls)}"]

    p = StdioServerProvider("files", ["cmd"], enabled=False, args_provider=args)
    assert calls == []  # construction with enabled=False never asks

    p.enabled = True  # first enable asks
    assert calls == [0]
    assert p.create("s1", None, lambda s: None).argv == ["cmd", "/root-1"]
    assert p.create("s2", None, lambda s: None).argv == ["cmd", "/root-1"]
    assert len(calls) == 1  # cached: sessions don't re-ask

    p.enabled = False  # disable invalidates
    assert p.enabled is False
    p.enabled = True  # re-enable asks anew
    assert calls == [0, 1]
    assert p.create("s3", None, lambda s: None).argv == ["cmd", "/root-2"]
    assert len(calls) == 2


def test_failed_enable_leaves_provider_disabled_and_unresolved():
    """If args_provider raises when enabling (e.g. the user cancels the dialog),
    the flag is not set and the next enable asks again."""
    calls = []

    def args():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("user cancelled")
        return ["/ok"]

    p = StdioServerProvider("files", ["cmd"], enabled=False, args_provider=args)
    with pytest.raises(RuntimeError, match="cancelled"):
        p.enabled = True
    assert p.enabled is False
    p.enabled = True  # asks anew
    assert p.enabled is True and len(calls) == 2
    assert p.create("s", None, lambda s: None).argv == ["cmd", "/ok"]


def test_inprocess_args_provider_becomes_factory_args():
    seen = []

    def factory(root):
        seen.append(root)
        return MCPServer("t")

    p = InProcessServerProvider("fs", factory, args_provider=lambda: ["/root"])
    inst = p.create("sess", send=None, on_exit=lambda s: None)
    assert inst._args == ("/root",)
    # scalar and None are normalized too
    p2 = InProcessServerProvider("s", factory, args_provider=lambda: "/r")
    assert p2.create("x", None, lambda s: None)._args == ("/r",)
    p3 = InProcessServerProvider("n", factory)
    assert p3.create("y", None, lambda s: None)._args == ()


def test_url_args_provider_appends_path_or_headers():
    p = UrlServerProvider("r", "http://x/mcp", args_provider=lambda: ["team", "42"])
    inst = p.create("s", None, lambda s: None)
    assert inst.url == "http://x/mcp/team/42" and inst.headers == {}
    p2 = UrlServerProvider("r", "http://x/mcp/", args_provider=lambda: {"X-Token": "t"})
    inst2 = p2.create("s", None, lambda s: None)
    assert inst2.url == "http://x/mcp/" and inst2.headers == {"X-Token": "t"}
    p3 = UrlServerProvider("r", "http://x/mcp", headers={"A": "1"})
    inst3 = p3.create("s", None, lambda s: None)
    assert inst3.url == "http://x/mcp" and inst3.headers == {"A": "1"}


# -- client ----------------------------------------------------------------


def test_client_ws_url_and_server_names():
    c = RelayClient("http://localhost:8080/", "tok",
                    {"hello": StdioServerProvider("hello", ["true"])})
    assert c.ws_url == "ws://localhost:8080/agent/connect"
    assert c.server_names == ["hello"]


def test_client_server_names_follows_enabled_flag():
    on = StdioServerProvider("on", ["true"])
    off = StdioServerProvider("off", ["true"], enabled=False)
    c = RelayClient("http://x/", "tok", {"on": on, "off": off})
    assert c.server_names == ["on"]
    off.enabled = True
    assert c.server_names == ["on", "off"]


def test_client_open_frame_for_disabled_or_failing_server_sends_closed():
    """An 'open' for a disabled server, or whose args_provider raises, must
    produce a 'closed' frame for that session only -- never an exception.

    create() runs in a worker thread (asyncio.to_thread) so a blocking
    args_provider (e.g. a GUI dialog) cannot stall the agent's event loop.
    """

    async def main() -> None:
        sent = []
        loop_thread = threading.get_ident()
        create_threads = set()

        class FakeWs:
            async def send(self, raw: str) -> None:
                sent.append(json.loads(raw))

        def boom():
            raise RuntimeError("user cancelled the dialog")

        def slow():
            create_threads.add(threading.get_ident())
            time.sleep(0.05)  # would block the event loop if run inline
            return ["/root"]

        providers = {
            "off": StdioServerProvider("off", ["true"], enabled=False),
            "boom": StdioServerProvider("boom", ["true"], args_provider=boom),
            "slow": StdioServerProvider("slow", ["true"], args_provider=slow),
        }
        c = RelayClient("http://x/", "tok", providers)
        ws = FakeWs()
        await c._handle(ws, {"type": "open", "session": "s1", "server": "off"})
        await c._handle(ws, {"type": "open", "session": "s2", "server": "boom"})
        await c._handle(ws, {"type": "open", "session": "s3", "server": "nope"})
        await c._handle(ws, {"type": "open", "session": "s4", "server": "slow"})
        closed = {(f["session"], f["reason"]) for f in sent if f.get("type") == "closed"}
        assert ("s1", "server 'off' is not enabled on this agent") in closed
        assert any(s == "s2" and "user cancelled" in r for s, r in closed)
        assert any(s == "s3" and "not enabled" in r for s, r in closed)
        # the slow args_provider really ran off the event-loop thread ...
        assert create_threads and loop_thread not in create_threads
        # ... and its args were used: 'slow' started (no closed frame for s4)
        assert not any(s == "s4" for s, _ in closed)
        assert "s4" in c.sessions
        await c._stop_all()

    asyncio.run(main())


def test_client_update_servers_sends_frame_only_when_connected_and_changed():
    async def main() -> None:
        sent = []

        class FakeWs:
            async def send(self, raw: str) -> None:
                sent.append(raw)

        off = StdioServerProvider("off", ["true"], enabled=False)
        c = RelayClient("http://x/", "tok", {"on": StdioServerProvider("on", ["true"]), "off": off})
        assert await c.update_servers() is False  # not connected
        c._ws = FakeWs()
        c._advertised = ["on"]
        assert await c.update_servers() is True and sent == []  # unchanged
        off.enabled = True
        assert await c.update_servers() is True
        assert sent == ['{"type":"servers","servers":["on","off"]}']
        assert await c.update_servers() is True and len(sent) == 1  # deduped

    asyncio.run(main())


def test_run_forever_goes_fatal_and_never_reconnects(monkeypatch):
    """Any lost connection must END run_forever rather than retry: state
    "fatal" plus a user-presentable Fatal, with exactly one connect attempt.

    Regression: run_forever used to be a backoff loop that swallowed every
    error (including Fatal), so a replaced agent kept silently reconnecting.
    """
    import mcp_relay.agent.client as client_mod

    attempts = []

    def fake_connect(*args, **kwargs):
        attempts.append(1)
        raise OSError("connection refused")

    states = []
    monkeypatch.setattr(client_mod, "connect", fake_connect)
    client = RelayClient("http://x/", "tok",
                         {"hello": StdioServerProvider("hello", ["true"])},
                         on_state=states.append)

    with pytest.raises(Fatal, match="connection refused"):
        asyncio.run(client.run_forever())

    assert len(attempts) == 1  # never reconnected on its own
    assert states == ["connecting", "fatal"]


def test_stop_is_the_only_clean_exit(monkeypatch):
    """stop() must end run_forever normally (state "offline", no Fatal):
    a deliberate disconnect is not a lost connection."""
    import mcp_relay.agent.client as client_mod

    async def fake_connect_once(self):
        # Stands in for "serve frames until asked to stop".
        await self._stop_event.wait()
        return None

    monkeypatch.setattr(client_mod.RelayClient, "_connect_once",
                        fake_connect_once)

    async def main() -> None:
        states = []
        client = RelayClient("http://x/", "tok",
                             {"hello": StdioServerProvider("hello", ["true"])},
                             on_state=states.append)
        run_task = asyncio.create_task(client.run_forever())
        await asyncio.sleep(0.01)  # let the run enter _connect_once
        await client.stop()
        await run_task  # returns cleanly instead of raising Fatal
        assert states == ["connecting", "offline"]

    asyncio.run(main())


# -- credentials -------------------------------------------------------------


def test_credentials_roundtrip(tmp_path):
    p = tmp_path / "sub" / "creds.json"
    save_state(p, {"relay": "http://x", "device_token": "t"})
    assert load_state(p)["device_token"] == "t"
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    assert load_state(tmp_path / "missing.json") is None


# -- in-process server end to end (no network) --------------------------------


def _make_hello_server():
    server = MCPServer("hello-test")

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

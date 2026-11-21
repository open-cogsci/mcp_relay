"""Offscreen Qt tests for RelayWidget's server toggles (needs qtpy + a Qt binding).

    QT_QPA_PLATFORM=offscreen pytest -q tests/test_widget.py
"""

import os
import time

import pytest

pytest.importorskip("qtpy")

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from qtpy.QtWidgets import QApplication

from mcp_relay.agent import (InProcessServerProvider, RelayWidget,
                             StdioServerProvider, save_state)


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app


@pytest.fixture
def widget(qapp, tmp_path):
    providers = {
        "hello": InProcessServerProvider("hello", lambda: None,
                                         display_name="Hello world"),
        "files": StdioServerProvider("files", ["true"], display_name="Files",
                                     enabled=False),
        "remote": StdioServerProvider("remote", ["true"]),  # no display_name
    }
    w = RelayWidget(relay_url="http://relay.test", providers=providers,
                    state_file=tmp_path / "creds.json", auto_connect=False)
    yield w
    w.close()
    w.deleteLater()


def test_toggle_buttons_reflect_display_names_and_enabled(widget):
    btns = widget._toggle_buttons
    assert set(btns) == {"hello", "files", "remote"}
    assert btns["hello"].text() == "Hello world"
    assert btns["files"].text() == "Files"
    assert btns["remote"].text() == "remote"  # falls back to the protocol name
    assert "files" in btns["files"].toolTip()
    assert btns["hello"].isChecked() and not btns["files"].isChecked()
    assert btns["remote"].isChecked()
    assert widget.enabled_servers() == ["hello", "remote"]


def test_toggling_updates_provider_and_emits(widget):
    seen = []
    widget.servers_changed.connect(seen.append)
    widget._toggle_buttons["files"].setChecked(True)
    assert widget._providers["files"].enabled is True
    assert widget.enabled_servers() == ["hello", "files", "remote"]
    assert seen == [["hello", "files", "remote"]]
    # programmatic API does the same as clicking
    widget.set_server_enabled("hello", False)
    assert widget._providers["hello"].enabled is False
    assert widget.enabled_servers() == ["files", "remote"]
    with pytest.raises(KeyError):
        widget.set_server_enabled("nope", True)


def test_args_provider_runs_on_enable_and_asks_anew_on_reenable(qapp, tmp_path):
    """args_provider must run when the server is enabled -- at widget init for
    initially-enabled servers and on every later (re-)enable -- not per session."""
    calls = []

    def args():
        calls.append(f"/root-{len(calls)}")
        return calls[-1]

    providers = {
        "files": StdioServerProvider("files", ["cmd"], display_name="Files",
                                     enabled=False, args_provider=args),
        "auto": StdioServerProvider("auto", ["cmd"], args_provider=args),
    }
    assert calls == []  # constructing with enabled=True does not ask yet
    w = RelayWidget(relay_url="http://relay.test", providers=providers,
                    state_file=tmp_path / "creds.json", auto_connect=False)
    try:
        # 'auto' starts enabled -> the widget asked at initialization
        assert calls == ["/root-0"]
        assert w._providers["auto"].enabled is True
        assert w._providers["files"].enabled is False
        assert w.enabled_servers() == ["auto"]
        # enabling 'files' through the GUI asks anew
        w._toggle_buttons["files"].setChecked(True)
        assert calls == ["/root-0", "/root-1"]
        assert w.enabled_servers() == ["files", "auto"]
        # sessions reuse the cached args (create() does not re-ask)
        inst = w._providers["files"].create("s", None, lambda s: None)
        assert inst.argv == ["cmd", "/root-1"]
        assert len(calls) == 2
        # disable + re-enable asks anew
        w.set_server_enabled("files", False)
        w.set_server_enabled("files", True)
        assert calls == ["/root-0", "/root-1", "/root-2"]
        assert w._providers["files"].create("s2", None, lambda s: None).argv == ["cmd", "/root-2"]
    finally:
        w.close()
        w.deleteLater()


def test_cancelled_args_dialog_at_startup_leaves_server_disabled(qapp, tmp_path):
    """A provider that starts enabled but whose args_provider raises (e.g. the
    user cancels the folder dialog) must not crash the widget: the server just
    stays disabled and its toggle is unchecked."""

    def boom():
        raise RuntimeError("user cancelled")

    providers = {
        "files": StdioServerProvider("files", ["cmd"], enabled=True, args_provider=boom),
        "hello": InProcessServerProvider("hello", lambda: None),
    }
    w = RelayWidget(relay_url="http://relay.test", providers=providers,
                    state_file=tmp_path / "creds.json", auto_connect=False)
    try:
        assert w._providers["files"].enabled is False
        assert not w._toggle_buttons["files"].isChecked()
        assert w.enabled_servers() == ["hello"]
    finally:
        w.close()
        w.deleteLater()


def test_cancelled_args_dialog_on_toggle_unchecks_button(qapp, tmp_path):
    """Same, but when enabling through the GUI at runtime: the toggle snaps
    back off and the server stays disabled."""
    state = {"n": 0}

    def args():
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("user cancelled")
        return f"/root-{state['n']}"

    providers = {"files": StdioServerProvider("files", ["cmd"], enabled=False,
                                              args_provider=args)}
    w = RelayWidget(relay_url="http://relay.test", providers=providers,
                    state_file=tmp_path / "creds.json", auto_connect=False)
    try:
        w._toggle_buttons["files"].setChecked(True)  # dialog cancelled
        assert w._providers["files"].enabled is False
        assert not w._toggle_buttons["files"].isChecked()
        assert w.enabled_servers() == []
        w._toggle_buttons["files"].setChecked(True)  # retry: asks anew, succeeds
        assert w._providers["files"].enabled is True
        assert w._toggle_buttons["files"].isChecked()
        assert w.enabled_servers() == ["files"]
        assert state["n"] == 2
    finally:
        w.close()
        w.deleteLater()


def test_call_on_gui_thread_roundtrip(qapp):
    """A worker thread must be able to run a callable on the GUI thread and get
    its result/exception back -- this is what args_provider dialogs need.

    Regression: QTimer.singleShot from a worker thread never fires (the timer is
    created in the worker, which has no event loop), so the old implementation
    deadlocked the agent thread until the app crashed.
    """
    import threading

    from qtpy.QtCore import QEventLoop

    from mcp_relay.agent import call_on_gui_thread

    loop = QEventLoop()

    def pump_while_alive(t, timeout=5.0):
        deadline = time.time() + timeout
        while t.is_alive() and time.time() < deadline:
            loop.processEvents()
        t.join(5)
        assert not t.is_alive(), "call_on_gui_thread deadlocked the calling thread"

    results, errors = [], []

    def work():
        try:
            results.append(call_on_gui_thread(lambda: 40 + 2))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=work)
    t.start()
    pump_while_alive(t)
    assert results == [42] and not errors

    def work2():
        try:
            call_on_gui_thread(_boom)
        except RuntimeError as exc:
            errors.append(exc)

    def _boom():
        raise RuntimeError("cancelled")

    t2 = threading.Thread(target=work2)
    t2.start()
    pump_while_alive(t2)
    assert any("cancelled" in str(e) for e in errors)

    # calling from the GUI thread itself runs inline (no marshal needed)
    assert call_on_gui_thread(lambda: "inline-ok") == "inline-ok"


def test_status_texts_cover_every_state_and_embed_the_chat_link():
    """STATUS_TEXTS must hold exactly one text per widget state, and every
    text must embed the one CHAT_LINK constant (defined once, so the URL
    cannot drift between messages). 'reconnecting' is gone together with the
    automatic reconnect it described."""
    from mcp_relay.agent import widget as widget_mod

    assert set(widget_mod.STATUS_TEXTS) == set(widget_mod.STATES)
    assert "reconnecting" not in widget_mod.STATES
    assert all(widget_mod.CHAT_LINK in t
               for t in widget_mod.STATUS_TEXTS.values())
    fatal = widget_mod.STATUS_TEXTS["fatal"]
    assert fatal.startswith("Connection to ")
    assert "was lost" in fatal
    assert ("another agent connected or because of a network error" in fatal)


def test_lost_connection_ends_in_fatal_state_with_connect_and_unpair(qapp, tmp_path):
    """End to end: a connection lost while the agent is online (e.g. the relay
    closing it because another agent connected) must leave the widget in the
    'fatal' state, which mirrors the disconnected state once the agent thread
    is gone: Connect as the primary button and Unpair (while paired) instead
    of the running buttons.

    Regression: the fatal state was entered while the thread was still alive,
    so the buttons kept showing Disconnect / Disconnect and Unpair -- and
    nothing ever recomputed them when the thread finished.
    """
    # Paired credentials and a relay address that is guaranteed unreachable
    # (port 1 is closed): the widget auto-connects and the connection to the
    # relay is lost immediately, exactly like another agent replacing this
    # one would make the relay close our connection.
    save_state(tmp_path / "creds.json", {"relay": "http://127.0.0.1:1",
                                         "device_id": "dev1",
                                         "device_token": "tok"})
    w = RelayWidget(relay_url="http://127.0.0.1:1",
                    providers={"hello": InProcessServerProvider(
                        "hello", lambda: None)},
                    state_file=tmp_path / "creds.json", auto_connect=True)
    try:
        assert w.state == "connecting"
        assert w._connect_btn.text() == "Disconnect"  # an agent is running
        assert w._action_btn.text() == "Disconnect and Unpair"
        # Pump the event loop until the agent thread has died and its queued
        # signals (fatal, finished) were delivered on the GUI thread.
        deadline = time.time() + 20
        while w._thread is not None and time.time() < deadline:
            qapp.processEvents()
            time.sleep(0.01)
        assert w._thread is None, "agent thread did not finish"
        assert w.state == "fatal"
        assert "was lost" in w._status_text()
        # Mirroring "disconnected" also means flipping the buttons back.
        assert w._connect_btn.text() == "Connect"
        assert w._action_btn.text() == "Unpair"
        assert w._action_btn.isVisibleTo(w)
        # And Connect from the fatal state really starts a fresh agent.
        w._on_connect_clicked()
        assert w.state == "connecting" and w._thread is not None
    finally:
        w.close()
        w.deleteLater()

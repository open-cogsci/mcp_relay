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
                             StdioServerProvider)


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

    # calling from the GUI thread itself must raise, not deadlock
    with pytest.raises(RuntimeError, match="GUI thread"):
        call_on_gui_thread(lambda: None)

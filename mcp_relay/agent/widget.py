"""Reusable Qt widget for the relay agent.

App-agnostic: pass your server providers and a state file; the widget handles
pairing (code entry), connect/disconnect, unpairing, status and a small log.

    widget = RelayWidget(relay_url="https://relay.example.com",
                         providers={"opensesame": my_provider},
                         state_file="~/.config/myapp/relay-credentials.json")

``relay_url`` is deployment configuration (set once by the embedding app,
e.g. from a command-line flag); the user never sees or edits it.

Each provider may carry optional metadata that drives this widget (see
``mcp_relay.agent.servers``): ``display_name`` (human-readable label on the
toggle button; the protocol name goes to the tooltip), ``enabled`` (initial
toggle state; default True) and ``args_provider`` (callable invoked when a
session for that server starts, e.g. to pop a folder-selection dialog; use
``widget.call_on_gui_thread(fn)`` inside it, because it runs on the agent
thread). The toggle buttons let the user enable/disable servers at runtime;
while connected the new set is sent to the relay in a "servers" frame,
otherwise it applies on the next connect.

The agent runs on its own QThread with its own asyncio loop; all cross-thread
communication goes through Qt signals, so the widget is safe to use from the
GUI thread of any Qt application (OpenSesame, a tray app, ...). This module
requires qtpy + a Qt binding, but the rest of the package does not:
``mcp_relay.agent`` imports ``RelayWidget`` lazily, so the CLI agent works
in environments without Qt.

User-facing state machine (see ``RelayWidget.state`` / ``state_changed``)::

    disconnected --Connect--> connecting --online--> connected     (if paired)
    disconnected --Connect--> needs_pairing                         (if not paired)
    needs_pairing --Pair ok--> connecting --online--> connected
    needs_pairing --Cancel--> disconnected
    connected     --Disconnect--> disconnected
    connected     --Disconnect & Unpair--> disconnected (credentials removed)
    connecting/connected --connection lost--> reconnecting --online--> connected
    connecting/connected --relay rejects device--> unpaired --> disconnected

The three primary user states are DISCONNECTED / NEEDS_PAIRING / CONNECTED.
"""

import asyncio
import logging
import queue
import socket
import threading
from pathlib import Path

from qtpy.QtCore import QMetaObject, QObject, Qt, QThread, Signal, Slot
from qtpy.QtWidgets import (QApplication, QHBoxLayout, QLabel, QLineEdit, QMessageBox,
                            QPlainTextEdit, QPushButton, QVBoxLayout, QWidget)

from .client import Fatal, RelayClient
from .credentials import (DEFAULT_STATE_FILE, PairingError, delete_state,
                          load_state, redeem_pairing_code, save_state)

log = logging.getLogger("relay-agent")


# The states the widget can be in. The three primary user states are
# "disconnected", "needs_pairing" and "connected"; the rest are auxiliary.
STATES = ("disconnected", "needs_pairing", "connecting", "connected",
          "reconnecting", "fatal")

_STATE_COLORS = {
    "disconnected": "#666666",
    "needs_pairing": "#b26a00",
    "connecting": "#b26a00",
    "reconnecting": "#b26a00",
    "connected": "#1a7f37",
    "fatal": "#c01c28",
}


class _GuiDispatcher(QObject):
    """Marshals callables onto the GUI thread.

    Created once per process on the GUI thread. ``submit()`` is safe to call
    from any thread: it enqueues the callable and posts ``dispatch`` to this
    object via ``QMetaObject.invokeMethod(..., QueuedConnection)``, which Qt
    delivers in the dispatcher's (GUI) thread event loop. This is the correct
    cross-thread marshal -- ``QTimer.singleShot`` called from a worker would
    create the timer in that worker thread (no event loop) and never fire.
    """

    def __init__(self) -> None:
        super().__init__()  # must run on the GUI thread
        self._q: queue.Queue = queue.Queue()

    @Slot()
    def dispatch(self) -> None:
        while True:
            try:
                fn = self._q.get_nowait()
            except queue.Empty:
                return
            fn()

    def submit(self, fn) -> None:
        self._q.put(fn)
        QMetaObject.invokeMethod(self, "dispatch", Qt.ConnectionType.QueuedConnection)


_dispatcher: _GuiDispatcher | None = None


def _gui_dispatcher() -> _GuiDispatcher:
    """Return the process-wide dispatcher, creating it if needed.

    Safe to call from any thread: an object created in a plain Python thread
    reports that thread as its affinity, and ``moveToThread(app.thread())``
    (legal from any thread for an object without a parent) moves it to the GUI
    thread, so ``invokeMethod(..., QueuedConnection)`` is delivered there.
    """
    global _dispatcher
    app = QApplication.instance()
    if app is None:
        raise RuntimeError("call_on_gui_thread() needs a QApplication instance")
    if _dispatcher is not None and _dispatcher.thread() is app.thread():
        return _dispatcher
    disp = _GuiDispatcher()
    disp.moveToThread(app.thread())
    _dispatcher = disp
    return _dispatcher


def call_on_gui_thread(fn):
    """Run ``fn()`` on the GUI thread and block the calling thread until it returns.

    ``args_provider`` callables run on the agent's asyncio thread when a
    session opens, so a provider that needs a Qt dialog (e.g. choosing the
    filesystem root) should use this, e.g.::

        StdioServerProvider("files", [fs_bin], args_provider=lambda: call_on_gui_thread(pick_folder))

    Works even before/without a RelayWidget, as long as a QApplication exists.
    Returns ``fn``'s result in the calling thread; re-raises whatever ``fn``
    raises there (e.g. "user cancelled"), which fails just that one session
    with a "closed" frame. Must not be called from the GUI thread itself
    (that would deadlock).
    """
    app = QApplication.instance()
    if app is None:
        raise RuntimeError("call_on_gui_thread() needs a QApplication instance")
    if QThread.currentThread() is app.thread():
        raise RuntimeError("call_on_gui_thread() must not be called from the GUI thread")
    dispatcher = _gui_dispatcher()  # created on/moved to the GUI thread; emit is queued
    result: dict = {}
    done = threading.Event()

    def wrapper() -> None:
        try:
            result["value"] = fn()
        except BaseException as exc:  # re-raised in the calling thread below
            result["error"] = exc
        finally:
            done.set()

    dispatcher.submit(wrapper)
    done.wait()
    if "error" in result:
        raise result["error"]
    return result.get("value")


class _AgentThread(QThread):
    """Runs RelayClient.run_forever() on a dedicated asyncio loop."""

    state_changed = Signal(str)
    log_message = Signal(str)

    def __init__(self, client: RelayClient, parent=None):
        super().__init__(parent)
        self._client = client
        self._loop: asyncio.AbstractEventLoop | None = None

    def run(self) -> None:
        try:
            asyncio.run(self._main())
        except Exception:
            # Never let an unexpected error kill the thread silently:
            # report it, keep the widget usable.
            log.exception("agent thread crashed")
            self.log_message.emit("Agent thread crashed unexpectedly; see log.")
            self.state_changed.emit("failed")

    async def _main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._client.on_state = self.state_changed.emit
        try:
            await self._client.run_forever()
        except Fatal as exc:
            self.log_message.emit(f"Fatal: {exc}")
            self.state_changed.emit("fatal")
        except asyncio.CancelledError:
            raise
        except BaseException:
            # Defensive: run_forever should not raise, but if it does we
            # must not let it propagate out of the thread (which on some
            # Qt builds aborts the process).
            log.exception("unexpected error in agent loop")
            self.log_message.emit("Unexpected agent error; see log.")
            self.state_changed.emit("failed")

    def stop_client(self) -> None:
        if self._loop is not None:
            asyncio.run_coroutine_threadsafe(self._client.stop(), self._loop)


class RelayWidget(QWidget):
    """Pair / connect / unpair widget for one relay agent.

    ``relay_url`` is fixed at construction (config, not user input). The
    pairing-code row is only shown while the state is "needs_pairing".

    One toggle button per registered provider shows its ``display_name`` and
    enables/disables the server at runtime (``set_server_enabled`` does the
    same programmatically). Emits ``state_changed(str)`` with one of
    ``STATES`` whenever the user-facing state changes, and
    ``servers_changed(list[str])`` with the enabled protocol names whenever a
    toggle changes; read the current ones via ``.state`` / ``.enabled_servers()``.
    """

    state_changed = Signal(str)
    pair_done = Signal(bool, str)
    servers_changed = Signal(list)

    def __init__(self, parent=None, *, relay_url: str, providers: dict,
                 state_file=DEFAULT_STATE_FILE, device_name: str | None = None,
                 auto_connect: bool = True):
        super().__init__(parent)
        if not relay_url:
            raise ValueError("relay_url is required (the relay's base URL; "
                             "deployment config, not user input)")
        if not providers:
            raise ValueError("providers must map server names to ServerProvider instances")
        self._relay_url = relay_url.rstrip("/")
        self._providers = dict(providers)
        self._state_file = Path(state_file)
        self._device_name = device_name or socket.gethostname()
        self._thread: _AgentThread | None = None
        self._credentials: dict | None = None
        self._state = "disconnected"
        self._stop_requested = False
        self._build_ui()
        self.pair_done.connect(self._on_pair_done)
        self._credentials = load_state(self._state_file)
        self._refresh()
        if self._credentials and auto_connect:
            self.connect_agent()

    # -- state -----------------------------------------------------------

    @property
    def state(self) -> str:
        """One of STATES: disconnected / needs_pairing / connecting /
        connected / reconnecting / fatal."""
        return self._state

    def _set_state(self, state: str) -> None:
        if state not in STATES:
            raise ValueError(f"unknown state {state!r}")
        if state == self._state:
            self._refresh()
            return
        self._state = state
        self._refresh()
        self.state_changed.emit(state)

    # -- UI --------------------------------------------------------------

    def _build_ui(self) -> None:
        self.setWindowTitle("MCP relay")
        layout = QVBoxLayout(self)

        # One toggle button per available MCP server. The button shows the
        # human-readable display_name (protocol name in the tooltip) and its
        # checked state mirrors provider.enabled, so servers can be turned on
        # and off at runtime instead of only at initialization.
        self._servers_container = QWidget()
        servers_row = QHBoxLayout(self._servers_container)
        servers_row.setContentsMargins(0, 0, 0, 0)
        servers_row.addWidget(QLabel("Servers:"))
        self._toggle_buttons: dict[str, QPushButton] = {}
        for name, provider in self._providers.items():
            btn = QPushButton(getattr(provider, "display_name", None) or name)
            btn.setCheckable(True)
            btn.setChecked(bool(getattr(provider, "enabled", True)))
            tooltip = f"Protocol name: {name}"
            if getattr(provider, "args_provider", None) is not None:
                tooltip += "\nRequires arguments when started (asked per session)"
            btn.setToolTip(tooltip)
            btn.toggled.connect(lambda checked, n=name: self._on_server_toggled(n, checked))
            self._toggle_buttons[name] = btn
            servers_row.addWidget(btn)
        servers_row.addStretch(1)
        layout.addWidget(self._servers_container)

        # Pairing input, wrapped in a container so it can be shown only
        # when the user actually needs to enter a pairing code.
        self._pair_container = QWidget()
        pair_row = QHBoxLayout(self._pair_container)
        pair_row.setContentsMargins(0, 0, 0, 0)
        self._code_edit = QLineEdit()
        self._code_edit.setPlaceholderText("Pairing code, e.g. A12B-3C4D")
        self._code_edit.returnPressed.connect(self._on_pair_clicked)
        self._pair_btn = QPushButton("Pair")
        self._pair_btn.clicked.connect(self._on_pair_clicked)
        pair_row.addWidget(self._code_edit, 1)
        pair_row.addWidget(self._pair_btn)
        layout.addWidget(self._pair_container)

        btn_row = QHBoxLayout()
        self._connect_btn = QPushButton("Connect")
        self._connect_btn.clicked.connect(self._on_connect_clicked)
        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.clicked.connect(
            lambda: self._set_state("disconnected"))
        self._action_btn = QPushButton()
        self._action_btn.clicked.connect(self._on_action_clicked)
        btn_row.addWidget(self._connect_btn)
        btn_row.addWidget(self._cancel_btn)
        btn_row.addWidget(self._action_btn)
        layout.addLayout(btn_row)

        self._status_label = QLabel()
        self._status_label.setWordWrap(True)
        layout.addWidget(self._status_label)

        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setMaximumBlockCount(500)
        layout.addWidget(self._log, 1)

    def _status_text(self) -> str:
        if self._state == "connected":
            names = self.enabled_servers()
            if names:
                return "Connected — exposing: " + ", ".join(names)
            return "Connected — no servers enabled (toggle one on above)"
        if self._state == "connecting":
            return "Connecting…"
        if self._state == "reconnecting":
            return "Connection lost. Reconnecting automatically. Press Disconnect to stop"
        if self._state == "needs_pairing":
            return ("Enable Sigmund's application-connector tool and ask for a pairing code")
        # The fatal state should never be shown because it transitions
        # immediately to disconnected (via _unpair)
        if self._state == "fatal":
            return ("The relay rejected this device (revoked or replaced). "
                    "Press Connect to retry, or Unpair to pair a new device.")
        # disconnected
        if self._credentials:
            dev = self._credentials.get("device_id", "?")
            return f"Paired as {dev}. Press Connect to go online."
        return "Not connected"

    def _refresh(self) -> None:
        state = self._state
        paired = bool(self._credentials)
        running = self._thread is not None
        self._status_label.setText(self._status_text())
        self._status_label.setStyleSheet(
            f"color: {_STATE_COLORS.get(state, '#000')}; font-weight: bold;")

        # pairing input is only visible when a code is actually needed
        self._pair_container.setVisible(state == "needs_pairing")
        self._code_edit.setEnabled(not running)
        self._pair_btn.setEnabled(not running)

        # primary button: Connect (disconnected/fatal) or Disconnect (running);
        # hidden in needs_pairing where Pair/Cancel are the actions
        self._connect_btn.setVisible(state != "needs_pairing")
        self._connect_btn.setText("Disconnect" if running else "Connect")
        self._connect_btn.setEnabled(True)
        self._cancel_btn.setVisible(state == "needs_pairing")

        # secondary action: while running -> "Disconnect & Unpair";
        # when disconnected/fatal and paired -> plain "Unpair"
        if running:
            self._action_btn.setText("Disconnect && Unpair")
            self._action_btn.setVisible(True)
            self._action_btn.setEnabled(True)
        elif paired and state in ("disconnected", "fatal"):
            self._action_btn.setText("Unpair")
            self._action_btn.setVisible(True)
            self._action_btn.setEnabled(True)
        else:
            self._action_btn.setVisible(False)

    def _append_log(self, text: str) -> None:
        self._log.appendPlainText(text)

    # -- server toggles ------------------------------------------------------

    def enabled_servers(self) -> list[str]:
        """Protocol names of the currently enabled servers, in registration order."""
        return [name for name, p in self._providers.items() if getattr(p, "enabled", True)]

    def set_server_enabled(self, name: str, enabled: bool) -> None:
        """Enable/disable a server programmatically (same as clicking its toggle)."""
        if name not in self._providers:
            raise KeyError(f"unknown server {name!r}; available: {list(self._providers)}")
        self._toggle_buttons[name].setChecked(bool(enabled))  # fires _on_server_toggled

    def _on_server_toggled(self, name: str, checked: bool) -> None:
        setattr(self._providers[name], "enabled", bool(checked))
        self._notify_servers_changed()
        self._refresh()
        self.servers_changed.emit(self.enabled_servers())

    def _notify_servers_changed(self) -> None:
        """Push the new enabled-set to the relay if the agent is connected.

        Offline (or mid-reconnect) this is a no-op: the next hello frame
        advertises the current set anyway. Old relays ignore the frame.
        """
        thread = self._thread
        loop = getattr(thread, "_loop", None) if thread is not None else None
        if loop is None:
            return
        try:
            asyncio.run_coroutine_threadsafe(thread._client.update_servers(), loop)
        except RuntimeError:
            pass  # loop not running (yet/anymore); the next hello carries the set

    def call_on_gui_thread(self, fn):
        """Widget-bound alias of the module-level :func:`call_on_gui_thread`.

        ``args_provider`` callables run on the agent's asyncio thread when a
        session opens, so a provider that needs a Qt dialog (e.g. choosing
        the filesystem root) should do::

            args_provider=lambda: widget.call_on_gui_thread(pick_folder_dialog)

        or, without a widget reference, the module-level function. Raises
        whatever ``fn`` raises in the calling thread; the session then fails
        cleanly with a "closed" frame. Must not be called from the GUI thread.
        """
        return call_on_gui_thread(fn)

    # -- user actions ------------------------------------------------------

    def _on_connect_clicked(self) -> None:
        if self._thread is not None:
            self.disconnect_agent()
        else:
            self.connect_agent()

    def _on_action_clicked(self) -> None:
        # "Disconnect & Unpair" while running, "Unpair" while disconnected
        self._unpair()

    def connect_agent(self) -> None:
        """The user pressed Connect.

        If paired, go online immediately (connecting -> connected).
        If not paired yet, move to NEEDS_PAIRING and ask for a code.
        """
        if self._thread is not None:
            return
        if not self._credentials:
            self._set_state("needs_pairing")
            self._code_edit.setFocus()
            return
        relay = self._relay_url
        client = RelayClient(relay, self._credentials["device_token"], self._providers)
        thread = _AgentThread(client, self)
        thread.state_changed.connect(self._on_thread_state)
        thread.log_message.connect(self._append_log)
        thread.finished.connect(lambda t=thread: self._on_thread_finished(t))
        self._thread = thread
        self._stop_requested = False
        self._set_state("connecting")
        thread.start()

    def disconnect_agent(self) -> None:
        """Disconnect but keep the stored credentials -> DISCONNECTED."""
        self._stop_requested = True
        self._stop_thread()
        self._set_state("disconnected")

    def _unpair(self, confirm: bool = True) -> None:
        """Disconnect (if running) and remove the stored credentials."""
        if not self._credentials:
            return
        if confirm:
            reply = QMessageBox.question(
                self, "Unpair",
                "Disconnect from the relay and remove the stored credentials "
                "for this device? You will need a new pairing code to connect again.",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply != QMessageBox.Yes:
                return
        self.disconnect_agent()
        delete_state(self._state_file)
        self._credentials = None
        self._set_state("disconnected")

    def _on_pair_clicked(self) -> None:
        code = self._code_edit.text().strip()
        if not code:
            QMessageBox.warning(self, "Pair", "Enter the pairing code.")
            return
        relay = self._relay_url
        self._pair_btn.setEnabled(False)
        self._append_log(f"Redeeming pairing code at {relay} …")

        def work() -> None:
            try:
                data = redeem_pairing_code(relay, code, self._device_name)
                save_state(self._state_file,
                           {"relay": relay, "device_id": data["device_id"],
                            "device_token": data["device_token"]})
            except PairingError as exc:
                self.pair_done.emit(False, str(exc))
            else:
                self.pair_done.emit(
                    True, f"Paired as {data['device_id']} for user {data['user_id']!r}.")

        threading.Thread(target=work, daemon=True).start()

    def _on_pair_done(self, ok: bool, message: str) -> None:
        self._pair_btn.setEnabled(True)
        self._append_log(message)
        if ok:
            self._credentials = load_state(self._state_file)
            self._code_edit.clear()
            # pairing succeeded -> connect right away (per the state machine)
            if self._state in ("needs_pairing", "disconnected"):
                self.connect_agent()
        else:
            QMessageBox.warning(self, "Pairing failed", message)

    # -- thread plumbing ----------------------------------------------------

    def _on_thread_state(self, state: str) -> None:
        """Map RelayClient states onto the user-facing state machine."""
        if self._stop_requested:
            return  # deliberate disconnect: ignore late signals
        if state == "online":
            self._set_state("connected")
        elif state == "connecting":
            if self._state in ("disconnected", "needs_pairing"):
                self._set_state("connecting")
        elif state == "offline":
            if self._state in ("connecting", "connected"):
                self._set_state("reconnecting")
        elif state == "fatal":
            self._unpair(confirm=False)
        elif state == "failed":
            self._append_log("Agent thread failed; press Connect to retry.")

    def _on_thread_finished(self, thread: _AgentThread) -> None:
        if self._thread is thread:
            self._thread = None
        if self._state != "fatal":
            self._set_state("disconnected")
        self._stop_requested = False

    def _stop_thread(self) -> None:
        if self._thread is not None:
            self._thread.stop_client()
            self._thread.wait(5000)
            self._thread = None

    def closeEvent(self, event) -> None:
        self._stop_thread()
        super().closeEvent(event)

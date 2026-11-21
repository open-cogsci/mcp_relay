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
toggle state; default True) and ``args_provider`` (callable invoked when the
server is *enabled* -- at widget initialization for servers that start
enabled, and anew on every later enable, e.g. to pop a folder-selection
dialog). The resolved arguments are cached and reused for every session until
the server is disabled again. Enabling happens on the GUI thread, so an
``args_provider`` may show a Qt dialog directly; use
``call_on_gui_thread(fn)`` only if it might run on another thread (it runs
``fn`` inline when already on the GUI thread). The toggle buttons let the
user enable/disable servers at runtime; while connected the new set is sent
to the relay in a "servers" frame, otherwise it applies on the next connect.

Each server's toggle is a simple two-line column: a QCheckBox showing
the provider's ``display_name`` (the qtawesome circle icons stand in for
the checkbox's own square) and, below it, the provider's ``status_message``
on a smaller, muted line indented to line up with the name ("Click to
enable" while disabled and "Click to disable" while enabled by default;
the in-process kind instead forwards the live ``MCPServer``'s own
``status_message`` property whenever a relay session is open). The widget
pulls that property on a short GUI-thread poll, so an in-process status
property must be a cheap read of thread-safe state.

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
    connecting/connected --connection lost--> fatal
    connecting/connected --relay rejects device--> fatal
    fatal --Connect--> connecting

The three primary user states are DISCONNECTED / NEEDS_PAIRING / CONNECTED.
The "fatal" state (the connection was lost, e.g. because another agent
connected or because of a network error) mirrors the "disconnected" state --
Connect retries, Unpair works while paired -- except that its status text
explains what happened; the agent never reconnects automatically from it.
"""
import os
import asyncio
import logging
import queue
import socket
import threading
import qtawesome as qta
from pathlib import Path

from qtpy.QtCore import (QMetaObject, QObject, QSize, Qt, QThread, QTimer,
                         Signal, Slot)
from qtpy.QtWidgets import (QApplication, QCheckBox, QHBoxLayout, QLabel,
                            QLineEdit, QMessageBox, QPlainTextEdit,
                            QPushButton, QVBoxLayout, QWidget)
from qtpy.QtGui import QPalette, QPixmap

from .client import Fatal, RelayClient
from .credentials import (DEFAULT_STATE_FILE, PairingError, delete_state,
                          load_state, redeem_pairing_code, save_state)
from .servers.base import STATUS_MESSAGE_DISABLED, STATUS_MESSAGE_ENABLED

log = logging.getLogger("relay-agent")
ICONS = {
    'enabled': 'mdi6.toggle-switch',
    'disabled': 'mdi6.toggle-switch-off-outline',
    # 'enabled': 'mdi6.checkbox-marked-circle',
    # 'disabled': 'mdi6.checkbox-blank-circle-outline',
    'connect': 'mdi6.play',
    'disconnect': 'mdi6.stop',
    'unpair': 'mdi6.close'
}

# The remote chat application the relay connects to, as one marked-up link;
# defined once so every user-facing status text shows the same URL.
CHAT_LINK = '<a href="https://chat.sigmundai.eu">chat.sigmundai.eu</a>'

# The states the widget can be in. The three primary user states are
# "disconnected", "needs_pairing" and "connected"; the rest are auxiliary.
STATES = ("disconnected", "needs_pairing", "connecting", "connected", "fatal")

# The text shown on the status label for each state ("RelayWidget._status_text"
# is simply this lookup). The "fatal" state mirrors the "disconnected" state
# except for its status text, which explains that the connection was lost; the
# agent never reconnects automatically after it (see the state machine above).
STATUS_TEXTS = {
    "disconnected": f"Not connected to {CHAT_LINK}",
    "needs_pairing": (f"Enable the get-pairing-code tool in {CHAT_LINK} "
                      f"and ask for a code"),
    "connecting": f"Connecting to {CHAT_LINK} …",
    "connected": f"Connected to {CHAT_LINK}",
    "fatal": (f"Connection to {CHAT_LINK}' was lost. This may be because "
              f"another agent connected or because of a network error."),
}

# How often (ms) the GUI thread polls the providers' status_message
# properties. A status_message is a simple property (pull, no "changed"
# event), and a live in-process server's status can change at any time -- e.g.
# because a tool call ran -- so the widget refreshes the toggle buttons on
# this timer, next to the refreshes on every state change. See
# RelayWidget._refresh_status_messages.
STATUS_POLL_INTERVAL_MS = 1000


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

    ``args_provider`` callables run when a server is enabled -- usually on the
    GUI thread already (from the widget's toggle buttons), in which case this
    helper simply calls ``fn()`` inline. When called from a non-GUI thread (a
    CLI agent, or the agent's asyncio thread via ``create()``), the callable is
    marshaled to the GUI thread through the process-wide dispatcher::

        StdioServerProvider("files", [fs_bin], args_provider=lambda: call_on_gui_thread(pick_folder))

    Works even before/without a RelayWidget, as long as a QApplication exists.
    Returns ``fn``'s result in the calling thread; re-raises whatever ``fn``
    raises there (e.g. "user cancelled"), which fails the enable / session.
    """
    app = QApplication.instance()
    if app is None:
        raise RuntimeError("call_on_gui_thread() needs a QApplication instance")
    if QThread.currentThread() is app.thread():
        return fn()  # already on the GUI thread: just run it
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
        """Ask the agent to stop; safe even if the thread already exited
        (e.g. the user clicks Disconnect just as a lost connection ends the
        thread) -- then there is nothing left to stop."""
        if self._loop is not None and self.isRunning():
            try:
                asyncio.run_coroutine_threadsafe(self._client.stop(), self._loop)
            except RuntimeError:
                pass  # the loop was closed as the thread exited: nothing to stop


class _ServerToggleRow(QWidget):
    """One server's toggle: a QCheckBox (toggle icon + display name) with the
    provider's status_message on a smaller, muted line below.

    Deliberately assembled from stock widgets -- no custom painting, no
    manual size calculations: the checkbox keeps its native keyboard
    handling, hover and accessible checked state, and the QLabel lays the
    status text out natively (wrapping included). The qtawesome circle icon
    handed to :meth:`set_icon` stands in for the checkbox's own square
    indicator (hidden with a one-line stylesheet), and the status line is
    indented to line up with the checkbox text.
    """

    toggled = Signal(bool)  # re-emitted from the internal checkbox

    _ICON_TEXT_GAP = 10  # px added after the icon when indenting the status line

    def __init__(self, label: str, parent=None):
        super().__init__(parent)
        self._status = ""
        self._check = QCheckBox(label, self)
        # One style rule, not custom painting: only the checkbox's own square
        # goes away, so the set_icon() circle is the single visible indicator.
        self._check.setStyleSheet("QCheckBox::indicator { width: 0; height: 0; }")
        self._check.toggled.connect(self.toggled.emit)
        self._status_label = QLabel(self)
        self._status_label.setWordWrap(True)
        self._status_label.hide()  # only shown once a status message is set
        self._status_label.setForegroundRole(QPalette.ColorRole.PlaceholderText)
        font = self._status_label.font()
        pt = font.pointSizeF()
        if pt > 0:
            font.setPointSizeF(max(7.0, pt * 0.85))
        elif font.pixelSize() > 0:
            font.setPixelSize(max(9, int(font.pixelSize() * 0.85)))
        self._status_label.setFont(font)
        row = QVBoxLayout(self)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(0)
        row.addWidget(self._check)
        row.addWidget(self._status_label)

    # -- checkbox passthrough -----------------------------------------------

    def text(self) -> str:
        """The checkbox label: the server's display name."""
        return self._check.text()

    def isChecked(self) -> bool:
        return self._check.isChecked()

    def setChecked(self, checked: bool) -> None:
        """Set the checked state. ``blockSignals()`` on this row works as
        expected: the row re-emits only its own ``toggled`` signal to the
        outside world, and that emission respects the block."""
        self._check.setChecked(bool(checked))

    def setToolTip(self, text) -> None:
        super().setToolTip(text)
        self._check.setToolTip(text)  # child widgets do not inherit tooltips

    def set_icon(self, icon, size: QSize | None = None) -> None:
        """Show ``icon`` as the visible indicator (optionally at ``size``).

        Also re-indents the status line so it keeps lining up with the
        checkbox text whatever the icon size.
        """
        if size is not None:
            self._check.setIconSize(size)
        self._check.setIcon(icon)
        self._status_label.setIndent(
            self._check.iconSize().width() + self._ICON_TEXT_GAP)

    # -- status ----------------------------------------------------------------

    def status_message(self) -> str:
        """The status text currently shown below the checkbox."""
        return self._status

    def set_status_message(self, message: str) -> None:
        """Show ``message`` on the small second line; a no-op when unchanged.

        An empty message clears the line: the label is hidden, so no blank
        gap remains in the layout.
        """
        message = "" if message is None else str(message)
        if message == self._status:
            return
        self._status = message
        self._status_label.setText(message)
        self._status_label.setVisible(bool(message))


class RelayWidget(QWidget):
    """Pair / connect / unpair widget for one relay agent.

    ``relay_url`` is fixed at construction (config, not user input). The
    pairing-code row is only shown while the state is "needs_pairing".

    One toggle row per registered provider shows its ``display_name`` with
    the provider's ``status_message`` in small print below (the "Click to ..."
    defaults, or live status while an in-process session runs) and
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
        # Providers' status messages are pull-based (plain properties) and a
        # live in-process server's can change whenever a tool runs, so next to
        # refreshing on every state change the widget also polls them on a
        # short GUI-thread timer (see _refresh_status_messages).
        self._status_timer = QTimer(self)
        self._status_timer.setInterval(STATUS_POLL_INTERVAL_MS)
        self._status_timer.timeout.connect(self._refresh_status_messages)
        self._status_timer.start()
        self._credentials = load_state(self._state_file)
        self._refresh()
        if self._credentials and auto_connect:
            self.connect_agent()

    # -- state -----------------------------------------------------------

    @property
    def state(self) -> str:
        """One of STATES: disconnected / needs_pairing / connecting /
        connected / fatal."""
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
        layout.setSpacing(10)
        
        pix_label = QLabel()
        pix_label.setAlignment(Qt.AlignCenter)
        pixmap = QPixmap(os.path.join(os.path.dirname(__file__),
                                      'sigmund-full.png'))
        pix_label.setPixmap(pixmap)
        layout.addWidget(pix_label)
        
        self._status_label = QLabel()
        self._status_label.setTextFormat(Qt.RichText)
        self._status_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self._status_label.setWordWrap(True)
        self._status_label.setOpenExternalLinks(True)
        layout.addWidget(self._status_label)

        # One toggle row per available MCP server: a checkbox (circle icon +
        # display name) with the provider's status_message in small print
        # beneath it (protocol name in the tooltip). Its checked state mirrors
        # provider.enabled, so servers can be turned on and off at runtime
        # instead of only at initialization.
        self._servers_container = QWidget()
        servers_row = QVBoxLayout(self._servers_container)
        servers_row.setContentsMargins(0, 0, 0, 0)
        servers_row.addWidget(QLabel("Allow Sigmund to"))
        self._toggle_buttons: dict[str, _ServerToggleRow] = {}
        for name, provider in self._providers.items():
            btn = _ServerToggleRow(getattr(provider, "display_name", None) or name)
            checked = bool(getattr(provider, "enabled", True))
            btn.setChecked(checked)
            btn.set_status_message(self._provider_status_message(provider))
            btn.set_icon(qta.icon(ICONS['enabled'] if checked else ICONS['disabled']),
                         QSize(24, 24))
            tooltip = f"Protocol name: {name}"
            if getattr(provider, "args_provider", None) is not None:
                tooltip += "\nStart arguments are requested when you enable this server"
            btn.setToolTip(tooltip)
            btn.toggled.connect(lambda checked, n=name: self._on_server_toggled(n, checked))
            self._toggle_buttons[name] = btn
            servers_row.addWidget(btn)
        # Servers that start enabled must ask for their start arguments now,
        # at initialization, on this (GUI) thread. If that fails (e.g. the
        # user cancels a folder dialog), the server simply stays disabled and
        # the toggle reflects that -- the app must not crash at startup.
        for name, provider in self._providers.items():
            if getattr(provider, "enabled", True):
                try:
                    provider.enabled = True  # asks args_provider (ProviderBase)
                except Exception as exc:
                    log.warning("server %r could not be enabled at startup: %s", name, exc)
                    self._set_provider_flag(provider, False)
                    btn = self._toggle_buttons[name]
                    btn.blockSignals(True)  # no toggled handler while the UI is still building
                    btn.setChecked(False)
                    btn.blockSignals(False)
        layout.addWidget(self._servers_container)

        # Pairing input, wrapped in a container so it can be shown only
        # when the user actually needs to enter a pairing code.
        self._pair_container = QWidget()
        pair_row = QHBoxLayout(self._pair_container)
        pair_row.setContentsMargins(0, 0, 0, 0)
        self._code_edit = QLineEdit()
        self._code_edit.setPlaceholderText("XXXX-XXXX")
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
        layout.addStretch()

    def _status_text(self) -> str:
        return STATUS_TEXTS[self._state]

    def _refresh(self) -> None:
        state = self._state
        paired = bool(self._credentials)
        running = self._thread is not None
        self._status_label.setText(self._status_text())
        self._refresh_status_messages()
    
        # pairing input is only visible when a code is actually needed
        self._pair_container.setVisible(state == "needs_pairing")
        self._code_edit.setEnabled(not running)
        self._pair_btn.setEnabled(not running)

        # primary button: Connect (disconnected/fatal) or Disconnect (running);
        # hidden in needs_pairing where Pair/Cancel are the actions
        self._connect_btn.setVisible(state != "needs_pairing")
        self._connect_btn.setText("Disconnect" if running else "Connect")
        self._connect_btn.setIcon(
            qta.icon(ICONS['disconnect'] if running else ICONS['connect']))
        self._connect_btn.setEnabled(True)
        self._cancel_btn.setVisible(state == "needs_pairing")

        # secondary action: while running -> "Disconnect & Unpair";
        # when disconnected/fatal and paired -> plain "Unpair"
        self._action_btn.setIcon(qta.icon(ICONS['unpair']))
        if running:
            self._action_btn.setText("Disconnect and Unpair")
            self._action_btn.setVisible(True)
            self._action_btn.setEnabled(True)
        elif paired and state in ("disconnected", "fatal"):
            self._action_btn.setText("Unpair")
            self._action_btn.setVisible(True)
            self._action_btn.setEnabled(True)
        else:
            self._action_btn.setVisible(False)

    def _provider_status_message(self, provider) -> str:
        """The provider's current status message; never raises.

        Providers without a status_message at all (plain provider objects
        registered without ProviderBase) still get the click-to-enable /
        click-to-disable defaults, so every toggle button explains itself.
        """
        try:
            message = provider.status_message
        except Exception:
            log.exception("reading status_message of provider %r failed; "
                          "falling back to the default",
                          getattr(provider, "name", provider))
            message = None
        if not isinstance(message, str):
            message = "" if message is None else str(message)
        if not message:
            enabled = bool(getattr(provider, "enabled", True))
            message = (STATUS_MESSAGE_ENABLED if enabled
                       else STATUS_MESSAGE_DISABLED)
        return message

    def _refresh_status_messages(self) -> None:
        """Push every provider's status_message into its toggle button.

        A status_message is pull-based: an in-process provider reports what
        its newest *live* MCPServer says right now (see
        InProcessServerProvider.status_message), so it can change without the
        widget doing anything. Called on every UI refresh and on
        self._status_timer.
        """
        for name, provider in self._providers.items():
            btn = self._toggle_buttons.get(name)
            if btn is not None:
                btn.set_status_message(self._provider_status_message(provider))

    def _append_log(self, text: str) -> None:
        log.info(text)

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
        provider = self._providers[name]
        btn = self._toggle_buttons[name]
        try:
            # ProviderBase.enabled resolves the start arguments when enabling
            # (each enable asks anew) and raises -- leaving the flag unchanged
            # -- when e.g. the user cancels the folder dialog.
            provider.enabled = bool(checked)
            btn.set_icon(qta.icon(ICONS['enabled'] if checked else ICONS['disabled']))
        except Exception as exc:
            log.info("server %r was not enabled: %s", name, exc)
            self._set_provider_flag(provider, False)
            btn.blockSignals(True)
            btn.setChecked(False)
            btn.blockSignals(False)        
        self._notify_servers_changed()
        self._refresh()
        self.servers_changed.emit(self.enabled_servers())

    @staticmethod
    def _set_provider_flag(provider, value: bool) -> None:
        """Set the raw flag without asking for args (used when an enable failed)."""
        if hasattr(provider, "_enabled"):
            provider._enabled = bool(value)
        else:
            provider.enabled = bool(value)  # plain provider object: no args to ask

    def _notify_servers_changed(self) -> None:
        """Push the new enabled-set to the relay if the agent is connected.

        Offline this is a no-op: the next hello frame advertises the current
        set anyway. Old relays ignore the frame.
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

        ``args_provider`` callables now usually run on the GUI thread (when a
        server is enabled), in which case you can show a Qt dialog directly;
        this helper is for the case where the callable runs on another thread
        (e.g. a CLI agent), e.g.::

            args_provider=lambda: call_on_gui_thread(pick_folder_dialog)

        On the GUI thread it just calls ``fn()`` inline. Raises whatever ``fn``
        raises; a failed enable leaves the server disabled.
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
        elif state == "fatal":
            # The connection was lost and the agent does not reconnect: show
            # the fatal state (which mirrors disconnected except for its
            # status text) until the user presses Connect (or Unpair).
            self._set_state("fatal")
        elif state == "failed":
            self._append_log("Agent thread failed")

    def _on_thread_finished(self, thread: _AgentThread) -> None:
        if self._thread is thread:
            self._thread = None
        if self._state == "fatal":
            # A lost connection ended the thread: keep the fatal state (its
            # status text explains why), but the buttons were last refreshed
            # while the thread was still alive -- now that it is gone they
            # must mirror "disconnected" again: Connect as the primary
            # button and plain Unpair (while paired).
            self._refresh()
        else:
            # The thread ended without a lost connection (e.g. after a
            # deliberate stop): go back to the plain disconnected state.
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

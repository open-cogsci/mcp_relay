"""Reusable Qt widget for the relay agent.

App-agnostic: pass your server providers and a state file; the widget handles
pairing (code entry), connect/disconnect, unpairing, status and a small log.

    widget = RelayWidget(providers={"opensesame": my_provider},
                         state_file="~/.config/myapp/relay-credentials.json")

The agent runs on its own QThread with its own asyncio loop; all cross-thread
communication goes through Qt signals, so the widget is safe to use from the
GUI thread of any Qt application (OpenSesame, a tray app, ...). Qt is optional
for the rest of the package: without qtpy/a Qt binding, importing this module
still works and instantiating RelayWidget raises ImportError.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import threading
from pathlib import Path

from .client import Fatal, RelayClient
from .credentials import (DEFAULT_STATE_FILE, PairingError, delete_state,
                          load_state, redeem_pairing_code, save_state)

log = logging.getLogger("relay-agent")

try:
    from qtpy.QtCore import QThread, Signal
    from qtpy.QtWidgets import (QFormLayout, QHBoxLayout, QLabel, QLineEdit,
                                QMessageBox, QPlainTextEdit, QPushButton,
                                QVBoxLayout, QWidget)

    QT_AVAILABLE = True
except ImportError:
    QT_AVAILABLE = False


if QT_AVAILABLE:

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
        """Pair / connect / unpair widget for one relay agent."""

        state_changed = Signal(str)
        pair_done = Signal(bool, str)

        def __init__(self, parent=None, *, providers: dict, state_file=DEFAULT_STATE_FILE,
                     relay_url: str = "", device_name: str | None = None,
                     auto_connect: bool = True):
            super().__init__(parent)
            if not providers:
                raise ValueError("providers must map server names to ServerProvider instances")
            self._providers = dict(providers)
            self._state_file = Path(state_file)
            self._device_name = device_name or socket.gethostname()
            self._thread: _AgentThread | None = None
            self._credentials: dict | None = None
            self._state = "not_paired"
            self._build_ui(relay_url)
            self.pair_done.connect(self._on_pair_done)
            state = load_state(self._state_file)
            if state:
                self._credentials = state
                self._relay_edit.setText(state.get("relay", relay_url))
                self._state = "paired"
            self._refresh()
            if self._credentials and auto_connect:
                self.connect_agent()

        # -- UI -------------------------------------------------------------

        def _build_ui(self, relay_url: str) -> None:
            self.setWindowTitle("MCP relay")
            layout = QVBoxLayout(self)
            form = QFormLayout()
            self._relay_edit = QLineEdit(relay_url)
            self._relay_edit.setPlaceholderText("https://relay.example.com")
            form.addRow("Relay URL", self._relay_edit)
            form.addRow("Servers", QLabel(", ".join(self._providers)))
            layout.addLayout(form)

            pair_row = QHBoxLayout()
            self._code_edit = QLineEdit()
            self._code_edit.setPlaceholderText("Pairing code, e.g. K7QM-2XPD")
            self._pair_btn = QPushButton("Pair")
            self._pair_btn.clicked.connect(self._on_pair_clicked)
            pair_row.addWidget(self._code_edit, 1)
            pair_row.addWidget(self._pair_btn)
            layout.addLayout(pair_row)

            btn_row = QHBoxLayout()
            self._connect_btn = QPushButton("Connect")
            self._connect_btn.clicked.connect(self._on_connect_clicked)
            self._unpair_btn = QPushButton("Unpair")
            self._unpair_btn.clicked.connect(self._on_unpair_clicked)
            btn_row.addWidget(self._connect_btn)
            btn_row.addWidget(self._unpair_btn)
            layout.addLayout(btn_row)

            self._status_label = QLabel()
            self._status_label.setWordWrap(True)
            layout.addWidget(self._status_label)

            self._log = QPlainTextEdit()
            self._log.setReadOnly(True)
            self._log.setMaximumBlockCount(500)
            layout.addWidget(self._log, 1)

        def _status_text(self) -> str:
            if self._state == "online":
                return f"Online — exposing: {', '.join(self._providers)}"
            if self._state == "connecting":
                return "Connecting…"
            if self._state == "offline":
                return "Disconnected — reconnecting automatically."
            if self._state == "fatal":
                return "The relay rejected this device (revoked or replaced?). Pair again."
            if self._state == "failed":
                return "The agent hit an unexpected error. Check the log; press Connect to retry."
            if self._state == "paired":
                dev = (self._credentials or {}).get("device_id", "?")
                return f"Paired as {dev}. Press Connect."
            return ("Not paired. Get a pairing code (the relay's chat tool or an admin) "
                    "and enter it above.")

        def _refresh(self) -> None:
            self._status_label.setText(self._status_text())
            connected = self._thread is not None
            self._connect_btn.setText("Disconnect" if connected else "Connect")
            self._connect_btn.setEnabled(bool(self._credentials))
            self._unpair_btn.setEnabled(bool(self._credentials))
            self._pair_btn.setEnabled(not connected)

        def _set_state(self, state: str) -> None:
            self._state = state
            self._refresh()
            self.state_changed.emit(state)

        def _append_log(self, text: str) -> None:
            self._log.appendPlainText(text)

        # -- actions ----------------------------------------------------------

        def _on_pair_clicked(self) -> None:
            code = self._code_edit.text().strip()
            relay = self._relay_edit.text().strip().rstrip("/")
            if not relay:
                QMessageBox.warning(self, "Pair", "Enter the relay URL first.")
                return
            if not code:
                QMessageBox.warning(self, "Pair", "Enter the pairing code.")
                return
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
                self._set_state("paired")
                self.connect_agent()
            else:
                QMessageBox.warning(self, "Pairing failed", message)

        def _on_connect_clicked(self) -> None:
            if self._thread is not None:
                self.disconnect_agent()
            else:
                self.connect_agent()

        def connect_agent(self) -> None:
            """Start the agent thread (no-op if already running or not paired)."""
            if self._thread is not None or not self._credentials:
                return
            relay = self._credentials.get("relay") or self._relay_edit.text().strip().rstrip("/")
            client = RelayClient(relay, self._credentials["device_token"], self._providers)
            self._thread = _AgentThread(client, self)
            self._thread.state_changed.connect(self._on_thread_state)
            self._thread.log_message.connect(self._append_log)
            self._thread.finished.connect(self._on_thread_finished)
            self._set_state("connecting")
            self._thread.start()

        def disconnect_agent(self) -> None:
            """Stop the agent thread; credentials are kept."""
            self._stop_thread()
            if self._state != "fatal":
                self._set_state("paired" if self._credentials else "not_paired")

        def _on_unpair_clicked(self) -> None:
            if not self._credentials:
                return
            reply = QMessageBox.question(
                self, "Unpair",
                "Remove the stored credentials for this device? "
                "The device will also be disconnected from the relay.",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply != QMessageBox.Yes:
                return
            self._stop_thread()
            delete_state(self._state_file)
            self._credentials = None
            self._set_state("not_paired")

        # -- thread plumbing ----------------------------------------------------

        def _on_thread_state(self, state: str) -> None:
            self._state = state
            self._refresh()
            self.state_changed.emit(state)

        def _on_thread_finished(self) -> None:
            self._thread = None
            if self._state not in ("fatal", "failed"):
                self._set_state("paired" if self._credentials else "not_paired")

        def _stop_thread(self) -> None:
            if self._thread is not None:
                self._thread.stop_client()
                self._thread.wait(5000)
                self._thread = None

        def closeEvent(self, event) -> None:
            self._stop_thread()
            super().closeEvent(event)

else:

    class RelayWidget:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "RelayWidget requires Qt: install qtpy plus a binding "
                "(PyQt5/PyQt6/PySide6). The non-GUI parts of "
                "sigmundai_mcp_relay_agent work without Qt."
            )

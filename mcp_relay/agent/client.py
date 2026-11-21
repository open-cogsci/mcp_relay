"""Relay client: dials OUT to the relay and bridges it to local MCP servers.

This is the generic, app-agnostic core of the desktop agent (the CLI in
agent.py and the Qt widget in widget.py are thin wrappers around it):

    LibreChat --HTTP--> relay <--WebSocket-- RelayClient --frames--> ServerProvider(s)

Never reconnects on its own: whenever the connection is lost -- the relay
closing it (e.g. close code 4001 because another agent connected, or 4401/
4403 because the device was rejected/revoked) or a network error -- the run
ends in the "fatal" state and raises Fatal, leaving the decision what to do
to the embedding app (the widget shows its "fatal" state, which mirrors the
disconnected state, and reconnects only when the user asks). The only clean
exit is stop(), which returns with state "offline". State changes are
reported through the optional ``on_state`` callback
("connecting" | "online" | "offline" | "fatal").
"""

import asyncio
import contextlib
import json
import logging
import re
from collections.abc import Callable

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from .protocol import MAX_MESSAGE, dumps, validate_server_list
from .servers import ServerProvider, check_providers, enabled_providers

log = logging.getLogger("relay-agent")


class Fatal(Exception):
    """The connection run ended; the caller decides whether and how to retry."""


class RelayClient:
    def __init__(self, relay_url: str, token: str,
                 providers: dict[str, ServerProvider],
                 on_state: Callable[[str], None] | None = None):
        self.ws_url = re.sub(r"^http", "ws", relay_url.rstrip("/"), count=1) + "/agent/connect"
        self.token = token
        self.providers = check_providers(providers)
        self.on_state = on_state
        self.sessions: dict[str, object] = {}  # session id -> LocalServerInstance
        self._background: set[asyncio.Task] = set()
        self._stop_event: asyncio.Event | None = None
        self._ws = None
        self._servers_lock = asyncio.Lock()  # guards _advertised vs the ws send
        self._advertised: list[str] | None = None  # last server list sent to the relay

    @property
    def server_names(self) -> list[str]:
        """Names of the currently *enabled* providers, in registration order."""
        return list(enabled_providers(self.providers))

    def enabled_server_names(self) -> list[str]:
        """Alias for server_names (the advertised list is always the enabled subset)."""
        return self.server_names

    async def update_servers(self) -> bool:
        """Advertise the current enabled-server set to the relay ("servers" frame).

        Returns True if the frame was sent, False when not connected (in that
        case the next hello carries the current set anyway). Safe to call from
        any thread via asyncio.run_coroutine_threadsafe on the client's loop.
        Relays that predate the "servers" frame ignore it.
        """
        names = self.server_names
        ws = self._ws
        if ws is None:
            return False
        async with self._servers_lock:
            if names == self._advertised:
                return True  # nothing changed since the last advertisement
            await ws.send(dumps({"type": "servers", "servers": names}))
            self._advertised = names
        log.info("advertised servers to relay: %s", ", ".join(names) or "(none)")
        return True

    # -- lifecycle ---------------------------------------------------------

    def _set_state(self, state: str) -> None:
        log.info("state -> %s", state)
        if self.on_state is not None:
            try:
                self.on_state(state)
            except Exception:
                log.exception("on_state callback failed")

    async def stop(self) -> None:
        """Ask run_forever() to return. Safe to schedule from another thread via
        asyncio.run_coroutine_threadsafe(client.stop(), loop)."""
        if self._stop_event is not None:
            self._stop_event.set()
        ws, self._ws = self._ws, None
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()

    async def run_forever(self) -> None:
        """Serve the relay until the connection ends; never reconnects.

        Any lost connection -- the relay closing it (e.g. close code 4001
        because another agent connected, or 4401/4403 because the device was
        rejected/revoked) or a network error -- ends the run: the state
        becomes "fatal" and a Fatal carrying a short, user-presentable
        reason is raised, leaving the decision to the embedding app (the
        widget shows its fatal state, which mirrors the disconnected one,
        and reconnects only when the user presses Connect). The only clean
        exit is stop(), which returns with state "offline".
        """
        self._stop_event = asyncio.Event()
        self._set_state("connecting")
        try:
            loss = await self._connect_once()
        except asyncio.CancelledError:
            raise  # loop teardown in progress, not a lost connection
        except InvalidStatus as exc:
            if exc.response.status_code in (401, 403):
                loss = ("The relay rejected this device's credentials "
                        "(revoked?). Pair again.")
            else:
                log.warning("relay answered HTTP %s", exc.response.status_code)
                loss = f"The relay answered with HTTP {exc.response.status_code}."
        except (OSError, ConnectionClosed, asyncio.TimeoutError) as exc:
            log.warning("connection problem: %s", exc or type(exc).__name__)
            loss = f"Connection to the relay was lost: {exc or type(exc).__name__}"
        except Exception:
            # Defensive: an unexpected error must not leave the caller
            # hanging in state "connecting" forever; it is a lost connection
            # too.
            log.exception("unexpected error in connection attempt")
            loss = "Connection to the relay was lost: see the log for details."
        if self._stop_event.is_set():
            # stop() won the race against the failing connection: clean exit
            self._set_state("offline")
            return
        self._set_state("fatal")
        raise Fatal(loss or "Connection to the relay was lost.")

    async def _connect_once(self) -> str | None:
        """One connection: dial, handshake, serve frames until it ends.

        Returns a short, user-presentable message saying why the connection
        was lost, or None when it was ended deliberately by stop().
        """
        log.info("connecting to %s", self.ws_url)
        async with connect(
            self.ws_url,
            additional_headers={"Authorization": f"Bearer {self.token}"},
            max_size=MAX_MESSAGE,
            open_timeout=15,
        ) as ws:
            self._ws = ws
            try:
                names = self.server_names
                validate_server_list(names)  # fail fast on a bad registry, before hello
                await ws.send(dumps({"type": "hello", "servers": names}))
                self._advertised = names
                welcome = json.loads(await asyncio.wait_for(ws.recv(), 10))
                if welcome.get("type") != "welcome":
                    raise ConnectionError(f"unexpected reply from relay: {welcome}")
                self._set_state("online")
                log.info("connected; paired to user %r; exposing: %s",
                         welcome.get("user_id"), ", ".join(self.server_names))
                async for raw in ws:
                    try:
                        await self._handle(ws, json.loads(raw))
                    except asyncio.CancelledError:
                        raise  # shutdown in progress: run the shielded cleanup below
                    except Exception:
                        log.exception("error while handling a frame from the relay")
            except ConnectionClosed:
                pass
            finally:
                self._ws = None
                await self._stop_all()
            if not self._stop_event.is_set():
                # The connection ended without the user asking for it: that
                # is a loss -- often the relay says why it closed (e.g. 4001
                # "another agent connected"), otherwise it was a network
                # error -- and the run must end fatally rather than
                # reconnect automatically.
                if ws.close_code is not None and ws.close_reason:
                    return (f"The relay closed the connection ({ws.close_code}: "
                            f"{ws.close_reason}).")
                if ws.close_code not in (None, 1000, 1001, 1006):
                    return f"The relay closed the connection (code {ws.close_code})."
                return "Connection to the relay was lost."
        return None  # stop() ended the connection: a clean exit, not a loss

    # -- frame handling ------------------------------------------------------

    async def _handle(self, ws, frame: dict) -> None:
        kind, sid = frame.get("type"), frame.get("session")

        async def send(f: dict) -> None:
            await ws.send(dumps(f))

        if kind == "open":
            name = frame.get("server")
            provider = self.providers.get(name)
            if provider is None or not getattr(provider, "enabled", True):
                await send({"type": "closed", "session": sid,
                            "reason": f"server {name!r} is not enabled on this agent"})
                return
            try:
                # create() normally just reuses the arguments resolved when the
                # provider was enabled (ProviderBase.resolve_args); it may only
                # call args_provider itself for providers that were never
                # enabled through the setter (e.g. a CLI-built provider). That
                # callable can block for a long time (e.g. a GUI folder dialog
                # marshaled via call_on_gui_thread), so run create() in a
                # worker thread: this asyncio loop keeps serving pings, other
                # frames and update_servers() meanwhile.
                instance = await asyncio.to_thread(
                    provider.create, sid, send, lambda s: self.sessions.pop(s, None))
                await instance.start()  # started before the next frame (the 'initialize') is handled
            except Exception as exc:
                log.warning("[%s] could not start server %r: %s", str(sid)[:8], name, exc)
                await send({"type": "closed", "session": sid, "reason": f"could not start server: {exc}"})
                return
            self.sessions[sid] = instance
        elif kind == "message":
            instance = self.sessions.get(sid)
            if instance is not None and isinstance(frame.get("data"), dict):
                await instance.write(frame["data"])
        elif kind == "close":
            instance = self.sessions.pop(sid, None)
            if instance is not None:
                task = asyncio.create_task(instance.stop())
                self._background.add(task)
                task.add_done_callback(self._background.discard)

    async def _stop_all(self) -> None:
        """Stop every live session instance.

        Shielded: this runs from a `finally` while the surrounding task may be
        cancelled (e.g. a session transport failure tearing the connection
        down). Without the shield the first `await` would re-raise CancelledError
        and leave instances running; with it they are always cleaned up. A
        per-instance timeout keeps one wedged instance from blocking the rest.
        """
        instances, self.sessions = list(self.sessions.values()), {}
        if not instances:
            return

        async def stop_one(inst) -> None:
            try:
                await asyncio.wait_for(asyncio.shield(inst.stop()), 15)
            except Exception:
                log.exception("error while stopping session instance %s",
                              getattr(inst, "sid", "?")[:8])

        # shield so cancellation of THIS task doesn't abort the cleanups: the
        # gather keeps running in the background and every instance is stopped
        try:
            await asyncio.shield(asyncio.gather(*(stop_one(i) for i in instances)))
        except asyncio.CancelledError:
            pass  # we are being cancelled (e.g. teardown after a failure); the
                  # shielded cleanups above still complete in the background
        except Exception:
            log.exception("error while stopping session instances")

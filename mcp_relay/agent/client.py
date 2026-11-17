"""Relay client: dials OUT to the relay and bridges it to local MCP servers.

This is the generic, app-agnostic core of the desktop agent (the CLI in
agent.py and the Qt widget in widget.py are thin wrappers around it):

    LibreChat --HTTP--> relay <--WebSocket-- RelayClient --frames--> ServerProvider(s)

Reconnects automatically with backoff; close codes 4001/4400/4401/4403 are
fatal (replaced / bad hello / unauthorized / revoked). State changes are
reported through the optional ``on_state`` callback
("connecting" | "online" | "offline" | "fatal").
"""

import asyncio
import contextlib
import json
import logging
import random
import re
from collections.abc import Callable

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from .protocol import FATAL_CLOSE_CODES, MAX_MESSAGE, dumps, validate_server_list
from .servers import ServerProvider, check_providers, enabled_providers

log = logging.getLogger("relay-agent")


class Fatal(Exception):
    """Reconnecting will not help."""


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
        self._stop_event = asyncio.Event()
        delay = 1.0
        while not self._stop_event.is_set():
            self._set_state("connecting")
            try:
                if await self._connect_once():
                    delay = 1.0  # we were connected for a while: start backoff afresh
            except InvalidStatus as exc:
                if exc.response.status_code in (401, 403):
                    self._set_state("fatal")
                    raise Fatal("The relay rejected this device's credentials (revoked?). Pair again.")
                log.warning("relay answered HTTP %s", exc.response.status_code)
            except (OSError, ConnectionClosed, asyncio.TimeoutError) as exc:
                log.warning("connection problem: %s", exc or type(exc).__name__)
            except Exception:
                # Never let an unexpected error kill the reconnect loop.
                log.exception("unexpected error in connection attempt")
            if self._stop_event.is_set():
                break
            self._set_state("offline")
            wait = delay * random.uniform(0.8, 1.2)
            log.info("reconnecting in %.0fs", wait)
            try:
                await asyncio.wait_for(self._stop_event.wait(), wait)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60.0)
        self._set_state("offline")

    async def _connect_once(self) -> bool:
        welcomed = False
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
                welcomed = True
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
            if ws.close_code in FATAL_CLOSE_CODES and not self._stop_event.is_set():
                self._set_state("fatal")
                raise Fatal(f"Relay closed the connection permanently ({ws.close_code}: {ws.close_reason}).")
        return welcomed

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
                # create() may run an args_provider, which can block for a long
                # time (e.g. a GUI folder dialog marshaled to the GUI thread);
                # run it in a worker thread so this asyncio loop keeps serving
                # pings, other frames and update_servers() meanwhile.
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

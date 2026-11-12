"""Pairing and credential storage for the relay agent.

Pairing works like Chromecast: the user types a short single-use code (shown
by the relay, via the /mcp/relay chat tool) into the agent; the agent exchanges
it at POST {relay}/pair/redeem for a long-lived device token, stored locally
with mode 0600.
"""

import contextlib
import json
import os
import sys
from pathlib import Path

import httpx

DEFAULT_STATE_FILE = Path.home() / ".config" / "mcp-relay-agent" / "credentials.json"


class PairingError(Exception):
    """Redeeming a pairing code failed; the message is user-presentable."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def redeem_pairing_code(relay_url: str, code: str, device_name: str, timeout: float = 15.0) -> dict:
    """Exchange a pairing code for {"device_id", "device_token", "user_id"}."""
    relay = relay_url.rstrip("/")
    try:
        r = httpx.post(f"{relay}/pair/redeem", json={"code": code, "device_name": device_name}, timeout=timeout)
    except httpx.HTTPError as exc:
        raise PairingError(f"Could not reach the relay at {relay}: {exc}") from exc
    if r.status_code != 200:
        try:
            message = r.json()["error"]["message"]
        except Exception:
            message = r.text[:200]
        raise PairingError(f"Pairing failed (HTTP {r.status_code}): {message}", status=r.status_code)
    return r.json()


def load_state(path=DEFAULT_STATE_FILE) -> dict | None:
    """Return the stored credentials dict, or None if not paired yet."""
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return None


def load_state_or_exit(path=DEFAULT_STATE_FILE) -> dict:
    state = load_state(path)
    if state is None:
        sys.exit(f"Not paired yet ({path} not found). Run:  python agent.py pair --relay URL --code CODE")
    return state


def save_state(path, data: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)  # the token is a credential


def delete_state(path=DEFAULT_STATE_FILE) -> None:
    with contextlib.suppress(FileNotFoundError):
        Path(path).unlink()

"""Persistent storage for pairing codes and device tokens (SQLite, stdlib only).

Pairing works like Chromecast / Bluetooth:

1. The user asks the relay (via the chat tool ``get_pairing_code`` on
   ``/mcp/relay``) for a short, single-use pairing code for their user id.
   The code is shown to the user.
2. The user types the code into the desktop agent.
3. The agent redeems the code for a long-lived *device token* that it stores
   locally.  The code is now burnt.  From then on the agent authenticates with
   the device token only.

Only SHA-256 hashes of codes and tokens are stored, so a leaked database does
not leak working credentials.  (Plain SHA-256 is appropriate here because
device tokens are 256-bit random values; it would not be appropriate for
user-chosen passwords.)
"""

from pathlib import Path
import hashlib
import secrets
import sqlite3
import time
from dataclasses import dataclass

# No 0/O/1/I/L so codes are easy to read aloud and type.
CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
CODE_LENGTH = 8  # 31**8 ~ 8.5e11 possibilities
CODE_TTL_SECONDS = 600
DEFAULT_DB = Path.home() / ".config" / "mcp-relay-server" / "relay.db"


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def normalize_code(code: str) -> str:
    """'k7qm-2xpd' -> 'K7QM2XPD' (users type codes sloppily)."""
    return "".join(ch for ch in code.upper() if ch.isalnum())


def format_code(code: str) -> str:
    half = len(code) // 2
    return f"{code[:half]}-{code[half:]}"


@dataclass(frozen=True)
class Device:
    id: str
    user_id: str
    name: str
    created_at: float
    last_seen: float | None


class Store:
    def __init__(self, path: str = DEFAULT_DB):
        # The relay is a single asyncio process and these queries are tiny, so
        # synchronous sqlite calls are fine for ~100s of users.
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False,
                                   isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS devices (
                id          TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL,
                name        TEXT NOT NULL,
                token_hash  TEXT NOT NULL UNIQUE,
                created_at  REAL NOT NULL,
                last_seen   REAL
            );
            CREATE INDEX IF NOT EXISTS devices_user ON devices(user_id);
            CREATE TABLE IF NOT EXISTS pairing_codes (
                code_hash   TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL,
                expires_at  REAL NOT NULL
            );
            """
        )

    # -- pairing codes -----------------------------------------------------

    def create_pairing_code(self, user_id: str) -> tuple[str, int]:
        """Return (display_code, ttl_seconds).  The plain code is never stored.

        A user has at most one live code: issuing a new one voids any earlier unused
        code for that user, so repeated requests cannot pile up valid codes.
        """
        now = time.time()
        self._db.execute("DELETE FROM pairing_codes WHERE expires_at < ? OR user_id = ?", (now, user_id))
        code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LENGTH))
        self._db.execute(
            "INSERT INTO pairing_codes (code_hash, user_id, expires_at) VALUES (?, ?, ?)",
            (_sha256(code), user_id, now + CODE_TTL_SECONDS),
        )
        return format_code(code), CODE_TTL_SECONDS

    def redeem_pairing_code(self, code: str, device_name: str) -> tuple[Device, str] | None:
        """Burn a code and mint a device token.  Returns (device, token) or None."""
        now = time.time()
        # DELETE ... RETURNING makes "check + burn" a single atomic statement.
        row = self._db.execute(
            "DELETE FROM pairing_codes WHERE code_hash = ? AND expires_at >= ? RETURNING user_id",
            (_sha256(normalize_code(code)), now),
        ).fetchone()
        if row is None:
            return None
        device_id = "dev_" + secrets.token_hex(6)
        token = "mcprelay_" + secrets.token_urlsafe(32)
        self._db.execute(
            "INSERT INTO devices (id, user_id, name, token_hash, created_at) VALUES (?, ?, ?, ?, ?)",
            (device_id, row["user_id"], device_name[:100], _sha256(token), now),
        )
        return Device(device_id, row["user_id"], device_name[:100], now, None), token

    # -- devices -----------------------------------------------------------

    def authenticate(self, token: str) -> Device | None:
        row = self._db.execute(
            "SELECT id, user_id, name, created_at, last_seen FROM devices WHERE token_hash = ?",
            (_sha256(token),),
        ).fetchone()
        return Device(**dict(row)) if row else None

    def touch(self, device_id: str) -> None:
        self._db.execute("UPDATE devices SET last_seen = ? WHERE id = ?", (time.time(), device_id))

    def list_devices(self, user_id: str | None = None) -> list[Device]:
        if user_id is None:
            rows = self._db.execute("SELECT id, user_id, name, created_at, last_seen FROM devices").fetchall()
        else:
            rows = self._db.execute(
                "SELECT id, user_id, name, created_at, last_seen FROM devices WHERE user_id = ?",
                (user_id,),
            ).fetchall()
        return [Device(**dict(r)) for r in rows]

    def get_device(self, device_id: str) -> Device | None:
        row = self._db.execute(
            "SELECT id, user_id, name, created_at, last_seen FROM devices WHERE id = ?", (device_id,)
        ).fetchone()
        return Device(**dict(row)) if row else None

    def revoke(self, device_id: str) -> bool:
        return self._db.execute("DELETE FROM devices WHERE id = ?", (device_id,)).rowcount > 0

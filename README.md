# MCP relay example

Lets a cloud MCP client (e.g. LibreChat) use MCP servers that run on users' desktops, where
no inbound connection is possible. The desktop side dials **out**.

```
LibreChat ──streamable HTTP──► relay ◄──outbound WebSocket── agent ──stdio──► MCP server ──► desktop app
  (cloud)                     (cloud)                         (user's PC)
```

| File | Role |
|---|---|
| `relay.py` | FastAPI relay. MCP-server-agnostic: forwards JSON-RPC without looking at tools. |
| `builtin_mcp.py` | The relay's own MCP server (`/mcp/relay`): get a pairing code, list and revoke devices from the chat. |
| `store.py` | SQLite store for pairing codes and device tokens (only hashes are stored). |
| `agent.py` | Desktop agent: pairs, connects, spawns local stdio MCP servers on demand. |
| `hello_server.py` | Example MCP server with one tool, `hello_world`. |
| `make_code.py` | Admin helper: looks up a LibreChat user id by email and creates a pairing code. |
| `test_relay.py` | pytest tests for the built-in server (`pip install pytest && pytest -q`). |

## Quick start

```bash
pip install -r requirements.txt

# --- cloud side -------------------------------------------------------------
# In production/ staging, use a safe keys.
export RELAY_API_KEY=1234    # LibreChat <-> relay
export RELAY_ADMIN_KEY=3456  # your backend <-> relay
python relay.py --port 8080 --host 0.0.0.0

# --- 1. your backend/admin creates a pairing code for a user -----------------
# In LibreChat, enable the desktop-relay MCP server. Ask for a pairing code.
# LibreChat will call `get_pairing_code` and show the result.
# Alternatively, you can get a pairing code programmatically.
# NOTE: "alice" is a placeholder. In real use, user_id MUST be the user's LibreChat id
# (what LibreChat sends as {{LIBRECHAT_USER_ID}}): a 24-char hex MongoDB _id such as
# 65f1c0a9e4b0a1b2c3d4e5f6. See "Finding a LibreChat user id" below, or use make_code.py.
curl -s -X POST localhost:8080/admin/pairing-codes \
     -H "Authorization: Bearer $RELAY_ADMIN_KEY" -H 'Content-Type: application/json' \
     -d '{"user_id": "alice"}'
# -> {"code": "GZW3-W7BP", "expires_in": 600, ...}   (show this code to the user)

# --- 2. run the agent (reconnects automatically) ------------------------------
python agent.py run --server hello="python hello_server.py"

# --- 3. on the user's PC: enter the code once ---------------------------------
python agent.py pair --relay http://localhost:8080 --code GZW3-W7BP
```

`--server NAME=COMMAND` is repeatable and accepts any stdio MCP server
(`--server blender="uvx blender-mcp" --server notes="python notes.py"`).

## LibreChat configuration

One entry per server name. `user_id` in the pairing step must equal the LibreChat user id.

```yaml
mcpServers:
  desktop-hello:
    type: streamable-http
    url: https://relay.example.com/mcp/hello
    headers:
      Authorization: "Bearer ${RELAY_API_KEY}"
      X-User-Id: "{{LIBRECHAT_USER_ID}}"
```

**LibreChat in Docker, relay on the host:** `relay.py` binds to `127.0.0.1` by default, which a
container cannot reach (`host.docker.internal` resolves to the Docker bridge address, not the host's
loopback). Start the relay with `python relay.py --host 0.0.0.0 --port 8080` (and make sure port 8080
is not reachable from untrusted networks), and give the `api` container
`extra_hosts: ["host.docker.internal:host-gateway"]`. If you use `mcpSettings.allowedDomains`, add
`host.docker.internal`. Symptom of the loopback problem: LibreChat shows the server but cannot
connect, and the relay log shows no incoming request at all.

Check your LibreChat version's docs for the exact header placeholder syntax. The relay rejects
a literal `{{...}}` user id so an unsubstituted placeholder cannot merge users.

### Finding a LibreChat user id

`{{LIBRECHAT_USER_ID}}` is the user's MongoDB `_id` as a string. With the stock LibreChat
docker-compose setup (container `chat-mongodb`, database `LibreChat`, no auth):

```bash
# one user, by email (LibreChat stores emails in lower case)
docker exec chat-mongodb mongosh --quiet LibreChat \
  --eval 'printjson(db.users.findOne({email: "alice@example.com"}, {_id: 1, email: 1, name: 1}))'
# -> { _id: ObjectId('65f1c0a9e4b0a1b2c3d4e5f6'), email: 'alice@example.com', name: 'Alice' }
#    the id is the hex string inside ObjectId('...')

# all users
docker exec chat-mongodb mongosh --quiet LibreChat \
  --eval 'db.users.find({}, {email: 1}).forEach(u => print(u._id.toString(), u.email))'
```

If your MongoDB has authentication or runs elsewhere, use your own connection string:
`mongosh "mongodb://user:pass@host:27017/LibreChat" --eval '...'`.

**Shortcut:** `make_code.py` does the lookup and creates the pairing code in one step:

```bash
export RELAY_URL=http://localhost:8080 RELAY_ADMIN_KEY=...
python make_code.py --email alice@example.com
# or, if you already know the id:
python make_code.py --user-id 65f1c0a9e4b0a1b2c3d4e5f6
```

It runs `docker exec chat-mongodb mongosh ...` by default; override with `--lookup-cmd` if your
setup differs. `make_code.py` needs shell access to the Docker host, so it is an admin tool. For
self-service pairing, have your own authenticated backend call `POST /admin/pairing-codes` with the
logged-in user's id. Never expose `RELAY_ADMIN_KEY` to browsers.

## Self-service pairing from the chat

The relay serves its own small MCP server at `/mcp/relay`. Users can ask LibreChat to pair their
computer, so no admin has to run `make_code.py` for them. Add a second entry next to the one for
your desktop server (same key, same user header):

```yaml
mcpServers:
  desktop-relay:
    type: streamable-http
    url: https://relay.example.com/mcp/relay
    headers:
      Authorization: "Bearer ${RELAY_API_KEY}"
      X-User-Id: "{{LIBRECHAT_USER_ID}}"
```

And tell the relay its public address, so the instructions it gives contain a URL that works from
the user's computer (not the Docker-internal one):

```bash
export RELAY_PUBLIC_URL=https://relay.example.com
```

The flow for the user:

1. In LibreChat: *"Pair my computer."* The model calls `get_pairing_code` and replies with a code
   such as `K7QM-2XPD` plus instructions.
2. On the computer: enter the code in the desktop agent (`python agent.py pair --relay URL --code CODE`).
3. Start the agent. Afterwards *"Which computers are paired?"* (`list_devices`) shows it as online,
   and *"Unpair my laptop"* (`revoke_device`) removes it.

This server works for users who have no computer paired yet, which is exactly when they need it.

Properties worth knowing:

* **Identity is not an argument.** The tools take no user id; the relay uses the authenticated
  `X-User-Id`, so one user cannot create codes for, list or revoke another user's devices.
* **One live code per user.** A new code voids the previous unused one, so repeated requests cannot
  pile up valid codes. Codes are single use and expire after 10 minutes.
* **The code passes through the model**, so the model provider sees it, and it is stored in the
  conversation history. Because it is short-lived and single use, the exposure is small. The risk
  that matters is prompt injection: text from some other tool or document could try to make the
  model call `get_pairing_code` and leak the result. Tool descriptions tell the model to call it only
  on an explicit request, but that is not a guarantee. If you want stronger protection, use
  LibreChat's tool-approval options for this server, if your version has them, and check
  `list_devices` periodically.
* **Device names are sanitized** before they are shown to the model (they are chosen by whoever ran
  `agent.py pair`).
* The name `relay` is reserved: an agent cannot expose a server called `relay`.

## Authentication

* **LibreChat -> relay:** shared secret (`RELAY_API_KEY`) + `X-User-Id`. Only trusted servers should hold the key.
* **Pairing (Chromecast-style):** the admin endpoint issues an 8-character, single-use code that expires in
  10 minutes. The agent exchanges it at `/pair/redeem` (rate limited per IP) for a long-lived
  device token, stored in `~/.config/mcp-relay-agent/credentials.json` (mode 0600).
* **Agent -> relay:** `Authorization: Bearer <device token>` on the WebSocket handshake.
* **Revocation:** `DELETE /admin/devices/{id}` removes the token and disconnects the agent;
  `GET /admin/devices?user_id=<librechat user id>` lists devices and online status.

## How routing works

* A user has at most one connected agent (a newer connection replaces the old one; the old agent exits
  rather than fighting over the slot). An agent can expose several named servers: `/mcp/<name>`.
* Each MCP session (one `initialize`) gets its **own stdio process** on the desktop, so no JSON-RPC id
  remapping is needed. Sessions end on `DELETE`, idle timeout (24h), eviction (16 per user) or agent disconnect.
* Responses are streamed as SSE with keepalives, so slow desktop actions don't hit proxy idle timeouts.
  Request timeout is 300s (`RELAY_REQUEST_TIMEOUT`); on timeout the relay sends `notifications/cancelled`.
* Errors: `503` no agent online, `404` unknown server or session (clients should re-initialize), `401` bad key.

Relay -> agent frames: `open`, `message`, `close`. Agent -> relay: `hello`, `message`, `closed`. See the code.

## Settings (environment)

`RELAY_API_KEY`, `RELAY_ADMIN_KEY` (required), `RELAY_PUBLIC_URL` (shown in pairing instructions),
`RELAY_DB`, `RELAY_USER_HEADER`, `RELAY_REQUEST_TIMEOUT`, `RELAY_SESSION_TTL`, `RELAY_MAX_SESSIONS_PER_USER`.

## Limitations / before production

* **Protocol:** implements the classic session-based streamable HTTP transport (`initialize` + `Mcp-Session-Id`).
  The newer stateless 2026-07-28 revision is not implemented. Clients that probe for it get a 400 and fall back.
  JSON-RPC batches are rejected. No SSE resumability (`Last-Event-ID`).
* **State is in memory:** run a single relay instance (fine for ~100 users). Scaling out needs sticky routing
  by user id or a shared registry.
* **TLS:** run behind a TLS-terminating proxy (Caddy, nginx) and use `wss://`. Raise its WebSocket idle timeout above 60s.
  Behind a proxy, make sure uvicorn sees the real client IP (`--forwarded-allow-ips`) or the pairing rate limit is per proxy.
* **Security:** an MCP server that drives a desktop is remote control of that machine. Keep tools narrow,
  consider local confirmation for destructive actions, and treat tool output as untrusted (prompt injection).
* **Packaging:** ship the agent as a tray app / service; consider OS keychain storage for the device token.

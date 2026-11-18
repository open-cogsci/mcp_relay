# MCP relay

Copyright Sebastiaan Mathôt 2026. Contains AI-generated code.

Lets a cloud MCP client (e.g. SigmundAI/ LibreChat) use MCP servers that run on users' desktops, where no inbound connection is possible.


## Quick start


### Server

On the server, set RELAY_API_KEY and start the server. The relay API key should be specified as the bearer token in the client (e.g. LibreChat).

```bash
export RELAY_API_KEY=1234
python server.py
```


### Desktop (agent)

First, `desktop-relay` tool in your MPC client and ask for a pairing code.

Next, on the desktop, start `example_qt_app.py` (or an application derived from it). This connects to the relay server and makes MCP servers available to the client. 

Alternatively, pair the agent and run and MCP server through the command line.

```bash
python agent.py pair --relay http://localhost:8080 --code 1234-5678
python agent.py run --server your-mcp-server="launch-mcp-command"
```


## Client (LibreChat)

One entry per server name. `user_id` in the pairing step must equal the LibreChat user id.

```yaml
mcpServers:
  # The desktop-relay is a built-in MCP server that handles pairing and needs to be enabled.
  desktop-relay:
    type: streamable-http
    url: https://relay.example.com/mcp/relay
    headers:
      Authorization: "Bearer ${RELAY_API_KEY}"
      X-User-Id: "{{LIBRECHAT_USER_ID}}"
  # In addition, add MCP servers offered by the relay. This depends on your setup.
  desktop-hello:
    type: streamable-http
    url: https://relay.example.com/mcp/hello
    headers:
      Authorization: "Bearer ${RELAY_API_KEY}"
      X-User-Id: "{{LIBRECHAT_USER_ID}}"
```

The `Authorization` token should match the RELAY_API_KEY of the server. The `X-User-Id` is a placeholder following LibreChat's config system, and does not depend on the relay configuration.


## License

`mcp-relay` is licensed under the [GNU General Public License v3](http://www.gnu.org/licenses/gpl-3.0.en.html).

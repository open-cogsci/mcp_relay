import argparse
import uvicorn
import logging
from mcp_relay.server.server import create_app, Config, MAX_WS_MESSAGE


def main() -> None:

    parser = argparse.ArgumentParser(description="MCP relay server")
    parser.add_argument("--host", default="0.0.0.0", help="bind address")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    uvicorn.run(create_app(Config.from_env()), host=args.host,
                port=args.port, ws_max_size=MAX_WS_MESSAGE)


if __name__ == "__main__":
    main()

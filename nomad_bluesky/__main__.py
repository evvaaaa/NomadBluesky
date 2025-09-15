import argparse
import os

from nomad_bluesky.callback import NomadCallback
from nomad_bluesky.callback import logger
from nomad_bluesky.tiled_listener import DEFAULT_POLL_PERIOD, NomadTiledListener


def main():
    parser = argparse.ArgumentParser(
        description="Nomad Writer Bluesky Interface, either over zmq or tiled."
    )
    parser.add_argument(
        "--log-level",
        "-l",
        type=str,
        default=os.environ.get("NOMAD_BLUESKY_LOG_LEVEL", "INFO"),
        help="Log level (default: from NOMAD_BLUESKY_LOG_LEVEL env var or INFO)",
    )

    parser.add_argument(
        "--nomad-api-token",
        type=str,
        default=os.environ.get("NOMAD_API_TOKEN"),
        help="Nomad API token (default: from NOMAD_API_TOKEN env var)",
    )
    parser.add_argument(
        "--nomad-api-url",
        type=str,
        default=os.environ.get("NOMAD_API_URL"),
        help="Nomad API URL (default: from NOMAD_API_URL env var)",
    )

    subparsers = parser.add_subparsers(dest="mode", required=True)

    zmq_parser = subparsers.add_parser("zmq")

    zmq_parser.add_argument(
        "--zmq-url",
        type=str,
        default=os.environ.get("ZMQ_URL"),
        help="ZMQ address (default: from ZMQ_URL env var)",
    )

    tiled_parser = subparsers.add_parser("tiled")

    tiled_parser.add_argument(
        "--tiled-url",
        type=str,
        default=os.environ.get("TILED_URL"),
        help="ZMQ address (default: from TILED_URL env var)",
    )
    tiled_parser.add_argument(
        "--tiled-api-key",
        type=str,
        default=os.environ.get("TILED_API_KEY"),
        help="ZMQ address (default: from TILED_API_KEY env var)",
    )

    tiled_parser.add_argument(
        "--tiled-poll-period",
        type=float,
        default=float(os.environ.get("TILED_POLL_PERIOD", DEFAULT_POLL_PERIOD)),
        help=f"Polling period in seconds (default: from TILED_POLL_PERIOD env var or {DEFAULT_POLL_PERIOD})",
    )

    args = parser.parse_args()
    logger.setLevel(args.log_level)
    if args.nomad_api_token is None:
        print(
            "nomad_bluesky: error: the following arguments are required: --nomad-api-token, "
            "alternatively set the environment variable NOMAD_API_TOKEN"
        )
        exit(1)
    if args.nomad_api_url is None:
        print(
            "nomad_bluesky: error: the following arguments are required: --nomad-api-url, "
            "alternatively set the environment variable NOMAD_API_URL"
        )
        exit(1)

    if args.mode == "zmq":
        if args.zmq_url is None:
            print(
                "nomad_bluesky zmq: error: the following arguments are required: --zmq-url, "
                "alternatively set the environment variable ZMQ_URL"
            )
            exit(1)
        callback = NomadCallback(args.nomad_api_url, args.nomad_api_token, args.zmq_url)
        logger.info(
            f"Listening on zmq `{args.zmq_url}` and will send data to nomad at `{args.nomad_api_url}`."
        )

        callback.serve()
    else:
        if args.tiled_url is None:
            print(
                "nomad_bluesky tiled: error: the following arguments are required: --tiled_url, "
                "alternatively set the environment variable TILED_URL"
            )
            exit(1)
        if args.tiled_api_key is None:
            print(
                "nomad_bluesky tiled: error: the following arguments are required: --tiled_api_key, "
                "alternatively set the environment variable TILED_API_KEY"
            )
            exit(1)

        listener = NomadTiledListener(
            args.nomad_api_token,
            args.nomad_api_url,
            args.tiled_url,
            args.tiled_api_key,
            poll_period=args.tiled_poll_period,
        )
        logger.info(
            f"Listening on tiled `{args.tiled_url}` and will send data to nomad at `{args.nomad_api_url}`."
        )
        listener.serve()


if __name__ == "__main__":
    main()

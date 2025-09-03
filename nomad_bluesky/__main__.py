import argparse
import os

from nomad_bluesky.callback import NomadCallback
from nomad_bluesky.callback import logger


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

    subparsers = parser.add_subparsers(dest="mode", required=True)

    zmq_parser = subparsers.add_parser("zmq")

    zmq_parser.add_argument(
        "--nomad-api-token",
        type=str,
        default=os.environ.get("NOMAD_API_TOKEN"),
        help="Nomad API token (default: from NOMAD_API_TOKEN env var)",
    )
    zmq_parser.add_argument(
        "--nomad-api-url",
        type=str,
        default=os.environ.get("NOMAD_API_URL"),
        help="Nomad API URL (default: from NOMAD_API_URL env var)",
    )
    zmq_parser.add_argument(
        "--zmq-address",
        type=str,
        default=os.environ.get("ZMQ_ADDRESS"),
        help="ZMQ address (default: from ZMQ_ADDRESS env var)",
    )

    tiled_parser = subparsers.add_parser("tiled")

    tiled_parser.add_argument(
        "--nomad-api-token",
        type=str,
        default=os.environ.get("NOMAD_API_TOKEN"),
        help="Nomad API token (default: from NOMAD_API_TOKEN env var)",
    )
    tiled_parser.add_argument(
        "--nomad-api-url",
        type=str,
        default=os.environ.get("NOMAD_API_URL"),
        help="Nomad API URL (default: from NOMAD_API_URL env var)",
    )
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

    args = parser.parse_args()
    logger.setLevel(args.log_level)
    if args.nomad_api_token is None:
        raise ValueError(
            "The `NOMAD_API_TOKEN` must be set via argument or environment variable."
        )
    if args.nomad_api_url is None:
        raise ValueError(
            "The `NOMAD_API_URL` must be set via argument or environment variable."
        )

    if args.mode == "zmq":
        if args.zmq_address is None:
            raise ValueError(
                "The `ZMQ_ADDRESS` must be set via argument or environment variable "
                "if the callback is to be used outside the bluesky process."
            )
        callback = NomadCallback(
            args.nomad_api_token, args.nomad_api_url, args.zmq_address
        )
        logger.info(
            f"Listening on zmq `{args.zmq_address}` and will send data to nomad at `{args.nomad_api_url}`."
        )
        callback.serve()
    else:
        logger.info(
            f"Listening on tiled `{args.tiled_url}` and will send data to nomad at `{args.nomad_api_url}`."
        )


if __name__ == "__main__":
    main()

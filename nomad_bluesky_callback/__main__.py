import os
import argparse
from nomad_bluesky_writer.logger import logger
from nomad_bluesky_writer.document_callback import NomadCallback


def main():
    parser = argparse.ArgumentParser(description="Nomad Writer Bluesky Callback")
    parser.add_argument(
        "--nomad-api-token",
        "-t",
        type=str,
        default=os.environ.get("NOMAD_API_TOKEN"),
        help="Nomad API token (default: from NOMAD_API_TOKEN env var)",
    )
    parser.add_argument(
        "--nomad-api-url",
        "-u",
        type=str,
        default=os.environ.get("NOMAD_API_URL"),
        help="Nomad API URL (default: from NOMAD_API_URL env var)",
    )
    parser.add_argument(
        "--zmq-address",
        "-z",
        type=str,
        default=os.environ.get("ZMQ_ADDRESS"),
        help="ZMQ address (default: from ZMQ_ADDRESS env var)",
    )
    parser.add_argument(
        "--log-level",
        "-l",
        type=str,
        default=os.environ.get("NOMAD_BLUESKY_LOG_LEVEL", "INFO"),
        help="Log level (default: from NOMAD_BLUESKY_LOG_LEVEL env var or INFO)",
    )

    args = parser.parse_args()

    # Check required environment variables if not provided
    if args.nomad_api_token is None:
        raise ValueError(
            "The `NOMAD_API_TOKEN` must be set via argument or environment variable."
        )
    if args.nomad_api_url is None:
        raise ValueError(
            "The `NOMAD_API_URL` must be set via argument or environment variable."
        )
    if args.nomad_api_url is None:
        raise ValueError(
            "The `ZMQ_ADDRESS` must be set via argument or environment variable "
            "if the callback is to be used outside the bluesky process."
        )

    logger.setLevel(args.log_level)
    callback = NomadCallback(args.nomad_api_token, args.nomad_api_url, args.zmq_address)
    callback.serve()


if __name__ == "__main__":
    main()

"""Interface for ``python -m daq_queuing_service``."""

import logging
from argparse import ArgumentParser
from collections.abc import Sequence
from pathlib import Path

import uvicorn

from daq_queuing_service.app._config import get_default_config_path, load_config
from daq_queuing_service.log import LOGGER

from . import __version__

__all__ = ["main"]


def main(args: Sequence[str] | None = None) -> None:
    """Argument parser for the CLI."""
    parser = ArgumentParser()
    parser.add_argument("-v", "--version", action="version", version=__version__)
    parser.add_argument("-p", "--port", type=int, default=8000)
    parser.add_argument("--dev", action="store_true", default=False)
    parser.add_argument("--config", type=Path, default=get_default_config_path())

    parsed_args = parser.parse_args(args)
    config = load_config(parsed_args.config)

    from daq_queuing_service.app.app import create_app

    LOGGER.setLevel(logging.INFO)
    app = create_app(config=config, dev=parsed_args.dev)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=parsed_args.port,
        workers=1,
        access_log=False,
    )


if __name__ == "__main__":
    main()

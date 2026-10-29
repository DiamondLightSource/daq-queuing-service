"""Interface for ``python -m daq_queuing_service``."""

from argparse import ArgumentParser
from collections.abc import Sequence
from pathlib import Path

import uvicorn

from daq_queuing_service.app.config import get_default_config_path, load_config
from daq_queuing_service.log import LOGGER, setup_logger
from daq_queuing_service.worker.worker import LOGGER as WORKER_LOGGER

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
    dev = parsed_args.dev

    for logger in (LOGGER, WORKER_LOGGER):
        setup_logger(
            logger,
            stream=config.logging.stream,
            graylog=config.logging.graylog,
            propagate=False,
            instrument=config.instrument,
        )

    from daq_queuing_service.app.app import create_app

    app = create_app(config=config, dev=dev)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=parsed_args.port,
        workers=1,
        access_log=False,
    )


if __name__ == "__main__":
    main()

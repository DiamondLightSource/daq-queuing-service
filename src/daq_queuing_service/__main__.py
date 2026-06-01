"""Interface for ``python -m daq_queuing_service``."""

from argparse import ArgumentParser
from collections.abc import Sequence

import uvicorn

from . import __version__

__all__ = ["main"]


def main(args: Sequence[str] | None = None) -> None:
    """Argument parser for the CLI."""
    parser = ArgumentParser()
    parser.add_argument("-v", "--version", action="version", version=__version__)
    parser.add_argument("-p", "--port", type=int, default=8000)
    parser.add_argument("--dev", action="store_true", default=False)

    parsed_args = parser.parse_args(args)

    from daq_queuing_service.app.app import create_app

    app = create_app(dev=parsed_args.dev)

    uvicorn.run(app, host="0.0.0.0", port=parsed_args.port, workers=1)


if __name__ == "__main__":
    main()

from collections.abc import Awaitable, Callable
from typing import TypeVar

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from daq_queuing_service.log import LOGGER
from daq_queuing_service.plugins.converter import ConverterError, ValidateError
from daq_queuing_service.task_queue.queue_utils import (
    NegativePositionError,
    QueueError,
    TaskInProgressError,
    TaskNotFoundError,
    TaskNotInQueueError,
)

# pyright: reportUnusedFunction=false

E = TypeVar("E", bound=Exception)

Handler = Callable[[Request, E], Awaitable[JSONResponse]]


def make_exception_handler(
    status_code: int, error_code: str
) -> Callable[[Request, Exception], Awaitable[JSONResponse]]:
    async def handler(request: Request, exception: Exception):
        LOGGER.exception("Error while handling request: %s", request)
        return JSONResponse(
            status_code=status_code,
            content={"error": error_code, "message": str(exception)},
        )

    return handler


def register_exception_handlers(app: FastAPI):
    app.add_exception_handler(
        TaskInProgressError,
        make_exception_handler(409, "task_in_progress"),
    )

    app.add_exception_handler(
        TaskNotFoundError,
        make_exception_handler(404, "task_not_found"),
    )

    app.add_exception_handler(
        TaskNotInQueueError,
        make_exception_handler(409, "task_not_in_queue"),
    )

    app.add_exception_handler(
        NegativePositionError,
        make_exception_handler(400, "negative_position"),
    )

    app.add_exception_handler(
        QueueError,
        make_exception_handler(409, "queue_error"),
    )

    app.add_exception_handler(
        ValidateError,
        make_exception_handler(422, "validation_error"),
    )

    app.add_exception_handler(
        ConverterError,
        make_exception_handler(422, "converter_error"),
    )

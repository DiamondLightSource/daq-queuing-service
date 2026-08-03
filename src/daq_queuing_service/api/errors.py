from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from daq_queuing_service.plugins.converter import ConverterError, ValidateError
from daq_queuing_service.task_queue.queue_utils import (
    NegativePositionError,
    QueueError,
    TaskInProgressError,
    TaskNotFoundError,
    TaskNotInQueueError,
)

# pyright: reportUnusedFunction=false


def register_exception_handlers(app: FastAPI):
    @app.exception_handler(TaskInProgressError)
    async def task_in_progress_handler(
        request: Request, exception: TaskInProgressError
    ):
        return JSONResponse(
            status_code=409,
            content={"error": "task_in_progress", "message": str(exception)},
        )

    @app.exception_handler(TaskNotFoundError)
    async def task_not_found_handler(request: Request, exception: TaskNotFoundError):
        return JSONResponse(
            status_code=404,
            content={"error": "task_not_found", "message": str(exception)},
        )

    @app.exception_handler(TaskNotInQueueError)
    async def task_not_in_queue_handler(
        request: Request, exception: TaskNotInQueueError
    ):
        return JSONResponse(
            status_code=409,
            content={"error": "task_not_in_queue", "message": str(exception)},
        )

    @app.exception_handler(NegativePositionError)
    async def negative_position_handler(
        request: Request, exception: NegativePositionError
    ):
        return JSONResponse(
            status_code=400,
            content={"error": "negative_position", "message": str(exception)},
        )

    @app.exception_handler(QueueError)
    async def queue_error_handler(request: Request, exception: QueueError):
        return JSONResponse(
            status_code=409,
            content={"error": "queue_error", "message": str(exception)},
        )

    @app.exception_handler(ValidateError)
    async def validation_error_handler(request: Request, exception: ValidateError):
        return JSONResponse(
            status_code=422,
            content={"error": "validation_error", "message": str(exception)},
        )

    @app.exception_handler(ConverterError)
    async def converter_error_handler(request: Request, exception: ConverterError):
        return JSONResponse(
            status_code=422,
            content={"error": "converter_error", "message": str(exception)},
        )

from blueapi.client.rest import InvalidParametersError, UnknownPlanError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from daq_queuing_service.api.api import InvalidExperimentDefinitionsError
from daq_queuing_service.task_queue.queue_utils import (
    NegativePositionError,
    QueueError,
    TaskIdInUseError,
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

    @app.exception_handler(TaskIdInUseError)
    async def task_id_in_use_handler(request: Request, exception: TaskIdInUseError):
        return JSONResponse(
            status_code=409,
            content={"error": "task_id_in_use", "message": str(exception)},
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

    @app.exception_handler(InvalidExperimentDefinitionsError)
    async def invalid_experiment_definitions_handler(
        request: Request, exception: InvalidExperimentDefinitionsError
    ):
        def format_error(error: InvalidParametersError | UnknownPlanError):
            match error:
                case InvalidParametersError():
                    return {"type": "invalid_parameters", "details": error.message()}
                case UnknownPlanError():
                    return {"type": "unknown_plan", "details": str(error)}

        return JSONResponse(
            status_code=409,
            content={
                "error": "invalid_experiment_definitions_error",
                "message": f"Found validation errors for {len(exception.errors.keys())}"
                " experiment definitions. No tasks have been added to the queue.",
                "details": {
                    index: format_error(error)
                    for index, error in exception.errors.items()
                },
            },
        )

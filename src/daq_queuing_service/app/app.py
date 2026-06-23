import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import NoReturn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from daq_queuing_service.api.api import create_api_router
from daq_queuing_service.api.errors import register_exception_handlers
from daq_queuing_service.blueapi_interaction.blueapi_adapter import BlueapiClientAdapter
from daq_queuing_service.blueapi_interaction.clients import get_blueapi_clients
from daq_queuing_service.broadcaster import Broadcaster
from daq_queuing_service.plugins.construct_task_request import (
    construct_blueapi_task_request,
)
from daq_queuing_service.plugins.converter_utils import get_converter
from daq_queuing_service.task_queue.queue import QUEUE_EVENTS, TaskQueue
from daq_queuing_service.worker.worker import QueueWorker

from ._config import load_config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)


def create_app(config_path: Path, dev: bool = False) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker_task = asyncio.create_task(app.state.worker.run_loop())
        app.state.worker_task = worker_task

        def log_task_exception(task: asyncio.Task[NoReturn]):
            try:
                exc = task.exception()
                if exc:
                    logging.error("Worker crashed", exc_info=exc)
            except asyncio.CancelledError:
                pass

        worker_task.add_done_callback(log_task_exception)
        try:
            yield
        finally:
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)

    config = load_config(config_path)

    broadcaster: Broadcaster[QUEUE_EVENTS] = Broadcaster()

    converter_path = config.converter.path
    converter_name = config.converter.name
    converter = get_converter(converter_path, converter_name)

    app = FastAPI(lifespan=lifespan)

    if dev:  # Allows local client/UI through CORS
        app.add_middleware(
            CORSMiddleware,
            allow_origin_regex=r"http://(localhost|127\.0\.0\.1):\d+",
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.state.queue = TaskQueue(converter, broadcaster)

    blueapi_rest_client, blueapi_client = get_blueapi_clients(config.blueapi)
    blueapi_client_adapter = BlueapiClientAdapter(blueapi_client)

    app.state.worker = QueueWorker(
        queue=app.state.queue,
        blueapi_client=blueapi_client_adapter,
        task_request_constructor=construct_blueapi_task_request,
    )

    register_exception_handlers(app)
    app.include_router(
        create_api_router(
            app.state.queue,
            blueapi_rest_client,
            construct_blueapi_task_request,
            broadcaster,
            config,
        )
    )

    return app

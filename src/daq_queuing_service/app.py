import asyncio
import logging
from contextlib import asynccontextmanager
from typing import NoReturn

from blueapi.client import BlueapiClient
from blueapi.client.rest import BlueapiRestClient
from blueapi.config import ApplicationConfig, RestConfig, StompConfig, TcpUrl
from fastapi import FastAPI
from pydantic import HttpUrl

from daq_queuing_service.api.api import create_api_router
from daq_queuing_service.api.errors import register_exception_handlers
from daq_queuing_service.blueapi_adapter import BlueapiClientAdapter
from daq_queuing_service.plugins.construct_task_request import (
    construct_blueapi_task_request,
)
from daq_queuing_service.task_queue.queue import TaskQueue
from daq_queuing_service.worker.worker import QueueWorker

LOCAL_BLUEAPI_URL = "http://localhost:8000/"
I15_1_BLUEAPI_URL = "https://i15-1-blueapi.diamond.ac.uk/"
STOMP_URL = "tcp://localhost:61613"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)


def create_app() -> FastAPI:
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

    app = FastAPI(lifespan=lifespan)

    app.state.queue = TaskQueue()

    rest_config = RestConfig(url=HttpUrl(LOCAL_BLUEAPI_URL))
    blueapi_rest_client = BlueapiRestClient(config=rest_config)
    blueapi_client = BlueapiClient.from_config(
        ApplicationConfig(
            api=rest_config,
            stomp=StompConfig(enabled=True, url=TcpUrl(STOMP_URL)),
        )
    )
    blueapi_client_adapter = BlueapiClientAdapter(blueapi_client)

    app.state.worker = QueueWorker(
        queue=app.state.queue,
        blueapi_client=blueapi_client_adapter,
        task_request_constructor=construct_blueapi_task_request,
    )

    register_exception_handlers(app)
    app.include_router(
        create_api_router(
            app.state.queue, blueapi_rest_client, construct_blueapi_task_request
        )
    )

    return app


app = create_app()

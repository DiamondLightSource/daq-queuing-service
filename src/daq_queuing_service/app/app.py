import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import NoReturn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.param_functions import Depends
from fastapi.params import Depends as DependsType

from daq_queuing_service.api.api import protected_routes, public_routes
from daq_queuing_service.api.errors import register_exception_handlers
from daq_queuing_service.app.authentication import (
    build_access_token_check,
    build_get_current_user,
)
from daq_queuing_service.app.authorisation import (
    build_ensure_current_user_is_in_whitelist,
)
from daq_queuing_service.blueapi_interaction.blueapi_adapter import BlueapiClientAdapter
from daq_queuing_service.blueapi_interaction.get_client import get_blueapi_client
from daq_queuing_service.broadcaster import Broadcaster
from daq_queuing_service.plugins.converter import get_converter
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

    dependencies: list[DependsType] = []
    whitelist_check = None
    if config.oidc:
        validate_token = build_access_token_check(config.oidc)
        get_current_user = build_get_current_user(validate_token)

        app.swagger_ui_init_oauth = {
            "clientId": "NOT_SUPPORTED",
        }

        dependencies.append(Depends(get_current_user))

        whitelist_check = build_ensure_current_user_is_in_whitelist(
            config.authorisation_whitelist, get_current_user
        )

    if dev:  # Allows local client/UI through CORS
        app.add_middleware(
            CORSMiddleware,
            allow_origin_regex=r"http://(localhost|127\.0\.0\.1):\d+",
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.state.queue = TaskQueue(converter, broadcaster)

    blueapi_client = get_blueapi_client(config.blueapi)
    blueapi_client_adapter = BlueapiClientAdapter(blueapi_client)

    app.state.worker = QueueWorker(
        queue=app.state.queue,
        blueapi_client=blueapi_client_adapter,
    )

    register_exception_handlers(app)
    app.include_router(public_routes(app.state.queue))
    app.include_router(
        protected_routes(
            app.state.queue, broadcaster, config, converter, whitelist_check
        ),
        dependencies=dependencies,
    )

    return app

import asyncio
from typing import NoReturn
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from daq_queuing_service.app import create_app
from daq_queuing_service.task_queue.queue import TaskQueue
from daq_queuing_service.worker.worker import QueueWorker


def test_create_app_returns_fast_api_object():
    app = create_app()
    assert isinstance(app, FastAPI)


def test_create_app_registers_exception_handlers():
    with patch(
        "daq_queuing_service.app.register_exception_handlers"
    ) as mock_register_exception_handlers:
        create_app()

    mock_register_exception_handlers.assert_called_once()


def test_create_app_adds_router():
    with patch("daq_queuing_service.app.create_api_router") as mock_create_api_router:
        create_app()

    mock_create_api_router.assert_called_once()


def test_lifespan_runs_without_error():
    app = create_app()

    with TestClient(app):
        # Startup run by now
        pass

    # Shutdown run by now


def test_worker_task_cancelled_on_shutdown():
    app = create_app()

    with TestClient(app):
        worker_task: asyncio.Task[NoReturn] = app.state.worker_task
        assert not worker_task.cancelled()

    assert worker_task.cancelled()


def test_queue_and_worker_added_to_app_state_and_queue_object_shared_across_app():
    with patch("daq_queuing_service.app.create_api_router") as mock_create_api_router:
        app = create_app()

    app_queue = app.state.queue
    app_worker = app.state.worker

    assert isinstance(app_queue, TaskQueue)
    assert isinstance(app_worker, QueueWorker)
    assert app_worker._queue is app_queue
    assert mock_create_api_router.call_args_list[0].args[0] is app_queue

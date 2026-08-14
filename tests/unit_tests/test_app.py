import asyncio
import logging
from pathlib import Path
from typing import NoReturn
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from pytest import LogCaptureFixture

from constants import TEST_CONFIG_PATH, TEST_CONFIG_WITH_AUTHN_PATH
from daq_queuing_service.app.app import create_app
from daq_queuing_service.task_queue.queue import TaskQueue
from daq_queuing_service.worker.worker import QueueWorker
from unit_tests.conftest import has_dependency_name


def test_create_app_returns_fast_api_object():
    app = create_app(Path(TEST_CONFIG_PATH))
    assert isinstance(app, FastAPI)


def test_create_app_registers_exception_handlers():
    with patch(
        "daq_queuing_service.app.app.register_exception_handlers"
    ) as mock_register_exception_handlers:
        create_app(Path(TEST_CONFIG_PATH))

    mock_register_exception_handlers.assert_called_once()


@patch("daq_queuing_service.app.app.public_routes")
@patch("daq_queuing_service.app.app.protected_routes")
def test_create_app_adds_routers(
    mock_public_routes: MagicMock, mock_protected_routes: MagicMock
):
    create_app(Path(TEST_CONFIG_PATH))

    mock_public_routes.assert_called_once()
    mock_protected_routes.assert_called_once()


def test_lifespan_runs_without_error():
    app = create_app(Path(TEST_CONFIG_PATH))

    with TestClient(app):
        # Startup run by now
        pass

    # Shutdown run by now


def test_worker_task_cancelled_on_shutdown():
    app = create_app(Path(TEST_CONFIG_PATH))

    with TestClient(app):
        worker_task: asyncio.Task[NoReturn] = app.state.worker_task
        assert not worker_task.cancelled()

    assert worker_task.cancelled()


@patch("daq_queuing_service.app.app.protected_routes")
def test_queue_and_worker_added_to_app_state_and_queue_object_shared_across_app(
    mock_protected_routes: MagicMock,
):

    app = create_app(Path(TEST_CONFIG_PATH))

    app_queue = app.state.queue
    app_worker = app.state.worker

    assert isinstance(app_queue, TaskQueue)
    assert isinstance(app_worker, QueueWorker)
    assert app_worker._queue is app_queue
    assert mock_protected_routes.call_args_list[0].args[0] is app_queue


@patch(
    "daq_queuing_service.worker.worker.QueueWorker._wait_for_next_task",
    AsyncMock(side_effect=Exception("Worker crashed!")),
)
def test_if_worker_crashes_then_error_logged(caplog: LogCaptureFixture):
    app = create_app(Path(TEST_CONFIG_PATH))

    with caplog.at_level(logging.ERROR):
        with TestClient(app):
            pass

    assert "Worker crashed!" in caplog.text


def test_if_dev_mode_cors_middlewhere_added_to_app():
    with patch(
        "daq_queuing_service.app.app.FastAPI.add_middleware"
    ) as mock_add_middleware:
        _ = create_app(Path(TEST_CONFIG_PATH), dev=True)

    mock_add_middleware.assert_called_once_with(
        CORSMiddleware,
        allow_origin_regex=r"http://(localhost|127\.0\.0\.1):\d+",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def test_create_app_adds_auth_dependencies_to_correct_routes():
    no_auth_required = ["read_root", "healthz", "get_queue_state"]
    app = create_app(Path(TEST_CONFIG_WITH_AUTHN_PATH))

    for route in app.routes:
        if isinstance(route, APIRoute):
            if route.name not in no_auth_required:
                assert has_dependency_name(route.dependant, "validate_bearer_token"), (
                    f"No access token check dependency for route {str(route)}"
                )
                assert has_dependency_name(route.dependant, "get_current_user"), (
                    f"No get user dependency for route {str(route)}"
                )
            else:
                assert not has_dependency_name(route.dependant, "validate_bearer_token")
                assert not has_dependency_name(route.dependant, "get_current_user")

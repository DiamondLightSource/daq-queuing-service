import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from blueapi.client.event_bus import OnAnyEvent
from blueapi.client.rest import (
    BlueskyRemoteControlError,
    InvalidParametersError,
    ParameterError,
    ServiceUnavailableError,
    UnknownPlanError,
)
from blueapi.core import DataEvent
from blueapi.service.model import TaskRequest
from blueapi.worker import ProgressEvent, TaskStatus, WorkerEvent, WorkerState
from pytest import LogCaptureFixture

from daq_queuing_service.blueapi_adapter import BlueapiClientAdapter, BlueapiResult
from daq_queuing_service.task import ExperimentDefinition, Status
from daq_queuing_service.task_queue.queue import TaskError, TaskQueue, TaskResult
from daq_queuing_service.worker.worker import QueueWorker


def _get_mock_blueapi_client(
    error: Exception | None = None, plan_error: TaskError | None = None
):
    client = BlueapiClientAdapter(client=MagicMock())
    client.get_state = AsyncMock(
        return_value=BlueapiResult(value=WorkerState.IDLE)
        if not isinstance(error, ServiceUnavailableError)
        else BlueapiResult(error=ServiceUnavailableError())
    )

    def mock_run_task(
        task_request: TaskRequest, on_event: OnAnyEvent
    ) -> BlueapiResult[
        TaskStatus,
        BlueskyRemoteControlError
        | InvalidParametersError
        | UnknownPlanError
        | ServiceUnavailableError,
    ]:
        mock_events = [
            WorkerEvent(
                state=WorkerState.RUNNING,
                task_status=TaskStatus(
                    task_id="worker_test",
                    result=None,
                    task_complete=False,
                    task_failed=False,
                ),
            ),
            ProgressEvent(task_id="worker_test"),
            DataEvent(name="data event", doc={}, task_id="worker_test"),
        ]
        if not error:
            for event in mock_events:
                on_event(event)

        return BlueapiResult(
            value=TaskStatus(  # type: ignore
                task_id="worker_test",
                result=TaskResult(outcome="success", result=None, type="NoneType")
                if not plan_error
                else plan_error,
                task_complete=not bool(plan_error),
                task_failed=bool(plan_error),
            )
            if not error
            else None,
            error=error,
        )

    client.run_task = AsyncMock(side_effect=mock_run_task)
    return client


@pytest.fixture
def worker(task_queue: TaskQueue):
    def construct_task_request(experiment_definition: ExperimentDefinition):
        return TaskRequest(name="sleep", params={}, instrument_session="cm12345-1")

    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(),
        task_request_constructor=construct_task_request,
        poll_time_s=0.01,
    )
    return worker


@pytest.fixture
def worker_with_parameter_error(task_queue: TaskQueue):
    task_request = TaskRequest(name="sleep", params={}, instrument_session="cm12345-1")

    def construct_task_request(experiment_definition: ExperimentDefinition):
        return task_request

    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(
            InvalidParametersError(
                errors=[
                    ParameterError(
                        loc=["bad_param"],
                        msg="fake_error",
                        type="extra_forbidden",
                        input=task_request.model_dump_json(),
                    )
                ]
            )
        ),
        task_request_constructor=construct_task_request,
    )
    return worker


@pytest.fixture
def worker_with_unknown_plan_error(task_queue: TaskQueue):
    def construct_task_request(experiment_definition: ExperimentDefinition):
        return TaskRequest(name="sleep", params={}, instrument_session="cm12345-1")

    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(UnknownPlanError()),
        task_request_constructor=construct_task_request,
    )
    return worker


@pytest.fixture
def worker_with_blueapi_error(task_queue: TaskQueue):
    task_request = TaskRequest(name="sleep", params={}, instrument_session="cm12345-1")

    def construct_task_request(experiment_definition: ExperimentDefinition):
        return task_request

    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(BlueskyRemoteControlError()),
        task_request_constructor=construct_task_request,
    )
    return worker


@pytest.fixture
def worker_with_plan_error(task_queue: TaskQueue):
    task_request = TaskRequest(name="sleep", params={}, instrument_session="cm12345-1")

    def construct_task_request(experiment_definition: ExperimentDefinition):
        return task_request

    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(
            plan_error=TaskError(
                outcome="error", type="ValueError", message="Error during plan"
            )
        ),
        task_request_constructor=construct_task_request,
    )
    return worker


@pytest.fixture
def only_loop_once():
    class EndLoopError(Exception):
        pass

    with patch(
        "daq_queuing_service.worker.worker.QueueWorker._at_loop_end",
        MagicMock(wraps=QueueWorker._at_loop_end, side_effect=EndLoopError),
    ):
        yield EndLoopError


async def test_worker_run_loop_cycle(
    worker: QueueWorker, only_loop_once: type[Exception]
):
    queue = await worker._queue.get_queue()
    first_task = worker._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker.run_loop()

    assert first_task.status == Status.SUCCESS
    worker._client.get_state.assert_called_once()  # type: ignore
    worker._client.run_task.assert_called_once()  # type: ignore


async def test_when_parameter_error_then_task_failed_and_error_added_to_task(
    worker_with_parameter_error: QueueWorker, only_loop_once: type[Exception]
):

    queue = await worker_with_parameter_error._queue.get_queue()
    first_task = worker_with_parameter_error._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker_with_parameter_error.run_loop()

    assert first_task.status == Status.ERROR
    assert first_task.errors == ["Unexpected field 'bad_param'"]
    worker_with_parameter_error._client.get_state.assert_called_once()  # type: ignore
    worker_with_parameter_error._client.run_task.assert_called_once()  # type: ignore


async def test_when_plan_name_error_then_task_failed_and_error_added_to_task(
    worker_with_unknown_plan_error: QueueWorker, only_loop_once: type[Exception]
):

    queue = await worker_with_unknown_plan_error._queue.get_queue()
    first_task = worker_with_unknown_plan_error._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker_with_unknown_plan_error.run_loop()

    assert first_task.status == Status.ERROR
    assert first_task.errors == ["Unknown plan", ""]
    worker_with_unknown_plan_error._client.get_state.assert_called_once()  # type: ignore
    worker_with_unknown_plan_error._client.run_task.assert_called_once()  # type: ignore


async def test_when_blueapi_error_then_task_put_back_into_queue(
    worker_with_blueapi_error: QueueWorker, only_loop_once: type[Exception]
):

    queue = await worker_with_blueapi_error._queue.get_queue()
    first_task = worker_with_blueapi_error._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker_with_blueapi_error.run_loop()

    assert first_task.status == Status.WAITING
    worker_with_blueapi_error._client.get_state.assert_called_once()  # type: ignore
    worker_with_blueapi_error._client.run_task.assert_called_once()  # type: ignore
    assert not await worker_with_blueapi_error._queue.get_history()


async def test_when_plan_error_then_task_failed_and_errors_added(
    worker_with_plan_error: QueueWorker, only_loop_once: type[Exception]
):

    queue = await worker_with_plan_error._queue.get_queue()
    first_task = worker_with_plan_error._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker_with_plan_error.run_loop()

    assert first_task.status == Status.ERROR
    assert first_task.errors == [
        TaskError(outcome="error", type="ValueError", message="Error during plan")
    ]
    worker_with_plan_error._client.get_state.assert_called_once()  # type: ignore
    worker_with_plan_error._client.run_task.assert_called_once()  # type: ignore


async def test__wait_for_next_task_polls_until_blueapi_ready(worker: QueueWorker):
    client = BlueapiClientAdapter(client=MagicMock())

    i = 0

    def mock_get_state():
        nonlocal i
        if i >= 5:
            result: BlueapiResult[WorkerState, ServiceUnavailableError] = BlueapiResult(
                value=WorkerState.IDLE
            )
        else:
            result = BlueapiResult(value=WorkerState.RUNNING)
        i += 1
        return result

    client.get_state = AsyncMock(side_effect=mock_get_state)
    worker._client = client

    await worker._wait_for_next_task()
    assert client.get_state.call_count == 6


async def test__wait_for_next_task_waits_for_queue_ready_to_give_task(
    worker: QueueWorker,
):
    queue = worker._queue
    queue._tasks[queue._queue[0]].status = Status.IN_PROGRESS

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(worker._wait_for_next_task(), timeout=0.05)


def test__at_loop_end_log_message(worker: QueueWorker, caplog: LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        worker._at_loop_end()
    assert "Loop finished" in caplog.text

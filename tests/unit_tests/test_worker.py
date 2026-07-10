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
from pytest import LogCaptureFixture, MonkeyPatch

from daq_queuing_service.blueapi_interaction.blueapi_adapter import (
    BlueapiClientAdapter,
    BlueapiResult,
)
from daq_queuing_service.blueapi_interaction.blueapi_call import CallStatus
from daq_queuing_service.task import Status
from daq_queuing_service.task_queue.queue import (
    PauseReason,
    TaskError,
    TaskQueue,
    TaskResult,
)
from daq_queuing_service.worker.worker import LOGGER, QueueWorker


@pytest.fixture(autouse=True)
def propagate_logs(monkeypatch: MonkeyPatch):
    # This is turned off in prod to avoid duplicate logs
    # but needed in tests for caplog to receive logs
    monkeypatch.setattr(LOGGER, "propagate", True)


def _get_mock_blueapi_client(
    error: Exception | None = None,
    plan_error: TaskError | None = None,
    mock_events: list[WorkerEvent | ProgressEvent | DataEvent] | None = None,
):
    client = BlueapiClientAdapter(client=MagicMock())
    client.get_state = AsyncMock(
        return_value=BlueapiResult(value=WorkerState.IDLE)
        if not isinstance(error, ServiceUnavailableError)
        else BlueapiResult(error=ServiceUnavailableError())
    )

    def mock_run_task(
        task_request: TaskRequest,
        on_event: OnAnyEvent,
    ) -> BlueapiResult[
        TaskStatus,
        BlueskyRemoteControlError
        | InvalidParametersError
        | UnknownPlanError
        | ServiceUnavailableError,
    ]:
        if not error and mock_events:
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

    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(mock_events=mock_events),
        poll_time_s=0.01,
    )
    return worker


@pytest.fixture
def worker_with_no_blueapi_events(task_queue: TaskQueue):
    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(),
        poll_time_s=0.01,
    )
    return worker


@pytest.fixture
def worker_with_parameter_error(task_queue: TaskQueue):
    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(
            InvalidParametersError(
                errors=[
                    ParameterError(
                        loc=["bad_param"],
                        msg="fake_error",
                        type="extra_forbidden",
                        input="blah",
                    )
                ]
            )
        ),
    )
    return worker


@pytest.fixture
def worker_with_unknown_plan_error(task_queue: TaskQueue):
    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(UnknownPlanError()),
    )
    return worker


@pytest.fixture
def worker_with_blueapi_error(task_queue: TaskQueue):
    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(BlueskyRemoteControlError()),
    )
    return worker


@pytest.fixture
def worker_with_plan_error(task_queue: TaskQueue):
    worker = QueueWorker(
        queue=task_queue,
        blueapi_client=_get_mock_blueapi_client(
            plan_error=TaskError(
                outcome="error", type="ValueError", message="Error during plan"
            )
        ),
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

    assert first_task.status == Status.COMPLETE
    worker._client.get_state.assert_called_once()  # type: ignore
    worker._client.run_task.assert_called_once()  # type: ignore


async def test_when_parameter_error_then_call_failed_and_error_added_to_call(
    worker_with_parameter_error: QueueWorker, only_loop_once: type[Exception]
):

    queue = await worker_with_parameter_error._queue.get_queue()
    first_task = worker_with_parameter_error._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker_with_parameter_error.run_loop()

    first_call = first_task.blueapi_calls[0]
    assert first_call.status == CallStatus.ERROR
    assert first_call.errors == ["Unexpected field 'bad_param'"]
    worker_with_parameter_error._client.get_state.assert_called_once()  # type: ignore
    worker_with_parameter_error._client.run_task.assert_called_once()  # type: ignore


async def test_when_plan_name_error_then_call_failed_and_error_added_to_call(
    worker_with_unknown_plan_error: QueueWorker, only_loop_once: type[Exception]
):

    queue = await worker_with_unknown_plan_error._queue.get_queue()
    first_task = worker_with_unknown_plan_error._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker_with_unknown_plan_error.run_loop()

    first_call = first_task.blueapi_calls[0]
    assert first_call.status == CallStatus.ERROR
    assert first_call.errors == ["Unknown plan", ""]
    worker_with_unknown_plan_error._client.get_state.assert_called_once()  # type: ignore
    worker_with_unknown_plan_error._client.run_task.assert_called_once()  # type: ignore


async def test_when_blueapi_error_then_call_put_back_into_queue(
    worker_with_blueapi_error: QueueWorker, only_loop_once: type[Exception]
):

    queue = await worker_with_blueapi_error._queue.get_queue()
    first_task = worker_with_blueapi_error._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker_with_blueapi_error.run_loop()

    first_call = first_task.blueapi_calls[0]
    assert first_call.status == CallStatus.WAITING
    assert first_task.status == Status.QUEUED
    worker_with_blueapi_error._client.get_state.assert_called_once()  # type: ignore
    worker_with_blueapi_error._client.run_task.assert_called_once()  # type: ignore
    assert not await worker_with_blueapi_error._queue.get_history()


async def test_when_plan_error_then_queue_paused_and_task_failed_and_errors_added(
    worker_with_plan_error: QueueWorker, only_loop_once: type[Exception]
):

    queue = await worker_with_plan_error._queue.get_queue()
    first_task = worker_with_plan_error._queue._tasks[queue[0].id]

    with pytest.raises(only_loop_once):
        await worker_with_plan_error.run_loop()

    assert worker_with_plan_error._queue.state.paused is True
    assert worker_with_plan_error._queue.state.last_pause_reason == PauseReason.ERROR
    first_call = first_task.blueapi_calls[0]
    assert first_call.status == CallStatus.ERROR
    assert first_call.errors == [
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
    queue._tasks[queue._queue[0]].blueapi_calls[0].status = CallStatus.IN_PROGRESS

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(worker._wait_for_next_task(), timeout=0.05)


def test__at_loop_end_log_message(worker: QueueWorker, caplog: LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        worker._at_loop_end()
    assert "Loop finished" in caplog.text


async def test_if_call_not_put_in_progress_by_event_then_call_put_in_progress_and_warn(
    worker_with_no_blueapi_events: QueueWorker,
    caplog: LogCaptureFixture,
    only_loop_once: type[Exception],
):
    queue = await worker_with_no_blueapi_events._queue.get_queue()
    first_task = worker_with_no_blueapi_events._queue._tasks[queue[0].id]

    with caplog.at_level(logging.WARNING):
        with pytest.raises(only_loop_once):
            await worker_with_no_blueapi_events.run_loop()

    first_call = first_task.blueapi_calls[0]

    assert first_call.status == CallStatus.SUCCESS
    assert (
        "Call (task_request=TaskRequest(name='test', params={}, instrument_session='')"
        + " parent_task_id='0' status=<CallStatus.CLAIMED: 'Claimed'> time_started=None"
        + " time_completed=None result=None errors=[] blueapi_id=None) status was not "
        + "updated to in progress even though the blueapi task is now complete!"
    ) in caplog.text

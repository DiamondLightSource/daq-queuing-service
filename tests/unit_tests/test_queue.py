import asyncio
import copy
from unittest.mock import MagicMock

import pytest
from blueapi.service.model import TaskRequest
from blueapi.worker.event import TaskError, TaskResult

from daq_queuing_service.blueapi_interaction.blueapi_call import (
    BlueapiCall,
    BlueapiCallResponse,
    CallStatus,
)
from daq_queuing_service.broadcaster import Broadcaster
from daq_queuing_service.plugins.converter import Converter, ConverterError
from daq_queuing_service.task_queue.queue import (
    PauseReason,
    QueueContents,
    QueueState,
    TaskQueue,
    TaskRegistry,
    TaskWithPosition,
)
from daq_queuing_service.task_queue.queue_utils import (
    NegativePositionError,
    TaskIdInUseError,
    TaskInProgressError,
    TaskNotClaimedError,
    TaskNotFoundError,
)
from daq_queuing_service.task_queue.task import (
    Experiment,
    ExperimentDefinition,
    Status,
    Task,
    TaskKind,
)

from .conftest import make_sample

pytest_plugins = ("pytest_asyncio",)


def make_new_task(id_str: str):
    return Task(
        id=id_str,
        experiment=Experiment(
            name="test_experiment",
            instrument_session="",
            experiment_definition=ExperimentDefinition(
                name="test",
                id=id_str,
                data={},
            ),
            sample=make_sample("test_sample", id_str),
        ),
    )


async def test_add_tasks_adds_to_end_when_no_position_given(task_queue: TaskQueue):
    new_task = make_new_task("new")
    await task_queue.add_tasks([new_task])
    assert task_queue._queue == ["0", "1", "2", "3", "4", "new"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}


async def test_add_tasks_adds_to_call_queue(converter: Converter):
    task_queue = TaskQueue(converter=converter, broadcaster=Broadcaster())
    await task_queue.add_tasks([make_new_task("new"), make_new_task("new_2")])
    assert task_queue._call_queue == [
        BlueapiCall(
            task_request=TaskRequest(name="test", params={}, instrument_session=""),
            parent_task_id="new",
        ),
        BlueapiCall(
            task_request=TaskRequest(name="test", params={}, instrument_session=""),
            parent_task_id="new_2",
        ),
    ]


async def test_add_tasks_with_position_works_as_expected(task_queue: TaskQueue):
    new_task = make_new_task("new")
    await task_queue.add_tasks([new_task], 2)
    assert task_queue._queue == ["0", "1", "new", "2", "3", "4"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}


async def test_add_tasks_adds_to_the_end_if_position_bigger_than_queue_length(
    task_queue: TaskQueue,
):
    new_task = make_new_task("new")
    await task_queue.add_tasks([new_task], 20)
    assert task_queue._queue == ["0", "1", "2", "3", "4", "new"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}


async def test_add_task_to_position_0_adds_to_position_1_if_first_task_in_progress(
    task_queue_in_progress: TaskQueue,
):
    new_tasks = [make_new_task("new"), make_new_task("new_2")]
    first_task = await task_queue_in_progress.get_task_by_position(0)
    assert first_task and first_task.status == Status.IN_PROGRESS

    await task_queue_in_progress.add_tasks(new_tasks, 0)

    assert task_queue_in_progress._queue == ["0", "new", "new_2", "1", "2", "3", "4"]
    assert set(task_queue_in_progress._tasks.keys()) == {
        "0",
        "1",
        "2",
        "3",
        "4",
        "new",
        "new_2",
    }


async def test_add_task_to_position_0_adds_to_position_0_if_first_task_waiting(
    task_queue: TaskQueue,
):
    new_tasks = new_tasks = [make_new_task("new"), make_new_task("new_2")]
    first_task = await task_queue.get_task_by_position(0)
    assert first_task and first_task.status == Status.QUEUED

    await task_queue.add_tasks(new_tasks, 0)

    assert task_queue._queue == ["new", "new_2", "0", "1", "2", "3", "4"]
    assert set(task_queue._tasks.keys()) == {
        "0",
        "1",
        "2",
        "3",
        "4",
        "new",
        "new_2",
    }


async def test_add_task_to_negative_position_raises_error(
    task_queue_in_progress: TaskQueue,
):
    new_tasks = [make_new_task("new"), make_new_task("new_2")]
    with pytest.raises(NegativePositionError):
        await task_queue_in_progress.add_tasks(new_tasks, -1)


async def test_add_task_with_repeated_task_id_raises_error(task_queue: TaskQueue):
    new_tasks = [task_queue._tasks["1"]]
    with pytest.raises(TaskIdInUseError):
        await task_queue.add_tasks(new_tasks)


@pytest.mark.parametrize(
    "task_to_move, new_position, expected_order, expected_return_value",
    [
        [2, 2, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 2],
        [5, 2, [0, 1, 5, 2, 3, 4, 6, 7, 8, 9], 2],
        [9, 0, [9, 0, 1, 2, 3, 4, 5, 6, 7, 8], 0],
        [0, 9, [1, 2, 3, 4, 5, 6, 7, 8, 9, 0], 9],
        [0, 20, [1, 2, 3, 4, 5, 6, 7, 8, 9, 0], 9],
    ],
)
async def test_move_task_works_as_expected_and_returns_new_position(
    task_to_move: int,
    new_position: int,
    expected_order: list[int],
    expected_return_value: int,
    converter: Converter,
):
    queue = TaskQueue(converter=converter, broadcaster=Broadcaster())
    tasks = [make_new_task(str(i)) for i in range(10)]
    await queue.add_tasks(tasks)
    task = str(task_to_move)

    return_value = await queue.move_task(task, new_position)

    assert return_value == expected_return_value
    result_order = [int(task_id) for task_id in queue._queue]
    assert result_order == expected_order


async def test_move_task_to_position_0_moves_to_position_1_if_first_task_in_progress(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    new_position = await task_queue_in_progress.move_task("4", 0)
    assert new_position == 1
    expected_order = ["0", "4", "1", "2", "3"]
    assert task_queue_in_progress._queue == expected_order
    assert [
        call.parent_task_id for call in task_queue_in_progress._call_queue
    ] == expected_order


async def test_move_task_to_position_0_moves_to_position_0_if_first_task_waiting(
    task_queue: TaskQueue,
):
    task = await task_queue.get_task_by_position(0)
    assert task and task.status == Status.QUEUED

    await task_queue.move_task("4", 0)
    assert task_queue._queue == ["4", "0", "1", "2", "3"]


async def test_move_task_does_not_move_task_that_is_in_progress_and_raises_error(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    with pytest.raises(TaskInProgressError):
        await task_queue_in_progress.move_task("0", 3)

    assert task_queue_in_progress._queue == ["0", "1", "2", "3", "4"]
    assert set(task_queue_in_progress._tasks.keys()) == {"0", "1", "2", "3", "4"}


async def test_move_task_raises_error_if_wrong_task_id_given(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    with pytest.raises(TaskNotFoundError):
        await task_queue_in_progress.move_task("10", 3)

    assert task_queue_in_progress._queue == ["0", "1", "2", "3", "4"]


async def test_cancel_tasks_works_as_expected(task_queue: TaskQueue):
    await task_queue.cancel_tasks(["4", "2"])
    assert task_queue._queue == ["0", "1", "3"]


async def test_cancel_tasks_does_not_cancel_task_that_is_in_progress_and_raises_error(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    with pytest.raises(TaskInProgressError):
        await task_queue_in_progress.cancel_tasks(["0", "2"])

    assert task_queue_in_progress._queue == ["0", "1", "2", "3", "4"]
    assert set(task_queue_in_progress._tasks.keys()) == {"0", "1", "2", "3", "4"}


async def test_cancel_tasks_raises_error_if_wrong_task_id_used(task_queue: TaskQueue):
    with pytest.raises(TaskNotFoundError):
        await task_queue.cancel_tasks(["4", "11", "2", "10"])
    assert task_queue._queue == ["0", "1", "2", "3", "4"]


async def test_cancel_all_tasks_cancels_all_queued_tasks(
    task_queue_in_progress: TaskQueue,
):
    cancelled_tasks = await task_queue_in_progress.cancel_all_tasks()
    assert [task.id for task in cancelled_tasks] == ["1", "2", "3", "4"]
    assert task_queue_in_progress._queue == ["0"]  # This one is in progress


async def test__remove_tasks_from_registry_does_not_remove_tasks_if_in_queue_or_history(
    task_queue_with_history: TaskQueue,
):
    assert "0" in task_queue_with_history._history
    assert "4" in task_queue_with_history._queue

    task_queue_with_history._remove_tasks_from_registry(["0", "4"])

    assert "0" in task_queue_with_history._tasks
    assert "4" in task_queue_with_history._tasks


async def test_get_queue_only_returns_tasks_in_queue(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history._history == ["0", "1"]
    result = await task_queue_with_history.get_queue()
    assert result == [
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="2", data={}
                ),
                sample=make_sample("test_8_2", "2"),
            ),
            id="2",
            status=Status.IN_PROGRESS,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="2",
                    status=CallStatus.IN_PROGRESS,
                    time_started="2026-04-17T15:02:00.000000",
                    time_completed=None,
                    errors=[],
                    result=None,
                    blueapi_id=None,
                )
            ],
            position=0,
            kind=TaskKind.EXPERIMENT,
        ),
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="3", data={}
                ),
                sample=make_sample("test_8_3", "3"),
            ),
            id="3",
            status=Status.QUEUED,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="3",
                    status=CallStatus.WAITING,
                    time_started=None,
                    time_completed=None,
                    errors=[],
                    result=None,
                    blueapi_id=None,
                )
            ],
            position=1,
            kind=TaskKind.EXPERIMENT,
        ),
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="4", data={}
                ),
                sample=make_sample("test_8_4", "4"),
            ),
            id="4",
            status=Status.QUEUED,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="4",
                    status=CallStatus.WAITING,
                    time_started=None,
                    time_completed=None,
                    errors=[],
                    result=None,
                    blueapi_id=None,
                )
            ],
            position=2,
            kind=TaskKind.EXPERIMENT,
        ),
    ]


async def test_get_history_only_returns_tasks_in_history(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history._queue == ["2", "3", "4"]
    result = await task_queue_with_history.get_history()
    assert result == [
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="0", data={}
                ),
                sample=make_sample("test_8_0", "0"),
            ),
            id="0",
            status=Status.ERROR,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="0",
                    status=CallStatus.ERROR,
                    time_started="2026-04-17T15:00:00.000000",
                    time_completed="2026-04-17T15:00:59.000000",
                    errors=[
                        TaskError(
                            outcome="error",
                            type="ValueError",
                            message="Error during plan",
                        )
                    ],
                    result=None,
                    blueapi_id=None,
                )
            ],
            position=None,
            kind=TaskKind.EXPERIMENT,
        ),
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="1", data={}
                ),
                sample=make_sample("test_8_1", "1"),
            ),
            id="1",
            status=Status.COMPLETE,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="1",
                    status=CallStatus.SUCCESS,
                    time_started="2026-04-17T15:01:00.000000",
                    time_completed="2026-04-17T15:01:59.000000",
                    errors=[],
                    result=TaskResult(result=None, type="NoneType"),
                    blueapi_id=None,
                )
            ],
            position=None,
            kind=TaskKind.EXPERIMENT,
        ),
    ]


async def test_get_tasks_returns_tasks_in_queue_and_history(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history._queue == ["2", "3", "4"]
    assert task_queue_with_history._history == ["0", "1"]
    result = await task_queue_with_history.get_tasks()
    assert result == [
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="0", data={}
                ),
                sample=make_sample("test_8_0", "0"),
            ),
            id="0",
            status=Status.ERROR,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="0",
                    status=CallStatus.ERROR,
                    time_started="2026-04-17T15:00:00.000000",
                    time_completed="2026-04-17T15:00:59.000000",
                    errors=[
                        TaskError(
                            outcome="error",
                            type="ValueError",
                            message="Error during plan",
                        )
                    ],
                    result=None,
                    blueapi_id=None,
                )
            ],
            position=None,
            kind=TaskKind.EXPERIMENT,
        ),
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="1", data={}
                ),
                sample=make_sample("test_8_1", "1"),
            ),
            id="1",
            status=Status.COMPLETE,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="1",
                    status=CallStatus.SUCCESS,
                    time_started="2026-04-17T15:01:00.000000",
                    time_completed="2026-04-17T15:01:59.000000",
                    errors=[],
                    result=TaskResult(result=None, type="NoneType"),
                    blueapi_id=None,
                )
            ],
            position=None,
            kind=TaskKind.EXPERIMENT,
        ),
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="2", data={}
                ),
                sample=make_sample("test_8_2", "2"),
            ),
            id="2",
            status=Status.IN_PROGRESS,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="2",
                    status=CallStatus.IN_PROGRESS,
                    time_started="2026-04-17T15:02:00.000000",
                    time_completed=None,
                    errors=[],
                    result=None,
                    blueapi_id=None,
                )
            ],
            position=0,
            kind=TaskKind.EXPERIMENT,
        ),
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="3", data={}
                ),
                sample=make_sample("test_8_3", "3"),
            ),
            id="3",
            status=Status.QUEUED,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="3",
                    status=CallStatus.WAITING,
                    time_started=None,
                    time_completed=None,
                    errors=[],
                    result=None,
                    blueapi_id=None,
                )
            ],
            position=1,
            kind=TaskKind.EXPERIMENT,
        ),
        TaskWithPosition(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="",
                experiment_definition=ExperimentDefinition(
                    name="test", id="4", data={}
                ),
                sample=make_sample("test_8_4", "4"),
            ),
            id="4",
            status=Status.QUEUED,
            blueapi_calls=[
                BlueapiCallResponse(
                    task_request=TaskRequest(name="test", instrument_session=""),
                    parent_task_id="4",
                    status=CallStatus.WAITING,
                    time_started=None,
                    time_completed=None,
                    errors=[],
                    result=None,
                    blueapi_id=None,
                )
            ],
            position=2,
            kind=TaskKind.EXPERIMENT,
        ),
    ]


async def test_get_task_by_id_returns_task_in_queue_or_history(
    task_queue_with_history: TaskQueue,
):
    assert (
        "0" not in task_queue_with_history._queue
        and "0" in task_queue_with_history._history
    )
    assert (
        "4" in task_queue_with_history._queue
        and "4" not in task_queue_with_history._history
    )
    assert isinstance(
        await task_queue_with_history.get_task_by_id("0"), TaskWithPosition
    )
    assert isinstance(
        await task_queue_with_history.get_task_by_id("4"), TaskWithPosition
    )


async def test_get_task_by_id_raises_error_if_task_id_does_not_exist(
    task_queue_with_history: TaskQueue,
):
    with pytest.raises(TaskNotFoundError):
        assert await task_queue_with_history.get_task_by_id("fake") is None


async def test_get_task_by_pos_returns_task_in_queue(
    task_queue_with_history: TaskQueue,
):
    task = await task_queue_with_history.get_task_by_position(2)
    assert isinstance(task, TaskWithPosition)
    assert task.position == 2


async def test_get_task_by_pos_returns_none_if_position_not_in_queue(
    task_queue: TaskQueue,
):
    assert await task_queue.get_task_by_position(5) is None
    assert await task_queue.get_task_by_position(-6) is None


async def test_get_task_by_pos_works_with_negative_indexing(task_queue: TaskQueue):
    last_task = await task_queue.get_task_by_position(-1)
    assert last_task and last_task.id == "4"
    last_task = await task_queue.get_task_by_position(-5)
    assert last_task and last_task.id == "0"


async def test_clear_history_removes_history_and_removes_completed_tasks_from_registry(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history._history == ["0", "1"]
    assert {"0", "1"}.issubset(task_queue_with_history._tasks.keys())

    await task_queue_with_history.clear_history()

    assert task_queue_with_history._history == []
    assert not {"0", "1"}.intersection(task_queue_with_history._tasks.keys())


async def test_pausing_queue_prevents_task_from_being_claimed(task_queue: TaskQueue):
    await task_queue.pause_queue(PauseReason.EMPTY_QUEUE)
    assert task_queue.state.paused
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.get_next_call_once_available(), timeout=0.05)


async def test_unpausing_queue_allows_tasks_to_being_claimed(task_queue: TaskQueue):
    await task_queue.resume_queue()
    assert not task_queue.state.paused
    await task_queue.get_next_call_once_available()


@pytest.mark.parametrize(
    "initial_state, pause_reason, expected_state",
    [
        (
            QueueState(paused=True, last_pause_reason=PauseReason.USER_REQUESTED),
            PauseReason.ERROR,
            QueueState(paused=True, last_pause_reason=PauseReason.ERROR),
        ),
        (
            QueueState(paused=True, last_pause_reason=PauseReason.ERROR),
            PauseReason.EMPTY_QUEUE,
            QueueState(paused=True, last_pause_reason=PauseReason.ERROR),
        ),
        (
            QueueState(paused=True, last_pause_reason=PauseReason.ERROR),
            PauseReason.USER_REQUESTED,
            QueueState(paused=True, last_pause_reason=PauseReason.ERROR),
        ),
        (
            QueueState(paused=True, last_pause_reason=PauseReason.USER_REQUESTED),
            PauseReason.EMPTY_QUEUE,
            QueueState(paused=True, last_pause_reason=PauseReason.USER_REQUESTED),
        ),
        (
            QueueState(paused=False, last_pause_reason=PauseReason.ERROR),
            PauseReason.USER_REQUESTED,
            QueueState(paused=True, last_pause_reason=PauseReason.USER_REQUESTED),
        ),
    ],
)
async def test__pause_queue_if_queue_already_paused_then_last_reason_kept_unless_error(
    initial_state: QueueState,
    pause_reason: PauseReason,
    expected_state: QueueState,
    task_queue: TaskQueue,
):
    task_queue._state = initial_state
    task_queue._pause_queue(reason=pause_reason)
    assert task_queue.state == expected_state


async def test_claim_next_call_once_available_claims_task_and_returns(
    task_queue: TaskQueue,
):
    next_task = task_queue._tasks[task_queue._queue[0]]
    next_call = next_task.blueapi_calls[0]
    assert next_task and next_task.status == Status.QUEUED
    assert next_call.status == CallStatus.WAITING

    claimed_call = await task_queue.get_next_call_once_available()
    assert claimed_call.parent_task_id == next_call.parent_task_id
    assert claimed_call.status == CallStatus.CLAIMED


async def test_claim_next_call_once_available_waits_if_next_task_is_already_claimed(
    task_queue: TaskQueue,
):
    claimed_call = await task_queue.get_next_call_once_available()
    assert claimed_call and claimed_call.status == CallStatus.CLAIMED
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.get_next_call_once_available(), timeout=0.05)


async def test_claim_next_call_once_available_waits_if_next_task_is_already_in_progress(
    task_queue: TaskQueue,
):
    claimed_call = await task_queue.get_next_call_once_available()
    claimed_call.put_in_progress()
    assert claimed_call and claimed_call.status == CallStatus.IN_PROGRESS
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.get_next_call_once_available(), timeout=0.05)


async def test_wait_until_call_available_waits_if_next_task_is_claimed(
    task_queue: TaskQueue,
):
    claimed_call = await task_queue.get_next_call_once_available()
    assert claimed_call and claimed_call.status == CallStatus.CLAIMED
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.wait_until_call_available(), timeout=0.05)


async def test_wait_until_call_available_waits_if_next_task_is_in_progress(
    task_queue: TaskQueue,
):
    claimed_call = await task_queue.get_next_call_once_available()
    claimed_call.put_in_progress()
    assert claimed_call and claimed_call.status == CallStatus.IN_PROGRESS
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.wait_until_call_available(), timeout=0.05)


async def test_wait_until_call_available_waits_if_queue_paused(
    task_queue: TaskQueue,
):
    await task_queue.pause_queue(PauseReason.EMPTY_QUEUE)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.wait_until_call_available(), timeout=0.05)


async def test_wait_until_call_available_waits_if_queue_empty():
    task_queue = TaskQueue(Converter(), Broadcaster())
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.wait_until_call_available(), timeout=0.05)


async def test_wait_until_call_available_does_not_wait_if_conditions_met(
    task_queue: TaskQueue,
):
    await asyncio.wait_for(task_queue.wait_until_call_available(), timeout=0.05)


async def test_complete_call_puts_call_in_history_and_updates_status_to_complete(
    task_queue: TaskQueue,
):
    call = await task_queue.get_next_call_once_available()
    call.put_in_progress()
    assert call.status == CallStatus.IN_PROGRESS
    await task_queue.complete_call(call, TaskResult(result=None, type="NoneType"))
    assert call.parent_task_id not in task_queue._queue
    assert call.parent_task_id in task_queue._history
    assert call not in task_queue._call_queue
    assert call in task_queue._call_history
    assert call.status == CallStatus.SUCCESS


async def test_complete_call_must_receive_exact_same_object_as_was_claimed(
    task_queue: TaskQueue,
):
    call = await task_queue.get_next_call_once_available()
    similar_call = call.model_copy()
    another_similar_call = copy.copy(call)
    with pytest.raises(AssertionError):
        await task_queue.complete_call(
            similar_call, TaskResult(result=None, type="NoneType")
        )
    with pytest.raises(AssertionError):
        await task_queue.complete_call(
            another_similar_call, TaskResult(result=None, type="NoneType")
        )


async def test_fail_call_puts_task_in_history_and_updates_status_to_complete(
    task_queue: TaskQueue,
):
    call = await task_queue.get_next_call_once_available()
    assert call.status == CallStatus.CLAIMED
    await task_queue.fail_call(call)
    assert call.parent_task_id not in task_queue._queue
    assert call.parent_task_id in task_queue._history
    assert call not in task_queue._call_queue
    assert call in task_queue._call_history
    assert call.status == CallStatus.ERROR


async def test_fail_call_must_receive_exact_same_object_as_was_claimed(
    task_queue: TaskQueue,
):
    call = await task_queue.get_next_call_once_available()
    similar_call = call.model_copy()
    another_similar_call = copy.copy(call)
    with pytest.raises(AssertionError):
        await task_queue.fail_call(similar_call)
    with pytest.raises(AssertionError):
        await task_queue.fail_call(another_similar_call)


async def test_fail_call_with_errors_adds_errors_to_call(
    task_queue: TaskQueue,
):
    call = await task_queue.get_next_call_once_available()
    error = "This task failed"
    await task_queue.fail_call(call, [str(error)])
    assert call.status == CallStatus.ERROR
    assert call.errors == ["This task failed"]


async def test_fail_call_pauses_queue(task_queue: TaskQueue):
    assert task_queue.state == QueueState(
        paused=False, last_pause_reason=PauseReason.EMPTY_QUEUE
    )
    call = await task_queue.get_next_call_once_available()
    error = "This task failed"
    await task_queue.fail_call(call, [str(error)])
    assert task_queue.state == QueueState(
        paused=True, last_pause_reason=PauseReason.ERROR
    )


async def test_return_call_to_queue_changes_task_status_to_waiting(
    task_queue: TaskQueue,
):
    call = await task_queue.get_next_call_once_available()
    assert call.status == CallStatus.CLAIMED
    await task_queue.return_call_to_queue(call)
    assert call.status == CallStatus.WAITING


async def test_return_task_to_queue_raises_error_if_task_has_not_been_claimed(
    task_queue: TaskQueue,
):
    call = await task_queue.get_next_call_once_available()
    call.status = CallStatus.SUCCESS
    with pytest.raises(TaskNotClaimedError):
        await task_queue.return_call_to_queue(call)


async def test_get_call_queue_returns_calls_in_call_queue(
    task_queue_with_history: TaskQueue,
):
    result = await task_queue_with_history.get_call_queue()
    assert result == [
        BlueapiCallResponse(
            task_request=TaskRequest(name="test", params={}, instrument_session=""),
            parent_task_id="2",
            status=CallStatus.IN_PROGRESS,
            time_started="2026-04-17T15:02:00.000000",
            time_completed=None,
            result=None,
            errors=[],
            blueapi_id=None,
        ),
        BlueapiCallResponse(
            task_request=TaskRequest(name="test", params={}, instrument_session=""),
            parent_task_id="3",
            status=CallStatus.WAITING,
            time_started=None,
            time_completed=None,
            result=None,
            errors=[],
            blueapi_id=None,
        ),
        BlueapiCallResponse(
            task_request=TaskRequest(name="test", params={}, instrument_session=""),
            parent_task_id="4",
            status=CallStatus.WAITING,
            time_started=None,
            time_completed=None,
            result=None,
            errors=[],
            blueapi_id=None,
        ),
    ]


async def test_get_call_history_returns_calls_in_call_history(
    task_queue_with_history: TaskQueue,
):
    result = await task_queue_with_history.get_call_history()
    assert result == [
        BlueapiCallResponse(
            task_request=TaskRequest(name="test", params={}, instrument_session=""),
            parent_task_id="0",
            status=CallStatus.ERROR,
            time_started="2026-04-17T15:00:00.000000",
            time_completed="2026-04-17T15:00:59.000000",
            result=None,
            errors=[
                TaskError(
                    outcome="error", type="ValueError", message="Error during plan"
                )
            ],
            blueapi_id=None,
        ),
        BlueapiCallResponse(
            task_request=TaskRequest(name="test", params={}, instrument_session=""),
            parent_task_id="1",
            status=CallStatus.SUCCESS,
            time_started="2026-04-17T15:01:00.000000",
            time_completed="2026-04-17T15:01:59.000000",
            result=TaskResult(outcome="success", result=None, type="NoneType"),
            errors=[],
            blueapi_id=None,
        ),
    ]


async def test__sync_correctly_moves_tasks_with_all_completed_calls_into_history(
    task_queue: TaskQueue,
):
    task_queue._modifying = MagicMock()

    a_task_id = task_queue._queue[3]
    a_task = task_queue._tasks[a_task_id]
    completed_blueapi_call = BlueapiCall(
        task_request=TaskRequest(name="sync_test", instrument_session=""),
        status=CallStatus.SUCCESS,
        parent_task_id="",
    )

    a_task.blueapi_calls = [
        completed_blueapi_call,
        completed_blueapi_call,
    ]

    assert a_task_id in task_queue._queue
    assert a_task_id not in task_queue._history
    task_queue._sync()
    assert a_task_id not in task_queue._queue
    assert a_task_id in task_queue._history


async def test__sync_correctly_moves_tasks_with_any_errored_calls_into_history(
    task_queue_one_to_many: TaskQueue,
):
    task_queue_one_to_many._modifying = MagicMock()

    a_task_id = task_queue_one_to_many._queue[3]
    a_task = task_queue_one_to_many._tasks[a_task_id]
    errored_blueapi_call = BlueapiCall(
        task_request=TaskRequest(name="sync_test", instrument_session=""),
        status=CallStatus.ERROR,
        parent_task_id="",
    )

    a_task.blueapi_calls[0] = errored_blueapi_call
    assert a_task.blueapi_calls[1].status == CallStatus.WAITING

    assert a_task_id in task_queue_one_to_many._queue
    assert a_task_id not in task_queue_one_to_many._history
    task_queue_one_to_many._sync()
    assert a_task_id not in task_queue_one_to_many._queue
    assert a_task_id in task_queue_one_to_many._history


async def test_task_with_single_blueapi_calls_new_task_is_received_after_first_complete(
    task_queue: TaskQueue,
):
    first_call = await task_queue.get_next_call_once_available()
    first_call.put_in_progress()
    assert first_call.status == CallStatus.IN_PROGRESS
    await task_queue.complete_call(first_call, TaskResult(result=None, type="NoneType"))

    second_call = await task_queue.get_next_call_once_available()

    assert first_call.parent_task_id != second_call.parent_task_id
    assert first_call != second_call


async def test_task_with_multiple_blueapi_calls_second_returned_when_first_complete(
    task_queue_one_to_many: TaskQueue,
):
    first_call = await task_queue_one_to_many.get_next_call_once_available()
    first_call.put_in_progress()
    assert first_call.status == CallStatus.IN_PROGRESS
    await task_queue_one_to_many.complete_call(
        first_call, TaskResult(result=None, type="NoneType")
    )

    second_call = await task_queue_one_to_many.get_next_call_once_available()

    assert first_call.parent_task_id == second_call.parent_task_id
    assert first_call != second_call


async def test__sync_skips_subsequent_calls_if_previous_one_failed_within_a_task(
    task_queue_one_to_many: TaskQueue,
):
    task_queue_one_to_many._modifying = MagicMock()

    a_task_id = task_queue_one_to_many._queue[3]
    a_task = task_queue_one_to_many._tasks[a_task_id]
    errored_blueapi_call = BlueapiCall(
        task_request=TaskRequest(name="sync_test", instrument_session=""),
        status=CallStatus.ERROR,
        parent_task_id="",
    )

    a_task.blueapi_calls[0] = errored_blueapi_call

    task_queue_one_to_many._sync()
    assert a_task.blueapi_calls[0].status == Status.ERROR
    assert all(call.status == CallStatus.SKIPPED for call in a_task.blueapi_calls[1:])


async def test__sync_pauses_queue_if_no_more_items(
    task_queue: TaskQueue,
):
    task_queue._modifying = MagicMock()
    task_queue.state.paused = False
    task_queue._queue = []
    task_queue._call_queue = []

    task_queue._sync()

    assert task_queue.state.paused
    assert task_queue.state.last_pause_reason == PauseReason.EMPTY_QUEUE


def test__copy_contents_creates_copies(task_queue: TaskQueue):
    contents = task_queue._copy_contents()
    assert isinstance(contents["tasks"]["4"].experiment, Experiment)
    a_task = task_queue._tasks["4"]
    assert isinstance(a_task.experiment, Experiment)

    assert a_task.experiment.sample.name == "test_8_4"
    a_task.experiment.sample.name = "changed_name"

    assert contents["tasks"]["4"].experiment.sample.name == "test_8_4"


def test__restore_from_contents_replaces_queue_contents(task_queue: TaskQueue):
    new_queue = ["10"]
    new_history = ["9"]
    new_call_queue = [
        BlueapiCall(
            task_request=TaskRequest(
                name="task_10", params={}, instrument_session="session_10"
            ),
            parent_task_id="10",
        )
    ]
    new_call_history = [
        BlueapiCall(
            task_request=TaskRequest(
                name="task_9", params={}, instrument_session="session_9"
            ),
            parent_task_id="9",
        )
    ]
    new_tasks = TaskRegistry()
    new_tasks["9"] = Task(
        experiment=TaskRequest(
            name="task_9", params={}, instrument_session="session_9"
        ),
        blueapi_calls=new_call_history,
    )
    new_tasks["10"] = Task(
        experiment=TaskRequest(
            name="task_10",
            params={},
            instrument_session="session_10",
        ),
        blueapi_calls=new_call_queue,
    )

    new_contents: QueueContents = {
        "tasks": new_tasks,
        "queue": new_queue,
        "history": new_history,
        "call_queue": new_call_queue,
        "call_history": new_call_history,
    }

    task_queue._restore_from_contents(new_contents)

    assert task_queue._queue == new_queue
    assert task_queue._history == new_history
    assert task_queue._tasks == new_tasks
    assert task_queue._call_queue == new_call_queue
    assert task_queue._call_history == new_call_history


async def test__last_good_contents_updated_when_modifying_lock_entered(
    task_queue: TaskQueue,
):
    task_queue._queue = ["should be copied"]

    async with task_queue._modifying:
        task_queue._queue = []

    assert task_queue._last_good_contents["queue"] == ["should be copied"]
    assert task_queue._queue == []


async def test_if_error_during_conversion_then_error_handled_and_contents_restored(
    task_queue: TaskQueue,
):
    def convert(
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ):
        for task_id in task_queue._queue:
            del task_queue._tasks[task_id]
        task_queue._queue = []

        raise ValueError("Conversion failed")

    task_queue._converter.construct_blueapi_calls = convert

    with pytest.raises(ConverterError):
        await task_queue.get_queue()

    assert task_queue._queue == ["0", "1", "2", "3", "4"]
    assert list(task_queue._tasks.keys()) == ["0", "1", "2", "3", "4"]


async def test_if_error_during_conversion_then__restore_latest_good_contents_called(
    task_queue: TaskQueue,
):
    def convert(
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ):
        raise ValueError("Conversion failed")

    task_queue._converter.construct_blueapi_calls = convert
    task_queue._restore_latest_good_contents = MagicMock()
    task_queue.__init__(task_queue._converter, task_queue._broadcaster)

    with pytest.raises(ConverterError):
        await task_queue.get_queue()

    task_queue._restore_latest_good_contents.assert_called_once()

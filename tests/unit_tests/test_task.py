from datetime import datetime

import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.external_interaction.blueapi.blueapi_call import (
    BlueapiCall,
    CallStatus,
)
from daq_queuing_service.task_queue.task import Status, Task


@pytest.mark.parametrize(
    "blueapi_call_statuses, expected_task_status",
    [
        ([CallStatus.WAITING], Status.QUEUED),
        ([CallStatus.CLAIMED], Status.IN_PROGRESS),
        ([CallStatus.IN_PROGRESS], Status.IN_PROGRESS),
        ([CallStatus.SUCCESS], Status.COMPLETE),
        ([CallStatus.ERROR], Status.ERROR),
        ([CallStatus.WAITING, CallStatus.WAITING], Status.QUEUED),
        ([CallStatus.WAITING, CallStatus.CLAIMED], Status.IN_PROGRESS),
        ([CallStatus.WAITING, CallStatus.IN_PROGRESS], Status.IN_PROGRESS),
        ([CallStatus.WAITING, CallStatus.SUCCESS], Status.IN_PROGRESS),
        ([CallStatus.WAITING, CallStatus.ERROR], Status.ERROR),
        ([CallStatus.CLAIMED, CallStatus.SUCCESS], Status.IN_PROGRESS),
        ([CallStatus.CLAIMED, CallStatus.ERROR], Status.ERROR),
        ([CallStatus.IN_PROGRESS, CallStatus.SUCCESS], Status.IN_PROGRESS),
        ([CallStatus.IN_PROGRESS, CallStatus.ERROR], Status.ERROR),
        ([CallStatus.SUCCESS, CallStatus.ERROR], Status.ERROR),
        ([], Status.QUEUED),
    ],
)
def test_task_status_derived_correctly_from_call_statuses(
    blueapi_call_statuses: list[CallStatus], expected_task_status: Status
):
    task = Task(
        blueapi_calls=[
            BlueapiCall(
                status=status,
                task_request=TaskRequest(name="", instrument_session=""),
                parent_task_id="",
            )
            for status in blueapi_call_statuses
        ],
        experiment=TaskRequest(name="", instrument_session=""),
    )
    assert task.status == expected_task_status


@pytest.mark.parametrize(
    "calls_start_end, expected_task_runtime",
    [
        ([], None),
        ([(datetime(2026, 11, 3, 10, 0, 0), datetime(2026, 11, 3, 10, 0, 10))], 10),
        ([(None, datetime(2026, 11, 3, 10, 0, 10))], None),
        (
            [
                (datetime(2026, 11, 3, 10, 0, 0), datetime(2026, 11, 3, 10, 0, 10)),
                (datetime(2026, 11, 3, 10, 5, 0), datetime(2026, 11, 3, 10, 10, 15)),
                (datetime(2026, 11, 3, 10, 4, 7), datetime(2026, 11, 3, 17, 23, 59)),
            ],
            10 + 315 + 26392,
        ),
        (
            [
                (datetime(2026, 11, 3, 10, 0, 0), datetime(2026, 11, 3, 10, 0, 10)),
                (datetime(2026, 11, 3, 10, 0, 0), datetime(2026, 11, 3, 10, 10, 15)),
                (datetime(2026, 11, 3, 10, 4, 7), None),
            ],
            None,
        ),
    ],
)
def test_runtime_calculated_correctly_from_blueapi_call_runtimes(
    calls_start_end: list[tuple[datetime, datetime]], expected_task_runtime: float
):
    tr = TaskRequest(name="", params={}, instrument_session="")

    blueapi_calls = [
        BlueapiCall(
            time_started=start, time_completed=end, task_request=tr, parent_task_id=""
        )
        for start, end in calls_start_end
    ]
    task = Task(blueapi_calls=blueapi_calls, experiment=tr)
    assert task.runtime == expected_task_runtime


def test_put_on_hold_changes_status_to_on_hold_and_updates_reason():
    task = Task(experiment=TaskRequest(name="", params={}, instrument_session=""))
    assert task.status == Status.QUEUED
    assert task.hold_reason is None

    task.put_on_hold("Testing the put_on_hold method")

    assert task.status == Status.ON_HOLD
    assert task.hold_reason == "Testing the put_on_hold method"

    task.take_off_hold()

    assert task.status == Status.QUEUED
    assert task.hold_reason is None


@pytest.mark.parametrize(
    "current_status",
    [
        Status.CANCELLED,
        Status.COMPLETE,
        Status.ERROR,
        Status.IN_PROGRESS,
        Status.ON_HOLD,
    ],
)
def test_put_on_hold_raises_error_if_current_status_is_not_queued(
    current_status: Status,
):
    class _Task(Task):
        @property
        def status(self):
            return current_status

    task = _Task(experiment=TaskRequest(name="", params={}, instrument_session=""))
    with pytest.raises(ValueError) as exc:
        task.put_on_hold("This should fail")

    assert (
        str(exc.value)
        == f"Cannot put task {task.id} on hold, its status is already {current_status}"
    )


def test_put_on_hold_raises_error_if_no_reason_given():
    task = Task(experiment=TaskRequest(name="", params={}, instrument_session=""))
    with pytest.raises(ValueError) as exc:
        task.put_on_hold("")

    assert str(exc.value) == "Must give a reason for putting this task on hold"


@pytest.mark.parametrize(
    "current_status",
    [
        Status.CANCELLED,
        Status.COMPLETE,
        Status.ERROR,
        Status.IN_PROGRESS,
        Status.QUEUED,
    ],
)
def test_take_off_hold_raises_error_if_task_is_not_on_hold(current_status: Status):
    class _Task(Task):
        @property
        def status(self):
            return current_status

    task = _Task(experiment=TaskRequest(name="", params={}, instrument_session=""))
    with pytest.raises(ValueError) as exc:
        task.take_off_hold()

    assert (
        str(exc.value)
        == f"Cannot take task {task.id} off hold, it is not currently on hold. "
        + f"Current status: {current_status}"
    )

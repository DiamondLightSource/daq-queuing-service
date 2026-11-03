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

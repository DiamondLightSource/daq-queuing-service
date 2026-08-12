import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall, CallStatus
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

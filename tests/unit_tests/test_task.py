import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall, CallStatus
from daq_queuing_service.task import ExperimentDefinition, Status, Task


@pytest.mark.parametrize(
    "blueapi_call_statuses, expected_task_status",
    [
        ([CallStatus.WAITING], Status.QUEUED),
        ([CallStatus.CLAIMED], Status.IN_PROGRESS),
        ([CallStatus.IN_PROGRESS], Status.IN_PROGRESS),
        ([CallStatus.SUCCESS], Status.COMPLETE),
        ([CallStatus.ERROR], Status.COMPLETE),
        ([CallStatus.WAITING, CallStatus.WAITING], Status.QUEUED),
        ([CallStatus.WAITING, CallStatus.CLAIMED], Status.IN_PROGRESS),
        ([CallStatus.WAITING, CallStatus.IN_PROGRESS], Status.IN_PROGRESS),
        ([CallStatus.WAITING, CallStatus.SUCCESS], Status.IN_PROGRESS),
        ([CallStatus.WAITING, CallStatus.ERROR], Status.IN_PROGRESS),
        ([CallStatus.CLAIMED, CallStatus.SUCCESS], Status.IN_PROGRESS),
        ([CallStatus.CLAIMED, CallStatus.ERROR], Status.IN_PROGRESS),
        ([CallStatus.IN_PROGRESS, CallStatus.SUCCESS], Status.IN_PROGRESS),
        ([CallStatus.IN_PROGRESS, CallStatus.ERROR], Status.IN_PROGRESS),
        ([CallStatus.SUCCESS, CallStatus.ERROR], Status.COMPLETE),
    ],
)
def test_task_status_derived_correctly_from_call_statuses(
    blueapi_call_statuses: list[CallStatus], expected_task_status: Status
):
    task = Task(
        blueapi_calls=[
            BlueapiCall(
                status=status, task_request=TaskRequest(name="", instrument_session="")
            )
            for status in blueapi_call_statuses
        ],
        experiment_definition=ExperimentDefinition(
            plan_name="", sample_id="", instrument_session=""
        ),
    )
    assert task.status == expected_task_status

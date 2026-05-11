import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall, Status
from daq_queuing_service.task import ExperimentDefinition, Task, TaskStatus


@pytest.mark.parametrize(
    "blueapi_call_statuses, expected_task_status",
    [
        ([Status.WAITING], TaskStatus.QUEUED),
        ([Status.CLAIMED], TaskStatus.IN_PROGRESS),
        ([Status.IN_PROGRESS], TaskStatus.IN_PROGRESS),
        ([Status.SUCCESS], TaskStatus.COMPLETE),
        ([Status.ERROR], TaskStatus.COMPLETE),
        ([Status.WAITING, Status.WAITING], TaskStatus.QUEUED),
        ([Status.WAITING, Status.CLAIMED], TaskStatus.IN_PROGRESS),
        ([Status.WAITING, Status.IN_PROGRESS], TaskStatus.IN_PROGRESS),
        ([Status.WAITING, Status.SUCCESS], TaskStatus.IN_PROGRESS),
        ([Status.WAITING, Status.ERROR], TaskStatus.IN_PROGRESS),
        ([Status.CLAIMED, Status.SUCCESS], TaskStatus.IN_PROGRESS),
        ([Status.CLAIMED, Status.ERROR], TaskStatus.IN_PROGRESS),
        ([Status.IN_PROGRESS, Status.SUCCESS], TaskStatus.IN_PROGRESS),
        ([Status.IN_PROGRESS, Status.ERROR], TaskStatus.IN_PROGRESS),
        ([Status.SUCCESS, Status.ERROR], TaskStatus.COMPLETE),
    ],
)
def test_task_status_derived_correctly_from_call_statuses(
    blueapi_call_statuses: list[Status], expected_task_status: TaskStatus
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

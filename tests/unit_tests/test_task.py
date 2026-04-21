import pytest
from blueapi.worker.event import TaskResult

from daq_queuing_service.task import ExperimentDefinition, Status, Task


@pytest.mark.parametrize(
    "old_status, new_status",
    [
        [Status.WAITING, Status.WAITING],
        [Status.WAITING, Status.IN_PROGRESS],
        [Status.WAITING, Status.SUCCESS],
        [Status.WAITING, Status.ERROR],
        [Status.CLAIMED, Status.CLAIMED],
        [Status.CLAIMED, Status.SUCCESS],
        [Status.CLAIMED, Status.CANCELLED],
        [Status.IN_PROGRESS, Status.WAITING],
        [Status.IN_PROGRESS, Status.CLAIMED],
        [Status.IN_PROGRESS, Status.IN_PROGRESS],
        [Status.IN_PROGRESS, Status.CANCELLED],
        [Status.SUCCESS, Status.WAITING],
        [Status.SUCCESS, Status.CLAIMED],
        [Status.SUCCESS, Status.IN_PROGRESS],
        [Status.SUCCESS, Status.SUCCESS],
        [Status.SUCCESS, Status.ERROR],
        [Status.SUCCESS, Status.CANCELLED],
        [Status.ERROR, Status.WAITING],
        [Status.ERROR, Status.CLAIMED],
        [Status.ERROR, Status.IN_PROGRESS],
        [Status.ERROR, Status.SUCCESS],
        [Status.ERROR, Status.ERROR],
        [Status.ERROR, Status.CANCELLED],
        [Status.CANCELLED, Status.WAITING],
        [Status.CANCELLED, Status.CLAIMED],
        [Status.CANCELLED, Status.IN_PROGRESS],
        [Status.CANCELLED, Status.SUCCESS],
        [Status.CANCELLED, Status.ERROR],
        [Status.CANCELLED, Status.CANCELLED],
    ],
)
def test__update_status_raises_error_when_transitioned_to_wrong_status(
    old_status: Status, new_status: Status
):
    task = Task(
        experiment_definition=ExperimentDefinition(
            plan_name="test", sample_id="sample", instrument_session=""
        ),
        status=old_status,
    )
    with pytest.raises(ValueError):
        task._update_status(new_status)


@pytest.mark.parametrize(
    "old_status, new_status",
    [
        [Status.WAITING, Status.CLAIMED],
        [Status.WAITING, Status.CANCELLED],
        [Status.CLAIMED, Status.WAITING],
        [Status.CLAIMED, Status.IN_PROGRESS],
        [Status.CLAIMED, Status.ERROR],
        [Status.IN_PROGRESS, Status.SUCCESS],
        [Status.IN_PROGRESS, Status.ERROR],
    ],
)
def test__update_status_changes_status_when_correct_new_status_given(
    old_status: Status, new_status: Status
):
    task = Task(
        experiment_definition=ExperimentDefinition(
            plan_name="test", sample_id="sample", instrument_session=""
        ),
        status=old_status,
    )
    task._update_status(new_status)
    assert task.status == new_status


def test_wait_updates_status_to_waiting():
    task = Task(
        experiment_definition=ExperimentDefinition(
            plan_name="test", sample_id="sample", instrument_session=""
        ),
        status=Status.CLAIMED,
    )
    task.wait()
    assert task.status == Status.WAITING


def test_claim_updates_status_to_claimed():
    task = Task(
        experiment_definition=ExperimentDefinition(
            plan_name="test", sample_id="sample", instrument_session=""
        ),
        status=Status.WAITING,
    )
    task.claim()
    assert task.status == Status.CLAIMED


def test_put_in_progress_updates_status_to_in_progress_and_adds_fields():
    task = Task(
        experiment_definition=ExperimentDefinition(
            plan_name="test", sample_id="sample", instrument_session=""
        ),
        status=Status.CLAIMED,
    )
    task.blueapi_id = "blueapi_id"
    task.put_in_progress()
    assert task.status == Status.IN_PROGRESS
    assert task.time_started is not None
    assert task.blueapi_id == "blueapi_id"


def test_succeed_updates_status_to_success_and_adds_time_completed():
    task = Task(
        experiment_definition=ExperimentDefinition(
            plan_name="test", sample_id="sample", instrument_session=""
        ),
        status=Status.IN_PROGRESS,
    )
    task.succeed(TaskResult(result=None, type="NoneType"))
    assert task.status == Status.SUCCESS
    assert task.time_completed is not None


def test_fail_updates_status_to_error_and_adds_time_completed_and_errors():
    task = Task(
        experiment_definition=ExperimentDefinition(
            plan_name="test", sample_id="sample", instrument_session=""
        ),
        status=Status.IN_PROGRESS,
    )
    task.fail(["errors", "more_errors"])
    assert task.status == Status.ERROR
    assert task.time_completed is not None
    assert task.errors == ["errors", "more_errors"]


def test_cancel_updates_status_to_cancelled():
    task = Task(
        experiment_definition=ExperimentDefinition(
            plan_name="test", sample_id="sample", instrument_session=""
        ),
        status=Status.WAITING,
    )
    task.cancel()
    assert task.status == Status.CANCELLED

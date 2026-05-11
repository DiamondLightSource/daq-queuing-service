import pytest
from blueapi.service.model import TaskRequest
from blueapi.worker.event import TaskResult

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall, Status


@pytest.mark.parametrize(
    "old_status, new_status",
    [
        [Status.WAITING, Status.WAITING],
        [Status.WAITING, Status.IN_PROGRESS],
        [Status.WAITING, Status.SUCCESS],
        [Status.WAITING, Status.ERROR],
        [Status.CLAIMED, Status.CLAIMED],
        [Status.CLAIMED, Status.SUCCESS],
        [Status.IN_PROGRESS, Status.WAITING],
        [Status.IN_PROGRESS, Status.CLAIMED],
        [Status.IN_PROGRESS, Status.IN_PROGRESS],
        [Status.SUCCESS, Status.WAITING],
        [Status.SUCCESS, Status.CLAIMED],
        [Status.SUCCESS, Status.IN_PROGRESS],
        [Status.SUCCESS, Status.SUCCESS],
        [Status.SUCCESS, Status.ERROR],
        [Status.ERROR, Status.WAITING],
        [Status.ERROR, Status.CLAIMED],
        [Status.ERROR, Status.IN_PROGRESS],
        [Status.ERROR, Status.SUCCESS],
        [Status.ERROR, Status.ERROR],
    ],
)
def test__update_status_raises_error_when_transitioned_to_wrong_status(
    old_status: Status, new_status: Status
):
    call = BlueapiCall(
        status=old_status,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    with pytest.raises(ValueError):
        call._update_status(new_status)


@pytest.mark.parametrize(
    "old_status, new_status",
    [
        [Status.WAITING, Status.CLAIMED],
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
    call = BlueapiCall(
        status=old_status,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call._update_status(new_status)
    assert call.status == new_status


def test_wait_updates_status_to_waiting():
    call = BlueapiCall(
        status=Status.CLAIMED,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.wait()
    assert call.status == Status.WAITING


def test_claim_updates_status_to_claimed():
    call = BlueapiCall(
        status=Status.WAITING,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.claim()
    assert call.status == Status.CLAIMED


def test_put_in_progress_updates_status_to_in_progress_and_adds_fields():
    call = BlueapiCall(
        status=Status.CLAIMED,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.blueapi_id = "blueapi_id"
    call.put_in_progress()
    assert call.status == Status.IN_PROGRESS
    assert call.time_started is not None
    assert call.blueapi_id == "blueapi_id"


def test_succeed_updates_status_to_success_and_adds_time_completed():
    call = BlueapiCall(
        status=Status.IN_PROGRESS,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.succeed(TaskResult(result=None, type="NoneType"))
    assert call.status == Status.SUCCESS
    assert call.time_completed is not None


def test_fail_updates_status_to_error_and_adds_time_completed_and_errors():
    call = BlueapiCall(
        status=Status.IN_PROGRESS,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.fail(["errors", "more_errors"])
    assert call.status == Status.ERROR
    assert call.time_completed is not None
    assert call.errors == ["errors", "more_errors"]

import pytest
from blueapi.service.model import TaskRequest
from blueapi.worker.event import TaskResult

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall, CallStatus


@pytest.mark.parametrize(
    "old_status, new_status",
    [
        [CallStatus.WAITING, CallStatus.WAITING],
        [CallStatus.WAITING, CallStatus.IN_PROGRESS],
        [CallStatus.WAITING, CallStatus.SUCCESS],
        [CallStatus.WAITING, CallStatus.ERROR],
        [CallStatus.CLAIMED, CallStatus.CLAIMED],
        [CallStatus.CLAIMED, CallStatus.SUCCESS],
        [CallStatus.IN_PROGRESS, CallStatus.WAITING],
        [CallStatus.IN_PROGRESS, CallStatus.CLAIMED],
        [CallStatus.IN_PROGRESS, CallStatus.IN_PROGRESS],
        [CallStatus.SUCCESS, CallStatus.WAITING],
        [CallStatus.SUCCESS, CallStatus.CLAIMED],
        [CallStatus.SUCCESS, CallStatus.IN_PROGRESS],
        [CallStatus.SUCCESS, CallStatus.SUCCESS],
        [CallStatus.SUCCESS, CallStatus.ERROR],
        [CallStatus.ERROR, CallStatus.WAITING],
        [CallStatus.ERROR, CallStatus.CLAIMED],
        [CallStatus.ERROR, CallStatus.IN_PROGRESS],
        [CallStatus.ERROR, CallStatus.SUCCESS],
        [CallStatus.ERROR, CallStatus.ERROR],
    ],
)
def test__update_status_raises_error_when_transitioned_to_wrong_status(
    old_status: CallStatus, new_status: CallStatus
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
        [CallStatus.WAITING, CallStatus.CLAIMED],
        [CallStatus.CLAIMED, CallStatus.WAITING],
        [CallStatus.CLAIMED, CallStatus.IN_PROGRESS],
        [CallStatus.CLAIMED, CallStatus.ERROR],
        [CallStatus.IN_PROGRESS, CallStatus.SUCCESS],
        [CallStatus.IN_PROGRESS, CallStatus.ERROR],
    ],
)
def test__update_status_changes_status_when_correct_new_status_given(
    old_status: CallStatus, new_status: CallStatus
):
    call = BlueapiCall(
        status=old_status,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call._update_status(new_status)
    assert call.status == new_status


def test_wait_updates_status_to_waiting():
    call = BlueapiCall(
        status=CallStatus.CLAIMED,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.wait()
    assert call.status == CallStatus.WAITING


def test_claim_updates_status_to_claimed():
    call = BlueapiCall(
        status=CallStatus.WAITING,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.claim()
    assert call.status == CallStatus.CLAIMED


def test_put_in_progress_updates_status_to_in_progress_and_adds_fields():
    call = BlueapiCall(
        status=CallStatus.CLAIMED,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.blueapi_id = "blueapi_id"
    call.put_in_progress()
    assert call.status == CallStatus.IN_PROGRESS
    assert call.time_started is not None
    assert call.blueapi_id == "blueapi_id"


def test_succeed_updates_status_to_success_and_adds_time_completed():
    call = BlueapiCall(
        status=CallStatus.IN_PROGRESS,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.succeed(TaskResult(result=None, type="NoneType"))
    assert call.status == CallStatus.SUCCESS
    assert call.time_completed is not None


def test_fail_updates_status_to_error_and_adds_time_completed_and_errors():
    call = BlueapiCall(
        status=CallStatus.IN_PROGRESS,
        task_request=TaskRequest(name="", params={}, instrument_session=""),
    )
    call.fail(["errors", "more_errors"])
    assert call.status == CallStatus.ERROR
    assert call.time_completed is not None
    assert call.errors == ["errors", "more_errors"]

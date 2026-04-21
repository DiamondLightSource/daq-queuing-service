from unittest.mock import MagicMock

import pytest
from blueapi.client import BlueapiClient
from blueapi.client.rest import (
    BlueskyRemoteControlError,
    InvalidParametersError,
    ServiceUnavailableError,
    UnknownPlanError,
)
from blueapi.service.model import TaskRequest
from blueapi.worker import WorkerState
from blueapi.worker.event import TaskResult, TaskStatus

from daq_queuing_service.blueapi_adapter import BlueapiClientAdapter


async def test_blueapi_adapter_get_state_returns_blueapi_response_with_state():
    blueapi_client = BlueapiClient(rest=MagicMock())
    blueapi_client.get_state = MagicMock(return_value=WorkerState.PANICKED)
    blueapi_adapter = BlueapiClientAdapter(client=blueapi_client)
    result = await blueapi_adapter.get_state()
    assert result.value == WorkerState.PANICKED
    assert result.error is None


async def test_blueapi_adapter_get_state_returns_error_if_raised():
    blueapi_client = BlueapiClient(rest=MagicMock())
    blueapi_client.get_state = MagicMock(side_effect=ServiceUnavailableError)
    blueapi_adapter = BlueapiClientAdapter(client=blueapi_client)
    result = await blueapi_adapter.get_state()
    assert result.value is None
    assert isinstance(result.error, ServiceUnavailableError)


async def test_blueapi_adapter_run_task_returns_blueapi_response_with_task_response():
    blueapi_client = BlueapiClient(rest=MagicMock())
    blueapi_client.run_task = MagicMock(
        return_value=TaskStatus(
            task_id="blueapi_task_id",
            result=TaskResult(outcome="success", result=None, type="NoneType"),
            task_complete=True,
            task_failed=False,
        )
    )
    blueapi_adapter = BlueapiClientAdapter(client=blueapi_client)

    result = await blueapi_adapter.run_task(
        TaskRequest(name="sleep", params={"time": 10}, instrument_session="abc")
    )
    assert result.value == TaskStatus(
        task_id="blueapi_task_id",
        result=TaskResult(outcome="success", result=None, type="NoneType"),
        task_complete=True,
        task_failed=False,
    )
    assert result.error is None


@pytest.mark.parametrize(
    "error",
    [
        BlueskyRemoteControlError(),
        InvalidParametersError(errors=[]),
        UnknownPlanError(),
        ServiceUnavailableError(),
    ],
)
async def test_blueapi_adapter_run_task_returns_error_if_raised(error: Exception):
    blueapi_client = BlueapiClient(rest=MagicMock())
    blueapi_client.run_task = MagicMock(side_effect=error)
    blueapi_adapter = BlueapiClientAdapter(client=blueapi_client)

    result = await blueapi_adapter.run_task(
        TaskRequest(name="sleep", params={"time": 10}, instrument_session="abc")
    )
    assert result.value is None
    assert isinstance(result.error, type(error))

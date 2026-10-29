from unittest.mock import ANY, MagicMock

from httpx2 import Response

from daq_queuing_service.external_interaction.blueapi.token_source import UDCTokenSource
from daq_queuing_service.external_interaction.runtime_estimator.client import (
    RuntimeEstimatorClient,
)
from daq_queuing_service.task_queue.task import Task


def test_estimate_runtime_makes_expected_request(tasks: list[Task]):
    token_source = UDCTokenSource()
    client = RuntimeEstimatorClient("", token_source)
    client._pool.request = MagicMock(return_value=Response(status_code=200, json=500))
    experiments = [task.experiment for task in tasks]
    result = client.estimate_runtime(experiments, "ixx")

    client._pool.request.assert_called_once_with(
        "POST",
        "/estimate-runtime/ixx",
        json=[experiment.model_dump(mode="json") for experiment in experiments],
        params=None,
        auth=ANY,  # type: ignore
    )
    assert result == 500


def test_estimate_runtime_per_task_makes_expected_request(tasks: list[Task]):
    token_source = UDCTokenSource()
    client = RuntimeEstimatorClient("", token_source)
    client._pool.request = MagicMock(
        return_value=Response(status_code=200, json=[100, 100, 100, 100, 100])
    )
    experiments = [task.experiment for task in tasks]
    result = client.estimate_runtime_per_task(experiments, "ixx")

    client._pool.request.assert_called_once_with(
        "POST",
        "/estimate-runtime-per-task/ixx",
        json=[experiment.model_dump(mode="json") for experiment in experiments],
        params=None,
        auth=ANY,  # type: ignore
    )
    assert result == [100, 100, 100, 100, 100]

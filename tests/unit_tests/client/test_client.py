from typing import Any
from unittest.mock import MagicMock, _Call, call, patch

import pytest
from requests import Response

from daq_queuing_service.api.api import protected_routes, public_routes
from daq_queuing_service.client.client import QueueClient
from daq_queuing_service.task_queue.queue_utils import QueueError


def test_all_endpoints_have_a_client_function():
    exceptions = ["read_root", "stream_events"]

    protected_router = protected_routes(
        MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    public_router = public_routes(MagicMock())

    endpoints: list[str] = [
        route.endpoint.__name__  # type: ignore
        for route in protected_router.routes + public_router.routes
    ]

    not_in_client = [
        endpoint
        for endpoint in endpoints
        if endpoint not in list(QueueClient.__dict__.keys()) + exceptions
    ]

    assert not not_in_client, (
        f"Found endpoints not covered by a client function: {not_in_client}"
    )


@pytest.mark.parametrize(
    "url, kwargs, expected_request_call",
    [
        (
            "https://google.com/",
            {"suffix": "/queue"},
            call("GET", "https://google.com/queue", json=None, params=None),
        ),
        (
            "http://google.com",
            {
                "suffix": "/queue",
                "method": "POST",
                "data": {"test": "data"},
                "params": {"test": "params"},
            },
            call(
                "POST",
                "http://google.com/queue",
                json={"test": "data"},
                params={"test": "params"},
            ),
        ),
    ],
)
def test__request_makes_request_with_expected_arguments(
    url: str, kwargs: dict[str, Any], expected_request_call: _Call
):
    client = QueueClient(url=url)

    with patch(
        "daq_queuing_service.client.client.requests.Session.request"
    ) as mock_request:
        client._request(**kwargs)

    mock_request.assert_called_once()
    mock_request.assert_has_calls([expected_request_call])


def test__request_expect_error_raises_error_with_error_content_if_validation_error():
    client = QueueClient(url="http://queue.com")
    mock_response = Response()
    mock_response.json = MagicMock(
        return_value={"error": "some_error", "message": "some_error_message"}
    )
    with patch(
        "daq_queuing_service.client.client.QueueClient._request",
        return_value=mock_response,
    ):
        with pytest.raises(QueueError) as exc:
            client.get_task_by_id("1")

    assert str(exc.value) == "{'error': 'some_error', 'message': 'some_error_message'}"

from unittest.mock import MagicMock

from daq_queuing_service.api.api import create_api_router
from daq_queuing_service.client.client import QueueClient


def test_all_endpoints_have_a_client_function():
    exceptions = ["read_root", "stream_events"]

    router = create_api_router(MagicMock(), MagicMock(), MagicMock())
    endpoints: list[str] = [route.endpoint.__name__ for route in router.routes]  # type: ignore

    not_in_client = [
        endpoint
        for endpoint in endpoints
        if endpoint not in list(QueueClient.__dict__.keys()) + exceptions
    ]

    assert not not_in_client, (
        f"Found endpoints not covered by a client function: {not_in_client}"
    )

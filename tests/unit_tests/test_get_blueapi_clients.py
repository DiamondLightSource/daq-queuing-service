from unittest.mock import MagicMock, patch

from blueapi.config import ApplicationConfig, RestConfig, StompConfig
from pydantic import HttpUrl

from daq_queuing_service.blueapi_interaction.clients import get_blueapi_clients


@patch("daq_queuing_service.blueapi_interaction.clients.UDCTokenRetriever")
@patch("daq_queuing_service.blueapi_interaction.clients.BlueapiClient")
@patch("daq_queuing_service.blueapi_interaction.clients.BlueapiRestClient")
def test_get_blueapi_clients_constructs_clients_with_expected_args_and_returns_clients(
    mock_rest_client: MagicMock,
    mock_blueapi_client: MagicMock,
    mock_token_retriever: MagicMock,
):
    rest_config = RestConfig(url=HttpUrl("http://test_url.com"))
    rest_client, blueapi_client = get_blueapi_clients(
        ApplicationConfig(api=rest_config)
    )

    mock_rest_client.assert_called_once_with(
        config=rest_config, session_manager=mock_token_retriever.return_value
    )
    mock_blueapi_client.assert_called_once_with(rest_client)

    assert rest_client is mock_rest_client.return_value
    assert blueapi_client is mock_blueapi_client.return_value


@patch("daq_queuing_service.blueapi_interaction.clients.EventBusClient")
@patch("daq_queuing_service.blueapi_interaction.clients.BlueapiClient")
@patch("daq_queuing_service.blueapi_interaction.clients.BlueapiRestClient")
def test_get_blueapi_clients_constructs_blueapi_client_with_stomp_if_enabled_in_config(
    mock_rest_client: MagicMock,
    mock_blueapi_client: MagicMock,
    mock_event_bus_client: MagicMock,
):
    rest_config = RestConfig(url=HttpUrl("http://test_url.com"))
    rest_client, _ = get_blueapi_clients(
        ApplicationConfig(api=rest_config, stomp=StompConfig(enabled=True))
    )

    mock_blueapi_client.assert_called_once_with(
        rest_client, mock_event_bus_client.return_value
    )

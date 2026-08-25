from unittest.mock import MagicMock, patch

from blueapi.config import RestConfig, StompConfig
from pydantic import HttpUrl

from daq_queuing_service.app._config import BlueapiConfig
from daq_queuing_service.blueapi_interaction.get_client import get_blueapi_client


@patch("daq_queuing_service.blueapi_interaction.get_client.UDCTokenSource")
@patch("daq_queuing_service.blueapi_interaction.get_client.BlueapiClient")
@patch("daq_queuing_service.blueapi_interaction.get_client.BlueapiRestClient")
def test_get_blueapi_clients_constructs_clients_with_expected_args_and_returns_clients(
    mock_rest_client: MagicMock,
    mock_blueapi_client: MagicMock,
    mock_token_retriever: MagicMock,
):
    rest_config = RestConfig(url=HttpUrl("http://test_url.com"))
    blueapi_client = get_blueapi_client(BlueapiConfig(api=rest_config))

    mock_rest_client.assert_called_once_with(
        config=rest_config, session_manager=mock_token_retriever.return_value
    )
    mock_blueapi_client.assert_called_once_with(mock_rest_client.return_value)

    assert blueapi_client is mock_blueapi_client.return_value


@patch("daq_queuing_service.blueapi_interaction.get_client.EventBusClient")
@patch("daq_queuing_service.blueapi_interaction.get_client.BlueapiClient")
@patch("daq_queuing_service.blueapi_interaction.get_client.BlueapiRestClient")
def test_get_blueapi_clients_constructs_blueapi_client_with_stomp_if_enabled_in_config(
    mock_rest_client: MagicMock,
    mock_blueapi_client: MagicMock,
    mock_event_bus_client: MagicMock,
):
    rest_config = RestConfig(url=HttpUrl("http://test_url.com"))
    _ = get_blueapi_client(
        BlueapiConfig(api=rest_config, stomp=StompConfig(enabled=True))
    )

    mock_blueapi_client.assert_called_once_with(
        mock_rest_client.return_value, mock_event_bus_client.return_value
    )

import logging
from unittest.mock import MagicMock, patch

from pydantic import SecretStr
from pytest import LogCaptureFixture

from daq_queuing_service.external_interaction.tiled.tiled import (
    TILED_URL,
    get_tiled_client,
)


def test_get_tiled_client_instantiates_client_from_uri():
    with patch(
        "daq_queuing_service.external_interaction.tiled.tiled.from_uri"
    ) as mock_from_uri:
        tiled_client = get_tiled_client()

    mock_from_uri.assert_called_once_with(TILED_URL, auth=None)
    assert tiled_client is mock_from_uri.return_value


@patch("daq_queuing_service.external_interaction.tiled.tiled.from_uri")
def test_get_tiled_client_warns_if_no_client_id_or_secret_found(
    mock_from_uri: MagicMock, caplog: LogCaptureFixture
):
    with caplog.at_level(logging.WARNING):
        get_tiled_client()

    assert "No UDC client ID found." in caplog.text
    assert "No UDC secret found." in caplog.text
    assert "Tiled auth will not be used."


@patch("daq_queuing_service.external_interaction.tiled.tiled.TiledAuth")
@patch("daq_queuing_service.external_interaction.tiled.tiled.ServiceAccount")
@patch("daq_queuing_service.external_interaction.tiled.tiled.from_uri")
@patch("daq_queuing_service.external_interaction.tiled.tiled.os.environ.get")
def test_get_tiled_client_adds_tiled_auth_with_client_id_and_secret_if_found(
    mock_env_var_get: MagicMock,
    mock_from_uri: MagicMock,
    mock_service_account_cls: MagicMock,
    mock_tiled_auth_cls: MagicMock,
):
    values = iter(["client_id", "secret"])

    def get_next_env_variable(*_, **__):  # type:ignore
        return next(values)

    mock_env_var_get.side_effect = get_next_env_variable

    get_tiled_client()

    mock_service_account_cls.assert_called_once_with(
        client_id="client_id",
        client_secret=SecretStr("secret"),
        token_url="https://identity.diamond.ac.uk/realms/dls/protocol/openid-connect/token",
    )
    mock_tiled_auth_cls.assert_called_once_with(
        tiled_auth=mock_service_account_cls.return_value
    )
    mock_from_uri.assert_called_once_with(
        TILED_URL,
        auth=mock_tiled_auth_cls.return_value,
    )

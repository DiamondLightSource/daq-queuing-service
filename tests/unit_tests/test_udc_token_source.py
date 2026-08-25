from unittest.mock import MagicMock, patch

import pytest
from pytest import MonkeyPatch
from requests import Response

from daq_queuing_service.blueapi_interaction.token_source import UDCTokenSource


@pytest.fixture(autouse=True)
def set_secret_env_vars(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("UDC_SECRET", "secret")
    monkeypatch.setenv("UDC_CLIENT_ID", "ixxudc")


@patch("daq_queuing_service.blueapi_interaction.token_source.requests.post")
def test_get_valid_access_token_makes_expected_request_and_returns_result(
    mock_post: MagicMock,
):
    mock_post.return_value = Response()
    mock_post.return_value.json = MagicMock(
        return_value={"access_token": "valid_token"}
    )
    mock_post.return_value.status_code = 200
    token_retriever = UDCTokenSource()

    assert token_retriever.get_valid_access_token() == "valid_token"

    mock_post.assert_called_once_with(
        "https://identity.diamond.ac.uk/realms/dls/protocol/openid-connect/token",
        data={
            "client_id": "ixxudc",
            "client_secret": "secret",
            "grant_type": "client_credentials",
        },
    )


@pytest.mark.parametrize("variable_name", ("UDC_SECRET", "UDC_CLIENT_ID"))
def test_get_valid_access_token_if_env_vars_not_found_then_returns_empty_string(
    variable_name: str, monkeypatch: MonkeyPatch
):
    monkeypatch.delenv(variable_name)
    token_retriever = UDCTokenSource()
    assert token_retriever.get_valid_access_token() == ""

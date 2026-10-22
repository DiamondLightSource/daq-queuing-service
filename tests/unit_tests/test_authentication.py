from unittest.mock import MagicMock, patch

import pytest
from blueapi.config import OIDCConfig
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from jwt import DecodeError, ExpiredSignatureError

from daq_queuing_service.app.authentication import (
    User,
    build_access_token_check,
    build_get_current_user,
    unchecked_bearer_token,
)


@pytest.fixture(autouse=True)
def jwk_client():
    with patch(
        "daq_queuing_service.app.authentication.jwt.PyJWKClient"
    ) as mock_client_class:
        yield mock_client_class.return_value


def test_unchecked_bearer_token_returns_credentials():
    expected = "fake credentials"
    result = unchecked_bearer_token(
        HTTPAuthorizationCredentials(scheme="", credentials=expected)
    )
    assert result == expected


def test_validate_bearer_token_gets_signing_key_from_jwt_and_decodes_token_with_it(
    oidc_config: OIDCConfig, jwk_client: MagicMock
):
    token = "fake_token"
    validate_bearer_token = build_access_token_check(oidc_config)
    with patch("daq_queuing_service.app.authentication.jwt.decode") as mock_decode:
        validate_bearer_token(token)

    jwk_client.get_signing_key_from_jwt.assert_called_once_with(token)
    signing_key = jwk_client.get_signing_key_from_jwt.return_value.key
    mock_decode.assert_called_once_with(
        token,
        signing_key,
        algorithms=oidc_config.id_token_signing_alg_values_supported,
        verify=True,
        audience=oidc_config.client_audience,
        issuer=oidc_config.issuer,
    )


def test_validate_bearer_token_raises_appropriate_http_exception_if_no_token_provided(
    oidc_config: OIDCConfig,
):
    validate_bearer_token = build_access_token_check(oidc_config)
    with pytest.raises(HTTPException) as exc:
        validate_bearer_token(None)

    assert exc.value.status_code == 401
    assert exc.value.detail == "Not authenticated"


def test_validate_bearer_token_raises_appropriate_http_exception_if_decode_error(
    oidc_config: OIDCConfig, jwk_client: MagicMock
):
    jwk_client.get_signing_key_from_jwt.side_effect = DecodeError
    validate_bearer_token = build_access_token_check(oidc_config)
    with pytest.raises(HTTPException) as exc:
        validate_bearer_token("token")

    assert exc.value.status_code == 401
    assert exc.value.detail == "Cannot decode token"


def test_validate_bearer_token_raises_appropriate_http_exception_if_expired_error(
    oidc_config: OIDCConfig, jwk_client: MagicMock
):
    jwk_client.get_signing_key_from_jwt.side_effect = ExpiredSignatureError
    validate_bearer_token = build_access_token_check(oidc_config)
    with pytest.raises(HTTPException) as exc:
        validate_bearer_token("token")

    assert exc.value.status_code == 401
    assert exc.value.detail == "Token expired"


def test_get_current_user_builds_user_from_validated_token_and_adds_to_request_state():
    def validate_user():
        return {
            "fedid": "abc12355",
            "email": "joe.blogs@diamond.ac.uk",
            "name": "Joe Blogs",
        }

    expected_user = User(
        fedid="abc12355", email="joe.blogs@diamond.ac.uk", name="Joe Blogs"
    )

    get_current_user = build_get_current_user(MagicMock())
    request = MagicMock()
    user = get_current_user(request, validate_user())

    assert user == expected_user
    assert request.state.user == expected_user


def test_get_current_user_raises_error_if_token_cannot_validate_to_user_class():
    def validate_user():
        return {"no fedid": "abc12355"}

    get_current_user = build_get_current_user(MagicMock())
    request = MagicMock()
    with pytest.raises(HTTPException) as exc:
        get_current_user(request, validate_user())

    assert exc.value.status_code == 401
    assert exc.value.detail == "Invalid token claims"

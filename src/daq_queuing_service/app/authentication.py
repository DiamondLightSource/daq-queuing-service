from collections.abc import Callable
from typing import Annotated, Any

import jwt
from blueapi.config import OIDCConfig
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import DecodeError
from pydantic import BaseModel, ValidationError
from starlette.status import HTTP_401_UNAUTHORIZED

from daq_queuing_service.worker.worker import LOGGER


class User(BaseModel):
    fedid: str
    email: str | None = None
    username: str | None = None


# Some of the following code was copied from blueapi
# See https://github.com/DiamondLightSource/blueapi/blob/2108ee0c89b4399d961106f7f23082a58d48a564/src/blueapi/service/authentication.py#L281-L340

bearer_scheme = HTTPBearer(auto_error=False)


def unchecked_bearer_token(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> str | None:
    if credentials is None:
        return None
    return credentials.credentials


UncheckedBearerToken = Annotated[str | None, Depends(unchecked_bearer_token)]


def build_access_token_check(
    config: OIDCConfig,
) -> Callable[[UncheckedBearerToken], dict[str, Any]]:
    """
    Create a function to validate the bearer token of requests

    The returned function should be used via fastAPI's 'Depends' mechanism to
    ensure users are authenticated
    """
    jwkclient = jwt.PyJWKClient(config.jwks_uri)

    def validate_bearer_token(token: UncheckedBearerToken):
        """Check that a bearer token is valid and inject into request state"""
        if not token:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
                headers={"WWW-Authenticate": "Bearer"},
            )

        try:
            signing_key = jwkclient.get_signing_key_from_jwt(token)
        except DecodeError as e:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Cannot decode token",
            ) from e
        decoded: dict[str, Any] = jwt.decode(
            token,
            signing_key.key,
            algorithms=config.id_token_signing_alg_values_supported,
            verify=True,
            audience=config.client_audience,
            issuer=config.issuer,
        )
        LOGGER.debug(f"Decoded valid token: {decoded}")
        return decoded

    return validate_bearer_token


def build_get_current_user(
    validate_token: Callable[..., dict[str, Any]],
) -> Callable[[Request, dict[str, Any]], User]:
    def get_current_user(
        request: Request,
        decoded: Annotated[dict[str, Any], Depends(validate_token)],
    ) -> User:
        try:
            user = User.model_validate(decoded)
        except ValidationError as e:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Invalid token claims",
            ) from e
        request.state.user = user
        return user

    return get_current_user

import os

import requests

from daq_queuing_service.log import LOGGER


class UDCTokenRetriever:
    """Implements `get_valid_access_token` to get a token using a sealed secret."""

    def __init__(
        self,
        secret_variable_name: str = "UDC_SECRET",
        client_id_variable_name: str = "UDC_CLIENT_ID",
    ):
        self._secret_variable_name = secret_variable_name
        self._client_id_variable_name = client_id_variable_name

    def get_valid_access_token(self) -> str:
        token_url = (
            "https://identity.diamond.ac.uk/realms/dls/protocol/openid-connect/token"
        )

        client_id = os.environ.get(self._client_id_variable_name)
        client_secret = os.environ.get(self._secret_variable_name)

        if not client_secret:
            LOGGER.debug("No UDC secret found")
            return ""
        if not client_id:
            LOGGER.debug("No UDC client ID found")
            return ""

        LOGGER.debug("Found UDC secret")

        response = requests.post(
            token_url,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        LOGGER.debug("Returning token")
        return token

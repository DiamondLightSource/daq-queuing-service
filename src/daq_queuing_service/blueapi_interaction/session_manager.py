import os

import requests
from blueapi.service.authentication import SessionManager

from daq_queuing_service.log import LOGGER


class UDCSessionManager(SessionManager):
    """Session manager for a UDC session. Overrides `get_valid_access_token` to get
    token using a sealed secret instead of from a file.
    """

    def get_valid_access_token(self) -> str:
        token_url = (
            "https://identity.diamond.ac.uk/realms/dls/protocol/openid-connect/token"
        )

        # Need to get this from secret
        client_id = "i15-1-udc"
        client_secret = os.environ.get("UDC_SECRET")

        if not (client_id and client_secret):
            LOGGER.debug("No UDC secret found")
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

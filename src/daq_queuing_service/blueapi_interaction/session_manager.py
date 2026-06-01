import os

import requests
from blueapi.client.rest import SessionManager


class UDCSessionManager(SessionManager):
    def get_valid_access_token(self) -> str:
        token_url = (
            "https://identity.diamond.ac.uk/realms/dls/protocol/openid-connect/token"
        )

        client_id = "i15-1-udc"
        client_secret = os.environ["UDC_SECRET"]

        if not (client_id and client_secret):
            return ""

        response = requests.post(
            token_url,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
        )
        response.raise_for_status()
        return response.json().get("access_token")

import os

from blueapi.config import ServiceAccount
from blueapi.service.authentication import TiledAuth
from pydantic import SecretStr
from tiled.client import from_uri
from tiled.client.container import Container as TiledContainer

from daq_queuing_service.log import LOGGER

# Ignoring the following rule as the tiled client is poorly typed
# pyright: reportUnknownVariableType=false

TILED_URL = "https://tiled.diamond.ac.uk"


def get_tiled_client(
    secret_variable_name: str = "UDC_SECRET",
    client_id_variable_name: str = "UDC_CLIENT_ID",
) -> TiledContainer:

    client_id = os.environ.get(client_id_variable_name, "")
    client_secret = SecretStr(os.environ.get(secret_variable_name, ""))

    if not client_id:
        LOGGER.warning("No UDC client ID found.")

    if not client_secret:
        LOGGER.warning("No UDC secret found.")

    if client_secret and client_id:
        tiled_auth = TiledAuth(
            tiled_auth=ServiceAccount(
                client_id=client_id,
                client_secret=client_secret,
                token_url="https://identity.diamond.ac.uk/realms/dls/protocol/openid-connect/token",
            )
        )
    else:
        LOGGER.warning("Tiled auth will not be used.")
        tiled_auth = None

    return from_uri(TILED_URL, auth=tiled_auth)

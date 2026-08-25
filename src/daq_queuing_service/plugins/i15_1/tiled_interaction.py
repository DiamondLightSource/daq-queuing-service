import os

from blueapi.config import ServiceAccount
from blueapi.service.authentication import TiledAuth
from cachetools import TTLCache, cached
from pydantic import SecretStr
from tiled.client import from_uri
from tiled.client.container import Container
from tiled.client.container import Container as TiledContainer
from tiled.queries import Eq

from daq_queuing_service.log import LOGGER
from daq_queuing_service.plugins.i15_1.backgrounds import BackgroundInfo

# Ignoring the following rules as the tiled client is poorly typed as scares the linter
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false

cache: TTLCache[tuple[BackgroundInfo, str], str | None] = TTLCache(maxsize=100, ttl=1)

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


def get_background_tiled_id(
    tiled_client: Container,
    required_background: BackgroundInfo,
    instrument_session: str,
) -> str | None:

    @cached(cache)
    def _get_background_tiled_id(
        required_background: BackgroundInfo, instrument_session: str
    ) -> str | None:

        result: Container = (
            tiled_client.search(Eq("start.instrument_session", instrument_session))
            .search(Eq("start.instrument", "i15-1"))
            .search(
                Eq(
                    "start.experiment_definition.metadata.background",
                    required_background.model_dump_json(),
                )
            )
        )

        if not len(result):
            LOGGER.debug(
                f"Found no scans in tiled matching background: {required_background}"
            )
            return

        items = sorted(
            ((key, value) for key, value in result.items()),
            key=lambda item: item[1].metadata["start"]["time"],
        )

        # return the tiled ID
        tiled_id = items[-1][0]
        LOGGER.debug(
            f"Found {len(items)} scans in tiled matching background: "
            + f"{required_background}. Returning the first: {tiled_id}"
        )
        return tiled_id

    return _get_background_tiled_id(required_background, instrument_session)

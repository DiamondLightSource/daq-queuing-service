import os
from typing import Any

from blueapi.config import ServiceAccount
from blueapi.service.authentication import TiledAuth
from cachetools import TTLCache, cached
from pydantic import SecretStr
from tiled.client import from_uri
from tiled.client.container import Container
from tiled.queries import Eq

from daq_queuing_service.external_interaction.blueapi.blueapi_call import BlueapiCall
from daq_queuing_service.log import LOGGER

# Ignoring the following rule as the tiled client is poorly typed
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false


TILED_URL = "https://tiled.diamond.ac.uk"

cache: TTLCache[tuple[Any, str], str | None] = TTLCache(maxsize=100, ttl=1)


def get_tiled_client(
    secret_variable_name: str = "UDC_SECRET",
    client_id_variable_name: str = "UDC_CLIENT_ID",
) -> Container:

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


def get_metadata_from_tiled(
    tiled_client: Container, instrument_session: str, blueapi_task_id: str
) -> list[tuple[str, dict[str, Any]]]:

    @cached(cache)
    def _query_tiled(
        instrument_session: str, blueapi_task_id: str
    ) -> list[tuple[str, dict[str, Any]]]:
        result: Container = tiled_client.search(
            Eq("start.instrument_session", instrument_session)
        ).search(Eq("start.blueapi_task_id", blueapi_task_id))

        items = sorted(
            ((key, dict(value.metadata)) for key, value in result.items()),
            key=lambda item: item[1]["start"]["time"],
        )
        return items

    return _query_tiled(instrument_session, blueapi_task_id)


def get_tiled_and_scan_ids(tiled_client: Container, call: BlueapiCall):
    tiled_ids: list[str] = []
    scan_ids: list[int] = []
    scan_metadatas = get_metadata_from_tiled(
        tiled_client, call.task_request.instrument_session, call.blueapi_id or ""
    )

    for item in scan_metadatas:
        tiled_id, metadata = item
        tiled_ids.append(tiled_id)
        scan_ids.append(int(metadata["start"]["scan_id"]))

    LOGGER.info(
        f"Found {len(tiled_ids)} scans in tiled for blueapi task id {call.blueapi_id}"
        + f": {tiled_ids}"
    )
    return tiled_ids, scan_ids

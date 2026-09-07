import os
import time
from pathlib import Path

from blueapi.config import ServiceAccount
from blueapi.service.authentication import TiledAuth
from cachetools import TTLCache, cached
from pydantic import SecretStr
from tiled.client import from_uri
from tiled.client.container import Container
from tiled.client.container import Container as TiledContainer
from tiled.queries import Comparison, Eq, KeyPresent

from daq_queuing_service.log import LOGGER
from daq_queuing_service.plugins.i15_1.backgrounds import (
    BackgroundInfo,
    TiledBackground,
)

# Ignoring the following rules as the tiled client is poorly typed and scares the linter
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false

cache: TTLCache[tuple[BackgroundInfo, str], str | None] = TTLCache(maxsize=100, ttl=1)

TILED_URL = "https://tiled.diamond.ac.uk"
BACKGROUND_SCAN = "Background"

TILED_STALE_TIME = 60 * 15


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


def get_tiled_background(
    tiled_client: Container,
    required_background: BackgroundInfo,
    instrument_session: str,
) -> TiledBackground | None:

    @cached(cache)
    def _query_tiled(instrument_session: str) -> list[TiledBackground]:

        oldest_valid_time = time.time() - TILED_STALE_TIME
        result: Container = (
            tiled_client.search(Eq("start.instrument", "i15-1"))
            .search(Eq("start.instrument_session", instrument_session))
            .search(Eq("stop.exit_status", "success"))
            .search(Comparison("ge", "stop.time", oldest_valid_time))
            .search(Eq("start.background", True))
            .search(KeyPresent("start.sample_info.data.capillary"))
            .search(KeyPresent("start.experiment_definition.data.time_per_pdf"))
        )

        items = sorted(
            ((key, value) for key, value in result.items()),
            key=lambda item: item[1].metadata["start"]["time"],
            reverse=True,
        )

        backgrounds: list[TiledBackground] = []

        for item in items:
            tiled_id = item[0]
            filepath = Path(item[1].metadata["start"]["data_session_directory"]) / Path(
                f"{item[1].metadata['start']['scan_file']}.nxs"
            )
            bg_type = item[1].metadata["start"]["sample_info"]["data"]["capillary"]
            time_per_pdf = item[1].metadata["start"]["experiment_definition"]["data"][
                "time_per_pdf"
            ]

            backgrounds.append(
                TiledBackground(
                    tiled_id=tiled_id,
                    filepath=filepath,
                    bg_type=bg_type,
                    time_per_pdf=time_per_pdf,
                )
            )

        LOGGER.debug(
            f"Found {len(backgrounds)} background scans in tiled since "
            + f"{TILED_STALE_TIME}s ago for visit {instrument_session}."
        )
        return backgrounds

    backgrounds = _query_tiled(instrument_session)
    for background in backgrounds:
        if background.is_suitable(required_background):
            LOGGER.info(f"Found suitable background in tiled: {background.tiled_id}")
            return background
    LOGGER.info(
        f"Found no suitable backgrounds in tiled matching: {required_background}."
    )

import time
from pathlib import Path

from cachetools import TTLCache, cached
from tiled.client.container import Container
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


TILED_STALE_TIME = 60 * 15


def get_suitable_tiled_background(
    tiled_client: Container,
    required_background: BackgroundInfo,
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
            start_doc = item[1].metadata["start"]
            filename = f"{start_doc['scan_file']}.nxs"
            instrument_session_directory = Path(start_doc["data_session_directory"])
            filepath = instrument_session_directory / filename

            bg_type = start_doc["sample_info"]["data"]["capillary"]
            time_per_pdf = start_doc["experiment_definition"]["data"]["time_per_pdf"]

            backgrounds.append(
                TiledBackground(
                    tiled_id=tiled_id,
                    instrument_session=instrument_session,
                    filename=filename,
                    instrument_session_directory=instrument_session_directory,
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

    backgrounds = _query_tiled(required_background.instrument_session)
    for background in backgrounds:
        if background.is_suitable(required_background):
            LOGGER.info(f"Found suitable background in tiled: {background.tiled_id}")
            return background
    LOGGER.info(
        f"Found no suitable backgrounds in tiled matching: {required_background}."
    )

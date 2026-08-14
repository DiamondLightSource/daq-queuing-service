from cachetools import TTLCache, cached
from tiled.client.container import Container
from tiled.queries import Eq

from daq_queuing_service.log import LOGGER
from daq_queuing_service.plugins.i15_1.backgrounds import BackgroundInfo

# Ignoring the following rules as the tiled client is poorly typed as scares the linter
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false

cache: TTLCache[tuple[BackgroundInfo, str], str | None] = TTLCache(maxsize=100, ttl=1)


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

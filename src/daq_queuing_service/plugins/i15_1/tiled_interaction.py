from tiled.client import from_uri
from tiled.client.container import Container
from tiled.queries import Eq

from daq_queuing_service.plugins.i15_1.backgrounds import BackgroundInfo


def get_background_tiled_id(
    required_background: BackgroundInfo, instrument_session: str
) -> str | None:
    client = from_uri("https://tiled.diamond.ac.uk/api/v1")

    result: Container = (
        client.search(Eq("start.instrument_session", instrument_session))
        .search(Eq("start.instrument", "i15-1"))
        .search(
            Eq(
                "start.experiment_definition.metadata.background",
                required_background.model_dump_json(),
            )
        )
    )

    if not len(result):
        return

    items = sorted(
        ((key, value) for key, value in result.items()),
        key=lambda item: item[1].metadata["start"]["time"],
    )

    # return the tiled ID
    return items[-1][0]

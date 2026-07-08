from tiled.client import from_uri
from tiled.client.container import Container
from tiled.queries import Eq

from daq_queuing_service.plugins.i15_1.i15_1_converter import BackgroundInfo


def get_background_tiled_id(required: BackgroundInfo) -> str | None:
    client = from_uri("https://tiled.diamond.ac.uk/api/v1")

    result: Container = (
        client.search(Eq("start.instrument_session", "cm44163-3"))
        .search(Eq("start.instrument", "i15-1"))
        .search(Eq("start.background", required))
    )
    assert isinstance(result, Container)

    if not len(result):
        return

    items = [(key, value) for key, value in result.items()].sort(
        key=lambda item: item[1].metadata["start"]["time"]
    )

    # return the tiled ID
    return items[-1][0]

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
from tiled.queries import Comparison, Eq, In, KeyPresent

from daq_queuing_service.log import LOGGER
from daq_queuing_service.plugins.i15_1.auxiliary import (
    AUXILIARY_SCAN_NAMES,
    AuxiliaryScan,
    TiledAuxiliary,
)
from daq_queuing_service.plugins.i15_1.standards import StandardsPin

# Ignoring the following rules as the tiled client is poorly typed and scares the linter
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false

cache: TTLCache[tuple[AuxiliaryScan, str], str | None] = TTLCache(maxsize=100, ttl=1)

TILED_URL = "https://tiled.diamond.ac.uk"


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


def get_suitable_tiled_scan(
    tiled_client: Container,
    required_scan: AuxiliaryScan,
) -> TiledAuxiliary | None:

    @cached(cache)
    def _query_tiled(instrument_session: str) -> list[TiledAuxiliary]:

        oldest_valid_time = time.time() - TILED_STALE_TIME
        result: Container = (
            tiled_client.search(Eq("start.instrument", "i15-1"))
            .search(Eq("start.instrument_session", instrument_session))
            .search(Eq("stop.exit_status", "success"))
            .search(Comparison("ge", "stop.time", oldest_valid_time))
            .search(In("start.scan_type", AUXILIARY_SCAN_NAMES))
            .search(KeyPresent("start.experiment_definition.data.time_per_pdf"))
        )

        items = sorted(
            ((key, value) for key, value in result.items()),
            key=lambda item: item[1].metadata["start"]["time"],
            reverse=True,
        )

        auxiliary_scans: list[TiledAuxiliary] = []

        for item in items:
            tiled_id = item[0]
            start_doc = item[1].metadata["start"]
            filename = f"{start_doc['scan_file']}.nxs"
            instrument_session_directory = Path(start_doc["data_session_directory"])
            filepath = instrument_session_directory / filename

            sample = start_doc.get("sample_info")

            if sample is None:
                pin = None
            else:
                capillary = sample["data"].get("capillary")
                if not capillary:
                    LOGGER.warning(
                        f"No capillary found in tiled scan {tiled_id}. Skipping."
                    )
                    continue
                pin = StandardsPin(
                    capillary=sample["data"]["capillary"],
                    contents=sample["data"].get("composition"),
                )
            time_per_pdf = start_doc["experiment_definition"]["data"]["time_per_pdf"]

            auxiliary_scan = TiledAuxiliary(
                tiled_id=tiled_id,
                instrument_session=instrument_session,
                filename=filename,
                instrument_session_directory=instrument_session_directory,
                filepath=filepath,
                pin=pin,
                time_per_pdf=time_per_pdf,
            )
            if auxiliary_scan.kind != start_doc["scan_type"]:
                LOGGER.warning(
                    f"Inferred auxiliary type: {auxiliary_scan.kind} does not match "
                    + f"scan type in metadata: {start_doc['scan_type']} for auxiliary "
                    + f"scan {auxiliary_scan}. Skipping."
                )
                continue
            auxiliary_scans.append(auxiliary_scan)

        LOGGER.debug(
            f"Found {len(auxiliary_scans)} auxiliary scans in tiled since "
            + f"{TILED_STALE_TIME}s ago for visit {instrument_session}."
        )
        return auxiliary_scans

    auxiliary_scans = _query_tiled(required_scan.instrument_session)
    for scan in auxiliary_scans:
        if scan.is_suitable(required_scan):
            LOGGER.info(f"Found suitable auxiliary scans in tiled: {scan.tiled_id}")
            return scan
    LOGGER.info(
        f"Found no suitable auxiliary scans in tiled matching: {required_scan}."
    )

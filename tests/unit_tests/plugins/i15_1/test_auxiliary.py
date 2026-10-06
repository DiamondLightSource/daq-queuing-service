import pytest
from daq_config_server.models.i15_1.standards_puck import StandardsPin

from daq_queuing_service.plugins.i15_1.auxiliary import AuxiliaryScan


@pytest.mark.parametrize(
    "auxiliary, required, expected_is_suitable",
    [
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                list_of_temperatures=[],
            ),
            True,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
            ),
            AuxiliaryScan(
                instrument_session="def",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.5", contents="Silicon"),
                time_per_pdf=50,
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                list_of_temperatures=[100, 200, 300],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                list_of_temperatures=[100, 200, 300],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                list_of_temperatures=[100, 200, 300, 400],
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                list_of_temperatures=[100, 200, 300, 400],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                list_of_temperatures=[100, 200, 300, 400],
            ),
            True,
        ),
    ],
)
def test_is_suitable_works_as_expected(
    auxiliary: AuxiliaryScan, required: AuxiliaryScan, expected_is_suitable: bool
):
    assert auxiliary.is_suitable(required) is expected_is_suitable


@pytest.mark.parametrize(
    "auxiliary, required, expected_result",
    [
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=10,
                list_of_temperatures=[150, 250, 350],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                list_of_temperatures=[100, 200, 300],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,  # Max time_per_pdf
                list_of_temperatures=[100, 200, 300, 400],  # Covers whole temp range
            ),
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=10,
                list_of_temperatures=[150, 250, 350],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Ga/In"),
                time_per_pdf=50,
                list_of_temperatures=[100, 200, 300],
            ),
            None,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=10,
                list_of_temperatures=[150, 250, 350],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                list_of_temperatures=[],
            ),
            None,
        ),
    ],
)
def test_attempt_to_combine_with_works_as_expected(
    auxiliary: AuxiliaryScan,
    required: AuxiliaryScan,
    expected_result: AuxiliaryScan | None,
):
    assert auxiliary.attempt_to_combine_with(required) == expected_result

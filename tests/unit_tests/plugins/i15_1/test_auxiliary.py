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
                q_max=30,
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
                list_of_temperatures=[],
            ),
            True,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=35,
                list_of_temperatures=[],
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
            ),
            AuxiliaryScan(
                instrument_session="def",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.5", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
                list_of_temperatures=[100, 200, 300],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
                list_of_temperatures=[100, 200, 300],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
                list_of_temperatures=[100, 200, 300, 400],
            ),
            False,
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=40,
                list_of_temperatures=[90, 290, 490],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=25,
                list_of_temperatures=[95, 250, 300, 450],
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
                q_max=30,
                list_of_temperatures=[150, 250, 350],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=25,
                list_of_temperatures=[100, 200, 300],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,  # Max time_per_pdf
                q_max=30,  # Max q_max
                list_of_temperatures=[100, 200, 300, 400],  # Covers whole temp range
            ),
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=100,
                q_max=40,
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=100,
                q_max=40,
            ),
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=40,
                q_max=30,
                list_of_temperatures=[50, 610],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=100,
                q_max=30,
                list_of_temperatures=[100, 200, 300],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=100,
                q_max=30,
                list_of_temperatures=[50, 150, 250, 350, 450, 550, 650],
            ),
        ),
    ],
)
def test_attempt_to_combine_with_works_as_expected(
    auxiliary: AuxiliaryScan,
    required: AuxiliaryScan,
    expected_result: AuxiliaryScan,
):
    assert auxiliary.attempt_to_combine_with(required) == expected_result


@pytest.mark.parametrize(
    "auxiliary, required",
    [
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=10,
                q_max=30,
                list_of_temperatures=[150, 250, 350],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Ga/In"),
                time_per_pdf=50,
                q_max=30,
                list_of_temperatures=[100, 200, 300],
            ),
        ),
        (
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=10,
                q_max=30,
                list_of_temperatures=[150, 250, 350],
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
                list_of_temperatures=[],
            ),
        ),
        (
            AuxiliaryScan(
                instrument_session="def",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=10,
                q_max=30,
            ),
            AuxiliaryScan(
                instrument_session="abc",
                pin=StandardsPin(capillary="bs1.0", contents="Silicon"),
                time_per_pdf=50,
                q_max=30,
            ),
        ),
    ],
)
def test_attempt_to_combine_returns_none_when_auxiliaries_cannot_be_combined(
    auxiliary: AuxiliaryScan,
    required: AuxiliaryScan,
):
    assert auxiliary.attempt_to_combine_with(required) is None

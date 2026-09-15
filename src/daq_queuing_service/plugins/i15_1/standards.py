from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from daq_queuing_service.plugins.i15_1.capillaries import ALLOWED_USER_CAPILLARIES

STANDARDS_PUCK_PLACEMENT = 1
STANDARD_SAMPLE = Literal[
    "Silicon", "Tungsten/Boron mix", "Si/Al2O3", "Pb", "LaB6 660b", "Ga/In"
]

STANDARD_CAPILLARY = ALLOWED_USER_CAPILLARIES | Literal["metal"]

# Should put this into the config server and create a config file for it.


class StandardsPin(BaseModel):
    capillary: STANDARD_CAPILLARY
    contents: STANDARD_SAMPLE | None  # None for empty capillary


class StandardsPuck(BaseModel):
    model_config = ConfigDict(validate_default=True)

    pins: dict[int, StandardsPin | None] = Field(
        default_factory=lambda: {
            1: StandardsPin(capillary="metal", contents=None),
            2: StandardsPin(capillary="bs1.0", contents="Silicon"),
            3: StandardsPin(capillary="fq1.0", contents="Silicon"),
            4: StandardsPin(capillary="bs1.5", contents="Silicon"),
            5: None,
            6: StandardsPin(capillary="bs2.0", contents="Silicon"),
            7: None,
            8: None,
            9: StandardsPin(capillary="bs1.0", contents="Si/Al2O3"),
            10: StandardsPin(capillary="fq1.0", contents="Si/Al2O3"),
            11: StandardsPin(capillary="fq1.5", contents="Si/Al2O3"),
            12: StandardsPin(capillary="fq2.0", contents="Si/Al2O3"),
            13: StandardsPin(capillary="bs1.0", contents="Pb"),
            14: StandardsPin(capillary="bs1.0", contents="LaB6 660b"),
            15: StandardsPin(capillary="bs1.0", contents=None),
            16: StandardsPin(capillary="fq1.0", contents=None),
            17: StandardsPin(capillary="bs1.5", contents=None),
            18: StandardsPin(capillary="fq1.5", contents=None),
            19: StandardsPin(capillary="bs2.0", contents=None),
            20: StandardsPin(capillary="fq2.0", contents=None),
            21: StandardsPin(capillary="bs1.0", contents="Ga/In"),
            22: StandardsPin(capillary="bs1.0", contents="Tungsten/Boron mix"),
        }
    )

    def get_pin_number(self, pin: StandardsPin):
        for pin_number, loaded_pin in self.pins.items():
            if pin == loaded_pin:
                return pin_number
        raise ValueError(f"No pin on the standards puck matching {pin}")

    @field_validator("pins")
    @classmethod
    def pins_must_be_1_to_22(cls, pins: dict[int, StandardsPin | None]):
        assert sorted(pins.keys()) == list(range(1, 23)), (
            f"Pins must be 1-22, with no gaps. Current pins: {list(pins.keys())}"
        )
        return pins


a = StandardsPuck()
print(a.get_pin_number(StandardsPin(capillary="bs1.0", contents=None)))

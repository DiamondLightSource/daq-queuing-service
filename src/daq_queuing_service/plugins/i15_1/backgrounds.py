from typing import Literal

from pydantic import BaseModel, ConfigDict

# This should be generated from the json schema
# https://github.com/DiamondLightSource/daq-queuing-service/issues/78
CAPILLARY = Literal["air", "bs", "fq", "pi"]


class BackgroundInfo(BaseModel):
    model_config = ConfigDict(frozen=True)
    bg_type: CAPILLARY
    cobra: bool
    blower: bool

    def add_tiled_id(self, tiled_id: str) -> "TiledBackground":
        return TiledBackground(
            bg_type=self.bg_type,
            cobra=self.cobra,
            blower=self.blower,
            tiled_id=tiled_id,
        )


class TiledBackground(BackgroundInfo):
    tiled_id: str

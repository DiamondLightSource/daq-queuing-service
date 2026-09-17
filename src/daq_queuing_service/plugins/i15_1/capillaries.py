from typing import Literal

# This should be generated from the json schema
# https://github.com/DiamondLightSource/daq-queuing-service/issues/78
ALLOWED_USER_CAPILLARIES = Literal[
    "bs1.0",
    "bs1.5",
    "bs2.0",
    "fq1.0",
    "fq1.5",
    "fq2.0",
]

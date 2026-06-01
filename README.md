[![CI](https://github.com/DiamondLightSource/daq-queuing-service/actions/workflows/ci.yml/badge.svg)](https://github.com/DiamondLightSource/daq-queuing-service/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/DiamondLightSource/daq-queuing-service/branch/main/graph/badge.svg)](https://codecov.io/gh/DiamondLightSource/daq-queuing-service)
[![PyPI](https://img.shields.io/pypi/v/daq-queuing-service.svg)](https://pypi.org/project/daq-queuing-service)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

# daq_queuing_service

A service to queue DAQ experiments and chain BlueAPI calls

The service combines a queue with a worker that consumes items in the queue and sends them to blueapi. A REST API is exposed to interact with the queue, allowing users to add, move, cancel, and get information about, items in the queue, and pause/unpause the queue.

|     What      |                                Where                                 |
| :-----------: | :------------------------------------------------------------------: |
|    Source     |     <https://github.com/DiamondLightSource/daq-queuing-service>      |
|     PyPI      |                  `pip install daq-queuing-service`                   |
|    Docker     |  `docker run ghcr.io/diamondlightsource/daq-queuing-service:latest`  |
| Documentation |      <https://diamondlightsource.github.io/daq-queuing-service>      |
|   Releases    | <https://github.com/DiamondLightSource/daq-queuing-service/releases> |

The server is build with FastAPI. For development and testing, it may be useful to have blueapi and the queue service running locally. To get started, install dependencies into a virtual environment and run the app:

```bash
uv sync
source .venv/bin/activate
podman-compose -f tests/system_tests/compose.yaml up             # Local rabbitmq
blueapi --config tests/test_data/test_blueapi_config.yaml serve  # Local blueapi
daq-queuing-service -p 8001 --dev                                # Local queue service
```

<!-- README only content. Anything below this line won't be included in index.md -->

See https://diamondlightsource.github.io/daq-queuing-service for more detailed documentation.

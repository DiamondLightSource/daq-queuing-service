import json
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from daq_queuing_service.api.api import create_api_router

app = FastAPI()
app.include_router(create_api_router(MagicMock(), MagicMock(), MagicMock()))

openapi = get_openapi(
    title=app.title,
    version=app.version,
    routes=app.routes,
)

with open("docs/reference/rest_api.json", "w") as f:
    f.write(json.dumps(openapi, indent=2))
    f.write("\n")

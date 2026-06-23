import os

import yaml
from blueapi.config import ApplicationConfig
from pydantic import BaseModel

CONFIG_PATH = "/etc/config/config.yaml"
TEST_CONFIG_PATH = "tests/test_data/test_config.yaml"


class ConverterConfig(BaseModel):
    path: str
    name: str


class AppConfig(BaseModel):
    blueapi: ApplicationConfig
    converter: ConverterConfig


def load_config() -> AppConfig:
    path = CONFIG_PATH if os.path.isfile(CONFIG_PATH) else TEST_CONFIG_PATH
    with open(path) as f:
        data = yaml.safe_load(f)
    return AppConfig(**data)

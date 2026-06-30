import os
from pathlib import Path

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


def get_default_config() -> str:
    return CONFIG_PATH if os.path.isfile(CONFIG_PATH) else TEST_CONFIG_PATH


def load_config(config_path: Path) -> AppConfig:
    with open(config_path) as f:
        data = yaml.safe_load(f)
    print(data)
    return AppConfig(**data)

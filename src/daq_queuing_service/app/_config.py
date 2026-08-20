import os
from pathlib import Path

import yaml
from blueapi.config import OIDCConfig, RestConfig, StompConfig
from pydantic import BaseModel, Field

CONFIG_PATH = "/etc/config/config.yaml"
TEST_CONFIG_PATH = "tests/test_data/test_config.yaml"


class ConverterConfig(BaseModel):
    path: str
    name: str


class BlueapiConfig(BaseModel):
    stomp: StompConfig = Field(default_factory=StompConfig)
    api: RestConfig = Field(default_factory=RestConfig)


class AppConfig(BaseModel):
    blueapi: BlueapiConfig
    converter: ConverterConfig
    oidc: OIDCConfig | None = None
    authorisation_whitelist: list[str] | None = None


def get_default_config_path() -> str:
    return CONFIG_PATH if os.path.isfile(CONFIG_PATH) else TEST_CONFIG_PATH


def load_config(config_path: Path) -> AppConfig:
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return AppConfig(**data)

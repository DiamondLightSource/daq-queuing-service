import logging
import os
from pathlib import Path

import yaml
from blueapi.config import OIDCConfig, RestConfig, StompConfig
from pydantic import BaseModel, Field, field_validator

CONFIG_PATH = "/etc/config/config.yaml"
TEST_CONFIG_PATH = "tests/test_data/test_config.yaml"


class ConverterConfig(BaseModel):
    path: str
    name: str


class BlueapiConfig(BaseModel):
    stomp: StompConfig = Field(default_factory=StompConfig)
    api: RestConfig = Field(default_factory=RestConfig)


class LoggingHandlerConfig(BaseModel):
    level: int = logging.INFO

    @field_validator("level", mode="before")
    @classmethod
    def convert_logging_str_to_int(cls, value: str | int) -> int:
        if isinstance(value, str):
            level = getattr(logging, value)
            if not isinstance(level, int):
                raise ValueError(f"Got invalid logging level: {value}")
            return level
        return value


class GraylogConfig(LoggingHandlerConfig):
    enabled: bool = False
    # https://gitlab.diamond.ac.uk/sscc-docs/developer-guide/-/merge_requests/295/diffs
    port: int = 12231  # dodal port - should make dedicated one for the queue
    url: str = "graylog-log-target.diamond.ac.uk"


class LoggingConfig(BaseModel):
    stream: LoggingHandlerConfig = Field(default_factory=LoggingHandlerConfig)
    graylog: GraylogConfig = Field(default_factory=GraylogConfig)


class AppConfig(BaseModel):
    blueapi: BlueapiConfig
    converter: ConverterConfig
    oidc: OIDCConfig | None = None
    authorisation_whitelist: list[str] | None = None
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    beamline: str | None = None


def get_default_config_path() -> str:
    return CONFIG_PATH if os.path.isfile(CONFIG_PATH) else TEST_CONFIG_PATH


def load_config(config_path: Path) -> AppConfig:
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return AppConfig(**data)

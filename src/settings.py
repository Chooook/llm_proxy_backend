import os
from functools import cached_property
from pathlib import Path
from typing import Tuple, Type

from pydantic_settings import (BaseSettings, PydanticBaseSettingsSource,
                               SettingsConfigDict, YamlConfigSettingsSource)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        yaml_file=Path(__file__).parent.parent / 'config.yaml')

    LOGLEVEL: str = 'DEBUG'
    DEBUG: bool = True

    HOST: str = '127.0.0.1'
    BACKEND_PORT: int = 8000
    FRONTEND_PORT: int = 5000
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_EXPIRE_DAYS: int = 7

    ACCESS_TOKEN_EXPIRE_DAYS: int = 90
    JWT_ALGORITHM: str = 'HS256'
    SECRET_KEY: str

    USE_GP_COLD_STORE: bool = False
    GP_SINGLE_CONNECTION: bool = True
    GP_HOST: str = ''
    GP_PORT: int = 5432
    GP_DATABASE: str = ''
    GP_SCHEMA: str = ''
    GP_TABLE: str = ''
    GP_USERNAME: str = os.getenv('GP_USERNAME', '')
    GP_PASSWORD: str = os.getenv('GP_PASSWORD', '')  # TODO: is needed?
    GP_MIN_CONNECTIONS: int = 1
    GP_MAX_CONNECTIONS: int = 1
    GP_MAX_INACTIVE_LIFETIME: int = 36000

    @cached_property
    def redis_store_seconds(self):
        return self.REDIS_EXPIRE_DAYS * 24 * 60 * 60

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (YamlConfigSettingsSource(settings_cls),)

settings = Settings()

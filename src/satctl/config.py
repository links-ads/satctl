from pathlib import Path
from typing import Any

import envyaml
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource
from pydantic_settings.sources.types import DEFAULT_PATH, PathType


class EnvYamlConfigSettingsSource(YamlConfigSettingsSource):
    def __init__(
        self,
        settings_cls: type[BaseSettings],
        *,
        yaml_file: PathType | None = DEFAULT_PATH,
        yaml_file_encoding: str | None = None,
        yaml_config_section: str | None = None,
        env_file: Path | str | None = None,
        env_file_encoding: str | None = None,
    ):
        self.env_file = env_file or settings_cls.model_config.get("env_file")
        self.env_file_encoding = env_file_encoding or settings_cls.model_config.get("env_file_encoding")
        super().__init__(
            settings_cls,
            yaml_file=yaml_file,
            yaml_file_encoding=yaml_file_encoding,
            yaml_config_section=yaml_config_section,
        )

    def _read_file(self, file_path: Path) -> dict[str, Any]:
        if Path(file_path).exists():
            return dict(envyaml.EnvYAML(file_path, self.env_file, flatten=False))
        return {}


class MainSettings(BaseSettings):
    model_config = SettingsConfigDict(
        yaml_file="config.yml",
        env_file=".env",
        extra="ignore",
    )

    download: dict[str, Any]
    auth: dict[str, Any]
    sources: dict[str, Any]

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            EnvYamlConfigSettingsSource(settings_cls),
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

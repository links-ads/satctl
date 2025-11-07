import os
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
        """Read YAML file with environment variable expansion.

        Args:
            file_path (Path): Path to YAML configuration file

        Returns:
            dict[str, Any]: Parsed configuration data with environment variables expanded
        """
        if Path(file_path).exists():
            return dict(envyaml.EnvYAML(file_path, self.env_file, flatten=False))
        return {}


class SatCtlSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

    yaml_file: str | Path | None = None
    # default to empty values
    download: dict[str, Any] = {}
    auth: dict[str, Any] = {}
    sources: dict[str, Any] = {}

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Customize settings sources to include YAML configuration.

        Args:
            settings_cls (type[BaseSettings]): Settings class being configured
            init_settings (PydanticBaseSettingsSource): Initialization settings source
            env_settings (PydanticBaseSettingsSource): Environment variable settings source
            dotenv_settings (PydanticBaseSettingsSource): Dotenv file settings source
            file_secret_settings (PydanticBaseSettingsSource): File secrets settings source

        Returns:
            tuple[PydanticBaseSettingsSource, ...]: Ordered tuple of settings sources
        """
        yaml_file = init_settings.init_kwargs.get("yaml_file", "config.yml")  # type: ignore
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            EnvYamlConfigSettingsSource(settings_cls, yaml_file=yaml_file),
            file_secret_settings,
        )


_instance: SatCtlSettings | None = None


def get_settings(**kwargs: Any) -> SatCtlSettings:
    """Get or create the global settings instance.

    Args:
        yaml_file (str, Path): path to the main configuration file, defaults to config.yml.
        **kwargs: Optional keyword arguments passed to SatCtlSettings constructor

    Returns:
        Global SatCtlSettings instance
    """
    global _instance
    if _instance is None:
        yaml_file = Path(os.getenv("SATCTL_CONFIG", "config.yml"))
        _instance = SatCtlSettings(yaml_file=yaml_file, **kwargs)
    return _instance

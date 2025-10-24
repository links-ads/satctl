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
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            EnvYamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


_instance: SatCtlSettings | None = None


def get_settings(**kwargs: Any) -> SatCtlSettings:
    """Get or create the global settings instance.

    Args:
        **kwargs: Optional keyword arguments passed to SatCtlSettings constructor

    Returns:
        Global SatCtlSettings instance
    """
    global _instance
    if _instance is None:
        _instance = SatCtlSettings(**kwargs)
    return _instance

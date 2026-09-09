from pathlib import Path

import yaml


class ConfigLoader:
    """Loads YAML configuration files."""

    @staticmethod
    def load(config_name: str) -> dict:
        config_path = (
            Path(__file__).resolve().parent.parent
            / "config"
            / config_name
        )

        with config_path.open("r", encoding="utf-8") as file:
            return yaml.safe_load(file)
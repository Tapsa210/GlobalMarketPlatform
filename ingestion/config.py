import os
from dataclasses import dataclass

from dotenv import load_dotenv


class ConfigurationError(ValueError):
    """Raised when required application configuration is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    alpha_vantage_api_key: str


def load_settings() -> Settings:
    load_dotenv()

    api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()

    if not api_key:
        raise ConfigurationError(
            "ALPHA_VANTAGE_API_KEY is missing."
        )

    if api_key.lower() == "your_api_key_here":
        raise ConfigurationError(
            "ALPHA_VANTAGE_API_KEY still contains the example placeholder."
        )

    return Settings(alpha_vantage_api_key=api_key)

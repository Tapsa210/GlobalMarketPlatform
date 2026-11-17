
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""


class Settings(BaseSettings):
    alpha_vantage_api_key: str = Field(
        default="", validation_alias="ALPHA_VANTAGE_API_KEY"
    )
    app_env: str = Field(default="development", validation_alias="APP_ENV")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    # PostgreSQL settings
    postgres_host: str = Field(default="127.0.0.1", validation_alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5433, validation_alias="POSTGRES_PORT")
    postgres_db: str = Field(default="", validation_alias="POSTGRES_DB")
    postgres_user: str = Field(default="", validation_alias="POSTGRES_USER")
    postgres_password: str = Field(default="", validation_alias="POSTGRES_PASSWORD")

    # Kafka settings
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    stock_symbols: str = Field(
        default="AAPL",
        validation_alias="STOCK_SYMBOLS",
    )

    @field_validator("alpha_vantage_api_key")
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        if not v or not v.strip() or v == "your_api_key_here":
            raise ValueError("ALPHA_VANTAGE_API_KEY is missing or invalid.")
        return v

    @field_validator("postgres_db", "postgres_user", "postgres_password")
    @classmethod
    def validate_db_settings(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f"{info.field_name.upper()} is missing or invalid.")
        return v


def load_settings() -> Settings:
    try:
        return Settings()
    except Exception as e:
        raise ConfigurationError(f"Configuration failed: {e}") from e

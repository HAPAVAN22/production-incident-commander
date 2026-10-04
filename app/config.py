from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "Production Incident Commander API"
    app_env: str = "local"
    log_level: str = "INFO"

    database_url: str = Field(repr=False)

    db_pool_min_size: int = Field(default=1, ge=1)
    db_pool_max_size: int = Field(default=5, ge=1)
    outbox_poll_interval_seconds : int = Field(default=5, ge=1)
    outbox_batch_size: int = Field(default=10, ge=1)
    outbox_max_attempts: int = Field(default=3, ge=1)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

@lru_cache
def get_settings() -> Settings:
    return Settings()
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    secop_app_token: str | None = Field(default=None, alias="SECOP_APP_TOKEN")
    secop_api_key_id: str | None = Field(default=None, alias="SECOP_API_KEY_ID")
    secop_app_secret: str | None = Field(default=None, alias="SECOP_APP_SECRET")
    deepseek_api_key: str | None = Field(default=None, alias="DEEPSEEK_API_KEY")
    deepseek_model: str = Field(default="deepseek-chat", alias="DEEPSEEK_MODEL")
    datos_gov_domain: str = Field(default="www.datos.gov.co", alias="DATOS_GOV_DOMAIN")
    secop_timeout_seconds: int = Field(default=30, alias="SECOP_TIMEOUT_SECONDS")
    secop_results_limit: int = Field(default=25, alias="SECOP_RESULTS_LIMIT")
    secop_alias_db_path: Path = Field(default=Path("app/data/aliases_db.json"), alias="SECOP_ALIAS_DB_PATH")
    telegram_bot_token: str | None = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_webhook_secret: str | None = Field(default=None, alias="TELEGRAM_WEBHOOK_SECRET")
    twilio_whatsapp_number: str | None = Field(default=None, alias="TWILIO_WHATSAPP_NUMBER")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @property
    def llm_enabled(self) -> bool:
        return bool(self.deepseek_api_key)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

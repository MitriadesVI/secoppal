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
        populate_by_name=True,
    )

    secop_app_token: str | None = Field(default=None, alias="SECOP_APP_TOKEN")
    secop_api_key_id: str | None = Field(default=None, alias="SECOP_API_KEY_ID")
    secop_app_secret: str | None = Field(default=None, alias="SECOP_APP_SECRET")
    deepseek_api_key: str | None = Field(default=None, alias="DEEPSEEK_API_KEY")
    deepseek_model: str = Field(default="deepseek-chat", alias="DEEPSEEK_MODEL")
    datos_gov_domain: str = Field(default="www.datos.gov.co", alias="DATOS_GOV_DOMAIN")
    secop_timeout_seconds: int = Field(default=15, alias="SECOP_TIMEOUT_SECONDS")
    secop_results_limit: int = Field(default=25, alias="SECOP_RESULTS_LIMIT")
    secop_alias_db_path: Path = Field(default=Path("app/data/aliases_db.json"), alias="SECOP_ALIAS_DB_PATH")
    telegram_bot_token: str | None = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_webhook_secret: str | None = Field(default=None, alias="TELEGRAM_WEBHOOK_SECRET")
    twilio_whatsapp_number: str | None = Field(default=None, alias="TWILIO_WHATSAPP_NUMBER")
    # OpenWA — gateway self-hosted de WhatsApp Web (https://github.com/rmyndharis/OpenWA).
    # Patrón JSON-in / REST-out (espeja Telegram), NO TwiML como Twilio.
    openwa_base_url: str = Field(default="http://localhost:2785", alias="OPENWA_BASE_URL")
    openwa_session_id: str | None = Field(default=None, alias="OPENWA_SESSION_ID")
    openwa_api_key: str | None = Field(default=None, alias="OPENWA_API_KEY")
    openwa_webhook_secret: str | None = Field(default=None, alias="OPENWA_WEBHOOK_SECRET")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    secop_feedback_path: Path = Field(default=Path("data/feedback.jsonl"), alias="SECOP_FEEDBACK_PATH")
    secop_feedback_db_path: Path = Field(default=Path("data/secoppal.db"), alias="SECOP_FEEDBACK_DB_PATH")
    secop_feedback_sqlite_enabled: bool = Field(default=True, alias="SECOP_FEEDBACK_SQLITE_ENABLED")
    secop_feedback_jsonl_dual_write: bool = Field(default=True, alias="SECOP_FEEDBACK_JSONL_DUAL_WRITE")
    # M3C-CG-I: ruta local opcional para telemetría de follow-ups de modalidad.
    # Desactivada por defecto (None). Cuando está configurada, append JSONL.
    modality_followup_telemetry_path: Path | None = Field(
        default=None, alias="MODALITY_FOLLOWUP_TELEMETRY_PATH"
    )
    # ── Autenticación de la UI ───────────────────────────────────────────────
    secoppal_auth_enabled: bool = Field(default=True, alias="SECOPPAL_AUTH_ENABLED")
    secoppal_admin_email: str | None = Field(default=None, alias="SECOPPAL_ADMIN_EMAIL")
    secoppal_admin_password: str | None = Field(default=None, alias="SECOPPAL_ADMIN_PASSWORD")
    secoppal_admin_name: str | None = Field(default=None, alias="SECOPPAL_ADMIN_NAME")

    @property
    def llm_enabled(self) -> bool:
        return bool(self.deepseek_api_key)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

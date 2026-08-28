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
    # AP3-B: artefacto SQLite local, regenerable y no versionado. El resolver
    # abre esta ruta en solo lectura; únicamente el builder la reemplaza.
    secop_actor_alias_index_path: Path = Field(
        default=Path("data/actor_alias_index.sqlite"),
        alias="SECOP_ACTOR_ALIAS_INDEX_PATH",
    )
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
    # ── Actor Profile (AP2) ──────────────────────────────────────────────────
    # Gobierna la huella contractual por actor: routing de documento exacto,
    # bloqueo por capacidad de dataset y render del perfil. Apagado, el parser,
    # el SoQL y la respuesta vuelven al comportamiento previo a AP2 — lo
    # verifica tests/test_actor_flag_off.py.
    #
    # Apagado POR DEFECTO a propósito. Si esta rama entra a main por
    # fast-forward, el merge no debe encender un routing nuevo en producción
    # sin que nadie lo haya decidido: la activación es un acto explícito del
    # entorno (SECOPPAL_ACTOR_PROFILE_ENABLED=true), no un efecto secundario
    # de integrar código. La suite corre con el flag encendido —ver
    # tests/conftest.py— porque lo que hay que ejercitar es el camino que se
    # va a desplegar el día que se encienda.
    secoppal_actor_profile_enabled: bool = Field(
        default=False, alias="SECOPPAL_ACTOR_PROFILE_ENABLED",
    )

    # ── Transporte Socrata (SODA3_TRANSPORT_V1) ──────────────────────────────
    # `soda3`   = transporte productivo default validado.
    # `classic` = rollback explicito sobre sodapy y GET /resource/.
    # Un valor distinto de esos dos es un error explicito al construir el
    # workflow, no una seleccion silenciosa.
    secoppal_socrata_transport: str = Field(
        default="soda3", alias="SECOPPAL_SOCRATA_TRANSPORT",
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


def actor_profile_enabled() -> bool:
    """Lectura del flag de AP2 desde el punto más bajo del pipeline.

    El parser y el planificador son puros y no reciben ``Settings``; leerlo
    aquí evita enhebrar la configuración por siete firmas para un booleano.
    Si la configuración no se puede construir (entorno sin ``.env``, como los
    scripts de corpus), cae al default declarado en ``Settings`` en vez de
    reventar: un flag ilegible no debe tumbar una consulta.
    """
    try:
        return bool(get_settings().secoppal_actor_profile_enabled)
    except Exception:  # pragma: no cover - configuración rota
        return True

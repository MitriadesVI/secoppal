from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import Field, TypeAdapter, model_validator
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
    # Encendido POR DEFECTO como capacidad productiva del beta. El flag se
    # conserva como kill-switch: SECOPPAL_ACTOR_PROFILE_ENABLED=false es el
    # rollback explícito y restaura el comportamiento anterior a AP2.
    #
    # El default ON no relaja la resolución: un índice ausente, corrupto o de
    # esquema incompatible sigue produciendo INDEX_UNAVAILABLE sin fallback.
    secoppal_actor_profile_enabled: bool = Field(
        default=True, alias="SECOPPAL_ACTOR_PROFILE_ENABLED",
    )

    # ── Entity Profile Frame (V1.2) ──────────────────────────────────────────
    # Enciende el trayecto nuevo COMPLETO: Frame como única autoridad del
    # significado, admisión superior antes del dispatcher y publicación
    # versionada por conversación. No es un modo híbrido: encendido entran a la
    # vez los lectores y los escritores nuevos, y apagado no se escribe ningún
    # sobre que un binario anterior tuviera que entender.
    #
    # Apagado POR DEFECTO. V1.2-B se entrega para revisión independiente; la
    # activación productiva es una decisión posterior y de otro gate.
    secoppal_entity_profile_frame_enabled: bool = Field(
        default=False, alias="SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED",
    )

    # Natural Alpha observes human jobs; the existing runtime keeps execution.
    secoppal_natural_alpha_mode: str = Field(default="off", alias="SECOPPAL_NATURAL_ALPHA_MODE")
    secoppal_natural_alpha_email: str | None = Field(default=None, alias="SECOPPAL_NATURAL_ALPHA_EMAIL")
    secoppal_natural_alpha_timeout_seconds: float = Field(
        default=10.0, gt=0, le=45, allow_inf_nan=False,
        alias="SECOPPAL_NATURAL_ALPHA_TIMEOUT_SECONDS",
    )
    # A2: a single Alpha-mode ANALYZE/SUPPLIERS job opens Entity Profile
    # suppliers through its input contract. Off by default; shadow never binds.
    secoppal_a2_suppliers_binding_enabled: bool = Field(
        default=False, alias="SECOPPAL_A2_SUPPLIERS_BINDING_ENABLED",
    )
    # Contract Activity is a separate Alpha binding.  The supplier pilot must
    # never make this route reachable as a side effect.
    secoppal_contract_activity_binding_enabled: bool = Field(
        default=False, alias="SECOPPAL_CONTRACT_ACTIVITY_BINDING_ENABLED",
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


def entity_profile_frame_enabled() -> bool:
    """Lectura del flag de V1.2 desde el punto más bajo que lo necesita.

    Mismo mecanismo que AP2, por la misma razón: la admisión y el turno de
    perfil son puros y no reciben ``Settings``. Una configuración ilegible cae
    al default declarado —apagado— en vez de encender un trayecto nuevo por
    accidente.
    """
    try:
        return bool(get_settings().secoppal_entity_profile_frame_enabled)
    except Exception:  # pragma: no cover - configuración rota
        return False


class SupplierPolaritySidecarState(str, Enum):
    DISABLED = "DISABLED"
    ENABLED = "ENABLED"
    ENABLED_BUT_UNAVAILABLE = "ENABLED_BUT_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class SupplierPolaritySidecarConfig:
    state: SupplierPolaritySidecarState
    api_key: str | None = None
    model: str | None = None
    timeout_seconds: float | None = None
    detail: str = ""


_SIDECAR_TIMEOUT = TypeAdapter(
    Annotated[float, Field(gt=0, le=45, allow_inf_nan=False)]
)


class SupplierSemanticsSettings(BaseSettings):
    """Canonical, isolated authority for the A2 supplier sidecar."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=False,
    )

    enabled: bool = Field(
        default=False, alias="SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED",
    )
    api_key: str | None = Field(default=None, alias="DEEPSEEK_API_KEY")
    model: str = Field(default="deepseek-chat", alias="DEEPSEEK_MODEL")
    timeout_seconds: float | str = Field(
        default=8.0, alias="SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS",
    )

    @model_validator(mode="after")
    def validate_enabled_prerequisites(self) -> SupplierSemanticsSettings:
        if not self.enabled:
            return self
        if not self.api_key or not self.api_key.strip():
            raise ValueError("missing sidecar credential")
        if not self.model.strip():
            raise ValueError("empty sidecar model")
        self.timeout_seconds = _SIDECAR_TIMEOUT.validate_python(self.timeout_seconds)
        return self


@lru_cache(maxsize=1)
def get_supplier_semantics_settings(
    _env_file: str | Path | None = ".env",
) -> SupplierSemanticsSettings:
    return SupplierSemanticsSettings(_env_file=_env_file)


def supplier_polarity_sidecar_config() -> SupplierPolaritySidecarConfig:
    """Load the sidecar once, preserving ON-but-unavailable as its own state."""
    try:
        settings = get_supplier_semantics_settings()
    except Exception:  # pragma: no cover - configuración rota
        return SupplierPolaritySidecarConfig(
            SupplierPolaritySidecarState.ENABLED_BUT_UNAVAILABLE,
            detail="CONFIGURATION_UNAVAILABLE",
        )

    if not settings.enabled:
        return SupplierPolaritySidecarConfig(SupplierPolaritySidecarState.DISABLED)
    return SupplierPolaritySidecarConfig(
        SupplierPolaritySidecarState.ENABLED,
        api_key=settings.api_key,
        model=settings.model,
        timeout_seconds=float(settings.timeout_seconds),
    )


def supplier_polarity_sidecar_enabled() -> bool:
    """Compatibility boolean; wiring consumes the full tri-state snapshot."""
    return supplier_polarity_sidecar_config().state is SupplierPolaritySidecarState.ENABLED


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

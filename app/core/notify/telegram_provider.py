"""Proveedor Telegram del piloto manual del Radar.

Habla con Bot API mediante HTTP JSON y no conoce CLI, scheduler ni persistencia.
La configuración sensible vive exclusivamente en el entorno; las excepciones de
este módulo son categorías estables que nunca incorporan URL, token, destinatario,
texto ni cuerpo de respuesta.
"""
from __future__ import annotations

import os
import http.client
import json as json_module
import ssl
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    NO_EXTERNAL_EFFECT,
    ProviderRequest,
    ProviderResult,
)


TELEGRAM_BOT_TOKEN_ENV = "SECOPPAL_TELEGRAM_BOT_TOKEN"
TELEGRAM_ALLOWED_CHAT_IDS_ENV = "SECOPPAL_TELEGRAM_ALLOWED_CHAT_IDS"
TELEGRAM_API_HOST = "api.telegram.org"
TELEGRAM_TIMEOUT_SECONDS = 10.0

SAFE_INVALID_ADDRESS = "telegram_recipient_not_available"
SAFE_RATE_LIMITED = "telegram_rate_limited"
SAFE_AUTH_ERROR = "telegram_authentication_failed"
SAFE_AMBIGUOUS = "telegram_delivery_outcome_unknown"
SAFE_CONFIGURATION = "telegram_configuration_invalid"
SAFE_API_RESPONSE = "telegram_api_response_invalid"


class TelegramError(Exception):
    """Base de errores seguros del adaptador Telegram."""


class TelegramConfigurationError(TelegramError):
    """Falta configuración obligatoria, sin revelar cuál era su contenido."""


class TelegramAmbiguousDeliveryError(TelegramError):
    """No se puede afirmar si Telegram produjo un efecto externo."""


class TelegramAPIResponseError(TelegramError):
    """Una operación de bootstrap recibió una respuesta no utilizable."""


class TelegramHTTPResponse(Protocol):
    status_code: int

    def json(self) -> Any:
        ...


class TelegramHTTPTransport(Protocol):
    def __call__(
        self, path: str, *, json: Mapping[str, Any], timeout: float
    ) -> TelegramHTTPResponse:
        ...


class _BufferedHTTPResponse:
    """Respuesta ya leída; nunca conserva socket, host, ruta ni request."""

    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self._body = body

    def __repr__(self) -> str:
        return f"{type(self).__name__}(status_code={self.status_code!r})"

    def json(self) -> Any:
        return json_module.loads(self._body.decode("utf-8"))


class _TelegramTransportError(Exception):
    """Error interno estable; reemplaza cualquier excepción cruda de red/TLS."""


def _https_post(
    path: str, *, json: Mapping[str, Any], timeout: float
) -> TelegramHTTPResponse:
    """POST JSON por HTTPS al único host autorizado, con cierre garantizado."""
    connection: http.client.HTTPSConnection | None = None
    failed = False
    result: _BufferedHTTPResponse | None = None
    try:
        body = json_module.dumps(
            dict(json), ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        connection = http.client.HTTPSConnection(
            TELEGRAM_API_HOST,
            timeout=timeout,
            context=ssl.create_default_context(),
        )
        connection.set_debuglevel(0)
        connection.request(
            "POST",
            path,
            body=body,
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        result = _BufferedHTTPResponse(response.status, response.read())
    except Exception:
        failed = True
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                failed = True
    if failed or result is None:
        raise _TelegramTransportError("telegram_transport_failed")
    return result


@dataclass(frozen=True)
class TelegramConfiguration:
    token: str = field(repr=False)
    allowed_chat_ids: frozenset[str] = field(repr=False)

    @classmethod
    def from_environment(cls) -> "TelegramConfiguration":
        token = (os.environ.get(TELEGRAM_BOT_TOKEN_ENV) or "").strip()
        raw_allowed = os.environ.get(TELEGRAM_ALLOWED_CHAT_IDS_ENV) or ""
        allowed = frozenset(
            item.strip() for item in raw_allowed.split(",") if item.strip()
        )
        if not token:
            raise TelegramConfigurationError(SAFE_CONFIGURATION)
        return cls(token=token, allowed_chat_ids=allowed)


class TelegramBotAPI:
    """Cliente pequeño de Bot API compartido por provider y bootstrap."""

    def __init__(
        self,
        configuration: TelegramConfiguration,
        *,
        transport: TelegramHTTPTransport | None = None,
        timeout: float = TELEGRAM_TIMEOUT_SECONDS,
    ) -> None:
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError("telegram_timeout_must_be_positive")
        self._token = configuration.token
        self._allowed_chat_ids = configuration.allowed_chat_ids
        self._transport = transport or _https_post
        self._timeout = float(timeout)

    @classmethod
    def from_environment(
        cls,
        *,
        transport: TelegramHTTPTransport | None = None,
        timeout: float = TELEGRAM_TIMEOUT_SECONDS,
    ) -> "TelegramBotAPI":
        return cls(
            TelegramConfiguration.from_environment(),
            transport=transport,
            timeout=timeout,
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(timeout={self._timeout!r})"

    def is_allowed(self, chat_id: str) -> bool:
        return chat_id in self._allowed_chat_ids

    def request(
        self, method: str, payload: Mapping[str, Any] | None = None
    ) -> TelegramHTTPResponse:
        path = f"/bot{self._token}/{method}"
        failed = False
        try:
            response = self._transport(
                path, json=dict(payload or {}), timeout=self._timeout
            )
        except Exception:
            # Se levanta fuera del bloque except para no conservar como __context__
            # una excepción cuya request pudiera contener token, chat o texto.
            failed = True
        if failed:
            raise TelegramAmbiguousDeliveryError(SAFE_AMBIGUOUS)
        return response


def decode_json_object(response: TelegramHTTPResponse) -> dict[str, Any]:
    """Decodifica sin propagar cuerpo crudo ni errores del cliente HTTP."""
    failed = False
    try:
        value = response.json()
    except Exception:
        failed = True
        value = None
    if failed or not isinstance(value, dict):
        raise TelegramAPIResponseError(SAFE_API_RESPONSE)
    return value


_INVALID_ADDRESS_MARKERS = (
    "bot was blocked by the user",
    "bot can't initiate conversation with a user",
    "bot was kicked from the group chat",
    "chat not found",
    "recipient not found",
    "user is deactivated",
    "user not found",
)


def _is_unambiguous_invalid_address(data: Mapping[str, Any]) -> bool:
    description = data.get("description")
    if not isinstance(description, str):
        return False
    normalized = " ".join(description.casefold().split())
    return any(marker in normalized for marker in _INVALID_ADDRESS_MARKERS)


def _aware_utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TelegramProvider:
    """Implementación real de ``NotificationProvider`` mediante ``sendMessage``."""

    def __init__(
        self,
        *,
        transport: TelegramHTTPTransport | None = None,
        clock: Callable[[], datetime] = _aware_utc_now,
        timeout: float = TELEGRAM_TIMEOUT_SECONDS,
    ) -> None:
        configuration = TelegramConfiguration.from_environment()
        if not configuration.allowed_chat_ids:
            raise TelegramConfigurationError(SAFE_CONFIGURATION)
        self._api = TelegramBotAPI(
            configuration, transport=transport, timeout=timeout
        )
        self._clock = clock

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"

    def is_allowed_recipient(self, external_recipient_id: str) -> bool:
        return self._api.is_allowed(external_recipient_id)

    def send(self, request: ProviderRequest) -> ProviderResult:
        if not self.is_allowed_recipient(request.external_recipient_id):
            return ProviderResult(
                effect=NO_EXTERNAL_EFFECT,
                retryable=False,
                error_class="invalid_address",
                error_message_safe=SAFE_INVALID_ADDRESS,
            )

        response = self._api.request(
            "sendMessage",
            {"chat_id": request.external_recipient_id, "text": request.text},
        )
        invalid_status = False
        try:
            status = response.status_code
        except Exception:
            invalid_status = True
            status = 0
        if invalid_status:
            raise TelegramAmbiguousDeliveryError(SAFE_AMBIGUOUS)

        if status == 401:
            return ProviderResult(
                effect=NO_EXTERNAL_EFFECT,
                retryable=False,
                error_class="auth_error",
                error_message_safe=SAFE_AUTH_ERROR,
            )
        if status >= 500:
            raise TelegramAmbiguousDeliveryError(SAFE_AMBIGUOUS)

        invalid_json = False
        try:
            data = decode_json_object(response)
        except TelegramAPIResponseError:
            invalid_json = True
            data = {}
        if invalid_json:
            raise TelegramAmbiguousDeliveryError(SAFE_AMBIGUOUS)

        if status == 429:
            parameters = data.get("parameters")
            seconds = parameters.get("retry_after") if isinstance(parameters, dict) else None
            if type(seconds) is not int or seconds <= 0:
                raise TelegramAmbiguousDeliveryError(SAFE_AMBIGUOUS)
            now = self._clock()
            if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
                raise TelegramAmbiguousDeliveryError(SAFE_AMBIGUOUS)
            return ProviderResult(
                effect=NO_EXTERNAL_EFFECT,
                retryable=True,
                error_class="rate_limited",
                error_message_safe=SAFE_RATE_LIMITED,
                retry_after=now + timedelta(seconds=seconds),
            )

        if status in (400, 403) and _is_unambiguous_invalid_address(data):
            return ProviderResult(
                effect=NO_EXTERNAL_EFFECT,
                retryable=False,
                error_class="invalid_address",
                error_message_safe=SAFE_INVALID_ADDRESS,
            )

        if status == 200 and data.get("ok") is True:
            result = data.get("result")
            message_id = result.get("message_id") if isinstance(result, dict) else None
            if type(message_id) is int and message_id > 0:
                return ProviderResult(
                    effect=CONFIRMED_EXTERNAL_EFFECT,
                    retryable=False,
                    provider_message_id=str(message_id),
                )

        raise TelegramAmbiguousDeliveryError(SAFE_AMBIGUOUS)


__all__ = [
    "SAFE_AMBIGUOUS",
    "SAFE_API_RESPONSE",
    "SAFE_AUTH_ERROR",
    "SAFE_CONFIGURATION",
    "SAFE_INVALID_ADDRESS",
    "SAFE_RATE_LIMITED",
    "TELEGRAM_ALLOWED_CHAT_IDS_ENV",
    "TELEGRAM_BOT_TOKEN_ENV",
    "TELEGRAM_TIMEOUT_SECONDS",
    "TelegramAPIResponseError",
    "TelegramAmbiguousDeliveryError",
    "TelegramBotAPI",
    "TelegramConfiguration",
    "TelegramConfigurationError",
    "TelegramError",
    "TelegramHTTPResponse",
    "TelegramHTTPTransport",
    "TelegramProvider",
    "decode_json_object",
]

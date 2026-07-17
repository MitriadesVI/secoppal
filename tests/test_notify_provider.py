"""Tests de app/core/notify/provider.py — el contrato con el mundo exterior (R2C).

Puros: sin base de datos, sin reloj, sin red. Lo que se prueba aquí es que el
contrato **no se puede** violar, no que alguien recuerde no violarlo.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    EFFECTS,
    NO_EXTERNAL_EFFECT,
    POSSIBLE_EXTERNAL_EFFECT,
    ProviderContractError,
    ProviderRequest,
    ProviderResult,
)
from app.core.notify.schema import ERROR_CLASSES

AWARE = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)


def _request(**overrides) -> ProviderRequest:
    fields = {
        "notification_id": "n1",
        "attempt_id": "a1",
        "channel": "telegram",
        "external_idempotency_key": "key-1",
        "external_recipient_id": "chat-ana-12345",
        "text": "🔔 Radar: PAE\n1 novedad",
        "payload_version": "p1b-payload-v2",
    }
    fields.update(overrides)
    return ProviderRequest(**fields)


# ══ Efectos ══════════════════════════════════════════════════════════════════════

def test_los_tres_efectos_son_el_vocabulario_completo():
    assert EFFECTS == (
        NO_EXTERNAL_EFFECT, POSSIBLE_EXTERNAL_EFFECT, CONFIRMED_EXTERNAL_EFFECT
    )


def test_effect_desconocido_es_error_de_contrato():
    with pytest.raises(ProviderContractError, match="effect inválido"):
        ProviderResult(effect="probably_sent")


# ══ M1 · possible_external_effect nunca es reintentable ══════════════════════════

def test_possible_external_effect_con_retryable_true_revienta():
    """**Mata M1.** Si no se sabe si el mensaje salió, reintentar es apostar."""
    with pytest.raises(ProviderContractError, match="no apuesta"):
        ProviderResult(effect=POSSIBLE_EXTERNAL_EFFECT, retryable=True)


def test_possible_external_effect_con_retryable_false_es_valido():
    result = ProviderResult(effect=POSSIBLE_EXTERNAL_EFFECT, retryable=False)
    assert result.retryable is False


def test_confirmed_external_effect_con_retryable_true_revienta():
    with pytest.raises(ProviderContractError, match="duplicado"):
        ProviderResult(effect=CONFIRMED_EXTERNAL_EFFECT, retryable=True)


def test_no_external_effect_admite_ambos_retryable():
    assert ProviderResult(effect=NO_EXTERNAL_EFFECT, retryable=True).retryable is True
    assert ProviderResult(effect=NO_EXTERNAL_EFFECT, retryable=False).retryable is False


def test_retryable_debe_ser_bool():
    with pytest.raises(ProviderContractError, match="retryable debe ser bool"):
        ProviderResult(effect=NO_EXTERNAL_EFFECT, retryable=1)


def test_retryable_por_defecto_es_false():
    """El default seguro es no reintentar: quien quiera un reintento lo pide."""
    assert ProviderResult(effect=NO_EXTERNAL_EFFECT).retryable is False


# ══ retry_after ══════════════════════════════════════════════════════════════════

def test_retry_after_naive_revienta():
    with pytest.raises(ProviderContractError, match="zona horaria"):
        ProviderResult(
            effect=NO_EXTERNAL_EFFECT, retryable=True,
            retry_after=datetime(2026, 7, 16, 10, 0),
        )


def test_retry_after_aware_es_valido():
    result = ProviderResult(
        effect=NO_EXTERNAL_EFFECT, retryable=True, retry_after=AWARE
    )
    assert result.retry_after == AWARE


def test_retry_after_no_datetime_revienta():
    with pytest.raises(ProviderContractError, match="retry_after debe ser datetime"):
        ProviderResult(effect=NO_EXTERNAL_EFFECT, retryable=True, retry_after=30)


def test_retry_after_con_otro_offset_es_valido():
    """Aware es aware: el contrato pide zona, no pide UTC."""
    bogota = timezone(timedelta(hours=-5))
    result = ProviderResult(
        effect=NO_EXTERNAL_EFFECT, retryable=True,
        retry_after=datetime(2026, 7, 16, 5, 0, tzinfo=bogota),
    )
    assert result.retry_after.utcoffset() == timedelta(hours=-5)


# ══ M9 · PII fuera del repr ══════════════════════════════════════════════════════

def test_repr_de_request_no_contiene_destinatario_ni_texto():
    """**Mata M9.** Este objeto es justo lo que alguien vuelca a un log al depurar."""
    text = repr(_request())
    assert "chat-ana-12345" not in text
    assert "novedad" not in text
    assert "Radar" not in text
    # Lo que sí debe estar: ids opacos que explican qué pasó sin decir a quién.
    assert "n1" in text and "a1" in text and "telegram" in text


def test_request_no_tiene_campo_para_el_claim_token():
    """El token es la prueba de propiedad del batch. Que no exista el campo es lo que
    hace imposible filtrarlo por aquí."""
    assert not hasattr(_request(), "claim_token")
    with pytest.raises(TypeError):
        _request(claim_token="secreto")


def test_repr_de_result_no_contiene_texto_libre_del_proveedor():
    result = ProviderResult(
        effect=NO_EXTERNAL_EFFECT, retryable=True,
        error_class="network_error", error_message_safe="transient_network_failure",
    )
    assert "chat-ana" not in repr(result)


def test_request_es_inmutable():
    request = _request()
    with pytest.raises(Exception):
        request.text = "otro"


def test_result_es_inmutable():
    result = ProviderResult(effect=NO_EXTERNAL_EFFECT)
    with pytest.raises(Exception):
        result.effect = CONFIRMED_EXTERNAL_EFFECT


# ══ ADR-053-corrección · error_class unido al vocabulario persistente ═══════════
#
# Sin esto, un proveedor podía construir un ProviderResult con una clase que
# ``provider.py`` aceptaba pero que la base rechazaba: el efecto observable era que
# un "no hubo efecto, reintentable" conocido y barato reventaba dentro de
# ``finish_attempt`` —después de haber llamado al proveedor—, dejaba el intento
# ``dispatching`` y la siguiente corrida lo reconciliaba como ``ambiguous``. La
# validación tiene que vivir aquí, al construir, no esperar a que SQLite lo
# descubra tres capas más abajo.

def test_a_error_class_desconocido_revienta_al_construir():
    with pytest.raises(ProviderContractError, match="error_class inválido"):
        ProviderResult(
            effect=NO_EXTERNAL_EFFECT, retryable=True, error_class="dns_error"
        )


def test_b_todas_las_error_classes_persistentes_son_aceptadas():
    """El vocabulario que ``provider.py`` admite es exactamente el que la base
    admite — ni más corto (rechazaría clases legítimas) ni más largo (aceptaría
    clases que luego revientan)."""
    for klass in ERROR_CLASSES:
        result = ProviderResult(
            effect=NO_EXTERNAL_EFFECT, retryable=True, error_class=klass
        )
        assert result.error_class == klass


def test_error_class_none_sigue_siendo_valido():
    assert ProviderResult(effect=CONFIRMED_EXTERNAL_EFFECT).error_class is None


def test_provider_y_schema_comparten_una_sola_lista():
    """No se duplica el vocabulario: ``provider.py`` importa ``ERROR_CLASSES`` de
    ``schema.py`` en vez de declarar su propia copia, que es justo lo que
    desincronizaría los dos lados."""
    import app.core.notify.provider as provider_module

    assert provider_module.ERROR_CLASSES is ERROR_CLASSES


# ══ ADR-053-corrección 2 · error_message_safe fuera del repr ════════════════════

def test_repr_de_result_no_expone_error_message_safe():
    """**Mata la mutación de repr.** ``error_message_safe`` es el único campo del
    contrato cuyo contenido no está acotado por un vocabulario cerrado: "seguro" es
    una promesa de quien lo escribe, no algo que este tipo pueda verificar. El
    sentinel imita justo lo que un implementador descuidado acaba metiendo ahí — una
    dirección real y un fragmento del mensaje—, y no puede aparecer en el repr aunque
    el campo siga conservándolo intacto.

    El test anterior no probaba esto: usaba un ``error_message_safe`` que ya era una
    categoría inocua y luego buscaba una cadena que nunca estuvo ahí, así que pasaba
    igual con el campo siendo impreso.
    """
    sentinel = "falló para ana@example.com: cuerpo secreto"
    result = ProviderResult(
        effect=NO_EXTERNAL_EFFECT, retryable=True,
        error_class="network_error", error_message_safe=sentinel,
    )

    assert sentinel not in repr(result)
    assert "ana@example.com" not in repr(result)
    assert "cuerpo secreto" not in repr(result)
    # El valor se conserva: no se imprime, pero se persiste.
    assert result.error_message_safe == sentinel
    # Y lo que sí queda es el vocabulario cerrado, que explica qué pasó sin decir a quién.
    assert "network_error" in repr(result)

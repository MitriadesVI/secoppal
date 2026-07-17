"""Tests de app/core/notify/fake_provider.py (R2C).

El fake es un instrumento de medida, así que aquí se calibra el instrumento: si
``external_effect_count`` mintiera, todos los tests del worker que dependen de él
pasarían sin probar nada.
"""
from __future__ import annotations

import threading

import pytest

from app.core.notify.fake_provider import (
    MODE_ACCEPT,
    MODE_PERMANENT_BEFORE_EFFECT,
    MODE_RAISE_UNCLASSIFIED,
    MODE_RETRY_BEFORE_EFFECT,
    MODE_TIMEOUT_AFTER_EFFECT,
    FakeProvider,
    FakeProviderError,
    UnclassifiedProviderError,
)
from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    NO_EXTERNAL_EFFECT,
    POSSIBLE_EXTERNAL_EFFECT,
    ProviderRequest,
)

JOIN_TIMEOUT = 30


def _request(key="key-1", **overrides) -> ProviderRequest:
    fields = {
        "notification_id": "n1",
        "attempt_id": "a1",
        "channel": "telegram",
        "external_idempotency_key": key,
        "external_recipient_id": "chat-ana-12345",
        "text": "🔔 Radar: PAE",
        "payload_version": "p1b-payload-v2",
    }
    fields.update(overrides)
    return ProviderRequest(**fields)


# ══ Semántica de los modos ═══════════════════════════════════════════════════════

def test_accept_confirma_efecto_externo():
    provider = FakeProvider([MODE_ACCEPT])
    result = provider.send(_request())
    assert result.effect == CONFIRMED_EXTERNAL_EFFECT
    assert result.retryable is False
    assert result.provider_message_id
    assert provider.external_effect_count == 1


def test_retry_before_effect_no_produce_efecto():
    provider = FakeProvider([MODE_RETRY_BEFORE_EFFECT])
    result = provider.send(_request())
    assert result.effect == NO_EXTERNAL_EFFECT
    assert result.retryable is True
    assert provider.external_effect_count == 0


def test_permanent_before_effect_no_produce_efecto_y_no_es_reintentable():
    provider = FakeProvider([MODE_PERMANENT_BEFORE_EFFECT])
    result = provider.send(_request())
    assert result.effect == NO_EXTERNAL_EFFECT
    assert result.retryable is False
    assert result.error_class == "invalid_address"
    assert provider.external_effect_count == 0


def test_timeout_after_effect_si_produce_efecto_y_no_es_reintentable():
    """El mensaje salió y la respuesta se perdió. Si el efecto no contara, un worker
    que reintentara tras un timeout parecería correcto."""
    provider = FakeProvider([MODE_TIMEOUT_AFTER_EFFECT])
    result = provider.send(_request())
    assert result.effect == POSSIBLE_EXTERNAL_EFFECT
    assert result.retryable is False
    assert provider.external_effect_count == 1


def test_raise_unclassified_lanza():
    provider = FakeProvider([MODE_RAISE_UNCLASSIFIED])
    with pytest.raises(UnclassifiedProviderError):
        provider.send(_request())
    assert provider.external_effect_count == 0


def test_modo_desconocido_es_error_de_uso():
    with pytest.raises(FakeProviderError, match="Modo desconocido"):
        FakeProvider(["teletransporte"])


# ══ M12 · Idempotencia ═══════════════════════════════════════════════════════════

def test_misma_clave_dos_veces_produce_un_solo_efecto():
    """**Mata M12.** Es la propiedad que da sentido a todo el bloque."""
    provider = FakeProvider([MODE_ACCEPT, MODE_ACCEPT])
    first = provider.send(_request(key="k"))
    second = provider.send(_request(key="k"))
    assert first.provider_message_id == second.provider_message_id
    assert provider.external_effect_count == 1
    assert provider.call_count == 2, "las dos llamadas se registran; el efecto es uno"


def test_claves_distintas_producen_efectos_distintos():
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    a = provider.send(_request(key="k1"))
    b = provider.send(_request(key="k2"))
    assert a.provider_message_id != b.provider_message_id
    assert provider.external_effect_count == 2


def test_clave_aceptada_se_deduplica_aunque_el_modo_diga_fallar():
    """La deduplicación va antes que el modo: así se comporta un proveedor idempotente
    real, y sin eso el escenario 'reintento tras timeout' no se podría distinguir de
    un duplicado."""
    provider = FakeProvider([MODE_ACCEPT, MODE_PERMANENT_BEFORE_EFFECT])
    first = provider.send(_request(key="k"))
    second = provider.send(_request(key="k"))
    assert second.effect == CONFIRMED_EXTERNAL_EFFECT
    assert second.provider_message_id == first.provider_message_id
    assert provider.external_effect_count == 1


def test_timeout_after_effect_repetido_cuenta_dos_efectos():
    """El fake no oculta un duplicado real: un timeout no registra la clave como
    aceptada —nadie confirmó nada—, así que una segunda llamada vuelve a tener
    efecto. Es justo la razón por la que el worker no debe reintentar."""
    provider = FakeProvider([MODE_TIMEOUT_AFTER_EFFECT, MODE_TIMEOUT_AFTER_EFFECT])
    provider.send(_request(key="k"))
    provider.send(_request(key="k"))
    assert provider.external_effect_count == 2


# ══ Configuración ════════════════════════════════════════════════════════════════

def test_secuencia_se_consume_en_orden_y_repite_el_ultimo():
    provider = FakeProvider([MODE_RETRY_BEFORE_EFFECT, MODE_ACCEPT])
    assert provider.send(_request(key="k1")).effect == NO_EXTERNAL_EFFECT
    assert provider.send(_request(key="k2")).effect == CONFIRMED_EXTERNAL_EFFECT
    assert provider.send(_request(key="k3")).effect == CONFIRMED_EXTERNAL_EFFECT


def test_mode_by_key_gana_sobre_la_secuencia():
    provider = FakeProvider(
        [MODE_ACCEPT], mode_by_key={"malo": MODE_PERMANENT_BEFORE_EFFECT}
    )
    assert provider.send(_request(key="malo")).retryable is False
    assert provider.send(_request(key="bueno")).effect == CONFIRMED_EXTERNAL_EFFECT


def test_registra_claves_y_llamadas():
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    provider.send(_request(key="k1"))
    provider.send(_request(key="k2"))
    assert provider.seen_keys == ["k1", "k2"]
    assert [call.notification_id for call in provider.calls] == ["n1", "n1"]


# ══ M9 · El instrumento no filtra ════════════════════════════════════════════════

def test_repr_del_fake_no_contiene_pii():
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    provider.send(_request())
    text = repr(provider)
    assert "chat-ana-12345" not in text and "Radar" not in text


def test_la_llamada_registrada_no_guarda_texto_ni_destinatario():
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    provider.send(_request())
    call = provider.calls[0]
    assert not hasattr(call, "text")
    assert not hasattr(call, "external_recipient_id")
    assert "chat-ana-12345" not in repr(call)


# ══ Concurrencia ═════════════════════════════════════════════════════════════════

def test_el_contador_no_pierde_incrementos_bajo_concurrencia():
    """Sin esto, una prueba de concurrencia del worker podría pasar por un contador
    roto en vez de por un worker correcto."""
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    barrier = threading.Barrier(8)

    def send(index: int) -> None:
        barrier.wait(timeout=JOIN_TIMEOUT)
        provider.send(_request(key=f"k{index}"))

    threads = [threading.Thread(target=send, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=JOIN_TIMEOUT)
        assert not thread.is_alive()
    assert provider.external_effect_count == 8
    assert provider.call_count == 8


def test_misma_clave_desde_varios_hilos_produce_un_solo_efecto():
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    barrier = threading.Barrier(8)

    def send() -> None:
        barrier.wait(timeout=JOIN_TIMEOUT)
        provider.send(_request(key="compartida"))

    threads = [threading.Thread(target=send) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=JOIN_TIMEOUT)
        assert not thread.is_alive()
    assert provider.external_effect_count == 1
    assert len({call.mode for call in provider.calls}) == 1

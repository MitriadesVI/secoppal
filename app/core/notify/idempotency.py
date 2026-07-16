"""idempotency.py — identidad determinista de un batch de notificación.

Funciones puras, sin I/O: dado el mismo conjunto de ``event_seq`` para el
mismo ``(endpoint_id, radar_id)`` bajo la misma política, siempre se obtiene
la misma ``idempotency_key`` — sin importar el orden de entrada, el texto
renderizado o cuándo se calcula. Esa estabilidad es lo que permite que
``UNIQUE(endpoint_id, idempotency_key)`` en ``outbox_store`` detecte "el mismo
batch lógico" en vez de "el mismo batch calculado con la misma prisa".
"""
from __future__ import annotations

import hashlib

# Versión de la política de agrupación/deduplicación. Vive solo aquí —no se
# duplica en outbox_store ni en tests— porque un segundo literal es una
# oportunidad de que diverjan y la deduplicación deje de ser determinista.
NOTIFICATION_POLICY_VERSION = "p1b-v1"


def canonical_event_set(event_seqs) -> str:
    """Serializa un conjunto de ``event_seq`` de forma estable.

    Deduplica, valida que cada secuencia sea un entero positivo (``bool``
    rechazado explícitamente pese a ser subclase de ``int`` en Python; ``float``
    o ``str`` también rechazados) y ordena antes de unir con comas —sin
    espacios—, para que el mismo conjunto produzca siempre el mismo texto sin
    importar en qué orden llegó ni cuántas veces se repitió una clave. Coaccionar
    tipos aquí (``int("1")``, ``int(1.9)``) dejaría que dos llamadas con datos
    distintos por error de tipo terminaran en la misma clave de idempotencia.
    """
    seqs: set[int] = set()
    for seq in event_seqs:
        if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
            raise ValueError(
                f"event_seq debe ser un entero positivo; recibido {seq!r}."
            )
        seqs.add(seq)
    if not seqs:
        raise ValueError(
            "El conjunto de event_seq no puede estar vacío: un batch sin "
            "eventos no es una intención de entrega, es un envío vacío."
        )
    return ",".join(str(seq) for seq in sorted(seqs))


def idempotency_key(
    *,
    endpoint_id: str,
    radar_id: str,
    event_seqs,
    policy_version: str = NOTIFICATION_POLICY_VERSION,
) -> str:
    """sha256(policy_version|endpoint_id|radar_id|canonical_event_set).

    No depende de ``payload_json``, del texto renderizado, de timestamps ni de
    min/max del conjunto: cualquiera de esas cosas puede variar entre dos
    intentos de construir "el mismo" batch, y la clave debe seguir
    coincidiendo para que la deduplicación funcione.
    """
    canonical = canonical_event_set(event_seqs)
    raw = f"{policy_version}|{endpoint_id}|{radar_id}|{canonical}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def batch_key(*, radar_id: str, endpoint_id: str) -> str:
    """Etiqueta gruesa para inspección/filtrado. No es única: no deduplica."""
    return f"{radar_id}:{endpoint_id}"

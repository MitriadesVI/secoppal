"""Agrupación neutral de resultados SECOP por identidad estable (Bloque G).

Sustituye el dedupe silencioso por una operación explícita. Antes, cuando dos
filas compartían ``id_del_proceso``, la segunda simplemente desaparecía: nadie
podía distinguir "SECOP devolvió una fila" de "SECOP devolvió tres filas para
el mismo proceso y nos quedamos con la primera". Ese conteo perdido es
información real de la fuente, y este módulo lo conserva.

Qué NO decide este módulo, por decisión explícita:

- **La identidad.** La resuelve ``app.core.result_identity`` (RG-1). Aquí solo
  se agrupa por el resultado de esa autoridad.
- **El orden.** La secuencia de entrada ya viene rankeada; los grupos salen en
  el orden de sus representantes y nada se reordena.
- **Qué significa el fan-out.** ``physical_row_count > 1`` se conserva como
  observación cruda. Varias filas físicas NO demuestran varios lotes, ni varias
  adjudicaciones, ni nada: solo que la fuente devolvió varias filas bajo la
  misma identidad. Interpretarlo es trabajo de otra capa, con su propia
  evidencia.

Módulo puro: sin I/O, red, reloj, configuración ni feature flags. No muta las
filas recibidas ni les inyecta claves: la metadata del grupo viaja por fuera,
como sidecar, para que una fila SECOP siga siendo exactamente lo que la fuente
entregó.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from app.core.result_identity import ResultIdentity, result_identity

__all__ = [
    "GROUPING_CONTRACT_VERSION",
    "Occurrence",
    "ResultGroup",
    "WindowOrigin",
    "group_results",
    "grouping_metrics",
    "group_to_dict",
    "group_from_dict",
    "groups_to_dicts",
    "groups_from_dicts",
    "merge_group_sequences",
    "representatives",
]

# Versión del contrato de grupo serializado. Vive aquí, junto a la forma que
# describe, y no en el buffer: quien serializa grupos es este módulo.
# g1-2: cada miembro pasa a llevar ``occurrence_key`` (G.1).
GROUPING_CONTRACT_VERSION = "g1-2"

FanoutStatus = Literal["single", "fanout"]


@dataclass(frozen=True, slots=True)
class WindowOrigin:
    """Contexto de la ventana de resultados SECOP observada.

    Es lo que hace que una ocurrencia sea identificable sin recurrir a la
    igualdad de campos. Dos filas idénticas en la misma ventana ocupan
    posiciones distintas y son dos ocurrencias; la MISMA ventana re-pescada
    produce exactamente las mismas claves, así que reintentar no infla nada.
    """

    query_fingerprint: str = ""
    dataset_id: str = ""
    source_offset: int = 0

    def occurrence_key(self, position: int) -> str:
        """Clave determinística de la ocurrencia en la posición dada."""
        return (
            f"{self.query_fingerprint}|{self.dataset_id}|"
            f"{self.source_offset}|{position}"
        )


@dataclass(frozen=True, slots=True)
class Occurrence:
    """UNA posición observada en una ventana, con su fila asociada.

    ``occurrence_key`` identifica una POSICIÓN dentro de una ventana de
    consulta y su contexto TTL, no una identidad física inmutable ni un hecho
    jurídico. No es identidad, no sirve para
    deduplicar entre consultas y no debe salir de este subsistema como si
    dijera algo sobre el proceso o el contrato: dos ocurrencias distintas
    pueden ser el mismo proceso, y la misma ocurrencia en otra consulta tiene
    otra clave. La identidad jurídica la decide ``result_identity`` (RG-1) y
    vive en ``ResultGroup.stable_key``.
    """

    occurrence_key: str
    row: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ResultGroup:
    """Un grupo de ocurrencias físicas que comparten identidad estable.

    ``members`` incluye SIEMPRE al representante como primer elemento, de modo
    que ``physical_row_count`` cuenta filas de la fuente y nunca queda en cero.
    Un grupo sin fan-out tiene exactamente un miembro, que es su propio
    representante.

    ``representative_row`` es una propiedad derivada de ``members[0]``, no un
    campo: así la regla "el representante es la primera ocurrencia del orden
    canónico" se cumple por construcción, y añadir miembros al final no puede
    cambiarlo por accidente.

    Las filas se guardan por referencia, no por copia: agrupar es una lectura,
    y copiar aquí escondería mutaciones aguas arriba en vez de prevenirlas.
    """

    dataset_id: str
    stable_key: str | None
    identity_field: str | None
    identity_status: Literal["stable", "unstable"]
    identity_reason_codes: tuple[str, ...]
    members: tuple[Occurrence, ...]

    @property
    def representative_row(self) -> Mapping[str, Any]:
        """La primera ocurrencia del grupo en el orden canónico."""
        return self.members[0].row

    @property
    def member_rows(self) -> tuple[Mapping[str, Any], ...]:
        """Las filas de todas las ocurrencias, en orden de llegada."""
        return tuple(member.row for member in self.members)

    @property
    def occurrence_keys(self) -> tuple[str, ...]:
        return tuple(member.occurrence_key for member in self.members)

    @property
    def physical_row_count(self) -> int:
        """Cuántas filas devolvió la fuente bajo esta identidad."""
        return len(self.members)

    @property
    def fanout_status(self) -> FanoutStatus:
        """``fanout`` cuando la fuente dio más de una fila. Sin interpretar."""
        return "fanout" if len(self.members) > 1 else "single"


def _group_from_identity(
    identity: ResultIdentity, members: Sequence[Occurrence],
) -> ResultGroup:
    return ResultGroup(
        dataset_id=identity.dataset_id,
        stable_key=identity.stable_key,
        identity_field=identity.identity_field,
        identity_status=identity.identity_status,
        identity_reason_codes=identity.reason_codes,
        members=tuple(members),
    )


def group_results(
    rows: Iterable[Mapping[str, Any]] | None,
    dataset_id: str,
    *,
    origin: WindowOrigin | None = None,
) -> tuple[ResultGroup, ...]:
    """Agrupa ``rows`` por identidad estable conservando el orden canónico.

    Reglas, todas verificables en ``tests/test_result_grouping.py``:

    1. La entrada es la secuencia YA rankeada. No se reordena nada.
    2. La primera fila de cada identidad, en ese orden, es el representante.
    3. El orden de los grupos es el orden de sus representantes.
    4. Una fila sin ``stable_key`` forma un grupo singleton propio.
    5. Dos filas sin identidad NUNCA se fusionan — aunque sean idénticas campo
       por campo. Sin identidad no hay evidencia de que sean el mismo hecho, y
       fusionarlas inventaría esa evidencia.
    6. Dos filas idénticas campo por campo bajo la MISMA identidad observadas
       en posiciones distintas son dos ocurrencias, no una; ambas se conservan.

    ``origin`` ancla las claves de ocurrencia a la ventana de la que salieron.
    Sin él, las claves solo distinguen posiciones DENTRO de esta llamada —
    suficiente para la primera página, insuficiente para mezclar dos ventanas.
    Quien pagina debe pasarlo; ``pagination._fetch_window`` lo hace.

    El índice usado es la posición en la secuencia canónica recibida, no en el
    JSON crudo de SECOP. Ambas son biyectivas y el ranking es determinista, así
    que re-procesar la misma ventana reproduce exactamente las mismas claves.
    """
    window = origin if origin is not None else WindowOrigin(dataset_id=dataset_id)
    # ``order`` fija la secuencia de salida: cada entrada apunta al grupo
    # estable por su clave, o al singleton inestable por su posición.
    order: list[tuple[str, str | int]] = []
    by_key: dict[str, list[Occurrence]] = {}
    identities: dict[str, ResultIdentity] = {}
    singletons: list[ResultGroup] = []

    for position, row in enumerate(rows or ()):
        identity = result_identity(row, dataset_id)
        occurrence = Occurrence(window.occurrence_key(position), row)
        key = identity.stable_key
        if key is None:
            # Regla 5: cada fila sin identidad es su propio grupo y ocupa su
            # propia posición en la salida.
            order.append(("unstable", len(singletons)))
            singletons.append(_group_from_identity(identity, [occurrence]))
            continue
        if key in by_key:
            # Regla 6: se anexa como ocurrencia adicional del mismo grupo.
            by_key[key].append(occurrence)
            continue
        by_key[key] = [occurrence]
        identities[key] = identity
        order.append(("stable", key))

    return tuple(
        singletons[ref] if kind == "unstable"  # type: ignore[index]
        else _group_from_identity(identities[ref], by_key[ref])  # type: ignore[index]
        for kind, ref in order
    )


def representatives(groups: Iterable[ResultGroup]) -> list[Mapping[str, Any]]:
    """La secuencia de filas visibles: un representante por grupo, en orden.

    Debe producir exactamente lo mismo que el dedupe heredado. Esa equivalencia
    es lo que permite que el modo ``off`` sea idéntico a la línea base, y está
    probada por mutación en la suite focal.
    """
    return [group.representative_row for group in groups]


def grouping_metrics(groups: Sequence[ResultGroup]) -> dict[str, int]:
    """Resumen aditivo del grouping. Solo cuenta; no concluye nada."""
    physical = sum(group.physical_row_count for group in groups)
    return {
        "physical_row_count": physical,
        "unique_group_count": len(groups),
        "duplicate_physical_rows": physical - len(groups),
        "unstable_identity_count": sum(
            1 for group in groups if group.identity_status == "unstable"
        ),
        "max_physical_fanout": max(
            (group.physical_row_count for group in groups), default=0
        ),
    }


def merge_group_sequences(
    current: Sequence[ResultGroup],
    incoming: Sequence[ResultGroup],
    *,
    limit: int,
) -> tuple[ResultGroup, ...]:
    """Extiende una secuencia de grupos con otra, sin perder fan-out.

    Es la operación que hace honesto el "muéstrame más": cuando la página
    siguiente trae otra fila física de un grupo YA mostrado, esa fila se suma
    como miembro del grupo existente en vez de descartarse. El dedupe heredado
    del buffer la tiraba, y con ella el conteo físico real.

    ``limit`` acota cuántos GRUPOS se conservan, no cuántas filas: un grupo ya
    admitido puede seguir creciendo aunque el límite esté alcanzado, porque
    negarle miembros falsearía su ``physical_row_count``.

    Dos grupos inestables nunca se fusionan (regla 5), así que cada uno cuenta
    contra el límite por separado.

    Un miembro entrante se reconoce por su ``occurrence_key``, NO por igualdad
    de campos (G.1). Comparar campos colapsaba dos filas idénticas devueltas
    por SECOP en la misma respuesta: eran dos hechos físicos y se contaban como
    uno. Con la clave de ocurrencia:

    - dos filas idénticas observadas en posiciones distintas de la misma
      ventana se conservan las dos;
    - re-procesar la MISMA ventana reproduce las mismas claves, así que un
      reintento no infla el conteo;
    - una ventana posterior tiene otro ``source_offset``, así que una posición
      equivalente observada después sí suma.

    Este contrato no afirma que Socrata ofrezca un snapshot inmutable entre
    lecturas: una ventana que cambie dentro del TTL puede reutilizar posiciones.

    El representante nunca cambia al fusionar: ``members[0]`` ya está fijado y
    los miembros nuevos se anexan al final.
    """
    out: list[ResultGroup] = list(current)
    index: dict[tuple[str, str], int] = {
        (group.dataset_id, group.stable_key): position
        for position, group in enumerate(out)
        if group.stable_key is not None
    }
    # Las claves ya presentes, por grupo. Se mantiene fuera del bucle para que
    # absorber una ventana grande no sea cuadrático.
    known: dict[int, set[str]] = {
        position: set(group.occurrence_keys) for position, group in enumerate(out)
    }
    # Toda ocurrencia ya admitida, sin importar en qué grupo. Es lo que impide
    # que re-procesar una ventana duplique los grupos SIN identidad: como no
    # tienen clave estable por la que reconocerlos, solo su ocurrencia los
    # distingue. Reconocer no es fusionar — dos filas sin identidad de
    # posiciones distintas siguen siendo dos grupos (regla 5).
    admitted: set[str] = {key for keys in known.values() for key in keys}
    for group in incoming:
        if group.stable_key is not None:
            slot = index.get((group.dataset_id, group.stable_key))
            if slot is not None:
                existing = out[slot]
                seen = known[slot]
                added = [m for m in group.members if m.occurrence_key not in seen]
                if added:
                    seen.update(m.occurrence_key for m in added)
                    admitted.update(m.occurrence_key for m in added)
                    out[slot] = ResultGroup(
                        dataset_id=existing.dataset_id,
                        stable_key=existing.stable_key,
                        identity_field=existing.identity_field,
                        identity_status=existing.identity_status,
                        identity_reason_codes=existing.identity_reason_codes,
                        members=existing.members + tuple(added),
                    )
                continue
        elif all(key in admitted for key in group.occurrence_keys):
            continue
        if len(out) >= limit:
            continue
        if group.stable_key is not None:
            index[(group.dataset_id, group.stable_key)] = len(out)
        known[len(out)] = set(group.occurrence_keys)
        admitted.update(group.occurrence_keys)
        out.append(group)
    return tuple(out)


def group_to_dict(group: ResultGroup) -> dict[str, Any]:
    """Serializa un grupo para persistirlo en el buffer de paginación.

    ``physical_row_count`` y ``fanout_status`` se emiten aunque sean derivados:
    quien lea el JSON no debería tener que recalcularlos para saber qué había.
    Al deserializar se vuelven a derivar de ``member_rows``, que es la fuente
    de verdad — un conteo persistido que no cuadre con los miembros se ignora
    en vez de creerse.
    """
    return {
        "dataset_id": group.dataset_id,
        "stable_key": group.stable_key,
        "identity_field": group.identity_field,
        "identity_status": group.identity_status,
        "identity_reason_codes": list(group.identity_reason_codes),
        "members": [
            {"occurrence_key": member.occurrence_key, "row": dict(member.row)}
            for member in group.members
        ],
        "physical_row_count": group.physical_row_count,
        "fanout_status": group.fanout_status,
    }


def group_from_dict(value: Mapping[str, Any]) -> ResultGroup:
    """Reconstruye un grupo persistido. Falla cerrado si no hay miembros.

    Las claves de ocurrencia se restauran verbatim: sin ellas, al extender el
    buffer no habría forma de saber qué ocurrencias ya estaban y la
    multiplicidad se falsearía en cuanto el usuario pidiera "más".
    """
    members = [
        Occurrence(str(member["occurrence_key"]), dict(member["row"]))
        for member in (value.get("members") or [])
    ]
    if not members:
        raise ValueError("un grupo serializado debe conservar al menos un miembro")
    if len({member.occurrence_key for member in members}) != len(members):
        raise ValueError("un grupo serializado no puede repetir occurrence_key")
    status = value.get("identity_status")
    if status not in {"stable", "unstable"}:
        raise ValueError(f"identity_status inválido: {status!r}")
    stable_key = value.get("stable_key")
    if status == "stable" and not isinstance(stable_key, str):
        raise ValueError("un grupo estable requiere stable_key de tipo str")
    if status == "unstable" and stable_key is not None:
        raise ValueError("un grupo inestable no puede tener stable_key")
    return ResultGroup(
        dataset_id=str(value["dataset_id"]),
        stable_key=stable_key,
        identity_field=value.get("identity_field"),
        identity_status=status,
        identity_reason_codes=tuple(value.get("identity_reason_codes") or ()),
        members=tuple(members),
    )


def groups_to_dicts(groups: Iterable[ResultGroup]) -> list[dict[str, Any]]:
    return [group_to_dict(group) for group in groups]


def groups_from_dicts(values: Iterable[Mapping[str, Any]] | None) -> tuple[ResultGroup, ...]:
    return tuple(group_from_dict(value) for value in (values or ()))

"""Proyección de nombres físicos de comprador por dataset (ADR-073).

`EntityResolver` devuelve un `official_name` que cumple a la vez tres papeles:
identidad, display humano y string proyectado contra la columna de entidad del
dataset. DAPRE demuestra que no son el mismo: el canónico
``DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA`` devuelve cero
filas en procesos y en contratos, mientras el nombre físico ``DAPRE`` devuelve
17.128 y 4.351.

Este módulo es la autoridad SEPARADA de esa tercera función. No conoce aliases
(lenguaje humano, autoridad de `EntityResolver`) ni display (autoridad del
formatter). Sólo responde: qué strings guarda ESTE dataset para ESTA identidad.

La comparación downstream es de IGUALDAD EXACTA unida por ``OR``, nunca LIKE:
un `LIKE '%MUNICIPIO DE SOACHA.%'` alcanza por accidente de substring la forma
de dos puntos que publica el otro dataset, que es justamente el defecto que
esta capa viene a eliminar. Por eso los nombres se congelan byte-for-byte:
puntuación, acentos y mayúsculas son parte del binding, no ruido a normalizar.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from app.core.buyer_fiscal_evidence import (
    FiscalAccreditation,
    FiscalEvidence,
    SourceObservation,
    parse_fiscal_evidence,
)
from app.utils.money import normalize_text

logger = logging.getLogger(__name__)

DEFAULT_SIDECAR_PATH = Path(__file__).resolve().parent.parent / "data" / "buyer_source_names.json"

# Datasets con columna de entidad conocida. Un dataset fuera de esta lista es
# un error de carga y no un binding que se ignora en silencio: un id mal
# escrito produciría fallback mudo al LIKE canónico, que es exactamente el
# fallo que ADR-073 prohíbe (INV-NO-SILENT-SEMANTIC-FALLBACK).
KNOWN_DATASET_IDS: frozenset[str] = frozenset({"p6dx-8zbt", "jbjy-vk9h"})

# Versión de schema que este loader sabe leer. Un sidecar con otra versión no
# se interpreta "lo mejor posible": se rechaza. Leer un schema futuro con las
# reglas de hoy es la forma silenciosa de proyectar un filtro que nadie revisó.
#
# Schema 3 separates operational BuyerUnit identity, fiscal attestations,
# source observations and exact source-name projection.
SCHEMA_VERSION = 3

_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


class BuyerSourceNameError(ValueError):
    """El sidecar está malformado. Falla al cargar, no en la primera query."""


@dataclass(frozen=True, slots=True)
class BuyerSourceBinding:
    """One BuyerUnit, keyed operationally by its normalized canonical name."""

    canonical_name: str
    source_names: dict[str, tuple[str, ...]]
    projection_evidence: ProjectionEvidence
    fiscal_evidence: tuple[FiscalEvidence, ...]

    @property
    def fiscal_attestations(self) -> tuple[FiscalAccreditation, ...]:
        return tuple(e for e in self.fiscal_evidence if isinstance(e, FiscalAccreditation))

    @property
    def source_observations(self) -> tuple[SourceObservation, ...]:
        return tuple(e for e in self.fiscal_evidence if isinstance(e, SourceObservation))

    def names_for(self, dataset_id: str) -> tuple[str, ...] | None:
        return self.source_names.get(dataset_id)


@dataclass(frozen=True, slots=True)
class ProjectionEvidence:
    """Provenance of exact source names, independent of fiscal evidence."""

    observed_at: date
    evidence: str
    observed_rows: dict[str, int]


@dataclass(frozen=True, slots=True)
class SidecarProvenance:
    """Metadata del REGISTRO. Deliberadamente sin `evidence`.

    La procedencia de los nombres físicos vive en `projection_evidence` de
    cada BuyerUnit; la evidencia fiscal vive en sus registros tipados. Un
    `evidence` de raíz atribuiría falsamente un mismo artefacto a otros lotes.
    """

    schema_version: int
    authority: str | None


class BuyerSourceNameRegistry:
    """Índice O(1) de identidad canónica normalizada → binding."""

    def __init__(
        self,
        bindings: list[BuyerSourceBinding],
        provenance: SidecarProvenance | None = None,
    ) -> None:
        self.provenance = provenance
        self._by_canonical: dict[str, BuyerSourceBinding] = {}
        for binding in bindings:
            key = normalize_text(binding.canonical_name)
            if key in self._by_canonical:
                raise BuyerSourceNameError(
                    f"canonical duplicado tras normalizar: {binding.canonical_name!r}"
                )
            self._by_canonical[key] = binding

    def __len__(self) -> int:
        return len(self._by_canonical)

    def bindings(self) -> tuple[BuyerSourceBinding, ...]:
        """Todos los bindings, por canónico. Existe para auditar procedencia.

        Recorrer el registro es una pregunta de trazabilidad —"¿qué acredita
        cada binding?"— y no de consulta: el runtime siempre entra por
        `project`, que resuelve una identidad concreta.
        """
        return tuple(
            sorted(self._by_canonical.values(), key=lambda item: item.canonical_name)
        )

    def binding_for(self, canonical_name: str) -> BuyerSourceBinding | None:
        if not canonical_name:
            return None
        return self._by_canonical.get(normalize_text(canonical_name))

    def project(self, canonical_name: str, dataset_id: str) -> tuple[str, ...] | None:
        """Nombres físicos de ``canonical_name`` en ``dataset_id``.

        ``None`` significa "sin binding": el llamador conserva su camino previo
        intacto. Nunca devuelve tupla vacía — eso sería un binding que filtra a
        nada, y se rechaza al cargar.
        """
        binding = self.binding_for(canonical_name)
        if binding is None:
            return None
        return binding.names_for(dataset_id)


def _require_str(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BuyerSourceNameError(f"{field} debe ser un string no vacío, recibido {value!r}")
    return value


def _require_iso_date(value: Any, field: str) -> date:
    """`YYYY-MM-DD` real. `2026-99-99` tiene la forma y no es una fecha."""
    text = _require_str(value, field)
    if not _ISO_DATE_RE.fullmatch(text):
        raise BuyerSourceNameError(f"{field} debe tener formato YYYY-MM-DD, recibido {text!r}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise BuyerSourceNameError(f"{field} no es una fecha válida: {text!r}") from exc


def _parse_observed_rows(
    raw: Any, canonical: str, declared_datasets: set[str],
) -> dict[str, int]:
    """Conteos observados. Opcional; si está, tiene que ser coherente.

    Se exige que cada dataset con filas observadas tenga también nombres
    declarados: medir un dataset que el binding no proyecta es evidencia de
    otra cosa, y aceptarlo en silencio dejaría pasar un sidecar a medio editar.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise BuyerSourceNameError(f"{canonical}: 'observed_rows' debe ser un objeto")

    rows: dict[str, int] = {}
    for dataset_id, value in raw.items():
        if dataset_id not in KNOWN_DATASET_IDS:
            raise BuyerSourceNameError(
                f"{canonical}/observed_rows: dataset desconocido {dataset_id!r}"
            )
        if dataset_id not in declared_datasets:
            raise BuyerSourceNameError(
                f"{canonical}/observed_rows: {dataset_id} no declara nombres físicos"
            )
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise BuyerSourceNameError(
                f"{canonical}/observed_rows/{dataset_id} debe ser un entero >= 0, "
                f"recibido {value!r}"
            )
        rows[dataset_id] = value
    return rows


def _parse_provenance(payload: dict[str, Any]) -> SidecarProvenance:
    raw_version = payload.get("schema_version")
    if isinstance(raw_version, bool) or not isinstance(raw_version, int):
        raise BuyerSourceNameError(
            f"schema_version debe ser un entero, recibido {raw_version!r}"
        )
    if raw_version != SCHEMA_VERSION:
        raise BuyerSourceNameError(
            f"schema_version {raw_version} no soportado; este loader lee {SCHEMA_VERSION}"
        )

    # `evidence` de raíz es un error, no un campo tolerado: si se aceptara en
    # silencio, un lote futuro volvería a declarar procedencia acumulativa que
    # no acredita a los bindings anteriores. Rechazarlo es lo que impide que el
    # defecto reaparezca por copiar el archivo viejo.
    if "evidence" in payload:
        raise BuyerSourceNameError(
            "'evidence' de raíz no existe en schema 3: la procedencia es por BuyerUnit"
        )

    authority = payload.get("authority")
    if authority is not None:
        authority = _require_str(authority, "authority")

    return SidecarProvenance(schema_version=raw_version, authority=authority)


def _parse_sources(raw: Any, canonical: str) -> dict[str, tuple[str, ...]]:
    if not isinstance(raw, dict) or not raw:
        raise BuyerSourceNameError(f"{canonical}: 'source_names' debe ser un objeto no vacío")

    sources: dict[str, tuple[str, ...]] = {}
    for dataset_id, names in raw.items():
        if dataset_id not in KNOWN_DATASET_IDS:
            raise BuyerSourceNameError(f"{canonical}: dataset desconocido {dataset_id!r}")
        if not isinstance(names, list):
            raise BuyerSourceNameError(
                f"{canonical}/{dataset_id}: los nombres deben venir en una lista, "
                f"recibido {type(names).__name__}"
            )
        if not names:
            raise BuyerSourceNameError(
                f"{canonical}/{dataset_id}: se declaró el dataset con una lista vacía"
            )
        cleaned: list[str] = []
        for index, name in enumerate(names):
            cleaned.append(_require_str(name, f"{canonical}/{dataset_id}[{index}]"))
        # Duplicados normalizados: dos entradas que sólo difieren en acento o
        # caja no son dos nombres físicos, son una declaración ambigua.
        seen = {normalize_text(name) for name in cleaned}
        if len(seen) != len(cleaned):
            raise BuyerSourceNameError(f"{canonical}/{dataset_id}: nombres duplicados")
        sources[dataset_id] = tuple(cleaned)
    return sources


def parse_registry(payload: Any) -> BuyerSourceNameRegistry:
    """Valida el payload del sidecar. Falla fuerte: nada malformado pasa."""
    if not isinstance(payload, dict):
        raise BuyerSourceNameError("el sidecar debe ser un objeto JSON")

    provenance = _parse_provenance(payload)

    if "identity_key" in payload:
        raise BuyerSourceNameError("'identity_key' de raíz no existe en schema 3")
    raw_bindings = payload.get("buyer_units")
    if not isinstance(raw_bindings, list) or not raw_bindings:
        raise BuyerSourceNameError("'buyer_units' debe ser una lista no vacía")

    bindings: list[BuyerSourceBinding] = []
    for record in raw_bindings:
        if not isinstance(record, dict):
            raise BuyerSourceNameError(f"BuyerUnit inválida: {record!r}")
        canonical = _require_str(record.get("canonical_name"), "canonical_name")
        if "identity_key" in record:
            raise BuyerSourceNameError(f"{canonical}: 'identity_key' no existe en schema 3")
        sources = _parse_sources(record.get("source_names"), canonical)
        raw_projection = record.get("projection_evidence")
        if not isinstance(raw_projection, dict):
            raise BuyerSourceNameError(f"{canonical}: 'projection_evidence' debe ser un objeto")
        try:
            fiscal = parse_fiscal_evidence(record.get("fiscal_evidence"), canonical)
        except ValueError as exc:
            raise BuyerSourceNameError(str(exc)) from exc
        bindings.append(
            BuyerSourceBinding(
                canonical_name=canonical,
                source_names=sources,
                projection_evidence=ProjectionEvidence(
                    observed_at=_require_iso_date(
                        raw_projection.get("observed_at"), f"{canonical}/projection_evidence.observed_at",
                    ),
                    evidence=_require_str(
                        raw_projection.get("evidence"), f"{canonical}/projection_evidence.evidence",
                    ),
                    observed_rows=_parse_observed_rows(
                        raw_projection.get("observed_rows"), canonical, set(sources),
                    ),
                ),
                fiscal_evidence=fiscal,
            )
        )
    return BuyerSourceNameRegistry(bindings, provenance)


def load_registry(path: Path | None = None) -> BuyerSourceNameRegistry:
    """Carga y valida el sidecar. Un sidecar ausente da un registro vacío.

    Ausente es distinto de malformado: sin archivo, ninguna identidad tiene
    binding y todo el runtime conserva el comportamiento previo, que es un
    estado seguro y declarado. Malformado sí levanta, porque un binding a
    medias proyectaría un filtro que nadie revisó.
    """
    sidecar = path or DEFAULT_SIDECAR_PATH
    if not sidecar.exists():
        logger.warning(
            "Sidecar de nombres físicos ausente en %s — ninguna identidad se proyecta.",
            sidecar,
        )
        return BuyerSourceNameRegistry([])
    with sidecar.open("r", encoding="utf-8") as handle:
        return parse_registry(json.load(handle))


_REGISTRY: BuyerSourceNameRegistry | None = None


def get_registry() -> BuyerSourceNameRegistry:
    """Registro por proceso. El sidecar se lee una vez, no por query."""
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = load_registry()
    return _REGISTRY


def project_buyer_names(canonical_name: str, dataset_id: str) -> tuple[str, ...] | None:
    """API pública: identidad canónica + dataset → nombres físicos exactos.

    ``None`` = sin binding. El llamador NO debe inventar un fallback: conserva
    su proyección previa tal cual.
    """
    return get_registry().project(canonical_name, dataset_id)

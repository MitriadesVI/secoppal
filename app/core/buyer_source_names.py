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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.utils.money import normalize_text

logger = logging.getLogger(__name__)

DEFAULT_SIDECAR_PATH = Path(__file__).resolve().parent.parent / "data" / "buyer_source_names.json"

# Datasets con columna de entidad conocida. Un dataset fuera de esta lista es
# un error de carga y no un binding que se ignora en silencio: un id mal
# escrito produciría fallback mudo al LIKE canónico, que es exactamente el
# fallo que ADR-073 prohíbe (INV-NO-SILENT-SEMANTIC-FALLBACK).
KNOWN_DATASET_IDS: frozenset[str] = frozenset({"p6dx-8zbt", "jbjy-vk9h"})


class BuyerSourceNameError(ValueError):
    """El sidecar está malformado. Falla al cargar, no en la primera query."""


@dataclass(frozen=True, slots=True)
class BuyerSourceBinding:
    """Los nombres físicos de UNA identidad, declarados por dataset."""

    canonical_name: str
    identity_key: str
    sources: dict[str, tuple[str, ...]]

    def names_for(self, dataset_id: str) -> tuple[str, ...] | None:
        return self.sources.get(dataset_id)


class BuyerSourceNameRegistry:
    """Índice O(1) de identidad canónica normalizada → binding."""

    def __init__(self, bindings: list[BuyerSourceBinding]) -> None:
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


def _parse_sources(raw: Any, canonical: str) -> dict[str, tuple[str, ...]]:
    if not isinstance(raw, dict) or not raw:
        raise BuyerSourceNameError(f"{canonical}: 'sources' debe ser un objeto no vacío")

    sources: dict[str, tuple[str, ...]] = {}
    for dataset_id, names in raw.items():
        if dataset_id not in KNOWN_DATASET_IDS:
            raise BuyerSourceNameError(f"{canonical}: dataset desconocido {dataset_id!r}")
        if not isinstance(names, list) or not names:
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


def _parse_identity_key(raw: Any, canonical: str) -> str:
    if not isinstance(raw, dict):
        raise BuyerSourceNameError(f"{canonical}: 'identity_key' debe ser un objeto")
    key_type = _require_str(raw.get("type"), f"{canonical}/identity_key.type")
    key_value = _require_str(raw.get("value"), f"{canonical}/identity_key.value")
    return f"{key_type}:{key_value}"


def parse_registry(payload: Any) -> BuyerSourceNameRegistry:
    """Valida el payload del sidecar. Falla fuerte: nada malformado pasa."""
    if not isinstance(payload, dict):
        raise BuyerSourceNameError("el sidecar debe ser un objeto JSON")

    raw_bindings = payload.get("bindings")
    if not isinstance(raw_bindings, list):
        raise BuyerSourceNameError("'bindings' debe ser una lista")

    bindings: list[BuyerSourceBinding] = []
    identity_keys: set[str] = set()
    for record in raw_bindings:
        if not isinstance(record, dict):
            raise BuyerSourceNameError(f"binding inválido: {record!r}")
        canonical = _require_str(record.get("canonical_name"), "canonical_name")
        identity_key = _parse_identity_key(record.get("identity_key"), canonical)
        if identity_key in identity_keys:
            raise BuyerSourceNameError(f"identity_key duplicada: {identity_key}")
        identity_keys.add(identity_key)
        bindings.append(
            BuyerSourceBinding(
                canonical_name=canonical,
                identity_key=identity_key,
                sources=_parse_sources(record.get("sources"), canonical),
            )
        )
    return BuyerSourceNameRegistry(bindings)


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

"""modality_models.py — modelo declarativo de modalidad V1 (MOD-M2 prep).

SOLO estructura de datos + loader + validación. Este módulo es DELIBERADAMENTE
puro: no importa el parser (query_router), el builder (soql_builder) ni el frame
(query_frame), y no toca Radar, warehouse, Telegram ni follow-ups. El resolver,
la integración con el parser y el cambio de builder llegan en fases posteriores
(M2+), no aquí.

Doble taxonomía (D4): `legal_selection_family` y `retrieval_family` son campos
separados y pueden diferir (p. ej. acuerdo marco, enajenación, régimen especial).

El loader es FAIL-CLOSED: cualquier desviación estructural, de tipo, de
vocabulario o semántica levanta CatalogValidationError con mensaje explícito.
Los booleanos se validan por identidad de tipo (`type(v) is bool`) y se
conservan sin coerción — un `1`/`"true"` en YAML es un error, no un True.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import yaml

DEFAULT_CATALOG_PATH = Path(__file__).parent.parent / "data" / "modality_catalog_v1.yaml"

# ── Vocabularios controlados ────────────────────────────────────────────────
# legal_selection_family: None (YAML null) representa "sin familia jurídica de
# selección". No hay sentinel string: el estado de clasificación se expresa
# aparte en legal_classification_status.
LEGAL_SELECTION_FAMILIES = frozenset({
    "licitacion_publica", "seleccion_abreviada", "concurso_de_meritos",
    "minima_cuantia", "contratacion_directa",
})

LEGAL_CLASSIFICATION_STATUSES = frozenset({
    "verified", "pending_review", "not_applicable",
})

RECORD_KINDS = frozenset({
    "procurement", "procurement_instrument", "state_asset_sale",
    "planning_rfi", "platform_test",
})
SUBMISSION_MODES = frozenset({"standard", "con_ofertas", "no_aplica"})
LEGAL_REGIMES = frozenset({"general", "especial", "no_aplica"})
PARENT_EXPANSION_POLICIES = frozenset({"family_default", "explicit_only", "never"})
EVIDENCE_CONFIDENCE = frozenset({"alta", "media", "baja"})

_KNOWN_DATASETS = frozenset({"procesos", "contratos"})

# Claves obligatorias (deben ESTAR presentes; algunas admiten valor null).
_REQUIRED_KEYS = (
    "procedure_id", "display_name",
    "legal_selection_family", "legal_classification_status",
    "retrieval_family", "procedure_variant", "legal_regime",
    "record_kind", "submission_mode",
    "user_searchable", "explicit_intent_required",
    "parent_expansion_policy", "dataset_values", "evidence_confidence",
)
# Claves cuyo valor puede ser null (pero la clave debe existir).
_NULLABLE_KEYS = frozenset({"legal_selection_family", "procedure_variant"})
_BOOL_KEYS = ("user_searchable", "explicit_intent_required")


class CatalogValidationError(ValueError):
    """El catálogo declarativo violó una invariante estructural o semántica."""


class _StrictSafeLoader(yaml.SafeLoader):
    """SafeLoader que rechaza claves repetidas en cualquier mapping YAML."""


def _construct_mapping_without_duplicates(
        loader: _StrictSafeLoader, node: yaml.MappingNode, deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise _fail(
                f"clave YAML no hashable en línea {key_node.start_mark.line + 1}: {key!r}"
            ) from exc
        if duplicate:
            raise _fail(
                f"clave YAML duplicada {key!r} en línea {key_node.start_mark.line + 1}"
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_without_duplicates,
)


@dataclass(frozen=True)
class ProcedureClassification:
    """Un registro canónico por valor raw de `modalidad_de_contratacion`."""

    procedure_id: str
    display_name: str
    legal_selection_family: str | None
    legal_classification_status: str
    retrieval_family: str
    procedure_variant: str | None
    legal_regime: str
    record_kind: str
    submission_mode: str
    user_searchable: bool
    explicit_intent_required: bool
    parent_expansion_policy: str
    dataset_values: Mapping[str, tuple[str, ...]]
    evidence_confidence: str
    notes: str = ""
    not_searchable_as_modality: bool = False

    def raw_values(self) -> tuple[str, ...]:
        """Todos los raw values de este registro, en todos los datasets."""
        out: list[str] = []
        for values in self.dataset_values.values():
            out.extend(values)
        return tuple(out)

    def applies_to(self, dataset: str) -> bool:
        return bool(self.dataset_values.get(dataset))


@dataclass(frozen=True)
class ModalityCatalog:
    """Colección inmutable de clasificaciones + índices de conveniencia."""

    version: int
    procedures: tuple[ProcedureClassification, ...]

    def by_id(self, procedure_id: str) -> ProcedureClassification | None:
        return self._by_id.get(procedure_id)

    def retrieval_family(self, family: str) -> tuple[ProcedureClassification, ...]:
        return tuple(p for p in self.procedures if p.retrieval_family == family)

    def raw_values_for_dataset(self, dataset: str) -> tuple[str, ...]:
        out: list[str] = []
        for p in self.procedures:
            out.extend(p.dataset_values.get(dataset, ()))
        return tuple(out)

    @property
    def _by_id(self) -> dict[str, ProcedureClassification]:
        return {p.procedure_id: p for p in self.procedures}


# ── Helpers de validación estricta (fail-closed, sin coerción) ──────────────

def _fail(msg: str) -> "CatalogValidationError":
    return CatalogValidationError(msg)


def _require_bool(record: dict, key: str, ctx: str) -> bool:
    value = record[key]
    if type(value) is not bool:  # noqa: E721 — identidad de tipo a propósito
        raise _fail(f"{ctx}: '{key}' debe ser booleano real (bool), no {type(value).__name__}: {value!r}")
    return value


def _require_nonempty_str(record: dict, key: str, ctx: str) -> str:
    value = record[key]
    if not isinstance(value, str) or not value.strip():
        raise _fail(f"{ctx}: '{key}' debe ser un string no vacío (recibido {value!r})")
    return value


def _require_nullable_str(record: dict, key: str, ctx: str) -> str | None:
    value = record[key]
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise _fail(f"{ctx}: '{key}' debe ser string no vacío o null (recibido {value!r})")
    return value


def _parse_dataset_values(record: dict, ctx: str) -> Mapping[str, tuple[str, ...]]:
    dv = record["dataset_values"]
    if not isinstance(dv, dict) or not dv:
        raise _fail(f"{ctx}: 'dataset_values' ausente o vacío")
    out: dict[str, tuple[str, ...]] = {}
    for dataset, values in dv.items():
        if dataset not in _KNOWN_DATASETS:
            raise _fail(f"{ctx}: dataset desconocido {dataset!r}")
        if not isinstance(values, list):
            raise _fail(f"{ctx}: dataset_values[{dataset!r}] debe ser una lista")
        if not values:
            raise _fail(f"{ctx}: dataset_values[{dataset!r}] no puede ser lista vacía")
        for raw in values:
            if not isinstance(raw, str) or not raw.strip():
                raise _fail(f"{ctx}: raw value inválido (vacío o no-string) en {dataset!r}: {raw!r}")
        out[dataset] = tuple(values)
    return MappingProxyType(out)


def _parse_record(record: Any, index: int) -> ProcedureClassification:
    ctx = f"procedures[{index}]"
    if not isinstance(record, dict):
        raise _fail(f"{ctx}: cada registro debe ser un mapping, no {type(record).__name__}")

    for key in _REQUIRED_KEYS:
        if key not in record:
            raise _fail(f"{ctx}: falta campo obligatorio '{key}'")

    procedure_id = _require_nonempty_str(record, "procedure_id", ctx)
    ctx = f"procedures[{index}]({procedure_id})"
    display_name = _require_nonempty_str(record, "display_name", ctx)
    retrieval_family = _require_nonempty_str(record, "retrieval_family", ctx)
    legal_selection_family = _require_nullable_str(record, "legal_selection_family", ctx)
    procedure_variant = _require_nullable_str(record, "procedure_variant", ctx)
    legal_classification_status = _require_nonempty_str(record, "legal_classification_status", ctx)
    legal_regime = _require_nonempty_str(record, "legal_regime", ctx)
    record_kind = _require_nonempty_str(record, "record_kind", ctx)
    submission_mode = _require_nonempty_str(record, "submission_mode", ctx)
    parent_expansion_policy = _require_nonempty_str(record, "parent_expansion_policy", ctx)
    evidence_confidence = _require_nonempty_str(record, "evidence_confidence", ctx)
    user_searchable = _require_bool(record, "user_searchable", ctx)
    explicit_intent_required = _require_bool(record, "explicit_intent_required", ctx)

    not_searchable_as_modality = False
    if "not_searchable_as_modality" in record:
        not_searchable_as_modality = _require_bool(record, "not_searchable_as_modality", ctx)

    notes = record.get("notes", "")
    if notes is None:
        notes = ""
    if not isinstance(notes, str):
        raise _fail(f"{ctx}: 'notes' debe ser string")

    p = ProcedureClassification(
        procedure_id=procedure_id,
        display_name=display_name,
        legal_selection_family=legal_selection_family,
        legal_classification_status=legal_classification_status,
        retrieval_family=retrieval_family,
        procedure_variant=procedure_variant,
        legal_regime=legal_regime,
        record_kind=record_kind,
        submission_mode=submission_mode,
        user_searchable=user_searchable,
        explicit_intent_required=explicit_intent_required,
        parent_expansion_policy=parent_expansion_policy,
        dataset_values=_parse_dataset_values(record, ctx),
        evidence_confidence=evidence_confidence,
        notes=notes,
        not_searchable_as_modality=not_searchable_as_modality,
    )
    _validate_vocab(p, ctx)
    _validate_semantics(p, ctx)
    return p


def _validate_vocab(p: ProcedureClassification, ctx: str) -> None:
    if p.legal_selection_family is not None \
            and p.legal_selection_family not in LEGAL_SELECTION_FAMILIES:
        raise _fail(f"{ctx}: legal_selection_family inválida {p.legal_selection_family!r}")
    if p.legal_classification_status not in LEGAL_CLASSIFICATION_STATUSES:
        raise _fail(f"{ctx}: legal_classification_status inválido {p.legal_classification_status!r}")
    if p.record_kind not in RECORD_KINDS:
        raise _fail(f"{ctx}: record_kind inválido {p.record_kind!r}")
    if p.submission_mode not in SUBMISSION_MODES:
        raise _fail(f"{ctx}: submission_mode inválido {p.submission_mode!r}")
    if p.legal_regime not in LEGAL_REGIMES:
        raise _fail(f"{ctx}: legal_regime inválido {p.legal_regime!r}")
    if p.parent_expansion_policy not in PARENT_EXPANSION_POLICIES:
        raise _fail(f"{ctx}: parent_expansion_policy inválida {p.parent_expansion_policy!r}")
    if p.evidence_confidence not in EVIDENCE_CONFIDENCE:
        raise _fail(f"{ctx}: evidence_confidence inválida {p.evidence_confidence!r}")


def _validate_semantics(p: ProcedureClassification, ctx: str) -> None:
    dataset_names = set(p.dataset_values)

    if p.record_kind == "planning_rfi":
        if p.legal_selection_family is not None:
            raise _fail(f"{ctx}: planning_rfi debe tener legal_selection_family=null")
        if p.legal_classification_status != "not_applicable":
            raise _fail(
                f"{ctx}: planning_rfi debe tener "
                f"legal_classification_status=not_applicable"
            )
        if p.legal_regime != "general":
            raise _fail(f"{ctx}: planning_rfi debe tener legal_regime=general")
        if p.submission_mode != "no_aplica":
            raise _fail(f"{ctx}: planning_rfi debe tener submission_mode=no_aplica")
        if p.retrieval_family != "solicitud_informacion_proveedores":
            raise _fail(
                f"{ctx}: planning_rfi debe tener "
                f"retrieval_family=solicitud_informacion_proveedores"
            )
        if p.user_searchable is not True:
            raise _fail(f"{ctx}: planning_rfi debe tener user_searchable=true")
        if p.explicit_intent_required is not True:
            raise _fail(
                f"{ctx}: planning_rfi debe tener explicit_intent_required=true"
            )
        if p.not_searchable_as_modality is not True:
            raise _fail(
                f"{ctx}: planning_rfi debe tener not_searchable_as_modality=true"
            )
        if p.parent_expansion_policy != "explicit_only":
            raise _fail(
                f"{ctx}: planning_rfi debe tener parent_expansion_policy=explicit_only"
            )
        if dataset_names != {"procesos"}:
            raise _fail(
                f"{ctx}: planning_rfi debe existir solo en dataset_values.procesos"
            )

    if p.record_kind == "platform_test":
        if p.legal_selection_family is not None:
            raise _fail(f"{ctx}: platform_test debe tener legal_selection_family=null")
        if p.legal_classification_status != "not_applicable":
            raise _fail(
                f"{ctx}: platform_test debe tener "
                f"legal_classification_status=not_applicable"
            )
        if p.legal_regime != "no_aplica":
            raise _fail(f"{ctx}: platform_test debe tener legal_regime=no_aplica")
        if p.submission_mode != "no_aplica":
            raise _fail(f"{ctx}: platform_test debe tener submission_mode=no_aplica")
        if p.retrieval_family != "platform_test":
            raise _fail(f"{ctx}: platform_test debe tener retrieval_family=platform_test")
        if p.user_searchable is not False:
            raise _fail(f"{ctx}: platform_test debe tener user_searchable=false")
        if p.explicit_intent_required is not False:
            raise _fail(
                f"{ctx}: platform_test debe tener explicit_intent_required=false"
            )
        if p.parent_expansion_policy != "never":
            raise _fail(f"{ctx}: platform_test debe tener parent_expansion_policy=never")
        if dataset_names != {"procesos"}:
            raise _fail(
                f"{ctx}: platform_test debe existir solo en dataset_values.procesos"
            )

    if p.record_kind == "state_asset_sale":
        if p.legal_selection_family != "seleccion_abreviada":
            raise _fail(
                f"{ctx}: state_asset_sale debe tener "
                f"legal_selection_family=seleccion_abreviada"
            )
        if p.legal_classification_status != "verified":
            raise _fail(
                f"{ctx}: state_asset_sale debe tener "
                f"legal_classification_status=verified"
            )
        if p.legal_regime != "general":
            raise _fail(f"{ctx}: state_asset_sale debe tener legal_regime=general")
        if p.retrieval_family != "enajenacion_bienes_estado":
            raise _fail(
                f"{ctx}: state_asset_sale debe tener "
                f"retrieval_family=enajenacion_bienes_estado"
            )
        if p.user_searchable is not True:
            raise _fail(f"{ctx}: state_asset_sale debe tener user_searchable=true")
        if p.explicit_intent_required is not True:
            raise _fail(
                f"{ctx}: state_asset_sale debe tener explicit_intent_required=true"
            )
        if p.parent_expansion_policy != "explicit_only":
            raise _fail(
                f"{ctx}: state_asset_sale debe tener "
                f"parent_expansion_policy=explicit_only"
            )

    if p.retrieval_family == "regimen_especial":
        if p.legal_selection_family is not None:
            raise _fail(
                f"{ctx}: regimen_especial debe tener legal_selection_family=null"
            )
        if p.legal_classification_status != "not_applicable":
            raise _fail(
                f"{ctx}: regimen_especial debe tener "
                f"legal_classification_status=not_applicable"
            )
        if p.legal_regime != "especial":
            raise _fail(f"{ctx}: regimen_especial debe tener legal_regime=especial")
        if p.record_kind != "procurement":
            raise _fail(f"{ctx}: regimen_especial debe tener record_kind=procurement")
        if p.submission_mode not in {"standard", "con_ofertas"}:
            raise _fail(
                f"{ctx}: regimen_especial debe tener submission_mode=standard "
                f"o con_ofertas"
            )
        if p.parent_expansion_policy != "family_default":
            raise _fail(
                f"{ctx}: regimen_especial debe tener "
                f"parent_expansion_policy=family_default"
            )

    if p.legal_classification_status == "verified" \
            and p.legal_selection_family is None:
        raise _fail(
            f"{ctx}: legal_classification_status=verified exige "
            f"legal_selection_family no-null"
        )
    if p.legal_classification_status == "pending_review" \
            and p.legal_selection_family is not None:
        raise _fail(
            f"{ctx}: legal_classification_status=pending_review exige "
            f"legal_selection_family=null"
        )
    if p.legal_classification_status == "not_applicable" \
            and p.legal_selection_family is not None:
        raise _fail(
            f"{ctx}: legal_classification_status=not_applicable exige "
            f"legal_selection_family=null"
        )

    # Un registro que NO es buscable no puede exigir intención explícita: la
    # intención explícita solo tiene sentido para algo alcanzable por el usuario.
    if p.user_searchable is False and p.explicit_intent_required is True:
        raise _fail(
            f"{ctx}: user_searchable=false es incompatible con "
            f"explicit_intent_required=true"
        )


def _validate_catalog(procedures: tuple[ProcedureClassification, ...]) -> None:
    seen_ids: set[str] = set()
    # Unicidad de raw value es POR DATASET: el mismo string ("Contratación
    # directa") aparece legítimamente en procesos y en contratos.
    seen_raws: set[tuple[str, str]] = set()
    for p in procedures:
        if p.procedure_id in seen_ids:
            raise _fail(f"procedure_id duplicado: {p.procedure_id}")
        seen_ids.add(p.procedure_id)
        for dataset, values in p.dataset_values.items():
            for raw in values:
                key = (dataset, raw)
                if key in seen_raws:
                    raise _fail(f"raw value {raw!r} asignado a más de un procedure "
                                f"en dataset {dataset!r}")
                seen_raws.add(key)


def load_catalog(path: Path | str | None = None) -> ModalityCatalog:
    """Carga y valida el catálogo declarativo. FAIL-CLOSED: cualquier desviación
    estructural, de tipo, de vocabulario o semántica levanta CatalogValidationError."""
    path = Path(path) if path is not None else DEFAULT_CATALOG_PATH
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.load(fh, Loader=_StrictSafeLoader)

    if not isinstance(data, dict):
        raise _fail("el YAML raíz debe ser un mapping")

    version = data.get("version")
    if type(version) is not int or version <= 0:  # noqa: E721
        raise _fail(f"'version' debe ser un entero > 0 (recibido {version!r})")

    if "procedures" not in data:
        raise _fail("falta 'procedures'")
    raw_procedures = data["procedures"]
    if not isinstance(raw_procedures, list):
        raise _fail("'procedures' debe ser una lista")
    if not raw_procedures:
        raise _fail("'procedures' no puede estar vacía")

    procedures = tuple(_parse_record(r, i) for i, r in enumerate(raw_procedures))
    _validate_catalog(procedures)
    return ModalityCatalog(version=version, procedures=procedures)

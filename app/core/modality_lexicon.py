"""Normalización trazable y lexicón declarativo fail-closed para M2.

El catálogo M1 conserva la verdad canónica. Este módulo declara únicamente cómo
se expresa el usuario y valida, antes de construir el resolver, que cada regla y
cada efecto de qualifier sean coherentes y determinísticos contra M1.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, IO, Mapping

import yaml

from app.core.modality_models import ModalityCatalog, load_catalog
from app.core.modality_text import normalize_phrase, normalize_with_map

DEFAULT_LEXICON_PATH = Path(__file__).parent.parent / "data" / "modality_lexicon_v1.yaml"

_TARGET_KINDS = frozenset({"retrieval_family", "procedure", "qualifier", "ambiguity"})
_MATCH_STRENGTHS = frozenset({
    "exact_legal", "exact_common", "colloquial", "contextual", "legacy_raw",
})
_CONFIDENCES = frozenset({"high", "medium", "low"})
_QUALIFIER_TYPES = frozenset({"submission_mode", "procedure_variant"})
_ATTACHMENT_DIRECTIONS = frozenset({"after", "before", "either"})
_FORBIDDEN_FAMILY_TARGETS = frozenset({"platform_test"})

_COMMON_RULE_KEYS = frozenset({
    "rule_id", "phrases", "target_kind", "match_strength", "confidence",
    "explicit_intent", "notes", "forbidden_context", "required_context",
})
_KIND_KEYS = {
    "retrieval_family": frozenset({"target_id", "assumption_code"}),
    "procedure": frozenset({"target_id", "assumption_code"}),
    "qualifier": frozenset({
        "qualifier_type", "qualifier_value", "allowed_retrieval_families",
        "attachment_direction", "max_attachment_tokens",
    }),
    "ambiguity": frozenset({"candidate_target_ids", "ambiguity_code"}),
}


class LexiconValidationError(ValueError):
    """El lexicón violó una invariante estructural o semántica."""


class _StrictSafeLoader(yaml.SafeLoader):
    """SafeLoader que rechaza claves repetidas en cualquier mapping."""


def _construct_mapping_without_duplicates(
    loader: _StrictSafeLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise LexiconValidationError(
                f"clave YAML no hashable en línea {key_node.start_mark.line + 1}: {key!r}"
            ) from exc
        if duplicate:
            raise LexiconValidationError(
                f"clave YAML duplicada {key!r} en línea {key_node.start_mark.line + 1}"
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_without_duplicates,
)


def strict_yaml_load(stream: str | IO[str]) -> Any:
    """Parsea YAML con SafeLoader estricto y rechazo de claves duplicadas."""
    return yaml.load(stream, Loader=_StrictSafeLoader)


# ``normalize_phrase`` y ``normalize_with_map`` se reexportan por compatibilidad.

@dataclass(frozen=True)
class LexiconRule:
    """Regla lingüística validada e inmutable."""

    rule_id: str
    phrases: tuple[str, ...]
    target_kind: str
    target_id: str | None
    candidate_target_ids: tuple[str, ...]
    match_strength: str
    confidence: str
    explicit_intent: bool
    assumption_code: str | None
    ambiguity_code: str | None
    allowed_retrieval_families: tuple[str, ...]
    qualifier_type: str | None
    qualifier_value: str | None
    attachment_direction: str | None
    max_attachment_tokens: int | None
    forbidden_context: tuple[str, ...]
    required_context: tuple[str, ...]
    notes: str


@dataclass(frozen=True)
class ModalityLexicon:
    """Reglas e índices inmutables, incluida la matriz de composición."""

    rules: tuple[LexiconRule, ...]
    phrase_index: Mapping[str, LexiconRule]
    qualifier_targets: Mapping[tuple[str, str, str], str]

    def rule_count(self) -> int:
        return len(self.rules)

    def phrase_count(self) -> int:
        return len(self.phrase_index)

    def composition_target(
        self, family: str, qualifier_type: str, qualifier_value: str,
    ) -> str | None:
        return self.qualifier_targets.get((family, qualifier_type, qualifier_value))


# ── Helpers de validación ───────────────────────────────────────────────────

def _fail(message: str) -> LexiconValidationError:
    return LexiconValidationError(message)


def _required_nonempty_str(record: dict[str, Any], key: str, ctx: str) -> str:
    if key not in record:
        raise _fail(f"{ctx}: falta campo obligatorio '{key}'")
    value = record[key]
    if not isinstance(value, str) or not value.strip():
        raise _fail(f"{ctx}: '{key}' debe ser string no vacío (recibido {value!r})")
    return value


def _optional_str(record: dict[str, Any], key: str, ctx: str) -> str | None:
    if key not in record or record[key] is None:
        return None
    value = record[key]
    if not isinstance(value, str) or not value.strip():
        raise _fail(f"{ctx}: '{key}' debe ser string no vacío o null (recibido {value!r})")
    return value


def _required_bool(record: dict[str, Any], key: str, ctx: str) -> bool:
    if key not in record:
        raise _fail(f"{ctx}: falta campo obligatorio '{key}'")
    value = record[key]
    if type(value) is not bool:  # noqa: E721 — bool estricto, sin coerción
        raise _fail(f"{ctx}: '{key}' debe ser booleano real (recibido {value!r})")
    return value


def _string_list(
    record: dict[str, Any], key: str, ctx: str, *, required: bool,
) -> tuple[str, ...]:
    if key not in record or record[key] is None:
        if required:
            raise _fail(f"{ctx}: falta lista obligatoria '{key}'")
        return ()
    value = record[key]
    if not isinstance(value, list):
        raise _fail(f"{ctx}: '{key}' debe ser una lista")
    if required and not value:
        raise _fail(f"{ctx}: '{key}' no puede ser lista vacía")
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise _fail(f"{ctx}: '{key}' contiene elemento vacío o no-string: {item!r}")
        if item in seen:
            raise _fail(f"{ctx}: '{key}' contiene valor duplicado {item!r}")
        seen.add(item)
        out.append(item)
    return tuple(out)


def _normalized_string_list(
    record: dict[str, Any], key: str, ctx: str,
) -> tuple[str, ...]:
    raw = _string_list(record, key, ctx, required=False)
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        normalized = normalize_phrase(item)
        if not normalized:
            raise _fail(f"{ctx}: '{key}' contiene valor vacío tras normalización")
        if normalized in seen:
            raise _fail(f"{ctx}: '{key}' contiene duplicado normalizado {normalized!r}")
        seen.add(normalized)
        out.append(normalized)
    return tuple(out)


def _validate_key_matrix(record: dict[str, Any], target_kind: str, ctx: str) -> None:
    allowed = _COMMON_RULE_KEYS | _KIND_KEYS[target_kind]
    unexpected = sorted(set(record) - allowed)
    if unexpected:
        raise _fail(
            f"{ctx}: campos incompatibles con target_kind={target_kind}: {unexpected}"
        )


def _qualifier_target(
    catalog: ModalityCatalog,
    family: str,
    qualifier_type: str,
    qualifier_value: str,
    ctx: str,
) -> str:
    candidates = []
    for proc in catalog.retrieval_family(family):
        value = (
            proc.submission_mode
            if qualifier_type == "submission_mode"
            else proc.procedure_variant
        )
        if value == qualifier_value:
            candidates.append(proc)
    if len(candidates) != 1:
        ids = [proc.procedure_id for proc in candidates]
        raise _fail(
            f"{ctx}: combinación ({family}, {qualifier_type}, {qualifier_value}) "
            f"debe resolver exactamente un procedimiento; obtuvo {ids}"
        )
    proc = candidates[0]
    if not proc.user_searchable or proc.parent_expansion_policy == "never":
        raise _fail(f"{ctx}: qualifier resuelve a procedimiento no buscable {proc.procedure_id!r}")
    return proc.procedure_id


def _check_explicit_intent_for_family(
    ctx: str, family: str, explicit_intent: bool, catalog: ModalityCatalog,
) -> None:
    children = catalog.retrieval_family(family)
    if not children:
        raise _fail(f"{ctx}: retrieval_family {family!r} sin procedimientos")
    ordinary = [p for p in children if p.parent_expansion_policy == "family_default"]
    if not ordinary and not explicit_intent:
        raise _fail(
            f"{ctx}: familia {family!r} solo tiene hijos explicit_only; "
            "debe declarar explicit_intent=true"
        )


def _build_rule(
    record: Any,
    index: int,
    catalog: ModalityCatalog,
    retrieval_families: frozenset[str],
) -> tuple[LexiconRule, dict[tuple[str, str, str], str]]:
    ctx = f"rules[{index}]"
    if not isinstance(record, dict):
        raise _fail(f"{ctx}: cada regla debe ser un mapping, no {type(record).__name__}")

    rule_id = _required_nonempty_str(record, "rule_id", ctx)
    ctx = f"rules[{index}]({rule_id})"
    target_kind = _required_nonempty_str(record, "target_kind", ctx)
    if target_kind not in _TARGET_KINDS:
        raise _fail(f"{ctx}: target_kind desconocido {target_kind!r}")
    _validate_key_matrix(record, target_kind, ctx)

    phrases_raw = _string_list(record, "phrases", ctx, required=True)
    phrases: list[str] = []
    seen_phrases: set[str] = set()
    for phrase in phrases_raw:
        normalized = normalize_phrase(phrase)
        if not normalized:
            raise _fail(f"{ctx}: frase vacía tras normalización: {phrase!r}")
        if normalized in seen_phrases:
            raise _fail(f"{ctx}: frase duplicada tras normalización: {normalized!r}")
        seen_phrases.add(normalized)
        phrases.append(normalized)

    match_strength = _required_nonempty_str(record, "match_strength", ctx)
    if match_strength not in _MATCH_STRENGTHS:
        raise _fail(f"{ctx}: match_strength desconocido {match_strength!r}")
    confidence = _required_nonempty_str(record, "confidence", ctx)
    if confidence not in _CONFIDENCES:
        raise _fail(f"{ctx}: confidence desconocida {confidence!r}")
    explicit_intent = _required_bool(record, "explicit_intent", ctx)

    notes = record.get("notes", "")
    if notes is None:
        notes = ""
    if not isinstance(notes, str):
        raise _fail(f"{ctx}: 'notes' debe ser string")

    forbidden_context = _normalized_string_list(record, "forbidden_context", ctx)
    required_context = _normalized_string_list(record, "required_context", ctx)
    overlap = sorted(set(forbidden_context) & set(required_context))
    if overlap:
        raise _fail(f"{ctx}: contextos simultáneamente required y forbidden: {overlap}")

    target_id: str | None = None
    candidate_ids: tuple[str, ...] = ()
    assumption_code: str | None = None
    ambiguity_code: str | None = None
    allowed_families: tuple[str, ...] = ()
    qualifier_type: str | None = None
    qualifier_value: str | None = None
    attachment_direction: str | None = None
    max_attachment_tokens: int | None = None
    composition_entries: dict[tuple[str, str, str], str] = {}

    if target_kind == "retrieval_family":
        target_id = _required_nonempty_str(record, "target_id", ctx)
        assumption_code = _optional_str(record, "assumption_code", ctx)
        if target_id in _FORBIDDEN_FAMILY_TARGETS:
            raise _fail(f"{ctx}: retrieval_family {target_id!r} no es buscable")
        if target_id not in retrieval_families:
            raise _fail(f"{ctx}: retrieval_family inexistente {target_id!r}")
        _check_explicit_intent_for_family(ctx, target_id, explicit_intent, catalog)

    elif target_kind == "procedure":
        target_id = _required_nonempty_str(record, "target_id", ctx)
        assumption_code = _optional_str(record, "assumption_code", ctx)
        proc = catalog.by_id(target_id)
        if proc is None:
            raise _fail(f"{ctx}: procedure_id inexistente {target_id!r}")
        if proc.explicit_intent_required and not explicit_intent:
            raise _fail(f"{ctx}: {target_id!r} exige explicit_intent=true")

    elif target_kind == "qualifier":
        qualifier_type = _required_nonempty_str(record, "qualifier_type", ctx)
        if qualifier_type not in _QUALIFIER_TYPES:
            raise _fail(f"{ctx}: qualifier_type desconocido {qualifier_type!r}")
        qualifier_value = _required_nonempty_str(record, "qualifier_value", ctx)
        allowed_families = _string_list(
            record, "allowed_retrieval_families", ctx, required=True,
        )
        attachment_direction = _required_nonempty_str(
            record, "attachment_direction", ctx,
        )
        if attachment_direction not in _ATTACHMENT_DIRECTIONS:
            raise _fail(f"{ctx}: attachment_direction inválida {attachment_direction!r}")
        if "max_attachment_tokens" not in record:
            raise _fail(f"{ctx}: qualifier requiere max_attachment_tokens")
        max_attachment_tokens = record["max_attachment_tokens"]
        if type(max_attachment_tokens) is not int or not 1 <= max_attachment_tokens <= 12:  # noqa: E721
            raise _fail(
                f"{ctx}: max_attachment_tokens debe ser entero real entre 1 y 12"
            )
        for family in allowed_families:
            if family not in retrieval_families:
                raise _fail(f"{ctx}: allowed_retrieval_families inexistente {family!r}")
            key = (family, qualifier_type, qualifier_value)
            composition_entries[key] = _qualifier_target(
                catalog, family, qualifier_type, qualifier_value, ctx,
            )

    else:  # ambiguity
        candidate_ids = _string_list(
            record, "candidate_target_ids", ctx, required=True,
        )
        ambiguity_code = _required_nonempty_str(record, "ambiguity_code", ctx)
        for candidate_id in candidate_ids:
            proc = catalog.by_id(candidate_id)
            if proc is None:
                raise _fail(f"{ctx}: candidate_target_id inexistente {candidate_id!r}")
            if not proc.user_searchable or proc.parent_expansion_policy == "never":
                raise _fail(
                    f"{ctx}: candidate_target_id no buscable {candidate_id!r}"
                )

    rule = LexiconRule(
        rule_id=rule_id,
        phrases=tuple(phrases),
        target_kind=target_kind,
        target_id=target_id,
        candidate_target_ids=candidate_ids,
        match_strength=match_strength,
        confidence=confidence,
        explicit_intent=explicit_intent,
        assumption_code=assumption_code,
        ambiguity_code=ambiguity_code,
        allowed_retrieval_families=allowed_families,
        qualifier_type=qualifier_type,
        qualifier_value=qualifier_value,
        attachment_direction=attachment_direction,
        max_attachment_tokens=max_attachment_tokens,
        forbidden_context=forbidden_context,
        required_context=required_context,
        notes=notes,
    )
    return rule, composition_entries


def build_lexicon(data: Any, catalog: ModalityCatalog) -> ModalityLexicon:
    """Construye un lexicón validado desde datos ya parseados."""
    if not isinstance(data, dict):
        raise _fail("el YAML raíz del lexicón debe ser un mapping")
    extra_root = sorted(set(data) - {"version", "rules"})
    if extra_root:
        raise _fail(f"claves raíz desconocidas: {extra_root}")
    version = data.get("version")
    if type(version) is not int or version <= 0:  # noqa: E721
        raise _fail(f"'version' debe ser entero > 0 (recibido {version!r})")
    raw_rules = data.get("rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        raise _fail("'rules' debe ser una lista no vacía")

    retrieval_families = frozenset(p.retrieval_family for p in catalog.procedures)
    rules: list[LexiconRule] = []
    phrase_index: dict[str, LexiconRule] = {}
    qualifier_targets: dict[tuple[str, str, str], str] = {}
    seen_ids: set[str] = set()

    for index, raw_rule in enumerate(raw_rules):
        rule, entries = _build_rule(raw_rule, index, catalog, retrieval_families)
        if rule.rule_id in seen_ids:
            raise _fail(f"rule_id duplicado: {rule.rule_id!r}")
        seen_ids.add(rule.rule_id)
        for phrase in rule.phrases:
            if phrase in phrase_index:
                other = phrase_index[phrase]
                raise _fail(
                    f"alias conflictivo: la frase {phrase!r} aparece en "
                    f"{other.rule_id!r} y {rule.rule_id!r}"
                )
            phrase_index[phrase] = rule
        for key, procedure_id in entries.items():
            previous = qualifier_targets.get(key)
            if previous is not None and previous != procedure_id:
                raise _fail(
                    f"composición qualifier conflictiva para {key}: "
                    f"{previous!r} vs {procedure_id!r}"
                )
            qualifier_targets[key] = procedure_id
        rules.append(rule)

    return ModalityLexicon(
        rules=tuple(rules),
        phrase_index=MappingProxyType(phrase_index),
        qualifier_targets=MappingProxyType(qualifier_targets),
    )


def load_lexicon(
    path: Path | str | None = None,
    catalog: ModalityCatalog | None = None,
) -> ModalityLexicon:
    """Carga y valida el lexicón declarativo con YAML estricto."""
    path = Path(path) if path is not None else DEFAULT_LEXICON_PATH
    if catalog is None:
        catalog = load_catalog()
    with path.open("r", encoding="utf-8") as handle:
        data = strict_yaml_load(handle)
    return build_lexicon(data, catalog)

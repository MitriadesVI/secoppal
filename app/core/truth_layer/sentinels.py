"""SECOP Truth Layer T1B-0A — Field Sentinel Semantics.

Pure, deterministic classification of observed scalar values against
per-(dataset_id, field_name) sentinel policies. No I/O, no clock, no
environment, no filesystem, no network. Neither `assess_sentinel` nor
`get_field_sentinel_policy` mutates the raw observation nor the policy.

Pipeline position (per
``docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md``)::

    FieldObservation  ->  SentinelAssessment  ->  Evidence (future)  ->  resolver (future)

This module implements ONLY the first two pieces: classification of observed
scalars into one of ``SentinelStatus`` values, preserving ``raw_value`` and
``raw_observation`` verbatim. It does not define ``Evidence``,
``ResolutionQuality``, lifecycle assessments, temporal assessments,
visibility assessments, opportunity assessments, or any lot/contract
resolver.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/planes/2026-07-28_TRUTH_LAYER_BENCHMARK_POLICY.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R1.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R2.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R3.md
"""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, Mapping

from app.core.truth_layer.models import FieldObservation, RawScalar

__all__ = [
    "SentinelStatus",
    "UnknownValueDisposition",
    "ComparisonMode",
    "SentinelRule",
    "FieldSentinelPolicy",
    "SentinelAssessment",
    "assess_sentinel",
    "get_field_sentinel_policy",
]


# ---------------------------------------------------------------------------
# Literals (public contract).
# ---------------------------------------------------------------------------

SentinelStatus = Literal[
    "meaningful",
    "sentinel_absence",
    "sentinel_unknown",
    "not_applicable",
    "unrecognized",
]

UnknownValueDisposition = Literal[
    "pass_through_as_meaningful",
    "classify_as_unrecognized",
]

ComparisonMode = Literal[
    "exact",
    "string_casefold",
    "string_casefold_strip",
]

# All three Literal unions above are closed; the validator sets below mirror
# them so we can runtime-check dataclass fields without importing
# ``typing.get_args`` (kept simple and stable).
_VALID_SENTINEL_STATUSES: frozenset[str] = frozenset(
    {
        "meaningful",
        "sentinel_absence",
        "sentinel_unknown",
        "not_applicable",
        "unrecognized",
    }
)
_VALID_UNKNOWN_DISPOSITIONS: frozenset[str] = frozenset(
    {"pass_through_as_meaningful", "classify_as_unrecognized"}
)
_VALID_COMPARISON_MODES: frozenset[str] = frozenset(
    {"exact", "string_casefold", "string_casefold_strip"}
)

# Scalar types accepted as raw aliases in ``exact`` mode (no None, no
# containers, no custom objects). ``bool`` is intentionally listed separately
# from ``int`` because ``type(True) is int`` is False while ``isinstance(True,
# int)`` is True: this preserves the bool/int distinction the binding
# contracts demand.
_EXACT_ALIAS_TYPES: tuple[type, ...] = (str, int, float, bool)


# ---------------------------------------------------------------------------
# Comparators (private, pure).
# ---------------------------------------------------------------------------


def _normalize_string(alias: object, *, strip: bool) -> str:
    """Convert ``alias`` to its normalized comparison form under string modes.

    The alias is NOT consumed as a replacement for the raw value; this is a
    pure comparison helper that does not mutate ``alias``.
    """
    if not isinstance(alias, str):
        raise TypeError(
            f"string comparison requires str alias; got {type(alias).__name__}"
        )
    if strip:
        return alias.strip().casefold()
    return alias.casefold()


def _alias_compatible_with_mode(
    alias: RawScalar, mode: ComparisonMode
) -> bool:
    """Return True when ``alias`` is a usable sentinel alias under ``mode``."""
    if alias is None:
        return False
    if mode == "exact":
        return type(alias) in _EXACT_ALIAS_TYPES
    # string modes
    return isinstance(alias, str)


def _match_alias(raw_value: RawScalar, alias: RawScalar, mode: ComparisonMode) -> bool:
    """Return True iff ``raw_value`` matches ``alias`` under ``mode``.

    For ``exact`` mode, type AND value are compared (so ``True`` is not equal
    to ``1`` and ``0`` is not equal to ``0.0``). For string modes the function
    raises ``TypeError`` if either side is not a string — comparisons are
    never silently coerced.
    """
    if mode == "exact":
        # strict type+value equality; True != 1, 0 != 0.0, "" != None
        return type(raw_value) is type(alias) and raw_value == alias
    if mode == "string_casefold":
        if not isinstance(raw_value, str) or not isinstance(alias, str):
            raise TypeError(
                "string_casefold comparator requires str raw and alias"
            )
        return raw_value.casefold() == alias.casefold()
    if mode == "string_casefold_strip":
        if not isinstance(raw_value, str) or not isinstance(alias, str):
            raise TypeError(
                "string_casefold_strip comparator requires str raw and alias"
            )
        return raw_value.strip().casefold() == alias.strip().casefold()
    raise ValueError(f"unsupported comparison_mode: {mode!r}")


def _normalize_for_compare(value: RawScalar, mode: ComparisonMode) -> RawScalar:
    """Return the normalized comparison form of ``value`` under ``mode``.

    The result is used for ``SentinelAssessment.normalized_comparison_value``
    and for aliases' precomputed normalized markers (collision detection).
    Under ``exact`` mode, the value is returned verbatim (no coercion); under
    string modes the value is ``casefold``-ed (and ``strip``-ed as the mode
    declares). The original ``raw_value`` is never mutated.
    """
    if mode == "exact":
        return value
    if mode == "string_casefold":
        return _normalize_string(value, strip=False)
    if mode == "string_casefold_strip":
        return _normalize_string(value, strip=True)
    raise ValueError(f"unsupported comparison_mode: {mode!r}")


def _collision_marker(normalized: RawScalar) -> tuple[str, object]:
    """Build a hashable collision marker that distinguishes scalar types."""
    return (type(normalized).__name__, normalized)


# ---------------------------------------------------------------------------
# SentinelRule.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SentinelRule:
    """An immutable sentinel rule for one observed sentinel value family.

    ``code`` is a stable reason-code; ``status`` is the classification the
    rule emits when ``raw_value`` matches any ``alias``; ``aliases`` is the
    tuple of expected raw forms (NOT a substitute for ``raw_value``).

    Invariants enforced here:
      * ``code`` is a non-empty string;
      * ``status`` is a valid ``SentinelStatus``;
      * ``status`` is not ``"meaningful"`` — meaningful values describe the
        absence of a sentinel, not a sentinel definition;
      * ``aliases`` is a non-empty tuple of non-null raw scalars
        (str / int / float / bool).
    """

    code: str
    status: SentinelStatus
    aliases: tuple[RawScalar, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code:
            raise ValueError(
                "SentinelRule.code must be a non-empty string"
            )
        if self.status not in _VALID_SENTINEL_STATUSES:
            raise ValueError(
                f"invalid SentinelRule.status: {self.status!r}"
            )
        if self.status == "meaningful":
            raise ValueError(
                "SentinelRule.status='meaningful' is not a sentinel "
                "(meaningful values do not need sentinel rules)"
            )
        if not isinstance(self.aliases, tuple) or not self.aliases:
            raise ValueError(
                "SentinelRule.aliases must be a non-empty tuple"
            )
        for alias in self.aliases:
            if alias is None:
                raise ValueError(
                    "SentinelRule.aliases cannot contain None"
                )
            if type(alias) not in _EXACT_ALIAS_TYPES:
                raise TypeError(
                    "SentinelRule alias must be a non-null scalar "
                    "(str / int / float / bool)"
                )

    def normalized_aliases(
        self, mode: ComparisonMode
    ) -> tuple[RawScalar, ...]:
        return tuple(
            _normalize_for_compare(alias, mode) for alias in self.aliases
        )


# ---------------------------------------------------------------------------
# FieldSentinelPolicy.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FieldSentinelPolicy:
    """An immutable sentinel policy for one ``(dataset_id, field_name)``.

    The policy declares its own comparator and how to treat values that are
    NOT catalogued by its rules. The rules are immutable tuples and must not
    collide when normalized: accidental double-classification of the same
    raw form would produce a non-deterministic assessment.

    Invariants enforced here:
      * ``dataset_id`` and ``field_name`` are non-empty strings;
      * ``comparison_mode`` is a valid ``ComparisonMode``;
      * ``unknown_value_disposition`` is a valid ``UnknownValueDisposition``;
      * ``rules`` is a non-empty tuple of ``SentinelRule``;
      * each alias is compatible with ``comparison_mode`` (string modes
        require str aliases);
      * rule ``code`` values are unique within the policy;
      * no two rule aliases normalize to the same comparison marker within
        the same policy (avoids ambiguous classification).
    """

    dataset_id: str
    field_name: str
    comparison_mode: ComparisonMode
    unknown_value_disposition: UnknownValueDisposition
    rules: tuple[SentinelRule, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise ValueError(
                "FieldSentinelPolicy.dataset_id must be a non-empty string"
            )
        if not isinstance(self.field_name, str) or not self.field_name:
            raise ValueError(
                "FieldSentinelPolicy.field_name must be a non-empty string"
            )
        if self.comparison_mode not in _VALID_COMPARISON_MODES:
            raise ValueError(
                f"invalid comparison_mode: {self.comparison_mode!r}"
            )
        if self.unknown_value_disposition not in _VALID_UNKNOWN_DISPOSITIONS:
            raise ValueError(
                f"invalid unknown_value_disposition: "
                f"{self.unknown_value_disposition!r}"
            )
        if not isinstance(self.rules, tuple) or not self.rules:
            raise ValueError(
                "FieldSentinelPolicy.rules must be a non-empty tuple"
            )

        seen_codes: set[str] = set()
        seen_markers: set[tuple[str, object]] = set()
        for rule in self.rules:
            if not isinstance(rule, SentinelRule):
                raise TypeError(
                    "FieldSentinelPolicy.rules entries must be SentinelRule"
                )
            if rule.code in seen_codes:
                raise ValueError(
                    f"duplicate SentinelRule code within policy: {rule.code!r}"
                )
            seen_codes.add(rule.code)
            for alias in rule.aliases:
                if not _alias_compatible_with_mode(
                    alias, self.comparison_mode
                ):
                    raise TypeError(
                        f"alias {alias!r} incompatible with "
                        f"comparison_mode={self.comparison_mode!r}"
                    )
                marker = _collision_marker(
                    _normalize_for_compare(alias, self.comparison_mode)
                )
                if marker in seen_markers:
                    raise ValueError(
                        f"policy has colliding normalized aliases: {alias!r}"
                    )
                seen_markers.add(marker)


# ---------------------------------------------------------------------------
# SentinelAssessment.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SentinelAssessment:
    """Outcome of ``assess_sentinel``: pure classification, raw preserved.

    ``raw_observation`` and its ``raw_value`` are NEVER mutated by the
    classifier. ``normalized_comparison_value`` is provided purely for
    downstream traceability and equality checks; it MUST NOT be treated as
    a substitute presentation of the raw value nor as a relational key.

    Invariants:
      * ``sentinel_code`` is required and non-empty for
        ``sentinel_absence``, ``sentinel_unknown`` and ``not_applicable``;
      * ``sentinel_code`` MUST be ``None`` for ``meaningful`` and
        ``unrecognized``;
      * ``raw_observation`` is a ``FieldObservation`` (not the raw scalar);
      * ``normalized_comparison_value`` is a non-null raw scalar.
    """

    dataset_id: str
    field_name: str
    raw_observation: FieldObservation
    status: SentinelStatus
    sentinel_code: str | None
    normalized_comparison_value: RawScalar

    def __post_init__(self) -> None:
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise ValueError(
                "SentinelAssessment.dataset_id must be a non-empty string"
            )
        if not isinstance(self.field_name, str) or not self.field_name:
            raise ValueError(
                "SentinelAssessment.field_name must be a non-empty string"
            )
        if not isinstance(self.raw_observation, FieldObservation):
            raise TypeError(
                "SentinelAssessment.raw_observation must be FieldObservation"
            )
        if self.status not in _VALID_SENTINEL_STATUSES:
            raise ValueError(
                f"invalid SentinelStatus: {self.status!r}"
            )
        if self.status in {
            "sentinel_absence",
            "sentinel_unknown",
            "not_applicable",
        }:
            if not isinstance(self.sentinel_code, str) or not self.sentinel_code:
                raise ValueError(
                    f"sentinel_code is required for status={self.status!r}"
                )
        else:  # {"meaningful", "unrecognized"}
            if self.sentinel_code is not None:
                raise ValueError(
                    f"sentinel_code must be None for status={self.status!r}"
                )
        if self.normalized_comparison_value is None:
            raise ValueError(
                "SentinelAssessment.normalized_comparison_value cannot be "
                "None for an observed scalar"
            )


# ---------------------------------------------------------------------------
# Production policy catalog (immutable).
# ---------------------------------------------------------------------------

_POLICY_PROCESS_AWARD_ID = FieldSentinelPolicy(
    dataset_id="p6dx-8zbt",
    field_name="id_adjudicacion",
    comparison_mode="string_casefold_strip",
    unknown_value_disposition="pass_through_as_meaningful",
    rules=(
        SentinelRule(
            code="PROCESS_AWARD_ID_NO_AWARD",
            status="sentinel_absence",
            aliases=("No Adjudicado",),
        ),
    ),
)

_POLICY_PROCESS_SUMMARY = FieldSentinelPolicy(
    dataset_id="p6dx-8zbt",
    field_name="estado_resumen",
    comparison_mode="string_casefold_strip",
    unknown_value_disposition="pass_through_as_meaningful",
    rules=(
        SentinelRule(
            code="PROCESS_SUMMARY_UNDEFINED",
            status="sentinel_unknown",
            aliases=("No Definido",),
        ),
    ),
)

_PRODUCTION_POLICIES: Mapping[tuple[str, str], FieldSentinelPolicy] = (
    MappingProxyType(
        {
            ("p6dx-8zbt", "id_adjudicacion"): _POLICY_PROCESS_AWARD_ID,
            ("p6dx-8zbt", "estado_resumen"): _POLICY_PROCESS_SUMMARY,
        }
    )
)


def get_field_sentinel_policy(
    dataset_id: str, field_name: str
) -> FieldSentinelPolicy | None:
    """Return the authorized sentinel policy for ``(dataset_id, field_name)``.

    Lookup is EXACT. No fuzzy matching, no implicit defaults. A ``None``
    return denotes "no authorized sentinel policy for this field" — it is
    NOT synonymous with ``meaningful`` and does NOT carry any classification
    decision. Callers must not interpret ``None`` as "treat as meaningful":
    only an explicit ``FieldSentinelPolicy`` can produce a
    ``SentinelAssessment``.

    The catalog is an immutable ``MappingProxyType``; this function performs
    zero filesystem reads, zero network lookups, zero environment lookups.
    """
    if not isinstance(dataset_id, str) or not isinstance(field_name, str):
        raise TypeError(
            "get_field_sentinel_policy requires str dataset_id and field_name"
        )
    return _PRODUCTION_POLICIES.get((dataset_id, field_name))


# ---------------------------------------------------------------------------
# assess_sentinel — the pure classifier.
# ---------------------------------------------------------------------------


def assess_sentinel(
    observation: FieldObservation,
    policy: FieldSentinelPolicy,
) -> SentinelAssessment:
    """Classify ``observation`` against ``policy`` and return a
    pure ``SentinelAssessment``. Deterministic, no I/O, no clock, no
    environment, no mutation.

    Rules:
      1. The classifier requires ``FieldObservation(presence="observed")``.
         ``missing`` and ``not_observed`` are structural signal from T1A
         and MUST NOT be converted into a semantic sentinel status. The
         function raises ``ValueError`` for those presences rather than
         emitting a silent fake absence.
      2. If ``raw_value`` matches any alias of any rule under the policy's
         ``comparison_mode``, the assessment uses the rule's ``status`` and
         ``code``. The raw observation is preserved verbatim; the
         ``normalized_comparison_value`` reports only the comparison form.
      3. If no rule matches, ``unknown_value_disposition`` decides:
         ``pass_through_as_meaningful`` → ``meaningful`` (``sentinel_code=None``);
         ``classify_as_unrecognized`` → ``unrecognized``
         (``sentinel_code=None``).
      4. The raw observation and policy are never mutated.
    """
    if not isinstance(observation, FieldObservation):
        raise TypeError(
            "assess_sentinel requires a FieldObservation"
        )
    if not isinstance(policy, FieldSentinelPolicy):
        raise TypeError(
            "assess_sentinel requires a FieldSentinelPolicy"
        )
    if observation.presence != "observed":
        raise ValueError(
            "assess_sentinel requires presence='observed'; "
            f"{observation.presence!r} cannot become a sentinel status"
        )

    raw_value = observation.raw_value
    # FieldObservation.__post_init__ already enforces non-null for observed.
    mode = policy.comparison_mode

    try:
        normalized_raw = _normalize_for_compare(raw_value, mode)
    except TypeError:
        # String modes cannot classify non-string raws — surface the
        # mismatch loudly rather than silently collapsing into a sentinel.
        raise

    for rule in policy.rules:
        for alias in rule.aliases:
            if _match_alias(raw_value, alias, mode):
                return SentinelAssessment(
                    dataset_id=policy.dataset_id,
                    field_name=policy.field_name,
                    raw_observation=observation,
                    status=rule.status,
                    sentinel_code=rule.code,
                    normalized_comparison_value=normalized_raw,
                )

    if policy.unknown_value_disposition == "pass_through_as_meaningful":
        fallback_status: SentinelStatus = "meaningful"
        fallback_code: str | None = None
    elif policy.unknown_value_disposition == "classify_as_unrecognized":
        fallback_status = "unrecognized"
        fallback_code = None
    else:  # pragma: no cover — guarded by FieldSentinelPolicy.__post_init__
        raise ValueError(
            f"unsupported unknown_value_disposition: "
            f"{policy.unknown_value_disposition!r}"
        )

    return SentinelAssessment(
        dataset_id=policy.dataset_id,
        field_name=policy.field_name,
        raw_observation=observation,
        status=fallback_status,
        sentinel_code=fallback_code,
        normalized_comparison_value=normalized_raw,
    )

"""T1B-5 — public visibility resolution."""
from __future__ import annotations

import ast
import dataclasses
import inspect
from pathlib import Path

import pytest

import app.core.truth_layer.visibility_resolver as visibility_mod
from app.core.truth_layer import (
    FieldObservation,
    PublicationVisibilityAssessment,
    PublicationVisibilitySignals,
    SourceError,
    build_resolution_quality,
    resolve_publication_visibility,
    validate_evidence_set,
)


ROOT = Path(__file__).resolve().parents[1]

DATASET = "p6dx-8zbt"
PUBLIC_URL = (
    "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/index"
    "?noticeUID=CO1.NTC.1234567"
)
LOGIN_URL = "https://community.secop.gov.co/Public/Account/Login"


def _obs(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _missing() -> FieldObservation:
    return FieldObservation("missing", None)


def _not_observed() -> FieldObservation:
    return FieldObservation("not_observed", None)


def _signals(
    *,
    raw_url: FieldObservation | None = None,
    normalized_url: FieldObservation | None = None,
    process_id: FieldObservation | None = None,
    reason: FieldObservation | None = None,
) -> PublicationVisibilitySignals:
    return PublicationVisibilitySignals(
        raw_url=raw_url or _missing(),
        normalized_url=normalized_url or _missing(),
        raw_process_id=process_id or _obs("CO1.NTC.1234567"),
        normalization_reason=reason or _missing(),
    )


# ---------------------------------------------------------------------------
# Core states.
# ---------------------------------------------------------------------------


def test_a_valid_public_url_makes_the_publication_publicly_viewable() -> None:
    assessment = resolve_publication_visibility(
        _signals(raw_url=_obs(PUBLIC_URL), normalized_url=_obs(PUBLIC_URL))
    )

    assert assessment.visibility == "public_view_available"
    assert assessment.url_verdict == "public"


def test_a_syntactic_url_cannot_claim_verified_quality() -> None:
    """While no host allow-list is authorized, the quality must say so.

    The syntactic fact stands — the state stays ``public_view_available`` —
    but reporting ``complete``/``high`` would contradict our own capability
    metadata, which declares the allow-list unavailable.
    """
    assessment = resolve_publication_visibility(
        _signals(normalized_url=_obs("https://cualquier-host.example/x"))
    )

    assert assessment.visibility == "public_view_available"
    assert assessment.quality.completeness == "partial"
    assert assessment.quality.confidence == "medium"
    assert "VISIBILITY_PUBLIC_URL_HOST_UNVERIFIED" in assessment.quality.reason_codes
    assert (
        "visibility_host_allowlist",
        "unavailable",
    ) in assessment.quality.capabilities


def test_a_login_url_is_not_a_public_url() -> None:
    assessment = resolve_publication_visibility(
        _signals(raw_url=_obs(LOGIN_URL), normalized_url=_missing())
    )

    assert assessment.visibility == "link_missing_or_broken"
    assert assessment.url_verdict == "login"
    assert "VISIBILITY_URL_LOGIN" in assessment.quality.reason_codes


@pytest.mark.parametrize(
    "raw",
    [
        "javascript:alert(1)",
        "data:text/html;base64,PHA+",
        "ftp://files.secop.gov.co/x",
        "mailto:contacto@entidad.gov.co",
    ],
)
def test_non_http_schemes_are_rejected(raw: str) -> None:
    assessment = resolve_publication_visibility(_signals(normalized_url=_obs(raw)))

    assert assessment.url_verdict == "unsupported_scheme"
    assert assessment.visibility == "link_missing_or_broken"


@pytest.mark.parametrize("raw", ["", "   ", "no-es-una-url", "https://"])
def test_unusable_url_text_is_malformed_never_a_public_view(raw: str) -> None:
    assessment = resolve_publication_visibility(_signals(normalized_url=_obs(raw)))

    assert assessment.url_verdict == "malformed"
    assert assessment.visibility == "link_missing_or_broken"


def test_an_absent_url_with_an_open_data_row_is_dataset_only_visible() -> None:
    assessment = resolve_publication_visibility(
        _signals(raw_url=_missing(), normalized_url=_missing())
    )

    assert assessment.visibility == "dataset_only_visible"
    assert assessment.url_verdict == "absent"


def test_an_absent_url_alone_proves_nothing() -> None:
    assessment = resolve_publication_visibility(
        _signals(process_id=_missing(), raw_url=_missing(), normalized_url=_missing())
    )

    assert assessment.visibility == "unknown_visibility"


def test_a_req_identifier_alone_is_not_a_prepublication() -> None:
    assessment = resolve_publication_visibility(
        _signals(process_id=_obs("CO1.REQ.7654321"))
    )

    assert assessment.visibility == "dataset_only_visible"
    assert assessment.visibility != "prepublication_supported"


def test_a_draft_alone_is_not_a_prepublication() -> None:
    assessment = resolve_publication_visibility(
        _signals(process_id=_obs("CO1.NTC.1234567")), stage_context="draft"
    )

    assert assessment.visibility == "dataset_only_visible"


def test_a_draft_with_a_valid_public_url_is_still_publicly_viewable() -> None:
    assessment = resolve_publication_visibility(
        _signals(raw_url=_obs(PUBLIC_URL), normalized_url=_obs(PUBLIC_URL)),
        stage_context="draft",
    )

    assert assessment.visibility == "public_view_available"


def test_prepublication_requires_the_joint_evidence() -> None:
    assessment = resolve_publication_visibility(
        _signals(
            process_id=_obs("CO1.REQ.7654321"),
            raw_url=_missing(),
            normalized_url=_missing(),
        ),
        stage_context="draft",
    )

    assert assessment.visibility == "prepublication_supported"
    assert "VISIBILITY_PREPUBLICATION_JOINT_EVIDENCE" in assessment.quality.reason_codes


def test_prepublication_also_holds_when_a_login_url_was_rejected() -> None:
    assessment = resolve_publication_visibility(
        _signals(
            process_id=_obs("CO1.NTC.1234567"),
            raw_url=_obs(LOGIN_URL),
            normalized_url=_missing(),
            reason=_obs("login_url_rejected"),
        ),
        stage_context="approval",
    )

    assert assessment.visibility == "prepublication_supported"


def test_prepublication_requires_missing_not_unobserved_publication() -> None:
    """``not_observed`` means nobody looked; it cannot support a conclusion."""
    assessment = resolve_publication_visibility(
        _signals(
            process_id=_obs("CO1.REQ.7654321"),
            raw_url=_not_observed(),
            normalized_url=_not_observed(),
        ),
        stage_context="draft",
    )

    assert assessment.visibility == "unknown_visibility"


def test_a_published_process_without_a_url_does_not_become_a_prepublication() -> None:
    assessment = resolve_publication_visibility(
        _signals(process_id=_obs("CO1.REQ.7654321")), stage_context="published"
    )

    assert assessment.visibility == "dataset_only_visible"


def test_a_source_error_is_unknown_visibility_never_an_absence() -> None:
    error = SourceError(code="PROCESS_LOOKUP_TIMEOUT", dataset=DATASET, kind="timeout")

    assessment = resolve_publication_visibility(
        _signals(raw_url=_not_observed(), normalized_url=_not_observed()),
        source_errors=(error,),
    )

    assert assessment.visibility == "unknown_visibility"
    assert assessment.quality.source_errors == (error,)
    assert assessment.quality.consistency == "unknown"
    assert assessment.visibility != "link_missing_or_broken"


def test_a_source_error_outranks_an_observed_url() -> None:
    error = SourceError(
        code="PROCESS_TRANSPORT_ERROR", dataset=DATASET, kind="transport_error"
    )

    assessment = resolve_publication_visibility(
        _signals(normalized_url=_obs(PUBLIC_URL)), source_errors=(error,)
    )

    assert assessment.visibility == "unknown_visibility"


def test_source_errors_from_another_dataset_are_rejected() -> None:
    error = SourceError(code="CONTRACT_TIMEOUT", dataset="jbjy-vk9h", kind="timeout")

    with pytest.raises(ValueError, match="p6dx-8zbt"):
        resolve_publication_visibility(_signals(), source_errors=(error,))


# ---------------------------------------------------------------------------
# Raw preservation and boundaries.
# ---------------------------------------------------------------------------


def test_raw_and_normalized_urls_are_preserved_separately() -> None:
    raw = _obs("  " + PUBLIC_URL + "  ")
    normalized = _obs(PUBLIC_URL)
    signals = _signals(raw_url=raw, normalized_url=normalized)

    assessment = resolve_publication_visibility(signals)

    assert assessment.raw_url is raw
    assert assessment.normalized_url is normalized
    assert assessment.raw_url.raw_value != assessment.normalized_url.raw_value
    assert assessment.visibility == "public_view_available"


def test_every_signal_becomes_evidence_exactly_once() -> None:
    assessment = resolve_publication_visibility(
        _signals(raw_url=_obs(LOGIN_URL), normalized_url=_missing())
    )

    fields = [item.field for item in assessment.evidence]
    assert fields == sorted(fields)
    assert len(fields) == len(set(fields)) == 4
    roles = {item.field: item.role for item in assessment.evidence}
    assert roles["urlproceso"] == "contradicts"
    assert roles["urlproceso_normalized"] == "missing"


def test_fixed_visibility_order_is_canonical_for_every_current_case() -> None:
    assert visibility_mod._EVIDENCE_FIELD_ORDER == tuple(
        sorted(visibility_mod._EVIDENCE_FIELD_ORDER)
    )
    error = SourceError(code="PROCESS_LOOKUP_TIMEOUT", dataset=DATASET, kind="timeout")
    cases = (
        (_signals(normalized_url=_obs(PUBLIC_URL)), None, ()),
        (_signals(raw_url=_obs(LOGIN_URL)), None, ()),
        (_signals(normalized_url=_obs("ftp://example.test/x")), None, ()),
        (_signals(normalized_url=_obs("not-a-url")), None, ()),
        (_signals(), None, ()),
        (_signals(process_id=_missing()), None, ()),
        (_signals(process_id=_obs("CO1.REQ.1")), "draft", ()),
        (
            _signals(
                raw_url=_obs(LOGIN_URL),
                process_id=_obs("CO1.NTC.1"),
                reason=_obs("login_url_rejected"),
            ),
            "approval",
            (),
        ),
        (
            _signals(raw_url=_not_observed(), normalized_url=_not_observed()),
            None,
            (),
        ),
        (
            _signals(raw_url=_not_observed(), normalized_url=_not_observed()),
            None,
            (error,),
        ),
    )

    for signals, stage_context, source_errors in cases:
        assessment = resolve_publication_visibility(
            signals,
            stage_context=stage_context,
            source_errors=source_errors,
        )
        assert validate_evidence_set(assessment.evidence) is assessment.evidence
        assert tuple(item.field for item in assessment.evidence) == (
            visibility_mod._EVIDENCE_FIELD_ORDER
        )


def test_the_resolver_never_rebuilds_a_url() -> None:
    executable = _executable_source(visibility_mod)

    for forbidden in ("http://", "https://", "noticeuid", "urljoin", "quote("):
        assert forbidden not in executable


def test_visibility_states_no_actionability() -> None:
    assessment = resolve_publication_visibility(_signals(normalized_url=_obs(PUBLIC_URL)))

    for forbidden in ("offerability", "role", "bid_ready", "actionable", "score"):
        assert not hasattr(assessment, forbidden)


def test_the_stage_is_injected_data_not_an_inference() -> None:
    signals = _signals(process_id=_obs("CO1.REQ.7654321"))

    without_stage = resolve_publication_visibility(signals)
    with_stage = resolve_publication_visibility(signals, stage_context="draft")

    assert without_stage.stage_context is None
    assert without_stage.visibility == "dataset_only_visible"
    assert with_stage.visibility == "prepublication_supported"


# ---------------------------------------------------------------------------
# Public invariants and mutation probes.
# ---------------------------------------------------------------------------


def test_the_assessment_is_frozen_slotted_and_hashable() -> None:
    assessment = resolve_publication_visibility(_signals(normalized_url=_obs(PUBLIC_URL)))

    assert PublicationVisibilityAssessment.__dataclass_params__.frozen is True
    assert "__slots__" in PublicationVisibilityAssessment.__dict__
    assert "__dict__" not in PublicationVisibilityAssessment.__dict__
    assert isinstance(hash(assessment), int)


def test_replace_cannot_rewrite_the_visibility() -> None:
    assessment = resolve_publication_visibility(
        _signals(raw_url=_obs(LOGIN_URL), normalized_url=_missing())
    )

    with pytest.raises(ValueError, match="visibility"):
        dataclasses.replace(assessment, visibility="public_view_available")


def test_replace_cannot_rewrite_the_url_verdict() -> None:
    assessment = resolve_publication_visibility(
        _signals(raw_url=_obs(LOGIN_URL), normalized_url=_missing())
    )

    with pytest.raises(ValueError, match="url_verdict"):
        dataclasses.replace(assessment, url_verdict="public")


def test_raw_observations_from_another_row_are_rejected() -> None:
    assessment = resolve_publication_visibility(_signals(normalized_url=_obs(PUBLIC_URL)))
    foreign = _obs("CO1.NTC.9999999")

    with pytest.raises(ValueError, match="raw_process_id"):
        dataclasses.replace(assessment, raw_process_id=foreign)


def test_evidence_from_another_row_is_rejected() -> None:
    assessment = resolve_publication_visibility(_signals(normalized_url=_obs(PUBLIC_URL)))
    other = resolve_publication_visibility(
        _signals(normalized_url=_obs(PUBLIC_URL), process_id=_obs("CO1.NTC.7777777"))
    )

    with pytest.raises(ValueError, match="evidence"):
        dataclasses.replace(assessment, evidence=other.evidence)


def test_quality_cannot_drop_a_source_error() -> None:
    error = SourceError(code="PROCESS_LOOKUP_TIMEOUT", dataset=DATASET, kind="timeout")
    assessment = resolve_publication_visibility(
        _signals(raw_url=_not_observed(), normalized_url=_not_observed()),
        source_errors=(error,),
    )
    stripped = build_resolution_quality(
        completeness=assessment.quality.completeness,
        consistency=assessment.quality.consistency,
        confidence=assessment.quality.confidence,
        reason_codes=assessment.quality.reason_codes,
        missing_fields=assessment.quality.missing_fields,
        source_errors=(),
        capabilities=assessment.quality.capabilities,
    )

    with pytest.raises(ValueError, match="quality"):
        dataclasses.replace(assessment, quality=stripped)


def test_quality_cannot_gain_a_conflict_that_was_never_derived() -> None:
    assessment = resolve_publication_visibility(_signals(normalized_url=_obs(PUBLIC_URL)))
    inflated = build_resolution_quality(
        completeness=assessment.quality.completeness,
        consistency="conflicting",
        confidence=assessment.quality.confidence,
        reason_codes=assessment.quality.reason_codes,
        capabilities=assessment.quality.capabilities,
    )

    with pytest.raises(ValueError, match="quality"):
        dataclasses.replace(assessment, quality=inflated)


def test_no_confidence_scores_are_produced() -> None:
    assessment = resolve_publication_visibility(_signals(normalized_url=_obs(PUBLIC_URL)))

    assert assessment.quality.confidence in {"high", "medium", "low"}
    assert ("visibility_host_allowlist", "unavailable") in assessment.quality.capabilities


# ---------------------------------------------------------------------------
# Purity and scope.
# ---------------------------------------------------------------------------


def _executable_source(module: object) -> str:
    tree = ast.parse(inspect.getsource(module))  # type: ignore[arg-type]
    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            node.body = body[1:] or [ast.Pass()]
    return ast.unparse(ast.fix_missing_locations(tree)).casefold()


def test_visibility_resolver_imports_nothing_outside_its_layer() -> None:
    tree = ast.parse(inspect.getsource(visibility_mod))
    roots: set[str] = set()
    app_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
            if node.module.startswith("app."):
                app_modules.add(node.module)

    assert roots <= {"__future__", "dataclasses", "typing", "app"}
    assert all(module.startswith("app.core.truth_layer.") for module in app_modules)
    # T1B-5 depends on T1B-1 only: the stage arrives as data.
    assert "app.core.truth_layer.process_lifecycle" not in app_modules
    assert not any(
        module.startswith("app.core.truth_layer.contract_") for module in app_modules
    )
    assert "app.core.truth_layer.temporal_resolver" not in app_modules


def test_visibility_resolver_performs_no_io_and_reads_no_clock() -> None:
    tree = ast.parse(inspect.getsource(visibility_mod))
    called: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            target = node.func
            if isinstance(target, ast.Attribute):
                called.add(target.attr)
            elif isinstance(target, ast.Name):
                called.add(target.id)

    for forbidden in ("now", "today", "open", "getenv", "request", "urlopen", "read_text"):
        assert forbidden not in called


def test_visibility_resolver_has_no_runtime_consumers() -> None:
    consumers: list[str] = []
    for path in (ROOT / "app").rglob("*.py"):
        if path.parent.name == "truth_layer" and path.name in {
            "visibility_resolver.py",
            "__init__.py",
        }:
            continue
        if "truth_layer.visibility_resolver" in path.read_text(encoding="utf-8"):
            consumers.append(str(path.relative_to(ROOT)))

    assert consumers == []


def test_visibility_resolver_is_single_pass_and_replace_rederives(
    monkeypatch,
) -> None:
    calls = 0
    captured = []
    original = visibility_mod._derive

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        derived = original(*args, **kwargs)
        captured.append(derived)
        return derived

    monkeypatch.setattr(visibility_mod, "_derive", counted)
    assessment = resolve_publication_visibility(
        _signals(raw_url=_obs(PUBLIC_URL))
    )
    assert calls == 1
    assert dataclasses.replace(assessment) == assessment
    assert calls == 2
    manual_kwargs = {
        field.name: getattr(assessment, field.name)
        for field in dataclasses.fields(assessment)
    }
    assert type(assessment)(**manual_kwargs) == assessment
    assert calls == 3
    assert (
        dataclasses.replace(
            assessment, _resolved_derivation=captured[0]
        )
        == assessment
    )
    assert calls == 3


def test_visibility_initvar_is_ephemeral_and_rejects_foreign_signals(
    monkeypatch,
) -> None:
    captured = []
    original = visibility_mod._derive

    def capture(*args, **kwargs):
        derived = original(*args, **kwargs)
        captured.append(derived)
        return derived

    monkeypatch.setattr(visibility_mod, "_derive", capture)
    assessment = resolve_publication_visibility(
        _signals(raw_url=_obs(PUBLIC_URL))
    )
    proof = captured[0]
    assert "_resolved_derivation" not in {
        field.name for field in dataclasses.fields(assessment)
    }
    assert "_resolved_derivation" not in assessment.__slots__
    assert "_resolved_derivation" not in repr(assessment)

    with pytest.raises(TypeError, match="internal visibility derivation"):
        dataclasses.replace(assessment, _resolved_derivation=object())
    foreign = dataclasses.replace(assessment.signals)
    with pytest.raises(ValueError, match="different signals"):
        dataclasses.replace(
            assessment,
            _resolved_derivation=dataclasses.replace(proof, signals=foreign),
        )
    with pytest.raises(ValueError, match="different evidence or quality"):
        dataclasses.replace(
            assessment,
            _resolved_derivation=dataclasses.replace(
                proof, evidence=tuple(list(proof.evidence))
            ),
        )
    with pytest.raises(ValueError, match="different evidence or quality"):
        dataclasses.replace(
            assessment,
            _resolved_derivation=dataclasses.replace(
                proof, quality=dataclasses.replace(proof.quality)
            ),
        )

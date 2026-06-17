"""E2E multi-turn dialogue invariants — anti "tailor's syndrome".

Drives the REAL workflow (SecopalWorkflow.run_query) with the REAL parser
and persistence round-trip; only SECOP is mocked. Assertions are generic
INVARIANTS derived from the user's declared intent, not cherry-picked
outputs — so a sequence cannot be quietly cut to fit current behavior.

Invariants (see fixture header):
  I1 block   ⇒ needs_clarification, ZERO SECOP calls, empty SoQL
  I2 execute ⇒ not needs_clarification, non-empty SoQL
  I3 the last territory the user named must persist until they name another
  I4 after a dataset change with no new date, fecha_desde must be None
"""
from __future__ import annotations

import unicodedata
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.opportunity_scope_policy import evaluate_opportunity_scope
from app.core.orchestrator import SecopalWorkflow

FIXTURE = Path(__file__).parent / "fixtures" / "dialogue_e2e_corpus.yaml"


def _norm(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (s or "").lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _mk_results(n: int = 5) -> list[dict]:
    return [{
        "nombre_del_procedimiento": f"Obra {i}",
        "entidad": "ENTIDAD X",
        "precio_base": str(100_000_000 * (i + 1)),
        "valor_del_contrato": str(100_000_000 * (i + 1)),
        "estado_de_apertura_del_proceso": "Abierto",
        "fecha_de_publicacion_del": "2026-02-15T00:00:00.000",
        "modalidad_de_contratacion": "Licitacion Publica",
        "urlproceso": "https://example.com",
    } for i in range(n)]


def _territory_present(params: dict, territory: str) -> bool:
    """El territorio puede vivir como scope (depto/ciudad) o embebido en una
    entidad concreta (p. ej. 'gobernación del atlántico')."""
    target = _norm(territory)
    for key in ("departamento_resolved", "ciudad", "departamento"):
        if _norm(str(params.get(key, ""))) == target:
            return True
    for key in ("entidad", "entidad_resolved", "entidad_like"):
        if target and target in _norm(str(params.get(key, ""))):
            return True
    return False


def _load_sequences():
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    return [(seq["id"], seq) for seq in data["sequences"]]


@pytest.mark.parametrize("seq_id,seq", _load_sequences(), ids=lambda v: v if isinstance(v, str) else "")
def test_dialogue_invariants(seq_id, seq, tmp_path):
    wf = SecopalWorkflow(Settings())
    calls = {"n": 0}

    def _query(dataset_id, soql_query):
        calls["n"] += 1
        return _mk_results(5)

    def _count(dataset_id, soql_query):
        calls["n"] += 1
        return 5

    wf.secop_client.query = _query  # type: ignore[method-assign]
    wf.secop_client.count = _count  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")

    chat_id = seq_id
    active_territory: str | None = None
    date_since_dataset = False

    for i, turn in enumerate(seq["turns"]):
        text = turn["text"]
        if turn.get("says_territory"):
            active_territory = turn["says_territory"]
        if turn.get("changes_dataset"):
            date_since_dataset = False
        if turn.get("says_date"):
            date_since_dataset = True

        calls["n"] = 0
        result = wf.run_query(text, channel="telegram", chat_id=chat_id)
        params = result.get("parsed_params", {}) or {}
        needs_clar = bool(result.get("needs_clarification"))
        soql = result.get("soql_query") or ""
        where = f"[{seq_id}] turno {i+1} {text!r}"

        # I1 / I2 — outcome contract + SECOP discipline
        expect = turn.get("expect")
        if expect == "block":
            assert needs_clar, f"{where}: se esperaba bloqueo (needs_clarification)"
            assert calls["n"] == 0, f"{where}: bloqueo NO debe llamar a SECOP (hizo {calls['n']})"
            assert not soql, f"{where}: bloqueo no debe generar SoQL, got {soql!r}"
        elif expect == "execute":
            assert not needs_clar, f"{where}: se esperaba ejecución, got clarification"
            assert soql, f"{where}: ejecución debe generar SoQL"

        # I2b — clarification message check (cuando se declara)
        want = turn.get("expect_reason_contains")
        if want:
            blob = _norm(result.get("response", "") + " " + str(result.get("clarification_reason", "")))
            assert _norm(want) in blob, (
                f"{where}: la aclaración no contiene {want!r}; "
                f"respuesta={result.get('response','')[:80]!r}"
            )

        # I3 — territory persistence
        if active_territory is not None:
            assert _territory_present(params, active_territory), (
                f"{where}: se perdió el territorio '{active_territory}' "
                f"(params territoriales: depto={params.get('departamento_resolved')!r} "
                f"ciudad={params.get('ciudad')!r})"
            )

        # I4 — no date hallucination after a dataset pivot
        if not date_since_dataset:
            assert params.get("fecha_desde") in (None, "", False), (
                f"{where}: fecha_desde={params.get('fecha_desde')!r} sin que el usuario "
                f"mencionara fecha desde el último cambio de dataset (poisoning)"
            )

        # I5 — an EXECUTED opportunity search must satisfy the scope policy
        # (no P15 bypass). Reuses the real policy as oracle, one direction only.
        if not needs_clar and params.get("intent_type") == "opportunity_search":
            verdict = evaluate_opportunity_scope(text, params)
            assert verdict.should_execute, (
                f"{where}: ejecutó una oportunidad que la política bloquearía "
                f"(reason={verdict.reason!r}) — posible bypass de P15. params={params!r}"
            )


@pytest.mark.xfail(
    reason="BUG hallado por el corpus adversarial: detect_direct_lookup "
    "clasifica 'contratos de salud en huila' como entity-ref 'SALUD-EN' "
    "(dispara en primer turno; ajeno al follow-up engine). Falso positivo "
    "en app/core/workflow/handlers/direct_lookup.py — secuestra búsquedas "
    "normales a la ruta de lookup por referencia.",
    strict=True,
)
def test_known_bug_detect_direct_lookup_false_positive():
    from app.core.workflow.handlers.direct_lookup import detect_direct_lookup
    value, kind = detect_direct_lookup("contratos de salud en huila")
    assert value is None, f"misclasificó como referencia {value!r} ({kind})"

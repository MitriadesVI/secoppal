"""Etapa 0 — Regresiones deterministas de la corrida dogfood_20260627.

Congela como tests permanentes los fallos DETERMINISTAS (params/SoQL, sin red)
detectados en el dogfood. Cada caso roto va ``xfail`` con la etapa del
PLAN_DOGFOOD_REMEDIACION_2026-06-27.md que lo arregla; cuando esa etapa lo
repare, se quita ``xfail`` en el fixture y el caso debe quedar verde.

Filosofía (igual que test_frame_leak_canary.py): asertar SUSTANCIA — el objeto
contractual y los params resueltos — no la etiqueta de intent. El nombre del
intent se documenta en ``note_observed_intent`` pero NO se asercta, para no
acoplar el fix a un identificador que el implementador podría renombrar.

Harness offline y determinista:
- ``QueryRouter().parse(query)``  → params del turno actual.
- ``detect_and_merge(text, current_params, previous_frame)`` → (intent, merged).
Ambos resuelven entidad/estado contra los datos locales (aliases_db.json), sin
tocar datos.gov.co. Los casos de RECALL VIVO (params correctos, fallo de
ejecución) se marcan ``skip``: se miden con run_dogfood.py + eval_dogfood.py.

Fuente: tests/fixtures/dogfood_followup_regressions.json
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.direct_lookup import detect_direct_lookup
from app.core.followup_engine import detect_and_merge
from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter

_FIXTURE = Path(__file__).parent / "fixtures" / "dogfood_followup_regressions.json"
_DATA = json.loads(_FIXTURE.read_text(encoding="utf-8"))
_CASES = _DATA["cases"]


# ── Helpers ─────────────────────────────────────────────────────────────────

def _flatten_objeto(objeto) -> list[str]:
    """objeto puede ser plano o anidado (grupos AND/OR). Devuelve tokens str."""
    out: list[str] = []
    if objeto is None:
        return out
    for item in (objeto if isinstance(objeto, list) else [objeto]):
        if isinstance(item, list):
            out.extend(_flatten_objeto(item))
        elif item is not None:
            out.append(str(item))
    return out


def _detected_entity(detection) -> str | None:
    """Extrae la entidad de la detección de lookup directo.

    Hoy detect_direct_lookup devuelve (referencia, kind) — SIN entidad → None.
    Etapa 1 debe exponerla (3er elemento de la tupla, o un objeto con .entity /
    .entidad). Este helper tolera ambas formas para que el caso pase en verde
    cuando se implemente, sin reescribir el test.
    """
    if detection is None:
        return None
    if isinstance(detection, (tuple, list)) and len(detection) >= 3:
        cand = detection[2]
        return str(cand) if cand else None
    for attr in ("entity", "entidad", "entidad_resolved"):
        cand = getattr(detection, attr, None)
        if cand:
            return str(cand)
    return None


def _check_params(params: dict, case: dict) -> None:
    """Asercta expected_params (==, null=ausente/falsy) y expected_param_contains."""
    for key, expected in case.get("expected_params", {}).items():
        actual = params.get(key)
        if expected is None:
            assert not actual, f"{key} debería estar ausente/vacío, es {actual!r}"
        else:
            assert actual == expected, f"{key}: esperado {expected!r}, obtenido {actual!r}"
    for key, substr in case.get("expected_param_contains", {}).items():
        actual = params.get(key)
        assert actual is not None, f"{key} ausente; debía contener {substr!r}"
        assert substr.lower() in str(actual).lower(), (
            f"{key}={actual!r} no contiene {substr!r}"
        )


# ── Parametrización con marcas (xfail / skip) desde el fixture ──────────────

def _param(case: dict):
    marks = []
    if case.get("skip"):
        marks.append(pytest.mark.skip(reason=case.get("skip_reason", "skip")))
    elif case.get("xfail"):
        # strict=True: cuando una etapa repare el caso, el XPASS ROMPE la suite y
        # obliga a retirar la marca del fixture (no se olvida una regresión muerta).
        marks.append(pytest.mark.xfail(reason=case.get("xfail_reason", "pendiente"), strict=True))
    return pytest.param(case, id=case["case_id"], marks=marks)


_PARAMS = [_param(c) for c in _CASES]


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


@pytest.mark.parametrize("case", _PARAMS)
def test_dogfood_regression(case: dict, router: QueryRouter) -> None:
    harness = case["harness"]

    if harness == "followup":
        cur = router.parse(case["query"]).params
        prev = case.get("prev_params") or {}
        _intent, merged = detect_and_merge(
            text=case["query"],
            current_params=cur,
            previous_frame=frame_from_params(prev),
        )
        objeto = _flatten_objeto(merged.get("objeto"))
        lowered = [t.lower() for t in objeto]
        for tok in case.get("forbidden_objeto_tokens", []):
            assert tok.lower() not in lowered, (
                f"objeto contaminado con token de control {tok!r}: {objeto}"
            )
        if "expected_objeto" in case:
            assert objeto == case["expected_objeto"], (
                f"objeto esperado {case['expected_objeto']}, obtenido {objeto}"
            )
        _check_params(merged, case)

    elif harness == "parse":
        params = router.parse(case["query"]).params
        _check_params(params, case)

    elif harness == "direct_lookup":
        detection = detect_direct_lookup(case["query"])
        entity = _detected_entity(detection)
        assert entity, (
            "el lookup directo no expone la entidad del query "
            f"(detección: {detection!r})"
        )
        assert case["expected_entity_contains"].lower() in entity.lower(), (
            f"entidad detectada {entity!r} no contiene "
            f"{case['expected_entity_contains']!r}"
        )

    else:  # pragma: no cover - guard de fixture malformado
        raise AssertionError(f"harness desconocido: {harness!r}")


def test_fixture_is_well_formed() -> None:
    """Meta-test (verde): el fixture trae las claves mínimas por caso."""
    seen_ids: set[str] = set()
    for case in _CASES:
        for key in ("case_id", "category", "stage", "harness", "query"):
            assert key in case, f"caso sin {key!r}: {case.get('case_id', case)}"
        assert case["case_id"] not in seen_ids, f"case_id duplicado: {case['case_id']}"
        seen_ids.add(case["case_id"])
        assert case["harness"] in {"followup", "parse", "direct_lookup"}

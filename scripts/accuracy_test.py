#!/usr/bin/env python3
"""
scripts/accuracy_test.py
Compara la accuracy de parseo/resolución entre:
  - Stack V1: query_router.py + entity_resolver.py (heurístico clásico)
  - Stack V2: query_router v2.py + entity_resolver v2.py (gazetteer-first, agresivo)
  - Stack V3: query_router v2.py + entity_resolver v3.py (gazetteer-first + disambiguación depts)

Métricas evaluadas por query:
  dataset, departamento, entidad, estado, modalidad,
  valor_min, valor_max, fecha_desde, fecha_hasta, objeto, needs_llm

Uso:
    cd /path/to/secoppal
    python scripts/accuracy_test.py
    python scripts/accuracy_test.py --verbose
    python scripts/accuracy_test.py --output results.json
    python scripts/accuracy_test.py --category entidad
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ── Setup de rutas ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
logging.disable(logging.CRITICAL)  # Silenciar logs internos durante testing

# ── Colores ANSI para terminal ─────────────────────────────────────────────────
_USE_COLOR = sys.stdout.isatty()
GREEN  = "\033[92m" if _USE_COLOR else ""
RED    = "\033[91m" if _USE_COLOR else ""
YELLOW = "\033[93m" if _USE_COLOR else ""
CYAN   = "\033[96m" if _USE_COLOR else ""
BOLD   = "\033[1m"  if _USE_COLOR else ""
RESET  = "\033[0m"  if _USE_COLOR else ""

# ── Importar componentes V1 (nombres normales) ─────────────────────────────────
from app.core.query_router import QueryRouter as QueryRouterV1
from app.core.entity_resolver import EntityResolver as EntityResolverV1
from app.utils.money import normalize_text

# ── Importar componentes V2 (archivos con espacios — via importlib) ────────────
def _load_module(name: str, path: Path):
    """Carga un módulo Python desde una ruta con espacios en el nombre de archivo."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod       # Registrar para que imports internos funcionen
    spec.loader.exec_module(mod)
    return mod

_core = PROJECT_ROOT / "app" / "core"
_er_v2_mod = _load_module("entity_resolver_v2", _core / "entity_resolver v2.py")
_er_v3_mod = _load_module("entity_resolver_v3", _core / "entity_resolver v3.py")
_qr_v2_mod = _load_module("query_router_v2",    _core / "query_router v2.py")
EntityResolverV2 = _er_v2_mod.EntityResolver
EntityResolverV3 = _er_v3_mod.EntityResolver
QueryRouterV2    = _qr_v2_mod.QueryRouter

# ── Inicializar instancias (se cargan una sola vez) ────────────────────────────
_ALIAS_DB = PROJECT_ROOT / "app" / "data" / "aliases_db.json"

print(f"{CYAN}Cargando gazetteer y modelos...{RESET}", flush=True)
_t0 = time.time()
_er_v1 = EntityResolverV1(_ALIAS_DB)
_er_v2 = EntityResolverV2(_ALIAS_DB)
_er_v3 = EntityResolverV3(_ALIAS_DB)
_qr_v1 = QueryRouterV1()
_qr_v2 = QueryRouterV2(entity_resolver=_er_v2)
_qr_v3 = QueryRouterV2(entity_resolver=_er_v3)   # mismo router v2 pero con er_v3
print(f"{CYAN}Listo en {time.time()-_t0:.1f}s{RESET}\n", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# DEFINICIÓN DE CASOS DE PRUEBA
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TestCase:
    query: str
    expected: dict[str, Any]
    category: str
    description: str

# Claves válidas en `expected`:
#   dataset           → "procesos" | "contratos"
#   departamento      → alias normalizado sin tildes (ej: "atlantico", "narino")
#   entidad_hint      → substring que DEBE aparecer en entidad extraída (ej: "sena")
#   estado            → valor oficial estado (ej: "Abierto", "liquidado")
#   modalidad         → valor oficial modalidad (ej: "Licitación pública")
#   valor_min/max     → int en COP (tolerancia ±5%)
#   fecha_desde/hasta → "YYYY-MM-DD"
#   objeto_contains   → lista de palabras que deben aparecer en objeto
#   needs_llm         → bool

TEST_CASES: list[TestCase] = [

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 1: Dataset (5 casos)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "contratos firmados en Bogota 2024",
        {"dataset": "contratos"},
        "dataset", "Dataset: contratos por 'firmados'",
    ),
    TestCase(
        "licitaciones abiertas en Valle del Cauca",
        {"dataset": "procesos"},
        "dataset", "Dataset: procesos para licitaciones abiertas",
    ),
    TestCase(
        "procesos de infraestructura vial en Cundinamarca",
        {"dataset": "procesos"},
        "dataset", "Dataset: procesos explícito",
    ),
    TestCase(
        "contratos en ejecucion del INVIAS",
        {"dataset": "contratos"},
        "dataset", "Dataset: contratos por 'en ejecucion'",
    ),
    TestCase(
        "contratos liquidados del SENA en 2023",
        {"dataset": "contratos"},
        "dataset", "Dataset: contratos por 'liquidados'",
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 2: Departamento (8 casos)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "procesos abiertos en Antioquia",
        {"dataset": "procesos", "departamento": "antioquia", "estado": "Abierto"},
        "departamento", "Departamento: Antioquia + estado",
    ),
    TestCase(
        "licitaciones de Cundinamarca por mas de 500 millones",
        {"dataset": "procesos", "departamento": "cundinamarca", "valor_min": 500_000_000},
        "departamento", "Departamento + monto mínimo: Cundinamarca",
    ),
    TestCase(
        "contratos del Valle del Cauca en 2023",
        {"dataset": "contratos", "departamento": "valle del cauca", "fecha_desde": "2023-01-01"},
        "departamento", "Departamento multi-palabra: Valle del Cauca",
    ),
    TestCase(
        "obras en Narino abiertas",
        {"departamento": "narino", "estado": "Abierto"},
        "departamento", "Departamento: Nariño",
    ),
    TestCase(
        "licitaciones en Bolivar por mas de 200 millones",
        {"departamento": "bolivar", "valor_min": 200_000_000},
        "departamento", "Departamento Bolívar + monto",
    ),
    TestCase(
        "contratos en el Cesar liquidados",
        {"dataset": "contratos", "departamento": "cesar"},
        "departamento", "Departamento: El Cesar",
    ),
    TestCase(
        "proyectos en Norte de Santander abiertos",
        {"departamento": "norte de santander", "estado": "Abierto"},
        "departamento", "Departamento compuesto: Norte de Santander (4 palabras)",
    ),
    TestCase(
        "procesos de construccion en Bogota abiertos",
        {"departamento": "bogota", "estado": "Abierto", "objeto_contains": ["construccion"]},
        "departamento", "Bogotá DC + objeto",
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 3: Entidad (8 casos)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "licitaciones de la Gobernacion de Antioquia",
        {"entidad_hint": "gobernacion", "departamento": "antioquia"},
        "entidad", "Entidad: Gobernación de Antioquia",
    ),
    TestCase(
        "contratos del SENA en Cundinamarca",
        {"dataset": "contratos", "entidad_hint": "sena", "departamento": "cundinamarca"},
        "entidad", "Entidad acrónimo: SENA + depto",
    ),
    TestCase(
        "procesos del ICBF abiertos",
        {"entidad_hint": "icbf", "estado": "Abierto"},
        "entidad", "Entidad acrónimo: ICBF + estado",
    ),
    TestCase(
        "contratos del Ministerio de Salud en 2024",
        {"dataset": "contratos", "entidad_hint": "ministerio", "fecha_desde": "2024-01-01"},
        "entidad", "Entidad: Ministerio de Salud + fecha",
    ),
    TestCase(
        "procesos del INVIAS en Cundinamarca",
        {"entidad_hint": "invias", "departamento": "cundinamarca"},
        "entidad", "Entidad: INVIAS + departamento",
    ),
    TestCase(
        "contratos de la Universidad Nacional de Colombia",
        {"dataset": "contratos", "entidad_hint": "universidad"},
        "entidad", "Entidad: Universidad Nacional",
    ),
    TestCase(
        "licitaciones de la Gobernacion de Santander abiertas",
        {"entidad_hint": "gobernacion", "estado": "Abierto"},
        "entidad", "AMBIGUO: 'Gobernacion de Santander' (v2 scan evita split)",
    ),
    TestCase(
        "procesos de la Alcaldia de Bogota de infraestructura",
        {"entidad_hint": "alcaldia", "objeto_contains": ["infraestructura"]},
        "entidad", "Entidad: Alcaldía de Bogotá + objeto",
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 4: Estado (5 casos)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "procesos adjudicados en Bogota 2024",
        {"dataset": "procesos", "estado": "Seleccionado", "departamento": "bogota"},
        "estado", "Estado: adjudicado → Seleccionado",
    ),
    TestCase(
        "contratos liquidados del SENA",
        {"dataset": "contratos", "estado": "liquidado", "entidad_hint": "sena"},
        "estado", "Estado contrato: liquidado (lowercase SECOP)",
    ),
    TestCase(
        "licitaciones en evaluacion en Antioquia",
        {"estado": "Evaluación", "departamento": "antioquia"},
        "estado", "Estado: evaluación",
    ),
    TestCase(
        "contratos en ejecucion del INVIAS",
        {"dataset": "contratos", "estado": "en ejecucion", "entidad_hint": "invias"},
        "estado", "Estado contrato: en ejecución",
    ),
    TestCase(
        "procesos cerrados del INVIAS en 2023",
        {"estado": "Cerrado", "entidad_hint": "invias"},
        "estado", "Estado: cerrado",
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 5: Modalidad (4 casos)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "licitacion publica en Antioquia por mas de 1000 millones",
        {"modalidad": "Licitación pública", "departamento": "antioquia", "valor_min": 1_000_000_000},
        "modalidad", "Modalidad: licitación pública",
    ),
    TestCase(
        "procesos de minima cuantia en Valle del Cauca",
        {"modalidad": "Mínima cuantía", "departamento": "valle del cauca"},
        "modalidad", "Modalidad: mínima cuantía",
    ),
    TestCase(
        "contratacion directa del ICBF en Bogota",
        {"modalidad": "Contratación directa", "entidad_hint": "icbf"},
        "modalidad", "Modalidad: contratación directa",
    ),
    TestCase(
        "seleccion abreviada en Cundinamarca abierta",
        {"modalidad": "Selección Abreviada de Menor Cuantía", "departamento": "cundinamarca"},
        "modalidad", "Modalidad: selección abreviada",
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 6: Montos (6 casos)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "procesos por mas de 1000 millones en Bogota",
        {"valor_min": 1_000_000_000, "departamento": "bogota"},
        "montos", "Valor mínimo: 1000 millones",
    ),
    TestCase(
        "contratos menores a 500 millones en Antioquia",
        {"dataset": "contratos", "valor_max": 500_000_000, "departamento": "antioquia"},
        "montos", "Valor máximo: 500 millones",
    ),
    TestCase(
        "licitaciones de mas de 2 billones",
        {"valor_min": 2_000_000_000_000},
        "montos", "Valor mínimo: 2 billones",
    ),
    TestCase(
        "contratos por mas de 50 palos en Cundinamarca",
        {"dataset": "contratos", "valor_min": 50_000_000, "departamento": "cundinamarca"},
        "montos", "Jerga 'palos' = millones",
    ),
    TestCase(
        "procesos superiores a 800 millones en Valle del Cauca",
        {"valor_min": 800_000_000, "departamento": "valle del cauca"},
        "montos", "Valor mínimo: 'superiores a'",
    ),
    TestCase(
        "licitaciones de menos de 200 millones en Narino",
        {"valor_max": 200_000_000, "departamento": "narino"},
        "montos", "Valor máximo: 'menos de'",
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 7: Fechas (5 casos)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "procesos desde 2022 en Antioquia",
        {"fecha_desde": "2022-01-01", "departamento": "antioquia"},
        "fechas", "Fecha desde año",
    ),
    TestCase(
        "contratos hasta 2023 del SENA",
        {"dataset": "contratos", "fecha_hasta": "2023-12-31", "entidad_hint": "sena"},
        "fechas", "Fecha hasta año",
    ),
    TestCase(
        "licitaciones en 2024 en Cundinamarca",
        {"fecha_desde": "2024-01-01", "fecha_hasta": "2024-12-31", "departamento": "cundinamarca"},
        "fechas", "Año completo: en 2024",
    ),
    TestCase(
        "procesos desde 2021-01-01 hasta 2021-06-30",
        {"fecha_desde": "2021-01-01", "fecha_hasta": "2021-06-30"},
        "fechas", "Fechas ISO explícitas",
    ),
    TestCase(
        "contratos del INVIAS desde 2023 hasta 2024",
        {
            "dataset": "contratos",
            "fecha_desde": "2023-01-01",
            "fecha_hasta": "2024-12-31",
            "entidad_hint": "invias",
        },
        "fechas", "Rango de años con entidad",
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 8: Queries combinados (7 casos)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas",
        {
            "departamento": "atlantico",
            "valor_min": 500_000_000,
            "estado": "Abierto",
            "objeto_contains": ["mantenimiento", "vial"],
        },
        "combinado", "Depto + monto + estado + objeto",
    ),
    TestCase(
        "contratos del INVIAS en Cundinamarca en 2024 en ejecucion",
        {
            "dataset": "contratos",
            "entidad_hint": "invias",
            "departamento": "cundinamarca",
            "fecha_desde": "2024-01-01",
            "estado": "en ejecucion",
        },
        "combinado", "Entidad + departamento + fecha + estado",
    ),
    TestCase(
        "procesos de construccion de acueducto en Bogota abiertos por mas de 1000 millones",
        {
            "departamento": "bogota",
            "estado": "Abierto",
            "valor_min": 1_000_000_000,
            "objeto_contains": ["construccion", "acueducto"],
        },
        "combinado", "Objeto + departamento + estado + monto",
    ),
    TestCase(
        "contratos del SENA en Antioquia en ejecucion desde 2023",
        {
            "dataset": "contratos",
            "entidad_hint": "sena",
            "departamento": "antioquia",
            "estado": "en ejecucion",
            "fecha_desde": "2023-01-01",
        },
        "combinado", "Entidad + depto + estado + fecha",
    ),
    TestCase(
        "licitaciones publicas de transporte masivo en Valle del Cauca por mas de 2000 millones",
        {
            "modalidad": "Licitación pública",
            "departamento": "valle del cauca",
            "valor_min": 2_000_000_000,
            "objeto_contains": ["transporte"],
        },
        "combinado", "Modalidad + depto + monto + objeto",
    ),
    TestCase(
        "contratos de suministro de equipos medicos por mas de 300 millones en 2024",
        {
            "dataset": "contratos",
            "valor_min": 300_000_000,
            "fecha_desde": "2024-01-01",
            "objeto_contains": ["suministro", "equipos"],
        },
        "combinado", "Dataset + monto + fecha + objeto",
    ),
    TestCase(
        "procesos adjudicados de acueducto en Cundinamarca desde 2022",
        {
            "estado": "Seleccionado",
            "departamento": "cundinamarca",
            "fecha_desde": "2022-01-01",
            "objeto_contains": ["acueducto"],
        },
        "combinado", "Estado + departamento + fecha + objeto",
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA 9: Edge cases / Ambiguos (5 casos — v2 debería ganar)
    # ─────────────────────────────────────────────────────────────────────────
    TestCase(
        "licitaciones de la gobernacion de santander que esten abiertas",
        {"entidad_hint": "gobernacion", "estado": "Abierto"},
        "edge_case", "AMBIGUO: 'gobernacion de santander' — v2 scan evita split incorrecto",
    ),
    TestCase(
        "mostrame todo lo del DANE en 2024",
        {"entidad_hint": "dane", "fecha_desde": "2024-01-01"},
        "edge_case", "Verbo conversacional + entidad corta DANE",
    ),
    TestCase(
        "contratos liquidados del ICBF en Atlantico 2022",
        {
            "dataset": "contratos",
            "entidad_hint": "icbf",
            "departamento": "atlantico",
            "fecha_desde": "2022-01-01",
        },
        "edge_case", "Entidad + departamento desambiguados correctamente",
    ),
    TestCase(
        "cuales son los contratos mas caros del INVIAS",
        {"dataset": "contratos", "entidad_hint": "invias"},
        "edge_case", "Ordering signal + entidad (sin objeto contaminado)",
    ),
    TestCase(
        "procesos de la alcaldia de medellin por contratacion directa",
        {"entidad_hint": "alcaldia", "modalidad": "Contratación directa"},
        "edge_case", "Entidad ciudad + modalidad (ciudad no es departamento)",
    ),
]


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def _run_v1(query: str) -> dict:
    """
    Simula el pipeline V1:
      1. QueryRouterV1.parse()
      2. EntityResolverV1.resolve_departamento() si hay departamento
      3. EntityResolverV1.resolve_entidad() si hay entidad
    Replica la lógica de resolve_entities en orchestrator.py
    """
    parsed = _qr_v1.parse(query)
    params = dict(parsed.params)
    resolved = dict(params)

    if params.get("departamento"):
        res = _er_v1.resolve_departamento(str(params["departamento"]))
        resolved["departamento_resolution"] = res.to_dict()
        if res.value:
            resolved["departamento_resolved"] = res.value

    if params.get("entidad"):
        depto = resolved.get("departamento_resolved")
        res = _er_v1.resolve_entidad(str(params["entidad"]), departamento=depto)
        resolved["entidad_resolution"] = res.to_dict()
        if res.value:
            resolved["entidad_resolved"] = res.value
        elif res.like_value:
            resolved["entidad_like"] = res.like_value

    resolved["needs_llm"]    = parsed.needs_llm
    resolved["route_reason"] = parsed.route_reason
    return resolved


def _run_v2(query: str) -> dict:
    """
    Simula el pipeline V2:
      1. QueryRouterV2(er_v2).parse() — gazetteer scan incluido
      2. EntityResolverV2.resolve_* solo si el scan no resolvió (fallback fuzzy)
    Replica la lógica de resolve_entities en orchestrator v2.py
    """
    parsed = _qr_v2.parse(query)
    params = dict(parsed.params)
    resolved = dict(params)

    if params.get("departamento") and not params.get("departamento_resolved"):
        res = _er_v2.resolve_departamento(str(params["departamento"]))
        resolved["departamento_resolution"] = res.to_dict()
        if res.value:
            resolved["departamento_resolved"] = res.value

    if params.get("entidad") and not params.get("entidad_resolved"):
        depto = resolved.get("departamento_resolved")
        res = _er_v2.resolve_entidad(str(params["entidad"]), departamento=depto)
        resolved["entidad_resolution"] = res.to_dict()
        if res.value:
            resolved["entidad_resolved"] = res.value
        elif res.like_value:
            resolved["entidad_like"] = res.like_value

    resolved["needs_llm"]    = parsed.needs_llm
    resolved["route_reason"] = parsed.route_reason
    return resolved


def _run_v3(query: str) -> dict:
    """
    Simula el pipeline V3:
      1. QueryRouterV2(er_v3).parse() — scan con disambiguación de departamentos
      2. EntityResolverV3.resolve_* solo si el scan no resolvió (fallback fuzzy)
    Misma lógica que V2 pero usa _er_v3 / _qr_v3 que rechazan nombres de depto
    como entidades cuando aparecen sin calificador ("gobernacion", "alcaldia"...).
    """
    parsed = _qr_v3.parse(query)
    params = dict(parsed.params)
    resolved = dict(params)

    if params.get("departamento") and not params.get("departamento_resolved"):
        res = _er_v3.resolve_departamento(str(params["departamento"]))
        resolved["departamento_resolution"] = res.to_dict()
        if res.value:
            resolved["departamento_resolved"] = res.value

    if params.get("entidad") and not params.get("entidad_resolved"):
        depto = resolved.get("departamento_resolved")
        res = _er_v3.resolve_entidad(str(params["entidad"]), departamento=depto)
        resolved["entidad_resolution"] = res.to_dict()
        if res.value:
            resolved["entidad_resolved"] = res.value
        elif res.like_value:
            resolved["entidad_like"] = res.like_value

    resolved["needs_llm"]    = parsed.needs_llm
    resolved["route_reason"] = parsed.route_reason
    return resolved


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE SCORING
# ═══════════════════════════════════════════════════════════════════════════════

def _check_departamento(params: dict, expected: str) -> bool:
    """Verifica departamento (alias crudo o nombre oficial resuelto)."""
    exp_norm = normalize_text(expected)
    for key in ("departamento_resolved", "departamento"):
        val = params.get(key)
        if val:
            v = normalize_text(str(val))
            if v == exp_norm or exp_norm in v or v in exp_norm:
                return True
    return False


def _check_entidad_hint(params: dict, hint: str) -> bool:
    """Verifica que la entidad extraída contenga el hint esperado."""
    hint_norm = normalize_text(hint)
    for key in ("entidad_resolved", "entidad", "entidad_like"):
        val = params.get(key)
        if val and hint_norm in normalize_text(str(val)):
            return True
    return False


def _check_valor(params: dict, key: str, expected: int, tol: float = 0.05) -> bool:
    """Verifica monto dentro de ±5% de tolerancia."""
    actual = params.get(key)
    if actual is None:
        return False
    try:
        return abs(int(actual) - expected) <= expected * tol
    except (TypeError, ValueError):
        return False


def _check_objeto(params: dict, expected_words: list[str]) -> bool:
    """Verifica que todas las palabras esperadas aparezcan en objeto."""
    obj = params.get("objeto")
    if not obj:
        return False
    obj_norms = {normalize_text(w) for w in obj}
    return all(normalize_text(w) in obj_norms for w in expected_words)


def _check_field(params: dict, key: str, expected_val: Any) -> bool:
    """Dispatcher: verifica un campo individual contra el valor esperado."""
    if key == "departamento":
        return _check_departamento(params, expected_val)
    if key == "entidad_hint":
        return _check_entidad_hint(params, expected_val)
    if key == "objeto_contains":
        return _check_objeto(params, expected_val)
    if key in ("valor_min", "valor_max"):
        return _check_valor(params, key, expected_val)
    if key == "needs_llm":
        return params.get("needs_llm") == expected_val
    # Comparación normalizada para: dataset, estado, modalidad, fecha_*
    actual = params.get(key)
    if actual is None:
        return False
    return normalize_text(str(actual)) == normalize_text(str(expected_val))


def score_params(params: dict, expected: dict) -> tuple[int, int, list[str]]:
    """Devuelve (correctos, total, lista_campos_fallidos)."""
    correct, total, failures = 0, 0, []
    for key, val in expected.items():
        total += 1
        if _check_field(params, key, val):
            correct += 1
        else:
            failures.append(key)
    return correct, total, failures


# ═══════════════════════════════════════════════════════════════════════════════
# RESULTADOS Y REPORTE
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TestResult:
    case: TestCase
    v1_params: dict
    v2_params: dict
    v3_params: dict
    v1_ok: int
    v1_total: int
    v1_fail: list[str]
    v2_ok: int
    v2_total: int
    v2_fail: list[str]
    v3_ok: int
    v3_total: int
    v3_fail: list[str]
    v1_time_ms: float = 0.0
    v2_time_ms: float = 0.0
    v3_time_ms: float = 0.0

    @property
    def v1_acc(self) -> float:
        return self.v1_ok / self.v1_total if self.v1_total else 0.0

    @property
    def v2_acc(self) -> float:
        return self.v2_ok / self.v2_total if self.v2_total else 0.0

    @property
    def v3_acc(self) -> float:
        return self.v3_ok / self.v3_total if self.v3_total else 0.0

    @property
    def winner(self) -> str:
        best = max(self.v1_acc, self.v2_acc, self.v3_acc)
        winners = [
            v for v, a in (("V1", self.v1_acc), ("V2", self.v2_acc), ("V3", self.v3_acc))
            if a == best
        ]
        return winners[0] if len(winners) == 1 else "TIE"


def run_all_tests(category_filter: str | None = None) -> list[TestResult]:
    cases = TEST_CASES
    if category_filter:
        cases = [c for c in cases if c.category == category_filter]

    results: list[TestResult] = []
    for i, case in enumerate(cases, 1):
        print(f"  [{i:02d}/{len(cases)}] {case.description[:60]:<60}", end="\r", flush=True)

        t0 = time.perf_counter()
        v1_params = _run_v1(case.query)
        v1_ms = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        v2_params = _run_v2(case.query)
        v2_ms = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        v3_params = _run_v3(case.query)
        v3_ms = (time.perf_counter() - t0) * 1000

        v1_ok, v1_tot, v1_f = score_params(v1_params, case.expected)
        v2_ok, v2_tot, v2_f = score_params(v2_params, case.expected)
        v3_ok, v3_tot, v3_f = score_params(v3_params, case.expected)

        results.append(TestResult(
            case=case,
            v1_params=v1_params, v2_params=v2_params, v3_params=v3_params,
            v1_ok=v1_ok, v1_total=v1_tot, v1_fail=v1_f,
            v2_ok=v2_ok, v2_total=v2_tot, v2_fail=v2_f,
            v3_ok=v3_ok, v3_total=v3_tot, v3_fail=v3_f,
            v1_time_ms=v1_ms, v2_time_ms=v2_ms, v3_time_ms=v3_ms,
        ))

    print(" " * 80, end="\r")  # Limpiar línea de progreso
    return results


def _pct(num: int, den: int) -> str:
    if den == 0:
        return " —  "
    p = num / den * 100
    c = GREEN if p >= 80 else (YELLOW if p >= 60 else RED)
    return f"{c}{p:5.1f}%{RESET}"


def _win(w: str) -> str:
    if w == "V2":
        return f"{GREEN} V2 {RESET}"
    if w == "V1":
        return f"{YELLOW} V1 {RESET}"
    return f"{CYAN}TIE{RESET}"


def print_report(results: list[TestResult], verbose: bool = False) -> None:
    # ── Totales globales ──────────────────────────────────────────────────────
    v1_ok_all = sum(r.v1_ok for r in results)
    v2_ok_all = sum(r.v2_ok for r in results)
    v3_ok_all = sum(r.v3_ok for r in results)
    tot_all   = sum(r.v1_total for r in results)

    v1_wins = sum(1 for r in results if r.winner == "V1")
    v2_wins = sum(1 for r in results if r.winner == "V2")
    v3_wins = sum(1 for r in results if r.winner == "V3")
    ties    = sum(1 for r in results if r.winner == "TIE")

    v1_avg_ms = sum(r.v1_time_ms for r in results) / len(results) if results else 0
    v2_avg_ms = sum(r.v2_time_ms for r in results) / len(results) if results else 0
    v3_avg_ms = sum(r.v3_time_ms for r in results) / len(results) if results else 0

    sep = "═" * 84

    print(f"\n{BOLD}{sep}{RESET}")
    print(f"{BOLD}  SECOPPAL — Accuracy Test: V1 vs V2 vs V3{RESET}")
    print(f"{BOLD}{sep}{RESET}")
    print(f"  Queries evaluadas : {len(results)}")
    print(f"  Campos evaluados  : {tot_all} (suma de todos los expected)")
    print()
    print(f"  {'':30s}   {'Stack V1':>10}   {'Stack V2':>10}   {'Stack V3':>10}")
    print(f"  {'Accuracy global':30s}   {_pct(v1_ok_all, tot_all):>10}   {_pct(v2_ok_all, tot_all):>10}   {_pct(v3_ok_all, tot_all):>10}")
    print(f"  {'Campos correctos':30s}   {v1_ok_all:>8}/{tot_all}   {v2_ok_all:>8}/{tot_all}   {v3_ok_all:>8}/{tot_all}")
    print(f"  {'Queries ganadas':30s}   {v1_wins:>10}   {v2_wins:>10}   {v3_wins:>10}   (empates: {ties})")
    print(f"  {'Latencia promedio':30s}   {v1_avg_ms:>8.1f}ms   {v2_avg_ms:>8.1f}ms   {v3_avg_ms:>8.1f}ms")

    # ── Por categoría ─────────────────────────────────────────────────────────
    cats: dict[str, dict] = {}
    for r in results:
        cat = r.case.category
        if cat not in cats:
            cats[cat] = {"v1_ok": 0, "v2_ok": 0, "v3_ok": 0, "total": 0, "n": 0}
        cats[cat]["v1_ok"] += r.v1_ok
        cats[cat]["v2_ok"] += r.v2_ok
        cats[cat]["v3_ok"] += r.v3_ok
        cats[cat]["total"] += r.v1_total
        cats[cat]["n"]     += 1

    print(f"\n{BOLD}  Por categoría:{RESET}")
    print(f"  {'Categoría':16s}  {'N':>4}  {'V1 Acc':>9}  {'V2 Acc':>9}  {'V3 Acc':>9}  {'Ganó':>5}")
    print("  " + "─" * 66)
    for cat, d in sorted(cats.items()):
        v1_a = _pct(d["v1_ok"], d["total"])
        v2_a = _pct(d["v2_ok"], d["total"])
        v3_a = _pct(d["v3_ok"], d["total"])
        best = max(d["v1_ok"], d["v2_ok"], d["v3_ok"])
        if d["v3_ok"] == best and d["v3_ok"] > min(d["v1_ok"], d["v2_ok"]):
            wl = f"{GREEN}V3{RESET}"
        elif d["v1_ok"] == best and d["v1_ok"] > min(d["v2_ok"], d["v3_ok"]):
            wl = f"{YELLOW}V1{RESET}"
        elif d["v2_ok"] == best and d["v2_ok"] > min(d["v1_ok"], d["v3_ok"]):
            wl = f"{CYAN}V2{RESET}"
        else:
            wl = f"{CYAN}=={RESET}"
        print(f"  {cat:16s}  {d['n']:>4}  {v1_a:>9}  {v2_a:>9}  {v3_a:>9}    {wl}")

    # ── Queries donde difieren los tres ──────────────────────────────────────
    diffs = [r for r in results if not (r.v1_ok == r.v2_ok == r.v3_ok)]
    if diffs:
        print(f"\n{BOLD}  Queries donde V1/V2/V3 difieren ({len(diffs)}):{RESET}")
        print(f"  {'#':3}  {'Query (54 chars)':54s}  {'V1':>5}  {'V2':>5}  {'V3':>5}  {'Ganó':>5}")
        print("  " + "─" * 84)
        for i, r in enumerate(diffs, 1):
            q   = r.case.query[:52]
            v1s = f"{r.v1_ok}/{r.v1_total}"
            v2s = f"{r.v2_ok}/{r.v2_total}"
            v3s = f"{r.v3_ok}/{r.v3_total}"
            print(f"  {i:3}.  {q:<54}  {v1s:>5}  {v2s:>5}  {v3s:>5}   {_win(r.winner)}")

    # ── Verbose: detalle por query ────────────────────────────────────────────
    if verbose:
        print(f"\n{BOLD}  Detalle completo por query:{RESET}")

        def _fmt_ok(ok: int, total: int) -> str:
            s = f"{ok}/{total}"
            return f"{GREEN}{s}{RESET}" if ok == total else f"{RED}{s}{RESET}"

        def _get(params: dict, k: str) -> str:
            if k == "departamento":
                return str(params.get("departamento_resolved") or params.get("departamento") or "—")
            if k == "entidad_hint":
                return str(params.get("entidad_resolved") or params.get("entidad") or params.get("entidad_like") or "—")
            if k == "objeto_contains":
                return str(params.get("objeto") or "—")
            return str(params.get(k, "—"))

        for idx, r in enumerate(results, 1):
            print(f"\n  {BOLD}[{idx:02d}] [{r.case.category}] {r.case.description}{RESET}")
            print(f"       Query : {r.case.query}")
            print(f"       V1    : {_fmt_ok(r.v1_ok, r.v1_total)}  fallas={r.v1_fail or '—'}")
            print(f"       V2    : {_fmt_ok(r.v2_ok, r.v2_total)}  fallas={r.v2_fail or '—'}")
            print(f"       V3    : {_fmt_ok(r.v3_ok, r.v3_total)}  fallas={r.v3_fail or '—'}")

            all_fail = set(r.v1_fail) | set(r.v2_fail) | set(r.v3_fail)
            for fk in sorted(all_fail):
                exp = r.case.expected.get(fk, "?")
                v1g = _get(r.v1_params, fk)
                v2g = _get(r.v2_params, fk)
                v3g = _get(r.v3_params, fk)
                ok1 = "✓" if fk not in r.v1_fail else "✗"
                ok2 = "✓" if fk not in r.v2_fail else "✗"
                ok3 = "✓" if fk not in r.v3_fail else "✗"
                print(f"         {fk:20s}: esperado={exp!r}")
                print(f"           V1 {ok1} → {v1g!r}")
                print(f"           V2 {ok2} → {v2g!r}")
                print(f"           V3 {ok3} → {v3g!r}")

    # ── Resumen final ─────────────────────────────────────────────────────────
    print(f"\n{BOLD}{sep}{RESET}")
    best_ok  = max(v1_ok_all, v2_ok_all, v3_ok_all)
    best_ver = "V3" if v3_ok_all == best_ok else ("V1" if v1_ok_all == best_ok else "V2")
    worst_ok = min(v1_ok_all, v2_ok_all, v3_ok_all)
    delta = (best_ok - worst_ok) / tot_all * 100 if tot_all else 0
    color = GREEN if best_ver == "V3" else (YELLOW if best_ver == "V1" else CYAN)
    print(f"  {color}{BOLD}Stack {best_ver} gana con {best_ok}/{tot_all} campos ({best_ok/tot_all*100:.1f}%) — ventaja {delta:.1f}pp sobre el peor{RESET}")
    print(f"{BOLD}{sep}{RESET}\n")


def export_json(results: list[TestResult], path: Path) -> None:
    """Exporta resultados a JSON para análisis posterior."""
    _skip = {"departamento_resolution", "entidad_resolution"}
    output = []
    for r in results:
        output.append({
            "query":        r.case.query,
            "category":     r.case.category,
            "description":  r.case.description,
            "expected":     r.case.expected,
            "v1": {
                "score":    f"{r.v1_ok}/{r.v1_total}",
                "accuracy": round(r.v1_acc * 100, 1),
                "failures": r.v1_fail,
                "time_ms":  round(r.v1_time_ms, 2),
                "params":   {k: v for k, v in r.v1_params.items() if k not in _skip},
            },
            "v2": {
                "score":    f"{r.v2_ok}/{r.v2_total}",
                "accuracy": round(r.v2_acc * 100, 1),
                "failures": r.v2_fail,
                "time_ms":  round(r.v2_time_ms, 2),
                "params":   {k: v for k, v in r.v2_params.items() if k not in _skip},
            },
            "v3": {
                "score":    f"{r.v3_ok}/{r.v3_total}",
                "accuracy": round(r.v3_acc * 100, 1),
                "failures": r.v3_fail,
                "time_ms":  round(r.v3_time_ms, 2),
                "params":   {k: v for k, v in r.v3_params.items() if k not in _skip},
            },
            "winner": r.winner,
        })

    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"Resultados exportados a: {path}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compara accuracy de Stack V1 vs Stack V2 en secoppal"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Mostrar detalle completo por query (campos fallidos y valores extraídos)",
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Ruta de archivo JSON para exportar resultados (ej: results.json)",
    )
    parser.add_argument(
        "--category", "-c", type=str, default=None,
        choices=["dataset", "departamento", "entidad", "estado", "modalidad",
                 "montos", "fechas", "combinado", "edge_case"],
        help="Filtrar por categoría (default: todas)",
    )
    args = parser.parse_args()

    print(f"{BOLD}Ejecutando {len(TEST_CASES)} casos de prueba...{RESET}\n")
    results = run_all_tests(category_filter=args.category)
    print_report(results, verbose=args.verbose)

    if args.output:
        export_json(results, Path(args.output))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Experimento A/B: extracción de params HEURÍSTICO vs LLM-first.

Pregunta: ¿extraer params con un LLM bien contextualizado sube precision@10 E2E
frente al parser heurístico, sobre queries reales? Decide LLM-first o no.

Diseño (solo cambia el EXTRACTOR; todo lo demás idéntico):

    query ──┬─ A: QueryRouter heurístico ──┐
            └─ B: LLM + contexto robusto ──┘
                                            ▼
            ambos → compute_query_plan (validators/invariantes)
                  → SoQLBuilder → SecopClient → RelevanceScorer (BM25)
                  → precision@10  (+ 0-results, latencia, dump de params)

El output del LLM PASA por los mismos validators (contract_invariants vía
compute_query_plan) que el heurístico — el LLM propone, los validators gobiernan.

NO toca el pipeline de producción. Mide, no implementa.

Uso:
    .venv/bin/python scripts/exp_llm_extraction.py --max 60
    .venv/bin/python scripts/exp_llm_extraction.py --smoke      # 3 queries, valida plumbing
"""
from __future__ import annotations

import argparse
import copy
import json
import sqlite3
import sys
import time
import unicodedata
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings
from app.core.llm_handler import SECOPPAL_TOOLS
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.relevance import RelevanceScorer
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

REPO = Path(__file__).resolve().parents[1]
SECOPPAL_DB = REPO / "data" / "secoppal.db"

# ⚠️ AJUSTAR a la tarifa REAL de tu plan DeepSeek (USD por 1M tokens).
# DeepSeek cobra distinto el input cache-HIT vs cache-MISS. Los tokens medidos
# son reales; el costo en USD solo es correcto si estas tarifas lo son.
PRICING = {
    "deepseek-v4-pro":   {"in_hit": 0.07, "in_miss": 0.55, "out": 2.19},   # placeholder
    "deepseek-v4-flash": {"in_hit": 0.02, "in_miss": 0.14, "out": 0.28},   # placeholder
    "deepseek-chat":     {"in_hit": 0.07, "in_miss": 0.27, "out": 1.10},   # DeepSeek V3 — VERIFICAR
}


def _cost_usd(model: str, prompt: int, cache_hit: int, completion: int) -> float:
    """Costo de UNA query en USD según PRICING. cache_hit son tokens de input
    que pegaron en caché (más baratos); el resto del prompt es cache-miss."""
    rates = PRICING.get(model, {"in_hit": 0.0, "in_miss": 0.0, "out": 0.0})
    miss = max(prompt - cache_hit, 0)
    return (cache_hit * rates["in_hit"] + miss * rates["in_miss"]
            + completion * rates["out"]) / 1_000_000

# ── SCHEMA DEL EXPERIMENTO (aislado de producción) ────────────────────────────
# Copia de SECOPPAL_TOOLS + dos parámetros que el schema de producción NO declara
# pero que el prompt sí exige: intent_type (la regla de oportunidad) y descripción
# ISO en fechas. Con tool-calling el SCHEMA es la mitad fuerte del prompt: pedir en
# prosa un campo ausente del schema produce el comportamiento NO determinista que el
# smoke ya mostró (intent ✓ a veces, ✗ otras, misma query). NO toca llm_handler.py.
EXP_TOOLS = copy.deepcopy(SECOPPAL_TOOLS)
_exp_props = EXP_TOOLS[0]["function"]["parameters"]["properties"]
_exp_props["intent_type"] = {
    "type": "string",
    "enum": ["opportunity_search"],
    "description": "Marcar SIEMPRE que el usuario busque oportunidades para ofertar o presentarse.",
}
_exp_props["fecha_desde"] = {"type": "string", "description": "Fecha inicial en ISO YYYY-MM-DD."}
_exp_props["fecha_hasta"] = {"type": "string", "description": "Fecha final en ISO YYYY-MM-DD."}

# ── INGENIERÍA DE CONTEXTO: el system prompt robustecido ──────────────────────
# Ensamblado del conocimiento de dominio ya consolidado (invariantes + vocabulario).
# Va en SYSTEM (estable, cacheable). El user message es solo la query.
# __TODAY__ se sustituye en runtime (build_system_prompt) — cambia 1 vez/día, el
# caché de DeepSeek lo tolera (cache-miss solo en el 1er query de cada día).
ROBUST_SYSTEM_PROMPT = """
Eres un extractor de parámetros de búsqueda para SECOP II (contratación pública
colombiana). Tu única tarea es llamar la herramienta buscar_procesos con los
parámetros correctos. Piensa en el SIGNIFICADO, no en las palabras literales.

## Esquema de scope
- "en Atlántico", "del Atlántico" → departamento
- "en Barranquilla", "en Soacha" → ciudad (municipio)
- "de la gobernación del Atlántico", "del SENA", "de la alcaldía de X" → entidad
- objeto = QUÉ se contrata (el concepto), NO ciudades/departamentos/entidades.

## INVARIANTES JURÍDICAS (nunca las violes)
- "firmados"/"suscritos"/"celebrados" → dataset=contratos, SIN estado.
  (todo contrato está firmado; "firmado" elige dataset, no estado).
- "en ejecución" → dataset=contratos, estado_contrato IN (En ejecución, Modificado, Prorrogado).
- "cerrados"/"terminados" → dataset=contratos + ese estado.
- Nunca emitas "Firmado" ni "Celebrado" como estado_contrato.
- Default: dataset=procesos.

## REGLA DE INTENT (CRÍTICA — NO la omitas)
SIEMPRE incluye intent_type="opportunity_search" cuando la query tenga CUALQUIER
señal de que el usuario busca oportunidades para presentarse/ofertar:
  "alguna oportunidad", "hay ... actualmente", "qué hay", "abiertas", "vigentes",
  "para presentarme", "donde pueda ofertar", "soy/vendo/ofrezco/proveedor de X",
  "convocatoria", "licitación abierta".
Ante la duda, una pregunta de un proponente es opportunity_search. Es un ERROR
GRAVE omitir intent_type en estas queries: sin él se mezclan procesos cerrados.

## VOCABULARIO DE DOMINIO (extrae el CONCEPTO canónico, no el verbo/forma literal)
El texto del contrato usa SUSTANTIVOS, no verbos. Normaliza:
- construir / constructor / construyendo / "soy constructor de X" → objeto: ["construccion", "X"]
- remodelar → remodelacion · pavimentar → pavimentacion · adecuar → adecuacion
- mantener → mantenimiento · suministrar/proveer → suministro · dotar → dotacion
- "soy/vendo/ofrezco/proveedor de X" → objeto = X (el bien/servicio), nunca el verbo de venta.
Sinónimos de tema (usa el término más común en SECOP):
- adulto mayor / ancianos / tercera edad / "centros día" / "centros de vida" → ["adulto mayor"]
- basuras / residuos → ["aseo"]  · alimentación escolar / PAE → ["alimentacion escolar"]

## Valores monetarios y polaridad
- "500 millones"=500000000 · "mil millones"=1000000000 · "200 palos"=200000000
- "un billón"=1000000000000 (10^12, semántica colombiana: millón de millones).
- "mayor a / más de / al menos X" → valor_min · "menor a / hasta / máximo X" → valor_max
- "que NO supere / NO sea mayor a X" → valor_max (negación invierte)
- "que NO baje de / NO sea menor a X" → valor_min

## Fechas (hoy es __TODAY__)
Emite fecha_desde/fecha_hasta SOLO si la query las menciona, en ISO YYYY-MM-DD,
calculadas respecto a hoy:
- "en mayo" (año actual) → fecha_desde=AAAA-05-01, fecha_hasta=AAAA-05-31.
- "este año" → AAAA-01-01 a hoy.  "el año pasado" → (AAAA-1)-01-01 a (AAAA-1)-12-31.
- "esta semana" → lunes a domingo de la semana de hoy.
- "últimos N días" → (hoy − N días) a hoy.

## Modalidad vs intent (no confundir)
"licitación pública", "selección abreviada", "mínima cuantía", "concurso de méritos"
son MODALIDAD (cómo se contrata). "licitación abierta"/"convocatoria abierta" como
señal de DISPONIBILIDAD es intent_type=opportunity_search. Una query puede tener ambas.

## Ejemplos
Q: "alguna oportunidad para construir parques"
→ {"dataset":"procesos","intent_type":"opportunity_search","objeto":["construccion","parques"]}
Q: "soy constructor de parques, alguna oportunidad"
→ {"dataset":"procesos","intent_type":"opportunity_search","objeto":["construccion","parques"]}
Q: "que oportunidad hay en adulto mayor actualmente"
→ {"dataset":"procesos","intent_type":"opportunity_search","objeto":["adulto mayor"]}
Q: "contratos firmados que estén en ejecución de mantenimiento en Bogotá"
→ {"dataset":"contratos","objeto":["mantenimiento"],"ciudad":"Bogotá","estado_contrato":["En ejecución","Modificado","Prorrogado"]}
Q: "transporte escolar en huila no mayores a 30 millones"
→ {"dataset":"procesos","objeto":["transporte","escolar"],"departamento":"Huila","valor_max":30000000}
""".strip()


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


# ── Extractores ───────────────────────────────────────────────────────────────
def extract_heuristic(router: QueryRouter, query: str) -> dict:
    return dict(router.parse(query).params)


def build_system_prompt(today: date) -> str:
    """Sustituye __TODAY__ por la fecha real. Se llama una vez por corrida
    (la fecha es constante dentro de la corrida → el caché del system se mantiene)."""
    return ROBUST_SYSTEM_PROMPT.replace("__TODAY__", today.isoformat())


_FORCED_TC = {"type": "function", "function": {"name": "buscar_procesos"}}
# Modo de tool_choice aprendido por modelo. Los modelos "thinking" de DeepSeek
# (v4-pro/v4-flash) RECHAZAN el tool_choice forzado ("Thinking mode does not
# support this tool_choice") y solo aceptan "auto"; los no-thinking (deepseek-chat)
# sí soportan forzarlo. Aprendemos por modelo en runtime, sin hardcodear nombres.
_TC_MODE: dict[str, object] = {}


def extract_llm_first(client, model: str, query: str, system_prompt: str) -> tuple[dict, float, dict]:
    """LLM extrae params DESDE CERO (no híbrido).
    Devuelve (params, latencia_s, usage) con tokens reales de la respuesta.
    Forzamos tool_choice cuando el modelo lo permite (el prompt dice "tu única tarea
    es llamar la herramienta"); si el modelo es 'thinking' y lo rechaza, caemos a
    'auto' y la red de seguridad no_tool_call captura cualquier respuesta sin tool."""
    def _call(tool_choice):
        return client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query},
            ],
            tools=EXP_TOOLS,
            tool_choice=tool_choice,
        )

    tc = _TC_MODE.get(model, _FORCED_TC)
    try:
        t0 = time.time(); resp = _call(tc); latency = time.time() - t0
    except Exception as exc:
        if tc != "auto" and "tool_choice" in str(exc).lower():
            _TC_MODE[model] = "auto"  # thinking model → recordar y reintentar
            t0 = time.time(); resp = _call("auto"); latency = time.time() - t0
        else:
            raise
    u = resp.usage
    usage = {
        "prompt": getattr(u, "prompt_tokens", 0) or 0,
        "completion": getattr(u, "completion_tokens", 0) or 0,
        "total": getattr(u, "total_tokens", 0) or 0,
        # campos específicos de DeepSeek (caché de prompt) — pueden no venir
        "cache_hit": getattr(u, "prompt_cache_hit_tokens", 0) or 0,
        "cache_miss": getattr(u, "prompt_cache_miss_tokens", 0) or 0,
        "no_tool_call": True,  # red de seguridad: se baja a False si llega la tool call
    }
    params: dict = {}
    msg = resp.choices[0].message
    for tc in (msg.tool_calls or []):
        if tc.function.name == "buscar_procesos":
            try:
                params = json.loads(tc.function.arguments)
                usage["no_tool_call"] = False
            except json.JSONDecodeError:
                pass
            break
    return params, latency, usage


# ── Resolución de scope común (igual para A y B, para no sesgar) ──────────────
def normalize_scope(params: dict) -> dict:
    """Passthrough mínimo: departamento → departamento_resolved.
    Igual para ambas ramas. Entity-resolution compleja queda fuera de alcance v1."""
    p = dict(params)
    if p.get("departamento") and not p.get("departamento_resolved"):
        p["departamento_resolved"] = p["departamento"]
    return p


def validate(params: dict, query: str) -> dict:
    """Pasa params por los validators/invariantes (compute_query_plan)."""
    plan = compute_query_plan(params, query, {"today": date.today()})
    return plan.params


# ── Métrica NEUTRAL (no usa el objeto de cada rama; usa la query) ─────────────
_STOP = {"de","la","el","en","los","las","para","que","hay","una","alguna","algun",
         "del","con","por","un","es","soy","y","o","actualmente","oportunidad",
         "procesos","contratos","proceso","contrato","muestrame","quiero"}


def concept_roots(query: str) -> list[str]:
    """Raíces de contenido de la query (≥5 chars, no stopword) → prefijo de 6."""
    roots = []
    for tok in _fold(query).replace("?", " ").replace(",", " ").split():
        if len(tok) >= 5 and tok not in _STOP:
            roots.append(tok[:6])
    return roots


def is_relevant(row: dict, field: str, roots: list[str]) -> bool:
    if not roots:
        return False
    title = _fold(str(row.get(field, "")))
    # relevante si el título contiene AL MENOS la mitad de las raíces core
    hits = sum(1 for r in roots if r in title)
    return hits >= max(1, len(roots) // 2)


def precision_at_k(rows: list[dict], field: str, roots: list[str], k: int) -> float:
    top = rows[:k]
    if not top:
        return 0.0
    return sum(1 for r in top if is_relevant(r, field, roots)) / len(top)


_OPP_SIGNALS = ("oportunidad", "presentarme", "presentar", "ofertar", "ofrezco",
                "vendo", "proveedor", "actualmente", "abierta", "abiertas",
                "vigente", "vigentes", "convocatoria", "licitacion", "para licitar")

# ── Juez LLM ciego de relevancia (compartido entre ramas A/B) ──────────────
_JUDGE_CACHE: dict[tuple[str, str], bool] = {}  # key=(query, title) → relevant?


def is_opportunity_query(query: str) -> bool:
    q = _fold(query)
    return any(sig in q for sig in _OPP_SIGNALS)


def intent_preserved(params: dict, query: str) -> bool | None:
    """True/False si la query es de oportunidad y el extractor marcó (o no)
    opportunity_search. None si la query no es de oportunidad (no aplica)."""
    if not is_opportunity_query(query):
        return None
    return params.get("intent_type") == "opportunity_search"


# ── Juez LLM de relevancia (ciego a la rama, no ve objeto extraído) ─────────
def judge_relevance(client, query: str, field: str, titles: list[str]) -> tuple[list[bool | None], dict]:
    """LLM judge: qué títulos son semánticamente relevantes a la intención de la query.

    Ciego a la rama (A/B) y al objeto extraído — solo ve (query, lista de títulos).
    Cache global _JUDGE_CACHE garantiza que el mismo título recibe el mismo veredicto
    en ambas ramas y ahorra ~40% de llamadas.

    Args:
        client: OpenAI client apuntando a DeepSeek API
        query: consulta original del usuario
        field: nombre del campo de título (solo para trazabilidad, no se usa en el prompt)
        titles: títulos de resultados a juzgar

    Returns:
        (veredictos, usage) — veredictos[i] ∈ {True, False, None}.
        None = juez falló para ESE título (query entera se considera inválida).
        usage = {prompt, completion, cache_hit} o {} si fue todo cache-hit.
    """
    if not titles:
        return [], {}

    # Separar cacheados de no cacheados
    result: list[bool | None] = []
    uncached_indices: list[int] = []
    uncached_titles: list[str] = []

    for i, title in enumerate(titles):
        key = (query, title)
        cached = _JUDGE_CACHE.get(key)
        if cached is not None:
            result.append(cached)
        else:
            result.append(None)  # placeholder, se llena tras el fallo del LLM
            uncached_indices.append(i)
            uncached_titles.append(title)

    if not uncached_titles:
        return result, {}  # todo en caché, cero costo

    # Construir prompt con lista numerada SOLO de títulos no cacheados
    numbered = "\n".join(f"{j}. {t}" for j, t in enumerate(uncached_titles))
    prompt = f"""Eres un juez estricto de relevancia para resultados de búsqueda de contratación pública colombiana (SECOP II).

Determina cuáles de estos títulos son semánticamente relevantes a la intención COMPLETA de esta consulta:

CONSULTA: {query}

TÍTULOS:
{numbered}

RELEVANCIA significa que el título describe un proceso o contrato que RESPONDE a lo que el usuario busca — no basta con que comparta una palabra o entidad geográfica. Evalúa la intención completa:
- Si la consulta pide un tipo de bien/servicio/obra, el título debe tratar de ESE tipo.
- Si la consulta menciona un lugar (Colombia, Bogotá, Atlántico), ignóralo: casi todos los procesos son en Colombia. El lugar NO hace a un título relevante por sí solo.
- Si la consulta menciona un umbral monetario ("mayores a X millones"), un título de una entidad cualquiera NO es relevante solo por estar en Colombia.
- Si la consulta usa verbos (limpiar, construir), el título relevante puede usar sustantivos equivalentes (limpieza, construcción, aseo).

Ante la duda entre relevante y no relevante, marca como NO RELEVANTE. Prefiere falsos negativos sobre falsos positivos.

Devuelve ÚNICAMENTE un JSON con este formato:
{{"relevant_indices": [0, 3, 5]}}"""

    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content or ""

        # Extraer JSON (puede venir con fences de markdown)
        if "```" in raw:
            block = raw.split("```")[1]
            if block.startswith("json"):
                block = block[4:]
            raw = block.strip()
        data = json.loads(raw)
        relevant_set = set(data.get("relevant_indices", []))

        u = resp.usage
        usage = {
            "prompt": getattr(u, "prompt_tokens", 0) or 0,
            "completion": getattr(u, "completion_tokens", 0) or 0,
            "cache_hit": getattr(u, "prompt_cache_hit_tokens", 0) or 0,
        }
    except Exception:
        # Fallo de red/parse: títulos NO cacheados quedan como None
        # (run_branch detecta None → invalida la query entera para p_judge)
        return result, {}

    # Volcar veredictos al caché global y al resultado
    for j, title in enumerate(uncached_titles):
        verdict = j in relevant_set
        _JUDGE_CACHE[(query, title)] = verdict
        result[uncached_indices[j]] = verdict

    return result, usage


# ── Ejecución de una rama ─────────────────────────────────────────────────────
def run_branch(params: dict, query: str, client_secop, builder, scorer,
               fetch: int, k: int, judge_client=None) -> dict:
    params = validate(normalize_scope(params), query)
    ds_id = builder.dataset_id_for(params.get("dataset"))
    field = "objeto_del_contrato" if ds_id == builder.CONTRATOS_DATASET else "nombre_del_procedimiento"
    ipres = intent_preserved(params, query)
    try:
        soql = builder.build(ds_id, {**params, "limit": fetch})
    except Exception as exc:
        return {"error": f"build: {exc!s:.40}", "params": params, "n": 0, "p": 0.0, "intent_ok": ipres}
    try:
        rows = client_secop.query(ds_id, soql)
    except Exception as exc:
        return {"error": f"fetch: {exc!s:.40}", "params": params, "n": 0, "p": 0.0, "intent_ok": ipres}
    topic = []
    for t in params.get("objeto", []) or []:
        topic.extend(t if isinstance(t, list) else [t])
    if topic and rows:
        rows = [r for r, _ in scorer.score_rows([str(x) for x in topic], rows, field=field)]
    roots = concept_roots(query)
    p_proxy = precision_at_k(rows[:50], field, roots, k)

    # ── Juez LLM ciego ──
    p_judge = None
    judge_usage: dict = {}
    judge_titles: list[str] = []
    judge_verdicts: list[bool | None] = []
    if judge_client is not None and rows:
        top_rows = rows[:k]
        titles = [str(r.get(field, "")) for r in top_rows]
        try:
            verdicts, judge_usage = judge_relevance(judge_client, query, field, titles)
            if verdicts and None not in verdicts:
                p_judge = sum(1 for v in verdicts if v) / len(verdicts)
            judge_titles = titles
            judge_verdicts = verdicts
        except Exception:
            pass

    return {
        "error": "", "params": params, "n": len(rows),
        "p": p_proxy,
        "p_judge": p_judge,
        "judge_usage": judge_usage,
        "judge_titles": judge_titles,
        "judge_verdicts": judge_verdicts,
        "intent_ok": intent_preserved(params, query),
    }


def _load_queries() -> list[str]:
    conn = sqlite3.connect(SECOPPAL_DB)
    rows = conn.execute(
        "SELECT DISTINCT user_query FROM traces "
        "WHERE user_query IS NOT NULL AND length(user_query) > 8"
    ).fetchall()
    conn.close()
    return [r[0].strip() for r in rows if r[0]]


SMOKE = [
    "alguna oportunidad para construir parques",
    "soy constructor de parques, alguna oportunidad",
    "que oportunidad hay en adulto mayor actualmente",
]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=60)
    ap.add_argument("--fetch", type=int, default=300)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--models", default="deepseek-v4-pro,deepseek-v4-flash",
                    help="lista separada por comas de modelos a comparar (todos DeepSeek, cache auto)")
    ap.add_argument("--smoke", action="store_true", help="3 queries, valida plumbing")
    args = ap.parse_args(argv)
    models = [m.strip() for m in args.models.split(",") if m.strip()]

    s = get_settings()
    if not s.llm_enabled or OpenAI is None:
        print("ERROR: DEEPSEEK_API_KEY no configurada o openai no instalado.", file=sys.stderr)
        return 2
    client_llm = OpenAI(api_key=s.deepseek_api_key, base_url="https://api.deepseek.com/v1", timeout=20)
    client_judge = OpenAI(api_key=s.deepseek_api_key, base_url="https://api.deepseek.com/v1", timeout=20)
    client_secop = SecopClient(domain=s.datos_gov_domain, app_token=s.secop_app_token,
                               timeout=30, max_retries=2)
    router = QueryRouter()
    builder = SoQLBuilder()
    scorer = RelevanceScorer()
    system_prompt = build_system_prompt(date.today())

    if args.smoke:
        queries = SMOKE
    else:
        # solo queries de tópico (objeto no vacío en heurístico) — donde p@k aplica
        cands = _load_queries()
        queries = []
        for q in cands:
            if extract_heuristic(router, q).get("objeto"):
                queries.append(q)
            if len(queries) >= args.max:
                break

    print(f"Queries: {len(queries)}  |  fetch={args.fetch}  k={args.k}\n")
    def _short(m): return m.replace("deepseek-", "")
    print(f"modelos: A(heur) vs {' vs '.join(_short(m) for m in models)}  |  temperature=0\n")

    # acumuladores: A (heur) + uno por modelo
    a_scores: list[float] = []; a_zero = 0; a_judge_scores: list[float | None] = []
    agg = {m: {"scores": [], "judge_scores": [], "iok": 0, "itot": 0, "zero": 0, "notool": 0,
               "prompt": 0, "compl": 0, "chit": 0, "cost": 0.0, "lat": []} for m in models}
    judge_cost_total = 0.0

    for q in queries:
        a = run_branch(extract_heuristic(router, q), q, client_secop, builder, scorer,
                       args.fetch, args.k, judge_client=client_judge)
        a_scores.append(a["p"]); a_zero += a["n"] == 0
        a_judge_scores.append(a.get("p_judge"))
        if a.get("judge_usage"):
            judge_cost_total += _cost_usd("deepseek-chat",
                                          a["judge_usage"]["prompt"],
                                          a["judge_usage"].get("cache_hit", 0),
                                          a["judge_usage"]["completion"])
        pa_str = f"{a['p']:.2f}"
        pj_str = f"{a.get('p_judge'):.2f}" if a.get('p_judge') is not None else "—"
        cells = [f"A={pa_str}|J={pj_str}"]
        objetos = {"A": a["params"].get("objeto")}
        for m in models:
            try:
                params, lat, usage = extract_llm_first(client_llm, m, q, system_prompt)
            except Exception as exc:
                cells.append(f"{_short(m)}=ERR")
                continue
            b = run_branch(params, q, client_secop, builder, scorer, args.fetch, args.k,
                           judge_client=client_judge)
            ag = agg[m]
            ag["scores"].append(b["p"]); ag["zero"] += b["n"] == 0
            ag["judge_scores"].append(b.get("p_judge"))
            if b.get("judge_usage"):
                judge_cost_total += _cost_usd("deepseek-chat",
                                              b["judge_usage"]["prompt"],
                                              b["judge_usage"].get("cache_hit", 0),
                                              b["judge_usage"]["completion"])
            ag["notool"] += int(usage.get("no_tool_call", False))
            ag["lat"].append(lat)
            ag["prompt"] += usage["prompt"]; ag["compl"] += usage["completion"]; ag["chit"] += usage["cache_hit"]
            ag["cost"] += _cost_usd(m, usage["prompt"], usage["cache_hit"], usage["completion"])
            if b["intent_ok"] is not None:
                ag["itot"] += 1; ag["iok"] += int(b["intent_ok"])
            im = "-" if b["intent_ok"] is None else ("✓" if b["intent_ok"] else "✗")
            pb_str = f"{b['p']:.2f}"
            pj_str = f"{b.get('p_judge'):.2f}" if b.get('p_judge') is not None else "—"
            cells.append(f"{_short(m)}={pb_str}{im}|J={pj_str}")
            objetos[_short(m)] = params.get("objeto")
        print(f"{q[:40]:<42} " + "  ".join(cells))
        # ── Smoke: mostrar veredictos del juez ──
        if args.smoke and a.get("judge_titles"):
            print(f"    JUEZ — A (top-{args.k}):")
            for i, (t, v) in enumerate(zip(a["judge_titles"], a["judge_verdicts"])):
                mark = "✓" if v else ("✗" if v is False else "?")
                print(f"      [{mark}] {t[:90]}")
        # dump objeto solo si las ramas difieren (señal de dónde el modelo cambia algo)
        if len({str(v) for v in objetos.values()}) > 1:
            for name, obj in objetos.items():
                print(f"    {name}: {obj}")
        time.sleep(0.15)

    print("─" * 100)
    n = len(a_scores)
    if not n:
        return 0
    ma = sum(a_scores) / n
    # p@10_judge promedio para A (solo queries donde el juez no falló)
    a_judge_valid = [s for s in a_judge_scores if s is not None]
    ma_judge = sum(a_judge_valid) / len(a_judge_valid) if a_judge_valid else None
    print(f"\n{'MODELO':<22} {'p@10_proxy':>10} {'vs A':>7} {'p@10_judge':>10} {'vs A_j':>7} {'intent':>8} {'0-res':>7} {'lat':>6} {'$/q':>9} {'$/mes@5k':>10}")
    ma_j_str = f"{ma_judge:>10.2f}" if ma_judge is not None else "         —"
    print(f"{'A (heurístico)':<22} {ma:>10.2f} {'—':>7} {ma_j_str} {'—':>7} {'—':>8} {str(a_zero)+'/'+str(n):>7} {'0s':>6} {'$0':>9} {'$0':>10}")
    for m in models:
        ag = agg[m]
        k2 = len(ag["scores"])
        if not k2:
            continue
        mb = sum(ag["scores"]) / k2
        lat = sum(ag["lat"]) / len(ag["lat"]) if ag["lat"] else 0
        cq = ag["cost"] / k2
        intent = f"{ag['iok']}/{ag['itot']}" if ag["itot"] else "—"
        # p@10_judge para este modelo
        mb_judge_valid = [s for s in ag["judge_scores"] if s is not None]
        mb_judge = sum(mb_judge_valid) / len(mb_judge_valid) if mb_judge_valid else None
        mb_j_str = f"{mb_judge:>10.2f}" if mb_judge is not None else "         —"
        vs_aj = f"{mb_judge - ma_judge:>+7.2f}" if (mb_judge is not None and ma_judge is not None) else "      —"
        print(f"{_short(m):<22} {mb:>10.2f} {mb-ma:>+7.2f} {mb_j_str} {vs_aj} {intent:>8} {str(ag['zero'])+'/'+str(k2):>7} "
              f"{lat:>5.1f}s {'$'+format(cq,'.5f'):>9} {'$'+format(cq*5000*30,'.0f'):>10}")
    cr = max((agg[m]["chit"] / agg[m]["prompt"] * 100) if agg[m]["prompt"] else 0 for m in models)
    print(f"\ncache-hit ~{cr:.0f}% del prompt (DeepSeek cachea el system automáticamente)")
    print(f"juez (deepseek-chat): ${judge_cost_total:.4f} total  |  ${judge_cost_total/n:.4f}/query" if n else "juez: $0")
    notool_total = sum(agg[m]["notool"] for m in models)
    if notool_total:
        print(f"⚠️  {notool_total} respuesta(s) SIN tool-call (params vacíos) pese a tool_choice forzado — revisar.")
    print("intent: ✓=marcó opportunity cuando tocaba, ✗=lo omitió, -=no aplica  |  tarifa PRICING — AJUSTAR")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

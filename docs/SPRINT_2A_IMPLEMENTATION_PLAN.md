# SECOPPAL — Plan de implementación Sprint 2A (para Codex)

**Fecha:** 2026-05-21
**Audiencia:** ejecutor (Codex) + revisor (humano + Claude)
**Alcance:** capa mínima `QueryPlan` + `DialoguePolicy` centralizando decisiones pre-ejecución. Los 5 bugs HIGH del backlog entran como **pruebas de aceptación de la capa nueva**, no como parches en `query_router.py`.
**Estimación total:** 17-20 horas de implementación + tests.
**Pre-requisitos:** Sprint 1A cerrado (PR mergeado a main), corpus v1 88/88 PASS, suite 612 passed / 3 xfailed.

---

## Cómo leer este documento

Cada bloque (B0..B6) es **atómico, verificable y reversible**:

- **Pre-requisitos:** qué debe estar listo antes de arrancar.
- **Archivos a tocar:** rutas exactas. Cualquier otro archivo es **off-limits**.
- **Especificación funcional:** qué debe hacer el código nuevo.
- **Tests:** rutas y contenido mínimo de tests.
- **Reglas locales:** restricciones específicas del bloque.
- **Criterios de aceptación:** condiciones medibles para cerrar el bloque.
- **Verificación pre/post:** comandos exactos para confirmar no-regresión.

**Orden de ejecución no negociable:** B0 → B1 → B2 → B3 → B4 → B5 → B6. Cada bloque debe quedar en verde antes de empezar el siguiente.

---

## Por qué este sprint es distinto a Sprint 1A

Sprint 1A fue **defensa táctica**: 9 fixes CRITICAL puntuales que destrababan el modelo de negocio. Cada fix vivió donde dolía (parser, SoQL builder, entity resolver, formatter).

Sprint 2A es el **primer paso estructural**: centralizar la decisión "¿qué hacer con esta query antes de ejecutarla?" en una capa nombrada (`QueryPlan`), alimentada por un clasificador (`DialoguePolicy`). Los 5 bugs HIGH que entran como casos de validación se resuelven **en la capa nueva**, no en parches dispersos del parser.

Esta decisión es deliberada y se basa en el dato de feedback humano: 80 queries naturales recientes muestran ~20% éxito. Seguir parchando `query_router.py` con condicionales gana batallas y pierde la guerra. La capa centralizada es el primer paso para revertir esa tendencia.

---

## Reglas globales (aplican a TODOS los bloques)

### Regla G1 — Línea roja de no-regresión

Al cerrar cada bloque, **DEBE** cumplirse:

```bash
pytest -q                                                      # ≥ 612 passed, ≤3 xfailed, 0 failed
python scripts/run_query_corpus.py --mode all                  # 88/88 PASS, 0 FAIL
make lint-core                                                 # exit 0
git diff --stat data/feedback.jsonl                            # vacío (sin cambios)
```

Si **cualquier** condición falla, el bloque queda abierto. No avanzar al siguiente.

A partir de B5, además:

```bash
python scripts/run_dialogue_corpus.py --mode all               # corpus dialogue verde según umbrales del bloque
```

### Regla G2 — Snapshot por bloque

Antes de empezar el bloque BN:

```bash
mkdir -p baseline
pytest -q > baseline/sprint2a_BN_pytest_before.txt 2>&1
python scripts/run_query_corpus.py --mode all > baseline/sprint2a_BN_corpus_before.txt 2>&1
make lint-core > baseline/sprint2a_BN_lint_before.txt 2>&1
git log --oneline -1 > baseline/sprint2a_BN_commit_before.txt
```

Al cerrar:

```bash
pytest -q > baseline/sprint2a_BN_pytest_after.txt 2>&1
python scripts/run_query_corpus.py --mode all > baseline/sprint2a_BN_corpus_after.txt 2>&1
make lint-core > baseline/sprint2a_BN_lint_after.txt 2>&1
diff baseline/sprint2a_BN_pytest_before.txt baseline/sprint2a_BN_pytest_after.txt
```

### Regla G3 — Commits atómicos por bloque

Un bloque = un commit. Formato:

```
feat(sprint2a): <BLOCK-ID> — <descripción breve>

<contexto>

<resumen del cambio>

Validation:
- pytest: 612 → N passed (Δ +M nuevos)
- corpus v1: 88/88 → 88/88 (sin regresión)
- corpus dialogue: <métrica relevante del bloque>
- lint-core: verde
```

### Regla G4 — No tocar archivos fuera del scope del bloque

Cada bloque lista archivos permitidos. **Cualquier modificación a archivos no listados invalida el bloque.** Si el bloque revela que un cambio externo es necesario, parar y consultar al revisor — no asumir.

### Regla G5 — No introducir dependencias nuevas

Si un bloque parece requerir un paquete pip nuevo, parar y consultar. La excepción son librerías estándar de Python ya en uso.

### Regla G6 — Tests deben ser específicos del bug, no genéricos

Cada bloque crea o extiende un archivo de tests. Los tests deben:
- Reproducir el caso real reportado (con la query exacta del usuario, citada del backlog).
- Verificar el comportamiento esperado post-fix con asserts concretos.
- Incluir al menos un caso de "no romper lo que funcionaba" (regresión inversa).

### Regla G7 — Lenguaje del código y comentarios

- Código en inglés.
- Identificadores semánticos (`is_command_incomplete`, no `cic`).
- Comentarios solo cuando el WHY no es obvio. Sin docstrings extensas.
- Mensajes de error / clarification al usuario en español.

### Regla G8 — Tope duro de LoC (disciplina arquitectónica)

Para evitar god-objects:

- `app/core/query_plan.py < 300 LoC`
- `app/core/dialogue_policy.py < 400 LoC`

Hay un test que falla si excede (B2 lo crea). Si el bloque empuja sobre el límite, **parar y consultar** — probablemente la regla pertenece a otro módulo o a un catálogo de datos (no código).

### Regla G9 — Prohibiciones absolutas del sprint

Lo siguiente **NO** entra a Sprint 2A bajo ninguna circunstancia. Si parece necesario, parar y consultar:

- ❌ Relevance layer (BM25, embeddings, sentence-transformers).
- ❌ LLM-as-classifier (Gemma fine-tuneado, modelo nuevo, tool router LLM).
- ❌ Catálogo SENA/ICBF/ministerios para ENTITY-MULTI-REGIONAL.
- ❌ Polisemia completa (alojamiento/mantenimiento/transporte/etc. con context_markers).
- ❌ Catálogo bidder completo (`BIDDER-CATALOG-AND-001`).
- ❌ Procurement-action `objeto_signal` shape.
- ❌ Migración de stopwords de `query_router.py` a `dialogue_policy.py`.
- ❌ Absorber `opportunity_policy.py` (sigue independiente; `QueryPlan` lo respeta).
- ❌ Modificar `data/feedback.jsonl`.
- ❌ Tocar `app/core/llm_handler.py` salvo en B4.5 si la regla lo requiere (consultar antes).

---

## B0 — Baseline y verificación de estado limpio

**Tiempo:** 30 min
**Pre-requisitos:** Sprint 1A mergeado a main; rama nueva `sprint2/queryplan-dialogue` creada desde main fresco.

### Objetivo

Confirmar el estado actual del sistema y dejar el snapshot M0 del sprint para diff posterior.

### Archivos a tocar (crear)

- `baseline/sprint2a_B0_pytest.txt`
- `baseline/sprint2a_B0_corpus.txt`
- `baseline/sprint2a_B0_lint.txt`
- `baseline/sprint2a_B0_commit.txt`
- `baseline/sprint2a_B0_git_status.txt`

### Pasos

1. Verificar rama:
   ```bash
   git branch --show-current  # debe ser sprint2/queryplan-dialogue
   git status --short          # debe estar limpio (o solo archivos no-sprint conocidos)
   ```
2. Capturar baseline:
   ```bash
   pytest -q > baseline/sprint2a_B0_pytest.txt 2>&1
   python scripts/run_query_corpus.py --mode all > baseline/sprint2a_B0_corpus.txt 2>&1
   make lint-core > baseline/sprint2a_B0_lint.txt 2>&1
   git log --oneline -1 > baseline/sprint2a_B0_commit.txt
   git status --short > baseline/sprint2a_B0_git_status.txt
   ```
3. Confirmar manualmente que los outputs son los esperados:
   - pytest: `612 passed, 3 xfailed`
   - corpus: `88/88 PASS, 0 FAIL`
   - lint: exit 0

### Criterio de aceptación

- 5 archivos en `baseline/` creados.
- Baseline coincide con el estado post-Sprint 1A.

### Commit

```
chore(sprint2a): B0 — capture baseline before QueryPlan/DialoguePolicy work
```

---

## B1 — `dialogue_policy.py` mínimo (clasificación pura)

**Tiempo:** 3-4 horas
**Pre-requisitos:** B0 cerrado.

### Objetivo

Crear una capa de clasificación que toma `raw_query` + `params` y produce **señales** de diálogo (no decisiones de ejecución, no SoQL, no SECOP). Esta capa NO modifica `params`; solo emite etiquetas que `QueryPlan` (B2) consumirá.

### Archivos a tocar

- `app/core/dialogue_policy.py` (crear)
- `tests/test_dialogue_policy.py` (crear)

### Especificación funcional

```python
# app/core/dialogue_policy.py

from dataclasses import dataclass, field
from typing import Literal

DialogueSignalKind = Literal[
    "command_incomplete",       # "filtra por departamento" sin valor
    "implicit_opportunity",     # "algún proceso para X" / "alguna oferta del Y"
    "discourse_filler",         # "hola, estoy interesado", quejas, verbos interrogativos
    "temporal_month",           # "en mayo", "este mes", "el mes pasado"
    "number_phrase_scrub",      # "supere", "exceda" cuando ya hay valor_max
    "city_mention_as_topic",    # token geográfico atrapado en objeto
    "non_query_followup",       # queja/comentario tras query previa
]

@dataclass
class DialogueSignal:
    kind: DialogueSignalKind
    payload: dict = field(default_factory=dict)
    confidence: Literal["high", "medium", "low"] = "high"
    source_tokens: list[str] = field(default_factory=list)

def classify(raw_query: str, params: dict, context: dict | None = None) -> list[DialogueSignal]:
    """
    Returns 0..N signals. Stateless. No SECOP calls.
    context contains: prev_params (dict | None), prev_intent_type (str | None), turn_index (int).
    """
```

#### Reglas de clasificación (cada una documentada con caso del backlog)

**R1 — command_incomplete (cubre COMMAND-SLOT-001):**

Detectar imperativos sin valor:
- `^(filtra|filtrar|filtre)\s+por\s+(departamento|ciudad|entidad|valor|modalidad|fecha)\b` sin token siguiente con contenido.
- `^(ordena|ordenar|muéstrame)\s+por\s*$`

Emit: `kind="command_incomplete"`, `payload={"slot": "departamento"|...}`.

**R2 — implicit_opportunity (cubre OPP-INTENT-001, 5 apariciones):**

Patrones:
- `\balgún\s+proceso\b` / `\balguna\s+oferta\b`
- `\bhay\s+algún\b` + sustantivo de SECOP
- `\bproceso[s]?\s+para\b` (cuando NO hay marcador histórico contrario: año pasado, anterior, etc.)
- `\bdónde\s+(me\s+)?(puedo|podría)\s+presentar\b`

Emit: `kind="implicit_opportunity"`, `payload={"trigger_phrase": "algún proceso para"}`.

**R3 — discourse_filler (cubre COURTESY-FILLER-001, 4 apariciones + NO-FOLLOWUP-DETECTION-001):**

Tokens/frases que NO son objeto contractual y deben scrubearse:
- Cortesía: `hola`, `buenas`, `gracias`, `por favor`.
- Intención usuario: `estoy interesado`, `actualmente`, `quisiera`, `me gustaría`.
- Quejas/evaluación (señal de `non_query_followup` si hay turno previo): `nada`, `terrible`, `incorrecto`, `mal`, `fracaso`, `no funcionó`, `está mal`.
- Verbos interrogativos solos: `fueron`, `fue`, `son`, `están`.
- Cuantificadores vagos: `algún`, `alguna`, `algo de` (sin sustantivo de SECOP detrás).

Emit:
- `kind="discourse_filler"`, `payload={"tokens_to_scrub": [...]}` siempre.
- Si hay `prev_params` Y la query es mayoritariamente quejas/evaluación sin sustantivos de SECOP: emit adicional `kind="non_query_followup"`, `payload={"reason": "complaint_after_query"}`.

**R4 — temporal_month (cubre TEMPORAL-MONTH-PARSING-001):**

Patrones:
- `\ben\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\b` sin año explícito → resolver a mes/año-actual.
- `\beste\s+mes\b` → mes/año actuales.
- `\bel\s+mes\s+pasado\b` → mes anterior.
- `\bhace\s+(\d+)\s+meses\b` → ventana relativa.

Emit: `kind="temporal_month"`, `payload={"month": 5, "year": 2026, "phrase": "en mayo"}`.

**R5 — number_phrase_scrub (cubre NUMBER-CONTEXT-PHRASE-001):**

Solo activar si `params.valor_max` o `params.valor_min` ya están poblados. Detectar tokens de polaridad residuales en `params.objeto`:

```python
SCRUB_CATALOG = {
    "supere", "exceda", "supera", "alcance", "rebase", "llegue",
    "baje", "suba", "cueste", "valga",
    "mayores", "menores", "mayor", "menor", "sea", "sean",
    "en", "desde", "hasta", "durante",
}
```

Emit: `kind="number_phrase_scrub"`, `payload={"tokens_to_scrub": ["supere", "los"]}`.

**R6 — city_mention_as_topic (cubre CITY-MENTION-AS-OBJECT-001, 3 apariciones):**

Cuando `params.objeto` contiene un token que el gazetteer reconoce como municipio Y `params.ciudad`/`params.departamento_resolved` están vacíos:

Llamar a la nueva interfaz `entity_resolver.geo_mention_resolution(token)` (B1 también la introduce — ver más abajo). Si retorna `{kind: "city", confidence: "high"}`:

Emit: `kind="city_mention_as_topic"`, `payload={"city": "Ipiales", "token_in_objeto": "ipiales"}`.

**Nota crítica:** `geo_mention_resolution` es interfaz nueva en `entity_resolver.py`. Su implementación mínima en este bloque:

```python
# en app/core/entity_resolver.py (extender, no reemplazar)

@dataclass
class GeoMention:
    kind: Literal["city", "department", "entity", "unknown"]
    canonical: str | None
    confidence: Literal["high", "medium", "low"]

def geo_mention_resolution(token: str) -> GeoMention:
    """Pure lookup: gazetteer-only, no fuzzy beyond exact lowercase match."""
```

**Implementación mínima:** lookup contra `GAZETTEER_CIUDADES` y `GAZETTEER_DEPARTAMENTOS` ya existentes. NO se agrega catálogo nuevo. NO se hace fuzzy. NO se incluyen entidades multi-regional.

### Tests (mínimos)

```python
# tests/test_dialogue_policy.py
import pytest
from app.core.dialogue_policy import classify, DialogueSignal

# R1
def test_command_incomplete_departamento():
    signals = classify("filtra por departamento", {}, None)
    assert any(s.kind == "command_incomplete" and s.payload["slot"] == "departamento" for s in signals)

def test_command_complete_does_not_trigger():
    signals = classify("filtra por departamento huila", {}, None)
    assert not any(s.kind == "command_incomplete" for s in signals)

# R2
@pytest.mark.parametrize("query", [
    "algún proceso para mantenimiento",
    "alguna oferta del sena",
    "hay algún proceso de aseo",
    "dónde me puedo presentar como proveedor de carpas",
])
def test_implicit_opportunity_triggers(query):
    signals = classify(query, {}, None)
    assert any(s.kind == "implicit_opportunity" for s in signals)

# R3 — caso real del backlog
def test_discourse_filler_courtesy():
    signals = classify("hola, estoy interesado en vender carpas", {}, None)
    filler = [s for s in signals if s.kind == "discourse_filler"]
    assert filler
    assert "hola" in filler[0].payload["tokens_to_scrub"]
    assert "estoy" in filler[0].payload["tokens_to_scrub"]

def test_non_query_followup_after_complaint():
    prev = {"objeto": ["mantenimiento"], "departamento_resolved": "Bolívar"}
    signals = classify("nada de eso sirve, no funcionó", {}, {"prev_params": prev, "turn_index": 2})
    assert any(s.kind == "non_query_followup" for s in signals)

# R4 — caso real Putumayo
def test_temporal_month_en_mayo():
    signals = classify("aseo en putumayo en mayo", {}, None)
    month_sig = [s for s in signals if s.kind == "temporal_month"]
    assert month_sig
    assert month_sig[0].payload["month"] == 5

# R5 — caso real Palermo (extiende B1 Sprint 1A)
def test_number_phrase_scrub_when_value_extracted():
    params = {"valor_max": 25_000_000, "objeto": ["transporte", "escolar", "supere", "los"]}
    signals = classify("transporte escolar no supere los 25 millones", params, None)
    scrub = [s for s in signals if s.kind == "number_phrase_scrub"]
    assert scrub
    assert "supere" in scrub[0].payload["tokens_to_scrub"]

def test_number_phrase_no_scrub_when_no_value():
    params = {"objeto": ["transporte", "escolar"]}
    signals = classify("transporte escolar", params, None)
    assert not any(s.kind == "number_phrase_scrub" for s in signals)

# R6 — casos reales Ipiales/Olaya/Yarumal
@pytest.mark.parametrize("query,token,city", [
    ("diseño ipiales", "ipiales", "Ipiales"),
    ("manejo de residuos sólidos olaya", "olaya", "Olaya"),
    ("silvopastoriles yarumal", "yarumal", "Yarumal"),
])
def test_city_mention_as_topic(query, token, city):
    params = {"objeto": query.lower().split()}
    signals = classify(query, params, None)
    geo = [s for s in signals if s.kind == "city_mention_as_topic"]
    assert geo, f"Expected city signal for {query!r}"
    assert geo[0].payload["city"] == city

# Regresión inversa: queries normales no deben emitir señales espurias
@pytest.mark.parametrize("query", [
    "contratos de mantenimiento en bogotá",
    "procesos abiertos de alimentación en atlántico",
    "consultoría cartagena",
])
def test_clean_queries_emit_no_signals(query):
    signals = classify(query, {"objeto": query.split()}, None)
    # Permitido: 0 signals, o solo señales benignas (no command_incomplete, no non_query_followup)
    blocking_kinds = {"command_incomplete", "non_query_followup"}
    assert not any(s.kind in blocking_kinds for s in signals)
```

### Reglas locales

- `dialogue_policy.py` es **stateless** y **puro**: misma entrada → misma salida, no toca SECOP, no toca DB, no toca disco.
- No importar de `query_router.py` (evitar ciclos).
- Puede importar de `entity_resolver.py` (solo `geo_mention_resolution`), `intent_vocabulary.py` (catálogos léxicos), `morphological_variants.py` (lectura).
- Catálogos léxicos (cortesía, scrub, etc.) declarados como constantes módulo-nivel al inicio del archivo.

### Criterios de aceptación

- `pytest tests/test_dialogue_policy.py` verde con ≥20 tests.
- `pytest -q` global verde (612 → ~632 passed).
- `make lint-core` verde.
- Corpus v1: 88/88 PASS.
- `dialogue_policy.py` LoC < 400.
- `geo_mention_resolution` agregada a `entity_resolver.py` con su test específico.

### Commit

```
feat(sprint2a): B1 — add dialogue_policy.py with 7 signal kinds (no execution)

Stateless classifier emitting signals for command_incomplete,
implicit_opportunity, discourse_filler, temporal_month,
number_phrase_scrub, city_mention_as_topic, non_query_followup.

QueryPlan (B2) will consume these. No changes to query_router yet.

Also extends entity_resolver with geo_mention_resolution() as
interface for QueryPlan and future ENTITY-MULTI-REGIONAL work.
```

---

## B2 — `query_plan.py` mínimo

**Tiempo:** 3 horas
**Pre-requisitos:** B1 cerrado.

### Objetivo

Crear el dataclass `QueryPlan` y la función `compute_query_plan()` que toma `params + raw_query + context` y produce un plan determinista de qué hacer. **Aún NO se inserta al orchestrator** — eso es B3.

### Archivos a tocar

- `app/core/query_plan.py` (crear)
- `tests/test_query_plan.py` (crear)
- `tests/test_module_size_limits.py` (crear) — disciplina G8

### Especificación funcional

```python
# app/core/query_plan.py

from dataclasses import dataclass, field
from typing import Literal

@dataclass
class QueryPlan:
    should_execute: bool
    params: dict                              # params (posiblemente modificados por las reglas)
    missing_slots: list[str] = field(default_factory=list)
    risk_flags: list[str] = field(default_factory=list)
    clarification_question: str | None = None
    reason: str = ""                          # razón legible: "command_incomplete", "valid_query", etc.
    signals: list = field(default_factory=list)  # lista de DialogueSignal (para debug/observability)

def compute_query_plan(
    params: dict,
    raw_query: str,
    context: dict | None = None,
) -> QueryPlan:
    """
    Pure function. No side effects. Consumes dialogue_policy.classify() signals
    and combines them with params to decide execution.
    """
```

#### Reglas de derivación (en orden estricto)

Estas reglas se evalúan en orden. La primera que dispara `should_execute=False` corta la evaluación.

**Regla P1 — command_incomplete:**
Si hay señal `command_incomplete`:
- `should_execute=False`
- `missing_slots=[signal.payload["slot"]]`
- `clarification_question="¿A qué <slot> quieres filtrar?"` (parametrizada)
- `reason="command_incomplete"`

**Regla P2 — non_query_followup:**
Si hay señal `non_query_followup`:
- `should_execute=False`
- `risk_flags=["complaint_detected"]`
- `clarification_question="Parece que algo no funcionó. ¿Quieres que diagnostiquemos por qué no apareció el resultado anterior?"`
- `reason="non_query_followup"`

**Regla P3 — city_mention_as_topic:**
Si hay señal `city_mention_as_topic`:
- Mover token de `params.objeto` a `params.ciudad`.
- Si `params.ciudad` ya estaba poblado con otro valor: NO sobrescribir, `risk_flags.append("ambiguous_city")`, dejar como estaba.
- Si `params.objeto` queda vacío tras mover el token Y no hay otros tokens significativos: `should_execute=False`, `missing_slots=["topic"]`, `clarification_question="Detecté '<city>' como ciudad. ¿Qué tema/rubro buscas?"`, `reason="city_extracted_topic_missing"`.
- Si `params.objeto` queda con tokens: continuar a P4 con params modificados.

**Regla P4 — implicit_opportunity:**
Si hay señal `implicit_opportunity` y `params.intent_type` está vacío o es `"history_search"`:
- `params.intent_type="opportunity_search"`
- Si `params.estado_family` está vacío: `params.estado_family="oferta_abierta"`
- `reason` ← se anexa con `"implicit_opportunity_promoted"`.

**Regla P5 — temporal_month:**
Si hay señal `temporal_month`:
- Si `params.fecha_desde`/`params.fecha_hasta` ya están poblados: NO sobrescribir, `risk_flags.append("temporal_conflict")`.
- Si están vacíos: poblar `fecha_desde=YYYY-MM-01`, `fecha_hasta=YYYY-MM-último_día` según payload.

**Regla P6 — number_phrase_scrub:**
Si hay señal `number_phrase_scrub`:
- Eliminar de `params.objeto` los tokens listados en `payload.tokens_to_scrub`.
- Si `params.objeto` queda vacío: `risk_flags.append("topic_emptied_by_scrub")` pero **NO** bloquear ejecución (las otras reglas decidirán si hay scope suficiente).

**Regla P7 — discourse_filler:**
Si hay señal `discourse_filler` y NO se disparó P2:
- Eliminar tokens listados en `payload.tokens_to_scrub` de `params.objeto`.
- `reason` ← anexar `"filler_scrubbed"`.

**Regla P8 — invalid_value_range:**
Si `params.valor_min` y `params.valor_max` ambos poblados Y `valor_min > valor_max`:
- `should_execute=False`
- `risk_flags=["invalid_range"]`
- `clarification_question="El rango de valor que diste es inválido (mínimo > máximo). ¿Cuál querías?"`
- `reason="invalid_value_range"`

**Regla final — default:**
Si ninguna regla bloqueó: `should_execute=True`, `reason="valid_query"`.

### Tests (mínimos)

```python
# tests/test_query_plan.py
import pytest
from app.core.query_plan import compute_query_plan

# P1
def test_command_incomplete_blocks_execution():
    plan = compute_query_plan({}, "filtra por departamento", None)
    assert plan.should_execute is False
    assert "departamento" in plan.missing_slots
    assert "departamento" in (plan.clarification_question or "").lower()

# P3 — caso real del backlog
@pytest.mark.parametrize("query,token,city", [
    ("diseño ipiales", "ipiales", "Ipiales"),
    ("manejo de residuos sólidos olaya", "olaya", "Olaya"),
    ("silvopastoriles yarumal", "yarumal", "Yarumal"),
])
def test_city_extracted_from_objeto(query, token, city):
    params = {"objeto": query.split(), "dataset": "procesos"}
    plan = compute_query_plan(params, query, None)
    assert plan.params.get("ciudad") == city
    assert token not in [t.lower() for t in plan.params.get("objeto", [])]

def test_city_extracted_topic_missing_asks_for_topic():
    plan = compute_query_plan({"objeto": ["ipiales"], "dataset": "procesos"}, "ipiales", None)
    assert plan.should_execute is False
    assert "topic" in plan.missing_slots or "tema" in (plan.clarification_question or "").lower()

# P4 — caso real
def test_implicit_opportunity_promotes_intent():
    plan = compute_query_plan(
        {"objeto": ["mantenimiento"], "dataset": "procesos"},
        "algún proceso para mantenimiento",
        None,
    )
    assert plan.params.get("intent_type") == "opportunity_search"
    assert plan.params.get("estado_family") == "oferta_abierta"

# P5 — caso real Putumayo
def test_temporal_month_resolves_to_current_year():
    from datetime import datetime
    plan = compute_query_plan(
        {"objeto": ["aseo"], "departamento_resolved": "Putumayo", "dataset": "procesos"},
        "aseo putumayo en mayo",
        None,
    )
    current_year = datetime.now().year
    assert plan.params.get("fecha_desde") == f"{current_year}-05-01"

# P6 — caso real Palermo (extiende Sprint 1A B1)
def test_number_phrase_scrub_removes_verb_from_objeto():
    plan = compute_query_plan(
        {"objeto": ["transporte", "escolar", "supere", "los"], "valor_max": 25_000_000},
        "transporte escolar no supere los 25 millones",
        None,
    )
    objeto = [t.lower() for t in plan.params.get("objeto", [])]
    assert "supere" not in objeto
    assert "los" not in objeto
    assert "transporte" in objeto

# P7 — caso real Barranquilla casa lúdica
def test_discourse_filler_scrubs_courtesy():
    plan = compute_query_plan(
        {"objeto": ["hola", "estoy", "interesado", "casas", "lúdicas"], "ciudad": "Barranquilla"},
        "hola estoy interesado en casas lúdicas en barranquilla",
        None,
    )
    objeto = plan.params.get("objeto", [])
    assert "hola" not in objeto
    assert "estoy" not in objeto

# P8
def test_invalid_value_range_blocks():
    plan = compute_query_plan(
        {"valor_min": 1_000_000_000, "valor_max": 5_000_000, "objeto": ["mantenimiento"]},
        "mantenimiento entre 1000M y 5M",
        None,
    )
    assert plan.should_execute is False
    assert "invalid_range" in plan.risk_flags

# default
def test_clean_query_executes():
    plan = compute_query_plan(
        {"objeto": ["mantenimiento"], "departamento_resolved": "Bolívar", "dataset": "contratos"},
        "contratos de mantenimiento en bolívar",
        None,
    )
    assert plan.should_execute is True
    assert plan.reason == "valid_query"

# Idempotencia
def test_compute_is_pure():
    params = {"objeto": ["mantenimiento"], "dataset": "contratos"}
    p1 = compute_query_plan(dict(params), "mantenimiento bolívar", None)
    p2 = compute_query_plan(dict(params), "mantenimiento bolívar", None)
    assert p1.should_execute == p2.should_execute
    assert p1.params == p2.params
```

```python
# tests/test_module_size_limits.py
from pathlib import Path

def _count_loc(path: Path) -> int:
    return sum(
        1 for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    )

def test_query_plan_under_300_loc():
    loc = _count_loc(Path("app/core/query_plan.py"))
    assert loc < 300, f"query_plan.py grew to {loc} LoC — refactor before merging"

def test_dialogue_policy_under_400_loc():
    loc = _count_loc(Path("app/core/dialogue_policy.py"))
    assert loc < 400, f"dialogue_policy.py grew to {loc} LoC — refactor before merging"
```

### Reglas locales

- `query_plan.py` solo importa de: `dialogue_policy.py`, `dataclasses`, `typing`, stdlib.
- **No** importar `entity_resolver`, `soql_builder`, `secop_client`, `orchestrator`, `query_router`.
- `compute_query_plan` debe ser **pura**: no I/O, no globals, no side effects.

### Criterios de aceptación

- `pytest tests/test_query_plan.py` verde con ≥15 tests.
- `pytest tests/test_module_size_limits.py` verde.
- `pytest -q` global verde.
- Corpus v1: 88/88 PASS (no se tocó orchestrator todavía).
- LoC: `query_plan.py < 300`, `dialogue_policy.py < 400`.

### Commit

```
feat(sprint2a): B2 — add query_plan.py consuming DialoguePolicy signals

QueryPlan dataclass + compute_query_plan() pure function with
8 derivation rules (P1..P8). Not yet wired into orchestrator.

Adds LoC guard tests (G8): query_plan<300, dialogue_policy<400.
```

---

## B3 — Integración Burr como no-op

**Tiempo:** 2 horas
**Pre-requisitos:** B2 cerrado.

### Objetivo

Insertar el nodo `plan_query` en el grafo Burr **entre `apply_context` y `resolve_entities`**, pero configurado como pasante (no-op). El nodo computa el plan, lo registra en el state, pero **no afecta el flujo todavía**.

Este commit es la prueba de que la inserción estructural no rompe el corpus v1 (88/88). En B4 se activarán las reglas progresivamente.

### Archivos a tocar

- `app/core/orchestrator.py`
- `tests/test_plan_query_node.py` (crear)

### Especificación funcional

En `orchestrator.py`:

1. Importar `compute_query_plan` de `query_plan.py`.
2. Crear el action Burr `plan_query`:

```python
@action(reads=["parsed_params", "raw_query", "prev_params", "turn_index"], writes=["query_plan", "parsed_params"])
def plan_query(state):
    params = state.get("parsed_params", {})
    raw = state.get("raw_query", "")
    context = {
        "prev_params": state.get("prev_params"),
        "turn_index": state.get("turn_index", 0),
    }
    plan = compute_query_plan(params, raw, context)
    # FASE B3: no-op pasante — guardamos el plan pero NO modificamos parsed_params.
    return state.update(query_plan=plan)
```

3. Insertar el nodo en el grafo entre `apply_context` y `resolve_entities`:

```
parse_query → apply_context → plan_query → resolve_entities → ...
```

4. **No** rama condicional aún. `plan_query` siempre transita a `resolve_entities`.

### Tests

```python
# tests/test_plan_query_node.py

def test_plan_query_attaches_plan_to_state(monkeypatch):
    from app.core import orchestrator
    result = orchestrator.run_query("contratos de mantenimiento en bolívar", chat_id="test_b3")
    assert "query_plan" in result
    assert result["query_plan"].should_execute is True

def test_b3_does_not_modify_parsed_params():
    """B3 es no-op: aunque QueryPlan compute moveria 'ipiales' a ciudad, el state final aun no debe aplicar el cambio."""
    from app.core import orchestrator
    result = orchestrator.run_query("diseño ipiales", chat_id="test_b3_noop")
    # En B3 todavía no se aplica el plan. El comportamiento sigue siendo el de Sprint 1A.
    # Sólo verificamos que el plan se computó:
    assert "query_plan" in result

def test_corpus_v1_unaffected():
    """Smoke: el corpus v1 debe seguir verde después de insertar el nodo."""
    import subprocess
    proc = subprocess.run(
        ["python", "scripts/run_query_corpus.py", "--mode", "all"],
        capture_output=True, text=True,
    )
    assert "88/88 PASS" in proc.stdout or proc.returncode == 0
```

### Reglas locales

- **NO** cambiar el shape de `parsed_params` ni de ningún otro campo del state.
- **NO** agregar transiciones condicionales todavía.
- **NO** modificar `response_policy.py`, `formatter.py`, `clarify_query`.

### Criterios de aceptación

- pytest verde.
- **Corpus v1: 88/88 PASS** (este es el commit que prueba que la inserción no rompe nada).
- `git diff app/core/orchestrator.py` muestra solo: import nuevo + definición del action + inserción en el grafo. Nada más.

### Commit

```
feat(sprint2a): B3 — wire plan_query node as no-op pass-through

Inserts plan_query between apply_context and resolve_entities.
Computes QueryPlan and stores in state.query_plan, but does NOT
modify parsed_params or alter flow. Pure structural insertion.

Proves corpus v1 unaffected by the insertion (88/88).
Rules activate progressively in B4.
```

---

## B4 — Activar reglas progresivamente

**Tiempo:** 4-5 horas
**Pre-requisitos:** B3 cerrado, corpus v1 verde.

### Objetivo

Activar el plan en el orchestrator y conectar las reglas al flujo. Se hace en sub-bloques (B4.1..B4.5), **cada uno con su commit propio** y verificación contra corpus v1 + tests específicos.

### Estructura

| Sub-bloque | Regla(s) activada(s) | Caso del backlog | Tiempo |
|---|---|---|---|
| B4.1 | P3 city_mention_as_topic | CITY-MENTION-AS-OBJECT-001 | 1h |
| B4.2 | P4 implicit_opportunity | OPP-INTENT-001 | 45min |
| B4.3 | P5 temporal_month + P6 number_phrase_scrub + P7 discourse_filler | TEMPORAL/NUMBER/COURTESY | 1.5h |
| B4.4 | P1 command_incomplete + P2 non_query_followup + P8 invalid_range | COMMAND-SLOT/NO-FOLLOWUP/VALUE-TYPO | 1h |
| B4.5 | Smoke integrado | Validar los 5 bugs canónicos pasan E2E | 30min |

### Archivos a tocar (acumulado)

- `app/core/orchestrator.py` (en cada sub-bloque)
- `app/core/response_policy.py` (B4.4 para clarification_question)
- Tests específicos por sub-bloque (crear)

### Especificación común

En `plan_query` action, cambiar de no-op a aplicación:

```python
@action(reads=[...], writes=["query_plan", "parsed_params", "should_execute_decision"])
def plan_query(state):
    params = state.get("parsed_params", {})
    raw = state.get("raw_query", "")
    context = {...}
    plan = compute_query_plan(params, raw, context)
    return state.update(
        query_plan=plan,
        parsed_params=plan.params,           # ← AHORA SÍ aplicamos params modificados
        should_execute_decision=plan.should_execute,
    )
```

Agregar transición condicional al grafo: si `should_execute_decision == False`, ir a `clarify_query` (que ya existe). Si `True`, continuar a `resolve_entities`.

### B4.1 — Activar P3 city_mention_as_topic

**Tests específicos:**

```python
# tests/test_plan_query_city_extraction.py

@pytest.mark.parametrize("query,expected_city,expected_objeto", [
    ("diseño ipiales", "Ipiales", ["diseño"]),
    ("manejo de residuos sólidos olaya", "Olaya", ["manejo", "residuos", "sólidos"]),
    ("silvopastoriles yarumal", "Yarumal", ["silvopastoriles"]),
])
def test_city_extracted_e2e(query, expected_city, expected_objeto):
    result = workflow.run_query(query, chat_id=f"test_{expected_city.lower()}")
    assert result["parsed_params"].get("ciudad") == expected_city
    objeto = [t.lower() for t in result["parsed_params"].get("objeto", [])]
    for tok in expected_objeto:
        assert tok in objeto
```

**Criterio:** los 3 casos pasan; corpus v1: 88/88.

**Commit:** `feat(sprint2a): B4.1 — activate city extraction (CITY-MENTION-AS-OBJECT-001)`

### B4.2 — Activar P4 implicit_opportunity

**Tests específicos:**

```python
# tests/test_plan_query_implicit_opportunity.py

@pytest.mark.parametrize("query", [
    "algún proceso para aseo en putumayo",
    "alguna oferta del sena en huila",
    "hay algún proceso de residuos en antioquia",
    "dónde me puedo presentar para transporte",
    "proceso para mantenimiento eléctrico",
])
def test_implicit_opportunity_e2e(query):
    result = workflow.run_query(query, chat_id="test_opp")
    assert result["parsed_params"].get("intent_type") == "opportunity_search"
    assert result["parsed_params"].get("estado_family") == "oferta_abierta"
```

**Criterio:** 5 casos pasan; corpus v1: 88/88.

**Commit:** `feat(sprint2a): B4.2 — promote implicit opportunity intent (OPP-INTENT-001)`

### B4.3 — Activar P5+P6+P7

**Tests específicos:** `tests/test_plan_query_scrub_and_temporal.py` con casos de Putumayo (en mayo), Palermo (supere), Barranquilla (hola estoy interesado).

**Criterio:** los 3 casos pasan; corpus v1: 88/88.

**Commit:** `feat(sprint2a): B4.3 — temporal + scrub + discourse filler`

### B4.4 — Activar P1+P2+P8 (clarification path)

Aquí sí hay que tocar `response_policy.py`: cuando `should_execute=False`, la respuesta debe usar `plan.clarification_question` directamente (sin LLM).

```python
# app/core/response_policy.py — agregar

def build_clarification_response(plan: QueryPlan) -> dict:
    return {
        "response": plan.clarification_question or "Necesito más información para buscar.",
        "needs_clarification": True,
        "reason": plan.reason,
        "missing_slots": plan.missing_slots,
        "risk_flags": plan.risk_flags,
    }
```

`orchestrator.clarify_query` consume `state.query_plan` y llama a `build_clarification_response`.

**Tests específicos:** `tests/test_plan_query_clarification.py`.

**Criterio:** comando incompleto → clarification, no ejecuta SECOP; rango inválido → clarification; queja tras query → ruta de diagnóstico.

**Commit:** `feat(sprint2a): B4.4 — clarification path for command/non-query/invalid-range`

### B4.5 — Smoke integrado

Test que ejercita los 5 bugs canónicos en una sola corrida:

```python
# tests/test_sprint2a_canonical_smoke.py

CANONICAL_CASES = [
    {"query": "diseño ipiales", "expect": {"ciudad": "Ipiales"}},
    {"query": "casas lúdicas barranquilla", "expect": {"ciudad": "Barranquilla", "entidad_resolved": None}},
    {"query": "aseo putumayo en mayo", "expect": {"fecha_desde_starts_with": "2026-05"}},
    {"query": "transporte escolar huila no supere 25 millones", "expect": {"valor_max": 25_000_000, "objeto_not_contains": ["supere"]}},
    {"query": "algún proceso para mantenimiento en bolívar", "expect": {"intent_type": "opportunity_search"}},
]
```

**Criterio:** los 5 casos pasan E2E; corpus v1: 88/88.

**Commit:** `test(sprint2a): B4.5 — canonical smoke for 5 HIGH backlog bugs`

---

## B5 — `dialogue_policy_corpus.yaml` + runner extendido

**Tiempo:** 3-4 horas
**Pre-requisitos:** B4 cerrado.

### Objetivo

Crear el corpus de diálogo (60 casos: 17 reales del backlog + 28 sintéticos derivados + 15 negativos) y extender el runner para validar los nuevos campos.

### Archivos a tocar

- `tests/fixtures/dialogue_policy_corpus.yaml` (crear)
- `scripts/run_dialogue_corpus.py` (crear; runner independiente)
- `docs/DIALOGUE_CORPUS.md` (crear, breve doc de convenciones)

### Composición del corpus

**17 reales del backlog** (literales del CORPUS_BACKLOG.md):

```yaml
# city_as_scope (3)
- id: CITY-ASOBJ-001
  query: "diseño ipiales"
  category: city_as_scope
  expected_params:
    ciudad: "Ipiales"
    objeto_contains: ["diseño"]
    objeto_not_contains: ["ipiales"]
  should_execute: true

- id: CITY-ASOBJ-002
  query: "manejo de residuos sólidos olaya"
  category: city_as_scope
  expected_params:
    ciudad: "Olaya"
    objeto_not_contains: ["olaya"]
  should_execute: true

- id: CITY-ASOBJ-003
  query: "silvopastoriles yarumal"
  category: city_as_scope
  expected_params:
    ciudad: "Yarumal"
  should_execute: true

# city_not_entity (2)
- id: CITY-NOENT-001
  query: "casas lúdicas barranquilla"
  category: city_not_entity
  expected_params:
    ciudad: "Barranquilla"
    entidad_resolved: null
  should_execute: true

- id: CITY-NOENT-002
  query: "aires acondicionados barranquilla"
  category: city_not_entity
  expected_params:
    ciudad: "Barranquilla"
    entidad_resolved: null
  should_execute: true

# implicit_opportunity (5)
- id: OPP-IMPL-001
  query: "algún proceso para aseo en putumayo"
  category: implicit_opportunity
  expected_params:
    intent_type: "opportunity_search"
    estado_family: "oferta_abierta"
  should_execute: true

# ... continuar con OPP-IMPL-002..005, TEMPORAL-001, NUM-SCRUB-001,
#     COURTESY-001..004, NON-QUERY-001 (1 caso real cada uno)
```

**28 sintéticos derivados** (variaciones de los reales — singular/plural, sinónimos, otras ciudades del mismo bug):
- city_as_scope: "obras palermo", "diseño paicol", "consultoría doncello", etc. (8 sintéticos)
- city_not_entity: "casa lúdica bogotá", "aires medellín", etc. (4)
- implicit_opportunity: variaciones de "hay algún", "dónde puedo", "proceso para" (8)
- temporal_month: "en junio", "este mes", "el mes pasado" (4)
- discourse_filler: "buenas tardes quisiera", "gracias", "estoy interesado en X" (4)

**15 negativos** (queries normales que NO deben activar reglas):
- "contratos de mantenimiento en bogotá" (no opportunity, no city extraction)
- "consultoría cartagena" (consultoría es topic legítimo, no filler)
- "ipiales" solo (city sí, pero clarification_question debe pedir topic — verifica P3.b)
- "transporte escolar 2026 bolívar" (no temporal_month, año explícito)
- "procesos del 2025" (no temporal_month relativo)
- ... 10 más

### Estructura YAML

```yaml
# tests/fixtures/dialogue_policy_corpus.yaml
metadata:
  version: "1.0"
  created: "2026-05-21"
  total_cases: 60
  source: "docs/CORPUS_BACKLOG.md (17 real) + synthetic (28) + negative (15)"

cases:
  - id: CITY-ASOBJ-001
    query: "diseño ipiales"
    category: city_as_scope
    severity: HIGH
    source: "CORPUS_BACKLOG.md CITY-MENTION-AS-OBJECT-001"
    should_execute: true
    expected_params:
      ciudad: "Ipiales"
      objeto_contains: ["diseño"]
      objeto_not_contains: ["ipiales"]
    expected_signals:
      - kind: city_mention_as_topic
    # ...
```

### Runner extendido

```python
# scripts/run_dialogue_corpus.py

# Validadores soportados por caso:
# - should_execute: bool exacto
# - expected_params.ciudad: igualdad
# - expected_params.objeto_contains: list (subset match case-insensitive)
# - expected_params.objeto_not_contains: list (negative match)
# - expected_params.intent_type: igualdad
# - expected_params.estado_family: igualdad
# - expected_params.fecha_desde_starts_with: prefix match
# - expected_params.valor_max / valor_min: igualdad numérica
# - clarification_contains: substring match en clarification_question
# - expected_signals: lista de kinds que deben aparecer en signals

# Modos:
#   --mode all          → corre todo
#   --category city_as_scope → filtra
#   --severity HIGH     → filtra
#   --id CITY-ASOBJ-001 → un caso
#   --json              → output JSON para diff
```

### Tests del runner

```python
# tests/test_dialogue_corpus_runner.py

def test_runner_smoke():
    """El runner ejecuta el corpus y emite resultados estructurados."""
    import subprocess
    proc = subprocess.run(
        ["python", "scripts/run_dialogue_corpus.py", "--mode", "all", "--json"],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["total"] == 60
```

### Criterios de aceptación

- 60 casos en YAML.
- Runner corre exitosamente.
- **Tasa de PASS por categoría:**
  - `city_as_scope`: ≥85% (de 3+8=11 casos, ≥9 PASS)
  - `city_not_entity`: ≥85% (6 casos, ≥5)
  - `implicit_opportunity`: ≥85% (13 casos, ≥11)
  - `temporal_month`: ≥80% (5 casos, ≥4)
  - `number_phrase_scrub`: ≥80% (1+3=4 casos, ≥3)
  - `discourse_filler`: ≥80% (4+4=8 casos, ≥6)
  - `non_query_followup`: ≥80% (1+2=3 casos, ≥2)
  - **negativos: 100%** (queries normales NO pueden activar reglas espurias)

Si alguna categoría no alcanza el umbral, documentar en commit qué casos fallan y por qué (puede ser deuda explícita para Sprint 2B).

### Commit

```
feat(sprint2a): B5 — add dialogue_policy_corpus.yaml (60 cases) + runner

17 real cases from CORPUS_BACKLOG.md + 28 synthetic + 15 negative.
Runner validates should_execute, expected_params subset,
clarification_contains, expected_signals.

Category PASS rates:
- city_as_scope: X/11
- city_not_entity: X/6
- implicit_opportunity: X/13
- temporal_month: X/5
- number_phrase_scrub: X/4
- discourse_filler: X/8
- non_query_followup: X/3
- negatives: 15/15 (required)
```

---

## B6 — Smoke final + métricas de cierre del sprint

**Tiempo:** 1-2 horas
**Pre-requisitos:** B5 cerrado.

### Objetivo

Verificar que los 5 bugs HIGH canónicos pasan E2E, capturar métricas finales del sprint, y registrar en docs.

### Archivos a tocar

- `baseline/sprint2a_FINAL_pytest.txt` (crear)
- `baseline/sprint2a_FINAL_corpus_v1.txt` (crear)
- `baseline/sprint2a_FINAL_corpus_dialogue.json` (crear)
- `docs/decisions_log.md` (extender con ADR-006 — QueryPlan/DialoguePolicy)
- `docs/CORPUS_BACKLOG.md` (marcar como cerrados los 5 bugs HIGH cubiertos)

### Pasos

1. Smoke manual de las 5 queries canónicas:

   | Query | Resultado esperado |
   |---|---|
   | "diseño ipiales" | Ipiales en ciudad, no en objeto |
   | "casas lúdicas barranquilla" | Barranquilla=ciudad, entidad_resolved=null |
   | "aseo putumayo en mayo" | fecha_desde=2026-05-01, fecha_hasta=2026-05-31 |
   | "transporte escolar huila no supere 25 millones" | valor_max=25M, "supere" scrubeado |
   | "algún proceso para mantenimiento en bolívar" | intent_type=opportunity_search |

2. Métricas finales:
   ```bash
   pytest -q > baseline/sprint2a_FINAL_pytest.txt 2>&1
   python scripts/run_query_corpus.py --mode all > baseline/sprint2a_FINAL_corpus_v1.txt 2>&1
   python scripts/run_dialogue_corpus.py --mode all --json > baseline/sprint2a_FINAL_corpus_dialogue.json 2>&1
   make lint-core
   ```

3. ADR-006 en `decisions_log.md`:

```markdown
## ADR-006: QueryPlan + DialoguePolicy como capa de decisión pre-ejecución

**Fecha:** 2026-05-21
**Contexto:** 80 queries humanas con ~20% éxito. Parchar `query_router.py` con condicionales aislados no escala. Sprint 1A demostró que los CRITICAL son resolubles puntualmente, pero los HIGH son patrones que requieren una capa que centralice la decisión.

**Decisión:** introducir `dialogue_policy.py` (clasificador puro de señales) + `query_plan.py` (compute determinístico con 8 reglas). Insertarlos como nodo Burr entre `apply_context` y `resolve_entities`. Los 5 bugs HIGH canónicos se resuelven en la capa nueva, no en parches.

**Alternativas descartadas:**
- 5 fixes puntuales en query_router (parche infinito).
- Capa con relevance/embeddings/LLM (sobrearquitectura para datos disponibles).
- Migrar stopwords inmediatamente (deferido a Sprint 3 o cuando dialogue_policy esté estabilizado).

**Consecuencias:** una sola capa nombrada para decisiones de "qué hacer con esta query". Cada bug nuevo cae en una regla nombrada (P1..P8) o se convierte en bug arquitectónico explícito. LoC topes (300/400) defienden contra god-object.
```

4. Actualizar `CORPUS_BACKLOG.md` marcando como cerrados:
   - CITY-MENTION-AS-OBJECT-001 → CLOSED Sprint 2A
   - CITY-TO-ENTITY-AUTOPROMOTION-001 → CLOSED Sprint 2A
   - TEMPORAL-MONTH-PARSING-001 → CLOSED Sprint 2A
   - NUMBER-CONTEXT-PHRASE-001 → CLOSED Sprint 2A (extensión de Sprint 1A B1)
   - OPP-INTENT-001 → CLOSED Sprint 2A
   - COURTESY-FILLER-001 → CLOSED Sprint 2A (parcial; discourse_filler base, polisemia diferida)
   - NO-FOLLOWUP-DETECTION-001 → CLOSED Sprint 2A
   - FOLLOWUP-DATASET-PURGE-001 → REVISAR (si el discourse_filler + non_query no lo cubre, queda HIGH)

### Criterios de aceptación

- 5 queries canónicas PASS en smoke manual.
- Métricas finales capturadas.
- ADR-006 escrita.
- Backlog actualizado.

### Commit

```
docs(sprint2a): B6 — close Sprint 2A, ADR-006, update backlog

Final metrics:
- pytest: 612 → N passed
- corpus v1: 88/88 PASS (no regression)
- corpus dialogue: M/60 PASS (categories above threshold)
- lint-core: clean

Closed bugs: CITY-MENTION-AS-OBJECT, CITY-TO-ENTITY-AUTOPROMOTION,
TEMPORAL-MONTH-PARSING, NUMBER-CONTEXT-PHRASE, OPP-INTENT,
COURTESY-FILLER (partial), NO-FOLLOWUP-DETECTION.
```

---

## Cómo el revisor valida la entrega

Al final de Sprint 2A (después de B6), el revisor verifica:

### Macro

```bash
pytest -q                                           # esperado: ≥640 passed (612 + ~30 nuevos)
python scripts/run_query_corpus.py --mode all       # 88/88 sin regresión
python scripts/run_dialogue_corpus.py --mode all    # umbrales por categoría
make lint-core                                       # verde
git log --oneline B0..HEAD                          # ≥10 commits (B0..B6 incl. sub-B4)
```

### Casos canónicos (smoke manual)

| Bug | Query | Resultado esperado |
|---|---|---|
| CITY-MENTION-AS-OBJECT | "diseño ipiales" | ciudad=Ipiales, objeto=[diseño] |
| CITY-TO-ENTITY-AUTOPROMOTION | "casas lúdicas barranquilla" | ciudad=Barranquilla, entidad_resolved=null |
| TEMPORAL-MONTH-PARSING | "aseo putumayo en mayo" | fecha_desde=2026-05-01 |
| NUMBER-CONTEXT-PHRASE | "transporte escolar huila no supere 25 millones" | valor_max=25M, objeto sin "supere" |
| OPP-INTENT | "algún proceso para mantenimiento" | intent_type=opportunity_search |

### Disciplina arquitectónica

```bash
wc -l app/core/query_plan.py            # < 300
wc -l app/core/dialogue_policy.py       # < 400
pytest tests/test_module_size_limits.py # verde
```

---

## Lo que NO está en Sprint 2A (aplazado consciente)

| Bug / Feature | Aplazado a |
|---|---|
| RELEVANCE-PHRASE-001 (BM25, embeddings) | Sprint 3 |
| ENTITY-MULTI-REGIONAL-001 (catálogo SENA/ICBF) | Sprint 3 |
| POLYSEMIC-TOPIC-001 (context_markers) | Sprint 3 |
| BIDDER-CATALOG-AND-001 (`objeto_or` extendido) | Sprint 2B |
| PROCUREMENT-ACTION-001 (`objeto_signal` shape) | Sprint 2B |
| MORPHO-EXPANSION-INCONSISTENT-001 | Sprint 2B |
| NIT-AMOUNT-CONFUSION-001 | Sprint 2B |
| Migración de stopwords `query_router.py` → `dialogue_policy.py` | Sprint 3+ (cuando dialogue_policy lleve 3 sprints estable) |
| Absorber `opportunity_policy.py` en `QueryPlan` | Post-Sprint 4 |
| LLM en clarification questions | Sprint 6 opcional |
| MCP server (BD propia + tools expuestas) | Sprint 8+, condicional a validación de mercado |

---

## Si algo sale mal — escalación al revisor

Codex **debe** parar y consultar antes de continuar si:

- Cualquier bloque hace fallar Regla G1 y la causa no es obvia.
- Un fix requiere modificar archivos fuera de la lista declarada del bloque.
- Aparece la necesidad de instalar una librería nueva.
- Un test escrito según especificación no se puede hacer pasar sin "trampa".
- Una regla del plan empuja `query_plan.py` o `dialogue_policy.py` sobre el límite de LoC (G8).
- Una prohibición de G9 parece necesaria para cerrar el bloque.

Comunicación esperada al revisor:

```
Bloque: B<N>.<M>
Estado: BLOQUEADO
Problema: <descripción precisa>
Hipótesis: <causa probable>
Archivos involucrados: <rutas>
Necesito decisión sobre: <opciones A / B / C>
```

---

## Resumen del cronograma

| Bloque | Tarea | Tiempo | Acumulado |
|---|---|---:|---:|
| B0 | Baseline | 0.5h | 0.5h |
| B1 | dialogue_policy.py | 3.5h | 4h |
| B2 | query_plan.py | 3h | 7h |
| B3 | Inserción Burr (no-op) | 2h | 9h |
| B4.1 | Activar P3 city | 1h | 10h |
| B4.2 | Activar P4 opportunity | 0.75h | 10.75h |
| B4.3 | Activar P5+P6+P7 | 1.5h | 12.25h |
| B4.4 | Activar P1+P2+P8 + clarification | 1h | 13.25h |
| B4.5 | Smoke integrado | 0.5h | 13.75h |
| B5 | Corpus YAML + runner | 3.5h | 17.25h |
| B6 | Cierre + ADR + backlog | 1.5h | 18.75h |

**Total: ~19 horas de implementación + tests.** Distribuible en 4-6 días de trabajo enfocado.

---

## Una última nota al ejecutor

Sprint 1A fue **defensa táctica**: parchar incendios CRITICAL. Sprint 2A es **el primer paso estructural**: introducir la capa que faltaba para decir "antes de ejecutar, ¿esto tiene sentido?".

La métrica de éxito **no es** "cuántos bugs cerré" sino **"los 5 casos canónicos del backlog funcionan E2E, el corpus v1 no regresó, y la nueva capa cabe en sus topes de LoC"**.

Si al cerrar B6 los 5 casos pasan, corpus v1 sigue en 88/88, corpus dialogue cumple umbrales por categoría, y `query_plan.py + dialogue_policy.py` están bajo sus límites, Sprint 2A está cerrado. Sprint 2B (BIDDER-CATALOG, PROCUREMENT-ACTION, MORPHO uneven, NIT-AMOUNT) puede arrancar.

**Recordatorio de Sprint 1A:** la última vez te desviaste de "QueryPlan estructural" a "9 fixes CRITICAL" porque los incendios pesaron más. Esta vez el plan es explícito sobre qué NO hacer (G9). Si una idea parece valer la pena pero está en G9, **es trampa** — apuntala en `docs/CORPUS_BACKLOG.md` como deuda Sprint 2B/3 y sigue. El sprint termina cuando termina, no cuando "ya que estamos".

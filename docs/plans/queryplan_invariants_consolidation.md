# Plan QP-INV: Consolidación de invariantes en el QueryPlan

**Estado:** Propuesto · **v2 — incorpora correcciones de revisión técnica** · listo para ejecución
**Autor del plan:** sesión de arquitectura 2026-06-03 (ver ADR-024)
**Ejecutor previsto:** Codex 5.5 (agente sin contexto de la conversación)
**Referencias:** ADR-024 (decisión de invertir en QueryPlan), [query_plan.py](../../app/core/query_plan.py)

> **Cambios v1→v2 (revisión técnica, todos aplicados):**
> 1. `has_substantial_filter` consume `SCOPE_TOPIC_KEYS` de `_followup_constants.py`
>    — **NO** se declara `SUBSTANTIAL_FILTER_KEYS` nuevo (evita tercera lista).
> 2. `apply_firmado_invariant(params)` → dividido en `apply_signed_dataset_invariant(raw_query, params)`
>    + `purge_invalid_contract_states(params)` — necesita `raw_query`, no solo params.
> 3. Garantía + test de que "firmados en ejecución" **conserva** estados activos.
> 4. P11 **idempotente, coexiste con P4** (no lo reemplaza de un salto).
> 5. P13 arranca **pasivo** (`risk_flag`); activación del bloqueo → subetapa 3.5
>    con prueba de equivalencia de respuesta.
> 6. `INVALID_CONTRACT_STATES` (firmado/celebrado) **separado** de
>    `NON_OPPORTUNITY_STATES` (cerrado/terminado/liquidado/adjudicado).
> 7. Etapa 0 define helper offline exacto que replica el orden REAL del pipeline
>    (`parse → llm_parse[opportunity_policy] → plan_query`, verificado en `graph.py`).

---

## 0. TL;DR para el ejecutor

Este plan **NO añade comportamiento nuevo**. Toma lógica jurídica/semántica que
hoy está **duplicada en 6+ archivos** y la consolida en **una sola fuente de
verdad pura y testeada**, que el `QueryPlan` aplica antes de ejecutar. El
objetivo es **0 regresiones** + eliminar la duplicación que causa los bugs
recurrentes ("firmado→Cerrado", opportunity inconsistente, etc.).

Regla de oro: **si una respuesta del sistema cambia para una query existente,
algo salió mal.** Esto es refactor de consolidación, no de features.

Frontera dura: el `QueryPlan` es **función pura sin I/O**. NO entra aquí nada que
necesite datos vivos (ranking, qué ciudades existen en SECOP, value-resolution
contra el dataset). Eso es otra fuga, otro plan.

---

## 1. Diagnóstico: el problema es fragmentación, no ausencia

La capa de "corrección semántica antes de ejecutar" **ya existe** —
`compute_query_plan` aplica 9 reglas P1–P9. El problema es que las **invariantes
jurídicas** (las que nunca deben fallar) viven dispersas y duplicadas. La misma
regla está re-escrita con listas distintas en cada sitio; cuando una cambia, las
demás divergen → bug.

### Inventario de fragmentación (verificado por grep el 2026-06-03)

| Invariante | Significado SECOP | Dónde vive hoy (fuentes paralelas) | Síntoma |
|---|---|---|---|
| **INV-1: "firmado" → dataset=contratos, SIN filtro de estado** | No existe campo "firmado" en SECOP. Es selector de dataset, no estado. | `query_router.py:134,378,661` (listas de tokens) · `llm_handler.py:34-38,102-103,177` (prompt+scrub) · `intent_vocabulary.py:68-74` (regex) · `estado_families.py:64-67,146` (solo comentarios) | 4 listas/regex independientes; ningún test garantiza la invariante |
| **INV-2: {Firmado, Celebrado} nunca son `estado_contrato`** | No son valores reales de `estado_contrato` en SECOP. | `llm_handler.py:116` (`_INVALID_CONTRACT_STATES={"Firmado","Celebrado"}`) · `opportunity_policy.py:28` (`_CLOSED_STATES`, lista DISTINTA) | dos listas de "estados a purgar" que no coinciden entre sí |
| **INV-3: intención de ofertar → dataset=procesos + estado_family=oferta_abierta** | Quien quiere presentarse busca procesos abiertos, no contratos. | `query_plan.py:162-172` (P4, vía signal `implicit_opportunity`) · `opportunity_policy.py:31-59` (frases bidder, llamado 3× en `llm_handler.py:224,245,281`) | DOS motores de "forzar opportunity" con disparadores distintos |
| **INV-4: `oferta_abierta` no aplica a contratos** | Un contrato ya está firmado; no recibe ofertas. | `estado_families.py:39-42` (lista vacía implícita) | implícito en datos, sin assert |
| **INV-5: `valor_min ≤ valor_max`** | — | `query_plan.py:219-235` (P8) ✅ ya consolidado | OK — referencia, no se toca |
| **INV-6: filtro sustancial (anti `WHERE 1=1`)** | scope/tema/estado/modalidad; fecha/valor/orden NO bastan. | `_followup_constants.py:53` (`SCOPE_TOPIC_KEYS`, marcada "H9: fuente única", usada por execution/analytics/followup/refinement) ✅ · PERO `soql_builder.py:25` (`_SUBSTANTIAL_FILTER_KEYS`) es una **segunda lista paralela** con claves distintas | dos listas que deberían ser una; `SCOPE_TOPIC_KEYS` ya es la canónica |

**Conclusión:** no hay que *construir* la capa de invariantes — hay que
*unificar* las que ya están escritas 2–6 veces. INV-5 ya está bien (P8);
sirve de patrón.

---

## 2. Arquitectura objetivo

### 2.1 Una sola fuente de verdad: `app/core/contract_invariants.py` (NUEVO)

Módulo **puro** (sin I/O, sin imports de SECOP/DB) que declara las invariantes
como **datos + funciones puras**. Es el único lugar donde se define qué es
"firmado", qué estados son inválidos, etc.

```python
# app/core/contract_invariants.py  (esqueleto objetivo — el ejecutor completa)
"""Invariantes jurídicas/semánticas de contratación SECOP.

Fuente ÚNICA de verdad. Cualquier otro módulo que necesite saber "¿es firmado
un estado?" o "¿qué estados son inválidos?" DEBE importar de aquí, nunca
redefinir listas locales.

Puro: sin I/O, sin SECOP, sin DB. Mismas entradas → misma salida.
"""
from __future__ import annotations

# INV-1 / INV-2 — semántica de "firmado" y estados inválidos
FIRMADO_TOKENS: frozenset[str] = frozenset({
    "firmado", "firmados", "firmada", "firmadas",
    "celebrado", "celebrados", "suscrito", "suscritos",
})
# DOS listas SEPARADAS — no mezclar (corrección revisor, ajuste B):
#  - INVALID: nunca son valores reales de estado_contrato → purgar SIEMPRE.
#  - NON_OPPORTUNITY: SÍ son estados reales, pero incompatibles con ofertar →
#    purgar SOLO en contexto opportunity.
INVALID_CONTRACT_STATES: frozenset[str] = frozenset({"firmado", "celebrado"})
NON_OPPORTUNITY_STATES: frozenset[str] = frozenset({
    "cerrado", "cerrados", "terminado", "terminados",
    "liquidado", "liquidados", "adjudicado", "adjudicados",
})

def _normalize(text: str) -> str:
    """Strip accents + lowercase. Copiado verbatim de opportunity_policy._normalize."""
    ...

def contains_signed_token(raw_query: str) -> bool:
    """True si raw_query menciona firmado/suscrito/celebrado (token-level)."""
    ...

def purge_invalid_contract_states(params: dict) -> tuple[dict, list[str]]:
    """INV-2: elimina SOLO estados inválidos ({firmado, celebrado}) de
    estado_contrato/estado, sea string o lista. NO toca estados reales
    (En ejecución, Modificado, Prorrogado, Cerrado...). No necesita raw_query.
    Devuelve (params_nuevos, risk_flags). NO muta el input.
    """
    ...

def apply_signed_dataset_invariant(raw_query: str, params: dict) -> tuple[dict, list[str]]:
    """INV-1: si raw_query contiene token de firmado → dataset=contratos.
    Luego llama purge_invalid_contract_states. CRÍTICO: NO borra estados
    válidos → "firmados en ejecución" conserva estado_contrato activo.
    """
    ...

# INV-3 — intención de ofertar
BIDDER_PHRASES: frozenset[str] = frozenset({...})  # consolida opportunity_policy._BIDDER_PHRASES
def apply_opportunity_invariant(raw_query: str, params: dict) -> tuple[dict, list[str]]:
    """INV-3: bidder/opportunity → dataset=procesos + estado_family=oferta_abierta,
    purga NON_OPPORTUNITY_STATES. IDEMPOTENTE: si ya está en opportunity, no
    duplica. Unifica enforce_bidder_opportunity_policy; coexiste con P4 (signal).
    """
    ...

def enforce_oferta_abierta_not_contracts(params: dict) -> tuple[dict, list[str]]:
    """INV-4: si estado_family=oferta_abierta y dataset=contratos → inconsistencia.
    (un contrato ya está firmado, no recibe ofertas). Resolver conservador.
    """
    ...

# INV-6 — filtro sustancial (chequeo PURO de params, sin construir SoQL)
# OJO: NO declarar una lista nueva aquí. La canónica YA existe:
#   from app.core._followup_constants import SCOPE_TOPIC_KEYS   # H9: fuente única
# has_substantial_filter() debe consumir SCOPE_TOPIC_KEYS, y la duplicación a
# eliminar es soql_builder._SUBSTANTIAL_FILTER_KEYS (segunda lista paralela).
from app.core._followup_constants import SCOPE_TOPIC_KEYS
def has_substantial_filter(params: dict) -> bool: ...
```

**Por qué módulo aparte y no más reglas dentro de `query_plan.py`:** `query_plan.py`
tiene el lock arquitectónico **G8 (<300 LOC)** y ya está en 247. Meter las
invariantes ahí lo revienta. Además separa conceptualmente "signals de diálogo"
(P1–P9, dependen de `dialogue_policy.classify`) de "invariantes jurídicas"
(P10–P13, dependen solo de params). Ver `tests/` de architectural-lock-in.

### 2.2 Punto de aplicación: nuevas reglas P10–P13 en `compute_query_plan`

`query_plan.py` importa `contract_invariants` y aplica las invariantes como un
bloque de reglas, en orden, **después** de P1–P9 (las correcciones de diálogo ya
ocurrieron) y **antes** del return. Cada invariante:
- corrige `working_params` si aplica,
- añade un `risk_flag` cuando corrige algo (observabilidad),
- nunca hace I/O.

```python
# dentro de compute_query_plan, antes del "Default: valid query"
# ── P10: firmado / estados inválidos (INV-1, INV-2) ──
if "P10" in rules_to_apply:
    working_params, flags = apply_signed_dataset_invariant(raw_query, working_params)
    risk_flags.extend(flags)
# ── P11: opportunity invariant (INV-3) — IDEMPOTENTE, NO reemplaza P4 ──
#   P4 sigue siendo el detector de signal (implicit_opportunity).
#   P11 normaliza dataset/estado de forma idempotente: si P4 (o
#   opportunity_policy en la rama LLM) ya puso opportunity_search, P11 NO
#   duplica enforcement — solo garantiza consistencia.
if "P11" in rules_to_apply:
    working_params, flags = apply_opportunity_invariant(raw_query, working_params)
    risk_flags.extend(flags)
# ── P12: oferta_abierta∉contratos (INV-4) ──
if "P12" in rules_to_apply:
    working_params, flags = enforce_oferta_abierta_not_contracts(working_params)
    risk_flags.extend(flags)
# ── P13: filtro sustancial (INV-6) — PASIVO en este plan ──
#   SOLO emite risk_flag. NO pone should_execute=False todavía: hoy el bloqueo
#   anti-global vive en SoQLBuilder (UnsafeGlobalQueryError) + execute_query
#   guard, y moverlo aquí cambiaría reason/route_reason/mensaje/trace = NO es
#   refactor puro. Activación → subetapa 3.5 con prueba de equivalencia.
if "P13" in rules_to_apply:
    if not has_substantial_filter(working_params):
        risk_flags.append("unsafe_global_candidate")
```

Añadir `"P10".."P13"` a `ALL_RULES` en `query_plan.py:20`.

> **Idempotencia (corrección revisor, Problema 3).** Orden real del pipeline
> (verificado en `graph.py`): `parse_query → llm_parse → plan_query`. En la rama
> `needs_llm`, `enforce_bidder_opportunity_policy` ya corrió en `llm_parse`
> ANTES de `compute_query_plan`. Por eso P11 **debe ser idempotente**: aplicar
> dos veces = aplicar una. En la rama heurística (la dominante: ~17/2437 queries
> usan LLM) `opportunity_policy` NO corre, y P4+P11 son el único motor.

---

## 3. Etapas con entregables

> Cada etapa = **1 commit atómico** en rama `qp-inv`, con suite verde antes de
> avanzar. Si una etapa rompe tests no relacionados, **detenerse y reportar** —
> no parchear a ciegas.

### Etapa 0 — Red de seguridad (caracterización)
**Objetivo:** congelar el comportamiento ACTUAL de las 6 fuentes antes de tocarlas.

**Helper offline obligatorio (corrección revisor, Problema 5).** El test DEBE
replicar el orden REAL del pipeline (verificado en `graph.py`:
`parse_query → llm_parse → plan_query`). `opportunity_policy` corre DENTRO de
`llm_parse`, **antes** de `compute_query_plan` — no después. Helper:

```python
from datetime import date
from app.core.query_router import QueryRouter
from app.core.query_plan import compute_query_plan
from app.core.opportunity_policy import enforce_bidder_opportunity_policy

def run_offline_invariant_pipeline(raw_query: str, *, simulate_llm: bool = False) -> dict:
    """Replica el orden real SIN red. simulate_llm=True añade el paso
    opportunity_policy que en producción vive en llm_parse (rama needs_llm)."""
    parsed = QueryRouter().parse(raw_query)
    params = dict(parsed.params)
    snap_after_parse = dict(params)
    if simulate_llm:                       # rama needs_llm: opportunity_policy ANTES del plan
        params = enforce_bidder_opportunity_policy(raw_query, params)
    snap_before_plan = dict(params)
    plan = compute_query_plan(params, raw_query, {"today": date.today()})
    return {
        "after_parse": snap_after_parse,
        "before_plan": snap_before_plan,
        "after_plan": plan.params,
        "should_execute": plan.should_execute,
        "risk_flags": plan.risk_flags,
        "reason": plan.reason,
    }
```

Caracterizar **ambas rutas** (`simulate_llm=False` dominante, `=True` LLM) y
snapshotear los 6 campos del dict.

**Entregables:**
- `tests/test_invariants_characterization.py` — tabla de casos sobre
  `run_offline_invariant_pipeline`, snapshots de los 6 campos. Casos mínimos:
  - "contratos firmados de mantenimiento" → `dataset=contratos`, sin `estado_contrato`.
  - "contratos firmados cerrados" → `dataset=contratos`, sin estado inválido.
  - **"contratos firmados que estén en ejecución de mantenimiento en Bogotá"**
    → `dataset=contratos` Y **conserva** `estado_contrato` activo (En ejecución…).
    ⚠️ ESTE ES EL CASO CRÍTICO (corrección revisor, Problema 2): la invariante
    firmado NO debe borrar estados válidos. Si este snapshot cambia = regresión.
  - "quiero presentarme a obras en Boyacá" → `dataset=procesos`, `intent_type=opportunity_search`, `estado_family=oferta_abierta`.
  - "algún proceso para mantenimiento" → opportunity (P4 actual). Verificar que
    **no** se genere doble filtro contradictorio (ajuste D): el fallback de
    `soql_builder.py` (`estado_de_apertura_del_proceso='Abierto'` cuando
    intent=opportunity_search, STATE-PRIORITY-001) NO debe colisionar con un
    `estado_del_procedimiento IN (...)` que P11 pudiera introducir. Snapshot del
    SoQL `WHERE` para este caso, y assert de que no aparecen ambos a la vez si
    hoy no aparecen.
  - "obras" (sin scope) → `should_execute` y `risk_flags` actuales (P13 NO debe
    cambiar esto: hoy bloquea downstream, no en el plan).
  - 1 caso por cada fila INV-1..INV-6 del inventario.

**Criterio de aceptación:** el test pasa **sobre el código actual sin modificar**
(captura el baseline). `pytest tests/test_invariants_characterization.py -q` verde.

### Etapa 1 — Módulo `contract_invariants.py` puro
**Objetivo:** crear la fuente única, con su test exhaustivo, sin integrarla aún.

**Entregables:**
- `app/core/contract_invariants.py` con:
  - **Datos:** `FIRMADO_TOKENS`, `INVALID_CONTRACT_STATES`, `NON_OPPORTUNITY_STATES`
    (dos listas SEPARADAS — ver ajuste B), `BIDDER_PHRASES`.
  - **Funciones puras:** `_normalize`, `contains_signed_token`,
    `purge_invalid_contract_states(params)`, `apply_signed_dataset_invariant(raw_query, params)`,
    `apply_opportunity_invariant(raw_query, params)`,
    `enforce_oferta_abierta_not_contracts(params)`, `has_substantial_filter(params)`.
  - **NO declarar `SUBSTANTIAL_FILTER_KEYS`** (corrección revisor, Problema 1).
    `has_substantial_filter` importa `SCOPE_TOPIC_KEYS` de `_followup_constants.py`
    (fuente única H9). Crear una lista nueva aquí = tercera fuente = anti-objetivo.
  - Todas las funciones que reciben `params` aceptan `estado_contrato`/`estado`
    como **string O lista** y normalizan (ajuste A).
- `tests/test_contract_invariants.py` — unitario, exhaustivo, sin I/O. Cubre:
  - `apply_signed_dataset_invariant`: firmado fuerza `dataset=contratos`.
  - `purge_invalid_contract_states`: {firmado, celebrado} purgado caso-insensible,
    string y lista; **NO purga** En ejecución/Modificado/Prorrogado/Cerrado.
  - **`test_firmados_en_ejecucion_preserves_active_states`** ← el más importante:
    firmado + estado activo conserva el estado.
  - `apply_opportunity_invariant`: bidder → procesos+oferta_abierta+purga
    `NON_OPPORTUNITY_STATES`; **idempotente** (aplicar 2× = 1×).
  - `enforce_oferta_abierta_not_contracts`: oferta_abierta+contratos → resuelto.
  - `has_substantial_filter`: fecha/valor/orden solos → False; +scope/tema → True;
    `assert has_substantial_filter` usa exactamente `SCOPE_TOPIC_KEYS` (test de identidad).
  - **invariantes como test** (lo que hoy es comentario):
    `"Firmado" not in purge_invalid_contract_states({"estado_contrato": ["Firmado","En ejecución"]})[0]["estado_contrato"]`
    y `"En ejecución" in` ese mismo resultado.

**Criterio de aceptación:** `pytest tests/test_contract_invariants.py -q` verde;
`make lint-core` limpio; `grep -n "import" app/core/contract_invariants.py` no
muestra SECOP/DB/red (solo stdlib + `_followup_constants`).

### Etapa 2 — Integrar P10–P12 activo, **P13 pasivo** en `compute_query_plan`
**Objetivo:** que el QueryPlan aplique las invariantes desde la fuente única,
sin cambiar comportamiento observable.

**Entregables:**
- `query_plan.py`: import de `contract_invariants`; reglas P10–P13; `ALL_RULES`
  actualizado; risk_flags nuevos: `firmado_dataset_forced`, `invalid_estado_purged`,
  `opportunity_enforced`, `oferta_abierta_not_contracts`, **`unsafe_global_candidate`**
  (P13 pasivo — flag, NO `unsafe_global_blocked`).
- **P11 coexiste con P4** (NO lo reemplaza — corrección Problema 3). P4 sigue como
  detector de signal; P11 normaliza idempotente.
- **P13 SOLO emite `risk_flag`** (corrección Problema 4). NO toca `should_execute`.
  El bloqueo anti-global sigue donde está hoy (builder + execute_query guard).
- Extender `tests/test_query_plan.py` con bloques P10–P13 (mismo estilo que P1–P9),
  incluyendo el test de idempotencia de P11 (aplicar el plan 2× = 1×).

**Criterio de aceptación:** `test_invariants_characterization.py` (Etapa 0) **sigue
verde sin cambios** ← prueba de no-regresión (ambas rutas). `pytest tests/test_query_plan.py -q`
verde. Respeta G8: `query_plan.py` < 300 LOC (`wc -l`).

### Etapa 3.5 — Activar P13 (SOLO si la respuesta final queda equivalente)
**Objetivo:** mover el bloqueo anti-global del builder/execute_query al QueryPlan,
*si y solo si* se prueba equivalencia de respuesta. Subetapa separada porque ya
NO es "0 behavior change" sino "behavior-equivalent clarification".

**Entregables:**
- P13 pasa de `risk_flag` a `should_execute=False` con `reason="unsafe_global"`.
- Test que compara **clase de respuesta final** (no solo `should_execute`) antes/
  después: el usuario debe recibir la misma clarificación.
- Si la equivalencia NO se puede probar → **dejar P13 pasivo y cerrar el plan ahí**;
  el bloqueo activo queda como plan futuro `QP-SAFETY-ACTIVATION`.

**Criterio de aceptación:** respuesta final idéntica en clase para todos los casos
sin scope del corpus; si no, no se activa.

### Etapa 3 — Eliminar duplicación (el corazón del plan)
**Objetivo:** que las 6 fuentes dispersas deleguen a `contract_invariants`. Una
sola definición de cada lista/regla.

**Entregables (uno por fuente, commits separados si conviene):**
- `opportunity_policy.py`: `enforce_bidder_opportunity_policy` → delega a
  `apply_opportunity_invariant`. `_CLOSED_STATES`/`_BIDDER_PHRASES` borradas,
  re-exportadas desde `contract_invariants` si algún import externo las usa.
- `llm_handler.py`: `_INVALID_CONTRACT_STATES` → importar `INVALID_CONTRACT_STATES`
  de `contract_invariants`. Las 3 llamadas a `enforce_bidder_opportunity_policy`
  se mantienen (ahora delegan).
- `intent_vocabulary.py:68-74`: la regex de firmado se mantiene como *detector*
  (parser-level), pero la **consecuencia** (dataset+purga estado) deja de estar
  inline y pasa por la invariante. Documentar con comentario que la verdad vive
  en `contract_invariants`.
- `soql_builder.py:25`: `_SUBSTANTIAL_FILTER_KEYS` → **eliminar** y consumir
  `SCOPE_TOPIC_KEYS` de `_followup_constants.py` (la fuente única H9 que ya usan
  execution/analytics/followup/refinement). NO crear lista nueva en
  `contract_invariants`; `has_substantial_filter` importa de `_followup_constants`.
  Si las dos listas hoy difieren en claves, **reconciliar conservador**: la unión
  documentada, verificando que ningún test de `UnsafeGlobalQueryError` se rompa.
  Mantener los dos guards (builder + execution) como defensa-en-profundidad.

**Criterio de aceptación:**
- `grep -rn "_INVALID_CONTRACT_STATES\|_CLOSED_STATES\|_BIDDER_PHRASES\|_SUBSTANTIAL_FILTER_KEYS" app/` →
  solo aparece en `contract_invariants.py` (o como re-export documentado).
- **Suite completa verde:** `pytest -q` (baseline: 1212 passed, 23 xfailed).
- `python scripts/run_query_corpus.py --mode all` → 89/89 PASS (sin regresión).
- `python scripts/eval_rated.py` → no baja respecto al baseline previo.

### Etapa 4 — Guardrails + observabilidad
**Objetivo:** dejar las invariantes auditable y protegidas contra regresión futura.

**Entregables:**
- Los `risk_flags` de P10–P13 se propagan al trace (verificar que
  `query_plan` → `state["query_plan"]["risk_flags"]` ya fluye; si no, conectar).
- `tests/test_invariants_lock.py` — test de lock-in que falla si alguien
  reintroduce una lista de estados inválidos fuera de `contract_invariants`
  (grep-test estilo los architectural-lock-in existentes).
- Actualizar `docs/decisions_log.md` con ADR-024 marcado `Accepted` y nota de
  cierre.

**Criterio de aceptación:** suite verde; el lock-test falla si se duplica una lista
(verificarlo introduciendo una duplicación temporal y viendo el rojo, luego
revertir).

---

## 4. Guardrails para el ejecutor (qué NO hacer)

1. **No cambiar comportamiento observable.** Si `run_query` devuelve algo distinto
   para una query que antes funcionaba, es regresión. La Etapa 0 es tu red.
2. **No meter I/O en `query_plan` ni en `contract_invariants`.** Nada de SECOP, DB,
   red, `datetime.now()` salvo el `today` ya inyectado por contexto.
3. **No tocar ranking/relevance** (`relevance.py`, BM25, fetch limit). Es otra fuga,
   otro plan. Fuera de alcance.
4. **No tocar el bug de `unaccent` en ciudad** (`soql_builder.py:230`). Va en el
   plan de fugas (B1), no aquí. No mezclar.
5. **Respetar G8:** `query_plan.py` debe quedar < 300 LOC. Por eso las invariantes
   van en módulo aparte.
6. **Preservar quirks documentados** (p.ej. el de suggestion-selection, los
   `# DOGFOOD-*` en estado_families). No "limpiar" comentarios que codifican
   decisiones verificadas contra SECOP real.
7. **Commits atómicos por etapa, suite verde antes de avanzar.** Si una etapa
   rompe algo inesperado, detente y reporta el diff del fallo; no acumules parches.
8. **El detector y la consecuencia son cosas distintas.** Los *detectores* de
   tokens (regex en `intent_vocabulary`, scan en `query_router`) pueden quedarse
   donde están; lo que se consolida es la *consecuencia jurídica* (qué params
   resultan). No muevas detección de parsing al plan.

---

## 5. Comandos de verificación (correr en cada etapa)

```bash
# unidad de la etapa
pytest tests/test_contract_invariants.py tests/test_query_plan.py \
       tests/test_invariants_characterization.py -q
# no-regresión global
pytest -q                                   # baseline 1212 passed, 23 xfailed
python scripts/run_query_corpus.py --mode all   # 89/89 PASS
python scripts/eval_rated.py                 # no bajar precisión parser-tier
make lint-core                               # limpio
wc -l app/core/query_plan.py                 # < 300 (G8)
# fuente única (Etapa 3)
grep -rn "_INVALID_CONTRACT_STATES\|_CLOSED_STATES\|_BIDDER_PHRASES\|_SUBSTANTIAL_FILTER_KEYS" app/
```

---

## 6. Definición de "hecho"

- [ ] Las 6 invariantes del inventario tienen **una sola definición** en
      `contract_invariants.py` (salvo INV-6, cuya fuente única es
      `_followup_constants.SCOPE_TOPIC_KEYS`), cada una con test que la afirma.
- [ ] `compute_query_plan` aplica P10–P12 activo y **P13 pasivo** desde la fuente única.
- [ ] `test_firmados_en_ejecucion_preserves_active_states` verde (caso crítico).
- [ ] P11 idempotente (test aplicar-2× = 1×); P4 intacto.
- [ ] `grep` confirma cero listas de estados/frases duplicadas fuera del módulo.
- [ ] Suite completa, corpus `--mode all` y `eval_rated` sin regresión.
- [ ] `query_plan.py` < 300 LOC.
- [ ] Lock-test impide reintroducir duplicación.
- [ ] (Opcional) Subetapa 3.5: P13 activo SOLO si la respuesta final queda equivalente.
- [ ] ADR-024 marcado `Accepted` en `decisions_log.md`.

**Lo que este plan deliberadamente NO hace:** subir precisión (eso es el plan de
fugas A/B), tocar ranking, añadir intents nuevos, ni introducir LLM. Es
puramente consolidación de invariantes para frenar la clase de bug "firmado→
Cerrado" y sus primos.

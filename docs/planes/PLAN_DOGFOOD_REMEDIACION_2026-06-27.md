# Plan de remediación y mejora dogfood — 2026-06-27 (v2.1)

> **Para:** Codex (ejecutor).
> **Origen:** corrida `dogfood_20260627` (48/48, 0 crashes) + 3 rondas de asesor.
> **Evaluador:** `scripts/eval_dogfood.py <jsonl>`. **Evidencia:**
> `artifacts/dogfood/dogfood_dogfood_20260627.{jsonl,json}`.
> **v2:** incorpora la 2ª ronda del asesor — mutaciones tipadas, auditoría de
> recall, resolución tipada geo/entidad, snapshots, instrumentación HTTP, SLO
> separado, paginación sobre buffer canónico, y Sprints B (producto) / C (validación).

## TL;DR

- **Robustez:** alta (48/48). **Calidad funcional:** ~70-75%. El cerebro entiende;
  **las manos fallan**. NO tocar el extractor LLM-first (descartado en A/B).
- El plan dejó de perseguir síntomas: organiza por **causa raíz**, con commits
  verificables, invariantes, y separación tests deterministas / SECOP vivo.
- **El plan es de 3 sprints**, no uno: A (integridad de ejecución) → B (calidad del
  resultado, lo que el usuario percibe) → C (validación no sobreajustada).

### Conteo formal de causas (post Etapa 0 — ya medido, no estimado)

```
Contaminación de topic (control):   5 casos   → Sprint A Etapa 2
Lookup directo (entidad/qualifier): 2 casos   → Sprint A Etapa 1
Geo/entidad (resolución tipada):    1 caso    → Sprint A Etapa 7
Recall/ejecución live (params OK):  2 casos   → Sprint A Etapa 3 (auditoría)
UX/latencia/paginación:             medidos aparte (instrumentación + p90)
Calidad de producto (comercial/                → Sprint B
  submercados/vigencia):            3 clases percibidas por el usuario final
```

Ya **no** se sostiene "12-15 fallas explicadas por contaminación": la Etapa 0
descompuso el problema y dos casos resultaron tener **params correctos** (fallo de
ejecución, no de parsing).

---

## Principios de ejecución (obligatorios)

1. **TDD por fixture.** Cada bug es regresión permanente antes de tocar producción
   (Etapa 0 ✅). `xfail(strict=True)`: un XPASS rompe CI y obliga a retirar la marca.
2. **No monolitos** (política del repo): módulo pequeño de una responsabilidad +
   enganche mínimo. Prohibido apilar funciones en `followup_engine.py` /
   `normal_flow.py`.
3. **La garantía estructural vence al léxico.** Donde haya elección entre "reconocer
   más palabras" y "restringir qué puede cambiar", elegir lo segundo (ver Etapa 2).
4. **Asertar sustancia, no etiquetas.** Tests sobre params/SoQL/WHERE, no sobre el
   nombre del intent (que el implementador puede renombrar).
5. **Medir antes de optimizar.** Instrumentar (Etapa 5) antes de tocar timeouts
   (Etapa 6). Y antes de culpar a la latencia, descartar recall (Etapa 3).
6. **Una sola fuente de verdad de estado.** No crear copias paralelas de params
   (`QueryFrame` manda; lo demás son snapshots de solo lectura — ver Etapa 4).
7. **Una etapa = un commit = un entregable verificable.** No avanzar sin verde.
8. **Git safety:** rama dedicada; no `stash`/`reset` fuera del worktree.

---

## Invariantes (codificadas como tests; fallan automático)

```
INV-1  change_order:    WHERE(antes) == WHERE(después). Solo cambia ORDER BY.
INV-2  change_dataset:  objeto NUNCA recibe tokens de control. Solo dataset (+limpia
                        framing incompatible: fechas, columnas de estado por dataset).
INV-3  change_scope:    cambia scope; conserva topic. Tokens de scope no entran a objeto.
INV-4  remove_filter:   elimina el filtro nombrado (valor/fecha/estado). No añade el
                        verbo de borrado a objeto.
INV-5  direct_lookup:   ref + qualifier ⇒ gana el candidato cuya entidad/ciudad/depto
                        coincide normalizado. Nunca devolver entidades ajenas como exactas.
INV-6  pagination:      fingerprint(página N)==fingerprint(N+1); offset opera sobre el
                        BUFFER rankeado, no sobre OFFSET de Socrata. total estable.
INV-7  typed_resolution: "Gobernación de X" (buyer_entity) y "departamento de X"
                        (territorial_department) resuelven a CAMPOS distintos.
INV-8  intent_gated:    el delta de un follow-up solo contiene campos de
                        ALLOWED_MUTATIONS[intent]; lo demás se descarta y se traza.
```

Métrica estrella del arco: **WHERE-invariance violations = 0** y
**intent-gated rejected-fields trazados**.

---

# SPRINT A — Integridad de ejecución

### Etapa 0 — Congelar evidencia ✅ COMPLETA

```
Estado: 1 passed · 8 xfailed(strict) · 2 live-monitor (skip)
```

- `tests/fixtures/dogfood_followup_regressions.json` (10 casos, prev_params reales).
- `tests/test_dogfood_regressions.py` (harness offline: `QueryRouter.parse` +
  `detect_and_merge`, sin red).
- Asertan **sustancia** (objeto/params), no intents. Recall vivo → `skip`
  (se mide con `run_dogfood.py` + `eval_dogfood.py`).

---

### Etapa 1 — Lookup directo robusto (exacto + qualifier, NO dependiente del resolver)

**Causa raíz #2 (2 casos).** La Etapa 0 probó que `scan_entity` resuelve "La
Estrella" pero **falla con "Leticia"** → el lookup NO puede depender del gazetteer
global para garantizar los 2 casos conocidos.

**Flujo recomendado (resiliente a huecos del gazetteer):**
```
1. Extraer referencia (UID/URL/entity_ref).
2. Buscar por referencia (exacta → tolerante: guiones/espacios/ceros).
3. Traer pool pequeño de candidatos (10-20), ambos datasets.
4. Detectar el qualifier crudo restante del query ("Alcaldía de Leticia", "La Estrella").
5. Comparar qualifier NORMALIZADO SOLO contra nombre_entidad/ciudad/departamento/
   aliases del candidato (reusar normalize_retrieval_text). NUNCA contra
   descripción/objeto: "La Estrella" puede aparecer incidentalmente en el texto
   contractual y daría un falso positivo.
6. Filtrar/rankear determinísticamente: el candidato cuyo nombre_entidad contiene
   "ALCALDIA MUNICIPIO DE LETICIA" gana aunque scan_entity no lo haya resuelto.
7. Antes de declarar ambigüedad (N3): DEDUPLICAR por UID/identificador canónico — una
   misma oportunidad puede venir como varias filas/versiones; eso NO son varias entidades.
```

**Modelo de datos (no tuplas sueltas):**
```python
@dataclass(frozen=True)
class DirectLookupRequest:
    reference_raw: str
    reference_normalized: str
    entity_qualifier_raw: str | None
    uid: str | None
    url: str | None
    match_mode: str   # "exact_reference_entity" | "exact_reference" | "fuzzy" | ...
```
Cada resultado declara su procedencia (alimenta "por qué apareció"):
```json
{"match_type":"exact_reference_entity","reference_match":"exact",
 "entity_match":"normalized_alias","confidence":"high"}
```

**Niveles:** N1 UID/URL (existe) · N2 ref+qualifier (nuevo, el core) · N3 ref sola
con >1 entidad → **ambigüedad explícita** · N4 tolerante etiquetada como aproximada.

**Archivos:** `app/core/direct_lookup.py`, `app/core/workflow/handlers/direct_lookup.py`
(`_handle_entity_ref:86`), `app/core/entity_resolver.py`,
`tests/test_direct_lookup_entity_scope.py` (nuevo).

**Checklist:**
- [ ] `DirectLookupRequest` con `entity_qualifier_raw`.
- [ ] Comparación normalizada qualifier↔candidato SOLO en campos de entidad (no descripción/objeto).
- [ ] Dedup por UID antes de N3; N3 ambigüedad dedicada; N4 etiquetada aproximada.
- [ ] `match_type`/`confidence` en cada resultado.
- [ ] Retirar `xfail` de `direct_lookup_leticia_lp005` y `direct_lookup_la_estrella_samc001`.

**Aceptación:** los 2 casos conocidos → (a) UID esperado, (b) ausencia honesta, o
(c) ambigüedad específica. **Devolver entidades ajenas como exactas = fail.**

**Verificación:** `pytest tests/test_direct_lookup*.py tests/test_dogfood_regressions.py -q`

---

### Etapa 2 — Mutaciones conversacionales tipadas *(P0 SISTÉMICO — sprint principal)*

**Causa raíz #1 (5 casos).** `DOGFOOD-FOLLOWUP-EXECUTION-001`.

**La garantía principal NO es limpiar palabras** (eso sería un STOPWORDS más elegante,
condenado a perseguir conjugaciones: ordenar/regresar/volver/mostrar/dejar/quitar/
cambiar/poner/filtrar…). La garantía es **intent-gated mutation**:

> Cada intención conversacional solo puede modificar un conjunto permitido de campos.

**MutationPolicy distingue modificar / borrar / limpiar-por-transición** (no basta un
set plano: "vuelve a contratos" debe *limpiar* columnas de estado del dataset viejo,
que el usuario no pidió y no están en `allowed_set`):
```python
@dataclass(frozen=True)
class MutationPolicy:
    allowed_set: frozenset[str]              # lo que el usuario puede fijar
    allowed_clear: frozenset[str] = frozenset()   # lo que el sistema limpia por coherencia
    transition_cleanup: str | None = None    # normalizador de transición (p.ej. cambio dataset)

MUTATION_POLICIES = {
  "change_order":   MutationPolicy({"ordering_signal","order_by"}),
  "change_dataset": MutationPolicy(
        {"dataset","dataset_explicit"},
        allowed_clear={"estado_del_procedimiento","estado_de_apertura_del_proceso",
                       "estado_contrato","fecha_desde","fecha_hasta","offset","_display_offset"},
        transition_cleanup="normalize_dataset_transition"),
  "change_scope":   MutationPolicy({"entidad","entidad_resolved","departamento",
                       "departamento_resolved","ciudad","ciudad_resolved","region"}),
  "refine_filter":  MutationPolicy({"valor_min","valor_max","fecha_desde","fecha_hasta",
                       "estado_family","estado","modalidad"}),
  "remove_filter":  MutationPolicy(frozenset(),
                       allowed_clear={"valor_min","valor_max","fecha_desde","fecha_hasta","estado_family"}),
  # Topic: tres intenciones distintas, no una bolsa
  "replace_topic":  MutationPolicy({"objeto","objeto_or","objeto_phrases"}),
  "refine_topic":   MutationPolicy({"objeto","objeto_or","objeto_phrases","objeto_exclude","topic_facets"}),
  "exclude_topic":  MutationPolicy({"objeto_exclude"}),
  "contextual_requery": MutationPolicy({"objeto","objeto_or","objeto_phrases"}),
}
```
**Exclusiones son campo propio** (`objeto_exclude`), NO tokens normales ni diferidas al
Sprint B — "excluye mantenimiento" y "excluye prestación de servicios profesionales"
fueron casos reales del dogfood (dieron 0):
```json
{"objeto":["aire acondicionado"],"objeto_exclude":["mantenimiento"]}
```

**Pipeline del merge:**
```
1. Clasificar intención.
2. Calcular delta (params del turno actual vs frame previo).
3. PODAR: conservar solo campos ∈ allowed_set; aplicar allowed_clear; correr transition_cleanup.
4. Aplicar el delta podado sobre el QueryFrame previo.
5. Validar invariantes (INV-1..4, INV-8).
```
Así "ordénalos por mayor valor" puede parsear `{objeto:["ordena"], ordering_signal:
"valor_desc"}`, pero como `change_order` no tiene permiso sobre `objeto`, ese campo
**se descarta**.

**Defensa DOBLE — el gate no basta si el clasificador yerra hacia `contextual_requery`**
(que SÍ puede tocar objeto). Casco además del cinturón: cuando el intent permite mutar
topic, exigir evidencia temática real antes de aceptarla.
```python
if policy.allows_topic_mutation():
    if is_control_only_turn(query, parsed_delta):   # orden reconocible + 0 sustantivos nuevos de dominio
        reject_topic_mutation(reason="control_only_without_new_domain_topic")
```
`is_control_only_turn` evalúa señales (¿hay orden reconocible? ¿sustantivos de
contratación nuevos? ¿el topic nuevo difiere de verbos/pronombres/operadores?
¿confianza temática suficiente?), NO una lista infinita de stopwords.
`app/core/followup_control_tokens.py` apoya al clasificador y a este chequeo, pero la
garantía principal sigue siendo el gate tipado.

**Trace (separa pedido / aceptado / limpiado-por-transición / rechazado):**
```json
{"followup_mutation":{"intent":"change_dataset",
  "requested_fields":["dataset","objeto"],
  "accepted_fields":["dataset"],
  "transition_cleared_fields":["estado_de_apertura_del_proceso","fecha_desde"],
  "rejected_fields":["objeto"]}}
```

**Archivos:** `app/core/followup_engine.py` (gate en `detect_and_merge`/`FollowupMerger`),
`app/core/followup_mutations.py` (nuevo: `MutationPolicy` + `MUTATION_POLICIES` +
`gate_delta` + `is_control_only_turn`), `app/core/followup_control_tokens.py` (nuevo,
secundario), `app/core/query_frame.py`, `tests/test_followup_mutations.py` (nuevo),
`tests/test_frame_leak_canary.py`.

**Checklist:**
- [ ] `MutationPolicy(allowed_set/allowed_clear/transition_cleanup)` + `gate_delta` en módulo dedicado.
- [ ] `objeto_exclude` como campo propio (parser + builder lo consumen; "excluye X").
- [ ] Intenciones de topic separadas: replace/refine/exclude/contextual_requery.
- [ ] Defensa doble: `is_control_only_turn` bloquea mutación de topic aunque el intent la permita.
- [ ] El merge calcula delta → poda (set) → limpia (clear) → cleanup transición → aplica.
- [ ] `followup_mutation` en el trace (requested/accepted/transition_cleared/rejected).
- [ ] Los 5 casos de contaminación pasan; INV-1..4 e INV-8 verdes.
- [ ] Retirar `xfail` de los 5 `control_token_*`.

**Aceptación:** 0 control-token leaks; `rejected_fields` poblado donde el parser
contaminó; canary de persistencia verde.

**Verificación:** `pytest tests/test_followup*.py tests/test_frame_leak_canary.py tests/test_dogfood_regressions.py -q`

---

### Etapa 3 — Auditoría diferencial de recall *(NUEVA — el mayor hueco del plan v1)*

**Causa raíz #4 (2 casos con params correctos que devuelven 0).** Reducir el timeout
NO arregla un cero de recall. Un refinamiento vacío con params correctos puede ser:
campo territorial equivocado · valor en otro campo · predicado muy estricto · estado
heredado incompatible · AND imposible · dataset incorrecto · ciudad-de-entidad vs
lugar-de-ejecución · dato ausente en Socrata · timeout convertido en 0.

**Mecanismo: predicate waterfall, por EXISTENCIA primero (no `COUNT(*)`).** El conteo es
sospechoso de la latencia (Etapa 5); correr un count por predicado convertiría el modo
diagnóstico en una fábrica de timeouts. Dos niveles:
- **Rápido (default):** `SELECT campos_minimos WHERE ... LIMIT 1` → ¿existe ≥1 fila?
- **Profundo (opcional):** `SELECT count(*) WHERE ...` solo si se necesita la magnitud.

Para hallar el predicado asesino basta saber si el conjunto quedó vacío:
```json
{"predicate_waterfall":[
  {"predicate":"tema (mantenimiento, ascensores)","exists":true},
  {"predicate":"+ ciudad = Medellín","exists":true},
  {"predicate":"+ precio_base BETWEEN 100M..150M","exists":false,"killer_predicate":true}]}
```
**Solo se ejecuta para:** 0-resultados tras refinamiento, casos marcados en dogfood, o
modo debug. NO para consultas normales (coste de red).

**Archivos:** `scripts/eval_dogfood.py` (consumir/mostrar waterfall), nuevo
`app/core/recall_audit.py` (`build_predicate_waterfall(params, deps)` — solo debug),
enganche en el path de zero-results (Etapa 4).

**Checklist:**
- [ ] `build_predicate_waterfall` ejecuta EXISTENCIA (`LIMIT 1`) incremental por predicado; count solo opcional.
- [ ] Se dispara solo en zero-after-refinement / debug (nunca en consulta normal).
- [ ] `eval_dogfood.py` reporta el `killer_predicate` por caso.
- [ ] Diagnóstico escrito de "solo en Medellín" y "entre 100 y 150 millones":
      ¿campo, valor, AND, o dato ausente? → arreglo dirigido (no a ciegas).

**Aceptación:** para los 2 casos live se identifica el predicado exacto que mata el
recall y se documenta la corrección (o se confirma "dato ausente en la fuente").

**Verificación:** `python scripts/run_dogfood.py --scenario ascensores_scope_value ... && python scripts/eval_dogfood.py <jsonl>`

---

### Etapa 4 — Exit reasons + rollback por snapshots

**Causa raíz: salidas mal etiquetadas.** `zero_results_after_refinement` se reporta
como `needs_clarification` (las "11 clarificaciones" eran vacíos disfrazados).

**Modelo BIDIMENSIONAL** (una sola razón no representa parciales: fetch OK + count
timeout no es ni `success` ni `timeout`):
```python
class Outcome:        SUCCESS, PARTIAL, EMPTY, CLARIFICATION, ERROR
class OutcomeReason:  RESULTS_FOUND, ZERO_RESULTS, ZERO_AFTER_REFINEMENT,
                      COUNT_TIMEOUT, FETCH_TIMEOUT, SOURCE_UNAVAILABLE, AMBIGUOUS_REFERENCE
```
```json
{"outcome":"partial","outcome_reason":"count_timeout",
 "rows_returned":25,"total_count":null,"total_count_approximate":true}
```

**Estado: snapshots, NO una copia paralela de params.** `previous_successful_params`
crearía una 2ª fuente de verdad que diverge del `QueryFrame`. En su lugar:
```python
@dataclass(frozen=True)
class ExecutionSnapshot:
    frame: FrozenQueryFrame      # inmutable de verdad: deep-freeze o frame_id a versión inmutable
    result_count: int
    outcome: str
    outcome_reason: str
    query_fingerprint: str
    created_at_utc: datetime     # fecha persistible (NO perf_counter)
```
Duración aparte, con reloj monotónico: `dur_ms = (perf_counter() - started) * 1000`.
**Congelar el dataclass no basta** si `QueryFrame` lleva listas/dicts mutables: el
snapshot debe guardar una versión inmutable / copia profunda / `frame_id`, o un frame
futuro mutaría el "histórico". `last_successful_snapshot` es una **referencia** al
último con resultados; `failed_refinement_delta` se **calcula** como diff
`last_successful_snapshot.frame` ↔ `current_attempt.frame` (no se guarda a mano).
Habilita "deshacer el último filtro" / "volver a la búsqueda anterior" sin 2º sistema
de estado.

**UX zero-after-refinement:** "No encontré nada al aplicar «X». La búsqueda anterior sí
tenía. ¿Deshacer ese filtro o ampliar?" + opciones.

**Archivos:** `app/core/conversation_store.py` (snapshots), `normal_flow.py`,
`refinement.py`, `eval_dogfood.py` (leer `exit_reason`).

**Checklist:**
- [ ] `outcome` + `outcome_reason` (bidimensional) en el resultado; parciales bien representados.
- [ ] `ExecutionSnapshot` con frame INMUTABLE (deep-freeze/frame_id) y `created_at_utc` datetime.
- [ ] `last_successful_snapshot` referencia; `failed_refinement_delta` calculado por diff de frames.
- [ ] UX deshacer/ampliar; `eval_dogfood` deja de contar falsas clarificaciones.

**Aceptación:** `false_clarifications ≈ 0`; "deshacer" funciona vía snapshot.

**Verificación:** `pytest -k "exit_reason or snapshot or rollback or clarif" -q`

---

### Etapa 5 — Instrumentación temporal (hasta el cliente HTTP)

**Medir alrededor de `execute_resolved_params` no basta** — diría "fetch tardó 60s"
pero no por qué. Bajar la telemetría al cliente HTTP.

```json
{"http_metrics":{"attempt_count":2,"connect_ms":40,"read_ms":30004,
  "retry_delay_ms":1000,"status_codes":[],"timeout_stage":"read",
  "dataset":"p6dx-8zbt","operation":"fetch","rows_requested":200,
  "rows_received":0,"cache_hit":false,"degraded":true,"partial":false}}
```
Etapas de `timings_ms`: `parse, apply_context, resolve_entities, build_query,
fetch_results, count, observe, rank, render, total`. **Reloj monotónico
`time.perf_counter()`** para duraciones (nunca `datetime.now()`).

**Archivos:** `app/core/secop_client.py` (http_metrics), `workflow/helpers.py`
(`execute_resolved_params`, `_query_first_then_count`), `normal_flow.py` (propagar),
`eval_dogfood.py` (p50/p90 **por etapa** + detección de mesetas 15/30/45/60s = retries).

**Checklist:**
- [ ] `http_metrics` con attempt_count/connect/read/retry/status/timeout_stage.
- [ ] `timings_ms` granular con `perf_counter`.
- [ ] `eval_dogfood` atribuye el minuto a una etapa concreta + mesetas.
- [ ] Re-correr 48 turnos → identificar etapa dominante (sospecha: count/observe tras
      fetch, o retry de 60s).

**Aceptación:** reporte que atribuye la latencia a una etapa/operación HTTP concreta.
**Sin esto NO se ejecuta la Etapa 6.**

**Verificación:** corrida completa + `eval_dogfood.py <jsonl>`.

---

### Etapa 6 — Política de latencia (SLO separado, no gate rígido sobre SECOP)

p90<15s es buen **objetivo de experiencia**, pero como gate rígido de CI es inestable
(Socrata es externo). Separar tres SLO:
```
Pipeline propio (sin red):     p95 < 500 ms      ← gate duro (controlable)
Fuente respondió normal:        p90 total < 10-15 s
Fuente degradada:               respuesta parcial/error honesto < 15 s
```
Meta real: **SECOPPAL nunca deja al usuario bloqueado más allá del presupuesto sin
responder honestamente** (no "SECOP siempre responde en 15s").

**Estrategia (según Etapa 5):** primeros resultados primero; cortar `count`/`observe`
si exceden presupuesto → total aproximado; eliminar retries que no aportan; marcar
`partial`; cache solo donde la evidencia muestre repetición.

**Archivos:** `workflow/helpers.py`, `app/config.py` (presupuestos), `secop_client.py`.

**Checklist:**
- [ ] Presupuesto configurable; count/observe se cortan al excederlo (respuesta parcial honesta).
- [ ] Retries inútiles eliminados (según mesetas de Etapa 5).
- [ ] SLO pipeline-propio como gate duro; total/degradado como objetivo.

**Aceptación:** p95 pipeline-propio <500ms (gate); p90 total <15s cuando la fuente
responde; recall conocido no empeora.

**Verificación:** corrida + `eval_dogfood.py` (comparar SLO vs base).

---

### Etapa 7 — Geo vs entidad: resolución TIPADA

**Es un problema de tipos, no de SoQL.** La Etapa 0 probó: "Gobernación del Atlántico"
→ `entidad_resolved='DEPARTAMENTO DEL ATLANTICO**'` — el resolver **colapsa territorio
y comprador**. Limpiar el departamento en `_merge_change_scope` no basta.

La resolución debe conservar el **tipo semántico**:
```
"departamento de X"  → {value:"Atlántico", kind:"territorial_department"} → departamento=…
"Gobernación de X"   → {value:"GOBERNACION DE …", kind:"buyer_entity"}     → nombre_entidad=…
```
Prioridad sintáctica por patrón: `en el departamento de X` / `de la Gobernación de X` /
`contratos en X` (territorio) / `contratos de X` (puede ser comprador).

**Archivos:** `app/core/entity_resolver.py` (kind tipado), esquema del gazetteer
(`app/data/aliases_db.json` / `docs/gazetteer`), `app/core/query_router.py` (patrones),
`followup_engine.py` (`_merge_change_scope` respeta el kind), `soql_builder.py`.

**Checklist:**
- [ ] La resolución emite `kind` (territorial_department vs buyer_entity).
- [ ] Patrones sintácticos mapean a campos distintos.
- [ ] Test asercta el **SoQL** (no el conteo) para los dos turnos del caso Atlántico.
- [ ] Retirar `xfail` de `geo_vs_entity_atlantico_gobernacion`.

**Aceptación:** "departamento de X" y "Gobernación de X" generan WHERE distintos (INV-7).

**Verificación:** `pytest tests/test_followup_engine.py -k "geo or entity or scope" tests/test_dogfood_regressions.py -q`

---

### Etapa 8 — Paginación sobre buffer canónico

Fingerprint es necesario pero **no suficiente**: el OFFSET de Socrata ≠ offset de los
resultados validados/rankeados (validación elimina filas, ranking reordena → páginas de
24, duplicados, totales que saltan).

**Modelo correcto — paginar sobre el buffer, no sobre Socrata:**
```
candidate_buffer · validated_ranked_buffer · display_cursor · source_cursor · query_fingerprint
1. Traer 200 candidatos.  2. Validar+rankear.  3. Guardar buffer.
4. Mostrar 1-25.  5. "más" → 26-50 del MISMO buffer.
6. Al agotar buffer → traer siguiente lote fuente (source_cursor), validar, fusionar, deduplicar.
```
**Orden fuente estable** (evita empates no deterministas):
`ORDER BY fecha DESC, precio DESC, id_del_proceso DESC` (el id final rompe empates).
**Fingerprint estable entre procesos:** `sha256(canonical_json(...)).hexdigest()`, NO
`hash()` (el hash de Python varía por `PYTHONHASHSEED`). Incluye dataset · filtros
normalizados · orden · versión de PhraseSpec/nodo semántico · config de ranking ·
versión del validador.
**Buffer:** limitado, con TTL, deduplicado por identificador estable, cursores
separados (display/source), sin guardar payloads enormes indefinidamente.
**Diferenciar los tres totales** (hoy se llama "total" a universos distintos → confusión):
```json
{"source_candidate_total":2272,"validated_total_known":194,"displayed_so_far":50}
```

**Archivos:** `app/core/workflow/handlers/pagination.py` (`handle_pagination:75`,
`_DISPLAY_OFFSET_KEY`), `conversation_store.py`, `soql_builder.py` (orden estable),
`tests/test_*pagination*.py`.

**Checklist:**
- [ ] Buffer canónico (candidate/validated_ranked) + display_cursor/source_cursor, con TTL y dedup por id.
- [ ] "más" pagina sobre buffer rankeado; refetch solo al agotarlo (dedup al fusionar).
- [ ] Orden fuente con desempate por id; los 3 totales separados (source/validated/displayed).
- [ ] Fingerprint = `sha256(canonical_json)` (no `hash()`); versiona PhraseSpec/ranking/validador.

**Aceptación:** "buscar → más → más" mantiene total y fingerprint; páginas completas de
25; sin re-consulta del universo; sin duplicados.

**Verificación:** `pytest tests/test_*pagination*.py -q`

---

# SPRINT B — Calidad del resultado (lo que el usuario percibe)

> El Sprint A arregla la infraestructura conversacional. El Sprint B arregla los tres
> comportamientos que el usuario final percibe directamente y que el dogfood expuso.

### B1 — Vigencia accionable (no "estado crudo")
"oportunidades ahora mismo" devolvió registros antiguos/dudosos. Distinguir:
```json
{"opportunity_stage":"receiving_offers","actionability":"confirmed",
 "reasons":["estado publicado","fecha de cierre futura"]}
{"opportunity_stage":"apparently_open","actionability":"unconfirmed",
 "reasons":["campo abierto","sin fecha de cierre verificable","publicación antigua"]}
```
Campos: `state_raw, opportunity_stage, actionability, actionability_confidence,
actionability_reasons`. (Ya existe `opportunity_stage` shadow en normal_flow:240 —
promover de shadow a accionable.)

### B2 — Ranking por intención comercial
"quiero suministrar aires acondicionados" puso mantenimiento sobre suministro.
**Separar la intención del usuario del perfil del candidato** (no inyectar la intención
dentro de la clasificación documental):
```
RequestedCommercialIntent   vs   CandidateActionProfile   → compatibilidad
{primary:["suministro","instalacion"],          candidato suministro+instalación → alto
 compatible:["adquisicion"],                     candidato mantenimiento+repuestos → medio
 negative:["mantenimiento_puro","interventoria"]} candidato mantenimiento puro     → bajo
                                                  candidato interventoría          → muy bajo
```
Acciones: suministro/adquisición/instalación/mantenimiento/alquiler/interventoría/repuestos.

### B3 — Submercados semánticos (primer nodo real: PAE)
Un término de dominio ≠ un solo mercado. No son aliases: son **facetas**.
```
PAE → operación · suministro · interventoría · cofinanciación · infraestructura
Adulto mayor → atención integral · centros de vida · centros de bienestar ·
               alimentación · infraestructura · arrendamientos · servicios profesionales
```
Empezar por **PAE** (un nodo), luego facetas de adulto mayor. Sprint posterior a la
estabilización, NO parches en aliases.

### B4 — "Por qué apareció este resultado"
Coincidencia directa con X en el objeto / match_type del lookup (alimentado por
Etapa 1 `match_type` y Etapa 2 `followup_mutation`).

### B5 — Verificación de discrepancias SECOP nativo ↔ Socrata
Para referencias conocidas, comparar lo que Socrata sirve vs el portal SECOP (detecta
lag de fuente, causa raíz #D del diagnóstico original).

---

# SPRINT C — Validación no sobreajustada

> Regla de oro: **no reutilizar solo los 48 turnos**. Arreglarlos y validar contra
> ellos es circular.

- [ ] **Batería ciega nueva** (no diseñada para los fixes), además de los 48.
- [ ] **≥20 referencias conocidas** (hoy solo 2) para recall real de lookup.
- [ ] **Métricas por escenario**, no solo por turno (un escenario de 8 turnos con 1
      fallo no es "87% OK").
- [ ] **Precision@10/@25** para ranking; **recall** de resultados conocidos.
- [ ] **SLO separado** pipeline / upstream / total (de Etapa 6).
- [ ] **Repetir la batería en distintos momentos del día** (Socrata varía con carga;
      el timeout de 2025 fue transitorio, no estructural).

---

## Métricas de la 2ª corrida (definición de "listo" para Sprint A)

```
Execution success:        48/48
Functional pass:          X/48        (objetivo ≥ 90%)
Known-reference recall:   X/2 (luego X/20 en Sprint C)
Control-token leaks:      0
WHERE invariance viol.:   0
Intent-gated rejections:  trazadas (requested/accepted/rejected)
False clarifications:     0
Zero-after-refinement:    diagnosticados por waterfall (no "clarif")
p95 pipeline-propio:      < 500 ms    (gate duro)
p90 total (fuente OK):    < 15 s
>15 s:                     ≤ 3/48
```

---

## Orden de ejecución (Sprint A)

```
0. Evidencia ✅ → 1. Lookup robusto → 2. Mutaciones tipadas (sprint) →
3. Auditoría de recall → 4. Exit reasons + snapshots → 5. Instrumentar (HTTP) →
6. Latencia (SLO separado) → 7. Geo/entidad tipada → 8. Paginación sobre buffer
```

> Cambios v2 (2ª ronda): Etapa 0 cerrada; Etapa 2 de "control tokens" a **mutaciones
> tipadas**; **nueva Etapa 3** de auditoría de recall ANTES de latencia; lookup
> **independiente del resolver**; geo/entidad **tipada**; **snapshots**; instrumentación
> **HTTP**; **SLO separado**; paginación sobre **buffer canónico**; + Sprints B y C.
>
> Cambios v2.1 (3ª ronda): `MutationPolicy` con **set/clear/transition_cleanup** (no set
> plano — "vuelve a contratos" debe limpiar estado del dataset viejo); intenciones de
> topic separadas + **`objeto_exclude`** campo propio; **defensa doble**
> `is_control_only_turn` (el gate no salva si el clasificador yerra a contextual_requery);
> waterfall por **existencia (LIMIT 1)**, no count; **outcome+outcome_reason
> bidimensional** (parciales); snapshot **realmente inmutable** + `created_at_utc`
> (perf_counter solo para duración); lookup **dedup por UID** + qualifier solo en campos
> de entidad; fingerprint **`sha256(canonical_json)`** no `hash()`; **3 totales**
> diferenciados; B2 separa **RequestedCommercialIntent vs CandidateActionProfile**.

## Para aprobar Etapas 1 y 2 (revisión conjunta acordada con el asesor)
Revisar juntos: `followup_engine.py`, `query_frame.py`, los dos `direct_lookup.py`,
`entity_resolver.py`, y el fixture/test de la Etapa 0.

# M3C-CG — Auditoría de telemetría y compuerta de producto C→D

- Fecha: 2026-07-25
- Worktree: `/Users/rodrigoortiz/Documents/secoppal-modality`
- Branch: `search/modality-contract-v1`
- HEAD: `4096a2c6a5c2cc3703e1be8adfec8ceb9c876397` (`feat(modality): wire conversational modality mutations`)
- Alcance: **auditoría + instrumento**. No se implementa M3C-D. No hay commit/merge/rebase/push.

---

## 1. Pregunta contractual

> ¿La frecuencia REAL de aclaraciones bloqueadas de modalidad justifica construir
> el ciclo persistente `PendingModalityClarification`?

**Respuesta: no se puede responder afirmativamente — no existe muestra real.**
`PRODUCT_GATE_SAMPLE_SIZE = 0` contra un mínimo de 100. M3C-D **NO AUTORIZADO**.

---

## 2. Mapa de evidencia (verificado contra el código que ejecuta la rama)

`apply_context` (`app/core/workflow/actions/parsing.py:260-343`) es el único nodo
que produce evidencia de una operación conversacional de modalidad. La decisión
del motor puro viaja en `state["followup_mutation"]["modality"]`
(`decision.trace`, construido en `app/core/modality_followup.py:374`).

| evento | nodo productor | vive en | ¿se persiste? | ¿dónde? | ¿recuperable offline? | ¿sensible? |
| --- | --- | --- | --- | --- | --- | --- |
| `operation` (replace/add_include/…/clear) | `apply_context` | `followup_mutation.modality.operation` | **NO** | — | **NO** | no |
| `instruction_status` / `control_code` | `classify_control` | `followup_mutation.modality` | **NO** | — | **NO** | no |
| `reason` (`mutation_applied`, `remove_absent_target`, `clear_noop`…) | motor puro | `followup_mutation.modality.reason` | **NO** | — | **NO** | no |
| `no_op` | motor puro | `followup_mutation.modality.no_op` | **NO** | — | **NO** | no |
| `clarification_code` | motor puro | `decision.clarification_code` | **NO** (solo derivable del mensaje) | — | parcial | no |
| `query_plan.reason = "modality_<code>"` | `apply_context` | `state["query_plan"]` | **NO** (`query_plan` no entra al trace) | — | **NO** | no |
| `previous`/`current_targets`/`final` (procedure_ids) | motor puro | `followup_mutation.modality` | **NO** | — | — | **SÍ** |
| `needs_clarification` | `apply_context` | resultado | SÍ | `traces.needs_clarification`, `trace_json` | sí | no |
| `clarification_reason` (mensaje fijo del catálogo) | `modality_wiring.message_for` | resultado | SÍ | `traces.clarification_reason` | sí | no |
| `followup`, `intent_type`, `followup_intent_type` | `apply_context` | resultado | SÍ | `traces.*` | sí | no |
| `parsed_params.modalidad_*_procedure_ids` | merge final | resultado | SÍ | `traces.parsed_params_json`, `Turn.parsed_params` | sí | **SÍ** |
| `user_query` completo | entrada | resultado | SÍ | `traces.user_query`, `Turn.user_query` | sí | **SÍ** |

`FeedbackStore.log_trace` (`app/core/feedback.py:193-260`) arma el trace con una
lista **cerrada** de claves; `followup_mutation` no está entre ellas. `Turn`
(`app/core/conversation_store.py:66-118`) tampoco lo transporta. Ninguna ruta
(`observer.py`, `analytics/`, `instrumentation.py`, logging) emite modalidad:
`instrumentation.py` solo cubre spans HTTP y `parsing.py` solo loguea el
allowlist del LLM (línea 223).

### Probes ejecutados (§7) — almacenamiento aislado en `tmp`, SECOP mockeado

13 casos conducidos sobre `SecopalWorkflow.run_query` real (REPLACE, ADD_INCLUDE,
ADD_EXCLUDE, REMOVE_MEMBER no-op, CLEAR, `control_conflict`,
`ambiguous_clear_or_null_value`, AMBIGUOUS, PARTIAL, NOT_SEARCHABLE,
DATASET_INAPPLICABLE, follow-up sin modalidad, consulta inicial con modalidad).
Resultado uniforme:

- `followup_mutation` presente en el **resultado en memoria** en los 12 casos con
  operación; **ausente** del `Turn` persistido, del `feedback.jsonl` y del
  `trace_json` de SQLite en los **13/13** casos.
- Casos bloqueados: dejan `needs_clarification=1` + el mensaje fijo del catálogo,
  y `Turn.outcome="clarification"`. El código exacto **no** se persiste; solo se
  puede inferir del mensaje, y ese mapeo **no es inyectivo**: el mensaje de
  `target_missing` es idéntico al `_DEFAULT_CLARIFICATION`.
- Casos resueltos: indistinguibles de un follow-up sin operación de modalidad.
  `CLEAR` termina sin claves de modalidad (igual que un turno que nunca la tuvo)
  y un `REMOVE_MEMBER` no-op deja los params idénticos a los heredados.

**Conclusión de la auditoría:** el numerador es parcialmente inferible; el
**denominador no lo es**. `modality_followup_turns_total` no se puede reconstruir
de los datos persistidos hoy.

---

## 3. Señales presentes / ausentes

**PERSISTED_SIGNALS_FOUND:** `followup`, `intent_type`, `followup_intent_type`,
`needs_clarification`, `clarification_reason`, `parsed_params` (con claves de
modalidad), `user_query`, `Turn.outcome`/`outcome_reason`,
`Turn.query_fingerprint`, `trace_id`, `timestamp`, `channel`.

**MISSING_SIGNALS:** `modality_operation`, `instruction_status`, `control_code`,
`clarification_code` (crudo), `no_op`, `reason` del motor, marca de "este turno
tuvo una operación de modalidad" (el denominador), y `query_plan.reason`.

**PRIVACY_FINDINGS:**

1. `traces.parsed_params_json` y `Turn.parsed_params` **ya persisten**
   `modalidad_include/exclude_procedure_ids` — el reporte agregado nunca debe
   reexportarlos.
2. `traces.user_query` y `Turn.user_query` guardan el texto íntegro del usuario.
3. `Turn.chat_id` identifica la conversación (canal + clave de usuario).
4. `clarification_reason` proviene de un catálogo fijo
   (`_CLARIFICATION_MESSAGES`), no del texto del usuario: es seguro como fuente
   de código, pero ambiguo (`target_missing` colisiona con el mensaje por defecto).
5. El analizador entregado normaliza cualquier `clarification_code` fuera de la
   lista blanca a `unknown_code`: un código envenenado no puede filtrar texto.

---

## 4. Tamaño de muestra real

Barrido read-only sobre todos los almacenes reales disponibles:

| fuente | registros | ventana | con señal de modalidad |
| --- | ---: | --- | ---: |
| worktree `data/feedback.jsonl` | 2 455 | 2026-04-17 → 2026-06-10 | **0** |
| worktree `data/secoppal.db` (`traces`) | 2 455 | 2026-04-17 → 2026-06-10 | **0** |
| worktree `data/conversations/*.jsonl` | 79 turnos | 2026-05-25 → 2026-06-10 | **0** |
| repo principal `data/feedback.jsonl` | 2 504 | 2026-04-17 → **2026-07-22** | **0** |
| repo principal `data/conversations/*.jsonl` | 6 356 turnos | 2026-05-21 → **2026-07-22** | **0** |

Además, en `secoppal.db`: 0 traces con `route_reason like 'modality%'`, 0 con
`followup_mutation` en `trace_json`, y **0** cuyo `clarification_reason` sea
alguno de los 10 mensajes del catálogo de modalidad (192 aclaraciones totales,
ninguna de modalidad).

Es coherente con la historia: la rama `search/modality-contract-v1` nunca se
mergeó ni se desplegó, así que **el pipeline de modalidad jamás corrió contra un
usuario real**. Cualquier bloqueo de modalidad que exista hoy proviene de tests o
corpus sintético, y por contrato eso no puede satisfacer el umbral.

---

## 5. Decision de instrumentacion: instalar ahora para acumular evidencia futura

No hay interacciones reales (`N = 0`), asi que **M3C-D sigue bloqueado**.
La ausencia de muestra, sin embargo, **justifica instalar la instrumentacion**
(`app/core/modality_followup_telemetry.py`) en este mismo entregable M3C-CG-I:

1. Sin ella, el denominador `modality_followup_turns_total` nunca sera
   reconstruible de los datos persistidos actuales (§2): `CLEAR` y los no-op
   dejan las claves finales indistinguibles de la herencia generica de params.
2. Es pasiva y desactivada por defecto (`modality_followup_telemetry_path=None`);
   cuando se configura una ruta local, solo hace append JSONL.
3. Es un observador: no muta `parsed_params`, `resolved_params`,
   `query_fingerprint`, decisiones de modalidad, mensajes al usuario ni
   persistencia de `ConversationStore`.
4. No almacena texto de usuario, `chat_id`, `procedure_ids`, resultados, SOQL
   ni valores contractuales.
5. Falla abierto: un error de proyeccion o escritura se loguea como warning sin
   datos sensibles y no altera la respuesta.
6. Reconoce todos los codigos de bloqueo del motor, incluido
   `ambiguous_clear_or_null_value`, para que el numerador sea contado
   correctamente.

### Contrato V1 de la instrumentacion instalada

- Punto de emision: unico, posterior al resultado final del turno en
  `app/core/workflow/handlers/normal_flow.py`, despues de persistir el turno.
- Evento agregado por turno: `is_followup=true` y al menos una senal real de
  modalidad (`has_modality_operation`, `had_previous_modality_filter`,
  `has_final_modality_filter` o `blocked`).
- `source="runtime"` es el unico valor cancnico del productor; el analizador
  solo cuenta eventos con ese `source`.
- `event_id` es UUID v4 aleatorio; el analizador valida, deduplica y excluye
  IDs malformados.
- `recorded_at` se valida como ISO-8601 UTC real en el analizador y se
  normaliza a forma cancnica (`Z`) antes de aparecer en el reporte; strings
  arbitrarios se rechazan como registros invalidos.
- `sample_is_real` nunca se infiere del contenido del evento: se declara con
  `--sample-provenance {real,synthetic,unknown}` o mediante `--manifest`. El
  analizador preserva el triestado completo en el reporte.

**Veredicto actual:** `sample_is_real=false`, `turns_total=0`,
`m3c_d_recommendation=NOT_AUTHORIZED`. La compuerta permanece cerrada hasta que
la telemetria acumule al menos 100 turnos reales.

---
## 6. Instrumento entregado

`app/core/modality_followup_telemetry.py` — productor **pasivo**, stdlib pura,
sin red, sin importar `QueryRouter`, `ConversationStore` ni `LLMHandler`. Emite un
solo evento JSONL por turno de follow-up con senal real de modalidad.

`scripts/analyze_modality_followup_gate.py` — analizador **read-only**, stdlib
pura, sin red, sin importar `app/`, sin escribir `data/`. Consume un JSONL de
eventos agregados (esquema V1 congelado) y emite el reporte contractual en JSON
y Markdown.

- **Turno valido** (congelado): `is_followup=true`, `source="runtime"` y al menos
  una senal de modalidad (`has_modality_operation`,
  `had_previous_modality_filter`, `has_final_modality_filter` o `blocked`).
  Los turnos que solo heredan la modalidad previa (sin operacion propia) entran
  al denominador.
- **Deduplicacion**: por `event_id` valido **antes** de cualquier conteo. Un
  registro repetido no puede inflar ningun contador. Un registro sin `event_id`
  o con UUID malformado se rechaza como invalido.
- **Procedencia**: `sample_is_real` se calcula a partir de `sample_provenance`, que
  se declara con `--sample-provenance {real,synthetic,unknown}` o `--manifest`;
  el analizador no lo infiere del `source` del evento. Solo una muestra real
  (`sample_provenance="real"`) puede autorizar M3C-D. El reporte preserva el
  triestado completo: `real`, `synthetic` o `unknown`.
- **Separacion de buckets**: `target_missing` (conversacional) y
  `control_conflict` van en contadores propios y **nunca** entran a
  `blocked_by_status`, que solo recibe los cinco estados juridicos M2.
  `clarification_candidate_total` es un subconjunto informativo de
  `blocked_total` y **no** participa en la aritmetica de la compuerta.
- **Privacidad**: la salida se arma solo con contadores y codigos de lista
  blanca; cualquier codigo desconocido colapsa a `unknown_code`. El analizador
  valida y normaliza `recorded_at` como UTC ISO-8601 real antes de usarlo en
  `sample_start`/`sample_end`; strings arbitrarios se rechazan.
- **Nota de aritmetica**: con la muestra minima de 100, la condicion
  `blocked_rate >= 5 % AND blocked_total >= 5` es redundante en su segundo
  conjunto (5 % de 100 ya son 5). El conjunto solo distingue por debajo del
  minimo, y asi queda fijado en los tests.

`tests/test_analyze_modality_followup_gate.py` — 38+ tests del analizador V1,
incluida validacion de `recorded_at`, exclusion de fuentes no reales, validacion
de UUID, deduplicacion y herencia pura.

`tests/test_modality_followup_telemetry.py` — 56 tests del productor: esquema,
semantica de senales, privacidad, fail-open, exactamente-una-vez, bloqueo
`ambiguous_clear_or_null_value` e integracion E2E.

---

## 7. Reporte de la compuerta (estado actual)

```json
{
  "schema_version": 1,
  "sample_source": "modality_followup_telemetry_v1",
  "sample_provenance": "unknown",
  "sample_is_real": false,
  "sample_start": null,
  "sample_end": null,
  "modality_followup_turns_total": 0,
  "modality_followup_blocked_total": 0,
  "blocked_rate": 0.0,
  "blocked_by_status": {
    "PARTIAL": 0,
    "AMBIGUOUS": 0,
    "CONFLICT": 0,
    "NOT_SEARCHABLE": 0,
    "DATASET_INAPPLICABLE": 0
  },
  "target_missing_total": 0,
  "control_conflict_total": 0,
  "noop_total": 0,
  "clear_total": 0,
  "clarification_candidate_total": 0,
  "duplicate_records_excluded": 0,
  "invalid_records_excluded": 0,
  "gate_minimum_sample_met": false,
  "gate_threshold_met": false,
  "m3c_d_recommendation": "NOT_AUTHORIZED"
}
```

```text
PRODUCT_GATE_SAMPLE_SIZE: 0
PRODUCT_GATE_MINIMUM_REQUIRED: 100
PRODUCT_GATE: INSUFFICIENT_SAMPLE
M3C_D: NOT_AUTHORIZED
```

---

## 8. Camino para volver a abrir la compuerta

1. Desplegar M3C-C a un canal real (la rama sigue sin mergear).
2. Configurar y habilitar la ruta de telemetría en el canal real (`modality_followup_telemetry_path`).
3. Acumular ≥100 turnos reales de follow-up con señal de modalidad.
4. Correr `scripts/analyze_modality_followup_gate.py --input <log> --format both`.
5. Solo con `sample_is_real=true`, `total >= 100` y
   (`blocked >= 10` ∨ (`rate >= 5 %` ∧ `blocked >= 5`)) se autoriza M3C-D.

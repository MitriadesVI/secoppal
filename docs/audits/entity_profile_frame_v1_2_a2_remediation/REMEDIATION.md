# Entity Profile Frame V1.2-A2 — remediación y adjudicación

Fecha: 2026-09-07.

```yaml
BASE: 9164df0606b0ba9424d627aa37b12e742a7cd707
TESTED_CODE_HEAD: 2ec5033edb5bea1a1394df427a9cdf966155da25
BRANCH: fix/entity-profile-frame-v1-2-a2-remediation
WORKTREE: /Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-a2-remediation
PRODUCTIVE_FRAME_ACTIVATION: OFF
NEW_PRODUCTIVE_FRAME_CONSUMERS: NONE
```

## Tesis

Los tres hallazgos eran reales sobre A1. F1 permitía reescribir un snapshot por
las implementaciones base de `dict` y `list`; F2 tenía dos autoridades
divergentes para reconocer fuente y confundía una coordinación temática con
dos fuentes; F3 proyectaba primero y, por eso, un estado físico de la fuente
equivocada desaparecía antes de validarse.

La reparación sustituye subclases mutables por un `Mapping` sobre copia propia
y secuencias inmutables; materializa copias nuevas únicamente al entrar al
builder legacy. La misma autoridad de ocurrencias sirve ahora al adapter del
Frame y a la decisión de alcance dual de Search. La coordinación posterior a
un complemento temático sólo acredita otra fuente si trae una calificación
temporal o de estado. La migración inspecciona todos los campos físicos antes
de proyectar y comprueba `estado_family` contra el catálogo existente.

No se activó el Frame ni se conectó a ningún workflow productivo. La política
escalar histórica de Search se conservó: `_select_dataset` no se subordinó a
la selección de fuente del Frame.

## Adjudicación por hallazgo

| Hallazgo | Base A1 real | Causa | Resultado en el código probado |
|---|---|---|---|
| F1 — integridad del plan | `dict.__setitem__`, `dict.update`, `dict.__init__`, `list.__setitem__`, `list.append` y `list.__init__` cambian el snapshot | Overrides de subclases no interceptan APIs base | **CONFIRMED_FIXED**. El adjunto y ataques sobre planes julio/agosto no cambian scope, predicados, SQL, fingerprint, `plan_id`, `frame_ref` ni jobs. |
| F2 — R3 fuente/tema | Cuatro encuadres no acreditan `contratos`; la coordinación final acredita una segunda cabecera y pierde tema | Bolsa local incompleta y regex global de BOTH | **CONFIRMED_FIXED**. Se preservan cortesía, interrogativas, tildes, mayúsculas, `y`/`o`/coma, nombres acreditados, exclusiones y frases. Las coordinaciones inequívocas por evento siguen en BOTH. |
| F3 — estado legacy ajeno | CONTRACTS + `estado_del_procedimiento` migra y elimina estado; inverso equivalente | Proyección previa a validación | **CONFIRMED_FIXED**. Ambos sentidos son `INVALID_CONTEXT`/sin Frame; params-only compatibles, duplicados equivalentes, BOTH y familias acreditadas se conservan. |

La expectativa de `contratos de gestión de procesos y contratos` quedó
adjudicada como fuente CONTRACTS y tema completo `gestión AND procesos AND
contratos`. No se promovió a BOTH. En `procesos de administración de contratos`,
Search conserva su dataset escalar histórico CONTRACTS mientras el Frame
acredita fuente PROCESSES; son dos decisiones distintas por diseño.

## Coherencia y propiedades

- El `ProfilePlan` toma copias de `frame_ref`, jobs, limitaciones y
  `source_plans`; BuyerScope, SourceSelection, provenance, temas, estados y
  selecciones normalizan entradas secuenciales mutables.
- La frontera `query_runner.execute_jobs` entrega al builder una materialización
  profunda nueva. Una mutación local del builder no cambia el plan.
- Las propiedades Hypothesis nuevas cubren independencia de inputs anidados,
  encuadres equivalentes y migración compatible/ajena. Se conservaron las
  propiedades anteriores de Frame.
- Los tests directos que pasaban `SourcePlan.params` al builder se adaptaron a
  la frontera explícita `mutable_copy`; las invariantes de columnas, periodo y
  estado no cambiaron.

## Paridad y activación

La comparación A1/A2 de 15 consultas fuera de los casos objetivo dio
`PARITY_UNAFFECTED=15/15` y cero divergencias inesperadas. Los cinco casos que
debían cambiar divergieron sólo en tema y, para la coordinación defectuosa, en
la eliminación del alcance BOTH falso. No hay imports productivos de
`EntityProfileFrame`, `ProfilePlan`, `compile_profile`, `collect_claims` ni
`migrate_legacy_context` fuera de `app/core/entity_profile/`; no hubo cambios
en workflow/UI.

## Hallazgo adyacente no mezclado

`tests/test_modality_followup_pipeline.py::test_blocked_turn_rolls_back_orthogonal_deltas`
espera que la frase `contratación directa de alimentación en procesos` no deje
`procesos` dentro de `objeto`. Falla tanto en A1 como en A2 con
`['alimentacion', 'procesos']`; no es una regresión de este diff. Corregirlo
exige adjudicar si una fuente pospuesta por `en` pertenece a la gramática de
claims —el diseño y esta orden sólo congelan cabeceras y coordinaciones—, por
lo que no se amplió silenciosamente el parser.

Este defecto está en la misma frontera semántica R3. Por criterio fail-closed
queda como bloqueo para autorizar V1.2-B, aunque los tres hallazgos encargados
estén corregidos.

## Validación del código probado

- Adjunto original contra A1: `12 failed, 1 passed in 0.33s`.
- Adjunto exacto contra A2: `13 passed in 0.21s`.
- Target agregado: `1692 passed, 1 deselected in 102.25s (0:01:42)`; el único
  deselect es el defecto adyacente anterior.
- Entity Profile completo dentro de ese conjunto: corrida separada
  `815 passed in 31.65s`.
- Fault-injection existente más adjunto: `29 passed in 0.39s`. No se ejecutó
  una campaña de mutación con motor y restauración, así que
  `MUTATION_CAMPAIGN=NOT_RUN`.
- Ruff: `All checks passed!`.
- Semgrep: `Findings: 10 (10 blocking)`, todos los mismos diez hallazgos A1 de
  `except-exception-que-silencia`; delta por regla/archivo/línea: cero.
- `make lint-core`: `app/core/ limpio`.
- Guard de tamaño: `2 passed in 0.10s`.
- `git diff --check 9164df0..2ec5033`: sin salida.
- Suite global: no ejecutada por contrato del repositorio.

## Límites

No se levantó la app, no hubo red, SECOP, LLM, persistencia real, merge, push,
V1.2-B ni cambios de dinero, identidad, geografía, catálogos o STOPWORDS. El
modelo y nivel de razonamiento efectivos no son verificables desde el
worktree.

```yaml
F1_PLAN_IMMUTABILITY: CONFIRMED_FIXED
F2_R3_SOURCE_TOPIC_OWNERSHIP: CONFIRMED_FIXED
F3_FOREIGN_LEGACY_STATE: CONFIRMED_FIXED
SEARCH_PARITY_OUTSIDE_TARGET_FIXES: PASS_15_OF_15
REMAINING_BLOCKERS:
  - PREEXISTING_POSTPOSED_SOURCE_TOPIC_LEAK
VERDICT: REMEDIATION_REQUIRED
B_START_RECOMMENDED: NO
```

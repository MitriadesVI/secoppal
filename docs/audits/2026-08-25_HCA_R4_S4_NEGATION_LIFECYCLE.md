# HCA-R4-S4 — Negation lifecycle & follow-up completion

**Fecha:** 2026-08-25
**Rama:** `feature/hca-r4-s4-negation-lifecycle`
**Base canónica:** `c7c5d85c9d84145f3ba36dbfd032fc32f4ac6528`
**Resultado:** **PASS**
**Cierre:** `DIMENSIONAL_NEGATION_V1: CLOSED`

## Alcance auditado

R4-S4 cierra el ciclo conversacional de las exclusiones que R4 ya permitía
persistir. No amplía la frontera de ejecución negativa de S3: OBJECT y MODALITY
siguen siendo ejecutables; DEPARTMENT sigue limitado a resta segura de un
conjunto materializado; CITY, TIME, BUYER, ACTOR, CONTRACT_TYPE y STATE siguen
en `CLARIFY_ONLY`.

La ruta confirmada es:

```text
NegationRequest
  → QueryFrame / autoridad dimensional existente
  → contexto confirmado
  → follow-up classifier
  → ExclusionMutation / MutationPolicy
  → requested_clear_fields cuando aplica
  → QueryFrame resultante
  → QueryPlan
```

No se creó pending de negación, Event Sourcing, AST de operaciones ni un campo
paralelo a `objeto_exclude`.

## Inventario y resultado del lifecycle

| capacidad | crear | heredar | retirar | reemplazar | re-incluir | resultado S4 |
|---|---:|---:|---:|---:|---:|---|
| OBJECT / `objeto_exclude` | sí | sí | sí | sí | sí | ciclo acotado completo |
| MODALITY / `modalidad_exclude_procedure_ids` | sí | sí | sí | autoridad existente | sí | preservada; cero regresiones |
| DEPARTMENT / `departamentos_in` | sí, por resta | sí | sólo con procedencia | n/a | sólo con `region` reconstruible | si falta procedencia: `CLARIFY` / requery |
| dimensiones no soportadas | no | contexto previo sí | n/a | n/a | n/a | aclaración transaccional, sin estado zombi |

Mutaciones OBJECT implementadas sobre la representación mínima existente:

- `ADD_EXCLUSION`: `también sin transporte` conserva las exclusiones previas.
- `REMOVE_EXCLUSION`: `quita la exclusión de alimentación` retira sólo ese
  valor; `quita esas exclusiones` limpia el conjunto cuando el referente es
  inequívoco.
- `REPLACE_EXCLUSION`: `mejor sin transporte` sustituye la exclusión previa.
- `REINCLUDE`: `ahora incluye/permite/vuelve a incluir X` retira X del conjunto
  negativo; no lo añade automáticamente al tema positivo.

La traza conserva `operation`, `previous_exclusions`, `requested_exclusions` y
`resulting_exclusions` sin persistir un historial. `raw_query` permanece
intacto. Añadir o retirar repetidamente el mismo valor es idempotente.

## Invariantes cerrados

**`INV-NEGATION-LIFECYCLE-SYMMETRIC`** — una exclusión OBJECT que el runtime
puede persistir tiene una operación conversacional segura para retirarla o
reemplazarla.

**`INV-NEGATION-REMOVAL-IS-DIMENSIONAL`** — retirar una exclusión sólo toca el
campo negativo gobernado. Tema, ciudad, dinero, modalidad, actor y las demás
exclusiones permanecen intactos.

Se preservan además los invariantes de S1–S3: dimensión antes de ejecución, no
drop silencioso, normalización que conserva intención, aclaración unsupported
sin mutación y aclaración transaccional.

## Gates conversacionales

- OBJECT: create, preserve, remove, reinclude, replace, partial clear e
  idempotence verificados en secuencias de 2–5 turnos.
- MODALITY: `ahora incluye contratación directa` retira la exclusión usando la
  autoridad especializada. El alias positivo `contratación directa sin
  ofertas` permanece intacto.
- DEPARTMENT: una región persistida permite reconstruir el conjunto canónico y
  re-incluir un departamento. Un conjunto materializado sin `region` no prueba
  qué resta ocurrió: se aclara y no se inventa procedencia.
- UNSUPPORTED: CITY, TIME, BUYER, ACTOR, STATE y CONTRACT_TYPE aclaran sin
  mutación. ACTOR no crea `actor_pending`.
- Después de una aclaración unsupported, `entonces déjalo así` recupera el
  último frame confirmado; no persiste `NegationRequest` pendiente.
- Una consulta completa nueva rompe el contexto cuando existe estado negativo
  persistido. `muéstrame más…` conserva el comportamiento histórico.
- `reset`, `empezar de nuevo` y `nueva búsqueda` limpian filtros positivos y
  negativos.
- Una remoción inexistente no finge éxito; aclara o produce un no-op explícito
  y honesto según la forma lingüística.
- Contradicciones positivas/negativas siguen cerrando con aclaración. La forma
  inequívoca `no X sino Y` se trata como reemplazo, no como contradicción
  artificial.

## Fixture pre-registrado

El archivo `tests/fixtures/audits/negation_focal_v1.yaml` no se modificó.

| medición | resultado |
|---|---:|
| antes de S4 | 21 PASS / 3 FAIL / 0 ERROR |
| después de S4 | 22 PASS / 2 FAIL / 0 ERROR |

Clasificación exacta de los tres casos inicialmente pendientes:

### `NEG-N11B`

- **CURRENT_BEHAVIOR:** `quita la exclusión` retira `objeto_exclude` sin crear
  tema positivo ni tocar los demás filtros.
- **EXPECTED_ORACLE:** el follow-up debe limpiar la exclusión OBJECT.
- **WHY_NOT_CHANGED:** el oráculo era correcto; se corrigió el lifecycle.
- **FINAL_CLASSIFICATION:** `S4_FIXED`.

### `NEG-N15`

- **CURRENT_BEHAVIOR:** `contratación directa sin ofertas` resuelve inclusión
  de modalidad, no exclusión OBJECT; el plan no ejecuta una consulta formada
  únicamente por modalidad porque exige tema, alcance u otro filtro de dominio.
- **EXPECTED_ORACLE:** `EXECUTE` y ninguna contaminación de OBJECT; la nota
  pre-registrada afirma que el caso ejecutaba antes de S4.
- **WHY_NOT_CHANGED:** parser y arbitraje de modalidad cumplen el contrato; la
  ejecutabilidad de una consulta sólo-modalidad pertenece a la política general
  de `QueryPlan`, fuera del lifecycle. La nota del fixture es obsoleta.
- **FINAL_CLASSIFICATION:** `VALID_DEFERRED`.

### `NEG-ANTI-002`

- **CURRENT_BEHAVIOR:** la consulta negada no contiene ciudad ni entidad
  positiva y difiere del control `NEG-N12` en `ciudad`; `entidad_resolved` es
  `None` en ambos, como exige la eliminación del CITY_TO_ENTITY leak de S3.
- **EXPECTED_ORACLE:** `differ_from.fields: [ciudad, entidad_resolved]`.
- **WHY_NOT_CHANGED:** el runner exige que cada campo listado difiera. Pedir
  diferencia individual en `entidad_resolved` contradice el frame seguro de S3;
  el oráculo debería comprobar diferencia colectiva o sólo `ciudad`. Fixture y
  runner se preservaron.
- **FINAL_CLASSIFICATION:** `WRONG_ORACLE_CANDIDATE`.

## HCA congelado

| medición | resultado |
|---|---:|
| baseline S3 | 164 PASS / 31 FAIL / 0 ERROR |
| final S4 | 164 PASS / 31 FAIL / 0 ERROR |
| casos con cambio de status, frame o detalle | 0 |

`HCA-ACTOR-011` permanece FAIL basal. Una versión intermedia y demasiado amplia
de la protección de búsqueda nueva lo llevó accidentalmente a PASS y produjo
165/195; esa mejora se retiró porque habría sido un arreglo general de Actor
Pending, reservado para HCA-R7. La versión final limita la protección a
contextos con estado negativo persistido.

## Validación final

| gate | resultado |
|---|---|
| lifecycle/adversarial multi-turn | 24 passed |
| focal S1/S2/S3/S4 + follow-up + modality + money + geography + actor | 650 passed |
| query corpus | 89/89 PASS |
| dialogue E2E | 44/44 PASS |
| dialogue policy | 60/61; único FAIL preexistente `GOB-DEPTO-001` |
| fixture pre-registrado | 22 PASS / 2 FAIL / 0 ERROR; clasificados arriba |
| HCA holístico congelado | 164 PASS / 31 FAIL / 0 ERROR; 0 changed cases |
| full pytest | 8436 passed, 2 skipped, 23 xfailed in 448.90 s |
| Ruff E/W/F de archivos cambiados | PASS |
| `make lint-core` | PASS |
| `git diff --check` | PASS |

Los mensajes 402 observados durante HCA activaron el fallback estructural
offline previsto; el runner terminó con cero errores y resultados comparables.

## Contradicciones y mejoras encontradas

1. El contrato exigía `ahora incluye contratación directa`, pero el clasificador
   de modalidad combinaba `ahora=REPLACE` con `incluye=ADD_INCLUDE` y bloqueaba
   la orden por conflicto. Se añadió la operación cerrada `RESTORE_INCLUDE`, que
   restaura el valor sin convertirlo en whitelist.
2. `entonces déjalo así` después de una aclaración unsupported podía contaminar
   el tema positivo. Se reconoció sólo esta construcción acotada para conservar
   el frame confirmado; no se implementó una limpieza general de control
   language.
3. La mejora accidental de `HCA-ACTOR-011` contradecía el alcance explícito. Se
   retiró y la protección new-search quedó limitada a estado negativo
   persistido.
4. Los oráculos `NEG-N15` y `NEG-ANTI-002` contradicen respectivamente la
   política actual de ejecutabilidad y el frame seguro de S3. Se documentaron,
   sin alterar expectativas para fabricar 24/24.

## Revisión de alcance

```text
SCOPE_CREEP: NO
GENERAL_ACTOR_PENDING_FIX_INCLUDED: NO
GENERAL_CONTROL_LANGUAGE_FIX_INCLUDED: NO
TIME_V2_INCLUDED: NO
CITY_EXECUTION_INCLUDED: NO
```

## Deuda posterior a R4

- Gazetteer/canonicalización por igualdad antes de ejecutar CITY negativa.
- Álgebra temporal V2 antes de ejecutar TIME negativa.
- Identidad canónica antes de ejecutar BUYER/ACTOR negativos.
- Revisión separada de los dos oráculos pre-registrados documentados arriba.
- Actor Pending general en HCA-R7.

R4 termina con soporte declaradamente parcial y seguro; no afirma que toda la
negación del español sea ejecutable.

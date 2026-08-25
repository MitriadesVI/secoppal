# HCA-R4-S2 — OBJECT negation end-to-end

**Estado:** PASS

**Base:** `7b7a79a8034bf2c539cb5d31c310b35dd15ea1ca`

**Rama:** `feature/hca-r4-s2-object-negation`

## Dictamen

```text
HCA_R4_S2_OBJECT_NEGATION: PASS

OBJECT:
  BINDING: PASS
  EXECUTION: PASS
  OBJETO_EXCLUDE_AUTHORITY: UNIQUE
  NEGATIVE_TO_POSITIVE_LEAKS: 0
  SILENT_DROPS: 0

SOQL:
  NULL_SAFE: PASS
  PROCESS_DATASET: PASS
  CONTRACT_DATASET: PASS

CONTRADICTION:
  POSITIVE_NEGATIVE_SAME_OBJECT: CLARIFY
  DETECTED_BEFORE_EXECUTION: YES
  ZERO_RESULTS_FABRICATION: 0

COORDINATION:
  NI_EXECUTION: PASS
  SINO_EXECUTION: PASS

RAW_QUERY:
  PRESERVED: YES

RUNTIME:
  CITY_EXECUTION_ADDED: NO
  TIME_EXECUTION_ADDED: NO
  BUYER_ACTOR_EXECUTION_ADDED: NO
```

## Implementación

La cadena cerrada es:

```text
raw query
  → binding S1 + span isolation
  → NegationRequest(OBJECT, EXECUTE) efímero
  → objeto_exclude
  → QueryFrame / MutationPolicy(exclude_topic)
  → SoQL NULL-safe
  → SECOP
```

No se añadió un AST, un segundo campo negativo ni una representación persistente.
`objeto_exclude` sigue siendo la única autoridad runtime. Para una consulta como
`de mantenimiento sin alimentación ni transporte`, `objeto=[mantenimiento]` y
`objeto_exclude=[alimentacion, transporte]`; ninguno de los spans negativos
puede alimentar `objeto`, el fallback LLM o un clasificador de alcance positivo.

`objeto_exclude` se incorporó a las dos autoridades anti-`WHERE 1=1` existentes.
Esto permite que `contratos sin alimentación` sea una consulta negativa real en
vez de caer a una consulta global o perder la exclusión. Las políticas generales
de alcance conservan su autoridad en rutas que ya las aplicaban.

## Forma SoQL NULL-safe

Por cada variante y por los campos reales del dataset se genera:

```sql
NOT (
  coalesce(unaccent(UPPER(field_a))
    LIKE unaccent(UPPER('%term%')), false)
  OR
  coalesce(unaccent(UPPER(field_b))
    LIKE unaccent(UPPER('%term%')), false)
)
```

Campos gobernados:

- Procesos `p6dx-8zbt`: `nombre_del_procedimiento` y
  `descripci_n_del_procedimiento`.
- Contratos `jbjy-vk9h`: `objeto_del_contrato` y
  `descripcion_del_proceso`.

La especificación fija N1–N6: NULL + no-match conserva; cualquier campo que
contenga el término excluye; ambos NULL conservan respecto de esa exclusión; y
dos no-match conservan. Los tests inspeccionan la expresión generada, no solo
una función booleana auxiliar.

`sin A ni B` produce dos predicados independientes unidos por `AND`:

```text
NOT(matches A) AND NOT(matches B)
```

Por tanto una fila sobrevive si y solo si no pertenece a A ni a B. No se pierde
el objeto positivo cuando existe.

## Contradicciones

La contradicción se evalúa sobre las variantes que la autoridad de retrieval ya
proyecta. No se añadieron equivalencias ni se modificó
`morphological_variants`. La comparación aplica el mismo `unaccent` conceptual
que el predicado SoQL.

Un término positivo obligatorio que solapa la exclusión bloquea la ejecución con
`OBJECT_NEGATION_CONTRADICTION` y pregunta si el usuario quiere incluirlo o
excluirlo. No se comunica “SECOP no encontró resultados”. En grupos positivos OR
se aclara solo si todas las alternativas quedan bloqueadas, evitando falsos
conflictos. En seguimiento, el delta contradictorio se rechaza y el frame previo
queda intacto.

## Raw query y follow-up limitado

El grafo conserva `user_query` y `raw_query` byte por byte; `positive_query` es
una vista interna separada y enmascarada. El test usa
`Contratos de alimentación sin transporte` y prueba que ambos campos raw siguen
idénticos mientras `transporte` no aparece en la vista positiva.

El seguimiento limitado exigido también queda cerrado:

```text
T1 mantenimiento sin alimentación en Barranquilla
T2 por más de 500 millones
```

conserva mantenimiento, `objeto_exclude=alimentacion` y Barranquilla, y añade
`valor_min=500000000`. No se implementó lenguaje para retirar exclusiones.

## Evidencia viva

Runner reproducible:
`scripts/audits/hca_r4_s2_object_live.py`.

Artefacto:
`artifacts/audits/hca_r4_s2_object_live_2026-08-25.json`, SHA-256
`354ebbf5b0971c7f26af20f51114ef318f94ab8c1071c5736a14297a8bdda3f5`.

Comparación acotada común: departamento `Atlántico`, fechas
`2025-01-01..2026-08-24`, objeto positivo `alimentacion`, exclusión
`transporte`, 100 filas inspeccionadas por variante y dataset.

| dataset | positivo total | con exclusión | positivos inspeccionados que contenían transporte | violaciones tras exclusión |
|---|---:|---:|---:|---:|
| Procesos `p6dx-8zbt` | 1 140 | 1 119 | 3/100 | **0/100** |
| Contratos `jbjy-vk9h` | 825 | 819 | 2/100 | **0/100** |

NULL live:

- Procesos: 6 candidatos con al menos un campo gobernado NULL; **6/6**
  sobrevivieron con un término sintético ausente; IDs perdidos: 0.
- Contratos: 0 candidatos NULL en el snapshot; PASS/NOT_APPLICABLE con la
  razón registrada. La forma exacta se validó contra el dataset y su spec real.

Todas las operaciones finales fueron GET exitosos. `HCA_R4_S2_OBJECT_LIVE: PASS`.

## Pruebas

- Focal OBJECT/binding/SoQL: **54 passed**.
- Query router/plan vecino: **124 passed**.
- Follow-up vecino: **92 passed**.
- Equivalencia de modalidad: **3 312 passed**.
- Money: **197 passed**.
- Fixture preregistrado: **19 PASS / 5 FAIL / 0 ERROR**, antes S1
  **13/11/0**. Los casos OBJECT S2 pasan. Restan DEPARTMENT/TIME de S3,
  lifecycle de S4 y dos limitaciones preexistentes del oráculo/control del
  runner; no hay fuga negativa→positiva ni drop silencioso S2.
- HCA holístico congelado: **164 PASS / 31 FAIL / 0 ERROR** antes y después.
  Caso materialmente cambiado: `HCA-NEG-005`, que ahora conserva
  `objeto_exclude=[alimentacion]`; ningún status ajeno se movió.
- Adversarial manual: 22 formas sobre marcadores, `ni`, `sino`, tildes,
  plural, puntuación, casing, lookalikes, money, modalidad, ESAL y frases
  jurídicas; 0 regresiones.
- Broad final: **8 358 passed, 2 skipped, 23 xfailed**.
- Changed-file Ruff: PASS. `git diff --check`: PASS.

El corpus holístico conserva SHA-256
`262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`.
El fixture preregistrado conserva SHA-256
`f4f64a33ea7009eb1da9fa7d3c910e67010e6dafc6b4c5b6932901c27fad9a1d`.

## Revisión de arquitectura

1. `NegationRequest` sigue efímero: **sí**.
2. `objeto_exclude` es la única autoridad runtime OBJECT negativa: **sí**.
3. Tablas de marcadores duplicadas: **no**.
4. Proyección NULL-safe: **sí**.
5. Un negative span puede activar objeto positivo: **no**.
6. Una contradicción puede llegar a SECOP: **no**.
7. Raw query intacta: **sí**.
8. Modalidad modificada: **no**.
9. Money modificado: **no**.
10. Lógica S3/S4 introducida: **no**.

## Diferido

### R4-S3

- CITY y TIME siguen CLARIFY; no se añadió `NOT LIKE`, canonicalización de
  ciudad, huecos temporales ni resta departamental.
- DEPARTMENT, STATE y CONTRACT TYPE no reciben ejecución negativa nueva.

### R4-S4

- BUYER/ACTOR siguen CLARIFY.
- No se implementó retirar/reincluir exclusiones ni lifecycle genérico.

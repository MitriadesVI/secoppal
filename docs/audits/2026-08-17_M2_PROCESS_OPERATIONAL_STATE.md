# M2 — PROCESS OPERATIONAL STATE

Fecha de ejecución: 2026-08-17
Bloque: M2 (PROCESS-OP-STATE-001)
Estado: listo para revisión independiente

---

## SOURCE_HEAD

```text
SOURCE_HEAD  = ad1bb2840a6de2a4da3ee4c27cb843b8fcdcba95
BRANCH       = feature/process-operational-state
TRUTH_LAYER_RESEARCH_REOPENED = NO
```

M2 consume la fundación del Truth Layer integrada en M1. No la modifica: ningún
archivo bajo `app/core/truth_layer/` cambia en este bloque.

---

## Por qué existe M2 — la medición

Medido en vivo contra `p6dx-8zbt` el 2026-08-17 (9.018.738 filas):

`estado_de_apertura_del_proceso = 'Abierto'` cubre **7.888.926 filas (87,5%)**.
Ese campo era el que la tarjeta mostraba como estado del proceso. Su desglose real:

| `estado_del_procedimiento` | Filas | % de los "Abierto" |
|---|---:|---:|
| Seleccionado | 4.742.109 | 60,1% |
| Publicado | 2.700.581 | 34,2% |
| Evaluación | 327.181 | 4,1% |
| Cancelado | 66.002 | 0,84% |
| Borrador | 37.643 | 0,48% |
| Aprobado | 7.382 | 0,09% |
| Abierto | 4.309 | 0,05% |
| En aprobación | 3.629 | 0,04% |
| Suspendido | 90 | 0,001% |

**El 65,7% de lo que la tarjeta rotulaba "Abierto" no está en recepción de
ofertas.** La auditoría del Truth Layer (230/450 ≈ 51%) era una submuestra
conservadora del mismo fenómeno.

---

## FIELDS_ADDED_TO_PROCESS_SELECT

```text
SOQL_SELECT_CHANGED = YES  (un solo campo)
SOQL_WHERE_CHANGED  = NO
SOQL_ORDER_CHANGED  = NO
FILTERING_CHANGED   = NO
RANKING_CHANGED     = NO
```

Añadido: **`estado_resumen`** — y nada más.

La auditoría previa determinó que la etapa la deciden **únicamente**
`estado_del_procedimiento` + `fase` (`_stage_from_signals`), y ambos ya viajaban
en el SELECT junto con `estado_de_apertura_del_proceso`, `fecha_de_recepcion_de`,
`urlproceso` e `id_del_proceso`. Para el estado visible, el mínimo estricto eran
**cero** campos nuevos.

`estado_resumen` se añade por una razón de honestidad, no de funcionalidad: el
observer distingue `not_observed` (campo ausente de la fila) de `missing`
(presente y nulo), y **solo `missing` penaliza la calidad**. Omitir un campo con
100% de cobertura y política de sentinelas ya definida hacía que la lectura se
declarara `complete` ignorándolo. Se transporta para que la calidad observada sea
la real.

**Diferidos por decisión de arquitectura** (no se transportan):
`adjudicado`, `fecha_adjudicacion`, `id_adjudicacion`, `id_del_portafolio` — el
eje `process_resolution` y la adjudicación visible quedan fuera de M2.

---

## OPERATIONAL_SUMMARY_CONTRACT

`app/core/process_operational_state.py` — kernel puro, sin I/O, sin reloj, sin LLM.

```python
ProcessOperationalSummary(
    state: str,             # etapa canónica del Truth Layer, sin traducir
    label: str,             # texto visible
    icon: str,
    raw_state: str,         # estado_de_apertura_del_proceso, intacto
    show_raw_state: bool,   # política de discrepancia
    deadline_passed: bool,  # eje temporal, solo como soporte
)
```

`as_dict()` expone exactamente esas seis claves.

```text
TRUTH_LAYER_FULL_PAYLOAD_EXPOSED = NO
REASON_CODES_EXPOSED             = NO
QUALITY_TREE_EXPOSED             = NO
```

No hay sidecar diagnóstico público, ni `generic_enrichment`, ni serialización del
assessment. La trazabilidad completa sigue siendo interna al Truth Layer.

### Autoridad y taxonomía

La autoridad del estado es `process_lifecycle` vía `observe_process_truth_axes`.
El mapping se construyó sobre los valores canónicos reales de
`ProcessLifecycleStage`, no sobre etiquetas inventadas:

| stage canónico | Label visible | Icono |
|---|---|---|
| `offer_phase_observed` | Recepción de ofertas | 🟢 |
| `published` | Publicado | 🔵 |
| `evaluation` | En evaluación | 🟠 |
| `selected` | Seleccionado | ⚫ |
| `cancelled` | Cancelado | 🔴 |
| `suspended` | Suspendido | 🟠 |
| `draft` | Borrador | ⚪ |
| `approval` | En aprobación | ⚪ |
| `unknown` | Estado por verificar | ⚪ |

`published` conserva etiqueta propia (34,2% de los "Abierto"): SECOP lo publica
sin confirmar fase de oferta. Rotularlo "Recepción de ofertas" sería inventar
disponibilidad; rotularlo "Abierto" es el error que M2 corrige.

Una etapa futura fuera del catálogo cae al fallback conservador, nunca a una
etiqueta adivinada.

---

## VISIBLE_BEHAVIOR_CHANGE

```text
VISIBLE_PROCESS_STATE_CHANGED = YES
CONTRACT_UI_CHANGED           = NO
```

Antes (la tarjeta mostraba el campo de apertura):

```text
💰 $69,016,710 | Abierto
```

Después, sobre la misma fila real (`CO1.REQ.10000001`, SCJ-CD-494-2026):

```text
💰 $69,016,710 | ⚫ Seleccionado
SECOP reporta apertura: Abierto
```

Esa fila es didáctica: `fase` = "Presentación de oferta" y `estado_resumen` =
"Presentación de oferta" también sugieren disponibilidad. Solo
`estado_del_procedimiento` = "Seleccionado" dice la verdad, y es precisamente el
campo al que el Truth Layer da autoridad.

El nombre "Truth Layer" y su vocabulario interno (etapas canónicas, reason codes,
sentinelas) nunca aparecen en la tarjeta; hay un test que lo comprueba.

---

## RAW_STATE_POLICY

```text
RAW_SECOP_STATE_PRESERVED = YES
```

Dos garantías distintas:

1. **En el dato**: `row["estado"]` sigue siendo el crudo de SECOP. Lo consumen el
   radar (`follows.py`, campo `status` de la membresía) y la persistencia; M2 no
   cambia dato persistido. Los campos operativos son aditivos (`estado_op*`).
2. **En la tarjeta**: cuando hay discrepancia material, el crudo se muestra.

### RAW_VS_OPERATIONAL_DISCREPANCY_POLICY

Determinista y simétrica:

- crudo `Abierto` + etapa que ya no admite ofertas → se muestra
- crudo `Cerrado` + recepción de ofertas observada → se muestra
- crudo fuera del catálogo `{Abierto, Cerrado}` → se muestra tal cual
- alineados (p. ej. `Abierto` + `published`) → **no** se muestra segunda línea

El texto es `SECOP reporta apertura: <valor>`. No se acusa a la fuente: SECOP
publica campos con semánticas distintas y el de apertura es corroboración, no el
estado del proceso. Hay un test que verifica la ausencia de lenguaje acusatorio.

---

## FALLBACK_POLICY

```text
unknown / missing / not_observed  →  NUNCA una categoría
```

Sin evidencia suficiente el resumen emite `state="unknown"`, label
"Estado por verificar", icono neutro, y conserva visible el crudo — que es lo
único que hay. `is_conclusive` lo expone para consumidores posteriores.

El eje temporal es **soporte, nunca motor**: solo puede retirarle la afirmación
de vigencia a una recepción de ofertas cuya fecha ya pasó. Una relación temporal
`unknown` jamás cuenta como vencimiento, y el eje nunca crea ni cambia una etapa.

---

## PAGINATION_BEHAVIOR

```text
PAGINATION_CONTRACT_CHANGED = NO
GROUPING_RUNTIME_WIRED      = NO
```

El enganche está en `Formatter.to_rows`, **después** del dedupe y del recorte a
`max_results`. Consecuencias:

- solo se enriquecen los resultados que de verdad se muestran; nunca el universo
  recuperado, nunca antes del ranking;
- las 7 rutas de render (flujo normal, paginación, direct lookup, sugerencia,
  rendering, dual dataset) quedan cubiertas por un único enganche;
- "muéstrame más" **recalcula** el resumen sobre las filas visibles de la nueva
  página. No se persiste ni se serializa ningún assessment; el buffer de
  paginación no cambia de contrato ni de fingerprint.

`result_identity` y `result_grouping` permanecen dormidos.

### Instante de referencia

`reference_instant` es inyectable y se usa **uno por invocación de formateo**,
nunca uno por fila. `format_grouped` obtiene uno solo y lo pasa a ambos grupos.
El observer nunca lee el reloj.

---

## KNOWN_CROSS_DATASET_LIMITATION

```text
CROSS_DATASET_LIMITATION — no corregido, fuera de alcance
```

M2 no consulta contratos para decidir el estado de un proceso. Un proceso que
sigue "Publicado" pero ya tiene contratos derivados (p. ej. el caso 338-2026) no
es detectable desde la fila de procesos y se clasifica como limitación conocida,
no como defecto de M2.

### Cobertura real del eje temporal (Caso C)

El eje temporal existe pero está casi ausente donde M2 lo necesitaría:

| Segmento | Filas con `fecha_de_recepcion_de` |
|---|---:|
| Seleccionado | 885.443 |
| Evaluación | 380.835 |
| Cancelado | 106.879 |
| Abierto | 27.439 |
| **Publicado** | **6.659** de 2.700.581 |

En `Abierto + Publicado` solo el **0,13%** trae fecha de recepción, y la mayoría
de esas la trae en el futuro. El campo se puebla mayoritariamente *después* de
que el proceso sale de recepción.

Consecuencia: el Caso C se materializa sobre todo en el segmento pequeño
`Abierto/Abierto` (4.309 filas, donde la fecha sí está poblada) — como se observó
en la muestra real — y es casi inexistente en `Publicado`. Se conserva como
fixture de borde determinista y **no se optimiza para él**.

---

## PERFORMANCE

Medición local en caliente, filas reales de SECOP, excluyendo red:

| Resultados visibles | Mediana | p95 | Gate |
|---|---:|---:|---|
| 25 | 1,99 ms | 2,28 ms | < 10 ms ✅ |
| 50 | 4,02 ms | **4,51 ms** | < 10 ms ✅ |

Margen de 2,2× sobre el gate. No se requirió optimización.

---

## TEST_EVIDENCE

### Corpus (idénticos al baseline de `ad1bb284`)

| | Baseline | Post-M2 |
|---|---|---|
| Query corpus | 89/89 | **89/89** |
| Dialogue corpus | 60/61 (GOB-DEPTO-001 basal) | **60/61 (mismo fallo basal)** |

### Suite completa

```text
Baseline M1 : 7396 passed,  2 skipped, 24 xfailed
Post-M2     : 7433 passed,  2 skipped, 24 xfailed

NEW_FAILURES = 0
NEW_SKIPS    = 0
NEW_XFAILS   = 0
```

Delta de +37 tests: 33 nuevos de M2 + 4 nuevos del guardián de paridad
reencuadrado.

### Casos obligatorios

| Caso | Cubierto |
|---|---|
| A — Abierto + Seleccionado | ✅ el estado principal no puede ser "Abierto" |
| B — Abierto + Evaluación | ✅ |
| C — Abierto + plazo vencido | ✅ fixture de borde; pierde el verde, conserva la etapa |
| D — recepción real | ✅ no se degrada, sin segunda línea redundante |
| E — evidencia insuficiente | ✅ fallback conservador, unknown no es categoría |
| F — cancelado | ✅ |
| G — borrador / en aprobación | ✅ |
| H — paginación | ✅ ambas páginas, recálculo sobre filas visibles |
| I — direct lookup | ✅ |
| J — dual dataset | ✅ contratos sin campos operativos ni cambios de texto |

### Guardián de paridad pública, reencuadrado

`test_truth_layer_public_parity.py` congelaba el producto porque el contrato de
M1 era `VISIBLE_PRODUCT_CHANGE = NO`. M2 autoriza una divergencia, así que
re-fijar los hashes a secas habría destruido el guardián. En su lugar la
divergencia se **acota**:

- quitar `estado_resumen` del SELECT restaura el oráculo `soql` pre-M2 byte a byte;
- quitar las claves `estado_op*` de las tarjetas restaura los oráculos `cards` y
  `tabular` pre-M2 byte a byte;
- `ranked`, `response`, `empty`, `buffer` y `page` **no se movieron** — ranking,
  orden, conteos y paginación intactos;
- `app/core/pagination_buffer.py`, `phrase_ranking.py`, `dual_dataset.py`, los
  handlers y `main.py` siguen byte-congelados;
- el Truth Layer sigue **dormido** al importar la aplicación (import perezoso):
  el test que lo verifica sigue pasando.

### Verificación por mutación

Los tests se validaron mutando el runtime, no solo comprobando que pasan:

| Mutación | Resultado |
|---|---|
| Campo extra en el SELECT | detectada (2 tests) |
| `unknown` convertido en categoría | detectada (Caso E) |
| Eje temporal deja de degradar el plazo vencido | detectada (Caso C) |
| Rama "Abierto" de la política de discrepancia | detectada (11 tests) |
| Rama "Cerrado" de la política de discrepancia | detectada |
| Rama catch-all de la política | **no detectada → hueco real, test añadido** |

El hueco encontrado (valor de apertura fuera del catálogo `{Abierto, Cerrado}`)
se cerró y la mutación pasó a ser detectada.

### Defecto encontrado con datos reales

Los fixtures sintéticos usaban `urlproceso` como cadena. SECOP lo entrega como
objeto `{"url": ...}`, y el transporte T1A del Truth Layer rechaza no-escalares:
el observer lanzaba `TypeError` sobre **toda** fila real. El best-effort del
formatter lo habría absorbido en silencio, dejando cada tarjeta de proceso sin
estado operativo — es decir, M2 no habría hecho nada en producción sin que
ningún test lo delatara.

Corregido en el consumidor (adaptar la forma del transporte es su
responsabilidad, no la del observer), sin mutar la fila original, con dos tests
de regresión que usan la forma real del dato.

---

## REAL_SAMPLE — §23

Muestra real reproducible, 27 procesos (3 por cada `estado_del_procedimiento` que
convive con `estado_de_apertura_del_proceso='Abierto'`), ordenada por
`id_del_proceso` para ser determinista.

```text
REAL_SAMPLE_CASES        = 27
CORRECTED_MISLEADING_RAW = 23
UNCHANGED_VALID_RAW      = 4
CONSERVATIVE_UNKNOWN     = 0
UNEXPECTED               = 0     ← gate cumplido
```

| Estado del procedimiento (raw = Abierto) | n | M2 operational state | Clasificación |
|---|---:|---|---|
| Seleccionado | 3 | `selected` — Seleccionado | CORRECTED_MISLEADING_RAW |
| Evaluación | 3 | `evaluation` — En evaluación | CORRECTED_MISLEADING_RAW |
| Cancelado | 3 | `cancelled` — Cancelado | CORRECTED_MISLEADING_RAW |
| Borrador | 3 | `draft` — Borrador | CORRECTED_MISLEADING_RAW |
| Aprobado | 3 | `approval` — En aprobación | CORRECTED_MISLEADING_RAW |
| Suspendido | 3 | `suspended` — Suspendido | CORRECTED_MISLEADING_RAW |
| En aprobación | 3 | `approval` — En aprobación | CORRECTED_MISLEADING_RAW |
| Publicado | 3 | `offer_phase_observed` — Recepción de ofertas | UNCHANGED_VALID_RAW |
| Abierto | 3 | `offer_phase_observed` (2 con plazo vencido) | 2 CORRECTED / 1 UNCHANGED |

Los dos casos con plazo vencido son Caso C observado en producción: el eje
temporal les retiró la afirmación de vigencia sin cambiarles la etapa canónica.

La muestra se usó solo para verificar la integración. **No se cambió ninguna
regla a partir de ella.**

---

## DEUDA REGISTRADA — no tocada en M2

### Duplicación preexistente de clasificadores

`app/core/opportunity_stage.py` (OPP-STAGE-001) clasifica la etapa de un proceso
con **otra taxonomía** (`offer_phase_indicated`, `post_offer_signal`,
`conflicting_signals`…) y está cableado como sidecar interno en
`normal_flow.py` (`result["opportunity_stage_shadow"]`, solo `opportunity_search`,
no visible al usuario).

Tras M2 conviven dos clasificadores de etapa. La duplicación **precede** a M2 y
por decisión de arquitectura se mantiene intacta y sin unificar en este bloque.
Queda registrada para un bloque posterior.

### STATE-PRIORITY-001 y el sesgo del conjunto de resultados

`soql_builder` fuerza `estado_de_apertura_del_proceso = 'Abierto'` en todo
`opportunity_search`. Ese WHERE selecciona el 87,5% del dataset: no filtra casi
nada y es el origen del sesgo. M2 **no lo toca** (prohibido y correcto).

Efecto visible anticipado: una búsqueda típica de oportunidades mostrará ahora
una mayoría de tarjetas "Seleccionado / SECOP reporta apertura: Abierto". Eso es
corrección, no regresión — pero es el argumento más fuerte para un bloque
posterior sobre el WHERE.

### SEARCH_INTELLIGENCE

No se tocó ningún defecto de interpretación de búsqueda durante M2.

---

## Archivos

```text
FILES_CREATED:
  app/core/process_operational_state.py          (kernel puro, ~200 líneas)
  tests/test_process_operational_state.py        (33 tests, casos A–J)
  docs/audits/2026-08-17_M2_PROCESS_OPERATIONAL_STATE.md

FILES_MODIFIED:
  app/core/soql_builder.py                       (+1 campo en el SELECT de procesos)
  app/core/formatter.py                          (costura + render del estado)
  app/core/response_policy.py                    (mismo estado visible en la lista asesor)
  tests/test_truth_layer_public_parity.py        (guardián reencuadrado y acotado)

UNAUTHORIZED_FILES: ninguno
PROCESS_OBSERVER_MODIFIED: NO
```

Ningún archivo nuevo de producción supera las 500 líneas. No se refactorizó el
formatter más allá de la responsabilidad del estado.

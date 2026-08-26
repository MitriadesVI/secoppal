# HCA-R6 — Control Language & Ordering Semantics

**Fecha:** 2026-08-25
**Rama:** `feature/hca-r6-control-language`
**Base:** `b661a09bfad5ecc180bab31aa9dc1a7e54d393aa`
**Estado:** `HCA_R6_CONTROL_LANGUAGE: PASS` · `CONTROL_LANGUAGE: CLOSED`

---

## 1. El defecto, en una frase

Una consulta mezcla dos lenguajes. **CONTENIDO** nombra lo que el usuario busca
(`obra`, `Cali`); **CONTROL** dice cómo presentarlo o navegarlo (`ordenados por
mayor valor`) y qué clase de pregunta es (`qué está contratando`). El sistema
solo consumía el *criterio* del orden (`mayor valor`) y dejaba el *comando* a su
alrededor (`ordenados`) en el residual, de donde el extractor temático lo tomaba
como objeto contractual.

Prueba viva sobre `b661a09`, hoy:

```text
Q: contratos de obra en cali ordenados por mayor valor
   objeto = ['ordenados']       ordering_signal = valor_desc
   SoQL contiene LIKE '%ORDENADOS%'
   filas devueltas por SECOP: 0
```

La misma consulta sin la cláusula de orden devuelve 50 filas. El sistema
convertía una consulta respondible en un vacío, y lo hacía silenciosamente.

---

## 2. Inventario de autoridades (estado previo)

| Concepto de control | Detector previo | Consumidor | ¿Fugaba a objeto? | ¿Mutaba scope? | Destino R6 |
|---|---|---|---|---|---|
| ORDER_BY_VALUE (desc) | `_ORDERING_VALOR_RE` en `query_router` | `soql_builder`, `order_semantics` | **Sí** — solo consumía el criterio | No | `control_language` |
| ORDER_BY_VALUE (asc) | — no existía | — | n/a | n/a | `control_language` + `soql_builder` |
| ORDER_BY_DATE (desc) | `_ORDERING_FECHA_RE` en `query_router` | `soql_builder` | **Sí** | No | `control_language` |
| ORDER_BY_DATE (asc) | — no existía | — | n/a | n/a | `control_language` + `soql_builder` |
| Verbo imperativo suelto (`ordénalos`) | `_ORDER_CONTROL_RE` (solo turnos completos de follow-up) | `followup_control_tokens` | **Sí** en turno único | No | `control_language` |
| TOP_N | — no existía | — | No (token corto) | No | reconocido, **no aplicado** (§8) |
| SHOW_MORE | `_PAGINATION_CONTROL_RE` | `followup_engine` | No | No | sin cambios |
| NEW_SEARCH | `NEW_SEARCH_VERBS` | `followup_engine` | No | n/a | sin cambios |
| DATASET_INTENT | `_DATASET_CONTROL_RE` | `followup_control_tokens` | No | Sí (por diseño) | sin cambios |
| Verbos de intención (`contratando`, `piensa contratar`) | — ninguno | — | **Sí** | No | `control_language` |
| Verbo de deadline (`cierran`) | — ninguno | — | **Sí** | No | `control_language` |
| Etiqueta de orden en cabecera | inlined ×3 (`formatter`, `response_policy`, `pagination`) | UI | n/a | n/a | `order_semantics.ordering_label` |

Dos autoridades desalineadas explicaban el defecto de follow-up: el parser
reconocía un vocabulario de orden y `FollowupClassifier._has_ordering_signal`
reconocía otro, más estrecho. `los más antiguos primero` no estaba en ninguno.

---

## 3. Diseño

### 3.1 Una autoridad, un span

`app/core/control_language.py` (286 LoC) recibe el residual positivo y devuelve
un `ControlRequest` con la señal de orden y el residual ya sin los spans
consumidos. No hay lista global de palabras prohibidas: el reconocimiento es
por span y contextual.

```text
query_router.parse
   │  (residual positivo, tras negación/modalidad/tipo/fecha)
   └── control_language.interpret_control_language
           ├── verbos de intención  → span consumido
           ├── TOP N                → span consumido (cardinalidad NO aplicada)
           ├── criterio de orden    → ordering_signal
           │      └── expansión del span sobre el comando que lo introdujo
           └── imperativo incompleto → span consumido, sin inferir dirección
   ⇣
   ordering_signal → soql_builder (ORDER BY) · order_semantics (gate H4 + etiqueta)
                   → followup_engine (change_order) · negation_binding (frontera)
```

### 3.2 Expansión del span

El criterio se localiza primero; luego el span se ensancha hacia la izquierda
en dos pasadas. La primera absorbe pegamento puro (`por`, `de`) — siempre
seguro. La segunda camina sobre artículos y sustantivos de relleno pero **solo
se compromete** si alcanza un imperativo real de «ordenar» o un ancla `primero`.
Un criterio sin comando alrededor nunca se traga contenido vecino.

```text
"ordena los contratos por mayor valor"  → span completo, residual vacío
"contratos de obra ... ordenados por mayor valor" → span = "ordenados por mayor valor"
"que contratos de mayor valor tiene la alcaldía"  → span = "de mayor valor"
```

### 3.3 Direcciones, y por qué el orden de las reglas importa

`de menor a mayor valor` contiene la subcadena `mayor valor`. Leída de un solo
lado invierte la intención del usuario — el mismo patrón
`NORMALIZATION_ERASES_USER_INTENT` documentado en HCA-R2. Por eso las parejas
direccionales explícitas (nivel 1) se leen antes que cualquier criterio de un
solo lado (nivel 2), y dentro del nivel 2 el vocabulario ascendente se prueba
antes que el descendente, que es más amplio por herencia.

`ordering_signal` gana dos valores nuevos: `valor_asc` y `fecha_asc`.
`soql_builder` los proyecta a `ASC`, y el desempate estable por identificador
(Etapa 8A) sigue la dirección pedida — un `ASC` desempatado por `id DESC`
recorre el eje del id hacia atrás mientras la paginación avanza.

### 3.4 Falsos positivos: qué NO se toca

La morfología imperativa de «ordenar» (`ordena`, `ordénalos`, `ordenados`) no
tiene lectura nominal, y los **sustantivos** de la misma familia (`orden`,
`órdenes`, `ordenamiento`) no coinciden con ninguna alternativa. Por eso:

| Frase | Resultado |
|---|---|
| `contratos de ordenamiento territorial` | intacta, sin `ordering_signal` |
| `sistemas de control de acceso` | intacta |
| `gestión de órdenes de compra` | intacta |
| `listado de precios de aseo` | intacta |
| `contratos de primeros auxilios` | intacta (`primeros` no final) |
| `selección abreviada de menor cuantía` | intacta — **`menor cuantía` es una MODALIDAD**, nunca una dirección de orden |
| `por valor de 500 millones` | filtro monetario, sin orden |
| `los últimos 30 días` | ventana temporal, sin orden |

`cuantía` quedó fuera del vocabulario **ascendente** por esa razón; sigue en el
descendente, donde `mayor cuantía` no nombra ninguna modalidad.

### 3.5 Verbos de control

`está contratando`, `piensa/planea/va a contratar` y `cierran` seleccionan el
modo de la pregunta, no su sujeto. Se consumen como span, conservando el
complemento temático: `qué está contratando la alcaldía **en mantenimiento**`
mantiene `mantenimiento` en `objeto`.

Sobre `cierran`, R6 garantiza **solo** que no es un tema contractual. La
semántica de deadline no se tocó: el comportamiento conservador vigente (dataset
`procesos`, ventana temporal de `hoy`) se conserva tal cual. No se abrió un
sprint de deadlines.

### 3.6 Frontera de comando en la negación

El span negativo de R4 corre hasta el siguiente marcador o frontera fuerte, así
que se tragaba la cláusula de orden final: `sin alimentación ordenados por mayor
valor` excluía el objeto **y la palabra `ordenados`**, y el orden nunca llegaba
al builder. R6 añade el comando como frontera del objetivo negado, y solo cuando
el span es **explícitamente encabezado por comando** (imperativo, ancla
`primero`, o pareja direccional). Un adjetivo suelto sigue siendo contenido:
`sin obras nuevas` conserva exactamente el comportamiento previo, verificado
contra `b661a09`.

---

## 4. Invariantes

| Invariante | Estado | Dónde se fija |
|---|---|---|
| `INV-CONTROL-LANGUAGE-NOT-CONTENT` | PASS | `tests/test_control_language.py`, corpus focal |
| `INV-CONTROL-SPAN-DOES-NOT-CLASSIFY-CONTENT` | PASS | 5 casos de lectura nominal + 8 de vocabulario ambiguo |
| `INV-ORDERING-FOLLOWUP-PRESERVES-UNIVERSE` | PASS | comparación de 18 campos del universo + comparación del `WHERE` generado |
| `RAW_QUERY_PRESERVED` | YES | `test_raw_query_is_preserved`; el enmascarado ocurre sobre el residual, nunca sobre `user_query` |
| `CONTROL_ONLY_FOLLOWUP_CAN_ACTIVATE_ENTITY_ECOSYSTEM = FALSE` | PASS | `test_ordering_followup_does_not_move_the_entity_ecosystem` |
| `CONTROL_VERB_CANNOT_BECOME_TOPIC` | PASS | verbos de intención + imperativo incompleto |

La comparación del universo es más fuerte que comparar frames: se compara el
`WHERE` que produce `soql_builder` antes y después del turno de orden, y debe
ser idéntico carácter por carácter.

---

## 5. Evidencia

### 5.1 HCA congelado

Corpus `conversation_holistic_v1.yaml`, sha `262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`
— **sin editar**, sha idéntico antes y después.

```text
antes (b661a09):  171 PASS / 24 FAIL / 0 ERROR de 195
después (R6):     175 PASS / 20 FAIL / 0 ERROR de 195
```

Diferencial caso por caso, no por el neto:

| Caso | Antes | Después | Qué cambió |
|---|---|---|---|
| `HCA-INTENT-007` | FAIL — `objeto=['contratando']` | **PASS** | verbo de intención consumido |
| `HCA-INTENT-008` | FAIL — `objeto=['piensa']` | **PASS** | verbo de intención consumido |
| `HCA-STATE-006` | FAIL — `objeto=['cierran']` | **PASS** | verbo de deadline consumido |
| `HCA-ORDER-004` | FAIL — `ordering_signal=None` | **PASS** | `top 10 … por valor` → `valor_desc` |
| `HCA-FOLLOWUP-016` | FAIL — `objeto=['antiguos','primero']` | FAIL (otra razón) | ver §7 |

**Rotos: 0.** `HCA-ORDER-001` (el caso histórico) ya pasaba porque solo verifica
`ordering_signal`; su fuga a `objeto` no estaba cubierta por el oráculo y por eso
el defecto sobrevivió al corpus. El corpus focal de R6 sí la cubre.

Ambas corridas se hicieron con el extractor LLM caído (HTTP 402, saldo agotado)
y por tanto en modo regex-only. La comparación es válida porque la condición fue
idéntica en ambos lados y R6 es trabajo determinístico sin LLM.

### 5.2 Corpus focal R6

```bash
make audit-control-language-v1
```

`tests/fixtures/audits/control_language_v1.yaml` — 42 casos, **42 PASS / 0 FAIL**,
sha `b082f2d86952c28ce17d75d95b25c31706b147f857c126f82ef312b4b1501aba`.
Pre-registrado antes del gate. Reutiliza el runner aislado; no se creó otro.

### 5.3 Adversarial

Dos barridos generados, 516 comprobaciones sobre el producto cartesiano de
20 formas de control × 8 semillas de contenido reales (dinero, tiempo, ciudad,
entidad, modalidad, negación, actor), en turno único y en follow-up:

| Clase buscada | Hallazgos |
|---|---|
| `CONTROL_TO_TOPIC_LEAK` | 0 |
| `CONTROL_TO_ACTOR_LEAK` | 0 |
| `CONTROL_TO_GEO_LEAK` | 0 |
| `CONTROL_TO_ENTITY_ECOSYSTEM` | 0 |
| `ORDERING_UNIVERSE_MUTATION` | 0 |
| `MONEY_ORDER_CONFUSION` | 0 |
| `TIME_ORDER_CONFUSION` | 0 |
| `NEW_SEARCH_MISCLASSIFICATION` | 0 |
| `SHOW_MORE_REGRESSION` | 0 |
| `WRONG_ORDERING` (dirección invertida) | 0 |
| `CONTENT_LOST` | 0 |

La primera pasada del barrido encontró dos defectos reales que se corrigieron
antes de cerrar: `menor cuantía` leída como orden ascendente (modalidad), y el
imperativo incompleto (`ordena`, `ordénalos`, `muéstrame primero`) que seguía
llegando a `objeto` por no llevar criterio.

### 5.4 Fuente viva

Reproducción del caso cero histórico contra Socrata (`www.datos.gov.co`),
mismo día, ambas ramas:

| | `b661a09` | R6 |
|---|---|---|
| `LIKE '%ordenados%'` en el SoQL | **sí** | no |
| filas devueltas | **0** | **50** |
| primeros valores | — | 2.24e16, 226.902M, 170.323M, 132.013M… (desc estricto) |
| ciudades en la página | — | solo `Cali` |

Direcciones nuevas, verificadas contra la fuente:

- `de menor a mayor valor` → primeros valores `0.0` (asc real);
- `los más antiguos primero` → `2017-12-25, 2018-03-05, 2018-03-23, 2018-03-28…`
  (asc estricto por `fecha_de_firma`).

`LIVE_HARD_FILTER_VIOLATIONS: 0` para las cláusulas que R6 toca. Nota honesta:
en una de las páginas aparece `Calima` bajo un filtro de `Cali` — es el
comportamiento de subcadena del predicado `ciudad LIKE '%Cali%'`, reproducible
idéntico en `b661a09` con una consulta sin lenguaje de control. No lo introduce
R6 y no se tocó.

### 5.5 Suite y no-regresión

```text
pytest tests/            8554 passed, 2 skipped, 24 xfailed
query corpus             89/89 PASS
dialogue corpus          60/61 PASS (único FAIL GOB-DEPTO-001, basal)
make audit-negation-v1   22/24 — idéntico a b661a09, mismos dos casos
make audit-time-v2       32/32
make lint-core           limpio
git diff --check         limpio
ruff (archivos tocados)  0 hallazgos nuevos frente a b661a09
```

`audit-negation-v1` se remidió sobre un worktree limpio de `b661a09`: 22/24 con
exactamente `NEG-N15` y `NEG-ANTI-002`, la deuda de oráculo ya registrada.
Regresiones en negación: **0**.

---

## 6. QueryPlan

`app/core/query_plan.py`: **no tocado**. 299 LoC antes y después.
`NEXT_TOUCH_RULE` respetada sin necesidad de invocarla: la responsabilidad de
R6 es lingüística y de proyección, y ninguna de las dos vive en el plan.

---

## 7. Hallazgos registrados, no corregidos

### `DEFERRED_CONTROL_LIMIT` — `top N` no aplica cardinalidad

`params["limit"]` **no** es la cardinalidad de display: es el tamaño del pool de
candidatos que se pide a SECOP (50, o 200 con frase recall-first), del que
después se valida y rankea. Fijarlo en 10 por un `top 10` encogería el pool
antes de validar y cambiaría *qué* filas sobreviven, no cuántas se muestran.
Aplicarlo bien exige un concepto separado de `display_limit` atravesando
formatter y paginación — arquitectura amplia, fuera de R6 por su propia regla.

R6 sí reconoce y consume el span (`top 10` no puede contaminar nada) y sí aplica
el orden que lo acompaña, que es lo que `HCA-ORDER-004` exigía.

### `DEFERRED_FOLLOWUP_SCOPE_TOPIC` — congelado como `xfail(strict=True)`

Un follow-up clasificado `change_scope` que trae un tema genuinamente nuevo
pierde ese tema: la política de mutación de `change_scope` no admite campos de
topic, el gate lo rechaza y el merger hereda el tema anterior.

```text
T1: contratos de mantenimiento en cali
T2: ahora muéstrame contratos de software en bogotá
    → objeto = ['mantenimiento']   (software se pierde)
```

Verificado **idéntico en `b661a09`**: no es una regresión de R6, y el parser
entrega `objeto=['software']` correctamente. Pertenece a la higiene de topic del
follow-up. Congelado en `tests/test_control_language.py` como `xfail(strict=True)`
según el ritual del proyecto.

### `ORACLE_REVIEW` — `HCA-FOLLOWUP-016` y `HCA-FOLLOWUP-007`

Ambos exigen `objeto_has: [obra]`, pero desde SEARCH-SH1 `obra` se proyecta a
`contract_type_include`, no a `objeto`. El oráculo es insatisfacible por
construcción, no por defecto del runtime. `HCA-FOLLOWUP-007` ya fallaba así en
la línea base; `HCA-FOLLOWUP-016` migró a esa misma causa. Su comportamiento R6
es el correcto y está verificado:

```text
T2 'los mas antiguos primero' → ordering_signal=fecha_asc, valor_min=300000000,
   ciudad=Cali, contract_type_include=['obra'], objeto sin contaminar
```

El corpus HCA **no se modificó**. Corregir estos dos oráculos es trabajo de
`ORACLE_REVIEW`.

### Limitación acotada — orden no encabezado por comando tras negación final

`contratos de mantenimiento sin alimentación los más antiguos primero`: el orden
no se reconoce porque el span negativo llega hasta el final y la frase de orden
no está encabezada por comando, que es la condición conservadora que protege
`sin obras nuevas`. 4 de 180 combinaciones del barrido. Antes de R6 fallaban las
15 y además contaminaban `objeto_exclude`; el saldo es estrictamente mejor.

---

## 8. Archivos

**Nuevos**

- `app/core/control_language.py` — la autoridad (286 LoC).
- `tests/test_control_language.py` — 72 tests + 1 `xfail(strict)`.
- `tests/fixtures/audits/control_language_v1.yaml` — 42 casos pre-registrados.

**Modificados**

- `app/core/query_router.py` — paso 4 delega en la autoridad; se eliminan las dos regex de orden.
- `app/core/soql_builder.py` — dos ramas `ORDER BY` nuevas + desempate direccional.
- `app/core/order_semantics.py` — `ordering_label()`, autoridad única de la etiqueta.
- `app/core/formatter.py`, `app/core/response_policy.py`, `app/core/workflow/handlers/pagination.py` — delegan la etiqueta.
- `app/core/followup_engine.py` — `_has_ordering_signal` delega; el colapso dual no lee un comando como dataset.
- `app/core/followup_control_tokens.py` — `is_pure_control_turn` reemplaza la regex local.
- `app/core/negation_binding.py` — el comando es frontera del objetivo negado.
- `Makefile` — `audit-control-language-v1`.
- `tests/test_truth_layer_public_parity.py` — re-pin acotado (ver abajo).
- `tests/test_followup_mutations.py` — una aserción de mecanismo pasa a ser de resultado.

**Sobre el re-pin de parity.** Cuatro archivos con hash fijado cambian. Tres
estaban ya autorizados a divergir bajo M2; el cuarto, `pagination.py`, estaba
*byte-frozen* y se re-fija explícitamente en su propio bloque con la razón: la
cabecera de paginación llevaba la tercera copia de la etiqueta de orden y, sin
tocarla, habría anunciado «más recientes» sobre una página ordenada de más
antiguo a más reciente — una afirmación falsa sobre las filas en pantalla. Los
oráculos estructurales del mismo módulo —los que comparan SoQL, ranking, conteos
y paginación byte a byte— pasan sin cambios, y son ellos los que acotan la
divergencia. Ningún cursor, buffer ni fingerprint se tocó.

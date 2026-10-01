# A3 — Foundation Gap Matrix (preliminar, sin Gold)

Fecha: 2026-09-30. Misión `M1_SEARCH_FOUNDATION_V1 — PHASE A`, carril A3. Base: `main` @ `4ec99bc1`.
Autoridad de lenguaje: `M1_0_SEARCH_CONTRACT.md`. Este documento **no contiene texto de consultas
reales**: sólo `gold_id` y agregados. Los datos con texto viven en
`~/Documents/tools/secoppal-diagnostics/m1_search_gold/captures/` (fuera de git).

**Estado:** esto es la matriz «sin Gold». Mide dónde se pierde el significado **etapa por etapa**
usando a legacy sólo como *referencia de contraste*, nunca como verdad (§1 del contrato). Un
«F READY» aquí significa «Foundation ejecutaría algo», no «ejecutaría lo correcto». La matriz
GOLD/LEGACY/FOUNDATION con la etapa culpable por condición se genera con el joiner en cuanto llegue
el Gold (§6).

## 1. Qué se capturó y cómo

620 entradas (REAL 585 = `real_a` 293 + `real_b` 292; SYNTHETIC 35), clave `gold_id`. Una sola pasada.

| Etapa | Qué corre | Red |
|---|---|---|
| ADMISSION_GATE | `first_search_ineligibility` + `specialized_owner` (como `probe.py`; sin historial, sin seed, Natural Alpha OFF) | ninguna |
| PRODUCER | llamada REAL a DeepSeek con el *mismo* closure del producto (`configured_producer(...).provider`, prompt v4.2 congelado, `temperature 0`, `max_tokens 2400`, `timeout 15`), **sobre las 620 aunque la puerta no las admita**; máx. 4 concurrentes; 1 reintento sólo ante error de transporte (nunca se usó); crudo del proveedor guardado | sólo `api.deepseek.com` |
| HOST | *replay* determinista desde el crudo: decode → H7 (`host_universe`) → `admit_proposal` → `project_accepted` (sin ejecutar). Además, por condición: admisión aislada y diagnóstico de proyección culpable | ninguna |
| LEGACY | `run_query` con Foundation OFF, sin clave LLM (heurístico), sobre fuente congelada vacía (`RecordingSource`, como `legacy_oracle.py`); se guardan `resolved_params`, SoQL emitido y dimensiones normalizadas | ninguna |

Salvaguardas: sockets bloqueados salvo el host de la base_url de DeepSeek (0 intentos bloqueados en
todas las corridas; datos.gov.co nunca se tocó); Burr off; `Settings()` cargado de `.env` con
`secop_feedback_path`, `secop_feedback_db_path`, `ConversationStore` (incl. `CONVERSATION_STORE_DIR`)
y alias de actores redirigidos a `captures/run/`; fecha fija 2026-09-30 (instante de referencia
12:00 America/Bogotá).

**Verificación del replay:** sobre las 270 entradas admitidas se ejecutó el camino real
(`run_query` en `foundation_v3` con el producer sustituido por el crudo grabado). Coinciden
270/270 en `foundation_terminal`, razón y SoQL emitido. El host del joiner es, por tanto, el del
producto, no una réplica aproximada.

Reproducir: `PYTHONPATH=$PWD .venv/bin/python scripts/audits/m1_search/capture_foundation_legacy.py --stage {det|producer|host|merge|stability|all}`
(cada etapa es reanudable; `host`/`merge` no usan red y regeneran todo desde el crudo).

Limitaciones declaradas: (i) gate sin historial → turnos de seguimiento se ven como primer mensaje;
(ii) Natural Alpha OFF (convención de M0); (iii) legacy sin LLM (12 entradas rutan
`heuristic_plus_llm` y aquí quedaron en su rama heurística); (iv) el LLM no es determinista pese a
`temperature 0` (§4): los conteos por etapa tienen ruido de orden ±1–3 %.

## 2. Distribución por etapa (620 entradas, sin Gold)

| Etapa donde termina la entrada | n | % | de ellas con puerta abierta |
|---|---:|---:|---:|
| Foundation ejecutaría (READY) y la puerta la admite | 117 | 18.9 | 117 |
| `GATE_EXCLUDED_F_READY` — Foundation ejecutaría, la puerta la deja fuera | 43 | 6.9 | 0 |
| Host pide aclarar (`NEEDS_CLARIFICATION`) | 159 | 25.6 | 76 |
| Host no soporta (`UNSUPPORTED`) | 171 | 27.6 | 33 |
| Proyección no soporta (admisión READY, falla `project_accepted`) | 57 | 9.2 | 44 |
| El productor no propone solicitud (`NO_ACTION`) | 73 | 11.8 | 0 |

- **Puerta** (270 admitidas = 43.5 %): 280 `INELIGIBLE_LEXICAL`; 70 vetadas por dueño (ACTOR 68,
  ANALYTICS 2). Dueño detectado con independencia del léxico: ACTOR 114, ENTITY_PROFILE 12,
  CONTRACT_ACTIVITY 3, ANALYTICS 2. Entre las 270 admitidas sólo **117 (43 %)** llegan a ejecutar.
- La puerta deja fuera **43 de las 160** entradas ejecutables de Foundation (27 %): 41 léxicas + 2 por ACTOR.
- Los 73 `NO_ACTION` son fragmentos/botones/seguimientos («y …», «en …», «opción …», «reset», emojis
  de menú): todos `INELIGIBLE_LEXICAL`; el productor no inventa solicitud (comportamiento correcto
  para M2, no es un hueco de SEARCH).
- Razones de admisión (por entrada): `UNRESOLVED_HOST_DIMENSION` GEOGRAPHY 30, BUYER 28, STATE 16,
  TIME 6, CONTRACT_TYPE 4, VALUE 3, MODALITY 2; `VALUE_OPERATOR_*` 33; `TOPIC_ONLY/OVERLAPS_REQUEST_UNIVERSE`
  37 (todas fuera de la puerta); `UNSUPPORTED_CONDITION` 159; `UNACCREDITED_MODALITY_FAMILY` 6;
  `UNSUPPORTED_OPERATION` 5 (COUNT 5 / SUM_VALUE 2 propuestos); `MULTI_REQUEST` 1.
- Proyección (217 con admisión READY → 57 caen): `BUYER_SOURCE_PROJECTION_REQUIRED` 41,
  `CONFLICTING_PROJECTION_KEY` 4, `NONCONJUNCTIVE_EXPRESSION` 4, `SOQL_PROJECTION_BLOCKED` 4,
  `UNSUPPORTED_OPPORTUNITY_OVERRIDE` 3, `UNSUPPORTED_CONDITION` 1.
- H7: 5 propuestas `records→opportunities` corregidas por el host (O3 20, O3-INTENT 14, O6A 5,
  O6B 6, O7 9 reglas disparadas).

## 3. Huecos por dimensión (condiciones propuestas por Foundation; contraste con legacy)

Embudo por condición (la admisión se mide **aislada** por condición; «e2e» = admisión + proyección):

| Dimensión | propuestas | host READY | proyección no soporta | e2e | observación |
|---|---:|---:|---:|---:|---|
| TOPIC | 263 | 220 (84 %) | 0 | 220 | 41 aclaraciones estructurales (tema = sustantivo de universo); legacy ve 303 |
| BUYER | 189 | 148 (78 %) | **54** | **94 (50 %)** | `BUYER_SOURCE_PROJECTION_REQUIRED`: el comprador se resuelve pero no tiene binding de nombres físicos (sidecar ADR-073); 37 sin resolver |
| GEOGRAPHY | 163 | 123 (75 %) | 0 | 123 | sólo departamentos; 40 sin resolver (municipio/región): legacy ve 54 municipio + 9 región |
| TIME | 197 | 183 (93 %) | 3 | 180 | todas `during_year`; legacy ve 23 mes + 20 rango (ver §3.1) |
| VALUE | 119 | 71 (60 %) | 1 | 70 | 42 rechazos de operador (§3.2); sólo `>`/`<` exclusivos |
| STATE | 56 | 37 (66 %) | 0 | 37 | host sólo resuelve abierto/publicado/terminado; 18 sin resolver (cerrado, adjudicado, en ejecución…) |
| MODALITY | 9 | 0 | 0 | **0** | 6 `UNACCREDITED_MODALITY_FAMILY` (selección abreviada, mínima cuantía…), 3 sin resolver; legacy ve 23 |
| CONTRACT_TYPE | 5 | 0 | 0 | **0** | todas sin resolver; legacy ve 12 (7 «convenio») |
| UNSUPPORTED | 175 | 0 | — | 0 | 116 tipo ACTOR, 11 tipo ORDEN, 10 tipo TIEMPO, 38 otros (en su mayoría nombres propios) |
| ACTOR / ORDER | no existen | — | — | — | `IR_CANNOT_EXPRESS`: legacy extrae ACTOR en 114 entradas y ORDER en 33 |

Cuando legacy ve una dimensión, resultado de Foundation sobre esas entradas:

| Legacy ve … | n | Foundation termina en |
|---|---:|---|
| ACTOR | 114 | UNSUPPORTED_CONDITION 94, aclara BUYER 11, NO_ACTION 4, READY 3, otras 2 |
| ORDER | 33 | UNSUPPORTED_CONDITION 20, `VALUE_OPERATOR_UNGROUNDED` 9, NO_ACTION 3, TIME 1; **0 READY** (no se pierde en silencio) |
| MUNICIPIO | 54 | READY 13; aclara GEOGRAPHY 17; resto estructural/otras |
| REGIÓN | 9 | READY 0 |
| TIME mes/rango | 43 | READY 7; UNSUPPORTED 10; NO_ACTION 8; aclara TIME 6; `CONFLICTING_PROJECTION_KEY` 4 |
| BUYER | 179 | READY 53 (30 %); `BUYER_SOURCE_PROJECTION_REQUIRED` 40; UNSUPPORTED_CONDITION 33; aclara BUYER 14 |
| VALUE | 106 | READY 39; `VALUE_OPERATOR_UNGROUNDED` 20; resto otras |
| MODALIDAD | 23 | READY 9; UNACCREDITED 6 |
| Vista oportunidad | 85 | READY 40 |

### 3.1 Candidatos a pérdida silenciosa (para triage con el Gold, no veredictos)

Entradas con `READY` donde legacy ve una dimensión que Foundation no propuso ni marcó UNSUPPORTED:
**TIME_NARROWED_TO_YEAR 7** (legacy ve mes/rango, Foundation cita sólo el año → ejecuta el año
completo; 7 admitidas), STATE 21 (13 admitidas; muchas son «convocatorias»/oportunidad que ya implica
abierto), MODALITY 9, CONTRACT_TYPE 5, ACTOR 3, TOPIC 2. En el cruce temporal hay además 19 entradas
con mes y 13 con rango donde Foundation propone sólo `during_year` (el resto lleva UNSUPPORTED o no
ejecuta). Es el único patrón del conjunto en que el producto podría **ensanchar** la búsqueda sin
decirlo (ADR-069). El Gold decide si legacy tenía razón en cada caso.

### 3.2 Rechazos del operador VALUE (42 condiciones; guardia `foundation_value_direction`)

El productor mapea a `greater_than`/`less_than` lenguaje que el IR no puede decir; el host lo frena
(correcto, sin pérdida silenciosa), pero la entrada no se atiende: rango «entre X y Y» 12;
lenguaje de orden («más caros», «mayor valor») 11; léxico de comparador fuera del cerrado
(«debajo de», «encima de», «en adelante», «mas 500 millones» sin «de») 8; inclusivos («o más», «a lo
sumo», «no menores») 8; otros 3. Es la evidencia más directa de **IR_CANNOT_EXPRESS** (inclusivos,
rango, ORDER) y de **F_PROPOSED_HOST_UNRESOLVED** (léxico del host más estrecho que el del usuario).

### 3.3 Foundation vs legacy sobre 547 entradas donde Foundation propone solicitud

| Dimensión | ambos | sólo Foundation | sólo legacy |
|---|---:|---:|---:|
| TOPIC | 245 | 10 | 39 |
| BUYER | 165 | 21 | 8 |
| GEOGRAPHY | 151 | 11 | 2 |
| TIME | 186 | 4 | 5 |
| VALUE | 99 | 12 | 0 |
| STATE | 50 | 4 | 42 |
| MODALITY | 8 | 1 | 15 |
| CONTRACT_TYPE | 4 | 1 | 8 |
| ACTOR | 0 | 0 | 110 |
| ORDER | 0 | 0 | 30 |

Universo: discrepancia 51/547 (sujeto proceso/contrato). Vista oportunidad: sólo Foundation 27, sólo
legacy 43 (legacy convierte «licitación(es)» coloquial en LP/oportunidad; contrato D3: defecto de legacy).
«Sólo legacy» en STATE/MODALITY refleja en parte que legacy fabrica estado/modalidad desde
«licitaciones/convocatorias» (DA-2bis) — el Gold lo separa.

## 4. Productor: latencia, errores, estabilidad

- Latencia (intento final, 620 llamadas): **p50 1626 ms, p90 1952 ms, máx 3009 ms**. Sin reintentos.
- **PROVIDER_ERROR 0/620, INVALID_PROPOSAL 0/620** (0 %): el prompt v4.2 produjo JSON decodificable
  en todas, incluidas 73 no-SEARCH (devuelve `requests: []` → `NO_ACTION`).
- Modelo reportado por el proveedor: `deepseek-flash` (alias de `deepseek-chat`).
- **Estabilidad** (muestra fija de 40 = 30 REAL + 10 SYNTHETIC, orden por sha256 de `gold_id`; segunda
  pasada idéntica configuración): texto crudo idéntico 23/40 (57.5 %); propuesta canonizada cambia
  **3/40 (7.5 %)** — R0201 (sin solicitud ↔ GEOGRAPHY), R0317 (un TOPIC vs dos TOPIC), R0007
  (`records` ↔ `opportunities`); estado de producción cambia **1/40 (2.5 %)** (R0201:
  `NO_ACTION` ↔ `NEEDS_CLARIFICATION`); status del productor cambia 0/40. El resultado de producción de R0007 no cambia.
  Latencia mediana 1686/1747 ms. Implicación: ni siquiera con `temperature 0` el LLM es una función; el
  Gold sellado necesita al menos 2 corridas para separar error sistemático de varianza (≈ 5–8 %).

## 5. Lectura (qué dice y qué no dice)

1. El cuello de Foundation no es comprensión sino **admisión + proyección**: 0 errores de contrato,
   pero sólo 18.9 % de las entradas terminan ejecutando con puerta, 25.8 % sin ella. La parte
   recuperable sin tocar el IR es la de `BUYER_SOURCE_PROJECTION_REQUIRED` (41 entradas, 6.6 %),
   `GATE_EXCLUDED_F_READY` (43, 6.9 %) y los clarifies estructurales de TOPIC (37, 6 %).
2. Lo que **requiere productor nuevo (v5)** (contrato §10): ACTOR (114 entradas), ORDER (33), TIME
   mes/rango (43), inclusivos/rango de VALUE (~20), GEOGRAPHY municipio/región si se quiere distinguir
   nivel, `UNSPECIFIED`. Foundation hoy los resuelve de forma **segura** (UNSUPPORTED/aclaración), salvo el
   patrón de §3.1.
3. Lo que **sólo el Gold puede decir**: la paridad (regresiones exclusivas), si «sólo legacy» es acierto o
   defecto D3, y cuántas de las 117 ejecuciones son correctas.

## 6. Cómo correr el joiner cuando llegue el Gold

```
.venv/bin/python scripts/audits/m1_search/gap_matrix.py \
  --captures ~/Documents/tools/secoppal-diagnostics/m1_search_gold/captures \
  --gold ~/Documents/tools/secoppal-diagnostics/m1_search_gold/labels_A \
  --out-json docs/audits/m1_search_foundation/A3_GAP_MATRIX.json \
  --out-md   docs/audits/m1_search_foundation/A3_GAP_MATRIX.md
```

- Stdlib only; valida cada registro con `gold_schema.validate` y se detiene si hay errores. Acepta
  directorios (`*.jsonl`) o archivos. Sin `--gold` produce esta distribución sin Gold.
- Por condición Gold asigna **etapa culpable** con las categorías exactas: `IR_CANNOT_EXPRESS`
  (vocabulario del prompt v4.2: sólo `TIME/during_year`, `VALUE/greater_than|less_than`, sin ACTOR/ORDER/
  `UNSPECIFIED`), `F_NOT_PROPOSED` (ausente, otra dimensión/polaridad/dirección/valor, o productor sin
  propuesta), `F_PROPOSED_HOST_UNRESOLVED` (admisión aislada ≠ READY), `HOST_RESOLVED_PROJECTION_UNSUPPORTED`
  (culpable del diagnóstico de proyección), `GATE_EXCLUDED` (intrínsecamente OK pero la puerta no admite),
  `OK`; legacy `L_OK|L_MISSING|L_WRONG` (+ `L_SPURIOUS` para condiciones que el Gold no tiene) con detalle
  (`POLARITY`, `DIRECTION`, `LEVEL:*`, `BOUNDARY_INCLUSIVE`, `PARTIAL_TOPIC`…).
- Salida: matriz por fila de dimensión (`GEOGRAPHY/<level>`, `TIME/<op>`, `VALUE/<op>`, `ORDER/<op>`,
  `UNIVERSE_VIEW`, `LOGIC_OR_NOT`, `OPERATION/<op>`), por clase de operación (`SEARCH:PROCESSES|CONTRACTS|OPPORTUNITIES|UNSPECIFIED|DUAL`
  y las no-SEARCH), nivel entrada, espurios, tabla de disposición (CLARIFY/DECLINE y operaciones no-SEARCH) y
  **paridad**: `regressions_legacy_ok_foundation_not` (lista `gold_id`+fila+etapa culpable; es la lista de
  regresiones exclusivas) y su recíproco. Las vistas `FOUNDATION` (con puerta) y `FOUNDATION_INTRINSIC`
  (sin puerta) conviven; la diferencia es exactamente `GATE_EXCLUDED`.
- Reglas que el Gold debe saber que se aplican: registros `AMBIGUOUS` se omiten; las condiciones sólo se
  miden para `operation` SEARCH (y jobs SEARCH de MULTI) con `expected_disposition EXECUTE`; el resto va a
  la tabla de operación/disposición. Los inclusivos de legacy (`valor_min` es `>=`) cuentan `L_OK` con
  detalle `BOUNDARY_INCLUSIVE` cuando el Gold dice `greater_than`.
- `--selftest` corre 14 fixtures sintéticos que cubren cada categoría de Foundation. Probado además con un
  mini-gold de 15 casos SINTÉTICOS (S-ids; fuera del repo) validado por `gold_schema.py`: el joiner de punta
  a punta asigna las categorías esperadas (p. ej. ACTOR → `IR_CANNOT_EXPRESS` con proxy, BUYER →
  `HOST_RESOLVED_PROJECTION_UNSUPPORTED:BUYER_SOURCE_PROJECTION_REQUIRED`, municipio →
  `F_PROPOSED_HOST_UNRESOLVED`). No se leyó ninguna etiqueta real.

## 7. Artefactos

- Worktree: `scripts/audits/m1_search/capture_foundation_legacy.py`, `scripts/audits/m1_search/gap_matrix.py`,
  este documento y `A3_GAP_MATRIX_PRELIM.json` (agregados, ids sólo).
- Privado (`~/Documents/tools/secoppal-diagnostics/m1_search_gold/captures/`): `gate_legacy.jsonl`,
  `producer_raw.jsonl` (crudo del proveedor + intentos + latencias), `producer_raw_rerun.jsonl`, `host.jsonl`,
  `captures.jsonl` (unión por `gold_id`), `stability.json`, `run/` (persistencia redirigida).

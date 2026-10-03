# M2A-3 — Fase 2: `compose` y `classify_turn` contra el oráculo congelado

**Fecha:** 2026-10-02. **Rama:** `mission/m2a-3-reducer`. **Contrato:** R3d sellado (`57530a07`, sello `7dd39b4b…`,
fusionado en `10c0c576`). **Oráculo:** M2A-8a0 congelado (`f7d1b8b8`, artefacto `4aa343c7…`, fusionado en
`b2ad0a62`; sólo lectura aquí).

**Estado: STOP antes de congelar.** DEV no llega al 100 %. Cinco casos DEV y uno TEST no se pueden ni construir: el
oráculo tipa una unidad heredada sin `inherited_from` y el contrato lo exige (§1). TEST **no se ha medido**: se mide
una sola vez, al congelar.

## 1. BLOCKER: unidad heredada sin origen

| Caso | Partición | Entrada del oráculo | Cláusula |
|---|---|---|---|
| SF103, SF104, SF106, SF167, SF183 | DEV | condición VALUE del parche con `unit_source = PREVIOUS_ACCEPTED_VALUE` e `inherited_from = None` | §15.4: «con regla `unit.inherited`, `unit_source = PREVIOUS_ACCEPTED_VALUE`, `inherited_from` y eco `UNIT_INHERITED`»; §11: `UNIT_INHERITED` «valor completo y de dónde sale» |
| SF101 | TEST | ídem (detectado al comprobar la representabilidad de las entradas; el resultado de TEST no se midió) | ídem |

El tipo (`Provenance`) sigue exigiendo el origen de una **unidad** heredada (`INHERITANCE_WITHOUT_ORIGIN`). No lo relajo:
esa exigencia la escribe el contrato. Las salidas posibles las decide el usuario:

- (a) el carril del oráculo tipa `inherited_from` con la traza del ancla (el valor previo es el de la base) y re-congela;
- (b) el usuario declara `inherited_from` opcional para la unidad, lo que enmienda la lectura de §15.4;
- (c) el oráculo declara esos casos `NO_MEDIBLE` (límite de tipado, como `LG-LECTURA-WITHOUT-TYPED-VALUE`).

El arnés los declara rojo con dos xfail estrictos y un pin que exige que el conjunto bloqueado sea exactamente ese
(`tests/test_m2a3_oracle_link.py`).

Corregidos en el tipo, porque no tenían cláusula y el contrato los contradice (§3.2, §11, §16):

- el acoplamiento `year ⇔ year_source` de 1a. `year` es la vigencia de una conversión SMMLV; `year_source` es la
  procedencia de un año, también el de un TIME;
- la exigencia de `inherited_from` para un **año** heredado. §11 `YEAR_INHERITED` sólo pide `year_source`.

Con esas dos correcciones, 23 de los 29 casos aplicables cuyas entradas el tipo rechazaba pasaron a ser construibles. Las cubren una batería y 4 mutantes en
`tests/test_foundation_accepted_m2a3_canonical.py` §7.

## 2. Lo implementado (tres módulos de una responsabilidad, registrados en `tests/m2a_steps/m2a_3.py`)

- **`app/core/foundation_delta.py`** — `compose(base, patch, *, base_trace_id) -> Composed | DeltaRefusal`. El
  parámetro `base_trace_id` existe porque una `AcceptedInterpretation` no lleva su propia traza.
  - **Causas de §6 paso 3b** con su precedencia cerrada y desempate por el orden de ranuras de §4.2. Cada causa lleva su
    cláusula.
  - **TARGET_NOT_ADDRESSABLE** (§4.2), también si la víctima de F16 o de A8.1 está dentro de un grupo.
  - **DELTA_SLOT_CONFLICT**, incluido `KEEP` + escritura aunque los valores coincidan (U22).
  - **KEEP_ON_EMPTY_SLOT.**
  - **OP_NOT_ACCREDITED:\<DIM\>**: F16 fail-closed y P-3; A8 con unidades no comparables; O1B (`REPLACE_UNIVERSE` a
    oportunidades ⇒ `UNIVERSE`, §19 DEP-O1B); política sin dueño (§6.6, inventario de la fase 1).
  - **DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER** (R3-1), decidido sobre el estado.
  - **Composición:** CLEAR → SET/SET_ORDER/REPLACE_UNIVERSE; arrastre byte-idéntico con la excepción P-1;
    sustituciones A8.1 y F16; efectos `VALUE_RANGE_REPLACED`, `POLARITY_INVERTED` y `TOPIC_REPLACED`.
  - **REPLACE_UNIVERSE** con ancla dual: se queda la petición nombrada, con `universe_origin` = el de la op y `dataset`
    = el del nuevo sujeto (tabla fijada contra `SoQLBuilder`).
  - Una política se va con su condición; la lógica OR/NOT se conserva y se poda sólo el AND de primer nivel.
  - El único camino de construcción es `canonical_interpretation`. Los ids colocados nunca reutilizan un id de la base.
- **`app/core/foundation_delta_outcome.py`** — `delta_outcome(base, patch, *, base_trace_id, anchor_expired)`.
  - **Causas** antes que las reglas del host. Con dos o más, o con el ancla vencida ⇒ `INFORM`. Con una ⇒ `CLARIFY`
    sólo si §8.4 lo permite.
  - **R3-1** sin E-ORDER lleva sólo C1 (§19).
  - **Reglas del host puras:**
    - H1: el vencimiento es un hecho inyectado; §10 es de M2A-6.
    - H3: catálogo SMMLV 2024–2026, fijado contra `app/data/smmlv.py`.
    - H6: `buyer_scope` arrastrado frente a la DIVIPOLA nueva.
    - H8: literales de STATE del dataset resultante.
    - H11 y H12.
  - **Dependencias declaradas de §19:** DEP-SMMLV-RECALC y DEP-MODALITY-UNIVERSE.
  - **Ley §8.4:**
    - el marco es válido y no tiene preguntas abiertas;
    - los candidatos componen;
    - hay al menos una respuesta que ejecuta, buscada con testigos abstractos de una operación, como en el modelo del
      contrato.
  - **Pendiente:** se devuelve como datos (`PendingQuestion`); la construye M2A-4.
  - **`relative_outcome`:** §7.1 fila 6.
- **`app/core/foundation_turn_scope.py`** — `classify_turn(TurnFacts)`: §7.1 filas 1–6 (incluida 4b) y §7.2 / §8 filas
  2–3 para el alcance.
  - Los hechos se leen en tres valores: el alcance sólo sale si no depende de los que faltan.
  - `ABSENT` se rechaza (`REFERENT_SIGNAL_ABSENT`). La fila que §7.2 no decide se rechaza (`UNDECIDED:ADMISSIBLE_ALONE`,
    ola F).
  - `search_effects` da los efectos de una búsqueda nueva (`CONTEXT_RESET`, `TOPIC_REPLACED`).
- **`app/core/foundation_accepted.py`** — la superficie de decodificación de R1 (`canonical_value`, `money_bound`,
  `unit_class`, `time_years`, `geo_divipola`, `state_datasets`) y las dos correcciones de §1.

Fuera, declarado:

- **H2** (pendiente vigente): M2A-4.
- **H5, H7, H9, H10:** lecturas de una autoridad que se resuelven al tipar el valor; un valor resuelto ya las pasó.
- **§8.3, §10 y §17.**
- **`HostViewDecision`:** el sujeto `unspecified` sigue fallando cerrado en el tipo.

## 3. Oráculo (DEV)

| Medida | Resultado |
|---|---|
| Gold `audit` | 54 aplicables: **49 coinciden** en disposición, código, efectiva y efectos; 0 discrepancias; **5 faltan** (§1) |
| `audit_transitions` | 0 violaciones: 42 transiciones y 3 rechazos comprobados |
| `audit_classification` | **52/52**, 0 discrepancias |
| no verificado por el oráculo | 4 efectivas de búsqueda nueva (`from_acceptance`, M2A-1b, `LG-FRESH-ACCEPTANCE`) |

Rojos declarados de DEV, con su dueño (`audit` → `declared_red`):

- M2A-4: SF047, SF048, SF050, SF059, SF061, SF189, SF191, SF199;
- M2A-6: SF145, SF146, SF150, SF151, SF152;
- E-MODALITY: SF007, SF160;
- F: SF082, SF126;
- `FOUNDATION_O1B_EXPLICIT_TIME_PARITY`: SF075.

No medibles de DEV:

- `TIME_MONTH_HINT`: SF031, SF033;
- `OPS_AB_DISAGREE`: SF025, SF163;
- `HOST_FACT_NOT_IN_GOLD:REGION_CLOSED_LIST`: SF159.

## 4. Propiedades y mutantes

- **`tests/test_foundation_delta_properties.py`** (Hypothesis con semilla fija en los gates). Las bases generadas
  llevan políticas, `receipt_rules`, procedencia rellena, ancla dual, oportunidades y grupos OR.
  - P1: `check_transition` del oráculo con 0 violaciones. Para las causas que el oráculo no ve (§4.2, O1B, §6.6) se
    exige además que las causas visibles sean exactamente las suyas.
  - P2: invariancia a la permutación de las ops. P3: determinismo. P4: forma canónica.
  - P5: propiedad universal; nunca excepción.
  - P6: políticas. P7: TARGET. P8: efectos.
  - Comprobaciones propias de H1, H3, H6, H8, H11, H12, «dos causas ⇒ INFORM» y DEP-O1B, escritas desde el contrato.
  - Estrés local: 6 semillas × 4.000 casos sin violaciones.
- **27 mutantes del reducer, todos muertos.** Incluyen los que el manifiesto del oráculo declara indiscriminables por el
  Gold: `f16_any_value`, `set_clears_whole_dimension`, `clear_dimension_only_positive`, `redirect_keeps_old_kind` y el
  sobredisparo de H6. El resto es uno por regla.
- **`tests/test_foundation_delta_tables.py`**, tablas exhaustivas del dominio cerrado:
  - INV-M2A-18 (A8.1): 408 filas.
  - INV-M2A-19 (F16, P-3, F17, F3): 15 filas.
  - §7.2 sobre las 768 combinaciones (con INV-M2A-05 e INV-M2A-20) y §7.1 filas 1–6.
  - Pines de las constantes copiadas.
- **`tests/test_foundation_accepted_m2a3_canonical.py`:** 4 mutantes nuevos de procedencia. Siguen vivos los de las
  fases 1b y 1c.

## 5. Gates (resultado literal)

- `pytest -p no:cacheprovider --hypothesis-seed=0`, 17 archivos: los focales y pines de las fases 1–1c, el barrido, el
  multi-op, la extracción s19, los tres tests del oráculo y los tres nuevos. Resultado: **1518 passed, 8 skipped, 2
  xfailed**. Los 8 saltados piden el REAL privado; con `M2A_GOLD_REAL_DIR` pasan (237 passed en
  `test_m2a_reducer_oracle.py` + `test_m2a_gold_seal.py`). Los 2 xfail son el BLOCKER de §1.
- `ruff` (configuración del repo): limpio.
- `olympus-semgrep-delta --base fa0b6de8`: `TOTAL=0 BASELINE=0 NEW_DELTA=0`. `semgrep` directo: 0.
- `m2a_gold_seal.py verify --real-dir`: `seal OK 7dd39b4b…`.
- `m2a_reducer_oracle.py check --real-dir`: `oracle OK 4aa343c7…`, sin deriva del artefacto.
- **No ejecutados:** TEST (se mide una vez, al congelar) y `olympus-state candidate freeze` (bloqueado por §1).

Corrí esos tests, no la suite global.

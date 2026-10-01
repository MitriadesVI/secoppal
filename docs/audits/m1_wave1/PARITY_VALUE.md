# M1 wave 1 — paridad de VALUE con `main` en el camino legacy

Rama `fix/m1-wave1-parity-value`, base `2dde6489` (paridad temporal), `main` = `4ec99bc1`. Origen: ZEUS7 NO_GO sobre `2dde6489`. La
paridad temporal estaba perfecta, pero `money_polarity` (legacy) consumía el léxico nuevo de D-MONEY (`money_comparators`:
cabezas «sin superar/sin exceder/sin pasar de/...», `NEGATION_FILLER`) y dejaba de activar la aclaración por negación de `main`:
la frase de dinero caía en la captura de comprador y, si el comprador resolvía por prefijo, se ejecutaba SIN filtro de valor
(«contratos de aseo universidad nacional sin superar 60 millones»: `main` aclara; `2dde6489` ejecutaba con
`nombre_entidad LIKE '%UNIVERSIDAD NACIONAL DE COLOMBIA%'` y sin `valor_del_contrato`). Misma clase que TIME: un léxico compartido
cambia lo que consume legacy. Decisión (V-A): paridad completa también en VALUE; frontera de modo `foundation_v3` explícita
(opción 1 del orquestador) para `foundation_value_direction` / `foundation_admission`.

## 1. Qué cambia

| archivo | destino | nota |
|---|---|---|
| `app/core/money_polarity.py` | VUELVE_A_MAIN | byte-idéntico (sha256 `1e4173ca…e948`); `git diff 4ec99bc1 -- app/core/money_polarity.py` vacío |
| `app/core/money_comparators.py` | SE_QUEDA, sólo cita/Foundation | ya no es alcanzable desde legacy |
| `app/core/money_suffix_event.py`, `money_suffix_place.py` | SE_QUEDA, sólo cita/Foundation | ídem |
| `app/core/value_authority.py` | SE_QUEDA, sólo cita | sin importadores en `app/`; importa `money_polarity` (de `main`) sólo para detectar rangos y contar montos |
| `app/core/foundation_value_direction.py`, `foundation_admission.py` | SE_QUEDAN, Foundation | importados en cada turno legacy, nunca ejecutados (ver §2) |
| `app/core/geo_authority.py`, `geo_window.py`, `geo_regions.py`, `app/data/geo_secop_variants.py` | SE_QUEDAN (D-GEO) | dejan de ser alcanzables desde legacy (sólo lo eran vía `money_polarity → money_suffix_place`) |
| `scripts/audits/m1_d_money_mutants.py` | editado | el mutante M10 (deriva del léxico de polaridad) se retira: `money_polarity` ya no comparte léxico |

Ningún otro archivo de `app/` cambia respecto de `2dde6489`. Los demás de wave 1 (TIME, H0, C1.1) quedan como en la ronda temporal.

## 2. Alcanzabilidad (cierre transitivo de imports)

`tests/test_m1_wave1_parity_pins.py` calcula, por AST, el cierre de imports desde la lista explícita de entrypoints legacy
(`streamlit_app`, `app.main`, `app.service`, `app.core.orchestrator`, `app.core.query_router`, `scripts.run_radar_scheduler`),
siguiendo imports estáticos, relativos, perezosos dentro de funciones, `from pkg import submodulo`, literales dotados exactos y
llamadas `importlib.import_module` / `__import__` (con arista comodín para f-strings y fallo si el argumento es dinámico sin prefijo).

* **Frontera de modo `foundation_v3`.** `orchestrator` importa `foundation_runtime` de forma perezosa en cada turno
  (`orchestrator.py:442`), y `foundation_runtime → foundation_admission → foundation_value_direction → money_comparators`. En modo
  `legacy`, `run_foundation_search` devuelve `None` antes de invocar admisión o dirección (`foundation_runtime.py:253`). El cierre se
  calcula CORTANDO únicamente esa arista (constante `MODE_FRONTIER`, con comentario). La frontera está fijada: debe seguir siendo un
  import de nivel de función (`test_the_mode_frontier_is_a_lazy_function_level_import`), ninguna otra arista puede alcanzar
  `foundation_runtime`/`foundation_admission`/`foundation_value_direction` (`test_no_edge_reaches_the_foundation_modules_except_the_declared_frontier`)
  y sin el corte el escáner SÍ llega a la cadena Foundation y a la ruta de cita
  (`test_without_the_frontier_the_scanner_reaches_the_foundation_chain_and_the_quote_route`).
* **Resultado:** cierre legacy con el corte = **389 módulos**; sin corte = 413. Con el corte NO contiene ningún módulo de la ruta de cita:
  TIME (`time_grammar`, `time_authority`, `time_relative`, `time_window`, `temporal_window_anchor`, `temporal_noun_anchor`,
  `non_canonical_year`, `normative_citation`, `temporal_bounds`), VALUE (`money_comparators`, `money_suffix_event`, `money_suffix_place`,
  `value_authority`), D-GEO (`geo_authority`, `geo_window`, `geo_regions`, `geo_secop_variants`), C1.1 (`foundation_authority_*`,
  `foundation_evidence_window`, `host_authorities`).
* **Módulos del cierre legacy que difieren de `main`: UNO**, `app/core/entity_resolver.py` (M1-H0, +9 líneas: sólo AÑADE
  `EntityResolver.alias_owners`, que sólo llaman `foundation_admission` y `foundation_buyer_choices`). Aceptado como SE_QUEDA(H0) en la
  ronda temporal; ahora fijado: `test_the_only_legacy_reachable_divergence_from_main_is_inert` falla si algún módulo del cierre legacy
  llama `alias_owners`. Sin el corte, además difieren `foundation_admission`, `foundation_buyer_choices`, `foundation_messages`,
  `foundation_runtime`, `foundation_time_coverage`, `foundation_value_direction`, `geo_*`, `money_comparators`, `money_suffix_*`
  (todos Foundation/cita).
* **Importados pero no ejecutados.** En modo `legacy` los módulos de la frontera SE IMPORTAN en cada turno y NO se ejecutan.
  Evidencia: `tests/test_m1_wave1_legacy_mode_runs_no_foundation.py` espía `foundation_value_direction.operator_reason`/`directions`,
  `foundation_admission.admit_proposal` y TODA función pública de `money_comparators` (en todos los módulos `app.*` que las enlazaron)
  y ejecuta `run_query` en modo `legacy`; exige 0 invocaciones y que `run_foundation_search` haya sido llamada y devuelto `None`.
  Con `M1_PV_SEQUENCES=zeus7/e2e_corpus.json` (las **2.092** secuencias / 2.238 turnos de la paridad e2e): `2 passed in 315.36s`, 0 invocaciones.

## 3. Tests

* Retirados (casos → `E_VALUE_REGRESSION_CASES.jsonl`, ver `README_E_VALUE_REGRESSION.md`): 15 funciones de test enteras y las mitades de
  legacy (polaridad / router) de otras 17 funciones que conservan su mitad de autoridad por cita. En casos de pytest: **233** de 1.908
  (`r1` 49→43, `r2` 1.038→1.001, `r3` 123→90, `r4` 53→38, `test_value_authority_m1d` 645→503). Ninguno existía en `main`: todos los
  tests de `main` siguen (§4).
* Nuevo corpus: `E_VALUE_REGRESSION_CASES.jsonl` (1.798 líneas = 1.786 casos + 12 familias que expanden a 58.504); incluye
  los «por lo menos X» (8 de router + cabezas de polaridad + ZEUS7), los 698/477 que pasaban de aclarar a ejecutar bien (run_query / parse),
  los 25 (ejecuta sin valor), 30+14 (main ejecutaba bien y ahora aclaraba), 20 (aclaraciones engañosas; 19 en el corpus) y 143 «más
  o menos X» (main inventa una cota superior).
* Se quedan: `test_value_authority_m1d.py` (503), `test_geo_authority.py`, `test_foundation_*`, y la mitad de autoridad por cita de
  `test_m1_wave1_r{1..4}_value_*.py` (`foundation_value_direction`, `read_comparators`, `money_suffix_*`).
* Pines nuevos en `tests/test_m1_wave1_parity_pins.py` (27 tests): `temporal_semantics` y `money_polarity` = sha de `main`; cierre
  transitivo vacío de ruta de cita; frontera; no-vacuidad (escáner positivo/negativo, con y sin corte); mutaciones en memoria (a)–(e).
  Reemplazan al pin «VALUE congelado» (que fijaba los bytes de D-MONEY, no la paridad).

## 4. Gates (corridos, resultados literales)

**Paridad de parse TOTAL (cualquier campo, sin excepción VALUE).** `QueryRouter.parse(text, today=2026-10-01)` en `main` (`git archive`
de `4ec99bc1`, árbol `secoppal-m1-base`) vs este árbol, volcado completo del `ParsedQuery` (params, evidencia, spans, residuales,
`analytic_controls`, `material_residual`), igualdad de objeto. Corpus: unión de `PARITY_PARSE_CORPUS.jsonl` (5.471), la sonda VALUE de
la ronda temporal (2.762), el corpus ZEUS7 `gen.py` (10.882), casos E-TIME (turnos y controles) y los textos de E-VALUE (casos
sueltos + muestra de cada 41.º de las familias): `PARITY_PARSE_CORPUS.jsonl` + `PARITY_PARSE_CORPUS_VALUE.jsonl` = **20.685 textos**.
Resultado: **`textos con diferencia: 0`** (19.116 en la unión base y 1.569 en la muestra de E-VALUE), 2 excepciones idénticas en ambos
árboles. **Sensibilidad:** el mismo volcado sobre `2dde6489` difiere de `main` en **1.446** textos de los 19.116 (899 de los 1.569 extra);
`PARITY_VALUE_DIFFS.jsonl` (712) es el registro histórico de la ronda anterior.

**Paridad run_query (e2e).** `zeus7/e2e.py` (SecopalWorkflow real, modo `legacy`, fuente congelada SQLite, reloj 2026-10-01, sin red):
**2.092 secuencias** (2.091 llaves distintas, 2.238 turnos: los 735 textos cuyo parse difería entre `main` y `2dde6489`, 700 que no,
510 sondas «cabeza de VALUE x 17 compradores x 2 órdenes» y 147 secuencias de dos turnos temporal/valor), `main` vs este árbol: **0 turnos distintos** (route, aclaración, respuesta[:300], SoQL emitido
y llamadas a la fuente idénticos, sin necesidad de normalizar). 0 errores. **Sensibilidad:** `2dde6489` difiere en **1.041** turnos
(1.021 de SoQL).

**HCA, 6 corpus, `main` vs este árbol** (`run_conversation_holistic_audit.py`, `PYTHONHASHSEED=0`, `selection_token`/timestamps normalizados):
369 casos, 442 resultados de turno: **verdicts distintos 0, turnos distintos 0** (pass/fail 171/24, 35/1, 41/1, 40/0, 22/2, 31/1; los 29 FAIL son
los de `main`).

**Autoridades por cita en verde.** TIME: `test_time_authority.py test_time_authority_r1.py test_time_grammar_*.py test_time_semantics_v2.py`
dentro de los focales (§ focales). VALUE: `test_value_authority_m1d.py` `503 passed`, `test_m1_wave1_r{1..4}_value_*.py` `1.172 passed`
(1.001 + 90 + 38 + 43). Diferencial de autoridad VALUE: `resolve_value_condition` x 5 operadores + `operator_reason` + `directions` sobre
4.331 textos (sonda VALUE + E-VALUE + muestras), `2dde6489` vs este árbol: **0 diferencias** (la autoridad por cita no depende de la polaridad legacy).

**Pines y mutantes.** `tests/test_m1_wave1_parity_pins.py`: `27 passed`; `tests/test_m1_wave1_legacy_mode_runs_no_foundation.py`: `2 passed`.
Mutantes sobre una COPIA (`scratch`), pines en rojo en todos (8/8 muertos): (a) `from app.core import time_grammar` y (a2)
`from app.core.money_comparators import ...` en `money_resolution` (módulo permitido del cierre) → `test_the_legacy_closure_holds_no_quote_route_module`;
(b) `importlib.import_module(f'app.core.{name}')` en `orchestrator` → ídem; (c) un byte en `money_polarity.py` →
`test_money_polarity_is_byte_identical_to_main` y (c2) un byte en `temporal_semantics.py` → `test_temporal_semantics_is_byte_identical_to_main`;
(d) la arista `orchestrator → foundation_runtime` como import de nivel de módulo → ídem (frontera, «module level»);
(e) `from app.core import foundation_admission` en `workflow/deps` → ídem; (f) `money_polarity` importando `money_comparators` → pin de sha.
Además `m1_d_money_mutants.py` (autoridad por cita): `19/19 killed` (el mutante M10 se retiró con la polaridad).

**Conteo de funciones de test.** AST de las funciones `test*` de los archivos de test rastreados: `main` **7.487**, este árbol **7.908**
(+421), **perdidas: 0** (lista vacía; `git ls-files` de `4ec99bc1` contra el árbol de trabajo). El «8.252 / 8.676» de ZEUS7 usa otro criterio de cuenta; el
criterio de este informe es nombre calificado `archivo::[Clase::]test*` y la inclusión es lo que importa.

**Focales, 5 procesos** (216 archivos: todos los que importan los módulos que difieren de `main` o `query_router`/`QueryRouter`/`temporal_semantics`,
más los de VALUE/`money_*`/`value_*`, golden, `test_truth_layer_public_parity`, `test_foundation_authority_contract`, los pines y el test de
ejecución): `26 failed, 2464 passed, 1 skipped, 1 xfailed` / `2163 passed, 2 skipped, 10 xfailed` / `1 failed, 11571 passed, 1 error` /
`23 failed, 6513 passed, 8 skipped, 13 xfailed` / `20 failed, 2200 passed, 13 xfailed`. Los 68 fallos + 1 error de 4 archivos
(`test_buyer_actor_independent_boundaries`, `test_buyer_actor_role_arbitration_v1`, `test_buyer_actor_role_controls_v1`,
`test_actor_conversation_corpus`) son el rojo PREEXISTENTE `date_extract_y`: el mismo `pytest` sobre `main` da el MISMO conjunto de ids
(`comm` sin diferencias) más `tests/contract_universe_independent_base_generator.py::test_generate_base_artifacts` (generador, no es un
`test_*.py`; rojo también en `main`). El intermitente `test_actor_pending_document_selection.py` (depende de la carga de la máquina; cada corrida falla un test distinto) falló 1
test dentro del reparto y es el único id que difiere con `main` en ese `comm`; en `main` también falla (1 test en 2 de 2 corridas aisladas con
carga) y en este árbol pasó `27 passed` x 2 con menos carga (2-3 fallos en 3 corridas con carga). Declarado, no atribuible a este cambio.

**Lint y semgrep.** `ruff check` sobre los 9 `.py` tocados: `All checks passed!`.
`olympus-semgrep-delta --base 2dde6489 --config .semgrep/secoppal-laws.yml` sobre los `.py` tocados: `TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0`.
Los dos jsonl de ruido (`docs/audits/contract_universe_v1_1_*`) no cambiaron.

## 5. Lo que no verifiqué

* La suite global (no se pide). Corrí focales, no la suite.
* Persistencia/live: ningún test contra SECOP real; e2e y HCA con fuente simulada.
* El comparador e2e usa `respuesta[:300]` y el SoQL completo (el harness de ZEUS7), no la respuesta completa.
* `scripts/audits/m1_search/authority_probes.py` y `docs/audits/m1_d_money/CANDIDATE.md`, `R1..R4_REPAIR.md` describen el estado previo
  (historia): no se reescribieron; `authority_probes.py` importa `money_polarity` para compararlo con el guard.
* El oráculo de E-VALUE parte ZEUS7 es una tabla escrita a mano; 193 diferencias de parse sin frase de dinero reconocible no tienen
  veredicto (no entran al corpus, pero sí al gate de paridad, que las compara byte a byte).
* `entity_resolver.py` (H0) sigue difiriendo de `main` en el cierre legacy: es aditivo e inerte (pin), pero no es byte-idéntico.

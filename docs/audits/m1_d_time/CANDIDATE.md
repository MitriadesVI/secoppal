# M1-D-TIME — CANDIDATE

Rama `mission/m1-d-time`, base `4ec99bc1` (main). Tesis: la autoridad temporal ejecutaba un
período distinto del que decía la cita sin avisar (comparadores, trimestres, «del <año>», rangos de
días) y el host no verificaba operador contra cita. Se corrigen los bugs de A2 §4, se añade una
autoridad pura nueva y se deja un contrato de entrada/salida pequeño para el adaptador de C1.

No se tocó el núcleo Foundation (`foundation_admission/projection/…`, prompts, productores) ni
`foundation_time_coverage.py` (rama H0) ni se creó `host_authorities/`.

## 1. Cambios por archivo

| Archivo | Cambio |
|---|---|
| `app/core/temporal_semantics.py` | `resolve_temporal_scope` ahora = `_resolve_base` + capa de cota (`:511`). `_YEAR_JOIN` (`:47`) acepta «de/del/,/-/ /» entre mes y año (TIME-006). `single_month_days_re` (`:330`, TIME-016). `_QUARTER`/`_quarter_resolution` (`:82`,`:136`, TIME-041/042). `_relative_resolution` (`:194`): `últimos N meses/años`, `hace N años`, `la última semana`, números en palabra (un..doce, quince, veinte, treinta). `_UNTIL_TODAY` (`:508`). Campo nuevo `TemporalResolution.clock` (`""|"relative"|"inferred_year"`, por defecto `""`: compatible). «mes de marzo», «día 15 de marzo» y «primer trimestre 2025» no dejan palabras sueltas. |
| `app/core/temporal_bounds.py` (nuevo, 80 LoC) | Comparadores cerrados FROM/AFTER/BEFORE/UNTIL + sufijos («en adelante», «a la fecha») y `apply_bound`. Puro, sin reloj. |
| `app/core/time_window.py` (nuevo, 112 LoC) | Léxico cerrado de calificadores y tres lecturas: `window_qualifier(left,right)`, `unparsed_qualifier(text)`, `numeric_date_in(text)`. Copia el léxico de H0 sin tocar su archivo. |
| `app/core/time_authority.py` (nuevo, 266 LoC) | `resolve_time_condition(quote, left, right, operator, polarity, *, today) -> TimeResolution` (`:237`). |
| `tests/test_time_authority.py` (nuevo, 870 casos) | Propiedades exhaustivas, reloj inyectado, negativos, ventana, Hypothesis. |
| `tests/test_time_semantics_v2.py` | +35 regresiones del legado (todas fallaban en la base). |

`date.today()` por defecto (TIME-054): `civil_today` se conserva para los consumidores legados; la
autoridad nueva exige `today` keyword-only sin defecto y lo prueba (`TypeError`; `Date.today`
parcheado para que explote).

## 2. Contrato de la autoridad nueva y mapeo a C0 §5.5

Entrada: `quote`, `left`, `right` (≤4 tokens plegados por lado, orden de lectura; >4 o no-str →
`ValueError`), `operator ∈ {during_year, during_month, between_dates, relative_period}`, `polarity`,
`today: date`. Nunca texto crudo.

Salida `TimeResolution(decision, value: TimeValue|None, reason, detail, authority="time-authority@1")`,
`TimeValue(start, end_exclusive, precision)` (≥1 extremo no nulo; `None` = cota abierta; el extremo
superior es exclusivo). Propiedades `fecha_desde`, `fecha_hasta_exclusive`, `precision` por
compatibilidad con la especificación de la misión.

| `detail` (diagnóstico) | `reason` C0 | decisión |
|---|---|---|
| `TIME_NEGATION_UNSUPPORTED` | `NEGATION_UNSUPPORTED:TIME` | BLOCK |
| `TIME_OPERATOR_EVIDENCE_MISMATCH` | `OPERATOR_EVIDENCE_MISMATCH:TIME` | CLARIFY |
| `TIME_UNCITED_QUALIFIER` (ventana) | `UNCITED_QUALIFIER:TIME` | CLARIFY |
| `TIME_POLARITY_INVALID`, `TIME_OPERATOR_UNKNOWN`, `TIME_DEICTIC_NOT_CONDITION` | `UNRESOLVED_HOST_DIMENSION:TIME` | BLOCK |
| `TIME_YEAR_NOT_STATED`, `TIME_NO_TEMPORAL_EVIDENCE`, `TIME_UNSUPPORTED_EXPRESSION`, `TIME_INVALID_DATE`, `TIME_REVERSE_RANGE`, `TIME_COUNT_OUT_OF_RANGE`, `TIME_RANGE_OR_LIST_AMBIGUOUS`, `TIME_MULTIPLE_TEMPORAL_FACTS`, `TIME_UNPARSED_QUALIFIER`, `TIME_NUMERIC_DATE_UNSUPPORTED` | `UNRESOLVED_HOST_DIMENSION:TIME` | CLARIFY |

Sugerencia al adaptador: las dos de ambigüedad (`RANGE_OR_LIST`, `MULTIPLE_FACTS`) podrían subir a
`AMBIGUOUS_HOST_DIMENSION:TIME` (existe en C0) si METIS lo prefiere; aquí se respetó la lista de
cuatro razones que pidió el coordinador.

**Monotonía de la ventana (INV-C0 «sólo retira»).** El valor sale de la cita sola
(`_resolve_quote`); `window_qualifier` sólo puede convertir un RESOLVED en `UNCITED_QUALIFIER`.
Propiedad Hypothesis (500 ejemplos): cita que no resuelve ⇒ resultado idéntico con cualquier contexto;
cita que resuelve ⇒ mismo valor o CLARIFY `TIME_UNCITED_QUALIFIER`, nunca otro valor. El camino de
H0 («antes de» + «2024» llegaba como «2024») queda cubierto con `left=("antes","de")`.

Reglas (todas fail-closed): operador vs cita por precisión resuelta (`during_year` con cota/mes/rango/
trimestre → MISMATCH); ~~el año no se infiere del reloj~~ (**R0, reemplazado en R1 §9**: mes/día/rango
sin año = la ocurrencia más reciente que no es posterior a `today`, con `year_inferred=True`); «A y B» sin «entre» →
lista-o-rango, CLARIFY; dos hechos temporales sólo se aceptan como cotas opuestas
(«después de 2020 y antes de 2024» → `[2021-01-01, 2024-01-01)`); fechas numéricas (`15/03/2025`,
`2025-03`, `2025-I`) y palabras calificadoras sobrantes en la cita (`finales de`, `mediados de`,
`2025 primer trimestre`, `Q1`) se preguntan; una fecha ISO sola es ese día; deícticos
`hoy/ahora/actualmente/en este momento` → BLOCK (M1.0 C3).

## 3. Decisiones de semántica que conviene confirmar (no son bugs)

1. ~~**Mes sin año → CLARIFY**~~ **RECHAZADA por el orquestador contra el Gold sellado** (8 consultas
   reales con mes sin año son EXECUTE con año implícito). Sustituida en R1 (§9): ocurrencia más
   reciente no posterior a `today`. El legado conserva la convención ADR-065 (año del reloj).
2. **«hoy» solo → BLOCK** (C3 literal), aunque el legado lo resuelva como el día (A2 TIME-028 PASS).
   «publicados hoy» (D6) queda como decisión del núcleo: si Gold exige TIME ahí, se invierte en
   `_resolve_quote`.
3. **AFTER/BEFORE excluyen el período citado** («después de 2022» = desde 2023-01-01; «antes de 2024» =
   hasta 2023-12-31); `desde/a partir de/hasta` lo incluyen. `últimos N meses/años`: ventana rodante
   `[hoy−N, hoy]` (inclusive, como `últimos N días` ya hacía; `hasta` exclusivo en la autoridad).
   `hace N años/meses`: el año/mes calendario (como `hace 3 meses` ya hacía).
4. ~~«último año», «este trimestre», `hace dos semanas` siguen sin resolver~~ **cerrado en R1 (§9)**.

## 4. Gates (comandos y resultados literales)

Todos con `PYTHONPATH=/Users/rodrigoortiz/Documents/secoppal-m1-dtime` y el venv de `secoppal/`.

- `ruff check` sobre los 6 archivos tocados → `All checks passed!`
- `pytest -q tests/test_time_authority.py tests/test_time_semantics_v2.py` → `944 passed` (870 + 74).
  Corrí esos dos archivos, no la suite.
- `olympus-semgrep-delta --base 4ec99bc1 --config .semgrep/secoppal-laws.yml` sobre los 4 módulos y los
  2 tests → `TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0` (con `--base`, no vacuo).
- **Mutantes ejecutados** (`scratchpad/mutants_dtime.py`: cada uno aplica un reemplazo textual, corre
  los dos archivos de tests con `-x`, restaura el archivo): **70 mutantes, 70 muertos** tras añadir el
  caso `("2025", (), ("100","dias"))`. Primera pasada: 2 supervivientes honestos — la caminata sobre
  números pequeños (test añadido) y `rest.error_code` ignorado (test `desde 2025-03-01 y hasta abril`;
  además se descubrió que `_LEAD` de `temporal_bounds` era redundante y se eliminó). Cubren: los 4
  comparadores y su exclusividad, cada marcador por familia, plegado de acentos, `_YEAR_JOIN`,
  trimestres/semestres, rango de días, reloj (bisiesto, conteos, semana), banderas `clock`, los
  chequeos de operador, polaridad, deícticos, lista/rango, compuestos, ventana (monotonía, anchura,
  pegamento, léxico), fechas numéricas, mapa `detail→reason`, validación de ventana y de `today`.

## 5. Impacto legado

Consumidores de `temporal_semantics`: `query_router`, `dialogue_policy`, `source_occurrence`,
`workflow/{context,graph}`, `contract_activity/filters`, `entity_profile/{claim_adapters,
semantic_request,temporal_event}`, `accreditation/{authorities,assembly}`,
`capability_binding/residual`. Ningún consumidor cambió.

**a) Tests de consumidores, base vs rama** (185 archivos de `tests/` que mencionan temporal/fecha/los
consumidores; misma máquina, worktree base `4ec99bc1` desechable):
`BASE  119 failed, 7096 passed, 8 skipped, 8 xfailed, 2 errors` /
`RAMA  119 failed, 7131 passed, 8 skipped, 8 xfailed, 2 errors`.
`comm` de los conjuntos FAILED/ERROR: **vacío en ambos sentidos**; +35 passed = las regresiones nuevas.
Los 121 rojos son el baseline conocido (`date_extract_y`: fixture sqlite sin la función; el workflow
cae a red real), así que **esos tests no pueden ver un cambio** en los caminos que cruzan el workflow:
límite declarado.

**b) Corpus del repo.** 3 990 cadenas (34 archivos `tests/fixtures/*` y gold) por
`resolve_temporal_scope`, base vs rama: **3 cambian** (1 duplicada): `el día 22 de mayo` (el span ahora
incluye «el día»; mismas fechas) y `contratos de aseo en los últimos 3 meses` (de sin fecha a
`2026-05-25..2026-08-25`, corrección: la base lo dejaba sin filtrar).
Auditoría HCA (`run_conversation_holistic_audit.py`, 6 corpus, `PYTHONHASHSEED=0`, selection_token
normalizado): `conversation_holistic_v1` 171/195, `time_v2_focal` 31/32, `negation_focal_v1` 22/24,
`control_language_v1` 41/42, `actor_hygiene_v1` 35/36, `hca_final_p1_fixes` 40/40 en base **y** rama;
**0 verdicts distintos y 0 de 442 turnos distintos** (params, SoQL, respuesta). Sin la semilla fija la
comparación mostraba 47 turnos distintos que eran ruido (orden de sinónimos de tema y tokens aleatorios).

**c) Cambios de comportamiento legado (todos correcciones; ejercitados en `QueryRouter.parse`,
reloj 2026-08-25):**

| Entrada | Base | Rama | Porqué |
|---|---|---|---|
| «después de 2022» / «posteriores a 2022» | año 2022; `objeto` += `despues`/`posteriores` | `desde 2023-01-01`, abierta | comparador ignorado |
| «antes de 2024» | año 2024; `objeto` += `antes` | `hasta 2023-12-31` | ídem |
| «a partir de 2023», «de 2024 a la fecha» | año; `objeto` += `partir`/`fecha` | `desde …`, abierta | ídem |
| «desde el año pasado» | año completo; `objeto` += `desde` | `desde 2025-01-01` | ídem |
| «antes de marzo» | marzo | `hasta 2026-02-28` | ídem (año del reloj, convención legada) |
| «septiembre del 2025», «marzo, 2025» | 2026 (año del reloj) | 2025 | `month_re` no leía `del <año>` |
| «primer trimestre de 2025» | 2025 completo; `objeto` += `primer` | 2025-01-01..03-31 | rama inalcanzable |
| «segundo semestre de 2025», «último trimestre …» | 2025 completo | 07-01..12-31 / 10-01..12-31 | ídem |
| «del 1 al 15 de marzo de 2025» | sólo el 15 | 1..15 | `day_range_re` exigía mes en ambos días |
| «desde el 1 de marzo de 2025», «hasta el 15 de marzo…», «antes del 15 de marzo…» | ese día | cota abierta | comparador ignorado |
| «últimos 6 meses», «los últimos 3 años», «hace 2 años», «la última semana», «hace dos meses» | sin fecha (la consulta corría sin filtrar) | resueltos con el reloj | `matched=False` |
| «hace tres meses», «últimas dos semanas» (palabra) | sin fecha | resuelto | números en palabra |
| «el mes de marzo del 2025», «el día 15 de marzo» | span sin el prefijo | span completo | no deja `mes`/`día` como tema |
| «antes de 2025-03-01» | `desde` esa fecha | `hasta 2025-02-28` | única ISO ignoraba el comparador |
| «desde 2024 hasta hoy» | sólo hoy | `desde 2024`, abierta | «hoy» ganaba al año |

Efecto lateral beneficioso medido: los comparadores ya no se filtran a `objeto`.

## 6. Hallazgos sin resolver / fuera de scope

- Legado intacto, protegido sólo por la autoridad nueva: «2023 y 2025» → rango 2023–2025 (incluye 2024),
  «enero y marzo de 2026» → rango; `15/03/2025`, `2025-03`, «2025 primer trimestre», «finales de 2025»
  → el año completo; «2024 y 2025» en el router; una ISO sola sin comparador es `open_from`. La
  autoridad nueva los pregunta (`RANGE_OR_LIST`, `NUMERIC_DATE`, `UNPARSED_QUALIFIER`).
- Cota compuesta en el legado («después de 2020 y antes de 2024») sólo toma la primera; la autoridad
  nueva la funde bien o pregunta.
- Los tokens plegados de la ventana no conservan separadores: `15/03/` fuera de la cita no se ve.
  Sobre-pregunta con nombres propios («Avenida Primero de Mayo 2025»). Límites de H0 heredados.
- `tests/fixtures/legacy_shadow_inventory.json` registra «contratos de aseo en los últimos 3 meses» sin
  fechas; con esta rama el `QueryRouter` vivo ya las pone. Los tests pasan (usan el fixture), pero el
  inventario de legacy_shadow queda desactualizado para esa consulta.
- Integración pendiente (núcleo): `foundation_admission.py:109-111` aún usa su regex propia; el prompt
  v4.2 (regla Y1) sigue citando sólo el año. La autoridad no se invoca desde ningún sitio.
- Rangos de años 19xx fuera del léxico (`20\d{2}`), como antes.

## 7. Rondas de reparación

3 iteraciones significativas (A: fallos de diseño de tests propios; B: giro del coordinador a
ventana/TimeValue/razones C0; C: dos supervivientes de mutación). Ningún hallazgo C–F.

## 8. Commits

`d66a9919` (legado + `temporal_bounds`), `df199987` (autoridad v0), `24965fb6` (ventana, razones C0,
guardas de cita) y el commit de este documento.

## 9. R1 — año implícito y periodos relativos del Gold sellado

Orden del orquestador tras contrastar §3 con el Gold (autoridad semántica). Rama `mission/m1-d-time`,
base `4ec99bc1`. Sólo la autoridad nueva cambia de comportamiento; el resolvedor legado conserva el
año del reloj (`resolve_temporal_scope` sin `implicit_year` = idéntico a R0).

### 9.1 Archivos

| Archivo | Cambio R1 |
|---|---|
| `app/core/temporal_semantics.py` | parámetro opcional `implicit_year` (por defecto `None` = legado) en `resolve_temporal_scope`/`_resolve_base`: sustituye al año civil en lo que el texto no dice y permite rangos de meses/días sin año. Campo nuevo `TemporalResolution.anchor` (sólo con `clock="inferred_year"`: primer día de la última unidad nombrada, día o mes). |
| `app/core/time_relative.py` (nuevo, 91 LoC) | las formas relativas que la legada no resuelve: «este trimestre/semestre», «el último año/mes», «estos dos últimos meses» (número ANTES de «últimos»), «hace N semanas». Devuelve `TemporalResolution(clock="relative")`; sólo se consulta si la legada no resolvió. |
| `app/core/time_authority.py` | `_read_quote` (una lectura con año dado) + `_resolve_quote` (bucle de año) + cierre «hasta hoy/acá/aquí». `TimeValue.year_inferred` y propiedad `moving_window`. |
| `tests/test_time_authority_r1.py` (nuevo) | propiedades por reloj × mes/día/rango/forma relativa × N, Hypothesis contra oráculo propio. |
| `tests/test_time_authority.py` | 4 tests de R0 actualizados (ver 9.4). |
| `tests/fixtures/legacy_shadow_inventory.json` | 1 entrada (ver 9.5). |

### 9.2 Regla de año implícito

Mes, día, «de X a Y», «desde X hasta Y», cotas («antes de marzo») **sin año** → el año más reciente en
que **la última unidad nombrada no empieza después de `today`**: mes ≤ mes actual → año en curso,
posterior → año anterior; día: la fecha no posterior a hoy (el 29 de febrero retrocede hasta el último
bisiesto, hasta 8 años); rango: un solo año para todo el rango, el que hace que la unidad final no
empiece después de hoy («de enero a junio» con hoy = 2026-09-30 → 2026-01-01..2026-06-30; con hoy =
2026-02-10 → 2025). El valor sale con `TimeValue.year_inferred=True` para que el host anuncie
«agosto de 2026».

Casos del Gold (reloj 2026-09-30): «mayo» → 2026-05; «abril»; «agosto»; «de enero a junio» →
2026-01-01..2026-06-30; «desde 1 agosto hasta acá» → 2026-08-01..hoy; «desde el 1 de agosto hasta el 9
de septiembre» → 2026-08-01..2026-09-09.

Decisiones de interpretación (declaradas, no están en la orden):
* **Mes actual cuenta como no posterior**: «junio» con hoy = 15 de junio es junio de este año; para
  rangos se usa el mismo criterio sobre el MES final (leyendo «el fin no supere `today`» de la orden
  literalmente, «de enero a junio» en junio daría el año anterior, y «junio» solo no).
* **Inicio posterior al fin → CLARIFY `TIME_REVERSE_RANGE`** («de noviembre a febrero», «del 20 al 5 de
  marzo»): nunca se envuelve el año.
* **Una cota con año dicho y la otra sin año → CLARIFY `TIME_MULTIPLE_TEMPORAL_FACTS`** («desde
  2025-03-01 y hasta abril», «después de 2020 y antes de marzo»): no se adivina si el año dicho
  gobierna a la otra cota. Las dos sin año comparten año (el del final).
* **«desde X hasta hoy/acá/aquí» cierra la cota en `today` inclusive** (`precision="bounded"`). Cambia
  un caso de R0: «desde 2024 hasta hoy» era `open_from` sin fin; ahora termina hoy. «a la fecha»,
  «en adelante», «hasta la fecha», «al presente» siguen abiertos. Un periodo cerrado + «hasta acá»
  («mayo hasta acá») se pregunta.
* Fecha inexistente en todos los años («31 de abril») o con año dicho («29 de febrero de 2025») sigue
  siendo `TIME_INVALID_DATE`.

### 9.3 Periodos relativos (reloj inyectado, `relative_period`)

| Forma | Valor | `precision` |
|---|---|---|
| «este mes», «este año» (ya existían) | calendario en curso | `month`, `year` |
| «este trimestre», «este semestre» | franja calendario en curso | `quarter`, `semester` |
| «últimos N días/semanas/meses/años», «los últimos…», N cifra o palabra (un..doce, quince, veinte, treinta) | `[hoy−N, hoy]` inclusive, móvil | `trailing_days/weeks/months/years` |
| «los ultimos dos años» | últimos 24 meses (móvil) | `trailing_years` |
| «estos dos últimos meses», «los dos últimos años», «3 últimas semanas» | igual que «últimos N» | `trailing_*` |
| «el último año» / «el último mes» | últimos 12 / 1 meses, MÓVIL (no el año calendario pasado) | `trailing_years/months` |
| «la última semana» (ya existía) | `[hoy−7, hoy]` | `trailing_weeks` |
| «hace N semanas» | **ventana** `[hoy−7N, hoy]`, no la semana calendario («hace dos semanas» → hoy−14d..hoy) | `trailing_weeks` |

`TimeValue.moving_window` es verdadero para toda `precision` `trailing_*`; el host debe anunciarlas
como ventana móvil. Límites de N como en la legada (365 días, 52 semanas, 120 meses, 30 años), fuera de
ellos `TIME_COUNT_OUT_OF_RANGE`. Todas aceptan sólo `relative_period` como operador salvo «este
mes/año», que conservan `during_month/during_year`. Una forma relativa junto a otro periodo
(«este trimestre de 2025», «hace dos semanas en marzo») se pregunta, nunca se descarta.

### 9.4 Tests de R0 actualizados (no se relajó ningún validador)

* `test_month_without_year_asks_…` (36 casos) → eliminado; lo cubre `test_time_authority_r1.py` con la regla nueva.
* `test_year_with_open_suffix_…[hasta hoy]` → «hasta hoy» sale de la lista de cotas abiertas (ahora cierra en hoy).
* `test_single_month_day_range_edges` → «del 1 al 15 de marzo» ya no es `YEAR_NOT_STATED`.
* `test_invalid_expressions_are_asked` → sin «entre enero y marzo» ni «este trimestre»; añade «entre noviembre y febrero» → `REVERSE_RANGE`.

### 9.5 `legacy_shadow_inventory.json`

Cambio legado de R0 (no de R1) que el inventario no reflejaba. Diff de una entrada, comprobado
re-parseando las 83 consultas con `QueryRouter.parse` (reloj 2026-09-30): sólo cambia
`contratos de aseo en los últimos 3 meses`: `+_temporal_replace_scope: true`, `+fecha_desde: 2026-06-30`,
`+fecha_hasta: 2026-09-30`. Las otras 82 coinciden. `tests/test_legacy_shadow.py` 41 passed.

### 9.6 Gates (mismos comandos que §4; corrí estos archivos, no la suite)

* `ruff check` app/core + los dos tests de tiempo → `All checks passed!`
* `pytest tests/test_time_authority.py tests/test_time_authority_r1.py tests/test_time_semantics_v2.py` →
  `10397 passed` (R0: 944).
* **Mutantes del delta** (`dtime_r1_mutants.py`, ejecutados sobre una copia `git archive` del SHA final):
  **58 mutantes, 58 muertos** (sobre `temporal_semantics`,
  `time_authority`, `time_relative`: anclas por sitio, año implícito ignorado, `_YEAR_SPAN` 1 y 7, bucle
  que se detiene/continúa, ancla vs `today` (≤ y <), `year_inferred`, bisiesto, cierre «hasta hoy/acá/aquí»,
  cota mixta explícita/implícita, marcador de rango sin año, largo de semana/mes/año/días, topes de N,
  `este trimestre`, pliegue ñ/í, las cuatro formas de `time_relative`). La primera pasada dejó 8
  supervivientes: 2 eran código redundante (eliminado), 1 equivalente (se excluye: el reintento con
  `implicit_year` tras enmascarar «hasta hoy» sólo puede hallar «hoy»/«ayer») y 5 hallaron tests
  faltantes (añadidos). La corrida final es sobre `git archive` del SHA `e84e4840` (el commit siguiente
  sólo toca el fixture JSON).
* Consumidores legados (182 archivos de tests, mismo conjunto de R0): `120 failed, 7130 passed, 8 skipped, 8 xfailed, 2 errors`
  frente a la base `119 failed, 7096+ passed…` de R0 (misma lista de rojos): `comm` de los conjuntos
  FAILED/ERROR = **un solo rojo nuevo**, `test_supplier_request_authority.py::test_arbitrary_digit_boundaries_never_raise_or_silently_authorize`,
  que es un test de Hypothesis (20 ejemplos) y **no depende del cambio**: con `digits='001'` la base
  `4ec99bc1` también devuelve `EXECUTABLE` para «001 proveedores» (reproducido en la base); el ejemplo
  quedó en `.hypothesis/` del worktree. Rojos desaparecidos: ninguno. Los 121 rojos del baseline
  (`date_extract_y`) siguen ocultando los caminos que cruzan el workflow, como en R0.
* Corpus del repo (3 991 cadenas, 34 archivos): `resolve_temporal_scope` base vs rama = los mismos 3
  cambios declarados en §5b (el resolvedor legado no cambió en R1).
* Auditoría HCA (6 corpus, `PYTHONHASHSEED=0`, selection_token normalizado, base y rama re-corridas el
  mismo día porque los turnos llevan el reloj real): `conversation_holistic_v1` 171/195, `time_v2_focal` 31/32,
  `negation_focal_v1` 22/24, `control_language_v1` 41/42, `actor_hygiene_v1` 35/36, `hca_final_p1_fixes` 40/40
  en base **y** rama; **0 verdicts distintos y 0 de 442 turnos distintos** (normalizando `selection_token`,
  «consultado el <fecha>» y `_opportunity_reference_instant`, que son relojes de pared).
* `olympus-semgrep-delta --base 4ec99bc1 --config .semgrep/secoppal-laws.yml` sobre los 5 módulos y los 3 tests →
  `TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0`.

### 9.7 Rondas de reparación y riesgos

Rondas R1: 2 (A: supervivientes de mutación → se simplificó el marcador de año —dos señales eran
redundantes— y se añadieron tests de «hasta acá» sobre periodos cerrados, de 29 de febrero con 7 años de
hueco y de rangos de días sin año; B: los dos supervivientes restantes → un test y código muerto
eliminado). Ningún hallazgo C–F.

Riesgos pendientes: (1) la lectura «mes actual no es posterior» en rangos (arriba) difiere de la orden
si se lee estrictamente; es un cambio de una línea (`anchor` del rango); (2) `hace N semanas` se anuncia
como ventana, no como «hace N semanas»; (3) la autoridad sigue sin invocarse desde el núcleo
(integración pendiente de C1); (4) un mutante equivalente excluido (el reintento
`implicit_year` tras enmascarar «hasta hoy» sólo puede hallar «hoy»/«ayer», sin año implícito).

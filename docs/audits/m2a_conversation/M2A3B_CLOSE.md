# M2A-3b — herencia tipada: la entrada separada de insumos del host

**Fecha:** 2026-10-03. **Rama:** `mission/m2a-3b-inheritance`, base `d7f53e9b`. **Contrato:** §15.4, §15.5, §16.1, §16.3,
§11, INV-M2A-07 y -17 (R3d sellado, sin tocar). **ADR-103:** §4.3 («entrada separada de insumos del host»), D10.

**Qué es.** `app/core/foundation_authority_inheritance.py` (puro, un módulo, registrado en `tests/m2a_steps/m2a_3b.py` con
`CLOSURE` exacto). `host_inheritance_inputs(base, *, base_trace_id) -> HostInheritanceInputs(unit, year, referent)`
deriva, **sólo de la base aceptada tipada**, los tres insumos que una autoridad recibirá por la entrada que sólo el host
llama. Cada uno es exactamente **PRESENT** (un valor y de dónde sale), **ABSENT** (la base no tiene nada que heredar) o
**AMBIGUOUS** (tiene algo que no se hereda, y la razón cerrada). Nunca elige entre varios.

**Qué no es.** No es una regla de recibo: no lee texto, registro previo ni ancla; no llama a ninguna autoridad; no lee
reloj; no convierte; no hace aritmética de periodo; no decide si la expresión de un turno nuevo encaja con el referente.
No cambia el runtime: ningún módulo fuera de M2a lo importa (pin `consumer_problems`).

## 1. Los tres insumos

| Insumo | Regla de recibo que lo consumirá (M2A-3c) | PRESENT lleva | AMBIGUOUS por |
|---|---|---|---|
| `UnitInheritance` (§15.4) | `unit.inherited` (+ `unit.smmlv` si la unidad es SMMLV) | `unit`, `inherited_from`, `source_condition_ids` | `MULTIPLE_UNITS`, `UNIT_UNRECORDED` |
| `YearInheritance` (§16.1) | `year.inherited` (ABSENT ⇒ `year.inferred`, del reloj del host) | `year`, `condition_id`, `inherited_from` | `NONCONJUNCTIVE_TIME`, `MULTIPLE_TIMES`, `RELATIVE_WINDOW`, `OPEN_INTERVAL`, `MULTI_YEAR` |
| `TimeReferent` (§16.3) | `time.relative_to_previous` | `condition_id`, `value` (codificación `time@1`), `first_year`, `last_year`, `months`, `inherited_from` | `NONCONJUNCTIVE_TIME`, `MULTIPLE_TIMES`, `RELATIVE_WINDOW`, `OPEN_INTERVAL`, `INCOMPATIBLE_GRANULARITY` |

`inherited_from` es el `base_trace_id` que el llamador nombra (el mismo hecho que recibe `compose`, porque una
`AcceptedInterpretation` no lleva su propia traza): es el origen que exigen §15.4 y §11 y el que el tipo `Provenance`
obliga. Procedencia, nunca significado.

## 2. Decisiones

- **M2A3B-D1 — la entrada.** Tres insumos tipados y una unión discriminada (el tipo rechaza toda combinación
  estado/razón/payload que no sea PRESENT con todo, ABSENT con nada, o AMBIGUOUS con una razón de su propio insumo). Las
  funciones sólo aceptan `(base, *, base_trace_id)` y exigen una `AcceptedInterpretation`; el pin de superficie de lectura
  (imports, atributos, firmas) es un test (INV-M2A-07).
- **M2A3B-D2 — unidad.** Cuentan **todas** las cotas VALUE de la base, estén donde estén en `logic` (la unidad es un hecho
  del monto dicho, no de cómo se combinan: contar más sólo puede añadir ambigüedad). Dos `Unit` distintas son dos unidades,
  **incluidas MILLONES y MIL_MILLONES** (SF102). Una cota cuya unidad la base no registra (`Provenance.unit is None`) no es
  una unidad: `UNIT_UNRECORDED`. «Sin cotas» es ABSENT. SMMLV es una unidad más (A2); su vigencia es otro insumo.
- **M2A3B-D3 — TIME activo.** Es el **único TIME positivo**. Los negativos se ignoran (§16.3 «único TIME positivo
  activo»). Varios positivos son ambiguos aunque estén en el mismo año. Cualquier TIME (de cualquier polaridad) dentro de un
  grupo OR/NOT es `NONCONJUNCTIVE_TIME`: un átomo positivo bajo NOT es una exclusión y uno negativo bajo NOT, una
  inclusión; la base no dice cuál sin evaluar la lógica (fail-closed: nunca un año de un periodo excluido).
  Precedencia cerrada: NONCONJUNCTIVE > MULTIPLE > RELATIVE > OPEN > MULTI_YEAR / INCOMPATIBLE_GRANULARITY.
- **M2A3B-D4 — ventana relativa.** Es un TIME con `year_source = RELATIVE_TO_CLOCK` **o** con el recibo
  `time.relative_to_previous`. Lectura fail-closed de «ventana relativa» (§16.1, §16.3, §1.2), que el contrato no define y
  que su eco `RELATIVE_WINDOW_RESOLVED` cubre por las dos vías (§11). Un mes cuyo año se heredó (`year.inherited`) es
  absoluto. **Alternativa a decidir por HERA/ZEUS:** tratar como absoluto el TIME resuelto contra el previo permitiría
  encadenar «el mes siguiente»; se quita con una línea de `_is_relative`. **Obligación aguas arriba:** la base debe llevar la
  marca; el tipado del oráculo no la lleva (ver §5, R2).
- **M2A3B-D5 — año.** El año único civil del TIME activo, **sin mirar la granularidad**: un día, un mes o un semestre dentro
  de un año dan ese año. Intervalo abierto o de varios años (un trimestre a caballo de dos años incluido): ambiguo. Nunca el
  primer año de varios (INV-M2A-17).
- **M2A3B-D6 — referente.** PRESENT lleva `months` (meses de calendario enteros) y los años que toca. Un referente
  plurianual (36 meses) **es PRESENT**: que «el año anterior» (12) encaje con él es de la autoridad, que compara `months`
  con la longitud de su expresión (SF175, frontera, es esa decisión, no ésta). Lo que el insumo sí rechaza es lo que no
  admite aritmética de periodo por calendario: un extremo abierto (`OPEN_INTERVAL`) o un extremo que no cae en día 1
  (`INCOMPATIBLE_GRANULARITY`). **Alternativa a decidir:** rechazar también el referente plurianual aquí (una línea en
  `time_referent`), a costa de impedir «los tres años siguientes».
- **M2A3B-D7 — lectura de `time@1`.** `foundation_accepted` (protegido, U3) no tiene un accesor de fechas. Para contar meses,
  el módulo lee los dos campos de fecha de la codificación canónica (la que el tipo ya validó al construir la condición),
  con comprobación estricta de etiqueta y orden de campos y un cruce contra `time_years` (un desacuerdo es error, nunca otro
  año). Es una excepción a R1. **Recomendación:** que el dueño de U3 añada `time_bounds(condition)` y se borre `_extent`'s
  lectura; ningún pin lo exige hoy.
- **M2A3B-D8 — sin importar los tipos cerrados.** El brief sugería importar los tipos de valor; el pin C1.1
  (`tests/test_foundation_authority_contract.py`, `CLOSED_TYPE_CONSUMERS`) sólo deja a `foundation_accepted` nombrarlos, y
  afirma que ese conjunto no crece con el registro. Se cumple el pin: los valores viajan como tuplas `encode()` (la única
  fuente del valor, §3.2) y la autoridad las decodifica con sus propios tipos.
- **M2A3B-D9 — el gate de los casos Gold.** Cada caso `DEP-INHERITANCE` se tipa desde su `prior` con el tipado del oráculo y
  su insumo esperado sale de la **etiqueta** (`ops[].inherited`, `disposition_reason`, IR previo y resultante), nunca del
  código de este carril. FRONTIER no entra (SF175–SF177 se descartaron). Los casos **siguen siendo rojo declarado** hasta
  M2A-3c: aquí sólo se prueba el insumo, no la disposición.

## 3. Casos `DEP-INHERITANCE`: insumo esperado frente a producido

Particiones del sello: pytest mide DEV; TEST se midió **una vez**, al congelar (`python -m
tests.test_foundation_authority_inheritance_gold TEST`, con la rebanada REAL privada fuera de git). De los REAL sólo se
registran ids y la clase del insumo, nunca contenido.

| Caso | Part. | Insumo esperado (de la etiqueta) | Producido |
|---|---|---|---|
| SF031 | DEV | año AMBIGUOUS por TIME plurianual (2023–2025) | AMBIGUOUS `MULTI_YEAR` |
| SF032 | TEST | año PRESENT (único año activo, 2024) | PRESENT 2024 |
| SF033 | DEV | año AMBIGUOUS por ventana relativa | AMBIGUOUS `RELATIVE_WINDOW` (con la marca de A2) |
| SF034 | DEV | año ABSENT (sin TIME previo; lo infiere el reloj) | ABSENT |
| SF035 | DEV | año ABSENT | ABSENT |
| SF101 | TEST | unidad PRESENT (MILLONES) | PRESENT MILLONES |
| SF103 | DEV | unidad PRESENT (MILLONES; el prior sólo trae cota superior) | PRESENT MILLONES |
| SF104 | DEV | unidad PRESENT (PESOS) | PRESENT PESOS |
| SF106 | DEV | unidad PRESENT (MILLONES, hereda en las dos cotas) | PRESENT MILLONES |
| SF167 | DEV | unidad PRESENT (SMMLV) | PRESENT SMMLV |
| SF183 | DEV | unidad PRESENT (MILLONES; el prior sólo trae cota inferior) | PRESENT MILLONES |
| R0496 | TEST | año PRESENT (TIME de un año) | PRESENT |
| R0497 | TEST | año PRESENT (TIME de un mes) | PRESENT |
| R0498 | TEST | año PRESENT (TIME de un mes) | PRESENT |
| R0531 | TEST | unidad PRESENT (única) | PRESENT |
| R0532 | TEST | unidad PRESENT (única) | PRESENT |

Extras del mismo gate (en `M2A_GOLD`, nunca FRONTIER): SF102 (TEST, dos unidades ⇒ AMBIGUOUS `MULTIPLE_UNITS`), SF037 y
SF042 (DEV) y SF174 (TEST): referente PRESENT con 1, 6 y 1 meses (y año PRESENT en SF037 y SF174). **SF168**
(SMMLV sobre 2023–2025) se descartó: el oráculo congelado no puede tipar su prior (`SMMLV_YEAR_NOT_SINGLE`, un
pre-estado inalcanzable), de modo que no llegaba al módulo. Adaptadores declarados (no cambian nada de lo que el insumo
lee): A1, R0531 y R0532 llevan `UNSPECIFIED` + vista del lote F y se tipan con su universo de registros; A2, la marca
`RELATIVE_TO_CLOCK` en la ventana relativa; A3, la traza de la base es `t0`.

## 4. El punto de enganche para M2A-3c (sin tocar C1.1 ni los tipos de valor)

**Lo que queda listo:** `HostInheritanceInputs` y sus tres tipos, con primitivas en cada PRESENT: `unit.unit.value`
(`str`), `year.year` (`int`), `referent.value` (tupla `time@1`), `referent.months`, `inherited_from`,
`source_condition_ids`.

**Lo que los pines imponen y M2A-3c debe respetar:**

1. Un módulo de autoridad (`app/core/host_authorities/*`) **no puede importar** este módulo (`consumer_problems`: ningún
   módulo no registrado importa uno registrado), y un módulo M2a **no puede alcanzar** una autoridad
   (`FORBIDDEN_PREFIXES`: `host_authorities`, registro, contrato, ventana de evidencia). El insumo cruza, pues, **como
   primitivas**, nunca como estos dataclasses.
2. La entrada separada vive en el archivo de cada autoridad, **fuera del Protocol `DimensionAuthority`** (que no cambia, ni
   `AuthorityContext`: ADR-103 D10 «Lo que no cambia»). Es una función o método que sólo el host llama y que devuelve un
   `AuthorityVerdict` con la regla en `receipt.rules`: D-MONEY recibe `unit: str` y `year: int | None` y emite `unit.inherited`
   / `unit.smmlv` / `year.inherited`; D-TIME recibe `year: int` (`year.inherited`) y `referent_value` + `months`
   (`time.relative_to_previous`). Sin insumo, `resolve` se comporta exactamente como hoy (INV-M2A-07).
3. La conversión dataclass → primitivas sólo puede vivir donde se ven los dos lados: el **adaptador de prueba del kit de
   cada autoridad** (en `tests/`) y, en producción, el **runtime del host (M2A-6)**, que deberá revisar a propósito
   `legacy_reach_problems` y los pines de consumo (ya previsto en su paso).
4. Después de la autoridad, los dos huecos de procedencia son de quien construye la condición (`from_acceptance`, M2A-1b, o
   el constructor del parche): `Provenance.unit_source = PREVIOUS_ACCEPTED_VALUE` con `inherited_from = unit.inherited_from`;
   `Provenance.year_source = PREVIOUS_ACCEPTED_TIME` con `inherited_from = year.inherited_from`; `receipt_rules` con
   `unit.inherited`, `year.inherited` o `time.relative_to_previous`. Los ecos `UNIT_INHERITED`, `YEAR_INHERITED` y
   `RELATIVE_WINDOW_RESOLVED` son de M2A-7.
5. **Bloque que el kit congelado necesitaría** (`tests/foundation_authority_kit.py`, C1.1; no se tocó): «conformidad de la
   entrada separada», con tres cláusulas por autoridad: (a) sin insumo, el resultado de `resolve` no cambia; (b) con un insumo
   PRESENT emite la regla y la procedencia; (c) la entrada recibe **un** valor (INV-M2A-17: «nunca de dos unidades ni de varios
   años» queda garantizado aquí, porque AMBIGUOUS no lleva valor). Si el kit no gana el bloque, cada carril lo prueba en su
   propio test.
6. La asignación insumo → código de A.3 no se decide aquí. La natural: unidad AMBIGUOUS, o ABSENT con un monto sin unidad ⇒
   `UNIT_AMBIGUOUS` (H9); año AMBIGUOUS ⇒ `MONTH_YEAR_AMBIGUOUS` (H10) o `SMMLV_YEAR_AMBIGUOUS` (H3); referente ABSENT o
   AMBIGUOUS, o incompatible con la expresión (`months` frente a su longitud) ⇒ `RELATIVE_TO_PREVIOUS_VALUE`.

## 5. Riesgos y puntos abiertos (no los resuelve este carril)

- **R1 — año SMMLV con ventana relativa.** H3 (`foundation_delta_outcome._smmlv_status`) acepta una ventana relativa dentro de
  un año civil (`year.from_time` sobre el TIME de la efectiva); `YearInheritance` dice AMBIGUOUS `RELATIVE_WINDOW`, más
  estricto. D-MONEY decide cuál gobierna `year.inherited` (D10) y los alinea.
- **R2 — la marca de la ventana relativa.** Sin `year_source = RELATIVE_TO_CLOCK` una ventana relativa no se distingue de un
  rango dentro de un año (el tipado del oráculo la omite: hay un test que lo fija). M2A-1b y la autoridad TIME deben emitirla.
- **R3 — pin protegido que se rompe con cualquier lane de la ola 2.** `tests/test_m2a_boundaries.py::
  test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports` parchea `STEPS` con los pasos de la ola 1 y no conoce los
  de la ola 2: en cuanto M2A-3b, M2A-4 o M2A-7 llena su `MODULES`, el mundo no blanquea el módulo real y lo ve como
  consumidor no registrado. Arreglo de una línea, **comprobado en una copia temporal (304 passed)** y no aplicado (archivo
  protegido): añadir `*(_step(n, lanes[n].modules, ("M2A-3",)) for n in ("M2A-3b", "M2A-4", "M2A-7"))` a la tupla
  `registered` de ese test.

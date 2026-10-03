# M2A-3b — herencia tipada: la entrada separada de insumos del host

**Fecha:** 2026-10-03. **Rama:** `mission/m2a-3b-inheritance`, base `d7f53e9b`. **Reparación del NO_GO de ZEUS sobre
`c8d22276`** (= `a2a6e64f` + el arreglo del pin de preparación): §6. **Contrato:** §15.4, §15.5, §16.1, §16.3,
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
| `YearInheritance` (§16.1) | `year.inherited` (ABSENT ⇒ `year.inferred`, del reloj del host) | `year`, `condition_id`, `inherited_from` | `NONCONJUNCTIVE_TIME`, `NEGATIVE_TIME_ONLY`, `MULTIPLE_TIMES`, `RELATIVE_WINDOW`, `OPEN_INTERVAL`, `MULTI_YEAR` |
| `TimeReferent` (§16.3) | `time.relative_to_previous` | `condition_id`, `value` (codificación `time@1`), `first_year`, `last_year`, `months`, `inherited_from` | `NONCONJUNCTIVE_TIME`, `NEGATIVE_TIME_ONLY`, `MULTIPLE_TIMES`, `RELATIVE_WINDOW`, `OPEN_INTERVAL`, `INCOMPATIBLE_GRANULARITY` |

`inherited_from` es el `base_trace_id` que el llamador nombra (el mismo hecho que recibe `compose`, porque una
`AcceptedInterpretation` no lleva su propia traza): es el origen que exigen §15.4 y §11 y el que el tipo `Provenance`
obliga. Procedencia, nunca significado.

## 2. Decisiones

- **M2A3B-D1 — la entrada.** Tres insumos tipados y una unión discriminada (el tipo rechaza toda combinación
  estado/razón/payload que no sea PRESENT con todo, ABSENT con nada, o AMBIGUOUS con una razón de su propio insumo). Las
  funciones sólo aceptan `(base, *, base_trace_id)` y exigen una `AcceptedInterpretation`; el pin de superficie de lectura
  (imports, atributos, firmas) es un test (INV-M2A-07). **Unión de insumos (ZEUS P3):** `HostInheritanceInputs` rechaza
  unir insumos PRESENT que dicen venir de bases distintas (`inherited_from` diferente) y un año y un referente PRESENT de
  condiciones TIME distintas. **Garantía limitada declarada:** un insumo ABSENT o AMBIGUOUS no lleva origen, así que un
  PRESENT de una base junto a un AMBIGUOUS de otra no es detectable en el tipo (hay un test que lo fija);
  `host_inheritance_inputs` es el modo de construir los tres juntos.
- **M2A3B-D2 — unidad.** Cuentan **todas** las cotas VALUE de la base, estén donde estén en `logic` (la unidad es un hecho
  del monto dicho, no de cómo se combinan: contar más sólo puede añadir ambigüedad). Dos `Unit` distintas son dos unidades,
  **incluidas MILLONES y MIL_MILLONES** (SF102). Una cota cuya unidad la base no registra (`Provenance.unit is None`) no es
  una unidad: `UNIT_UNRECORDED`. «Sin cotas» es ABSENT. SMMLV es una unidad más (A2); su vigencia es otro insumo.
- **M2A3B-D3 — TIME activo.** Es el **único TIME positivo**. Los negativos que acompañan a un positivo se ignoran (§16.3
  «único TIME positivo activo»). Varios positivos son ambiguos aunque estén en el mismo año. Cualquier TIME (de cualquier
  polaridad) dentro de un grupo OR/NOT es `NONCONJUNCTIVE_TIME`: un átomo positivo bajo NOT es una exclusión y uno negativo
  bajo NOT, una inclusión; la base no dice cuál sin evaluar la lógica (fail-closed: nunca un año de un periodo excluido).
  **Una base con TIME sólo negativo NO es «sin TIME previo» (§16.1)** (decisión de HERA, ZEUS P2-1): «no en 2024» excluye un
  periodo y no afirma ninguno, y el reloj podría elegir justo el año que el usuario excluyó. Año y referente son
  AMBIGUOUS `NEGATIVE_TIME_ONLY` (razón propia: llamarla `MULTIPLE_TIMES` sería falso con un solo TIME). ABSENT queda para la
  base que no tiene **ninguna** condición TIME. Precedencia cerrada: NONCONJUNCTIVE > MULTIPLE > RELATIVE > OPEN > MULTI_YEAR
  / INCOMPATIBLE_GRANULARITY; `NEGATIVE_TIME_ONLY` no tiene positivo, así que sólo NONCONJUNCTIVE lo precede.
- **M2A3B-D4 — ventana relativa (reescrita por ZEUS P1-1: el contrato SÍ la define).** §16.4: «Relativos al reloj («este
  año», «últimos seis meses»): autoridad TIME del primer turno (`relative_period`)»; §11 la marca con
  `year_source = RELATIVE_TO_CLOCK`; §16.3 la resuelta contra el TIME previo lleva `time.relative_to_previous`; y §3.2 permite
  `year_source = None`, de modo que una base con `relative_period` sin marca de procedencia es alcanzable (así tipa el oráculo
  congelado SF033). Una ventana es **relativa por cualquiera de las tres señales**: el operador `relative_period`, la marca
  `RELATIVE_TO_CLOCK` o el recibo `time.relative_to_previous`. Es AMBIGUOUS `RELATIVE_WINDOW` en año (§16.1 ⇒
  `MONTH_YEAR_AMBIGUOUS`) y en referente (§16.3 ⇒ `RELATIVE_TO_PREVIOUS_VALUE`). Un mes cuyo año se heredó (`year.inherited`) es
  absoluto. **Alternativa a decidir por HERA/ZEUS:** tratar como absoluto el TIME resuelto contra el previo (sólo el recibo)
  permitiría encadenar «el mes siguiente»; se quita un término de `_is_relative`. **Hueco residual declarado (R2):** sólo
  lo que ninguna de las tres señales marca, p. ej. un «este año» tipado `during_year` sin marca ni recibo: es PRESENT y nada en
  el tipo aceptado lo distingue de un año dicho por el usuario; es deber de la autoridad TIME emitir `relative_period` (§16.4).
  Un test lo fija. El oráculo de las pruebas toma el operador del **texto** del contrato (§16 ítem 4), no del módulo.
- **M2A3B-D5 — año.** El año único civil del TIME activo, **sin mirar la granularidad**: un día, un mes o un semestre dentro
  de un año dan ese año. Intervalo abierto o de varios años (un trimestre a caballo de dos años incluido): ambiguo. Nunca el
  primer año de varios (INV-M2A-17).
- **M2A3B-D6 — referente.** PRESENT lleva `months` (meses de calendario enteros) y los años que toca. Un referente
  plurianual (36 meses) **es PRESENT**: que «el año anterior» (12) encaje con él es de la autoridad, que compara `months`
  con la longitud de su expresión (SF175, frontera, es esa decisión, no ésta). Lo que el insumo sí rechaza es lo que no
  admite aritmética de periodo por calendario: un extremo abierto (`OPEN_INTERVAL`) o un extremo que no cae en día 1
  (`INCOMPATIBLE_GRANULARITY`). **Alternativa a decidir:** rechazar también el referente plurianual aquí (una línea en
  `time_referent`), a costa de impedir «los tres años siguientes». **El tipo valida el valor (ZEUS P3):** un `TimeReferent`
  PRESENT sólo acepta un `value` que sea un `time@1` canónico (etiqueta, cinco campos en orden, `excluded=0`, fechas en su
  grafía canónica) y cuyas fechas den exactamente `first_year`, `last_year` y `months`; un valor ajeno (`money@1`) o con
  números que no concuerdan se rechaza.
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
| SF033 | DEV | año AMBIGUOUS por ventana relativa | AMBIGUOUS `RELATIVE_WINDOW` (tipado del oráculo tal cual: `relative_period`, sin `year_source`) |
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
lee): A1, R0531 y R0532 llevan `UNSPECIFIED` + vista del lote F y se tipan con su universo de registros; A3, la traza de la
base es `t0`. **El adaptador A2 (una marca `RELATIVE_TO_CLOCK` añadida a la ventana del oráculo) se retiró: el arnés pasa con
el tipado del oráculo tal cual** (ZEUS P1-1: el arnés pasaba sólo por esa marca).

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
- **R2 — la ventana relativa que ninguna señal marca.** Resuelta para `relative_period`, `RELATIVE_TO_CLOCK` y
  `time.relative_to_previous` (D4). Residual declarado: un «este año» tipado `during_year` sin marca ni recibo es
  indistinguible de un año dicho. M2A-1b y la autoridad TIME deben emitir `relative_period` (§16.4).
- **R3 — pin protegido que se rompía con cualquier lane de la ola 2 (resuelto fuera de esta rama).**
  `tests/test_m2a_boundaries.py::test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports` parcheaba `STEPS` con los pasos
  de la ola 1 y no conocía los de la ola 2. El arreglo es del coordinador y vive en `mission/m2a-wave2-prep` (`1d498be4`,
  `e269af2f`); **esta rama no lo contiene ni lo toca**: sin él ese único test falla en esta rama (con él, el candidato
  `c8d22276` pasó los focales).

## 6. Reparación del NO_GO de ZEUS sobre `c8d22276` (iteración 4 de 4)

| Hallazgo | Reparación | Dónde |
|---|---|---|
| **P1-1** (fondo): `_is_relative` ignoraba el operador `relative_period` (§16.4); con `year_source = None` (§3.2) el año de «este mes», «este año», «últimos 3 meses» y de SF033 con el tipado del oráculo salía PRESENT | una ventana es relativa por **cualquiera** de tres señales: el operador, `RELATIVE_TO_CLOCK`, el recibo `time.relative_to_previous` (D4 reescrita) | `_is_relative` |
| P1-1 (2) `operator` en la superficie de lectura | `READ_SURFACE` lo incluye, y también `NEGATIVE_TIME_ONLY` e `isoformat` | `…_mutants.py` |
| P1-1 (3) test «indistinguible» | invertido: SF033 con el tipado del oráculo tal cual es AMBIGUOUS `RELATIVE_WINDOW`, y la etiqueta dice `MONTH_YEAR_AMBIGUOUS` | `…_gold.py` |
| P1-1 (4) adaptador A2 | **retirado**; el arnés pasa con el oráculo sin tocar (DEV 11/11, TEST 9/9) | `…_gold.py` |
| P1-1 (5) operador en tablas e Hypothesis | forma × operador (4) × `year_source` (5) × recibo (2) en las tablas; el operador y las fechas «rodantes» en las propiedades; el oráculo toma el operador relativo **del texto de §16 ítem 4** (`relative_operators_of_the_contract`), y las frases que parafrasean las tablas están fijadas contra el contrato | `…inheritance.py`, `…_properties.py` |
| P1-1 (6)(7) residual y D4 | residual declarado y fijado por un test (un «este año» tipado `during_year` sin marca es PRESENT); D4 y R2 reescritas | §2 D4, §5 R2 |
| **P2-1** (decisión de HERA): TIME sólo negativo ≠ «sin TIME previo» | año y referente AMBIGUOUS `NEGATIVE_TIME_ONLY` (razón propia); ABSENT sólo sin ninguna condición TIME; D3 reescrita | `_active_time` |
| P3a: insumos de bases distintas | `HostInheritanceInputs` los rechaza (`INHERITANCE_INPUTS_FROM_DIFFERENT_BASES`); año y referente PRESENT de TIME distintos también; límite declarado y fijado (un AMBIGUOUS no lleva origen) | `HostInheritanceInputs` |
| P3b: `TimeReferent` aceptaba cualquier `value` | sólo un `time@1` canónico cuyas fechas den los años y meses que declara | `_referent_value_ok` |

**Un hallazgo propio, de las mismas sondas.** El mutante ZM6 (día del mes igual cuenta como meses) sólo moría por una
propiedad generada: ninguna forma de las tablas tenía el mismo día de mes distinto de 1. Añadí `rolling_month` y
`rolling_year` (y la forma «rodante» a las propiedades): ahora muere por las tablas directas.

### Mutantes (corrí el arnés de ZEUS, `mutate.py`, y el mío)

| Mutante | Resultado | Cómo muere |
|---|---|---|
| **ZM1** (el operador `relative_period` es relativa) = **la reparación** | **SOBREVIVE (pasa): 1041 passed** | variante equivalente e idempotente; los mutantes de la regla reemplazan la función entera, así que siguen revirtiéndola aunque el archivo ya la tenga escrita dos veces |
| **ZR1** (revierte P1-1: el operador no es una señal) | muerto, 66 fallos | tablas, propiedades y arnés Gold sobre SF033 |
| ZM2 (NONCONJUNCTIVE sólo en positivos) | muerto, 12 fallos | tablas de lógica |
| ZM3 (las escalas de pesos son una unidad) | muerto, 56 fallos | tabla 8×8 de cotas, SF102 |
| ZM6 (mismo día de mes cuenta como meses) | muerto, 63 fallos | tablas (`rolling_*`) y propiedades |
| ZM7 (TIME sólo negativo) | su ancla ya no existe (la reparación reemplazó esa línea); invertido: **ZR2** (revierte P2-1) muerto, 66 fallos; y «razón `MULTIPLE_TIMES`» muerto, 65 | tablas, propiedades |
| ZM8 (el año ignora la ventana relativa) | muerto, 393 fallos | tablas |
| **ZR3** (revierte P3a) | muerto, 9 fallos | batería de uniones |
| **ZR4** (revierte P3b) | muerto, 28 fallos | batería de valores forjados |

En total: **53 mutantes de comportamiento** (24 nuevos: unidad 7, TIME 22, argumentos 2, cruce de fechas 1, uniones 4,
valores forjados 10, forma de los tipos 7), **14 variantes que leen más allá de la base** (las caza el pin de superficie de
lectura) y **1 variante equivalente** que debe pasar. 27 de los mutantes de la derivación mueren además por las propiedades
generadas.

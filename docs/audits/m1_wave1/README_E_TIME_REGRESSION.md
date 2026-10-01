# Corpus de regresión para E-TIME

`E_TIME_REGRESSION_CASES.jsonl` recoge, como casos y no como tests, lo que las rondas R1-R6 de
M1 wave 1 fijaron sobre el comportamiento TEMPORAL del camino legacy (`QueryRouter`, Contract
Activity, SUPPLIERS A2, Entity Profile, workflow, paginación). Ese comportamiento se retiró del
suite de legacy por decisión del usuario (opción A, paridad temporal con `main`): legacy vuelve a
leer el tiempo como `main` y el intérprete que se va a borrar ya no recibe parches de delimitación.
E-TIME (autoridad temporal activa de Foundation v5) hereda estos casos como contrato.

Una línea por caso (`1633` casos, salidos de `922` tests retirados):

| campo | contenido |
|---|---|
| `id` | `ETIME-NNNN` |
| `text` | texto del turno (se parsea con reloj civil 2026-10-01) |
| `previous_turns` | turnos previos de la conversación, en orden; `[]` si el caso es autocontenido |
| `today` | reloj inyectado (`2026-10-01`; `expected.today_override` lo cambia en los casos de reloj inyectado) |
| `consumer` | quién lo ejecuta: `QueryRouter.parse`, `workflow.run_query`, `contract_activity`, `suppliers_a2`, `entity_profile`, `pagination`... |
| `expected_behavior` | comportamiento correcto en lenguaje de contrato (aclarar / ejecutar con periodo / comprador conservado) |
| `expected` | forma máquina del mismo contrato: `action` (`execute`, `clarify`, `refuse`), `fecha_desde`/`fecha_hasta` (`null` = cota abierta), `source_calls`, `entidad_resolved`, `buyer_same_as_without_period` (+ `control_text`), `topic_excludes`... |
| `params_override` | sólo en los casos que manipulan los params del parse para probar un guard |
| `round` | ronda de origen (`R1`..`R6`) |
| `zeus_finding` | hallazgo de ZEUS que lo motivó |
| `source_test` | `archivo::test` retirado de donde sale |

`expected_behavior` es la conducta CORRECTA que E-TIME debe lograr, no la salida de `main` ni la de
R6: `main` falla muchos de estos casos por diseño (por eso eran hallazgos). No es una suite ejecutable
hoy; es la entrada de la que E-TIME construye sus oráculos.

## Qué NO está en el corpus (se queda como test)

Los tests que prueban la autoridad por cita o la gramática directamente, sin `QueryRouter`, siguen en
`tests/` y apuntan a `app/core/time_grammar.py`: `test_time_authority*.py`, `test_time_grammar_*.py`
(ventanas ancladas, léxico de ventanas y citas normativas, ancla nominal, cantidad vs año, residuo
congelado como xfail estricto, y el bloque D-TIME movido desde `test_time_semantics_v2.py`).
Los tests de VALUE: su mitad de legacy pasó a `E_VALUE_REGRESSION_CASES.jsonl` (ver `README_E_VALUE_REGRESSION.md`); la mitad de
autoridad por cita (`test_m1_wave1_r{1..4}_value_*.py`, `test_value_authority_m1d.py`) se queda.

## Distribución

Por ronda: R1 204, R2 131, R3 245, R4 44, R5 346, R6 663. Las familias enumeradas por propiedad
(comprador x forma x posición, R5/R6) están expandidas caso por caso; las propiedades de gramática
pura (ventana x ancla) no se expanden: siguen vivas como tests de `time_grammar`.

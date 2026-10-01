# Corpus de regresión para E-VALUE

`E_VALUE_REGRESSION_CASES.jsonl` recoge, como casos y no como tests, lo que M1 wave 1 (D-MONEY y R1-R4) fijó sobre el
comportamiento de VALUE (dinero) del camino LEGACY (`money_polarity`, `QueryRouter`, `workflow.run_query`). Ese comportamiento
se retiró del suite de legacy por decisión del usuario (V-A, paridad completa de VALUE con `main`): legacy vuelve a leer el
dinero como `main` (`app/core/money_polarity.py` es byte-idéntico) y el léxico D-MONEY vive sólo al servicio de la ruta de
cita / Foundation (`money_comparators`, `money_suffix_event`, `money_suffix_place`, `value_authority`,
`foundation_value_direction`). E-VALUE (autoridad VALUE activa de Foundation v5) hereda estos casos como contrato.

`expected_behavior` es la conducta CORRECTA que E-VALUE debe lograr, no la salida de `main` ni la de `2dde6489`: `main` falla muchos
de estos casos por diseño (por eso eran hallazgos). No es un suite ejecutable hoy: es la entrada de la que E-VALUE construye sus
oráculos.

Una línea por caso (`1798` líneas: `1786` casos sueltos + `12` familias que expanden a `58504` casos).

| campo | contenido |
|---|---|
| `id` | `EVALUE-NNNN` |
| `text` | texto del turno (se parsea con reloj civil 2026-10-01); en una familia, la primera instancia |
| `previous_turns` | turnos previos de la conversación, en orden; `[]` si el caso es autocontenido |
| `today` | reloj inyectado (`2026-10-01`) |
| `consumer` | quién lo ejecuta: `money_polarity.resolve_money_polarity`, `QueryRouter.parse`, `workflow.run_query (legacy)` |
| `expected_behavior` | comportamiento correcto en lenguaje de contrato |
| `expected` | forma máquina: `action` (`execute`, `clarify`), `valor_min`/`valor_max` o `bounds` (`bound`: `min`/`max`, `amount` en pesos, `inclusive`), `relation` (`LOWER_BOUND`/`UPPER_BOUND`/`UNKNOWN`) para el consumidor `money_polarity`, `never` (lo que jamás debe ocurrir, p. ej. «ejecutar sin el filtro de valor») |
| `round` | origen: `R1`..`R4` (rondas de wave 1), `D-MONEY` (léxico unificado, M1-D), `ZEUS7` (diferencias main vs `2dde6489` con oráculo escrito a mano) |
| `zeus_finding` | hallazgo que lo motivó |
| `source_test` | `archivo::test` retirado de donde sale (casos `R*`/`D-MONEY`) o la corrida de la que sale (`ZEUS7`) |
| `main_meets_contract`, `main_observed` | (casos sueltos de R1-R4/D-MONEY) lo que `main` hace hoy con ese texto y si cumple el contrato |
| `main_sample_meets_contract` | (familias) `k/n`: sobre una muestra (cada 41.º elemento del producto) cuántos casos cumple `main` |
| `diff_class`, `verdict`, `main_correct`, `cand_2dde6489_correct`, `observed` | (casos `ZEUS7`) clase de la diferencia main vs `2dde6489`, quién acierta según el oráculo y lo observado (`action`, cláusulas `valor_*` emitidas, o `valor_min`/`valor_max`/`money_clarification`/`entidad` del parse) |

## Familias (`kind: "family"`)

Las propiedades de producto (sufijo x continuación, sufijo x lugar...) están en forma compacta: `text_template` + `axes` (la
lista de valores de cada eje; en `suffix` cada elemento es `[frase, familia]`) + `count` (tamaño del producto). El contrato vale
para CADA elemento del producto; expandirlo es `itertools.product` sobre `axes` y `text_template.format(...)`. La «familia» del
sufijo adjetival es: `o|y mayor|mayores|superior|superiores` = cota inferior inclusiva (`valor_min`); `o|y
menor|menores|inferior|inferiores` = cota superior inclusiva (`valor_max`).

## Los casos explícitos del encargo

* **«por lo menos X»** (`main` lo invierte a `valor_max`, cota superior; correcto: `valor_min`, cota inferior inclusiva): 8 casos de
  router `round: D-MONEY` + la cabeza en los casos de polaridad + los textos de la sonda.
* **Pasaban de aclarar a ejecutar bien** (main pregunta, `2dde6489` ejecuta con el filtro de valor correcto): `diff_class =
  main_aclara__cand_ejecuta_con_valor` (698 secuencias de run_query) y, a nivel parse, `diff_class = parse` con `verdict =
  main_incorrecto` (477 textos).
* **25** (`main_aclara__cand_ejecuta_sin_valor`): `main` aclara y `2dde6489` ejecuta SÓLO con el comprador y sin `valor_del_contrato`
  («contratos de aseo universidad nacional sin superar 60 millones»). Contrato correcto: ejecutar con el comprador Y `valor_max =
  60 000 000`, o en su defecto aclarar; jamás ejecutar sin el filtro.
* **38 / 30** (`main_ejecuta__cand_aclara` con `verdict = main_correcto`): casos que `main` ejecutaba bien (sufijo + nombre de
  lugar/comprador, «o mayores la gobernación del atlántico») y `2dde6489` pasaba a aclarar. El número que mide el oráculo escrito
  a mano es **30** a nivel run_query y **14** más a nivel parse (`diff_class = parse`, `verdict = main_correcto`); ZEUS7 contó 38 con otro clasificador; los 143 restantes de la misma clase son todos «más o menos X»,
  donde `main` ejecuta una cota superior inventada y aclarar es lo correcto.
* **20** (`ambos_aclaran_texto_distinto`; 19 con `verdict = ninguno_correcto` están en el corpus, el vigésimo lo resuelven bien ambos y no entra): aclaraciones engañosas,
  p. ej. «alcaldía de Barranquilla hace dos años sin superar 60 millones» pregunta por el nombre de un comprador que contiene la
  frase de dinero. Contrato correcto: ejecutar con el comprador y `valor_max`.

## Qué NO está en el corpus (se queda como test)

Los tests que prueban la autoridad por cita o el léxico directamente, sin legacy, siguen en `tests/`:
`test_value_authority_m1d.py` (guardia, autoridad, léxico; 503), `test_m1_wave1_r{1..4}_value_*.py` (mitad `foundation_value_direction` /
`read_comparators` / `money_suffix_*`) y los casos de legacy que `main` ya cumple.
`foundation_value_direction` y D-MONEY siguen activos en `foundation_v3`.

## Construcción y límites

El oráculo de la parte `ZEUS7` es una tabla de frases escrita a mano (las de `test_value_authority_m1d.py`), sin leer el léxico de
`app/`; los textos sin veredicto del oráculo (193 diferencias de parse sin frase de dinero reconocible) y los que `main` y
`2dde6489` resuelven igual de bien en valor (141 de parse y 40 de run_query, que difieren sólo en hojas no-valor) no entran.
Los rellenos de `previous_turns` son los turnos anteriores de la secuencia e2e.

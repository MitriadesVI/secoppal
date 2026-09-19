# ADR-097 — SUPPLIERS V1 → V1.1: petición explícita de ranking

**Fecha:** 2026-09-18.
**Estado:** candidato entregado para revisión independiente; sin activación, merge, push ni reinicio.
**Misión:** `SUPPLIERS_REQUEST_FLEX_V1` + `ADAPTIVE_PRESENTATION`.
**Base:** `795b628e34cb891266c745f3d588010fc35612bc`.
**Rama:** `feature/suppliers-request-flex-v1`.
**Worktree:** `/Users/rodrigoortiz/Documents/secoppal-suppliers-request-flex-v1`.

## Contexto y autoridad

La capability SUPPLIERS servía una sola vista: los 5 principales por número de
contratos y por valor. Ese 5 era una constante del SQL (`SUPPLIER_LIMIT`) y la
pregunta por una métrica no cambiaba la ejecución: siempre corrían los dos
rankings. La cardinalidad ejecutable era un único número (`EXACT_5`); cualquier
otro exacto terminaba en aclaración.

V1.1 separa las dos decisiones que la constante confundía —qué se ordena y
cuántos se piden— en una petición inmutable que la autoridad de cardinalidad
acredita y que compilador, ejecutor y presentación consumen tal cual.

## Decisión

**`SupplierRankingRequest`** (dataclass frozen en
`app/core/entity_profile/supplier_ranking_request.py`): `metric` + `limit`,
validados en construcción. Es la única forma en que la vista recibe su corte;
nunca se interpola un entero que no pasó por él (el constructor del SoQL lo
vuelve a validar como defensa en profundidad).

**Métricas.**
- `COUNT` = `CONTRACT_COUNT` — «más contratos», «mayor número/cantidad de
  contratos», «quién tiene más contratos».
- `VALUE` = `CONTRACT_VALUE` — «mayor valor contratado», «más valor»,
  «contratos de mayor valor».
- `BOTH` — cuando la pregunta no elige criterio («proveedores de X», «con quién
  ha contratado X»). Un criterio que nombre ambos se lee BOTH.

**Cardinalidad exacta 1..25.** `DEFAULT=5`, `MAX_REQUESTED_LIMIT=25`; el rango
es ejecutable y la spec declara sus miembros generados del mismo máximo. Fuera
de rango, cero/negativo, relaciones no exactas (LOWER/UPPER/RANGE/UNKNOWN),
cuantificadores indefinidos o aproximados («varios», «unos», «todos», «el 10%»)
y evidencia incompleta quedan fail-closed: aclaración, cero llamadas, sin
truncar y sin reinterpretar. Singular implícito (`limit=1`) sólo con evidencia
conjunta: criterio de ranking explícito **y** forma singular inequívoca
(sustantivo en singular o interrogativo «quién»).

**Ejecución: un ranking = un job.** `COUNT` planifica sólo `supplier_count`;
`VALUE` sólo `supplier_value`; `BOTH` ambos. No hay una segunda llamada que
después se oculte; el tope de presupuesto de llamadas no cambia (≤6 por turno).
El carril legacy recibe la misma petición por dos primitivas JSON-safe
(`_supplier_ranking_limit` / `_supplier_ranking_metric`), reconstruida y
validada al llegar; un transporte malformado aclara, nunca cae al default.

**Corte pedido vs testigo interno.** El SQL pide `limit + 1` filas: la fila
extra (`MAX_INTERNAL_FETCH = 26`) es un testigo de empate, nunca un resultado.
Los consumidores publican exactamente `limit` filas.

**Empates.** Cada ranking ejecutado se publica como `RankingWindow`: filas
visibles y si el valor del último visible es igual al del testigo. Un empate en
el primer lugar NUNCA se presenta como líder único: se nombra el valor observado
y el empate («El mayor número observado es 12 contratos y existe empate en el
primer lugar»). En un top-N se declara el empate del corte sin decidir el
orden. Si el borde no es comparable, no se afirma ni unicidad ni empate.

**Completitud.** SUPPORTED / PARTIAL / UNAVAILABLE se preservan. Un ranking que
no respondió no se interpreta como vacío («No se obtuvo el TOP por…; no se
interpreta como un resultado vacío»); un fallo total nombra la causa y su
cláusula impide leerlo como cero. Un ranking que no se pidió no deja fact ni
advertencia: «no lo pediste» no es «no lo obtuve».

**Presentación adaptativa** (módulo propio `supplier_presentation`):
escalar para el top-1 pedido («Proveedor con más contratos / • nombre: 12
contratos»), lista numerada con el N ejecutado para una métrica única
(«Proveedores con más contratos (top 3)»), la vista dual V1 para BOTH, y la
vista JSON con la operación ejecutada (`ranking.{metric,limit,rows,ties}`)
manteniendo `by_count`/`by_value` para los consumidores V1. El texto nunca
muestra el dato del ranking que no se ejecutó.

**Compatibilidad V1.** `V1 ⊂ V1.1`: «proveedores de X» sigue produciendo
BOTH/5, las dos llamadas, los títulos V1 y 5 filas visibles; los vetos de
dinero, modalidad, tema, geografía y actor siguen siendo terminales con cero
llamadas. La paridad BASE↔AFTER sobre los diez casos §3 no tiene regresiones;
los deltas son EXPECTED: una métrica explícita deja de consultar el ranking que
no pidió, y el fetch interno pasa a `corte+1`.

## Remediaciones de la revisión independiente

ZEUS (tres revisores en contexto limpio) encontró tres defectos reales del
candidato y varios observations; los defectos se remediaron con contraejemplo y
test, y las fronteras quedaron fijadas en el corpus:

- **D1 · cantidad colgante.** «el top 30 de los proveedores de X» — el enlace y
  el determinante separaban la cantidad del grupo nominal y el turno se servía
  con la vista por defecto. La gramática ahora marca esa cantidad como residuo
  ambiguo (fail-closed), con ventana corta: un año u otra cantidad lejana no
  califican (el decoy `en 2025 quiero ver proveedores` sigue resolviendo).
- **D2 · familia numérica en palabras.** «veintiséis/doscientos proveedores»
  caían a WORD y el número desaparecía sin residuo. El lexer marca veinti- y
  las centenas como no resueltas → UNKNOWN_QUANTITY (fail-closed).
- **D3 · proporción en palabras.** «el 10 por ciento» ejecutaba mientras
  «el 10%» aclaraba. El detector de proporción cubre el símbolo y las palabras;
  «la mitad/un tercio/la mayoría» se suman a los cuantificadores indefinidos.
- Además: el transporte presente-con-None ya no cae al default; con un solo
  cupo el escalar cubre también BOTH (un empate no nombra líder); sin cifra
  acreditable el título escalar no afirma un primer puesto; y el verificador de
  paridad dejó de ser ciego (dos bugs propios: desempaque de los call
  records y `limits(after)` en vez de `limits(current)`).

Evidencia final: dogfood de 73 casos con los siete ceros duros en cero;
paridad fresca con el mismo harness a ambos lados (30 pares, 13 deltas, todos
EXPECTED_V1_1_DELTA: corte+1 y métrica explícita, 0 regresiones); Ruff limpio;
Semgrep 25→24 con 0 findings nuevos.

## Consecuencias

- Todo el camino de proveedores (Frame y legacy) comparte un único resolver; el
  texto se lee una vez y el resto consume el valor acreditado.
- La futura `CONTINUATION_FOUNDATION_V1` podrá consumir esta petición sin
  rediseñar SUPPLIERS; este ADR no implementa continuidad.
- El carril legacy sólo sirve lo que su plan puede ejecutar; su presentación
  usa el mismo módulo adaptativo.

## Fuera de alcance (diferido explícitamente)

Follow-ups y memoria conversacional nueva; `Continuation Foundation`; filtro de
contratos por monto; nuevos filtros temáticos o de modalidad; composición con
otras capabilities; TOTAL_VALUE; Excel; Radar; guardar/actualizar búsqueda;
nuevos modelos o prompts LLM; cambios al gazetteer; ADR-073; SENA; Actor
Profile. La limitación preexistente `ACTOR_BUYER_SPAN_CONFLICT` (el span del
actor contiene el del comprador en «quién ha contratado por mayor valor con la
Alcaldía de X») queda documentada y adjudicada fuera de este ADR: idéntica en
BASE y candidato, sin autoridad tocada por este cambio.

## Evidencia

`docs/audits/suppliers_request_flex_v1/`: baseline V1 (10 casos × 3 modos),
dogfood del corpus de 68 casos (43 servidas / 24 aclaradas / 1 veto preexistente
adjudicado, siete ceros duros), paridad BASE↔AFTER (30 pares, 0 regresiones),
adjudicación del conflicto actor/comprador, y los harnesses
(`suppliers_flex_probe.py`, `suppliers_flex_dogfood_check.py`,
`suppliers_flex_parity_check.py`). Tests: corpus + propiedades (Hypothesis) +
integración por `SecopalWorkflow.run_query`.

# ADR-100 — Integridad de estados de proceso (PSRI) y cierre de la cadena «convocatoria»

**Fecha:** 2026-09-26.
**Estado:** candidato entregado para revisión independiente (ZEUS del usuario); sin merge, push, activación ni reinicio.
**Misión:** `PROCESS_STATE_INTEGRITY_CLOSE_V1` (opción A, decisión humana del 2026-09-26).
**Base:** `main` `74a9e4d4`.
**Rama:** `fix/process-state-integrity-close-v1` = PSRI (`070aa798`, `99a10b02`) + `f1911a15` + este ADR.
**Numeración:** ADR-098 está en `codex/admission-contract-v1` y ADR-099 en `fix/convocatoria-capability-close-v1` (candidato `NO_GO`, no se integra); 100 verificado libre.

## Contexto

La pila de «convocatoria» (PSRI + papeles de ocurrencia + NOMINAL_COORDINATION_EVIDENCE R1–R3) recibió cinco `NO_GO` de ZEUS. El último, sobre `5efa6568`, mostró que el MARKER de la pila borra «convocatoria» del objeto en una sola petición («contratos de divulgación y convocatoria en Bogotá», «… de beneficiarios»), donde `main` conserva el término. Cada ronda decidía cuándo ese borrado es seguro y ZEUS encontraba otra construcción: una clase sin fondo.

PSRI, en cambio, es independiente de los papeles de «convocatoria»: deja esa palabra exactamente como en `main` y cierra una pérdida silenciosa real, el segundo estado de proceso que `main` descarta o mezcla.

## Decisión

1. **Se integra sólo PSRI.** Los papeles de ocurrencia y NCE R1–R3 quedan aparcados; sus `NO_GO` se conservan como evidencia y sus contraejemplos adjudicados pasan al gold de la base de interpretación por petición.
2. **El embudo abierto absorbe su propio literal bajo una disyunción desnuda.** Exactamente dos menciones de estado unidas sólo por «o»/«u», una que nombra el embudo `oferta_abierta` y otra un literal de ese mismo embudo, son una sola petición del embudo. Se conserva así el comportamiento que el turno real de Manizales tiene en `main` desde mayo (`data/feedback.jsonl`, 2026-05-13). No es una unión de conjuntos: el embudo lleva la política de oportunidades actuales (O1B) que un «publicados» suelto no lleva. La paridad de SoQL acredita compatibilidad con `main`, no el significado de producto. Cualquier otro conector, una tercera mención, un literal de otra familia o una familia que no es el embudo sigue aclarando.

## Garantía

Sobre `procesos`: un estado explícito se aplica exacto (literal) o como familia; dos o más lecturas distintas aclaran con 0 llamadas, salvo el punto 2. Ningún estado nombrado se convierte en tema, se descarta o cambia la fuente a contratos por sí solo.

## Deuda declarada (aparcada, no empeora frente a `main`)

- «convocatoria» sigue como en `main`: «procesos de gestión de convocatorias» se lee como búsqueda de oportunidades (`xfail(strict)` vigente) y «procesos de divulgación, o convocatoria» ejecuta una intersección.
- Fronteras entre peticiones de un mismo turno: `main` pierde una de dos peticiones («procesos publicados en julio y además quiero contratos de aseo» ejecuta sólo contratos).
- Uniones dentro de una familia distinta del embudo («cancelados o suspendidos», «adjudicados o seleccionados») aclaran: fricción declarada (4 textos de tests, 0 consultas reales).
- `SHARED_CONTINUATION_STATE_V1` y `PROCESS_HEAD_CONTRACT_LIFECYCLE_ROUTING_V1` (previas de PSRI).

## Evidencia

Candidato de código `f1911a15`, congelado en el run `r-20260926-131149-f983` (`candidate check = VALID`). Todo contra `main` `74a9e4d4` en worktrees equivalentes de la misma sesión.

- **Adjudicación** (`docs/audits/process_state_integrity_close_v1/ADJUDICATION.md`): de 569 consultas reales únicas cambian 4 (3 arreglos y 1 ambigüedad real); de 164 textos de tests cambian 56, clasificados uno por uno. Ninguno pasa de aclarar a ejecutar. Fricción declarada: 4 casos sintéticos y 0 reales.
- **Consulta efectiva** (`SecopalWorkflow.run_query`, cliente falso, persistencia temporal): los seis turnos del embudo, Manizales incluido, llegan a la fuente con el mismo SoQL que en `main`, byte a byte. En «procesos publicados o adjudicados» `main` pierde «adjudicados» sin avisar; el candidato aclara con 0 llamadas. El P1 de ZEUS sobre `5efa6568` no aplica: «convocatoria» se conserva como en `main`.
- **Global secuencial:** 121 failed + 2 errors en el candidato y 122 + 2 en `main`; nodeids únicos 122 y 123, con 122 compartidos. `CANDIDATE_ONLY = 0`. El único `MAIN_ONLY` es una propiedad de Hypothesis sin `derandomize` en SUPPLIERS (`test_arbitrary_digit_boundaries…`) que expone un comportamiento idéntico en ambos árboles («010 proveedores» se autoriza); es ajena a PSRI y queda anotada aparte.
- **Expandido** (32 archivos): 823 passed / 23 xfailed; 0 regresiones en nodeids comunes; el único XFAIL→PASSED es el `xfail(strict)` que PSRI cumple.
- **Mutaciones** de la regla de absorción: 5/5 muertas. **Ruff:** 0 nuevos. **Semgrep delta** `--base 74a9e4d4`: `NEW_DELTA = 0`.
- **Contratos cambiados y declarados:** la propiedad de conectores de PSRI exige el embudo sólo para ese par bajo «o» desnuda; se retira el `xfail(strict)` «convocatorias publicadas → Publicado», que ya estaba en XPASS-strict (rojo) en `99a10b02`.

## R1 — la decisión se aplica completa (tras ZEUS `NO_GO` sobre `4402f1e1`)

**Hallazgo de ZEUS (P1, clase B, `MODEL_GAP=NO`).** «procesos abiertos o publicados de obra» consultaba el embudo completo, pero «procesos publicados o abiertos de obra» ejecutaba con `estado_del_procedimiento = 'Publicado'`: la garantía del punto 2 no se cumplía en ambos órdenes. La decisión PSRI era la misma en los dos; el defecto estaba al aplicarla.

**Causa.** El productor de familias escribe los filtros del primer estado que lee, y las formas de varias palabras («en borrador») van antes que cualquier palabra suelta. La decisión sólo reemplazaba los filtros cuando elegía un literal; con una familia, dejaba los del productor, y `project_estado_family` respeta, por contrato, cualquier filtro que encuentre. Sobre las 266 parejas que admite la excepción, enumeradas desde el léxico en ambos órdenes, fallaban 128: 112 con el literal primero y 16 con un literal de varias palabras.

**Arreglo, sin nueva regla lingüística.** `apply_process_state_decision` (en `process_state_request.py`) es el único punto que aplica la decisión. Reemplaza todas las claves de estado y escribe el literal exacto o la familia, ésta a través de la autoridad de proyección existente; entidad, fechas, tema y demás condiciones no se tocan. El escalar heredado `estado`/`estado_field` lo calcula una sola función (`legacy_estado_scalar`), que comparten productor y decisión.

**Evidencia R1.**
- Propiedad enumerada desde el léxico (19 superficies de embudo × 7 literales × 2 órdenes = 266), con consulta efectiva vía `run_query`, cliente falso y persistencia temporal: 0 diferencias frente al embudo solo, con tema solo y con comprador y fechas. En `4402f1e1`: 128.
- El literal solo sigue exacto (`= 'Publicado'`, `= 'Borrador'`); comprador y rango de fechas se conservan.
- Planes completos de 665 textos (571 consultas reales únicas de `data/feedback.jsonl` y 94 textos de tests): cambian 3, todos textos de test del caso de ZEUS; 0 consultas reales.
- Mutaciones: 6 de 6 no equivalentes muertas. La séptima (`setdefault` en lugar de asignación del literal) es equivalente por construcción, porque todas las claves se borran antes.

Rollback: revertir la rama; no hay flag ni migración.

## Aclaración (2026-09-26)

Por decisión humana de Rodrigo Ortiz para Foundation V3, la expresión «lleva O1B»
del punto 2 se refiere a la semántica de oferta abierta (familia `oferta_abierta`
+ apertura `Abierto`), no a la ventana de vigencia de fechas. El estado explícito
reemplaza esa ventana, también cuando el embudo absorbe su literal en D03/D04.
La absorción sigue sin ser una unión de conjuntos.

La paridad byte a byte con el embudo de main `68090765` lo confirma. La evidencia
offline aportada con la decisión muestra que ambos órdenes del par, los procesos
abiertos y las oportunidades con estado abierto explícito no emiten la ventana
temporal en el WHERE; las oportunidades sin estado explícito sí la aplican.
El Gold adjudicado por HERMES en `d24653e` ya incluye F3-P16 (2025, recepción
desconocida), de la misma clase que F3-P20 (2015), que permanece incluido.

Exigir vigencia temporal a «abiertos» sería una decisión de producto aparte,
no incluida aquí. Esta aclaración conserva el cuerpo y la evidencia del ADR.

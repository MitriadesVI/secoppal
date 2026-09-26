# Adjudicación PROCESS_STATE_INTEGRITY_CLOSE_V1 frente a `main` 74a9e4d4

Plan real del router (`QueryRouter.parse` + `compute_query_plan`), mismo reloj (2026-09-23), candidato `f1911a15` frente a `main`. Se compara ejecutar/aclarar, fuente, intención, campos de estado y objeto.

## Consultas reales (569 únicas de `data/feedback.jsonl`, 2026-04-17 → 2026-09-24)

Cambian 4; las 565 restantes producen el mismo plan.

| Consulta | `main` | Candidato | Adjudicación |
|---|---|---|---|
| procesos en evaluacion en Antioquia | familia (Evaluación, Seleccionado, Aprobado, En aprobación) | exactamente Evaluación | arreglo: hace lo pedido |
| procesos aprobados en Cundinamarca | sin estado; «aprobados» como palabra del tema | estado Aprobado | arreglo |
| procesos seleccionados en Valle del Cauca | sin estado; «seleccionados» como palabra del tema | estado Seleccionado | arreglo |
| oportunidades de alimentación en evaluación | ejecuta una búsqueda de oportunidades con los estados de evaluación (combinación contradictoria) | aclara | ambigüedad real: oportunidades abiertas y «en evaluación» se contradicen |

El turno de Manizales (`que procesos en convocatoria o publicados tiene la alcaldia de manizales?`) no cambia: misma consulta efectiva que `main`.

## Textos de tests (164; cambian 56)

| Grupo | Qué pasaba en `main` | Adjudicación | N |
|---|---|---|---|
| A | Dos o más estados: `main` ejecutaba uno y descartaba o mezclaba el resto (a veces convertido en tema) | arreglo: no se pierde ningún estado sin avisar | 23 |
| B | Un estado literal: `main` lo ampliaba a toda su familia («cancelados» → Cancelado + Suspendido) | arreglo: se aplica exactamente lo pedido | 8 |
| C | «aprobados»/«seleccionados»: `main` los buscaba como palabra del tema | arreglo: se aplican como estado | 3 |
| D | `main` cambiaba la fuente a contratos al ver «cancelados» junto a otro estado | arreglo: se queda en procesos y aclara | 7 |
| E | Aclaración evitable: unión dentro de una misma familia («cancelados o suspendidos», «adjudicados o seleccionados») o sustantivo leído como estado («evaluación de las convocatorias docentes») | fricción declarada; `main` también ejecutaba mal estos casos (cambiaba a contratos, tomaba la palabra como tema o aplicaba un estado que no se pidió) | 4 |
| F | Contraejemplos HADES/X3/ZEUS de la cadena NCE que el adjudicador exigió aclarar | ambigüedad adjudicada: aclara (aquí por estado) | 6 |
| G | Ya aclaraban en `main`; sólo cambia el motivo | neutral | 5 |

Total: 56.

### A

- procesos (abiertos y aprobados) o abiertos de obra
- procesos abiertos aprobados de obra
- procesos abiertos o (abiertos y aprobados) de obra
- procesos abiertos o (abiertos y aprobados) de obra en antioquia
- procesos abiertos o aprobados de obra
- procesos abiertos o aprobados y publicados de obra
- procesos abiertos publicados de obra
- procesos abiertos y aprobados de obra
- procesos abiertos y publicados de obra
- procesos abiertos ó aprobados de obra
- procesos abiertos, aprobados de obra
- procesos abiertos, publicados de obra
- procesos adjudicados y en evaluacion de obra
- procesos aprobados o abiertos de obra
- procesos aprobados o publicados de obra
- procesos aprobados y abiertos de obra
- procesos cancelados y adjudicados de obra
- procesos cancelados y desiertos de obra
- procesos en convocatoria aprobados de obra
- procesos o contratos abiertos o aprobados de obra
- procesos o contratos aprobados y seleccionados de obra
- procesos publicados o adjudicados de obra
- procesos publicados o aprobados de obra

### B

- procesos cancelados de construccion
- procesos cancelados de construccion en antioquia
- procesos cancelados de obra
- procesos cerrados de mantenimiento en atlantico
- procesos en evaluacion de obra
- procesos o contratos cancelados de construccion
- procesos suspendidos de construccion
- procesos suspendidos de obra

### C

- procesos aprobados de obra
- procesos aprobados de software
- procesos seleccionados de obra

### D

- procesos abiertos y cancelados de obra
- procesos adjudicados y cancelados de obra
- procesos aprobados y cancelados de construccion
- procesos cancelados y aprobados de construccion
- procesos cancelados y publicados o aprobados de obra
- procesos cancelados y suspendidos de obra
- procesos seleccionados y cancelados de obra

### E

- procesos adjudicados o seleccionados
- procesos cancelados o suspendidos de obra
- procesos de evaluación de las convocatorias docentes
- procesos suspendidos o cancelados de obra

### F

- procesos de divulgación y en evaluación y convocatoria
- procesos de divulgación y evaluación y convocatoria
- procesos de divulgación y publicados y convocatoria
- procesos de divulgación y que sean cancelados y convocatoria
- procesos o contratos de divulgación y en evaluación y convocatoria
- procesos y contratos de divulgación y en evaluación y convocatoria

### G

- ... y evaluación y convocatoria
- procesos abiertos o cancelados
- procesos abiertos o publicados o cancelados
- procesos aprobados de obra en sucre
- procesos seleccionados de alimentacion en bolivar

## Lectura

Ningún cambio convierte en ejecución algo que `main` aclaraba. El único costo son las aclaraciones de E (4 casos sintéticos): uniones representables dentro de una familia que PSRI no compone por diseño; en los cuatro `main` también ejecutaba mal. En tráfico real la fricción observada es 0 y la ambigüedad legítima es 1 de 569.

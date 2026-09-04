# ADR-077 — Calificador territorial oficial terminal en identidad municipal

**Fecha:** 2026-09-04
**Estado:** Accepted — autoridad candidata de conocimiento offline; runtime sin cambios.
**Evidencia:** [Municipal M2B.1](../gazetteer/MUNICIPAL_M2B_1.md),
[new_identity_rules.json](../../artifacts/audits/municipal_m2b_1/new_identity_rules.json),
[adjudicated_delta.json](../../artifacts/audits/municipal_m2b_1/adjudicated_delta.json).

## Problema y experimento

ADR-076 acepta la omisión de una etiqueta legal ESE cuando el núcleo institucional
restante coincide exactamente con la entidad propia SIGEP. M2B.1 encuentra otro
desacuerdo repetido: una superficie agrega al final el nombre completo del
municipio acreditado y la otra lo omite. El topónimo puede describir ubicación;
por sí solo nunca identifica una BuyerUnit.

El experimento `M2B1_R1_OFFICIAL_LOCATION_QUALIFIER` contrasta los 497 perfiles
nuevos de las clases dirigidas F/C/I/H. Empareja 72, acepta 28 y rechaza 44 por
la cadena de autoridad y los canarios. La ganancia aceptada es 7.659 procesos
únicos y 162 filas físicas de contratos. No hay aplicación productiva.

## Decisión acotada

Se acepta el retiro de **un municipio completo acreditado, exclusivamente como
calificador terminal**, para formar un candidato de identidad offline cuando:

1. El perfil conserva afiliación territorial `VERIFIED` congelada y no es Bogotá.
2. El calificador es la cadena DIVIPOLA municipal completa; en municipios
   compuestos se trata como una unidad indivisible. No se retiran departamentos,
   gentilicios, prefijos, barrios ni aproximaciones del topónimo.
3. La operación ocurre una sola vez, en el extremo final, después del manejo de
   etiqueta legal ESE de ADR-076. Debe existir el calificador en al menos una
   superficie y las superficies originales deben ser distintas.
4. Tras retirarlo queda un núcleo institucional distintivo. La secuencia de tokens
   restante coincide exactamente y no puede reducirse a hospital, empresa,
   instituto, centro, puesto, salud, local, regional o el municipio solo.
5. Una única entidad propia SIGEP del mismo territorio satisface el match. Su
   naturaleza jurídica acredita una familia institucional compatible y todo el
   código SIGEP mantiene alcance Municipal/Distrital en ese territorio.
6. La cuenta y el nombre físico aparecen byte-exactos en el directorio propio.
   Cualquier otra superficie no explicada en esa cuenta, otra cuenta que reclame
   la entidad o un canonical competidor bloquea.
7. El NIT participa únicamente como veto de contradicción. No genera el candidato,
   no selecciona la entidad y no define el ID lógico ni una fusión.

El ID candidato conserva el código SIGEP y la cuenta. No se editan nombres
físicos ni evidencia M0–M2B.

## Exclusiones y consecuencias

No se acepta eliminar un topónimo interno, encadenar este retiro con otra regla
nueva, inferir identidad desde territorio/familia, corregir ortografía, usar
fuzzy/substring o unir cuentas. Un segundo código institucional o una cuenta
competidora conserva `REVIEW`.

La regla amplía sólo la adjudicación de conocimiento offline. Sus 28 casos son
candidatos; no se promueven a aliases, bindings, BuyerGroups ni filtros runtime.
La ausencia de competidores está limitada a la captura congelada y produce
falsos negativos deliberados cuando la evidencia es incompleta.

## Numeración y validación

Después del experimento se escanearon `docs/`, incluidos ocultos e ignorados,
en los 29 worktrees registrados. El máximo global fue ADR-076; el barrido está
en [adr_scan.json](../../artifacts/audits/municipal_m2b_1/adr_scan.json). Por eso
esta decisión reserva ADR-077. Los tests cubren municipio compuesto, núcleo
genérico, municipio incorrecto, conflictos fiscales, cuentas competidoras,
Bogotá y dos hospitales distintos con el mismo NIT.

# M1-B — Adjudicación y sellado del Gold REAL (v1)

Fecha: 2026-09-30. Adjudicador: HERA (orquestador, Opus). Contrato: M1.0 v1.3 (§11–§13).

## Etiquetado

| Paso | Quién | Alcance |
|---|---|---|
| Etiquetador A | Opus (dos instancias ciegas, shards A y B) | 585 REAL + 35 SYNTHETIC |
| Etiquetador B | Sonnet, ciego a A | muestra estratificada de 365: todas las no-SEARCH, todas las sintéticas, 110 SEARCH con dimensiones raras, 60 SEARCH al azar (semilla 20260930) |
| Adjudicación | HERA | 74 desacuerdos crudos → 8 reglas de contrato (v1.3) + 16 decisiones caso a caso |

Las consultas REAL contienen datos personales: el Gold con texto vive en
`~/Documents/tools/secoppal-diagnostics/m1_search_gold/sealed_v1/`. En git sólo van el sello y los
agregados sin texto (`gold/REAL_SEAL_v1.json`, `gold/agreement_real_*.json`).

## Acuerdo (365 doble etiqueta)

| Campo | Crudo | Normalizado (reglas mecánicas v1.3) |
|---|---|---|
| operation (κ) | 0,979 | 0,979 |
| operation | 98,6 % | 98,6 % |
| universes | 93,7 % | 98,9 % |
| turn_context | 94,5 % | 98,9 % |
| product_view | 98,6 % | 98,6 % |
| expected_disposition | 99,2 % | 99,2 % |
| dimensions+polarity (SEARCH) | 91,0 % | 91,0 % |
| conditions exactas (SEARCH) | 87,1 % | 89,6 % |

F1 por dimensión (normalizado): ACTOR 1,00 · BUYER 0,99 · GEOGRAPHY 0,99 · VALUE 0,98 ·
TOPIC 0,99 · TIME 0,96 · ORDER 0,95 · MODALITY 0,94 · STATE 0,85 · CONTRACT_TYPE 0,67 ·
UNSUPPORTED 0,67. Las dos bajas se explican por C5 («convenios») y C8 (etiquetas libres); STATE
por C3/C4. Tras v1.3 no queda ninguna clase de desacuerdo sin regla.

Las 255 consultas con una sola etiqueta se aceptan con la etiqueta A normalizada: el acuerdo de
la muestra estratificada (sobremuestreada en dimensiones raras) no justifica un tercer pase.
Riesgo residual declarado: errores aislados de A fuera de la muestra; el triage por etapa de A3
los hará visibles como «Gold dudoso» si aparecen.

## Clases de desacuerdo → regla

| Regla | Casos | Ganó |
|---|---|---|
| C1 quote ACTOR sin marcador | R0020, R0021 (+ mecánico en todo el Gold) | B |
| C2 turn_context de FOLLOWUP/PROTOCOL | 20 | B |
| C2 referente anafórico con operación clara | R0398, R0444, R0441, R0459 | A |
| C3 deícticos no son TIME | R0191, R0273, R0354, R0528, R0535 | A (R0191 cita mínima de B) |
| C4 is vs family en STATE | R0069, R0151, R0216, R0298 | regla |
| C5 «convenios» = CONTRACTS + CONTRACT_TYPE | R0328, R0329, R0340, R0343, R0557, R0558 | A |
| C6 sustantivo explícito > verbo | R0342 | A |
| C7 no-SEARCH sin universo | 15 RANK_SUPPLIERS/PROFILE | B |
| C8 UNSUPPORTED por span; «mayor cuantía» = ORDER; jerga monetaria = VALUE | R0229, R0231, R0507, R0579, S022, S023 | A |
| §6 nombre desnudo = PROFILE_ACTOR ejecutable | R0346 | B |
| B6 municipio sujeto = BUYER | R0216, R0553 | B |
| Ambigüedad sobredeclarada | R0219, R0361, R0377, R0389, R0415, R0435, R0510, R0511, R0525 | CLEAR |
| Operación analítica | S034 SUM_VALUE·BY_YEAR | A |

Decisiones con su porqué: `gold/REAL_SEAL_v1.json` → `decisions`.

## Decisión de producto P1

Sin regla BUYER→CONTRACTS en la Product View Policy; PV1 con año en curso se mantiene. Ver
contrato §13.

## Sello

`schema_version m1-search-gold/1.3` · 620 registros · 365 doble etiqueta ·
sha256 del archivo privado en `gold/REAL_SEAL_v1.json`. Reproducible con
`scripts/audits/m1_search/gold_adjudicate.py labels_A labels_B sealed_v1`.
Los slices sintéticos (NEIGHBOR, MINIMAL_PAIR, ADVERSARIAL) se sellan aparte tras su segundo
etiquetado ciego (Opus, porque los escribió Sonnet).

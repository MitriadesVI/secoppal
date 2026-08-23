# SRADAR-2 — veredicto final de Adulto Mayor Radar

**Fecha:** 2026-08-23

**Rama:** `exp/semantic-radar-adulto-mayor-final`

**Freeze de labels:** `405f35892e47242b9efc8532470dc229558406d8`

**Alcance:** evaluación post-freeze; sin cambios a clasificadores, Registry ni motor genérico

```text
LABEL_FREEZE:
PASS

MAP_OPENED_AFTER_FREEZE:
YES

LABEL_ROWS:
240

B2:
notifications: 67 (58 sobre gold yes/no; 9 sobre uncertain)
precision: 0.913793
recall: 0.913793
f1: 0.913793
false_positive_rate: 0.029412
abstention_rate: 0.000000
hard_negative_notifications: 1
false_notifications_per_100_candidates: 2.083333
false_notifications_per_100_notifications: 8.620690

D:
notifications: 42 (42 sobre gold yes/no; 0 sobre uncertain)
precision: 1.000000
recall: 0.724138
f1: 0.840000
false_positive_rate: 0.000000
abstention_rate: 0.808333
hard_negative_notifications: 0
false_notifications_per_100_candidates: 0.000000
false_notifications_per_100_notifications: 0.000000

D_FIXES_B2_FALSE_POSITIVES:
5

D_FIXES_B2_HARD_NEGATIVES:
1

D_RECOVERS_B2_MISSES:
2

D_LOSES_B2_POSITIVES:
13

SEMANTIC_CAUSAL_WINS:
2

SEMANTIC_CAUSAL_LOSSES:
1

R1_PRECISION:
PASS

R2_HARD_NEGATIVES:
PASS

R3_UNIQUE_VALUE:
PASS

R4_RECALL_COST:
FAIL

R5_MAINTAINABILITY:
PASS

ADULTO_MAYOR_RADAR_SEMANTIC_NO_GO
```

## Respuesta de producto

En 100 candidatos similares, B2 produciría aproximadamente **2,1
notificaciones falsas** y D **0**. Por cada 100 oportunidades Adulto Mayor
reales, B2 avisaría aproximadamente **91,4** y D **72,4**.

D limpia las cinco falsas alertas observadas y la única contaminación hard
negative, pero el precio es once alertas verdaderas netas menos: recupera dos
misses de B2 y pierde trece positivos que B2 sí notificaba. El intercambio no
es aceptable para un Radar diario: eliminar unas dos falsas por cada 100
candidatos no compensa dejar de avisar casi diecinueve oportunidades reales
adicionales por cada 100 oportunidades reales.

## Cadena de custodia

Antes de decodificar el MAP se verificó:

- `blind_review.csv` en 240 filas, SHA-256
  `e196ed2045da8bc32c7a3861a44d115301307146aac50002d936a4fe90043f27`,
  idéntico al manifiesto de muestra;
- CSV adjudicado separado, 240 filas, SHA-256
  `c885f865a87e222d4473ef16de2657109e7d3010aec4db8022b4618d58efa6fd`;
- 240 `review_id` y 240 `stable_key` únicos, sin labels vacíos y con valores
  restringidos a `yes | no | uncertain`;
- conteos `market_membership`: 58 yes, 170 no, 12 uncertain;
- conteos `hard_negative`: 38 yes, 190 no, 12 uncertain;
- cero cambios en los siete campos protegidos frente al blind original;
- B2 congelado y las seis autoridades congeladas de D con hash exacto.

El commit atómico de labels incluyó únicamente el CSV adjudicado y
`radar_labels_manifest.json`. Después de ese commit se decodificó el MAP. El
join por `review_id` y `stable_key` cerró 240/240, sin faltantes ni duplicados.
Los labels no se editaron después de abrir el MAP.

## Política métrica y matrices

El gold principal incluye 228 filas (`yes/no`); las 12 `uncertain` quedan fuera
de los denominadores principales. `POSITIVE` significa NOTIFY. `NEGATIVE` y
`ABSTAIN` significan NO NOTIFY en las métricas binarias de producto, pero las
abstenciones permanecen separadas en la matriz de traza.

| Brazo | TP | FP | TN explícito | FN explícito | Abstain/positivo | Abstain/negativo |
|---|---:|---:|---:|---:|---:|---:|
| B2 | 53 | 5 | 165 | 5 | 0 | 0 |
| D | 42 | 0 | 4 | 0 | 16 | 166 |

Al plegar `ABSTAIN` como NO NOTIFY, D tiene TN=170 y FN=16. B2 no emite
abstenciones. La tasa de falso negativo de notificación es 8,62% en B2 y
27,59% en D. D abstiene 16/58 positivos humanos y 166/170 negativos humanos;
su tasa total de abstención es 194/240 = 80,83%.

La contaminación hard negative se reporta sobre notificaciones cuyo label
hard-negative es evaluable (`yes/no`): B2 1/58 = 1,72%; D 0/42 = 0%. Vista
como tasa de alerta sobre los 38 hard negatives humanos, B2 notifica 1/38 =
2,63% y D 0/38.

`false_notifications_per_100_candidates` usa las 240 filas de la muestra.
`false_notifications_per_100_notifications` usa `FP/(TP+FP)` sobre el gold
principal, por lo que es exactamente el complemento de precision.

## Diferencias B2 frente a D

### D_FIXES_B2_FALSE_POSITIVES — 5

Los cinco casos terminan en `ABSTAIN` con
`NO_SUFFICIENT_FROZEN_SEMANTIC_EVIDENCE`. Su atribución es
`ABSTENTION_POLICY`, no una victoria semántica causal.

- `SR1-7B56C718BE8839`: “centro de bienestar universitario”; hard negative
  real. D suprime la falsa alerta, pero sin concepto, relación o qualifier
  causante.
- `SR1-F1B36763A88BC4`: servicio de medicina interna y geriatría; D abstiene
  sin cobertura semántica.
- `SR1-AD38FF6F58FA60`: servicio Centro Día Casas de la Sabiduría; D abstiene
  sin traza semántica.

El subconjunto `D_FIXES_B2_HARD_NEGATIVES` contiene solo
`SR1-7B56C718BE8839` y también se atribuye a `ABSTENTION_POLICY`.

### D_RECOVERS_B2_MISSES — 2

Ambos son `ROW_EVIDENCE` y sí cuentan como victorias semánticas causales:

- `SR1-9FCB03D110B931`: la fila contiene `personas mayores` y, por separado,
  `centro de vida sensorial`. B2 aplica la exclusión lexical global
  `sensorial`; D conserva la evidencia directa de `persona_mayor` y solo
  bloquea la herencia de la asociación del centro sensorial.
- `SR1-18B920DEF95F22`: la fila contiene explícitamente `adulto mayor` además
  de otro lote sensorial. D usa la evidencia directa requerida; B2 vuelve a
  caer por la exclusión global.

### D_LOSES_B2_POSITIVES — 13

Los trece son abstenciones `NO_SUFFICIENT_FROZEN_SEMANTIC_EVIDENCE`, por lo
que se atribuyen a `ABSTENTION_POLICY`/cobertura, no a reglas semánticas
activas. Los grupos dominantes son:

- `envejecimiento activo`: 3;
- `gerontología`: 4;
- `centro día`, `centro gerontológico` o `centro de bienestar`: 5;
- `hogar geriátrico`: 1.

Ejemplos representativos: `SR1-5D9137A27504B1` (envejecimiento activo),
`SR1-CB5A015BD59DD3` (gerontología y tercera edad),
`SR1-82C6A1EB63108D` (centro gerontológico/centro día) y
`SR1-595DEF42DD8100` (hogar geriátrico).

### Atribución semántica

Hay dos `SEMANTIC_CAUSAL_WINS`, ambos `ROW_EVIDENCE`, descritos en las
recuperaciones anteriores. Hay un `SEMANTIC_CAUSAL_LOSS` de tipo `QUALIFIER`:
`SR1-2A552648D79BB7`. La relación
`centro_de_vida--QUALIFIER-->sensorial` bloquea la herencia y D abstiene, aunque
la misma descripción dice explícitamente “población ... adulta mayor”.

La relación `centro_de_vida--ASSOCIATED_WITH-->adulto_mayor` y su qualifier
`sensorial` aparecen juntos en siete trazas. Ningún positivo de D fue causado
por `ASSOCIATED_WITH + optional role`; los 42 positivos de D provienen de
evidencia directa de un concepto poblacional requerido.

## LABEL_REVIEW_CANDIDATE

Sin modificar los labels congelados, quedan registrados como candidatos de
revisión futura `SR1-AD38FF6F58FA60` y `SR1-8E74E4534E2957`: ambos textos
visibles dicen “servicio Centro Día Casa(s) de la Sabiduría”, mientras la nota
adjudicada afirma que no hay evidencia visible vinculante. No se recalculó
ninguna métrica con labels alternativos.

## Gates congelados

| Gate | Resultado | Evidencia |
|---|---|---|
| R1 Precision | PASS | D 1,000000 vs B2 0,913793; delta exacto +0,086207, mayor que el margen -0,03. No existía bootstrap congelado para este Radar y no se inventó uno. |
| R2 Hard negatives | PASS | D 0 notificaciones hard negative vs B2 1. |
| R3 Unique value | PASS | D recupera dos misses reales mediante evidencia directa de rol requerido; no son simples abstenciones. |
| R4 Recall cost | FAIL | Recall cae 0,189655; D abstiene 16/58 positivos, emite 11 TP netos menos y abstiene 80,83% de toda la muestra. |
| R5 Maintainability | PASS | Registry, B2 y D intactos; cero reglas post-hoc y cero lógica Adulto-Mayor-specific añadida al motor genérico. |

## Complejidad y coste humano

| Medida | B2 | D |
|---|---:|---:|
| Términos positivos | 36 | — |
| Exclusiones | 14 | — |
| Entradas revisadas | 50 | — |
| Pasadas humanas | 2 | — |
| Conceptos configurados, incluidos blockers | — | 6 |
| Conceptos de soporte relacional | — | 2 (`centro_de_vida`, `sensorial`) |
| Relaciones consumidas | — | 1 `ASSOCIATED_WITH` + 1 `QUALIFIER` |
| Config Adulto Mayor, LOC físicas | — | 37 |
| Adapter D del harness Radar, LOC físicas | — | 118 |
| Evaluador genérico, LOC físicas | — | 343 |
| Contratos genéricos, LOC físicas | — | 97 |

El número de LOC no es por sí mismo una desventaja. El problema es el retorno:
la capa compra dos recuperaciones semánticas y cero FP, pero su cobertura
congelada deja fuera vocabulario de producto importante y produce trece
pérdidas frente al OR humano.

## Validación

```text
joins 240/240: PASS
review_id faltantes: 0
review_id duplicados: 0
stable_key duplicados: 0
hashes intactos después de abrir MAP: PASS
labels intactos después de abrir MAP: PASS
B2/D intactos: PASS
recomputación independiente de métricas: PASS
git diff --check: PASS
tests focales: 63 passed
suite completa: no ejecutada; no se modificó código compartido
```

## Veredicto y cierre

`ADULTO_MAYOR_RADAR_SEMANTIC_NO_GO`

D no debe adoptarse como filtro obligatorio de notificación para Adulto Mayor.
Su precisión perfecta depende materialmente de abstenerse: casi nunca declara
`NEGATIVE` (4/240) y abstiene 194/240. El valor semántico causal existe, pero
es insuficiente frente al coste de recall. Conforme a la regla de cierre, no se
abre otro sprint de corrección Adulto Mayor.

```text
NEXT:
CLOSE_SEMANTIC_MARKET_PACK_SPIKE
```

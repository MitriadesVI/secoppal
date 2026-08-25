# HCA-R4-S1 — Negation Binding Infrastructure

**Estado:** PASS — implementación, regresión amplia y revisión adversarial
cerradas en el commit único de R4-S1.

**Base:** `84b64e56ca63acd37c987770ca5f074753e5bd72`

**Rama:** `feature/hca-r4-s1-negation-binding`

## Arquitectura implementada

- `negation_markers.py` es la única fuente de vocabulario compartido. Modalidad
  importa sus operadores, anti-negaciones y blockers desde allí sin cambiar su
  modelo de polaridad.
- `negation_binding.py` produce `NegationRequest(dimension, value/span, marker,
  scope_anchor, verdict, context)` antes de geografía, actor, tiempo y objeto.
  Distingue OBJECT, CITY, DEPARTMENT, BUYER, ACTOR, MODALITY, CONTRACT_TYPE,
  STATE y TIME.
- La disciplina de gap protege aliases ya resueltos, comparadores monetarios y
  anti-negaciones de dominio. Los marcadores solo gobiernan texto residual.
- Los spans negativos se sustituyen por espacios, conservando offsets, antes de
  cualquier extractor positivo y antes del fallback LLM. Por ello no pueden
  activar ciudad, `_CITY_TO_ENTITY`, fechas, actor/comprador, objeto ni intentos
  positivos de follow-up.
- La representación se transporta como metadata efímera del turno y no amplía
  `QueryFrame`. Las políticas diferidas fallan cerradas antes de clasificación
  contextual o ejecución. MODALITY y las formas OBJECT históricas ya
  ejecutables conservan su comportamiento.
- `ni` propaga la polaridad; `sino` cierra el span negativo y abre el residual
  positivo. La coordinación no introduce un AST ni lógica de ejecución OBJECT.

## Pruebas y auditoría

- Suite focal nueva: 42 passed.
- Equivalencia completa de modalidad: 3371 passed en 181,41 s sobre la forma
  final de la fuente compartida.
- Parser/money relacionado: 287 passed.
- Follow-up y mutaciones: 347 passed.
- Planning, diálogo, SoQL y workflow vecinos: 231 passed.
- Actor/profile vecino: 151 passed.
- Regresión amplia final: `make test` → 8339 passed, 2 skipped, 23 xfailed en
  417,47 s; `lint-core` PASS.
- Corpus holístico congelado: 164 PASS / 31 FAIL / 0 ERROR en 14,52 s. El
  registro histórico original era 156/39; esta ejecución no atribuye toda la
  diferencia a S1. Cuatro llamadas LLM devolvieron 402 por saldo insuficiente y
  el runner aplicó su fallback regex previsto, sin errores de caso.
- Auditoría adversarial manual: 26 variantes sobre marcadores, gap, `ni`,
  `sino`, dinero, aliases y fugas positivas. Encontró y corrigió dos regresiones
  dentro de alcance: comparadores monetarios protegidos incompletamente y el
  conector `pero` filtrándose a OBJECT.
- Fixture preregistrado `negation_focal_v1.yaml`: sha256
  `f4f64a33ea7009eb1da9fa7d3c910e67010e6dafc6b4c5b6932901c27fad9a1d`,
  intacto. El runner longitudinal da 13 PASS / 11 FAIL / 0 ERROR. Los fallos
  restantes corresponden a oráculos end-to-end de S2/S3/S4 (OBJECT ejecutable,
  resta departamental, contradicción y lifecycle), más dos controles que el
  runner compara solo contra `params` y no observa la metadata efímera de S1.
  Los invariantes estructurales equivalentes sí están cubiertos por tests.

## Trabajo deliberadamente diferido

- **R4-S2:** ejecución OBJECT end-to-end, SoQL null-safe, contradicción OBJECT y
  proyección completa de `ni`/`sino`.
- **R4-S3:** políticas CITY/TIME, canonicalización geográfica, álgebra temporal,
  resta DEPARTMENT y contradicciones de esas dimensiones.
- **R4-S4:** BUYER/ACTOR/STATE/CONTRACT_TYPE y lifecycle genérico de exclusión.

## Revisión de regresiones

Modalidad, dinero, consultas positivas y follow-ups vecinos: **0 regresiones
observadas**. No se modificó el fixture preregistrado ni se añadieron campos de
negación a `QueryFrame`. `ruff` sobre las 10 rutas Python tocadas: PASS. El
target global `make lint` sigue fallando por 271 hallazgos preexistentes en
rutas ajenas al sprint; no se modificaron. Los demás gates fueron `lint-core`,
`git diff --check` y la suite completa.

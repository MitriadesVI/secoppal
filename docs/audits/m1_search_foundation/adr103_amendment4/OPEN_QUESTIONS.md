# Preguntas abiertas de ADR-103 enmienda 4, v2

v3. Las versiones anteriores se conservan en `OPEN_QUESTIONS_v1.md` y `OPEN_QUESTIONS_v2.md`. El usuario respondió el 2026-10-01: «ok vamos con tus recomendaciones»
(`USER_DECISIONS_2026-10-01.md`). Después decidió congelar los datos de legacy (D1(d)).

## Decididas (incorporadas a la enmienda v2)

| Id | Decisión | Dónde |
|---|---|---|
| OQ-1 | SMMLV sin TIME ⇒ año en curso por reloj: `year.inferred`, `CLOCK_INFERRED`, cifra en pesos anunciada. Sin re-sello (R0305 queda `EXECUTE`). Multianual o año no catalogado ⇒ `SMMLV_YEAR_AMBIGUOUS`. | D10 |
| OQ-2 | Cobertura C. Gate duro del CUTOVER sólo para vocabulario cerrado: 0 `L-only-OK` añadidos, y la sobre-aclaración se mide y se publica **sin umbral**. Activar la cobertura completa queda fuera del CUTOVER: exige ≤ 5 % en EXECUTE CLEAN con v5 y los verbos citados como evidencia de operación. (El «≤ 5 %» de la v2 en el CUTOVER era un error del orquestador y queda retirado en la v3.) | D5 |
| OQ-3 | Relativo temporal en `FOLLOWUP_DELTA` ⇒ gana el único referente activo de granularidad compatible, anunciado con fechas absolutas. El reloj sólo aplica a consultas autosuficientes. | D7 |
| OQ-4 | Rango sin interior ⇒ CLARIFY con dos opciones-evento. | D7 |
| OQ-5 | AMBIGUOUS fuera de los gates de cero. Los CLEAR que dependen de X7–X17 o de `UNIVERSE_FRAGMENT` se adjudican por lote antes del sello. | D7 |
| OQ-6 | Cifra de top-N ≥ 10⁹ ⇒ CLARIFY `CARDINALITY_FIGURE_OUT_OF_RANGE`. Es política de E-ORDER; el tipo sólo lleva el límite de representación 10¹⁸ − 1. | D9; `C1_2_TOPN_SPEC.md` §7 |
| D1(d) | Datos que consume legacy congelados byte a byte hasta el CUTOVER. Las correcciones van a `app/host_data/`. | D1(b,d); `C1_2_FRONTIER_SPEC.md` pin 3 |
| WIP C1.2 | Se commitea tal cual antes de aplicar las specs. | §4.4; specs §0 |

## Sigue abierta (deuda de METIS, no de producto)

- **OQ-7.** El contrato M1 (§17.4, §18, §19.2 y §19.3.4) pide cambios en C0 §5.6 antes de que F escriba:
  `HostViewDecision` para ACTOR×STATE, el aviso `ACTOR_TIME_ON_SIGNATURE_DATE` y la incoherencia de PV2.
- **Qué bloquea:** F y, por transitividad, el CUTOVER. No bloquea C1.2 ni los E-steps.
- **Siguiente paso:** encargo METIS aparte.

## Desviaciones de la revisión ZEUS que necesitan el visto bueno del orquestador (no son de producto)

1. **Lista blanca de `SemanticDimension` (punto 6).** ZEUS pidió permitir en legacy «sólo `import` y
   `SemanticDimension.<miembro de main>`». Sin más, el pin falla sobre código idéntico a `main`:
   `contract_type_resolver.py:48` usa `SemanticDimension` como **anotación de tipo**. El spec añade una tercera
   posición permitida, «dentro de una anotación». Con `from __future__ import annotations` una anotación no se evalúa.
   **Recomendación:** aceptar.
2. **`ALLOWED_DIVERGENCE` mínima (punto 5).** En C1.2 se cumple: `{semantic_dimensions, foundation_guard}`. Pero D3
   (reutilizar el **tipo** `AccreditedSpan` sin importar el flujo de `accreditation`, como pidió ZEUS en el punto 11)
   obliga a ampliarla en **E-0**:
   - con `accreditation.authorities`, cuyo cambio es reexportar el tipo desde la hoja;
   - y con la hoja nueva `accredited_span`.
   **Alternativa:** un tipo de span propio del núcleo. No toca legacy, pero añade un tercer tipo a la deuda H13.
   **Recomendación:** ampliar en E-0, con su motivo declarado.

## Resolución del orquestador (2026-10-01)

- **(a) Las dos desviaciones de la v2 quedan ACEPTADAS.**
  1. La lista blanca de `SemanticDimension` admite también la posición «dentro de una anotación de tipo».
  2. Las divergencias permitidas crecen en E-0 por `AccreditedSpan` (`accreditation.authorities` y la hoja
     `accredited_span`), cada una con motivo declarado y revisión de ZEUS.
- **(b) `foundation_shadow_hook` sobrevive hasta M4** como instrumento SHADOW observacional. Aceptado; ZEUS verificó que
  no viola INV-OWN.
- **(c) Dos P3 de ZEUS quedan como deuda:**
  1. un pin de no-consumo de las filas SHADOW/LEGACY_SHADOW del ledger y de `legacy_shadow.compare` fuera del hook;
  2. precisar o quitar la mención «directorio de datos» del pin 3(b) de `C1_2_FRONTIER_SPEC.md`.

# ADR-093 — Money Polarity V2: relación determinista y fallo cerrado

**Fecha:** 2026-09-06
**Estado:** Implemented in feature branch; pendiente de integración
**Rama:** `feature/money-polarity-v2`
**Base:** `691ca352c510d3e1b4bb8ca1717ce6c171d43253`

## Problema y evidencia previa

La demo «contratos firmados por distrito de barranquilla en agosto de 2026 por
mas 200 millones» producía `valor_max=200000000` y un WHERE `<=`, pese a pedir
un mínimo. Se reprodujo sobre la base real antes de modificar runtime. El
fallback del extractor aplicaba `params.setdefault("valor_max", amount)` a
una relación desconocida. Negation binding y arbitraje NIT/dinero mantenían
catálogos monetarios adicionales. El corpus MPV2 de 105 casos se congeló en
`32db2c3`: 43 fallaban y 62 pasaban; su prueba de estructura también pasaba.

## Decisión

**INV-NO-SILENT-MONEY-POLARITY-GUESS.** Reconocer un monto no acredita su
dirección económica. Una relación desconocida produce `money_clarification`,
ningún `valor_min`/`valor_max`, `should_execute=False` y cero llamadas SECOP.
Una parte resuelta no permite ignorar otra parte monetaria desconocida.

Las autoridades conservan una sola responsabilidad:

- `money_resolution.py`: representación numérica, sin cambios.
- `app/utils/money.py`: unidades y conversión COP, sin cambios.
- `money_admission.py`: admisibilidad y atomicidad preservadas; compone la
  protección de spans con esas mismas reglas de evidencia monetaria.
- `money_polarity.py`: relación y spans de origen, sin conversión, I/O, LLM,
  DB ni estado mutable. Devuelve LOWER_BOUND, UPPER_BOUND, RANGE, APPROXIMATE
  o UNKNOWN; la decisión es inmutable.
- `QueryRouter._extract_amounts`: compone decisión, conversión y admisión,
  conserva metadata SMMLV y consume los spans monetarios completos.

La microgramática une familias flexionadas con conectores cerrados y adyacencia
al monto. Un sustantivo interpuesto rompe el enlace: «más contratos de 200
millones» no equivale a «más 200 millones». Negar una familia invierte su
dirección cuando la gramática acredita ese enlace. Los rangos simétricos
ordenan extremos; las desigualdades coordinadas mantienen sus roles y P8
rechaza un mínimo mayor al máximo. Un rango parcial pide precisión.

`negation_binding` consume `money_admission.protected_money_spans`; ya no
tiene regex monetarias propias. Este adaptador consulta exclusivamente las
decisiones de `money_polarity` y exige evidencia monetaria o admisión numérica:
«menos 2025» sigue siendo una exclusión temporal. La primera suite completa
detectó esa colisión y se añadió una propiedad para toda la familia 2000–2099. El arbitraje NIT/dinero consulta el mismo comparador,
conservando la prioridad de etiquetas explícitas de identificador. El
corrector ortográfico recibe spans protegidos para conservar también símbolos
como `>=`. Si una negación no comprendida enmascara evidencia monetaria
explícita, el router conserva una aclaración en vez de perder esa dimensión.

`money_query_gate.py` se extrajo mecánicamente antes del cambio funcional
(`ff6bb85`), respetando NEXT_TOUCH_RULE. P16 distingue:

- incertidumbre genérica: reason y risk flag `money_interpretation_uncertain`;
- vigencia SMMLV no soportada: `smmlv_year_scope_unsupported`.

El gate de mutaciones conserva `money_clarification` como evidencia bloqueante
del turno, no como filtro ejecutable. Una mutación monetaria válida elimina
una aclaración obsoleta. Las reglas existentes de intersección coherente,
reemplazo de rango y eliminación de cota opuesta incompatible no cambian.

## Compatibilidad y cambio deliberado de producto

Esta decisión complementa ADR-069 y reemplaza exclusivamente la aceptación
histórica de bare amounts como máximos, documentada en A13–A15 del closeout
ADR-059. Los valores numéricos, el catálogo SMMLV, la revalidación por año y
la proyección DatasetSpec permanecen bajo sus autoridades existentes.

«Contratos por 500 millones» conserva exactamente el rango legacy ±20 %:
400–600 millones. «Contratos de 500 millones» pide dirección. No se añaden
unidades: la notación `200M` de ejemplos conceptuales del plan no constituye
una nueva unidad de entrada. Tampoco se modifica el formato público actual
de COP con comas; se comprueba que ≥/≤ refleja el predicado autorizado.

`llm_handler.py`, prompts y `REGEX_PRIORITY_KEYS` permanecen byte-idénticos.
La demo y las aclaraciones monetarias usan `heuristic_only`. No se incorpora
fallback LLM ni un esquema persistente nuevo.

## Validación e integración

El cierre registra resultados ejecutados, baseline y delta en
[`2026-09-06_MONEY_POLARITY_V2_CLOSEOUT.md`](../audits/2026-09-06_MONEY_POLARITY_V2_CLOSEOUT.md).
Se mantienen separados los guards de integridad M1B y la paridad Truth Layer;
ninguno se repinea por este sprint. No hay merge a main ni push implícitos.

# Money Polarity V2 — cierre y evidencia

**Rama:** `feature/money-polarity-v2`

**Base:** `691ca352c510d3e1b4bb8ca1717ce6c171d43253`

**Runtime final:** `a81c39dad2b63128f6e7897980d93a9190df1b56`

**Worktree:** `/Users/rodrigoortiz/Documents/secoppal-money-polarity-v2`

## Tesis

Se elimina la inversión silenciosa unknown → `valor_max`. Una sola
microgramática determina la relación monetaria y conserva los operandos;
Money V2 convierte/admite los montos. Una relación o conversión incierta
elimina ambas cotas, pide aclaración y bloquea SECOP antes del builder.
La defensa también opera cuando una negación enmascara dinero y cuando
un follow-up intenta pasar por el gate de mutaciones.

El incidente exacto se reprodujo sobre main antes de tocar runtime:

```text
contratos firmados por distrito de barranquilla en agosto de 2026 por mas 200 millones
BEFORE: valor_max=200000000; valor_min absent; WHERE <= 200000000
AFTER:  valor_min=200000000; valor_max absent; WHERE >= 200000000
```

La rama parte de main real, con cambios versionados limpios. Los archivos
untracked del usuario se quedaron en main. No se reutilizaron los commits
en cuarentena ni se arrancó la app. No se hizo merge, rebase ni push.

## Autoridades y alcance

| Responsabilidad | Autoridad final |
|---|---|
| Representación numérica | `money_resolution.py`, intacta |
| Unidades y conversión COP | `app/utils/money.py`, intacta |
| Relación y spans | `money_polarity.py`, puro y determinista |
| Admisión y atomicidad | `money_admission.py`, reglas preservadas |
| Protección de spans | `money_admission.protected_money_spans`, desde la autoridad léxica y admisión existentes |
| Integración / metadata SMMLV / scrub | `QueryRouter._extract_amounts` |
| Bloqueo P16 | `money_query_gate.py`, reason genérico o SMMLV según procedencia |
| Follow-up | `followup_mutations.py` conserva blockers y limpia aclaraciones obsoletas |

No se tocan LLM, prompts ni `REGEX_PRIORITY_KEYS`. La demo y las
aclaraciones usan `heuristic_only`. No cambia SoQLBuilder ni presentación:
DatasetSpec proyecta COP a `valor_del_contrato` o `precio_base`, y el
formatter conserva los símbolos ≥/≤ correspondientes.

«Por 500 millones» mantiene el rango legacy 400–600 millones. «De 500
millones» deja de significar máximo y pide dirección: es el cambio explícito
de producto de ADR-093, que sustituye A13–A15 del closeout ADR-059 sin
reescribir su evidencia histórica. No se añaden unidades ni tarifas SMMLV.

El archivo adicional de producción es `money_query_gate.py`: extrae la
responsabilidad P16 antes de cambiarla. `query_plan.py` pasa de 276 a 263 LoC
según el contador vigente; el comentario histórico 299/300 estaba desactualizado,
pero NEXT_TOUCH_RULE se cumplió sin subir el tope.

El otro ajuste de integración es `followup_mutations.py`: un test demostró
que la allowlist descartaba `money_clarification` y permitía ejecutar con
una cota heredada. Ahora transporta esa evidencia bloqueante. La política
existente conserva intersecciones coherentes, reemplaza rangos completos y
elimina el extremo anterior si resulta incompatible; no se redefine.

## Corpus y propiedades

105 casos humanos congelados **antes** de runtime en `32db2c3`. Su SHA-256:

```text
2f14310634fff9ebcca35c8698c645eaf9b22cb8f8fbc5870013bc7d31b87354
```

| Categoría exclusiva | Casos |
|---|---:|
| lower | 33 |
| upper | 16 |
| negated | 21 |
| range | 10 |
| approximate | 4 |
| unknown | 12 |
| adversarial | 9 |

Por resultado semántico: LOWER 41, UPPER 29, RANGE 10, APPROXIMATE 8,
UNKNOWN 17. Las categorías negated/adversarial son transversales a estos
resultados, no se suman otra vez.

Hypothesis ejecuta ocho propiedades deterministas, con `derandomize=True`,
40 ejemplos por propiedad y sin I/O externo: dirección LOWER/UPPER,
inversión por negación, UNKNOWN sin cotas, rango atómico y escala heredada,
omisión de conectores, barrera de sustantivos, conversión parcial y frontera
entre años y dinero explícito. La omisión comparte propiedad con LOWER.

## Baseline, final y delta

| Gate | BASELINE | FINAL | DELTA |
|---|---|---|---|
| Dinero focal previo | 282 passed in 3.51s | cubierto por focal ampliado | sin expectativas legacy reescritas |
| Corpus MPV2 + estructura | 43 failed, 63 passed in 10.97s | 105/105 casos + estructura PASS | 43 casos corregidos |
| Focal ampliado + negación | — | 486 passed in 57.00s | verde |
| Recuperación tras UNKNOWN | — | 1 passed in 1.06s | verde |
| Query corpus | 89/89 | 89/89 | 0 |
| Dialogue corpus | 60/61; GOB-DEPTO-001 | 60/61; mismo GOB-DEPTO-001 | 0; reporte byte-idéntico |
| Ruff | All checks passed! | All checks passed! | 0 |
| Semgrep | 10 findings | 10 findings | 0 nuevos |
| lint-core | app/core/ limpio | app/core/ limpio | 0 |
| Suite completa | no se ejecutó completa antes del fix | 11465 passed, 11 skipped, 14 xfailed in 487.54s (0:08:07) | 0 fallos en runtime final |

Las salidas literales viven en [`money_polarity_v2/`](money_polarity_v2/).
`semgrep.json` conserva el stderr original como string, incluido su whitespace.
Ruff corre sobre `app/ tests/ scripts/`; Semgrep usa el ejecutable
`.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off`.
Los diez hallazgos basales siguen en formatter, response_policy y handlers;
sus reglas, rutas, líneas y mensajes coinciden exactamente. No se omiten ni
se corrigen fuera de lote.

El focal ampliado incluyó: `test_money_polarity_v2`, `test_money_polarity_e2e`,
`test_value_polarity`, `test_value_ranges`, `test_money_range_atomicity`,
`test_money_v2_invariants`, `test_money_smmlv`, `test_search_sh1_money_followups`,
`test_money_v2_canary`, `test_money_identifier_arbitration`,
`test_money_presentation`, `test_money_verbal_numbers`, `test_money_resolution`,
`test_money`, `test_module_size_limits` y `test_safe_negation_policies`.

En esa fase corrí los focales indicados, no la suite. Luego ejecuté la suite
completa en serie, por petición explícita de la orden MPV2. La primera ejecución
produjo **1 failed, 11462 passed, 11 skipped, 14 xfailed in 501.87s**:
`contratos de aseo menos 2025` quedó protegido como dinero. Se corrigió la
frontera lexical → admisión, se amplió Hypothesis y se repitió la suite por
esa regresión concreta. La salida inicial se conserva en `full-suite-first.txt`.

Preflight: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10 y Codegraph 0.9.6.
Codegraph MCP estuvo cargado y CLI status declaró el índice al día (795
archivos, 18.650 nodos, 41.759 aristas). Sus homónimos eligieron una copia
archivada del extractor y `affected` devolvió cero dependencias para archivos
con importadores conocidos. Se declaró fallback a `rg` y lectura directa para
router, negación, frame, mutaciones, planificación, SoQL y presentation. El
índice de main no se trató como validación de la rama editada.

## Demos finales

Los parámetros completos, reason, ruta y SoQL están en
[`evidence.json`](money_polarity_v2/evidence.json). Montos de esta tabla en COP.

| ID | Query | valor_min | valor_max | Ejecución |
|---|---|---:|---:|---|
| DEMO-001 | contratos firmados por distrito de barranquilla en agosto de 2026 por mas 200 millones | 200000000 | — | sí |
| DEMO-002 | contratos firmados por distrito de barranquilla en agosto de 2026 por más 200 millones | 200000000 | — | sí |
| DEMO-003 | contratos firmados por distrito de barranquilla en agosto de 2026 más 200 millones | 200000000 | — | sí |
| DEMO-004 | quiero más contratos de alimentación por 200 millones | 160000000 | 240000000 | sí, aproximación legacy |
| DEMO-005 | contratos de alimentación no superiores a 200 millones | — | 200000000 | sí |
| DEMO-006 | contratos de alimentación no menores a 200 millones | 200000000 | — | sí |
| DEMO-007 | contratos de alimentación por encima 200 millones | 200000000 | — | sí |
| DEMO-008 | contratos de alimentación por debajo 200 millones | — | 200000000 | sí |
| DEMO-009 | contratos de alimentación de 200 millones | — | — | no; aclaración |
| DEMO-010 | contratos por 500 millones | 400000000 | 600000000 | sí, aproximación legacy |

DEMO-001 conserva `dataset=contratos`,
`entidad_resolved=DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA`,
`fecha_desde=2026-08-01`, `fecha_hasta=2026-08-31`, sin topic espurio.
La prueba E2E usa un cliente simulado con spies de query/count/LLM; no atribuye
resultados de una llamada viva a la demo. Todos los UNKNOWN E2E hacen cero
llamadas SECOP y cero llamadas al parser LLM.

SoQL generado para DEMO-001 (solo se insertan saltos de línea):

```sql
SELECT id_contrato, referencia_del_contrato, nombre_entidad, departamento, ciudad, objeto_del_contrato, descripcion_del_proceso, tipo_de_contrato, modalidad_de_contratacion, valor_del_contrato, estado_contrato, fecha_de_firma, proveedor_adjudicado, documento_proveedor, urlproceso
WHERE unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA%'))
  AND valor_del_contrato >= 200000000
  AND fecha_de_firma >= '2026-08-01'
  AND fecha_de_firma <= '2026-08-31'
ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC
LIMIT 50
```

## Hallazgos fuera del carril y recomendaciones

- Dialogue corpus: `GOB-DEPTO-001` ya falla en baseline; no se toca resolución
  de Gobernación de Boyacá para hacer verde una puerta ajena.
- La degradación de cero filas en `workflow/actions/execution.py` puede llamar
  a `_relax_params` y retirar fechas; el helper también contempla retirar
  valor. Se observó la segunda consulta sin fecha con un cliente simulado.
  Recomiendo revisar esa política por separado: cambia el alcance pedido,
  aunque exista un hint de degradación. No se modifica en MPV2.
- `.claude/CLAUDE.md` prescribe ruff → pytest → semgrep, mientras AGENTS.md
  prescribe ruff → semgrep → pytest. Se siguió AGENTS.md.
- El contrato general de carriles prohíbe la suite completa al carril; la
  orden específica MPV2 la pide explícitamente. Se ejecutó desde el worktree
  propio, en serie y comprobando ausencia de otro pytest activo.
- No se usó el atajo de ampliar REGEX_PRIORITY_KEYS ni de pedirle polaridad
  al LLM. No se agregaron unidades `M`, UVT, divisas o reglas SMMLV nuevas.

## Commits y cierre

1. `32db2c3`: congela corpus humano antes de runtime.
2. `ff6bb85`: extrae P16 mecánicamente antes de cambiar semántica.
3. `6206605`: autoridad de polaridad, bloqueo, negación, follow-up y E2E.
4. `a81c39d`: admisión de spans protegidos, exclusiones temporales y recuperación.
5. El commit documental que contiene este cierre registra ADR-093 y evidencia.

**VERDICT: GO**

La entrega no hace merge ni push. Los guards M1B y Truth Layer, fuentes,
identidades, aliases, BuyerGroups y artefactos históricos quedan intactos.

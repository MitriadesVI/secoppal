# Entity Profile Frame V1.2-A.1 — cierre de remediación CORE

Fecha: 2026-09-07.

```yaml
BASE: 9da35b527e88b6d8612e94a5c5828f854d53a138
AUDIT: 6a6ec5c6c65019a1fae0a56a97df178851bb1d99
DESIGN_AUTHORITY: 02d65ddfdc221f74a8fac75352931dcf6673a171
RUNTIME_TEST_HEAD: d5cfa2f401a1b5efca5fc80a770a365d37a70030
PRODUCTIVE_FRAME_ACTIVATION: OFF
VERDICT: CORE_GO_FOR_V1_2_B
B_START_RECOMMENDED: YES
```

## Tesis

El Plan vuelve a ser un programa autorizado congelado: toma snapshots
transitivamente inmutables, conserva predicados, SQL, fingerprints y alcance,
y valida que cada job pertenece al SourcePlan correspondiente. Una observación
ya no puede acreditarse con etiquetas sueltas: requiere el Plan, un job real y
coincidencia exacta de plan id, dataset, fingerprint y SQL enviado.

La migración legacy compara las dos copias como semántica, no por forma
incidental. Tema AND/OR usa canon conmutativo; exclusiones, límites, modalidad,
tipo contractual, estado, evento y fuente se comparan por separado. Toda
divergencia o estado ambiguo produce `INVALID_CONTEXT`; no gana request, params
ni un superset. El estado inequívoco se conserva como `CurrentState` tipado.

## R3 y R2

La pérdida R3 quedó localizada antes de Claims, en
`QueryRouter._extract_object_terms`: `STOPWORDS` eliminaba todas las ocurrencias
de palabras de fuente. La reparación global acredita por span sólo las cabezas
estructurales, las enmascara antes del tema y conserva ocurrencias posteriores
si el prefijo residual contiene lenguaje material. Pasan las variantes con
gestión/automatización/administración de procesos, administración de contratos,
gestión contractual, BOTH, OR y nombres acreditados de comprador con esas
palabras. No se reconstruye tema desde raw text dentro del Frame.

R2 ahora decide gobierno por posición: un marcador de actualidad sólo gobierna
una ocurrencia posterior. Por eso publicación/firma + periodo + estado actual
soportado queda `READY`; terminación histórica + periodo + estado actual sigue
en aclaración y emite cero jobs.

## Evidencia de validación

- CORE/candidato, propiedades y mutaciones: `113 passed in 0.87s`.
- Counterexamples independientes: `46 passed in 0.61s`.
- Adyacentes del audit: `131 passed in 7.91s`.
- Todos los tests `test_entity_profile*.py` más el corpus R3 global:
  `771 passed in 30.93s`.
- Actor/QueryRouter relevantes: `269 passed, 8 skipped in 63.45s`.
- Mutaciones A.1 más las cuatro independientes: `20 passed in 0.43s`.
- Ruff: `All checks passed!`.
- `make lint-core`: `app/core/ limpio`.
- Semgrep: `Findings: 10 (10 blocking)`; son los diez hallazgos baseline en
  archivos no tocados. Hallazgos nuevos en archivos propios: 0.
- `git diff --check 9da35b5..HEAD`: sin salida.
- Suite global: no ejecutada, por contrato del carril.

## Instrumentación y límites

Preflight: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph CLI no
pudo ejecutarse con Node 26.8.1 por su guarda de versión; no había MCP cargado.
Se usó el fallback declarado `rg` más inspección manual de consumidores y los
tests adyacentes del audit. No se levantó la app, no hubo I/O SECOP, no se tocó
persistencia, no se implementó V1.2-B y no se hizo merge ni push.

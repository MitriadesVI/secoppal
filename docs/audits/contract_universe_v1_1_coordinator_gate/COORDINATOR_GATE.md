# Contract Universe V1.1 — coordinator gate

**CONTRACT_UNIVERSE_V1_1_COORDINATOR_NO_GO**

No se integra. El gate global demuestra una regresión introducida por el candidato.
Conforme a la instrucción del usuario, se detiene el análisis tras reproducirla;
no se modifica runtime ni se inicia una nueva auditoría adversarial.

## Alcance y preflight

- Main requerido y observado: `c52d7eda355f44278164d4cb9803a199eec83486`.
- Candidato original: `fix/contract-universe-v1-1-f2-2`, `3c323d5cad9b104f82aeb63e788bccbeae3796ed`.
- Runtime/tests previamente probado: `4a22efc330101c3511c034a53cd02d4790bb192f`.
- Geometría inicial: left/right 0/14; merge-base exacto = main.
- Worktree propio: `/Users/rodrigoortiz/Documents/secoppal-contract-universe-v1-1-coordinator-gate`.
- Rama de entrega: `codex/contract-universe-v1-1-coordinator-gate`.
- Main y candidato original no tenían cambios tracked/staged. **No estaban limpios
  de untracked**: main contenía archivos locales; candidato tenía observations.jsonl.
  Se registraron íntegramente en preflight.json y se preservaron. No se declara
  `git status` completamente limpio para esos árboles.
- AGENTS.md y CARRILES.md leídos; la orden explícita autoriza al coordinador
  a ejecutar la global y condiciona cualquier integración a todos los gates.

## Único cambio de tests

Se actualizaron exclusivamente los números adjudicados de los diez oráculos
Aprobado y su comentario. Tres archivos, sin cambios de fixture, filas, helpers
ni otras aserciones semánticas. Runtime y flags intactos.

Antes: `10 failed, 33 passed in 6.56s`.
Después: `43 passed in 4.98s`, sin shim de política.

Ver FROZEN_ORACLE_ADJUDICATION.md, oracle_integrity.json y frozen_before.json.
El dialecto SQL de SQLite se habilita con tests.contract_universe_sql_dates;
no altera reglas de elegibilidad. Los nodeids antes/después están conservados.

## Gates focales repetidos

| Bloque | Resultado literal |
|---|---|
| Auditor final | 19 passed in 3.88s |
| F2.2 + Hypothesis | 34 passed in 12.34s |
| Symmetry | 46 passed in 4.97s |
| Completeness | 248 passed in 30.82s |
| Findings anteriores | 8 passed in 1.49s |
| Auditor restantes | 390 passed in 9.53s |
| Remediation | 85 passed in 8.83s |
| Author targets | 1217 passed in 67.96s (0:01:07) |
| Frozen actualizado | 43 passed in 4.98s |
| Corpus | Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none) |

No sumar bloques que se solapan. Comandos y stdout/stderr exactos:
focal_static_literal.json. No se agregaron xfail, skip o deselect.

## Global completa, una sola corrida

Colección sin selección de archivos, según pytest.ini; sin -k, -m, --ignore,
--deselect, -x ni maxfail. Plugin de dialecto SQL, sin guard offline global
ni shim V1. PYTEST_ADDOPTS y PYTEST_PLUGINS no definidos.

```text
collected 14219 items
116 failed, 14069 passed, 19 skipped, 15 xfailed in 824.47s (0:13:44)
```

Exit code pytest: 1. Duración externa del proceso: 826.8673606249977 s.
Los 19 skipped y 15 xfailed provienen de la suite existente; todos están
listados con sus razones en global_skips_xfails.json. No equivalen a validación.

global_literal.json conserva stdout/stderr exactos, incluido whitespace;
global_stdout.log es la vista legible con espacios finales normalizados.
global_junit.xml (espacios finales normalizados; original en global_literal.json), global_failures.json y global_summary.json conservan resultados.
La ejecución terminó normalmente: no fue ENOSPC, timeout ni interrupción.

## Clasificación reproducida y condición de parada

Los siguientes tests están byte a byte idénticos entre main y candidato
(regression_test_identity.json). Se ejecutaron secuencialmente, después de la
global, con el mismo Python/pytest, sin plugins Contract Universe:

1. tests/test_actor_pending_document_selection.py::test_document_of_one_candidate_matches_that_candidate
2. tests/test_actor_pending_document_selection.py::test_year_survives_explicit_document_selection

Base aislada `c52d7ed`:

```text
2 passed in 1.57s
```

Candidato:

```text
AssertionError: assert 'NO_DOCUMENT' == 'MATCH'
KeyError: 'fecha_desde'
2 failed in 1.11s
```

El documento de un candidato pendiente ya no se reconoce como selección y
el seguimiento pierde el filtro de año 2025. Es una regresión funcional del
candidato contra la base requerida, no un fallo de infraestructura ni una
expectativa congelada de Aprobado. Los tres archivos de oráculos modificados
no participan en esta reproducción. Evidencia literal: regression_reproduction.json.

Se detiene conforme a la orden: no se atribuyen los otros 114 fallos a deuda
preexistente ni se afirma causalidad individual sin reproducirlos. Permanecen
listados. No se investiga ni se implementa una reparación o un nuevo frente.

## Estáticos e instrumentos

```text
Ruff 0.16.4: All checks passed!
Semgrep 1.176.0: Ran 3 rules on 331 files: 10 findings.
lint-core: app/core/ limpio
size: 2 passed in 0.11s
diff-check inicial: exit 0, stdout vacío
```

Semgrep contrastado en vivo contra main: 10 históricos, 10 candidato, 0 nuevos,
0 retirados, 0 errores. El exit 0 de Semgrep no significa cero hallazgos.
Detalle: semgrep_delta.json y ambos JSON de scan.

Hypothesis 6.165.10 ejecutado en F2.2, symmetry, completeness y demás bloques
con propiedades existentes; no se agregan pruebas ornamentales para cambiar
constantes. Codegraph --version y status fallaron por Node 26.8.1 no soportado;
no se afirma índice activo. Fallback declarado: rg y lectura directa de los
tests adjudicados/consumidores, más Git y comparación byte a byte/AST.

Corrí los diez bloques focales indicados y la suite global completa, una sola
vez; además dos tests idénticos sobre base y candidato para atribuir regresión.
No corrí una segunda global ni nueva auditoría de mutaciones.

## Preservación y deudas

SEMANTIC_PRESERVATION.md documenta F1/F3/F4/F5, Aprobado, Buyer–Actor focal,
procesos sin predicado, política 1.1 y rechazo de artefactos V1. Ese verde
focal no cubre la regresión de selección documental demostrada por la global.
Frame sigue OFF por defecto y app/config.py coincide con main.

DEFERRED.md conserva CONTRACT_STATE_LEXICON_GAPS_001 y todas las demás deudas
indicadas por el usuario. Ninguna fue tocada.

## Postflight e integración

Ver postflight.json e INTEGRATION.md. Main y candidato original no se mueven.
No fast-forward, merge, squash, rebase, cherry-pick, push, deploy ni restart.
Solo se entrega el commit de oráculos autorizados y evidencia en la rama propia.
La integración permanece bloqueada por regresión reproducida, no pendiente
por falta de permiso. No se emite GO_FOR_FAST_FORWARD ni INTEGRATED.

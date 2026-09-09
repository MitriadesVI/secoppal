# CONTRACT_UNIVERSE_V1_1_READY_FOR_INDEPENDENT_REVIEW

Se remediaron exclusivamente F1–F5 y la adjudicación de Aprobado. El universo
se acredita antes de ejecutar; las etiquetas se eligen antes de interpolar
contenido fuente. No se concede GO_FOR_MERGE.

**SHA probado:** `8fc5643651d6a603d3bc3849369d9773ab55ea11`.
**Base del candidato:** `067b39770592640b5aef3898cbc99d8f82c59e0f`.
**Auditoría:** `2fb9c6e2974803a8ae46b3fcc27257e200e4609d`.
**BASE main:** `c52d7eda355f44278164d4cb9803a199eec83486`.
El HEAD final añade sólo documentación/evidencia; se comunica completo en la
entrega y se obtiene mediante git rev-parse HEAD. No procede del HEAD auditor.

## Matriz antes/después

| Grupo | Antes reproducido | Después medido |
|---|---|---|
| F1: palabra del objeto/Actor | Borradores autorizaba source_records, consumía tema o contaminaba Actor | Universo celebrado, tema preservado e identidad exacta; Search/perfil y Frame OFF/ON; incluye Actor REGISTROS CANCELADOS |
| F2: dos estados | Ejecutaba sólo Cancelado o Borrador según orden | Aclaración, cero SQL, ambos órdenes; control singular positivo y mezcla celebrado/excluido negativa |
| F3: fuente publicada | Descripción cambiaba contratos por registros | Cadena fuente idéntica; etiquetas desde el plan, sin reemplazo global; Search y perfiles OFF/ON |
| F4: params materializados | Estado excluido heredado ampliaba universo | Celebrado con conflicto previo a SQL; campos de procesos/familia heredada no acreditan consentimiento; autoridad versionada positiva |
| F5: dual | Sólo ejecutaba procesos y descartaba registros | Composición de registros pide aclaración sin ningún hijo ejecutado; dual ordinario ejecuta ambos, procesos sin estado contractual |

**F5 tiene un límite deliberado:** no se implementó una gramática dual nueva.
La forma explícita de registros no tiene un segundo hijo acreditado por el
adaptador vigente en ambos caminos. Se adopta la alternativa de aclaración que
la orden permite. Las pruebas verifican cero SQL y los dos órdenes de fuentes.
También se adopta aclaración para dos estados excluidos, nunca first-match-wins.

La revisión final añadió cuatro contrapruebas OFF/ON del mismo F1:
`contratos de borradores de documentos...` y `todos los contratos de elaboración
de borradores de documentos...`. Antes ampliaban o pedían aclaración indebida;
ahora la fuente inicial conserva su complemento y el default celebrado no
autoriza extraer un estado excluido del objeto. Se conservan
head_ownership_red.txt (4 failed) y head_ownership_green.txt (116 passed focales).

## Aprobado y los tests congelados

Aprobado con fecha utilizable entra. Sin fecha utilizable es indeterminado y
no entra; no se infiere firma por estado, fecha ni año. Los otros siete estados
posteriores conservan su política, y una fecha no rescata estados excluidos.
[POLICY_DELTA.md](POLICY_DELTA.md) define versión, SQL y compatibilidad.

La reproducción inicial dio **12 failed, 29 passed**, sin los dos controles de
tamaño. El lote final completo original da **10 failed, 33 passed**. No se
presenta como 43/43 verde ni se ocultan los rojos. Los diez fallos corresponden
exclusivamente a expectativas numéricas de V1 que admitían los tres Aprobado:

| Expectativa congelada | Adjudicación V1.1 | Port separado |
|---|---|---|
| Matriz Search/count/sum: 24, cuatro combinaciones semilla/flag | 22; sólo Aprobado fechado | test_contract_universe_remediation.py, matrix |
| Actor: 26, OFF/ON | 24, conserva los dos controles sin filtro monetario | test_contract_universe_remediation_consumers.py, actor |
| Lookup seguido de nueva Search: 24 | 22 | remediation.py, direct_record |
| Radar: 24 → 23 | 22 → 21, mismo evento de cancelación | remediation.py, radar |
| Dual materializado celebrado: 24 | 22; registros sigue en 94 | remediation_consumers.py, materialized_dual |
| Búsqueda nueva tras rechazar conversación BASE: 24 | 22; rechazo y prefijo histórico intactos | remediation_legacy.py, main_base_conversation_adjudicated |

Los dos tests originales del Actor ya pasan las aserciones de identidad y
universo; fallan sólo al comparar el antiguo conteo 26 con 24. Los ports
adjudicados pasan completos. No se editó ninguna de las seis fuentes Python
independientes copiadas: [hashes](independent_preservation.json).
No se usó xfail, skip ni deselect para evitar fallos.

Tests originales que conservan esos rojos:

- `tests/test_contract_universe_independent.py::test_independent_sql_matrix[False-seed0]`
- `tests/test_contract_universe_independent.py::test_independent_sql_matrix[False-seed1]`
- `tests/test_contract_universe_independent.py::test_independent_sql_matrix[True-seed0]`
- `tests/test_contract_universe_independent.py::test_independent_sql_matrix[True-seed1]`
- `tests/test_contract_universe_independent.py::test_direct_record_does_not_leak_to_search`
- `tests/test_contract_universe_independent.py::test_radar_cancellation_event_and_digest`
- `tests/test_contract_universe_independent_consumers.py::test_actor_name_state_word_has_no_universe_authority[False]`
- `tests/test_contract_universe_independent_consumers.py::test_actor_name_state_word_has_no_universe_authority[True]`
- `tests/test_contract_universe_independent_consumers.py::test_materialized_dual_children_and_source_names[celebrated-24]`
- `tests/test_contract_universe_independent_legacy.py::test_real_base_conversation_requires_review`

## Verificación literal

Corrí 85 tests de remediación y 1.217 targets dirigidos del autor/consumidores,
no la suite. Además corrí los 43 controles independientes originales. No sumo
los ejemplos Hypothesis ni tests repetidos como una nueva aceptación.

```text
ruff: exit 0
All checks passed!
semgrep: exit 0
Ran 3 rules on 330 files: 10 findings.
remediation: exit 0
85 passed in 8.82s
independent: exit 1
10 failed, 33 passed in 5.27s
author: exit 0
1217 passed in 72.20s (0:01:12)
corpus: exit 0
Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
lint-core: exit 0
app/core/ limpio
diff-check: exit 0
(sin salida)
```

Los comandos completos y stdout/stderr están en [validation.json](validation.json).
Semgrep es baseline rojo: **10 existentes, 0 nuevos, 0 resueltos**, comparados
por regla, ruta y fragmento con multiplicidad, no sólo por conteo.
[semgrep_delta.json](semgrep_delta.json). No se hicieron repins.

Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10 y Codegraph CLI 0.9.6 se
comprobaron. Codegraph rechazó Node 26; funcionó con el Node 22 ya instalado,
sin instalar ni cambiar el entorno del usuario. Se indexó el worktree y se
usaron status/query/callers/affected, complementados por rg y lectura directa.
No se presenta el índice como un MCP activo. Hypothesis ejecutó invariantes de
palabras dentro de nombres/objetos, permutación de estados, paridad SQL/política
y conservación de métricas ante filas excluidas. [preflight.json](preflight.json).

Los dobles SQL históricos desconocían date_extract_y. Un plugin optativo de
emulación temporal permite ejecutar los tests congelados sin editar sus filas,
helpers ni oráculos. El ledger offline permanece activo; los canarios de red
absorbida siguen detectados. No hay SECOP live ni LLM en la aceptación.
Los errores de emulación de las primeras iteraciones se conservan como
diagnóstico y no se cuentan como fallos funcionales ni kills.

## Mutaciones y persistencia

Seis mutaciones nuevas detectadas con baseline verde: match léxico global,
fallback params, reemplazo global del render (OFF/ON), primer estado, veto dual
por procesos y admisión SQL de Aprobado sin fecha. Todas fallan por aserciones
de producto, sin import errors ni timeouts. [mutations.json](mutations.json).
Las cuatro previas también se repitieron y detectaron: count, Frame, post-LIMIT
y recarga. [prior_mutations.json](prior_mutations.json).

El replay usa artefactos emitidos por el candidato V1 exacto, además de los
artefactos independientes de main. V1.1 bloquea continuación/export y conserva
conversación, buffer, Frame/plan/registro y Radar; no hay migración silenciosa.
El Frame sigue en schema 2 y el buffer conserva su formato: cambia la versión
semántica del universo y del catálogo de planes, no los tipos de sus campos.

## Runtime modificado

- `app/core/contract_universe.py`
- `app/core/contract_universe_intent.py`
- `app/core/entity_profile/claim_adapters.py`
- `app/core/entity_profile/frame_compiler.py`
- `app/core/entity_profile/frame_response.py`
- `app/core/entity_profile/render.py`
- `app/core/entity_profile/universe_claims.py`
- `app/core/query_router.py`
- `app/core/truth_layer/contract_signature.py`

[Huellas del runtime](runtime_fingerprints.json). QueryPlan, identidades,
aliases, BuyerGroups, territorios y archivos compartidos permanecen intactos.
La expectativa autoral de firma de Aprobado sin fecha fue sustituida por su
prueba de indeterminación; los tests independientes no se modificaron.

## Fuera del carril y pendiente

No se corrigieron los diez hallazgos históricos Semgrep. No se abrieron otros
frentes ni se modificaron montos, deduplicación, ranking, identidad o flags.
Los conteos son de fixtures controladas y no una estimación de SECOP real.
La emulación SQLite no sustituye certificación nacional del dialecto ni una
adjudicación jurídica de expedientes. No se inventaron umbrales de centinela.

Pendiente: nueva revisión independiente y cualquier gate/integración del
coordinador. No hubo suite global, merge, push, despliegue, activación ni
reinicio. La alternativa de aclaración en F2/F5 es un límite explícito de esta
entrega, no una ejecución parcial oculta.

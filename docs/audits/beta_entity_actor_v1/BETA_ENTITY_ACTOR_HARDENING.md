# BETA_ENTITY_ACTOR_HARDENING

Fecha: 2026-09-06. Implementación en worktree propio. Sin merge ni push.

## Tesis y alcance

El cero institucional de Galapa provenía de heredar el actor del turno anterior:
no era ausencia de contratos ni un defecto de su identidad. Una solicitud
explícita de análisis institucional ahora inicia una búsqueda nueva; la
construcción del perfil rechaza una divergencia entre scope y parámetros.
Además, una búsqueda fuerte de actor puede llegar al resolver y las preguntas
contextuales de proveedores conservan el comprador con ambos rankings.

No se cambian EntityResolver, aliases, bindings, BuyerGroups, NIT, QueryRouter,
QueryPlan, presupuestos, transporte, Offers, Competition, PAA, Radar ni graph DB.
No se crea ADR: se restaura el contrato institucional/conversacional existente.

## Base y entrega

- MAIN_BASE: `c546eb6bb69f69be996840a29edb7afb2349e249`.
- BRANCH: `fix/beta-entity-actor-conversation-v1`.
- WORKTREE: `/Users/rodrigoortiz/Documents/secoppal-beta-entity-actor-conversation-v1`.
- HEAD de runtime verificado: `a39186410a51ed25858cb19c7254b170d7cf34e5`.
- Evidencia: [`artifacts/audits/beta_entity_actor_v1/`](../../../artifacts/audits/beta_entity_actor_v1/).

## GALAPA_FALSE_ZERO

**ROOT_CAUSE:** trace productivo `1c758e0b5d88`, 2026-09-06 19:20:12 UTC:
`followup=true`, `intent=change_scope`, `dataset=contratos`, actor seleccionado
anterior todavía en `contratista` y `actor_scope`. El modelo del perfil sólo
representaba BuyerUnit, periodo, modalidad y tema; los parámetros ejecutables
retenían además un Actor. El builder de procesos ignora contratista porque esa
fuente carece del campo; el builder de contratos sí lo aplica. Dos universos
incompatibles se presentaban bajo el mismo título institucional.

**BEFORE:** reproducción live del frame real: 4.418 procesos y «Se observan 0
contratos por $0». Las seis SourceCalls terminaron correctamente. La consulta
contratos con el actor heredado confirma `count=0` y `LIMIT 3=[]`: el cero es
cierto para la intersección accidental, pero falso como conclusión institucional.

**AFTER:** la misma solicitud sobre el mismo historial se clasifica
`new_search`, descarta actor/filtros anteriores y conserva el comprador
`MUNICIPIO DE GALAPA.`. En el live posterior, cinco llamadas terminaron y el
agregado total tuvo timeout: se mostraron actividad de procesos, proveedores y
contratos recientes; el total se declaró no disponible. No se afirmó cero.

**SOURCE_PROBE:**

| Predicado | count | LIMIT 3 |
|---|---:|---|
| Frame real: comprador + actor heredado | 0 | vacío |
| Comprador canonical/fallback, sin actor | timeout | 3 contratos de Galapa |
| Nombre físico exacto `MUNICIPIO DE GALAPA.` | 4.273 | 3 contratos de Galapa |
| Binding proyectado | no existe | no aplica |

El conteo histórico del audit (4.193 contratos) no se usa como cifra actual ni
como dependencia runtime. No hay evidencia para atribuir este bug a
`entidad_like`, puntuación, source binding o NIT.

Capturas completas con `call_id`, dataset, SoQL, status, rows y latencia:
`pre_live_galapa_context.json`, `pre_live_galapa.json`, `post_live.json`.

**Defensas:** un perfil con Actor o predicados alternativos de comprador falla
antes de red; se comprueba que el scope coincide con los parámetros. Un
agregado debe contener un conteo entero explícito y valores válidos; `[]`,
campos ausentes o inválidos no se convierten en cero. Si un agregado declara
cero y otra llamada exitosa de contratos devuelve filas, actividad/valores
contractuales pasan a no disponibles y se informa la contradicción. Los
estados originales de transporte se preservan en SourceCalls.

**Prueba causal:** `test_entity_profile_galapa_does_not_report_false_zero`
ejecuta nombre ambiguo → selección 2 → análisis Galapa mediante el parser,
el grafo Burr y ConversationStore reales. Un cliente SQL evalúa el WHERE
emitido sobre una fuente sintética con contratos de Galapa y contratos de otro
comprador para el actor previo; no devuelve respuestas prefijadas por ruta.
La prueba falla en la base mostrando exactamente el cero erróneo.

## ACTOR_NEW_SEARCH

**ROOT_CAUSE:** `apply_context` reconoce `SET_ACTOR`, usa `intent=set_actor` y
limpia el frame anterior, pero `_finish_apply_context` requiere scope ya
resuelto antes de dejar llegar a `resolve_actor_name`. La hipótesis de la orden
se confirma con ese matiz: la rama sistémica SET_ACTOR precede al clasificador
genérico, cuyo resultado aislado sí era `new_search`.

**BEFORE:** reset → CODETEC → Rodrigo Ortiz acaba en «perdí el contexto»,
con `resolved_params={}` y sin llegar al índice.

**AFTER:** sólo una operación nueva SET_ACTOR con construcción fuerte, nombre
no vacío, rol contractor y método pendiente coherente puede cruzar esa
frontera. `needs_llm=false`; resolver y scope policy mantienen la autoridad de
ejecución. AMBIGUOUS/CANDIDATES piden selección; UNRESOLVED pide documento;
INDEX_UNAVAILABLE explica indisponibilidad; RESOLVED continúa. El guard global
y las reglas de SoQL no cambian. Las pruebas verifican cero llamadas SECOP
para todas las resoluciones no autoritativas y para búsqueda sin scope.

## ENTITY_FOLLOWUP

**ROOT_CAUSE:** normal_flow sí persiste y recupera `resolved_params`, incluyendo
buyer y `_entity_profile_active`; QueryFrame tampoco los pierde al convertir.
La pérdida ocurre en `detect_and_merge`: «cuales son sus mayores proveedores»
se parsea como dataset explícito contratos sin topic/scope. Si el perfil previo
está en procesos se clasifica `change_dataset` y funciona; si ya está en
contratos cae en `new_search` y descarta el comprador. Otras variantes dejan
palabras de pregunta como topic o activan el ranking genérico de entidades.

**AFTER:** una gramática acotada de preguntas completas, dentro de un perfil
activo con comprador acreditado, usa el merge `refine_filter` existente. Así
conserva comprador, periodo y tema, rechaza residuos interrogativos como topic
y llega al foco suppliers antes del ranking genérico que interpretaba otra
pregunta. La suma/promedio explícitos conservan su precedencia analítica.

Se persiste también `_entity_profile_focus`. «Y por valor» después del foco
suppliers mantiene ambos rankings; no cambia a un listado tabular. El marcador
se retira junto al perfil cuando se abandona esa ruta. Cada pregunta suppliers
ejecuta exactamente dos llamadas bounded, una por número y otra por valor.

El corpus completo queda en 41 turnos simples y 20 conversaciones. Se conservan
todos los casos originales y se añaden Galapa, las cuatro variantes pedidas,
repetición de pregunta y «y por valor». Las pruebas focales cruzan explícitamente
las variantes con perfiles históricos en procesos y contratos.

## CODETEC / CAMILA_ARAUJO / RODRIGO_ORTIZ

Resultados del resolver directo contra el índice productivo y del workflow
posterior, concordantes:

| Nombre | STATUS | METHOD | CANDIDATES |
|---|---|---|---:|
| CODETEC | UNRESOLVED | no_match | 0 |
| CAMILA_ARAUJO | CANDIDATES | fts5_trigram_bm25 | 10 |
| RODRIGO_ORTIZ | AMBIGUOUS | exact_ambiguous_alias | 3 |

Los candidatos de Camila y Rodrigo sí se presentan; no se promueve ninguno
sin selección. CODETEC es un límite de recall del índice actual, no evidencia
de inexistencia del proveedor. No se inventaron candidatos ni se cambió el
resolver. Feedback ahora persiste explícitamente `actor_resolution_status`,
`actor_resolution_method` y `candidates_count`, además del frame existente.

## Verificación

Instrumentación: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10 y Codegraph
0.9.6. Codegraph MCP se consultó sobre la base verificada; después se creó un
índice independiente en este worktree y se usó el CLI para status/impact/affected.
`affected` devuelve cero dependientes/tests: no se interpreta como ausencia de
impacto. Los importadores y suites de conversación se verificaron manualmente.

Hypothesis comprueba pérdida completa del historial ajeno en análisis nuevo,
conservación de buyer/periodo/tema en preguntas equivalentes, separación entre
admisión al resolver y autoridad de ejecución, y ausencia de hechos provenientes
de llamadas fallidas incluso con filas residuales.

Salidas literales de las puertas, en el orden exigido:

```text
$ .venv/bin/ruff check app/ tests/ scripts/
All checks passed!

$ .venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
 • Findings: 10 (10 blocking)

$ .venv/bin/python -m pytest <9 archivos focales enumerados en manifest.json> -q
247 passed in 26.71s
```

Semgrep conserva exactamente los mismos sitios de excepción que la base:
`semgrep_delta.json` acredita delta cero, incluidos los archivos propios.
La salida literal con espacios terminales se preserva en
`raw_tool_stdout.json` y sus SHA-256. Las vistas `.log` sólo recortan espacios
terminales/fin de archivo para pasar `git diff --check`; su baseline sigue rojo.

En esa puerta corrí los nueve archivos focales, no la suite. Después se lanzó
la suite completa en serie y en background, por autorización explícita de esta
orden y tras comprobar que no había otro pytest activo. Su resultado final
se registra en `full_suite.log` y `full_suite_meta.json`:

```text
$ .venv/bin/python -m pytest -q
11509 passed, 11 skipped, 14 xfailed in 480.09s (0:08:00)
```

FULL_SUITE: PASS (con las omisiones y xfails informados; no se añadieron
marcas de skip/xfail en este sprint). Runtime congelado en el SHA indicado;
tras esta corrida sólo se completó documentación y evidencia.

- QUERY CORPUS: `Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)`.
- DIALOGUE CORPUS: `GOB-DEPTO-001` falla en `entidad_resolved`; salida idéntica
  byte por byte en `c546eb6`, ejecutada en otro worktree propio aislado.
- ENTITY CORPUS: completo dentro de la puerta focal: 41 simples + 20 conversaciones.
- `make lint-core`: `app/core/ limpio`.
- Reproducciones PRE: 14 failed / 4 passed de conversación, 13 failed / 3 passed
  de evidencia. No se añadieron xfails ni se eliminaron casos históricos.

## Commits y archivos

- `a201365ddcc52666e64ecea2dde594224e726377` — test(beta): freeze entity and actor dogfood regressions
- `01bf7699d40eac8befa9454eef1d16f9bfa22a3b` — fix(entity-profile): prevent false zero for buyer contract activity
- `639c8d999696e01b2b976144acb701a403506608` — fix(actor): let strong new actor search reach resolution after context
- `c5b2fbcc0d869f5ce5bd6277a6c82911812536dc` — fix(entity-profile): preserve buyer scope across supplier followups
- `a39186410a51ed25858cb19c7254b170d7cf34e5` — fix(actor): record resolution status and candidate count in traces

Los archivos de runtime y tests modificados, con nombres completos, están en
`manifest.json`; las capturas y documentos son evidencia de este sprint.

## Fuera del carril, recomendaciones y pendientes

1. `GOB-DEPTO-001` del dialogue corpus ya está rojo en la base. Se conserva para
   un lote de autoridad/gazetteer/corpus; no se cambia su expectativa aquí.
2. Los diez findings Semgrep son preexistentes; no se corrige el backlog global.
3. El agregado institucional de contratos sin actor puede exceder ocho segundos.
   La igualdad física directa respondió, mientras el fallback LIKE tuvo timeout.
   Recomiendo una auditoría acotada de rendimiento/proyección por fuente; estos
   probes no autorizan crear un binding, aumentar presupuestos ni usar NIT.
4. Los rankings live muestran montos de decenas de billones. Merecen una
   auditoría independiente de calidad de los valores originales y sus formatos;
   aquí se conservan los valores reportados sin afirmar que sean valores
   económicos depurados ni corregirlos por intuición.
5. CODETEC continúa sin candidatos en el índice actual: mejora de cobertura
   separada del bug conversacional. Las pruebas ordinarias no dependen del
   índice productivo; B4 se contrastó directamente en modo read-only.
6. `.claude/CLAUDE.md` tiene un orden de puertas distinto y menciona tooling aún
   pendiente. Se aplicó `AGENTS.md`. CARRILES reserva suite al orquestador; la
   orden nueva la solicita expresamente, por eso se ejecutó una sola en serie.

VERDICT: **PARTIAL**. B1, B2 y B3 corregidos y verificados; B4 diagnosticado
y observable. Suite completa y query corpus pasan, delta Semgrep cero.
El dialogue corpus conserva su fallo probado en la base y el total
contractual live sigue sujeto al timeout reportado. No se declara verde
global ni se realiza merge/push.

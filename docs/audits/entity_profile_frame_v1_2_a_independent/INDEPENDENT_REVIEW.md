# ENTITY_PROFILE_FRAME_V1_2_A_INDEPENDENT_REVIEW

Fecha: 2026-09-07. Auditoría del candidato congelado; cero cambios en runtime,
cero merge, cero push. La auditoría no reevalúa dispatcher, Burr, paginación ni
ConversationStore. Los SQL observados son **planificados**, no ejecutados en SECOP.

```yaml
ENTITY_PROFILE_FRAME_V1_2_A_INDEPENDENT_REVIEW:
  CANDIDATE: 9da35b527e88b6d8612e94a5c5828f854d53a138
  BASE: 62ec330a04f054cbd8ff6028bda3a67d08ce3a28
  DESIGN_AUTHORITY: 02d65ddfdc221f74a8fac75352931dcf6673a171
  AUDIT_REFERENCE: e024753a7a3e26a80aa0c07359078544f45f4ee5
  PRODUCTIVE_ISOLATION: PASS
  AUTHORITY_PURITY: PARTIAL_SQL_FROM_FRAME_PASS_CHAIN_INCOMPLETE
  R1: PASS
  R2: UNSUPPORTED_BLOCKS_PASS_SUPPORTED_EVENT_PLUS_STATE_FAIL
  R3:
    SOURCE_PRESERVED: YES
    FULL_TOPIC_PRESERVED: NO
    LOSS_LAYER: QueryRouter._extract_object_terms_STOPWORDS
    GLOBAL_PARSER_FIX_REQUIRED: YES_IN_EXISTING_TOPIC_AUTHORITY_BEFORE_B
  R4:
    AND_OR: PASS
    NEGATIVE_MODALITY: PASS_OWN_COLUMN_AND_VISIBLE_EXCLUSION
  FRAME_PATCH: PASS_REQUESTED_VALID_FRAME_CASES
  MIGRATION: FAIL_MATERIAL_EQUIVALENCE_BEYOND_PERIOD
  SQL_COMPILER: PASS_NO_LEGACY_WHERE_REENTRY
  EVIDENCE: NUMERIC_PASS_BINDING_AND_CONSUMPTION_INCOMPLETE
  UTILITY: REQUIRED_BASIC_CORPUS_PASS_SUPPORTED_EVENT_STATE_FAIL
  LEGACY_AUTHORITY_LEAK: NO_POST_FRAME_PARAMS_REENTRY_OBSERVED
  NEW_P0: 0
  NEW_P1: 4
  NEW_P2: 1
  EXISTING_P1_STILL_OPEN: R3_FULL_TOPIC
  VERDICT: REMEDIATION_REQUIRED
  B_START_RECOMMENDED: NO
```

**Tesis:** el límite Frame → SQL sí elimina la segunda entrada conversacional,
pero eso no acredita una cadena semántica completa. Se puede confirmar un tema
incompleto, migrar dos significados distintos eligiendo uno silenciosamente,
perder un estado suministrado al adaptador y desalinear un plan con su alcance.
La evidencia nueva conserva etiquetas de procedencia, pero todavía no verifica
su correspondencia con un plan ni alimenta un renderer de métricas.

## 1. Preflight y alcance de escritura

- Rama creada: `audit/entity-profile-frame-v1-2-a-independent`.
- Worktree propio: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-a-independent`.
- Candidato inicial y worktree de autor: tracked clean. Main y demás worktrees
  no se editaron; se preservaron sus archivos no rastreados.
- `62ec330` es la **base ancestro**, no el padre inmediato de `9da35b5`.
  El padre inmediato es `216174bcc621e5ce8c1d4eb9e406a1bc185f99b2`.
- Son **11 commits**, incluido `f5a68b2`, cherry-pick documental del diseño.
  El blob de diseño coincide exactamente con el de `02d65dd`.
- Diff de `app/`: **13 adiciones, cero modificaciones y cero eliminaciones**.
  Inventario AST de imports más búsquedas manuales: ningún consumidor productivo
  externo a esos 13 módulos. Los consumidores externos observados son tests.
- **`data/` NO está vacío:** contiene 20 JSONL rastreados, heredados de la base.
  El diff `62ec330..9da35b5 -- data/` está vacío. No se limpiaron ni se inspeccionaron
  textos conversacionales; se verificaron rutas y hashes. Esta discrepancia
  de preflight no se cuenta como regresión runtime del candidato.
- Se leyeron `CARRILES.md` y la autoridad de diseño. La escritura de este carril
  se limita a dos archivos nuevos de tests y este directorio de auditoría.
  No se modifican autoridades compartidas ni se asigna ADR.

Evidencia: [preflight-and-consumers.json](preflight-and-consumers.json),
[commit-by-commit.txt](commit-by-commit.txt).

## 2. Hallazgos que bloquean el pase

### F1 — P1 existente: R3 conserva la fuente, pero pierde contenido del tema

**Dónde:** `app/core/query_router.py:1831`,
`app/data/stopwords.py:60` y `:62`; adaptación en
`app/core/entity_profile/claim_filters.py:43`.

| Consulta real | Fuente | Tema observado en Frame/visible | Pérdida |
|---|---|---|---|
| contratos de software y gestion de procesos industriales | CONTRACTS | software y gestion y industriales | procesos |
| contratos de software, gestion de procesos industriales | CONTRACTS | software y gestion y industriales | procesos |
| procesos de software y administracion de contratos | PROCESSES | software y administracion | contratos |

Las tres son `READY`. El objeto que entra al extractor todavía contiene las
palabras completas; su salida ya carece de ellas. Esas mismas ausencias aparecen
en parser params, claim TOPIC, TopicNode, predicado SQL y línea de tema visible.
El adaptador elimina además ruido de petición como `analiza`, correctamente;
no es ahí donde se pierden `procesos` y `contratos`.

La traza [router-loss-trace.json](router-loss-trace.json) observa el extractor
real sin cambiar su lógica. [R3-A.json](R3-A.json), [R3-B.json](R3-B.json) y
[R3-C.json](R3-C.json) conservan las cinco capas. Los tres tests independientes
fallan por la invariante temática, después de comprobar fuente y READY.

**Adjudicación:** es un defecto de la autoridad compartida de tema, previo al
Core. Se requiere corregir esa autoridad antes de B. No basta quitar dos entradas
globales de STOPWORDS: eso volvería a convertir las cabezas de fuente en filtros
de objeto. Una remediación compatible consiste en que el parser existente
consuma únicamente las ocurrencias acreditadas como fuente y preserve las
ocurrencias temáticas. Los offsets/ownership deben participar antes del descarte
y viajar con la salida de esa misma autoridad. Hoy el claim TOPIC tiene el span
del turno completo, y no posee las ocurrencias descartadas.

Usar ownership puede evitar un segundo parser, pero **no existe hoy un arreglo
puramente posterior al Frame** que recupere esa información. No se aprueba
reconstruir o añadir temas arbitrariamente desde raw text en composition/Core.
Esta auditoría no implementa la reparación global.

### F2 — P1 nuevo: la migración sólo acredita equivalencia temporal

**Dónde:** `app/core/entity_profile/frame_migration.py:112` y `:245`.

Con la misma fecha acreditada en ambas copias, estos contextos migran y compilan
como `READY`:

| Request legacy | Params materiales | Resultado |
|---|---|---|
| topic software | objeto mantenimiento | conserva software |
| topic plano software, vias | objeto OR anidado software/vias | convierte en ALL |
| value_min 100 | valor_min 200 | conserva 100 |
| sin exclusión | modalidad_exclude_procedure_ids de contratación directa | elimina exclusión |

`_period` compara request y params; las otras dimensiones seleccionan una copia
sin demostrar equivalencia. `_topic` reconoce explícitamente que se perdió el
agrupamiento y aun así lo migra como conjunción. Esto contradice la tabla de
migración del diseño: equivalencia y completitud de **todas** las dimensiones.

Son cuatro fallos independientes del test de migración; los artefactos
`migration-*.json` guardan los payloads, Frame, estado de compilación y SQL.
Remediación: acreditar todas las dimensiones o invalidar con razón explícita;
un periodo coincidente no autoriza el resto del significado.

### F3 — P1 nuevo: el port de estado legacy se perdió en la entrada de claims

**Dónde:** `app/core/entity_profile/claim_adapters.py:159` y `:385`;
`app/core/entity_profile/claim_filters.py:106`.

Se repite el caso `state-legacy` de `e024753`: `analiza contratos de alcaldia de
baranoa`, con `estado=["Suspendido"]` y `estado_field="estado_contrato"`.
El adaptador acepta el turno y produce `READY`; Frame, WHERE y alcance carecen
del filtro. Véase [legacy-state-port.json](legacy-state-port.json).

Esto es un caso deliberadamente inyectado del audit original, **no** una prueba
de parser real como R3/R4. `_state_claims` sólo observa texto y el emisor de
materiales no acredita ese campo. Hay un test de migración de estado legacy,
pero prueba otra frontera: no cubre la entrada material que se declaró portada.

No se solicita reabrir params después del Frame. El adaptador de admisión debe
acreditar el estado inequívoco como claim tipado, o bloquear esa entrada y exigir
la vía de migración apropiada. Aceptarla descartando el filtro amplía el universo.

### F4 — P1 nuevo: ProfilePlan es mutable por dentro

**Dónde:** `app/core/entity_profile/frame_compiler.py:205` y `:300`;
`app/core/entity_profile/frame_render.py:82`.

El Frame protege su mapping con `MappingProxyType`, pero `ProfilePlan.source_plans`
conserva un dict mutable pese a `frozen=True`. Asignar el source plan de agosto al
plan de julio cambia la línea visible de julio a agosto, manteniendo el mismo
`plan_id`, `frame_ref` y todos los jobs/SQL de julio.

[mutable-plan.json](mutable-plan.json) registra antes/después y SQL. Es una
mutación del objeto público compilado, sin monkeypatch del runtime ni nueva
compilación. La invariante que el renderer dice garantizar estructuralmente no
se sostiene. Se requiere congelar transitivamente el plan y mantener sus
predicados, jobs y referencias coherentes.

### F5 — P1 nuevo: evidencia etiquetada sin verificación ni consumidor Plan + Evidence

**Dónde:** `app/core/entity_profile/frame_evidence.py:94`;
`app/core/entity_profile/frame_render.py:74` y `:105`.

`bind_observation` recibe strings sueltos, no un plan/job acreditado. Cambiar
job_id, fingerprint, dataset_id o SQL a un valor ajeno al plan sigue dando una
métrica `contract_count=4` con estado VALID. No hay función nueva que verifique
esa correspondencia: sus únicos consumidores son tests. El test del autor usa
`p1/j1/fp` arbitrarios y comprueba que los mismos strings vuelven a salir.

Esto demuestra **ausencia de validación en el contrato disponible**, no una
llamada SECOP mal atribuida en producción. La validez numérica del 4 sí está
acreditada; la pertenencia a la ejecución indicada no lo está. Para B hace falta
un binding desde un plan y su job, o una verificación obligatoria equivalente
antes de consumir observaciones. Los metadatos discordantes deben bloquear la
acreditación para ese plan.

También falta el consumidor nuevo Plan + Evidence pedido por el diseño:
`build_compiled_scope(plan)` y `render_compiled_scope(scope)` sólo producen
alcance planificado; no reciben ni presentan métricas/estado de ObservedCall.
Ni siquiera se transporta un buyer legible en el plan: `CompiledScope.buyer`
recibe el WHERE completo y no se renderiza. No se declara completada esta arista
por los verdes aislados de `accredit_metric`. No se reaudita el renderer legacy.

### F6 — P2 nuevo: actualidad de una cláusula elimina eventos soportados

**Dónde:** `app/core/entity_profile/claim_adapters.py:278` y `:286`.

- `procesos publicados en julio actualmente abiertos`;
- `contratos firmados en agosto actualmente terminados`.

Ambas dan `AMBIGUOUS_PERIOD_ANCHOR`, cero jobs, en vez de conservar evento/periodo
y estado actual por separado. Un marcador de actualidad en cualquier posición
de la cláusula hace que `_event_claims` descarte todas sus ocurrencias de
publicación/firma. La guarda posterior detecta el periodo huérfano creado por
ese descarte. Son entradas soportadas explícitas del encargo, no ambigüedades
que deban resolverse cambiando el evento.

La contención es correcta (no se consulta otra fecha), de ahí P2 y no una
acusación de SQL incorrecto. El ownership debe ser por ocurrencia; pasar los
cuatro ejemplos históricos bloqueados no acredita esta utilidad.

## 3. Qué sí pasa y qué no permite concluir

| Puerta | Resultado observado |
|---|---|
| R1 asociación incompatible | Contratos/publicación y procesos/firma permanecen asociados al sujeto expresado; aclaración y cero jobs. |
| R1 ventanas independientes | READY en ambos órdenes; `fecha_de_publicacion_del >= '2026-07-01'` para procesos y `fecha_de_firma >= '2026-08-01'` para contratos. |
| R2 historia no soportada | Las cuatro formas requeridas (pero, que están, aunque y hoy) bloquean, cero jobs. |
| R4 tema real | ANY y ALL, fingerprints, WHERE y render distintos. |
| R4 modalidad real | `modalidad_de_contratacion NOT IN` en su propia columna y exclusión visible. |
| Frame/Patch | Roundtrip exacto de payload, versiones desconocidas probadas inválidas, KEEP, SET, CLEAR e invalidación de SET contradictorios. |
| CLEAR periodo | El Frame resultante queda sin periodo; mutar los antiguos params no lo restaura. Compila incluso con `_build_where` legacy reemplazado por una excepción. |
| Migración temporal | Request agosto + params sin fecha o julio: INVALID_CONTEXT. `unsupported_event` singular/plural conserva bloqueo; spans malformados probados invalidan sin crash. |
| Numérico | NaN, +Infinity y -Infinity no acreditan monto; count 4 se conserva. 1e-999 y 4.0000000000000001 no se truncan; 9007199254740993 conserva entero exacto. |
| Planificación | Se usa `planned_sources`; no se presenta como fuente ejecutada en esta auditoría. No hay ejecución de red. |
| Utilidad básica | Overview comprador, contratos, procesos, BOTH default, dos meses, AND, OR, exclusión soportada y proveedores sobre contratos: READY. |

El SQL usa Frame y el builder/capability catalog de su API. Los emisores privados
reutilizan proyección de comprador, variantes, frases, estados y modalidades;
no pasan por decisiones conversacionales, democión de AND a OR ni defaults de
oportunidades. Los aspectos reciben `{}` y un WHERE ya construido.

Hay catálogos compartidos consultados por funciones (`get_groups`, frases,
proyección de compradores y modalidades), no todos inyectados como argumentos.
Se distinguieron de params legacy: son autoridad de proyección, no evidencia de
una segunda interpretación conversacional. La paridad actual de cuatro tests
pasa; no equivale a una prueba de evolución de versiones de catálogo.

No se encontró lectura post-Frame de raw_query, raw_params, resolved_params,
SemanticRequest ni del scope temporal/topic legacy. La migración los lee **antes**
del Frame; su discrepancia es F2. La divergencia posterior demostrada es F4,
por mutabilidad del Plan, no por params legacy ocultos.

## 4. Calidad de tests y los 28 CORE del audit de referencia

Se revisaron los 11 commits individualmente, incluidos cambios posteriores a
los tests congelados. La relajación de R2 desde un código específico a bloqueo
con término histórico conserva la invariante original. El refuerzo de NOT IN
sobre su propia columna es correcto. La etiqueta «28 CORE» no es un recuento de
28 tests de pytest independientes en el candidato.

| Grupo original | Observación del port |
|---|---|
| Coordinación: 4 | Fuente/OR cubiertos. El audit original tampoco exigía el complemento completo; el diseño sí lo añadió. Tres rojos nuevos exigen esa parte de R3. |
| Fuente/evento/estado: 5 | Sujetos y bloqueos preservados; dos nuevos controles de utilidad R2 fallan. |
| Dos ventanas: 2 | Cubierto; nueva comprobación exige columna exacta además del mes. |
| OR real: 1 | Cubierto; fingerprint también comprobado independientemente. |
| Filtros negativos/legacy: 3 | Modalidad cubierta; `excepto obra` es tema, no tipo contractual negativo. El caso material de estado no se conserva en claims (F3). |
| Switch de fuente: 1 | Core cubierto; sin afirmar continuidad durable de B. |
| CLEAR: 1 | Core cubierto; sin afirmar recarga real de B. |
| Propiedad actualidad: 1 | Determinista y cubierta sobre bloqueo histórico. |
| Conteos decimales: 3 | Se añadieron los tres valores exactos originales al acreditador nuevo. |
| Conteo diminuto/render cero: 1 | No acredita cero numérico; falta renderer de evidencia nuevo para probar la presentación original (F5). |
| Monto temporal no finito: 3 | Acreditación aislada pasa, pero no prueba composición/render con filas temporales nuevas (F5). |
| Unsupported legacy: 1 | Singular y plural revalidados. |
| Span malformado: 1 | Revalidado, ampliado con negativos, booleanos, strings y span vacío. |
| Propiedad underflow: 1 | Port independiente Hypothesis, determinista y sin I/O externo. |

Los siete casos de integración contabilizados en el audit no se ejecutan.
El archivo `integration_contract` del candidato tiene **12 tests**, incluidos
**8 tests que sólo comprueban pertenencia a su propia lista de casos**. Son
marcadores documentales, no evidencia E2E. El cierre del autor decía 13 en esa
fila: el resultado real es 12, y el agregado real es 86. No se trata esa
diferencia de conteo como defecto runtime.

Se ejecutaron las 10 mutaciones del candidato. Para las cuatro requeridas se
agregó una prueba independiente que corre **la misma aserción antes y después**
de mutar comportamiento y exige AssertionError después: globalizar periodo,
ANY→ALL, omitir modalidad negativa y delegar WHERE a params legacy. Las cuatro
mueren por invariantes conductuales. No se escribió ninguna mutación en `app/`.

## 5. Instrumentos, puertas y salida literal

Entorno canónico: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/`;
cwd siempre el worktree de auditoría. Ruff 0.16.4, Semgrep 1.176.0,
Hypothesis 6.165.10, Codegraph 0.9.6 con Node 22.

Codegraph no tiene MCP cargado. El CLI bajo Node 26 rechazó arrancar; bajo Node
22 se inicializó/indexó el worktree (864 archivos Python; status 886 incluyendo
YAML) y reportó índice actualizado. Se usaron callers/callees y affected.
`affected` devolvió vacío pese a callers reales; se declara ese límite y se usó
fallback **rg + AST de imports + verificación manual** para cerrar consumidores
y seleccionar focales. No se interpretó el índice vacío de affected como cero
impacto. Logs de Codegraph incluidos.

Orden inicial ejecutado: Ruff → Semgrep → siete archivos de tests del candidato.
Luego pruebas independientes y focales según impacto. Las salidas completas
están en los archivos enlazados; estos son sus renglones literales de cierre.
Los `.txt.gz` conservan los bytes originales; las vistas `.txt` sólo normalizan
espacios finales y controles de terminal para pasar `diff --check`:

Ruff, todo `app/ tests/ scripts/` ([ruff.txt](ruff.txt)):

```text
All checks passed!
```

Semgrep, `--config .semgrep/secoppal-laws.yml app/ --metrics=off`
([semgrep.txt](semgrep.txt)):

```text
Ran 3 rules on 295 files: 10 findings.
```

Baseline rojo: 10. Delta Core: 0. Los diez hallazgos están en archivos cuya
**totalidad de bytes coincide con 62ec330**, demostrado en
[semgrep-delta.json](semgrep-delta.json); el JSON no contiene errores de scanner.
No se atribuye el verde de Ruff a Semgrep ni se afirma cero global.

Tests nuevos del candidato, siete archivos ([candidate-tests.txt](candidate-tests.txt)):

```text
86 passed in 1.03s
```

Tests independientes, dos archivos ([independent-final.txt](independent-final.txt)):

```text
15 failed, 31 passed in 0.66s
```

Los 15 rojos son aserciones de las invariantes descritas: R3 ×3, R2 soportado ×2,
migración ×4, estado material ×1, mutabilidad del plan ×1 y binding ×4.
No hay xfails ni skips nuevos; no se ocultaron rojos. Los dos archivos de tests
pertenecen a una rama de auditoría y no se presentan como una entrega verde.

Focales adyacentes ([adjacent-tests.txt](adjacent-tests.txt)):

```text
131 passed in 7.93s
```

Alcance: Entity Profile queries; SoQL builder, frases, negación temática;
QueryRouter; límites de tamaño. Se escogieron por proyecciones/emisores
reutilizados y la pérdida R3. No incluyen reevaluación de dispatcher, Burr,
paginación o ConversationStore.

`make lint-core` ([lint-core.txt](lint-core.txt)):

```text
app/core/ limpio
```

`git diff --check` se verifica para base→candidato y candidato→auditoría,
incluidos los archivos staged. Salida vacía y exit 0 documentados en postflight.

**Corrí los siete archivos Frame del candidato, los dos independientes y esos
seis focales adyacentes, no la suite.** La disponibilidad de Hypothesis no se
confunde con validación: se ejecutaron las propiedades del candidato y una
propiedad independiente de underflow. No hubo SECOP real ni E2E de B.

## 6. Fuera del carril y trabajo restante

No se tocaron QueryRouter/STOPWORDS (F1), los diez hallazgos Semgrep preexistentes,
los 20 JSONL heredados, documentación global ni los worktrees de otros agentes.
El informe del autor y el diseño se conservaron íntegros para comparación.

Antes de B: cerrar F1–F5 y la utilidad F6; repetir los tests rojos sobre el
candidato reparado y comprobar que se preservan los verdes y las cuatro
mutaciones. El trabajo de B —persistencia, admisión, dispatcher y paginación—
sigue diferido expresamente. No se ejecutó suite global ni se integró el Core.

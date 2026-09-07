# Entity Profile Semantic Plan — remediación de la revisión independiente (V1.1)

**Fecha:** 2026-09-07. **Base productiva:** `main` 7868664.
**Candidato V1:** `fix/entity-profile-semantic-plan-v1` @ 991a02f — **NO_GO**.
**Auditoría:** `audit/entity-profile-semantic-plan-v1-independent` @ 2358b2b.
**Rama:** `fix/entity-profile-semantic-plan-v1-remediation` (sin merge, sin push).

---

## 1. El veredicto que se remedia

La revisión independiente declaró **NO_GO / MERGE_RECOMMENDED: NO** con siete
hallazgos P1 y dos P2. Su diagnóstico fue correcto en los nueve casos y su
reproducción exacta: al portar sus contraejemplos sobre 991a02f se obtuvo
**34 failed, 18 passed**, el mismo recuento que reportó.

El error de fondo del cierre V1 fue mío y es sencillo de nombrar: **verifiqué la
autoridad única sobre el assembler y nunca sobre el workflow completo**. Todo lo
que vivía una capa más afuera quedó fuera de mi evidencia, y ahí estaba F5.

## 2. Los siete P1

| | Hallazgo | Causa | Estado |
|---|---|---|---|
| F1 | la petición temporal se perdía | un ESCALAR con precedencia fija | **CERRADO** |
| F2 | el marcador actual borraba eventos históricos | búsqueda global del marcador | **CERRADO** |
| F3 | palabras del tema inventaban fuentes | presencia en cualquier parte del texto | **CERRADO** |
| F4 | filtros materiales invisibles; AND ≡ OR | el alcance copiaba la petición aplanada | **CERRADO** |
| F5 | un segundo ejecutor tras el plan | nadie declaraba dueño del resultado | **CERRADO** |
| F6 | la conversación perdía su contrato | herencia sólo de fuente; pendiente pisaba confirmada | **CERRADO** |
| F7 | ceros inválidos en procesos | `int(float())` y contradicción asimétrica | **CERRADO** |
| P2-1 | metadata de estado huérfana | se purgaban sólo las claves físicas | **CERRADO** |
| P2-2 | evidencia del último día sobre-generalizada | conclusión más amplia que la muestra | **PARTIAL, documentado** |

**53/53 contraejemplos de la auditoría en verde.**

## 3. Los cuatro contratos, no siete parches

### A. ONE EXECUTION OWNER (F5)

`handle_normal_flow` llamaba incondicionalmente a `build_dataset_groups`
DESPUÉS del plan del perfil:

| petición | plan | observado |
|---|---:|---:|
| procesos y contratos publicados en agosto | 2 | 2 + 1 contrato |
| proveedores de procesos y contratos | 2 | 3 contratos |
| montos de procesos y contratos | 1 | 2 contratos |

Y la respuesta se reemplazaba por `Resultados en Procesos y Contratos`, que
llegaba a afirmar «Procesos: No encontré coincidencias» sin ningún conteo que
sostuviera esa ausencia: **el falso cero volviendo por la ruta que el sprint no
había mirado**.

`result_ownership.py` NO añade una segunda fuente de verdad: lee tipadamente la
que ya existe. `analytical_intent` lo escribe `entity_profile_state` en los tres
finales del perfil —respuesta, aclaración y error honesto—, así que ya
identificaba esos turnos; faltaba ponerle nombre y centralizar la decisión.

Guarda upstream en `normal_flow` + cinturón downstream en `build_dataset_groups`,
que falla cerrado. Cubre además el arranque de paginación: sembrar un buffer
tabular para un turno de perfil haría que un "más" posterior recorriera filas
que su plan nunca mostró.

Explícitamente **no** se resolvió borrando `dataset_scope`. El problema es de
propiedad del resultado, no una palabra en params.

### B. COMPLETE SEMANTIC REPRESENTATION (F1 + F2 + F3 + F4)

F1, F2 y F3 son el MISMO defecto: buscar una palabra en toda la frase y
atribuirle un significado que pertenece a una cláusula concreta. `clauses.py`
hace una sola cosa —decir dónde empieza y termina cada cláusula— y las dos
autoridades la consumen, de modo que no quedan dos escáneres reclamando la misma
ocurrencia. Se coordina por `y`/`e`/`,`/`;` y **no** por `o`, que es lenguaje
temático.

**F1.** `procesos publicados y contratos firmados en agosto` y su orden inverso
daban ambos `CONTRACT_SIGNATURE`: la firma ganaba por PRIORIDAD, no por
posición. No era dependencia del orden textual sino la imposibilidad de que un
escalar represente dos hechos. Ahora cada hecho es una reclamación por
ocurrencia y cada esquema honra la suya.

**F2.** Un tramo gobernado por un marcador de actualidad **de su propia
cláusula** describe el estado de hoy, y nunca se enmascara al releer
`estado_families`: es justo la evidencia de estado que debe sobrevivir.

**F3.** Sólo la PRIMERA palabra de fuente de cada cláusula elige fuente. Una
coordinación real pone una al frente de cada cláusula; una aparición dentro de
un complemento del tema, no. Estructura posicional, sin catálogo nuevo ni
inferencia probabilística.

**F4.** El alcance copiaba el `SemanticRequest`, cuya tupla `topic` ya había
aplanado los operadores. Ahora se deriva de los params YA PROYECTADOS por cada
SourcePlan —los mismos que el builder convierte en WHERE—, así que
`RenderedScope == ExecutedScope` deja de ser una afirmación y pasa a ser una
consecuencia estructural. `TopicExpression` declara AND con " y ", OR con " o ",
frases entre comillas y exclusiones con "excepto". Los IDs de modalidad se
traducen con `display_name` del catálogo existente. El filtro de valor se
declara por fuente, porque `precio_base` es presupuesto estimado y
`valor_del_contrato` es valor pactado.

### C. TRANSACTIONAL CONVERSATION (F6)

`EntityProfileConversationState` separa **confirmada** —lo último que la persona
pidió y el sistema pudo ejecutar— de **pendiente** —la lectura que provocó una
aclaración—. Una aclaración retiene, no sustituye: preguntar no puede destruir
el frame ya aceptado. El esquema va versionado porque se persiste en disco y lo
lee una sesión posterior.

`inherit_request` compone por dimensión: conservar, fijar o limpiar. Un cambio
de periodo es ortogonal al foco y deja de forzar overview.

`sources_origin` conserva de dónde salió la fuente ORIGINALMENTE, separado de
cómo llegó a este turno. Es lo que impide que una fuente nombrada hace tres
turnos se acote en silencio ahora.

Un «sí» a secas no selecciona nada cuando la aclaración ofrecía alternativas:
se repite la MISMA pregunta en vez de elegir por la persona o caer a una
búsqueda tabular heurística.

### D. STRICT EVIDENCE TRUTH (F7)

`int(float(value))` truncaba: `"0.5"` → **0 procesos** y `"-1"` → **-1
procesos**, ambos presentados como hechos. Se usa `Decimal` precisamente para
distinguir `"4.0"` —entero exacto serializado con decimal, que se acepta— de
`"0.5"`, que se rechaza; con `float` son indistinguibles tras truncar.

La reconciliación de ceros existía sólo para contratos. Ahora cada agregado
declara qué otra evidencia del MISMO plan puede desmentir su cero, y sólo
llamadas del mismo dataset cuentan.

Se conserva intacta la independencia válida entre conteo y monto ausente, que
V1 sí había resuelto bien.

## 4. EXPLICIT REQUEST COMPLETENESS

V1 aclaraba sólo cuando el conjunto ejecutable quedaba VACÍO; un conjunto propio
no vacío pasaba, y con él la ejecución parcial de una petición hecha entera.

La distinción que hace todo el trabajo es la **procedencia**: una fuente que
sólo entró por DEFAULT o herencia puede acotarse, porque nadie la pidió; una que
la persona NOMBRÓ, no. Por eso `analiza X` + proveedores sigue respondiendo
contratos, y `analiza proveedores de procesos y contratos de X` pregunta.

## 5. Adjudicaciones

**Un conflicto interno de la auditoría.** Sus dos artefactos se contradicen en
`procesos publicados y contratos firmados en agosto`: el texto de F1 fija como
aceptación «ambos órdenes conservan ambos eventos **o** ambos aclaran», y una
parametrización de evidencia exigía sólo la segunda mitad. La petición es
enteramente representable —procesos por publicación, contratos por firma, un
único periodo— y aclararla sería negarse a contestar una pregunta bien formada.
Se conserva como control POSITIVO de extremo a extremo, verificando que cada
esquema consulta su propia fecha; las otras dos frases siguen estrictas.

**Cinco expectativas de V1**: EXPLICIT BOTH con foco de una sola fuente pasa de
acotar-y-declararlo a preguntar (4), y el aviso de agregado ilegible de procesos
pasa a ser el unificado de agregado inválido, porque la asimetría entre las dos
fuentes era precisamente F7 (1).

**El arnés del freeze V1** ignoraba la aclaración y seguía construyendo el
perfil donde la ruta productiva se detiene.

## 6. El guard de paridad por SHA

La suite completa reveló lo que las focales no podían ver: `normal_flow.py`
(SEARCH_SH1_MODIFIED) y `dual_dataset.py` (BASELINE, byte-idéntico a main pre-M1
y sin una sola modificación hasta hoy) están bajo contrato de paridad.

El módulo declara el ritual: «Re-pinning them alone would throw away the guard,
so the divergence is pinned AND bounded». Se re-pinean con comentario causal y
**dos tests que acotan**: la condición nueva responde False para todo intent
tabular o analítico —incluido uno futuro— y True sólo para el perfil; y sin
dueño terminal la expansión dual sigue llegando al hijo secundario.

## 7. LAST_DAY_GATE = PARTIAL

La auditoría no refutó la comparación SQL: objetó que la conclusión era más
amplia que la muestra. Tenía razón. Evidencia nueva con procedencia en
[`last_day_provenance.json`](last_day_provenance.json)
(sha256 `fa58d5ec63cda5e1028cc2715eb4fc62d23135e640ec9ebecb6fa1a49e67b3ae`):

| dataset | campo | tipo de columna | scope | `<= último` | `< siguiente` | Δ |
|---|---|---|---|---:|---:|---:|
| `jbjy-vk9h` | `fecha_de_firma` | `calendar_date` | Atlántico 2026-07 | 2180 | 2180 | 0 |
| `p6dx-8zbt` | `fecha_de_publicacion_del` | `calendar_date` | Atlántico 2026-07 | 3535 | 3535 | 0 |
| `jbjy-vk9h` | `fecha_de_firma` | `calendar_date` | Antioquia 2026-05 | 1391 | 1391 | 0 |

Muestras del día frontera: `T00:00:00.000` en los tres scopes. Se añade query
exacta, respuesta cruda, tipo de columna y hora de captura.

**Conclusión honesta:** `calendar_date` PUEDE llevar hora; estos datasets, en
tres scopes independientes, no la llevan. Eso sostiene «no se observó componente
horario», no una afirmación universal. **La semántica global de fechas no se
toca**, que era y sigue siendo la decisión operativa.

## 8. Validación

| Puerta | Resultado |
|---|---|
| Contraejemplos de la auditoría | **53/53** (34 failed → 0) |
| Propiedades nuevas P13–P23 | **28** casos, 11 propiedades |
| Mutación sobre P13–P23 | **11/11 detectadas**, una por P1 |
| Propiedades V1 P1–P12 | conservadas, verdes |
| Corpus Entity Profile | **588** |
| Corpus de consultas | **89/89 PASS** |
| Suite completa | **12413 passed, 11 skipped, 16 xfailed, 0 failed (9:10)** |
| Ruff | verde |
| `make lint-core` | `app/core/ limpio` |
| Semgrep | 10 en `app/`, **delta 0** |
| Topes de LoC | 2/2 |
| `git diff --check` | limpio |

La suite completa se corrió UNA vez, en serie, sobre el SHA de cierre de la
documentación. La revisión independiente observó con razón que el total de V1
quedaba «reportado por el autor, no acreditado independientemente»; el log de
esta corrida queda para que el auditor lo repita sobre el HEAD final.

### Dogfood en vivo (2026-09-07)

```text
"analizame que contratos firmo alcaldia de baranoa en agosto"
  CONTRACTS/EXPLICIT · CONTRACT_SIGNATURE · topic ()
  planned=4  observed=4  datasets=[jbjy-vk9h]
  47 contratos por $911.5 millones

"analiza procesos publicados y contratos firmados en agosto de X"
  BOTH/EXPLICIT · [PROCESS_PUBLICATION, CONTRACT_SIGNATURE]
  planned=6  observed=6
  Procesos: del 1 al 31 de agosto, según fecha de publicación.  -> 48
  Contratos: del 1 al 31 de agosto, según fecha de firma.       -> 47

"analiza contratos terminados en agosto de X"
  unsupported=['terminados'] · CLARIFICACIÓN · 0 llamadas
```

## 9. Mapa de autoridades

| Pregunta | Respuesta canónica única |
|---|---|
| ¿Quién es dueño de ejecución y presentación? | `result_ownership.result_owner` |
| ¿Quién decide `requested_sources`? | `source_request` (núcleo por cláusula) |
| ¿Quién decide los hechos temporales? | `temporal_event.resolve_temporal_events` |
| ¿Quién decide el estado actual? | `estado_families` (sin cambio) |
| ¿Quién decide la expresión temática? | `material_filters.TopicExpression`, desde las dimensiones del parser |
| ¿Quién decide los filtros materiales? | `material_filters.MaterialFilters`, desde los params proyectados |
| ¿Quién decide la proyección por fuente? | `source_plan.project_source` |
| ¿Quién decide si un hecho está acreditado? | `evidence` + parsers de `aspects/` |
| ¿Quién decide qué alcance se renderiza? | `effective_scope`, derivado de los SourcePlans |
| ¿Quién decide el estado conversacional? | `conversation_state` |

**ANTI_FRAGMENTATION: sin autoridades duplicadas.** `result_ownership` lee
`analytical_intent`, no lo duplica. `material_filters` consume las dimensiones
del parser, no las reparsea. La herencia de TEMA sigue siendo de
`followup_mutations` (ADR-061).

## 10. Deuda declarada

| Id | Qué |
|---|---|
| `PUBLICATION_EVENT_VS_STATE_GLOBAL_DEBT` | fuera del perfil, `publicados` sigue activando estado actual |
| `SCALAR_DATASET_SIGNAL_SOUP_DEBT` | `_select_dataset` sigue eligiendo por sopa de señales |
| `LAST_DAY_UNIVERSAL_PROOF` | PARTIAL: tres scopes, no una prueba universal |
| `PROFILE_PLURAL_AGREEMENT_DEBT` | el render dice "1 contratos"; cosmético y preexistente |
| `PROCESS_BUDGET_AGGREGATE_DEFERRED` | `values` sobre procesos exigiría agregar `precio_base` |
| `CONTRACT_MODALITY_ASPECT_DEFERRED` | `modalities` sobre contratos, sin capacidad en V1 |

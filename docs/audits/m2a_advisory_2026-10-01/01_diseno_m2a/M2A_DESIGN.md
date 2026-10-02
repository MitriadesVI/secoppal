# M2a — Conversation mínimo indispensable antes del CUTOVER de SEARCH

Fecha: 2026-10-01. Autor: METIS (diseño; sólo lectura de código, ninguna ejecución de runtime ni de tests).
Estado: PROPUESTA para el orquestador. Sin número de ADR (lo asigna el orquestador sobre el máximo global).

Bases leídas (todas las citas `archivo:línea` son de estas revisiones):

| Alias | Worktree | Rama / HEAD |
|---|---|---|
| `main` | `/Users/rodrigoortiz/Documents/secoppal` | `main` @ `4ec99bc1` |
| `c12` | `/Users/rodrigoortiz/Documents/secoppal-m1-c12` | `mission/m1-c12-ir-v2` @ `67c0badb` (IR v2, candidato C1.2) |
| `m1` | `/Users/rodrigoortiz/Documents/secoppal-m1` | `mission/m1-search-foundation-phase-a` @ `4940b73e` (Gold v1.9, C0, ADR-103) |
| `par` | `/Users/rodrigoortiz/Documents/secoppal-m1-int-par` | candidato `2dde6489` (sólo leído: `time_grammar.py`, `time_authority.py`) |
| `r5` | `/Users/rodrigoortiz/Documents/secoppal-m1-int-r5` | `docs/audits/m1_wave1/R5_REPAIR.md` |
| Gold REAL | `~/Documents/tools/secoppal-diagnostics/m1_search_gold/sealed_v1/gold_v1.jsonl` | fuera de git; sha `6e470e89…` (`m1:gold/GOLD_SEAL.json`) |

Convención: **HECHO** = verificado en código o datos; **INFERENCIA** = deducido, no ejecutado; **DECISIÓN
PROPUESTA** = lo que este documento decide. El Gold REAL se cita sólo por id y conteo.

---

## 0. Precondición: sí hay decisión de arquitectura

| Eje | Por qué no es trabajo de ARES |
|---|---|
| Persistencia | Hoy la aceptación de un turno Foundation no se guarda en ningún sitio (§1, H3). M2a crea el primer estado durable de Foundation. |
| Primitiva de estado | Hay cuatro estados conversacionales vivos (`Turn`, sobre, buffer de paginación, marcas en `parsed_params`). Añadir un quinto sin regla de autoridad reproduce el problema que el roadmap quiere cerrar. |
| Contrato entre componentes | Una aceptación compuesta cita evidencia de varios turnos; el guard actual ata la aceptación a un único texto crudo (§1, H5). |
| Frontera de confianza | El intérprete pasa a recibir estado como entrada. Hay que fijar qué ve y qué no. |
| Irreversibilidad | Registros nuevos en disco con datos personales; hashes que vuelven a cambiar; rollback `v5_previous` que debe leerlos o rechazarlos. |

---

## 1. Hechos verificados (además de los del encargo)

| Id | Hecho | Evidencia |
|---|---|---|
| H1 | Un turno Foundation escribe `parsed_params = {}` y `outcome_reason = "foundation_v3"`; `outcome` es `"RESULTS"`/`"NO_RESULTS"`/`"SOURCE_ERROR"`. | `main:app/core/foundation_runtime.py:185,221-234`; `c12:…/foundation_runtime.py:222-235` |
| H2 | Todo follow-up tras un turno Foundation termina en `FOUNDATION_CONTINUATION_UNSUPPORTED` con 0 llamadas. La decisión «¿es búsqueda nueva completa?» se toma con `QueryRouter.parse` + `NEW_SEARCH_VERBS` + `is_complete_new_search`. | `main:app/core/foundation_route_owner.py:93-114`; `foundation_runtime.py:238-258` |
| H3 | **La aceptación no se persiste.** `result["foundation_acceptance"]` sólo vive en memoria; la traza guarda `foundation_telemetry` y el ledger guarda un digest. C0 §5.6 dice «carga la aceptación inmutable del turno anterior por `trace_id`»: ese almacén no existe. | `main:app/core/feedback.py:263-264`; `main:app/core/turn_ledger.py:193`; grep de `foundation_acceptance` en `app/` (3 lectores, ninguno escribe a disco) |
| H4 | La IR v2 ya tiene `Operation.FOLLOWUP_DELTA`, `ProposedJob.referent_evidence` y `ProposedOmission` con `Omission.NO_CHANGE`. Un job no-SEARCH exige evidencia propia y puede transportar peticiones con condiciones (C7). | `c12:app/core/foundation_operations.py:28,59-62`; `c12:app/core/interpretation_contract.py:44-48,171-202,293-302` |
| H5 | El guard ata la aceptación a **un** texto: `acceptance.validate_raw(raw)` con el `ctx.user_query` del turno, y `interpretation_hash = digest(accepted.to_dict())`. Todas las citas de una propuesta deben enlazar con ese único `raw_sha256`. | `c12:app/core/foundation_guard.py:41-49`; `c12:…/interpretation_contract.py:475-492`; `c12:…/foundation_projection.py` (`project_accepted`, `interpretation_hash`) |
| H6 | `HostResolution.resolved_ids: tuple[str, ...]` es el único transporte de valor; `AuthorityVerdict.to_resolution` codifica el `ExecutableValue` ahí. El registro de autoridades está vacío y la admisión no lo importa. | `c12:…/interpretation_contract.py:498-515`; `c12:…/foundation_authority_contract.py:277-280`; `c12:…/foundation_authority_registry.py:54` |
| H7 | La proyección sólo compila expresiones conjuntivas; un READY con OR/NOT de grupo devuelve `UNSUPPORTED`. Por tanto toda aceptación **ejecutada** es conjuntiva hoy. | `c12:…/foundation_projection.py` (`_conjunctive`, `NONCONJUNCTIVE_EXPRESSION`) |
| H8 | `ConversationStore.get_history` omite en silencio las líneas corruptas y `append_turn` escribe sin exclusión. El sobre (`ConversationEnvelope`) sí es estricto, con revisión, época y lock, pero sólo se publica con `SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED`, cuyo default es `False`. | `main:app/core/conversation_store.py:201-256`; `main:app/core/conversation_envelope.py:9-16`; `main:app/core/workflow/turn_coordination.py:35-46`; `main:app/config.py:83-85` |
| H9 | Con el Frame encendido, un turno Foundation publica dueño `NONE` y la admisión **aclara antes de despachar** toda paginación (`no_referent_to_continue`) y toda selección ordinal/afirmativa (`no_current_selection`). | `main:app/core/orchestrator.py:447`; `main:app/core/workflow/admission_rules.py:161-181` |
| H10 | Lectores legacy que miran **más allá** de un turno Foundation: `radar._last_search_params` recorre 10 turnos y toma el primero con alcance o tema (salta el `{}`); `last_successful_turn` usa `had_results()`, que compara con `Outcome.SUCCESS/PARTIAL` (`"success"`/`"partial"`) y por tanto nunca reconoce `"RESULTS"`. | `main:app/core/workflow/handlers/radar.py:54-66`; `main:app/core/conversation_store.py:129-134,266-275`; `main:app/core/outcome.py:20-21` |
| H11 | La ventana de follow-up es de 30 minutos; `NEW_SEARCH_VERBS` se lee en 6 módulos además de su definición. | `main:app/core/_followup_constants.py:15,31-35`; grep: `followup_engine`, `actor_conversation`, `conversation_store`, `foundation_route_owner`, `workflow/external_admission`, `workflow/handlers/actor_selection` |
| H12 | Un monto desnudo por debajo de 1.000.000 COP no se admite como dinero; un mes sin año se resuelve por reloj («ocurrencia más reciente no posterior a hoy») con `year_inferred=True`. | `c12:app/core/money_admission.py:62,78-85`; `par:app/core/time_authority.py:15-20,263-264` |
| H13 | El follow-up legacy fusiona por **campo** (`_MERGE_POLICY`: `replace` para fechas, valor mínimo, valor máximo, estado, entidad; `keep_previous_unless_explicit` para tema y dataset) y clasifica 16 intenciones leyendo texto. | `main:app/core/conversation_store.py:55-70`; `main:app/core/followup_engine.py:66-72,954-1032` |
| H14 | El contrato de continuación del sobre **referencia** el resultado publicado y no lo copia («si contuviera copias, volvería a haber dos versiones del mismo universo»). | `main:app/core/workflow/continuation.py:9-14` |

### 1.1 Caracterización de los `FOLLOWUP_DELTA` del Gold

**HECHO.** El Gold REAL tiene 65 `FOLLOWUP_DELTA` (todos `FOLLOWUP_FRAGMENT`, `EXECUTE`; 64 `CLEAR`, 1 `AMBIGUOUS`)
y 6 `NEEDS_HISTORY` (4 `SEARCH`, 1 `RANK_SUPPLIERS`, 1 `GROUP_BY`). Los 65 llevan `requests: []`: **el delta no está
etiquetado**, sólo descrito en `notes`, y **ningún registro guarda el turno anterior** (los insumos `inputs/real_*.jsonl`
son textos deduplicados con `freq`/`channels`, sin sesión). En git hay 3 sintéticos (`P159`, `P161`, `P163`, con gemelos
«consulta completa» `P160/P162/P164`) y 4 vecinos `NEEDS_HISTORY` (`N013`, `N076`, `N089`, `N101`).
C0 §5.3.6 punto 5 dice «64»; el sello vigente tiene 65.

Clasificación propia (METIS, a partir de `notes` y texto, **no** es etiqueta Gold; ids en el apéndice A):

| Clase de delta | n | Operación mínima | ¿Referente al result-set? | ¿Respuesta a aclaración? | ¿Cabe en M2a? |
|---|---|---|---|---|---|
| TIME (año, rango, cota abierta) | 14 | SET TIME | no | no | sí (exige E-TIME) |
| TIME sólo mes, sin año | 3 | SET TIME | no | no | sí, con decisión Q4 |
| VALUE una cota | 10 | SET VALUE | 1 | no | sí (E-VALUE) |
| VALUE rango (dos condiciones) | 6 | SET VALUE ×2 | 1 | no | sí: ver tensión T1 |
| VALUE sin unidad | 2 | SET VALUE | no | no | aclara: ver tensión T3 |
| GEOGRAPHY (6 departamento/municipio, 2 región) | 8 | SET GEOGRAPHY | no | no | sí (E-GEO) |
| ORDER (4 valor, 1 fecha) | 5 | SET ORDER | no | no | sí (E-ORDER) |
| ORDER + referencia a una condición arrastrada | 1 | SET ORDER + KEEP TIME | sí | no | sí (directiva KEEP) |
| STATE | 2 | SET STATE | 1 | no | sí (E-STATE) |
| MODALITY | 2 | SET MODALITY | no | no | sí (1 es `AMBIGUOUS`) |
| TOPIC | 2 | SET TOPIC | no | no | sí, con decisión Q2 |
| Dos dimensiones (ORDER+TIME, VALUE+TIME) | 2 | 2 SET | 1 | no | sí: T1 |
| Universo + STATE | 1 | SET universo + SET STATE | no | no | sí |
| BUYER + **quitar** VALUE | 1 | SET BUYER + REMOVE VALUE | no | no | sí: único REMOVE |
| ACTOR pegado como opción de desambiguación | 1 | ANSWER | no | **sí** | fuera: el ancla es Actor (legacy) |
| Ancla que no es SEARCH (actor ×3, conteo ×1, probable perfil ×1) | 5 | — | — | — | **fuera** (dueño legacy) |
| **Total** | **65** | | **5** | **1** | **59 dentro, 6 fuera** |

Lectura: 49/65 son un SET sobre una sola ranura; 6 son rangos de valor; 4 tocan dos ranuras (uno de ellos quita un filtro); 6 quedan fuera;
**0** piden con claridad «añadir estrechando» (ADD). La nota Gold dice «añade» en 12 casos (3 de ellos «añade/cambia»): sin el turno anterior no se
puede saber si es insertar o reemplazar. De ahí sale la forma del álgebra (§5.2): la operación es **SET por
ranura** y el host deriva «insertar» o «reemplazar» del estado, no del texto.

Evidencia secundaria en git: `main:tests/fixtures/dogfood_followup_regressions.json` (10 casos con el esquema
`prev_params` / `query` / `expected_params`) es el precedente del esquema «anterior, delta, resultante» de §8.1.

`PROTOCOL` REAL = 22 (heurística propia sobre `notes`: ≈13 selección de opción, ≈4 paginación, ≈4 reset, 1 entrada
truncada). Tras el CUTOVER la paginación y la selección operan sobre resultados Foundation.

---

## 2. DECISIÓN (resumen)

1. **Un tipo nuevo `AcceptedInterpretation`** (aceptación efectiva, nivel conversación): requests resueltos +
   condiciones aceptadas, cada una con su valor codificado **y su origen** (turno y cita, o evento de protocolo).
   No es una `InterpretationAcceptance`: aquélla sigue siendo «lo que el host admitió de **este** texto».
2. **Un único camino de proyección**: todo turno, también el primero, proyecta una `AcceptedInterpretation`
   (`from_acceptance` para el primer turno). No hay dos proyecciones.
3. **Un almacén propio, append-only y estricto**, un archivo por chat, con clave `trace_id`. Guarda por turno:
   la `InterpretationAcceptance` v2 **tal cual** (auditoría y replay, vía `load_acceptance`), la efectiva
   materializada, el recibo de ejecución, el referente de resultado y la aclaración pendiente. `Turn.parsed_params`
   de un turno Foundation sigue siendo `{}`; el `Turn` sólo aporta el puntero (`trace_id`).
4. **Álgebra por ranuras**: `SET`, `REMOVE`, `KEEP` sobre ranuras de dimensión; `SET ORDER`; `SET universo`. Un delta
   es un conjunto de operaciones, a lo sumo una por ranura, que se acredita **entero o nada**. Se arrastran los
   **valores**; se re-deciden las **políticas de petición** (PV, ACTOR×STATE, C5).
5. **El intérprete propone el delta; las mismas autoridades lo acreditan.** El LLM recibe una *tarjeta de estado*
   hecha sólo de enumerados (sin texto de turnos previos, sin valores). «Delta o búsqueda nueva» sale de la
   **estructura** de la propuesta (job `FOLLOWUP_DELTA` o `referent_evidence`), con una guarda estructural del host:
   un fragmento nunca se ejecuta solo.
6. **Aclaración pendiente** como registro tipado de una sola ranura, vida de un turno, con dos puertas (evento de
   opción sin LLM; texto libre vía intérprete) y la regla «o respuesta, o búsqueda nueva completa, o se vuelve a
   preguntar»: nunca un híbrido.
7. **Referente = intensión** (la consulta aceptada y su plan), no las filas visibles. Lo usan paginación, orden,
   botones de vista y export; todo lo extensional («el segundo», «de esos diez») queda fuera.
8. **Se descarta el puente Foundation → `parsed_params`** (§4.1).

---

## 3. Decisiones vigentes respetadas y tensiones

### 3.1 Respetadas

| Vigente | Cómo |
|---|---|
| Invariantes 1–6 del roadmap | Una sola interpretación por turno; el LLM no entrega valores (la tarjeta no contiene ninguno que repetir); nada cae a legacy. |
| ADR-069 | Todo delta inválido termina en aclaración con código cerrado; ningún arrastre se hace sin eco. |
| ADR-103 §3, C0 INV-C0-3/5 | El enrutamiento sigue siendo por estructura (operación + dueño del ancla), nunca por el resultado de una resolución. |
| Contrato de autoridades C1.1 congelado | **No se toca.** Las autoridades siguen viendo sólo la ventana del texto actual. Lo que dependería del estado (unidad heredada, año heredado) se resuelve fuera de la autoridad o se aclara (T3). |
| Decisión 1 (Wave1 opción A) | La gramática temporal se usa sólo por cita; M2a no añade lectura de texto legacy. |
| Decisión 4 y 5 (span con dueño, registro de ocurrencias, opacos) | El registro de ocurrencias es por turno y cubre sólo el texto actual; una condición arrastrada no es una ocurrencia del turno (ver T7). |
| Decisión 7 (rollback `v5_previous`) | Ver T8: el rollback debe leer el esquema del almacén o tratarlo como ancla inválida. |
| Decisión 8 (un escritor del núcleo) | Todos los módulos nuevos son `foundation_*`; los dos cambios fuera del núcleo tienen dueño aparte (§9). |
| `continuation.py:9-14` (referenciar, no copiar) | El sobre, cuando exista, referencia el registro M2a por `trace_id`; no contiene la aceptación. |

### 3.2 Tensiones (no las rodeo)

| Id | Tensión | Evidencia | Consecuencia |
|---|---|---|---|
| **T1** | «REFINE/REPLACE de **UNA** condición» no cubre el Gold: 6/65 son rangos de valor (dos condiciones) y 4/65 tocan dos dimensiones. | §1.1 | El alcance correcto es «**un delta = conjunto atómico de operaciones, una por ranura**». No añade complejidad: las ranuras son independientes. Si se mantiene «una condición» literal, 10/65 aclaran. |
| **T2** | «ADD/REMOVE si la evidencia lo exige»: REMOVE sí (1/65, y es además el modo de corregir un arrastre excesivo); ADD **no** aparece con claridad (0/65). | §1.1 | REMOVE entra; ADD queda fuera de M2a. |
| **T3** | «El valor nuevo se resuelve como en un primer turno» falla en 5/65: 2 montos sin unidad (un primer turno los rechaza, H12) y 3 meses sin año (un primer turno los fecha por reloj, H12). | §1.1, H12 | No es inviable, pero el resultado «como primer turno» es *aclarar* o *año del reloj*, no lo que el Gold marca como `EXECUTE`. Decisiones Q3 y Q4. Heredar unidad o año **dentro** de la autoridad exigiría cambiar `AuthorityContext` (contrato congelado): no lo propongo. |
| **T4** | C0 §5.6 presupone un almacén de aceptaciones por `trace_id` que no existe (H3). | H3 | El paso M2A-2 (almacén) es precondición del `handle_foundation_view_action` de F, no al revés. |
| **T5** | Una aceptación compuesta no puede ser una `InterpretationAcceptance`: sus citas pertenecen a varios textos y el guard valida uno (H5). | H5 | Tipo nuevo + un camino de proyección; `interpretation_hash` vuelve a cambiar (declarado). Es el coste real de M2a; «diminuto» no significa «sin tocar la proyección». |
| **T6** | El Gold sellado no permite medir «65 sin degradación»: no hay turno anterior ni delta etiquetado, y ≥6 de los 65 tienen ancla no-SEARCH. | §1.1 | Hace falta el Gold M2A (§8.1) antes del código. `followup_engine` **no puede morir en M2a**: sigue sirviendo a los dueños legacy (Actor, perfil). |
| **T7** | La invariante «nadie deriva significado positivo fuera de un span con dueño» no cubre botones ni opciones: un evento no tiene span. | C0 §5.6; ADR-103 enmienda 3 | La invariante debe decir «span con dueño **o evento de protocolo acreditado por el host**». Va a la enmienda 2. |
| **T8** | `v5_previous` como rollback: si esa versión es anterior a M2a, tras el rollback todo follow-up vuelve a bloquearse; y los registros escritos por la versión nueva pueden no ser legibles. | decisión 7 | Regla: esquema desconocido ⇒ ancla inválida ⇒ aclarar. `v5_previous` debe ser ≥ M2a para que el rollback no degrade 10 % del tráfico. |
| **T9** | Dos sistemas de estado deciden la admisibilidad: con el Frame encendido, la admisión veta paginación y selección sobre un turno Foundation antes de que Foundation lo vea (H9). | H9 | O el CUTOVER se hace con el Frame apagado, o M2a incluye el paso M2A-7 (dueño `FOUNDATION` en el contrato de continuación). Decisión Q8. |
| **T10** | Identidad de valor frente a cambio de universo: STATE y MODALITY se resuelven con `AuthorityContext.datasets`; al cambiar de universo el valor arrastrado puede no tener literales en el dataset nuevo. | `c12:…/foundation_authority_contract.py:162-176` | El arrastre nunca re-resuelve: si el valor tipado no cubre el dataset nuevo, se aclara (`CARRIED_CONDITION_NOT_IN_UNIVERSE`). |

---

## 4. Opciones comparadas

### 4.1 Cómo continuar tras un turno Foundation

| Opción | Fallos | Reversibilidad | Coste | Decisión |
|---|---|---|---|---|
| A. Dejar el bloqueo (`foundation_continuation_unsupported`) y hacer el CUTOVER | 65/620 (10,5 %) del tráfico REAL pierde respuesta; viola «no debe degradarse» (`m1:M1_0_SEARCH_CONTRACT.md` §6). | Total | 0 | No |
| B. **Puente**: proyectar la aceptación a `parsed_params` legacy para que `FollowupEngine` siga fusionando | Ver abajo | Fácil de quitar, pero ancla lo que el CUTOVER borra | Bajo ahora, alto después | **No** |
| **C. Delta nativo sobre `AcceptedInterpretation`** | Depende de que el productor proponga bien el delta (medible) y de un tipo nuevo (T5) | Alta: módulo nuevo, perfil `foundation_v3` conserva el bloqueo | Medio | **Sí** |

Por qué no B, con evidencia:

1. **Reintroduce un segundo intérprete que gobierna lo ejecutable.** `detect_and_merge(text, current_params, …)`
   parte de `QueryRouter.parse` y clasifica 16 intenciones leyendo texto (`main:followup_engine.py:66-72,954-1011`).
   Rompe los invariantes 1 y 6 y la decisión 4.
2. **Exige mantener vivo lo que el CUTOVER borra.** El resultado de la fusión son params legacy que sólo el SEARCH
   tabular legacy ejecuta; el roadmap M1.5 borra esa interpretación «sin fallback». Con el puente, M1-I no puede cerrar
   (invariante 5: transferencia con borrado).
3. **La proyección a params pierde significado.** `parsed_params` no expresa universo `UNSPECIFIED` + vista, filtro por
   token (`whole_token_folded` con `TokenFilter`), `BuyerValue` de grupo, `GeoValue` de región, ventana móvil, ni las
   políticas con recibo (H7, ADR-100, familias de modalidad). El follow-up ejecutaría otra consulta que el primer
   turno, sin decirlo. Ya hay un síntoma: Foundation tuvo que sellar `{"dataset": "contratos"}` sólo para un lector
   legacy del export (`main:foundation_presentation.py:32-39`).
4. **Es la clase de fallo que Wave1 R5 tuvo que reparar.** La fusión por lista finita de claves produjo los híbridos de
   §P2 y deja fuera lo que no está en la lista (`r5:R5_REPAIR.md:207`).
5. **Convierte el turno en legacy.** El turno siguiente tendría `parsed_params` legacy y saldría de Foundation: el
   estado Foundation se pierde en el segundo follow-up.

A favor de B (lo digo completo): no toca el prompt, es rápido y por construcción «no degrada frente a hoy». No basta:
la barra del CUTOVER es el Gold, no el comportamiento de hoy, y B hace el CUTOVER imposible.

### 4.2 Qué se persiste

| Opción | Fallos | Decisión |
|---|---|---|
| A. Sólo la `InterpretationAcceptance` v2 de cada turno; la efectiva se recalcula plegando la cadena de deltas | Recorrido de cadena sin tope; cualquier turno ilegible invalida todo lo posterior; exige releer textos viejos para validar | No |
| **B. La v2 del turno tal cual + la efectiva materializada + origen por condición** | Dos representaciones del mismo turno: se cierra con INV-M2A-6 y con el digest del registro | **Sí** |
| C. Sólo la efectiva | Se pierde el replay («la salida del LLM es lo único no determinista y queda guardada», roadmap §2) y `load_acceptance` queda sin uso | No |

### 4.3 Dónde vive

| Opción | Fallos | Decisión |
|---|---|---|
| A. Campo nuevo en `Turn` | Lectura tolerante: una línea corrupta se omite y se retrocede en silencio a un turno viejo (H8); sin exclusión al escribir; todo lector legacy de `Turn` vería el campo | No |
| B. Sección nueva del sobre | Sólo existe con el Frame encendido (H8); `ENVELOPE_SCHEMA_VERSION = 1` es estricto y un cambio deja chats en `admission_rollback_incompatible`; acopla M2a al Frame de Entity Profile, que es M2b; contradice H14 | No en M2a |
| **C. Almacén propio por chat, estricto, clave `trace_id`; el `Turn` aporta el puntero** | Un archivo más por chat; hay que borrarlo en `clear()` | **Sí** |

### 4.4 Qué ve el LLM del estado

| Opción | Fallos | Decisión |
|---|---|---|
| A. Nada (sólo el texto) | No puede distinguir «en 2026» fragmento de consulta nueva; todo se decidiría por un detector de texto | No |
| **B. Tarjeta de estado de enumerados** (hay ancla; universo; ranuras ocupadas por dimensión/polaridad/operador; hay orden; aclaración pendiente: dimensión + razón) | Puede no bastar para anáforas finas (medible en DEV) | **Sí** |
| C. Texto del turno anterior y/o valores resueltos | El LLM puede repetir valores (invariante 3); mete texto no confiable de otro turno en el prompt | No. Sólo se reabre si B no pasa DEV, y entonces texto del usuario, **nunca** valores (§13) |

### 4.5 Granularidad de la ranura de VALUE

| Opción | Efecto | Decisión |
|---|---|---|
| A. Una ranura por lado (`valor_min`/`valor_max`, como H13) | Un límite nuevo conserva el del otro lado: puede dejar un rango vacío o inesperado | Sólo si producto lo elige (Q1) |
| **B. Una ranura para toda la dimensión** | Un límite nuevo sustituye todo el rango previo; «entre X y Y» fija los dos | **Recomendada**; visible en el eco |

---

## 5. Diseño

### 5.1 `AcceptedInterpretation` y el registro persistido

Módulo nuevo `app/core/foundation_accepted.py` (núcleo). Registros inmutables (`frozen`, `slots`), `to_dict` con JSON
estándar, mismas reglas de construcción estricta que `interpretation_contract`.

```text
ConditionOrigin(frozen):
  kind: SPAN | OPTION_EVENT | VIEW_ACTION | HOST_POLICY
  trace_id: str                 # turno que introdujo la condición
  raw_sha256: str | None        # del texto de ese turno; obligatorio con SPAN
  condition_id: str | None      # id en la propuesta de ese turno; obligatorio con SPAN
  event_id: str | None          # opción o acción; obligatorio con OPTION_EVENT / VIEW_ACTION

AcceptedCondition(frozen):
  slot: Slot                    # (dimension, polarity); VALUE = una ranura (4.5-B)
  request_ids: tuple[str, ...]
  dimension, operator, polarity, role, level      # la afirmación, sin citas
  resolved: tuple[str, ...]     # HostResolution.resolved_ids tal cual (ExecutableValue.encode cuando la autoridad es ACTIVE)
  authority: str
  policies: tuple[(policy_id, version, effect), ...]
  origin: ConditionOrigin

AcceptedRequest(frozen):
  request_id, scope_id, subject: Subject, dataset: str
  universe_origin: ConditionOrigin | None          # None sí y sólo sí subject is UNSPECIFIED
  view: HostViewDecision | None                    # obligatorio sí y sólo sí subject is UNSPECIFIED

AcceptedInterpretation(frozen):
  schema: "m2a-accepted/1"
  requests: tuple[AcceptedRequest, ...]            # 1, o 2 con SHARES_SCOPE (como hoy)
  conditions: tuple[AcceptedCondition, ...]        # conjunción de primer nivel; nada más es direccionable
  base_trace_id: str | None                        # ancla de la que se compuso; None en un turno nuevo
  delta: tuple[AppliedOp, ...]                     # vacío en un turno nuevo
```

`AcceptedInterpretation.from_acceptance(acceptance, trace_id)` es pura y es el único constructor para un primer turno.
La proyección (`foundation_projection`) pasa a recibir `AcceptedInterpretation`; `project_accepted(InterpretationAcceptance)`
queda como `project(from_acceptance(…))`. El guard deja de llamar a `validate_raw` sobre el todo y verifica un
**enlace por origen**: las condiciones con origen en el turno actual se validan contra el texto actual (como hoy); las
arrastradas se validan contra el registro de su `trace_id` (existe, mismo chat, valor byte-idéntico).

Registro por turno, en `app/core/foundation_accepted_store.py` (núcleo):

```text
AcceptedTurnRecord(frozen):
  schema: "m2a-record/1"
  trace_id, chat_id, turn_id, created_at_utc, profile ("foundation_v5"), prompt_sha256
  turn_acceptance: dict          # InterpretationAcceptance.to_dict() v2 de ESTE turno; se relee con load_acceptance
  effective: AcceptedInterpretation | None     # None si el turno no llegó a READY
  execution: ExecutionReceipt | None
  result_ref: ResultSetRef | None              # §5.5
  pending: PendingClarification | None         # §5.4
  anchor_trace_id: str | None                  # última efectiva EJECUTADA que este turno deja como ancla
  record_sha256: str                           # digest del registro sin este campo

ExecutionReceipt(frozen):
  executed: bool                 # todas las peticiones contestadas por la fuente (ok, ok_order_sample o sin filas)
  reference_instant: str         # el que usó la proyección
  interpretation_hash: str       # digest(effective.to_dict())
  plans: tuple[(request_id, dataset, plan_hash_query, plan_hash_count), ...]
```

| Aspecto | Decisión propuesta |
|---|---|
| Ubicación | `<CONVERSATION_STORE_DIR>/<chat>.accepted.jsonl`, append-only, junto al historial. Nunca en git ni en el ledger (el ledger sigue con digests). |
| Clave | `trace_id` (único por archivo; reescribirlo es error). El `Turn` ya lo lleva (`foundation_runtime.py:232`). |
| Lectura | Estricta: claves cerradas, cada registro reconstruido por su constructor, `record_sha256` recomputado. Una cola ilegible = continuidad **inválida**, nunca retroceder a una línea vieja (misma política que `conversation_envelope.py:9-16`). |
| Escritura | Registro **antes** que el `Turn`, ambos bajo `conversation_lock` del archivo; antes de escribir se comprueba que el último `Turn` del chat sigue siendo el ancla leída. Si cambió: se devuelve la respuesta con el aviso de «contexto no conservado» (precedente `turn_publication.py:40-43`) y `anchor_trace_id = None`. |
| Borrado | `ConversationStore.clear()` añade el archivo a su tupla (`conversation_store.py:283-285`). Sin TTL propio en M2a: la vigencia se evalúa al leer (§5.3). |
| Versión | `m2a-record/1` y `m2a-accepted/1`. Otro valor ⇒ `UNSUPPORTED_SCHEMA_VERSION` ⇒ ancla inválida. Nunca se adivina por la forma. |
| Relación con `load_acceptance` | `turn_acceptance` es exactamente lo que `load_acceptance` sabe leer (`c12:foundation_acceptance_loader.py:134-151`). La efectiva tiene su propio lector en `foundation_accepted.py`. |
| `Turn.parsed_params` | Sigue `{}` en todo turno Foundation, también en los delta, de página y de aclaración. |

**Supuestos sobre los lectores legacy de `Turn.parsed_params`** (otro agente los tabula; si su tabla contradice alguno,
es condición de STOP 6):

| Lector | Supuesto |
|---|---|
| `handlers/pagination.py:155-158` (último turno) | Con `{}` no encuentra buffer y acaba en `pagination_no_context` (INFERENCIA por lectura de `:366-374`). M2a lo reemplaza con un handler propio que corre antes (§5.5). |
| `handlers/radar.py:54-66` | **Mira más allá** del turno Foundation y puede guardar una búsqueda legacy anterior (H10). Debe tratar el turno Foundation como **barrera** (paso M2A-8). |
| `conversation_store.last_successful_turn` | Nunca reconoce un turno Foundation (H10); sus consumidores son del follow-up legacy, que no corre con ancla Foundation. Barrera igualmente. |
| `handlers/actor_selection.py:115` | Lee `actor_pending` del último turno: con `{}` no hay pendiente; correcto. |
| `workflow/tabular_capability.py:62-70` (export con Frame) | Sólo con dueño `TABULAR`; Foundation publica `NONE`; devuelve `None`; correcto. |
| `handlers/normal_flow.py:92-94` | No se alcanza con ancla Foundation (INV-M2A-4). |

Regla general (**R-BARRERA**): un turno Foundation es opaco para legacy y es una barrera; ningún lector legacy puede
usar como referente un turno anterior a él.

### 5.2 Álgebra de deltas

**Ranura** (`Slot`): `(dimension, polarity)` para TOPIC, BUYER, ACTOR, GEOGRAPHY, TIME, VALUE, STATE, MODALITY,
CONTRACT_TYPE_OR_NATURE; `ORDERING` (ORDER + CARDINALITY juntos); `UNIVERSE` (sujeto de la petición). Sólo son
direccionables los átomos de la conjunción de primer nivel (H7).

| Operación | Quién la propone | Efecto | Precondiciones | Inválido ⇒ aclarar con |
|---|---|---|---|---|
| `SET(slot, condición nueva)` | condición del job delta | Quita **todas** las condiciones del ancla en esa ranura y pone la nueva (inserta si no había) | la condición nueva se resuelve `RESOLVED` por su autoridad | la razón cerrada de la autoridad |
| `SET ORDERING` | condición ORDER (± CARDINALITY) | Reemplaza orden y cardinalidad; el cursor vuelve a cero | — | `NEGATION_UNSUPPORTED:ORDER`, etc. |
| `SET UNIVERSE` | sujeto explícito en la petición del delta | Cambia el dataset; con ancla dual se queda con el nombrado | toda condición arrastrada dependiente de dataset cubre el nuevo (T10) | `CARRIED_CONDITION_NOT_IN_UNIVERSE` |
| `REMOVE(slot)` | directiva con cita | Quita la ranura | la ranura existe en el ancla; el resultado no queda como consulta global insegura | `DELTA_TARGET_ABSENT`, `DELTA_LEAVES_UNSAFE_QUERY` |
| `KEEP(slot)` | directiva con cita («de ese año») | No cambia nada; da dueño al span | la ranura existe en el ancla | `REFERENT_CONDITION_ABSENT` |
| `ANSWER(pending)` | §5.4 | Rellena la ranura abierta | pendiente vigente | §5.4 |

Reglas de composición (`app/core/foundation_delta.py`, puro):

1. **Atómico.** `compose(anchor, ops) → AcceptedInterpretation | DeltaRefusal`. Si una sola operación no se acredita,
   no se aplica ninguna y el ancla no cambia.
2. **A lo sumo una operación por ranura**; dos sobre la misma ranura ⇒ `DELTA_SLOT_CONFLICT` (salvo las dos cotas de
   VALUE, que forman un único SET).
3. **Arrastre por identidad.** Toda condición de una ranura no tocada pasa byte-idéntica (valor, autoridad,
   políticas, origen). Nunca se re-resuelve, tampoco las ventanas temporales móviles.
4. **Políticas de petición re-decididas.** Sobre el estado compuesto se vuelven a ejecutar las políticas puras que
   leen el conjunto: Product View (salvo `USER_OVERRIDE`, que es pegajoso mientras no haya `SET UNIVERSE`),
   ACTOR×STATE/ACTOR×TIME (Gold v1.8/1.9), `universe-c5@1`. H7 **no** se re-ejecuta sobre lo arrastrado (su evidencia
   es de otro turno); el universo arrastrado ya es una decisión aceptada.
5. **Consistencia tipada** (lista cerrada, sobre valores decodificados, nunca sobre texto): cota inferior > superior;
   mismo valor canónico con polaridades opuestas en una dimensión; CARDINALITY sin ORDER. Fallo ⇒
   `DELTA_CONTRADICTS_CARRIED`.
6. **Petición dual.** Un SET sobre una condición compartida vale para las dos peticiones; un SET cuya dimensión sólo
   existe en un dataset aclara.
7. **Sin ancla direccionable** (objetivo dentro de un OR/NOT): `DELTA_TARGET_NOT_ADDRESSABLE`.

`DeltaRefusal` lleva un código de un conjunto cerrado (los de la tabla más `DELTA_WITHOUT_EXECUTED_ANCHOR`,
`ANCHOR_EXPIRED`, `ANCHOR_UNREADABLE`, `ANCHOR_FOREIGN_OWNER`, `DELTA_OR_NEW_AMBIGUOUS`) y, cuando aplica, opciones.

**Quién propone.** El productor v5, con la tarjeta de estado como entrada. Forma en la IR v2 (H4), sin vocabulario
nuevo salvo una pieza:

- job `FOLLOWUP_DELTA` (evidencia obligatoria: el marcador o el fragmento entero) con una petición `UNSPECIFIED`
  (o con sujeto y su cita si nombra universo) cuyas condiciones son los SET;
- job `SEARCH` con `referent_evidence`: también es delta (C0 §5.3.2, última fila), y todas sus condiciones son SET;
- **pieza nueva**: `ProposedSlotDirective(kind: REMOVE | KEEP, request_id, dimension, evidence)` en
  `interpretation_contract.py`, válida sólo dentro de un job delta, validada por `validate_raw`.
  `ProposedOmission` no sirve: no lleva cita y un REMOVE sin cita sería significado sin span.

**Tarjeta de estado** (`app/core/foundation_state_card.py`, puro): `{anchor: bool, subject, slots: [(dimension,
polarity, operator, level?)], ordering: bool, pending: (dimension, reason) | null}`. Sólo miembros de enumerados
cerrados. Ni citas, ni textos, ni valores, ni nombres para mostrar.

**Quién acredita.** El host, igual que en un primer turno: `validate_raw` contra el texto actual → ventana → autoridad
de la dimensión → `HostResolution`. La autoridad **no recibe el ancla** (contrato C1.1 intacto).

**Delta frente a búsqueda nueva, sin detector de texto.** Decide la estructura de la propuesta:

| Propuesta | Hay ancla Foundation vigente | Resultado |
|---|---|---|
| job `FOLLOWUP_DELTA`, o job con `referent_evidence` | sí | delta |
| ídem | no (sin historia, vencida, ilegible) | aclarar: `FOLLOWUP_WITHOUT_HISTORY` / `ANCHOR_EXPIRED` / `ANCHOR_UNREADABLE` |
| ídem | el último turno es de un dueño legacy | lo que ya dice C0 §5.3.2: entrega de la **operación** a `LEGACY_FOLLOWUP` (R1–R4) |
| job `SEARCH` completo: sujeto explícito, o `UNSPECIFIED` con ≥1 condición de contenido (TOPIC, BUYER o ACTOR) | cualquiera | búsqueda nueva: no hereda nada; el ancla se sustituye |
| job `SEARCH` sin sujeto y sin condición de contenido (sólo modificadores) | sí | **nunca se ejecuta solo**: `DELTA_OR_NEW_AMBIGUOUS` con dos opciones-evento `[Aplicar a la búsqueda anterior]` `[Buscar sólo esto]` |
| ídem | no | camino SEARCH normal de M1 (PV decide o bloquea) |

La última guarda es estructural y monótona: sólo retira ejecución. Sustituye a `continues_foundation_turn`
(`foundation_route_owner.py:93-114`) y a su lectura de `NEW_SEARCH_VERBS` **en el perfil v5**; el perfil
`foundation_v3` conserva el bloqueo actual. Los otros lectores de `NEW_SEARCH_VERBS` son de dueños legacy y no se tocan.

**Eco obligatorio.** Todo turno delta responde con el conjunto resultante de filtros, marcando lo cambiado y lo
arrastrado, y con una acción para quitar cada filtro arrastrado (REMOVE como evento). Sin eco el arrastre es invisible.
C0 §13 dejó el eco general fuera de C1; M2a lo necesita para sus turnos.

### 5.3 Ancla

El ancla de un chat es la efectiva del `anchor_trace_id` del registro del **último turno visible**. Reglas:

| Turno | `anchor_trace_id` que deja |
|---|---|
| búsqueda nueva ejecutada | él mismo |
| búsqueda nueva no ejecutada (aclaración, no soportado, error de fuente) | `None` (marco nuevo sin ejecutar: no hay sobre qué aplicar un delta) |
| delta ejecutado | él mismo |
| delta rechazado, delta con error de fuente, re-pregunta | el ancla anterior (no se consume) |
| página, acción de vista | él mismo (misma efectiva o efectiva con la vista cambiada) |
| caída del proveedor («no pude interpretar ahora») | el ancla anterior |
| turno de un dueño legacy | no escribe registro ⇒ el último turno no es Foundation ⇒ no hay ancla Foundation |

Vigencia: edad del turno ancla ≤ `FOLLOWUP_WINDOW_MINUTES` (30, H11), evaluada con el instante de referencia
inyectado. «Ejecutó» = `ExecutionReceipt.executed` (incluye cero filas: relajar un filtro tras «ninguna fila» es un
delta legítimo).

### 5.4 Aclaración pendiente

Módulo `app/core/foundation_pending.py` (núcleo).

```text
PendingClarification(frozen):
  pending_id: str
  asked_trace_id: str            # turno que preguntó
  base_trace_id: str | None      # ancla sobre la que se aplicará la respuesta; None si el marco es una búsqueda nueva
  frame: AcceptedInterpretation  # todo lo ya resuelto del marco (sin la ranura abierta)
  open_slot: Slot                # exactamente UNA en M2a
  open_condition: (dimension, operator, polarity, level)   # la afirmación sin valor
  reason: str                    # de AUTHORITY_REASONS (cerrado) o DELTA_* (cerrado)
  detail: str | None             # AuthorityVerdict.detail
  options: tuple[PendingOption, ...]   # puede ser vacío
  expires_at_utc: str
  asks: int                      # 1 al crearse; tope 2

PendingOption(frozen):
  option_id: str
  display_key: str               # lo que se muestra, generado por el host
  resolved: tuple[str, ...]      # valor codificado que la AUTORIDAD ofreció al preguntar
  authority: str
```

Se crea sólo si la aceptación quedó en `NEEDS_CLARIFICATION` con **una** ranura sin resolver y todo lo demás
`RESOLVED`. Con dos o más ranuras abiertas no hay pendiente: mensaje y reformulación (lista pendiente general = M2b).

| Momento | Regla |
|---|---|
| Entrada por **evento de opción** (`option_id` + `pending_id`, capa de protocolo, sin LLM) | El host toma `PendingOption.resolved` tal cual (origen `OPTION_EVENT`), compone `frame + opción` y ejecuta. Token caducado o ajeno ⇒ «esa opción ya no está vigente». |
| Entrada por **texto libre** | El intérprete recibe la tarjeta con `pending: (dimensión, razón)`. El host clasifica la propuesta con valores **tipados**, no con claves: |
| — respuesta | Toda condición propuesta es (a) de la ranura abierta, o (b) de una ranura del marco y resuelve al **mismo** valor codificado. Se ejecuta `frame + respuesta`. |
| — búsqueda nueva completa | job `SEARCH` completo (tabla de §5.2). Se ejecuta **sólo lo nuevo**; el pendiente se cancela; nada se fusiona. |
| — cambia el marco | Cualquier otra cosa (ranura nueva, valor distinto en una ranura del marco, otro universo sin ser búsqueda completa). **0 llamadas**; se vuelve a preguntar nombrando la salida («escríbela completa» o reset); `asks += 1`; marco y pregunta intactos. |
| Caducidad | `expires_at_utc` = 30 min; o `asks` llega a 2 y la respuesta vuelve a cambiar el marco. Entonces se cancela y se dice. |
| Cancelación | reset; búsqueda nueva completa; cualquier turno que no sea respuesta ni re-pregunta (vida de un turno); turno de otro dueño. |
| La autoridad no resuelve la respuesta | Sigue pendiente (cuenta como re-pregunta). |

Esto reproduce las 11 filas de `r5:R5_REPAIR.md` §P2 (líneas 84-96): las cinco que cambian tema, comprador, año o
valor ⇒ re-pregunta; la que cambia a otro universo con consulta completa ⇒ ejecuta lo nuevo sin fusionar; «repite el
marco» y «sólo el período» ⇒ respuesta; la salida nombrada ⇒ búsqueda nueva. La diferencia con R5 es que la
comparación es por valor tipado en cada ranura, no por una lista finita de claves (limitación declarada en
`R5_REPAIR.md:207`).

No reemplaza `actor_pending`: es del dueño Actor (legacy) y queda fuera.

### 5.5 Referente al conjunto de resultados actual

```text
ResultSetRef(frozen):
  trace_id: str                  # registro cuya efectiva produjo el resultado
  interpretation_hash: str
  groups: tuple[GroupRef, ...]   # uno por petición ejecutada

GroupRef(frozen):
  request_id, dataset, plan_hash_query
  order_contract: str            # OrderValue codificado, o "presentational:date_desc"
  page_limit: int
  shown_ids: tuple[str, ...]     # identificadores ya mostrados, en orden
  next_offset: int | None        # None con filtro local por token (el cursor es len(shown_ids) sobre los miembros)
  total: int | None; total_quality: str
```

El referente es la **intensión**: la consulta aceptada y su plan. `shown_ids` existe para paginar sin solapes y para
exportar, no para filtrar.

| Uso | Cómo | LLM |
|---|---|---|
| «ver más» / paginación | Handler de protocolo `handle_foundation_page` (antes de `handle_pagination` en `orchestrator.py:410-417`), activo sólo si el último turno es Foundation con `result_ref`. Re-proyecta la efectiva guardada con el `reference_instant` **guardado**, exige `plan_hash` igual al del registro, pide la página siguiente bajo un permiso de página del guard y descarta ids ya mostrados. Con ancla dual pregunta cuál grupo (como hoy). Escribe un registro nuevo con la misma efectiva y el cursor avanzado. | no |
| Orden por texto («los de mayor valor») | Delta `SET ORDERING`: **nueva ejecución** con el orden en la consulta a la fuente; nunca ordenar en memoria la página visible (INV-C0-12 se extiende). Cursor a cero. | sí |
| Botón `[Mayor valor]` | Evento → `SET ORDERING` con un `OrderValue` de una tabla cerrada de acciones (origen `VIEW_ACTION`). | no |
| Botón `[Elegir período]` | Evento → abre una aclaración pendiente de ranura TIME; la respuesta entra por texto e intérprete. | sí, en la respuesta |
| Botones de vista (F) | `handle_foundation_view_action` lee este mismo almacén; `USER_OVERRIDE` produce una efectiva nueva. | no |
| Export | Exporta lo mostrado (`shown_ids` y sus filas del turno), como hoy; con dos grupos sigue bloqueado (`foundation_presentation.py:32-39`). | no |
| Pronombre de conjunto («los», «esos», «aquellos»; 5/65) | `referent_evidence` ⇒ el delta se aplica a la consulta, no a las filas visibles. | sí |

Fuera, explícitamente: referencia a una fila («el segundo», «ese contrato»), subconjunto de lo visible («de esos
diez…»), comparar con un resultado anterior, deshacer («vuelve a la anterior»), anáfora a una entidad de otra
capacidad («sus proveedores»: `N013`, operación no-SEARCH), guardar como Radar (`N076`), exportar «todo» (`N089`),
`MULTI` con historia (`N101`). En M2a terminan en aclaración o mensaje con código cerrado
(`REFERENT_EXTENSIONAL_UNSUPPORTED`, o la fila de C0 §5.3.2 que corresponda).

### 5.6 Qué necesita M2a del chequeo de residuo/cobertura (no se diseña aquí)

1. Que el léxico único de palabras funcionales incluya los conectores conversacionales («y», «ahora», «sólo»,
   «también», «ok») y los pronombres de conjunto; hoy serían residuo en todo fragmento.
2. Que un span citado por una directiva `KEEP`/`REMOVE` o por `referent_evidence` cuente como **cubierto**.
3. Que la cobertura se calcule sólo sobre el texto del turno actual; lo arrastrado no es ocurrencia del turno.
4. Una medición separada sobre el Gold M2A: los fragmentos tienen 1–6 tokens y sus proporciones de residuo no son
   comparables con las de una consulta completa.
5. Mientras no sea gate duro, M2a no depende de él: la guarda «fragmento nunca solo» es estructural. Cuando lo sea,
   residuo material en un turno delta ⇒ aclarar; nunca aplicar el delta parcial.

---

## 6. Invariantes

### 6.1 Preservados (no se relajan)

P-1…P-7 e INV-C0-1…12 de C0 §3. En particular INV-C0-3 (una autoridad que no resuelve nunca desvía a legacy),
INV-C0-7/8 (autoridades puras sobre la ventana) e INV-C0-12 (ORDER semántico prohíbe el fallback H4).
P-5 se reformula sin debilitarse: READY no autoriza llamar; el guard valida el manifiesto **y el enlace por origen**.

### 6.2 Introducidos

| Id | Invariante | Verificación (propiedad / mutante que debe morir) |
|---|---|---|
| INV-M2A-1 | **Cierre.** `compose(ancla, delta)` devuelve una `AcceptedInterpretation` que pasa la misma validación que la de un primer turno, o un `DeltaRefusal` de código cerrado. Nunca un estado parcial. | Hypothesis: anclas × deltas generados. Mutantes: aplicar la primera operación y fallar en la segunda; código libre. |
| INV-M2A-2 | **Identidad de lo no tocado.** Para toda ranura sin operación, la condición resultante es byte-idéntica a la del ancla (valor, autoridad, políticas, origen). | Propiedad. Mutantes: re-resolver lo arrastrado; perder una condición; cambiar el origen. |
| INV-M2A-3 | **Sólo sobre lo ejecutado.** `compose` rechaza un ancla no ejecutada, vencida, de otro chat, de esquema desconocido o ilegible. | Un test por causa; mutante por causa. |
| INV-M2A-4 | **Nada cruza a legacy.** Con ancla Foundation, un turno delta, de página o de aclaración hace 0 llamadas a `handle_normal_flow` y a `QueryRouter.parse`, y su `Turn.parsed_params` es `{}`. | Espías en ambos; mutante «puente» (escribir params) y mutante «entrega a `LEGACY_FOLLOWUP` con ancla Foundation». |
| INV-M2A-5 | **Un fragmento nunca se ejecuta solo.** Con ancla vigente, una propuesta `SEARCH` sin sujeto y sin condición de contenido produce 0 llamadas a la fuente. | Propiedad sobre propuestas generadas; mutante que quita la guarda. |
| INV-M2A-6 | **El primer turno no cambia.** Proyectar `from_acceptance(a)` da el mismo SoQL, cláusulas, políticas y disposiciones que `project_accepted(a)` de la base; sólo cambia `interpretation_hash`. | Replay del corpus de regresión + las 620 capturas del productor (el método de C1.2 §4). |
| INV-M2A-7 | **El valor nuevo no depende del ancla.** El veredicto de la autoridad para una condición del delta es el mismo con cualquier ancla (incluida ninguna). | Propiedad: misma ventana, anclas distintas. Mutante: pasar el ancla a la autoridad. |
| INV-M2A-8 | **Sin híbridos.** Tras una aclaración pendiente, el conjunto ejecutado es `marco + respuesta` o `sólo lo nuevo`; cualquier otro caso hace 0 llamadas. | Las 11 filas de R5 §P2 como corpus + propiedad. Mutantes AN-1…AN-14 de R5 reexpresados sobre ranuras. |
| INV-M2A-9 | **Registro inmutable.** Append-only; un `trace_id` repetido es error; `record_sha256` se verifica al leer; cola ilegible ⇒ continuidad inválida. | Mutantes: sobrescribir; omitir la línea corrupta y seguir; no verificar el digest. |
| INV-M2A-10 | **Paginar es el mismo plan.** Un turno de página no llama al LLM, usa el `reference_instant` guardado, su `plan_hash` es el del registro y sus ids son disjuntos de `shown_ids`. | Test con productor que lanza si se le llama; mutantes: reloj nuevo; offset inventado; solape. |
| INV-M2A-11 | **Vista re-decidida y anunciada.** Tras un delta, con sujeto `UNSPECIFIED` y sin `USER_OVERRIDE`, la vista es `decide_view` del estado compuesto; si difiere de la del ancla, la respuesta lo anuncia. | Test por regla PV1–PV6 cruzada con cada SET; mutante «arrastra la vista». |
| INV-M2A-12 | **Eco completo.** La respuesta de todo delta ejecutado lista cada condición resultante, generada desde valores tipados, distinguiendo cambiada y arrastrada. | Test de presentación; mutante que omite las arrastradas. |
| INV-M2A-13 | **La tarjeta no lleva valores.** Toda tarjeta serializada usa sólo miembros de enumerados cerrados; no contiene subcadena alguna de textos ni de valores para mostrar. | Propiedad sobre registros generados; mutante que añade el nombre del comprador. |
| INV-M2A-14 | **Un origen por condición.** Toda `AcceptedCondition` tiene exactamente un origen verificable: span enlazado al texto de su turno, o evento de protocolo emitido por el host. | Validación de contrato + mutante «origen vacío». |
| INV-M2A-15 | **Barrera.** Ningún lector legacy toma como referente un turno anterior al último turno Foundation. | Test por lector de la tabla de §5.1 (radar, `last_successful_turn`). |

---

## 7. Contratos entre componentes

| Productor → consumidor | Contrato |
|---|---|
| Almacén → host | `load_anchor(chat_id, last_turn) → Anchor | AnchorInvalid(code)`; nunca `None` silencioso. |
| Host → productor | `LinguisticProducer.produce(raw, state_card)`; la tarjeta forma parte del perfil v5 (misma en el proveedor de failover). |
| Productor → host | IR v2 + `ProposedSlotDirective`; un delta es job `FOLLOWUP_DELTA` o job con `referent_evidence`. |
| Host → autoridades | Sin cambio (C1.1). |
| Host → `compose` | `(Anchor, InterpretationAcceptance del turno) → AcceptedInterpretation | DeltaRefusal`. Puro. |
| `compose` → proyección | `AcceptedInterpretation`. La proyección no conoce deltas ni anclas. |
| Guard | Enlace por origen + manifiesto, como hoy. Permiso de página: una consulta por grupo y turno, mismo `plan_hash`. |
| Runtime → almacén | Un `AcceptedTurnRecord` por turno Foundation, también en los terminales sin ejecución. |
| Protocolo (página, opción, vista, quitar filtro) → host | Evento tipado con `pending_id`/`trace_id`; valor siempre producido por el host. |
| `decide_operation` (C1.4) | La fila `FOLLOWUP_DELTA` pasa a depender del **dueño del ancla**: Foundation ⇒ M2a; dueño legacy ⇒ `LEGACY_FOLLOWUP`. |

---

## 8. Plan

### 8.1 Gold de follow-ups antes del código (M2A-0; HERA + etiquetadores; no toca `app/`)

Esquema `m2a-followup-gold/1` (un registro por línea; validador en `scripts/audits/m1_search/`):

```json
{
  "gold_id": "F0001",
  "slice": "REAL_FRAGMENT | SYNTHETIC | MINIMAL_PAIR | PENDING | PROTOCOL",
  "fragment_ref": "R0092",
  "text": "…",
  "anchor": {
    "state": "EXECUTED | EXECUTED_EMPTY | NOT_EXECUTED | PENDING | EXPIRED | FOREIGN_OWNER | NONE",
    "accepted": { "requests": [ … forma del Gold M1: universe, view, conditions, logic … ], "product_view": null },
    "pending": { "dimension": "BUYER", "reason": "AMBIGUOUS_HOST_DIMENSION:BUYER", "options": 3 }
  },
  "delta": [
    {"op": "SET | REMOVE | KEEP | SET_ORDERING | SET_UNIVERSE | ANSWER",
     "dimension": "TIME", "operator": "during_year", "polarity": "POSITIVE",
     "quote": "…", "value_hint": "…", "level": null}
  ],
  "referent": {"kind": "NONE | RESULT_SET | CONDITION", "quote": null},
  "expected": {
    "disposition": "EXECUTE_DELTA | NEW_SEARCH | CLARIFY | REASK | HANDOFF_LEGACY",
    "reason": null,
    "resulting": { "requests": [ … ], "product_view": null },
    "carried": ["c1", "c3"], "replaced": ["c2"], "removed": [], "view_changed": false
  },
  "ambiguity": "CLEAR | AMBIGUOUS", "alternatives": [], "notes": "…"
}
```

Contenido mínimo:

| Bloque | Contenido | n aprox. |
|---|---|---|
| REAL_FRAGMENT | Los 65 fragmentos REAL + los 4 `SEARCH·NEEDS_HISTORY`, cada uno con ≥1 **ancla sintética** escrita por el etiquetador (el texto REAL sigue fuera de git; el ancla y la etiqueta pueden ir en git si no citan el fragmento) | 69–100 |
| MINIMAL_PAIR | Mismo fragmento con ancla que **tiene** y que **no tiene** la ranura; ancla dual; ancla `UNSPECIFIED` cuya vista cambia con el delta; ancla con ORDER+CARDINALITY; gemelos «fragmento / consulta completa con historia» (`P159`–`P164`) | 40 |
| Anclas inválidas | no ejecutada, vacía, vencida, dueño legacy, ilegible | 15 |
| PENDING | Las 11 filas de R5 §P2 reexpresadas + opción por evento + respuesta que no resuelve + segundo cambio de marco | 20 |
| PROTOCOL | página tras búsqueda, tras delta, tras cambio de orden, dual; botón de orden; quitar filtro | 15 |
| Decisiones Q1–Q4 | Un par por decisión, etiquetado **después** de que producto decida | 8 |

Doble etiquetado ciego, acuerdo por campo, adjudicación y sello, igual que el Gold M1. Partición DEV/TEST fijada
antes de tocar el prompt (`sha256(gold_id + "m2a-split-v1") mod 4`).

### 8.2 Pasos (un escritor del núcleo; cada paso es un commit revertible)

| Paso | Contenido | Dueño y archivos | Depende de | Gates |
|---|---|---|---|---|
| **M2A-0** | Gold M2A + validador + sello + partición | HERA: `scripts/audits/m1_search/gold_schema_m2a.py`, `docs/audits/m1_search_foundation/gold/followup*.jsonl` | Q1–Q4 decididas (o sus pares aparcados) | validador 0 errores; acuerdo medido; sello |
| **M2A-1** | `foundation_accepted.py` (tipos, `from_acceptance`, lector); `ProposedSlotDirective` en `interpretation_contract.py` + `foundation_acceptance_loader.py`; proyección y guard sobre `AcceptedInterpretation`. Sin cambio de comportamiento | NÚCLEO | C1.2 integrado. **Antes de congelar el wire v5 (C1.3)**, para que la directiva entre en el esquema 2 sin subir versión | INV-M2A-6 (corpus + 620); INV-M2A-14; `tests/test_foundation_*.py` verdes; mutantes |
| **M2A-2** | `foundation_accepted_store.py`; `_record` escribe el registro; línea de `clear()` | NÚCLEO (`foundation_*`) + una línea en `conversation_store.py:283-285` (adyacente declarado) | M2A-1 | INV-M2A-9; test de concurrencia (dos escrituras, ancla cambiada); comprobación de que traza y ledger no ganan campos con texto |
| **M2A-3** | `foundation_delta.py` (ranuras, `compose`, rechazos, consistencia) | NÚCLEO | M2A-1; Gold M2A | INV-M2A-1/2/3 con **IR oráculo** sobre el Gold M2A: 100 % de disposición y de estado resultante |
| **M2A-4** | `foundation_state_card.py`; ancla (`load_anchor`); fila `FOLLOWUP_DELTA` por dueño del ancla en `foundation_operation_owners.py`; guarda «fragmento nunca solo»; eco; sección de estado y de delta del prompt v5; retirar `continues_foundation_turn` **sólo en el perfil v5** | NÚCLEO (`foundation_runtime.py`, `foundation_route_owner.py`, `foundation_operation_host.py`, `foundation_presentation.py`, `foundation_messages.py`, `foundation_prompts_v5.py`, `foundation_producers.py` hasta su traspaso a G) | M2A-2, M2A-3; C1.3, C1.4; E-* de cada clase (abajo) | INV-M2A-4/5/7/11/12/13; gates por clase de §8.3 con productor real |
| **M2A-5** | `foundation_pending.py`; `app/core/handlers/foundation_option.py` (evento) conectado en `orchestrator.py:410-417` | NÚCLEO (el handler puede ser de F si el orquestador lo prefiere; un solo dueño) | M2A-4 | INV-M2A-8; bloque PENDING del Gold |
| **M2A-6** | `ResultSetRef`; `app/core/handlers/foundation_page.py`; permiso de página en `foundation_guard.py`; eventos `[Mayor valor]`, `[Elegir período]`, quitar filtro | NÚCLEO (guard) + **F** (handlers y botones) | M2A-4; E-ORDER | INV-M2A-10; bloque PROTOCOL |
| **M2A-7** | Sólo si Q8 = Frame encendido: `ContinuationOwner.FOUNDATION`, reglas de admisión, `delegated_contract` | Dueño de `app/core/workflow/` (**no** el núcleo): `continuation.py`, `admission_rules.py`, `turn_publication.py`, `tabular_capability.py` | M2A-6 | tests de admisión existentes verdes; test de rollback (versión anterior lee el sobre nuevo y aclara, no ejecuta) |
| **M2A-8** | Barrera en lectores legacy | ARES no-núcleo: `app/core/workflow/handlers/radar.py` y el consumidor de `last_successful_turn` que el inventario señale | tabla de lectores del otro agente | INV-M2A-15 |
| **M2A-9** | Freeze | HERA | todo lo anterior | suite global pareada por nodeid; semgrep delta con `--base`; `candidate freeze`; ZEUS fresco en paralelo |

Paralelizable: M2A-0 ‖ M2A-1; M2A-8 ‖ cualquiera; M2A-7 ‖ M2A-5. El resto es serie en el núcleo.

Dependencias por clase de delta (una clase no puede pasar su gate hasta que su autoridad esté `ACTIVE`):

| Clase | Requiere |
|---|---|
| TIME rango/cota/mes (8 de 17), GEOGRAPHY municipio/región (5 de 8), VALUE inclusivos y rangos (≥7) | E-TIME, E-GEO, E-VALUE |
| ORDER (6+) y botón `[Mayor valor]` | E-ORDER+CARDINALITY (v4.2 no tiene ORDER) |
| Universo `UNSPECIFIED`, re-decisión de vista | F (Product View) integrado |
| STATE, MODALITY | E-STATE/MOD |
| Caída del proveedor en un turno delta | G: «no pude interpretar ahora», ancla intacta, tarjeta idéntica en el proveedor B |
| Prompt | C1.6: la sección de estado entra **antes** de congelar el SHA de v5. Si v5 ya está congelado, es una versión nueva del prompt con su propia corrida ACTIVATE sobre TEST (HERA) |

### 8.3 Gates de M2a (tolerancia cero, peor de ≥2 pasadas, Gold M2A completo)

| Gate | Exigencia |
|---|---|
| Fragmento ejecutado solo (pérdida silenciosa del ancla) | 0 |
| Híbrido tras aclaración | 0 |
| Condición arrastrada cambiada o perdida sin operación | 0 |
| Delta aplicado sobre ancla inválida | 0 |
| Turno con ancla Foundation que llega a legacy | 0 |
| Búsqueda nueva completa tratada como delta (arrastre indebido) | 0 en casos `CLEAR` |
| Vista cambiada sin anuncio | 0 |
| Por clase de delta: casos que el follow-up legacy acierta hoy y M2a no (ni ejecuta bien ni aclara con la razón Gold) | 0 |
| IR oráculo por el host | 100 % de disposición y estado resultante |

### 8.4 Precondición de CUTOVER que M2a añade a C0 §5.3.6

Sustituir el punto 5 («Historia liberada: los 64 `FOLLOWUP_DELTA` REAL entregados a legacy sin degradación») por:

> 5. **Conversation mínimo (M2a).** (a) Todo turno Foundation persiste su `AcceptedTurnRecord`; (b) los gates de
> §8.3 en 0 sobre el Gold M2A sellado, ≥2 pasadas; (c) con ancla Foundation, 0 turnos llegan a `handle_normal_flow`
> (INV-M2A-4) y con ancla de un dueño legacy la entrega cumple INV-C0-4/6; (d) paginación y selección de opción sobre
> resultados Foundation funcionan en la configuración de CUTOVER (Frame encendido o apagado, Q8); (e) los lectores
> legacy de `Turn.parsed_params` cumplen R-BARRERA; (f) `v5_previous` lee `m2a-record/1` o lo trata como ancla
> inválida.

Y en el borrado del mismo commit: `continues_foundation_turn` y `last_foundation_turn` se borran porque los reemplaza
el ancla; **no** se borra `followup_engine` (sigue sirviendo a dueños legacy; muere en M2b/M3).

---

## 9. Mapa archivo → dueño

| Archivo | Dueño | Nota |
|---|---|---|
| `app/core/foundation_accepted.py`, `foundation_accepted_store.py`, `foundation_delta.py`, `foundation_state_card.py`, `foundation_pending.py` (nuevos) | NÚCLEO | una responsabilidad cada uno |
| `app/core/interpretation_contract.py`, `foundation_acceptance_loader.py`, `foundation_projection.py`, `foundation_guard.py`, `foundation_runtime.py`, `foundation_route_owner.py`, `foundation_operation_host.py`, `foundation_operation_owners.py`, `foundation_presentation.py`, `foundation_messages.py`, `foundation_prompts_v5.py` | NÚCLEO | — |
| `app/core/foundation_authority_contract.py`, `foundation_authority_values.py`, `foundation_evidence_window.py` | **nadie** | congelados (C1.1); M2a no los cambia |
| `app/core/foundation_prompts.py` | **nadie** | byte-congelado |
| `app/core/conversation_store.py` (una línea en `clear`), `app/core/orchestrator.py` (tupla de handlers) | NÚCLEO | adyacentes declarados |
| `app/core/handlers/foundation_page.py`, `foundation_option.py`, botones y anuncio | F | el núcleo los conecta |
| `app/core/workflow/continuation.py`, `admission_rules.py`, `turn_publication.py`, `tabular_capability.py` | dueño de `workflow/` | sólo M2A-7 |
| `app/core/workflow/handlers/radar.py` | ARES no-núcleo | M2A-8 |
| `app/core/followup_engine.py`, `followup_mutations.py`, `followup_control_tokens.py`, `workflow/handlers/pagination.py`, `pagination_buffer.py` | **nadie en M2a** | no se tocan |
| `scripts/audits/m1_search/*`, `docs/audits/m1_search_foundation/gold/*` | HERA | — |
| `tests/test_foundation_accepted*.py`, `test_foundation_delta*.py`, `test_foundation_pending*.py` | NÚCLEO | — |

---

## 10. Irreversibilidad y fronteras de confianza

| Cambio | Tipo | Mitigación |
|---|---|---|
| El intérprete recibe estado | Frontera de confianza | La tarjeta sólo tiene enumerados generados por el host (INV-M2A-13): no entra texto ajeno ni valores. Lo que proponga sigue sin ser ejecutable sin autoridad. |
| El LLM decide «delta o búsqueda nueva» | Frontera de confianza | Guarda estructural del host (INV-M2A-5), ancla verificada (INV-M2A-3) y eco (INV-M2A-12). El error residual es «arrastre indebido visible», no pérdida silenciosa. |
| Almacén nuevo con datos personales (citas del usuario, `ActorValue` con documento de persona natural, compradores) | Persistencia | Mismo directorio, misma retención y mismo borrado que el historial, que ya guarda `user_query`; fuera de git; el ledger y la traza siguen con digests. Política ADR-060 para personas naturales. |
| Esquemas `m2a-record/1`, `m2a-accepted/1` en disco | Persistencia | Lector estricto; esquema desconocido ⇒ ancla inválida. Cambiar el esquema exige lector de la versión anterior o caducar las anclas (30 min de vida útil: el coste de caducar es bajo). |
| `interpretation_hash` cambia otra vez (digest de la efectiva) | Declarado | `plan_hash` no cambia para un primer turno (INV-M2A-6). |
| `ProposedSlotDirective` en la propuesta | Contrato del núcleo | Dentro del esquema 2 si entra antes de la primera activación de v5; si no, decisión del núcleo sobre subir versión. |
| `ContinuationOwner.FOUNDATION` (sólo M2A-7) | Enumerado persistido en el sobre | Una versión anterior lo lee como contrato ilegible ⇒ continuidad inválida ⇒ aclara en entradas con referente; el texto libre sigue. Probado en el gate de M2A-7. |
| Eventos de protocolo como origen de significado | Frontera de confianza | Token ligado a `pending_id`/`trace_id`, vigencia, y valor producido por el host; un token ajeno o caducado no ejecuta. |

---

## 11. Riesgos y rollback

| Riesgo | Señal | Respuesta |
|---|---|---|
| El productor emite `SEARCH` para un fragmento | Gate «fragmento solo» lo convierte en `DELTA_OR_NEW_AMBIGUOUS`; tasa alta de esa aclaración en DEV | ≤3 iteraciones del prompt; después STOP |
| El productor emite delta para una consulta completa | Gate «arrastre indebido» | Ídem; el eco y «quitar filtro» acotan el daño |
| Arrastre excesivo en `SEARCH` + `referent_evidence` (pronombre de entidad) | Casos `R0444`/`R0207` del Gold | Regla uniforme + eco; si el Gold M2A lo marca como error, esos casos pasan a aclarar |
| Dos mensajes simultáneos del mismo chat | Ancla cambiada al escribir | Aviso de contexto no conservado; `anchor_trace_id = None` |
| Cruce de medianoche con ventana temporal arrastrada | Fechas del eco distintas de «hoy» | El valor es el del turno de origen; el eco muestra fechas absolutas |
| Paginación con filtro local por token (tope de candidatos) | `TOKEN_CANDIDATES_TRUNCATED` | Página = siguiente tramo de miembros; si el tope trunca, se dice (como hoy) |
| Divergencia entre `turn_acceptance` y `effective` | INV-M2A-6 rojo | Bloquea M2A-1 |

**Rollback.** M2A-1…3 no cambian comportamiento. M2A-4…6: `foundation_v3` conserva el bloqueo actual; `legacy`
reproduce `main`. Tras el CUTOVER: `v5_previous` (T8). Los archivos `.accepted.jsonl` son inertes para cualquier
versión que no los lea.

---

## 12. Condiciones de STOP

1. Hace falta cambiar `foundation_authority_contract.py` (p. ej. pasar el valor anterior a la autoridad para heredar
   unidad o año) → METIS.
2. INV-M2A-6 rojo y no explicable por un defecto de `from_acceptance` → METIS.
3. Tras 3 iteraciones del prompt sobre DEV queda algún «fragmento ejecutado solo» o algún híbrido → NO_GO con informe;
   sin bajar gates.
4. Algún paso necesita escribir params legacy a partir del estado Foundation para satisfacer a un lector → STOP: es el
   puente descartado.
5. El Gold M2A muestra una clase ADD (estrechar) con n ≥ 5 `CLEAR`, o referentes extensionales frecuentes → revisar el
   álgebra antes de seguir (METIS).
6. El inventario de lectores de `Turn.parsed_params` contradice un supuesto de §5.1 de forma que un guard pequeño no
   arregla → METIS.
7. M2A-7 deja chats en `admission_rollback_incompatible` o sin salida distinta de reset → STOP.
8. Una sola pérdida silenciosa de ancla o un híbrido en dogfood con M2a activo → volver a `v5_previous` y avisar.
9. Cualquier decisión Q1–Q10 cambia una fila de §5.2 o §5.4 → actualizar este documento antes de seguir.

---

## 13. Non-goals

- Frame de Entity Profile, `ProfileConversationState`, `ContinuationContract` como modelo único (M2b).
- Anáforas entre capacidades («sus proveedores», «su perfil»), Suppliers, MULTI con historia.
- Listas pendientes generales (más de una ranura abierta) y `actor_pending`.
- ADD (estrechar con una segunda condición en la misma ranura), deltas sobre grupos OR/NOT, deshacer.
- Referentes extensionales (fila, subconjunto visible), comparación entre resultados.
- Guardar como Radar y exportar «todo» desde un resultado Foundation.
- Borrar `followup_engine`, `followup_mutations`, `followup_control_tokens`, `pagination_buffer` o `QueryFrame`.
- Diseñar el chequeo de residuo/cobertura o el léxico de palabras funcionales.
- Redactar la enmienda 2 de ADR-103 o asignar número de ADR.
- Unificar `Turn`, sobre y buffer de paginación en un modelo de estado.

---

## 14. Decisiones que no puedo tomar (producto), con recomendación

| Id | Pregunta exacta | Recomendación | Casos Gold |
|---|---|---|---|
| Q1 | Tras «más de 500 millones», un «menos de 2.000 millones» nuevo: ¿sustituye todo el filtro de valor o sólo el límite superior (dejando un rango)? | **Sustituye todo el filtro**, con eco; «entre X y Y» fija los dos. | 16 de valor; decisivo en 2–3 |
| Q2 | Un fragmento que sólo nombra un tema nuevo: ¿reemplaza el tema o estrecha el anterior? | **Reemplaza**; estrechar (ADD) queda fuera hasta que el Gold M2A lo pida. | 2 |
| Q3 | Un monto sin unidad en un follow-up («más de 300» tras hablar de millones): ¿heredar la unidad o preguntar? | **Preguntar con una opción de un toque** (aclaración pendiente). Heredar exige cambiar el contrato de autoridades. | 2 |
| Q4 | Un mes sin año en un follow-up: ¿año del reloj (regla de primer turno) o año de la condición temporal anterior? | Si ambos coinciden, ejecutar anunciando el año; **si difieren, preguntar con las dos opciones**. | 3 |
| Q5 | Con universo no especificado, añadir un período puede cambiar la vista (de oportunidades vigentes a procesos históricos): ¿re-decidir o conservar la vista del ancla? | **Re-decidir y anunciar**; la elección explícita del usuario (botón) se conserva. | lo mide el Gold M2A |
| Q6 | ¿Vigencia del ancla y de una aclaración pendiente? | **30 minutos** las dos (la ventana de hoy). | — |
| Q7 | «Los de mayor valor» tras mostrar 25 de 1.234: ¿los 25 visibles o toda la consulta? | **Toda la consulta** (nueva ejecución ordenada en la fuente). | 5 |
| Q8 | ¿El CUTOVER se hace con `SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED` encendido? | Si sí, **M2A-7 es obligatorio**; si no, se omite y queda como deuda para M2b. No puedo decidirlo: es configuración de producto. | 4 paginación + 13 selección (PROTOCOL) |
| Q9 | «Guarda esta búsqueda como radar» sobre un resultado Foundation en M2a | **Mensaje explícito «todavía no»** (y barrera para que no guarde una búsqueda vieja). | `N076` |
| Q10 | ¿Aceptar «un delta = conjunto atómico de operaciones, una por ranura» en lugar de «una condición»? | **Sí** (T1): si no, 10/65 aclaran. | 10 |

---

## 15. Qué haría cambiar la decisión

- **El productor no distingue delta de búsqueda nueva con la tarjeta de enumerados** (gate «fragmento solo» o
  «arrastre indebido» > 0 tras 3 iteraciones) → abrir la opción 4.4-C limitada: añadir al prompt el texto del turno
  anterior del usuario (nunca valores). Si tampoco basta, mantener el bloqueo y no hacer el CUTOVER; no añadir un
  detector de texto.
- **El Gold M2A muestra ADD frecuente o ranuras de VALUE por lado** → cambia la tabla de ranuras (datos), no la
  arquitectura.
- **El orquestador decide adelantar M2b** → la opción 4.3-B (sección del sobre) pasa a ser la correcta y el almacén
  M2a se convierte en lo que el contrato de continuación referencia; el tipo `AcceptedInterpretation` no cambia.
- **Las autoridades necesitan contexto conversacional con frecuencia** (unidad, año, «el mismo comprador») → revisar
  `AuthorityContext` con METIS antes de que haya más consumidores; hoy son 5/65.
- **El inventario de lectores legacy encuentra un consumidor crítico de `parsed_params` tras un turno Foundation** que
  no admite barrera → ese consumidor migra a leer el registro M2a (lector nuevo), nunca al revés.

---

## 16. Qué debe ir a la enmienda 2 de ADR-103 (lista corta; no la redacto)

1. **M2a es precondición de CUTOVER**: texto de §8.4 como nuevo punto 5 de C0 §5.3.6 (y corregir «64» → 65).
2. **Invariante reformulada con origen**: toda condición aceptada tiene exactamente un origen, «span con dueño en su
   turno **o evento de protocolo acreditado por el host**» (T7, INV-M2A-14).
3. **Arrastre**: una condición arrastrada conserva identidad de valor y no se re-resuelve; las políticas de petición
   se re-deciden sobre el estado compuesto (INV-M2A-2, regla 4 de §5.2).
4. **Ventana y ocurrencias por turno**: la ventana de evidencia y el registro de ocurrencias cubren sólo el texto del
   turno actual; el estado nunca entra en una autoridad (INV-M2A-7).
5. **Cobertura**: un span citado por `KEEP`/`REMOVE`/`referent_evidence` cuenta como cubierto; conectores
   conversacionales en el léxico único (§5.6).
6. **Rollback**: `v5_previous` debe leer el esquema del almacén vigente o tratarlo como ancla inválida; nunca
   ejecutar sobre un registro que no entiende (T8).
7. **Fila `FOLLOWUP_DELTA` de la tabla de dueños**: el dueño lo decide el **dueño del ancla**, no el texto.
8. **Barrera**: un turno Foundation es opaco y es barrera para todo lector legacy (R-BARRERA).
9. **Hashes**: `interpretation_hash` pasa a ser el digest de la aceptación efectiva.

---

## Apéndice A — ids del Gold REAL por clase (clasificación METIS, no etiqueta Gold)

| Clase | Ids |
|---|---|
| TIME | R0092, R0106, R0197, R0276, R0303, R0304, R0312, R0313, R0314, R0409, R0431, R0432, R0450, R0556 |
| TIME sólo mes | R0496, R0497, R0498 |
| VALUE una cota | R0107, R0187, R0205, R0223, R0295, R0301, R0308, R0309, R0310, R0464 |
| VALUE rango | R0180, R0192, R0282, R0311, R0375, R0529 |
| VALUE sin unidad | R0531, R0532 |
| GEOGRAPHY | R0075, R0093, R0201, R0233, R0274, R0281, R0306, R0321 |
| ORDER | R0081, R0083, R0087, R0089, R0515 |
| ORDER + referencia a condición arrastrada | R0099 |
| STATE | R0189, R0483 |
| MODALITY | R0459 (AMBIGUOUS), R0561 |
| TOPIC | R0286, R0484 |
| Dos dimensiones | R0079, R0336 |
| Universo + STATE | R0094 |
| BUYER + quitar VALUE | R0202 |
| Respuesta a aclaración (ACTOR) | R0435 |
| Ancla no-SEARCH | R0319, R0320, R0418, R0441, R0560 |
| Pronombre de conjunto | R0079, R0099, R0189, R0192, R0223 |
| `NEEDS_HISTORY` | SEARCH: R0091, R0095, R0207, R0444 · RANK_SUPPLIERS: R0398 · GROUP_BY: R0564 |

## Apéndice B — límites de este documento

- No ejecuté tests, runtime ni el productor; todo lo marcado INFERENCIA debe confirmarse con un test en su paso.
- La clasificación de §1.1 es de un solo lector y sin turno anterior: es insumo para M2A-0, no Gold.
- No leí el estado legacy de los 4 `SEARCH·NEEDS_HISTORY` en la matriz A3; si legacy los acierta, entran en el gate
  `L-only-OK` de su clase SEARCH y M2a debe cubrirlos (por eso están en el bloque REAL_FRAGMENT de §8.1).
- El conteo por tipo de `PROTOCOL` es heurístico.
- `secoppal-m1-int-par` está bajo una suite global y un ZEUS: sólo leí dos archivos; no afirmo nada de su estado.

# M1-C0 — Diseño del SEARCH SPINE V5 de Foundation

Fecha: 2026-09-30. Autor: METIS (diseño, sólo lectura de código). Worktree `secoppal-m1`, rama
`mission/m1-search-foundation-phase-a`, base `main` @ `4ec99bc1`, con M1-H0 (`fb34c4aa`) supuesto
integrado. Autoridad: roadmap `docs/planes/2026-09-30_FOUNDATION_SINGLE_INTERPRETER_ROADMAP.md`
(invariantes 1–6), contrato `M1_0_SEARCH_CONTRACT.md` v1.4 (§1–§14, D1–D6), ADR-101, ADR-102,
evidencia de Fase A (A2–A5, `PHASE_A_CLOSE.md`).

No implementa nada. Las líneas citadas son de `4ec99bc1` salvo que diga «+H0».

---

## 0. Precondición: sí hay decisión de arquitectura

| Eje | Por qué es arquitectura y no trabajo de ARES |
|---|---|
| Contrato entre componentes | La IR cambia de forma (operación por turno, sujeto sin universo, nivel geográfico, operadores cerrados); productor, decoder, host, proyección, ledger y harness de evaluación la consumen. |
| Frontera de confianza | Hoy el LLM no decide a quién pertenece un turno (lo decide una regex). Con v5 la **operación propuesta por el LLM gobierna el enrutamiento**. Hay que acotar qué puede hacer esa afirmación. |
| Persistencia / irreversibilidad | `SCHEMA_VERSION 2` cambia `interpretation_hash`, `plan_hash` y los registros guardados en trazas. |
| Concurrencia de trabajo | Seis carriles D-* escriben autoridades en paralelo con el núcleo; sin un contrato congelado, el núcleo se convierte en el cuello o en el sitio del conflicto. |

---

## 1. DECISIÓN (resumen)

1. **IR v2 en memoria, wire v1 elevado.** Una sola clase de propuesta (`SCHEMA_VERSION = 2`).
   El wire de v4.2 se decodifica como hoy y se eleva a v2 con una función pura sin pérdida
   (`lift_v1`). El wire v5 tiene su propio decoder. **El perfil elige el wire; el decoder nunca lo
   adivina a partir del payload.**
2. **La operación es un `job` de primer nivel** (`jobs[]`, uno por operación; `MULTI` es una propiedad
   derivada, no un valor del wire). Sólo `SEARCH` se ejecuta en M1. Cualquier otra operación pasa a
   **una tabla cerrada de dueños**: o se entrega a un dueño vigente que *reclama* el turno con su
   propia admisión, o termina en un mensaje explícito. **Nunca hay un segundo salto, y nunca termina
   en el SEARCH tabular legacy.**
3. **Regla de interpretación única ante una entrega (handoff):** Foundation sólo transfiere la
   *operación* (y el dueño). Sus condiciones no se mezclan con nada; van al ledger como sombra. El
   dueño legacy es el único intérprete de los valores ejecutables de ese turno, y su enrutamiento es
   auxiliar monótono: sólo puede ejecutar dentro del conjunto permitido para esa operación. Si sale
   de él, es una violación de conformidad que se mide.
4. **Autoridades como funciones puras sobre una ventana de evidencia**: `DimensionEvidence` (citas +
   operador + polaridad + nivel + K=4 tokens plegados a cada lado) → `AuthorityVerdict`
   (RESOLVED/CLARIFY/BLOCK + valor tipado + recibo). El contexto fuera de la cita **sólo puede
   retirar autoridad**. Los carriles D-* las escriben en `app/core/host_authorities/<dim>.py` contra
   un contrato y un kit de conformidad que el núcleo congela primero (C1.1). El núcleo las enchufa
   con un registro de una línea por dimensión y madurez `SHADOW → ACTIVE`.
5. **Productor v5 = archivo de prompt nuevo + perfil nuevo** `foundation_v5` (v4.2 intacto, perfil
   `foundation_v3` seleccionable). Lo normativo se cierra en el host antes de escribir el prompt; un
   conjunto de mutantes de «prompt amnésico» lo verifica. Se evalúa sobre una partición TEST del Gold
   que el autor del prompt no ve, con ≥2 pasadas, más el held-out DA-6 a cargo de un custodio.
6. **Product View Policy**: módulo puro fuera del núcleo, que corre después de admitir las condiciones
   y antes de proyectar. Devuelve un `HostViewDecision` por petición `UNSPECIFIED`, registrado en la
   aceptación, y siempre se anuncia.
7. **C1 cambia lo mínimo en producto**: la puerta léxica y los vetos previos se mantienen; el cambio
   visible es que los no-SEARCH que hoy cruzan la puerta (6/58) dejan de secuestrarse. El borrado de
   la puerta es M1-I, con las precondiciones del §5.3.6.

Requiere **una ADR nueva** (número = máximo global de todos los worktrees + 1; verificarlo antes de
escribir), que enmiende ADR-102 §2/§4 (de un productor único a perfiles cerrados) y registre el
conflicto entre ADR-102 §9 y D1 (§2).

---

## 2. Decisiones vigentes respetadas y tensiones

| Vigente | Cómo lo respeta este diseño | Tensión |
|---|---|---|
| Invariante 1 (una interpretación autoritativa) | Una interpretación gobierna lo ejecutable: Foundation en SEARCH, el dueño en una entrega. Ver §5.3.4. | Natural Alpha `alpha` ya es un segundo LLM antes de Foundation (preexistente; non-goal, M3). |
| Invariante 2 (lo auxiliar es monótono) | El contexto de la ventana, la reclamación del dueño y la conformidad sólo retiran autoridad. | — |
| Invariante 3 (el LLM no entrega valores) | Se quitan `value_hint` y `limit_n` del wire; la cardinalidad es una cita que resuelve el host. | — |
| Invariante 4 (sin fallback silencioso, ADR-069) | Las operaciones sin dueño terminan en mensaje explícito, no en tabular legacy. | Cambio visible: es decisión de producto P1 (§11). |
| Invariante 5 (un dueño, transferencia con borrado) | Enrutamiento por estructura declarada (operación), nunca por el resultado de una resolución. Cada paso E borra la rama del host que reemplaza. | — |
| Invariante 6 (nadie relee el texto crudo) | Las autoridades sólo ven la ventana. | Las reclamaciones de los dueños legacy (§5.3.3) **sí** releen el texto: deuda declarada que muere en M3, la misma que hoy tiene `specialized_owner`. |
| ADR-101 (H7) | H7 sigue siendo obligatoria en todo perfil; con `UNSPECIFIED` no decide (autoridad `NOT_APPLICABLE`). | — |
| ADR-102 §2/§4 «productor único, sin selector» | Los perfiles son paquetes cerrados (productor+wire+H7+host). No hay selector de productor suelto. | **Enmienda necesaria**: `secoppal_foundation_mode ∈ {legacy, foundation_v3, foundation_v5}`. El test `test_foundation_is_off_by_default_and_has_one_profile` se reemplaza por uno de conjunto cerrado (cambio declarado; no se relaja). |
| ADR-102 §9 (búsqueda básica rotulada ante `PROVIDER_ERROR`) vs D1 («sin legacy») | C1 no toca el fallback; G lo resuelve. | D1 (2026-09-30) es posterior y contradice §9. La ADR nueva debe decir cuál rige en `foundation_v5`. Recomendación: D1. |
| Prompt v4.2 congelado por SHA | `foundation_prompts.py` no se toca: ningún escritor es dueño de ese archivo. | — |
| M1-H0 | `alias_owners` y la cobertura TIME se vuelven la base de las autoridades BUYER y TIME (D-BUYER y D-TIME las absorben). | `foundation_time_coverage.py` lee `raw`; D-TIME lo migra a la ventana. |

---

## 3. Invariantes

### 3.1 Preservados (verificados por tests existentes; no se relajan)

- P-1 Prompt v4.2 byte-congelado (`tests/test_foundation_prompts.py:76-79`).
- P-2 `universe_kind` sin default en ningún runtime (`test_foundation_promotion_boundary.py:37`).
- P-3 H7 inseparable de todo perfil Foundation (`:54`).
- P-4 La proyección no lee el turno crudo ni ejecuta (`foundation_projection.py:398`).
- P-5 READY no autoriza llamar a la fuente; el guard valida el manifiesto (`foundation_guard.py:41-46`).
- P-6 G1/G2/T4: toda cita se lee y la ocurrencia del universo no es tema (`foundation_evidence.py`).
- P-7 Ledger sin texto libre (`turn_ledger.py` `_clean`).

### 3.2 Introducidos (cada uno con su verificación)

| Id | Invariante | Verificación |
|---|---|---|
| INV-C0-1 | **El perfil fija el wire.** Ningún decoder infiere la versión del payload; un payload v1 en el decoder v2 (y al revés) es `INVALID_PROPOSAL`. | Test cruzado de ambos decoders; mutante «sniffing» muere. |
| INV-C0-2 | **`lift_v1` no pierde nada**: para todo payload v1 válido, proyectar `lift_v1(p)` produce el mismo SoQL y las mismas disposiciones que hoy (sólo cambian los hashes). | Replay del corpus `tests/fixtures/foundation_v3_regression_corpus.yaml` + capturas DA-6 (offline). |
| INV-C0-3 | **Enrutamiento por estructura, no por resultado.** La decisión «Foundation ejecuta / entrega / mensaje» depende sólo de `jobs` (operación, aspecto, `referent_evidence`) de la propuesta. Que una autoridad no resuelva **nunca** desvía el turno a legacy: aclara o bloquea. | Propiedad: para toda propuesta SEARCH, cambiar los veredictos de autoridad nunca produce `HANDOFF`. |
| INV-C0-4 | **Una entrega transfiere sólo la operación.** En `HANDOFF` el `ctx` y los `deps` que recibe el dueño son idénticos (igualdad profunda) a los de un turno sin Foundation, y antes de la entrega Foundation hizo 0 llamadas a la fuente. | Test con productor stub por operación; `source_calls_snapshot()` vacío antes del dueño. |
| INV-C0-5 | **Sin segundo salto.** Una operación no-SEARCH se resuelve en su dueño de tabla o en un terminal explícito; nunca en `handle_normal_flow` tabular salvo que la tabla lo permita (`FOLLOWUP_DELTA`, D6). | Test exhaustivo sobre `Operation` × reclamación del dueño (verdadera/falsa). |
| INV-C0-6 | **Conformidad de la entrega.** Dueño ejecutado ∈ `allowed_executors(op)`. En C1 se mide (ledger); para M1-I la cifra debe ser 0 en la sonda Gold. | Sonda `boundary_probe` extendida; fila `CAPABILITY` del ledger. |
| INV-C0-7 | **Monotonía de la ventana.** Para toda autoridad: si el veredicto con contexto es RESOLVED(v), con el contexto vaciado también es RESOLVED(v). El contexto sólo puede convertir RESOLVED en CLARIFY/BLOCK. | Propiedad del kit de conformidad (Hypothesis) para cada autoridad. |
| INV-C0-8 | **Las autoridades son puras**: sin red, sin reloj (el instante se inyecta), sin `get_settings()`, sin texto crudo, deterministas. Los índices de sólo lectura se inyectan por constructor. | Kit: sockets bloqueados, `date.today` parcheado para lanzar, dos llamadas producen el mismo veredicto. |
| INV-C0-9 | **Valor tipado con ida y vuelta**: `decode(encode(v)) == v` para cada `ExecutableValue`; la proyección sólo lee valores decodificados. | Hypothesis por tipo. |
| INV-C0-10 | **Toda vista por defecto se anuncia**: READY con una petición `UNSPECIFIED` exige exactamente un `HostViewDecision` con `announced=True`, y la respuesta contiene su eco. | Validación de contrato + test de presentación. |
| INV-C0-11 | **El host no depende de la memoria del prompt**: para cada convención normativa de la lista cerrada (§5.4.2), una propuesta que la viola termina con la disposición Gold o con CLARIFY, nunca con un EXECUTE distinto. | Suite de mutantes «prompt amnésico» con IR oráculo. |
| INV-C0-12 | **El ORDER semántico prohíbe el fallback H4**: una petición con ORDER no puede degradar a `NO_PRESERVE` (`foundation_runtime.py:117`). | Test + mutante. |

---

## 4. Opciones comparadas (≤3 por eje)

### 4.1 Compatibilidad v1/v2

| Opción | Fallos | Reversibilidad | Migración | Decisión |
|---|---|---|---|---|
| A. v2 en sitio y v1 eliminado | El replay DA-6 y el perfil v4.2 mueren; no hay rollback que no sea legacy | Baja | Reescribir fixtures | No |
| **B. v2 en memoria + `lift_v1` para el wire 4.2 y los registros guardados** | Un solo host; el riesgo se concentra en `lift_v1` (INV-C0-2 lo cubre) | Alta: el perfil `foundation_v3` sigue | Hashes cambian (declarado) | **Sí** |
| C. Dos contratos y dos hosts paralelos | Derivan; duplicación (monolito ×2); dos autoridades por dimensión | Alta | Doble mantenimiento | No |

### 4.2 Declinación por operación

| Opción | Fallos | Superficie | Coste | Decisión |
|---|---|---|---|---|
| A. Quedarse con la puerta regex y los vetos previos | 25/58 secuestros al borrarla; la puerta no se puede retirar | Regex = frontera | 0 ahora, M1-I imposible | No |
| **B. Operación en la IR + tabla cerrada de dueños + reclamación previa del dueño + entrega al grafo legacy + conformidad medida** | Residual: el grafo legacy elige otro dueño (medido, INV-C0-6) | El LLM decide a qué dueño *ofrecer* el turno; el dueño decide si lo acepta | Pequeño, sin tocar los dueños | **Sí** |
| C. Operación en la IR + invocar directamente los ejecutores con la IR tipada (manifiestos M3) | El más correcto | Mínima | Migrar 5 dueños ahora = M3 completo | No en M1 (es el destino) |

### 4.3 Interfaz de autoridad

| Opción | Problema | Decisión |
|---|---|---|
| A. La autoridad recibe el texto crudo (como `resolve_temporal_scope`, `resolve_actor_params`) | Viola el invariante 6; A2 lo marca como la causa de que ACTOR no sea llamable | No |
| **B. Ventana de evidencia (citas + K tokens plegados a cada lado), con el contexto monótono** | Una K fija puede no ver un calificador lejano (falla hacia aclarar; H0 ya lo declara) | **Sí** |
| C. Sólo la cita | No detecta «agosto de» fuera de la cita ni «no» antes del verbo: aclara de más u obliga a depender de la memoria del prompt | No |

### 4.4 Lugar de la Product View Policy

| Opción | Problema | Decisión |
|---|---|---|
| A. En el prompt | Lo normativo vuelve al LLM (la lección de H7) | No |
| B. Dentro de H7 | H7 lee la cita de universo antes de admitir; PV2 necesita el estado *resuelto* | No |
| **C. Módulo puro después de admitir y antes de proyectar, con su decisión registrada en la aceptación** | Un registro nuevo en el contrato | **Sí** |

---

## 5. Diseño

### 5.1 IR v2 (`interpretation_contract.py`, `SCHEMA_VERSION = 2`)

Las piezas nuevas viven en módulos propios del núcleo para no engordar el contrato (577 LoC hoy):
`foundation_operations.py` (operación, aspecto, job) y `foundation_operator_table.py` (operadores por
dimensión y niveles). `interpretation_contract.py` sólo los importa y valida.

#### 5.1.1 Tipos nuevos

```text
Operation(str, Enum): SEARCH, COUNT, SUM_VALUE, GROUP_BY, RANK_SUPPLIERS, PROFILE_BUYER,
                      PROFILE_ACTOR, FOLLOW, EXPORT, FOLLOWUP_DELTA, OUT_OF_DOMAIN, PROTOCOL
Aspect(str, Enum):    TOTAL, BY_MODALITY, BY_YEAR, BY_BUYER, BY_SUPPLIER, BY_STATE, BY_TYPE, OTHER
Subject(str, Enum):   PROCESSES="processes", CONTRACTS="contracts", UNSPECIFIED="unspecified"
GeoLevel(str, Enum):  DEPARTMENT, MUNICIPALITY, REGION, UNKNOWN

ProposedJob(frozen):
  job_id: str
  operation: Operation
  aspect: Aspect | None
  aspect_label: str | None          # sólo con Aspect.OTHER (p. ej. MARKET_ANALYSIS, D10)
  request_ids: tuple[str, ...]      # SEARCH: ≥1; otras: ≥0 (C7: sólo transportan condiciones)
  evidence: tuple[RawSpan, ...]     # señal de la operación; obligatoria si operation ≠ SEARCH
  referent_evidence: RawSpan | None # anáfora («sus», «esos») → NEEDS_HISTORY (C2, D9)
```

`MULTI` no existe en el wire: `InterpretationProposal.is_multi := len(jobs) > 1`.

#### 5.1.2 Cambios en los registros existentes

| Registro | Cambio |
|---|---|
| `ProposedRequest` (`:163`) | **Sale** `operation` (pasa al job). `subject: Subject` (antes `str`). `universe_claim/universe_evidence` son `None` **sí y sólo sí** `subject is UNSPECIFIED`. |
| `ProposedCondition` (`:196`) | **Entra** `level: GeoLevel \| None`: obligatorio con GEOGRAPHY, prohibido con el resto. `operator ∈ OPERATORS[dimension]` (tabla cerrada); UNSUPPORTED conserva el operador libre + `unsupported_label`. |
| `InterpretationProposal` (`:260`) | **Entra** `jobs: tuple[ProposedJob, ...]`, no vacío. `schema_version == 2` (`:275`). |
| `SemanticDimension` (`semantic_dimensions.py`) | **Entran** `ORDER` y `CARDINALITY` (aditivo; `legacy_shadow.DIMENSION_ORDER` debe incluirlos o `tests/test_legacy_shadow.py:375` falla: cambio declarado). |
| `AcceptanceStatus` (`:55`) | **Entra** `HANDOFF` (con propuesta, sin resoluciones ni políticas, `reason = OPERATION_HANDOFF:<OP>:<OWNER>`). |
| `InterpretationAcceptance` (`:415`) | **Entra** `view_decisions: tuple[HostViewDecision, ...] = ()` (§5.6). |

#### 5.1.3 Tabla cerrada de operadores (`foundation_operator_table.py`)

| Dimensión | Operadores | Notas de contrato |
|---|---|---|
| TOPIC | `contains`, `whole_token_folded` | K1 lo decide el host (§5.4.2); el productor puede emitir `contains` para «obras». |
| BUYER | `is` | — |
| ACTOR | `is` | La cita es el identificador **sin** el marcador de rol (C1). |
| GEOGRAPHY | `is` | `level` obligatorio. |
| TIME | `during_year`, `during_month`, `between_dates`, `relative_period` | La cita es **la expresión temporal entera** con sus comparadores (adiós a Y1). `between_dates` con una sola cota cubre «desde 2023» y «hasta marzo». |
| VALUE | `greater_than`, `greater_or_equal`, `less_than`, `less_or_equal` | «entre X y Y» = dos condiciones bajo AND. Monto sin comparador → UNSUPPORTED `value_without_comparator` (B5). |
| STATE | `is`, `family` | — |
| MODALITY | `is`, `family` | — |
| CONTRACT_TYPE_OR_NATURE | `is` (`family` reservado, D3) | — |
| ORDER | `value_desc`, `value_asc`, `date_desc`, `date_asc` | Como mucho uno por petición; sólo como átomo del AND de primer nivel (igual que REQUESTED_UNIVERSE, `:317`). |
| CARDINALITY | `top_n` | Cita la cifra («10», «cinco»); como mucho una por petición; exige ORDER en la misma petición SEARCH, o un job RANK_SUPPLIERS (cierra el hueco 9 de A4). |
| RETRIEVAL_INTENT | `is` | Sólo lo escribe el host (H7 / `universe_kind`), como hoy. |
| SOURCE | (sigue en `source_evidence` de la petición) | — |

#### 5.1.4 Reglas de validación nuevas (`ContractError`, todas cerradas)

- `MISSING_JOBS`, `DUPLICATE_JOB_ID`, `DUPLICATE_JOB_OPERATION` (como mucho un job por operación; dos
  búsquedas son un job con varias peticiones).
- `ORPHAN_REQUEST` / `REQUEST_IN_TWO_JOBS`: toda petición pertenece exactamente a un job.
- `SEARCH_JOB_WITHOUT_REQUEST`, `UNGROUNDED_OPERATION` (job no-SEARCH sin evidencia).
- `ASPECT_REQUIRED` (GROUP_BY), `ASPECT_FORBIDDEN` (fuera de GROUP_BY/COUNT/SUM_VALUE),
  `COUNT_ASPECT_NOT_TOTAL` (D1), `ASPECT_LABEL_MISMATCH`.
- `UNSPECIFIED_WITH_UNIVERSE_EVIDENCE` / `UNGROUNDED_REQUEST_UNIVERSE` (la que ya existe, ahora
  condicionada al sujeto).
- `GEOGRAPHY_LEVEL_REQUIRED`, `LEVEL_OUTSIDE_GEOGRAPHY`, `OPERATOR_OUTSIDE_DIMENSION`.
- `MULTIPLE_ORDER`, `MULTIPLE_CARDINALITY`, `CARDINALITY_WITHOUT_ORDER`,
  `NONCONJUNCTIVE_ORDER`.
- `UNIVERSE_KIND_ON_UNSPECIFIED` (wire v2: `universe_kind` obligatorio con sujeto especificado y
  prohibido con `unspecified`).

La polaridad NEGATIVE en ORDER/CARDINALITY **no** es error de contrato: el host aclara
(`NEGATION_UNSUPPORTED:ORDER`). Un error de contrato tira toda la propuesta y es más caro.

#### 5.1.5 Lo que deliberadamente no está en el wire

- `value_hint` del Gold: sería un valor que el host tendría la tentación de leer (invariante 3).
- `limit_n` numérico: lo resuelve el host a partir de la cita de CARDINALITY.
- `turn_context`: se deriva (`FOLLOWUP_DELTA` ⇒ fragmento; `referent_evidence` ⇒ necesita historia).
- `MULTI` y `PROTOCOL` como «modo»: MULTI se deriva; PROTOCOL es una operación más que siempre termina
  en mensaje (la capa 1 corre antes).

#### 5.1.6 Correspondencia con el Gold (para el harness, no para el runtime)

Gold `operation=MULTI, jobs[]` ↔ `jobs`; Gold ORDER con `limit_n` ↔ ORDER + CARDINALITY; Gold
`universe=UNSPECIFIED` ↔ `Subject.UNSPECIFIED`; Gold `view` de SEARCH explícito ↔ sujeto +
`universe_kind`. El adaptador vive en `scripts/audits/m1_search/` (HERA), no en `app/`.

### 5.2 Versionado y decoder

| Pieza | Diseño |
|---|---|
| Wire 4.2 (perfil `foundation_v3`) | `foundation_producers.decode_proposal` (`:49`) sin cambio semántico → `lift_v1`: cada petición `SEARCH` entra en un job SEARCH; cada `COUNT`/`SUM_VALUE` en su job con `aspect=TOTAL` y evidencia = la de la petición; `subject` pasa al enum; GEOGRAPHY recibe `level=DEPARTMENT`, que es exactamente lo que el host asume hoy (`foundation_admission.py:101-104`), así que no hay cambio de significado. |
| Wire 5 (perfil `foundation_v5`) | Módulo nuevo `foundation_decoder_v2.py`, mismas reglas de evidencia por `candidate_ids` (brazo B) y misma `host_candidates` (`:34`). Reutiliza `expand_universe_kind`, cuya lista de versiones (`foundation_universe_kind.py:19`) pasa a `{"4.2", "5"}`, con `unspecified` sin `universe_kind`. |
| Selección | `PROFILES = {"foundation_v3": (prompt 4.2, WireV1), "foundation_v5": (prompt 5, WireV2)}` en el núcleo; `LinguisticProducer(profile)` sustituye a la constante. No hay selector suelto de productor ni de wire. |
| Registros guardados | `load_acceptance(dict)` acepta `schema_version` 1 (y lo eleva) o 2; cualquier otro valor da `UNSUPPORTED_SCHEMA_VERSION`. Las herramientas de traza y replay pasan por ahí. |
| Hashes | `interpretation_hash`/`plan_hash` cambian para todo turno (el `to_dict` cambia). Cambio declarado; no encontré tests que fijen un hash literal (búsqueda con grep; `test_foundation_projection.py:184` lo recalcula): verificarlo en C1.2. |

### 5.3 Declinación por operación

#### 5.3.1 Dónde entra

```text
run_foundation_search (foundation_runtime.py:250)
  producer → decode(+lift) → H7 → decide_operation(proposal)          ← NUEVO (C1.4)
     ├─ EXECUTE_SEARCH  → admit (autoridades) → PV → project → guard → present
     ├─ HANDOFF(op, owner)       → return OperationHandoff (sin llamadas a la fuente)
     └─ TERMINAL(status, reason) → mensaje determinista (+ botones), _record
```

`decide_operation` vive en `foundation_operation_host.py`: es pura sobre `proposal.jobs` más un
`OwnerClaims` precalculado (§5.3.3). Sustituye a los dos `UNSUPPORTED_OPERATION` de hoy
(`foundation_admission.py:61-62`, `foundation_projection.py:415-416`), que se borran en el mismo paso.

`run_foundation_search` devuelve `dict | OperationHandoff | None`. El hook del orquestador
(`orchestrator.py:444-448`) trata `OperationHandoff` así: registra la fila `CAPABILITY` en el ledger,
llama a `handle_normal_flow` con el **mismo** `ctx` y los mismos `deps` (INV-C0-4) y, tras el
resultado, registra `executed_owner(result)` (INV-C0-6).

#### 5.3.2 Tabla cerrada de dueños (`foundation_operation_owners.py`, datos puros)

| Operación (Gold) | Dueño en M1 | Ejecutores permitidos (conformidad) | Sin reclamación del dueño → terminal | Código |
|---|---|---|---|---|
| SEARCH | FOUNDATION | — | — | — |
| COUNT, SUM_VALUE, GROUP_BY | CONTRACT_ACTIVITY \| ANALYTICS | `{contract_activity, analytics_*}` | UNSUPPORTED | `OPERATION_OWNER_DECLINED:<OP>` |
| PROFILE_BUYER | ENTITY_PROFILE | `{entity_profile}` | UNSUPPORTED | ídem |
| PROFILE_ACTOR | ACTOR_PROFILE | `{actor_profile, actor_resolution}` | UNSUPPORTED | ídem |
| RANK_SUPPLIERS | SUPPLIERS | `{entity_profile.suppliers}` (sólo vía binding/perfil activo) | UNSUPPORTED | ídem |
| FOLLOW | RADAR | `{radar}` (el comando literal ya corrió antes) | UNSUPPORTED + guía «guardar radar <nombre>» | `OPERATION_NOT_HOSTED:FOLLOW` |
| EXPORT | — | — | UNSUPPORTED + guía «busca y usa Exportar» | `OPERATION_NOT_HOSTED:EXPORT` |
| FOLLOWUP_DELTA | LEGACY_FOLLOWUP (D6) | `{tabular, actor, entity_profile}` | si el turno previo es Foundation: el `FOUNDATION_CONTINUATION_UNSUPPORTED` que ya existe; sin historia: CLARIFY | `FOLLOWUP_WITHOUT_HISTORY` |
| PROTOCOL | — | — | NO_ACTION | `PROTOCOL_WITHOUT_STATE` |
| OUT_OF_DOMAIN | — | — | NO_ACTION | `OUT_OF_DOMAIN` |
| MULTI (derivada) | — | — | NEEDS_CLARIFICATION, un botón por job | `MULTI_OPERATION` |
| cualquier job con `referent_evidence` | como FOLLOWUP_DELTA | ídem | CLARIFY | `ANAPHORIC_WITHOUT_REFERENT` |

Con un job SEARCH cuyas peticiones no son SEARCH-ejecutables (ACTOR sin autoridad activa, etc.) el
turno **no** se desvía: sigue el camino SEARCH y el host aclara o bloquea (INV-C0-3).

#### 5.3.3 Reclamación del dueño (`OwnerClaims`)

Una función por dueño, en `foundation_route_owner.py` (núcleo). Es la misma lógica que ya usa
`specialized_owner` (`:41-60`), partida por dueño, con una corrección:

- ACTOR → `actor_claims` (`:20`).
- ENTITY_PROFILE → **`classify_entity_profile_turn`** ejecutable (comprador resuelto), no
  `detect_entity_profile_turn` (A4 rec. 5: hoy se cede ante perfiles que luego no se ejecutan, N051/N067).
- CONTRACT_ACTIVITY → `admit_contract_activity(...).status is READY` (no basta con «no es HANDOFF»:
  un CLARIFY del CA no es una reclamación de capacidad; A4 rec. 6).
- ANALYTICS → `any(REGISTRY[i].detect)`.
- SUPPLIERS → `binding is not None and binding.capability == suppliers`, o seguimiento de perfil activo.

Estas funciones leen texto (QueryRouter). Es la deuda declarada de §2; no se añade ningún lector nuevo.
En C1 el veto previo `decline_reason` (`:74`) **sigue** igual (paridad), así que el dueño casi nunca
reclamará algo que llegó a Foundation: el efecto práctico de C1 es convertir en terminal explícito lo
que hoy se secuestra. Las reclamaciones cobran sentido en M1-I, cuando se borra el veto previo.

#### 5.3.4 Regla de interpretación única (respuesta a «¿y si el dueño vuelve a interpretar?»)

1. **R1 — Bloqueo de operación.** La operación de Foundation es la operación autoritativa del turno.
   Lo único que cruza a legacy es `OperationHandoff(operation, aspect, owner, job_id,
   table_version)`. Ni condiciones, ni spans, ni params.
2. **R2 — Intérprete único de valores.** En una entrega, el dueño legacy es el único que decide los
   valores ejecutables. Las condiciones de Foundation quedan en el ledger (`SHADOW`) como instrumento
   de paridad para M3, nunca como entrada.
3. **R3 — Monotonía del enrutamiento legacy.** El grafo legacy puede *no* ejecutar (aclarar), pero si
   ejecuta debe hacerlo con un ejecutor de `allowed_executors(op)`. Si no, es
   `HANDOFF_CONFORMANCE_VIOLATION` en el ledger.
4. **R4 — Sin segundo salto.** Si el dueño no reclama, el turno termina en Foundation con su mensaje.
   No se prueba con otro dueño ni con tabular.

Verificación: INV-C0-4 (test de igualdad de `ctx`/`deps` y cero llamadas), INV-C0-5 (test exhaustivo
de operación × reclamación), INV-C0-6 (sonda Gold). En C1, R3 se **mide**, no se impone en caliente:
reemplazar una respuesta legacy ya calculada no deshace sus efectos (`append_turn`, radar). Para M1-I
la violación debe ser 0 en la sonda, y una sola violación en dogfood es STOP (§10).

`executed_owner(result)` es una función pura con una tabla cerrada `route_reason → owner`. Un
`route_reason` desconocido da `UNKNOWN_EXECUTOR`, que cuenta como violación. Un test enumera todos los
`route_reason` que emite legacy.

#### 5.3.5 Ledger

`StageName.CAPABILITY` (ya existe en `turn_ledger.py`): `authority = operation-owners@1`,
`decision ∈ {EXECUTE_SEARCH, HANDOFF, TERMINAL}`, `reason_code` del §5.3.2, `detail = {operation,
aspect, owner, owner_claimed, executed_owner, conformant}`. Los `AdmissionCode` de la puerta no
cambian en C1.

#### 5.3.6 Qué se exige para borrar `eligible_first_search`, la regex y los vetos en M1-I

1. La tabla §5.3.2 cubre las 12 operaciones; el test con IR oráculo sobre NEIGHBOR-TEST da la
   disposición esperada en el 100 %.
2. Con el productor v5 y la puerta retirada (simulada): **0** secuestros en NEIGHBOR y **0** SEARCH
   Gold que legacy acierta enviado a otra operación (ADR-103 enmienda 2: el gate CUTOVER es de
   tolerancia cero; el 2 % sólo vale para ACTIVATE_V5).
3. Conformidad (INV-C0-6) = 0 violaciones en la sonda NEIGHBOR + REAL con los vetos previos retirados.
4. Cada clase SEARCH con `L-only-OK = 0` exclusivo (ACTOR, TIME mes/rango, VALUE inclusivos, ORDER,
   GEO municipio/región, STATE, MODALITY…), sin dispensas: una diferencia deliberada se resuelve
   corrigiendo el Gold por adjudicación y nueva versión sellada (ADR-103 enmienda 2). Además, 0
   ensanches silenciosos, 0 inversiones de universo en casos CLEAR y 0 inversiones de polaridad.
5. Historia liberada: los 64 `FOLLOWUP_DELTA` REAL entregados a legacy sin degradación frente a hoy.
6. G integrado (failover según A5/D1): sin puerta, todo primer turno depende del proveedor.
7. Latencia medida: todo primer turno no protocolario paga la llamada al LLM (p90 ≈ 2 s con v4.2).

Al cumplirse, en el mismo commit se borran `first_search_ineligibility` (`foundation_runtime.py:30-49`),
el veto previo `decline_reason`/`specialized_owner` (`foundation_route_owner.py:41-79`) y
`continues_foundation_turn` (`:93-114`), que reemplaza la operación `FOLLOWUP_DELTA`. Sin fallback.

### 5.4 Productor v5

#### 5.4.1 Archivos y perfil

- `app/core/foundation_prompts_v5.py` (nuevo): `PROMPT_VERSION = "5"`, `PRODUCER_ARM = "B"`,
  `PROMPT_SHA256` fijado en su propio test **al congelar** (no durante el desarrollo). Ensamblado
  desde secciones propias; no importa `_CONVENTIONS_V4*` (así un cambio en v4.2 nunca arrastra a v5 y
  al revés).
- `foundation_prompts.py` no se toca. Su test de «ninguna otra versión es alcanzable» (`:84`) se
  mantiene para *ese* módulo; el conjunto de perfiles se fija en un test nuevo.
- `config.py:94`: `Literal["legacy", "foundation_v3", "foundation_v5"]`, default `legacy`. Rollback:
  `foundation_v3` (v4.2 bajo el host v2) o `legacy` (el único rollback que reproduce `main`).

#### 5.4.2 Qué sale del prompt (cierre normativo en el host)

Regla: el prompt enseña **segmentación y vocabulario** (qué span, qué dimensión, qué operador
lingüístico, qué operación pide el usuario). Toda regla cuyo incumplimiento el host puede detectar en
la IR estructurada se cierra en el host como política cerrada con recibo (patrón H7), y en el prompt
queda como mucho como defensa en profundidad.

| Convención | Hoy | v5 |
|---|---|---|
| Y1 «cita sólo el año» (`foundation_prompts.py:160`) | En el prompt; causa del ensanche | **Eliminada.** Regla nueva: «TIME cita la expresión temporal completa». El host verifica con la cobertura de ventana (H0 → D-TIME). |
| V1 sólo exclusivos | En el prompt | Cuatro operadores; la guarda de dirección con negación e inclusivos es de D-MONEY. |
| O3/O3-INTENT/O6/O7 | Prompt + H7 | Sólo H7 (ya cerrado; ADR-101). |
| K1/T2 «obra(s)» → `whole_token_folded` | Prompt | Política de host `topic-k1@1` sobre el valor de TOPIC. |
| M3/O5 «licitación(es)» coloquial nunca es MODALITY | Prompt | Guarda de host (D-STATE/MOD): `strength=colloquial` ⇒ política `modality-o6@1`, no filtra, con recibo. |
| C5 «convenios» = CONTRACTS + CONTRACT_TYPE (COMPOSITE) | Etiqueta Gold | Política de host `universe-c5@1`: añade la condición citando la evidencia de universo (mismo mecanismo con que H7 añade RETRIEVAL_INTENT). |
| S3 embudo «publicados o abiertos» | Prompt + ADR-100 en host | Sigue en el host (ADR-100). |
| PV1–PV6 | — | Sólo el host (§5.6). |
| Frontera de operación (§2 del contrato) | — | **En el prompt** (es lingüística: lo que el usuario quiere recibir), con ejemplos inventados. |

Ningún ejemplo del prompt puede parecerse a un texto Gold: un script compara n-gramas (Jaccard ≥ 0,6 ⇒
rechazo) contra los 999 registros antes de congelar.

#### 5.4.3 Protocolo de evaluación (antes de activar)

1. **Particiones fijadas antes de escribir una línea del prompt** (C1.6.0): por cada slice,
   `sha256(gold_id + "m1c0-split-v1") mod 4 == 0` → DEV (≈25 %), el resto → TEST. Se guarda la lista
   de ids y su digest en `gold/SPLIT_v1.json`; el texto REAL sigue fuera de git. El autor del prompt
   (ARES) sólo ve DEV. TEST lo ejecuta HERA.
2. **Iteración acotada**: como mucho 3 versiones del prompt sobre DEV (hardening acotado). Si la
   tercera no pasa DEV, STOP.
3. **Congelado**: SHA del prompt + commit del host. Toda corrida TEST cita ambos.
4. **Corridas**: 2 pasadas a temperatura 0 de v5 y de v4.2 (control) sobre TEST, con el mismo host;
   IR oráculo (Gold → IR v2) por el host para separar errores del productor de errores del host.
5. **DA-6**: custodio independiente, comparación v5+host contra v4.2+host con su métrica
   (equivalencia de obligaciones). Sin acceso del implementador.

Criterios numéricos de **ACTIVATE_V5** (con la puerta todavía puesta; dogfood experimental; los gates
de CUTOVER son otros, de tolerancia cero, ver §5.3.6 y ADR-103 enmienda 2):

| Métrica (TEST, peor de 2 pasadas) | Umbral |
|---|---|
| `INVALID_PROPOSAL` + `PROVIDER_ERROR:TRUNCATED` | ≤ 1 % |
| Exactitud de operación (REAL-TEST, 12 clases) | ≥ 95 % |
| No-SEARCH clasificado SEARCH (NEIGHBOR-TEST) | ≤ 1 caso (0 para CUTOVER) |
| SEARCH clasificado no-SEARCH (REAL-TEST) | ≤ 2 % |
| Inversiones de polaridad; procesos↔contratos en casos CLEAR | **0** |
| Dimensiones ya soportadas por v4.2 (TOPIC, BUYER, GEO/DEPT, `during_year`, VALUE gt/lt, STATE, MODALITY): exactitud de propuesta (dimensión + operador + polaridad + solape de span) | sin regresión > 2 pp frente a v4.2, n ≥ 20 |
| Dimensiones nuevas (ACTOR, ORDER, CARDINALITY, mes/rango, inclusivos, UNSPECIFIED, `level`) | ≥ 90 % de exactitud de propuesta |
| Estabilidad: proposición canónica idéntica en 2 pasadas | ≥ 95 % (operación ≥ 99 %) |
| DA-6 | ningún caso que v4.2+host pasa y v5+host no (o firmado uno por uno) |
| Latencia p90 del productor | ≤ 1,5 × la de v4.2 y ≤ 8 s |
| Mutantes de prompt amnésico (INV-C0-11, IR oráculo) | 100 % disposición Gold o CLARIFY |

Ningún umbral se baja para que v5 pase. Un fallo es un NO_GO con informe.

### 5.5 Interfaz de autoridades

> **Enmendada por la revisión METIS de C1.1** (`C1_1_METIS_REVIEW.md`, veredicto FREEZE_WITH_CHANGES,
> lista cerrada W1–W2, V2, V3, R1, V5a–e, K1–K5). Donde este apartado y la revisión difieran, manda la
> revisión: ventana con segmentos y huecos (≤8 tokens, INV-C0-7b), `datasets` nunca vacío y
> `local_date()` Colombia, `detail` en el veredicto, `excluded` en los valores, `BuyerValue` unidad|grupo
> (ADR-074), `GeoValue.department`, `StateValue` por dataset/columna, `CardinalityValue.basis`.

#### 5.5.1 Contrato (`foundation_authority_contract.py`, núcleo, congelado en C1.1)

```text
EvidenceWindow(frozen):
  spans: tuple[RawSpan, ...]          # las citas, con offsets
  quote: str                          # frase material (material_phrase), NFC
  left: tuple[str, ...]               # ≤ WINDOW_TOKENS tokens plegados antes de la primera cita
  right: tuple[str, ...]              # ≤ WINDOW_TOKENS tokens plegados después de la última
WINDOW_TOKENS = 4                     # invariante (la cadena H0 más larga cabe; ver foundation_time_coverage)

AuthorityContext(frozen):
  reference_instant: datetime          # con zona; obligatorio aunque la dimensión no lo use
  datasets: frozenset[str]             # datasets de las peticiones que llevan la condición; vacío con UNSPECIFIED

DimensionEvidence(frozen):
  condition_id: str; dimension: SemanticDimension; operator: str; polarity: Polarity
  level: GeoLevel | None; window: EvidenceWindow

AuthorityVerdict(frozen):
  decision: ResolutionDecision         # RESOLVED | CLARIFY | BLOCK (ya existe, :64)
  authority: str                       # "<id>@<version>", p. ej. "time@2"
  value: ExecutableValue | None        # obligatorio sí y sólo sí RESOLVED
  reason: str | None                   # obligatorio si no es RESOLVED; de AUTHORITY_REASONS
  effect: Effect                       # EQUIVALENT para READY salvo HostPolicy que lo cubra
  policies: tuple[str, ...]            # "<policy>@<v>" que la autoridad aplicó (p. ej. topic-k1@1)
  receipt: AuthorityReceipt            # reglas disparadas, nº de candidatos, digests; nunca texto libre

class DimensionAuthority(Protocol):
  authority_id: str; version: str
  dimensions: frozenset[SemanticDimension]; operators: frozenset[str]
  def resolve(self, evidence: DimensionEvidence, ctx: AuthorityContext) -> AuthorityVerdict: ...
```

`ExecutableValue` es una unión cerrada; cada tipo tiene `encode() -> tuple[str, ...]` (con etiqueta de
tipo y versión) y `decode(...)`, con ida y vuelta probada. Así `HostResolution.resolved_ids` (`:380`) no
cambia de forma y la proyección decodifica:

| Tipo | Campos |
|---|---|
| `TopicValue` | `phrase_folded`, `token_mode: bool` |
| `BuyerValue` | `canonical`, `owners: frozenset[str]` (=1 para RESOLVED) |
| `ActorValue` | `document`, `document_kind (NIT/CC/…)`, `display` |
| `GeoValue` | `level`, `canonical`, `divipola: str \| None`, `members: tuple[str, ...]` (región) |
| `TimeValue` | `start: date \| None`, `end_exclusive: date \| None` (al menos uno), `precision` |
| `MoneyValue` | `amount_cop: int`, `comparator ∈ {>, >=, <, <=}` |
| `StateValue` | `canonical_class` (open, published, closed, awarded, terminated, in_execution, cancelled, draft, in_evaluation, signed, liquidated), `by_dataset: {dataset: literals}` |
| `ModalityValue` | `procedure_ids`, `family_policy: str \| None`, `excluded: bool` |
| `ContractTypeValue` | `type_ids` (residuo vacío obligatorio) |
| `OrderValue` | `key ∈ {value, date}`, `direction` |
| `CardinalityValue` | `n: int` (1..`MAX_TOP_N`, decisión P8) |

`AUTHORITY_REASONS` (cerrado; el núcleo los traduce a mensaje): `UNRESOLVED_HOST_DIMENSION:<D>`,
`AMBIGUOUS_HOST_DIMENSION:<D>` (el de H0 entra aquí), `UNCITED_QUALIFIER:<D>`,
`OPERATOR_EVIDENCE_MISMATCH:<D>`, `NEGATION_UNSUPPORTED:<D>`, `LEVEL_MISMATCH:GEOGRAPHY`,
`GENERIC_IDENTITY:<D>` (ACTOR/BUYER genéricos de A2), `AUTHORITY_UNAVAILABLE:<D>` (BLOCK: índice ausente,
nunca un vacío silencioso), `RESIDUE_UNREAD:<D>`, más los `VALUE_*` existentes de
`foundation_messages.py` como alias. CLARIFY se traduce a `NEEDS_CLARIFICATION`; BLOCK a `UNSUPPORTED`.

Regla de polaridad: una autoridad recibe la polaridad y la honra (RESOLVED con `excluded`/complemento
y su política) o devuelve `NEGATION_UNSUPPORTED`. Nunca ignora una NEGATIVE.

#### 5.5.2 Ventana

`foundation_evidence_window.py` (núcleo) es el **único** lugar que construye `EvidenceWindow` a partir
del texto crudo. Se construye después de `validate_raw` y la autoridad nunca recibe `raw`. Reutiliza el
tokenizador de `foundation_time_coverage.py` (`[a-z0-9]+` sobre `normalize_text`).

#### 5.5.3 Registro y punto de enchufe

`foundation_authority_registry.py` (núcleo):

```text
AuthorityEntry(dimension, factory: Callable[[AuthorityDeps], DimensionAuthority], maturity)
Maturity: DECLARED | SHADOW | ACTIVE
AuthorityDeps: entity_resolver, actor_name_resolver, territories, catalogs (sólo lectura)
REGISTRY: tuple[AuthorityEntry, ...]   # una línea por dimensión
```

- `SHADOW`: `admit_proposal` calcula el veredicto, lo registra en el ledger (`HOST`, detalle
  `shadow_disagrees`) y decide con la rama vigente del host.
- `ACTIVE`: `admit_proposal` decide con el veredicto y **la rama en línea de esa dimensión se borra en
  el mismo commit** (invariante 5). Punto de enchufe: el bucle por dimensión de
  `foundation_admission.py:74-160`, que termina reducido a «construir evidencia → registro → veredicto →
  `HostResolution`».
- La proyección (`foundation_projection._condition_params`, `:216`) gana una rama por tipo decodificado;
  es del núcleo (pasos E).

#### 5.5.4 Cómo trabajan los carriles sin tocar el núcleo

1. C1.1 congela `foundation_authority_contract.py`, `foundation_evidence_window.py` y el kit
   `tests/foundation_authority_kit.py`, y se integra **antes** que el resto de C1. Los carriles D-*
   parten de ese commit.
2. Cada carril escribe `app/core/host_authorities/<dim>.py` y `tests/test_host_authority_<dim>.py`, que
   llaman al kit: `assert_conforms(authority, cases)`, que comprueba INV-C0-7/8/9, razones cerradas,
   polaridad y determinismo, más sus casos de dominio (sondas A2 + slice Gold de su dimensión con IR
   oráculo).
3. El carril entrega «autoridad conforme + informe de su slice». El enchufe (registro SHADOW → ACTIVE +
   proyección + mensaje) lo hace el núcleo en su paso E. Si un carril necesita cambiar el contrato:
   STOP → METIS (§10).

### 5.6 Product View Policy

- **Módulo**: `app/core/product_view_policy.py` (puro, dueño F). Sin E/S, sin texto crudo, sin prompt.
- **Entrada**: `ViewInput(request_id, positives: tuple[ResolvedCondition, ...])`, donde
  `ResolvedCondition = (dimension, operator, value: ExecutableValue)` y **sólo condiciones POSITIVE**
  (D4). Se construye a partir de la aceptación ya resuelta; PV2 lee `StateValue.canonical_class` (por eso
  STATE debe resolverse sin depender del dataset).
- **Salida**:

```text
HostViewDecision(frozen):            # registro nuevo de interpretation_contract (núcleo)
  request_id: str
  view: ProductView                  # HISTORICAL_PROCESSES | CONTRACTS | CURRENT_OPPORTUNITIES
  rule: str                          # "PV1".."PV6" | "USER_OVERRIDE"
  policy: str                        # "product-view@1"
  trigger_condition_ids: tuple[str, ...]   # vacío sólo en PV6 y USER_OVERRIDE
  effect: Effect                     # APPROXIMATION en PV1–PV6 (vista no pedida); EQUIVALENT en USER_OVERRIDE
  announced: bool                    # True obligatorio para READY (INV-C0-10)
```

- **Validación** (contrato): con READY, cada petición `UNSPECIFIED` tiene exactamente un
  `HostViewDecision` y una `HostResolution(request_id, RESOLVED, "product-view@1", (dataset,))` coherente
  con la vista; las peticiones con sujeto explícito no tienen ninguno.
- **Orden en el runtime**: H7 (no decide sobre UNSPECIFIED) → autoridades → `decide_view` → aceptación
  READY → proyección. `CURRENT_OPPORTUNITIES` proyecta los mismos params O1B que una oportunidad
  explícita (`foundation_projection.py:240-244`). Si el STATE resuelto no tiene literales para el dataset
  de la vista, la proyección bloquea con `STATE_NOT_IN_VIEW`.
- **Anuncio** (dueño F): `app/core/product_view_announcement.py`, que expone
  `announce(decision) -> ViewAnnouncement(echo: str, actions: tuple[ViewAction, ...])`. El eco nombra la
  vista y la razón («No dijiste procesos ni contratos y no hay periodo: te muestro oportunidades
  abiertas»). Las acciones son las vistas alternativas.
- **Botones como evento, no como texto**: `ViewAction(action_id, label, override: ProductView)`. El
  manejador de protocolo `handle_foundation_view_action` (capa 1, módulo de F, conectado por el núcleo en
  la tupla de handlers de `orchestrator.py:410-417`) carga la aceptación **inmutable** del turno
  Foundation inmediatamente anterior del mismo chat por `trace_id`, sustituye la decisión por
  `USER_OVERRIDE`, vuelve a proyectar y no llama al LLM. Es la única lectura de estado permitida en M1;
  cualquier otra cosa es M2.
- `[Elegir período]` y `[Mayor valor]` del contrato §5 exigen un delta de IR (M2): ver decisión P7.

---

## 6. Plan C1 (un único escritor del núcleo; pasos pequeños)

Cada paso: ruff sobre los archivos tocados → tests focales → mutantes listados (aplicar, correr,
restaurar) → `olympus-semgrep-delta --base <commit previo>`. «Corrí X, no la suite» salvo en C1.7.

| Paso | Contenido | Gates específicos | Mutantes que deben morir |
|---|---|---|---|
| **C1.0** | ADR nueva (perfiles cerrados, D1 vs §9, INV-C0-*) + aceptación de este diseño | Sí del usuario | — |
| **C1.1** | `foundation_authority_contract.py`, `foundation_evidence_window.py`, `foundation_authority_registry.py` (vacío), paquete `host_authorities/__init__.py`, kit de conformidad. Sin cambio de runtime. **Se integra primero; los D-* parten de aquí.** | Hypothesis: ventana ≤ K, ida y vuelta de cada `ExecutableValue`; kit autoprobado con una autoridad juguete conforme y 4 no conformes | K+1; ventana que filtra texto crudo; RESOLVED sin valor aceptado; razón fuera del conjunto cerrado; contexto que crea un RESOLVED |
| **C1.2** | IR v2 en memoria: `foundation_operations.py`, `foundation_operator_table.py`, contrato v2, `ORDER/CARDINALITY` en `SemanticDimension` + `legacy_shadow.DIMENSION_ORDER`, `lift_v1`, `load_acceptance`, adaptación de admisión/proyección/H7/mensajes al nuevo `subject`/jobs | Todos los `tests/test_foundation_*.py` verdes; corpus de regresión: SoQL y disposiciones idénticos (INV-C0-2); replay DA-6 offline idéntico (HERA) | job huérfano aceptado; UNSPECIFIED con evidencia; GEOGRAPHY sin `level`; operador fuera de tabla; `lift_v1` que pierde COUNT; ORDER bajo OR |
| **C1.3** | `foundation_decoder_v2.py`, `PROFILES`, `LinguisticProducer(profile)`; el perfil v5 **no** es seleccionable todavía | Test cruzado de wires (INV-C0-1); forma documentada decodifica | sniffing de versión; `universe_kind` por defecto en v2 |
| **C1.4** | `foundation_operation_host.py`, `foundation_operation_owners.py`, reclamaciones por dueño en `foundation_route_owner.py`, `OperationHandoff`, hook del orquestador, `executed_owner`, códigos de ledger y mensajes. Se borran los dos `UNSUPPORTED_OPERATION` | IR oráculo sobre NEIGHBOR (en git): 100 % de la disposición §5.3.2; con la puerta simulada fuera: 0/58 secuestros con IR oráculo; INV-C0-4/5; sonda de conformidad corrida e informada | RANK_SUPPLIERS→FOUNDATION; reclamación siempre verdadera; MULTI sin detectar; la entrega muta `ctx`; segundo salto a tabular; `route_reason` desconocido tratado como conforme |
| **C1.5** | Cierres normativos en el host (`topic-k1@1`, `universe-c5@1`, H7 salta UNSPECIFIED; `modality-o6@1` si D-STATE/MOD no lo trae antes) | Suite de prompt amnésico con IR oráculo: 100 % (INV-C0-11) | quitar cada política, una por una |
| **C1.6** | `foundation_prompts_v5.py`, partición `SPLIT_v1`, adaptador del harness v2 (HERA, `scripts/audits/m1_search/`), ≤3 iteraciones sobre DEV, congelado de SHA, `foundation_v5` en `config.py` | Criterios ACTIVATE_V5 (§5.4.3) sobre TEST ×2 + DA-6 por custodio | — (evaluación) |
| **C1.7** | Freeze del candidato | Suite global pareada por nodeid frente a la base, semgrep delta con `--base`, `olympus-state candidate freeze`, ZEUS fresco (opus, no implementador) | — |

Después de C1 (mismo escritor del núcleo, en serie):

- **E-steps** (uno por dimensión, en orden de tráfico y riesgo: TIME, VALUE, STATE/MOD, GEO, BUYER,
  ORDER+CARDINALITY, ACTOR, PV): entrada en el registro como SHADOW → slice Gold de la dimensión con IR
  oráculo + captura v5 → ACTIVE con borrado de la rama en línea → proyección → mensaje. Gate por E: en
  esa fila de A3, `L-only-OK` → 0 o firmado; ensanches silenciosos 0; mutantes del enchufe.
  E-ORDER además: INV-C0-12 y el presupuesto de candidatos de `whole_token_folded` con orden por valor
  (riesgo §9).
- **G** recibe la propiedad de `foundation_producers.py` y `foundation_basic_fallback.py` en el freeze
  de C1.

---

## 7. Mapa archivo → dueño (olas C, D, E, F, G)

| Archivo | Dueño | Notas |
|---|---|---|
| `app/core/interpretation_contract.py` | NÚCLEO (C1/E) | — |
| `app/core/semantic_dimensions.py` | NÚCLEO | sólo aditivo |
| `app/core/foundation_operations.py` (nuevo) | NÚCLEO | — |
| `app/core/foundation_operator_table.py` (nuevo) | NÚCLEO | — |
| `app/core/foundation_decoder_v2.py` (nuevo) | NÚCLEO | — |
| `app/core/foundation_prompts_v5.py` (nuevo) | NÚCLEO | congelado al final de C1.6 |
| `app/core/foundation_prompts.py` | **nadie** | byte-congelado |
| `app/core/foundation_operation_host.py`, `foundation_operation_owners.py` (nuevos) | NÚCLEO | — |
| `app/core/foundation_authority_contract.py`, `foundation_evidence_window.py`, `foundation_authority_registry.py` (nuevos) | NÚCLEO | contrato congelado en C1.1 |
| `app/core/foundation_admission.py`, `foundation_projection.py`, `foundation_runtime.py`, `foundation_route_owner.py`, `foundation_universe_policy.py`, `foundation_universe_kind.py`, `foundation_messages.py`, `foundation_presentation.py`, `foundation_guard.py`, `foundation_evidence.py`, `foundation_trace_view.py` | NÚCLEO | — |
| `app/core/foundation_producers.py`, `foundation_basic_fallback.py` | NÚCLEO hasta el freeze de C1 → **G** | traspaso secuencial, nunca en paralelo |
| `app/core/legacy_shadow.py` (`DIMENSION_ORDER`), `app/core/turn_ledger.py` (códigos) | NÚCLEO | adyacentes declarados |
| `app/config.py` (una línea, `:94`), `app/core/orchestrator.py` (`:410-417`, `:440-448`) | NÚCLEO | sólo esas líneas |
| `app/core/foundation_value_direction.py` | **D-MONEY** | excluido del núcleo |
| `app/core/foundation_time_coverage.py` | **D-TIME** | excluido del núcleo; migra a la ventana |
| `app/core/host_authorities/__init__.py` | NÚCLEO | vacío |
| `app/core/host_authorities/time.py` + `temporal_semantics.py` | D-TIME | — |
| `app/core/host_authorities/value.py` + `money_polarity.py` (léxico único) | D-MONEY | `money_polarity` lo usa también el sidecar A2: paridad obligatoria |
| `app/core/host_authorities/state.py`, `modality.py` + `estado_families.py` | D-STATE/MOD | `modality_resolver.py` no se toca (A2: sano) |
| `app/core/host_authorities/geography.py` + `municipal_territory.py`, `colombia_geography.py`, datos municipales | D-GEO | — |
| `app/core/host_authorities/buyer.py` + `entity_resolver.py`, `aliases_db.json`, `buyer_source_names.json` | D-BUYER | — |
| `app/core/host_authorities/actor.py` + `actor_alias_resolver.py`, `actor_alias_index.py`, `actor_identity.py` | D-ACTOR (METIS → ARES) | — |
| `app/core/host_authorities/topic.py`, `contract_type.py`, `order.py` | NÚCLEO (E) | extraídos de la rama en línea |
| `app/core/product_view_policy.py`, `product_view_announcement.py`, `app/core/handlers/foundation_view.py` | **F** | el núcleo los conecta |
| `app/core/foundation_failover.py` (nuevo) | **G** | — |
| `tests/foundation_authority_kit.py`, `tests/test_foundation_*.py` | NÚCLEO | — |
| `tests/test_host_authority_<dim>.py` | carril D de la dimensión | — |
| `scripts/audits/m1_search/*` (adaptador v2, split, sonda extendida) | HERA | fuera de `app/` |

---

## 8. Irreversibilidad y fronteras de confianza

| Cambio | Tipo | Mitigación |
|---|---|---|
| El LLM decide la operación y con ello a qué dueño se ofrece el turno | **Frontera de confianza nueva** | Sólo dueños de una tabla cerrada; el dueño debe reclamar; sin segundo salto; ningún valor del LLM se ejecuta; ACTIVATE exige ≤1 secuestro y CUTOVER 0. |
| `SCHEMA_VERSION 2` en trazas y ledger | Persistencia: tras activar `foundation_v5` hay registros v2 que el código v1 no lee | `load_acceptance` y `lift_v1` permanentes hasta M4; el rollback a `foundation_v3`/`legacy` no lee esos registros para ejecutar (sólo trazas). |
| Contrato de autoridad congelado en C1.1 | Coste alto de cambio (seis carriles dependen de él) | Revisión de METIS antes de integrar C1.1. |
| Partición DEV/TEST del Gold | Irreversible metodológicamente: una vez vista, una fila TEST no vuelve a ser TEST | Semilla y lista fijadas antes del prompt; acceso a TEST sólo para HERA. |
| ACTOR en SEARCH (E-ACTOR) | Datos personales (cédulas de personas naturales) | La misma política que Actor Profile (ADR-060); el ledger ya redacta; decisión P9. |
| Borrado de la puerta (M1-I, fuera de C1) | Producto visible + disponibilidad (todo turno depende del proveedor) | Precondiciones §5.3.6; G antes. |

---

## 9. Riesgos y rollback

| Riesgo | Señal | Respuesta |
|---|---|---|
| El grafo legacy ejecuta otro dueño tras una entrega (R3) | `HANDOFF_CONFORMANCE_VIOLATION` > 0 en la sonda | Ajustar la reclamación del dueño; si no basta, adelantar la opción C (§12). |
| v5 confunde operación (p. ej. «proveedores de aseo», N010) | Exactitud < 95 % o secuestros | ≤3 iteraciones; después STOP; la puerta se queda. |
| La ventana K=4 no ve un calificador lejano | Casos Gold con ensanche | Falla hacia aclarar (declarado, igual que H0); subir K es un cambio de contrato (METIS). |
| `lift_v1` cambia el significado de v4.2 | INV-C0-2 rojo | Bloquea C1.2. |
| El host v2 cambia el comportamiento de `foundation_v3` (correcciones fail-closed compartidas) | Diff del corpus de regresión | Declarado; replay DA-6 de `foundation_v3` bajo el host v2 como gate de C1.2/E. |
| ORDER por valor con `whole_token_folded` (el tope de candidatos se toma por fecha) | Orden falso | E-ORDER: el orden va en la SoQL de candidatos; si no cabe, `ORDER_WITH_TOKEN_FILTER_UNSUPPORTED` (aclara). |
| Fallback H4 que quita el ORDER BY | Orden presentado como semántico | INV-C0-12. |
| STATE dependiente del dataset frente a PV2 | Vista equivocada | `StateValue.canonical_class` sin dataset + `STATE_NOT_IN_VIEW`. |
| Coste/latencia del prompt v5 (más largo, salida con jobs) | p90 o truncados | Criterio ACTIVATE; `max_tokens` lo revisa G. |
| La sesión de otro agente toca el worktree (memoria) | Commits ajenos | `git log` antes de cada freeze. |

**Rollback**: `SECOPPAL_FOUNDATION_MODE=foundation_v3` (v4.2 bajo el host v2) o `legacy` (paridad con
`main`, ADR-102). Cada paso C1.x es un commit revertible; C1.1 no cambia el runtime.

---

## 10. Condiciones de STOP

1. Un carril D-* necesita cambiar `foundation_authority_contract.py` después de C1.1 → METIS.
2. La sonda de conformidad muestra violaciones que la reclamación del dueño no puede evitar sin editar
   módulos de un dueño legacy → METIS (opción C).
3. v5 no pasa DEV en la tercera iteración, o regresa DA-6 → NO_GO con informe; sin bajar umbrales.
4. INV-C0-2 rojo y no explicable por un defecto de `lift_v1` → METIS.
5. Hace falta cambiar algo de lo que dice `foundation_prompts.py` o relajar un validador congelado
   (memoria: guardas de bytes) → STOP y declarar.
6. En dogfood: una sola violación de conformidad o un secuestro con `foundation_v5` activo → volver a
   `foundation_v3`/`legacy` y avisar.
7. Una decisión de §11 cambia un umbral o una fila de la tabla §5.3.2 → actualizar este documento antes
   de seguir.

---

## 11. Decisiones que no puedo tomar (producto), con recomendación

| Id | Pregunta exacta | Recomendación |
|---|---|---|
| P1 | Una operación sin dueño que la reclame en M1 (ranking de proveedores sin Natural Alpha, seguir/alertar en lenguaje natural, exportar con consulta propia, preguntas conceptuales): ¿mensaje explícito «todavía no hago esto aquí» con guía, o seguir dándole al usuario la búsqueda tabular legacy (que según el Gold responde otra cosa)? | **Mensaje explícito con guía** (ADR-069). No es regresión frente al Gold, pero el usuario pierde una respuesta, aunque sea mala. |
| P2 | MULTI con un job SEARCH («busca contratos de aseo en Cali y avísame»): ¿ejecutar la búsqueda y avisar de lo que no se hizo, o pedir «hazlas por pasos» sin ejecutar? | **«Por pasos», con un botón por job** en M1 (el mismo comportamiento que Natural Alpha hoy; una sola regla). |
| P3 | Superlativo en singular («el contrato más grande de la Alcaldía de Cali»): ¿lista ordenada por valor o limitarla a 1? | **Enmendada (ADR-103 enmienda 1, veto del asesor alineado con el Gold N012/R0507):** singular → `ORDER` + `CARDINALITY 1`; plural sin cifra → lista ordenada; cifra explícita → `CARDINALITY N` (≤100). |
| P4 | Activar `foundation_v5` en dogfood tras ACTIVATE_V5 | Etapas de ADR-102 (dogfood Rodrigo → canary → general), cada una con autorización expresa. |
| P5 | Enmendar ADR-102 §2/§4 a perfiles cerrados (`legacy`, `foundation_v3`, `foundation_v5`) | **Sí**; es la condición para tener v4.2 seleccionable como rollback. |
| P6 | En `foundation_v5`, ante caída del proveedor: ¿D1 («no pude interpretar ahora», sin legacy) o la búsqueda básica rotulada de ADR-102 §9? | **D1** (es la decisión posterior); se implementa en G y se registra en la ADR. |
| P7 | Botones de la Product View en M1: ¿sólo las vistas alternativas (Procesos históricos / Contratos / Oportunidades vigentes) como evento, dejando `[Elegir período]` y `[Mayor valor]` para M2 (necesitan un delta de IR)? En canales de texto, ¿opciones numeradas? | **Sí a ambas**: en M1, las vistas; en M2, período y orden. En texto, opciones numeradas con el protocolo de selección existente. |
| P8 | Tope de `top_n` («los 500 más caros») | **Máximo 100, anunciado** («te muestro los 100 de mayor valor»); por encima, aclarar. |
| P9 | SEARCH filtrado por persona natural (nombre o cédula) | **La misma política que Actor Profile** (ADR-060); sin política nueva. |

---

## 12. Qué haría cambiar la decisión

- **Conformidad de la entrega por encima del 2 % en la sonda, o violaciones que piden editar un
  dueño** → adelantar la opción C: invocar el ejecutor del dueño con la IR tipada (manifiestos de M3)
  sólo para la operación que falla, empezando por PROFILE_ACTOR, y borrar su detector.
- **v5 no alcanza la exactitud de operación** con un prompt único → mantener la puerta y los vetos más
  allá de M1 y no añadir un segundo LLM clasificador (rompería el invariante 1 salvo que sólo pudiera
  retirar autoridad). En ese caso, un clasificador auxiliar sólo puede *rechazar* SEARCH, nunca decidir
  el dueño.
- **Las autoridades D-* necesitan contexto más allá de K tokens** con frecuencia → revisar la ventana
  (por ejemplo, la cláusula entera de la cita) antes de que haya más carriles conectados.
- **El Gold muestra que el universo `UNSPECIFIED` con PV6 engaña** (producto, cola A6) → la tabla PV es
  datos; cambiarla no toca la arquitectura.

---

## 13. Non-goals

- Ejecutar cualquier operación que no sea SEARCH desde Foundation (M3).
- Conversation IR, deltas, «y en 2025?», `[Mayor valor]`/`[Elegir período]` (M2).
- Fusionar Natural Alpha o tocar `capability_binding` (M3).
- Borrar la puerta, los vetos o `continues_foundation_turn` (M1-I, con §5.3.6).
- Elegir el proveedor de failover (G/A5).
- Eco general de filtros para cualquier condición (capa 6 del roadmap); en C1 sólo hay eco de vista.
- Unificar los 14 léxicos de sustantivos de universo (A4 §5): deuda aparte. La IR v2 lo hace
  innecesario en Foundation, pero no en legacy.
- Editar `foundation_prompts.py` o cualquier ejecutor legacy.

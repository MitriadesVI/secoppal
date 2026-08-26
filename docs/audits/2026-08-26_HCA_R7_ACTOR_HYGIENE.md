# HCA-R7 — Actor Hygiene V1

Fecha: 2026-08-26. Rama: `feature/hca-r7-actor-hygiene`.

**Actualización de integración:** `23f307c` integrado ff-only en main;
`HCA_R7_INTEGRATION_REVIEW: PASS`, sin push. Véase
[revisión y cierre](2026-08-26_HCA_R7_INTEGRATION_REVIEW.md).
El resto de este informe conserva la evidencia y el estado histórico de entrega
de la rama, anteriores a esa integración.
Base canónica: `39234f873fb345b9de72ded7092afacadfdd646c` (docs-only R6,
autorizado tras detectar el preflight sucio). Integración funcional R6:
`8bd5467aa7aa203d55605a758eedc0bf18ca33b5`.

Estado: **HCA_R7_ACTOR_HYGIENE: PASS · ACTOR_HYGIENE_V1: CLOSED en rama**,
pendiente de integration review. Sin merge ni push. No se modifica Actor Profile,
normalización N2, autoridad exact-only, identificadores ni `query_plan.py`.

## Autoridades reconstruidas

| STAGE | AUTHORITY | INPUT | OUTPUT | CAN_AUTO_RESOLVE | CAN_CREATE_PENDING | HCA_RISK |
|---|---|---|---|---|---|---|
| Dispatch | `SecopalWorkflow._dispatch` | raw query / callback | reset → actor selection → radar → pagination → suggestion → lookup → normal flow | No | No | selección antes de parsing |
| Selection | `handle_actor_selection` | rango y token / texto; último pending, TTL, versión | frame original + identidad elegida; continuación normal | No (elección explícita) | No | selección inválida o stale |
| Detection | `QueryRouter`, `actor_intent`, `actor_conversation` | residual positivo tras binding de negación | rol, nombre, documento o exact probe | Solo documento tipado | No | inferir actor por contenido de búsqueda |
| Context | `apply_context`, `FollowupClassifier`, `detect_and_merge` | frame actual + anterior | new_search o mutación validada | No | Hereda, no genera | pending heredado secuestra búsqueda |
| Bare arbitration | `actor_arbitration`, acción `resolve_actor_name` | raw + parámetros; nombre completo y cola reconocida | sonda exacta; candidatos solo con evidencia independiente | No por heurística | Mediante proyección | casing / cola monetaria mal recortada |
| Exact resolution | `ActorNameResolver.resolve_exact` | N2, variantes legales deterministas | RESOLVED / AMBIGUOUS / UNRESOLVED / INDEX_UNAVAILABLE | Exact unique únicamente | No | perder frontera completa del nombre |
| Candidate retrieval | `ActorNameResolver.resolve` | exact miss autorizado | FTS5 trigram/BM25 → CANDIDATES | **No** | No | token literal + BM25 + top-K |
| Pending projection | `project_actor_resolution` | resultado del resolver + frame | candidatos, frame original, versión, nonce | Solo resultado exacto | Sí | confundir candidato con identidad |
| Execution policy | `evaluate_actor_scope` vía plan | estado actor | bloquear/aclarar o ejecutar | No | No | dejar pasar fuzzy a perfil |
| Continuation | profile/search execution, normal-flow persistence | frame autorizado | SoQL, respuesta, último frame y buffer | No | Persiste resultado | residuos de pending en siguiente turno |

## Contrato mínimo

- `INV-ACTOR-EVIDENCE-MUST-BE-SEMANTIC`: casing nunca autoriza actor. Rol,
  documento, alias exacto y nombre desnudo sin señales de búsqueda son evidencias
  distintas. `STRONG_SEARCH_INTENT > BARE_ACTOR_HEURISTIC`; un alias exacto real
  puede coexistir con filtros estructurados.
- `INV-FUZZY-NEVER-DECIDES-IDENTITY`: `EXACT > FUZZY`. Fuzzy solo recupera y
  ordena candidatos. `FUZZY_CANDIDATE_RECALL: CAN_EXPAND_CANDIDATES`;
  `FUZZY_IDENTITY_AUTHORITY: FALSE`. Compatibilidad con la autoridad AP3-B/AP3-F
  documentada en `docs/architecture/ACTOR_PROFILE_V1.md`; no ADR duplicado.
- `INV-ACTOR-PENDING-CONSUMES-ONLY-PLAUSIBLE-SELECTION`: clasificación mínima
  `PLAUSIBLE_ACTOR_SELECTION / NEW_SEARCH / AMBIGUOUS`. Números/ordinales y
  nombres exactos inequívocos de la lista seleccionan; deícticos sin referente
  único aclaran. Una búsqueda completa cede al merger new_search existente.
- `INV-ACTOR-PENDING-YIELDS-TRANSACTIONALLY`: se reemplaza el frame completo,
  nunca se hace clear parcial de actor. El nonce viejo deja de ser seleccionable
  cuando se persiste la búsqueda nueva. Reset conserva su lifecycle.
- Negación ACTOR sigue fail-closed antes de resolver; índice ausente no abre
  fallback, red ni LLM. `query_plan.py`: NEXT_TOUCH_RULE no invocada.

## Línea base y causas

HCA congelado: **175 PASS / 20 FAIL / 0 ERROR**, reproducido offline. SHA256:
`262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`.
Índice real: 662409216 bytes, SHA256
`464add1dbb479e616ee80a079b11c256db0d67dc6b979e4f285612233bdb8690`;
apertura `mode=ro&immutable=1`, `query_only=ON`.

1. **UPPER:** `has_entity_like_name_evidence` usaba `isupper`/Title Case.
   Además, la cola `por más de 100 millones` dejaba `por` huérfano y la mención
   terminaba en `aseo en barranquilla por`; se perdía ciudad al limpiar fugas.
2. **PENDING:** selección solo consumía rango/ordinal. El fallo no estaba ahí:
   `change_scope`/`change_year`/`refine_filter` heredaban `actor_pending` y el
   plan volvía a imprimir candidatos. `new_search` ya tiene reemplazo atómico.
3. **TYPO:** `"lmp" OR "ingieneria"` sobre tokenizer trigram exige que cada
   token citado sea contiguo; no hace edit-distance. BM25 premia coincidencias
   parciales cortas. Con **este** índice, LMP no desaparece del top10: baja de
   rank 1 a **7** (`ingeneria`: 5, `ingeniera`: 7). La desaparición histórica
   total no se reprodujo en esta base; no se confunden ambos resultados.
   Clases: **FTS_QUERY → RANKING / CANDIDATE_POOL**, no normalización N2.
   Remediación acotada: pool FTS mayor antes del top10; cobertura de tokens con
   un ancla literal y un typo pequeño. Ningún scan fuzzy de 1,2M identidades.

Los nombres completamente desnudos (`FUNSOCOM`, `jorge verbel`, UT y LMP)
no entraban al actor flow basal: la sonda solo admitía `contratos de X`.
La ampliación R7 se restringe a nombres sin intención estructurada de búsqueda.
La forma de nombre personal necesita además cobertura de palabras completas
en un alias recuperado; una coincidencia de substrings no basta. Las palabras
de dominio y los comandos reconocidos no adquieren autoridad por casing.

La medición bloqueó sockets externos. Detectó que el runner mockeaba query y
count pero no aggregate y que `.env` podía reactivar el extractor LLM. Se
completan esos stubs en el runner existente, sin tocar el contrato de stores.

## Diseño implementado y límites

- **Routing:** se elimina `isupper`/Title Case de la evidencia. Las formas
  societarias pueden ser internas (`LA PREVISORA S.A. COMPAÑÍA DE SEGUROS`),
  pero un sufijo suelto no constituye un nombre. La puerta `busca X` reutiliza
  el mismo criterio semántico; `busca aseo en Barranquilla` conserva la búsqueda.
- **Fronteras:** el consumidor de dinero incorpora la preposición inmediata
  usando `_append_span` existente; no cambia Money V2. Tras un alias exacto,
  `en Barranquilla` puede recuperar la ciudad aunque el parser genérico también
  produzca `city_to_entity`: esa proyección derivada se descarta, sin inferir
  una relación comprador-actor. Los compradores nombrados siguen impidiendo
  un corte indebido del nombre completo.
- **Pending:** NEW_SEARCH usa `detect_and_merge` y su limpieza completa.
  La selección textual compara nombres exactos de la lista actual y exige un
  único rango; conserva TTL, versión y nonce. Un nombre de candidato que tenga
  forma de búsqueda completa no captura el turno. Un documento explícito pasa
  al parser de identificadores; sus dígitos no se interpretan como rango.
  Respuestas ambiguas conservan el frame original sin aplicar deltas.
  Payloads malformados o duplicados no producen selección textual ni excepciones.
- **Control:** no existe paginación de candidatos. `muéstrame más` conserva
  `pagination_no_context`, sin consumir la lista; después aún sirve `1`.
  `muéstrame más contratos de aseo en Barranquilla` sí reemplaza el pending.
- **Recall:** FTS5/BM25 recupera hasta **100 identidades distintas**, conservando
  colapso de aliases antes del límite. Se ordena por cobertura total de tokens
  y luego BM25/alias_id; se devuelven como máximo **10**. Un token literal de
  al menos 3 caracteres sirve de ancla; como máximo un token admite typo:
  1 edición en 5–6 letras, 2 desde 7 letras, comparación acotada a 64 caracteres.
  Sin ancla literal o con varios tokens dañados no se amplía la búsqueda.
  Exactos/variantes legales/ambigüedad exacta terminan **antes** del fuzzy.
  N2, SQLite, tokenizer, esquema y documentos no cambian.

## Casos y relación entre IDs

Los IDs de defectos del informe histórico no son aliases de IDs del corpus.

| CASE_ID | QUERY / SEQUENCE | BEFORE | AFTER | STATUS |
|---|---|---|---|---|
| HCA-ACTOR-UPPER-001 (defecto) | `CONTRATOS DE ASEO EN BARRANQUILLA POR MÁS DE 100 MILLONES` | candidatos UT; ciudad perdida | búsqueda con aseo, Barranquilla y >100M | CLOSED |
| HCA-META-002B (corpus) | mismo texto, comparación con HCA-META-002A | FAIL | PASS | corregido |
| HCA-ACTOR-PENDING-001 (defecto) | `buscate al proveedor jorge verbel` → `contratos de aseo en barranquilla en 2025` | lista vieja heredada | búsqueda independiente, actor/nonce viejos no seleccionables | CLOSED |
| HCA-ACTOR-011 (corpus) | secuencia anterior exacta | FAIL | PASS | corregido |
| HCA-ACTOR-TYPO-001 (defecto) | resolver `lmp ingenieria` / `lmp ingieneria` | rank 1 / 7 con índice actual | rank 1 / 1, ambos CANDIDATES | recall/ranking cerrado; ausencia histórica total no reproducida |
| HCA-ACTOR-004 (corpus) | `contratos de lmp ingieneria en 2025` | FAIL: exige `actor_scope`; candidato LMP rank 7 | FAIL: misma exigencia; candidato LMP rank 1 | discrepancia de oráculo preservada; focal AH-22 y tests sí fijan NIT/rank sin autorizar identidad |

HCA con el mismo arnés offline en una exportación de main y en R7:
**175/20/0 → 177/18/0**, 2 corregidos, **0 rotos**, comparación caso a caso.
El corpus HCA original permanece intacto.

`HCA-ACTOR-004` declara en sus notas que espera candidatos, pero su aserción
exige `actor_scope`. Los resultados crudos, antes y después, muestran
`CANDIDATES`, `actor_pending` y ninguna identidad autorizada. La mejora de
ranking no debe fabricar un scope para poner ese caso verde. Se registra para
revisión de oráculo posterior; no se cambia su resultado ni el corpus.

## Adversarial y correcciones durante el sprint

`scripts/audits/generate_actor_hygiene_variants.py` genera **242 casos** para
el runner existente (no contiene otro ejecutor). Corrida final: **242 PASS**,
0 FAIL, 0 ERROR. Distribución:

| Clase | Casos | Fallos finales |
|---|---:|---:|
| SEARCH_TO_ACTOR_HIJACK (incluye casing) | 144 | 0 |
| PENDING_NEW_SEARCH_HIJACK | 18 | 0 |
| ACTOR_TO_SEARCH_FALSE_NEGATIVE | 27 | 0 |
| TYPO_RECALL_FAILURE / FUZZY_AUTO_RESOLUTION | 27 | 0 |
| Actor + tiempo/dinero/geografía/modalidad/orden | 15 | 0 |
| NEGATIVE_ACTOR_POSITIVE_LEAK | 9 | 0 |
| Identificadores después de pending | 2 | 0 |

La primera ampliación de name-like produjo 16 regresiones HCA: corregidas
restringiendo esa evidencia a consultas realmente desnudas y reutilizando el
vocabulario de dominio. El primer barrido adversarial encontró 75 fallos en la
puerta preexistente `busca X` con filtros: corregidos con la evidencia compartida.
La auditoría de negación encontró `quita la exclusión` capturado como nombre:
corregido delegando el reconocimiento al control existente. Los tests fijan
estas fronteras; no se modificó ningún oráculo HCA/negación/control/tiempo.

La fixture focal nueva usa **36 casos**, 36 PASS, 0 FAIL, 0 ERROR. Durante su
registro se corrigió un error del propio oráculo: `contratista` no es un string,
sino la lista de variantes documentales de compatibilidad. Ahora verifica
`actor_identity` tipada, sin tocar el comportamiento de identificadores.
SHA256 final: `217df608a9a5dfb0abd1c89cda27d1c79f04a3000d9086457eb54dc916418698`.
SHA256 variantes: `70119ed7dce82c4bd8fe9d22abb4d4a20825088427d57620366129e939992e09`.

## Reproducción

```bash
make audit-actor-hygiene-v1 AUDIT_OUT=/tmp/r7-actor
.venv/bin/python3 scripts/audits/generate_actor_hygiene_variants.py --out /tmp/r7-variants.yaml
.venv/bin/python3 scripts/audits/run_conversation_holistic_audit.py --corpus /tmp/r7-variants.yaml --out /tmp/r7-adversarial
.venv/bin/python3 scripts/audits/run_conversation_holistic_audit.py --out /tmp/r7-hca
```

El índice real se abre immutable; no se escribe ni se reconstruye. Los stores
escribibles quedan dentro del sandbox temporal del runner y se borran al salir;
`isolation.json` confirma `outside_sandbox=[]`. Se mockean query/count/aggregate
de SECOP y el narrador; el extractor LLM se desactiva incluso si `.env` tiene
clave. No hay resultados de Socrata ni afirmaciones de fuente viva en este sprint.
La validación final añade `sandbox-exec -p '(version 1) (allow default) (deny network*)'`
en macOS como bloqueo externo de red. Un primer bloqueo vía `sitecustomize`
contaminaba el test de imports puros por importar `socket`: se descartó ese
instrumento, sin tocar el test ni el módulo de identidad.

Artefactos completos de esta ejecución: `/tmp/secoppal-hca-r7.N29YnA/`
(`hca-before-final`, `hca-verified`, `actor-verified`, `adversarial-verified`, `control`,
`time`, `negation-final`, logs de pytest/corpus y benchmarks).

## Gates finales y rendimiento

Medición en la misma máquina, secuencial, sin pytest ni otras auditorías en
paralelo; exportación de main frente a R7, índice idéntico, 5 warmups + 40
muestras por consulta. Incluye conexión read-only y verificación del índice.
P95 por rango más cercano. Milisegundos:

| Consulta | Antes p50 | Antes p95 | Después p50 | Después p95 | CANDIDATE_COUNT |
|---|---:|---:|---:|---:|---:|
| Exact: FUNSOCOM (`resolve_exact`) | 0,165 | 0,270 | 0,163 | 0,218 | 0 |
| Candidate: lmp ingenieria | 30,055 | 31,482 | 30,794 | 31,764 | 10 |
| Typo: lmp ingieneria | 9,430 | 13,368 | 9,551 | 10,572 | 10 |
| Ambiguo: jorge verbel | 70,589 | 83,876 | 68,027 | 78,132 | 10 |

Sin regresión material observada. No es un benchmark de carga ni una garantía
de latencia en producción. `LMP INGENIERIA SAS` exacto no cambia; `lmp ingenieria`
sin sufijo sigue CANDIDATES, rank 1. El typo pasa de rank 7 a 1. Fuzzy jamás
devuelve una identidad resuelta. Hash del índice idéntico antes/después.

| Validación final | Resultado |
|---|---|
| pytest completo, red bloqueada | **8641 passed, 2 skipped, 24 xfailed**, 215,00 s |
| Focal actor/perfil/negación/control/tiempo/dinero/modalidad/follow-up | **813 passed, 1 xfailed** |
| Última revisión de pending, incluidos 3 controles de entrada vacía | **74 passed** (subconjunto, no sumar al completo) |
| Query corpus, antes/después | **89/89 → 89/89** |
| Dialogue corpus, antes/después | **60/61 → 60/61**; único FAIL `GOB-DEPTO-001` |
| Actor fixture | **36/36**, 0 ERROR |
| HCA original | **175/195 → 177/195**, 0 ERROR, 0 regresiones |
| Variantes adversariales | **242/242**, 0 ERROR |
| Control Language V1 | **42/42** |
| Time V2 | **32/32** |
| Negation V1 | **22/24**; mismos `NEG-N15`, `NEG-ANTI-002` basales |
| Ruff archivos Python cambiados (`E,W,F`, salvo `E501`) | PASS |
| `make lint-core` / `git diff --check` | PASS / PASS |
| `query_plan.py` | **Sin cambios, 299/300 LoC**; NEXT_TOUCH_RULE no invocada |

Comando de la suite final:

```bash
DEEPSEEK_API_KEY='' PYTHONPATH=/Users/rodrigoortiz/Documents/secoppal \
sandbox-exec -p '(version 1) (allow default) (deny network*)' \
.venv/bin/python3 -m pytest tests/ -q
```

Gates observados en los sentinels y barridos ejecutados:

| Gate | Resultado |
|---|---|
| SEARCH_UPPERCASE_AS_ACTOR / NEW_SEARCH_HIJACK | **0 / 0** |
| UPPER_LOWER_INTENT_EQUIVALENCE / BARE_REAL_ACTOR | **PASS / PASS** |
| VALID_SELECTION / PENDING_YIELDS_TRANSACTIONALLY | **PASS / PASS** |
| LMP_INGIENIERIA_CANDIDATE_PRESENT | **YES**, NIT 900739049, rank 1 |
| LMP_INGENIERIA_EXACT_BEHAVIOR | **UNCHANGED**; nombre legal exacto resuelve, parcial sigue candidato |
| FUZZY_AUTO_RESOLUTIONS / EXACT_DISPLACED | **0 / 0** |
| INDEX_UNAVAILABLE / CC_NIT | **UNCHANGED**; sentinels de resolver/workflow verdes |
| NEGATIVE_ACTOR_POSITIVE_LEAKS | **0** |
| Regresiones control / negación / tiempo / dinero / modalidad / follow-up | **0 / 0 / 0 / 0 / 0 / 0** |

## Diferidos y entrega

HCA conserva 18 FAIL: `HCA-TOPIC-007`, `HCA-TOPIC-012`, `HCA-MODALITY-002`,
`HCA-MODALITY-012`, `HCA-ENTITY-001`, `HCA-ENTITY-004`, `HCA-ENTITY-005`,
`HCA-MONEY-020`, `HCA-MONEY-026`, `HCA-ACTOR-004`, `HCA-STATE-003`,
`HCA-STATE-005`, `HCA-STATE-007`, `HCA-TYPO-003`, `HCA-FOLLOWUP-007`,
`HCA-FOLLOWUP-016`, `HCA-META-001B`, `HCA-SOURCE-002`.

Fuera de R7 permanecen `GOB-DEPTO-001`, pérdida de topic en `change_scope`
sin pending actor, `CONTROL_LIMIT_SEMANTICS`, revisión de oráculos
`HCA-FOLLOWUP-016`/`NEG-N15`/`NEG-ANTI-002`, Semantic Authority Cleanup,
`morphological_variants`, relaciones actor/comprador, state family y formatter
general. No se abre AP v2 ni se implementa Actor Relationships. Se añade la
discrepancia documentada de `HCA-ACTOR-004` a la revisión posterior de oráculos.

Archivos R7 (19; no incluye el commit docs-only R6 de base):

```text
Makefile
app/core/actor_arbitration.py
app/core/actor_conversation.py
app/core/actor_intent.py
app/core/followup_engine.py
app/core/workflow/actions/actor_resolution.py
app/core/workflow/actions/parsing.py
app/core/workflow/handlers/actor_selection.py
app/data/actor_alias_resolver.py
scripts/audits/run_conversation_holistic_audit.py
scripts/audits/generate_actor_hygiene_variants.py
tests/test_actor_conversation.py
tests/test_actor_conversation_dogfood.py
tests/test_actor_name_resolver.py
tests/test_actor_resolution_orchestration.py
tests/test_audit_runner_isolation.py
tests/fixtures/audits/actor_hygiene_v1.yaml
docs/audits/2026-08-26_HCA_R7_ACTOR_HYGIENE.md
docs/documento_tecnico_secoppal_arquitectura_bitacora.md
```

Entrega en un commit R7: `fix(actor): harden actor routing and candidate recall`.
Main conserva la base docs-only `39234f873fb345b9de72ded7092afacadfdd646c`.
Los untracked ajenos se preservan; staging mediante lista explícita de archivos.

**MERGE: NO · PUSH: NO · NEXT: READY_FOR_HCA_R7_INTEGRATION_REVIEW**.

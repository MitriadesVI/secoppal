# M2A-3 — Forma canónica de `AcceptedInterpretation` (R4: orden observable frente a set-like)

**Fecha:** 2026-10-02. **Rama:** `mission/m2a-3-reducer` (base Paso 0 `fa0b6de8`). **Autoridad:** contrato
`docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` (§3.2, §4.1, §4.2, §4.3, §12, INV-M2A-06); decisiones del usuario en
`M2A_WAVE1_PREP.md` §4 y del 2026-10-02 sobre la fase 1 (§6 abajo). Este documento **no** cambia el contrato ni el Gold.

Regla de partida: **nada se ordena a ciegas**. Un orden se canonicaliza sólo si se demuestra, con la proyección real
(`app/core/foundation_projection.py`), que no se ve en `soql`, en `plan_hash`, en el orden de los grupos ni en el efecto
de las políticas. Precedente de 1a.1: el tipo **rechaza** la forma no canónica; nunca reordena al leer. Los
constructores (`compose` y el futuro `from_acceptance`) emiten la forma canónica con **una sola** función compartida:
`foundation_accepted.canonical_interpretation`.

Evidencia ejecutada y reproducible: `tests/test_m2a3_order_observability.py` (11 pines sobre la proyección real; las
aceptaciones se construyen en memoria y nada llama a la fuente).

## 1. Veredicto por colección

| Colección | Qué se ve (evidencia) | Regla vigente |
|---|---|---|
| `conditions` | **El orden dentro de TOPIC/POSITIVE se ve; el orden entre ranuras, no.** Las 720 permutaciones de 6 condiciones dan exactamente 6 clases de `soql`/`plan_hash`, una por orden relativo de las TOPIC/POSITIVE (`test_condition_order_is_observable_exactly_through_the_topic_positive_terms`). Ningún orden entre ranuras cambia nada (`test_cross_slot_order_is_unobservable_with_every_projected_dimension`). La lista `foundation_topic_terms` sigue el orden de las condiciones y entra en el WHERE y en los recibos de `plan_hash` | **Agrupadas por ranura** según la tabla cerrada y explícita `CONDITION_SLOT_ORDER`, en el orden en que §4.2 lista las ranuras y nunca alfabético. **Dentro de una ranura se conserva el orden de la aceptación.** Agrupar nunca mueve `soql` ni `plan_hash` (`test_the_canonical_slot_grouping_never_moves_soql_or_plan_hash`, 720 órdenes). El tipo y el lector rechazan `NONCANONICAL_CONDITION_ORDER` |
| `requests` (ancla dual) | **Se ve el orden de los grupos.** El `plan_hash` por petición no cambia, pero el manifiesto y los `groups`/`rows` de `foundation_runtime.execute_accepted` siguen el orden de las peticiones (`test_request_order_is_the_group_order_of_a_dual_anchor`) | **Sin orden canónico**: se conserva el dado. `compose` nunca las permuta: pasan tal cual, o `REPLACE_UNIVERSE` con ancla dual deja una sola |
| `policies` | **No se ve**, salvo que un `policy_id@version` lleve dos efectos: la proyección mapea ref → efecto y el último gana en `plan_hash` (`test_one_ref_with_two_effects_makes_policy_order_observable`). Con efectos únicos, las 6 permutaciones dan 1 plan | **Forma partida**: un registro por aplicación (política, **una** condición), como emite `main`. Registros ordenados por `(policy_id, version, condition_ids, effect)`. El tipo rechaza `MULTI_CONDITION_POLICY`, `FOREIGN_POLICY_CONDITION`, `CONFLICTING_POLICY_EFFECT`, `DUPLICATE_POLICY` (incluye `(p1, p1)`, independiente del orden) y `UNSORTED_POLICIES` |
| `logic` | Las formas de un AND (orden, anidamiento, `None` frente a AND explícito) no se ven: 9 formas dan 1 plan. OR no se ejecuta (`UNSUPPORTED NONCONJUNCTIVE_EXPRESSION`), así que nada de lo que cuelga de OR/NOT está probado | **Sólo lo que decide el contrato §3.2: conjunción pura ⇒ `logic = None`.** Cualquier árbol con OR/NOT se conserva **tal cual**, sin aplanar ni ordenar. El tipo y el lector rechazan `NONCANONICAL_LOGIC`, que hoy sólo es una conjunción pura escrita como árbol |

**Garantía limitada declarada (logic).** «Mismo significado ⇒ mismos bytes» **no** se garantiza dentro de un árbol con
OR/NOT mientras no exista un consumidor que pruebe qué reescrituras son seguras. Dos árboles OR/NOT equivalentes pueden
persistirse con bytes distintos.

Sin cambios respecto de 1a.1: `request_ids`, `receipt_rules` y `policies` de una condición, y `answer_slots`, siguen
siendo listas ordenadas y únicas. `condition_ids` de una política persistida tiene ahora exactamente un elemento.

Fuera de este paso, porque no se ordenan (R6): `candidate_patches` (es el orden de presentación, M2A-4), el eco y
`ResultSetRef.groups`. Las `ops` de un parche **no** se ordenan: `compose` tendrá que ser invariante a su permutación
(propiedad en la fase 2). Su forma canónica queda como deuda del productor (M2A-5).

## 2. La tabla de ranuras (`CONDITION_SLOT_ORDER`)

Es la tupla constante del orden en que §4.2 lista las ranuras de condición:

TOPIC±, BUYER±, ACTOR±, GEOGRAPHY±, TIME±, STATE±, MODALITY±, CONTRACT_TYPE_OR_NATURE± (POSITIVE antes que NEGATIVE),
`VALUE/lower`, `VALUE/upper`, `ORDER`, `CARDINALITY`.

- `test_the_slot_table_lists_every_condition_slot_of_4_2_exactly_once` fija que cada ranura de condición de §4.2
  aparece exactamente una vez, en ese orden, que no es alfabético, y que toda ranura que un valor tipado puede ocupar
  tiene rango.
- `UNIVERSE` es la ranura de §4.2 a nivel de petición: ninguna condición la ocupa (`slot_of` nunca la devuelve), así que
  no tiene rango.
- Por qué esto no rompe INV-M2A-06: el único orden observable es el de dentro de TOPIC/POSITIVE, y la agrupación es
  estable dentro de cada ranura (pin de agrupación). `from_acceptance` (M2A-1b) aplica `canonical_interpretation` sobre
  las condiciones en el orden de la aceptación.
- `compose` no puede crear un orden nuevo dentro de TOPIC/POSITIVE: esa ranura pasa intacta, la sustituye **un** `SET`
  (dos son `DELTA_SLOT_CONFLICT`) o se borra.

## 3. Lo implementado (`app/core/foundation_accepted.py`)

- `CONDITION_SLOT_ORDER` y `canonical_conditions`. Esta última es estable dentro de cada ranura y nunca ordena por id
  ni por valor. `AcceptedInterpretation` rechaza `NONCANONICAL_CONDITION_ORDER`.
- `_canonical_policy` rechaza `MULTI_CONDITION_POLICY`, en el constructor y en el lector. El IR v2 `HostPolicy` no se
  estrecha; sólo su forma persistida en M2a.
- `_check_policies` comprueba: condiciones citadas existentes, una ref con un solo efecto, aplicaciones únicas y orden.
  `canonical_policies` ordena; nunca fusiona, deduplica ni descarta.
- `canonical_logic` devuelve `None` para una conjunción pura y, en cualquier otro caso, el árbol tal cual.
- `canonical_interpretation(requests, conditions, logic, policies, base_trace_id, root_trace_id)` es el **único**
  camino de construcción.

Tests:

- `tests/test_foundation_accepted_m2a3_canonical.py`: tabla, cuatro baterías (orden de condiciones, políticas, lógica,
  constructor), O1B no representable, una propiedad Hypothesis de invariancia a las permutaciones no observables y
  18 + 3 mutantes. Incluye:
  - los mutantes pedidos: «tabla alfabética», «ordena dentro de la ranura», «acepta no canónico»;
  - «el tipo exige ids ordenados dentro de la ranura», «el constructor no agrupa» y «el constructor ordena
    peticiones»;
  - «acepta política multi-condición», «colapsa (p1, p1)»;
  - «ordena los hijos de un árbol OR» y «aplana un AND dentro de un árbol OR».
- Fixtures de 1a/1a.1 ajustados:
  - la política conjunta `topn(c1, c4)` de `rich_interpretation` pasa a forma partida;
  - `condition_ids` sale de `SET_LIKE_KEYS`;
  - el mutante de `condition_ids` desordenados pasa a «acepta política multi-condición»;
  - `test_host_policy_itself_is_not_narrowed` espera `MULTI_CONDITION_POLICY`;
  - el generador `st_interpretation` construye con `canonical_interpretation`.

## 4. Consecuencias que heredan otros carriles

- **M2A-1b (`from_acceptance`).**
  - Llama a `canonical_interpretation` con las condiciones en el orden de la aceptación y copia las políticas
    partidas como las emite `main`.
  - O1B y `opportunity-explicit-state` citan la condición REQUESTED_UNIVERSE, que no es ranura, así que **no son
    representables** (`test_the_request_universe_policies_cannot_be_persisted`). Un ancla de oportunidades no se
    persiste hasta que se decida su representación.
  - Ancla dual: `main` lleva una copia de la condición por petición (corpus NC-002), mientras que
    `AcceptedInterpretation` exige condiciones compartidas y la proyección de `main` rechaza una condición compartida
    (`INCOMPLETE_PROJECTION_DISPOSITIONS`). M2A-1b y M2A-6 tienen que reconciliarlo.
- **M2A-2 (store).** Importa el tipo, que ahora rechaza más formas: condiciones fuera del orden de ranura, lógica pura
  como árbol, y políticas multi-condición, colgantes, duplicadas, con dos efectos o desordenadas. Sus fixtures deben
  construir con `canonical_interpretation` o con los builders de `tests/test_foundation_accepted_contract.py`.
- **M2A-6 (proyección de la efectiva).** Recorre `conditions` y `requests` en el orden persistido. Los pines de §1
  fallan primero si la proyección empieza a depender del orden entre ranuras o del orden de `policies`.

## 5. Una política conjunta futura

Si un dueño necesita de verdad una política sobre varias condiciones (P(c1, c2)), la declara expresamente y se abre
una decisión: hoy el tipo la rechaza. Se verificó que `main` no emite ninguna:

- los cuatro `HostPolicy(` de `foundation_admission.py` (líneas 84, 141, 152 y 154) citan `(condition.condition_id,)`;
- `foundation_acceptance_loader.py:145` sólo lo usan tests;
- 0 de los 3291 JSON/JSONL rastreados contienen una política de más de una condición.

## 6. Decisiones del usuario (2026-10-02) sobre la fase 1

| Punto | Decisión |
|---|---|
| D-CANON-1 (orden entre ranuras) | **Sí**: tabla cerrada por ranura; orden de la aceptación dentro de la ranura (§2) |
| D-CANON-2 (forma partida / fusionada) | **Partida**, como emite `main`; nunca fusionar por equivalencia de proyección; las de más de una condición se rechazan (§5) |
| logic | «No ordenar `logic.children` a ciegas»: sólo `None` para una conjunción pura; árboles OR/NOT tal cual; garantía limitada (§1). Sustituye el aplanado y ordenado de AND de la fase 1 (`30d073a4`) |
| STOP-1, STOP-2 | Ver `M2A3_PHASE1_CLOSE.md` §5 |

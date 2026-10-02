# M2A-3 — Forma canónica de `AcceptedInterpretation` (R4: orden observable frente a set-like)

**Fecha:** 2026-10-02. **Rama:** `mission/m2a-3-reducer` (base Paso 0 `fa0b6de8`). **Autoridad:** contrato
`docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` (§3.2, §4.1, §4.3, §12, INV-M2A-06); decisiones del usuario en
`M2A_WAVE1_PREP.md` §4. Este documento **no** cambia el contrato ni el Gold.

Regla de partida: **nada se ordena a ciegas**. Una colección recibe orden canónico sólo si se demuestra, con la
proyección real (`app/core/foundation_projection.py`), que su orden no se ve en `soql`, en `plan_hash`, en el orden de
los grupos ni en el efecto de las políticas. Precedente de 1a.1: el tipo **rechaza** la forma no canónica; nunca
reordena al leer. Los constructores (`compose` y el futuro `from_acceptance`) emiten la forma canónica con **una sola**
función compartida: `foundation_accepted.canonical_interpretation`.

Evidencia ejecutada y reproducible: `tests/test_m2a3_order_observability.py` (10 pines sobre la proyección real; las
aceptaciones se construyen en memoria y nada llama a la fuente).

## 1. Veredicto por colección

| Colección | Veredicto | Evidencia (pin) | Qué se ve | Regla |
|---|---|---|---|---|
| `conditions` | **ORDEN OBSERVABLE** (sub-orden TOPIC/POSITIVE) | `test_condition_order_is_observable_exactly_through_the_topic_positive_terms`: las 720 permutaciones de 6 condiciones dan exactamente 6 clases de `soql`/`plan_hash`, una por orden relativo de las 3 TOPIC/POSITIVE; `test_cross_slot_order_is_unobservable_with_every_projected_dimension` (TOPIC, TIME, VALUE, GEO, MODALITY familia, CONTRACT_TYPE, STATE negado con su política) | la lista `foundation_topic_terms` sigue el orden de las condiciones: cambia el orden de los `LIKE` en el WHERE y los recibos `foundation_topic_terms:<i>` que entran en `plan_hash` | **sin orden canónico**. El tipo acepta cualquier orden y `canonical_interpretation` conserva el dado |
| `requests` (ancla dual) | **ORDEN OBSERVABLE** | `test_request_order_is_the_group_order_of_a_dual_anchor` | `plan_hash` por petición idéntico, pero el manifiesto sigue el orden de las peticiones, y `foundation_runtime.execute_accepted` arma `groups`, `rows` y el texto en ese orden | **sin orden canónico**; se conserva |
| `policies` | **SET-LIKE**, con dos estados excluidos | `test_policy_order_is_unobservable_for_distinct_refs` (las 6 permutaciones de 3 políticas dan 1 plan); `test_one_ref_with_two_effects_makes_policy_order_observable` | sólo se vería si un mismo `policy_id@version` llevara dos efectos: la proyección mapea ref → efecto con un `dict` y el último gana en `plan_hash` | **ordenadas** por `(policy_id, version, condition_ids, effect)`; el tipo rechaza `UNSORTED_POLICIES`, `DUPLICATE_POLICY` y `CONFLICTING_POLICY_EFFECT` |
| hijos de un AND en `logic` | **SET-LIKE** (y asociativo) | `test_and_shape_and_order_are_unobservable`: 9 formas (6 órdenes y 2 anidamientos) dan 1 plan; control: OR ⇒ `UNSUPPORTED NONCONJUNCTIVE_EXPRESSION` | nada: la proyección compila desde la lista de condiciones; del árbol sólo lee si es conjuntivo | **conjunción pura ⇒ `logic = None`** (el contrato §3.2 lo dice: «None = AND de todas; si hay OR/NOT, la expresión»); todo AND **plano** (sin un AND hijo de otro AND) y con los hijos **ordenados** por su JSON canónico. El tipo rechaza `NONCANONICAL_LOGIC` |
| hijos de OR / NOT | **NO PROBADO** | la proyección no ejecuta OR (`NONCONJUNCTIVE_EXPRESSION`): no hay forma de demostrar ni lo uno ni lo otro | — | **se conserva el orden dado** (un AND dentro de ellos sí se normaliza) |

Sin cambios respecto de 1a.1: `request_ids`, `receipt_rules` y `policies` de una condición, `condition_ids` de una
política persistida y `answer_slots` siguen siendo listas ordenadas y únicas.

Fuera de este paso, porque no se ordenan (R6): `candidate_patches` (es el orden de presentación, M2A-4), el eco, y
`ResultSetRef.groups` (el orden de ejecución). Las `ops` de un parche **no** se ordenan: `compose` tendrá que ser
invariante a su permutación (propiedad en la fase 2). Su forma canónica queda como deuda del productor (M2A-5).

## 2. Por qué `conditions` y `requests` no son un STOP

El orden observable **sí lo decide** el contrato, y `compose` nunca tiene que inventarlo:

- **Primer turno.** INV-M2A-06 exige la misma SoQL y el mismo `plan_hash` que `main`. `main` proyecta la aceptación en
  el orden de la propuesta. Por tanto el orden de la aceptación **es** el canónico, y `from_acceptance` (M2A-1b) debe
  copiarlo tal cual. Un orden canónico distinto rompería INV-M2A-06 en cuanto un turno trajera dos TOPIC/POSITIVE.
  Ejemplo ejecutado (`test_the_concrete_example_two_topics`): `[c1 aseo, c2 papelería, c4 2025]` frente a
  `[c2, c1, c4]` cambia `soql` y `plan_hash`; `[c4, c1, c2]` no los cambia.
- **`compose`.** No puede crear un orden nuevo dentro de TOPIC/POSITIVE. Esa ranura pasa intacta (arrastre, en el orden
  de la base), la sustituye **un** `SET` (dos `SET` en la misma ranura son `DELTA_SLOT_CONFLICT`) o se borra. Las
  sustituciones F3 y F16 dejan como mucho una condición. En cuanto a `requests`, `compose` nunca las permuta: o
  pasan tal cual, o `REPLACE_UNIVERSE` con ancla dual deja una sola.
- **Lo que `compose` sí elige** (dónde coloca una condición nueva entre las arrastradas) **no se ve** en `soql` ni en
  `plan_hash` (pin de orden entre ranuras). Sólo cambia `interpretation_hash`. Por eso es una regla de determinismo
  de la fase 2, no un orden de producto. Tiene que ser invariante a la permutación de `ops`: por ejemplo, las
  arrastradas en el orden de la base y después las nuevas en un orden canónico de la op.

## 3. Residuos declarados (no decididos aquí; pendientes de decisión)

- **D-CANON-1: orden entre ranuras de `conditions`.** No se ve (pin), pero dos aceptaciones que sólo difieren en el
  orden entre ranuras persisten bytes distintos. La obligación «mismo significado ⇒ mismos bytes» se cumple para
  `policies` y `logic`, y para `conditions` sólo **salvo permutación entre ranuras**. Opción B, preparada y no
  aplicada: agrupar por clave de ranura de forma estable, conservando el orden dentro de cada ranura (el observable).
  Cambiaría el orden que 1a aceptaba (fixtures de 1a/1a.1 y de cualquier consumidor, M2A-2 incluido) y la proyección
  que haga M2A-1b. Es una decisión de representación entre carriles, no de este paso. La determinista
  `compose(base, patch)` no depende de ella.
- **D-CANON-2: forma partida frente a fusionada de una misma política.** `[P(c1), P(c2)]` y `P(c1, c2)`, misma ref y
  mismo efecto, proyectan igual (`test_split_and_merged_forms_of_one_ref_project_alike`), y el tipo admite las dos.
  Fusionar no siempre preserva el sentido (citas solapadas ⇒ la proyección falla; por eso el solape se rechaza como
  `DUPLICATE_POLICY`). En `main` la forma partida con ≥ 2 citas no es ejecutable: dos MODALITY, dos STATE… chocan en
  la proyección. Queda declarado.

## 4. Lo implementado (`app/core/foundation_accepted.py`)

- `_check_policies` (dentro de `AcceptedInterpretation`), en este orden:
  - `FOREIGN_POLICY_CONDITION`: `policy.condition_ids ⊆` las condiciones. Un `CLEAR` nunca deja una política
    colgando.
  - `CONFLICTING_POLICY_EFFECT`: una ref `policy_id@version`, tal como la escribe la proyección, tiene un solo
    efecto.
  - `DUPLICATE_POLICY`: cada par (ref, condición) se cita una sola vez. Cubre `(p1, p1)` y el solape, y no depende
    del veredicto de orden.
  - `UNSORTED_POLICIES`.
- `canonical_policies`: ordena; nunca deduplica, fusiona ni descarta.
- `canonical_logic`: `None` para una conjunción pura; ANDs planos y ordenados; OR y NOT conservan su orden.
  `AcceptedInterpretation` rechaza `NONCANONICAL_LOGIC`.
- `canonical_interpretation(requests, conditions, logic, policies, base_trace_id, root_trace_id)`: el **único** camino
  de construcción para `compose` y `from_acceptance`. Ordena sólo lo que es set-like y conserva `requests` y
  `conditions`.

Tests: `tests/test_foundation_accepted_m2a3_canonical.py`, con tres baterías (políticas, lógica, constructor), una
propiedad Hypothesis de invariancia a la permutación y 15 + 3 mutantes. Cada mutante muere por la razón prevista; se
incluyen los de sobre-ordenación: «ordena condiciones», «ordena peticiones», «el tipo rechaza el orden de la
aceptación» y «ordena los hijos de un OR». El generador de 1a (`st_interpretation`) ahora construye su `logic`
con `canonical_logic`; la propiedad que prueba no cambia.

## 5. Consecuencias que heredan otros carriles

- **M2A-1b (`from_acceptance`).**
  - Debe llamar a `canonical_interpretation` y copiar el orden de la aceptación.
  - O1B y `opportunity-explicit-state` citan la condición REQUESTED_UNIVERSE (dimensión `RETRIEVAL_INTENT`). Esa
    condición no es ranura y no existe como `AcceptedCondition`, así que con `⊆` **no son representables**
    (`test_the_request_universe_policies_cannot_be_persisted`). Un ancla de oportunidades no puede persistirse hasta
    que se decida su representación (ver `M2A3_PHASE1_CLOSE.md` §1).
  - Ancla dual: en `main` cada petición lleva su copia de la condición (corpus NC-002), mientras que
    `AcceptedInterpretation` exige condiciones compartidas (`DUAL_ANCHOR_CONDITIONS_DIFFER`). Además, la proyección de
    `main` rechaza una condición compartida (`INCOMPLETE_PROJECTION_DISPOSITIONS`). M2A-1b y M2A-6 tienen que
    reconciliar las dos formas.
- **M2A-2 (store).** Importa el tipo; ahora rechaza más formas: lógica no canónica, políticas colgantes, duplicadas,
  con dos efectos o desordenadas. Sus fixtures deben construir con `canonical_interpretation` o con los builders de
  `tests/test_foundation_accepted_contract.py`.
- **M2A-6 (proyección de la efectiva).** Debe recorrer `conditions` y `requests` en el orden persistido. Los pines de
  §1 fallan primero si la proyección empieza a depender del orden de `policies` o de los hijos de un AND.

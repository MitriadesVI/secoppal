# M2A-4 — Aclaración pendiente: gate de entrada y cierre

**Rama:** `mission/m2a-4-pending` (base `d7f53e9b` = `main` `5a829d5d` + registro del DAG de la ola 2).
**Contrato:** R3d sellado (`7dd39b4b…`). No se modifica su texto ni se re-sella: las deudas se cierran en el código y en
las pruebas de M2A-4 y se registran aquí (precedente U29). **Deudas de entrada:** `M2A4_M2A6_ENTRY_DEBTS.md` (U28).

Esta primera sección es el **gate de entrada** (escrita antes de implementar). Las secciones de implementación,
mutantes y gates se añaden al final de la rama y no cambian estas decisiones.

## 1. Gate de entrada: las deudas de M2A-4 y su cierre

Ninguna deuda exige cambiar texto sellado, re-sellar ni deja un estado alcanzable sin respuesta representable. Cada una se
cierra del lado que falla cerrado (defaults del orquestador). Dónde vive cada decisión: `app/core/foundation_pending*.py`
y `tests/test_foundation_pending*.py`.

| Id | Deuda | Decisión | Cierre |
|---|---|---|---|
| **M2A4-D1** | P1-1: cancelar `CONTEXT_EXPIRED_CONFIRM` no exige ¬Continuación(P) | Con esa pendiente, «reset + fragmento»: el resto de P es búsqueda nueva **sólo si ¬Continuación(P) ∧ Admisible sola(P)**; si no, `CLARIFY NO_ANCHOR`. Nunca se compone sobre el `frame` vencido. Motivo: A9 («con Continuación(P) nunca es búsqueda nueva», contrato §7.2) prevalece sobre la redacción de la fila 2 y de §17.3 | regla en la fachada (`foundation_pending.respond`); test por clase de P × tipo de pendiente; mutantes «sin ¬Continuación» y «compone sobre el frame vencido» |
| **M2A4-D2** | P1-2: «continuar» se alcanza en lenguaje añadiendo un `KEEP` | Toda selección **en lenguaje** sobre una pendiente `CONTEXT_EXPIRED_CONFIRM` es la fila 7 (`REASK ANSWER_UNRESOLVED`): «continuar» sólo se confirma por evento (contrato §8.3 filas 1 y «Selección» 2, §10.1; U27). Las filas 5 y 8 no cambian (también son `REASK`) | barrido de **todas** las pendientes H1 que crea `delta_outcome` × respuestas en lenguaje con `KEEP` de relleno: 0 `EXECUTE`; mutante «F24 antes de normalizar sólo» |
| **M2A4-D3** | P1-3 (parte M2A-4): qué cancelaciones eliminan el ancla vencida | Lectura literal de U27: **toda** causa de §17.2 (a, b, c, d, e) que cancela una pendiente `CONTEXT_EXPIRED_CONFIRM` elimina el ancla vencida. M2A-4 lo expresa en el resultado de la cancelación (`Cancellation.eliminates_expired_anchor`); el efecto en runtime (`anchor_trace_id = None`) es de M2A-6 | tabla exhaustiva causa × tipo de pendiente; mutante «sólo a, c y d» (la lectura de §10.1) |
| **M2A4-D4** | P2-4: un `KEEP` en una respuesta sin candidatos acaba en la fila 5 | **Coste declarado, no se cambia.** U27 fijó que la normalización sólo se aplica con candidatos. Garantía limitada: el resultado es un `REASK ANSWER_EDITS_OTHER_SLOT`, que falla seguro (0 llamadas, pendiente intacta). Un test lo fija para que relajarlo sea una decisión | `test_declared_cost_keep_without_candidates` |
| **M2A4-D5** | P2-6: F24 en H5 depende de la etiqueta del productor (y el coste de F24 en H6) | **Coste declarado, no se cambia.** F24 compara con el parche que preguntó: la opción igual a él sólo se alcanza por evento. En H5 qué opción es depende de la etiqueta provisional del productor; en H6, «cambiar de comprador» (A) sólo se alcanza repitiendo sus dos operaciones y «buscar con ambos» (B) con un `KEEP` del comprador. Falla seguro (`REASK`) | tabla de las dos etiquetas de H5 y de A/B de H6 fijada en un test |
| **M2A4-D6** | H5/H7/H9/H10 sin punto de entrada | `ask_authority`: construye la pendiente de una regla cuyo disparo es de la autoridad o del host (INV-M2A-07, M2A-3b); M2A-4 no evalúa el disparo, aplica §8.4 sobre el `frame`, los candidatos y las `answer_slots`. Mismo punto de entrada para `UNRESOLVED_VALUE:<DIM>` con candidatos y `VALUE_WITHOUT_COMPARATOR` | `foundation_pending_create.ask_authority`; casos por regla + ley |
| **M2A4-D7** | `classify_turn`, fila 2 con `CONTEXT_EXPIRED_CONFIRM` (`TurnFacts` no lleva el tipo de pendiente) | Lo decide la fachada, que sí conoce la pendiente. `foundation_turn_scope.py` no se toca | D1 |
| **M2A4-D8** | P3: `laundering_problems` no mira las respuestas de una operación | El barrido de respuestas compara cada respuesta compuesta sobre el `frame` con `frame_ops + respuesta` compuesto desde la base: una dependencia de §19 no se pierde por pasar por el `frame` | propiedad en el barrido |
| **M2A4-D9** | Gates de §8.4: barrer las respuestas a TODAS las pendientes; una respuesta se compone sobre `frame`, nunca sobre la base | Barrido diferencial contra el modelo simbólico del contrato (`respond`/`answer` de `tests/test_m2a_contract_r3_multiop.py`) proyectado a valores tipados, más el barrido sobre las pendientes que crea `delta_outcome`. La respuesta no recibe la base: `compose_answer(frame, …)` es la costura, y el mutante `answer_on_base_not_frame` la sustituye por la base | barrido + mutante |

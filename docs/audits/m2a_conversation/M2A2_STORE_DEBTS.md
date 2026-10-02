# M2A-2 — deudas y restricciones registradas (no se reparan en este carril)

**Registrada:** 2026-10-02, tras ZEUS GO sobre `de968b7e` (0 P1). **Alcance:** `app/core/foundation_accepted_store.py` y el
delta declarado de `ConversationStore.clear()`. Nada de esto cambia el comportamiento ya entregado; cada punto dice quién
lo hereda. Las marcadas **PRE-EXISTENTE** no las introduce M2A-2.

## Deudas

### D1. `AcceptedStoreLocation` no ata `chat_id` con `envelope_path`
El constructor directo `AcceptedStoreLocation(chat_id, envelope_path)` acepta cualquier pareja: un llamador puede cruzar el
sobre de un chat con el `chat_id` de otro. El lector y `append` comparan `chat_id` de forma literal contra el de la
ubicación, de modo que un cruce se ve como `FOREIGN_CHAT` (falla cerrado), pero no se previene al construir.
**Quien lo hereda:** M2A-6 debe construir la ubicación **sólo** con `AcceptedStoreLocation.for_chat(store, chat_id)`, que
pide la ruta al `ConversationStore` en lugar de recalcularla.

### D2. Colisión de nombre `<X>.accepted.jsonl` con el historial plano de un chat «X.accepted» (PRE-EXISTENTE, misma clase que `.envelopes`)
`_sanitize_chat_id` devuelve sin sanear cualquier id con prefijo `tg_`, `wa_` o `st_`, puntos incluidos. El chat
`st_abc.accepted` tiene como historial plano `st_abc.accepted.jsonl`, que es el nombre del almacén del chat `st_abc`
(reproducido por ZEUS, `collide.py`): `clear()` de `st_abc` borra el historial de `st_abc.accepted`, el almacén de
`st_abc` se lee como `Turn`s de ese otro chat, y un `append_turn` de ese otro chat deja el almacén `CORRUPT`. Es la misma clase de colisión que ya tiene el sobre (`<X>.envelopes` frente a `<X>.envelopes.jsonl`), no la crea M2A-2.
**Dueño:** la tarea que investigue D6; el remedio natural es sanear de verdad el id, no renombrar el almacén.

### D3. `ConversationLockTimeout` expone `path.name` (PRE-EXISTENTE)
`store_lock.conversation_lock` lanza `ConversationLockTimeout(f"conversation busy: {path.name}")`. El nombre del archivo
lleva el chat saneado y, en WhatsApp, un teléfono. M2A-2 no añade nada: sus errores y razones llevan códigos fijos, nunca
contenido ni `chat_id` (lo fija un test), pero el timeout del lock compartido sale con el nombre. **Dueño:** Núcleo
(`store_lock`); fuera del cierre de M2a.

### D4. Coste O(n) bajo el lock compartido, con timeout de 10 s
`append` relee y valida el archivo entero bajo `conversation_lock(envelope_path)`; `read` y `resolve_anchor` también. Con el
timeout por defecto (10 s) ZEUS estimó el límite alrededor de ~27.000 registros por chat (`perf.py`): pasado ese tamaño el
propio coste de validar agota el lock de los demás escritores del chat, incluidos `publish_turn` y `clear()`. Con el uso
previsto (decenas de turnos) no se acerca. **Decisión pendiente:** si algún día hace falta, un índice o un tope de registros
por chat; nunca saltar la validación.

### D5. Un almacén `CORRUPT` bloquea los `append` hasta el `/reset`
Es la decisión de fondo (cualquier línea mala invalida el almacén entero; `append` no entierra la evidencia; nada
recupera). Una escritura interrumpida (cola sin terminar) deja al chat sin continuidad M2a hasta el reset.
**Quien lo hereda:** M2A-6 debe hacerlo **visible** (un `ANCHOR_UNREADABLE`/`STORE_CORRUPT:<código>` explícito con la salida
«reinicia la conversación»), no tratarlo como «sin ancla».

### D6. Path traversal de `_sanitize_chat_id` (PRE-EXISTENTE; lo investiga una tarea aparte)
Un id con prefijo `tg_`, `wa_` o `st_` pasa sin sanear: `"tg_/../../x"` produce `<base>/tg_/../../x.envelopes.jsonl`. Afecta
por igual al historial, al sobre y, desde M2A-2, al almacén y a su borrado (el literal de `clear()` usa el mismo saneador). El store
deriva el archivo del sobre y el literal de `clear()` usa el mismo saneador, así que no divergen, pero ninguno contiene la
ruta. No se toca aquí.

## Restricciones que hereda M2A-6

1. **Historial publicado, no el de dentro del staging.** Dentro de `stage_continuity`, `get_history` ya contiene el `Turn` en
   vuelo (se escribe en la generación privada). `resolve_anchor` alimentado con esa foto da `ANCHOR` para un turno que nunca
   se publica (crash, `StaleRevisionError`). Hay que resolver el ancla con el historial **publicado**: la foto previa al
   `append_turn` del propio turno, o posterior a la publicación. Caracterizado por
   `test_constraint_for_m2a_6_inside_the_staging_get_history_already_holds_the_turn_in_flight`; el store no puede distinguirlo.
2. **Historial entero.** `get_history` tiene `last_n=5` por defecto; una ventana hace que un ancla vieja parezca no publicada
   (falla cerrado, pero por la razón equivocada).
3. **Registro huérfano.** Registro y `Turn` son dos secciones críticas seguidas, no un par atómico. Quien consuma otro campo
   de la cola (p. ej. `pending`) debe aplicar la misma prueba de historial publicado.
4. **Punteros sin validar al escribir.** `base_trace_id` y `anchor_trace_id` no se comprueban en `append` (el store no
   interpreta); un puntero colgante sólo se detecta al resolver.
5. **Forma canónica.** «Idéntico o inválido»: si M2A-3 cambia la forma canónica de `to_dict`, las líneas ya escritas quedan
   inválidas. Hoy nadie escribe, así que no hay datos que migrar.

## Retención y git

- `data/conversations/*.accepted.jsonl` está en `.gitignore` (patrón estrecho, con test). Los 20 archivos de
  `data/conversations/` que ya están trackeados son deuda preexistente registrada en
  `docs/audits/m1_c1/C1_2_FRONTIER_PIN_DEBT.md`; no se tocan.
- `.generations/<chat>/` sobrevive a `clear()` (`DEBT_CLEAR_GENERATIONS_RETENTION.md`); caracterizado por un test, sin reparar.

## Delta documental para HERA/CLÍO

El contrato §3.8 dice que el enganche legacy «exige regenerar el manifiesto con motivo declarado». La decisión del usuario
(`M2A_WAVE1_PREP.md` §7) es lo contrario: el manifiesto **no** se regenera, se declara `ALLOWED_DIVERGENCE` con un pin que sólo
admite el delta. El contrato y el Gold no se tocan en este carril.

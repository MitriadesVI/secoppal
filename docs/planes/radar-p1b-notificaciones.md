# Radar P1B — Plan de ejecución de notificaciones (Preflight 1–3 → P1B-1 → P1B-6)

> **Contrato:** [`docs/radar/P1_NOTIFICATION_AUDIT_ADDENDUM.md`](../radar/P1_NOTIFICATION_AUDIT_ADDENDUM.md)
> **Decisión:** ADR-051 provisional (`docs/decisions_log.md`) — verificar número
> contra refs remotos antes de integrar (ver §M del addendum).
> **Rama base:** `radar/mvp-manual` · **HEAD:** `02ffcfd`
>
> **Regla estratégica (heredada del plan post-P0):** Verdad → Operabilidad → Push → Valor.
> **Corolario de P1B-0:** no se construye el outbox sobre una verdad con fugas.
> Por eso el roadmap abre con tres bloques **Preflight** que cierran deuda de P0
> verificada durante la auditoría (N1, N15, `needs_attention`) — precondición
> dura de **P1B-2**, no solo de la activación en P1B-6.

## Relación con el plan post-P0 (E5–E7)

Este plan **sustituye** los bloques E5, E6 y E7 de
`docs/planes/radar-plan-ejecucion-post-p0.md`. Aquellos quedan como registro
histórico. Qué cambia y por qué:

| E5–E7 proponía | P1B hace | Motivo |
|---|---|---|
| `radar_notification_targets(owner, channel, destination)` | `principals` + `notification_endpoints` + `radar_notification_preferences` | Una tabla con tres responsabilidades (identidad, dirección, preferencia). `owner` no es una persona (N4/N5/N6). |
| `radar_notification_attempts` como única tabla | Outbox + batch_events + attempts | Intención ≠ intento. Sin outbox no hay reintento, ni claim, ni trazabilidad. |
| Idempotencia por `digest_hash` | `UNIQUE(endpoint_id, idempotency_key)` sobre event_seqs | El hash del texto falla en ambos sentidos (§B.1.1 del addendum). |
| Consumo de `outcomes` del scheduler | Cursor sobre `radar_events.event_seq` | E6 es fire-and-forget sobre RAM: un crash pierde novedades (N10). |
| Sin verificación de destino | `verified_at` obligatorio | Un destino sin prueba de posesión entrega a un desconocido. |
| `enabled` como flag único | `enabled` (operativo) + opt-in (consentimiento) | "Lo apagamos" ≠ "lo apagó la persona". |
| Sin `ambiguous` | `ambiguous` de primera clase | El borde actual no distingue timeout de 400 (N8). |

**Lo que E5–E7 acertó y se conserva:** módulos nuevos y pequeños (no tocar
`store.py`); Telegram primero; dry-run antes que envío real; agrupación por
persona; no notificar `radar_empty`/`radar_timeout`; baseline suprimida.

**Los tres bloques Preflight que siguen no vienen de E5–E7** — son deuda de P0
encontrada al auditar el sustrato sobre el que P1B tendría que construirse
(N1, N15, `needs_attention`). Se resuelven **antes** de P1B-1 porque el outbox
de P1B-2 lee `radar_events` a través de un cursor, y un cursor sobre eventos
que el runner puede perder o duplicar hereda ese defecto sin remedio posterior.

---

## P1B-Preflight 1 — Materialización atómica de la corrida

**Objetivo:** cerrar N1 (addendum §B.1.2) — que un crash entre `upsert_seen` y
`_emit_events` no pierda eventos para siempre. La corrida pasa de "varias
conexiones en autocommit" a **una transacción corta**, abierta solo después de
terminar toda red, que revalida el lease como su **primera escritura**.

> Es el único bloque de este plan que toca código productivo del núcleo del
> Radar (no de notificaciones). Se aísla como su propio bloque revisable, con
> su propio commit, precisamente por eso: es la pieza más delicada y no debe
> mezclarse con la primera línea de código de notificaciones.

**Archivos esperados**
- `app/core/radar/store.py` — nuevo método `materialize_run(...)` que abre una
  única conexión, ejecuta `BEGIN IMMEDIATE`, hace como **primera sentencia** el
  UPDATE condicional de renovación de lease (mismo predicado que `lease.renew`,
  `lease.py:130-144`: `WHERE id=? AND run_lease_token=? AND
  run_lease_expires_at>?`); si `rowcount==0` hace `ROLLBACK` y devuelve "lease
  perdido"; si `rowcount==1`, inserta `radar_run`, hace los upserts de `seen`,
  inserta los `radar_events` y aplica la limpieza operativa de fallo/éxito
  (ver P1B-Preflight 3 para el detalle exacto de qué limpia `needs_attention`),
  todo sobre la misma conexión, y cierra con `COMMIT`.
- `app/core/radar/runner.py` — `_run_radar_locked` deja de llamar
  `store.record_run` / el bucle de `upsert_seen` / `_emit_events` /
  `update_last_run` como pasos sueltos tras `_still_owner`; llama una vez a
  `store.materialize_run(...)` con todo lo ya computado (diff, digest,
  metadatos de ejecución). Ninguna llamada de red debe ocurrir dentro de esa
  función — ya terminó antes de invocarla.
- `tests/test_radar_store.py`, `tests/test_radar_runner.py` — fault injection.

**Invariantes**
- **INV-AT1** — Ninguna llamada de red ocurre dentro de la transacción de materialización.
- **INV-AT2** — La transacción revalida `run_lease_token` como su **primera**
  escritura; si falla, `ROLLBACK` total, cero efectos parciales.
- **INV-AT3** — Un crash en cualquier punto dentro de los límites de la
  transacción no deja escritura parcial: o `radar_run`+`seen`+`events`+limpieza
  están completos, o no está nada.
- **INV-AT4** — Un propietario de lease vencido no puede materializar (el
  `UPDATE` condicional lo impide antes de escribir nada más).

**Tests mínimos**
1. Fault injection: excepción forzada justo antes del `COMMIT` → al reabrir la
   conexión, **cero** filas nuevas de `radar_run`/`seen`/`events` (verificado
   con una conexión `sqlite3` directa, no vía la API que podría enmascarar el
   rollback).
2. Fault injection: excepción forzada justo después del `COMMIT` (en el
   caller) → las cuatro escrituras están completas y consistentes.
3. Lease robado entre `_execute_radar_window` y la llamada a
   `materialize_run` (simulado escribiendo un token distinto directamente en
   la fila) → `materialize_run` hace `ROLLBACK`, devuelve `lease_lost`, y
   **no** hay ninguna fila nueva.
4. Dos hilos llamando `materialize_run` para el mismo radar con tokens
   distintos (uno vencido) → solo uno escribe; el otro obtiene `lease_lost`
   sin escritura parcial.
5. Verificación **estructural**, no solo de comportamiento: `materialize_run`
   no debe poder invocar `deps.secop_client` ni `fetch_current_rows_by_keys`
   — un chequeo en CI (grep o AST) sobre sus imports/argumentos, no solo un test.
6. Caso feliz: outcome `radar_ok` con diff no vacío → run+seen+events+limpieza
   consistentes en una sola inspección post-commit.
7. Caso de fallo (outcome de error) → run+registro de fallo equivalentes,
   atómicos igual que el caso feliz.

**GO** — los 7 verdes; el test 5 es innegociable: si `materialize_run` puede
tocar red, el diseño se rompió.
**NO-GO** — cualquier fault injection que deje una escritura parcial detectable.

**Modelo:** **Opus** — concurrencia + atomicidad + fencing sobre SQLite en
autocommit es la pieza más delicada de todo el plan.

---

## P1B-Preflight 2 — Concurrencia e idempotencia de `follows`

**Objetivo:** cerrar N15 (addendum §B.1.4) **sin prescribir la solución de
antemano**. Antes de decidir "envolver `refresh_follows` en el mismo lease de
radar", investigar si la identidad del radar sintético de seguidos —uno por
owner, autocreado por `get_or_create_follows_radar` (`follows.py:39-52`), con
`frequency=FOLLOWS_FREQUENCY` (`follows.py:30`)— admite el mismo modelo de
lease que un radar de búsqueda, o si hace falta una variante.

**Archivos esperados**
- Ninguno de producción todavía. Este bloque **empieza como investigación**,
  documentada en `docs/radar/FOLLOWS_LEASE_INVESTIGATION.md` *(nuevo)*, antes
  de tocar código.
- Tras la investigación (solo si confirma que el modelo aplica sin fricción):
  `app/core/radar/follows.py` — `refresh_follows` (`follows.py:158-229`)
  envuelto en `try_acquire_run_lease`/`release_run_lease`, igual que
  `run_radar` (`runner.py:422-430`). Si la investigación revela fricción, este
  bloque documenta la variante necesaria antes de escribir código, no después.
- `tests/test_radar_follows.py` — casos de concurrencia.

**Invariantes**
- **INV-FW1** — Dos procesos no pueden ejecutar `refresh_follows` para el
  mismo owner simultáneamente.
- **INV-FW2** — Un evento de `follows` no se duplica por una carrera de scheduler.
- **INV-FW3** *(a confirmar en la investigación, no asumida de antemano)* — El
  radar sintético de `follows` tiene una identidad (`radar.id`) estable y única
  por owner, apta para lease igual que cualquier otro radar.

**Preguntas que la investigación debe cerrar antes de escribir código**
1. ¿`get_or_create_follows_radar` puede crear dos filas para el mismo owner
   bajo carrera (dos llamadas concurrentes antes de que exista la fila)? Si
   sí, el lease no soluciona nada por sí solo: el problema está upstream, en
   la creación.
2. ¿El ciclo de vida de este radar sintético (nunca se borra, nunca cambia
   `frequency`) introduce algún caso que `lease.py` no contempló —pensado para
   radares con `enabled`/`needs_attention` normales?
3. ¿Conviene un lease de duración distinta? `refresh_follows` no hace el
   barrido de 500 de `run_radar`; `LEASE_DURATION` (`lease.py:29`, 600s) está
   calibrado para ese peor caso, que aquí no aplica.

**Tests mínimos**
1. `get_or_create_follows_radar` llamado concurrentemente (dos hilos) para el
   mismo owner → **una sola** fila (responde la pregunta 1, con o sin lease).
2. Dos `refresh_follows` concurrentes para el mismo owner → un solo conjunto
   de eventos, no duplicado.
3. `refresh_follows` con lease ya tomado por otro proceso → se omite
   limpiamente (mismo patrón que `_already_running_result`, `runner.py:379-384`).

**GO** — la investigación documentada + los 3 tests verdes, con la decisión de
diseño (lease igual / lease variante) explícita y justificada por las
respuestas a las tres preguntas.
**NO-GO** — aplicar `try_acquire_run_lease` a `follows` sin haber respondido
la pregunta 1 primero.

**Modelo:** Sonnet High para la investigación y los tests; **Opus** si la
pregunta 1 revela una carrera de creación (cambiaría el diseño de
`get_or_create_follows_radar`, no solo el de `refresh_follows`).

---

## P1B-Preflight 3 — Ciclo explícito de `needs_attention`/reactivación

**Objetivo:** separar "el radar funciona" (`needs_attention`) de "el usuario
lo quiere corriendo" (`enabled`), y que ninguna corrida —automática o
manual— borre el primero sin una acción explícita. **Corrección de precisión
respecto de la auditoría original:** el defecto verificado **no** es que
`update_last_run` reactive el radar — no toca `enabled`. El defecto es que
limpia `needs_attention` (y todo el estado de fallo) en cualquier corrida
exitosa, incluida una disparada manualmente sobre un radar pausado, borrando
el indicador que explica *por qué* sigue pausado.

**Alcance ejecutado (2026-07-15).** El bloque se acotó a **impedir el borrado
automático**, no a diseñar el flujo de revisión. La razón: `clear_needs_attention`
sería una API pública sin un solo caller —no hay comando, botón ni endpoint que
la invoque—, y agregarla "para completar el diseño" es superficie muerta que
habría que mantener y testear desde ya. La acción explícita queda como deuda
declarada abajo, y se implementará junto con la UX que la use.

**Archivos tocados**
- `app/core/radar/store.py` — `_MARK_SUCCESS_SQL` deja de escribir
  `needs_attention = 0`. Es la única sentencia de éxito: `update_last_run` y
  `materialize_successful_run` la comparten vía `_mark_success`, así que ambas
  cambian a la vez y no pueden divergir.
- `tests/test_radar_store.py`, `tests/test_radar_runner.py`.
- `app/core/radar/runner.py` — **sin cambios**: la semántica no estaba
  duplicada ahí, solo delega en el materializador.

**Invariantes**
- **INV-NA1** — Una corrida exitosa (automática o manual, `radar_ok` o
  `radar_empty`) limpia `failure_count`, `permanent_failure_streak`,
  `last_error_*`, `last_failure_reason`, `next_run_after` y actualiza
  `last_run_at`. **Nunca** toca `needs_attention` ni `enabled`.
- **INV-NA2** — `needs_attention` es **sticky**: ningún camino automático la
  baja. La sube solo `_RECORD_FAILURE_SQL` al alcanzar el umbral de racha
  permanente; bajarla exige una acción humana explícita que hoy no existe.
- **INV-NA3** — `enabled` y `needs_attention` son **ejes independientes**. La
  alerta se preserva igual con `enabled=0` que con `enabled=1`: sin `CASE`, sin
  condicional. Reactivar (`set_enabled`) no reconoce; reconocer no reactiva.
- **INV-NA4** — Correr manualmente un radar con `enabled=0` (vía `_do_run`, que
  no valida `is_enabled`) no lo reactiva ni le limpia `needs_attention`: sigue
  sirviendo como "probar" dentro del flujo revisar/probar/reactivar.
- **INV-NA5** — El éxito confirma **recuperación técnica**, no **reconocimiento
  humano**. Que SECOP vuelva a responder no prueba que alguien entendiera ni
  corrigiera la causa que exigió revisión.

**Deuda declarada — reconocimiento humano explícito**
No existe hoy ninguna vía para bajar `needs_attention`. Es deliberado y es
deuda: un radar alertado conserva la marca para siempre hasta que se implemente
la acción explícita (`clear_needs_attention(radar_id, *, reactivate: bool)` o
equivalente) **junto con** el comando/UI que la dispare. Precondición de P1B-6,
que es donde la ficha del radar expone el estado al usuario. Mientras tanto la
marca es informativa (`needs_attention` no bloquea el selector de eventos, §K) y
`set_enabled` sigue siendo la vía de reactivación.

**Tests ejecutados** (`test_radar_store.py` + `test_radar_runner.py`)
1. Éxito sobre radar pausado (`enabled=0`, `needs_attention=1`, contadores en 3)
   → alerta y pausa intactas, contadores/errores/backoff en cero.
2. Éxito tras `set_enabled(True)` explícito → `enabled=1`, `needs_attention=1`.
3. `radar_empty` tras reactivación → misma preservación; un vacío no reconoce.
4. Radar sin alerta → sigue en `needs_attention=0` (preservar no es encender).
5. `update_last_run` directo → misma semántica que el materializador.
6. Fallo permanente tras recuperación → racha reinicia en 1, alerta sigue, no
   re-pausa (1 < 3).
7. Fallo transitorio tras alerta → corta la racha, no toca la alerta.
8. Estructural sobre `_MARK_SUCCESS_SQL`: no menciona `needs_attention` ni
   `enabled` —cierra también la puerta al `CASE WHEN enabled = 1`.
9. Rollback de éxito sobre radar alertado → estado del contrato intacto
   (atomicidad de Preflight 1).
10. `materialize_follows_refresh` sobre radar alertado → no aplica semántica de
    radar de búsqueda.

**GO** — cumplido: 311 tests de radar verdes, suite completa verde (2150 passed).
Los dos tests que congelaban la semántica vieja se corrigieron explícitamente,
no se bajó la política: el de caracterización de Preflight 1 y
`test_success_does_not_silently_reenable_disabled_radar`, que asertaba
`needs_attention == 0` de refilón y quedó absorbido por el test 5.
Validado por mutación: reintroducir `needs_attention = 0` mata 6 tests; añadir
`enabled = 1` mata 3.

**Modelo:** Sonnet High (cambio acotado, contrato ya cerrado).

---

## P1B-1 — Identidad canónica, endpoints y preferencias

**Objetivo:** poder responder "¿quién es esta persona y dónde acepta recibir?"
**Sin enviar nada. Sin outbox.**

**Archivos esperados**
- `app/core/notify/identity_store.py` *(nuevo)* — principals + owner_keys.
- `app/core/notify/endpoint_store.py` *(nuevo)* — endpoints + verificación.
- `app/core/notify/preference_store.py` *(nuevo)* — preferencias por (radar, endpoint).
- `app/core/notify/masking.py` *(nuevo)* — saneado de logs y direcciones.
- `app/core/radar/store.py` *(mínimo)* — `archived_at` + `archive_radar`/
  `restore_radar` (semántica de **archivo**, no borrado — ver addendum
  §D/§E/§K; `delete_radar` deja de ejecutar un `DELETE` duro).
- `tests/test_notify_identity.py`, `tests/test_notify_endpoints.py`,
  `tests/test_notify_preferences.py` *(nuevos)*.

> **No tocar** `runner.py`, `diff.py`, `digest.py`, `scheduler.py`.
> Tres módulos pequeños de una responsabilidad, no un `notify_store.py` que
> acumule todo (política "no monolitos").

**Migración**
- `principals`, `principal_owner_keys`, `notification_endpoints`,
  `radar_notification_preferences` (DDL §I del addendum).
- `ALTER TABLE radars ADD COLUMN archived_at TEXT` + `archive_radar(radar_id)`
  (`UPDATE ... SET archived_at`) y `restore_radar(radar_id)`
  (`UPDATE ... SET archived_at = NULL`); todos los `SELECT` de radares activos
  filtran `archived_at IS NULL`. **No se toca `UNIQUE(owner, name)`**: el
  nombre sigue reservado mientras el radar esté archivado, a propósito
  (addendum §E.C) — reutilizar el nombre de un radar archivado exigiría
  migrar esa constraint y **no** se hace en este bloque.
- Aditiva e idempotente, patrón `_ensure_columns` (`store.py:226`).

**Invariantes cubiertos:** INV-ID1..ID5, INV-PR5.

**Tests mínimos**
1. Endpoint sin `verified_at` → no elegible.
2. Endpoint `enabled=0` → no elegible.
3. Endpoint `revoked_at` → no elegible, **la fila sigue existiendo**.
4. `UNIQUE(channel, address_hash)` impide dos endpoints al mismo destino.
5. Un `owner_key` no puede apuntar a dos principals.
6. `principal_id` de un `owner_key` inexistente → resuelve a `None`, no crea nada.
7. Preferencia `enabled=0` por defecto (opt-in explícito).
8. `UNIQUE(radar_id, endpoint_id)`.
9. `timezone` default `America/Bogota` persistido **explícitamente**.
10. Borrar un principal con endpoints → `RESTRICT` (falla ruidosamente).
11. `archive_radar` marca `archived_at`; el radar desaparece de `list_radars` y
    de `list_all_enabled_radars`; **sus runs y events siguen existiendo**.
12. Radar archivado → crear otro con el **mismo** `(owner, name)` falla
    (`RadarExistsError`): el nombre sigue reservado.
13. `restore_radar` limpia `archived_at`; el radar reaparece en `list_radars`
    con su historial (`runs`/`events`/`seen`) intacto.
14. `masking.safe_owner("email:foo@bar.com")` no devuelve el email.

**GO** — todo lo anterior verde + suite completa verde + `archive_radar` ya no
destruye historia.
**NO-GO** — si el cambio a archivo rompe cualquier test de radar existente:
parar y reportar (no ajustar el test para que pase).

**Modelo:** Sonnet High (CRUD + constraints, contrato ya cerrado).

---

## P1B-2 — Outbox y delivery ledger idempotentes

**Objetivo:** crear intenciones de entrega y registrar intentos. **Sin proveedor.
Sin selección de eventos.** Las tablas y su concurrencia, aisladas.

**Precondición:** P1B-Preflight 1 cerrado. `_emit_events` ya escribe dentro de
`materialize_run`; este bloque solo añade la población de `event_seq` a esa
misma transacción, no reabre la cuestión de atomicidad.

**Archivos esperados**
- `app/core/notify/outbox_store.py` *(nuevo)* — batches + batch_events + claim.
- `app/core/notify/claim.py` *(nuevo)* — UPDATE condicional + fencing.
  Espejo de `app/core/radar/lease.py` en rigor, **independiente** de él.
- `app/core/notify/ledger_store.py` *(nuevo)* — delivery attempts.
- `tests/test_notify_outbox.py`, `tests/test_notify_claim.py`,
  `tests/test_notify_ledger.py` *(nuevos)*.

**Migración**
- `radar_notification_batches` (con `UNIQUE(endpoint_id, idempotency_key)` **y**
  `UNIQUE(notification_id, endpoint_id)` como destino de FK compuesta),
  `radar_notification_batch_events` (con `endpoint_id` + `UNIQUE(endpoint_id,
  event_id)` + FK compuesta `(notification_id, endpoint_id)` — ver addendum §G.5),
  `radar_delivery_attempts` (con `phase` y el CHECK dispatching/finished — ver
  addendum §G.6), `radar_notification_cursors` (`last_examined_seq`, no `last_seq`).
- `radar_event_seq` (tabla contador AUTOINCREMENT, independiente de
  `radar_events` — decisión y justificación de retención en addendum §I) +
  `radar_events.event_seq` + índice único. La reserva del `event_seq`
  (`INSERT` en `radar_event_seq` + `last_insert_rowid()`) y la inserción del
  `radar_event` correspondiente ocurren **sobre la misma conexión y dentro de
  la misma transacción `materialize_run`** (Preflight 1) — no son un paso
  previo suelto.
- **`_emit_events`, ya dentro de `materialize_run` (Preflight 1), empieza a
  poblar `event_seq`.** Eventos previos quedan con `event_seq IS NULL` →
  **invisibles para el cursor a propósito** (son historia anterior al push;
  notificarlos sería el backfill que INV-ELG1 prohíbe).

**Invariantes cubiertos:** INV-ID-1..ID-3, INV-ID-5, INV-ID-6, INV-CU2, INV-TR2.

**Tests mínimos**

*Identidad del batch (addendum §G.4 — Corrección 1):*
1. `{1,5}` y `{1,2,3,4,5}` producen `idempotency_key` **distintas** (min/max no
   identifica el conjunto).
2. El mismo conjunto de `event_seq` en distinto orden de entrada produce la
   **misma** `idempotency_key` (ordenar antes de serializar).
3. Cambiar el texto renderizado (formatter) **no** cambia la `idempotency_key`
   — el hash depende solo de `policy_version`+`endpoint_id`+`radar_id`+conjunto.
4. Crear el mismo batch (mismo conjunto exacto) dos veces → **una** fila
   (`UNIQUE(endpoint_id, idempotency_key)`).
5. `batch_key` de dos batches con el mismo `(radar_id, endpoint_id)` pero
   distinto conjunto de eventos **coincide** (es una etiqueta gruesa, no única) —
   confirma que `batch_key` no sustituye a `idempotency_key`.

*Unicidad evento-endpoint entre batches (addendum §G.5 — Corrección 2):*
6. El mismo `event_id` no entra dos veces **en el mismo** batch
   (`PRIMARY KEY(notification_id, event_id)`).
7. El mismo `event_id`, mismo `endpoint_id`, **no puede** asociarse a **dos
   batches distintos** (`UNIQUE(endpoint_id, event_id)`).
8. Insertar una fila de `batch_events` cuyo `endpoint_id` no coincide con el
   `endpoint_id` del batch referenciado → falla por la FK compuesta.
9. **Concurrente:** dos selectores (`threading`) intentan materializar el
   mismo evento para el mismo endpoint en batches diferentes y simultáneos →
   solo una asociación gana; el otro selector detecta el conflicto y no deja
   un batch huérfano de un solo evento.

*Claim y concurrencia general:*
10. Dos claims concurrentes (`threading`, patrón `test_radar_store.py:6`) → **uno**
    gana; el otro no ve la fila.
11. Claim vencido → otro worker lo recupera.
12. Worker con token viejo intenta marcar `sent` → `rowcount=0`, **no escribe**.
13. Worker con token viejo intenta liberar → no toca el claim del sucesor.
14. `not_before` futuro → no reclamable.
15. `event_seq` es estrictamente creciente y **no se reutiliza tras borrar
    eventos** (crear, archivar radar → CASCADE de runs/events *si aplicara*,
    crear otro; el seq sigue subiendo). Nota: con archivado (no borrado) en
    P1B-1, este caso solo se ejercita si se fuerza un borrado directo en el
    test — documentar que en producción `archive_radar` no dispara CASCADE.
16. Marcar `sent` sin attempt registrado → error (INV-TR2).
17. Attempt con `outcome='ambiguous'` deja el batch en `ambiguous`, **no** en
    `pending`.
18. Attempt en `phase='dispatching'` con `outcome` no NULL → rechazado por el
    CHECK (violación de fase).
19. Attempt en `phase='finished'` con `outcome` NULL → rechazado por el CHECK.
20. `UNIQUE(notification_id, attempt_number)`.
21. Borrar el batch **no** borra sus attempts (`RESTRICT`).

**GO** — dos workers concurrentes reales (threads) no reclaman el mismo batch;
el test de concurrencia evento-endpoint (9) no deja huérfanos; `event_seq`
demostrado no reutilizable; suite verde.
**NO-GO** — cualquier test de concurrencia flaky. Un claim que falla 1 de 100
veces es un claim roto.

**Modelo:** **Opus** — concurrencia, fencing, atomicidad bajo autocommit.

---

## P1B-3 — Selector, agrupación y formatter (shadow-only)

**Objetivo:** producir **exactamente lo que se enviaría**, materializado en
outbox real. **Sin worker. Sin proveedor.**

**Archivos esperados**
- `app/core/notify/selector.py` *(nuevo)* — cursor + elegibilidad.
- `app/core/notify/grouping.py` *(nuevo)* — batch_key, colapso por `result_key`, orden.
- `app/core/notify/render.py` *(nuevo)* — payload → texto por canal, con links.
- `tests/test_notify_selector.py`, `tests/test_notify_grouping.py`,
  `tests/test_notify_render.py` *(nuevos)*.

**Migración:** ninguna.

**Contrato del cursor (addendum §H/Cursor — Corrección 3):** `last_examined_seq`
es un **watermark de lo examinado**, no de lo entregado. Selección elegible +
creación de batches + inserción de `batch_events` + avance de
`last_examined_seq` ocurren en **una sola transacción**: si algo falla,
`ROLLBACK` completo y el cursor no avanza sobre trabajo a medias. Una ventana
sin ningún evento elegible **también** avanza el cursor —de lo contrario se
reescanearían para siempre eventos ya clasificados como no elegibles
(`baseline`, `window_churn`, etc.)—. Para `max_items`: el batch asocia
**todos** los eventos elegibles de la ventana en `batch_events` (representados,
trazables), el payload renderiza los primeros N + `truncated_count`, y el
cursor avanza sobre el total examinado porque todo quedó representado, no
porque se haya descartado en silencio.

**Invariantes cubiertos:** INV-ELG1..ELG8, INV-CU1..CU7, INV-PR2..PR4, INV-TR1.

**Tests mínimos**
1. Cada kind de la matriz §C → elegible / no elegible (una aserción por fila).
2. **20 eventos con `ts` idéntico** (el caso real de `_emit_events`) → los 20 se
   seleccionan, ninguno se repite. *El test que justifica el cursor.*
3. Evento ya en `batch_events` → no se reselecciona aunque el cursor retroceda.
4. Cursor avanza solo hasta el `event_seq` **materializado**, no hasta el leído.
5. Tres eventos del mismo `result_key` → una línea; el batch referencia los tres.
6. Orden determinista: mismo input → mismo output byte a byte, 100 iteraciones.
7. `max_items=3` con 10 eventos elegibles → `batch_events` contiene los **10**;
   el render muestra 3 + "7 más" (`truncated_count=7`); el cursor avanza sobre
   los 10 porque los 10 quedaron representados en la asociación, no solo los 3
   renderizados.
8. Quiet hours en `America/Bogota` → `not_before` = próximo fin de ventana
   **en hora local**, no UTC.
9. Quiet hours **cruzando medianoche** (22:00→07:00) — el caso que rompe una
   comparación ingenua.
10. Item sin `urlproceso` → se incluye sin link; `links_missing` en el payload.
11. Render de un batch al límite → **un solo mensaje**, nunca troceado (N7).
12. `include_value_changes=0` → los `changed_value` no entran, los `new` sí.
13. **Ventana con solo eventos `baseline`** (ninguno elegible) → el cursor
    **avanza** hasta el último `event_seq` examinado y se crean **cero** batches
    (INV-CU6 — evita rescaneo infinito de eventos ya clasificados).
14. **Fallo al insertar un batch** dentro de la transacción de selección →
    `ROLLBACK` completo: el cursor **no avanza**, ningún `batch_events` parcial
    queda escrito (INV-CU5).
15. Radar en `needs_attention` con eventos elegibles ya materializables de una
    corrida exitosa anterior → **sí se selecciona** (INV-ELG8: el estado
    operativo del radar no invalida eventos ya detectados).
16. Radar con `next_run_after` activo (backoff de SECOP) con eventos elegibles
    pendientes → **sí se selecciona**, por la misma razón que 15.
17. Endpoint no verificado/deshabilitado/revocado → no se selecciona para él
    (sí puede seleccionarse para otro endpoint del mismo principal).
18. Radar archivado → no se selecciona para ningún endpoint.
19. Principal `suspended` → no se selecciona para ninguno de sus endpoints.

**GO** — matriz §C completa con test por fila; los tests (2), (13) y (14)
verdes; render determinista; los tests (15)/(16) confirman que el selector no
filtra por estado operativo del radar.
**NO-GO** — cualquier fila de la matriz sin test, o cualquier filtro de
selección no listado en INV-ELG8 (opt-out, endpoint inválido, radar archivado,
batch expirado, principal suspendido).

**Modelo:** Sonnet High (selector/grouping) · **Opus** para revisar el cursor.

---

## P1B-4 — Worker de entrega con proveedor fake y fault injection

**Objetivo:** el ciclo completo contra un proveedor **falso** programable.
**Ningún byte sale a la red.**

**Archivos esperados**
- `app/core/notify/worker.py` *(nuevo)* — claim → revalidar → render → enviar → registrar.
- `app/core/notify/provider.py` *(nuevo)* — protocolo `Provider` + `SendResult`
  (`error_class`, `retry_after`, `provider_message_id`).
- `app/core/notify/providers/fake.py` *(nuevo)* — fake programable.
- `app/core/notify/retry.py` *(nuevo)* — política por `error_class`.
- `tests/test_notify_worker.py` *(nuevo)*.

**Migración:** ninguna.

**Invariantes cubiertos:** INV-ID-5, INV-ID-6, INV-PR1, INV-PR3, INV-TR2, INV-TR3.

**Contrato de write-ahead (addendum §G.6 — Corrección 4):** el worker inserta
el `DeliveryAttempt` en `phase='dispatching'` (`outcome=NULL`) y hace `COMMIT`
**antes** de llamar a `provider.send(...)`; solo después llama al proveedor y
recién entonces actualiza `phase='finished'` con el `outcome`. Sin este
registro previo, un crash antes de llamar al proveedor y uno durante la
llamada son **indistinguibles localmente** — por eso ambos casos de fault
injection de abajo (2 y 3) terminan en el **mismo** estado (`ambiguous`), y
**no** se afirma que un reenvío "quede registrado como duplicado detectable":
sin evidencia de qué pasó con el intento anterior, no hay forma de saberlo.

**Fault injection — un test por escenario de §D**
1. Envío OK → `phase='finished'`, `sent` + `provider_message_id` + attempt con outcome.
2. Crash **justo después de persistir `phase='dispatching'` y antes de llamar
   al proveedor** (fake lanza antes de `provider.send`) → el claim vence, otro
   worker reclama, encuentra el `dispatching` huérfano, lo cierra a
   `finished`/`ambiguous` (§F "reconciliación al reclamar de nuevo"), y decide
   si vale un intento nuevo según política de reintento — **no** asume que no
   se envió.
3. Crash **dentro/durante** la llamada al proveedor (el fake simula la
   excepción en medio de `provider.send`, después de que "salió" la petición) →
   mismo resultado que (2): `ambiguous`. **Ambos casos (2) y (3) quedan
   indistinguibles localmente** — es exactamente lo que el diseño reconoce, no
   lo que pretende resolver. *El test que demuestra que effectively-once, con
   sus límites declarados, es lo que hay.*
4. Timeout tras aceptación → `ambiguous`, **no** `failed`; batch en `ambiguous`;
   **cero reintentos automáticos**.
5. 429 con `Retry-After: 60` → `not_before = now+60s`; no se martillea.
6. 500 → `retry` con backoff exponencial y tope.
7. 400 / dirección inválida → `dead` inmediato, **sin reintento**; el endpoint
   queda `needs_verification`, **no** revocado.
8. Endpoint revocado entre crear y reclamar → `dead`, `endpoint_revoked`.
9. **Opt-out** entre crear y reclamar → `dead`, `opted_out`. Nada se envía.
10. Radar archivado entre crear y reclamar → `dead`, `radar_archived`; **el
    batch conserva su payload**.
11. Preferencia editada (`max_items`) tras crear → **no** reabre el batch.
12. `expires_at` vencido → `dead`, `expired`, **registrado**.
13. `max_attempts` agotado → `dead`, con el último `error_class`.
14. **Dos workers en threads** sobre 50 batches → 50 envíos, cero duplicados
    (en ausencia de crash — la garantía de (2)/(3) es distinta: ahí el sistema
    admite que puede duplicar, acotado y medido, no que lo impida).
15. El fake devuelve un error con un token dentro → `error_message_safe` **no lo
    contiene**.

**GO** — los 15 verdes, especialmente (2)/(3) *demostrando que ambos crashes
son indistinguibles y ambos van a `ambiguous`* y (4) *demostrando que
`ambiguous` no reintenta solo*. Métricas expuestas: `sent/ambiguous/dead/retry`.
**NO-GO** — si (2) o (3) intentan clasificar el crash como "seguro reintentar"
o "duplicado confirmado" sin evidencia: el ledger estaría afirmando algo que
no puede saber.

**Modelo:** **Opus** — es donde vive la semántica de fallo.

---

## P1B-5 — Adaptador real de un proveedor (deshabilitado)

**Objetivo:** hablar Telegram de verdad. **Apagado por defecto.**
**Canal:** Telegram (el más estable; OpenWA después).

**Archivos esperados**
- `app/core/notify/providers/telegram.py` *(nuevo)* — implementa `Provider`,
  **síncrono** (`httpx.Client`): el scheduler no tiene event loop (N9).
- `app/config.py` *(mínimo)* — `radar_notify_enabled: bool = False`.
- `tests/test_notify_provider_telegram.py` *(nuevo)* — respuestas mockeadas, sin red.

> **No reusar `_send_telegram` de `app/main.py`.** Es `async`, vive en el router y
> colapsa todos los errores en un bool (N8/N9). Se reusa **el conocimiento del
> protocolo** (URL, payload, troceo), no la función. `app/main.py` **no se toca**:
> el webhook sigue respondiendo como hoy.

**Migración:** ninguna.

**Invariantes cubiertos:** INV-ID-4 (`external_idempotency_key` queda NULL:
Telegram no la soporta — **se documenta en el código, no se finge**).

**Tests mínimos** (todos con transporte mockeado)
1. 200 → `sent` + `provider_message_id` del `result.message_id`.
2. 429 → `rate_limited` + `retry_after` de `parameters.retry_after`.
3. 400 `chat not found` → `invalid_address`.
4. 401 → `auth_error` (token malo) — **no** `invalid_address`.
5. 500 → `provider_error`.
6. Timeout → `timeout_after_send`, **no** `network_error` (ya salió a la red).
7. Error de conexión previo → `network_error` (**no** salió).
   *(6) vs (7) es la distinción de la que depende toda §E.J.*
8. `radar_notify_enabled=False` → el worker **no instancia** el proveedor real.
9. El bot token **no** aparece en ningún log ni en `error_message_safe`.

**GO** — flag **False** en `main`; suite verde; ninguna llamada real en tests.
**NO-GO** — si el flag por defecto queda `True`, o si (6)/(7) no se distinguen.

**Modelo:** Sonnet High (contrato cerrado; es traducción de protocolo).

---

## P1B-6 — Shadow run, métricas y activación controlada

**Objetivo:** correr en producción **sin enviar**, comparar, y activar a un solo
usuario con vuelta atrás.

**Precondiciones duras (NO-GO si falta una)**
- Preflight 1, 2 y 3 cerrados — pero nótese que esto ya se exigió **al
  principio del roadmap**, no aquí: Preflight 1 es precondición dura de
  P1B-2 (§ arriba), así que si se llegó hasta P1B-6 siguiendo el orden de
  este plan, ya está cerrado. Esta lista es una **doble verificación**, no el
  primer punto de control.
- P1B-1..5 cerrados.

**Archivos esperados**
- `scripts/run_radar_notifier.py` *(nuevo)* — entrypoint del worker
  (dry-run **default**, `--send` explícito).
- `app/core/notify/metrics.py` *(nuevo)*.
- `deploy/secoppal-notifier.{service,timer}` *(nuevos)*.
- `docs/radar/NOTIFIER_RUNBOOK.md` *(nuevo)*.

**Fases**
1. **Shadow (≥7 días).** Outbox real, cero envíos. Se mide: batches creados,
   eventos por batch, `expired`, quiet-hours pospuestos, tamaño de render,
   `links_missing`.
2. **Revisión humana.** Leer 20 payloads reales. Pregunta: *"¿esto le sirve a
   alguien que vigila un mercado?"* Si la respuesta es tibia, el problema es de
   producto y **no lo arregla activar el envío**.
3. **Activación a un usuario** (el propio equipo), un radar, Telegram.
   ≥7 días. Vuelta atrás = `radar_notify_enabled=False`.
4. **Beta.** Solo si `ambiguous_rate ≈ 0` y `duplicate_reports = 0`.

**Métricas GO**
| Métrica | Umbral |
|---|---|
| `ambiguous_rate` | < 1% (si sube: el timeout del proveedor está mal calibrado) |
| `duplicate_reports` | **0** |
| `dead_rate` por `expired` | < 5% (si sube: el worker no corre bastante) |
| `sent_rate` | > 95% de los no-`dead` |
| Falsos push (baseline/churn/empty) | **0 — cualquiera es NO-GO inmediato** |

**GO** — 7 días de shadow + 7 de un usuario, umbrales cumplidos, runbook escrito.
**NO-GO** — **un solo** push de `baseline`, `window_churn` o `radar_empty`.
Un push falso destruye más confianza que diez ausentes.

**Modelo:** **Opus** (análisis del shadow y decisión de activación).

---

## Resumen de bloques

| Bloque | Entrega | Toca código productivo del núcleo | Migración | Modelo |
|---|---|---|---|---|
| Preflight 1 | Materialización atómica de la corrida | **Sí** (`store.py`, `runner.py`) | Ninguna nueva (reordena escrituras existentes) | **Opus** |
| Preflight 2 | Investigación + concurrencia de `follows` | Solo si la investigación lo confirma | Ninguna | Sonnet High (Opus si hay carrera de creación) |
| Preflight 3 | `needs_attention` sticky (el comando explícito queda como deuda) | Sí (`store.py`) | Ninguna | Sonnet High |
| P1B-1 | Identidad, endpoints, preferencias | No | 4 tablas + `radars.archived_at` | Sonnet High |
| P1B-2 | Outbox, ledger, claim | No | 4 tablas + `event_seq` | **Opus** |
| P1B-3 | Selector, agrupación, render | No | — | Sonnet High (+Opus review) |
| P1B-4 | Worker + fake + fault injection | No (fake) | — | **Opus** |
| P1B-5 | Adaptador Telegram | No (flag off) | — | Sonnet High |
| P1B-6 | Shadow, métricas, activación | No | — | **Opus** |

**Cada bloque es un commit.** Ninguno activa el envío real. El único que
envía es P1B-6, tras 14 días de evidencia y con los tres Preflight cerrados
desde el inicio del roadmap — no como parche de último momento.

## Nota de gobernanza (fuera de alcance de P1B)

La numeración de ADR está colisionada entre ramas (addendum §M: dos ADR-043,
dos ADR-045, dos ADR-046 distintos). **No se corrigen esas colisiones
históricas en este plan.** Se registra como tarea de gobernanza separada,
sin bloque ni modelo asignado aquí: un chequeo de CI que falle si algún
`## ADR-NNN` aparece más de una vez en `docs/decisions_log.md` tras fusionar
ramas — detección automática, no reescritura del historial existente.

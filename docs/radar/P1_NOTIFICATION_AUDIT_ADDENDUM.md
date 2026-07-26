# P1 — Addendum de auditoría de notificaciones (P1B-0)

> **Estado:** contrato de diseño. No hay código productivo asociado.
> **Rama:** `radar/mvp-manual` · **HEAD:** `02ffcfd` (P0D.1) sobre `80aac0e` (P1A).
> **Fecha:** 2026-07-15
>
> Este documento **amplía**, no reemplaza, la auditoría histórica del Radar
> (`docs/planes/radar-produccion-scheduler-push.md`, Fase B) y el plan de ejecución
> post-P0 (`docs/planes/radar-plan-ejecucion-post-p0.md`, bloques E5–E7). Ambos
> siguen siendo el registro de lo que se pensó entonces. Donde este addendum
> contradice a E5–E7 lo dice explícitamente y explica por qué.

---

## A. Resumen ejecutivo

El Radar hoy **detecta novedades y no las entrega**. Todo lo que existe aguas
arriba de la entrega (verdad del diff, ventana, outcomes, backoff, lease) está
cerrado en P0 y P1A. Todo lo que existe aguas abajo —identidad, canal,
preferencia, intención de entrega, intento de entrega— **no existe**: no hay un
subsistema de notificación del Radar en el repositorio (ni tabla, ni módulo, ni
test dedicados a esa responsabilidad).

Eso es una ventaja, no una deuda: no hay que desmontar nada. Pero implica que
P1B no es "enchufar un notificador al scheduler". El plan previo (E5/E6/E7)
asumía justamente eso, y esa asunción no sobrevive a la revisión del código real.

**Los tres hallazgos que cambian el diseño:**

1. **No existe identidad canónica.** `radars.owner` es un `TEXT` libre sin tabla
   ni FK, producido por `radar_owner()` con tres fuentes incompatibles
   (`channel_user_key` → `chat_id` → literal `"anon"`). La tabla `users` ya
   declara `primary_user_id` para vincular identidades entre canales, pero se
   inserta **siempre NULL** y **nadie la lee**: el modelo de identidad canónica
   está dibujado y nunca fue implementado. Adjuntar teléfonos a un `owner` así
   es adjuntarlos a una cadena de texto, no a una persona.

2. **El push no puede consumir "lo que devolvió el scheduler".** El flujo del
   plan E6 (`outcomes del scheduler → agrupar → notificar`) es fire-and-forget
   sobre memoria de proceso: si el proceso muere después de correr el radar y
   antes de notificar, los eventos quedan persistidos y **nadie los vuelve a
   mirar nunca**. El push necesita un cursor sobre `radar_events`, no sobre el
   retorno de `run_due_radars`.

3. **`ts` por sí solo es inseguro —no imposible— y `(ts, id)` sí bastaría
   técnicamente; se prefiere `event_seq` por claridad operativa.**
   `_emit_events` calcula `ts = _now()` **una sola vez** y lo estampa en todos
   los eventos de la corrida (`runner.py:323-327`). Que dos eventos compartan
   timestamp exacto no es un riesgo teórico: **es el caso normal**. Un cursor
   que compare solo por `ts` pierde o repite la corrida completa según use `>`
   o `>=`. Pero `(ts, id)` —con `id` el UUID4 de `store.py:32`— **sí forma un
   orden total estable**: `id` no correlaciona con nada, pero es único, y el
   par ordena sin ambigüedad. La razón para no usarlo no es que falle: es que
   un cursor de producción gana en **legibilidad operativa** ("¿cuántos
   eventos hay desde X?" no se responde restando dos strings concatenados) y
   en **consumo incremental simple** (`WHERE event_seq > :cursor` es un rango
   sobre un entero indexado; `WHERE (ts, id) > (:ts, :id)` es una comparación
   de tupla). Por eso se agrega **`event_seq` INTEGER explícito** — mismo
   orden total que `(ts, id)`, más auditoría y consumo por rango. `rowid`
   **no** se considera opción: además de reutilizarse tras el `ON DELETE
   CASCADE` de `delete_radar` (`store.py:168`), no es un contrato público
   sobre el que apoyar un diseño — SQLite no garantiza su estabilidad de
   significado entre reconstrucciones de tabla.

**Veredicto de diseño:** el contrato requiere seis entidades separadas
(principal, endpoint, preferencia, batch/outbox, asociación batch-evento,
intento), un cursor por secuencia monotónica dedicada, un claim propio para el
worker de entrega —independiente del lease de radar— y semántica
**effectively-once**, no exactly-once. Ni Telegram Bot API ni OpenWA REST
**documentan**, en las fuentes oficiales revisadas el 2026-07-15, una clave de
idempotencia para el envío saliente (ver §E y fuentes citadas allí); un
endpoint de reconciliación específicamente no se buscó ni se descarta —no
documentado en las fuentes revisadas, no verificado su ausencia—. Es una foto
puntual, no una limitación permanente declarada. El borde de envío actual, en
cualquier caso, ni siquiera distingue un timeout de un 400 (verificado en el
código, no en documentación de terceros).

---

## B. Riesgos nuevos o no cubiertos completamente por la auditoría inicial

La auditoría inicial (Fase B del roadmap push) cubría dos riesgos: carga a SECOP
y ruido de notificaciones. Su única regla de idempotencia era *"registrar el
envío (evitar reenviar el mismo `run_id`)"*. El plan E5–E7 añadió targets,
attempts, quiet hours y `digest_hash`. Lo que sigue es lo que ninguno de los dos
resuelve, verificado contra el código en `HEAD=02ffcfd`.

### B.1 — Los 15 riesgos del encargo

#### 1. Eventos correctos pero entregas duplicadas

**Cubierto parcialmente y mal.** E6 propone que *"el mismo `digest_hash` no se
re-registra como `would_send`"* (plan post-P0, línea 311). Esto falla en ambas
direcciones:

- **Falso duplicado (suprime envío legítimo):** un radar que detecta el mismo
  cambio de estado dos semanas seguidas produce el mismo `digest_hash`. El
  segundo envío —legítimo, sobre eventos nuevos— se suprime.
- **Falso distinto (duplica):** cualquier cambio cosmético en el formatter
  (un emoji, un salto de línea, el umbral de "top 3") cambia el hash. Un
  reintento tras un despliegue reenvía lo ya enviado.

La idempotencia debe anclarse en **la identidad de los eventos agrupados**
(`endpoint_id` + conjunto de `event_seq`), no en el texto renderizado. El texto
es una proyección; la identidad es el conjunto.

#### 2. Crash entre envío y marcado como enviado

**No cubierto, y el sustrato lo agrava.** `connect_sqlite` abre con
`isolation_level=None` (`app/core/db.py:14`) — **autocommit, sin transacciones**.
Cada método de `RadarStore` abre su propia conexión corta. No existe hoy una sola
unidad atómica multi-tabla en el Radar.

Consecuencia directa para el push: `sent` no puede escribirse "junto con" nada
más. Y si el worker cae entre `provider.send()` y `UPDATE ... SET status='sent'`,
al reiniciar encuentra un outbox en `claimed` con un mensaje ya entregado y sin
prueba de ello. **Ese estado es indistinguible de "nunca se envió"** salvo por
reconciliación externa; no se encontró una operación de reconciliación en las
referencias revisadas y no se verificó exhaustivamente su ausencia.

> **Hallazgo nuevo N1 — la pérdida ya existe aguas arriba.** El mismo problema
> afecta hoy a la *verdad*, no solo al push. En `_run_radar_locked`, el bucle de
> `upsert_seen` (`runner.py:552-557`) y `_emit_events` (`runner.py:562`) son
> operaciones separadas, cada una con su conexión y su commit. Si el proceso
> muere entre ambas, los resultados quedan **marcados como vistos** y sus eventos
> **nunca se emiten**; la corrida siguiente los clasifica `unchanged` y la novedad
> se pierde **permanentemente**. No es un riesgo de P1B, pero P1B lo hereda como
> **precondición dura, no como deuda a resolver en paralelo**: construir un
> outbox sobre un cursor de eventos que el propio runner puede perder es
> prometer entregar lo que nunca se escribió. La corrección exigida —una
> transacción corta que abarque `radar_run` + upserts de `seen` +
> `radar_events` + limpieza operativa, abierta solo después de terminar toda
> red, con la vigencia del lease revalidada como **primera escritura de esa
> misma transacción**— se especifica como **P1B-Preflight 1**
> (`docs/planes/radar-p1b-notificaciones.md`) y **bloquea el inicio de P1B-2**
> (outbox), no solo la activación de P1B-6.

#### 3. Timeout ambiguo del proveedor

**No cubierto en absoluto, y el borde actual lo hace irreparable.** Ver §B.2/N8:
`_send_openwa` (`app/main.py:313-335`) colapsa `status_code >= 400` y
`httpx.HTTPError` en el mismo `ok=False`. Un 429 (reintentable), un 400
(permanente) y un timeout de red (ambiguo) son hoy **el mismo valor**. Toda
política de reintento construida sobre ese booleano es incorrecta por
construcción. Tratamiento completo en §E y §D.

#### 4. Doble notificador

**No cubierto, y el lease de radar no protege.** El lease de P1A protege
`run_radar` (`runner.py:422-430`). El notifier es otro proceso, sobre otras
filas, con otro ciclo de vida: puede correr con el radar en reposo y debe poder
reintentar sin volver a correr el radar. Un segundo `run_radar_scheduler` con el
notifier enganchado tomaría los mismos eventos.

> **Hallazgo nuevo N15 — hay un camino que ya hoy corre sin lease.** El radar
> sintético de "procesos seguidos" **no pasa por `run_radar`**: el scheduler
> llama a la función `refresh_follows` (definida en `follows.py:158-229`)
> directamente desde `scheduler.py:128-138`. `refresh_follows` consulta la red
> y llama `store.record_events(events)` en `follows.py:229` **sin tomar ningún
> lease**. Dos schedulers concurrentes duplican hoy eventos `changed_state` de
> procesos seguidos. La afirmación "P1A cerró la concurrencia del Radar" es
> cierta para `run_radar` y **falsa para `follows`**.
>
> El radar sintético **sí** tiene una fila propia en `radars`
> (`get_or_create_follows_radar`, `follows.py:39-52`, con
> `frequency=FOLLOWS_FREQUENCY` — `follows.py:30`), con columnas de lease
> técnicamente reutilizables. Pero no se prescribe aquí "envolver
> `refresh_follows` en `try_acquire_run_lease`" sin antes revisar si la
> identidad de ese radar sintético —uno por owner, autocreado, sin la semántica
> de `enabled`/`needs_attention` de un radar de búsqueda normal— interactúa
> limpiamente con un mecanismo pensado para radares de búsqueda. Se abre como
> **P1B-Preflight 2** (investigación primero, código después).

#### 5. Cursor basado solo en timestamp

**No cubierto (E6 no tiene cursor de ningún tipo).** Y aquí el sistema es
hostil a la solución obvia:

> **Hallazgo nuevo N2 — el timestamp compartido es la regla, no la excepción.**
> `_emit_events` fija `ts = _now()` una vez (`runner.py:323`) y lo reparte a
> todos los eventos vía el dict `base` (`runner.py:324-327`). Una corrida con 40
> novedades produce **40 eventos con el mismo `ts` al microsegundo**.

> **Hallazgo nuevo N3 — no hay columna monotónica de repuesto.**
> `radar_events.id` es `uuid.uuid4().hex` (`store.py:32`, `store.py:550`):
> aleatorio, no ordenable. Queda el `rowid` implícito —la tabla no es
> `WITHOUT ROWID`—, pero SQLite **reutiliza rowids liberados** salvo
> `AUTOINCREMENT`, y sí hay borrado: `delete_radar` dispara `ON DELETE CASCADE`
> sobre `radar_events` (`store.py:168`) con `PRAGMA foreign_keys=ON`
> (`db.py:22`). Un cursor sobre `rowid` puede **saltarse eventos nuevos que
> reciclaron un rowid por debajo del cursor**.

Conclusión: `ts` solo falla por el timestamp compartido (N2), y `(ts, id)`
formaría un orden total técnicamente válido. Pero un cursor de producción gana
en claridad operativa, auditoría y consumo incremental por rango con una
**columna de secuencia monotónica dedicada** (`event_seq`, §G/§I). `rowid`
queda descartado: no solo se reutiliza tras CASCADE (N3), sino que no es un
contrato público sobre el que apoyar el diseño.

#### 6. Identidad fragmentada por canal

**No cubierto (E5 agrava).** E5 propone `radar_notification_targets(owner, channel, destination)`
— es decir, indexar los destinos por la misma cadena `owner` cuya fragilidad es
el problema.

> **Hallazgo nuevo N4 — `"anon"` es un owner compartido global.**
> `radar_owner()` (`commands.py:30-40`) cae, en último término, al literal
> `"anon"`. Todo usuario sin `channel_user_key` ni `chat_id` comparte **un mismo
> owner**. Hoy eso solo mezcla radares; con push, sería **entrega cruzada entre
> personas distintas**.

> **Hallazgo nuevo N5 — `primary_user_id` es una columna muerta.**
> `app/core/feedback.py:54` la declara con FK a `users(user_id)`;
> `feedback.py:106` le crea índice; `feedback.py:430` la inserta **literalmente
> `NULL`** en el único INSERT que existe; ninguna consulta del repositorio la
> lee. El vínculo entre la identidad Telegram y la identidad Streamlit de la
> misma persona **está modelado y nunca fue implementado**.

Hay, además, **tres universos de identidad desconectados**:

| Universo | Tabla | Clave | Quién lo escribe |
|---|---|---|---|
| Cuentas reales | `app_accounts` | `email` (PK) | `auth.py` — login con contraseña |
| Identidades por canal | `users` | `(channel, channel_user_key)` | `feedback.py:_upsert_user` |
| Dueño de radares | `radars.owner` | `TEXT` libre | `radar_owner()` |

Nada los une. `app_accounts.email` y `users.email` no tienen FK entre sí.

#### 7. Canal no verificado

**No cubierto.** E5 no tiene `verified_at`. Un `destination` insertado es un
destino que recibe. Sin prueba de posesión, un número tecleado con un dígito de
más entrega los radares de una persona a un desconocido.

> **Hallazgo nuevo N6 — el owner anónimo es un secreto que viaja en la URL.**
> `streamlit_app.py:538-544` persiste `anonymous_user_id` en
> `st.query_params["uid"]`. Compartir la URL **cede el owner**: quien tenga el
> link es ese owner a todos los efectos. Si un owner así pudiera registrar
> endpoints, un tercero con el link registraría **su propio teléfono** para
> recibir los radares ajenos. De ahí el invariante: el opt-in exige un principal
> **autenticado** (`app_accounts`), nunca un `anon:<uid>`.

#### 8. Ausencia de opt-in/opt-out

**No cubierto.** E5 tiene `enabled` en el target, que es un flag operativo
(admin apaga un canal roto), no un consentimiento (la persona pidió recibir).
Colapsarlos impide responder "¿esta persona aceptó?" — y borra la diferencia
entre "lo apagamos nosotros" y "lo apagó quien lo recibe".

#### 9. Eventos antiguos enviados tardíamente

**No cubierto.** Sin `expires_at`, un notifier detenido tres días y reanudado
entrega un lote de novedades muertas. En contratación pública el daño es
concreto: avisar de un proceso cuya fecha de cierre ya pasó no es solo ruido,
es una **falsa oportunidad**.

#### 10. Tormentas de mensajes

**Cubierto parcialmente.** E6 agrupa por owner, lo que ataca el caso "muchos
radares, un usuario". No cubre: primera corrida de un radar amplio (mitigado por
E1/baseline, aún no implementado en esta rama), reindexación masiva de SECOP que
cambia el estado de 400 filas a la vez, ni límite duro por ventana de tiempo.

#### 11. Privacidad del contenido

**No cubierto.**

> **Hallazgo nuevo N12 — el email en claro es la clave de partición y se
> replica en cada fila.** Con un usuario autenticado, `owner = "email:foo@bar.com"`
> (`streamlit_app.py:364`). Ese literal se almacena en `radars.owner` y se
> **denormaliza en cada fila de `radar_events.owner`** (`store.py:169`,
> `store.py:552`), tabla sin retención. Y se escribe a los logs:
> `scheduler.py:132-133` y `follows.py:185` loguean `owner=%s` en `logger.exception`.
> Hoy son PII en logs de aplicación; con push serían PII en logs junto a fallos
> de entrega.

#### 12. Falta de trazabilidad

**No cubierto.** El `radar_notification_attempts` de E6 guarda `digest_hash`,
no los eventos. Ante "¿por qué recibí esto?" el sistema puede mostrar un hash y
un texto, pero **no puede probar qué eventos lo originaron**. La cadena
`evento → batch → intento` no existe.

#### 13. Retención indefinida

**No cubierto en ninguna capa.** No hay política de retención para
`radar_events`, `radar_runs` (que guarda `soql` y `params_json` completos) ni
`radar_seen_results` (que guarda `last_snapshot_json`, la fila SECOP entera).
Un ledger de entregas añadiría destinos e identificadores de proveedor a un
sistema que hoy no borra nada.

#### 14. Proveedor caído o rate-limited

**No cubierto.** `_send_openwa` no lee `Retry-After` (`main.py:325-335`); no hay
circuit breaker; no hay backoff de canal. El backoff de P0D es **por radar y por
fallo de SECOP** — no aplica al proveedor de mensajería. Un 429 global haría que
N radares reintentaran a la vez contra el mismo proveedor caído.

#### 15. Eliminación de radar con entregas pendientes

**No cubierto, y el CASCADE actual es destructivo.**

> **Hallazgo nuevo N13 — borrar un radar borra la prueba de lo entregado.**
> `delete_radar` (`store.py:473-478`) ejecuta un `DELETE` duro. Con
> `foreign_keys=ON`, el `ON DELETE CASCADE` de `radar_runs`, `radar_events` y
> `radar_seen_results` (`store.py:147,168,188`) borra toda la historia. Si el
> outbox y el ledger colgaran de `radars` con CASCADE, **borrar un radar borraría
> la prueba de los mensajes ya enviados** — incluidos los que la persona recibió
> y sobre los que puede preguntar.

### B.2 — Hallazgos nuevos adicionales (no en la lista del encargo)

> **N7 — El troceo convierte un digest en N mensajes, y el reintento duplica.**
> `_send_openwa` (`main.py:313-335`) itera `_chunk_message(text)` (`main.py:224`)
> y hace un POST **por fragmento**, devolviendo un único `bool`. Si el fragmento
> 3 de 5 falla, `ok=False` — pero los fragmentos 1 y 2 **ya se entregaron**. Un
> reintento a nivel digest los reenvía. La duplicación parcial no es un riesgo:
> es el comportamiento garantizado del código actual. **Consecuencia de diseño:**
> o la unidad de idempotencia es el fragmento, o el render debe caber en un solo
> mensaje por canal. P1B elige lo segundo (§E.H).

> **N8 — El borde de envío destruye la información necesaria para reintentar.**
> `main.py:331-334`: `if resp.status_code >= 400: ok = False` /
> `except httpx.HTTPError: ok = False`. Timeout, 400, 429 y 500 son el mismo
> `False`. `_send_telegram` (`main.py:28-52`) tiene la misma forma. Ningún
> `error_class`, ningún `retry_after`, ningún `provider_message_id`.

> **N9 — Los clientes de envío no son adapters: viven en el router HTTP.**
> `_send_telegram` y `_send_openwa` son funciones `async` privadas dentro de
> `app/main.py`, escritas para responder **dentro del ciclo de un webhook**. El
> scheduler es un proceso **síncrono** (`scripts/run_radar_scheduler.py`). E7
> dice *"reusar el path de envío existente; NO reinventar cliente"* — pero
> reusarlo literalmente obliga al scheduler a arrastrar un event loop y el
> `settings` del proceso web. Lo reusable es el **conocimiento del protocolo**
> (URL, headers, troceo), no la función.

> **N10 — El plan E6 no tiene cursor: consume memoria de proceso.** El flujo
> documentado arranca en *"outcomes del scheduler"* (plan post-P0, línea 296):
> el `SchedulerSummary.outcomes` de `run_due_radars` (`scheduler.py:174-179`),
> una lista en RAM. Si el proceso muere tras persistir eventos y antes de
> notificar, esos eventos **no se vuelven a considerar jamás**: nada los marca
> como pendientes. Push fire-and-forget.

> **N14 — `radars.channel` es un default de presentación que nadie elige.**
> Se rellena desde `ctx.channel` al guardar (`handlers/radar.py:85`) y su default
> es `'web'` (`store.py:126`). No hay UI de selección — la auditoría original ya
> lo anotaba como decisión abierta (*"Selector de canal por radar en la UI (hoy
> `channel` se setea pero no se elige)"*). Confirmado: sigue sin elegirse. El
> enrutamiento de push **no** debe leer esta columna.

---

## C. Matriz de elegibilidad

Estado **verificado** contra `runner.py:_emit_events` (330-362) y
`follows.py:98-107, 200-229`. La columna "¿crea radar_event?" describe el
comportamiento **actual**, no el deseado.

| Kind / outcome | ¿Crea `radar_event` hoy? | ¿Elegible para notificar? | ¿Visible al usuario? | ¿Solo operacional? | Motivo |
|---|---|---|---|---|---|
| `baseline` | **Sí** (1 evento, `runner.py:331-335`) | **No** | Sí (feed) | No | Es la línea base de vigilancia, no una novedad. Notificarla envía el backfill entero como si fuera nuevo. **INV-E1.** |
| `new` | **Sí** (`runner.py:337-344`) | **Sí** | Sí | No | Novedad real: clave nunca vista. Núcleo del valor del Radar. |
| `changed_state` | **Sí** (`runner.py:345-353`) | **Sí** | Sí | No | Cambio de estado verificado contra `snapshot_hash`. |
| `changed_value` | **Sí** (`runner.py:354-362`) | **Sí** | Sí | No | Cambio de valor verificado contra `snapshot_hash`. |
| `followed` | **Sí** (`follows.py:98-107`) | **No** | Sí (feed) | No | Lo generó **la propia acción del usuario** al pulsar "seguir". Notificárselo es devolverle su propio clic. |
| `disappeared` (confirmado) | **No** — `classify` lo computa (`diff.py:151-152`) pero `_emit_events` **no lo emite** | **Sí, cuando exista** | Hoy solo en el digest | No | Semánticamente elegible: `confirmed_disappeared_keys` exige verificación explícita. Pero **hoy nadie llena ese conjunto** — `run_radar` llama `classify` sin ese argumento (`runner.py:524`). Elegible **en cuanto se emita**; no antes. **INV-E6.** |
| `window_churn` | **No** — se computa (`diff.py:154`), no se emite | **Nunca** | Solo agregado (`window_churn_count` en digest) | Sí | Ausencia de la ventana de 100 ≠ desaparición. Notificarlo es **afirmar un hecho falso**. **INV-E2.** |
| `radar_ok` (outcome) | No (es outcome de corrida) | No por sí mismo | Indirecto | Sí | Un outcome no es una novedad. Lo elegible son los eventos que produjo, no el hecho de correr. |
| `radar_empty` (outcome) | No | **Nunca** | No | Sí | "Sin novedades" no se envía. Regla de oro heredada de la auditoría original y confirmada. **INV-E3.** |
| `radar_timeout` (outcome) | No | **Nunca** | Solo en la ficha del radar | Sí | Fallo transitorio de SECOP. Ya tiene backoff (P0D). No es información para quien vigila un mercado. **INV-E4.** |
| `radar_transient_error` | No | **Nunca** | Ficha del radar | Sí | Ídem. Se resuelve solo o escala a permanente. |
| `radar_permanent_error` | No | **No como push comercial** | Sí, en la ficha (`needs_attention`) | Sí | El radar dejó de funcionar: **es información legítima**, pero de **canal operativo/UX**, no del digest de novedades. Aviso operativo, fuera de alcance de P1B (§I). |
| `radar_invalid_frame` | No | **No como push comercial** | Ficha (`needs_attention`) | Sí | Ídem — el radar está roto, requiere flujo de revisar/probar/reactivar (§I). |
| `radar_guard_blocked` | No | **No como push comercial** | Ficha (`needs_attention`) | Sí | Ídem — radar sin alcance ni tema. |
| `already_running` | No (`runner.py:53-55`: no toca `radar_runs`) | **Nunca** | No | Sí | Estado de **control**, no corrida. Otro ejecutor está haciendo el trabajo; sus eventos se notificarán por su propio camino. **INV-E5.** |
| `lease_lost` | No | **Nunca** | No | Sí | Estado de control. Los datos de ese ejecutor **no se persistieron**; el dueño actual produce los suyos. Notificar sería anunciar trabajo descartado. **INV-E5.** |
| `radar_not_found` | No | **Nunca** | No | Sí | El radar se borró en carrera. No hay a quién notificar de qué. **INV-E5.** |

**Elegibles en P1B:** `new`, `changed_state`, `changed_value`, y
`disappeared` **solo cuando se emita con confirmación explícita**. Nada más.

**Nota de honestidad:** el conjunto elegible que P1B puede entregar **hoy** es
`{new, changed_state, changed_value}`. `disappeared` está en el contrato porque
su semántica ya está decidida (§Invariante ELG-6), no porque exista.

---

## D. Modelo de amenazas y fallos

| # | Escenario | Qué pasa sin diseño | Decisión de P1B |
|---|---|---|---|
| 1 | **Dos workers** toman el mismo outbox | Dos mensajes idénticos | Claim atómico con fencing (`claim_token`), **independiente del lease de radar** (§F). El `UPDATE` admite `pending` o un `claimed` ya vencido, y decide por `rowcount`, nunca por SELECT previo. |
| 2 | **Crash después del claim y antes de persistir `dispatching`** | Batch `claimed`, pero no existe evidencia de que se haya iniciado una llamada externa | Al vencer `claim_until`, otro worker puede recuperar el batch de forma segura: no existe ningún `DeliveryAttempt` en fase `dispatching`. |
| 3 | **Crash después del COMMIT de `dispatching`** —antes, durante o después de `provider.send`— | Existe un intento iniciado cuyo efecto externo no puede determinarse localmente | Al vencer el claim, el worker sucesor cierra ese intento como `finished/ambiguous`, deja el batch `ambiguous` y **no reintenta automáticamente**. Después del write-ahead, un crash antes de la llamada y uno durante la llamada son indistinguibles desde SQLite. |
| 4 | **Timeout después de que el proveedor aceptó** | El worker cree que falló; reintenta; el usuario recibe dos | `outcome='ambiguous'` — **no** `failed`. Estado propio con política propia (§E.J). No se reintenta automáticamente salvo idempotencia externa. |
| 5 | **Proveedor devuelve 429** | Reintento inmediato → más 429 | `error_class='rate_limited'`, `retry_after` leído de la cabecera. `not_before = now + retry_after`. Backoff **por endpoint/canal**, no por radar (el backoff de P0D es de SECOP y no aplica). |
| 6 | **Proveedor devuelve 500** | Indistinguible de 400 hoy (N8) | `error_class='provider_error'`, transitorio, backoff exponencial con tope. Reintentable. |
| 7 | **Dirección inválida** (400 / número inexistente) | Reintentos infinitos contra un destino muerto | `error_class='invalid_address'`, **permanente**: `dead` inmediato, sin reintento. Marca el endpoint como `needs_verification` y **no** lo revoca solo (un 400 puede ser del payload, no del destino). |
| 8 | **Canal desvinculado** (revocado durante el ciclo) | Envío a un destino que la persona ya soltó | El worker **revalida el endpoint al reclamar**, no al crear. `revoked_at IS NOT NULL` → `dead`, motivo `endpoint_revoked`. **INV-ID5.** |
| 9 | **Opt-out con entregas pendientes** | Se envía lo ya encolado tras el opt-out | El worker revalida la preferencia **al reclamar**. Opt-out gana: `dead`, motivo `opted_out`. La intención es determinista; el consentimiento es **actual**. **INV-PR1.** |
| 10 | **Radar archivado** con outbox pendiente | Con `DELETE`+CASCADE real, se borraría el ledger (N13) | `radars` pasa a **archivado** (`archived_at`) — no a un `DELETE`, ni siquiera lógico: la fila y el nombre siguen reservados, restaurable con `restore_radar`. Outbox pendiente → `dead`, motivo `radar_archived`. El batch **conserva** `radar_id` y el payload congelado: la prueba de lo enviado sobrevive al archivado. |
| 11 | **Preferencia modificada durante el procesamiento** | Carrera: ¿aplica la vieja o la nueva? | Regla explícita: **la preferencia de contenido/agrupación se congela al crear el batch** (ya está en el payload); el **consentimiento y las quiet hours se evalúan al reclamar**. Cambiar `max_items` no reabre un batch creado; hacer opt-out sí lo mata. |
| 12 | **Dos eventos con el mismo timestamp** | Con cursor por `ts` solo: se pierden o se repiten | **Es el caso normal aquí** (N2), no un borde. `(ts, id)` sería un orden total válido; se prefiere `event_seq` monotónico dedicado por claridad operativa (§G/§I). |
| 13 | **Digest mayor al límite del canal** | Troceo → duplicación parcial en reintento (N7) | El render **cabe en un mensaje**: `max_items` corta, y el sobrante se resume con "y N más — ver en la app" + link. Nunca se trocea un batch en varios mensajes. **INV-PR4.** |
| 14 | **Evento demasiado antiguo** | Se entrega una oportunidad muerta | `expires_at` en el batch. Vencido → `dead`, motivo `expired`, **registrado** (no silenciado). **INV-PR3.** |

---

## E. Semántica de entrega: qué se promete y qué no

### Las cuatro semánticas, sin eufemismos

| Semántica | Definición | ¿Aplica a P1B? |
|---|---|---|
| **At-most-once** | Cada mensaje se entrega 0 o 1 veces. Nunca duplica; **puede perder**. Se logra marcando `sent` **antes** de llamar al proveedor. | **No.** Perder una novedad silenciosamente es el peor fallo posible para un producto de vigilancia: el usuario no sabe que no supo. |
| **At-least-once** | Cada mensaje se entrega ≥1 veces. Nunca pierde; **puede duplicar**. Se logra marcando `sent` **después** de confirmar, y reintentando ante la duda. | **Es la base mecánica de P1B.** Marcar `sent` solo tras confirmación del proveedor (**INV-ID5**). |
| **Effectively-once** | At-least-once + supresión de duplicados por clave de idempotencia, aceptando una ventana residual donde el duplicado es posible pero acotado y **observable**. | **Lo que P1B promete.** |
| **Exactly-once (real)** | Cada mensaje se entrega exactamente 1 vez, siempre, incluso con crash arbitrario. Exige **commit atómico entre el efecto externo y el registro local** — es decir, transacción distribuida con el proveedor, o idempotencia de servidor + reconciliación. | **Imposible aquí y no se promete.** |

### Por qué exactly-once es imposible en este sistema (no es prudencia: es el hecho)

Tres condiciones lo impiden, verificables hoy:

1. **No se encontró una clave de idempotencia documentada para los
   endpoints salientes revisados —conclusión acotada a las fuentes consultadas,
   no una afirmación universal sobre el proveedor.**
   - **Telegram Bot API.** La referencia oficial de `sendMessage`
     ([`core.telegram.org/bots/api#sendmessage`](https://core.telegram.org/bots/api#sendmessage),
     consultada 2026-07-15) no publica un parámetro de idempotencia o
     deduplicación aportado por el cliente. `message_id` aparece en la respuesta
     del servidor y no sirve como clave previa del envío. El issue
     [`tdlib/telegram-bot-api#126`](https://github.com/tdlib/telegram-bot-api/issues/126)
     es evidencia complementaria de una solicitud de esa capacidad, no un
     contrato normativo de la API.
   - **OpenWA.** En la referencia oficial revisada
     ([`docs.open-wa.org/reference/api-conventions/`](https://docs.open-wa.org/reference/api-conventions/),
     consultada 2026-07-15) no se encontró una clave de idempotencia documentada
     para el endpoint saliente considerado. La misma documentación describe
     validación estricta de payloads, pero este ADR **no afirma** que agregar un
     campo concreto vaya a romper `send-text` sin citar el schema exacto del
     endpoint. La idempotencia de webhooks entrantes, cuando exista, es una
     capacidad en la dirección opuesta y no demuestra idempotencia del envío.
   - **Consecuencia:** `external_idempotency_key` queda reservado y NULL. Las
     capacidades deben revalidarse contra documentación oficial inmediatamente
     antes de P1B-5.
2. **No se encontró un endpoint de reconciliación en las referencias revisadas.**
   No se verificó exhaustivamente su ausencia. Mientras no haya evidencia de
   una operación tipo “¿entregaste el mensaje con clave K?”, un estado
   `ambiguous` no puede resolverse automáticamente y requiere decisión explícita.
3. **No hay commit atómico entre el efecto externo y el registro local.** El
   efecto (mensaje en el teléfono) es irrevocable e insertado por un tercero;
   el registro es un UPDATE en SQLite en autocommit (`db.py:14`). No existe
   transacción que abarque ambos.

> **Fuentes consultadas (2026-07-15):**
> - Telegram Bot API — método `sendMessage`: <https://core.telegram.org/bots/api#sendmessage>
> - GitHub `tdlib/telegram-bot-api`, issue #126 — "Idempotence for method calls": <https://github.com/tdlib/telegram-bot-api/issues/126>
> - OpenWA — convenciones de API (validación estricta de payload, endpoint `send-text`): <https://docs.open-wa.org/reference/api-conventions/>
>
> Las tres son fotos puntuales de las respectivas APIs en la fecha de consulta,
> no se asumen permanentes. Cualquiera de los dos proveedores puede añadir
> soporte de idempotencia en el futuro; el diseño (`external_idempotency_key`
> reservada, §G) está pensado para adoptarlo sin migración si ocurre.

**Por tanto:** una `idempotency_key` en P1B es **local** — deduplica *nuestras*
decisiones (no crear dos batches para el mismo conjunto de eventos y endpoint),
no *las entregas del proveedor*. Se envía al proveedor **solo si algún día lo
soporta**, y el diseño reserva el campo `external_idempotency_key` para ese día
(**INV-ID4**).

### Flujo

```
radar_events (event_seq monotónico)
  → selección elegible        (kind ∈ {new, changed_state, changed_value};
                               cursor por endpoint: event_seq > last_examined_seq)
  → aplicación de preferencias (opt-in, include_*, max_items)
  → agrupación determinista    (canonical_event_set = event_seq únicos, orden asc.)
  → creación idempotente       (idempotency_key = sha256(policy_version|endpoint_id|
                                radar_id|canonical_event_set);
                                UNIQUE(endpoint_id, idempotency_key))
  → claim del worker           (UPDATE condicional + claim_token, con fencing)
  → revalidación               (endpoint vigente + consentimiento + quiet hours
                                + freshness)  ← el consentimiento se evalúa AQUÍ
  → render final               (desde el payload congelado)
  → INSERT DeliveryAttempt (phase=dispatching, outcome=NULL) + COMMIT
                               ← write-ahead: el intento existe ANTES de llamar
  → llamada al proveedor
  → UPDATE DeliveryAttempt (phase=finished, outcome=...)
  → sent | retry | ambiguous | dead
```

### Preguntas del encargo, resueltas

**A. ¿El payload se congela al crear el outbox o se renderiza al enviar?**
**Se congela al crear; se renderiza al enviar, desde lo congelado.** Los *datos*
(qué eventos, qué títulos, qué valores, qué URLs) se snapshotean en
`payload_json` al crear el batch: son un hecho fechado, y un evento de hace 6
horas debe contar lo que pasó hace 6 horas, no lo que dice SECOP ahora. El
*texto* se renderiza al enviar, porque el formato depende del canal
(WhatsApp ≠ Telegram ≠ email) y un reintento tras arreglar un bug de formato
debe salir bien. Congelar el texto ataría la entrega a la versión del formatter
del momento de creación; congelar los datos preserva la verdad histórica.
Corolario: **el texto no puede ser la clave de idempotencia** (§B.1.1).

**B. ¿Opt-out después de crear el outbox, antes del envío?**
**El opt-out gana.** La intención de entrega es determinista, no un derecho
adquirido. `dead`, motivo `opted_out`, registrado. **INV-PR1.**

**C. ¿Radar archivado?** No se borra: **`archive_radar`** marca
`radars.archived_at` y lo saca de listados/scheduler; **`restore_radar`** lo
recupera. El nombre permanece reservado mientras esté archivado —
`UNIQUE(owner, name)` sigue vigente porque la fila nunca se va—, así que
"crear otro radar con el mismo nombre" no está soportado mientras el original
siga archivado. Si se quisiera permitir reutilizar el nombre de un radar
archivado, eso requeriría migrar la restricción `UNIQUE(owner, name)` (p. ej.
a un índice único parcial `WHERE archived_at IS NULL`) — **no se declara
aditivo, es una migración de constraint que se decide aparte, no en P1B-0**.
Outbox pendiente al archivar → `dead`, motivo `radar_archived`. **Lo ya
enviado permanece en el ledger**: el batch guarda `radar_id` y el payload
congelado, sin FK CASCADE que lo borre. Si el radar se borrara de verdad (no
ocurre en este diseño), el sistema no podría responder "¿por qué recibí esto?".

**D. ¿Endpoint revocado?** Pendientes → `dead`, motivo `endpoint_revoked`.
Revocar es `revoked_at = now`, nunca `DELETE`: los intentos históricos apuntan a
él. **INV-ID5.**

**E. ¿Falta una URL?** El item **se envía igual, sin link**. `urlproceso` es
`_as_str(row.get(spec.url))` y puede quedar `""` (`keys.py:68`) — SECOP no
siempre la trae. Una novedad sin link sigue siendo la novedad; suprimirla sería
perder información real por un defecto de la fuente. Se marca
`links_missing = N` en el payload y el digest cierra con "ver en la app" hacia el
feed, que sí tiene el contexto. Nunca se inventa ni se reconstruye una URL.

**F. ¿Varios eventos del mismo proceso?** **Colapsan a uno por `result_key`**,
conservando el **último estado** y el **primer previo** de la ventana: si un
proceso pasó de Borrador → Publicado → Adjudicado, el usuario quiere
"Borrador → Adjudicado", no dos líneas. El batch referencia **todos** los
`event_seq` que colapsó (trazabilidad completa: la tabla de asociación mantiene
las N filas aunque el render muestre una). **INV-TR1.**

**G. ¿Cómo se ordenan los eventos dentro del digest?** Orden **determinista y
total**: (1) `kind` por prioridad fija —`new` > `changed_state` >
`changed_value` > `disappeared`—, (2) `value` descendente (lo caro primero: es
el criterio de interés en contratación), (3) `event_seq` ascendente como
desempate final. El tercer criterio **garantiza totalidad**: dos filas nunca
empatan, así que el mismo batch renderiza idéntico siempre. Sin él, el orden
dependería del orden de lectura de SQLite.

**H. ¿Cómo se limita la longitud por canal?** `max_items` en la preferencia,
con tope duro por canal en el adapter. El render **debe caber en un mensaje**
(consecuencia de N7). El sobrante no se trocea: se resume ("y 12 más — ver en la
app"). Un `max_items` que corta **se registra** en el payload
(`truncated_count`), no se silencia. **INV-PR4.**

**I. ¿Qué antigüedad máxima puede enviarse?** `expires_at = created_at + TTL`,
con TTL por frecuencia: `cada_6h` → 12h, `diaria` → 36h, `semanal` → 8d
(≈2× el periodo: tolera una pasada perdida, no una semana muerta). Vencido →
`dead`, motivo `expired`, **auditado**. **INV-PR3.**

**J. ¿Cómo se representa `ambiguous_delivery`?** Como un **outcome de primera
clase** del intento, jamás como `failed`:

- `DeliveryAttempt.outcome = 'ambiguous'`, `error_class = 'timeout_after_send'`.
- El **batch** queda en `status = 'ambiguous'` — **no vuelve a `pending`**.
- **No se reintenta automáticamente.** Reintentar un ambiguo es elegir duplicar;
  no reintentarlo es elegir quizá perder. Ninguna de las dos es correcta por
  defecto, así que la elección **no se toma en silencio**: el batch queda
  visible en métricas (`ambiguous_rate`) y solo se resuelve por decisión
  explícita —humana o, si algún día existe, por reconciliación del proveedor.
- Si el proveedor llegara a ofrecer idempotencia, `ambiguous` **sí** se
  reintentaría con la misma `external_idempotency_key`, que es exactamente lo
  que convierte at-least-once en effectively-once de verdad.

---

## F. Concurrencia: claim de entrega, independiente del lease de radar

**El lease de radar no protege al notifier.** Protege `run_radar`
(`runner.py:422-430`) sobre la fila del radar. El worker de entrega opera sobre
otras filas, en otro proceso, con otro ciclo: reintenta sin correr el radar, y
corre con el radar en reposo. Reusar el lease de radar acoplaría dos ciclos
independientes y bloquearía las corridas mientras se entrega. Y —N15— el lease
ni siquiera cubre `follows` hoy.

### Comparación de mecanismos

| Mecanismo | Cómo | Veredicto |
|---|---|---|
| **UPDATE condicional con lease** (copiar `lease.py`) | Columnas de lease en el outbox; `try_acquire` por fila | **Correcto pero mal encajado.** El lease de radar es *por recurso de larga vida*; un batch es *efímero y de un solo uso*. Copiar el módulo arrastra `renew`/`release` que un batch no necesita. |
| **claim_until + claim_token** | `UPDATE` condicional sobre `pending` o `claimed` vencido | **Elegido.** Un solo statement atómico; el ganador se decide por `rowcount`. El `claim_token` da fencing y la expiración permite recuperar un worker muerto. |
| **Status transition atómica sola** (`pending → claimed`) | `UPDATE ... WHERE status='pending'` | **Insuficiente por sí sola.** Sin `claim_until`, un worker muerto deja el batch en `claimed` **para siempre**. Sin `claim_token`, un worker vencido puede marcar `sent` sobre el trabajo de su sucesor. |

**Decisión:** `claim_until` + `claim_token` + transición de estado **en un solo
UPDATE condicional**. Es el patrón de `lease.py` (mismo rigor: decisión por
`rowcount`, nunca por SELECT previo) aplicado a un recurso efímero.

```sql
UPDATE radar_notification_batches
   SET status = 'claimed',
       claim_token = :token,
       claim_until = :until
 WHERE notification_id = :notification_id
   AND (
        status = 'pending'
        OR (status = 'claimed' AND claim_until <= :now)
   )
   AND (not_before IS NULL OR not_before <= :now)
```

La recuperación no se decide únicamente por el status:

- `claimed` vencido **sin** `DeliveryAttempt phase='dispatching'`: el sucesor
  puede continuar y crear su intento.
- `claimed` vencido **con** `dispatching` huérfano: el sucesor cierra ese intento
  a `finished/ambiguous`, deja el batch `ambiguous` y no inicia un envío nuevo.
- `claimed` no vencido: no es reclamable.

### Fencing — los tres ataques

Toda escritura posterior al claim lleva `AND claim_token = :token` en el WHERE.
Eso cierra:

1. **Worker vencido marcando `sent`.** Su token ya no está en la fila (el
   sucesor lo sobrescribió) → `rowcount=0` → detecta que perdió el claim y
   **aborta sin escribir**. Análogo a `_still_owner` en `runner.py:433-440`.
2. **Worker viejo liberando un claim nuevo.** `release` exige token, igual que
   `lease.release` (`lease.py:58-65`).
3. **Dos workers enviando el mismo outbox.** El claim inicial lo decide un solo
   UPDATE: solo uno obtiene `rowcount=1`.

### El límite que no se cierra (y se declara)

> Con `claim_until` y fencing correctos, un worker todavía puede enviar,
> sufrir una pausa mayor que el claim y perder la capacidad de registrar
> `sent`. El sucesor **no reenvía automáticamente**: encuentra el intento
> write-ahead en `dispatching`, lo cierra a `finished/ambiguous` y deja el batch
> `ambiguous`. El sistema sabe que hubo una intención iniciada, pero no si el
> proveedor produjo el efecto.
>
> **El lock local no resuelve esa incertidumbre.** El efecto ocurrió fuera del
> sistema protegido por SQLite. Una reanudación manual —o una política futura
> respaldada por idempotencia/reconciliación externa— todavía podría duplicar;
> por eso la ambigüedad se mide y no se oculta.
>
> **Mitigación honesta:** `claim_until ≥ 3 × provider_timeout`, write-ahead antes
> de la red y fencing al finalizar. Si el token se perdió, el worker viejo no
> escribe `sent`; el estado `ambiguous` decidido por el sucesor prevalece. Esto
> acota la ventana, no convierte el borde externo en exactly-once.

---

## G. Contrato de dominio

Seis entidades. La regla que las separa: **un evento es un hecho del mundo; una
entrega es un acto nuestro**. Nunca comparten tabla.

### 1. `Principal` — la persona/cuenta lógica

`principal_id` · `created_at` · `status` (`active` | `suspended`)

- **No es** `chat_id`, `session_id`, ni `owner`. **INV-ID4.**
- **Se ancla en `app_accounts.email`** (identidad autenticada, ADR-033). Esta es
  la decisión clave: dado N6 (owner anónimo en la URL), **un `anon:<uid>` nunca
  puede ser un principal con endpoints**. Vigilar anónimamente es aceptable;
  recibir mensajes exige una cuenta.
- Reutiliza la intención muerta de `users.primary_user_id` (N5): las filas de
  `users` pasan a **resolver** a un principal, en vez de ser identidades sueltas.
- `radars.owner` **no se migra en P1B**. Se añade un mapa
  `principal_owner_keys(principal_id, owner_key)` que asocia las cadenas `owner`
  existentes a un principal. Migrar `owner` es una migración de datos con riesgo
  real sobre radares en producción; asociarlo es aditivo y reversible.
- Crear un principal exige una `app_account` existente y activa. No se crea uno
  “al vuelo” desde un email libre, `chat_id`, owner anónimo o sesión.
- Crear una preferencia resuelve `radars.owner → principal_owner_keys` y exige
  que ese `principal_id` coincida con el del endpoint. Si el owner no está
  vinculado, es anónimo o pertenece a otro principal, la operación falla.

### 2. `NotificationEndpoint` — un destino concreto

`endpoint_id` · `principal_id` · `channel` · `address_hash` · `address_last4` ·
`external_recipient_id` · `verified_at` · `enabled` · `created_at` · `revoked_at`

- **Direcciones sensibles:** se guarda el **hash** (`address_hash`, HMAC-SHA256
  con una clave del secret manager) más un **mínimo mostrable**
  (`address_last4`: "···4821"), suficiente para que la persona reconozca su
  destino en la UI. El envío usa `external_recipient_id` — el `chat_id` de
  Telegram o el `chatId` de OpenWA —, que es un identificador **del proveedor**,
  no un dato personal directo, y es lo que el adapter necesita de verdad. Así el
  teléfono en claro **no vive en la tabla de dominio**.
- **No se inventa criptografía.** HMAC-SHA256 de `hashlib`/`hmac` (ya usado en
  `main.py:287` para el webhook de OpenWA) con clave en configuración, nunca en
  la base. Si en el futuro hiciera falta recuperar la dirección en claro, se
  añade cifrado con una librería establecida (`cryptography`) — **no en P1B**.
- `enabled` (operativo, lo mueve el sistema) ≠ consentimiento (§3, lo mueve la
  persona). Separados a propósito.
- `verified_at NULL` → **no recibe**. **INV-ID3.**

**Contrato de verificación del endpoint:**

- Crear o editar un endpoint por CRUD normal **siempre** deja `verified_at=NULL`;
  ese campo no se acepta como argumento público de alta ni de edición.
- Solo un `ChannelVerifier` puede establecer `verified_at`, después de una
  prueba de posesión vinculada al `principal_id` autenticado.
- P1B-1 implementa el estado pendiente, las restricciones y la imposibilidad de
  auto-verificación. La verificación real de Telegram se implementa en P1B-5:
  vinculación iniciada desde la cuenta web y confirmada desde el chat exacto.
- Una cuenta `app_accounts` inexistente o inactiva no puede crear principal,
  vincular owner keys, iniciar verificación ni habilitar preferencias.

### 3. `RadarNotificationPreference` — qué quiere recibir y cómo

`radar_id` · `endpoint_id` · `enabled` (= opt-in) · `frequency='per_run'` · `quiet_hours_start` ·
`quiet_hours_end` · `timezone` · `max_items` · `include_new` · `include_state_changes` ·
`include_value_changes` · `include_disappeared` · `created_at` · `updated_at`

**Decisión: la preferencia se asocia a `(radar_id, endpoint_id)`, no al
principal.** Justificación:

- La misma persona puede querer canales distintos por radar. El MVP envía por
  corrida (`per_run`); los resúmenes diarios/semanales se diseñarán después. Colgar
  la preferencia del principal fuerza
  una preferencia única y hace imposible expresarlo.
- El **opt-in es por destino**: "acepto recibir *en este número*". Un opt-in por
  principal se propagaría a un canal nuevo sin que nadie lo pidiera — que es
  exactamente lo que un opt-in debe impedir.
- **Coste aceptado:** N radares × M endpoints = N×M filas, y "silenciar todo"
  exige tocar varias. Se resuelve con un default heredado del endpoint al crear
  la preferencia y un `principal.status='suspended'` como interruptor global.
  La alternativa —preferencia por principal + override por canal— es una tabla
  con dos significados; **preferimos filas de más a semántica ambigua**.

**Timezone:** el sistema **no conoce ninguna zona horaria hoy** — verificado:
todo es `datetime.now(timezone.utc)` y no hay una sola referencia a `America/Bogotá`
en `app/`. Quiet hours **exige** tz explícita: 22:00–07:00 en UTC es 17:00–02:00
en Colombia, es decir, silenciar la tarde laboral y avisar de madrugada — el
error exacto que quiet hours pretende evitar. **Default documentado:
`America/Bogota`** (el producto es de contratación pública colombiana), guardado
**explícitamente** en la fila, nunca inferido.

**Opt-in sin backfill (operación habilitada desde P1B-2, cuando ya existen cursor y `event_seq`):** habilitar una preferencia no significa “enviar todo lo
que ocurrió desde el inicio del Radar”. La transición `enabled: 0 → 1` es una
operación transaccional:

1. leer `MAX(event_seq)` del radar;
2. crear o actualizar `radar_notification_cursors.last_examined_seq` con ese
   máximo;
3. habilitar la preferencia;
4. hacer `COMMIT`.

Si cualquier paso falla, se revierte todo. Solo se notifican eventos posteriores
al opt-in. Tras opt-out y posterior reactivación se aplica la misma regla: no se
envía el periodo silenciado y los batches muertos por `opted_out` nunca se
reviven. Un futuro “incluir las últimas 24 horas” tendría que ser una opción
explícita, nunca el default.

**Frecuencia MVP:** P1B inicial admite únicamente `frequency='per_run'`.
`daily` y `weekly` quedan diferidos y rechazados por validación hasta que exista
un contrato de ventanas, cierres, zona horaria y concurrencia propio. `per_run`
significa un batch por `(endpoint, radar, source_cycle_id)` con todos los eventos
elegibles de esa materialización. Para radares ordinarios `source_cycle_id` es
el `radar_run_id`; Preflight 2 debe definir el identificador durable equivalente
para un refresh de `follows` antes de que esos eventos entren al notifier.

### 4. `NotificationBatch` (outbox) — la intención determinista

`notification_id` · `principal_id` · `endpoint_id` · `radar_id` · `source_cycle_id` · `batch_key` ·
`idempotency_key` · `external_idempotency_key` · `payload_json` · `status` ·
`not_before` · `claim_token` · `claim_until` · `attempt_count` · `created_at` ·
`expires_at`

- **Identidad exacta, no un rango.** `min_seq`/`max_seq` **no identifican el
  conjunto**: `{1,5}` y `{1,2,3,4,5}` comparten el mismo `min`/`max` pero son
  novedades distintas — un rango se descarta como base de idempotencia.
  En su lugar:

  ```
  canonical_event_set = ",".join(str(s) for s in sorted(unique(event_seqs)))
  idempotency_key = sha256(
      f"{policy_version}|{endpoint_id}|{radar_id}|{canonical_event_set}"
  )
  ```

  `policy_version` versiona la **política de elegibilidad/agrupación** (qué
  kinds cuentan, cómo se colapsan eventos del mismo `result_key`): si esa
  política cambia más adelante, un batch calculado bajo la política vieja para
  "el mismo" conjunto de `event_seq` no debe colisionar silenciosamente con uno
  calculado bajo la nueva — el conjunto de eventos puede ser idéntico y aun así
  significar algo distinto si cambió cómo se interpretan.

  **Propiedades exigidas** (tests futuros, P1B-2):
  1. `{1,5}` y `{1,2,3,4,5}` producen claves **distintas**.
  2. El mismo conjunto en distinto orden de entrada produce la **misma** clave
     (el `sorted()` antes de serializar lo garantiza).
  3. Cambios cosméticos del formatter (un emoji, un salto de línea) **no**
     cambian la clave — el hash no depende del texto renderizado, solo de
     `event_seq`, identidad y versión de política.

- **`batch_key` ≠ `idempotency_key`: responsabilidades distintas, no
  redundantes.** `batch_key` es una etiqueta **gruesa y no única** —
  `f"{radar_id}:{endpoint_id}:{source_cycle_id}"` — para agrupar/filtrar batches en consultas y
  UI ("todos los batches de este radar para este endpoint"); no participa en
  ninguna restricción de unicidad. `idempotency_key` es la identidad
  **criptográfica y exacta** del conjunto de eventos; es la que sostiene
  `UNIQUE(endpoint_id, idempotency_key)`. Un batch se busca por `batch_key`
  para inspección humana y se deduplica por `idempotency_key`.
- **Estable ante cambios posteriores** de radar o preferencias: el `payload_json`
  está congelado. Pero **no inmune al consentimiento**: opt-out, revocación y
  archivado lo matan antes del envío (§E.B/C/D). La intención es estable; el
  permiso es actual.
- `status`: `pending` | `claimed` | `sent` | `ambiguous` | `dead`.

### 5. `NotificationBatchEvent` — la asociación (tabla propia)

`notification_id` · `endpoint_id` · `event_id` · `event_seq`

Tabla separada, no un array JSON: es lo que hace consultable "¿qué eventos
generaron este mensaje?" y "¿este evento ya se entregó?" — **la pregunta del
cursor** (**INV-CU4**). Un JSON no se indexa ni se une.

**`(notification_id, event_id)` no basta.** Esa PK solo impide que un evento
entre dos veces **en el mismo batch** (INV-ID-1); no impide que el **mismo**
evento, para el **mismo** endpoint, termine asociado a **dos batches
distintos** creados por dos selectores concurrentes — que es exactamente el
escenario de doble notificador (§B.1.4). Se agrega `endpoint_id` a la tabla y
`UNIQUE(endpoint_id, event_id)`: esa restricción, no la PK, es la que
realmente protege "este evento, para este endpoint, entra a un batch una sola
vez", sin importar cuántos batches distintos existan.

**Consistencia `endpoint_id` batch↔asociación.** `endpoint_id` en esta tabla
debe coincidir con el `endpoint_id` del batch al que pertenece — de lo
contrario la fila de asociación mentiría sobre a quién va dirigido el evento.
Se exige con una FK compuesta: `radar_notification_batches` gana
`UNIQUE(notification_id, endpoint_id)` (trivial, ya que `notification_id` es
PK) para servir de destino, y `radar_notification_batch_events` declara
`FOREIGN KEY (notification_id, endpoint_id) REFERENCES
radar_notification_batches(notification_id, endpoint_id)`. Así la base de
datos rechaza, no solo la aplicación, una fila de asociación cuyo
`endpoint_id` no coincide con el de su batch.

**Test de concurrencia (P1B-2):** dos selectores intentan materializar el
mismo evento para el mismo endpoint en batches diferentes y simultáneos → el
`UNIQUE(endpoint_id, event_id)` deja pasar la primera inserción y rechaza la
segunda; **solo una asociación gana**, y el selector perdedor detecta el
conflicto y no crea un batch huérfano de un solo evento.

### 6. `DeliveryAttempt` — cada intento, uno por fila, con fase write-ahead

`attempt_id` · `notification_id` · `attempt_number` · `claim_token` · `phase` (`dispatching` |
`finished`) · `started_at` · `finished_at` · `provider_message_id` ·
`provider_status` · `error_class` · `error_message_safe` · `retry_after` ·
`outcome`

- **Por qué write-ahead, no "registrar al terminar".** Si el intento se
  escribe **solo** cuando termina, el sistema no puede distinguir un crash
  *antes* de llamar al proveedor (nada se envió, reintentar es seguro) de uno
  *durante o después* de la llamada (pudo enviarse, reintentar puede
  duplicar). Ambos casos, sin un registro previo, se ven idénticos al
  reiniciar: no hay fila de intento. Por eso el flujo es:

  ```
  claim del batch
    → INSERT DeliveryAttempt (phase='dispatching', outcome=NULL)
    → COMMIT                                    ← el intento YA quedó registrado
    → provider.send(...)                        ← el crash entre aquí y el fin
    → UPDATE DeliveryAttempt SET phase='finished', outcome=..., finished_at=...
  ```

  Un `dispatching` que nunca llega a `finished` porque el worker murió y su
  `claim` expiró se resuelve explícitamente a `outcome='ambiguous'` — **no**
  se reintenta automáticamente ni se asume `sent` ni se asume `failed`.
  Corrige una afirmación que no debía sostenerse: **no es cierto que un
  reenvío tras crash "quede registrado como duplicado detectable"** si no
  existía ya un intento con `phase='dispatching'` antes del envío — sin ese
  registro previo, un crash antes de llamar al proveedor y uno durante la
  llamada son **indistinguibles localmente**, y ambos deben tratarse como
  `ambiguous`, no como "duplicado confirmado" ni como "seguro reintentar".
- **Nunca se mezcla con `radar_events`.** Un evento es un hecho de SECOP; un
  intento es un acto nuestro contra un tercero. Distinta vida, distinto dueño,
  distinta retención, distinta sensibilidad.
- `error_class` es un **enum cerrado**: `rate_limited` | `invalid_address` |
  `provider_error` | `timeout_after_send` | `network_error` | `auth_error`.
  Es la reparación directa de N8: la política de reintento se decide por
  `error_class`, no por un bool.
- `error_message_safe` es **saneado**: sin tokens, sin direcciones, sin payload.
  El error técnico completo va al log con los identificadores enmascarados.
- **Constraint de fase:** `outcome IS NULL` mientras `phase='dispatching'`;
  `outcome` obligatorio cuando `phase='finished'` (CHECK en el DDL, §I). Un
  intento no puede quedar "terminado sin resultado" ni "en curso con resultado
  ya escrito".
- **Fencing al finalizar:** el intento guarda el `claim_token` bajo el que fue
  creado. `finalize_attempt` abre una transacción y comprueba que el batch sigue
  en `claimed`, conserva ese token y no ha vencido. Solo entonces cambia el
  intento `dispatching→finished` y actualiza el batch. Si el worker perdió el
  claim, no puede marcar `sent`; el estado decidido por el sucesor prevalece.

---

## H. Invariantes

### Identidad

- **INV-ID1** — Un radar pertenece a **un** principal canónico (vía `principal_owner_keys`).
- **INV-ID2** — Un endpoint pertenece a **un solo** principal. Nunca compartido.
- **INV-ID3** — Solo endpoints con `verified_at IS NOT NULL` **y** `enabled=1` **y**
  `revoked_at IS NULL` reciben mensajes.
- **INV-ID4** — Un `chat_id`, `session_id`, `uid` de URL u `owner` **no es una
  persona** sin vinculación explícita a un principal. Un `anon:<uid>` **nunca**
  puede tener endpoints (N6).
- **INV-ID5** — Revocar un endpoint impide **todo** intento pendiente sobre él,
  no solo los futuros. Se evalúa al reclamar.
- **INV-ID6** — El principal del radar, resuelto por `principal_owner_keys`, debe
  coincidir con el principal del endpoint. Ningún batch o preferencia puede
  cruzar tenants.
- **INV-ID7** — Solo una `app_account` existente y activa puede originar un
  principal, vincular owner keys, iniciar verificación o habilitar preferencias.

### Elegibilidad

- **INV-ELG1** — `baseline` **nunca** genera push de novedad.
- **INV-ELG2** — `window_churn` **nunca** genera push (no prueba desaparición).
- **INV-ELG3** — `radar_empty` **nunca** genera push.
- **INV-ELG4** — Los outcomes de error (`timeout`, `transient`, `permanent`,
  `invalid_frame`, `guard_blocked`) **nunca** generan push comercial.
- **INV-ELG5** — `already_running`, `lease_lost` y `radar_not_found` **nunca**
  generan push (son control, no corridas).
- **INV-ELG6** — `disappeared` es elegible **solo** si fue confirmado
  explícitamente (`confirmed_disappeared_keys`). Hoy nadie lo confirma → hoy no
  se emite.
- **INV-ELG7** — Solo `new`, `changed_state`, `changed_value` y `disappeared`
  confirmado son elegibles. `followed` no (lo generó el propio usuario).
- **INV-ELG8** — El selector **no** filtra por estado operativo del radar
  (`needs_attention`, `next_run_after`): esos campos gobiernan si el radar
  *volverá a correr*, no si un evento *ya materializado* sigue siendo válido.
  Un evento `new`/`changed_state`/`changed_value` detectado en una corrida
  exitosa pasada sigue siendo una novedad real aunque el radar haya entrado en
  `needs_attention` después. El selector solo frena por: opt-out del endpoint,
  endpoint no verificado/deshabilitado/revocado, radar archivado, batch
  expirado, o principal suspendido. Cualquier supresión operativa adicional
  que se decida más adelante debe ser **explícita y auditada** (un `dead_reason`
  propio, no un `skip` silencioso).

### Idempotencia

- **INV-ID-1** — Un `radar_event` no entra dos veces en el mismo batch
  (`UNIQUE(notification_id, event_id)`).
- **INV-ID-2** — `UNIQUE(endpoint_id, idempotency_key)`: un batch lógico por
  endpoint, una vez.
- **INV-ID-3** — Dos workers no reclaman simultáneamente la misma notificación
  (UPDATE condicional, decisión por `rowcount`).
- **INV-ID-4** — Un reintento reutiliza la **misma** `external_idempotency_key`
  cuando el proveedor la documente y soporte. En los endpoints revisados no se
  encontró esa capacidad → el campo se reserva y queda NULL.
- **INV-ID-5** — **Nunca** se marca `sent` antes de la confirmación del
  proveedor. (Marcar antes sería at-most-once: perdería silenciosamente.)
- **INV-ID-6** — Un timeout ambiguo **no** se convierte automáticamente en
  `failed`+retry. Va a `ambiguous`, que no se reintenta solo.

### Cursor

`last_examined_seq` es un **watermark de lo examinado**, no de lo entregado ni
de lo elegible: avanza porque la ventana se procesó por completo (elegibles
materializados, no elegibles descartados con motivo), no porque todo lo
examinado haya generado una notificación.

- **INV-CU1** — El consumo **no** depende solo del timestamp (N2: el ts es
  compartido por construcción).
- **INV-CU2** — El orden del cursor es **total y estable**: `event_seq`
  (INTEGER, monotónico, sin reutilización).
- **INV-CU3** — Eventos con `ts` idéntico **no se pierden** (los desempata
  `event_seq`).
- **INV-CU4** — Un evento ya materializado en outbox **no vuelve a
  seleccionarse** (verificable por `notification_batch_events`, no por confianza
  en el cursor).
- **INV-CU5** — Selección, creación de batches, inserción de `batch_events` y
  avance de `last_examined_seq` ocurren en **una sola transacción**. Si
  cualquier parte falla, `ROLLBACK` completo: el cursor no avanza sobre trabajo
  a medias.
- **INV-CU6** — Una ventana examinada **sin ningún evento elegible** también
  avanza el cursor. No hacerlo reescanearía indefinidamente eventos ya
  clasificados como no elegibles (`baseline`, `window_churn`, etc.) en cada
  ciclo del selector.
- **INV-CU7** — `max_items` no compromete INV-CU5: el batch asocia **todos**
  los eventos elegibles de la ventana en `batch_events` (representados), aunque
  el render solo muestre los primeros N + `truncated_count`. El cursor avanza
  sobre el total examinado porque todo quedó representado, no porque se haya
  descartado silenciosamente el resto.

### Preferencias

- **INV-PR1** — Opt-out **prevalece** sobre una entrega pendiente no enviada.
- **INV-PR2** — Quiet hours **posponen** (`not_before`), no descartan.
- **INV-PR3** — Eventos vencidos se descartan **con motivo auditable**
  (`dead` + `expired`), nunca en silencio.
- **INV-PR4** — Los límites (`max_items`) **agrupan o resumen**; no silencian
  sin registro (`truncated_count` en el payload).
- **INV-PR5** — La timezone es **explícita** en la fila. Default documentado:
  `America/Bogota`. Nunca inferida del servidor.
- **INV-PR6** — Al habilitar o reactivar una preferencia, el cursor se inicializa
  al `MAX(event_seq)` del radar en la misma transacción. No hay backfill implícito.
- **INV-PR7** — El MVP acepta solo `frequency='per_run'`; `daily` y `weekly`
  fallan por validación hasta contar con un contrato propio.

### Trazabilidad

- **INV-TR1** — Todo mensaje se remonta a **eventos concretos**, vía
  `notification_batch_events`. Incluidos los colapsados por `result_key`.
- **INV-TR2** — Todo intento en `phase='finished'` tiene `outcome`. Un intento
  `dispatching` puede tener `outcome=NULL` solo mientras su claim esté vigente;
  si queda abandonado, debe reconciliarse a `finished/ambiguous`.
- **INV-TR3** — Los errores almacenados **no** incluyen secretos
  (`error_message_safe` saneado).
- **INV-TR4** — Los logs **no** exponen teléfonos, correos, tokens ni payloads.
  (Repara N12: hoy `scheduler.py:132` y `follows.py:185` loguean `owner=%s`, que
  puede ser `email:foo@bar.com`.)

---

## I. Schema propuesto (DDL conceptual — **no implementar en P1B-0**)

```sql
-- 1. Principal: la persona lógica. Anclada en app_accounts (autenticada).
CREATE TABLE principals (
    principal_id  TEXT PRIMARY KEY,
    email         TEXT NOT NULL,               -- → app_accounts.email
    status        TEXT NOT NULL DEFAULT 'active'
                  CHECK (status IN ('active','suspended')),
    created_at    TEXT NOT NULL,
    UNIQUE (email)                             -- identidad externa canónica
);
-- Sin FK a app_accounts: vive en la misma DB pero es otro módulo (auth.py).
-- La integridad se valida en el borde; una FK acoplaría dos schemas
-- independientes y rompería tests que crean uno sin el otro.

-- 2. Mapa de owners existentes → principal. Aditivo: NO migra radars.owner.
CREATE TABLE principal_owner_keys (
    owner_key     TEXT PRIMARY KEY,            -- 'email:foo@bar.com', 'st_anon_x'
    principal_id  TEXT NOT NULL REFERENCES principals(principal_id)
                  ON DELETE RESTRICT,
    linked_at     TEXT NOT NULL
);
-- owner_key es PK: un owner pertenece a un principal y solo a uno (INV-ID1).
-- RESTRICT: borrar un principal con radares vivos debe fallar ruidosamente.

-- 3. Endpoints. Direcciones minimizadas: hash + last4 + id del proveedor.
CREATE TABLE notification_endpoints (
    endpoint_id           TEXT PRIMARY KEY,
    principal_id          TEXT NOT NULL REFERENCES principals(principal_id)
                          ON DELETE RESTRICT,
    channel               TEXT NOT NULL
                          CHECK (channel IN ('telegram','whatsapp','email')),
    address_hash          TEXT NOT NULL,        -- HMAC-SHA256(addr, key_del_secret_manager)
    address_last4         TEXT,                 -- '4821' — solo para reconocerlo en la UI
    external_recipient_id TEXT,                 -- chat_id Telegram / chatId OpenWA
    verified_at           TEXT,                 -- NULL → no recibe (INV-ID3)
    enabled               INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
    created_at            TEXT NOT NULL,
    revoked_at            TEXT,                 -- revocación, no borrado: los intentos lo referencian
    UNIQUE (channel, address_hash),             -- un destino, un endpoint (INV-ID2)
    UNIQUE (endpoint_id, principal_id)              -- destino de FK compuesta; impide batch cross-tenant
);
CREATE INDEX idx_endpoints_principal ON notification_endpoints(principal_id);

-- 4. Preferencias por (radar, endpoint).
CREATE TABLE radar_notification_preferences (
    preference_id         TEXT PRIMARY KEY,
    radar_id              TEXT NOT NULL REFERENCES radars(id) ON DELETE RESTRICT,
    endpoint_id           TEXT NOT NULL REFERENCES notification_endpoints(endpoint_id)
                          ON DELETE RESTRICT,
    enabled               INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0,1)),
                          -- DEFAULT 0: opt-in explícito. Nadie recibe por existir.
    frequency             TEXT NOT NULL DEFAULT 'per_run'
                          CHECK (frequency = 'per_run'), -- daily/weekly diferidos
    quiet_hours_start     TEXT,                 -- 'HH:MM' local
    quiet_hours_end       TEXT,
    timezone              TEXT NOT NULL DEFAULT 'America/Bogota',  -- INV-PR5
    max_items             INTEGER NOT NULL DEFAULT 10,
    include_new           INTEGER NOT NULL DEFAULT 1,
    include_state_changes INTEGER NOT NULL DEFAULT 1,
    include_value_changes INTEGER NOT NULL DEFAULT 1,
    include_disappeared   INTEGER NOT NULL DEFAULT 0,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    UNIQUE (radar_id, endpoint_id)              -- una preferencia por par
);
-- ON DELETE RESTRICT en radar_id: obliga a archivar (no borrar) radars (N13).

-- 5. Outbox: la intención de entrega.
CREATE TABLE radar_notification_batches (
    notification_id          TEXT PRIMARY KEY,
    principal_id             TEXT NOT NULL,
    endpoint_id              TEXT NOT NULL,
    radar_id                 TEXT NOT NULL,     -- SIN FK: sobrevive al radar (N13)
    source_cycle_id          TEXT NOT NULL,     -- radar_run_id o ciclo durable equivalente de follows
    batch_key                TEXT NOT NULL,     -- etiqueta gruesa NO única: agrupar/filtrar, no deduplicar
    idempotency_key          TEXT NOT NULL,     -- identidad exacta: sha256(policy_version|endpoint_id|radar_id|canonical_event_set)
    external_idempotency_key TEXT,              -- reservado: no documentada en endpoints revisados
    payload_json             TEXT NOT NULL,     -- datos congelados (no el texto)
    status                   TEXT NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending','claimed','sent','ambiguous','dead')),
    dead_reason              TEXT,              -- opted_out|endpoint_revoked|radar_archived|expired|invalid_address
    not_before               TEXT,              -- quiet hours / retry_after (INV-PR2)
    claim_token              TEXT,
    claim_until              TEXT,
    attempt_count            INTEGER NOT NULL DEFAULT 0,
    created_at               TEXT NOT NULL,
    expires_at               TEXT NOT NULL,     -- freshness (INV-PR3)
    UNIQUE (endpoint_id, idempotency_key),      -- INV-ID-2: identidad exacta del conjunto
    UNIQUE (endpoint_id, radar_id, source_cycle_id), -- MVP per_run: un batch por ciclo
    UNIQUE (notification_id, endpoint_id),      -- destino de FK compuesta de batch_events
    FOREIGN KEY (endpoint_id, principal_id)
        REFERENCES notification_endpoints(endpoint_id, principal_id)
        ON DELETE RESTRICT                       -- impide batch cross-principal
);
CREATE INDEX idx_batches_claimable
    ON radar_notification_batches(status, not_before);
CREATE INDEX idx_batches_key ON radar_notification_batches(batch_key);
-- radar_id sin FK a propósito: el ledger debe sobrevivir al archivado del radar.
-- Integridad referencial cedida a cambio de trazabilidad; es la elección correcta
-- para un registro de auditoría.

-- 6. Asociación evento↔batch: la cadena de trazabilidad (INV-TR1).
CREATE TABLE radar_notification_batch_events (
    notification_id TEXT NOT NULL,
    endpoint_id     TEXT NOT NULL,             -- debe coincidir con el endpoint_id del batch (FK compuesta abajo)
    event_id        TEXT NOT NULL,             -- SIN FK: radar_events se borra en CASCADE
    event_seq       INTEGER NOT NULL,
    PRIMARY KEY (notification_id, event_id),   -- INV-ID-1: no duplica DENTRO del mismo batch
    UNIQUE (endpoint_id, event_id),            -- el mismo evento no entra a DOS batches del mismo endpoint
    FOREIGN KEY (notification_id, endpoint_id)
        REFERENCES radar_notification_batches(notification_id, endpoint_id)
        ON DELETE CASCADE                      -- si el batch muere, su asociación no significa nada
);
CREATE INDEX idx_batch_events_seq ON radar_notification_batch_events(event_seq);
-- event_id sin FK: si alguien archiva un radar, el ledger conserva qué se envió.
-- La UNIQUE(endpoint_id, event_id) es la que realmente cierra el doble notificador
-- (§B.1.4/N15-adyacente): la PK sola solo protege duplicados DENTRO de un batch.

-- 7. Intentos: uno por llamada al proveedor. Write-ahead: dispatching antes de
-- llamar al proveedor, finished después — nunca se registra solo al terminar.
CREATE TABLE radar_delivery_attempts (
    attempt_id         TEXT PRIMARY KEY,
    notification_id    TEXT NOT NULL REFERENCES radar_notification_batches(notification_id)
                       ON DELETE RESTRICT,     -- un intento no se borra con su batch
    attempt_number     INTEGER NOT NULL,
    claim_token        TEXT NOT NULL,           -- fencing: token vigente al iniciar el intento
    phase              TEXT NOT NULL DEFAULT 'dispatching'
                       CHECK (phase IN ('dispatching','finished')),
    started_at         TEXT NOT NULL,
    finished_at        TEXT,
    provider_message_id TEXT,
    provider_status    TEXT,
    error_class        TEXT CHECK (error_class IN
                       ('rate_limited','invalid_address','provider_error',
                        'timeout_after_send','network_error','auth_error')),
    error_message_safe TEXT,                   -- saneado (INV-TR3)
    retry_after        INTEGER,                -- segundos, de la cabecera del proveedor
    outcome            TEXT
                       CHECK (outcome IN ('sent','retry','ambiguous','dead')),
    UNIQUE (notification_id, attempt_number),  -- sin intentos duplicados
    CHECK (
        (phase = 'dispatching' AND outcome IS NULL)
        OR (phase = 'finished' AND outcome IS NOT NULL)
    )                                          -- outcome NULL solo mientras dispatching
);
CREATE INDEX idx_attempts_notification ON radar_delivery_attempts(notification_id);

-- 8. Cursor por endpoint: watermark de examinado, NO de entregado.
CREATE TABLE radar_notification_cursors (
    endpoint_id        TEXT NOT NULL,
    radar_id           TEXT NOT NULL,
    last_examined_seq  INTEGER NOT NULL DEFAULT 0,  -- event_seq examinado, NO timestamp (INV-CU1)
    updated_at         TEXT NOT NULL,
    PRIMARY KEY (endpoint_id, radar_id)
);
```

**Transición de opt-in:** `enabled 0→1` y el upsert del cursor al
`MAX(event_seq)` del radar ocurren en una sola transacción. El mismo contrato
aplica a la reactivación tras opt-out; los eventos del periodo silenciado no se
reprocesan y los batches `dead/opted_out` no reviven.

### Migración requerida en `radar_events` (P1B-2)

```sql
ALTER TABLE radar_events ADD COLUMN event_seq INTEGER;
ALTER TABLE radar_events ADD COLUMN source_cycle_id TEXT;
CREATE UNIQUE INDEX idx_radar_events_seq ON radar_events(event_seq);
CREATE INDEX idx_radar_events_cycle ON radar_events(radar_id, source_cycle_id, event_seq);
```

**Decisión explícita: `event_seq` es una columna añadida con unicidad propia,
no el PK autoincremental de `radar_events`.** Las dos opciones consideradas:

| Opción | Mecanismo | Interacción con retención |
|---|---|---|
| **A — `event_seq` = PK autoincremental de `radar_events`** | Reconstruir la tabla con `id INTEGER PRIMARY KEY AUTOINCREMENT` (SQLite no permite dos columnas autoincrementales; habría que dejar de usar el UUID como PK). | AUTOINCREMENT no reutiliza valores aunque se borren filas (retención futura de `radar_events` no rompe la garantía). Pero **acopla** la generación de secuencia a la misma tabla que un futuro job de retención purgaría, y exige migrar el PK de una tabla con datos reales (`data/secoppal.db` está versionado y mutado). |
| **B — columna explícita, generada por una tabla contador independiente** *(elegida)* | `ALTER TABLE radar_events ADD COLUMN event_seq INTEGER` + tabla `radar_event_seq` separada (ver DDL abajo). | El generador de secuencia **no vive en la tabla que se purga**: una retención futura de `radar_events` (hoy inexistente, ver §J) puede borrar filas viejas sin tocar `radar_event_seq`, y la garantía de "nunca reutilizado" es independiente de esa política. Migración puramente aditiva (`ALTER TABLE ADD COLUMN` + tabla nueva), sin reescribir una tabla con datos en producción. |

**Se elige B** por dos razones que pesan más que el coste de una fila extra por
evento: (1) es coherente con el patrón de migración aditiva ya usado en el
repositorio (`_ensure_columns`, `store.py:226`) — cero riesgo sobre datos
existentes; (2) desacopla explícitamente "cómo se genera un `event_seq` que
nunca se repite" de "cuánto tiempo vive una fila de `radar_events`", que son
dos decisiones independientes y no deberían compartir mecanismo.

```sql
ALTER TABLE radar_events ADD COLUMN event_seq INTEGER;
ALTER TABLE radar_events ADD COLUMN source_cycle_id TEXT;
CREATE UNIQUE INDEX idx_radar_events_seq ON radar_events(event_seq);
CREATE INDEX idx_radar_events_cycle ON radar_events(radar_id, source_cycle_id, event_seq);

CREATE TABLE radar_event_seq (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,     -- AUTOINCREMENT: jamás reutiliza
    ts  TEXT NOT NULL
);
```
Se inserta en `radar_event_seq`, se toma `last_insert_rowid()` y se usa como
`event_seq` al insertar en `radar_events`. **Ambas operaciones —reservar el
`event_seq`, asignar `source_cycle_id` e insertar el `radar_event`— ocurren sobre la misma conexión Y
dentro de la misma transacción `materialize_run`** (P1B-Preflight 1): no son
un paso previo separado, son dos statements consecutivos de la transacción
corta que ya abarca `radar_run` + `seen` + `events` + limpieza operativa. Un
`INSERT` + `last_insert_rowid()` sobre la misma conexión es seguro incluso en
autocommit — no hay carrera porque `last_insert_rowid()` es un valor de
conexión, no una consulta separada a la tabla —, pero aquí además viven dentro
de una transacción explícita, no en autocommit suelto.

**Política de gaps.** El cursor tolera huecos: `WHERE event_seq > :cursor` es
una comparación de orden, no exige contigüidad. Bajo este diseño, dado que la
reserva y la inserción del evento están en la **misma** transacción, un
`ROLLBACK` deshace ambas — no queda un `event_seq` consumido sin su
`radar_event`, ni un `radar_event` con un `event_seq` a medias. Si en el
futuro se reservaran secuencias por adelantado en lote (no es el diseño
actual), podrían aparecer huecos legítimos; el cursor los soportaría igual,
porque su contrato es orden total y monotonicidad, no consecutividad. Lo que
el diseño **no permite nunca** es que una transacción revertida deje un evento
parcial: reserva sin evento, o evento sin reserva.

**Coste de crecimiento, declarado.** `radar_event_seq` crece una fila por
evento, indefinidamente mientras no exista una política de poda — hoy no
existe (§J la marca fuera de alcance de P1B). El coste es acotable sin tocar
el diseño: SQLite conserva el máximo histórico de un `AUTOINCREMENT` en su
tabla interna `sqlite_sequence` **independientemente de las filas que sigan
existiendo** — borrar filas viejas de `radar_event_seq` no reduce ni permite
reutilizar el contador. Eso significa que una futura poda de
`radar_event_seq` (fuera de alcance aquí) sería segura para la garantía de
monotonicidad; no se implementa en P1B-2, se deja anotada como opción abierta.

### Resumen de constraints UNIQUE exigidas

| Concepto | Constraint |
|---|---|
| Identidad externa | `principals(email)`; `principal_owner_keys(owner_key)` PK |
| Endpoint | `notification_endpoints(channel, address_hash)`; `(endpoint_id, principal_id)` |
| Preferencia | `radar_notification_preferences(radar_id, endpoint_id)` |
| Batch / idempotencia exacta | `radar_notification_batches(endpoint_id, idempotency_key)` |
| Batch / frecuencia MVP | `radar_notification_batches(endpoint_id, radar_id, source_cycle_id)` |
| Aislamiento principal-endpoint | FK compuesta batch `(endpoint_id, principal_id)` → endpoint |
| Batch / destino de FK compuesta | `radar_notification_batches(notification_id, endpoint_id)` (trivial) |
| Asociación evento-batch (dentro del batch) | `radar_notification_batch_events(notification_id, event_id)` PK |
| Asociación evento-endpoint (entre batches) | `radar_notification_batch_events(endpoint_id, event_id)` |
| Consistencia batch↔asociación | FK compuesta `(notification_id, endpoint_id)` |
| Secuencia de evento | `radar_events(event_seq)` |
| Intento | `radar_delivery_attempts(notification_id, attempt_number)` |

### FK y ON DELETE — análisis por caso

| Caso | Comportamiento | Por qué |
|---|---|---|
| **Archivar radar** (no borrar) | `radars.archived_at` (**archivo, no soft-delete genérico** — ver §K/§L, semántica archive/restore). Las FK `RESTRICT` desde preferencias impiden el `DELETE` duro actual. | El `DELETE` de `store.py:473` + CASCADE borraría runs, events, seen y —si colgara— el ledger (N13). Archivar preserva la fila (y el nombre reservado); un borrado real destruiría la prueba de lo entregado. **Cambio productivo requerido en P1B-1.** |
| **Borrar principal** | `RESTRICT` desde endpoints y `principal_owner_keys`. Vía normal: `status='suspended'`. | Borrar un principal huérfana el ledger. Suspender es reversible y auditable. |
| **Revocar endpoint** | `revoked_at = now`. **Nunca** `DELETE`. | Los intentos históricos lo referencian. Borrarlo rompe "¿a qué destino se envió?". |
| **Conservar auditoría** | Attempts con `RESTRICT` a batches; batch_events con `event_id` **sin FK** (pero `endpoint_id` **sí** con FK compuesta al batch). | El ledger es un registro de auditoría: sobrevive a los objetos que lo originaron. Se cede integridad referencial hacia `radar_events`/`radars` a cambio de trazabilidad; la consistencia interna batch↔asociación sí se exige, porque ahí no hay tensión con la auditoría. |

**Regla general: archivo/revocación donde el borrado destruiría trazabilidad —
nunca `DELETE` real sobre entidades con historia que preservar.**

---

## J. Privacidad y seguridad

### Inventario de campos sensibles

| Campo | Dónde | Sensibilidad | Tratamiento |
|---|---|---|---|
| Teléfono | Endpoint (futuro) | **Alta** — PII directa | `address_hash` (HMAC-SHA256) + `address_last4`. Nunca en claro en la tabla. |
| Correo | `principals.email`, `app_accounts.email`, `users.email`, **`radars.owner`**, **`radar_events.owner`** | **Alta** | `principals.email` en claro es **necesario** (es la clave de login). Lo que **no** es aceptable es su réplica en cada fila de `radar_events.owner` (N12) → sustituir por `principal_id` (deuda, P1B-1 lo prepara). |
| External user id | `external_recipient_id`, `chat_id` | Media — pseudónimo del proveedor | En claro (el adapter lo necesita). **Enmascarado en logs.** |
| URLs (`urlproceso`) | `radar_events`, payload | Baja — dato público de SECOP | Sin tratamiento. Es información pública. |
| Nombres de radares | `radars.name`, `radar_events.radar_name` | **Media** — revelan estrategia comercial | No salen del sistema salvo al propio destino. **Fuera de logs.** |
| Filtros / QueryFrame | `radars.query_frame_json` | **Media** — ídem | No entran en payloads de notificación. |
| SoQL | `radar_runs.soql` | Media | Solo auditoría interna. Sujeto a retención. |
| Mensajes de error | `error_message_safe` | Variable | **Saneado**: sin tokens, direcciones ni payload. El técnico va al log con máscaras. |
| Tokens de proveedor | `settings.*` (env) | **Crítica** | **Ya están bien**: `telegram_bot_token`, `openwa_api_key`, `openwa_webhook_secret` viven en `app/config.py` desde env, **no en tablas**. **Preservar.** |

### Medidas

1. **Minimización.** El endpoint guarda lo mínimo para enviar y reconocer:
   `external_recipient_id` + hash + last4. El teléfono en claro no se persiste.
2. **Masking en logs.** Helper de saneado obligatorio en el módulo de notificación:
   `endpoint_id` sí, dirección nunca; `principal_id` sí, email nunca. Repara
   `scheduler.py:132-133` y `follows.py:185` (`owner=%s` → puede ser un email).
3. **Tokens fuera del dominio.** Confirmado como práctica actual correcta
   (`config.py:30-38`). Ninguna tabla de P1B tiene columna de token. Si un
   adapter necesita credenciales, las lee de `settings`.
4. **Retención** (nueva política, no existe hoy):
   - `radar_delivery_attempts`: 90 días → luego se borra `error_message_safe` y
     `provider_message_id`, se conservan outcome y fechas (métricas sin PII).
   - `radar_notification_batches`: 90 días en estado terminal → `payload_json`
     se vacía, la fila queda como esqueleto de auditoría.
   - `notification_endpoints` revocados: 1 año → `address_hash`/`address_last4`
     a NULL; la fila queda para que los intentos históricos no apunten a nada.
   - `radar_events`: **fuera de alcance de P1B** — es de P0 y no tiene política
     hoy. Se registra como deuda.
5. **Eliminación / anonimización.** Suspender un principal ≠ borrarlo. Un
   borrado real (derecho de supresión) anonimiza: `email → 'deleted:<hash>'`,
   endpoints a NULL, batches con `payload_json` vacío. La **forma** del registro
   sobrevive; su **contenido** personal no.
6. **Error seguro vs técnico.** `error_message_safe` (persistido, para la UI y
   el soporte) ≠ excepción completa (log, con máscaras, retención corta). Nunca
   se persiste una respuesta cruda del proveedor: puede traer eco del destino.

---

## K. Experiencia de usuario

| Situación | Comportamiento visible |
|---|---|
| **Opt-in** | Explícito y por destino: "recibir novedades de *este radar* en *este canal*". Exige endpoint verificado. Default `enabled=0` — nadie recibe por existir. |
| **Opt-out** | Un paso, sin fricción, efecto inmediato: mata lo pendiente (**INV-PR1**). Confirmación clara de qué deja de llegar. |
| **Quiet hours** | "No te escribo entre las 22:00 y las 07:00 (hora de Colombia)". Pospone, no descarta: a las 07:00 llega lo acumulado, agrupado. |
| **Radar pausado** (`enabled=0`, no archivado) | No notifica ni corre solo, pero **sigue visible** en listados y puede reactivarse o probarse manualmente. Distinto de archivado: pausado es reversible con un clic, sin flujo de restauración. |
| **Radar archivado** (`archived_at` no nulo) | **Desaparece** de listados y del scheduler (distinto de "pausado", que sigue visible). El nombre queda reservado — no se puede crear otro radar con el mismo nombre mientras siga archivado. `restore_radar` lo recupera con su historial intacto. |
| **Radar `needs_attention`** | **Flujo explícito de revisar/probar/reactivar**: solo una operación explícita limpia `needs_attention`, y solo si el usuario lo ordena esa misma operación pone `enabled=1`. El aviso es operativo (ficha/UI), no digest comercial. **El defecto real, verificado en el código, no es que el radar se reactive solo** — `update_last_run` (`store.py:340-344`) **no toca la columna `enabled`**, así que un radar pausado sigue pausado. El defecto es que **cualquier corrida exitosa limpia `needs_attention` y todo el estado de fallo** (`failure_count`, `permanent_failure_streak`, `last_error_*`, `next_run_after`) **sin que el radar deje de estar pausado** — incluida una corrida disparada manualmente sobre un radar en pausa, porque `_do_run` (`app/core/workflow/handlers/radar.py`) llama a `run_radar` sin comprobar `radar.is_enabled`. Se pierde el indicador que explica *por qué* está pausado, aunque siga estándolo. Política fijada: éxito limpia `failure_count`/`permanent_failure_streak`/errores/backoff; **no** limpia `needs_attention` cuando el radar fue pausado por atención; solo `review`/`reactivate` explícito limpia `needs_attention` y, si el usuario lo ordena, activa `enabled=1`. **Deuda P0 identificada (ver P1B-Preflight 3, plan de ejecución); precondición dura antes de P1B-6.** |
| **Canal inválido** | "No pudimos entregar en ···4821. Verifícalo." Endpoint a `needs_verification`, no revocado solo. |
| **Proveedor caído** | Silencio hacia el usuario (no es su problema) + banner si supera un umbral: "Entregas demoradas; tus radares siguen corriendo." La detección **no** se pierde: los eventos están persistidos. |
| **Mensaje agrupado** | Un mensaje por radar por ciclo. Varios radares → un mensaje con secciones. Nunca N mensajes seguidos. |
| **"Ver proceso"** | `urlproceso` como link directo por item. Es la acción principal: el usuario recibe para **hacer algo**. |
| **Digest sin URL** | Se envía igual, sin link, y cierra con "ver en la app" hacia el feed. Nunca se inventa una URL. |
| **Reactivación manual** | Revisar (qué falló) → Probar (correr una vez, sin notificar) → Reactivar (acto explícito). Tres pasos, no uno. |

---

## L. Riesgos abiertos y deuda identificada

**Deuda de P0 que P1B hereda y no puede arreglar sola:**

1. **N1 — pérdida de eventos por no-atomicidad `upsert_seen`/`_emit_events`.**
   Un crash entre ambos pierde novedades **para siempre**. Es un defecto de
   *verdad*, no de entrega. Se resuelve con **P1B-Preflight 1** (transacción
   corta, con el lease revalidado como primera escritura de esa misma
   transacción — ver plan de ejecución). **Precondición dura antes de
   construir el outbox (P1B-2)**, no solo antes de activar el envío (P1B-6):
   un cursor sobre eventos que el runner puede perder hereda la pérdida.
2. **N15 — `follows.refresh_follows` corre sin lease** (`scheduler.py:128-138`;
   función `refresh_follows` en `follows.py:158-229`; escritura en
   `store.record_events` en `follows.py:229`). Duplica eventos con dos
   schedulers. Contradice la afirmación "P1A cerró la concurrencia del Radar".
   El radar sintético (`get_or_create_follows_radar`, `follows.py:39-52`) tiene
   una fila propia en `radars` con columnas de lease técnicamente reutilizables,
   pero no se prescribe aquí reusar `try_acquire_run_lease` sin antes revisar
   si su identidad (uno por owner, autocreado) interactúa limpiamente con ese
   mecanismo. Se abre como **P1B-Preflight 2** — investigación antes que fix.
3. **`needs_attention` se limpia en cualquier corrida OK, incluida una
   disparada manualmente sobre un radar pausado** (`store.py:340-344`;
   `_do_run` en `handlers/radar.py` no comprueba `is_enabled`). **No** es
   reactivación silenciosa —`enabled` no se toca—, pero **sí** es pérdida del
   indicador que explica por qué el radar sigue pausado, contra la política
   fijada en §K. Se cierra con **P1B-Preflight 3**.
4. **`radar_events.owner`/`radars.owner` replican el email en claro** (N12) y se
   loguean (`scheduler.py:132`, `follows.py:185`).
5. **`radar_events` sin política de retención.**

**Riesgos abiertos de P1B:**

6. **La ventana de duplicación de §F no se cierra.** Es inherente. Se mide
   (`ambiguous_rate`) y se acota; no se elimina.
7. **`disappeared` no se emite hoy.** Está en el contrato, no en el código.
8. **El default `America/Bogota` es una decisión de producto**, no un dato del
   usuario. Una persona fuera de Colombia recibiría en horario colombiano hasta
   que lo cambie.
9. **Numeración de ADR colisionada entre ramas** (dos ADR-043, dos ADR-045, dos
   ADR-046 distintos). Ver §M.
10. **Reutilizar el nombre de un radar archivado no está soportado** —
    `UNIQUE(owner, name)` sigue vigente mientras la fila exista archivada. Si
    se decide permitirlo, requiere migrar esa restricción (p. ej. a un índice
    único parcial `WHERE archived_at IS NULL`); no se declara aditivo ni se
    implementa en este contrato.

---

## M. Nota sobre la numeración de ADR

La convención real del repositorio es **una entrada append-only en
`docs/decisions_log.md`** (no hay directorio `docs/adr/` ni índice separado).

En `radar/mvp-manual` el registro llega a **ADR-041**. Pero la numeración está
**colisionada entre ramas**: hay dos ADR-043 distintos (`Cierre Sprint A` vs
`Spike W0S`), dos ADR-045 (`Entity Identity Reconciliation` vs
`Warehouse candidate pool`) y dos ADR-046. Los números 042–050 están tomados en
`warehouse/*` y `codex/*`.

Tomar el "siguiente libre local" (042) crearía **una colisión nueva** con el
ADR-042 de perfil/dossier de contratista. Por eso el ADR de P1B se registra como
**ADR-051** — el primer número libre verificado contra las ramas locales con
`git grep '## ADR-0' <ramas locales>`. **El número es provisional**: antes de
integrar, se debe repetir la verificación contra refs remotos
(`git fetch --all && git grep -h '^## ADR-0' $(git branch -a --format='%(refname:short)') | sort -u`),
porque esta auditoría solo comprobó lo que existía localmente en el momento de
escribirla. **Se reasigna si otra rama lo toma antes de la integración.**

**No se corrigen aquí las colisiones históricas** (dos ADR-043, dos ADR-045,
dos ADR-046 distintos) — está fuera de alcance de P1B-0. Se registra como
tarea de gobernanza en el plan de ejecución: un detector automático de números
de ADR duplicados (p. ej., un chequeo de CI que falle si algún `## ADR-NNN`
aparece más de una vez en `docs/decisions_log.md` tras fusionar ramas)
evitaría que esta colisión vuelva a crecer, sin implicar reescribir el
historial existente.

---

**Fin del addendum.** El plan de ejecución vive en
[`docs/planes/radar-p1b-notificaciones.md`](../planes/radar-p1b-notificaciones.md).
La decisión formal, en `docs/decisions_log.md` (ADR-051).

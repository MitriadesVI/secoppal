# P1B-Preflight 2 — Investigación: concurrencia, atomicidad e idempotencia de `follows`

**Fecha:** 2026-07-15 · **Rama:** `radar/mvp-manual` · **HEAD:** `0d34b7c`
**Estado:** investigación cerrada → **GO** con la arquitectura de §F.1

Este documento se escribió **antes** de modificar código productivo. Todo hallazgo
está **reproducido**, no inferido: las sondas ejecutadas se transcriben literalmente.

---

## Resumen ejecutivo

El hallazgo de entrada (N15: `refresh_follows` corre sin lease) es real, pero es
**el menos grave de los cinco defectos** que la investigación encontró. Cerrar solo
el lease dejaría abiertos tres agujeros de *verdad* —no de entrega—:

| # | Defecto | Reproducido | Gravedad |
|---|---|---|---|
| 1 | `refresh_follows` no toma lease → dos schedulers duplican eventos | Sí | Alta |
| 2 | `seen` y `events` en autocommit separados → crash pierde el cambio **para siempre** | Sí | **Crítica** |
| 3 | `unfollow` durante el fetch → el UPSERT final **resucita** la fila borrada | Sí | **Crítica** |
| 4 | La identidad del radar follows es **el nombre**, no `frequency` → un radar de búsqueda es **secuestrado** como watchlist | Sí | **Crítica** |
| 5 | El fetch no deduplica por clave → una clave devuelta dos veces emite dos eventos | Sí (por lectura) | Media |
| 6 | `follow` re-escribe `snapshot_hash=''` sobre una fila existente → re-seguir **borra la línea base y silencia el cambio siguiente** | Sí | **Crítica** |
| 7 | `follow` escribe membresía y evento en autocommit separados | Sí (por lectura) | Media |
| 8 | `unfollow`+`follow` de la misma clave durante la red → el UPDATE acierta sobre **otra membresía**: le mata la baseline y emite un evento de la encarnación muerta | Sí | **Crítica** |
| 9 | `follow` devuelve `True` sin comprobar que se escribió; los materializadores no validan el nombre canónico dentro de la transacción | Sí | Alta |

Los defectos 6-7 los encontró el **review de la ronda 1**; los 8-9, el de la
**ronda 2**. Ver §F.0. Ninguno de los cuatro era visible desde la investigación
inicial: aparecen solo cuando se mira la interacción entre `follow`, `unfollow` y
el refresco como **tres escritores concurrentes sobre la misma fila**, no como tres
funciones.

El defecto 4 no estaba en el enunciado del bloque como confirmado (era la pregunta
"¿el nombre es identidad o presentación?"). La respuesta es que **hoy el nombre es
identidad de facto, y eso permite escribir la watchlist de un usuario dentro de su
radar de búsqueda**. Es el defecto que más justifica que este bloque exigiera
investigación antes que fix.

---

## A. Identidad

### Cómo se resuelve hoy

`follows.get_or_create_follows_radar` (`follows.py:39-52`) resuelve el radar
**exclusivamente por nombre**:

```python
existing = store.get_radar_by_name(owner, FOLLOWS_NAME)   # ← solo nombre
if existing is not None:
    return existing
```

No comprueba `frequency`. `store.get_radar_by_name` (`store.py:611-620`) consulta:

```sql
SELECT * FROM radars WHERE owner = ? AND name = ? COLLATE NOCASE
```

### ¿Hay exactamente un radar follows por owner?

**No garantizado.** La unicidad la aporta `UNIQUE (owner, name)` (`store.py:247`),
que es unicidad **de nombre**, no de tipo. El esquema no impide:

- dos filas `frequency='follows'` con nombres distintos para el mismo owner;
- una fila `frequency='diaria'` **con el nombre reservado**.

En la práctica solo existe un follows por owner porque el único creador es
`get_or_create_follows_radar` con la constante `FOLLOWS_NAME`. La unicidad es
**del código, no de SQLite** — y se rompe en cuanto un radar normal toca ese nombre.

### ¿El nombre es identidad o presentación?

Hoy es **identidad de facto**. Debería ser presentación. Dos reproducciones:

**A.1 — Colisión con el nombre reservado exacto**

```
=== A · COLISIÓN CON NOMBRE RESERVADO (exacto) ===
  radar normal id    : b58941c9 freq='diaria'
  follows resolvió a : b58941c9 freq='diaria'
  ¿es el normal?     : True
  follow() devolvió  : True
  seen escritos      : [('b58941c9', 'P-1')]
  VEREDICTO          : CONTAMINA el radar de búsqueda
```

El radar de búsqueda **se acepta silenciosamente como watchlist**. `follow()`
escribe en su `radar_seen_results`, que es exactamente la tabla que el diff del
radar normal usa como "lo ya visto": el proceso seguido queda marcado como visto
sin haber sido nunca un resultado de esa búsqueda, y por tanto **nunca se
anunciará como nuevo cuando lo sea**. Y `create_radar` acepta el nombre:

```
=== NOMBRE RESERVADO SIN PROTECCIÓN ===
  create_radar(name=FOLLOWS_NAME, freq='diaria') -> ACEPTADO fcb2b401
  VEREDICTO          : el nombre reservado NO está protegido hoy
```

**A.2 — Divergencia `UNIQUE`(binario) vs `SELECT`(`COLLATE NOCASE`)**

El `UNIQUE (owner, name)` es binario; el lookup es `NOCASE`. Los dos no coinciden,
así que hay nombres que el `SELECT` considera iguales y el `UNIQUE` distintos:

```
=== A · DIVERGENCIA UNIQUE(binario) vs SELECT(NOCASE) ===
  canónico           : 52d618d7 name='📌 Procesos seguidos'
  variante de caja   : '📌 procesos seguidos'
  create_radar       : ACEPTADO e4e6b6d2 (UNIQUE binario no lo vio)
  filas NOCASE-match : 2
  VEREDICTO          : AMBIGUO — el SELECT elige fila arbitraria
```

Dos filas satisfacen el `WHERE ... COLLATE NOCASE` y la consulta **no tiene
`ORDER BY`**: cuál gana es una decisión del plan de SQLite, no del modelo. Con el
orden inverso el daño es completo:

```
=== A · SECUESTRO NOCASE (variante creada primero) ===
  radar normal       : ecb5bb4f name='📌 procesos seguidos' freq='diaria'
  follows resolvió a : ecb5bb4f freq='diaria'
  ¿secuestrado?      : True
  filas radars       : 1 (el canónico nunca se creó)
  VEREDICTO          : SECUESTRO — follows escribe en un radar de búsqueda
```

El radar follows **nunca llega a existir**. Toda la watchlist del usuario vive
dentro de su radar de búsqueda.

### ¿Todos los callers obtienen el mismo `radar.id`?

Sí, mientras nadie toque el nombre: todos pasan por `get_or_create_follows_radar`
(`is_following`, `list_follows`, `follow`, `unfollow`, `refresh_follows`, y
`streamlit_app.py:145`). Es el único punto de resolución — lo cual es bueno: **un
solo sitio que arreglar**.

### Conclusión A

La identidad debe pasar a ser **`(owner, frequency='follows')`** para la
*resolución*, y el nombre canónico exacto debe seguir siendo parte del
*invariante de unicidad* — porque el `UNIQUE (owner, name)` es el único índice
único que hay, y sin él dos filas `follows` del mismo owner son legales.

**La unicidad no se puede afirmar hasta que las tres reglas estén aplicadas a la
vez:** (1) resolución por `frequency`, (2) reserva bidireccional en
`create_radar`, (3) detección fail-closed de cardinalidad al leer. Con solo la
primera —como en la ronda 1— la watchlist sigue pudiendo partirse en dos, y un
`LIMIT 1` lo haría además invisible. Ver §F.0.

---

## B. Creación concurrente — reproducida

Ocho hilos, ocho conexiones SQLite independientes, una `threading.Barrier`, mismo
owner, radar inexistente. Sin `sleep`.

```
=== B · CARRERA DE CREACIÓN (8 hilos) ===
  ids devueltos      : 8 -> distintos=1 nones=0
  excepciones        : []
  filas finales      : 1
    id=922d4b8d name='📌 Procesos seguidos' freq='follows'
  VEREDICTO          : OK (una fila, un id)
```

**La carrera de creación ya está resuelta hoy** y el mérito es del `UNIQUE
(owner, name)` más el patrón insert-or-reread que ya implementa
`get_or_create_follows_radar` (`create_radar` traduce `sqlite3.IntegrityError` a
`RadarExistsError` en `store.py:593-596`; el `except` re-lee la fila ganadora).

**Esto es un hallazgo, no un trámite:** el fix de identidad de §A **no debe
romperlo**. Al mover la búsqueda a `frequency`, la carrera la sigue decidiendo el
`UNIQUE` sobre el **nombre** — el único índice único disponible. La creación
concurrente sigue siendo insert-or-reread; solo cambia el predicado de lectura.

Riesgo latente detectado de paso: `get_or_create_follows_radar` declara devolver
`Radar` pero el `return store.get_radar_by_name(...)` final puede devolver `None`
si otro proceso borra la fila entre el `create` y el `get`. Todos los callers
hacen `radar.id` acto seguido → `AttributeError`. Ventana ínfima y preexistente;
se documenta como deuda, no se amplía el bloque para cerrarla.

---

## C. Lease — ¿aplica el de P1A?

Sí, **limpiamente**, y la evidencia es que el lease no toca nada de lo que hace
raro al radar sintético:

| Requisito | ¿Lo cumple el radar follows? | Evidencia |
|---|---|---|
| Fila persistente con `id` | Sí | `get_or_create_follows_radar` crea fila real en `radars` |
| Columnas de lease | Sí | Son de `radars`; el sintético es una fila de `radars` |
| El predicado del lease **no** mira `enabled`/`needs_attention` | Sí | `_ACQUIRE_SQL` (`lease.py:36-47`) filtra por `id` + expiración |
| `acquire`/`renew`/`release` solo exigen id+token+expiración | Sí | `lease.py:107-150` |
| Exclusión por fila, no por owner | Sí | `test_lease_is_per_radar` (`test_radar_store.py:432`) |

`lease.py` es SQL puro sobre `radars.id`. No conoce `frequency`, ni `run_radar`, ni
la semántica de búsqueda. **Un radar sintético es, para el lease, una fila más.**
Que dos owners distintos no se bloqueen es consecuencia directa: son `id` distintos.

**`LEASE_DURATION = 600s` se reutiliza sin cambios.** El peor caso de un refresh de
follows es más corto que el de `run_radar` (no hay consulta de ventana ni count:
solo el barrido por clave en lotes de 100). 600s ya cubre `run_radar`, luego cubre
follows con más margen. Una duración más larga de lo necesario no rompe corrección;
multiplicar políticas de lease sí añade complejidad sin comprar nada. **No se crea
una duración nueva.**

---

## D. Atomicidad — el defecto crítico

```
=== D · ATOMICIDAD seen+events en refresh_follows ===
  upsert_seen abre conexión propia  : True
  record_events abre conexión propia: True
  refresh_follows usa BEGIN         : False
  refresh: upsert_seen dentro del bucle de red: True
  VEREDICTO          : seen y events son transacciones INDEPENDIENTES
```

Confirmado. Peor que en Preflight 1, por dos razones:

1. **`upsert_seen` corre dentro del bucle de red** (`follows.py:222`), una conexión
   autocommit por clave; `record_events` corre **una sola vez al final**
   (`follows.py:229`), en otra conexión. La ventana no es un instante entre dos
   escrituras: es **todo el resto del fetch**.
2. Es exactamente la pérdida que cerró Preflight 1: `seen` avanza → crash →
   el evento nunca entra al feed → el refresh siguiente ya ve el `snapshot_hash`
   nuevo y **no vuelve a detectar el cambio**. Se pierde para siempre.

**El lease por sí solo es insuficiente.** El lease impide dos escritores
simultáneos; no hace atómico a un escritor que muere a la mitad. Este bloque debe
cerrar también la atomicidad de *snapshot de seguidos + eventos derivados*.

---

## E. Carrera follow/unfollow — el UPSERT resucita

Secuencia exacta del enunciado, ejecutada de verdad (el `unfollow` ocurre **dentro**
de la llamada remota, que es la ventana real):

```
=== E · UNFOLLOW DURANTE EL FETCH ===
  summary            : {'checked': 1, 'baseline': 1, 'changed_state': 0, ...}
  seen tras refresh  : ['P-1']
  eventos            : [('followed', 'P-1')]
  VEREDICTO          : RESUCITADO — bug confirmado
```

**Bloqueo confirmado.** `refresh_follows` llama `store.upsert_seen`
(`follows.py:222`), y el `ON CONFLICT ... DO UPDATE` (`store.py:340-359`) **inserta
si no hay fila**. La fila que `unfollow` borró vuelve a nacer. El usuario dejó de
seguir un proceso y el sistema se lo devuelve, silenciosamente, en la siguiente
pasada del scheduler.

La causa de raíz es un error de reparto de responsabilidades: **`follow()` es quien
crea la membresía; `refresh_follows` solo debería refrescarla.** Un refresh no tiene
por qué poder crear filas, y mientras pueda, resucitará borrados.

### E-ter. La carrera fina: `unfollow` → `follow` de la **misma clave**

Cambiar el UPSERT por un UPDATE cierra la resurrección, pero **no** esta variante,
que el review de la ronda 2 detectó:

```
refresh lee K1 (first_seen_at = T0)
→ empieza la consulta remota
→ el owner hace unfollow(K1)      → la fila T0 desaparece
→ el owner hace follow(K1)        → nace una fila NUEVA, misma clave, first_seen_at = T1,
                                    snapshot_hash = '' (pendiente de baseline)
→ SECOP devuelve un cambio de estado
→ refresh materializa
```

Con un `WHERE radar_id = ? AND result_key = ?`, el UPDATE **acierta**: rowcount 1.
Y hace dos daños a la vez:

1. **pisa el `snapshot_hash=''`** de la membresía recién nacida — le mata la línea
   base, y con ella el derecho a que su primer refresco sea silencioso;
2. **emite un `changed_state`** que compara contra el snapshot de una membresía
   que el usuario ya borró. El evento describe algo que dejó de existir.

La lección es de modelado: **`(radar_id, result_key)` identifica el proceso, no la
membresía.** Dos altas separadas por un `unfollow` son dos membresías distintas que
comparten clave, igual que dos filas distintas comparten un valor de columna.

**`first_seen_at` como versión inmutable.** Durante la vida de una membresía nadie
lo toca: el `UPDATE` del refresco lo excluye del `SET` a propósito, y el
`INSERT ... ON CONFLICT DO NOTHING` del alta no reescribe la fila existente. Solo
cambia cuando la membresía muere y nace otra — que es exactamente el evento que hay
que detectar. Sirve de versión sin añadir columna ni migración.

Por eso `FollowsSeenUpdate.expected_first_seen_at` es **obligatorio** (sin default):
un campo opcional aquí sería un campo olvidable, y olvidarlo devuelve el bug en
silencio. El `WHERE` del refresco lo exige:

```sql
WHERE radar_id = ? AND result_key = ? AND first_seen_at = ?
```

rowcount 0 significa ahora dos cosas, y ambas piden lo mismo (no escribir, no
emitir): la membresía **desapareció**, o **fue reemplazada por otra**.

**Límite conocido:** dos encarnaciones con el mismo `first_seen_at` al microsegundo
serían indistinguibles. `_now()` es ISO-8601 con microsegundos, así que exigiría un
`unfollow`+`follow` dentro del mismo microsegundo. Si alguna vez importa, la
solución es una columna de versión explícita, no afinar el reloj.

---

## E-bis. Hallazgo adicional — el fetch no deduplica

`_fetch_current_rows_batch` (`follows.py:114-128`) construye:

```sql
WHERE id_del_proceso in(...) OR referencia_del_proceso in(...)
```

Un `OR` sobre dos campos que **ambos** identifican la fila: SECOP puede devolver la
misma fila más de una vez, y el bucle de `refresh_follows` (`follows.py:189-225`) no
salta las claves ya procesadas — `found_keys` es un `set`, pero se usa para contar
`missing`, no para cortar el bucle. Dos filas con la misma clave ⇒ **dos eventos
`changed_state` y dos upserts**.

`runner._refresh_seen_rows` **sí** deduplica (`seen_fetched`, `runner.py:312-316`).
La ruta de follows es la que quedó sin esa protección. Como este bloque trata
precisamente de la duplicación de eventos de follows, se cierra aquí.

---

## F. Decisión

### F.0 — Corrección de la ronda 1 (review PARTIAL)

La primera implementación **afirmó identidad única sin haberla aplicado**. El
review lo detectó y tenía razón en los cinco puntos. Queda registrado porque el
error es instructivo: el fix de identidad resolvía por `frequency` pero leía con
`ORDER BY created_at, rowid LIMIT 1`, es decir, **elegía una fila y ocultaba
exactamente la corrupción que decía cerrar**. Un `LIMIT 1` sobre un invariante de
cardinalidad no lo verifica: lo enmascara.

| Punto | Estado ronda 1 | Corrección |
|---|---|---|
| Identidad | `LIMIT 1` elegía en silencio; un follows podía nacer con nombre alternativo | Lectura de hasta 2 filas + reglas fail-closed; `create_radar` exige nombre canónico para `follows` |
| `follow` | `upsert_seen` + `record_events` autocommit separados; re-follow reescribía `snapshot_hash=''` y **silenciaba el cambio siguiente** | `materialize_follow_if_absent`: transacción, `INSERT ... DO NOTHING`, evento atado al rowcount |
| Conteos | `changed_state`/`changed_value` **construidos**: contaban claves que el unfollow luego descartó | Conteos derivados de los updates **live** dentro de la transacción |
| `not_follows` | Tratado como skip → una watchlist corrupta parecía sana en cada pasada | Excepción tipada `FollowsRadarIntegrityError` ⇒ error |
| Nombre reservado | `RadarReservedNameError` subía sin manejar desde el handler | Capturado con `route_reason="radar_save_reserved_name"` |

### F.0-bis — Corrección de la ronda 2 (segundo PARTIAL)

Dos carreras finales, ambas fundadas. El patrón que las une: **cerrar una carrera
con un `WHERE` más estrecho no basta si el `WHERE` sigue identificando la cosa
equivocada.**

| Punto | Estado ronda 2 | Corrección |
|---|---|---|
| Membresía por encarnación | El `UPDATE` filtraba por `(radar_id, result_key)`, que identifica **el proceso**, no la membresía: un `unfollow`+`follow` durante la red hacía que acertara sobre la fila nueva | `expected_first_seen_at` obligatorio + `AND first_seen_at = ?` en el `WHERE`. Ver §E-ter |
| Éxito ficticio | `follow()` ignoraba el resultado y devolvía `True` aunque no se hubiera escrito nada; los materializadores no validaban el **nombre canónico** dentro de la transacción | `_read_canonical_follows` (existe + es follows + nombre canónico) en ambos materializadores; `follow()` levanta `FollowsRadarIntegrityError` si `not_follows` |
| Asociación evento↔membresía | Nada impedía que un alta llevara el evento de otra clave | Validación de `kind`/`result_key`/`dataset` antes del INSERT |

### F.0-ter — Corrección de la ronda 3 (arquitectura aprobada, consistencia pendiente)

Una sola inconsistencia, de alcance quirúrgico: `materialize_follows_refresh`
tenía **un solo parámetro `run_id`** para dos destinos que la decisión de §F.2 ya
distinguía por escrito — `radar_events.run_id` (siempre NULL) y
`radar_seen_results.last_run_id` (siempre el centinela `'follows_refresh'`).
`_refresh_locked` pasaba `run_id=None`, así que el evento quedaba correcto por
casualidad y el seen quedaba NULL por el mismo motivo que debía ser el centinela.
El propio `_REFRESH_RUN_ID` de `follows.py`, definido y documentado, **nunca se
usaba** — la señal más clara de que la intención escrita no llegó al código.

| Punto | Estado ronda 3 | Corrección |
|---|---|---|
| Identidades mezcladas | Un parámetro `run_id` alimentaba evento y seen a la vez | `seen_run_id` (default `'follows_refresh'`) solo para seen; `run_id=None` hardcodeado en el `_insert_events` interno, sin parámetro que lo exponga a error |
| Docstring de `record_events` | Seguía afirmando que `follows.py` lo usa | Corregido: follows va por los materializadores transaccionales desde la ronda 1 |

### F.1 — Conclusión elegida: **(1) Reusar el lease de radar + materializador específico de follows**

Con una condición que el enunciado no anticipaba y la investigación impone:
**la identidad canónica se arregla primero**. Poner un lease sobre un radar que
puede ser el radar de búsqueda equivocado es candarle la puerta a la casa del
vecino.

Piezas:

1. **Identidad canónica por tipo, con el nombre como parte del invariante.** La
   búsqueda pasa a `(owner, frequency='follows')`. El nombre deja de ser el
   criterio de resolución, pero **sigue siendo parte del invariante**: la unicidad
   de un follows por owner la sostiene el `UNIQUE (owner, name)`, que es el único
   índice único disponible. Por eso `create_radar` exige el nombre **canónico
   exacto** cuando `frequency='follows'`: un follows llamado de otra forma pasaría
   el UNIQUE y sería un segundo follows del mismo owner.

2. **Reserva bidireccional en `create_radar`.**
   - `frequency != 'follows'` ⇒ prohibida toda variante `casefold` del nombre
     reservado (cierra A.1 y A.2: la colisión no puede nacer).
   - `frequency == 'follows'` ⇒ exigido el nombre canónico exacto (cierra la
     cardinalidad por delante).

3. **Detección fail-closed de duplicados heredados.** La resolución pide **hasta
   dos filas** y decide con las dos a la vista — nunca `LIMIT 1`, que devolvería
   una cualquiera y escondería la corrupción:

   | Filas `frequency='follows'` | Resultado |
   |---|---|
   | 0 | no existe → se crea |
   | 1, nombre canónico | válida |
   | 1, nombre no canónico | `FollowsRadarIntegrityError` |
   | ≥ 2 | `FollowsRadarCardinalityError` |

   Ninguna de esas ramas **modifica una sola fila**: reparar una base heredada es
   una decisión humana, no un efecto colateral de un refresco. El scheduler aísla
   al owner roto y **sigue con los demás**.

4. **Bases con el nombre secuestrado.** Si un radar de búsqueda ya ocupa el nombre
   reservado, la resolución por `frequency` **no lo toca** y el `create` choca
   contra el `UNIQUE` → `FollowsRadarNameConflictError`, explícito y accionable. El
   dato del usuario se respeta. **No requiere migración destructiva** (ver Gate).

5. **`follow` atómico e idempotente: `materialize_follow_if_absent`.** Alta de
   membresía y evento `followed` en **una** transacción, con
   `INSERT ... ON CONFLICT DO NOTHING` y el evento condicionado al `rowcount`.
   Sin lease: `follow` es una acción del usuario sobre **su** fila, no una corrida,
   y el ganador de dos altas simultáneas lo decide la PRIMARY KEY dentro de la
   transacción; un lease serializaría clics que no compiten por nada.

   Cierra un defecto que la ronda 1 no vio: el `upsert_seen` de `follow` reescribía
   `snapshot_hash=''` **también cuando la fila ya existía**. Re-seguir algo ya
   seguido borraba su línea base, y el refresco siguiente trataba un cambio real
   como baseline — es decir, **lo silenciaba**.

6. **Lease reusado tal cual** (§C), `LEASE_DURATION=600s`, `release` token-fenced
   en `finally`.

7. **Materializador propio: `materialize_follows_refresh`.** No se reusa
   `materialize_successful_run` porque obligaría a las cuatro cosas que el enunciado
   prohíbe y que follows nunca tuvo: insertar un `radar_run` ficticio, limpiar
   `failure_count`/`permanent_failure_streak`, tocar `needs_attention` y aplicar
   `_MARK_SUCCESS_SQL`, que es semántica de radar de búsqueda. El materializador de
   follows escribe **solo** snapshot + eventos.

8. **UPDATE, nunca UPSERT** en el refresh (cierra §E). Los eventos van **ligados a
   su clave**: si el `UPDATE` afecta 0 filas (`unfollow` ganó), esa clave no
   actualiza nada **y no emite evento**. La imposibilidad de "evento sin fila" queda
   en la estructura de datos, no en un `if` que alguien pueda olvidar.

9. **Conteos materializados, no construidos.** `baseline`, `changed_state`,
   `changed_value` y `events` se derivan de los updates **live** dentro de la
   transacción, no de lo que la fase de red planeó escribir. La distinción no es
   cosmética: si un `unfollow` descarta una clave, el conteo construido
   reportaría `changed_state=1` y el scheduler —y mañana el notifier— anunciarían
   un cambio **que nunca entró al feed**. Solo `missing` y `lookup_errors` siguen
   siendo construidos: son hechos de la fase de red, no de la escritura.

10. **Dedupe por clave** al construir los updates (cierra §E-bis).

### F.2 — Semántica de `run_id` (decisión documentada)

Hoy `refresh_follows` pasa **`run_id=radar.id`** a eventos y seen
(`follows.py:202, 224`). Eso **no es la semántica histórica que conviene preservar**:
es un identificador que **no resuelve a ninguna fila de `radar_runs`** — un run
ficticio ya presente en los datos, con forma de FK que no lo es.

**Decisión:** `radar_events.run_id = NULL` para los eventos de refresh de follows.
La columna es nullable (`store.py:276`) y no hay corrida que auditar: follows no
crea `radar_runs`, y este bloque tiene prohibido inventar uno. NULL dice la verdad;
`radar.id` miente. Para `radar_seen_results.last_run_id` se usa el centinela
`'follows_refresh'`, coherente con el `'follow_manual'` que `follow()` ya escribe
(`follows.py:32`): ambos declaran "esto no vino de una corrida".

**Corrección de consistencia (ronda 4).** La primera implementación de esta
decisión tenía un solo parámetro `run_id` que alimentaba **los dos** destinos:
`_refresh_locked` llamaba con `run_id=None`, así que el evento quedaba NULL
(correcto) pero `seen.last_run_id` quedaba **también NULL** (incorrecto: debía
llevar el centinela). El propio código delataba el bug — `_REFRESH_RUN_ID` estaba
definido en `follows.py` y **nunca se usaba**, señal de una intención escrita que
no llegó a aplicarse.

Son dos identidades distintas y ahora van en parámetros distintos:

- `materialize_follows_refresh(..., seen_run_id: str = "follows_refresh")` — se
  aplica a `_update_seen` → `radar_seen_results.last_run_id`.
- `_insert_events(..., run_id=None)` — hardcodeado dentro del materializador, sin
  parámetro: no existe ningún caso en que `radar_events.run_id` deba ser otra
  cosa, así que no se ofrece la posibilidad de pasarlo mal.

`follows._refresh_locked` pasa explícitamente `seen_run_id=_REFRESH_RUN_ID`
(el centinela privado del módulo, mismo valor que el default de `store.py`, sin
que `store.py` importe `follows.py` — la fuente de verdad del valor literal vive
en `store.py`, igual que `FOLLOWS_RADAR_NAME`).

### F.3 — Estados de control

`refresh_follows` mantiene el `dict` que devuelve hoy (la UI y el scheduler lo
consumen por clave; cambiarlo a dataclass rompería `test_radar_follows` y
`streamlit_app.py` sin comprar nada) y **añade** `status`:

| status | Significado | Para el scheduler |
|---|---|---|
| `refreshed` | Consultó y materializó | cuenta en `follows` |
| `no_items` | El owner no sigue nada; no hubo red | cuenta en `follows` |
| `already_running` | Otro proceso tiene el lease. **No consulta SECOP, no escribe** | **skip, no error** |
| `lease_lost` | El lease se perdió durante la red; no se escribió nada | **skip, no error** |

`REFRESH_SKIPPED` contiene **solo concurrencia legítima**. Una violación de
invariante no es un estado de control: el radar que resolvimos como watchlist
dejando de serlo bajo el fence (`not_follows`) es corrupción, y contarlo como
omisión haría que una watchlist rota **pareciera sana en cada pasada**. Se lanza
como `FollowsRadarIntegrityError`; el scheduler ya convierte la excepción en
`status="error"` y sigue con los demás owners.

### F.4 — Política de errores remotos: **preservada**

Un `lookup` que falla no confirma desaparición, no borra membresía, no emite evento
falso y libera el lease. Se **añade** el contador `lookup_errors` (aditivo,
informativo). **No se toca `failure_count` del radar sintético**: el contrato actual
no lo hace, y mezclar P0D con follows está fuera de este bloque.

### F.5 — Baseline: **preservada**

`snapshot_hash == ''` ⇒ baseline silenciosa, sin evento retroactivo. Sigue igual, y
ahora **también es atómica**: el snapshot de baseline se escribe entero o nada. El
UPDATE-only no la afecta: la fila ya existe porque la creó `follow()`.

---

## Gate de decisión

| Condición | Veredicto |
|---|---|
| Identidad estable | **Sí, una vez aplicadas las tres reglas de §A** (resolución por tipo + reserva bidireccional + cardinalidad fail-closed). Con solo la primera, **no** |
| Carrera de creación resoluble con insert-or-reread | **Sí** — ya funciona hoy (§B); el fix la preserva |
| El lease existente aplica | **Sí** — §C, sin variante ni duración nueva |
| Escrituras materializables en transacción corta | **Sí** — toda la red termina antes del `BEGIN IMMEDIATE` |
| ¿Identidad única sin migración destructiva? | **Sí** — el predicado cambia; ninguna fila se borra ni se convierte |
| ¿El nombre reservado colisiona de forma irresoluble? | **No** — se reserva hacia adelante; hacia atrás, error explícito sin tocar datos |
| ¿El scheduler depende de múltiples filas follows por owner? | **No** — itera `radars` y despacha por `frequency` (`scheduler.py:128`) |
| ¿Exige cambiar el modelo de ADR-035? | **No** — lo *restaura*: ADR-035 dijo `frequency='follows'`; el código resolvía por nombre |

**→ GO. Se continúa con la implementación en esta misma sesión.**

---

## Call graph

**Antes**

```
follows.follow(store, owner, row)                    ← acción del usuario
  ├─ get_or_create_follows_radar → get_radar_by_name(owner, NAME)  ← identidad por nombre
  ├─ store.upsert_seen(snapshot_hash='')  (autocommit)  ← BORRA la baseline si ya existía
  └─ store.record_events([...])           (autocommit, otra conexión)

scheduler.run_due_radars
  └─ follows.refresh_follows(deps, store, owner)      ← SIN LEASE
       ├─ get_or_create_follows_radar → get_radar_by_name(owner, NAME)
       ├─ store.get_seen
       ├─ _fetch_current_rows → deps.secop_client.query   ← RED
       ├─ store.upsert_seen   (autocommit, por clave, DENTRO del bucle de red)  ← resucita
       └─ store.record_events (autocommit, al final, otra conexión)
```

**Después**

```
follows.follow(store, owner, row)
  ├─ get_or_create_follows_radar → _resolve_follows_radar   ← fail-closed (0|1|1-no-canónica|≥2)
  └─ store.materialize_follow_if_absent      ← BEGIN IMMEDIATE
        verificar follows → INSERT ... DO NOTHING → evento SOLO si rowcount=1 → COMMIT

scheduler.run_due_radars
  └─ follows.refresh_follows(deps, store, owner)
       ├─ get_or_create_follows_radar → _resolve_follows_radar
       │     └─ list_radars_by_frequency(owner,'follows',limit=2)   ← nunca LIMIT 1
       ├─ store.try_acquire_run_lease(radar.id)   ← no acquired ⇒ already_running, sin red
       └─ try:
       │    _refresh_locked
       │      ├─ store.get_seen                       (snapshot bajo lease)
       │      ├─ _fetch_current_rows → secop_client   ← ÚLTIMA RED
       │      ├─ (memoria) dedupe → FollowsSeenUpdate[] + EventRecord[] ligados a su clave
       │      └─ store.materialize_follows_refresh    ← BEGIN IMMEDIATE, sin red dentro
       │            └─ conteos derivados de `live`, no de `updates`
       └─ finally: store.release_run_lease(radar.id, token)   ← token-fenced
```

## Orden exacto de materialización

**`materialize_follows_refresh`**

```
1. BEGIN IMMEDIATE
2. lease.renew(...)              ← PRIMERA DML; rowcount 0 ⇒ ROLLBACK + lease_lost
3. _read_canonical_follows(conn, radar_id)
   └─ no existe | frequency != 'follows' | nombre no canónico ⇒ ROLLBACK + not_follows
4. por cada update:
   UPDATE radar_seen_results
      SET ..., last_run_id = seen_run_id ('follows_refresh')
    WHERE radar_id=? AND result_key=? AND first_seen_at=?   ← fence de versión
   └─ rowcount 0 ⇒ la membresía murió (unfollow) o fue reemplazada por otra
      encarnación (unfollow+follow) ⇒ la clave NO entra a `live`
5. INSERT eventos SOLO de las claves de `live`; identidad (radar_id/owner/radar_name)
   derivada del paso 3, run_id=NULL siempre (no toma seen_run_id: son identidades
   distintas — ver §F.2)
6. COMMIT        (cualquier excepción ⇒ ROLLBACK total)
7. conteos (baseline/changed_state/changed_value/events) derivados de `live`
```

**`materialize_follow_if_absent`** (sin lease: el ganador lo decide la PRIMARY KEY)

```
0. validar asociación evento↔membresía (kind='followed', misma result_key, mismo
   dataset) ⇒ ValueError antes de abrir nada
1. BEGIN IMMEDIATE
2. _read_canonical_follows(conn, radar_id)  ⇒ ROLLBACK + not_follows
3. INSERT INTO radar_seen_results ... ON CONFLICT DO NOTHING
   └─ rowcount 0 ⇒ ya lo seguía: no se toca snapshot_hash ni first_seen_at
4. INSERT evento 'followed' SOLO si rowcount fue 1
5. COMMIT        (cualquier excepción ⇒ ROLLBACK total)
```

## Cierre de implementación (misma sesión, tras el GO)

Los cinco defectos quedan cerrados y **verificados por mutación** — que un test de
concurrencia pase no prueba nada si también pasaría sin el fix:

| Defecto | Cierre | Prueba de que el test no pasa trivialmente |
|---|---|---|
| 1 · sin lease | `refresh_follows` toma el run lease del radar | Con el lease neutralizado, `test_second_refresh_of_same_owner_skips_without_touching_secop` **falla** |
| 2 · atomicidad refresh | `materialize_follows_refresh` (BEGIN IMMEDIATE, fence, todo-o-nada) | Fault injection en las 5 fases: rollback total en todas |
| 3 · resurrección | `UPDATE` + eventos atados a su clave | Con `_update_seen` mutado a UPSERT, `test_unfollow_during_fetch_is_not_resurrected` **falla** |
| 4 · identidad | resolución por tipo + reserva bidireccional + cardinalidad fail-closed | Con `list_radars_by_frequency` mutado a `LIMIT 1`, `test_two_legacy_follows_radars_raise_instead_of_picking_one` **falla** |
| 5 · duplicados | corte por `found_keys` al construir | `test_duplicate_rows_from_secop_emit_one_event` |
| 6 · baseline destruida | `INSERT ... DO NOTHING` en `materialize_follow_if_absent` | Con `_insert_seen_if_absent` mutado a UPSERT, `test_refollow_after_baseline_does_not_silence_the_next_change` **falla** |
| 7 · atomicidad follow | membresía + evento en una transacción, evento atado al rowcount | Fault injection en las 4 fases del alta |
| 8 · encarnación | `expected_first_seen_at` + `AND first_seen_at = ?` | Con el fence quitado del `WHERE`, `test_unfollow_then_refollow_during_fetch_does_not_touch_the_new_membership` **falla** |
| 9 · éxito ficticio | `_read_canonical_follows` en ambos materializadores; `follow()` levanta si `not_follows` | Con la comprobación de nombre relajada, `test_follow_does_not_report_success_if_the_radar_lost_its_canonical_name` **falla** |
| — · conteos | derivados de los updates `live` | Con los conteos mutados a `updates`, `test_unfollow_during_fetch_reports_no_change` **falla** |
| — · asociación | `kind`/`result_key`/`dataset` validados antes del INSERT | 3 tests de rechazo |

## Deuda residual declarada

1. `get_or_create_follows_radar` puede devolver `None` si la fila se borra entre el
   `create` y el reread (§B). Preexistente, ventana ínfima; no se amplía el bloque.
2. `unfollow()` **crea** el radar follows si no existe (efecto secundario raro pero
   inofensivo). Preexistente, fuera de scope.
3. `summary["missing"]` se calcula contra el snapshot inicial: si un `unfollow`
   entra durante el fetch, la clave cuenta como "missing" aunque el motivo sea otro.
   Contador informativo, sin efecto en escrituras.
4. `radar_events.owner` sigue siendo el email en claro y se loguea (N12). Fuera de
   este bloque.
5. La reserva del nombre no protege bases **ya** colisionadas: se reporta con error
   explícito y se documenta; cerrarlo requeriría renombrar datos del usuario.

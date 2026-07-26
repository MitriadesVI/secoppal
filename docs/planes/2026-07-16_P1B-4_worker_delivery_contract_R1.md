# P1B-4 — Worker Delivery Contract (R1.2)

**Fecha:** 2026-07-16 · **Rama:** `radar/mvp-manual` · **HEAD:** `c8c59a9`
**Estado:** diseño R1.2 — tres carreras cerradas y el alcance del huérfano
simplificado. Ningún código escrito. Ningún test escrito. Ningún commit.

**Cambios sobre R1.1:**
1. **Linearización explícita** autorización↔revocación en el COMMIT de
   `authorize_and_begin_dispatch` (§B.5). El invariante imposible de R1.1 se
   sustituye (INV-W11 reescrito).
2. **`claim_candidate`** captura `now` **después** de adquirir `BEGIN IMMEDIATE`
   (§D.2), y la autorización + clave externa + attempt se colapsan en **una sola
   transacción** (§B.6). Solo quedan **dos escrituras bloqueables** durante la
   vida efectiva del claim → se ratifica la fórmula (§D.1).
3. **`claim_for_reconciliation`** acepta también `pending + dispatching` (§I.4),
   e INV-W25 prohíbe `claim.release` tras insertar un `dispatching`.
4. **Política única de huérfano:** todo `dispatching` huérfano termina
   `ambiguous`, incluso con proveedor de idempotencia fuerte (§I). El worker ya
   **no** reanuda huérfanos; se registra P1B-X.
5. **Orden de `process_one`** reformulado: reconciliar *cualquier* huérfano →
   barrer → procesar (§B.3).
6. **`security_blocked`** gana salida estructurada + logging seguro inyectable
   (§E.5).

---

## 0. Los dos hallazgos que ordenan el bloque

### 0.1 El estado varado (R1)

`try_claim` exige `expires_at > :now`; `finish_attempt` exige `status='claimed'`
con claim vigente. Componiéndolos: un batch vencido no se puede reclamar → no se
puede cerrar → **no se puede marcar `dead`/`expired`**. `pending` vencido es
terminal de facto y silencioso. **INV-W10 no se cumple en `main`.**

### 0.2 El estado varado **peligroso** (corrección bloqueante, R1.1)

Un sweep que mate lo vencido sin más construye una mentira:

> El proveedor **acepta**. La respuesta se pierde. El proceso muere. El claim
> vence. El batch expira. El sweep lo marca `dead`/`expired` — afirmando *"nada
> salió"* sobre un mensaje **que está en el teléfono del usuario**, y borrando el
> `dispatching` que era la única prueba de la ambigüedad.

De aquí: **INV-W16**, el `NOT EXISTS` del sweep (§L.2) y `claim_for_reconciliation`
(§I.4).

### 0.3 Las tres carreras (R1.2)

R1.1 dejaba tres ventanas que un adversario concurrente abre:

- **C-REVOKE:** la revalidación (una transacción) y el `begin_attempt` (otra) eran
  operaciones separadas. Una revocación podía commitear *entre ambas* y el envío
  salía igual. La garantía literal *"revocar antes del `send` impide el envío"* es
  **imposible** sin mantener una transacción SQLite abierta durante la red — lo que
  INV-W1 prohíbe. Se sustituye por una **linearización** (§B.5).
- **C-CLAIM:** `try_claim` recibía `now` como parámetro, capturado **antes** de
  entrar en `BEGIN IMMEDIATE`. Bajo contención, el `busy_timeout` para adquirir el
  lock consumía parte de la ventana del claim **antes** de que el worker fuera
  dueño. Se corrige capturando `now` **después** del lock (§D.2).
- **C-INHERIT:** un huérfano podía quedar `pending + dispatching` (por un `finally`
  mal escrito, una versión anterior, o manipulación manual), y ese estado no lo
  recuperaba ni el sweep ni la reconciliación. Se amplía `claim_for_reconciliation`
  (§I.4) y se prohíbe estructuralmente su causa (INV-W25).

---

## Ratificadas (2026-07-16)

| # | Decisión | Dónde |
|---|---|---|
| R1 | `external_idempotency_key` se persiste **siempre**: `sha256("p1b-extid-v1\|"+notification_id)`, UNIQUE parcial `WHERE NOT NULL` | §G |
| R2 | `claim_duration = provider_timeout + 2×sqlite_busy_timeout + safety_margin`; default **120 s**; función pura con inputs explícitos; `now` capturado tras el lock | §D |
| R3 | `dead` terminal: `opted_out`, `endpoint_revoked`, `radar_archived`, `invalid_address`, `expired` | §E.3 |
| R4 | `hold`: `endpoint_disabled`, `endpoint_unverified`, `recipient_missing`, `principal_suspended`, `owner_not_linked` | §E.4 |
| R5 | `principal_mismatch` = **`security_blocked`**: cero llamada, `not_before=expires_at`, alta severidad con salida estructurada + logging seguro, **no aborta el drain** | §E.5 |
| R6 | Retry 1/5/15/60 min, máx. 5; agotado → `not_before=expires_at` → muere `expired` | §K |
| R7 | **No** existe `release_hold` manual | §Producto E |
| R8 | Taxonomía **conservadora por evidencia**: sin prueba de no-envío, `ambiguous` | §J |
| **R9** | **Linearización** autorización↔revocación en `authorize_and_begin_dispatch` COMMIT; la garantía literal previa se descarta | §B.5, INV-W11 |
| **R10** | `claim_candidate` captura `now` tras `BEGIN IMMEDIATE`; autorización+clave+attempt en **una** transacción | §B.6, §D.2, INV-W26, INV-W27 |
| **R11** | **Política única de huérfano** → `ambiguous`; el worker no reanuda ni con idempotencia fuerte | §I, INV-W28 |

---

## A. Modelo de estados

### A.1 Batch

```
                         selector (P1B-3)
                               │
                               ▼
      ┌───────────────────►┌──────────┐◄── hold branch  (not_before = now+15m)
      │   sweep_expired    │ pending  │◄── security branch (not_before = expires_at)
      │  (vencido, SIN     │          │◄── finish_attempt(retry)
      │   dispatching,     └────┬─────┘
      │   SIN claim vivo)       │ claim_candidate  (NOT EXISTS dispatching)
      │                         ▼
      │                   ┌──────────┐   authorize_and_begin_dispatch (1 tx):
      │                   │ claimed  │     ├─ revalida → dead / hold / security  [sin attempt]
      │                   └────┬─────┘     └─ autoriza → INSERT dispatching  → provider.send
      │                        │
      │  finish_attempt(sent)  ├───────────────────────────────► sent      [terminal]
      │  finish_attempt(amb.)  ├───────────────────────────────► ambiguous [terminal]
      │  finish_attempt(dead)  ├───────────────────────────────► dead      [terminal]
      ▼  finish_attempt(retry) ├──► pending
   ┌──────┐                    └──► claim vence sin escribir ──► reclamable
   │ dead │◄────┐
   └──────┘     │   ┌──────────────────────────────────────────────────────┐
                └───┤ claim_for_reconciliation (§I.4)                        │
                    │ (claimed vencido  OR  pending)  +  dispatching abierto │──► ambiguous
                    │ ignora expires_at · JAMÁS envía · única salida: amb.   │    [terminal]
                    └──────────────────────────────────────────────────────┘
```

Terminales: `sent`, `ambiguous`, `dead`. Ninguno con salida automática.

### A.2 Transiciones válidas

| # | Transición | Actor | Predicado obligatorio |
|---|---|---|---|
| T1 | ∅ → `pending` | selector (P1B-3) | ≥1 evento elegible; consentimiento vigente al crear |
| T2 | `pending` → `claimed` | `claim_candidate` | `expires_at > now` **AND** `not_before<=now` **AND** `status='pending'` **AND NOT EXISTS** dispatching. `now` capturado tras el lock. Ganador por `rowcount=1` |
| T3 | `claimed`(vencido) → `claimed` | `claim_candidate` | `claim_until<=now` **AND** `expires_at>now` **AND NOT EXISTS** dispatching |
| T4 | `claimed` → `sent`/`ambiguous`/`dead`/`pending` | `finish_attempt` | attempt `dispatching` del batch **AND** token exacto **AND** `claim_until>now` **AND** (según §J) |
| T5 | `claimed` → `dead` (revalidación) | `authorize_and_begin_dispatch` | dentro de su tx: claim vigente **AND** causa terminal (§E.3). **Sin attempt, sin send** |
| T6 | `claimed` → `pending` (hold) | `authorize_and_begin_dispatch` | ídem + causa de R4. `not_before = now + HOLD_INTERVAL`. **Sin attempt** |
| T7 | `claimed` → `pending` (security) | `authorize_and_begin_dispatch` | ídem + `principal_mismatch`. `not_before = expires_at` (§E.5). **Sin attempt** |
| T8 | `claimed` → `claimed` + attempt `dispatching` | `authorize_and_begin_dispatch` | ídem + autorizado: fija `external_idempotency_key` write-once, `attempt_count++`, INSERT `dispatching`. **Es el punto de linearización** (§B.5) |
| T9 | `pending`/`claimed`(vencido) → `dead`/`expired` | `sweep_expired` | `expires_at<=now` **AND** claim no vigente **AND NOT EXISTS** dispatching |
| T10 | `pending` **o** `claimed`(vencido) con dispatching → `claimed` (reconc.) | `claim_for_reconciliation` | **EXISTS** dispatching **AND** (`status='pending'` **OR** `claim_until<=now`). **Ignora `expires_at`**. Renueva claim; no envía |
| T11 | reconciliación → `ambiguous` | `finish_attempt('ambiguous')` sobre el attempt exacto | claim de reconciliación vigente |

**No hay T de `release` tras un `dispatching`.** El worker nunca ejecuta
`claim.release()` una vez insertado un attempt (INV-W25): sus únicas salidas son
`finish_attempt` o **abandonar el claim** (dejar que venza) para que la
reconciliación lo tome.

### A.3 Attempt

```
authorize_and_begin_dispatch (claim vigente)   finish_attempt (claim vigente)
        │                                                 │
        ▼                                                 ▼
  ┌─────────────┐                                   ┌──────────┐
  │ dispatching │──────────────────────────────────►│ finished │ [terminal]
  │ outcome=NULL│                                    │ outcome≠∅│
  └─────────────┘                                    └──────────┘
        │ proceso muere → HUÉRFANO (siempre AUTORIZADO — §A.4)
        └───────────────────────────────────────────────────► §I: claim_for_reconciliation → ambiguous
```

El `CHECK` de `schema.py:194-197` hace irrepresentable `dispatching` con outcome.

### A.4 Propiedad nueva: un `dispatching` implica autorización completa

Porque `authorize_and_begin_dispatch` es **una** transacción (§B.6), no existe el
estado "attempt insertado pero revalidación a medias" ni "clave puesta sin
attempt". Un `dispatching` abierto es **prueba atómica** de que el batch fue
revalidado, su `external_idempotency_key` fijada, y el worker estaba a punto de
llamar (o llamó) al proveedor. Esto es más fuerte que R1.1, donde `begin_attempt`
era una transacción aparte de la revalidación.

### A.5 Transiciones imposibles

| Prohibida | Qué la impide |
|---|---|
| `pending` → `sent` | Solo `finish_attempt` escribe `sent`, y exige `status='claimed'` + attempt |
| Autorizar tras una revocación confirmada | Linearización: la revalidación vive en la misma tx que el INSERT (§B.5). **INV-W11** |
| `claim_candidate` sobre un batch con dispatching abierto | `NOT EXISTS` en T2/T3. Esos van a reconciliación |
| `claim_for_reconciliation` → `provider.send` | §I.5: la reconciliación no recibe proveedor |
| Batch vencido con dispatching → `dead`/`expired` | `NOT EXISTS` del sweep. **INV-W16** |
| `release` tras insertar dispatching | El worker no lo llama. **INV-W25** |
| Reanudar un huérfano (crear 2.º attempt) | Política única → `ambiguous`. **INV-W28** |
| Dos attempts `dispatching` simultáneos | Un solo claim vigente + `UNIQUE(notification_id, attempt_number)` |

**Regla que gobierna el bloque:** `sent` afirma un hecho externo (hubo
`ProviderResult(sent)` con el claim nuestro); `expired` afirma que **nada** salió
(no hay dispatching que lo contradiga); cualquier duda es `ambiguous`.

---

## B. Ciclo del worker

### B.1 API mínima

```python
@dataclass(frozen=True)
class WorkerResult:
    notification_id: str | None
    outcome: str
    attempt_id: str | None
    provider_message_id: str | None
    retry_at: str | None
    reason: str | None

class NotificationWorker:
    def __init__(self, db_path, *, provider: NotificationProvider,
                 policy: RetryPolicy = DEFAULT_RETRY_POLICY,
                 clock: Callable[[], datetime],
                 security_sink: SecurityEventSink = _NULL_SINK,
                 _fault_hook=None) -> None: ...

    def process_one(self, *, now: datetime) -> WorkerResult: ...
    def process_batch(self, notification_id: str, *, now: datetime) -> WorkerResult: ...
    def drain(self, *, now: datetime, limit: int) -> list[WorkerResult]: ...
    def reconcile_orphans(self, *, now: datetime, limit: int) -> list[WorkerResult]: ...
    def explain(self, notification_id: str, *, now: datetime) -> Explanation: ...
```

`clock` es inyectable y es la fuente de `now` para las operaciones que lo capturan
tras el lock (`claim_candidate`, `claim_for_reconciliation`,
`authorize_and_begin_dispatch`). El `now` de `process_one(*, now)` se usa solo para
las lecturas de listado (candidatos, huérfanos, sweep), que no computan `claim_until`
y donde una foto ligeramente vieja solo hace el filtro **más** estricto.

### B.2 `WorkerResult.outcome`

`sent` · `retry` · `ambiguous` · `dead` · `held` · `security_blocked` · `expired`
· `idle` · `lost`. Los cuatro primeros existen en `radar_delivery_attempts`; los
cinco restantes son resultados del worker, no outcomes de un intento.

### B.3 Orden de `process_one` — **normativo (R1.2)**

```
 FASE 1 — RECONCILIAR TODO HUÉRFANO                              §I.4
   Cualquier batch con un attempt 'dispatching' abierto cuyo claim NO esté
   vigente — sea 'claimed' vencido o 'pending', esté o no expirado el batch:
     for nid in list_orphans(now, limit=RECONCILE_LIMIT):
         token, attempt_id = claim_for_reconciliation(nid)   # captura now tras el lock
         if token:
             finish_attempt(attempt_id, nid, token, outcome='ambiguous',
                            error_class='timeout_after_send')
     # NUNCA se instancia una ProviderRequest en esta fase

 FASE 2 — BARRER EXPIRADOS SEGUROS                              §L
   sweep_expired(now, limit=SWEEP_LIMIT)
     # expires_at<=now AND claim no vigente AND NOT EXISTS dispatching

 FASE 3 — PROCESAR CANDIDATOS DUE                               §B.6
   nid = next(list_candidates(now, limit=1)) or return idle
   → process_batch(nid, now)
```

**El orden 1 → 2 → 3 es correctitud, no preferencia.** FASE 1 reconcilia *todos*
los huérfanos (incluidos los `pending+dispatching`, C-INHERIT). Solo después FASE 2
barre — y como su predicado exige `NOT EXISTS dispatching`, jamás toca lo que FASE 1
todavía no procesó. FASE 3 solo ve batches limpios: `list_candidates` exige
`NOT EXISTS dispatching`, así que ningún huérfano entra al camino normal. La
mutación M26 (invertir 1↔2) muere en T-ORDER.

**Partición limpia:** un batch con dispatching abierto pertenece a FASE 1; uno sin
dispatching, a FASE 2 (si venció) o FASE 3 (si está due). Los predicados son
disjuntos en la dimensión `EXISTS dispatching`, así que ningún batch cae en dos
fases.

### B.4 Dos escrituras, no tres

R1.1 tenía tres transacciones antes de la red: `try_claim`,
`ensure_external_idempotency_key` y `begin_attempt`. R1.2 tiene **dos**:

```
 claim_candidate            ── tx 1: reclama, fija claim_until = (now tras lock)+duration
 authorize_and_begin_dispatch ── tx 2: revalida + external key + attempt_count + INSERT
 provider.send              ── SIN transacción SQLite
 finish_attempt             ── tx 3 (post-red): cierra attempt y transiciona batch
```

Durante la **vida efectiva del claim** (desde que `claim_candidate` fija
`claim_until`) las escrituras bloqueables son exactamente **dos**:
`authorize_and_begin_dispatch` y `finish_attempt`. Esa es la derivación del `2×` de
la fórmula (§D.1), ahora sin el término espurio de la adquisición del claim.

### B.5 Punto de linearización — autorización↔revocación

`authorize_and_begin_dispatch` es **una sola transacción** que incluye la
revalidación **y** el INSERT del attempt. Su COMMIT es el punto de linearización.

**Mecanismo:** SQLite en WAL tiene **un único escritor**. `revoke`
(`endpoint_store.py:270`, un UPDATE) y `authorize_and_begin_dispatch` (`BEGIN
IMMEDIATE` que lee el endpoint y escribe el attempt) se serializan sobre el lock de
escritura. Hay, por tanto, un orden total entre ambos COMMIT:

- **Revocación commitea primero** → cuando `authorize_and_begin_dispatch` adquiere
  el lock y lee el endpoint, ve `revoked_at` puesto → rama terminal `dead`/
  `endpoint_revoked`, **sin attempt, sin `send`**.
- **`authorize_and_begin_dispatch` commitea primero** → leyó el endpoint válido,
  insertó el attempt; el intento queda **autorizado y en vuelo**. Una revocación
  posterior bloquea *intentos futuros* pero **no puede cancelar con certeza** el ya
  autorizado (el efecto puede haber ocurrido — §H).

La garantía honesta, que sustituye a la imposible de R1.1:

> **INV-W11 (reescrito):** Ningún intento nuevo se autoriza después de que la
> revocación haya quedado confirmada. La linearización es el COMMIT de
> `authorize_and_begin_dispatch`.

No se promete "revocar antes del `send` impide el envío": eso exigiría una
transacción abierta durante la red, que INV-W1 prohíbe. Se promete que la decisión
de autorizar y el hecho de la revocación tienen un orden total, y que del lado
"revocación primero" **no hay llamada**.

### B.6 `authorize_and_begin_dispatch` — la transacción

```python
def authorize_and_begin_dispatch(conn, nid, claim_token, *, clock, ext_key
                                 ) -> AuthzOutcome:
    """Una transacción. Todas las ramas terminan en COMMIT o ROLLBACK atómico.
    Ninguna toca la red. `now` se captura tras el lock."""
    conn.execute("BEGIN IMMEDIATE")
    now = clock()                                    # tras el lock
    # 1. claim vigente
    if not claim_still_valid(conn, nid, claim_token, now):
        conn.execute("ROLLBACK"); return AuthzOutcome.LOST
    # 2. revalidación JIT (una lectura, mismo SQL que el selector — §E.1)
    cls, reason = target_authorization(conn, nid, now)
    if cls == TERMINAL:
        _dead(conn, nid, reason); conn.execute("COMMIT"); return AuthzOutcome.dead(reason)
    if cls == HOLD:
        _hold(conn, nid, now + HOLD_INTERVAL); conn.execute("COMMIT"); return AuthzOutcome.held(reason)
    if cls == SECURITY:
        _security_block(conn, nid, expires_at); conn.execute("COMMIT")
        return AuthzOutcome.security_blocked(reason)
    # 3. AUTORIZADO — clave write-once + contador + attempt, en esta misma tx
    conn.execute("UPDATE ... SET external_idempotency_key = COALESCE(external_idempotency_key, :k) WHERE ... fenced")
    attempt_id = _begin_attempt_on_connection(conn, nid, claim_token, now)  # attempt_count++ + INSERT dispatching
    conn.execute("COMMIT")                           # ◄── PUNTO DE LINEARIZACIÓN
    return AuthzOutcome.authorized(attempt_id, ext_key)
```

`ext_key = external_idempotency_key(nid)` (§G) se calcula fuera —es puro— y se
pasa; se **persiste** dentro, write-once, en la misma tx. **No hay una tercera
transacción de clave externa** (M-KEY-SEP la detecta). Las ramas `_dead`/`_hold`/
`_security_block` son UPDATEs fenced (`status='claimed' AND token AND
claim_until>now`) dentro de esta misma tx: absorben lo que R1.1 exponía como
`mark_dead_fenced`/`hold_fenced`/`security_block_fenced`.

### B.7 `process_batch` (cuerpo de FASE 3)

```
1. claim = claim_candidate(nid)                      §D.2
   └─ None → return WorkerResult('lost'|'expired'|'idle')
2. az = authorize_and_begin_dispatch(nid, claim.token, ext_key=external_idempotency_key(nid))
   ├─ LOST            → return 'lost'
   ├─ dead(reason)    → return 'dead' (reason)
   ├─ held(reason)    → return 'held' (reason)
   └─ security(reason)→ security_sink.emit(...); return 'security_blocked' (reason)
3. batch = get_batch(nid); msg = render_payload(json.loads(batch.payload_json))   [puro]
4. result = provider.send(ProviderRequest(..., ext_key, msg.text))   ← CERO TX ABIERTAS
5. decision = classify(result, batch, clock(), policy)               §J/§K
6. finish_attempt(az.attempt_id, nid, claim.token, **decision) → WorkerResult
```

No hay paso de "orphan check" como en R1.1: FASE 1 ya reconcilió todo huérfano, y
`claim_candidate` excluye por predicado los batches con dispatching abierto. Entre 3
y 5 no hay conexión SQLite abierta — INV-W1 por construcción.

---

## C. Búsqueda de candidatos

### C.1 SQL conceptual

```sql
SELECT notification_id, endpoint_id, radar_id, principal_id,
       COALESCE(not_before, created_at) AS effective_at
  FROM radar_notification_batches b
 WHERE expires_at > :now
   AND NOT EXISTS (SELECT 1 FROM radar_delivery_attempts a          -- ◄── R1.2
                    WHERE a.notification_id = b.notification_id AND a.phase='dispatching')
   AND (
        (status='pending'  AND (not_before IS NULL OR not_before<=:now))
     OR (status='claimed'  AND claim_until IS NOT NULL AND claim_until<=:now)
   )
 ORDER BY effective_at ASC, created_at ASC, notification_id ASC
 LIMIT :limit
```

El `NOT EXISTS dispatching` es nuevo: garantiza que FASE 3 nunca toca un huérfano
(esos son de FASE 1). Reproduce el predicado de `claim_candidate`: si propusiera un
candidato que `claim_candidate` rechaza, se gastaría un claim en un batch imposible.
**No es exclusión mutua** — decide `claim_candidate` por `rowcount`.

### C.2 Orden determinista

`effective_at`, `created_at`, `notification_id` (desempate total). Sin el tercero el
orden dependería del plan de SQLite y `drain` no sería reproducible.

### C.3 Fairness — diferida (D-FAIR-1)

Sin cambios respecto de R1.1: se mide en P1B-6 (`drain_principal_skew`); si supera
5:1 sostenido, entra con diseño propio.

---

## D. Claim

### D.1 Fórmula (R2, ratificada — derivación afinada en R1.2)

Durante la vida efectiva del claim hay **dos** escrituras bloqueables (§B.4):

```
claim_candidate ─(fija claim_until)─┬─► authorize_and_begin_dispatch (≤ busy_timeout de lock)
                                    ├─► provider.send               (≤ provider_timeout)
                                    └─► finish_attempt               (≤ busy_timeout de lock)
```

`db.py:23` fija `busy_timeout=30000`. La adquisición del claim en `claim_candidate`
**no** cuenta: su `now` se captura *después* del lock (§D.2), así que su espera es
previa a la vida del claim.

```python
def claim_duration(*, provider_timeout, sqlite_busy_timeout, safety_margin):
    """Función pura. Sin defaults implícitos, sin leer configuración global."""
    return provider_timeout + 2 * sqlite_busy_timeout + safety_margin

def reconciliation_claim_duration(*, sqlite_busy_timeout, safety_margin):
    """La reconciliación no envía: sin término de proveedor."""
    return 2 * sqlite_busy_timeout + safety_margin
```

**Default operacional: 120 s** = `10 s` (provider_timeout Telegram, P1B-5) + `2×30 s`
+ `50 s` (safety_margin: pausas de GC + `httpx` aplica timeout *por fase*).
`reconciliation_claim_duration` → **70 s**.

Tres inputs explícitos; `sqlite_busy_timeout` se **deriva de `db.py`**, no se copia.
Que sea pura permite verificarla sin base de datos (T-CLAIM-PURE) y recalibrarla en
P1B-6 con el p99 medido.

Sustituye la regla `claim_until ≥ 3 × provider_timeout` del addendum §F, que da 30 s
— menos que **una** escritura bloqueable (§T, C1).

### D.2 `claim_candidate` — `now` tras el lock (C-CLAIM)

```python
def claim_candidate(conn, nid, *, clock, duration) -> Claim | None:
    conn.execute("BEGIN IMMEDIATE")          # espera hasta busy_timeout por el lock
    now = clock()                            # ◄── capturado DESPUÉS del lock
    until = now + duration
    cur = conn.execute("""
        UPDATE radar_notification_batches
           SET status='claimed', claim_token=:tok, claim_until=:until
         WHERE notification_id=:nid
           AND expires_at > :now
           AND NOT EXISTS (SELECT 1 FROM radar_delivery_attempts a
                            WHERE a.notification_id=:nid AND a.phase='dispatching')
           AND ( (status='pending' AND (not_before IS NULL OR not_before<=:now))
              OR (status='claimed' AND claim_until IS NOT NULL AND claim_until<=:now) )
    """, ...)
    conn.execute("COMMIT")
    return Claim(tok, until) if cur.rowcount == 1 else None
```

**Por qué importa capturar `now` después.** Si `now` se capturara antes de
`BEGIN IMMEDIATE` y el worker esperara 25 s por el lock bajo contención,
`claim_until = now_viejo + 120 s` daría solo 95 s reales de propiedad — la ventana
se comería a sí misma. Capturándolo tras el lock, `claim_until` mide desde el
instante en que el worker *es* dueño. **INV-W26.** El mutante que mueve la captura
antes del `BEGIN IMMEDIATE` muere en T-CLAIM-LOCK.

`claim_candidate` sustituye a `try_claim` para el worker; `try_claim` (`claim.py`)
permanece para P1B-2. La diferencia —punto de captura de `now` y exclusión de
dispatching— es exactamente lo que hace incorrecto reutilizar `try_claim` aquí.

### D.3 Sin `renew`

La duración del trabajo está acotada por la fórmula. `claim.py:6-8` ya lo declara;
se mantiene.

### D.4 Verificación obligatoria

`NotificationWorker.__init__` falla si
`claim_duration <= provider_timeout + 2 × sqlite_busy_timeout`. Fallo de arranque,
no sorpresa en producción. **INV-W21**, T-CLAIM-GUARD, M23.

### D.5 Claim vencido en cada punto

| Momento | Qué pasa | Resultado |
|---|---|---|
| Antes de `authorize_and_begin_dispatch` | Su check de claim vigente falla → ROLLBACK | `lost`. Sin attempt. Otro worker reclamará. **Cero riesgo** |
| Durante `provider.send` | La llamada termina; `finish_attempt` → `ClaimLostError` | Attempt `dispatching` **huérfano** → FASE 1 de otro → `ambiguous`. **Ventana irreducible** |
| Tras la respuesta, antes de `finish_attempt` | Idéntico — indistinguible | `ambiguous` |
| Otro worker roba el claim | Nuestras escrituras dan `rowcount=0` → abortamos sin escribir | El sucesor ve el huérfano en FASE 1 → `ambiguous` |
| **El batch además expiró** | `claim_candidate` lo rechaza (`expires_at`) | **§I.4** `claim_for_reconciliation` → `ambiguous`. **Nunca `expired`** |

---

## E. Revalidación just-in-time

### E.1 Qué se revalida — y dónde

Exactamente el predicado de `selector._target_skip_reason` (`selector.py:186-207`)
más la expiración, **dentro de `authorize_and_begin_dispatch`** (§B.6), en la misma
transacción que el INSERT del attempt. Ésa es la clave de la linearización (§B.5):
la revalidación y la autorización no pueden separarse.

`preference.enabled=1` · `endpoint.verified_at IS NOT NULL` ·
`external_recipient_id` presente · `endpoint.enabled=1` ·
`endpoint.revoked_at IS NULL` · `principal.status='active'` ·
`radar.archived_at IS NULL` · owner vinculado ·
owner.principal_id == endpoint.principal_id · `batch.expires_at > now`

**Reutilización, no reimplementación** (`endpoint_store.py:21-23`): `_TARGET_SQL` y
`_target_skip_reason` se promueven a `authorization.py` (§P) y **selector y worker
los importan**. INV-W2 estructural.

### E.2 Qué NO se filtra

`radar.enabled`, `needs_attention`, `next_run_after`, `failure_count`,
`permanent_failure_streak`: describen la salud del *scrapeo*, no el consentimiento.
Un evento materializado es un hecho ya ocurrido. **INV-W12.** Ya se cumple por
construcción (`_TARGET_SQL` no los selecciona); M4 lo vigila.

### E.3 Tabla completa de autorización

| Razón | Clase | Resultado | `dead_reason` / `not_before` | Attempt | Llamada |
|---|---|---|---|---|---|
| *(ninguna)* | ok | procede | — | sí | sí |
| `preference_disabled` / `preference_not_found` | terminal | `dead` | `opted_out` | no | **no** |
| `endpoint_revoked` | terminal | `dead` | `endpoint_revoked` | no | **no** |
| `radar_archived` | terminal | `dead` | `radar_archived` | no | **no** |
| `expires_at <= now` | terminal | `dead` | `expired` | no | **no** |
| `endpoint_disabled` | hold | `pending` | `now + HOLD_INTERVAL` | no | **no** |
| `endpoint_unverified` | hold | `pending` | `now + HOLD_INTERVAL` | no | **no** |
| `recipient_missing` | hold | `pending` | `now + HOLD_INTERVAL` | no | **no** |
| `principal_suspended` | hold | `pending` | `now + HOLD_INTERVAL` | no | **no** |
| `owner_not_linked` | hold | `pending` | `now + HOLD_INTERVAL` | no | **no** |
| `principal_mismatch` | **security_blocked** | `pending` | **`= expires_at`** | no | **no** |
| *(post-proveedor)* 400 | terminal | `dead` | `invalid_address` | sí | sí |

Las cinco muertes (R3) son las decisiones irreversibles. `invalid_address` es
**exclusivamente post-proveedor**: un recipient ausente es restituible con
`mark_verified` (`endpoint_store.py:246`), luego es hold, no muerte (T-RECIP).

### E.4 Los cinco holds (R4) — el enum de 5 alcanza

`endpoint_disabled` (pausa reversible, `endpoint_store.py:260`), `endpoint_unverified`
(irrepresentable hoy → hold+anomalía), `recipient_missing` (restituible),
`principal_suspended` (se levanta), `owner_not_linked` (se restituye). Ninguno es
muerte; toda espera termina, si no se resuelve, en `expired` (§L).

Ampliar el `CHECK` exigiría recrear `radar_notification_batches` con dos FK
entrantes, `PRAGMA foreign_keys=OFF`, copia y rename — poner el ledger de auditoría
en riesgo para expresar estados no terminales. No se hace.

La rama de hold (dentro de `authorize_and_begin_dispatch`) escribe `not_before =
now + HOLD_INTERVAL`, **sin** attempt (INV-W17). La razón **no se persiste**: es
derivable con `explain()`; un `hold_reason` persistido sería un fósil que miente
cuando la causa desaparece.

### E.5 `security_blocked` (R5) — salida estructurada + señal segura

`principal_mismatch` = el endpoint es de **otra persona** que la dueña del radar. El
fallo que P1B-1 existió para impedir (`preference_store.py:9-19`).

Rama dentro de `authorize_and_begin_dispatch` (**cero llamada**, ocurre antes de que
exista una `ProviderRequest`):

```python
_security_block(conn, nid, expires_at)   # UPDATE fenced: not_before = expires_at
# tras el COMMIT, en el worker:
security_sink.emit(SecurityEvent(
    kind='principal_mismatch',
    notification_id=nid, endpoint_id=..., radar_id=..., principal_id=...,
    at=now))                              # SIN PII: ids opacos, nunca recipient/address
return WorkerResult(outcome='security_blocked', reason='principal_mismatch')
```

Las cinco propiedades:

1. **Cero `provider.send`.** Rama pre-request. **INV-W22.**
2. **`not_before = expires_at`**, no `HOLD_INTERVAL`: la condición no se resuelve
   sola; 48 reevaluaciones serían un hot loop. Muere `expired` (§L). **M30.**
3. **Salida estructurada:** `WorkerResult(outcome='security_blocked',
   reason='principal_mismatch')` — observable por el caller y por métricas
   (`principal_mismatch_holds`, que **debe ser 0**).
4. **Señal segura inyectable:** `SecurityEventSink` es un `Protocol` con `emit(event)`;
   el default es un sink nulo. No hay tabla de auditoría nueva; el evento no
   contiene PII (solo ids opacos). **Deuda D-SEC-1 (P1B-6):** la señal necesita un
   consumidor con retención — hoy tiene productor y contrato, no destino.
5. **No aborta el drain.** Los demás batches se procesan. **M31.**

La tensión (3/4) vs (5) es deliberada: la severidad se expresa **midiendo y
emitiendo**, no negándose a trabajar.

### E.6 Pre-provider `dead` sin attempt — Opción B, ahora absorbida

Sigue vigente (§B.6): las ramas terminal/hold/security de
`authorize_and_begin_dispatch` **no** crean attempt ni incrementan `attempt_count`
(INV-W17), preservando "attempt ⇔ llamada" (`schema.py:174`). La diferencia con
R1.1 es que ya no son tres métodos públicos separados: son ramas de la única
transacción de autorización.

---

## F. Proveedor

```python
DispatchEvidence = Literal["dispatched", "not_dispatched", "unknown"]

@dataclass(frozen=True)
class ProviderRequest:
    notification_id: str; channel: str; external_recipient_id: str
    external_idempotency_key: str; text: str; payload_version: str

@dataclass(frozen=True)
class ProviderResult:
    outcome: str; evidence: DispatchEvidence
    provider_message_id: str | None; provider_status: str | None
    retry_after: int | None; error_class: str | None; error_message_safe: str | None

class NotificationProvider(Protocol):
    channel: str; timeout: timedelta; supports_strong_idempotency: bool
    def send(self, request: ProviderRequest) -> ProviderResult: ...
```

El proveedor **no lanza excepciones de negocio**: timeout/429/500 son
`ProviderResult`. `evidence` (default `unknown`) es responsabilidad exclusiva del
adaptador (solo él sabe si la petición salió del socket) y decide la taxonomía (§J).
`timeout` vive en el proveedor; `claim_duration` lo lee.

`supports_strong_idempotency` **sigue existiendo** en la interfaz —el FakeProvider lo
implementa para probar la interfaz y la estabilidad de `external_idempotency_key`—
pero **el worker no lo consulta para recuperar huérfanos** (§I, R11). Sin registry en
P1B-4. Solo `FakeProvider` (§M); sin `requests`/`httpx`/Telegram/red.

---

## G. Idempotencia externa (R1)

### G.1 Las dos idempotencias

Interna (`idempotency_key`, `UNIQUE(endpoint_id, idempotency_key)`, la hacemos
cumplir nosotros) vs externa (`external_idempotency_key`, la haría cumplir el
proveedor **si la soportara** — ninguno real lo hace, addendum §E). Confundirlas es
afirmar que no duplicamos cuando lo único probado es que no creamos dos batches.

### G.2 Fórmula

```python
EXTERNAL_IDEMPOTENCY_VERSION = "p1b-extid-v1"
def external_idempotency_key(notification_id: str) -> str:
    return hashlib.sha256(f"{EXTERNAL_IDEMPOTENCY_VERSION}|{notification_id}".encode()).hexdigest()
```

Estable, sin PII, independiente del texto, idéntica en todos los reintentos. Se
**persiste siempre**; se **envía** solo si `supports_strong_idempotency`.

### G.3 Persistencia write-once — dentro de la tx de autorización

```sql
UPDATE radar_notification_batches
   SET external_idempotency_key = COALESCE(external_idempotency_key, :key)
 WHERE notification_id=:nid AND status='claimed' AND claim_token=:token AND claim_until>:now
```

Ejecutado **dentro** de `authorize_and_begin_dispatch` (§B.6), **no** en una
transacción aparte. `COALESCE` la hace inmutable entre intentos (INV-W8). Que viva en
la misma tx que el INSERT del attempt es lo que garantiza que "clave puesta sin
attempt" sea irrepresentable (§A.4, INV-W27). **M-KEY-SEP** (reintroducir una tx
separada) muere en T-KEY-ATOMIC.

### G.4 Constraint

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_batches_external_idem
    ON radar_notification_batches(external_idempotency_key)
 WHERE external_idempotency_key IS NOT NULL;
```

Aditivo e idempotente. Vuelve la unicidad verificable por la base.

### G.5 Desviación documental (C4) — ratificada (R1); pendiente reflejar en el addendum cuando sea editable.

---

## H. Crash matrix

`d` = attempt `dispatching`. Flujo R1.2: `claim_candidate` → `authorize_and_begin_dispatch`
(1 tx) → `send` → `finish_attempt`.

| # | Muere en | Estado persistido | Sucesor | ¿Reintenta? | ¿Amb.? | Duplicado evitado por |
|---|---|---|---|---|---|---|
| 1 | Antes de `claim_candidate` | `pending` intacto | reclama y procesa | Sí, seguro | No | No hubo llamada |
| 2 | Tras `claim_candidate` (claim held, **sin `d`**) | `claimed`, `claim_until` futuro | al vencer, `claim_candidate` lo re-reclama (`NOT EXISTS d`) | Sí, seguro | No | No hay `d`: no es huérfano |
| 3 | **Dentro** de `authorize_and_begin_dispatch`, antes de su COMMIT | **Nada**: ROLLBACK (sin `d`, sin key, sin count) | como 2 | Sí, seguro | No | Atomicidad de la tx única |
| 4 | Tras COMMIT de `authorize_and_begin_dispatch` (**`d` + autorizado**), antes de `send` | `claimed`, `d`, key fijada | **huérfano** → FASE 1 → `ambiguous` | **No** | **Sí** | No se reenvía: se declara |
| 5 | Durante `send` | como 4 | como 4 | **No** | **Sí** | 4 y 5 idénticos en disco |
| 6 | Aceptó, respuesta perdida | como 4 | como 4 | **No** | **Sí** | **El caso que justifica todo** |
| 7 | Respondió `sent`, muere antes de escribir | como 4 | como 4 | **No** | **Sí** | El `message_id` murió con el proceso |
| 8 | Antes de `finish_attempt` | como 4 | como 4 | **No** | **Sí** | — |
| 9 | Dentro de `finish_attempt` (ROLLBACK) | como 4 | como 4 | **No** | **Sí** | Atomicidad de `finish_attempt` (P1B-2) |
| 10 | Tras COMMIT de `finish_attempt` | outcome real, attempt `finished` | terminal | No | No | `claim_candidate` excluye terminales |
| 11 | Claim vence en 1–3 | como la fila (sin `d`) | reclama y repite | Sí, seguro | No | No hay `d` |
| 12 | Claim vence en 4–8, **batch vivo** | huérfano, claim expirado | FASE 1 → `ambiguous` | **No** | **Sí** | §I.4 |
| 13 | Claim vence en 4–8 **y el batch expira** | huérfano + expirado | FASE 1 lo alcanza (ignora `expires_at`) | **No** | **Sí** | **§I.4 · nunca `expired`** |
| 14 | Igual que 13 **y el sweep corre** | sin `NOT EXISTS`: `expired` ← **mentira** | — | — | **Debe ser sí** | `NOT EXISTS` (§L.2) + orden F1→F2 |

**Simplificación frente a R1.1:** desaparecen las filas "antes/después de persistir la
key" y "antes de `begin_attempt`" — porque autorización, clave y attempt son **una**
transacción. La región pre-frontera colapsa a tres estados (1, 2, 3), todos "reclamo
seguro sin huérfano". **La frontera es el COMMIT de `authorize_and_begin_dispatch`**
(fila 3→4): antes, no hay `d` y todo es seguro; después, hay `d` y toda duda es
`ambiguous`. Un `d` abierto implica autorización completa (§A.4), así que el sucesor
nunca tiene que preguntarse si la revalidación llegó a correr.

Filas 8–9 (14–15 en R1.1) no aportan riesgo nuevo: `finish_attempt` es una sola
transacción, ya probada en P1B-2. Fila 14 es la peor del documento (el sistema
afirmando con certeza algo falso); todo §L existe para que no ocurra.

---

## I. Huérfano — política única (R11)

Un `dispatching` abierto cuyo claim no está vigente es un huérfano. Su destino es
**desconocido y permanecerá desconocido** (no hay endpoint de reconciliación en
ningún proveedor, addendum §E.2). **En P1B-4, todo huérfano → `ambiguous`. Sin
excepciones, sin ramas por capacidad del proveedor.**

### I.1 La política

```python
# FASE 1, por cada huérfano:
token, attempt_id = claim_for_reconciliation(nid, clock=clock)   # §I.4
if token:
    finish_attempt(attempt_id, nid, token, outcome='ambiguous',
                   error_class='timeout_after_send',
                   error_message_safe='orphan dispatching reconciled')
```

Batch `ambiguous` **terminal**. Sin reintento. **INV-W7.**

### I.2 Por qué se elimina la reanudación con idempotencia fuerte (R11)

R1.1 diseñaba un camino que, con `supports_strong_idempotency=True`, cerraba el
huérfano y **reintentaba** con la misma clave. R1.2 lo **descarta** del worker:

- el schema define **un attempt por llamada** (`schema.py:174`); reutilizar el
  attempt escondería otra llamada, y crear uno nuevo deja el viejo abierto o exige
  una transición inexistente (`superseded`);
- Telegram **no** ofrece idempotencia fuerte (verificado);
- la complejidad no aporta valor al próximo bloque real.

`reconcile_orphan_attempt` (el desacople attempt↔batch de R1.1) **se elimina** de la
lista de módulos: con política única, la reconciliación es siempre `finish_attempt
(ambiguous)`, que cierra ambos a la vez.

El **FakeProvider mantiene** `supports_strong_idempotency` (§M) para probar la
interfaz y que `external_idempotency_key` es estable entre llamadas — pero el worker
**no la consulta** para huérfanos. T-STRONG-NOCALL prueba que, incluso con el flag en
`True`, la reconciliación no llama al proveedor.

**Deuda futura P1B-X:** reanudar huérfanos con un proveedor idempotente exigiría una
semántica explícita de attempts `superseded`/`reconciled`, o una tabla de
`provider_calls`. **No se amplía el schema para eso en P1B-4.**

### I.3 Seguridad independiente del proveedor

Con política única, la seguridad ya no necesita ni siquiera el argumento "no depende
de I.2": no hay I.2. Todo huérfano es `ambiguous` bajo cualquier proveedor.

### I.4 `claim_for_reconciliation` — alcance exacto (C-INHERIT)

Acepta un huérfano en **dos** formas:

- **A:** `status='claimed'` **AND** `claim_until <= now` **AND** dispatching abierto.
- **B:** `status='pending'` **AND** dispatching abierto (heredado: `finally`
  incorrecto, versión anterior, manipulación manual, o uso indebido de
  `claim.release` — que INV-W25 prohíbe en el worker, pero no en un operador).

Rechaza: `claimed` con claim **vigente**; `sent`; `ambiguous`; `dead`; y cualquier
batch **sin** dispatching abierto (ese es del sweep o de FASE 3).

```python
def claim_for_reconciliation(conn, nid, *, clock, duration) -> tuple[str, str] | None:
    conn.execute("BEGIN IMMEDIATE")
    now = clock()                                   # tras el lock (INV-W26)
    cur = conn.execute("""
        UPDATE radar_notification_batches
           SET status='claimed', claim_token=:tok, claim_until=:until   -- pending→claimed; claimed→renovado
         WHERE notification_id=:nid
           AND ( status='pending'
              OR (status='claimed' AND claim_until IS NOT NULL AND claim_until<=:now) )
           AND EXISTS (SELECT 1 FROM radar_delivery_attempts a
                        WHERE a.notification_id=:nid AND a.phase='dispatching')
        -- SIN predicado sobre expires_at: es la única operación que puede ignorarlo
    """, ...)
    if cur.rowcount != 1:
        conn.execute("ROLLBACK"); return None
    attempt = _open_dispatching(conn, nid)          # el attempt exacto
    conn.execute("COMMIT")
    return (tok, attempt.attempt_id)
```

Propiedades:

- **Toma un claim de reconciliación** (`duration = reconciliation_claim_duration`,
  70 s). Renovar el claim —en vez de escribir `ambiguous` directo— permite usar
  `finish_attempt`, que cierra attempt **y** batch en una transacción ya probada en
  P1B-2. Un `UPDATE SET status='ambiguous'` directo dejaría el `dispatching` abierto
  para siempre.
- **Ignora `expires_at`** (necesario para los huérfanos expirados; inofensivo para
  los vivos).
- **No llama al proveedor** (§I.5).
- **Cierra el attempt exacto** como `ambiguous` y el batch como `ambiguous`.
- **Idempotente:** tras reconciliar, el batch es `ambiguous` (terminal); una segunda
  llamada falla el predicado.
- **Carrera entre dos reconciliadores: exactamente un ganador.** SQLite serializa;
  el `UPDATE` da `rowcount=1` a uno solo; el perdedor ve el estado ya cambiado y su
  `finish_attempt` falla por token/status. T-RECONC-RACE (50 iteraciones).

### I.5 Cómo se garantiza que la reconciliación no envía

Tres capas: **estructural** (`reconcile_orphans` no construye `ProviderRequest` ni
referencia `self._provider`), **de fase** (FASE 1, antes de que exista FASE 3), y **de
prueba** (T-RECONC-NOCALL: `provider.calls == []`; M27 la mata). Es una prohibición
de diseño verificada por test, no un tipo del lenguaje — y se declara como tal.
**INV-W23.**

---

## J. Taxonomía conservadora por evidencia (R8)

### J.1 El principio

> **Sin prueba de que el proveedor NO aceptó el mensaje (`evidence='not_dispatched'`),
> el resultado es `ambiguous`.**

Un 5xx no es prueba de no-entrega: un 502 puede llegar *después* de que el backend
procesó el mensaje. La decisión se toma sobre `evidence`, cuyo default es `unknown`.

### J.2 La tabla

| `outcome` | `evidence` | `error_class` | Decisión | Estado |
|---|---|---|---|---|
| `sent` | `dispatched` | NULL | `sent` + `provider_message_id` | `sent` |
| `rate_limited` | `not_dispatched` | `rate_limited` | `retry`, `not_before=now+retry_after` | `pending` |
| `network_error` | `not_dispatched` | `network_error` | `retry` (backoff) | `pending` |
| `network_error` | `unknown` | `network_error` | **`ambiguous`** | `ambiguous` |
| `provider_error` | `not_dispatched` | `provider_error` | `retry` (solo con prueba explícita) | `pending` |
| `provider_error` | `unknown` | `provider_error` | **`ambiguous`** (default del 5xx) | `ambiguous` |
| `timeout_after_send` | `unknown` | `timeout_after_send` | **`ambiguous`** | `ambiguous` |
| `invalid_address` | `not_dispatched` | `invalid_address` | `dead` | `dead`/`invalid_address` |
| `auth_error` | cualquiera | `auth_error` | **`hold`** / `provider_unavailable` (§J.4) | `pending` |
| Excepción inesperada | — | `timeout_after_send` | **`ambiguous`** | `ambiguous` |

### J.3 `network_error` — mismo `error_class`, dos destinos

`not_dispatched` (connect refused: nada salió) → `retry` seguro. `unknown` (conexión
rota a mitad) → `ambiguous`. El `error_class` es el mismo; **la evidencia decide**.
El adaptador **debe** declarar `unknown` cuando no pueda probar lo contrario. En
P1B-5 esta distinción es **la** condición de GO.

### J.4 `auth_error` — ni `dead` ni `retry`

Un 401 es configuración **nuestra**. Matar batches sería pérdida definitiva para
todos los usuarios a la vez; reintentar martillea con un token roto. → `hold` /
`provider_unavailable`, `not_before=now+HOLD_INTERVAL`, métrica
`provider_unavailable_holds` que **debe ser 0** (T86).

---

## K. Retry policy (R6)

Escalera **1/5/15/60 min**, `MAX_ATTEMPTS=5`. `jitter_fn` inyectable, default 0.
`retry_after` gana por `max(retry_after, backoff[n])` (§K.4). `HOLD_INTERVAL=15 min`
(holds, sin consumir `attempt_count`). `security_blocked` usa `not_before=expires_at`,
no `HOLD_INTERVAL` (§E.5).

**Dos reglas duras con `expires_at`:** (1) `next_not_before >= expires_at` →
`not_before=expires_at`, muere por §L (M15); (2) agotados los intentos →
`not_before=expires_at` → `dead`/`expired`, con el último `error_class` conservado en
el attempt (no existe `dead_reason` para "nos rendimos"; `expired` es literalmente
cierto y `explain()` muestra la causa). **Nunca se reintenta:** `ambiguous`, `sent`,
`dead`, `invalid_address`, ni nada con `evidence='unknown'`.

---

## L. Expirados

### L.1 Problema (§0.1) y trampa (§0.2)

Sin sweep, vencido = `pending` eterno (INV-W10 incumplido). Con sweep ingenuo,
vencido + dispatching = `expired` mentiroso (§H.14). Van juntas.

### L.2 Sweep seguro — SQL conceptual

```sql
UPDATE radar_notification_batches
   SET status='dead', dead_reason='expired', claim_token=NULL, claim_until=NULL
 WHERE expires_at <= :now
   AND ( status='pending'
      OR (status='claimed' AND (claim_until IS NULL OR claim_until<=:now)) )
   AND NOT EXISTS (SELECT 1 FROM radar_delivery_attempts a           -- ◄── INV-W16
                    WHERE a.notification_id = radar_notification_batches.notification_id
                      AND a.phase='dispatching')
```

Atómico, idempotente, sin envío, visible (`expired_before_delivery`). **El `NOT
EXISTS` es la cláusula más importante del documento.** T30, T-CHAIN, M24. Índice
aditivo `idx_batches_expiry(status, expires_at)`.

### L.3 Los dos fencings

1. **Temporal** (`claim_until<=now`): protege el presente (alguien trabaja en él).
   INV-W15, T29, M21.
2. **Por evidencia** (`NOT EXISTS d`): protege el pasado (alguien ya trabajó y no
   sabemos cómo acabó). INV-W16, T30, M24.

Ortogonales: un batch con claim vencido **y** `d` abierto pasa el primero y muere en
el segundo — es decir, **no** muere: va a FASE 1.

### L.4 Ventana claim→send

`claim_candidate` (`expires_at > now`) + revalidación JIT (`expires_at > now` dentro
de `authorize_and_begin_dispatch`) cierran el envío de vencidos. Si expira **durante**
el `send` (ya autorizado), se completa: el efecto ya ocurrió. **INV-W9** se enuncia
con precisión: ningún `provider.send` *se inicia* sobre un batch vencido.

### L.5 Dónde corre

FASE 2 de `process_one`, acotado (`SWEEP_LIMIT`), tras FASE 1, más método público
`sweep_expired`. Sin janitor. **D-SWEEP-1** (Q7): con el scheduler de P1B-6, verificar
que F1 y F2 corren aunque F3 no tenga candidatos.

---

## M. Fake provider

```python
class FakeProvider:
    channel = "telegram"; timeout = timedelta(seconds=10)
    supports_strong_idempotency = False        # el default es el caso real
    def __init__(self, *, script=None, default=SENT, strong_idempotency=False,
                 crash_on_call=None, crash_after_effect=False,
                 latency=timedelta(0), clock=None) -> None: ...
    calls: list[FakeCall]        # FakeCall guarda text_len, NO text
    delivered: list[str]         # oráculo de duplicados: keys realmente "entregadas"
```

Doce comportamientos (success con `evidence='dispatched'`; rate_limited; network_error
`not_dispatched`; network_error `unknown`; invalid_address; auth_error; provider_error
`unknown`; timeout_after_send que **sí** toca `delivered`; respuesta perdida
`crash_after_effect`; **strong idempotency** —misma key, mismo message_id, `delivered`
no crece, para probar la interfaz aunque el worker no la use; sin idempotencia —misma
key, `delivered` crece a 2; latencia lógica con reloj inyectado, **cero `sleep`**;
crash hook). `delivered` es el oráculo: mide **cuántas veces llegó**, no cuántas veces
el estado dice `sent`.

---

## N. Fault injection

`_fault_hook(phase)`, `None` en runtime. Fases nuevas/renombradas para el flujo R1.2:
`after_reconcile`, `after_sweep`, `after_candidate` (tras `claim_candidate`),
`in_authorize_before_commit` (dentro de la tx de autorización), `after_authorize`
(**frontera**: `d` + autorizado + COMMIT), `before_send`, `after_send`,
`before_finish`, y las delegadas a `ledger_store` (`after_attempt_update`,
`after_batch_update`, `before_commit`), `after_commit`.

**`after_authorize` es la frontera.** Antes de ella no hay `d` (reclamo seguro);
después, `d` autorizado (toda duda → `ambiguous`). Se pone donde la evidencia empieza
a existir, no donde nos gustaría.

---

## O. Invariantes

> **Renumeración (R1.2):** el prompt define "el worker nunca hace `release` tras
> `dispatching`" como INV-W17, pero ese número ya está asignado en R1.1 ("rechazo
> pre-proveedor no crea attempt"). Para no romper referencias, ese invariante entra
> como **INV-W25**, y se añaden W26–W28. **INV-W11** se **reescribe** en su slot
> (era el de revocación). Mapa: prompt-"INV-W17" → doc-**INV-W25**.

| ID | Invariante | Cómo se hace cumplir |
|---|---|---|
| INV-W1 | Ninguna red dentro de una transacción SQLite | `authorize_and_begin_dispatch` cierra su tx antes del `send`. Estructural. M11 |
| INV-W2 | Ningún `send` sin revalidación JIT | `authorization.py` compartido; la revalidación vive en la tx de autorización. M2, M3 |
| INV-W3 | Ningún `send` sin attempt `dispatching` commiteado | Orden §B.7. M1 |
| INV-W4 | Ningún `sent` sin attempt `finished`/`sent` | `finish_attempt` única puerta. M12 |
| INV-W5 | Un claim vencido no cierra un attempt | `claim_until>:now` en `finish_attempt`. M6 |
| INV-W6 | Un timeout posterior a posible envío nunca vuelve a `pending` | `timeout_after_send`→`ambiguous`. M7, M20 |
| INV-W7 | Todo huérfano termina `ambiguous` | §I.1. M8 |
| INV-W8 | `external_idempotency_key` no cambia entre intentos | `COALESCE` + UNIQUE parcial. M9 |
| INV-W9 | Ningún `send` **se inicia** sobre un batch vencido | `claim_candidate` + revalidación JIT. §L.4 |
| INV-W10 | Un vencido no permanece `pending` para siempre | `sweep_expired`. **Hoy no se cumple**. M10 |
| **INV-W11** ⭐ | **Ningún intento nuevo se autoriza tras una revocación confirmada; la linearización es el COMMIT de `authorize_and_begin_dispatch`** | §B.5. Revalidación + INSERT en una tx; serialización de escritor único de WAL. **T-LIN-A/B/C/D/E**, M-LIN |
| INV-W12 | `radar.enabled`/`needs_attention` no invalidan una novedad materializada | `_TARGET_SQL` no los selecciona. M4 |
| INV-W13 | Nunca se loguea recipient, dirección, token ni payload | `error_message_safe`; `FakeCall.text_len`; `SecurityEvent` sin PII. M16 |
| INV-W14 | Todo resultado es explicable vía batch+attempt+reason | `explain()`; ningún `dead` sin `dead_reason` |
| INV-W15 | El sweep nunca toca un batch con claim vigente | `claim_until<=:now`. §L.3. M21 |
| INV-W16 ⭐ | Un batch expirado con `dispatching` abierto nunca se marca `expired`; se reconcilia `ambiguous` antes de cualquier sweep | `NOT EXISTS` (§L.2) + F1 antes de F2 + `claim_for_reconciliation`. T30, T-CHAIN, T-ORDER, M24/M25/M26 |
| INV-W17 | Un rechazo pre-proveedor no crea attempt ni incrementa `attempt_count` | Ramas terminal/hold/security de `authorize_and_begin_dispatch` no tocan attempts. M22 |
| INV-W18 | Un hold nunca es terminal | Solo escribe `not_before` |
| INV-W19 | `attempt_count` cuenta llamadas al proveedor, nada más | §E.6. M22 |
| INV-W20 | Dos workers nunca abren dos attempts concurrentes | Un claim vigente + `UNIQUE(notification_id, attempt_number)`. M17 |
| INV-W21 | `claim_duration > provider_timeout + 2×busy_timeout` siempre | Verificación en construcción. §D.4. M23 |
| INV-W22 ⭐ | `security_blocked` nunca llama al proveedor y nunca reevalúa en bucle | Rama pre-request + `not_before=expires_at`. M30 |
| INV-W23 ⭐ | La reconciliación de un huérfano nunca envía | §I.5, tres capas. T-RECONC-NOCALL, M27 |
| INV-W24 ⭐ | Sin `evidence='not_dispatched'`, nunca `retry` | §J. M28, M29 |
| **INV-W25** ⭐ | **Tras insertar un attempt `dispatching`, el worker nunca ejecuta `claim.release()`; sus únicas salidas son `finish_attempt` o abandonar el claim para reconciliación** | El worker no llama `release` en ningún camino post-autorización. **T-NORELEASE**, M-RELEASE |
| **INV-W26** ⭐ | **`claim_until` se calcula con el instante capturado DESPUÉS de `BEGIN IMMEDIATE`** | `claim_candidate` y `claim_for_reconciliation` capturan `now` tras el lock. **T-CLAIM-LOCK**, M-CLOCK |
| **INV-W27** ⭐ | **Revalidación + `external_idempotency_key` write-once + INSERT `dispatching` ocurren en UNA transacción; la clave nunca se persiste en una tx separada** | `authorize_and_begin_dispatch` (§B.6). **T-KEY-ATOMIC**, M-KEY-SEP |
| **INV-W28** ⭐ | **El worker nunca reanuda un huérfano, ni con `supports_strong_idempotency=True`** | Política única (§I). **T-STRONG-NOCALL**, M-RESUME |

---

## P. Módulos para R2

| Módulo | Responsabilidad exacta | No hace |
|---|---|---|
| `authorization.py` | `_TARGET_SQL` + `target_authorization` (ok/terminal/hold/security) promovidos desde `selector.py` | No escribe. No conoce proveedores |
| `provider.py` | `ProviderRequest`, `ProviderResult`, `DispatchEvidence`, Protocol, `external_idempotency_key()`, `SecurityEventSink` (Protocol) | No implementa proveedor. No importa red |
| `fake_provider.py` | `FakeProvider`, `FakeCall`. Determinista, reloj lógico, strong-idem para la interfaz | No toca la base |
| `retry.py` | `RetryPolicy`, `decide()`, `claim_duration()`, `reconciliation_claim_duration()`. **Puras** | No escribe |
| `worker_store.py` | SQL fenced: `claim_candidate`, `authorize_and_begin_dispatch`, `claim_for_reconciliation`, `list_orphans`, `list_candidates`, `sweep_expired`, `metrics`. Reutiliza `_begin_attempt_on_connection` | No llama proveedores. No decide política |
| `worker.py` | Orquestación: `NotificationWorker`, `WorkerResult`, `explain`, las tres fases, inyección de `clock` y `security_sink` | **Ningún SQL propio** |

**Cambios de módulos vs R1.1:** desaparecen `ensure_external_idempotency_key`,
`mark_dead_fenced`, `hold_fenced`, `security_block_fenced` y `reconcile_orphan_attempt`
como operaciones sueltas — **absorbidas** en `authorize_and_begin_dispatch` (las tres
ramas de rechazo) y en `claim_for_reconciliation` (la reconciliación). Un módulo con
menos superficie y una sola transacción de autorización.

**Refactor aditivo en `ledger_store.py`:** extraer `_begin_attempt_on_connection(conn,
nid, token, now, started_at)` (patrón de `outbox_store._create_batch_on_connection`),
que `begin_attempt` (P1B-2, intacto por fuera) y `authorize_and_begin_dispatch`
comparten — una sola definición del INSERT `dispatching` + `attempt_count++`.

Tests: `test_notify_authorization.py`, `test_notify_provider.py`,
`test_notify_fake_provider.py`, `test_notify_retry.py`, `test_notify_worker_store.py`,
`test_notify_worker.py`.

Schema: sin `ALTER TABLE`; dos índices aditivos (`idx_batches_expiry`,
`idx_batches_external_idem`) en `_SCHEMA`.

---

## Q. Matriz de pruebas de R2 (99)

Convenciones: conexiones SQLite **separadas** por hilo, `threading.Barrier`, **cero
`sleep`**, reloj inyectado, proveedor determinista, **50 iteraciones** en carreras.

### `test_notify_retry.py` — política pura (T1–T12) — sin cambios respecto de R1.1.

### `test_notify_fake_provider.py` (T13–T22) — sin cambios; T18/T19 (strong idem) ahora prueban la **interfaz**, no la reanudación.

### `test_notify_worker_store.py` — SQL fenced (T23–T48)

| # | Prueba |
|---|---|
| T23 | `list_candidates` excluye `not_before` futuro |
| T24 | `list_candidates` excluye vencidos y terminales |
| T25 | `list_candidates` incluye `claimed` con claim vencido, batch vivo, **sin `d`** |
| T26 | ⭐ `list_candidates` **excluye** batches con `dispatching` abierto (van a FASE 1) |
| T27 | Orden `effective_at`/`created_at`/`notification_id`: total y reproducible |
| T28 | `sweep_expired` mata `pending` vencido sin `d` |
| T29 | `sweep_expired` mata `claimed` con claim vencido sin `d` |
| T30 | `sweep_expired` **NO** toca `claimed` con claim vigente (INV-W15) |
| T31 | ⭐ `sweep_expired` **NO** toca batch expirado con `d` abierto (INV-W16) |
| T32 | `sweep_expired` idempotente |
| T33 | ⭐ `claim_candidate` fija `claim_until = (now capturado tras el lock) + duration` |
| T34 | ⭐ **`claim_candidate` con `now` capturado tras el lock**: otra conexión retiene el lock, el reloj avanza durante la espera, y `claim_until` refleja el instante posterior (INV-W26) |
| T35 | `claim_candidate` rechaza vencido, terminal, y `d` abierto |
| T36 | ⭐ `authorize_and_begin_dispatch` autorizado: fija key write-once, `attempt_count++`, INSERT `d`, **una** tx |
| T37 | ⭐ `authorize_and_begin_dispatch` terminal/hold/security **no** crea attempt (INV-W17) |
| T38 | ⭐ Crash `in_authorize_before_commit` → ROLLBACK total: sin key, sin `d`, sin `count++` (INV-W27) |
| T39 | ⭐ `external_idempotency_key` write-once: segunda autorización conserva la primera (INV-W8) |
| T40 | ⭐ **No hay una segunda transacción de clave externa**: un solo COMMIT entre claim y send (M-KEY-SEP) |
| T41 | ⭐ `claim_for_reconciliation` recupera **A** (`claimed` vencido + `d`) |
| T42 | ⭐ `claim_for_reconciliation` recupera **B** (`pending` + `d`, heredado) |
| T43 | ⭐ `claim_for_reconciliation` recupera un huérfano **expirado** (ignora `expires_at`) |
| T44 | ⭐ `claim_for_reconciliation` **rechaza** `claimed` con claim vigente |
| T45 | ⭐ `claim_for_reconciliation` **rechaza** batch sin `d`, y estados terminales |
| T46 | ⭐ `claim_for_reconciliation` idempotente: segunda llamada tras `ambiguous` → None |
| T47 | ⭐ **Dos reconciliadores, `Barrier`, 50 iteraciones** → exactamente 1 gana; 1 attempt cerrado; ningún hilo vivo |
| T48 | `metrics(now)` deriva los conteos de §Producto B |

### `test_notify_authorization.py` (T49–T60) — la lógica de `target_authorization`

| # | Prueba |
|---|---|
| T49 | Válido → ok |
| T50 | `preference.enabled=0` / borrada → terminal / `opted_out` |
| T51 | `endpoint.revoked_at` → terminal / `endpoint_revoked` |
| T52 | `radar.archived_at` → terminal / `radar_archived` |
| T53 | `expires_at<=now` → terminal / `expired` |
| T54 | ⭐ `recipient_missing` → **hold** (R4), no `dead` |
| T55 | `principal_suspended` → hold |
| T56 | `endpoint.enabled=0` → hold |
| T57 | `endpoint.verified_at IS NULL` → hold |
| T58 | `owner_not_linked` → hold |
| T59 | ⭐ Endpoint de otro principal → **`security_blocked`** (R5) |
| T60 | `radar.enabled=0`, `needs_attention=1`, `failure_count=99` → autorizado (INV-W12) |

### `test_notify_worker.py` — ciclo (T61–T99)

| # | Prueba |
|---|---|
| T61 | Ciclo feliz → `sent`, `delivered==1` |
| T62 | `process_one` sin candidatos → `idle`, cero llamadas |
| T63 | Crash `after_candidate` (claim, sin `d`) → al vencer, re-reclamo seguro, `delivered==1` (H.2) |
| T64 | Crash `in_authorize_before_commit` → ROLLBACK, sin `d`, re-reclamo seguro (H.3) |
| T65 | Crash `after_authorize` (`d` autorizado) → huérfano → FASE 1 → `ambiguous`, cero llamadas nuevas (H.4, INV-W7) |
| T66 | Crash `before_send` → **mismo estado que T65** → `ambiguous` (H.5) |
| T67 | Crash `after_send` con efecto real → `ambiguous`, `delivered==1`, no se reenvía (H.6) |
| T68 | `timeout_after_send` → `ambiguous`; segundo `process_batch` no lo toca (INV-W6) |
| T69 | `ambiguous` terminal: jamás vuelve a `pending` |
| T70 | ⭐ **`T-LIN-A/B` linearización — revocación gana:** `Barrier` entre worker y `revoke`; si `revoke` commitea primero → `provider.calls == []`, batch `dead`/`endpoint_revoked` |
| T71 | ⭐ **`T-LIN-C/D` linearización — autorización gana:** si `authorize_and_begin_dispatch` commitea primero → **como máximo una** llamada; **nunca dos**; revocación posterior no cancela el intento en vuelo |
| T72 | ⭐ **`T-LIN-E`** 50 iteraciones: el resultado es siempre explicable por quién ganó la linearización; `delivered` ≤ 1 |
| T73 | Endpoint revocado **antes** del ciclo → `dead`/`endpoint_revoked`, cero llamadas |
| T74 | Radar archivado → `dead`/`radar_archived`; payload intacto |
| T75 | ⭐ `T-CLAIM-GUARD` `claim_duration(10,30,50)==120`; `<= pt+2×busy` → error en construcción (INV-W21) |
| T76 | ⭐ `T-CLAIM-PURE` fórmula verificada sin base de datos |
| T77 | Error con token dentro → `error_message_safe` no lo contiene; ningún log con recipient (INV-W13) |
| T78 | Claim vence durante el `send` → `lost` + huérfano; FASE 1 → `ambiguous` |
| T79 | Batch vencido **sin `d`** → sweep → `dead`/`expired`, cero llamadas (INV-W9/W10) |
| T80 | Batch expira entre claim y revalidación → `dead`/`expired` dentro de `authorize_and_begin_dispatch`, cero llamadas (§L.4) |
| T81 | 429 `retry_after=60` → `pending`, `not_before=now+60`; a los 61 s → `sent` |
| T82 | `MAX_ATTEMPTS` agotado → `not_before=expires_at` → sweep → `dead`/`expired`; último attempt conserva `provider_error` |
| T83 | **Dos workers, `Barrier`, 50 iter.**, mismo batch → 1 `sent`, `len(delivered)==1`, 1 attempt |
| T84 | **Dos workers, 50 batches, conexiones separadas** → 50 `sent`, 50 `delivered`, cero duplicados/perdidos |
| T85 | Dos endpoints del mismo principal → dos envíos, recipients distintos, sin cruce |
| T86 | `drain(limit=10)` con 25 candidatos → orden §C.2, reproducible |
| T87 | Fairness: se **registra** el skew (D-FAIR-1) |
| T88 | ⭐ **`T-CHAIN` cadena completa:** proveedor acepta → respuesta perdida → claim vence → batch expira → `process_one` (F1+F2) → **`ambiguous`, JAMÁS `expired`**, `delivered==1`, `dead_reason IS NULL` (INV-W16) |
| T89 | ⭐ **`T-ORDER`** con huérfano expirado presente: F1 corre antes que F2; invertir → `expired` → el test muere (M26) |
| T90 | ⭐ **`T-RECONC-NOCALL`** `reconcile_orphans` → `provider.calls == []` (INV-W23) |
| T91 | ⭐ **`T-STRONG-NOCALL`** huérfano + `supports_strong_idempotency=True` → **`ambiguous`, sin llamada** (el worker no reanuda, INV-W28) |
| T92 | ⭐ **`T-NORELEASE`** el worker nunca ejecuta `claim.release()` tras insertar `d`: una excepción tras `authorize_and_begin_dispatch` **no** devuelve el batch a `pending` (queda `claimed`+`d` → FASE 1) (INV-W25) |
| T93 | ⭐ **`pending+d` heredado** (inyectado como base preexistente) → FASE 1 lo reconcilia → `ambiguous` (C-INHERIT) |
| T94 | ⭐ **Huérfano no expirado** (batch vivo, claim vencido, `d`) → FASE 1 → `ambiguous` |
| T95 | ⭐ `security_blocked`: `WorkerResult(outcome='security_blocked', reason='principal_mismatch')`, cero llamadas, `not_before==expires_at`, `security_sink` recibe el evento **sin PII**, los demás batches del drain se procesan (INV-W22) |
| T96 | ⭐ `recipient_missing` → hold; tras `mark_verified` → `sent` (R4) |
| T97 | ⭐ `provider_error`+`unknown` → `ambiguous` end-to-end, no `retry` (R8) |
| T98 | `auth_error` → `hold`/`provider_unavailable`; token arreglado → `sent` (§J.4) |
| T99 | ⭐ `pending+d` que el sweep **no** mata; `pending_with_open_dispatching` lo cuenta (D-ORPHAN-2) |

**Total: 99** (12 + 10 + 26 + 12 + 39). R1.1 tenía 87.

---

## R. Mutation plan (41)

| # | Mutación | Test que **debe** romper |
|---|---|---|
| M1 | `send` antes de `authorize_and_begin_dispatch` | T65/T66 |
| M2 | Quitar revalidación de `preference.enabled` | T73 |
| M3 | Quitar revalidación de `endpoint.revoked_at` | T73 |
| M4 | `AND r.enabled=1` en la autorización | T60 |
| M5 | Quitar `claim_token` del WHERE de `finish_attempt` | T83 |
| M6 | Ignorar `claim_until` (solo token) | T78 |
| M7 | `timeout_after_send`→`retry` | T68 |
| M8 | Huérfano → `retry` | T65 (`delivered==2`) |
| M9 | Regenerar la key en cada intento (quitar `COALESCE`) | T39 |
| M10 | Quitar `sweep_expired` | T79 |
| M11 | Abrir tx y llamar `send` dentro | T84 + T83 |
| M12 | `sent` sin `ProviderResult` | T61 |
| M13 | `auth_error`→`retry` infinito | T98 |
| M14 | Ignorar `retry_after` | T81 |
| M15 | `next_not_before > expires_at` permitido | (regla §K.6.1) |
| M16 | Recipient en `error_message_safe` | T77 |
| M17 | Dos attempts `dispatching` concurrentes | T83 |
| M18 | Quitar el claim (dos workers envían) | T83/T84 |
| M19 | Reenviar tras crash post-`send` | T67 (`delivered==2`) |
| M20 | `ambiguous`→`pending` permitido | T69 |
| M21 | Sweep sin `claim_until<=now` | T30 |
| M22 | Rechazo pre-proveedor crea attempt | T37 |
| M23 | `claim_duration = pt + busy` (un solo `busy`) | T75 |
| M24 | ⭐ Quitar `NOT EXISTS d` del sweep | T31 y T88 |
| M25 | ⭐ Quitar `claim_for_reconciliation` | T41 y T88 |
| M26 | ⭐ Invertir F2 (sweep) antes de F1 (reconciliar) | T89 |
| M27 | ⭐ Hacer que la reconciliación llame al proveedor | T90 |
| M28 | ⭐ `provider_error`+`unknown`→`retry` | T97 |
| M29 | ⭐ `network_error`+`unknown`→`retry` | (T-net, §J.3) |
| M30 | ⭐ `security_blocked` con `not_before=now+HOLD_INTERVAL` | T95 |
| M31 | ⭐ `security_blocked` aborta el drain | T95 |
| M32 | ⭐ `recipient_missing`→`dead`/`invalid_address` | T96 |
| **M33** | ⭐ **Capturar `now` antes de `BEGIN IMMEDIATE` en `claim_candidate`** (M-CLOCK) | **T34** |
| **M34** | ⭐ **Persistir `external_idempotency_key` en una tx separada** (M-KEY-SEP) | **T40 y T38** |
| **M35** | ⭐ **Separar revalidación del INSERT del attempt** (dos tx) — reabre C-REVOKE | **T70** |
| **M36** | ⭐ **`claim.release()` tras insertar `d`** (M-RELEASE) | **T92** |
| **M37** | ⭐ **Reanudar huérfano con `supports_strong_idempotency`** (M-RESUME) | **T91** |
| **M38** | ⭐ **`claim_candidate` sin `NOT EXISTS d`** (toca huérfanos por el camino normal) | **T26** |
| **M39** | ⭐ **`claim_for_reconciliation` acepta `claimed` con claim vigente** | **T44** |
| **M40** | ⭐ **`claim_for_reconciliation` con predicado sobre `expires_at`** (no recupera expirados) | **T43** |
| **M41** | ⭐ **`security_sink.emit` incluye recipient/address** | **T95 (sin PII)** |

**Total: 41.** R1.1 tenía 32.

---

## S. Cómo rompería este worker si fuera un adversario

Diecisiete ataques. **[ajuste]** = cambió el contrato.

1. **Dos workers, mismo batch, mismo ms.** `claim_candidate` decide por `rowcount`.
   **Resiste** (T83).
2. **Reloj desincronizado entre hosts.** **NO resiste** → **D-CLOCK-1**: P1B-4 asume
   host único. En multi-host, reloj de base o NTP. **[ajuste declarado]**.
3. **SQLite `busy` en `finish_attempt`.** **[ajuste]** → `2×busy_timeout` en la
   fórmula (§D.1); INV-W21, T75.
4. **`busy` en la adquisición del claim consume la ventana.** **NO resistía (C-CLAIM).**
   **[ajuste]** → `claim_candidate` captura `now` tras el lock (§D.2, INV-W26, T34,
   M33).
5. **Revocación entre revalidación y attempt.** **NO resistía (C-REVOKE):** dos tx
   dejaban la ventana. **[ajuste]** → linearización en una sola tx (§B.5, INV-W11,
   T70–T72, M35). La garantía imposible se sustituye por una honesta.
6. **Proveedor lento (90 s / timeout 10 s).** Claim (120 s) aguanta el caso moderado;
   el patológico → huérfano → `ambiguous`. **Resiste** (T78).
7. **Respuesta perdida.** Sin idempotencia externa no hay defensa. **Resiste
   declarando** `ambiguous` (T67).
8. **Batch expirando durante el `send`.** **[ajuste]** → `claim_until<=now` en el
   sweep (§L.3, INV-W15).
9. **Huérfano que nadie reconcilia porque su batch expiró.** El peor bug de R1.
   **[ajuste mayor]** → §0.2, INV-W16, `NOT EXISTS`, `claim_for_reconciliation`, orden
   F1→F2 (T31/T88/T89, M24/M25/M26).
10. **Huérfano `pending+d` (C-INHERIT).** Caía fuera del sweep y de la reconciliación.
    **[ajuste]** → `claim_for_reconciliation` acepta la forma B (§I.4, T42/T93); su
    causa (release tras dispatching) prohibida por INV-W25 (T92); si aun así aparece,
    D-ORPHAN-2 lo cuenta (T99).
11. **Huérfano sobre batch terminal.** **[ajuste]** → `dispatching_open_expired` lo
    cuenta aparte (D-ORPHAN-1): hecho histórico, no basura.
12. **Proveedor con dos `message_id` para la misma key.** Ya **no aplica** al worker:
    política única, no reanuda (§I.2, R11). **Resiste por simplificación.**
13. **`error_class`/`evidence` incorrectos.** **[ajuste]** → decisión sobre `evidence`,
    default `unknown` (§J); en P1B-5 es la condición de GO.
14. **Timeout incompatible con el claim.** Ver 3. **Resiste** (T75, M23).
15. **Operador que altera estados a mano.** **NO resiste** — ningún diseño resiste a
    quien tiene la base. **[ajuste]** → §Producto E.3 enumera lo prohibido; control de
    proceso.
16. **Base restaurada desde backup posterior al efecto.** **NO resiste.** **[ajuste]**
    → **D-RESTORE-1**: runbook que exige marcar `ambiguous` lo posterior al punto de
    restauración.
17. **`security_blocked` en bucle.** **[ajuste]** → R5: `not_before=expires_at`, una
    evaluación (INV-W22, T95, M30).

---

## T. Contradicciones con el contrato documentado

| # | Documento | Dice | R1.2 | Estado |
|---|---|---|---|---|
| C1 | Addendum §F | `claim_until ≥ 3 × provider_timeout` | `pt + 2×busy + margin` = 120 s | Ratificada (R2) |
| C2 | Plan P1B-4 §2 | El huérfano "decide si vale un intento nuevo" | **Política única → `ambiguous`; no decide** (R11) | Resuelta y **simplificada**: R1.1 lo hacía condicional al proveedor; R1.2 lo hace uniforme |
| C3 | Plan P1B-4 §7 | Tras `invalid_address`, endpoint `needs_verification` | El endpoint no se toca (D-ENDPOINT-1) | Resuelta |
| C4 | Addendum INV-ID-4 | `external_idempotency_key` "queda NULL" | Se persiste siempre | Ratificada (R1) |
| C5 | Plan P1B-4 §13 | `max_attempts` agotado → `dead` | Duerme hasta `expires_at` → `dead`/`expired` | Ratificada (R6) |
| C6 | Plan P1B-4 | `providers/fake.py` | `fake_provider.py` | Resuelta |
| C7 | Plan P1B-4 §6 | "500 → `retry`" | 500 → `ambiguous` salvo `not_dispatched` | Ratificada (R8) |
| **C8** | Addendum §F "reconciliación al reclamar de nuevo" | Reconcilia y **puede reintentar** con idempotencia | **No reintenta en P1B-4** (R11); reanudación → **P1B-X** | Nueva: el addendum contemplaba reanudación; R1.2 la difiere explícitamente |

`decisions_log.md` y los `_FINAL` están fuera de alcance. C1/C4/C5/C7 ratificadas por
decisión humana (2026-07-16); C2/C3/C6/C8 son correcciones/simplificaciones
verificables contra el código.

---

## U. Preguntas abiertas / deuda futura

- **Q4 (parcial, R5):** `principal_mismatch` tiene salida estructurada y `security_sink`,
  pero el **consumidor y la retención** de la señal son **D-SEC-1 (P1B-6)**. Sin
  consumidor, la severidad alta es un contador que nadie mira.
- **Q7 (D-SWEEP-1):** ¿quién garantiza que F1 y F2 corran sin candidatos? Cableado de
  P1B-6.
- **P1B-X (nueva, C8):** reanudación de huérfanos con proveedor idempotente —requiere
  semántica `superseded`/`reconciled` o tabla `provider_calls`. **No** se amplía el
  schema en P1B-4.
- **D-CLOCK-1 / D-RESTORE-1:** host único y runbook de restauración (§S).

Ninguna bloquea R2.

---

**Fin del contrato R1.2.** Ningún archivo de `app/` o `tests/` fue creado o
modificado. Documento de trabajo, sin commit.

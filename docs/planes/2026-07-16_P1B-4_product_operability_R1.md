# P1B-4 — Product & Operability (R1.2)

**Fecha:** 2026-07-16 · **Rama:** `radar/mvp-manual` · **HEAD:** `c8c59a9`
**Documento hermano:** `2026-07-16_P1B-4_worker_delivery_contract_R1.md`

El contrato responde *"¿es correcto?"*. Este documento responde *"¿sirve, y podemos
operarlo?"*.

**Cambios sobre R1.1:**
- **Vocabulario honesto de revocación** (§A.6): la promesa literal se sustituye por
  la del punto de linearización.
- **`security_blocked`** gana salida estructurada + señal segura inyectable (§A.4,
  §B, §E, §F).
- **Métrica y operación del huérfano heredado** `pending+dispatching` (§B, §E).
- **Política única de huérfano**: se retira toda promesa de reanudación (§I).

---

## A. Promesa de producto

### A.1 Lo que SECOPPAL puede decir después de P1B-4

El sistema **no envía nada** todavía. Gana la capacidad de decir la verdad sobre lo
que *pasaría*:

| Estado interno | El usuario ve | **NO** se puede decir |
|---|---|---|
| `pending`, due | "Lista para enviar" | "Enviada" |
| `pending`, quiet hours | "Programada para las 7:00" | "Retenida" |
| `pending`, hold | "En espera: tu cuenta está pausada" / "Falta confirmar tu destino" | "Fallida" |
| `pending`, `security_blocked` | *(no se muestra — §A.4)* | — |
| `claimed` | "Enviándose" | "Enviada" |
| `sent` | **"El proveedor la aceptó"** | **"Entregada"** · "Leída" |
| `ambiguous` | **"No sabemos si llegó"** | "Fallida" · "Enviada" |
| `dead`/`opted_out` | "No se envió: retiraste el consentimiento" | "Cancelada por el sistema" |
| `dead`/`endpoint_revoked` | "No se envió: diste de baja ese destino" | — |
| `dead`/`radar_archived` | "No se envió: archivaste este radar" | — |
| `dead`/`invalid_address` | "El destino rechazó el mensaje" | "Tu número no existe" |
| `dead`/`expired` | **"Venció antes de poder enviarse"** | "Fallida" (a veces lo es; a veces fue un proveedor caído) |

### A.2 La palabra prohibida: "entregada"

`sent` = "el proveedor la aceptó" (un 200 de un tercero), no "llegó al teléfono". Es
el techo de lo que se puede afirmar. La diferencia importa: un usuario que confía en
una promesa falsa se pierde una licitación.

### A.3 La segunda afirmación peligrosa: `expired`

`expired` afirma *"nada salió"*. Si se escribiera sobre un batch con `dispatching`
abierto, negaría un envío que pudo ocurrir (contrato §0.2, INV-W16). Por eso ningún
batch con un intento abierto puede decir "venció sin enviarse": dirá "no sabemos si
llegó".

### A.4 `security_blocked` — sin vocabulario de usuario, con señal operativa

`principal_mismatch` = el endpoint es de **otra persona** que la dueña del radar. No
hay mensaje correcto para el usuario, porque no está claro **cuál** usuario:
mostrarle algo a la persona equivocada es exactamente el fallo que el bloqueo
previene. La ruta es **operativa**:

- salida estructurada: `WorkerResult(outcome='security_blocked',
  reason='principal_mismatch')`;
- señal segura vía `SecurityEventSink` inyectable (ids opacos, **sin PII**);
- métrica `principal_mismatch_holds` que **debe ser 0**.

El batch muere `expired` sin explicación en la app. Correcto: la explicación se la
debemos a un humano de guardia, no a la interfaz. **Deuda D-SEC-1 (P1B-6):** la señal
tiene productor y contrato; le falta **consumidor y retención**.

### A.5 La revocación: vocabulario honesto (R1.2)

R1.1 prometía "revocar antes del envío impide el envío". Es **imposible** sin mantener
una transacción abierta durante la red (lo que el diseño prohíbe). R1.2 promete lo
que sí es cierto (contrato §B.5, INV-W11):

| Lo que ocurrió | Lo que el sistema dice |
|---|---|
| Revocaste **antes** de que autorizáramos el intento | "No se envió: retiraste el consentimiento" (`dead`/`opted_out` o `endpoint_revoked`) |
| Revocaste **después** de que autorizáramos el intento | "El intento ya estaba en curso cuando revocaste; puede haberse enviado" (`sent` o `ambiguous`) — **y ningún intento futuro se autoriza** |

La frontera es un instante (el COMMIT de la autorización), no una promesa borrosa. Del
lado "revocaste primero", **no hay llamada**. Del lado "autorizamos primero", no
mentimos afirmando que lo detuvimos.

### A.6 Vocabulario visible vs técnico

| Técnico | Usuario | Operador |
|---|---|---|
| `sent` | "Aceptada por Telegram · 10:42" | `sent` |
| `ambiguous` | "Estado desconocido · revísala en la app" | `ambiguous` (`timeout_after_send` / `unknown` / `reconciliation`) |
| `dead`/`expired` | "Venció sin poder enviarse" | `expired` + `error_class` del último attempt |
| `pending` + hold | "En espera" + causa de `explain()` | `held(principal_suspended)` |
| `pending` + `security_blocked` | *(nada)* | `security_blocked` — **incidente** |

---

## B. Métricas mínimas

Derivables de SQLite + `WorkerResult` + `SecurityEventSink`. Sin dashboards, sin
Prometheus, sin proceso nuevo.

| Métrica | Derivación / nota |
|---|---|
| `pending_due` | `pending`, `expires_at>now`, `not_before<=now`, **sin `d`** |
| `pending_quiet_hours` | `pending`, `not_before` futuro, revalidación OK |
| `pending_held` | `pending`, `not_before` futuro, revalidación en hold |
| `pending_security_blocked` | `pending`, `not_before=expires_at`, `principal_mismatch` |
| `pending_expired` | `pending`, `expires_at<=now` — **debe ser ≈0** |
| `claimed_active` / `claimed_expired` | por `claim_until` vs now |
| `dispatching_open` | attempts `d` cuyo batch **no** es terminal |
| `dispatching_open_expired` | attempts `d` cuyo batch **sí** es terminal → D-ORPHAN-1 |
| `expired_orphans_pending` | expirados con `d` abierto aún no reconciliados → cola de FASE 1 |
| `orphans_reconciled` | attempts cerrados por `claim_for_reconciliation` |
| `pending_with_open_dispatching` | `pending` con `d` abierto → **D-ORPHAN-2, debe ser 0** |
| `sent` / `ambiguous` | `status=...` |
| `ambiguous_by_cause` | `timeout_after_send` vs `unknown` (red/proveedor) vs `reconciliation` |
| `dead_by_reason` | los cinco |
| `retries_per_batch` | distribución de `attempt_count` |
| `age_to_sent_p50/p90` | (`finished_at` sent − `created_at`) |
| `duplicates_prevented` | `create_batch(created=False)` — idempotencia **interna** (B.3) |
| `opt_out_prevented` | `dead_reason='opted_out'` |
| `expired_before_delivery` | `dead_reason='expired'` |
| `provider_outcomes` | `GROUP BY error_class` |
| `provider_unavailable_holds` | `auth_error` holds — **debe ser 0** |
| `principal_mismatch_holds` | **debe ser 0** — incidente de aislamiento |
| `security_events_emitted` | conteo desde `SecurityEventSink` |
| `drain_principal_skew` | máx ÷ media por drain → D-FAIR-1 |

### B.1 Las cuatro que deben ser cero

`pending_expired` · `pending_with_open_dispatching` · `provider_unavailable_holds` ·
`principal_mismatch_holds`. Son alarmas con forma de número: sweep caído; huérfano
heredado varado (D-ORPHAN-2); token roto; **aislamiento entre personas fallado**. La
última, cuyo >0 es un **incidente de seguridad**, ahora tiene además su propio canal
(`SecurityEventSink`) — pero R5 decidió que no aborta el drain, así que solo sirve si
alguien escucha (D-SEC-1).

### B.2 La métrica que no se debe malinterpretar

`duplicates_prevented` es idempotencia **interna** (no dos batches para los mismos
eventos). **No** dice nada sobre duplicados del proveedor. Etiqueta obligatoria:
"batches duplicados evitados (interno)". El único oráculo real de duplicados es
`FakeProvider.delivered` en tests; en producción con Telegram **no existe** — por eso
`ambiguous` es la métrica que importa.

### B.3 La métrica que revela si las correcciones funcionan

`expired_orphans_pending` y `pending_with_open_dispatching` deben **volver a 0 en cada
`process_one`**. Si crecen: FASE 1 no corre (D-SWEEP-1 / Q7), o `claim_for_reconciliation`
rechaza lo que debería recuperar. Y la señal negativa capital: **ningún batch con `d`
abierto puede terminar en `dead_reason='expired'`** — un solo caso es NO-GO (§H.1).

---

## C. Señales de utilidad real

Doce señales que P1B-4 debe producir con FakeProvider antes de conectar Telegram
(idénticas a R1.1): batches shadow, % en ≤5 min, % quiet hours, % truncado, enlaces
faltantes, consentimiento revocado entre selección y entrega, expirados, ambiguos,
duplicados internos, densidad por usuario/radar, riesgo de fatiga, y **oportunidades
reales a tiempo** (muestreo manual de 20 payloads — la única que no se automatiza y la
única que importa: si 18 de 20 no son accionables, el worker es perfecto y el producto
no existe).

### C.1 Umbrales GO/PARTIAL/NO-GO para P1B-5

| Señal | GO | PARTIAL | NO-GO |
|---|---|---|---|
| Duplicados en FakeProvider (sin crash) | 0 | — | ≥1 |
| `ambiguous` reintentado automáticamente | 0 | — | ≥1 |
| Batch con `d` abierto marcado `expired` | 0 | — | ≥1 |
| `pending_expired` tras el sweep | 0 | — | ≥1 |
| `pending_with_open_dispatching` | 0 | — | ≥1 |
| Consentimiento revocado que no bloqueó (revocación previa a la autorización) | 0 | — | ≥1 |
| Linearización revocación↔autorización explicable en 50 iter. | sí | — | no |
| Política de huérfano (vivo, expirado, heredado) | sí | — | no |
| Trazabilidad batch→events→attempt | 100% | — | <100% |
| `age_to_sent_p90` (reloj lógico, sin fallos) | <5 min | 5–30 min | >30 min |
| Cada `dead_reason` explicable | 5/5 | — | <5 |
| Señal 12 (accionables de 20) | ≥12 | 6–11 | ≤5 |
| Enlaces faltantes | <20% | 20–50% | >50% |
| Fatiga | <3/día/usuario | 3–10 | >10 |

Las siete primeras no tienen columna PARTIAL: un duplicado, una mentira o una carrera
"a veces" no es un producto a medias.

---

## D. Fatiga y calidad

Sin cambios de fondo respecto de R1.1. Vectores no cubiertos, con su medida:
**D-FATIGA-1** (fragmentación por `scan_limit`: `batches_per_radar_per_hour>1`),
**D-FATIGA-2** (duplicación cross-radar, arquitectónicamente imposible hoy;
`result_key` totales ÷ únicos por principal/día; fuera de P1B-4), **D-FATIGA-3**
(`changed_value` binario; distribución de `|Δvalue|/prev`), **D-FATIGA-4** (nodos
semánticos, fuera de alcance).

**>10 alertas/día/usuario ⇒ NO-GO.** La fatiga no degrada el producto gradualmente:
lo apaga de golpe y no avisa.

---

## E. Operación manual

### E.1 Las ocho preguntas

Siete se responden con las APIs de P1B-1/2/3 (`explain`, `list_attempts`,
`list_batch_events`, `get_batch`, `revoked_at`). La única nueva es `explain()`.

### E.2 `explain()`

```python
@dataclass(frozen=True)
class Explanation:
    notification_id: str; status: str; dead_reason: str | None
    current_authorization: str      # revalidación AHORA (recalculada, no leída)
    next_eligible_at: str | None
    attempts: list[DeliveryAttempt]
    open_dispatching: bool          # ¿hay un intento abierto ahora?
    event_ids: list[str]; payload_version: str; item_count: int
```

Responde *"¿por qué recibí esto?"* (`event_ids`+payload), *"¿por qué no?"*
(`status`+`dead_reason`+`current_authorization`), *"¿dos veces?"* (`attempts`, uno por
llamada). `open_dispatching` es la bandera que un operador mira antes de concluir nada
sobre un batch vencido: si es `True`, el batch **no puede** decir `expired` (INV-W16),
y si lo dice, hay un bug grave.

### E.3 Acciones manuales prohibidas

| Acción | Por qué |
|---|---|
| `ambiguous`→`pending` sin revisión | Es elegir duplicar (M20 a mano) |
| `dead`→`pending` automático | Envía sin consentimiento |
| Alterar `payload_json` | Es un hecho fechado; editarlo hace mentir al sistema |
| Cambiar `external_idempotency_key` | Rompe INV-W8 |
| Borrar attempts | Destruye la prueba de qué se intentó |
| Cerrar a mano un `dispatching` abierto | Es la evidencia de una ambigüedad (INV-W16) |
| **`UPDATE ... SET status='pending'` sobre un batch con `d` abierto** | Produce D-ORPHAN-2 a mano; lo prohíbe INV-W25 en el código, no en un operador. FASE 1 lo reconcilia si aparece; la métrica lo cuenta |
| **`claim.release()` manual tras un `dispatching`** | Genera `pending+d`; el worker nunca lo hace (INV-W25), un operador podría |
| Restaurar la base sin marcar `ambiguous` | D-RESTORE-1: reenvía todo lo ya enviado |

P1B-4 **no expone API para ninguna**. Control de proceso, declarado como tal.

### E.4 No hay reintento manual (R7)

`release_hold(nid)` **no existe**. Ahorraría ≤15 min sobre un TTL de 12 h y añadiría
una API que un script puede usar mal (forzar en bucle una espera no resuelta — el hot
loop que R5 evita). Si hay prisa, se resuelve la causa (reactivar endpoint, verificar
destino, arreglar token) y el worker hace su trabajo.

La única operación manual que P1B-4 sí expone es **`reconcile_orphans(now, limit)`** —
y no es excepción: no fuerza envíos, solo cierra ambigüedades que ya existen. Es una
operación de integridad.

---

## F. Seguridad y privacidad

### F.1 Logging

**Permitido:** `notification_id`, `endpoint_id`, `radar_id`, `attempt_id`,
`principal_id`, `channel`, `status`, `dead_reason`, `error_class`, `evidence`,
conteos, latencias, `attempt_count`, `provider_message_id` *(solo si no PII)*.

**Prohibido:** `external_recipient_id`, email, teléfono, `address`, `address_last4`,
`address_hash` *(pseudónimo estable: correlaciona)*, `claim_token`, texto renderizado,
`payload_json`, error crudo del proveedor, bot token.

### F.2 `SecurityEventSink` — la señal de aislamiento sin PII

```python
@dataclass(frozen=True)
class SecurityEvent:
    kind: str                    # 'principal_mismatch'
    notification_id: str; endpoint_id: str; radar_id: str; principal_id: str
    at: datetime
    # NUNCA: recipient, address, email, teléfono, token, payload

class SecurityEventSink(Protocol):
    def emit(self, event: SecurityEvent) -> None: ...
```

Inyectable; default nulo. Solo ids opacos: un `endpoint_id` no revela a quién
pertenece sin cruzar la base, y ese cruce es una decisión deliberada del operador, no
un dato en el log. **No hay tabla de auditoría nueva** — el sink es una interfaz; su
implementación con retención es D-SEC-1 (P1B-6). T-SEC (contrato T95, M41) prueba que
el evento no contiene PII.

### F.3 `error_message_safe`

Lo construye el **proveedor** (tiene el error crudo), sanitizando con los valores
`forbidden` **explícitos** de la llamada (recipient, key, token) por sustitución
literal, no regex heurística. El worker nunca ve la respuesta cruda. Contrato T77.

### F.4 El payload no sale

`payload_json` → render → texto al proveedor. Nunca en logs, `WorkerResult` ni
`FakeCall` (que guarda `text_len`). Solo existe completo en memoria durante `send`.

---

## G. Shadow dogfood

### G.1 Diseño

Copia de la base (`sqlite3 .backup`), **nunca** `data/secoppal.db`. FakeProvider con
reloj lógico. Cero red, cero dependencias, Telegram no importado. Muestra: **20
batches reales** (o todos si hay menos; con <10 no informa).

**Escenarios (20):** 12 `SENT`; 2 `rate_limited`; 1 `timeout_after_send`; 1
`provider_error`+`unknown`→`ambiguous` (R8); 1 revocación inyectada **antes** de la
autorización → `dead`/`opted_out`; 1 TTL vencido **sin** `d` → `dead`/`expired`; 1
**endpoint de otro principal** → `security_blocked` (que la señal se emite sin PII); y
**1 cadena completa**: proveedor acepta → respuesta perdida → claim vence → batch
expira → `process_one` → **`ambiguous`, nunca `expired`**.

### G.2 Reporte

```
P1B-4 SHADOW DOGFOOD — <fecha> — FakeProvider, cero red
  batches procesados:              N
  sent / ambiguous / dead:         a / b / c
  ambiguous por causa:             timeout=… unknown=… reconciliation=…
  dead por razón:                  opted_out=… endpoint_revoked=… expired=…
  held:                            d (causas de explain())
  security_blocked:                (conteo) · eventos emitidos sin PII: sí/no
  delivered (oráculo):             == sent   ← si difiere, NO-GO
  duplicados:                      0         ← si >0, NO-GO
  expired con d abierto:           0         ← si >0, NO-GO (INV-W16)
  expired_orphans_pending final:   0
  pending_with_open_dispatching:   0         ← D-ORPHAN-2
  trazabilidad:                    N/N con events+attempts+payload
  privacidad:                      0 ocurrencias de recipient/token en logs ni en SecurityEvent
```

### G.3 Criterios de aceptación

1. `len(delivered) == count(sent)` — **el oráculo cuadra**.
2. Cero duplicados.
3. Revocación inyectada (previa) → `dead`/`opted_out`, cero llamadas.
4. TTL vencido sin `d` → `dead`/`expired`, cero llamadas.
5. `timeout_after_send` → `ambiguous`; segundo `drain` no lo toca.
6. **Cadena completa → `ambiguous`, `delivered==1`, `dead_reason IS NULL`.** Ningún
   batch con `d` abierto acabó `expired`.
7. `security_blocked` → señal emitida sin PII; el drain siguió con los demás.
8. Los 20 responden vía `explain()`.
9. `grep` de recipient/token/payload sobre logs **y** `SecurityEvent`: cero.
10. Reproducible con la misma semilla.

Los criterios 1 y 6 valen por todos: `sent` es lo que creemos, `delivered` lo que
pasó; el dogfood con FakeProvider es **la única oportunidad del proyecto** de comparar
los dos, porque con Telegram nunca podremos.

---

## H. Criterios para P1B-5

### H.1 NO-GO absoluto (uno basta)

1. Duplicado en FakeProvider sin crash.
2. `ambiguous` reintentado automáticamente.
3. Batch con `d` abierto terminó en `dead_reason='expired'` (INV-W16).
4. Batch vencido permanece `pending` tras el sweep.
5. `pending_with_open_dispatching>0` sin explicación (D-ORPHAN-2).
6. Revocación **previa a la autorización** que no bloqueó (INV-W11).
7. Huérfano sin política explícita y testeada — **en sus tres formas**: vivo,
   expirado, heredado (`pending+d`).
8. Trazabilidad incompleta.
9. Latencia no computable.
10. Algún `dead_reason` no explicable vía `explain()`.
11. Logs (o `SecurityEvent`) con recipient/token/payload.
12. `claim_duration <= provider_timeout + 2×busy_timeout` (INV-W21).
13. `security_blocked` llama al proveedor o reevalúa en bucle (INV-W22).
14. Algún `retry` sin `evidence='not_dispatched'` (INV-W24).
15. `claim_until` calculado antes del lock (INV-W26).
16. `external_idempotency_key` persistida en tx separada del attempt (INV-W27).
17. El worker reanuda un huérfano con idempotencia fuerte (INV-W28).
18. Fatiga >10/día/usuario.
19. Señal 12 ≤5 de 20.

### H.2 PARTIAL

Ninguno de los 19, y alguno de: `age_to_sent_p90` 5–30 min; enlaces 20–50%; señal 12
6–11/20; `drain_principal_skew`>5:1; D-FATIGA-1/2/3 sobre umbral; **Q4/D-SEC-1 sin
consumidor** para la señal de `security_blocked`.

**PARTIAL** = P1B-5 puede escribirse (traducción de protocolo); **P1B-6 no activa**
hasta cerrarlas.

### H.3 GO

Los 19 NO-GO ausentes, ninguna PARTIAL. Las decisiones humanas (R1–R11) ya están
ratificadas.

### H.4 Las dos condiciones que P1B-5 hereda

1. **`supports_strong_idempotency=False` para Telegram no es configurable** (hecho
   verificado). El flag solo pasa a `True` con un test contra el proveedor real. Y
   dado R11, ese flag **hoy no cambia el comportamiento del worker** ni siquiera en
   `True` — solo habilita la interfaz que P1B-X usaría.
2. **`evidence` debe ser real** o la taxonomía es decorativa. Si el adaptador no
   distingue `not_dispatched` (nada salió) de `unknown` (respuesta desconocida), el
   sistema duplica creyendo que reintenta. Es **la** condición de GO de P1B-5, con
   default `unknown`.

---

## I. Lo que este bloque **no** demuestra

- No demuestra que Telegram funcione (P1B-5).
- No demuestra que no duplicaremos en producción: demuestra que no duplicamos contra
  un doble que se comporta como esperamos. `ambiguous` existe porque el proveedor real
  puede sorprender.
- No demuestra que las alertas sirvan (señal 12 = 20 payloads, no un estudio).
- No cierra la ventana de duplicación (el efecto ocurre fuera del lock;
  *effectively-once*, ventana acotada y medida).
- **No promete detener una entrega ya autorizada al revocar** (§A.5): promete que
  ningún intento *futuro* se autoriza tras la revocación, y que del lado "revocaste
  primero" no hay llamada.
- No resiste a un operador con acceso a la base (§E.3), ni a relojes desincronizados
  entre hosts (D-CLOCK-1: host único), ni a un backup restaurado en silencio
  (D-RESTORE-1).
- No garantiza que F1 y F2 corran si nadie llama al worker (D-SWEEP-1, Q7).
- **No reanuda huérfanos** aunque el proveedor sea idempotente (R11); esa capacidad es
  deuda futura P1B-X.

Lo que sí demuestra: que cuando diga `sent`, hubo confirmación; que cuando diga
`expired`, ningún intento quedó abierto que lo contradiga; que cuando no sepa, lo
dirá; que la revocación y la autorización tienen un orden total honesto; y que nada se
autoriza sin consentimiento vigente en el instante de la linearización.

---

**Fin del documento de producto R1.2.** Ningún archivo de `app/` o `tests/` fue
creado o modificado. Documento de trabajo, sin commit.

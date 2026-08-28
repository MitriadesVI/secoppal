# ADR-070 — SODA3 `/query` como transporte Socrata productivo por defecto

> **Registro canónico.** Esta decisión está registrada en
> [`docs/decisions_log.md`](../decisions_log.md), índice canónico de ADR del
> repositorio. La autoridad *as-built* son
> `docs/audits/2026-08-28_SODA3_PRODUCTIVE_LIVE_SMOKE.md` y
> `docs/audits/2026-08-28_SODA3_FULL_RUNTIME_DOGFOOD.md`.

**Estado:** Accepted — `SODA3_TRANSPORT_V1_INTEGRATED`
**Fecha:** 2026-08-28
**Rama:** `feature/soda3-transport-v1`, integrada a `main` por fast-forward en `dd51ccd`

```text
DEFAULT_TRANSPORT               = SODA3
CLASSIC_TRANSPORT               = rollback explícito
AUTO_FALLBACK_SODA3_TO_CLASSIC  = NO
SODAPY_RETAINED                 = YES
```

## Contexto

SECOPPAL hablaba con Socrata por un único camino: `GET /resource/{dataset}.json`
a través de `sodapy`, con el timeout horneado en el objeto `Socrata` y una sola
llamada en línea dentro de `SecopClient`. El discovery de `/query` (SODA3) midió
ese camino contra el endpoint moderno y encontró una mejora de rendimiento
sostenida con paridad estricta de filas en los pares comparables.

La migración no era, sin embargo, un cambio de URL. El camino clásico no tiene el
eje `orderingSpecifier`, y el discovery descubrió que imponerlo indiscriminadamente
rompía justo la ruta donde el runtime retira el `ORDER BY` a propósito para
abaratar la consulta —el fallback H4—. Decidir *qué* declararle al backend, y
desde dónde, era el problema real; el transporte HTTP era la parte fácil.

## Decisión

**SODA3 `/query` es el transporte Socrata productivo por defecto.** `classic`
permanece instalado y seleccionable como rollback explícito, no como red de
seguridad automática.

### La selección es explícita y falla cerrado

El transporte se elige con `SECOPPAL_SOCRATA_TRANSPORT`. Ausencia de configuración
resuelve a `soda3`; `classic` activa el camino `sodapy` legacy. **Cualquier otro
valor aborta la construcción del workflow** con un error que nombra el valor
recibido y los válidos: una configuración inválida no selecciona un transporte por
accidente.

La selección vive en `SecopalWorkflow.__init__` y solo ahí. Nada aguas abajo lee
`Settings` para decidir por dónde sale la red. En consecuencia, **cambiar de
transporte exige reconstruir o reiniciar el workflow**: no es un interruptor
caliente y no se relee por turno.

### No hay fallback automático SODA3 → classic

No existe modo shadow ni cascada entre transportes. Un fallback automático
exigiría una política observada y reason codes propios, y este bloque no los
tiene. Son dos transportes seleccionables, no una degradación silenciosa.

### `orderingSpecifier` es intención de ejecución, no lectura del SoQL

La política vigente:

```text
query normal        → total
count               → total
aggregate           → total
fallback H4 query   → no_preserve
fallback H4 count   → total
```

La señal viaja **desde el control flow**, declarada por la ruta que conoce su
propio propósito. Queda prohibido inferirla inspeccionando el texto del SoQL: la
ausencia de `ORDER BY` también aparece de forma legítima en `count`, `aggregate` y
direct lookup, que **no** son H4. Confundir esa ausencia con la de H4 aplicaría la
excepción a rutas que no la pidieron.

El `count` del fallback H4 **no** hereda `no_preserve`: es un eje independiente y
conserva `total`.

### El transporte no toca la consulta

SECOPPAL sigue siendo la autoridad única sobre el SoQL. El transporte lo emite
**literal**: no lo construye, no lo reescribe, no lo normaliza y no lo inspecciona.
La normalización de filas SECOP sigue siendo exclusiva de
`SecopClient._normalize_row`, y leer el entero de un `count` sigue siendo de
`SecopClient.count`. Un transporte hace una cosa: emitir y devolver filas crudas.

SODA3 usa `POST /api/v3/views/{dataset}/query.json` con el payload que el discovery
congeló, sin variantes inventadas del protocolo.

### Un fallo no se disfraza de resultado vacío

Errores HTTP, JSON no decodificable y payload con forma inesperada levantan
excepción tipada con categoría estable. **Ninguno se degrada a `[]`**: eso
convertiría un 400 de SoQL en un «no hay resultados» indistinguible de una
búsqueda legítimamente vacía —exactamente el fallo que ADR-069 prohibió en el
plano semántico, aquí en el plano del transporte.

La distinción importa operativamente: `Soda3TimeoutError` **es** un `TimeoutError`
real, y por eso el workflow lo reconoce como recuperable y arma H4. Un 429, un 400
o un 401 no lo son, y siguen el camino honesto sin fallback. Ninguna categoría
distinta de timeout menciona esa palabra en su mensaje. La política de retry no
cambia: `max_retries=1` sigue significando un intento total.

## Consecuencias y límites

`sodapy` **permanece instalado** (`requirements.txt`): sin él el rollback sería
teórico. Retirarlo no es parte de esta decisión.

El timeout deja de estar horneado en el objeto de transporte y viaja **por
llamada**, lo que permite compartir una instancia SODA3 —y su pool de conexiones—
entre clientes derivados sin que se pisen. El camino clásico conserva su semántica
previa: `with_timeout` abre un `Socrata` propio por llamada, que es lo que el
contrato de deadlines y H4 espera.

La mejora de latencia observada en el discovery es un efecto medido, **no una
promesa contractual** de esta decisión: lo que se congela aquí es la corrección del
transporte y la política de ordering, no un número de p90.

## Fuera de alcance

ADR-070 **no** decide, autoriza ni evalúa:

- `orderingSpecifier=discard`;
- `/export`;
- SODA3 `page`;
- `SEARCH` como recuperación de candidatos;
- `:updated_at` incremental;
- la retirada de `sodapy`;
- el fallback automático entre transportes;
- deterministic TOP-N.

Todos quedan como experimentos o deuda posterior. **Pendiente de evaluación no es
autorizado para runtime.**

## Evidencia de aceptación

| Fase | Resultado | Informe |
|---|---|---|
| Discovery V0 | `GO_SODA3_QUERY` — 16 casos controlados, paridad estricta en 78/78 pares comparables | `docs/audits/2026-08-27_SODA3_DISCOVERY_V0.md` |
| Runtime canary V1 | `NO_GO_DIVERGENCE` — 26 consultas, 83 operaciones, 249 pares; divergencias detectadas | `docs/audits/2026-08-27_SODA3_RUNTIME_CANARY_V1.md` |
| Adjudicación V1.1 | `V1_1_BLOCKERS_CONFIRMED` — divergencias adjudicadas una por una | `docs/audits/2026-08-27_SODA3_V1_1_DIVERGENCE_ADJUDICATION.md` |
| Microprobe H4 | `ORDERING_SPECIFIER_CAUSAL_SIGNAL` — sobre el mismo SoQL, `total` 0/5 contra `no_preserve` 5/5 | `docs/audits/2026-08-27_SODA3_FALLBACK_ORDERING_PROBE.md` |
| Replay H4 | `H4_NO_PRESERVE_POLICY = PASS` — 0 regresiones SODA3-only alcanzables | `docs/audits/2026-08-27_SODA3_H4_POLICY_REPLAY.md` |
| Adjudicación final | `GO_INTEGRATION` — cierra el discovery de `/query` | `docs/audits/2026-08-27_SODA3_QUERY_FINAL_ADJUDICATION.md` |
| Productive live smoke | `PRODUCTIVE_LIVE_SMOKE_GO` | `docs/audits/2026-08-28_SODA3_PRODUCTIVE_LIVE_SMOKE.md` |
| Full runtime dogfood | `SODA3_FULL_RUNTIME_DOGFOOD_GO` | `docs/audits/2026-08-28_SODA3_FULL_RUNTIME_DOGFOOD.md` |

El dogfood es la evidencia decisiva porque ejercitó el producto entero contra
fuente remota, no el transporte aislado: **19 turnos humanos, 41 llamadas remotas**
sobre **ambos datasets** (`jbjy-vk9h`, `p6dx-8zbt`), con búsqueda normal,
follow-ups, paginación, direct lookup, Actor Profile, analytics/observer, H4 y
Radar todos en `PASS`.

```text
HTTP_CONTRACT_ERRORS             = 0
SODA3_ONLY_REPRODUCIBLE_FAILURES = 0
SEMANTIC_REGRESSIONS             = 0
```

Los 16 timeouts registrados son operaciones individuales —primaries que activaron
H4, counts best-effort, un aggregate analítico—, no turnos fallidos: ninguno
escapó como excepción fatal del workflow.

Cutover (`dd51ccd`): default live smoke `PASS`, rollback classic explícito `PASS`,
suite completa **9461 passed, 2 skipped, 24 xfailed, 0 failed**.

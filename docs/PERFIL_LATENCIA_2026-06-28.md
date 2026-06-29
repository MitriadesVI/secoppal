# Perfil de latencia — insumo para Etapa 6 (2026-06-28)

> Generado con la instrumentación de Etapa 5. **Corrida dirigida, exploratoria** — NO
> es todavía un p90 definitivo (con n pequeño, 1-2 timeouts dominan el percentil). Antes
> de FIJAR cifras de la política de latencia: correr los 48 turnos ×2 (asesor).
> Fuente: `artifacts/dogfood/dogfood_profile_dirigido_02.jsonl` (22 turnos, delay 1.5,
> 5 escenarios sintomáticos). La corrida con delay 0.3 (`_01`) se descarta para
> percentiles: el delay agresivo causó 26 `ConnectionError` (throttling), no un fallo
> del pipeline.

## CIERRE Etapa 6 — gate final 48 turnos (2026-06-29, commit `6a3792d`)

Corrida `etapa6_final_01` (fixture `fa6a848b`, delay 1.5) tras cablear el deadline en
direct_lookup. **No queda ningún path conocido sin presupuesto** (confirmado en corrida
completa, no solo smoke).

| criterio del gate | resultado |
|-------------------|-----------|
| p50 estable | **7.9s** (= H2) ✅ |
| p90 < 15s | **11.6s** ✅ |
| max ~deadline | **15.5s** (H2: 63.4s) ✅ |
| turnos >15s = 0 o explicado | **1, explicado** ✅ |
| falsos EMPTY = 0 | **0** ✅ |
| rankings falsos = 0 | **0** ✅ |
| **direct_lookup >15s = 0** | **0** (max 12.1s) ✅ |

**El único turno >15s (15.5s) está explicado:** es un turno con fallback H4 — la query
principal se acota a 8s (cap), el fallback sin ORDER BY añade ~2s y el count (H1) ~3s →
15.5s. El deadline acota cada llamada; un turno con fallback las APILA (primary+fallback+
count). Palanca opcional futura si molesta: omitir el count en el fallback (el total ya es
aproximado en una muestra). No se hizo: 15.5s es marginal y el conteo exacto es útil.

**Inspección de los 3 turnos de referencia (direct_lookup):**
- `leticia_lp005` — 2 probes, **match exacto**, 4.5s → recall de referencias PRESERVADO.
- `la_estrella_samc001` — 9 probes, **`budget_cut` disparó**, honesto "⏱️ en el tiempo
  disponible", 12.1s.
- `ascensores_scope_value` — 12 probes (1 acotada/falló), honesto "una fuente no respondió",
  11.6s.
→ El recorte de latencia NO dañó el recall: la referencia encontrable se halla; las
no-confirmadas se reportan honesto sin colgar 60s.

**Inconclusas sin filas:** 12 → **7** en esta corrida (12→4 en `h4_02`). El recall recuperado
es real pero VARIABLE con la franja de Socrata (5–8 turnos recuperados según la velocidad
de la fuente; cuando Socrata está lenta, también el fallback se corta).

### Decisión de cierre — H3/H5 DEFERRED (no implementar por inercia)

La evidencia no justifica seguir con todas las hipótesis:
- **H3 (dual concurrente) — DEFERRED.** H4 recuperó la mayoría (8/12 en la mejor franja);
  H3 solo tocaría los pocos duales. Beneficio/costo no justificado hoy.
- **H5 (narrador fallback) — DEFERRED.** El narrador no aparece como ofensor; el flujo
  normal quedó en p90 11–12s. Reabrir solo si la telemetría lo señala.

**Etapa 6 CERRADA:** H1 ✅ H2 ✅ H2.1 ✅ H4 ✅ + direct_lookup acotado ✅. p90/max colapsados
(60s→11.6s/15.5s), 0 falsos EMPTY, 0 rankings falsos, recall de referencias intacto.

---

## H4 (fallback degradado sin ORDER BY) — validación 48 turnos (2026-06-29)

Corridas `dogfood_profile_h4_01` (`3a2b3ad`) y `_h4_02` (`be8fb37`, fixture `fa6a848b`,
delay 1.5). Diagnóstico H4.0: los 12 timeouts comparten `ORDER BY` compuesto + `LIKE`, pero
esa forma está también en el 100% de los éxitos → el diferenciador es el universo, no la
sintaxis. H4 = UN fallback sin ORDER BY (LIMIT≤50), gate semántico, reparto 8/3/4.

| métrica | H2 | **H4 (h4_02)** |
|---------|----|----|
| total p50 | 7.9s | 8.0s |
| total p90 | 11.3s | **11.6s** |
| inconclusas sin filas (error/fetch_timeout) | 12 | **4** |
| success/results_found | 15 | **21** |
| falsos EMPTY | 0 | **0** |
| rankings falsos sobre muestra | — | **0** |
| fallback ante orden semántico | — | **0** |

Fallbacks: intentados 5, exitosos 3 (todos `substantive_filters_preserved`, WHERE idéntico
a la principal). **max=67s NO es H4**: es un turno `direct_lookup` (12 variantes de
referencia en serie, una de 52s) que NO pasa por el deadline — fuga preexistente (H2 igual
a 63s), fuera del alcance de H4. Tarea aparte: cablear el deadline en direct_lookup.

---

## H2.1 — cierre: seguridad concurrente + desglose de los 14 timeouts

**Bloqueo concurrente (RESUELTO).** `get_workflow()` es `@lru_cache` (singleton → un
`SecopClient` compartido) y FastAPI sirve con `run_in_executor` (ThreadPool) → turnos
**concurrentes** sobre la misma instancia. La mutación in-place del timeout
(`temporary_timeout`) tenía race real. Sustituida por `SecopClient.derived_for_timeout`:
cliente **derivado por llamada**, no muta el compartido (en prod Socrata propio por
turno; preserva mocks de instancia devolviendo `self`; reutiliza fakes inyectados).
Test concurrente: dos turnos con budgets 3s y 11s no se alteran entre sí ni al original.

**Desglose de los 14 timeouts (utilidad de la respuesta):**

| dimensión | valor |
|-----------|-------|
| inconclusas SIN filas (ERROR/FETCH_TIMEOUT) | **12** |
| parciales CON filas | 0 |
| falsos EMPTY | 0 |
| otros (None/None) | 2 |
| simples vs duales | **10 simples / 4 duales** |
| procesos vs contratos | 5 procesos / 7 contratos (2 s/d) |

**Lectura para producto:** el costo de H2 es real — 12/48 (~25%) quedan **inconclusos
sin ninguna fila** (no "parcial con 25 resultados"). No es falso EMPTY (honesto), pero es
recall perdido: queries que antes esperaban 30-60s y *a veces* completaban, ahora se
cortan a ~11s. Recuperación:
- **10 son simples** → la mayor palanca es **H4** (recuperación degradada: si la consulta
  precisa falla pronto y queda presupuesto, intentar una variante más barata, marcada).
- **4 son duales** → **H3** (concurrencia) los ayuda: ejecutar procesos+contratos en
  paralelo dentro del deadline en vez de gastar el presupuesto secuencialmente.

→ Predominan las **simples** (10 vs 4): la prioridad de recall es **H4**, no H3.

---

## H2 (deadline total) — validación antes/después

Corrida `profile_h2_01` (commit `3dd647d`) cierra la cadena de optimización:

| métrica | Run 1 | Run 2 | H1 | **H2** |
|---------|-------|-------|----|--------|
| total p50 (s) | 7.6 | 7.4 | 6.4 | 7.5 |
| total p90 (s) | 60.3 | 45.1 | 13.1 | **11.3** |
| total p95 (s) | 80.8 | 60.0 | 20.9 | **11.3** |
| **total MAX (s)** | 120.6 | 70.5 | 60.4 | **11.3** |
| **turnos >15s** | 15 | 12 | 3 | **0** |
| falsos EMPTY | 0 | 0 | 0 | **0** |

H2 `count_status`: skipped 31 · timed_out 11 · exact 6. Nuevos: 8 turnos con
`deadline_exhausted`/`fetch skipped due to budget` (los que antes bloqueaban 60s).

**Criterios de aceptación (todos cumplidos):**
- **Ningún fetch permanece bloqueado cerca de 60s** — total MAX 60.4s → **11.3s**.
- **Cola acotada por el deadline + margen** — p90/p95/max colapsan a ~11s (< 15s).
- **turnos >15s: 3 → 0.**
- **p50 no empeora sensiblemente** (7.5s, ~igual a Run2 7.4s).
- **0 falsos EMPTY**; los timeouts se representan como inconcluso/parcial.

**Trade-off honesto (a vigilar):** `query timeouts` subió de ~7 a 14. NO es una
regresión de bloqueo — es la conducta deseada: queries que antes esperaban 30-60s (y a
veces completaban) ahora se **cortan a ~11s** y devuelven respuesta honesta inconclusa
en vez de bloquear. El costo es que ~14/48 quedan inconclusos rápido en lugar de
completar lento. H3 (concurrencia dual) y un eventual ajuste fino de la reserva (4s)
pueden recuperar parte de ese recall sin reintroducir la cola de 60s.

---

## H1 (count no bloqueante) — validación antes/después

Corrida `profile_h1_01` (commit `bea06ff`, 48 turnos, delay 1.5) vs Run 1/2 (pre-H1,
`676aa35`). **H1 confirmado:**

| métrica | Run 1 | Run 2 | **H1** |
|---------|-------|-------|--------|
| count >10s | 10 | 11 | **0** |
| count max | 60.0s | 39.4s | **4.0s** |
| **post_fetch_blocking p90** | 28.1s | 22.8s | **6.0s** |
| post_fetch_blocking max | 62.6s | 39.4s | **6.6s** |
| **turnos >15s** | 15 | 12 | **3** |
| total p50 | 7.6s | 7.4s | 6.4s |
| **total p90** | 60.3s | 45.1s | **13.1s** |
| total max | 120.6s | 70.5s | 60.4s |
| falsos EMPTY | 0 | 0 | **0** |

`count_status` H1: skipped 26 · exact 10 · **timed_out 12** (los que antes bloqueaban
22-60s, ahora cortados al presupuesto de 3s y devueltos con filas + total aproximado).

**Lectura:**
- **H1 cumplió su hipótesis**: `count >10s` 10-11 → **0**; `post_fetch_blocking` p90
  28s → **6s**; turnos >15s ~25-31% → **6%**; `total p90` 45-60s → **13.1s** (ya bajo el
  objetivo de 15s) — **solo con H1, sin tocar deadline/fetch/dual/narrator**.
- **Sin regresión de recall**: 0 falsos EMPTY (Etapa 4 lo garantiza).
- **El `total max` residual (60.4s) NO es count** sino un `query/fetch` que aún hace
  timeout de 60s → es trabajo de **H2 (deadline total + fetch acotado)**, no de H1.

---

## Run 1 vs Run 2 — qué es ESTRUCTURAL y qué es de la franja

Dos corridas de 48 turnos, mismo código (`676aa35`) y fixture (`986aef58070c`), ~7h de
diferencia (Run1 18:54 UTC, Run2 01:49 UTC). Esto es lo que el asesor buscaba: separar el
defecto estructural del ruido de Socrata.

| métrica | Run 1 | Run 2 | lectura |
|---------|-------|-------|---------|
| **count >10s** | 10/19 | 11/? | **ESTRUCTURAL** — el count lento no depende de la franja |
| turnos >15s | 15 | 12 | ESTRUCTURAL (~25-31%) |
| total p50 | 7.6s | 7.4s | ESTRUCTURAL (muy estable) |
| post_fetch_blocking p90 | 28.1s | 22.8s | **ESTRUCTURAL** (~23-28s ganables) |
| zero_after_refinement | 3 | 3 | ESTRUCTURAL |
| query timeout | 7 | 3 | variable (franja) |
| count timeout | 2 | 0 | variable |
| degradada | 7 (15%) | 3 (6%) | variable |
| contratos p90 | 60.0s | 4.9s | **MUY variable** (Run1 degradado, Run2 sano) |
| contratos timeouts | 6 | 1 | muy variable |
| dual p90 | 120.3s | 8.7s | **MUY variable** (sigue al dataset degradado de la franja) |
| total p90 | 60.3s | 45.1s | variable (cola de fuente lenta) |

**Conclusión:**
- **El `count` lento es un defecto estructural** (10-11 counts >10s en AMBAS) → **H1
  (count fuera de la ruta crítica) es la decisión más sólida**, válida en cualquier franja.
- **`post_fetch_blocking` ~23-28s p90 en ambas** → el premio de H1 es real y estable.
- **Los timeouts de fuente y el dual son MUY variables** (contratos 60s↔5s, dual 120s↔9s):
  **no se puede fijar el deadline mirando una sola corrida** — exactamente por esto se
  pidieron dos. Pero AMBAS tienen ~25-31% de turnos >15s → **el deadline total (H2) hace
  falta en cualquier franja**.
- **`total p50` ~7.5s estable** → la mayoría de turnos ya responde bien; el daño está en
  la cola (count + fuente lenta), no en el caso típico.

**Cifras fijadas (con evidencia de las 2 corridas):** deadline total **15s** (la cohorte
sana de Run1 cabe en <12.8s; 15s la captura y corta la cola); `count` no bloqueante
(filas primero, count solo con presupuesto restante); fetch acotado bien por debajo de
60s. Margen para bajar a 12s tras validar H1-H5.

---

## Corrida completa Run 1 (48 turnos · `profile_full_01` · commit 676aa35 · delay 1.5)

**Evidencia principal** (el perfil dirigido de abajo fue exploratorio). Metadata en
`artifacts/dogfood/profile_full_01.meta.json`. Pendiente Run 2 (otra franja horaria,
mismo commit) para variabilidad.

**Cohortes de 3 niveles:**

| cohorte | turnos | total p50 | p90 | max |
|---------|--------|-----------|-----|-----|
| 1 · SANA (≤15s, sin fallo) | 31/48 (**65%**) | 4.1s | 10.4s | 12.8s |
| 2 · LENTA-COMPLETA (>15s, respondió) | 10/48 (**21%**) | 40.5s | 80.8s | 80.8s |
| 3 · DEGRADADA (timeout/connErr) | 7/48 (**15%**) | 60.3s | 120.6s | 120.6s |

→ **65% sano; 35% problemático** (lento o fallo). `turnos>15s: 15/48 (31%)`.

**`post_fetch_blocking` (count + narrator) — el premio de Etapa 6:**
`p50=1.7s · p90=28.1s · max=62.6s`. En el 10% peor, el usuario espera **28s después de
tener filas**. Sacar `count` de la ruta crítica (H1) recupera eso.

| operación | n | p50 | p90 | max | timeouts |
|-----------|---|-----|-----|-----|----------|
| **count** | 19 | **13.5s** | 60.0s | 60.0s | 2 |
| query | 76 | 1.7s | 58.8s | 60.3s | 7 |

`count>10s: 10/19` (más de la mitad). Mesetas: `60s=10`, 45s=1, 30s=2, 15s=2 → el
timeout de 60s confirmado a escala. Reparto: HTTP domina (~100%; pipeline propio ~10%).

**Variabilidad confirmada (por qué hace falta Run 2):** aquí **contratos (jbjy) tuvo más
timeouts (6) que procesos (3)** — al revés que en el dirigido. La franja horaria mueve
qué dataset sufre; no fijar cifras sobre una sola tanda.

Outcomes: `success 26 · empty/zero 10 · None/None 4 · error/fetch_timeout 4 ·
zero_after_refinement 3 · partial/count_timeout 1`.

---

## (Exploratorio) Perfil dirigido — por operación HTTP

| operación | n | p50 | p90 | max | timeouts |
|-----------|---|-----|-----|-----|----------|
| **count** | 8 | **22.1s** | 60.0s | 60.0s | 1 |
| query | 22 | 2.97s | 43.6s | 60.0s | 2 |

## Por dataset

| dataset | n | p50 | p90 | timeouts |
|---------|---|-----|-----|----------|
| procesos (p6dx) | 26 | 3.3s | 60.0s | 3 |
| contratos (jbjy) | 4 | 4.2s | 6.7s | 0 |

## Dos cohortes (no descartar los fallos — son comportamiento de producto)

| cohorte | turnos | total p50 | p90 | max |
|---------|--------|-----------|-----|-----|
| **A — fuente respondió** | 19 | 4.8s | 30.2s | 75.0s |
| **B — fuente degradada** | 3 (14%) | 60.0s (hasta error honesto) | — | 66.4s |

## Narrator vs pipeline determinista (cohorte sana)

| | p50 | p90 | max |
|---|-----|-----|-----|
| narrator_ms (LLM) | 0.00s | 3.16s | 3.27s |
| determinista_ms (parse/resolve/build/observe/rank/format) | 0.03s | 4.56s | 4.69s |

## Por escenario

| escenario | n | total p50 | p90 | fallos |
|-----------|---|-----------|-----|--------|
| air_conditioning_supply | 3 | **60.0s** | 75.0s | 2 |
| adulto_mayor_pagination | 4 | **30.2s** | 66.4s | 1 |
| dual_adulto_mayor_cartagena | 5 | 11.4s | 13.8s | 0 |
| adulto_mayor_now | 3 | 4.9s | 29.2s | 0 |
| ascensores_scope_value | 7 | 2.2s | 9.4s | 0 |

## Outcomes · mesetas · reparto

```
success/results_found: 12   empty/zero_results: 4   error/fetch_timeout: 2
None/None: 2   empty/zero_after_refinement: 1   partial/count_timeout: 1
mesetas (±2s): 45s=1, 60s=3  (cero en 30/45 → llamadas únicas, NO retries acumulados)
reparto: HTTP=90%  pipeline_propio=11%  (total 421s/22 turnos)
```

## Hallazgos defendibles (evidencia, no sospecha)

1. **El `count` es el mayor ofensor EVITABLE**: p50=22s, hasta 60s, y a menudo
   `status=completed` (no es un fallo, *bloquea*). Cuando ya hay filas, esperar 22s por
   un número exacto es mala transacción de producto.
2. **Los timeouts son llamadas ÚNICAS de 60s** (mesetas solo en 60s, ninguna en 30/45):
   no hay retries acumulados → un `timeout=60s` que hay que **acotar**, no un rediseño
   de retries.
3. **El daño se concentra en `procesos`** (3 timeouts; contratos sano) y en dos
   escenarios: `air_conditioning_supply` (queries lentas) y `adulto_mayor_pagination`
   (count en "más").
4. **`dual` cuesta ~2×** (p50 11.4s vs single ~3-5s) → probable ejecución **secuencial**
   de los dos hijos.
5. **Cuando la fuente responde, el sistema es razonable** (cohorte A p50 4.8s); el daño
   lo hacen count lento + timeouts puntuales, NO el pipeline propio (~4.5s determinista
   + 3s narrator, medido pero no el cuello).
6. **Cuando falla, el usuario espera 60s** hasta el error honesto (cohorte B) → acortar.

## Decisiones candidatas para Etapa 6 (NO implementar aún; confirmar con 48×2)

- **Deadline total por turno** (~15s, cifra a fijar con los 48), no un timeout por llamada.
- **`count` no bloqueante**: responder con filas primero; count solo con presupuesto
  restante; `PARTIAL/COUNT_TIMEOUT` con total aproximado; NUNCA cero por count fallido.
- **`fetch` acotado**: deadline de lectura mucho menor a 60s; degradación honesta.
- **`dual`**: concurrencia controlada o presupuestos hijos dentro del mismo deadline.
- **narrator**: presupuesto propio o fallback determinista en fuente degradada / "más".

> Regla invariante para Etapa 6 (ya soportada por el modelo de outcome de Etapa 4):
> filas + count timeout → PARTIAL/COUNT_TIMEOUT (mostrar filas); fetch timeout sin filas
> → ERROR/FETCH_TIMEOUT (honesto); una fuente responde y otra falla → PARTIAL/
> partial_source_failure (conservar lo útil).

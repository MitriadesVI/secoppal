# Perfil de latencia — insumo para Etapa 6 (2026-06-28)

> Generado con la instrumentación de Etapa 5. **Corrida dirigida, exploratoria** — NO
> es todavía un p90 definitivo (con n pequeño, 1-2 timeouts dominan el percentil). Antes
> de FIJAR cifras de la política de latencia: correr los 48 turnos ×2 (asesor).
> Fuente: `artifacts/dogfood/dogfood_profile_dirigido_02.jsonl` (22 turnos, delay 1.5,
> 5 escenarios sintomáticos). La corrida con delay 0.3 (`_01`) se descarta para
> percentiles: el delay agresivo causó 26 `ConnectionError` (throttling), no un fallo
> del pipeline.

## Por operación HTTP

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

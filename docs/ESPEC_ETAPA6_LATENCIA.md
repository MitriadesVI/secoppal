# Etapa 6 — Política de latencia (especificación preliminar)

> **Estado: DISEÑO, no implementado.** Las cifras concretas (¿10/12/15 s?) se fijan
> tras la corrida de 48 turnos ×2 (en franjas horarias distintas, sin cambios de código
> entre ambas — réplica válida). Insumo: `docs/PERFIL_LATENCIA_2026-06-28.md`.
> Commit base de medición: `676aa35`.

## Qué demostró el perfil (no especulación)

- `count` está en la ruta crítica y **no debería**: p50 22s, a menudo `completed`
  (bloquea sin fallar).
- Los timeouts son **llamadas únicas de 60s** (mesetas solo en 60s) → una sola llamada
  puede secuestrar el turno un minuto.
- `dual` paga **ejecución secuencial** (~2× single).
- HTTP domina; el pipeline propio no es el culpable principal **pero no es gratis**.

## Aclaración de medición (evitar doble conteo)

`pipeline_self_ms = total_ms − http_wall_ms`. El narrator (`advisor_response_ms`) es
trabajo NO-HTTP → **está incluido en `pipeline_self_ms`**. Por tanto:
```
determinista_ms = pipeline_self_ms − advisor_response_ms   (parse/resolve/build/observe/rank/format)
```
**NO sumar `pipeline_self` + `narrator`** (uno contiene al otro). En el perfil dirigido:
determinista p90 ≈ 4.56s, narrator p90 ≈ 3.16s, y narrator ⊂ pipeline_self.

## Cohortes de 3 niveles (no 2)

"Respondió" ≠ "se comportó sano". El análisis debe separar:
```
1. Fuente SANA           : ninguna operación excede el presupuesto.
2. Fuente LENTA-COMPLETA : respondió, pero alguna op (típicamente count) tardó > presupuesto.
3. Fuente DEGRADADA      : timeout / connection_error / unavailable.
```
La cohorte "sana" del perfil dirigido (p50 4.8s pero p90 30s, max 75s) escondía
count lentos `completed` → eran cohorte 2, no 1.

## Métrica clave a añadir al reporte

```
tiempo_hasta_primeras_filas   (parse+resolve+build+fetch)   ── lo que el usuario espera para VER algo
tiempo_hasta_respuesta_final  (total)                        ── lo que espera para el mensaje completo
post_fetch_blocking ≈ count_ms + advisor_response_ms (+observe si corre después del fetch)
```
`post_fetch_blocking` define **exactamente cuánto se puede ganar en Etapa 6**: si las
filas están en 4s pero la respuesta tarda 24s por count+narrator, ahí está el premio.

## Políticas (diseño)

1. **Deadline total por turno** — presupuesto único compartido, NO un timeout por llamada.
   No se reinicia el reloj con cada operación/retry.
2. **Fetch primero** — las filas tienen prioridad sobre count exacto, observer y narración.
3. **Count oportunista** — solo si queda presupuesto; si no, `PARTIAL/COUNT_TIMEOUT` con
   total aproximado. NUNCA cero por count fallido.
4. **Fuente degradada rápida y honesta** — jamás esperar 60s para informar que SECOP no
   respondió.
5. **Dual con hijos coordinados** — presupuesto compartido, ejecución concurrente o
   coordinada, conservar el resultado del hijo rápido, no bloquear por el lento
   (`PARTIAL/partial_source_failure`). Telemetría con ContextVar segura bajo concurrencia
   (token ya soporta anidación; validar con hilos).
6. **Fallback determinista del narrator** — si queda poco presupuesto, respuesta
   determinista breve en vez de exceder el límite (y "más"/paginación no re-narra todo).

## Invariantes (ya soportadas por el modelo de outcome de Etapa 4)

```
filas + count timeout         → PARTIAL / COUNT_TIMEOUT          → mostrar filas
fetch timeout sin filas       → ERROR / FETCH_TIMEOUT            → respuesta honesta
una fuente responde, otra no  → PARTIAL / partial_source_failure → conservar lo útil
```

## Hipótesis verificables (criterio de aceptación medible)

| # | Hipótesis | Cómo se verifica |
|---|-----------|------------------|
| **H1** | `count` sale de la ruta crítica | `post_fetch_blocking` p90 baja drásticamente; 0 turnos esperando >X s por count; `PARTIAL/COUNT_TIMEOUT` cuando aplica |
| **H2** | El deadline total elimina las esperas de 60s | 0 turnos con `total > deadline`; mesetas de 60s desaparecen |
| **H3** | `dual` concurrente reduce ~½ su latencia | dual p50 baja de ~11s a ~6s; resultado parcial si un hijo falla |
| **H4** | Las respuestas parciales conservan recall | filas mostradas en PARTIAL == filas que se mostrarían sin el corte; sin falsos EMPTY |
| **H5** | El fallback determinista mantiene el turno bajo presupuesto | en fuente degradada/poco presupuesto, `total ≤ deadline` y respuesta honesta |

Gate de no-regresión: las invariantes de outcome y los xfail de regresión siguen verdes;
recall conocido (referencias) no empeora.

## Cifras a fijar tras la corrida 48×2

Candidato de trabajo (NO definitivo): deadline total 15s · fetch inicial 8-10s · count
≤2-3s del presupuesto restante · narrator presupuesto propio o fallback. Se ajustan con
los percentiles reales de las dos corridas (Run1 ahora, Run2 otra franja horaria).

# H4.0 — Taxonomía de los timeouts (microfase de diagnóstico previa a H4)

> Gate del asesor: *"Antes de implementar H4, una microfase de diagnóstico sobre las
> consultas que hacen timeout. Sin eso, H4 puede convertirse en una colección de parches
> generales cuando quizá ocho de los diez comparten el mismo patrón."*
>
> Fuente: `artifacts/dogfood/dogfood_profile_h2_01.jsonl` (48 turnos, commit `3dd647d`,
> el mismo run que cerró H2). Universo analizado: los **12 turnos `error/fetch_timeout`**
> (inconclusos sin filas) — el objetivo declarado de H4.

## TL;DR — el hallazgo desmonta la hipótesis de partida

1. **La forma de la query NO es el diferenciador.** `ORDER BY` compuesto (`fecha DESC,
   valor/precio DESC`) + predicado `LIKE` están en el **100% de los turnos** — incluidos
   los **15 éxitos**, que completan con esa misma forma en **1.5–4.7s**. No hay una familia
   de queries "malas por construcción"; la misma query a veces completa en 2s y a veces
   se corta a los 11s.
2. **El diferenciador es el universo + variabilidad del servidor**, no la sintaxis. Cuando
   el filtro `LIKE` + territorio acota poco (tema amplio, sin departamento, o `LIMIT 200`),
   el `ORDER BY` obliga a Socrata a **ordenar todo el conjunto filtrado** antes de devolver
   las primeras 50 filas → la query supera el deadline.
3. **La maquinaria de degradación que YA existe está estructuralmente hambreada por el
   deadline de H2.** En los 12 timeouts `degraded=False`: las dos rutas de fallback de
   `execute_query` se ejecutan pero su `_query_first_then_count` cae en `fb <
   MIN_USEFUL_FETCH` (presupuesto 0, porque la query principal ya gastó ~11s) → devuelven
   vacío → `if results:` falso → timeout honesto.

→ **H4 no es "construir un fallback nuevo".** Es: (a) **generalizar** el retry-sin-ORDER-BY
que ya existe pero está sobre-restringido, y (b) **reservar presupuesto** para que ese
fallback pueda correr dentro de los 15s. Eso preserva 100% de la intención (solo cambia el
*orden*, que el asesor lista explícitamente como degradable) sin tocar filtros sustantivos.

## Taxonomía de los 12 (todos comparten una sola familia raíz)

`fetch_status`: 4 `timed_out` (la query principal se cortó) + 8 `skipped` (la query
principal se cortó a ~11s **y** un segundo fetch del turno —dual/refinamiento— quedó sin
presupuesto). En **los 12** hay exactamente **una** op HTTP `query` `attempt=1` que falla a
~11s. `degraded=False` y `risk_flag_timeout=True` en los 12.

| # | escenario | turno | dataset | dual | LIMIT | filtros sustantivos | fetch_status |
|---|-----------|-------|---------|------|-------|---------------------|--------------|
| 1 | air_conditioning_supply | suministro+instalación | procesos | no | **200** | — | timed_out |
| 2 | air_conditioning_supply | excluye mantenimiento | procesos | no | **200** | (exclusión) | timed_out |
| 3 | air_conditioning_supply | incluye si tiene sumin. | procesos | no | **200** | (exclusión) | timed_out |
| 4 | la_estrella_samc001 | mantenimiento ascensores | procesos | no | 50 | estado | skipped |
| 5 | pae_santander_facets | operación PAE Santander | contratos | no | 50 | fecha | skipped |
| 6 | pae_santander_facets | solo interventorías | contratos | no | 50 | fecha | skipped |
| 7 | cartagena_long_conv | vuelve a contratos | contratos | no | 50 | — | timed_out |
| 8 | cartagena_long_conv | solo los activos | contratos | no | 50 | estado | skipped |
| 9 | dual_adulto_mayor | procesos y contratos | contratos | **sí** | 50 | ciudad+fecha | skipped |
| 10 | dual_adulto_mayor | ordena por mayor valor | contratos | **sí** | 50 | ciudad+fecha | skipped |
| 11 | dual_adulto_mayor | más procesos | procesos | no | 50 | ciudad | skipped |
| 12 | dual_adulto_mayor | solo contratos activos | contratos | no | 50 | ciudad | skipped |

**Familias (la microtaxonomía que pidió el asesor):**

| familia | n | descripción | qué la causa |
|---------|---|-------------|--------------|
| **ORDER_BY_COSTLY sobre universo amplio** | **12/12** | `ORDER BY fecha DESC, valor DESC` fuerza ordenar todo el conjunto `LIKE`-filtrado | universo grande + sort server-side |
| ↳ subcaso WIDE_LIMIT | 3 | además `LIMIT 200` (opportunity_search) amplía el material a ordenar | 200 filas a materializar+ordenar |
| ↳ subcaso BROAD_NO_TERRITORY | 4 | tema amplio sin departamento/ciudad → universo enorme | filtro poco selectivo |
| ↳ subcaso SECONDARY_STARVED | 8 | el turno hace un 2º fetch (dual/refinamiento) ya sin presupuesto | deadline gastado por la 1ª query |

`TEXT_PREDICATE_COSTLY`, `TOO_MANY_OR_TERMS`, `NON_INDEXED_FILTER`, `WIDE_SELECT`,
`RETRY_EXHAUSTED` → **0 casos puros** (la forma está en éxitos también; no discrimina).
`DATASET_VARIABILITY` → es el factor de fondo, no accionable por reescritura.

## Lo que ya existe y por qué no salva estos turnos

`execute_query` (`app/core/workflow/actions/execution.py`) ya tiene en su `except`:

1. **Retry sin ordering** (líneas 122–150): **es exactamente la palanca correcta** —pero
   está gateada a `ordering_signal == "valor_desc" AND not fecha_desde`. La mayoría de los
   12 no cumple ese gate (orden compuesto por defecto, o con `fecha_desde`).
2. **`_relax_params`** (líneas 152–175): la palanca **prohibida** por el asesor — quita
   `estado → fecha → valor → territorio` (cambia la pregunta). Además requiere `if results:`.

Ambas re-ejecutan vía `_query_first_then_count`, que con la query principal ya consumida
(~11s de 15s) calcula `fetch_budget < MIN_USEFUL_FETCH` → **omite** la llamada → vacío →
ningún fallback "prende". De ahí `degraded=False` y `fetch_status=skipped` en los 8.

## Diseño H4 que se deriva (a aprobar antes de codificar)

**Bisturí, no martillo** — un único fallback controlado, intención sustantiva intacta:

```
deadline total: 15s  (sin cambios)

primary fetch  → ACOTADO a PRIMARY_BUDGET (≈7–8s, ver decisión abajo)
  si responde  → resultado normal
  si timeout y quedan ≥ (reserve + MIN_FALLBACK):
      fallback ÚNICO = misma query SIN ORDER BY costoso, LIMIT≤50
                       (todos los filtros sustantivos intactos)
      ordenar la página localmente por el criterio pedido
      si responde → outcome = PARTIAL/DEGRADED_ORDERING, registrar transformación
      si falla     → outcome = ERROR/FETCH_TIMEOUT (honesto, como hoy)
  si timeout y NO queda presupuesto → ERROR/FETCH_TIMEOUT (honesto)

máximo UN fallback por dataset. Sin cascada A→B→C→D.
```

**Transformaciones permitidas (solo estas):** `removed_order_by`, `reduced_limit`
(200→50), `local_sort_applied`. **Nunca**: quitar entidad, territorio, fechas, valor,
estado, exclusiones, dataset, proveedor, referencia.

**Cambios concretos:**
- Generalizar la ruta (1) de `execute_query`: disparar el retry-sin-ORDER-BY en
  **cualquier** `_is_timeout_exception`, no solo el gate `valor_desc`. Ordenar localmente.
- Sacar `_relax_params` de la ruta automática de timeout (o restringirlo a ORDER_BY/LIMIT,
  jamás filtros sustantivos).
- Subcaso WIDE_LIMIT: el fallback usa `LIMIT 50`, no 200.

**Telemetría H4 (la que pidió el asesor):** `fallback_attempted`, `fallback_reason`,
`fallback_strategy`, `fallback_budget_ms`, `fallback_elapsed_ms`, `fallback_rows`,
`primary_query_status`, `fallback_query_status`, `substantive_filters_preserved` (bool),
y lista `transformations_applied`.

## La ÚNICA decisión de diseño que requiere tu visto bueno

**Hay que reasignar el presupuesto, no solo añadir un fallback.** Hoy la query principal
puede consumir `remaining − reserve ≈ 11s` de los 15s → al hacer timeout **quedan 0s** para
cualquier fallback. Por eso los 8 "skipped" nunca recuperan. Para que H4 sea viable:

- **Acotar la query principal a `PRIMARY_BUDGET ≈ 7–8s`** (no 11s), reservando ~4s para un
  fallback sin ORDER BY (los éxitos hacen 1.5–2s → cabe de sobra) dentro de los mismos 15s.

Esto toca una cifra "fijada con evidencia" (el reparto interno del deadline de 15s, **no**
los 15s en sí). El trade-off: las queries que hoy *a veces* completan entre 8–11s pasarían
a cortarse antes y depender del fallback. Dado que el fallback sin ORDER BY es más barato
que la principal, la apuesta es **recuperar más recall del que se pierde**. Es exactamente
la reasignación que el diagnóstico justifica, pero la marco explícita porque cambia un
número que fijamos con las 2 corridas.

**Criterio de éxito H4 (medible en la próxima corrida):**
- inconclusas sin filas: **12 → objetivo ≤ 4**
- fallbacks con filas **relevantes** (filtros sustantivos intactos) = éxito útil
- fallbacks con ruido = regresión semántica (debe ser 0)
- **0 falsos EMPTY**, **max < 15s**, **p50 no empeora**, consultas rápidas intactas.

---

## Estado de implementación (2026-06-29) — H4 codificado, suite verde

Implementado tras la revisión del asesor (8 ajustes aplicados). Suite: **1900 passed**.

**Módulos / cambios:**
- `app/core/order_semantics.py` (puro): gate `order_is_semantic/presentational` sobre
  `ordering_signal`; `sort_page_presentational`; constantes `exact_order/unordered_sample/
  unavailable`.
- `app/core/deadline.py`: reparto **8/3/4** (`PRIMARY_FETCH_SECONDS=8`,
  `FALLBACK_FETCH_SECONDS=3`, reserva 4); `fetch_budget_s(..., cap_s=)`.
- `app/core/soql_builder.py`: `build(..., include_order=False)` para el fallback.
- `app/core/workflow/actions/execution.py`: en `except` ante timeout **recuperable** y
  orden presentacional → UN fallback sin ORDER BY (LIMIT≤50), sort local, marcado degradado.
  `_relax_params` ELIMINADO del camino de timeout.
- `app/core/formatter.py` + `rendering.py`: la respuesta NO afirma orden sobre una muestra
  (`_shown_phrase` → "coincidencias (muestra sin orden garantizado)") + nota explícita.

**Refinamientos sobre el diseño inicial (feedback del asesor):**
1. **Gate semántico** (el ajuste mayor): `ordering_signal` presente ("más caros"→valor_desc,
   "últimos"→fecha_desc) ⟹ orden SEMÁNTICO ⟹ **no** se degrada (sería responder otra
   pregunta). Ausente ⟹ presentacional ⟹ fallback permitido.
2. **Orden y conteo son ejes independientes**: el fallback NO fuerza `total_count_approximate`;
   si el `count` termina, el total es exacto aunque la muestra no tenga orden garantizado.
3. **`fallback_attempted` ≠ `fallback_used`**: telemetría distingue intentado de exitoso.
4. **Solo errores recuperables** (timeout) disparan el fallback; un 400 de SoQL/esquema/auth
   cae al camino honesto (no se oculta el defecto).
5. **Preservación estructural**: el WHERE del fallback es idéntico al de la principal
   (solo cambian ORDER BY y LIMIT) — verificado byte-a-byte en tests.

**Pendiente de cierre (gate del asesor):** perfil de 48 turnos con H4, medir fallbacks
intentados/exitosos, filas recuperadas, inconclusas restantes, p50/p90/max, 0 falsos EMPTY,
0 afirmaciones falsas de ranking; inspección manual de los turnos que usaron fallback.

**Fuera de alcance de H4 (anotado):** la posible *fuga* de `ordering_signal` entre turnos
("los más caros" → nueva búsqueda) es territorio de la política de mutaciones (Etapa 2). El
gate de H4 lee `ordering_signal` **del turno actual**; si la conversación lo filtrara, el
gate sería sobre-conservador (no degrada cuando podría) — fallo SEGURO, nunca el peligroso
(degradar una consulta semántica). Tests `test_gate_reads_current_turn_only` lo fijan.

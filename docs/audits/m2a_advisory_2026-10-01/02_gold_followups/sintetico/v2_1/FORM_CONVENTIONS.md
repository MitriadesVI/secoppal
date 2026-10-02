# Convenciones de forma de `prior` (slice sintético, v2.1)

Fijadas tal como están en los 164 casos; el esquema `m2a-followup-gold/2` no las define. No cambian respecto de v2.

1. **Orden.** `prior.ir.order = {"operator": "value_desc|value_asc|date_desc|date_asc", "polarity": "POSITIVE"}` o `null`.
2. **Cardinalidad.** `prior.ir.cardinality = {"operator": "top_n", "value_hint": "10"}` o `null`. Sólo aparece junto con `order`.
3. **Escalas de `unit`** (en condiciones VALUE): `MILLONES`, `MIL_MILLONES`, `SMMLV`, `PESOS`, o `null` (toda condición no-VALUE).
   `value_hint` lleva cifra y unidad como las diría el usuario («200 millones», «5 mil millones», «500 SMMLV», «40.000.000 pesos»).
   Una cota inferior previa es `greater_or_equal` y una superior `less_or_equal`.
4. **Ancla dual.** `universe = "DUAL"` más `universes = ["PROCESSES","CONTRACTS"]`, `view_source = "USER"` (casos SF076, SF077, SF148).
5. **Universo y vista.** Universo dicho (`CONTRACTS`/`PROCESSES`): `view_source = "USER"`, `product_view = null`. Universo
   `UNSPECIFIED`: `product_view` calculada con PV1–PV6 y `view_source = "DERIVED"`.
6. **Vista elegida por botón.** `universe = "UNSPECIFIED"`, `product_view` fijada, `view_source = "USER"` y
   `chain = ["PROTOCOL:option"]` (SF081).
7. **`prior.chain`.** Sólo turnos de protocolo entre el previo y el follow-up: `PROTOCOL:reask`, `PROTOCOL:option`, `PROTOCOL:page`.
8. **`prior.state`.**
   - `EXECUTED`: corrió (valor por defecto; con `gap_seconds` ≤ 1800).
   - `PENDING`: hay una aclaración abierta. `prior.pending = {dimension, reason, options, span, candidates}`; `span` es el texto literal
     ambiguo del turno previo y `candidates` la lista de lecturas ofrecidas `{dimension, gloss}` (vacía cuando la pregunta es
     abierta, `options = 0`, p. ej. un periodo sin año).
   - `EXPIRED`: contexto vencido (`gap_seconds` > 1800: 1860, 18000, 46800; un pendiente caducado conserva `pending`, SF064).
   - `NOT_EXECUTED`: el IR se propuso pero el turno no corrió (timeout o bloqueo).
   - `NONE`: no hay búsqueda previa; `owner = "NONE"`, `gap_seconds = null`, IR de cero condiciones.
9. **`prior.ir = null`.** Ancla guardada pero ilegible (no decodificable); `state = "EXECUTED"`, `owner = "FOUNDATION_SEARCH"` (SF142).
10. **Dueño legacy.** `owner = "LEGACY:COUNT"` / `"LEGACY:RANK_SUPPLIERS"`, `ir.operation` = `COUNT` / `RANK_SUPPLIERS`,
    `view_source = null`.
11. **Opción por evento.** El `followup_text` es la etiqueta del botón (SF058, SF059, SF060); no existe un campo de evento.

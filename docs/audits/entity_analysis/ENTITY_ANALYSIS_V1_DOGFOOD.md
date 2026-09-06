# Entity Analysis V1 — dogfood y consistencia

**Fecha:** 2026-09-05. **Transporte:** SODA3. **Warehouse:** no usado.
**Periodo:** 2026. **Overview:** seis llamadas SECOP por intento.

## Método

Se ejecutó el plan productivo directamente contra `p6dx-8zbt` y `jbjy-vk9h`,
sin `Settings()` ni persistencia. No se fijaron cifras esperadas antes de
observar la fuente. Cada scope provino del resolver/BuyerGroup productivo; no
se construyeron predicados `LIKE` ad hoc.

Las latencias son una muestra de dogfood, no un SLA. El mismo intento lanza las
seis llamadas de manera concurrente y registra el wall time del perfil. Se
conservan los primeros timeouts como evidencia; una repetición exitosa no los
reescribe.

## Observaciones

| Scope | Intento | Llamadas | Wall ms | Estado | Procesos | Contratos | Valor contractual COP |
|---|---:|---:|---:|---|---:|---:|---:|
| Alcaldía Baranoa | 1 | 6 | 693,6 | completo | 384 | 378 | 12.510.332.980,00 |
| Barranquilla | 1 | 6 | 6.595,6 | parcial: tres llamadas contractuales timeout | 1.619 | no afirmado | no afirmado |
| Bucaramanga | 1 | 6 | 929,1 | completo | 4.591 | 4.566 | 303.498.261.247,62 |
| Yumbo | 1 | 6 | 6.005,4 | parcial: tres llamadas contractuales timeout | 4.538 | no afirmado | no afirmado |
| Gobernación Atlántico | 1 | 6 | 4.663,8 | completo | 994 | 2.867 | 514.006.073.535,14 |
| Bogotá, BuyerGroup de 38 miembros | 1 | 6 | 1.717,4 | completo | 37.735 | 34.450 | 3.038.582.631.364,19 |
| Barranquilla | 2 | 6 | 535,9 | completo | 1.619 | 10.252 | 1.861.405.892.968,22 |
| Yumbo | 2 | 6 | 557,1 | completo | 4.538 | 4.543 | 185.269.656.102,24 |

Sobre los ocho intentos, `calls_p50 = 6`, `calls_max = 6`, la mediana de wall
time fue 1.323,25 ms y el P95 empírico por nearest-rank fue 6.595,6 ms. Los dos
primeros fallos parciales fueron timeouts de transporte, no ceros; el render
conservó las secciones de procesos y declaró las contractuales no disponibles.

## Consistencia con consultas puntuales

Se reconstruyeron `count` de procesos y `SUM + COUNT` de contratos usando el
mismo `SoQLBuilder`, parámetros resueltos y por tanto el mismo `WHERE` del
perfil. No se compararon universos parecidos.

```text
BARANOA PROFILE (384, 378, 12510332980.0)
        POINT   (384, 378, 12510332980.0) MATCH True CALLS 6

BOGOTA PROFILE (37735, 34450, 3038582631364.19)
       POINT   (37735, 34450, 3038582631364.19) MATCH True CALLS 6 MEMBERS 38
```

El contraste acredita los tres agregados compartidos. No afirma una relación
1:1 entre procesos y contratos y no usa esta coincidencia para validar otros
aspectos.

## Canarios de autoridad

- `analiza Baranoa`: aclaración Buyer-vs-Territory, cero llamadas.
- `analiza la Alcaldía de Baranoa`: BuyerUnit productiva y metadata
  `PRODUCT_RECOGNIZED` de DIVIPOLA 08078.
- Bogotá: membresía exacta tomada de `buyer_groups.json`; no reconstruida.
- candidata territorial no reconocida: no produce scope y no consulta SECOP.

## Limitación histórica y adjudicación de integración

El postprocesador compartido de BuyerGroups en `app/core/orchestrator.py`
antepone a cualquier respuesta la declaración completa de miembros. Para el
canario Bogotá eso añade 38 nombres (1.777 caracteres en la declaración) y
excede el contrato de 5–8 hechos. No se modificó porque está fuera de la
propiedad del carril. El cálculo de BuyerGroup queda acreditado; su presentación
de primer turno quedó `PARTIAL` en el carril fuente. La integración sobre
`fac51c0` conserva la membresía estructurada y omite la declaración textual
completa sólo para `analytical_intent == "entity_profile"`. Las demás rutas
mantienen la declaración existente. El canario de workflow exige 38 miembros,
referencia de grupo intacta, 5–8 hechos y seis llamadas.

## Dogfood post-integración — 2026-09-06

Tres perfiles contra SODA3, ejecutados secuencialmente, más cuatro consultas
puntuales de contraste: 22 requests HTTP en total. No se construyó `Settings()`
ni se escribió persistencia de producto. Las seis llamadas internas de cada
perfil conservan concurrencia bounded y timeout de ocho segundos. No se
repitieron Barranquilla ni la campaña histórica; estas muestras no son un SLA.

| Scope (2026) | Calls / HTTP | Wall ms | Actividad | Valores | Modalidades | Proveedores | Temporal | Recientes |
|---|---:|---:|---|---|---|---|---|---|
| Baranoa | 6 / 6 | 1.119,4 | SUPPORTED | SUPPORTED | SUPPORTED | SUPPORTED | SUPPORTED | SUPPORTED |
| Bogotá, 38 miembros | 6 / 6 | 3.370,8 | SUPPORTED | SUPPORTED | SUPPORTED | SUPPORTED | SUPPORTED | SUPPORTED |
| Barranquilla | 6 / 6 | 8.344,4 | PARTIAL | UNAVAILABLE | SUPPORTED | UNAVAILABLE | PARTIAL | SUPPORTED |

Baranoa y Bogotá muestran siete hechos visibles. Barranquilla conserva 1.632
procesos y los recientes; `contract_values`, `supplier_count` y `supplier_value`
fallaron con `Soda3TimeoutError`. Su `contract_count` y valor contractual son
`null`, nunca cero. Se preservan también los timeouts históricos de Barranquilla
y Yumbo descritos arriba.

| Scope | process_count perfil / puntual | contract_count perfil / puntual | contract_value COP perfil / puntual | Igualdad |
|---|---:|---:|---:|---|
| Baranoa | 386 / 386 | 383 / 383 | 12.543.832.980,00 / 12.543.832.980,00 | EXACTA |
| Bogotá | 37.960 / 37.960 | 34.546 / 34.546 | 3.044.068.665.270,19 / 3.044.068.665.270,19 | EXACTA |

Cada contraste reutiliza parámetros productivos y compara la igualdad literal
del `WHERE` con el de `source_calls`. Los conteos se comparan como enteros y los
valores como `Decimal`, sin tolerancia ni universos aproximados. Las cifras
live son observaciones nuevas y no sustituyen las de 2026-09-05.

Recibos, SoQL literal, estados por llamada y respuestas:
[`integration/dogfood.json`](integration/dogfood.json).

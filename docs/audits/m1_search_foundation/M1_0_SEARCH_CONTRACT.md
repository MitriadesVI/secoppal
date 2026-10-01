# M1.0 — Contrato SEARCH congelado (v1)

Fecha: 2026-09-30. Base: `main` @ `4ec99bc1` (Foundation V3 + M0 integrados).
Misión: `M1_SEARCH_FOUNDATION_V1 — PHASE A`. Rama `mission/m1-search-foundation-phase-a`.
Roadmap: `docs/planes/2026-09-30_FOUNDATION_SINGLE_INTERPRETER_ROADMAP.md` §M1.

Este documento fija **el lenguaje** de M1 antes de medir nada: qué significa SEARCH, con qué
vocabulario se etiqueta el Gold, qué operadores existen y qué decisiones de producto quedan
cerradas. Es la autoridad compartida de los carriles A1–A5. Un carril que encuentre un caso que
este contrato no cubre **no inventa**: lo escribe en su cola `CONTRACT_GAPS` y sigue.

No implementa nada. No cambia el prompt congelado (`a5867d40…`), ni el host, ni el runtime.

## 1. Principio rector

**El Gold etiqueta significado, no rutas.** Cada caso dice qué operación pide el usuario, sobre
qué universo y con qué condiciones. Qué capacidad lo ejecuta se **deriva** del significado con la
tabla §6, que tiene una columna de transición para M1. Así el Gold no envejece cuando M3 cambie
quién ejecuta qué, y una regresión se mide contra significado, nunca contra lo que hace legacy.

Legacy nunca es verdad. «LEGACY ✓» significa «tiene código para esto», no «acierta».

## 2. Operaciones (cerradas)

| `operation` | Significado | ¿M1 SEARCH? |
|---|---|---|
| `SEARCH` | listar registros (procesos y/o contratos) que cumplen condiciones | **sí** |
| `COUNT` | cuántos registros | no (vecino: Contract Activity TOTAL) |
| `SUM_VALUE` | suma de valores | no (vecino: Contract Activity) |
| `GROUP_BY` | desglose por una dimensión («por modalidad», «por año») — lleva `aspect` | no (vecino: Contract Activity) |
| `RANK_SUPPLIERS` | principales proveedores/contratistas de un comprador o tema | no (vecino: Suppliers A2) |
| `PROFILE_BUYER` | perfil/dossier de una entidad compradora | no (vecino: Entity Profile) |
| `PROFILE_ACTOR` | perfil/huella de un proveedor/contratista | no (vecino: Actor Profile) |
| `FOLLOW` | seguir/alertar/radar | no (vecino: Radar) |
| `EXPORT` | exportar/descargar resultados | no |
| `PROTOCOL` | reset, paginación («ver más»), ID SECOP exacto, selección de opción, saludo | no (capa 1, determinista) |
| `FOLLOWUP_DELTA` | fragmento que sólo tiene sentido sobre el turno anterior («y en 2025?») | no (M2) |
| `OUT_OF_DOMAIN` | no es contratación pública | no |
| `MULTI` | varias operaciones distintas en un turno → lista `jobs[]` | parcial: los jobs SEARCH sí |

`aspect` (sólo con `GROUP_BY`/`COUNT`/`SUM_VALUE`): `TOTAL`, `BY_MODALITY`, `BY_YEAR`,
`BY_BUYER`, `BY_SUPPLIER`, `BY_STATE`, `BY_TYPE`, `OTHER:<etiqueta>`.

Regla de frontera: la operación la decide **lo que el usuario quiere recibir**, no las palabras
que aparecen. «contratos por contratación directa» → `SEARCH` + MODALITY. «desglosa los contratos
por modalidad» → `GROUP_BY·BY_MODALITY`. «proveedores de aseo» (lista de contratos de aseo
mirados por proveedor) es ambiguo: ver §8 cola de producto.

## 3. Universos y vistas de SEARCH

| Clase | `universe` | `view` | Representación futura |
|---|---|---|---|
| procesos | `PROCESSES` | `RECORDS` | SEARCH · PROCESSES |
| contratos | `CONTRACTS` | `RECORDS` | SEARCH · CONTRACTS |
| procesos y contratos | dos requests | `RECORDS` ×2 | 2 requests `SHARES_SCOPE` |
| oportunidad / intención de participar | `PROCESSES` | `OPPORTUNITIES` | SEARCH · PROCESSES + RETRIEVAL_INTENT |
| tema sin sustantivo de universo | `UNSPECIFIED` | la decide §5 | UNSPECIFIED + Product View Policy |

- Las reglas O3/O3a/O6/O7 vigentes (prompt v4.2 + H7, ADR-101) **son** el contrato de oportunidad.
  En particular O6: «licitación(es)» coloquial **no es modalidad** (DA-2bis). Que legacy la
  convierta en `licitación pública` es un defecto de legacy, no una diferencia deliberada.
- `universe_quote`: el sustantivo mínimo que nombra lo recuperado, o la señal de oportunidad.
  Con `UNSPECIFIED` es `null`.
- «convocatoria», «convenio», «selección abreviada», «oportunidad» (singular) son sustantivos de
  universo/modalidad reales del corpus: el etiquetador los clasifica por significado aunque hoy
  la regex de la puerta no los reconozca.

## 4. Dimensiones y operadores (cerrados)

Cada condición: `{dimension, operator, polarity, quote, value_hint, level?}`.
`quote` es la subcadena **exacta** del texto (para spans). `value_hint` es la lectura humana del
valor («Barranquilla», «2025-03», «500000000») — **no** es el valor ejecutable: el valor lo decide
la autoridad del host. `polarity ∈ {POSITIVE, NEGATIVE}`.

| `dimension` | `operator` | Notas |
|---|---|---|
| `TOPIC` | `contains` · `whole_token_folded` | `whole_token_folded` sólo si el tema entero es «obra(s)» |
| `BUYER` | `is` | entidad compradora nombrada (alcaldía, gobernación, ministerio, universidad, SENA, sigla) |
| `ACTOR` | `is` | proveedor/contratista **como filtro** (razón social, persona, NIT/CC con o sin marcador) |
| `GEOGRAPHY` | `is` | `level ∈ {DEPARTMENT, MUNICIPALITY, REGION, UNKNOWN}` obligatorio |
| `TIME` | `during_year` · `during_month` · `between_dates` · `relative_period` | `between_dates` admite una cota abierta («desde 2023», «hasta marzo»); `relative_period` («este mes», «últimos 6 meses», «este año») sólo si el Gold lo justifica — se etiqueta igual y A3 cuenta frecuencia |
| `VALUE` | `greater_than` · `greater_or_equal` · `less_than` · `less_or_equal` | «entre X y Y» = AND de `greater_or_equal` + `less_or_equal` |
| `STATE` | `is` · `family` | `value_hint` canónico: `open`, `published`, `closed`, `awarded`, `terminated`, `in_execution`, `cancelled`, `other:<x>` |
| `MODALITY` | `family` · `is` | modalidad **como filtro** |
| `CONTRACT_TYPE_OR_NATURE` | `is` · `family` | tipo nombrado («prestación de servicios», «obra pública» como tipo, «convenio» = COMPOSITE ADR-061) |
| `ORDER` | `value_desc` · `value_asc` · `date_desc` · `date_asc` | «más caros», «los más recientes». `limit_n` opcional si hay cifra («los 10 más caros») |
| `SOURCE` | `is` | sólo si nombra «SECOP I/II» |
| `UNSUPPORTED` | etiqueta libre corta | lenguaje material que no cabe arriba; nunca se descarta |

Operadores inclusivos: «al menos», «mínimo», «desde X millones», «no menos de» →
`greater_or_equal`; «hasta», «máximo», «no más de» → `less_or_equal`. «más de/superior a» siguen
siendo exclusivos. **Decisión §7-D4.**

Lógica: `logic` es una expresión sobre los ids de condición: `AND(c1,c2,OR(c3,c4))`,
`NOT(OR(c5,c6))`. Negación de un átomo = `polarity NEGATIVE`, nunca `NOT(átomo)` (E2).
«X o Y» se conserva como OR aunque hoy no ejecute.

## 5. Product View Policy (universo omitido)

Función pura, determinista, sobre la IR aceptada (nunca sobre texto crudo). Se aplica sólo cuando
`universe = UNSPECIFIED`. Primera regla que casa gana:

| # | Señal en la IR | `product_view` |
|---|---|---|
| PV1 | TIME presente (año, mes, rango, período pasado) | `HISTORICAL_PROCESSES` |
| PV2 | STATE terminal (`closed`, `awarded`, `terminated`, `cancelled`) o `in_execution` | `HISTORICAL_PROCESSES` |
| PV3 | ACTOR presente (proveedor como filtro) | `CONTRACTS` |
| PV4 | CONTRACT_TYPE_OR_NATURE presente | `CONTRACTS` |
| PV5 | ORDER por valor sin TIME | `HISTORICAL_PROCESSES` |
| PV6 | ninguna de las anteriores | `CURRENT_OPPORTUNITIES` |

Ejemplos: «pavimentaciones en Córdoba» → PV6 `CURRENT_OPPORTUNITIES`; «pavimentaciones en
Córdoba en 2024» → PV1; «pavimentaciones terminadas en Córdoba» → PV2.
Toda vista por defecto se **anuncia** con eco y botones: `[Procesos históricos] [Contratos]
[Elegir período] [Mayor valor]` (ADR-069: nada silencioso). El orden PV1–PV6 está congelado para
etiquetar; un caso que la tabla resuelva contra la intuición evidente va a la cola de producto,
no se reetiqueta a mano.

## 6. Significado → capacidad (columna de transición M1)

| Significado | Dueño destino (M3+) | Dueño aceptable durante M1 |
|---|---|---|
| `SEARCH` cualquier universo/dimensión | Foundation SEARCH | Foundation SEARCH |
| `SEARCH` cuyo único filtro es ACTOR («contratos de CONSORCIO X») | Foundation SEARCH + ACTOR | **Foundation SEARCH o Actor Profile** (ambos muestran contratos del actor; no es regresión que siga en Actor Profile hasta M3) |
| `SEARCH` con ACTOR + otra dimensión | Foundation SEARCH + ACTOR | Foundation SEARCH; Actor Profile sólo si honra todas las otras condiciones (A3/A4 lo miden) |
| `PROFILE_ACTOR` | Actor Profile | Actor Profile (**SEARCH no puede capturarlo**) |
| `PROFILE_BUYER` | Entity Profile | Entity Profile (**ídem**) |
| `RANK_SUPPLIERS` | Suppliers (A2) | Suppliers (**ídem**) |
| `COUNT` / `SUM_VALUE` / `GROUP_BY` | Contract Activity | Contract Activity / legacy actual (**ídem**) |
| `FOLLOW` | Radar | Radar (**ídem**) |
| `FOLLOWUP_DELTA` | Conversation IR (M2) | legacy follow-up (fuera de la paridad M1; no debe degradarse) |
| `PROTOCOL` | capa 1 | capa 1 |

Frontera ACTOR (decisión §7-D5): la operación manda. Pedir **registros** de un actor es SEARCH con
filtro ACTOR; pedir **sobre el actor** («perfil», «quién es», «información de», «qué tal es»,
nombre/NIT desnudo) es `PROFILE_ACTOR`. Lo mismo para compradores con `PROFILE_BUYER`.

## 7. Decisiones de producto cerradas en M1.0

| Id | Decisión | Origen |
|---|---|---|
| D1 | Caída del proveedor: failover técnico A→B sólo ante `PROVIDER_ERROR`; nunca ante `NEEDS_CLARIFICATION`, `UNSUPPORTED`, `INVALID_PROPOSAL`. Si A y B caen: «no pude interpretar ahora» + botones de protocolo. **Sin legacy.** | roadmap §4.1 + plan M1-G |
| D2 | Tema desnudo → `UNSPECIFIED` + Product View Policy §5 (por defecto oportunidades vigentes, anunciado) | roadmap §4.2 |
| D3 | «licitaciones» coloquial sigue O6/DA-2bis; la conversión legacy a modalidad es defecto | roadmap §4.3 |
| D4 | Inclusivos se proyectan (`>=`/`<=`), no se aclaran | roadmap §4.4 |
| D5 | Frontera ACTOR por operación (§6) con aceptación dual durante M1 | este contrato |
| D6 | Follow-ups (`FOLLOWUP_DELTA`) fuera de la paridad M1; sí entra «búsqueda completa enviada con historial» (se libera el historial al cierre de M1) | roadmap §M1.5 |

## 8. Esquema del Gold (v1)

Un registro JSON por línea. El validador determinista es
`scripts/audits/m1_search/gold_schema.py` (cualquier carril lo corre antes de entregar).

```json
{
  "gold_id": "R0042",
  "slice": "REAL | SYNTHETIC | ADVERSARIAL | MINIMAL_PAIR | NEIGHBOR",
  "text": "…",                          // en REAL vive fuera de git
  "text_sha256": "…",
  "turn_context": "SELF_CONTAINED | FOLLOWUP_FRAGMENT | NEEDS_HISTORY",
  "operation": "SEARCH",
  "aspect": null,
  "jobs": [],                             // sólo con MULTI: lista de {operation, aspect, requests}
  "requests": [
    {"request_id": "q1", "universe": "CONTRACTS", "view": "RECORDS",
     "universe_quote": "contratos", "scope_id": "s1",
     "conditions": [
       {"id": "c1", "dimension": "TOPIC", "operator": "contains", "polarity": "POSITIVE",
        "quote": "pavimentación", "value_hint": "pavimentación", "level": null}
     ],
     "logic": "AND(c1)"}
  ],
  "product_view": null,                   // obligatorio si algún universe = UNSPECIFIED
  "expected_disposition": "EXECUTE | CLARIFY | DECLINE",
  "disposition_reason": null,             // código corto si no es EXECUTE
  "ambiguity": "CLEAR | AMBIGUOUS",
  "alternatives": [],                     // lecturas alternativas si AMBIGUOUS
  "contract_gap": null,                   // texto si el contrato no cubre el caso
  "labeler": "…", "notes": "…"
}
```

`expected_disposition` describe lo que **debería** pasar en producto ideal, no lo que hace hoy:
`EXECUTE` si el significado está completo; `CLARIFY` si el propio texto es insuficiente o
genuinamente ambiguo (p. ej. «contratos de Santander»: ¿departamento o banco?); `DECLINE` si es
fuera de dominio. Un hueco del host **no** es `CLARIFY`: eso lo mide A3.

## 9. Gates de la Fase A (lo que debe existir al cerrar)

1. Gold v1 etiquetado (REAL 585 + SYNTHETIC 35 + ADVERSARIAL/MINIMAL_PAIR/NEIGHBOR nuevos),
   validado por `gold_schema.py`, con SHA por archivo.
2. Segundo etiquetado ciego e independiente (otro modelo) sobre muestra estratificada + todo lo
   sintético; acuerdo medido por campo; cola de desacuerdos.
3. Authority Health: veredicto `HEALTHY | FIX_REQUIRED | UNKNOWN` por autoridad con evidencia
   ejecutada.
4. Gap matrix GOLD / LEGACY / FOUNDATION por dimensión y clase, con la etapa culpable.
5. Inventario de fronteras de capacidad con casos vecinos congelados.
6. Especificación del benchmark de failover (sin elegir proveedor).

Nada de esto toca `app/`. Cualquier carril que necesite cambiar runtime para medir, para y lo
reporta.

## 10. Implicación conocida para M1-C (registrada ahora para no descubrirla tarde)

El prompt v4.2 está congelado por SHA y medido sobre el held-out sellado DA-6. ACTOR, ORDER,
`UNSPECIFIED`, `during_month`/`between_dates` y los inclusivos **no existen** en su vocabulario.
El spine V2 es, por tanto, un **productor nuevo (v5)**: exige su propia evaluación sobre el Gold
sellado de M1 **y** sobre el held-out DA-6 (no regresión), nunca una edición del prompt actual.

## 11. Enmienda v1.1 (2026-09-30, tras shard A del Gold)

Resuelve las dudas recurrentes del primer etiquetado. Ratifica lo que ya se etiquetó salvo donde
se indica.

| Id | Regla |
|---|---|
| A1 | «convocado», «en convocatoria» como estado de un proceso → `STATE family`, `value_hint: open`. «convocatoria(s)» encabezando la petición sigue siendo O7 (oportunidad). |
| A2 | Tipos SECOP (`suministro`, `prestación de servicios`, `obra` como tipo, `consultoría`, `interventoría`, `compraventa`, `arrendamiento`, `concesión`, `comodato`, `seguros`) son `CONTRACT_TYPE_OR_NATURE` **sólo** cuando califican al universo sin objeto propio («contratos de suministro», «de prestación de servicios»). Con objeto propio («suministro de alimentos», «arrendamiento de maquinaria») la frase entera es `TOPIC contains`. «obra(s)» sola sigue T2/K1 (`TOPIC whole_token_folded`). |
| A3 | Coordinación con «y» dentro de la materia («aseo y cafetería») es **un** `TOPIC` con la frase completa: la autoridad de tema decide la tokenización. Sólo «o»/«u» produce `OR` de átomos. |
| A4 | «(del) municipio de X», «distrito de X» como dueño de los registros → `BUYER is`. «en el municipio de X» → `GEOGRAPHY is` con `level MUNICIPALITY`. Mismo patrón que «gobernación de» vs «departamento de». |
| A5 | Valores canónicos de `STATE` añadidos: `draft` (borrador), `in_evaluation` (en evaluación), `signed` (celebrado/firmado como estado), `liquidated` (liquidado). PV2 cuenta `liquidated` y `signed` como terminales. |
| A6 | Cola de producto, no reetiquetar: PV1 con el año en curso y la falta de regla BUYER→CONTRACTS quedan para adjudicación M1-B. Lo que depende del turno anterior es M2 (`NEEDS_HISTORY`), no Product View. |

## 12. Enmienda v1.2 (2026-09-30, tras shard B del Gold)

| Id | Regla |
|---|---|
| B1 | `product_view` existe **sólo** para requests SEARCH (o jobs SEARCH de un MULTI) con `UNSPECIFIED`. En operaciones no-SEARCH es `null`; sus `requests` son opcionales y sólo transportan condiciones (BUYER, TIME…). Normalización mecánica aplicada a las etiquetas ya hechas. |
| B2 | Un verbo de contratar con el comprador/actor como sujeto («qué ha contratado X», «contratos celebrados por X», «X contrató») nombra el universo `CONTRACTS`; `universe_quote` = el verbo/participio. |
| B3 | «analiza (los contratos de) X» sin aspecto concreto → `PROFILE_BUYER` o `PROFILE_ACTOR` según X. No hay operación ANALYZE abierta. |
| B4 | «licitaciones/procesos celebrados/adjudicados» → universo del sustantivo (`PROCESSES`), `STATE is awarded` (adjudicado) o `signed` (celebrado). Nunca cambia el universo. |
| B5 | Monto sin comparador («de 300 millones») → condición `UNSUPPORTED` con `operator: value_without_comparator` y `expected_disposition: CLARIFY` (`disposition_reason: VALUE_WITHOUT_COMPARATOR`). |
| B6 | Municipio como sujeto que contrata («Montería contrató…», «lo que ha contratado Soledad») → `BUYER is` (extiende A4). |

## 13. Enmienda v1.3 (2026-09-30, adjudicación M1-B: 74 desacuerdos sobre 365)

Reglas que resuelven las clases de desacuerdo entre los etiquetadores A (opus) y B (sonnet).
Se aplican de forma mecánica a todo el Gold, no sólo a la muestra doble. Log:
`docs/audits/m1_search_foundation/M1_B_ADJUDICATION.md`.

| Id | Regla |
|---|---|
| C1 | La `quote` de ACTOR es el identificador (nombre, razón social, NIT/CC) **sin** el marcador de rol («proveedor», «contratista», «del proveedor»). |
| C2 | `FOLLOWUP_DELTA` siempre lleva `turn_context: FOLLOWUP_FRAGMENT`. `PROTOCOL` siempre `SELF_CONTAINED` (la capa 1 resuelve su estado). `NEEDS_HISTORY` se reserva para una operación identificable cuyo referente es anafórico («sus mayores proveedores», «sus contratos recientes»): se etiqueta la operación, no FOLLOWUP_DELTA. |
| C3 | Deícticos «ahora», «hoy», «en este momento», «actualmente» **no** son TIME. Junto a un estado u oportunidad no añaden condición. Solos con procesos («qué selecciones abreviadas hay en este momento») son `STATE family open` citando el deíctico. |
| C4 | `STATE is` para estados SECOP exactos (borrador, en evaluación, adjudicado, celebrado, terminado, liquidado, cerrado, publicado). `STATE family` sólo para la familia abierta (abierto, vigente, disponible, activo, en curso, convocado, en concurso). «firmados» junto a contratos sigue Y1 (sin condición). |
| C5 | «convenio(s)» como sustantivo de universo: `CONTRACTS` **más** `CONTRACT_TYPE_OR_NATURE is` citando «convenios» (COMPOSITE, ADR-061). Quitarlo ensancharía a todos los contratos. |
| C6 | Un sustantivo de universo/modalidad explícito («selecciones abreviadas», «licitaciones») prevalece sobre un verbo de contratar (B2 aplica sólo sin sustantivo). |
| C7 | En operaciones no-SEARCH las requests sólo transportan condiciones: `universe: UNSPECIFIED`, `view: RECORDS`, `universe_quote: null`; una request sin condiciones se omite. El universo no se compara fuera de SEARCH. |
| C8 | UNSUPPORTED se compara por dimensión y span, no por la etiqueta libre. «mayor cuantía» como superlativo de valor es `ORDER value_desc`, no MODALITY. Jerga monetaria («barras», «palos») es VALUE. |

Decisión de producto P1 (cola §5/A6): **no** se añade regla BUYER→CONTRACTS. Un comprador existe en
procesos y en contratos, y «oportunidades de la Alcaldía de X» es una pregunta comercial legítima.
PV1 con el año en curso se mantiene: «procesos de X en 2026» son todos los procesos de 2026,
abiertos incluidos. La vista por defecto siempre se anuncia con botones.

## 14. Enmienda v1.4 (2026-09-30, adjudicación de los slices sintéticos: 35 desacuerdos sobre 379)

| Id | Regla |
|---|---|
| D1 | «cuántos … por X» = `GROUP_BY·BY_X`. `COUNT` sólo lleva `TOTAL`. |
| D2 | **Corrige A2**: «obra(s)» nunca es CONTRACT_TYPE (convención K1 del prompt medido): siempre `TOPIC whole_token_folded`. Se retira «obra» de la lista A2. |
| D3 | `CONTRACT_TYPE_OR_NATURE` usa siempre `is` en el Gold (incluidos «convenios de asociación» y tipos largos); `family` queda reservado. |
| D4 | Las reglas de Product View (§5) sólo miran condiciones **POSITIVE**: un estado terminal negado («que no esté cerrado») no activa PV2. |
| D5 | Referencias normativas («ley 80 de 1993», «decreto 1082») son `UNSUPPORTED`, no TOPIC (U1). |
| D6 | Un participio que ancla una fecha («publicados hace dos semanas», «firmados en marzo») no es STATE: la fecha va a TIME (extiende Y1). |
| D7 | «de <lugar>» sin cabeza institucional = GEOGRAPHY (lectura dominante) con `AMBIGUOUS` y `EXECUTE`; `CLARIFY` sólo si el nombre tiene un homónimo no geográfico dominante (Santander/banco). |
| D8 | «excepto los de X y los de Y» = `NOT(OR(X,Y))` (E2: exclusión de un grupo compuesto). |
| D9 | En MULTI cada job lleva sus propias requests; los jobs no-SEARCH siguen C7. Una pregunta anafórica con operación clara («y sus proveedores?») es esa operación con `NEEDS_HISTORY` (C2). |
| D10 | «analiza <tema> en <lugar>» (sin entidad) = `GROUP_BY·OTHER:MARKET_ANALYSIS`. «qué ha contratado <actor>» = SEARCH·CONTRACTS con ACTOR (B2). Predicción = `OUT_OF_DOMAIN`/`DECLINE`. |

## 15. Enmienda v1.5 (2026-09-30) — cardinalidad de superlativos (alinea el texto con el Gold sellado)

La §4 decía «`limit_n` opcional si hay cifra»; los dos etiquetadores, de forma independiente,
etiquetaron además el superlativo singular con `limit_n: 1` (N012, R0507). El Gold manda:

| Forma | Etiqueta |
|---|---|
| «el/la <sustantivo> más …», «el de mayor valor» (singular) | `ORDER` + `limit_n: 1` |
| «los más caros», «las más recientes» (plural sin cifra) | `ORDER` sin `limit_n` |
| «los 10 más caros» (cifra explícita) | `ORDER` + `limit_n: N` |

En la IR v2 (C0) `limit_n` se representa como la dimensión `CARDINALITY`, resuelta por el host.

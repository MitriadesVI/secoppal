# HCA-R4.1 — Negation live source precheck

- **Fecha de fuente:** 2026-08-24 (America/Bogota)
- **Base runtime:** `9ecf3be0e1b5acdda31fc8fe6ead6073df3f137f`
- **HEAD medido:** `8e4e62512a1e67f205e2471c1bacf47d2d8343e9`
- **Runtime diff contra base:** ninguno bajo `app/`
- **Fuentes:** Procesos `p6dx-8zbt`; Contratos `jbjy-vk9h`
- **Artefacto:** `artifacts/audits/hca_r4_1_live_precheck_2026-08-24.json`
**SHA-256 artefacto:** `7fdf0fc7cc3ba4ed23b77a62bee8723c31f834607d3b7e9cbb17f54f95833e5d`

Este bloque cierra solo las verificaciones de fuente exigidas antes de
implementar ADR-064. No implementa negación ni modifica `SoQLBuilder`,
`QueryFrame`, `QueryPlan`, `followup_engine` o cualquier otro runtime. El runner
aislado es `scripts/audits/hca_r4_1_live_precheck.py`; hizo GET autenticados con
app token, no writes contra Socrata.

## Dictamen

```text
HCA_R4_1_LIVE_PRECHECK: PASS

OBJECT_NULL_RATE:
  p6dx-8zbt / nombre_del_procedimiento:
    NULL 5 / 9 055 609 = 0,00005521 %; EMPTY 0
  p6dx-8zbt / descripci_n_del_procedimiento:
    NULL 1 / 9 055 609 = 0,00001104 %; EMPTY 0
  jbjy-vk9h / objeto_del_contrato:
    NULL 0 / 5 980 312 = 0 %; EMPTY 0
  jbjy-vk9h / descripcion_del_proceso:
    NULL 0 / 5 980 312 = 0 %; EMPTY 0

CURRENT_OBJECT_EXCLUDE_NULL_SAFE: NO
NULL_SAFE_FORM_VERIFIED: YES

GEOGRAPHY_SENTINELS:
  p6dx-8zbt / ciudad_entidad:
    TOTAL 9 055 609; NULL 0; EMPTY 0;
    "No Definido" 2 154 643 = 23,79346326 %; otros 0
  jbjy-vk9h / ciudad:
    TOTAL 5 980 312; NULL 0; EMPTY 0;
    "No Definido" 1 286 689 = 21,51541592 %; otros 0

CITY_NEGATION_SAFE_WITH_CURRENT_MATCHING: NO
EXPECTED: NO

CITY_TO_ENTITY_NEGATIVE_HAZARD: CONFIRMED
TEMPORAL_NEGATION_TRANSPORT: SAFE
ADR_064_FACTUAL_CHANGES_REQUIRED: YES

READY_FOR_HCA_R4_NEGATION_IMPLEMENTATION
```

`SAFE` en TIME significa exclusivamente que el transporte explorado no tuvo
timeouts ni rebasó el gate orientativo. No activa soporte temporal ni cambia
`TEMPORAL_NEGATION_V1 = CLARIFY_UNSUPPORTED`.

## 1. OBJECT NULL semantics

La inspección de `SoQLBuilder.SPECS` fijó los campos realmente usados por
`objeto_exclude`; no se extrapoló desde fixtures.

| dataset | campo | total | NULL | NULL % | vacío | vacío % |
|---|---|---:|---:|---:|---:|---:|
| Procesos `p6dx-8zbt` | `nombre_del_procedimiento` | 9 055 609 | 5 | 0,00005521 % | 0 | 0 % |
| Procesos `p6dx-8zbt` | `descripci_n_del_procedimiento` | 9 055 609 | 1 | 0,00001104 % | 0 | 0 % |
| Contratos `jbjy-vk9h` | `objeto_del_contrato` | 5 980 312 | 0 | 0 % | 0 | 0 % |
| Contratos `jbjy-vk9h` | `descripcion_del_proceso` | 5 980 312 | 0 | 0 % | 0 | 0 % |

Combinación NULL:

| dataset | ambos NULL | solo f1 NULL | solo f2 NULL | ninguno NULL | cualquier NULL |
|---|---:|---:|---:|---:|---:|
| Procesos | 0 | 5 | 1 | 9 055 603 | 6 (0,00006626 %) |
| Contratos | 0 | 0 | 0 | 5 980 312 | 0 |

### Prueba A — una fila sin el término no desaparece por NULL

Término sintético reproducible: `zznegprecheck20260824`. En Procesos, las 6
filas con algún campo NULL fueron candidatas y ninguna contenía el término:

| forma | supervivientes de las 6 |
|---|---:|
| actual `NOT (p1 OR p2)` | **0** |
| NULL-safe `NOT (COALESCE(p1,false) OR COALESCE(p2,false))` | **6** |

SoQL aceptó y evaluó la forma NULL-safe directamente:

```sql
NOT (
  coalesce(unaccent(upper(nombre_del_procedimiento))
    LIKE unaccent(upper('%zznegprecheck20260824%')), false)
  OR
  coalesce(unaccent(upper(descripci_n_del_procedimiento))
    LIKE unaccent(upper('%zznegprecheck20260824%')), false)
)
```

Contratos no tiene candidatos NULL en el snapshot: ambas formas devolvieron 0
sobre ese subconjunto vacío. Esto no vuelve segura a la expresión actual; solo
significa que hoy no materializa el defecto en ese dataset.

### Prueba B — una fila con el término sigue excluida

Control `servicios` sobre IDs públicos reproducibles:

| dataset | ID | contiene término | superviviente actual | superviviente NULL-safe |
|---|---|---:|---:|---:|
| Procesos | `CO1.REQ.3292622` | 1 | 0 | 0 |
| Contratos | `CO1.PCCNTR.4168447` | 1 | 0 | 0 |

En Procesos el control además tiene `nombre_del_procedimiento` NULL: prueba que
`TRUE OR NULL` sigue excluyéndose tras aplicar `COALESCE`.

**Conclusión:** `CURRENT_OBJECT_EXCLUDE_NULL_SAFE: NO` y
`NULL_SAFE_FORM_VERIFIED: YES`. El impacto vivo observado es pequeño (6 filas),
pero la corrección sigue siendo necesaria para
`INV-NO-SILENT-NEGATION-DROP`.

## 2. Geography unknown values

El único centinela relevante hallado entre 1 001 valores CITY de Procesos y
910 de Contratos fue exactamente `No Definido`, con N y D mayúsculas y sin
acento. No aparecieron variantes de casing, `N/A`, `Sin información`,
`Desconocido` u otros marcadores equivalentes. NULL y cadena vacía fueron cero.

| dataset | campo CITY | total | NULL | vacío | `No Definido` | porcentaje |
|---|---|---:|---:|---:|---:|---:|
| Procesos | `ciudad_entidad` | 9 055 609 | 0 | 0 | 2 154 643 | **23,79346326 %** |
| Contratos | `ciudad` | 5 980 312 | 0 | 0 | 1 286 689 | **21,51541592 %** |

La cifra de diseño 6,61 % / 2,01 % no era CITY sino el centinela de
DEPARTMENT. Repetida sobre los campos correctos:

| dataset | campo DEPARTMENT | `No Definido` | porcentaje vivo | diseño | ¿se reproduce a 2 decimales? |
|---|---|---:|---:|---:|---|
| Procesos | `departamento_entidad` | 582 818 | **6,43598901 % → 6,44 %** | 6,61 % | **NO** |
| Contratos | `departamento` | 120 469 | **2,01442667 % → 2,01 %** | 2,01 % | **SÍ** |

Los conteos de ADR-064 eran 569 832 y 112 820; el dataset mutable creció. La
corrección factual aplicada conserva la semántica decidida: desconocido es una
cadena real, no NULL, y una exclusión debe declarar si lo incluye.

## 3. CITY substring collisions

El predicado vigente es `unaccent(UPPER(ciudad)) LIKE
unaccent(UPPER('%term%'))`. Los agrupamientos reales mostraron:

| término | valor fuente adicional | Procesos | Contratos |
|---|---|---:|---:|
| `Cali` | `Calima` | 1 548 | 1 075 |
| `Cali` | `San Calixto` | 121 | 5 |
| `Cali` | `California` | 46 | 0 |
| `San Andres` | `San Andrés Sotavento` | 2 890 | 24 |
| `Cartagena` | `Cartagena Del Chairá` | 819 | 58 |

La fuente almacena `San Andrés Sotavento` sin la preposición `de`. Para
`San Andrés de Cuerquia` el problema es aún más estructural: la fuente almacena
el CITY exacto `San Andrés` y usa DEPARTMENT para distinguirlo.

| valor CITY `San Andrés` / variante | departamento | Procesos | Contratos |
|---|---|---:|---:|
| `San Andrés` | San Andrés, Providencia y Santa Catalina | 38 355 | 37 311 |
| `San Andrés` | Santander | 487 | 231 |
| `San Andrés` | Antioquia | 296 | 227 |
| `San Andrés Sotavento` | Córdoba | 2 890 | 24 |

Por tanto, incluso una igualdad ingenua sobre el texto CITY `San Andrés` no
distinguiría el Archipiélago, San Andrés (Santander) y el valor de Antioquia;
el LIKE actual además captura Sotavento. Una exclusión por subcadena borraría
filas correctas de forma invisible.

**Conclusión:** `CITY_NEGATION_SAFE_WITH_CURRENT_MATCHING: NO`. CITY sigue
`CLARIFY_ONLY` porque concurren tres riesgos medidos: centinela desconocido en
más de una quinta parte del universo, colisiones de subcadena, y valores CITY
no unívocos entre departamentos. No se diseñó ni creó gazetteer.

## 4. Temporal negation cost

Benchmark exploratorio directo contra `p6dx-8zbt`. Ventana común
`[2024-01-01, 2027-01-01)`:

- A: rango positivo continuo 2024..2026.
- B: la unión 2024 ∪ 2026, que representa 2024..2026 excepto 2025 mediante
  `OR` sobre `fecha_de_publicacion_del`.

Se alternó el orden A→B / B→A. No hubo retries en ninguna muestra temporal.

| shape | variante | rondas | éxitos | timeouts | p50 | p90 nearest-rank | max |
|---|---|---:|---:|---:|---:|---:|---:|
| fetch, proyección Search + `ORDER BY` + `LIMIT 25` | A | 8 | 8 | 0 | 0,2291 s | 0,2883 s | 0,2883 s |
| fetch, proyección Search + `ORDER BY` + `LIMIT 25` | B | 8 | 8 | 0 | 0,3842 s | 0,4582 s | 0,4582 s |
| `count(*)` de toda la ventana | A | 4 | 4 | 0 | 1,5250 s | 6,2443 s | 6,2443 s |
| `count(*)` de toda la ventana | B | 4 | 4 | 0 | 2,4844 s | 2,5275 s | 2,5275 s |

Forma B:

```sql
WHERE (
  (fecha_de_publicacion_del >= '2024-01-01T00:00:00.000'
   AND fecha_de_publicacion_del < '2025-01-01T00:00:00.000')
  OR
  (fecha_de_publicacion_del >= '2026-01-01T00:00:00.000'
   AND fecha_de_publicacion_del < '2027-01-01T00:00:00.000')
)
```

La autoridad canónica de Etapa 6 (`docs/PERFIL_LATENCIA_2026-06-28.md`) fija
el criterio **p90 < 15 s** y reporta **11,6 s** como resultado observado. No son
la misma cifra. Las muestras B quedaron por debajo de ambos valores, pero la
comparabilidad es limitada:

- fetch comparte proyección, `ORDER BY` y límite con Search, pero omite parser,
  follow-up, fallback, count lazy y render;
- count es un agregado fuente-a-fuente, no la latencia end-to-end del turno;
- la prueba mide `OR` en una misma columna, no el `OR` entre columnas que el
  comentario de oportunidad identifica como penalización severa;
- n es exploratorio y el max de A count muestra efecto de franja/caché.

**Conclusión:** `TEMPORAL_NEGATION_TRANSPORT: SAFE` para este sondeo. No se
concluye que B sea más rápida que A y no se activa soporte. TIME permanece
CLARIFY por la representación no convexa y por `HCA-TIME-001/002/003/004`.

## 5. CITY_TO_ENTITY negative hazard

### Turno único

```text
"contratos en atlantico menos barranquilla"
  departamento_resolved = Atlántico
  ciudad                 = Barranquilla
  entidad                = Barranquilla
  entidad_resolved       = DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA
  entidad_resolution.method = city_to_entity
```

Barranquilla activa `_CITY_TO_ENTITY` dentro del paso 10.5 de
`QueryRouter.parse`. El span gobernado por `menos` contamina cuatro campos
positivos: `ciudad`, `entidad`, `entidad_resolved` y `entidad_resolution`.

### Follow-up

```text
T1 "contratos en atlántico"
  → departamento_resolved = Atlántico

T2 "menos barranquilla"
  → parse: ciudad=Barranquilla + entidad_resolved=DISTRITO ...
  → intent: change_scope
  → normalize_scope_transition limpia:
      departamento, departamento_resolution, departamento_resolved
  → merged: ciudad/entidad de Barranquilla; Atlántico desaparece
```

Capas confirmadas:

1. `app/core/query_router.py`: detección CITY y `_CITY_TO_ENTITY` crean los
   campos positivos.
2. `app/core/followup_engine.py`: el scope contaminado clasifica
   `change_scope`.
3. `app/core/followup_mutations.py`: `normalize_scope_transition` limpia
   `departamento_resolved` por reemplazo de scope.

Fix surface mínimo esperado, no implementado:

1. Atar spans negativos antes de la detección CITY; una ciudad negada no puede
   poblar `ciudad` ni activar `_CITY_TO_ENTITY`.
2. Clasificar follow-ups desde el residuo positivo; una ciudad negada no puede
   ser evidencia de `change_scope` ni disparar `normalize_scope_transition`.

No se requiere tocar `QueryFrame` o `SoQLBuilder` para cerrar este hazard.

**Conclusión:** `CITY_TO_ENTITY_NEGATIVE_HAZARD: CONFIRMED`.

## 6. Correcciones factuales de ADR-064

`ADR_064_FACTUAL_CHANGES_REQUIRED: YES`, limitadas a hechos demostrados:

1. Los conteos/porcentajes DEPARTMENT mutaron: Procesos ya no reproduce 6,61 %;
   Contratos sí reproduce 2,01 % al redondear.
2. CITY no comparte esos porcentajes: `No Definido` es 23,79 % / 21,52 %.
3. El defecto NULL-safe se materializa hoy en 6 filas de Procesos y 0 de
   Contratos; el ejemplo anterior usaba solo nombres de campos de Contratos.
4. El gate Etapa 6 es p90 < 15 s; 11,6 s es el resultado de cierre.

Las correcciones se aplicaron a ADR-064 y a la guía de diseño sin cambiar la
frontera semántica: OBJECT soportado solo con fix NULL-safe; CITY y TIME
CLARIFY; los dos invariantes permanecen intactos.

## 7. Cierre de scope

- `app/`: sin diff contra `9ecf3be`.
- Fixture pre-registrado `negation_focal_v1.yaml`: sin modificar;
  SHA-256 `f4f64a33ea7009eb1da9fa7d3c910e67010e6dafc6b4c5b6932901c27fad9a1d`.
- Sin merge ni push.

La evidencia no exige revisar el diseño; elimina sus incertidumbres de fuente y
corrige cuatro hechos. Resultado final:

```text
READY_FOR_HCA_R4_NEGATION_IMPLEMENTATION
```

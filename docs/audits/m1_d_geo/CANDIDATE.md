# M1-D-GEO — autoridad pura de GEOGRAFÍA (candidato)

Rama `mission/m1-d-geo`, base `4ec99bc1`. Sólo archivos nuevos: 0 archivos existentes modificados
(núcleo Foundation, `entity_resolver.py`, `municipal_territory.py`, su JSON y `colombia_geography.py`
intactos).

## Tesis

GEOGRAPHY pasa de «sólo departamentos» a tres niveles decididos por una función pura que sólo ve la
cita (más el nivel tipado, una pista de departamento y una ventana de contexto que **sólo retira**
autoridad). Nunca adivina: homónimo, conflicto de nivel o nombre desconocido => CLARIFY; negación o
país => BLOCK.

## Archivos

| Archivo | Papel |
|---|---|
| `app/core/geo_authority.py` | `resolve_geography(quote, level, polarity, *, department_hint, left, right) -> GeoResolution`; `to_geo_value`, `host_reason`, `secop_city_values`, `department_of`, `municipal_candidates` |
| `app/core/geo_regions.py` | función pública de región: `region_lookup(text) -> tuple[RegionMatch,...]` (0, 1 o varias = ambigua) |
| `app/core/geo_window.py` | retiro por contexto (`WINDOW_TOKENS = 4`) |
| `app/data/geo_secop_variants.py` | alias coloquiales, grafías SECOP de `ciudad`, isla 88001, país, región ambigua |
| `tests/test_geo_authority.py` | 2511 pruebas: barridos de catálogo, Gold, ventana, propiedades, 15 mutantes de batería |
| `scripts/audits/m1_d_geo_secop_city_evidence.py` + `docs/audits/m1_d_geo/secop_ciudad_evidence.json` | evidencia congelada de SECOP (solo lectura; el test no usa red) |

## Reglas (decisión -> razón)

- `level` explícito manda. `UNKNOWN` usa el marco tipado **dentro de la cita** (`municipio de X`,
  `departamento de X`, `región del X`); si contradice el nivel declarado => CLARIFY `GEO_LEVEL_CONFLICT`.
- `UNKNOWN` con homónimo departamento/municipio (Córdoba, Nariño, Boyacá, Sucre, San Andrés, Arauca,
  Caldas, Risaralda…) => CLARIFY `GEO_LEVEL_AMBIGUOUS` (candidatos `DEPARTMENT:` + `MUNICIPALITY:` +
  `REGION:`). Único caso que es una sola entidad: Bogotá => RESOLVED nivel DEPARTMENT con
  `municipality_divipola=11001` (`GEO_CAPITAL_DISTRICT`).
- Municipio homónimo sin departamento (San Luis, La Unión, Armenia, Manaure…) => CLARIFY
  `GEO_MUNICIPALITY_AMBIGUOUS`; con `department_hint` o `San Luis, Tolima` => RESOLVED. 64+ nombres,
  146+ municipios en grupos (barrido completo en el test).
- «Colombia»: `UNKNOWN/DEPARTMENT/REGION` => BLOCK `GEO_COUNTRY_IS_NOT_A_FILTER`; nivel MUNICIPALITY sin
  marco => CLARIFY `GEO_COUNTRY_VS_MUNICIPALITY`. Sólo `municipio de Colombia` o hint Huila => 41206.
- Región => `departments_in_region` (valores oficiales SECOP) con recibo `region_alias:<alias>`.
  `la costa` => CLARIFY (Caribe | Pacífica): el repo la mapeaba a Caribe y A2 la marcó como ambigua.
- Polaridad NEGATIVE => BLOCK `GEO_NEGATIVE_UNSUPPORTED`. Polaridad/nivel/cita inválidos => BLOCK.
- Ventana (`left`/`right`, <= 4 tokens plegados): retira RESOLVED si `left` fija al dueño
  (`alcaldía de`, `del municipio de`: A4/B6), si el marco tipado contradice el nivel, o si `right`
  nombra otro departamento. Nunca convierte CLARIFY/BLOCK en RESOLVED (probado por propiedad y por mutante).

## Mapeo al destino (C0 §5.5; el módulo se mueve a `host_authorities` con un adaptador)

| Origen | Destino |
|---|---|
| `GeoResolution` RESOLVED | `GeoValue(level, canonical, divipola, members)` vía `to_geo_value`: DEPARTMENT `canonical`=valor SECOP, `members=()`; MUNICIPALITY `canonical`=nombre DANE, `divipola`, `members=(departamento contenedor,)`; REGION `canonical`=clave de `ZONES_BY_NAME`, `members`=departamentos |
| `decision` CLARIFY / BLOCK | `NEEDS_CLARIFICATION` / `UNSUPPORTED` (lo traduce el núcleo) |
| `reason` | `host_reason()`: `NEGATION_UNSUPPORTED`, `GENERIC_IDENTITY` (país), `AMBIGUOUS_HOST_DIMENSION` (nivel/municipio/región/pista/contexto de departamento), `LEVEL_MISMATCH` (conflicto de nivel y pista de dueño), `UNRESOLVED_HOST_DIMENSION` (no encontrado, departamento incompatible, cita vacía, entradas inválidas), todos `:GEOGRAPHY` |
| `authority` | `geo-authority@1` |
| entrada | `quote` + `left`/`right` de `EvidenceWindow` + `level` + `polarity.value`; sin texto crudo |

`level` llega como `str` o `Enum`. El adaptador no debe pasar `department_hint` salvo que otra
condición del mismo turno ya sea un departamento RESOLVED.

## Proyección SoQL (C1; no implementada aquí)

Columnas ya usadas por `soql_builder`: procesos `departamento_entidad`/`ciudad_entidad`, contratos
`departamento`/`ciudad`.

- DEPARTMENT: `departamento* = '<valor SECOP>'` (33 valores; Bogotá = `Distrito Capital de Bogotá`).
- REGION: `departamento* IN (<members>)`.
- MUNICIPALITY: `departamento* = '<department>'` **y** `ciudad* IN (<secop_city_values(divipola)>)`
  con comparación plegada de acentos/mayúsculas. **No** usar el nombre DANE: SECOP guarda `Cali`,
  `Cartagena`, `Cúcuta`, `Bogotá`, `Tumaco`… (el nombre DANE da 0 filas).
- DIVIPOLA no existe como columna en estos datasets: el código sólo identifica; el filtro es por texto.

Evidencia (datos.gov.co, 2026-09-30; `secop_ciudad_evidence.json`): 1085 pares distintos
`(ciudad, departamento)` con ciudad definida, 11.81 M filas (procesos + contratos).
El nombre oficial DIVIPOLA acierta 7.56 M (64 %); con `geo_authority`: 11.81 M (100 %). Los 29 pares que
el DIVIPOLA falla están todos cubiertos (Cali 851 k filas, Cartagena 259 k, Cúcuta 217 k, Bogotá 2.8 M,
San Andrés 77 k, Tumaco, Mompós, Santafé de Antioquia, Mariquita, Piendamó, Sincé…).

Riesgos de cobertura que C1 debe declarar al usuario:

1. `ciudad = 'No Definido'` es 23.6 % de procesos y 21.5 % de contratos (39-40 % en Bogotá, 6-12 % en
   otros departamentos). Un filtro por municipio **pierde** esas filas; el de departamento no.
   Por eso Bogotá sin nivel se resuelve a DEPARTMENT.
2. `ciudad*` es la ubicación de la entidad compradora, no el lugar de ejecución de la obra.
   Una búsqueda «obras en Cali» filtra por entidades de Cali.

## Hallazgos de datos corregidos (sin tocar legacy)

- DANE clasifica 88001 San Andrés como `ISLA`, no `MUNICIPIO`: el registro de 1103 la omite aunque SECOP
  guarda 76 843 filas `ciudad='San Andrés'`. Se añade como único suplemento (`SUPPLEMENT_TERRITORIES`).
- Alias que DIVIPOLA no indexa: Cali, Cartagena, Cúcuta, Bogotá, Tumaco, Buga, Mompós/Mompox y 22
  claves de grafías SECOP (Mariquita, Don Matías, Santafé de Antioquia, Piendamó, Leguízamo, Sincé, Tolú Viejo,
  Cuaspud, Purísima, San Vicente, San Pedro, San Andrés (Antioquia), Manaure (Cesar)…).
  Los alias **no** se metieron en el registro DIVIPOLA: `MunicipalTerritoryRegistry.consume` proyecta
  `ciudad = <oficial>.title()` con igualdad, y un alias que apuntara a `Santiago De Cali` habría dado
  0 filas. Los consumidores legacy conservan su comportamiento; no se aplicó el guard M1A.2 de
  compuestos porque no se alteró `by_alias`.
- `DEPARTAMENTOS` (legacy) archiva la ciudad `cucuta` como departamento Norte de Santander; la autoridad
  la ignora como departamento (`CITY_KEYS_FILED_AS_DEPARTMENT`).

## Impacto legacy

Ninguno por construcción: `git diff 4ec99bc1 HEAD --stat` = 7 archivos nuevos, 0 modificados, y ningún
módulo existente importa los nuevos. Cambios legacy a listar: **ninguno**. Corrí 135 pruebas focales
(`test_municipal_territory`, `test_municipal_m1a`, `test_regiones`, `test_geo_mention_resolution`,
`test_city_detection`, `test_geo_vs_entity_typed`, `test_module_size_limits`,
`test_compound_identity_guard`): 135 passed. No corrí los corpus entity_types 299/300, query_plan ni
cross-interaction municipal, porque el delta de código existente es vacío; no corrí la suite global.

## Deuda detectada (no corregida; fuera de scope)

1. `entity_resolver.py` (rama BUYER/H0): `geo_mention_resolution('Armenia'|'San Andrés'|'Bogotá'|'Cúcuta')`
   devuelve `city/high` sin desambiguar (Armenia ×2, San Andrés ×3 con la isla); `GAZETTEER_CIUDADES`
   tiene `doncello` (oficial: `EL DONCELLO`).
2. `MunicipalTerritoryRegistry.consume` (legacy) resuelve sin preguntar: `en colombia` => 41206 Huila
   (GEOGRAPHY-044), `el municipio de san andres` => Santander 68669 (omite la isla y el departamento
   homónimo) y `en manaure` => La Guajira (el Manaure del Cesar existe en SECOP).
3. `app/data/departamentos.py`: alias `cucuta` => Norte de Santander (ciudad como departamento).
4. El registro DIVIPOLA exige `aliases == (key(oficial),)`: cualquier alias futuro debe vivir en
   `geo_secop_variants.py` o rediseñar la proyección `ciudad` de `consume`.
5. Las 18 áreas no municipalizadas de DANE siguen fuera (no aparecen como `ciudad` en SECOP).

## Gates (literales)

- `pytest tests/test_geo_authority.py`: 2511 passed (incluye Hypothesis 400 ejemplos y 15 mutantes de batería).
- Mutación a nivel fuente: 18 mutaciones ad hoc de `geo_authority.py`/`geo_window.py`/`geo_regions.py`;
  3 sobrevivieron en la primera ronda (conflicto pista-departamento a nivel DEPARTMENT, recorte de ventana,
  marcos de `region_lookup`); se añadieron pruebas y la segunda ronda: 0 supervivientes. Archivos restaurados.
- `ruff check` sobre los 7 archivos: All checks passed.
- `olympus-semgrep-delta --base 4ec99bc1`: TOTAL=40 BASELINE=40 NEW_DELTA=0.
- 135 pruebas focales legacy: passed (lista arriba). No se corrió la suite global.
- `olympus-state own claim` se colgó (también `own list`); no hay ownership registrado. El worktree
  `secoppal-m1-dgeo` fue de uso exclusivo.

Rondas de reparación usadas: 2 (alias `cucuta`/marco de ciudad con nombre propio; supervivientes de mutación).

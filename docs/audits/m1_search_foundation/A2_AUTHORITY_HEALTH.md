# A2 — Authority Health (M1_SEARCH_FOUNDATION_V1 · Fase A)

Fecha de medición: 2026-09-30 (reloj inyectado 2026-09-30). Rama `mission/m1-search-foundation-phase-a`.
Reproducible: `PYTHONPATH=. .venv/bin/python scripts/audits/m1_search/authority_probes.py`
→ `a2_authority_probes.json` (529 sondas, determinista: dos corridas producen el mismo JSON). Sin red
(`socket.connect` lanza), sin telemetría Burr, persistencia redirigida a un tmpdir, sólo entradas públicas
(razones sociales de personas jurídicas, topónimos, frases genéricas; las cédulas se reportan como «CC persona natural»,
sin número). No se tocó `app/`, `tests/` ni git.

Pregunta de la misión: ¿qué autoridades existentes están sanas como «evidencia estructurada → resolución», sin releer texto crudo?

## 0. Veredictos

| Autoridad | Veredicto | Sondas (auth / host / agg) | PASS | FAIL_SILENT (alcanzables hoy / latentes) | FAIL_LOUD | GAP | Tamaño del arreglo |
|---|---|---|---|---|---|---|---|
| BUYER | **FIX_REQUIRED** | 69 / 50 / 3 | 42 | **19 / 0** (+ 18 GATED) | 33 | 0 | S-M: 1 función + datos; los *bindings* son el cuello |
| ACTOR | **FIX_REQUIRED** | 55 / 2 / 2 | 22 | 0 / **11** | 7 | 2 | M: 3-4 archivos + rebuild del índice (+ rama host nueva) |
| GEOGRAPHY | **FIX_REQUIRED** | 59 / 10 / 1 | 42 | 0 / 3 | 4 | 6 | M: 3-4 archivos (MUNICIPALITY/REGION son trabajo nuevo) |
| TIME | **FIX_REQUIRED** | 54 / 11 | 31 | **4 / 11** | 4 | 5 | M: 3 archivos |
| MONEY | **FIX_REQUIRED** | 53 / 11 | 38 | **6 / 2** (+ 3 GATED) | 3 | 2 | S-M: 3 archivos |
| MODALITY | **FIX_REQUIRED** (el resolver no; el *host* sí) | 26 / 11 | 23 | **6 / 0** | 1 | 1 | S: 1-2 archivos, el resolver no se toca |
| CONTRACT_UNIVERSE | **HEALTHY** (con huecos de cobertura loud) | 72 / 3 / 1 | 65 | 1 (LOW) / 0 | 7 | 2 | XS-S: 1-2 archivos |
| STATE | **FIX_REQUIRED** | 28 / 8 | 20 | **1 / 1** | 6 | 4 | S: 2 archivos |

Ninguna autoridad quedó `UNKNOWN`: todas se pudieron ejercitar offline. Lo que NO se pudo medir sin red ni LLM está en §10.

Leyenda: `PASS` correcto · `DECLINE_OK` declinó con razón · `FAIL_LOUD` declinó donde había un valor único (seguro: el usuario ve una pregunta) ·
`FAIL_SILENT` valor equivocado sin señal (el grave) · `GATED` equivocado en la autoridad pero contenido por otro control · `GAP` capacidad ausente.
«Alcanzable hoy» = el host Foundation actual lo ejecuta tal cual; «latente» = la función no la llama el host todavía, se expone al conectarla.

### Hallazgo transversal: el host re-implementa en vez de llamar a la autoridad

`foundation_admission.py` sólo llama a la autoridad de dominio en 5 de 8 dimensiones, y en varias la consume a medias:

| Dimensión | Autoridad que existe | Lo que el host llama hoy (`foundation_admission.py`) |
|---|---|---|
| BUYER | `EntityResolver.resolve_entidad` | sí (:106-108), filtrando por `method`; luego exige *binding* de nombres físicos |
| GEOGRAPHY | `department_mention_resolution`, DIVIPOLA, regiones | sólo departamento (:101-104); sin `level` en `ProposedCondition` |
| TIME | `resolve_temporal_scope` (ADR-065) | **no la llama**: una regex propia `\b(19\|20)\d{2}\b` (:109-111) |
| VALUE | `money_to_cop` + `operator_reason` | sí (:112-118), sólo `greater_than`/`less_than` |
| MODALITY | `modality_resolver.resolve` | sí (:122-124) pero ignora `status`, `strength` y polaridad |
| CONTRACT_TYPE | `resolve_contract_types` | sí (:138-139), `include_ids[0]`, descarta el residuo |
| STATE | `resolve_estado` (ADR-067/068) | **no la llama**: `state_value` de 3 patrones (:30-39) |
| ACTOR | `ActorNameResolver`, `normalize_identity` | **nada** (`UNRESOLVED_HOST_DIMENSION:ACTOR`) |

### Fallos silenciosos más graves (ordenados)

1. **BUYER — 136 de 137 aliases de varios owners resuelven `exact`/`high` a uno solo** (INVIAS→ANI, SENA→una regional, ICBF→ICBF Tolima, IDRD→Secretaría de Cultura, ITM→Instituto de Turismo del Meta, INDER, ESE Hospital San Rafael). Causa: `entity_resolver.py:851` (`alias_index[alias]=official`, el último gana) y `:540-541` (Tier 1 lee `alias_index`, no `owners_by_alias`). El host **ejecuta** ICBF y IDRD hoy (`nombre_entidad = 'ICBF REGIONAL TOLIMA'`); INVIAS/SENA/ITM/INDER sólo se frenan por *accidente* (esos compradores no tienen *binding*).
2. **TIME — el host ejecuta un año que la cita no dice**: `antes de 2024`, `después de 2022`, `desde 2023`, `primer trimestre de 2025` con `during_year` → `fecha_de_firma >= '2024-01-01' AND < '2025-01-01'` etc. (`foundation_admission.py:109-111`; la regla Y1 del prompt v4.2 manda citar *sólo* el año, así que el comparador ni siquiera llega al host). La autoridad temporal tiene los mismos huecos (`temporal_semantics.py:391-401`, `:235`, `:363`).
3. **MONEY — la guarda de dirección no lee la negación**: `que no superen 300 millones` / `no mayor a 300 millones` con `greater_than` pasan y se ejecutan como `valor_del_contrato > 300000000` (inversión). `foundation_value_direction.py:43-59`; además `operator_reason` devuelve `None` sin leer la cita para cualquier operador inclusivo (`:59`).
4. **MODALITY — el host ignora lo que el resolver señala**: `licitaciones` se ejecuta como licitación pública (O6/DA-2bis), `no por contratación directa` con polaridad POSITIVA se ejecuta como directa (inversión), `directa o licitación` pierde la rama «directa». `foundation_admission.py:122-124`.
5. **ACTOR — el directorio resuelve a una identidad concreta** para `Consorcio`, `Hospital`, `salud`, `construcción`, `Barranquilla`, `Huila` (alias con 1-12 observaciones) y la ESP `Empresa de Acueducto y Alcantarillado de Bogotá` a una **cédula de persona natural** (1 observación) mientras el NIT real (7 obs.) está bajo otro alias. `actor_alias_index.py:86` (`minimum_total_occurrences=1`). Hoy latente (el host no hospeda ACTOR), pero `resolve_actor_params` ya lo hace en legacy (`proveedor Consorcio`).
6. **STATE — `adjudicados` ensancha a estados previos a la adjudicación** (Evaluación+Seleccionado+Aprobado+En aprobación) contra su propio léxico (`ESTADO_PROCEDIMIENTO_SYNONYMS`: adjudicado→Seleccionado) por `estado_families.py:174-175`; y el host ejecuta `terminados anticipadamente` como `estado_contrato='terminado'`.
7. **TIME (autoridad) — `septiembre del 2025` devuelve septiembre de 2026**, `del 1 al 15 de marzo de 2025` devuelve sólo el día 15, `segundo semestre de 2025` devuelve el año completo (la rama de «no soportado» es inalcanzable cuando hay año, `temporal_semantics.py:403`).

---

## 1. BUYER — FIX_REQUIRED

**Punto de entrada.** `EntityResolver.resolve_entidad(user_input, departamento=None) -> ResolutionResult(value, method, confidence, like_value, clarification_needed)` — `app/core/entity_resolver.py:488`. Pura sobre el span (índice cargado en `__init__` desde `aliases_db.json`; sin red, sin reloj). Devuelve `method` (`exact`, `fuzzy(N)`, `like`, `rewrite_unresolved`, `generic_buyer_unqualified`…) y `confidence`. El host sólo admite `exact`/`exact_official`/`exact_normalized`/`strict_*` (`foundation_admission.py:106-108`) y luego exige `project_buyer_names(canonical, dataset)` (`buyer_source_names.py:342`, singleton de sólo lectura).
Los resolvers de ecosistema `sena_resolver.resolve_sena_reference` / `icbf_resolver.resolve_icbf_reference` responden bien «paraguas» sobre el span (BUYER-117/118: SENA→`sena_national`, 3 nombres; ICBF→`icbf_national`, 33 nombres) y **el host no los consulta**.

**Batería (122 sondas).** 69 de autoridad + 50 de *host chain* + 3 barridos; incluye los casos pedidos: INVIAS (3 formas), ANI, 11 de gobernaciones (9 con ficha; Cundinamarca y Valle sin ficha raíz), 5 ministerios, 6 universidades (Nacional, de Antioquia, del Atlántico, Distrital, de Cartagena, Pedagógica), SENA (4), ICBF (4), 9 alcaldías (Barranquilla, Soledad, Bogotá, Medellín, Cali, Cartagena, Santa Marta, Villavicencio, San Gil), hospitales/ESE (5), Fondo Adaptación, IDU, EPM, DIAN, IDRD, ITM, INDER, genéricos desnudos.

**Evidencia.**
- Barrido completo del gazetteer (BUYER-120): 137 aliases con >1 owner; **136 resuelven `exact`/`high`** a un owner arbitrario (1 `rewrite_unresolved`). Ejemplos: `invias`→ANI (la ficha ANI trae `invias`, `el invias`, `el instituto nacional de vias` como alias propios), `idrd`→Secretaría Distrital de Cultura, `itm`→Instituto de Turismo del Meta, `inder`→INDER Envigado.
- Wrong-owner silencioso (como INVIAS→ANI): BUYER-001/003/005 (INVIAS, invías, el INVIAS → `ANI`), -055/-057/-059/-061 (SENA, Servicio Nacional de Aprendizaje, SENA Regional Antioquia, SENA Atlántico → una unidad al azar con 12/4/4/2 owners), -063/-065/-067 (ICBF, Instituto Colombiano de Bienestar Familiar, ICBF Bogotá → ICBF Tolima / Bogotá / Cundinamarca), -086/-088 (ESE Hospital San Rafael → Facatativá, 3-6 owners), -107 (IDRD), -109 (ITM), -111 (INDER).
- **Alcanzable hoy en el host**: BUYER-064 (`ICBF` → `nombre_entidad = 'ICBF REGIONAL TOLIMA'`), -068 (`ICBF Bogotá` → ICBF Cundinamarca), -108 (`IDRD` → `nombre_entidad = 'SECRETARÍA DISTRITAL DE CULTURA, RECREACIÓN Y DEPORTE'`). INVIAS/SENA/ITM/INDER/hospitales quedan `GATED` por `BUYER_SOURCE_PROJECTION_REQUIRED`: protección accidental, no una guarda de identidad.
- Correctos y ejecutables hoy: Universidad del Atlántico, ICBF Regional Atlántico, Alcaldía de Barranquilla, de Soledad, de Cartagena. Alcaldías de Bogotá/Cali/San Gil: declinan (`rewrite_unresolved`; no hay ficha raíz). Medellín: la ficha existe pero `fuzzy(90)` la descarta la puerta (FAIL_LOUD). Genéricos (`hospital`, `ESE Hospital`, `Secretaría de Salud`, `Universidad`, `Policía Nacional`, `Fondo Adaptación`): la autoridad devuelve un vecino `fuzzy(85-95)`/`medium`; sólo la puerta de `method` del host lo contiene (GATED).
- **Cobertura de ejecución (el otro cuello)**: sólo 579 de las 1096 fichas del gazetteer tienen *binding* de nombres físicos (593 en el sidecar). De 35 compradores con ficha correcta en la batería, **5 se ejecutan** en el host; 30 mueren en `BUYER_SOURCE_PROJECTION_REQUIRED` (INVIAS, ANI, Gobernaciones, ministerios, UdeA, UNAL, IDU, EPM, DIAN, Contraloría…: ninguno tiene *binding*). Es seguro (loud) pero es el techo real de «Foundation dueño de SEARCH» para compradores.
- 129 aliases de un solo owner resuelven `rewrite_unresolved` (falso negativo seguro).

**Arreglo (sin hacerlo).** `entity_resolver.py` (Tier 1 debe leer `owners_by_alias`; >1 owner ⇒ ambiguo/aclarar, y paraguas ⇒ ecosistema); `aliases_db.json` (aliases de INVIAS duplicados en ANI); `buyer_source_names.json` (517 fichas sin *binding*: trabajo de datos/adjudicación ADR-073). Tamaño: S-M de código, M de datos.

**Falta para Foundation.** Una función pura `resolve_buyer(span) -> {RESOLVED|AMBIGUOUS|UNRESOLVED, owners, canonical}` que respete `owners_by_alias` y consulte el ecosistema; *bindings* para los nacionales/departamentales.

## 2. ACTOR — FIX_REQUIRED

**Puntos de entrada.**
- `ActorNameResolver.resolve_exact(name)` / `.resolve(name) -> ActorResolutionResult(status, identity, method, candidates)` — `app/data/actor_alias_resolver.py:87` / `:78`. Determinista; **lee SQLite por llamada** (índice de 660 MB; el default de `Settings` es relativo `data/…`: sin `data/` en el worktree cae a `INDEX_UNAVAILABLE` sin fallback). `resolve` abre además FTS (sólo sugerencias).
- `actor_identity.normalize_identity(raw, document_type=None) -> IdentityMatch` — `app/core/actor_identity.py:414`. Pura. No interpreta el marcador «NIT» (lo hace `QueryRouter._consume_actor_document` sobre texto crudo).
- `resolve_actor_params(raw_query, parsed_params, actor_name_resolver, negation_requests=())` — `app/core/workflow/actions/actor_resolution.py:109`. **No es llamable con evidencia estructurada**: exige el texto crudo y el parse de `QueryRouter`, instancia `QueryRouter()` por dentro (`:87`) y lee el flag global `actor_profile_enabled()` (`get_settings`).

**Batería (59 sondas):** 36 nombres (positivos, variante de forma societaria, marca sin forma societaria, homónimos, genéricos, topónimos, nombres de comprador, centinela, vacío), 9 documentos (DV válido/falso, sin DV, marcador, rango, centinela, forma de teléfono, CC declarada), 10 vía `resolve_actor_params` sobre texto, 2 de host, 2 barridos.

**Evidencia.**
- Sano: razones sociales completas resuelven bien (`Caja de Compensación Familiar Compensar`, `Universidad de Antioquia`, `Cafam`, `Caracol Televisión S.A.`, `Seguros del Estado S.A.`…), la variante de forma societaria (`SAS`≡`S.A.S.`), y los homónimos reales se declaran `AMBIGUOUS` (Universidad Libre, Banco Popular, Municipio de San Luis, Fundación Vida Silvestre).
- **FAIL_SILENT latentes (11):** ACTOR-022…025 (`Consorcio`→NIT 901802139, 2 obs.; `Hospital`, `salud`, `construcción` → un NIT cada uno, 1-12 obs.), -029/-030 (`Barranquilla`, `Huila` → cédula de persona natural), **-034** (`Empresa de Acueducto y Alcantarillado de Bogotá` → cédula de persona natural con 1 observación; el NIT real, 899999094, vive bajo el alias con «- E.S.P.» y 7 obs.), -040 (`860.066.942-4`, DV falso, queda `NIT` con `confidence=inferred_from_check_digit`; sólo el `reason_code` lo delata), -044 (un teléfono de 10 dígitos pasa como documento plausible), -053 (`proveedor Consorcio` en legacy ya produce `contratista=901802139`), -058 (barrido: de 33 departamentos + 1022 nombres de municipio DIVIPOLA + 27 palabras temáticas, **40 resuelven `RESOLVED`**, 15 de ellos a cédulas de persona natural).
- Razón social sin marcador (la brecha conocida): `Compensar`, `Colsubsidio`, `Caracol Television`, `Seguros del Estado` → `exact`=`UNRESOLVED` (el `resolve` completo da `CANDIDATES`, sólo sugerencias); en texto `contratos de codetec en 2026` queda como TEMA (`objeto=['codetec']`). Es FAIL_LOUD en Foundation, silencioso sólo mientras el parse lo trate como tema.
- Host: `UNRESOLVED_HOST_DIMENSION:ACTOR` (GAP): no hay rama de admisión ni proyección `contratista`.

**Arreglo.** `actor_alias_resolver.py` (guarda de nombres genéricos/topónimos/solo-forma-societaria, soporte mínimo de observaciones), `actor_alias_index.py` + reconstrucción (política de exclusión), `actor_identity.py` (DV falso no debe quedar `inferred_from_check_digit`), y la rama ACTOR en `foundation_admission.py` + `foundation_projection.py`. Tamaño M.

**Falta para Foundation.** Rama host ACTOR; entrada para documento citado sin pasar por `QueryRouter`; una guarda de genericidad; el contrato M1.0 §4 (`ACTOR is`) no tiene hoy autoridad llamable.

## 3. GEOGRAPHY — FIX_REQUIRED

**Puntos de entrada.** `department_mention_resolution(token) -> GeoMention(kind, canonical, confidence)` — `entity_resolver.py:129` (pura; sin precedencia de ciudad). `get_municipal_territories().lookup(name, department=None) -> tuple[MunicipalTerritory,…]` — `municipal_territory.py:79` (pura; **devuelve candidatos, no decide**). Regiones: `REGION_ALIASES`/`ZONES_BY_NAME` (`colombia_geography.py:97,87`), **datos sin función pública**. `MunicipalTerritoryRegistry.consume(text)` (`:101`) lee texto crudo (`en X`).

**Batería (70 sondas):** 12 departamentos, 4 declinaciones, 4 homónimos dpto/municipio, 10 municipios únicos, 8 homónimos municipales (+1 desambiguado por departamento), 4 nombres coloquiales, 1 topónimo-país, 3 del gazetteer legacy, 8 regiones + 3 declinaciones + 1 homónimo región/departamento, 1 barrido de catálogo, 10 de host.

**Evidencia.**
- Sano: departamentos (Norte de Santander≠Santander, Bogotá→Distrito Capital, Guajira), homónimos municipales detectados (San Luis ×2, San Francisco ×3, La Unión ×4, Córdoba ×3, Armenia ×2…; 64 nombres con >1 municipio, 146 municipios en grupos homónimos) y desambiguación por departamento.
- **FAIL_LOUD:** `Cali`, `Cartagena`, `Tumaco`, `Bogotá` no resuelven en DIVIPOLA (sólo se indexa el nombre oficial, `municipal_territory.py:61`: `SANTIAGO DE CALI`, `CARTAGENA DE INDIAS`, `SAN ANDRÉS DE TUMACO`); el gazetteer legacy sí los reconoce.
- **FAIL_SILENT latentes:** GEOGRAPHY-044 (`Colombia` → municipio 41206 del Huila; `consume('contratos en colombia')` lo devuelve `status=resolved`), -045/-046 (`geo_mention_resolution('Armenia'|'San Andrés')` → `city`/`high` sin desambiguar: segunda autoridad municipal con confianza alta).
- Homónimo departamento/municipio (Córdoba, Nariño, Boyacá, San Andrés): la autoridad es correcta **sólo si el nivel llega tipado**; `ProposedCondition` no tiene `level` (`interpretation_contract.py:196`) y el host asume DEPARTAMENTO (`foundation_admission.py:101-104`).
- Host: sólo departamentos. `Barranquilla`, `Soledad`, `Caribe`, `Eje Cafetero`, `municipio de Córdoba`, `San Luis, Tolima` → `UNRESOLVED_HOST_DIMENSION:GEOGRAPHY` (GAP; loud). `consume` ya produce `ciudad`+`departamento_resolved` en legacy; no existe proyección municipal/región en `foundation_projection.py`.

**Arreglo.** Alias coloquiales y política de homónimos en `municipal_territory.py` + `municipal_territories_v1.json`; campo `level` en `interpretation_contract.py` (+ prompt v5); ramas MUNICIPALITY/REGION en admisión y proyección. Tamaño M.

**Falta para Foundation.** MUNICIPALITY no tiene autoridad pura que decida; REGION no tiene función; falta `level`; falta proyección.

## 4. TIME — FIX_REQUIRED

**Punto de entrada.** `resolve_temporal_scope(text, *, today=None, protected_spans=()) -> TemporalResolution(matched, fecha_desde, fecha_hasta, precision, error_code, clarification)` — `temporal_semantics.py:136`. Pura **con `today` inyectado**; sin él lee `date.today()` (`civil_today`, `:73`; TIME-054). Opera sobre el span. **El host no la llama**: usa su propia regex (`foundation_admission.py:109-111`).

**Batería (65 sondas).** 47 frases (años, meses, rangos, abiertos, relativos, comparadores, trimestres, inválidas), 6 con reloj inyectado distinto (año nuevo, bisiesto, semana dom/lun), 1 de reloj por defecto, 11 de host.

**Evidencia.**
- Sano: años, mes+año, rangos mes/año, abiertos `desde/hasta`, relativos con reloj inyectado (`este mes`, `el mes pasado`, `hace 3 meses`, `esta semana`, `últimos 30 días`; cruce de año y bisiesto correctos), fechas inválidas y rangos invertidos se aclaran.
- **FAIL_SILENT (autoridad, latentes):** TIME-034…039 (`después de 2022`, `antes de 2024`, `a partir de 2023`, `posteriores a 2022`, `de 2024 a la fecha`, `desde el año pasado` → el año completo, ignoran el comparador: `closed_year_re` casa «de 2024», `temporal_semantics.py:391-401`), TIME-040 (`antes de marzo`), TIME-041/042 (`primer trimestre`/`segundo semestre de 2025` → todo 2025; la rama `unresolved` de trimestre/semestre `:403` es inalcanzable con año), TIME-016 (`del 1 al 15 de marzo de 2025` → sólo el 15, `:235`), TIME-006 (`septiembre del 2025` → septiembre de **2026**: `month_re` `:363` no acepta `del 2025`).
- **FAIL_LOUD:** `últimos 6 meses`, `los últimos 3 años`, `hace 2 años`, `la última semana` → `matched=False` (el host debe tratarlo como no resuelto).
- **FAIL_SILENT alcanzable hoy (host):** TIME-056…059: con `during_year`, `antes de 2024` → `fecha_de_firma >= '2024-01-01' AND < '2025-01-01'`; `después de 2022` → año 2022; `desde 2023` → sólo 2023; `primer trimestre de 2025` → 2025. El host acepta cualquier cita con un solo año para `during_year` y, a diferencia de VALUE, no verifica operador contra cita.
- GAP: `during_month`, `between_dates`, `relative_period` → `UNRESOLVED_HOST_DIMENSION:TIME` (TIME-061…064); `during_year` NEGATIVE → `UNSUPPORTED_CONDITION`.

**Arreglo.** `temporal_semantics.py` (comparadores como parámetro, rango de días de un mes, trimestre/semestre, `del <año>`, últimos N meses/años); `foundation_admission.py` (usar la autoridad y verificar operador vs cita); `foundation_projection.py` (los tres operadores con reloj inyectado). Tamaño M.

**Falta para Foundation.** `resolve_temporal_scope(span, operator, today)`; el contrato M1.0 exige `during_month/between_dates/relative_period` y el prompt v4.2 sólo conoce `during_year` citando el año desnudo.

## 5. MONEY — FIX_REQUIRED

**Puntos de entrada.** `money_to_cop(raw_text) -> int|None` — `utils/money.py:83` (pura). `operator_reason(operator, text) -> str|None` — `foundation_value_direction.py:57-59` (guarda operador vs cita; pura). `is_admissible_amount(amount, span)` — `money_admission.py:75` (piso anti-ruido; **el host no la llama**). `resolve_money_polarity(text)` (`money_polarity.py:145`) lee texto.

**Batería (64 sondas):** 25 conversiones (escalas es-CO), 23 operador-vs-cita, 5 de vocabularios divergentes, 11 de host.

**Evidencia.**
- Sano: `500 millones`, `1,5 millones`, `1.5 mil millones`, `tres mil millones`, `$500.000.000`, `500,000,000`, `200 palos`, `2 billones`, `mil quinientos millones`; SMMLV/`un millón y medio`/`medio millón` → `None` (se aclara); comparadores exclusivos («más de», «superior a», «por encima de», «menos de»…) bien verificados; contradicciones simples (`greater_than` + «menos de») se aclaran.
- **FAIL_SILENT alcanzables (host):** MONEY-058/059 (`que no superen 300 millones`, `no mayor a 300 millones` con `greater_than` → `valor_del_contrato > 300000000`, inversión; MONEY-040…042 en la autoridad: `directions()` no lee la negación salvo «no más de/no menos de»), MONEY-056 (`más de 5` → `valor_del_contrato > 5`: el host admite cualquier cifra ≥ 0 sin unidad, `foundation_admission.py:117-118`).
- **FAIL_SILENT latentes (inclusivos):** MONEY-045/046: `operator_reason('greater_than_or_equal', 'menos de 300 millones')` → `None`; la guarda no lee la cita para cualquier operador fuera de {greater_than, less_than} (`:59`). Al activar D4 (inclusivos se proyectan) queda ciega.
- `money_to_cop` no es seguro por rango: `entre 100 y 500 millones` → 100.000.000, `100 millones y 200 millones` → 100.000.000, `-5 millones` → 5.000.000 (GATED: el host exige comparador compatible antes de convertir).
- Dos léxicos para el mismo comparador (`money_polarity._UP/_DOWN` conoce *mínimo/máximo/rebasan*; `value_direction._MARKERS`, no): MONEY-049/050/053.
- GAP: `greater_than_or_equal`/`less_than_or_equal` → `UNSUPPORTED_CONDITION` (`foundation_projection.py` sólo `valor_{min,max}_exclusive`, :313).

**Arreglo.** `foundation_value_direction.py` (negación + léxico unificado + inclusivos), `foundation_admission.py` (piso), `foundation_projection.py` (`>=`/`<=`). Tamaño S-M.

## 6. MODALITY — FIX_REQUIRED (el defecto está en el host, no en el resolver)

**Punto de entrada.** `modality_resolver.resolve(text, dataset=None) -> ModalityResolution(status, includes, excludes, issues, assumption_codes)` — `modality_resolver.py:403`. Pura (singleton de catálogo/lexicón YAML). Devuelve `strength` (`exact_legal`/`colloquial`…), `confidence`, polaridad, `status` (`resolved`/`resolved_with_assumption`/`ambiguous`…).

**Batería (37 sondas):** 23 de resolver (positivas, negación, O6 coloquial, no-modalidad, OR, argot), 3 de consistencia entre datasets, 11 de host.

**Evidencia.**
- Resolver sano: los 10 nombres legales resuelven `exact_legal`/`high`; `subasta` desnuda es ambigua (`BARE_SUBASTA_MULTIPLE_LEGAL_MEANINGS`); `no por/sin/excepto/que no sean de` → `excludes`; `licitación(es)` coloquial sale `resolved_with_assumption` con `strength=colloquial` (O6 señalado); `convenio`/`convocatoria` → `no_match`. Resultado idéntico en `procesos` y `contratos`.
- Defecto del resolver: MODALITY-022 `directa o licitación` devuelve sólo `licitacion_publica` (la rama «directa» se pierde sin `issue`).
- **FAIL_SILENT alcanzables (host), todos con `requires_producer_error`** (el prompt v4.2 M3/M4/E1 impide que el productor los proponga así, pero el host no verifica por su cuenta): MODALITY-029/030 (`licitaciones`/`licitación` POSITIVE → `modalidad_de_contratacion IN ('Licitación pública','Licitación pública Obra Publica')`), -031/-032 (cita con negación embebida y polaridad POSITIVE → ejecuta directa / licitación: inversión), -033 (`directa o licitación` → sólo licitación pública). Causa: `foundation_admission.py:122-124` exige `not issues and len(matches)==1` y mezcla `includes+excludes` sin comparar `strength`, `status` ni polaridad del match con la de la condición.
- GAP: modalidad NEGATIVE → `UNSUPPORTED_MODALITY_POLARITY` (`foundation_projection.py:203`).

**Arreglo.** Sólo `foundation_admission.py` (rama MODALITY) y la polaridad negativa en `foundation_projection.py`. Tamaño S.

## 7. CONTRACT_UNIVERSE — HEALTHY (huecos de cobertura loud)

**Puntos de entrada.** `contract_universe.eligible/stamp/predicate/conflict_message/universe_from_params` — `contract_universe.py:34-90` (puras sobre fila/params). `contract_type_resolver.resolve_contract_types(text)` — `:118` (pura sobre el span). `contract_universe_intent.request(text)` — `:39`, **lee el turno crudo** (regex, `split_clauses`, `source_occurrence`): no es llamable con evidencia.

**Batería (76 sondas):** 20 de elegibilidad por estado, 4 de `stamp`, 1 de versión, 5 de conflicto estado-vs-universo, 17 de petición explícita de registros (texto), 25 de tipo/naturaleza, 1 cobertura contra los valores reales de SECOP, 3 de host.

**Evidencia.** Las 20 filas de elegibilidad, los `stamp`, el rechazo de versión distinta, los conflictos y las 17 peticiones de registros pasan; la negación nunca ensancha el universo (`contratos que no estén cancelados`→`None`). Tipo/naturaleza: los 12 patrones enumerados resuelven bien; el sustantivo desnudo (`suministro`, `obra`, `compraventa`) es TEMA por diseño.
- FAIL_LOUD (seguro): tipos reales de SECOP sin patrón: `obra pública` (el contrato M1.0 §4 la lista como tipo; el prompt v4.2 T4/K1 la manda a TOPIC: CONTRACT_GAP), `Consultoría`, `Interventoría`, `Comodato`, `Seguros`, `Concesión`; 6 de 24 valores de `tipo_de_contrato` tienen patrón (91,9 % de las filas de contratos).
- FAIL_SILENT (LOW): `prestación de servicios profesionales` → el host usa `include_ids[0]` y descarta el residuo `profesionales` (`foundation_admission.py:138-139`): ensancha a todo el tipo.
- INFO: un `Aprobado` con `fecha_de_firma=1900-01-01` cuenta como firmado (la fecha es calendario-válida; política V1.1).
- GAP: `SOURCE_RECORDS` (estados excluidos: cancelados, borrador) no es hospedable: la proyección fija `CELEBRATED`; la decisión hoy se toma leyendo texto.

**Arreglo (opcional).** `contract_type_resolver.py` (tipos faltantes), `foundation_admission.py` (residuo no vacío ⇒ aclarar). Tamaño XS-S.

## 8. STATE — FIX_REQUIRED

**Puntos de entrada.** `estado_families.resolve_estado(token, dataset) -> {family, filters, kind}|None` — `estado_families.py:392` (pura; **exige token ya plegado**). `read_process_state_mentions(text)` (`:527`). Lo que el host usa hoy: `foundation_admission.state_value(condition)` — `:30-39`, tres patrones (abiertos/publicados/terminados).

**Batería (36 sondas):** 28 de autoridad (exacto vs familia, vigencia por dataset, tildes, conflictos de léxico, dataset equivocado, aproximaciones), 8 de host.

**Evidencia.**
- Sano (ADR-067/068): `publicados`→`Publicado` exacto, `abiertos`/`vigentes`→familia, `vigentes` dataset-aware (procesos→funnel; contratos→En ejecución+Modificado+Prorrogado), literales en minúscula de SECOP (`terminado`, `cedido`), `publicados` sobre contratos y `terminados` sobre procesos no resuelven (correcto).
- **FAIL_LOUD:** STATE-015/016 `en ejecución`, `en evaluación` con tilde → `None` (`token_lower = natural_token.lower().strip()`, `:405`, no pliega tildes; sólo `en ejecucion` resuelve); STATE-018/019/020 `liquidados`, `en curso`, `finalizados` → `None` aunque `ESTADO_CONTRATO_SYNONYMS` los mapea (tres catálogos divergen).
- **FAIL_SILENT:** STATE-017 `adjudicados` (procesos) → familia `en_evaluacion` (Evaluación+Seleccionado+Aprobado+En aprobación) pese a que `ESTADO_PROCEDIMIENTO_SYNONYMS` dice Seleccionado (`estado_families.py:174-175`); latente. STATE-036 (host): `terminados anticipadamente` → `estado_contrato = 'terminado'`.
- INFO: `desiertos` se proyecta a Cancelado+Suspendido sin marcarse aproximado; `adjudicados` en contratos proyecta estados fuera del universo celebrado (lo frena `conflict_message`).
- GAP (capacidad existente no conectada): STATE-031/033/034/035 — `cancelados` (procesos), `cerrados`, `en ejecución`, `vigentes`: `resolve_estado` los resuelve y el host los declina.

**Arreglo.** `estado_families.py` (plegar tildes; unificar `adjudicado`), `foundation_admission.py` (`state_value` → `resolve_estado` + verificar cita completa). Tamaño S.

## 9. Orden sugerido (no se arregló nada)

1. **Defensa del host sobre lo que ya devuelven las autoridades** (barato, alto valor): MODALITY (status/strength/polaridad), STATE (usar `resolve_estado`), VALUE (negación, piso, inclusivos), TIME (operador vs cita) — todos en `foundation_admission.py`/`foundation_projection.py`.
2. **BUYER**: Tier 1 por `owners_by_alias` + ecosistema SENA/ICBF; después la cobertura de *bindings* (dato, es el techo de ejecución).
3. **TIME** (autoridad) comparadores/trimestres/`del <año>` y **GEOGRAPHY** (alias coloquiales, `level`).
4. **ACTOR**: guarda de genericidad + rebuild + rama host (trabajo nuevo, no arreglo).

## 10. Límites de la medición

- Sin red: no se verificó contra SECOP en vivo que un nombre físico exista ni cuántas filas devuelve; los oráculos son el propio gazetteer/DIVIPOLA/léxicos del repo y conocimiento de dominio. El juicio «wrong owner» de BUYER se apoya en `owners_by_alias` (evidencia objetiva del repo), no en mi criterio.
- Sin LLM: la columna *host chain* construye la propuesta de una condición a mano; las sondas con `requires_producer_error: true` sólo se alcanzan si el productor se equivoca (el prompt v4.2 lo prohíbe). Esa defensa en profundidad falta en el host, pero el riesgo real depende del productor v5.
- El directorio de actores es el artefacto del 2026-08-20; los resultados de ACTOR dependen de él (ruta absoluta o `SECOP_ACTOR_ALIAS_INDEX_PATH`).
- No se midió legacy/Foundation extremo a extremo (eso es A3/A4); esto es salud de autoridades.

## Anexo HERA — verificación de punta a punta con el productor real (2026-09-30)

Productor DeepSeek real (prompt v4.2, temperatura 0, 2 pasadas por consulta) → H7 → `admit_proposal`
→ `project_accepted`, sin ejecutar contra SECOP. Script: scratchpad de la sesión (no versionado).

| Consulta | Resultado real | Veredicto |
|---|---|---|
| «contratos del ICBF en 2025» | READY → `nombre_entidad = 'ICBF REGIONAL TOL…'` (2/2) | **CONFIRMADO P0**: comprador equivocado en silencio, alcanzable hoy con `foundation_v3` |
| «contratos de aseo desde 2023» | READY `during_year 2023` (2/2) | **CONFIRMADO P1**: cota abierta colapsada a un año |
| «contratos de aseo antes de 2024» | pasada 1 READY `during_year 2024`; pasada 2 CLARIFY | **CONFIRMADO P1** (inestable): ejecuta el año opuesto a lo pedido |
| «contratos de aseo de septiembre del 2025» | READY `during_year 2025` (2/2) | **CONFIRMADO P1**: mes ensanchado a año (mismo patrón que 7 consultas REAL de A3) |
| «contratos de aseo que no superen 300 millones» | el productor propone `less_than` (correcto) y la guarda lo rechaza: CLARIFY (2/2) | **NO es inversión alcanzable**: es aclaración falsa (P2). La inversión de MONEY-058/059 sólo ocurre si el productor yerra |
| «contratos de aseo no mayores a 300 millones» | `less_than` citando «mayores a 300 millones» (sin el «no») → CLARIFY | ídem; además la cita omite la negación |
| «contratos del SENA en 2025» | READY → `BUYER_SOURCE_PROJECTION_REQUIRED` | techo de BUYER confirmado |

Reclasificación: MONEY pasa de «6 alcanzables» a 0 alcanzables con el productor actual + falsas
aclaraciones; BUYER y TIME se confirman alcanzables en dogfood.

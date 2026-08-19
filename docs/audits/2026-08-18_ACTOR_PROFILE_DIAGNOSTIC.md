# ACTOR PROFILE — DIAGNOSTIC 2026-08-18

**Tipo:** auditoría READ-ONLY previa a implementación (AP0).
**Alcance:** viabilidad de "Perfil integral de proponente/contratista" sobre el SECOPPAL real de agosto de 2026.
**Regla aplicada:** el código, los tests y los ADR actuales mandan sobre la documentación histórica. Lo que un documento afirma pero el código no demuestra se clasifica `DOCUMENTED_ONLY`, no implementado.
**Prohibiciones respetadas:** cero cambios en `app/`, tests, datasets, dependencias, `.env`, Radar, Truth Layer o warehouse. Sin commit/merge/rebase/push.

---

## 1. Executive verdict

**GO — pero sobre un slice distinto del que propuso ADR-042, y por una razón distinta.**

El diagnóstico encontró tres cosas que reordenan la feature:

1. **Ya existe casi toda la maquinaria de la "huella contractual".** El parámetro `contratista` es ciudadano de primera clase del pipeline: lo produce el parser desde un NIT desnudo, viaja en `QueryFrame.modifiers`, pasa el guard anti-`WHERE 1=1` (`SCOPE_TOPIC_KEYS`), habilita la capa analítica (`has_analytics_scope`) y se traduce a `documento_proveedor = '…'` en el SoQL de contratos. Compradores, años, modalidades, valor y rango de fechas por NIT son composiciones de builders **que ya existen** (`build_top_entities`, `build_temporal_dist`, `build_top_modalities`, `build_value_stats`, `build_date_range`), todos filtrando por el mismo `_build_where`.

2. **Y sin embargo, hoy el sistema miente en las dos consultas centrales de la feature.** Verificado emitiendo el SoQL real (§11):
   - **Un nombre de proveedor nunca llega al campo del proveedor.** `"contratos de FUNDACARIBE"` produce `objeto: ["fundacaribe"]` → `LIKE '%fundacaribe%'` sobre `objeto_del_contrato`/`descripcion_del_proceso`. Los contratos de FUNDACARIBE cuyo objeto no la nombre son invisibles; los contratos de terceros que la mencionen se atribuyen a ella.
   - **Un documento se descarta en silencio en el dataset equivocado.** `DatasetSpec` de procesos tiene `contractor=None`, y `_build_where` filtra `if params.get("contratista") and spec.contractor`. `"qué ha celebrado la persona 1140835135 en 2025"` cae a `dataset=procesos`, **pierde la cédula** y ejecuta una búsqueda por la palabra `"persona"` en 2025. La respuesta se presentaría como el historial de esa persona.

   Estas dos son exactamente el "riesgo legal, no bug" que ADR-042 identificó — solo que no viven en el JOIN entre fuentes externas, sino **dentro del pipeline actual, hoy, sin ninguna fuente externa involucrada.**

3. **Nada del enriquecimiento jurídico existe.** Cero menciones de RUES, RUP, SIRI, responsabilidad fiscal o Grupos de Proveedores en `app/`, `scripts/` o `tests/`. Y el warehouse **no puede ayudar**: no contiene contratos, no contiene ningún campo de proveedor, y su propio veredicto W0D es `primary_source = NO_GO`.

**Conclusión.** El menor cambio coherente que entrega valor real no es un dossier: es **cerrar el circuito actor→documento→contratos con honestidad de atribución**. Eso vale por sí solo (responde "qué ha contratado X" correctamente por primera vez), es reversible, y es precondición estricta de todo lo jurídico. El dossier RUES/SIRI/fiscal es un bloque posterior, no el V1.

---

## 2. Git baseline

```
BRANCH:     main
HEAD:       2403ebed12eeec243ff195e87496885fc6bdc8bd
MAIN_HEAD:  2403ebed12eeec243ff195e87496885fc6bdc8bd   (idénticos)
```

Últimos commits:

```
2403ebe (HEAD -> main, feature/money-v2) docs(money): reconcile final patch audit
c44b28f fix(money): close final follow-up authority gaps
1fef027 docs(money): close money v2 arc
7aacb5f feat(money): expose monetary filter interpretation
1432140 feat(money): resolve SMMLV with explicit year semantics
0d60b3d feat(money): parse practical Spanish monetary cardinals
aa51cb4 fix(money): arbitrate money and identifiers
245147c feat(money): establish money v2 numeric foundation
2d4e24a fix(search): align opportunity deadlines to calendar dates
11ddf11 feat(search): prioritize current opportunity candidates
81993dd fix(search): complete operational process state rendering
d622c84 feat(search): show operational process state
```

Working tree previo a esta auditoría — **87 entradas, ninguna tocada**:

| Clase | Contenido |
|---|---|
| `M` (2) | `docs/decisions_log.md`, `docs/documento_tecnico_secoppal_arquitectura_bitacora.md` |
| `??` docs | `docs/architecture/`, `docs/archive/` (directorios sin trackear) |
| `??` data | `data/conversations/*.jsonl` + `.pagination.json` (artefactos de dogfood), `data/.fuse_hidden*` (~50, residuo de FUSE) |
| `??` raíz | `Icon\r` (residuo de macOS/Drive) |

**Nota de numeración ADR.** `docs/decisions_log.md` llega a ADR-058; existe `docs/decisions/2026-08-18_ADR-059_money-v2-deterministic-authority.md` **aún no reflejado en el log** (`grep -c ADR-059 docs/decisions_log.md` → 0). El máximo comprometido observado sobre los ~21 worktrees es **ADR-059**. El ADR de esta feature será **ADR-060 o superior**, reconciliado contra el máximo global al momento de escribirlo — **no** ADR-042. El número 042 quedó libre porque el ADR nunca se comprometió; reutilizarlo ahora confundiría el orden temporal de decisiones.

**Worktrees relevantes:** 22 activos. `warehouse/*` (13 de ellos) contienen todo el trabajo de warehouse; `exp/generic-enrichment-runtime-v1` el de enriquecimiento genérico. **Ninguno está fusionado a main.**

---

## 3. What ADR-042 got right

Sigue vigente, y en varios puntos la evidencia nueva lo **refuerza**:

| # | Afirmación del ADR | Estado | Evidencia |
|---|---|---|---|
| 1 | **La identidad es el spine, no un detalle de preprocesamiento** | **VIGENTE Y AGRAVADO** | Confirmado dos veces por evidencia interna: el pipeline actual pierde el documento en procesos y nunca lo obtiene desde un nombre (§11). El riesgo se materializa **sin** fuentes externas. |
| 2 | **INV-ID-TYPE:** dos números iguales de tipos distintos no se fusionan | **VIGENTE** | Nada en el repo distingue tipo documental. `_extract_nit` (`query_router.py:1375`) acepta 8–11 dígitos sin decidir si es NIT o cédula; el resultado va a `documento_proveedor` sin tipo. |
| 3 | **El tipo documental nunca se decide por longitud de dígitos** | **VIGENTE** | Correcto en el dominio (cédula y NIT se solapan). Y existe una fuente con **columna explícita de tipo** que el ADR no citó: SECOP I `f789-7hwg` expone `tipo_identifi_del_contratista` junto a `identificacion_del_contratista` (`docs/secop1_procesos/FIELD_MAPPING.md:56-58`). Es la señal preferente del orden que el propio ADR propuso. Dataset **no integrado** (`AUDIT ONLY`). |
| 4 | **"No encontré" ≠ "está limpio"** | **VIGENTE, y parcialmente implementado** | `app/core/outcome.py` ya separa `EMPTY` ("0 filas **confirmadas** por la fuente") de `ERROR/FETCH_TIMEOUT` y `ERROR/SOURCE_UNAVAILABLE`. La doctrina ya es código en producción; falta extenderla a fuentes por actor. |
| 5 | **Transporte Socrata ≠ normalización SECOP** | **VIGENTE, sin avance** | `SecopClient.query()` sigue aplicando `_normalize_row()` incondicionalmente (`secop_client.py:125`), y esa normalización es lógica SECOP pura: reparación de `urlproceso` con prefijos `CO1.NTC`/`CO1.REQ` y coerción de `precio_base`/`valor_del_contrato`. Meter RUES o SIRI por ahí corrompe datos. |
| 6 | **Fan-out concurrente con presupuesto por fuente y ensamblaje parcial** | **VIGENTE — y ya no requiere decisión** | Ver §4.2: el patrón existe en producción. |
| 7 | **INV-PII:** sin domicilio, correo ni teléfono personal en el render | **VIGENTE, y hoy se cumple de facto** | `documento_proveedor` se transporta en `select_fields` pero **no se renderiza en ninguna parte**: cero ocurrencias en `formatter.py`, `narrator.py`, `streamlit_app.py`. El formatter solo expone `proveedor_adjudicado` como `contratista` (`formatter.py:424`). El estado actual ya es el estado deseado — falta convertirlo en política explícita antes de añadir fuentes con PII. |
| 8 | **Riesgo D (integrantes de UT) resuelto por el dataset de Grupos** | **VIGENTE como diseño, NO verificado** | Ninguna línea de código consulta ese dataset. La estructura descrita (`nit_grupo`, `nit_participante`, `participacion`, `es_lider_del_grupo`) es plausible pero **no está verificada contra el esquema vivo** en este repo. Sigue siendo E0. |
| 9 | **Descartar embeddings para nombre→identidad** | **VIGENTE** | Decidir si `860034594-1` y `8,600,345,941` son la misma persona jurídica es normalización, no semántica. El repo además ya adoptó determinismo en la capa monetaria por la misma razón (ADR-059). |

---

## 4. What ADR-042 is now obsolete

### 4.1 "RapidFuzz **no está** hoy en el proyecto (se verificó)" — **FALSO HOY**

```
requirements.txt:            rapidfuzz>=3.10.0
app/core/entity_resolver.py:9        from rapidfuzz import fuzz, process
app/utils/spell_correction.py:13     from rapidfuzz.distance import DamerauLevenshtein
```

La "decisión de E2" (¿añadir dependencia o rerankear con la maquinaria existente?) **ya no existe**: RapidFuzz es dependencia declarada y se usa en dos módulos de producción. El rerank fuzzy no cuesta una dependencia nueva.

### 4.2 "La decisión de concurrencia SÍ cambia aquí" — **el patrón ya existe en producción**

El ADR argumenta que H3 se difirió en Sprint A y que un dossier justifica introducir fan-out. La premisa quedó atrás: `observer.py:211-231` ya ejecuta fan-out concurrente acotado **con exactamente la forma que el ADR pide**:

```python
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(
        secop_client.with_timeout(OBSERVER_AGG_TIMEOUT_PER_TASK).aggregate, dataset_id, q
    ): name for name, q in queries.items()}
    try:
        for future in as_completed(futures, timeout=OBSERVER_AGG_TIMEOUT_GLOBAL):
            ...  # fallo por tarea → agg_results[name] = None, no derriba el conjunto
    except FuturesTimeout:
        ...  # "using partial results"
```

Presupuesto por tarea + techo global + ensamblaje parcial + `partial=True` propagado. Además `deadline.py` ya expone `Deadline.fetch_budget_s(source_timeout_s, cap_s)` y `can_afford(min_s)`. **Actor Profile no diseña fan-out: lo reusa.**

### 4.3 "FTS5 local es el único componente que justifica copia local" — **premisa incompleta**

Cierto que no hay FTS5 (cero `VIRTUAL TABLE`/`fts5` en `app/` y `scripts/`). Pero el ADR planteó el índice como decisión aislada, y hoy hay dos hechos que la reencuadran:

- **Infra SQLite ya establecida.** `app/core/db.py::connect_sqlite` centraliza pragmas de producción (WAL, `foreign_keys=ON`, `busy_timeout`), y hay cuatro stores encima (radar, notify, conversation, feedback). Un índice de nombres es un store más, no infraestructura nueva.
- **El índice ya no es el único candidato a copia local**, porque apareció el warehouse. Pero el warehouse tampoco sirve para esto (§7), así que la conclusión final coincide con la del ADR **por razones distintas**.

### 4.4 "Capa C — Contratos: reúso del pipeline SECOP filtrando por `contractor_document`" — **subestima el trabajo**

El filtro existe (`soql_builder.py:494-502`) y funciona **en contratos**. Pero:
- se **descarta en silencio** en procesos (`spec.contractor is None`), y
- **no hay ninguna ruta** de nombre → documento.

Capa C no es "reúso", es "reúso + dos correcciones de atribución". Esas correcciones son el V1, no un preliminar.

### 4.5 El paisaje arquitectónico que el ADR no pudo conocer

| Novedad post-29-jun | Efecto sobre la feature |
|---|---|
| **Warehouse local (Parquet/DuckDB)** | **Ninguno.** No contiene contratos ni campos de proveedor, y vive fuera de main (§7). |
| **Truth Layer fusionado a main** | Aporta primitives reales de evidencia/conflicto/calidad — pero dormidas y acopladas a campos SECOP (§9). |
| **Radar (P0/P1B cerrados)** | Ya puede seguir un contratista sin cambios estructurales (§10). |
| **Workflow refactorizado a grafo Burr** | Los nodos son explícitos y componibles; añadir una rama de actor es aditivo, no invasivo. |
| **ADR-059 (money v2)** | Establece el precedente de *autoridad determinista con interpretación expuesta*, el patrón correcto para atribución de identidad. |
| **`_extract_nit` con arbitraje monto/identificador** | `_number_role(...) == "identifier"` ya evita confundir un NIT con un monto: base sólida para AP1. |

### 4.6 Numeración

"ADR-042" está **obsoleto como identificador**. Ver §2.

---

## 5. Existing capabilities inventory

Estados: `EXISTS_PRODUCTION` · `EXISTS_DORMANT` · `PARTIAL` · `DOCUMENTED_ONLY` · `ABSENT` · `UNKNOWN`

| Capacidad | Estado | Archivo(s) | Tests | Reutilizable | Observaciones |
|---|---|---|---|---|---|
| identidad tipada | **ABSENT** | — | — | — | No existe noción de `document_type`. Un número entra al SoQL sin tipo. |
| normalización NIT | **PARTIAL** | `query_router.py:1375-1411` (`_extract_nit`) | `test_query_router.py`, `test_money_identifier_arbitration.py` | **Sí, como semilla** | Detecta por forma, extrae dígitos, arbitra contra montos (`_number_role`). No normaliza a llave canónica ni valida DV. |
| NIT + DV | **PARTIAL** | ídem | ídem | Sí | Emite variantes `[base, completo]` **solo si el crudo traía separador antes del último dígito** (`re.search(r"[.\- ]\d$", raw)`). `8600345941` sin separador → una sola variante. No calcula ni valida DV módulo 11. |
| cédula | **ABSENT (peligroso)** | — | — | — | Una cédula entra como `contratista` y se aplica contra `documento_proveedor` sin distinción de tipo, o se descarta si el dataset es procesos. |
| ActorId equivalente | **ABSENT** | — | — | — | `result_identity.py` identifica **filas** (proceso/contrato), no actores. |
| aliases de actor | **ABSENT** | — | — | — | `aliases_db.json` (580 KB) es de entidades **compradoras** (`total_procesos`, `total_contratos`, `departamento`). Ningún alias de proveedor. |
| resolución por nombre | **ABSENT para proveedores** | `entity_resolver.py` | `test_query_router.py` | **Patrón sí, datos no** | `EntityResolver` es gazetteer-first + fuzzy sobre compradores. La arquitectura es directamente trasladable; el corpus no existe. |
| fuzzy matching | **EXISTS_PRODUCTION** | `entity_resolver.py:9`, `spell_correction.py:13` | sí | **Sí** | RapidFuzz `fuzz`/`process`/`DamerauLevenshtein`. |
| FTS5 | **ABSENT** | — | — | — | Cero `VIRTUAL TABLE`/`fts5`. Existe `db.py::connect_sqlite` con pragmas de producción como base. |
| direct lookup por documento | **ABSENT** | `direct_lookup.py` | `test_query_router.py` | Patrón sí | Resuelve `CO1.*`, URLs y referencias internas (`CD-08-2026-5534`). **No** documentos de persona. Verificado: las 8 queries de referencia dan `direct_lookup: None`. |
| contratos por `contractor_document` | **PARTIAL** | `soql_builder.py:169-170, 491-515` | `test_query_router.py` | Sí | Funciona en contratos; **se descarta en silencio en procesos**. |
| warehouse de contratos | **ABSENT** | — | — | No | Warehouse fuera de main y solo procesos (§7). |
| delta Socrata | **ABSENT en main** | — | — | No | Existe watermark/freshness en worktrees warehouse (`freshness.py`), no en main. |
| agregación por contratista | **EXISTS_PRODUCTION (composicional)** | `analytics/aggregate_sum.py`, `base.py::has_analytics_scope` | `test_query_router.py` | **Sí** | `contratista ∈ SCOPE_TOPIC_KEYS` → pasa el guard de scope. Verificado: `"cuanto suma lo contratado por el NIT …"` → `analytical_intent: aggregate_sum`. |
| agrupación por comprador | **EXISTS_PRODUCTION** | `soql_builder.py:519` (`build_top_entities`) | sí | **Sí** | `GROUP BY spec.entity` sobre `_build_where(params)` → respeta `contratista`. |
| agrupación por año | **EXISTS_PRODUCTION** | `soql_builder.py:554` (`build_temporal_dist`) | sí | **Sí** | `date_trunc_y` + `GROUP BY yr`, mismo `_build_where`. Analyzer registrado (`TemporalAnalyzer`). |
| agrupación por modalidad | **EXISTS_PRODUCTION (builder), sin analyzer** | `soql_builder.py:536` (`build_top_modalities`) | sí | **Sí** | Lo consume `observer.py:204`; no hay analyzer conversacional. |
| agrupación por territorio | **ABSENT** | — | — | — | No hay `build_top_territories`. Sería el único builder nuevo del slice. |
| grupos de proveedores | **ABSENT** | — | — | — | Cero menciones en `app/`, `scripts/`, `tests/`. |
| integrantes UT/consorcios | **ABSENT** | — | — | — | `consorcio`/`union temporal` aparece solo en `query_router.py` (vocabulario de parsing), nunca como relación. |
| relación inversa participante→grupo | **ABSENT** | — | — | — | — |
| RUES | **ABSENT** | — | — | — | Cero menciones en código. |
| RUP | **ABSENT** | — | — | — | Cero menciones en código. |
| representante legal | **ABSENT** | — | — | — | Cero menciones en código. |
| SIRI | **ABSENT** | — | — | — | Cero menciones en código. |
| responsabilidad fiscal | **ABSENT** | — | — | — | Cero menciones en código. |
| `SourceResult` equivalente | **PARTIAL — dos implementaciones parciales** | `outcome.py` (turno) · `truth_layer/evidence.py::SourceError` (campo) | `test_truth_layer_evidence.py`, tests de outcome | **Sí, ambas** | `outcome.py` separa vacío-confirmado de timeout/fuente-caída; `SourceError(kind=timeout\|transport_error\|invalid_payload)` hace lo propio por campo. **Ninguna lleva `queried_at`.** |
| evidence/provenance | **EXISTS_DORMANT** | `truth_layer/evidence.py` (663 líneas) | `test_truth_layer_evidence.py` | **Parcialmente** (§9) | Solo `observers.py` está enganchado a producción, vía `process_operational_state.py:32`. |
| fan-out concurrente | **EXISTS_PRODUCTION** | `observer.py:211-231` | sí | **Sí, directo** | Ver §4.2. |
| presupuesto/deadline por fuente | **EXISTS_PRODUCTION** | `deadline.py:62,73`; `secop_client.py:38,58` | sí | **Sí** | `fetch_budget_s`, `can_afford`, `derived_for_timeout` (thread-safe declarado). |
| formatter compartido | **EXISTS_PRODUCTION** | `formatter.py` | sí | **Sí** | Un render para WhatsApp y Streamlit; el ADR acertó en el reúso. |
| routing actor/provider | **ABSENT** | — | — | — | Ver §11: colisiona con `objeto`. |
| Radar por actor | **PARTIAL (costura ya existe)** | `query_frame.py:61`, `_followup_constants.py:53-56`, `radar/builder.py` | `test_radar_follows.py`, `test_radar_keys.py` | **Sí** | §10. |
| feature flags aplicables | **EXISTS_PRODUCTION (patrón)** | `config.py:42-50` | sí | **Sí** | pydantic-settings con alias de entorno (`SECOPPAL_AUTH_ENABLED`, etc.). Añadir `SECOPPAL_ACTOR_PROFILE_ENABLED` es una línea. |

---

## 6. Identity architecture today

**No hay arquitectura de identidad de actor. Hay un número que viaja sin tipo y a veces se cae.**

### 6.1 El camino real de un documento

```
"contratos del NIT 860.034.594-1"
  → query_router._extract_nit()          # forma + _number_role == "identifier"
  → params["contratista"] = ["860034594", "8600345941"]     # list[str], sin tipo
  → QueryFrame.modifiers["contratista"]                      # query_frame.py:61
  → SoQLBuilder._build_where()
      if params.get("contratista") and spec.contractor:      # ← el punto de fuga
          → OR sobre documento_proveedor                     # contratos: correcto
          → filtro DESCARTADO                                # procesos: silencioso
```

Verificado (§11): el WHERE de contratos emitido es `(documento_proveedor = '900123456')`; el de procesos no contiene el documento en absoluto.

### 6.2 Qué falta, exactamente

| Pieza | Estado | Por qué importa |
|---|---|---|
| tipo documental (`NIT` \| `CC` \| `CE` \| …) | ausente | Sin tipo, `1140835135` puede ser cédula o NIT truncado; el pipeline no lo sabe y aplica el mismo filtro. Rompe INV-ID-TYPE por construcción. |
| llave canónica `(document_type, base_number)` | ausente | Hoy la "llave" es una `list[str]` de variantes textuales resuelta dentro del builder de SoQL. |
| DV separado + validación módulo 11 | ausente | `_extract_nit` solo separa DV si el crudo traía separador. `8600345941` (sin puntos) produce una variante que no coincidirá con `860034594`. **Falso negativo silencioso.** |
| procedencia (`raw_value`, `source`, `confidence`) | ausente | Sin ella no se puede auditar por qué se atribuyó un contrato a un actor. |
| nombre → documento | ausente | La brecha más cara: sin ella el nombre cae a `objeto`. |
| ambigüedad explícita | ausente | No hay forma de decir "este nombre resuelve a 3 actores". El sistema responde como si resolviera a uno. |

### 6.3 Qué SÍ hay y no debe reescribirse

- **Arbitraje número-vs-monto:** `_number_role(normalized, candidate) == "identifier"` (ADR-059) ya impide que un NIT se lea como monto y viceversa. Es la entrada correcta a AP1.
- **Identidad de fila:** `result_identity.py` resuelve identidad de proceso/contrato con una doctrina explícita — *falla CERRADO*, no adivina el dataset, no fabrica una clave desde un no-`str`. **Es el modelo mental a copiar para identidad de actor**, no la implementación a extender (opera sobre otro sujeto).
- **Gazetteer + fuzzy:** `EntityResolver` demuestra la arquitectura completa (índice de alias → escaneo → desambiguación → `ResolutionResult` con `confidence`) sobre compradores. AP3 es "lo mismo, otro corpus".

---

## 7. Warehouse capabilities

**Veredicto: `NOT_A_BACKEND` para Actor Profile. No es un problema de madurez — es que no contiene el dato.**

### 7.1 Dónde vive

En **worktrees**, nunca en main. `grep -rn "duckdb\|parquet" app/ scripts/` en main → **cero resultados**. El worktree canónico es `/Users/rodrigoortiz/Documents/secoppal-warehouse` (`warehouse/w2-0c3c-frontier-completeness` @ `214cf15`), con 23 módulos en `app/warehouse/` y 7.1 GB en `data/warehouse/` (1.783 archivos Parquet).

### 7.2 Qué contiene realmente

| Dimensión | Hallazgo |
|---|---|
| **Datasets** | `p6dx_8zbt` (**procesos**) y `discovery_processes`. **No existe `jbjy-vk9h` (contratos)** — verificado: `find data -iname '*jbjy*'` → vacío. |
| **Esquema raw** (24 columnas) | `source_row_id, process_id, source_updated_at, referencia_del_proceso, entidad, departamento_entidad, ciudad_entidad, nombre_del_procedimiento, descripci_n_del_procedimiento, tipo_de_contrato, modalidad_de_contratacion, precio_base, estado_de_apertura_del_proceso, estado_del_procedimiento, fase, fecha_de_publicacion_del, urlproceso, nit_entidad, source_dataset_id, ingestion_run_id, ingested_at, fetch_page_number, fetch_query_hash, raw_schema_version` |
| **Campos de proveedor** | **CERO.** El único `nit_*` es `nit_entidad` — el NIT del **comprador**, no del proveedor. |
| **Cobertura temporal** | Worktree canónico: 3 particiones (`published_month=2026-04..06`). La rama `w0d-discovery-slice` alcanzó 12 meses / 1.844.827 filas / 373 MB, en otra rama. |
| **Actualización** | Por corridas de ingesta (`ingestion_run_id`, `_staging`/`_promoted`/`_manifests`). No hay delta automático. |
| **Latencia** | Excelente donde aplica: p50 304 ms local vs 13,8 s Socrata; speedup ~59×. Irrelevante aquí. |
| **Veredicto propio (W0D.6B)** | `candidate_pool = GO_FUERTE`, **`primary_source = NO_GO`**, `W1 = GO_PARTIAL_LOCAL_POOL_PLUS_SOCRATA_VERIFICATION`. |

### 7.3 Clasificación exigida

| Pregunta de Actor Profile | Veredicto |
|---|---|
| contratos por NIT | **warehouse no contiene el dato** (sin dataset de contratos) |
| contratos por nombre | **warehouse no contiene el dato** |
| compradores por NIT | **warehouse no contiene el dato** |
| años por NIT | **warehouse no contiene el dato** |
| modalidades por NIT | **warehouse no contiene el dato** |
| territorio por NIT | **warehouse no contiene el dato** |
| valor acumulado por NIT | **warehouse no contiene el dato** |
| índice local de **nombres** de proveedor | **warehouse no contiene el dato**, y es un problema distinto (índice de nombres ≠ almacén columnar de filas) |

**Consecuencia de diseño.** La huella contractual del V1 se sirve **en vivo desde SECOP contratos (`jbjy-vk9h`)**. Incorporar contratos al warehouse es un proyecto propio (nueva ingesta, nueva normalización, nueva frontera de cobertura, nueva política de promoción) y **no debe ser precondición** de Actor Profile. Si algún día existe, entra por detrás como *candidate pool*, exactamente como decidió W0D para procesos — sin cambiar el contrato de la feature.

---

## 8. Live data sources and adapters

### 8.1 Lo que hay

| Componente | Estado | Nota |
|---|---|---|
| `SecopClient` (sodapy) | `EXISTS_PRODUCTION` | Reintentos con backoff, telemetría (`http_span`), `derived_for_timeout` thread-safe, `with_timeout`. |
| `SecopClient.query()` | acoplado | Aplica `_normalize_row()` a **toda** fila (`:125`). |
| `SecopClient.aggregate()` | **desacoplado** | *No* normaliza — best-effort para el observer. Es el punto natural de extracción del transporte genérico. |
| `SecopClient.count()` | parcialmente desacoplado | No normaliza; extrae el primer escalar. |
| Transporte genérico Socrata | `ABSENT` | El ADR tenía razón: hace falta separarlo. |
| Adapters por fuente (RUP/RUES/Fiscal/SIRI/Grupos) | `ABSENT` | — |
| Presupuesto por fuente | `EXISTS_PRODUCTION` | `deadline.py:62` `fetch_budget_s(source_timeout_s, cap_s)`. |
| Fan-out + ensamblaje parcial | `EXISTS_PRODUCTION` | `observer.py:211`. |

### 8.2 Las cinco fuentes del ADR-042

Las cinco son `ABSENT` en código. Verificado: `grep -rilE "\bRUES\b|\bRUP\b|SIRI|responsabilidad fiscal|procuraduria|contraloria|grupos de proveedores" app/ scripts/ tests/` → **cero archivos**.

Sus esquemas, formatos de llave y frescura son `DOCUMENTED_ONLY` — descritos en el ADR, **nunca verificados contra el servicio vivo en este repo**. E0 del ADR sigue íntegramente pendiente y sigue siendo precondición de cualquier bloque jurídico.

### 8.3 Una fuente que el ADR no consideró

**SECOP I `f789-7hwg`** (`docs/secop1_procesos/`, auditado 2026-05-28, `AUDIT ONLY — NO INTEGRAR TODAVÍA`) expone:

```
nom_razon_social_contratista      → nombre del contratista
identificacion_del_contratista    → documento
tipo_identifi_del_contratista     → TIPO DOCUMENTAL EXPLÍCITO
```

Es la única fuente inventariada con **columna explícita de tipo documental**, justo la señal de máxima prioridad que ADR-042 definió para INV-ID-TYPE. También aporta actividad legacy real en 2026 (mínima cuantía, régimen especial, municipios pequeños) que hoy es invisible. **Recomendación:** evaluarla en el bloque de identidad, no darla por integrada.

---

## 9. Truth Layer reuse assessment

**Estado:** fusionado a main y **dormido**. Único enganche a producción: `process_operational_state.py:32` → `truth_layer.observers.observe_process_truth_axes`. `evidence.py` no tiene consumidores fuera del propio paquete y sus 14 archivos de test.

**Principio rector: Actor Profile no debe convertirse en Truth Layer 2.** Se rescatan primitives, no arquitectura.

| Pieza | Veredicto | Justificación |
|---|---|---|
| `SourceError(code, dataset, kind)` con `kind ∈ {timeout, transport_error, invalid_payload}` | **REUSE_DIRECT** | Es literalmente la distinción que la feature necesita: *"una falla de la fuente nunca es una ausencia del hecho"*. Se adopta tal cual. |
| `AssessmentConflict` | **REUSE_DIRECT** | Preserva ≥2 evidencias incompatibles, **no nombra ganador, no descarta, no puntúa**. Cumple INV-CONFLICT sin escribir nada nuevo. |
| `ResolutionQuality` | **REUSE_DIRECT (como forma)** | Ejes categóricos cerrados (`completeness`/`consistency`/`confidence`) + `reason_codes` + `missing_fields` + `source_errors` + `conflicts` + `capabilities`, **sin score 0–1 inventado**. Invariante clave ya codificado: `conflicts` no vacío ⇒ `consistency != "consistent"`. |
| `build_resolution_quality()` | **REUSE_DIRECT** | Único punto que deduplica y canonicaliza; el dataclass permanece estricto. |
| `_require_stable_code` (UPPER_SNAKE_CASE) | **REUSE_PATTERN_ONLY** | Impedir estructuralmente que prosa de producto se filtre a la capa de razones es exactamente lo que un dossier jurídico necesita. Es privado; se replica el patrón. |
| `Evidence` | **REUSE_PATTERN_ONLY** | Dos desajustes duros: (a) el sujeto es `(dataset, field)` de una fila SECOP, no `(actor, predicado)`; (b) exige que `sentinel_assessment.raw_observation` sea **el mismo objeto** (`is`) — provenance por identidad de objeto, correcto en un pipeline de campos, inaplicable a respuestas de cinco APIs distintas. **Y crucialmente: `Evidence` no lleva `queried_at`.** Un claim jurídico sin fecha de consulta viola INV-EVIDENCE. Se replica el patrón (rol explícito, error de fuente separado, sin conclusiones) sobre un tipo propio. |
| `FieldObservation` (`observed`/`missing`/`not_observed`) | **REUSE_PATTERN_ONLY** | La tricotomía es correcta y debe conservarse. El tipo es escalar-por-campo. |
| `sentinels.py` (`SentinelStatus`, políticas por `(dataset_id, field_name)`) | **DO_NOT_REUSE** | Catálogo de centinelas SECOP (`"No Definido"`, etc.). Fuera de dominio. |
| `process_lifecycle`, `contract_*`, `temporal_*`, `visibility_resolver` | **DO_NOT_REUSE** | Resuelven ciclo de vida de procesos y contratos. Otro sujeto, otra pregunta. |
| `local_wall_time.py` | **UNKNOWN / evaluar en AP** | Candidato para el `queried_at` que falta, pero no inspeccionado en profundidad aquí. |

### 9.1 La pieza más reutilizable no está en Truth Layer

`app/core/outcome.py` — 85 líneas, pura, en producción, consumida por `normal_flow.py` y `conversation_store.py` — ya implementa la regla más importante del ADR-042:

```python
if source_unavailable: return Outcome.ERROR, OutcomeReason.SOURCE_UNAVAILABLE
if query_failed:       return Outcome.ERROR, (FETCH_TIMEOUT if query_timed_out else QUERY_ERROR)
...
# rows_returned == 0 → EMPTY: "0 filas CONFIRMADAS por la fuente"
```

Su docstring lo dice explícitamente: *un fetch que falló por timeout NO es `EMPTY` (ausencia confirmada)*. **INV-NO-CLEAN ya es doctrina de casa.** Actor Profile la extiende por fuente y por actor; no la inventa.

---

## 10. Radar integration assessment

| Pregunta | Respuesta |
|---|---|
| **¿Cómo representa un radar?** | Fila en SQLite (`radar/store.py`) con `(owner, name)` único + un `QueryFrame` serializado a JSON (`builder.py::save_radar_from_params`), más `dataset`, `channel`, `frequency`. |
| **¿Qué guarda?** | `SeenRecord` (membresía: `result_key`, `dataset`, `fields`, `snapshot_hash`) y `EventRecord` (inmutable: `kind`, `ts`, título, entidad, valor, estado, url, referencia, contratista). |
| **¿Qué identidad usa?** | `radar_result_key(row, dataset_id)` → `id_del_proceso`\|`referencia_del_proceso` (procesos) o `id_contrato`\|`referencia_del_contrato` (contratos). **Identidad de resultado, no de actor.** |
| **¿Puede seguir contratistas hoy?** | **Sí, estructuralmente.** `contratista ∈ QueryFrame._MODIFIER_KEYS` (`query_frame.py:61`) → se serializa y se replaya. `contratista ∈ SCOPE_TOPIC_KEYS` (`_followup_constants.py:53-56`) → `has_scope_or_topic()` lo acepta, así que un radar **solo** por NIT **no** es rechazado por el guard anti-`WHERE 1=1`. No está en `RADAR_SAVE_DENY_KEYS`. |
| **¿`QueryFrame` soporta contratista/NIT adecuadamente?** | Sí como transporte. **No** como identidad: guarda `["860034594","8600345941"]` — variantes textuales sin tipo ni actor. Dos usuarios que escriban el mismo NIT distinto obtienen dos radares que no se reconocen como el mismo actor. |
| **Limitación heredada** | Un radar por contratista solo funciona con `dataset=contratos`. En procesos el filtro se descarta y el radar vigilaría un universo equivocado — **sin avisar**. |
| **Costura mínima futura** | Producir `params["contratista"] = [documento_canónico]` desde un actor resuelto y llamar `save_radar_from_params`. **Cero cambios en `radar/`.** El único cambio deseable (posterior) es persistir `actor_id` junto al frame para que el radar sepa *a quién* sigue, no solo *qué cadena* filtra. |
| **Novedades futuras cubiertas** | "nuevo contrato" y "cambio de estado/valor" ya los da `diff.py` sobre `snapshot_hash`. "Nueva entidad contratante" y "nueva UT" **no**: requieren comparar agregados, no filas — trabajo real, fuera del V1. |

**Veredicto:** `REUSE_DIRECT` de la costura, `DO_NOT_MODIFY` del módulo. Radar es el consumidor natural de AP y debe entrar **último**.

---

## 11. Current routing behavior

Sonda ejecutada contra `QueryRouter` + `SoQLBuilder` reales (puros, sin red, sin `Settings`, sin persistencia). Ninguna consulta externa. Script en scratchpad, fuera del repo.

### 11.1 Resultados

| # | Query | route_reason | params relevantes | dataset | Resultado | Riesgo |
|---|---|---|---|---|---|---|
| 1 | `contratos de FUNDACARIBE` | `heuristic_only` | `objeto:["fundacaribe"]` | contratos | LIKE sobre objeto | **Colisión `objeto`. Falso negativo + falsa atribución.** |
| 2 | `contratos de FUNDACARIBE en 2025` | `heuristic_only` | `objeto:["fundacaribe"]`, `fecha_desde/hasta` 2025 | contratos | ídem, acotado | ídem |
| 3 | `contratos del NIT 900123456` | `heuristic_only` | `contratista:["900123456"]` | contratos | **`WHERE (documento_proveedor = '900123456')`** | **Correcto** |
| 4 | `qué ha celebrado la persona 1140835135 en 2025` | `heuristic_only` | `contratista:["1140835135"]`, `objeto:["persona"]` | **procesos** | **`WHERE (… LIKE '%persona%') AND fecha … 2025`** | **CRÍTICO: cédula descartada.** |
| 5 | `quién conforma la UT Esperanza` | `heuristic_only` | `objeto:["quien","conforma","esperanza"]` | procesos | búsqueda textual | Sin ruta. Ruido. |
| 6 | `qué hay de FUNSOCOM` | `heuristic_only` | `objeto:["funsocom"]` | procesos | LIKE sobre objeto | Sin ruta de dossier. |
| 7 | `revisa integralmente FUNSOCOM` | `heuristic_only` | `objeto:["revisa","integralmente","funsocom"]` | procesos | LIKE con verbos | Sin ruta. Ruido. |
| 8 | `el representante legal de FUNSOCOM tiene inhabilidades` | `heuristic_only` | `objeto:["representante","legal","funsocom","inhabilidades"]` | procesos | LIKE con "inhabilidades" | **Riesgo de lectura jurídica.** |
| 9 | `contratos del NIT 860.034.594-1` | `heuristic_only` | `contratista:["860034594","8600345941"]` | contratos | OR sobre ambas variantes | **Correcto** |
| 10 | `cuánto suma lo contratado por el NIT 900123456` | `heuristic_only` | `contratista:["900123456"]`, `objeto:["contratado"]` | **procesos** | `SUM(precio_base) WHERE … LIKE '%contratado%'` | **CRÍTICO: suma un universo ajeno.** |
| 11 | `evolución de los contratos del NIT 900123456` | `heuristic_only` | `contratista:["900123456"]` | contratos | `temporal` sobre `documento_proveedor` | **Correcto** |

`detect_direct_lookup` devolvió `None` en las 11.

### 11.2 Las dos patologías, con el SoQL que las prueba

**P1 — El filtro por documento se descarta en silencio en procesos.**

```python
# soql_builder.py:491
if params.get("contratista") and spec.contractor:   # procesos: spec.contractor is None
```

Caso 4, SoQL emitido (recortado):
```sql
SELECT id_del_proceso, … FROM p6dx-8zbt
WHERE (unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%persona%'))
    OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%persona%')))
  AND fecha_de_publicacion_del >= '2025-01-01' AND fecha_de_publicacion_del <= '2025-12-31'
```
La cédula `1140835135` **no aparece**. La respuesta a "qué ha celebrado la persona 1140835135" sería una lista de procesos que contienen la palabra "persona". Caso 10 agrava: `SUM(precio_base)` sobre `LIKE '%contratado%'` presentado como el total de un NIT.

*Por qué el degradado es la peor opción posible:* no falla, no avisa, y produce una respuesta con forma correcta. Es indistinguible de un acierto para quien pregunta.

**P2 — Un nombre de proveedor nunca llega al campo del proveedor.**

`_extract_contractor` (`query_router.py:1413`) exige el disparador léxico `contratista|proveedor [adjudicado]` **y** `dataset == contratos`. `"contratos de FUNDACARIBE"` no lo contiene, así que el nombre cae a `objeto`:

```sql
WHERE (unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%fundacaribe%'))
    OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%fundacaribe%')))
```
Nunca se toca `proveedor_adjudicado`. Doble error simétrico: se pierden los contratos reales cuyo objeto no nombra al proveedor, y se atribuyen contratos ajenos que sí lo mencionan.

### 11.3 Colisiones de routing a gobernar

| Ruta existente | Colisión | Gobierno propuesto |
|---|---|---|
| `objeto` (búsqueda textual) | **La colisión principal.** Todo nombre de actor no reconocido cae aquí. | Un actor resuelto con confianza consume el span y **no** deja residuo en `objeto`. Sin resolución → se declara, no se degrada a texto. |
| `entity_query` / gazetteer de compradores | FUNDACARIBE/FUNSOCOM no están en `aliases_db.json` (es de compradores) — hoy no colisiona, pero **existen nombres que son ambos**. | ADR-040 ya resolvió la familia territorio-vs-comprador: mismo criterio. Si un nombre resuelve a comprador **y** a proveedor → preguntar, nunca elegir. |
| `direct_lookup` | Sin colisión: solo `CO1.*`, URLs y referencias `CD-…`. | Un documento desnudo es un lookup **de actor**, ruta hermana, no extensión de esta. |
| `opportunity_search` | P15 exige tema; un actor **es** alcance. | `contratista` ya ∈ `SCOPE_TOPIC_KEYS`: cubierto. |
| `analytics` | Ninguna: componen bien (casos 10-11). | Heredan el arreglo de P1 automáticamente. |
| follow-up | `contratista` ya está en `FILTER_FIELDS` con reglas de purga (`followup_mutations.py:166`) y test dedicado (`test_followup_contratista_purge.py`). | Cubierto; verificar que un actor resuelto purgue igual. |

---

## 12. Data/privacy risks

### 12.1 Estado actual — mejor de lo que el ADR asumía

| Campo | ¿Se transporta? | ¿Se renderiza? |
|---|---|---|
| `documento_proveedor` | **Sí** (`select_fields` de contratos) | **No** — cero ocurrencias en `formatter.py`, `narrator.py`, `streamlit_app.py` |
| `proveedor_adjudicado` (nombre) | Sí | **Sí**, como `contratista` (`formatter.py:424`) |
| domicilio / correo / teléfono | **No** | **No** — ninguna fuente actual los trae |

La recomendación base del §15 del encargo (**documento completo internamente para joins exactos, enmascarado en render; sin domicilio, correo ni teléfono**) **ya se cumple de hecho**. Lo que falta es que sea **política**, no coincidencia — antes de que RUP entre y la rompa.

### 12.2 Riesgos que se activan al implementar

| # | Riesgo | Momento | Mitigación |
|---|---|---|---|
| R1 | **Persona natural contratista.** Nombre + cédula de un ciudadano son PII aunque SECOP los publique. | Ya presente (el nombre se renderiza) | INV-PII: documento de persona natural enmascarado en render; completo solo en memoria para el join. Requiere tipo documental ⇒ **depende de AP1**. |
| R2 | **RUP trae domicilio, correo y teléfono de personas naturales.** | Al integrar RUP | Lista blanca de campos en el adapter, no lista negra en el render: lo que no se proyecta no puede filtrarse. |
| R3 | **Falsa atribución jurídica por homónimo.** | Al integrar SIRI/fiscal | INV-AMB: nombre ambiguo ⇒ **cero** aserciones jurídicas. Solo documento exacto desambigua. |
| R4 | **Falso "limpio" por timeout.** El bug más peligroso. | Al integrar cualquier fuente jurídica | INV-NO-CLEAN, con `outcome.py` + `SourceError` como base. Redacción obligatoria: *"No se encontró coincidencia exacta … en la fuente X consultada a las HH:MM del DD/MM/AAAA"*. |
| R5 | **Presencia leída como vigencia.** Sanción de 2006 de 20 años sigue viva; multa de 2006 ya no inhabilita. | Al integrar SIRI | `vigencia_no_determinable` como estado de primera clase, con la regla de cálculo expuesta. |
| R6 | **Perfilamiento acumulativo.** Un dossier persistido de un ciudadano es una base de datos de terceros. | Al añadir cache | ADR-042 ya lo excluyó del alfa ("cache persistente de datos jurídicos" está *Fuera*). **Mantener.** |
| R7 | **Cruce empresa → representante legal → antecedentes.** Un dato **inferido** sobre alguien que no fue consultado. | Al integrar RUP+SIRI | Bloque separado, con evidencia y fecha explícitas, y nunca como conclusión sobre la empresa. |

### 12.3 Un riesgo que el ADR no anticipó

`data/conversations/*.jsonl` (untracked, ~24 archivos) persiste transcripciones de consultas. Si un usuario consulta la cédula de un ciudadano, **queda escrita ahí**. Hoy son artefactos de dogfood; cuando Actor Profile exista, esa persistencia entra en el alcance de la política de retención. **Documentado como riesgo de diseño, no corregido aquí.**

---

## 13. Recommended target architecture

**Principio.** Actor Profile es una **familia pequeña de capacidades sobre el pipeline existente**, no una capa nueva. Se cuelga de la identidad, se sirve del SoQL que ya hay, y añade lo mínimo.

### 13.1 Sobre H1 — ¿familia de capacidades o dossier único?

**Sí a la familia, no a las seis rutas.** Seis intents (`actor_profile`, `actor_contracts`, `actor_relationships`, `actor_registry`, `actor_compliance`, `actor_full_dossier`) multiplican la superficie de colisión con `objeto` **por seis** — y la colisión con `objeto` es precisamente el defecto que hay que cerrar. El repo ya demostró el patrón correcto en `analytics/registry.py`: **una ruta, un registro de aspectos**. Se recomienda:

```
1 ruta de routing:   actor_query
1 slot de alcance:   actor (resuelto, tipado)
N aspectos:          contracts | buyers | years | modalities | territories | relationships | registry | compliance
```

Un aspecto es un módulo con una responsabilidad y una línea en un registro — el patrón de `analytics/` y de la política "no monolitos". El "dossier completo" es *todos los aspectos disponibles*, no un séptimo intent.

### 13.2 Sobre H4 — ¿tres dimensiones de estado?

**Sí, pero con tres nombres que el repo ya usa, y una cuarta pieza que falta.**

```
transport_status        ← ya modelado: SourceError.kind {timeout, transport_error, invalid_payload}
                          + Outcome.ERROR {FETCH_TIMEOUT, SOURCE_UNAVAILABLE, QUERY_ERROR}
match_status            ← NUEVO {exact, none_confirmed, ambiguous, not_queried}
interpretation_status   ← NUEVO {determined, vigencia_no_determinable, out_of_scope}
queried_at              ← FALTA EN TODO EL REPO. Sin esto no hay INV-EVIDENCE.
```

El enum único del ADR (`SourceResult.status`) colapsaba tres preguntas ortogonales: *¿respondió la fuente?*, *¿coincidió?*, *¿pude interpretarlo?*. `coincidencia_exacta` y `consulta_agotó_tiempo` **no son valores del mismo eje**. El repo ya separó ejes así en `outcome.py` (Outcome × OutcomeReason) y en `ResolutionQuality` (completeness × consistency × confidence): la casa ya sabe hacer esto.

**`queried_at` es el hallazgo silencioso de esta auditoría.** Ni `Evidence`, ni `SourceError`, ni `Outcome` llevan fecha de consulta. Cada afirmación jurídica la necesita. Es campo nuevo obligatorio.

### 13.3 Sobre H2 — ¿`Actor` separado de `ActorIdentity`?

**Sí, y por una razón que el ADR no tenía a la vista:** el pipeline actual ya produce **nombres observados sin identidad conciliada** (`proveedor_adjudicado` se renderiza; `documento_proveedor` se transporta pero no se usa para agrupar). Un modelo con solo `IdentityKey` no puede representar *"vi este nombre, todavía no sé de quién es"* — que es el estado más frecuente hoy. La separación es necesaria, no aspiracional.

Se conserva la corrección más valiosa del ADR: **la llave de join no lleva `source`** (si lo llevara, dos registros de la misma entidad desde fuentes distintas no comparan iguales y el JOIN falla). La procedencia va en el match.

### 13.4 Primitives propuestas

| Primitive | Veredicto | Nota |
|---|---|---|
| `Actor(actor_id, actor_type, canonical_name)` | **NEW** | Mínimo. `actor_type ∈ {juridica, natural, grupo, desconocido}`. |
| `ActorIdentity(actor_id, document_type, base_number, check_digit, source, confidence)` | **NEW** | Llave de join = `(document_type, base_number)`. DV **separado**. `document_type` obligatorio, con `desconocido` explícito — nunca inferido por longitud. |
| `ActorAlias` | **NEW, pero diferido a AP3** | No hace falta para el V1 por documento. |
| `ActorRelationship` | **NEW, diferido a AP5** | Ver H7. |
| `Claim(subject, predicate, value)` | **NEW, mínimo** | Solo cuando entren fuentes jurídicas. Para huella contractual es sobreingeniería: un contrato **es** el hecho. |
| `Evidence` | **EXTEND_EXISTING → en la práctica, NEW con patrón prestado** | Ver §9: el `Evidence` de Truth Layer no encaja (sujeto campo-de-fila, provenance por identidad de objeto, sin `queried_at`). Se replica su **disciplina**. |
| `SourceError` | **REUSE_EXISTING** | Tal cual, desde `truth_layer.evidence`. |
| `AssessmentConflict` | **REUSE_EXISTING** | Tal cual. |
| `ResolutionQuality` | **REUSE_EXISTING** | Tal cual, para el ensamblaje parcial del perfil. |
| `SourceResult` (enum único del ADR) | **UNNECESSARY** | Sustituido por las tres dimensiones + `queried_at`. |
| `ActorProfile` | **NEW** | Contenedor: `actor` + aspectos + `ResolutionQuality` + `queried_at`. |
| `ActorQueryPlan` | **UNNECESSARY** | `QueryPlan` ya existe y es extensible por reglas (P1…P16). Un plan paralelo duplica el guard de scope y el arbitraje de dataset. |
| Fan-out | **REUSE_EXISTING** | Patrón de `observer.py` + `deadline.fetch_budget_s`. |
| Feature flag | **REUSE_EXISTING** | `config.py` pydantic-settings. |
| Índice de nombres | **NEW, sobre infra existente** | `db.py::connect_sqlite` + FTS5 + rerank RapidFuzz. Solo en AP3. |

### 13.5 Sobre H7 — relaciones sin motor de grafos

**Confirmado: nada de Neo4j.** Sin evidencia técnica que lo justifique y con costo desproporcionado. Cuatro tablas SQLite (`actors`, `actor_aliases`, `actor_identities`, `actor_relationships`) con SQL directo bastan para "quién conforma la UT X" (directa) y "en qué UT participa X" (inversa). El repo ya opera cuatro stores SQLite sin fricción. La recursión queda **acotada** como decidió el ADR (INV-UT): composición completa + señales básicas, dossier profundo bajo demanda.

### 13.6 Sobre H3 — histórico local + verificación viva

**La forma es correcta; el contenido cambia.** Hoy:

```
histórico contractual, compradores, años, modalidades, territorios, valor
  → SECOP contratos EN VIVO (jbjy-vk9h)      [el warehouse no tiene el dato]

índice de NOMBRES de proveedor
  → único candidato legítimo a copia local (SQLite FTS5), en AP3

RUP · RUES · SIRI · fiscal · Grupos
  → en vivo, por identidad exacta            [responsabilidad legal exige frescura]
```

### 13.7 Autoridad por predicado (§14 del encargo)

Actor Profile **sí** necesita autoridad por predicado, no por fuente global — el mismo principio que `select_authoritative_evidence` del warehouse aplica por `authority_level`, nunca por timestamp. **Pero la jerarquía concreta no puede fijarse hoy:** cinco de las seis fuentes son `DOCUMENTED_ONLY`, sin un solo esquema verificado en este repo. Lo único sostenible con evidencia:

| Predicado | Autoridad | Sustento |
|---|---|---|
| `contracts/*` (huella contractual) | **SECOP II contratos `jbjy-vk9h`** | Verificado: `documento_proveedor`, `proveedor_adjudicado`, `valor_del_contrato` en `select_fields`. |
| `identity/document_type` | **SECOP I `f789-7hwg`** (candidato) | Verificado en `FIELD_MAPPING.md`: `tipo_identifi_del_contratista`. Dataset no integrado. |
| todo lo demás | **UNKNOWN — pendiente de E0** | Fijar la jerarquía sin esquemas verificados sería inventarla. |

**Regla de diseño:** la tabla de autoridad se declara **por predicado, en datos, con la fuente de verificación citada**, y se llena a medida que E0 valida cada fuente. Ninguna fuente entra al código antes de su verificación.

---

## 14. Minimal vertical slice

### 14.1 Qué es

```
NIT / cédula / nombre
      ↓  [AP1]  identidad del actor — tipada, con DV, con confianza, o AMBIGUA declarada
      ↓  [AP2]  huella contractual observada — SECOP contratos en vivo
   contratos · compradores · años · modalidades · territorios · valor acumulado
```

**Sin RUP. Sin RUES. Sin SIRI. Sin fiscal. Sin UT. Sin representante legal.**

### 14.2 Por qué este y no otro

1. **Es lo único que ya está casi construido.** Cinco de los seis agregados existen como builders probados que respetan `contratista`. El sexto (territorios) es un builder de ~8 líneas idéntico a `build_top_entities`.
2. **Repara dos defectos de atribución que hoy están vivos** (§11) — valor inmediato, con o sin dossier.
3. **No toca ninguna fuente con PII nueva.** SECOP contratos ya se consulta. Riesgo jurídico incremental ≈ 0.
4. **Es precondición estricta de todo lo demás.** Sin identidad tipada, RUP/RUES/SIRI/fiscal no se pueden cruzar sin arriesgar falsas atribuciones. El orden no es negociable.
5. **Es reversible.** Detrás de un flag; apagado, el sistema se comporta exactamente como hoy.

### 14.3 Salida esperada del producto

```
FUNDACARIBE  ·  NIT 860.034.594-1  (tipo: NIT, confianza: exacta)

Huella contractual observada — SECOP II contratos, consultado 18/08/2026 14:32
- 37 contratos
- $18.4 mil millones
- actividad observada 2021–2026

Principales compradores        Evolución anual        Modalidades
…                              …                      …

Territorios
…

Esta es la actividad contractual publicada en SECOP II. No incluye
registros (RUP/RUES), antecedentes disciplinarios ni responsabilidad fiscal.
```

**Las dos últimas líneas no son decoración: son la mitad del producto.** El límite declarado es lo que impide que un usuario lea "37 contratos" como "empresa verificada".

### 14.4 Qué NO entra, y por qué

| Excluido | Razón |
|---|---|
| RUP / RUES / SIRI / fiscal | E0 sin hacer. Ningún esquema verificado. Riesgo legal alto, valor incremental menor que la reparación de atribución. |
| UT / consorcios | Dataset no verificado; `es_lider_del_grupo`/`participacion` son `DOCUMENTED_ONLY`. |
| Representante legal | Requiere RUP **y** trae PII de un tercero no consultado. |
| Índice FTS5 de nombres | AP3. El V1 acepta **nombre solo si resuelve sin ambigüedad**; si no, lo declara. |
| Cache persistente | ADR-042 lo excluyó del alfa. Se mantiene. |
| Radar por actor | AP7. Necesita identidad estable primero. |

---

## 15. Proposed implementation blocks

Siete bloques. Se **fusionaron** E3–E7 del ADR (adapters, contratos, UT, render, routing) porque el repo demostró que el fan-out y los agregados ya existen; se **eliminó** el bloque de decisión sobre RapidFuzz (ya resuelto); se **adelantó** el routing a AP2 porque sin él la huella no es alcanzable por el usuario.

---

### AP0 — Diagnóstico ✅ (este documento)

**Gate:** informe entregado, `GO/NO-GO` explícito, cero cambios en producción. **CERRADO.**

---

### AP1 — Actor identity foundation

| | |
|---|---|
| **Objetivo** | Módulo puro de identidad tipada: `(document_type, base_number)` como llave de join, `check_digit` separado, DV módulo 11 validado, procedencia y confianza en el match. Ambigüedad como valor de primera clase. |
| **Dependencias** | Ninguna. Puro, sin I/O. |
| **Archivos probables** | **nuevo** `app/core/actor_identity.py` · **nuevo** `tests/test_actor_identity.py` · lectura de `query_router._extract_nit` y `_number_role` como fuente de la extracción existente |
| **Riesgo** | **Bajo** de implementación. **Alto de corrección**: si la llave está mal, todo lo posterior miente con confianza. Modelo a copiar: `result_identity.py` — falla cerrado, no adivina, no fabrica claves. |
| **Valor visible** | Ninguno directo. Es el spine. |
| **Tests necesarios** | DV módulo 11 (válidos e inválidos); `8600345941` sin separador ≡ `860.034.594-1` (**regresión conocida hoy**); mismo número con tipos distintos **no** se fusiona (INV-ID-TYPE); tipo desconocido se declara, nunca se infiere por longitud; `raw_value` preservado verbatim. |
| **Gate de cierre** | INV-ID e INV-ID-TYPE congelados en tests. Cero red, cero reloj, cero filesystem en el módulo (test de frontera, como `test_truth_layer_import_boundary.py`). Sin tocar `app/core/` existente. |

---

### AP2 — Contractual actor profile *(el vertical slice)*

| | |
|---|---|
| **Objetivo** | Que un documento de actor produzca su huella contractual completa, correcta y con límites declarados. Incluye **cerrar las dos patologías de §11**. |
| **Dependencias** | AP1. |
| **Archivos probables** | **nuevo** `app/core/actor_profile/` (aspectos + ensamblador, patrón `analytics/registry.py`) · `app/core/soql_builder.py` (**cambio mínimo**: `build_top_territories` + decisión explícita sobre `contratista` en procesos) · `app/core/query_router.py` (ruta `actor_query`) · `app/core/formatter.py` (render del perfil) · `app/config.py` (flag) |
| **Riesgo** | **Medio-alto.** Es el bloque que toca módulos cargados. Mitigación por política de casa (*no monolitos*): aspectos en módulos propios de una responsabilidad, con enganche mínimo. `soql_builder` recibe **un** builder nuevo, no lógica de perfil. |
| **Decisión de diseño obligatoria** | **`contratista` en procesos.** Tres opciones: (a) declararlo no soportado y **bloquear** con clarificación — *recomendada*, cumple INV-NO-CLEAN; (b) promover el dataset a contratos cuando hay documento; (c) dejarlo como está. **(c) es inaceptable**: es el defecto. Decidir antes de codificar. |
| **Valor visible** | **Alto.** Primera respuesta correcta a "qué ha contratado X". Y desaparecen dos formas de mentir. |
| **Tests necesarios** | Regresión de P1 (documento en procesos **nunca** se descarta en silencio); regresión de P2 (nombre resuelto **no** deja residuo en `objeto`); los seis agregados por documento; ensamblaje parcial (fuente caída ⇒ el perfil sobrevive con la ausencia declarada); `queried_at` presente en todo agregado; disclaimer de límites **siempre** presente. |
| **Gate de cierre** | INV-NO-CLEAN, INV-PARTIAL e INV-EVIDENCE congelados. Suite completa verde. Corpus sin regresión. Flag apagado ⇒ comportamiento byte-idéntico al de hoy. |

---

### AP3 — Name/alias resolution

| | |
|---|---|
| **Objetivo** | `nombre → actor`. Índice SQLite FTS5 + rerank RapidFuzz, con desambiguación explícita. |
| **Dependencias** | AP1, AP2. |
| **Archivos probables** | **nuevo** `app/core/actor_name_index.py` · **nuevo** `scripts/build_actor_name_index.py` · reúso de `db.py::connect_sqlite` y del patrón de `entity_resolver.py` |
| **Riesgo** | **Medio.** Riesgo real = falsa atribución por nombre parecido. |
| **Decisión abierta** | **Corpus del índice.** El ADR asumió RUP+RUES; ambos son `ABSENT` y `DOCUMENTED_ONLY`. Alternativa disponible sin E0: construirlo desde `proveedor_adjudicado`+`documento_proveedor` **del propio SECOP contratos** — datos que el sistema ya consulta, con la ventaja de que nombre y documento vienen apareados en la misma fila. Evaluar primero esta vía. |
| **Valor visible** | **Alto.** "contratos de FUNDACARIBE" empieza a funcionar de verdad. |
| **Tests necesarios** | Homónimos ⇒ desambiguación, **nunca** elección; nombre no resuelto ⇒ se declara, **no** cae a `objeto`; siglas y variantes; el candidato muestra **por qué** coincidió. |
| **Gate de cierre** | INV-AMB congelado. Corpus mínimo: 10 jurídicas, 10 naturales, 5 homónimos, 5 formatos sucios. |

---

### AP4 — Source contract (E0 del ADR-042)

| | |
|---|---|
| **Objetivo** | Verificar contra los servicios vivos: dataset ID, dominio, columnas reales, tipos, tasa de nulos, formatos de documento, volumen, límites de `app_token`, muestras congeladas, fecha máxima de actualización. Una fuente sin contrato verificado **no entra al código.** |
| **Dependencias** | Ninguna técnica; **bloquea AP5 y AP6.** |
| **Archivos probables** | **nuevo** `docs/audits/<fecha>_ACTOR_SOURCES_CONTRACT.md` · **nuevo** `tests/fixtures/actor_sources/` (muestras congeladas) |
| **Riesgo** | **Bajo** técnico, **alto** de omisión. Sin esto, AP5/AP6 se escriben a ciegas — que es exactamente lo que el ADR advirtió y nunca se hizo. |
| **Valor visible** | Ninguno directo. Es el que evita construir sobre una suposición. |
| **Gate de cierre** | Las 5 fuentes + `f789-7hwg` con esquema verificado y muestras congeladas, **o** declaradas fuera de alcance con razón escrita. |

---

### AP5 — UT / consortium relationships

| | |
|---|---|
| **Objetivo** | Composición directa (`nit_grupo` → integrantes con `participacion` y `es_lider_del_grupo`) e inversa (`nit_participante` → grupos). SQL sobre SQLite; **sin motor de grafos.** |
| **Dependencias** | AP1, AP4 (contrato del dataset de Grupos). |
| **Archivos probables** | **nuevo** `app/core/actor_relationships.py` + adapter de Grupos |
| **Riesgo** | **Medio.** Recursión no acotada = explosión de consultas. |
| **Valor visible** | **Alto** — "quién conforma la UT X" no tiene hoy ninguna respuesta. |
| **Gate de cierre** | INV-UT congelado: composición completa + señales básicas; **cero** dossiers profundos automáticos. |

---

### AP6 — Registry & compliance enrichment

| | |
|---|---|
| **Objetivo** | RUP/RUES (identidad registral, vigencia, representante legal) y SIRI/fiscal (antecedentes con **vigencia calculada**). |
| **Dependencias** | AP1, AP2, AP4. **Estrictamente después.** |
| **Archivos probables** | **nuevo** `app/core/actor_sources/` (transporte Socrata genérico + un adapter pequeño por fuente) · extracción del transporte de `SecopClient` dejando `_normalize_row` como su adapter |
| **Riesgo** | **ALTO — el más alto del plan.** Falso "limpio", falsa atribución por homónimo, presencia leída como vigencia, PII del RUP, dato inferido sobre el representante legal. |
| **Valor visible** | **Alto**, y el más peligroso si sale mal. |
| **Tests necesarios** | Timeout **nunca** produce "sin antecedentes" (INV-NO-CLEAN); vigencia calculada con regla expuesta, o `vigencia_no_determinable`; nombre ambiguo ⇒ cero aserciones jurídicas; lista blanca de campos del RUP (INV-PII); fuentes discrepantes ⇒ `AssessmentConflict` preservado (INV-CONFLICT). |
| **Gate de cierre** | Los 6 criterios de aceptación del alfa del ADR-042, verificados sobre el corpus mínimo. **Este es el gate donde el owner decide si la feature sale del flag.** |

---

### AP7 — Radar actor integration

| | |
|---|---|
| **Objetivo** | "Seguir a este actor" desde un perfil. |
| **Dependencias** | AP1, AP2. |
| **Archivos probables** | Costura en `actor_profile/` → `radar/builder.py::save_radar_from_params`. **Cero cambios en `radar/`** para la v1 de la costura. |
| **Riesgo** | **Bajo** — la costura existe (§10). |
| **Valor visible** | Medio-alto. |
| **Gate de cierre** | Un radar creado desde un perfil replaya el mismo universo que el perfil mostró. **Nunca** se crea un radar de actor sobre un dataset que descarta el filtro. |

---

## 16. Files likely involved by block

| Bloque | Nuevos | Modificados | Solo lectura |
|---|---|---|---|
| **AP1** | `app/core/actor_identity.py`, `tests/test_actor_identity.py` | — | `query_router.py` (`_extract_nit`, `_number_role`), `result_identity.py` |
| **AP2** | `app/core/actor_profile/{__init__,aspects,registry,assembler}.py`, tests | `soql_builder.py` (+`build_top_territories`, decisión `contratista`/procesos), `query_router.py` (ruta), `formatter.py` (render), `config.py` (flag) | `observer.py` (patrón fan-out), `deadline.py`, `analytics/registry.py`, `outcome.py`, `truth_layer/evidence.py` |
| **AP3** | `app/core/actor_name_index.py`, `scripts/build_actor_name_index.py`, tests | `actor_profile/` (enganche) | `db.py`, `entity_resolver.py`, `spell_correction.py` |
| **AP4** | `docs/audits/…_ACTOR_SOURCES_CONTRACT.md`, `tests/fixtures/actor_sources/` | — | `docs/secop1_procesos/`, `secop_client.py` |
| **AP5** | `app/core/actor_relationships.py`, adapter de Grupos, tests | `actor_profile/` (aspecto) | `db.py` |
| **AP6** | `app/core/actor_sources/{transport,rup,rues,fiscal,siri}.py`, tests | `secop_client.py` (**extracción del transporte** — el cambio más delicado del plan) | `truth_layer/evidence.py`, `deadline.py`, `instrumentation.py` |
| **AP7** | — | costura en `actor_profile/` | `radar/builder.py`, `query_frame.py`, `_followup_constants.py` |

---

## 17. Risks and invariants

### 17.1 Invariantes exigidos

| ID | Enunciado | Bloque | Anclaje en el repo |
|---|---|---|---|
| **INV-ID** | Misma identidad real ⇒ misma llave desde cualquier fuente, cuando hay evidencia suficiente. | AP1 | — (nuevo) |
| **INV-ID-TYPE** | Documentos de tipos distintos **no** se fusionan automáticamente. Tipo nunca se infiere por longitud. | AP1 | Modelo: `result_identity.py` (falla cerrado) |
| **INV-AMB** | Nombre ambiguo **no** produce atribución jurídica ni contractual. Se declara. | AP3 | `EntityResolver.ResolutionResult` |
| **INV-NO-CLEAN** | Timeout / no-match **nunca** se convierte en "sin antecedentes" ni en ausencia de actividad. | AP2, AP6 | `outcome.py`; `SourceError.kind` |
| **INV-EVIDENCE** | Cada claim jurídico lleva fuente, documento consultado y **`queried_at`**. | AP6 | **Campo ausente hoy en todo el repo** |
| **INV-CONFLICT** | Fuentes discrepantes **no** se reconcilian en silencio. | AP6 | `AssessmentConflict` (REUSE_DIRECT) |
| **INV-PII** | PII personal no se muestra innecesariamente. Documento de persona natural enmascarado en render, completo solo para joins internos. Sin domicilio, correo ni teléfono. | AP2, AP6 | Estado actual ya conforme (§12.1) |
| **INV-PARTIAL** | Una fuente caída **no** derriba el perfil. | AP2, AP6 | `observer.py`; `ResolutionQuality.completeness="partial"` |

### 17.2 Invariantes adicionales que el repo reveló

| ID | Enunciado | Por qué | Bloque |
|---|---|---|---|
| **INV-NO-SILENT-DROP** | Un filtro de identidad que el dataset no soporta **bloquea o migra** la consulta; **jamás** se descarta en silencio. | §11 P1: hoy la cédula desaparece del WHERE y la respuesta parece correcta. Es el defecto más peligroso del diagnóstico. | AP2 |
| **INV-NO-TOPIC-FALLBACK** | Un nombre de actor **no resuelto** no se degrada a búsqueda textual sobre el objeto contractual. | §11 P2: falso negativo y falsa atribución simultáneos. | AP2, AP3 |
| **INV-SCOPE-DECLARED** | Todo perfil declara qué fuentes consultó **y cuáles no**. | Evita que "37 contratos" se lea como "empresa verificada". | AP2 |
| **INV-NO-VERDICT** | El perfil **no** emite conclusiones agregadas de riesgo ("proveedor riesgoso"). | Mina legal; ADR-042 ya lo puso *Fuera*. Se eleva a invariante. | todos |
| **INV-SOURCE-VERIFIED** | Ninguna fuente entra al código sin contrato verificado (AP4). | Cinco de seis son `DOCUMENTED_ONLY`. | AP4 |

### 17.3 Riesgos de proyecto

| # | Riesgo | Mitigación |
|---|---|---|
| 1 | **AP2 se convierte en refactor de `soql_builder`.** | Un builder nuevo y una decisión explícita. Los aspectos viven en `actor_profile/`. Política *no monolitos*. |
| 2 | **AP6 se adelanta a AP4.** Tentador: es donde está el valor percibido. | AP4 es gate duro. Sin contrato verificado, no hay adapter. |
| 3 | **Actor Profile se vuelve Truth Layer 2.** | Lista cerrada de §9: tres primitives `REUSE_DIRECT`, nada más. |
| 4 | **Se espera al warehouse.** | §7: el warehouse no tiene el dato. Esperarlo es esperar un proyecto distinto. |
| 5 | **El número ADR-042 se reutiliza.** | §2: será ADR-060+, reconciliado contra el máximo global de los ~21 worktrees. |
| 6 | **La deuda `DEPLOY-TZ-001` toca `queried_at`.** | Fijar zona explícita para `queried_at` en AP2; evaluar `truth_layer/local_wall_time.py`. |

### 17.4 Hallazgos incidentales

> Documentados, **no corregidos**, según la regla de la auditoría.

**`INCIDENTAL_FINDING #1` — Filtro `contratista` descartado en silencio en `dataset=procesos`.**
`soql_builder.py:491` — `if params.get("contratista") and spec.contractor:`, con `spec.contractor is None` para procesos. Una consulta con NIT o cédula que resuelva a procesos ejecuta un WHERE sin el documento y devuelve resultados no relacionados con esa persona. Alcanza también a la capa analítica (`SUM` sobre el universo equivocado). Afecta a Actor Profile **y a las búsquedas actuales**. *No corregido: es material de decisión de AP2.*

**`INCIDENTAL_FINDING #2` — `_extract_nit` pierde el DV sin separador.**
`query_router.py:1403-1411` — solo separa DV si el crudo traía separador antes del último dígito. `8600345941` produce `["8600345941"]` y **no** coincide con `documento_proveedor = '860034594'`. Falso negativo silencioso. *No corregido: es AP1.*

**`INCIDENTAL_FINDING #3` — ADR-059 ausente de `decisions_log.md`.**
Existe `docs/decisions/2026-08-18_ADR-059_money-v2-deterministic-authority.md` pero `grep -c ADR-059 docs/decisions_log.md` → 0. El log está modificado sin commitear; puede ser trabajo en curso. Afecta al cálculo de numeración de futuros ADR. *No corregido.*

**`INCIDENTAL_FINDING #4` — Consultas persistidas en `data/conversations/`.**
~24 pares `.jsonl`/`.pagination.json` sin trackear con transcripciones de consultas. Hoy son dogfood; con Actor Profile pasarían a contener documentos de ciudadanos. Necesita política de retención **antes** de AP6. *No corregido.*

---

## 18. GO / NO-GO decision

### **GO — con tres condiciones vinculantes.**

**Se aprueba:** AP1 (identidad) → AP2 (huella contractual). Ese es el vertical slice.
**Se aprueba condicionalmente:** AP3, AP5, AP7.
**Se bloquea hasta AP4:** AP6 (RUP/RUES/SIRI/fiscal) — y con él, todo lo jurídico.

### Condiciones

1. **AP2 decide explícitamente qué hacer con `contratista` en procesos** antes de escribir código. La opción recomendada es bloquear con clarificación (INV-NO-SILENT-DROP). Dejarlo como está no es una opción: es el defecto que justifica el bloque.
2. **AP4 es gate duro para AP6.** Ninguna fuente jurídica entra al código sin esquema verificado y muestras congeladas. Cinco de las seis fuentes del ADR-042 llevan catorce meses siendo `DOCUMENTED_ONLY`.
3. **Todo detrás de flag** (`config.py`), con la garantía verificada por test de que apagado el sistema se comporta byte-idéntico a hoy.

### Por qué GO y no NO-GO

Porque el diagnóstico encontró que **el sistema ya responde estas preguntas — y a veces las responde mal, en silencio**. Un usuario que hoy escribe "contratos de FUNDACARIBE" o "qué ha celebrado la persona 1140835135" recibe una respuesta con forma correcta y contenido equivocado. La feature no añade una superficie de riesgo nueva: cierra una que ya está abierta.

### Por qué NO todo el dossier

Porque el 80% del valor del ADR-042 estaba en las fuentes jurídicas, y **el 100% de esas fuentes sigue sin verificar**. Construirlas ahora sería escribir cinco adapters contra esquemas supuestos, para un dominio donde equivocarse significa decir que un ciudadano tiene antecedentes que no tiene, o que no tiene los que sí. El ADR lo dijo mejor que este informe: *"si falla, todo miente con confianza"*.

### Respuesta a la pregunta del encargo

> **¿Cuánto de Actor Profile ya está medio construido, y cuál es el menor cambio coherente que nos entrega valor real?**

**Ya construido (~55% del slice mínimo):** el parámetro `contratista` de extremo a extremo, la detección de NIT con arbitraje contra montos, cinco de seis agregados por documento, el guard de scope, la capa analítica, el fan-out concurrente con presupuesto y ensamblaje parcial, la doctrina "timeout ≠ vacío", RapidFuzz, la infra SQLite, el formatter compartido, el patrón de feature flags, y la costura de Radar.

**Ausente por completo (~100% del dossier jurídico):** identidad tipada, resolución por nombre de proveedor, y las seis fuentes externas.

**El menor cambio coherente:** dar tipo y llave canónica a la identidad del actor (AP1), y hacer que esa identidad gobierne de verdad la consulta contractual (AP2) — incluyendo dejar de descartar el documento en silencio y dejar de convertir un nombre de proveedor en una búsqueda de texto.

---

## Apéndice A — Comandos de verificación ejecutados

```bash
git branch --show-current && git rev-parse HEAD && git rev-parse main
git status --short | wc -l && git log --oneline --decorate --max-count=12
git worktree list && git branch -a

grep -rn "documento_proveedor|proveedor_adjudicado" app/ --include='*.py'
grep -rn "fts5|VIRTUAL TABLE" app/ scripts/ --include='*.py'          # → vacío
grep -rn "duckdb|parquet" app/ scripts/ --include='*.py'              # → vacío
grep -rilE "\bRUES\b|\bRUP\b|SIRI|responsabilidad fiscal|grupos de proveedores" \
     app/ scripts/ tests/ --include='*.py'                            # → vacío
grep -rn "rapidfuzz" app/ --include='*.py'                            # → 3 hits
grep -rn "truth_layer" app/ --include='*.py' | grep -v "^app/core/truth_layer/"
grep -rn "documento_proveedor" app/core/formatter.py streamlit_app.py \
     app/core/narrator.py                                             # → vacío

# Warehouse (worktree, solo lectura)
find /Users/rodrigoortiz/Documents/secoppal-warehouse/data -iname '*jbjy*'   # → vacío
.venv/bin/python -c "pyarrow.parquet.read_schema(...)"                # → 24 columnas, sin proveedor

# Sondas de parser/SoQL (puras, sin red, en scratchpad)
probe_routing.py    # 11 queries → route/params/analytical_intent
probe_soql.py       # 4 casos    → SoQL emitido verbatim

# Tests focales
.venv/bin/python -m pytest tests/test_query_router.py \
    tests/test_result_identity.py tests/test_truth_layer_evidence.py -q
# → 240 passed in 2.90s
```

## Apéndice B — Estado del árbol

```
WORKTREE_BEFORE:  87 entradas (2 M en docs, 85 untracked)
WORKTREE_AFTER:   88 entradas (+ docs/audits/2026-08-18_ACTOR_PROFILE_DIAGNOSTIC.md)

PRODUCTION_CODE_CHANGED:  NO
TESTS_CHANGED:            NO
DEPENDENCIES_CHANGED:     NO
DATASETS_CHANGED:         NO
COMMIT / MERGE / REBASE / PUSH:  NO
```

# C1.2: frontera de vocabulario Foundation ↔ legacy y congelamiento del cierre legacy, v3

Rama `mission/m1-c12-ir-v2`, worktree `/Users/rodrigoortiz/Documents/secoppal-m1-c12` (HEAD `99bce8fe`, con el WIP de
C1.2 ya commiteado). Autoridad: ADR-103 enmienda 4, D1(b), D1(d), D1(e) y D13. Entra antes del freeze de C1.2,
con el mismo escritor del núcleo y en serie con `C1_2_TOPN_SPEC.md`; los archivos son disjuntos.

**Qué cambia respecto de v2 (N1, N2, N4, N5):**
- el congelamiento de datos cubre todo archivo versionado que no sea `.py` y que lea el cierre (artefactos municipales incluidos), con casación por ruta resuelta y exenciones explícitas;
- la hoja `foundation_guard` queda fijada por sha y por AST de sus funciones;
- pin de uso del atributo `.query_router` dentro de F;
- el cierre tras la división tiene 386 módulos.

**Qué cambia respecto de v1:**
- la lista a mano `C12_FOUNDATION_MODULES` se sustituye por un **pin generado** de sha sobre el cierre legacy;
- el pin cubre también los **datos** (`app/data/**`), por la decisión del usuario D1(d) de congelar;
- el pin de `semantic_dimensions` pasa a **lista blanca**;
- quedan declarados `foundation_text_predicates` y `foundation_trace_view`.

## 1. Diagnóstico (escáner del propio pin sobre c12, sin escribir en el árbol)

- **Cadena rota:** `scripts.run_radar_scheduler → radar.scheduler → workflow.deps → secop_client →(perezoso :26 :200
  :259) foundation_guard →(nivel de módulo :11-12) {foundation_projection, interpretation_contract} →
  {foundation_geo_level, foundation_operations, foundation_operator_table}`.
- **Hoy**, el cierre (392 módulos) contiene cinco módulos que difieren de `88653752`: `foundation_operations` y
  `foundation_operator_table` (nuevos), y `foundation_projection`, `interpretation_contract` y `semantic_dimensions`
  (cambiados).
- **Con la división**, el cierre tiene 386 módulos: la simulación que quita la arista da 385, y la hoja
  `foundation_guard` sigue dentro. Difieren de `88653752` `semantic_dimensions` y `foundation_guard`; las dos son
  divergencias permitidas. El cierre también contiene `foundation_text_predicates` (`soql_builder.py:537`) y
  `foundation_trace_view` (`streamlit_app.py:375,487`). Los dos son idénticos a `main` y no importan nada de `app`.

## 2. Cambio de código

| Archivo | Cambio |
|---|---|
| `app/core/foundation_guard.py` (hoja de frontera de fuente) | Se quedan `_GUARD`, `_RAW_ROWS`, `execution_guard`, `verify_effective_source_call`, `foundation_source_active`, `raw_source_rows`, `source_rows_raw` y `validate_foundation_count_total`, con el mismo cuerpo. Sólo importa `contextlib`, `contextvars` y `app.core.source_call_receipts`. `_GUARD` se tipa como `ContextVar[object \| None]` o con un `Protocol` local, sin `TYPE_CHECKING`, porque el escáner cuenta ese import. |
| `app/core/foundation_execution_guard.py` (nuevo, sólo Foundation) | `FoundationGuardError`, `_without_order`, `RequestExecution` y `ExecutionGuard`, movidos sin cambio. No reexporta nada de la hoja. |
| `app/core/foundation_runtime.py:18` | `ExecutionGuard` y `FoundationGuardError` se importan desde `foundation_execution_guard`; `execution_guard`, desde `foundation_guard`. |
| `tests/test_foundation_runtime.py:14`, `tests/test_foundation_token_runtime.py:11` | Sólo cambian las rutas de import. |
| `app/core/secop_client.py`, `foundation_basic_fallback.py`, `foundation_text_predicates.py`, `foundation_trace_view.py` | **Sin cambio.** |

## 3. Pines (`tests/test_m1_wave1_parity_pins.py` y un fixture)

1. **Manifiesto generado.**
   - Fixture: `tests/fixtures/legacy_closure_manifest_88653752.json`, con `{ruta: sha256}` en la base `88653752`.
   - Contenido: **todo archivo versionado** en la base (`git ls-tree -r 88653752`, sin `__pycache__`): los `.py` y
     todos los archivos que no son `.py`, incluidos `app/data/**` y `artifacts/**`.
   - Lo genera `scripts/audits/m1_search/legacy_closure_manifest.py --base 88653752`, que usa `git show
     <base>:<ruta>`. El script y el fixture se commitean; el test no llama a git.
   - Regenerarlo exige un motivo declarado en el commit y ZEUS.
2. **`test_every_legacy_closure_module_matches_main`.**
   - Para cada módulo del cierre (cortando `MODE_FRONTIER`), su sha tiene que ser el del manifiesto.
   - `ALLOWED_DIVERGENCE` es explícita y mínima, `{app.core.semantic_dimensions: "aditivo, lista blanca (pin 4)",
     app.core.foundation_guard: "hoja de frontera D13 (pin 6)"}`. **Ninguna de las dos queda abierta por nombre.**
     `semantic_dimensions` la cubren los pines 4 y 5. `foundation_guard` la cubre el pin 6: sha fijado y
     equivalencia de AST con `main`. Se amplía en E-0 con la reexportación de
     `AccreditedSpan` (ADR D3), con su motivo.
   - Un módulo del cierre que no está en el manifiesto es violación. `entity_resolver` ya es idéntico en la base;
     su pin de `alias_owners` sigue.
   - Este test **sustituye** a la lista a mano. `WAVE1_MODULES` y sus tests se conservan, porque son redundantes y
     baratos.
3. **`test_legacy_data_is_frozen` (D1(d)).**
   - **(a) Árbol de datos.** Cada archivo bajo `app/data/**` tiene el sha del manifiesto, y no hay archivos nuevos
     en `app/data/**`.
   - **(b) Datos leídos por el cierre, casados por ruta resuelta.** Se recorre cada literal de texto de cada módulo
     del cierre con un nombre de archivo que no sea `.py`: `.json`, `.yaml`, `.yml`, `.csv`, `.parquet`, `.txt` o un
     directorio de datos. Se resuelve:
     - como ruta relativa a la raíz del repo (`"artifacts/audits/municipal_m2a/logical_units.json"`);
     - como ruta relativa al módulo (`Path(__file__)…/"data"/…`);
     - como sufijo único de una ruta versionada del manifiesto.
     Cada literal que resuelve a un archivo versionado tiene que tener el sha del manifiesto.
     - Un literal que no resuelve y no está exento es violación.
     - Un literal que casa más de una ruta versionada también es violación (ambiguo).
   - **(c) Exenciones**, en una lista explícita y congelada con su motivo:
     - `pagination.json` / `.pagination.json` (`conversation_store.py:286,477`; `workflow/continuity_staging.py:45`):
       estado de runtime por chat, no versionado;
     - `/query.json` (`socrata_transport/soda3.py:123` y el docstring de `base.py:15`): URL de la API.
     Una exención nueva exige un motivo declarado.
   - **(d) Inalcanzabilidad.** Ningún módulo del cierre contiene el literal `host_data` ni importa `app.host_data`.
   - **No-vacuidad:** el escáner encuentra y congela `app/data/aliases_db.json` (`query_router.py:318`),
     `app/data/buyer_groups.json` (`buyer_groups.py:82`),
     `artifacts/audits/municipal_m2a/logical_units.json` y
     `artifacts/audits/municipal_m2b_1/impact.json` (`territorial_knowledge.py:43-53`). Además, un literal sintético
     `"artifacts/audits/nope.json"` en un override es violación.
4. **`test_semantic_dimensions_is_main_plus_new_members` (lista blanca, archivo).** Quita del archivo actual el
   bloque final de la clase que sigue al último miembro de `main` (`CONTROL_LANGUAGE`). Ese bloque sólo puede tener:
   - líneas en blanco;
   - comentarios;
   - asignaciones `NOMBRE = "NOMBRE"` con nombres que no estén en `MAIN_MEMBERS`.
   El resultado tiene que ser **byte-idéntico** a `main` (sha del manifiesto).
   `MAIN_MEMBERS = {UNSUPPORTED, RETRIEVAL_INTENT, TOPIC, MODALITY, CONTRACT_TYPE_OR_NATURE, ACTOR, BUYER, GEOGRAPHY,
   TIME, VALUE, STATE, CONTROL_LANGUAGE}`, congelado.
5. **`test_semantic_dimensions_use_in_legacy_is_whitelisted` (lista blanca, usos).** En cada módulo del cierre salvo
   `semantic_dimensions`, todo nodo AST que nombre `SemanticDimension` (como `Name` o como `.SemanticDimension`)
   tiene que estar en una de estas posiciones:
   - un alias de `import`/`from … import`;
   - el `value` de un `Attribute` cuyo `attr ∈ MAIN_MEMBERS`;
   - dentro de una anotación de tipo (`AnnAssign.annotation`, `arg.annotation`, `FunctionDef.returns`).
   Cualquier otra posición es violación: iterar, `len`, `list`, llamar a `SemanticDimension(x)`, `__members__`,
   pasarlo como valor, o un miembro nuevo.
   - La **excepción de anotaciones** es necesaria: `contract_type_resolver.py:48` declara
     `dimension: SemanticDimension = SemanticDimension.CONTRACT_TYPE_OR_NATURE`.
   - Sin la excepción, el pin fallaría sobre código idéntico a `main`. Con `from __future__ import annotations` una
     anotación no se evalúa.
6. **`test_foundation_guard_is_a_source_boundary_leaf`.**
   - **(a) Aristas.** Las aristas `scope="any"` de `foundation_guard` son un subconjunto de `{app, app.core,
     app.core.source_call_receipts}`.
   - **(b) sha fijado de la hoja tras la división.** Constante `FOUNDATION_GUARD_LEAF_SHA256`, con un comentario que
     cita el commit de la división.
   - **(c) Equivalencia de AST con `main`.** Para cada función que se queda en la hoja (`execution_guard`,
     `verify_effective_source_call`, `foundation_source_active`, `raw_source_rows`, `source_rows_raw`,
     `validate_foundation_count_total`) y para las asignaciones `_GUARD` y `_RAW_ROWS`, el `ast.dump` sin
     anotaciones es igual al de `main`.
     - El fuente de `main` se guarda como fixture, `tests/fixtures/foundation_guard_main_88653752.py.txt`, con su sha
       igual al del manifiesto.
     - La única diferencia admitida es la anotación de tipo de `_GUARD`. No-vacuidad con tres overrides en memoria: un
   import de nivel de módulo de `foundation_projection`, un `TYPE_CHECKING` que importa `foundation_execution_guard`
   y un import perezoso de `interpretation_contract`.
7. **`test_query_router_use_inside_foundation_is_declared` (D1(f), N4).**
   - En cada módulo de F (`foundation_*` salvo la hoja y los dos auxiliares, `host_authorities.*` e
     `interpretation_contract`), todo `ast.Attribute` con `attr == "query_router"` tiene que estar en un módulo de
     `QUERY_ROUTER_USERS_IN_F = {foundation_route_owner, foundation_shadow_hook, foundation_basic_fallback}`.
   - En el commit del CUTOVER, `foundation_basic_fallback` sale del conjunto, porque se borra.
   - **Destino de `foundation_shadow_hook`:** es un instrumento SHADOW observacional y sobrevive hasta M4 (roadmap);
     no es un papel de interpretación.
   - Hoy los usos son `foundation_shadow_hook.py:20` y `foundation_basic_fallback.py:89`, más
     `foundation_route_owner.py:17`.
   - No-vacuidad: un override con `workflow.deps.query_router` en `foundation_admission` es violación.
8. `MODE_FRONTIER` y su comentario `foundation_v3` cambian en C1.6, no aquí (deuda del ADR).

## 4. Mutantes (aplicar, correr, restaurar; todos deben morir)

| # | Mutación | La mata |
|---|---|---|
| F1 | `foundation_guard` vuelve a importar `foundation_projection` | pines 2 y 6 |
| F2 | `secop_client` importa `foundation_execution_guard` | pin 2 (sha de `secop_client` y módulo fuera del manifiesto) |
| F3 | Import perezoso de `interpretation_contract` en la hoja | pin 6 |
| F4 | `contract_type_resolver` hace `for d in SemanticDimension` | pin 5 (y pin 2: sha) |
| F5 | Añadir `def helper()` al final de `semantic_dimensions` | pin 4 |
| F6 | Cambiar un byte de `foundation_text_predicates.py` | pin 2 |
| F7 | Cambiar un byte de `app/data/aliases_db.json` | pin 3 |
| F8 | `query_router` lee `app/host_data/buyer/aliases.json` | pin 3 |
| F9 | Reexportar `ExecutionGuard` desde la hoja | pin 6 |
| F10 | `source_rows_raw` devuelve `True` | pin 6(b) (sha) y 6(c) (AST frente a `main`) |
| F11 | `artifacts/audits/municipal_m2b/adjudicated_candidates.json` cambia un byte | pin 3(b) |
| F12 | Un módulo de F (por ejemplo `foundation_admission`) llama a `workflow.deps.query_router.parse` | pin 7 |
| F13 | Quitar la exención de `pagination.json` | pin 3(c): el literal no resuelve, lo que prueba que la exención es necesaria |

## 5. Gates

1. `pytest -p no:cacheprovider tests/test_m1_wave1_parity_pins.py`, con los dos tests que hoy fallan en verde.
2. `tests/test_m1_wave1_legacy_mode_runs_no_foundation.py` en verde.
3. Tests focales: `tests/test_foundation_runtime.py`, `tests/test_foundation_token_runtime.py`, `tests/test_foundation_*.py`
   y todo test que mencione `secop_client`. Compara los rojos con la base pareada por nodeid con `comm`, porque
   `date_extract_y` ya estaba rojo antes.
4. Mutantes F1–F13.
5. `ruff check` sobre lo tocado y `olympus-semgrep-delta --base <commit del WIP>`.

## 6. STOP

- Algún consumidor legacy necesita algo de `ExecutionGuard` además de los ganchos ⇒ METIS. No se reexporta.
- Al congelar C1.2, el manifiesto muestra otro módulo del cierre que difiere de la base ⇒ METIS. No se amplía
  `ALLOWED_DIVERGENCE` sin decisión.
- Algún carril necesita cambiar `app/data/**` antes del CUTOVER ⇒ STOP. Por D1(d), la corrección va a
  `app/host_data/<dim>/`.

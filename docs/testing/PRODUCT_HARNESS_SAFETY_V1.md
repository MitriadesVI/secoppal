# PRODUCT_HARNESS_SAFETY_V1 — Fase 2.5

> **Revalidación F3:** los tests de seguridad del harness se ejecutan en main
> `04426d19`, con sondas explícitas para Socrata, proveedor LLM y Burr, y
> equivalencia de Text Lineage opt-in. Ver resultados y límites en
> [TEST_ARCHITECTURE_V1_F3.md](TEST_ARCHITECTURE_V1_F3.md). El estado WAIT que
> aparece abajo describe la entrega histórica F2.5.

**Estado:** entregado. Harness sin escenarios ejecutables todavía: F3 espera a que Text Lineage V1.2 esté en `main`.
**Origen:** decisión del asesor del 2026-09-12 tras F0/F1 (H10).
**Código:** [`tests/product_contract/`](../../tests/product_contract/) · **Fixtures:** [`tests/fixtures/product_contract_v1/`](../../tests/fixtures/product_contract_v1/)

> *No quiero construir un corpus precioso y descubrir luego que tres escenarios
> estaban verdes porque salieron silenciosamente a datos.gov.co.* — criterio del asesor.

## 1. Fase nueva en el plan

```text
F0  Baseline                         ✅
F1  Inventory                        ✅
F2  Invariant Registry               ✅  (INVARIANT_REGISTRY_V1.md)
F2.5 Product Harness Safety          ✅  (este documento)
     ├─ network forbidden
     ├─ frozen source adapter
     ├─ actor collision pack
     └─ deterministic persistence
F3  Product Contract 20              WAIT (Text Lineage V1.2 en main + refresh + revisión)
F4  Product Contract 100
F5  pytest taxonomy / gates
F6  consolidation                    ni un test se toca antes
```

## 2. Garantías y su prueba

Cada garantía tiene un autotest. Las críticas se verificaron **por mutación**:
se inyectó el defecto con un plugin de pytest desde el scratchpad, sin editar
el código, y se comprobó que el test cae.

| Garantía | Autotest | Mutante que lo mata |
|---|---|---|
| Un intento de red atrapado por el runtime igual pone el test en rojo | `test_a_swallowed_connection_attempt_still_fails_the_ledger` | el ledger deja de registrar → caen 3 tests |
| DNS, TCP y UDP quedan registrados; `AF_UNIX` no da falsos positivos | `test_dns_raw_connect_and_datagrams_are_all_recorded`, `test_local_unix_sockets_are_not_false_positives` | ídem |
| **H10 demostrado:** el workflow productivo sin fuente congelada sale a la red y el guard lo ve | `test_an_unmocked_production_workflow_is_caught_reaching_the_network` | ídem. Sonda: `socket.create_connection(('www.datos.gov.co', 443))` |
| El SoQL emitido se **evalúa**; no hay respuestas enlatadas | `test_soql_is_evaluated_not_canned`, `test_the_real_workflow_runs_on_the_frozen_source_without_network` | fuente que ignora el WHERE → caen 4 (el decoy entra al resultado) |
| SoQL no evaluable o dataset no congelado fallan ruidosos | `test_unevaluable_soql_and_unfrozen_datasets_fail_loudly` | ídem |
| `with_timeout` / `derived_for_timeout` nunca abren un Socrata real | `test_timeout_derivation_keeps_the_frozen_wire` | — (identidad del transporte) |
| Timeout o indisponibilidad inyectados atraviesan el `SecopClient` real | `test_injected_failures_surface_through_the_real_client` | fuente que ignora el WHERE |
| La persistencia vive bajo la raíz del escenario | `test_persistence_is_confined_to_the_scenario_root` | verificación ciega → cae 1 |
| El follow-up ve los params del turno anterior | `test_a_followup_turn_carries_the_previous_params` | — |
| El pack de actores pasa por el builder y el resolver reales | `test_the_actor_pack_builds_through_the_real_index_builder` | — |
| Una fila del pack sin forma de colisión se rechaza | `test_pack_rows_must_declare_their_collision_shape` | — |
| Los oráculos reportan **todas** las violaciones; una clave desconocida falla cerrado | `test_every_violated_oracle_is_reported_not_only_the_first` | — |
| Procedencia: los sha256 del manifest coinciden y ningún archivo queda fuera | `test_fixture_manifest_hashes_match_the_committed_files` | — |
| El corpus DESIGN es válido y cubre los 20 obligatorios | `test_the_design_corpus_is_valid_and_covers_the_twenty_mandatory_items` | — |
| Un defecto conocido nunca cuenta como verde | `test_known_product_defects_are_quarantined_and_never_green` | cuarentena con `counted_green` → el validador lo rechaza |

## 3. Diseño

### 3.1 Red prohibida por construcción

- `tests/product_contract/conftest.py` instala el guard como fixture **autouse** y
  verifica el ledger en el teardown. No es opt-in.
- Puertas cubiertas: `socket.connect`, `connect_ex`, `sendto`,
  `socket.create_connection` y `socket.getaddrinfo`. Sólo `AF_UNIX` queda libre.
- **`live` está prohibido en ese directorio.** Un test que necesite la fuente
  real pertenece a canary/dogfood y jamás a `test-product`.
- Burr trae la telemetría **encendida por defecto** (`g_telemetry_enabled = True`,
  destino PostHog). El harness la apaga explícitamente (ver §5 H13).

**Límites declarados:** no cubre subprocesos, extensiones nativas que abran
sockets por fuera del módulo `socket`, ni sockets abiertos antes de instalar el
guard. Para el runtime actual (httpx/sodapy/sqlite) no hay otra puerta.

### 3.2 Fuente congelada: se sustituye el cable, no el cliente

`frozen_client(FrozenTransport(...))` construye un **`SecopClient` productivo**
con transporte explícito. Así se conservan reintentos, instrumentación,
`_normalize_row` y derivación por timeout. `_respawn` propaga el transporte,
de modo que ninguna derivación puede abrir un Socrata nuevo.

- El SoQL emitido se evalúa en SQLite: se inserta `FROM` antes de la primera
  cláusula de nivel superior, fuera de los literales.
- Dialecto registrado: `unaccent`, `date_trunc_y` y `date_extract_y`,
  reutilizando `tests/buyer_actor_sql_dialect.py` y
  `tests/contract_universe_sql_dates.py`.
- Las filas salen como texto, igual que Socrata. Las columnas
  `valor_*`/`precio_*` tienen afinidad numérica para que las comparaciones sean
  numéricas.
- Las fallas (timeout, indisponibilidad) se inyectan en el cable con
  `InjectedFailure`, por operación y dataset.
- Todo SoQL no evaluable o dataset no congelado se registra como error, y el
  harness rompe el turno.

**Límite conocido (L1):** `LIKE` de SQLite ignora mayúsculas en ASCII y el de
Socrata no. Un predicado sin `upper()` podría pasar aquí y fallar en vivo. El
runtime actual normaliza con `upper(unaccent(...))`; F3 debe añadir un oráculo
`soql.contains` sobre esa normalización en los escenarios de texto.

### 3.3 Pack de colisiones de actores: tres niveles, funciones distintas

```text
Frozen collision pack (JSONL, versionado)  →  gate cotidiano determinista (make test-product)
Índice de producción (662 MB)              →  certificación pre-release / cambios grandes de Actor
                                              (make test-product-full-index, fuera de CI)
SECOP en vivo                              →  canary y dogfood
```

- **Formato:** `actor_alias_rows.jsonl`. Cada fila lleva los campos del builder
  más `_shape` (forma de colisión) y `_provenance`. Las claves `_*` nunca llegan
  al builder.
- **Hoy** el pack sólo tiene placeholders sintéticos (`_provenance:
  synthetic_placeholder`) para demostrar el camino JSONL → builder real →
  SQLite → resolver real.
- **Derivación real (F3), a revisar por el usuario antes de commitear** porque
  el pack contendría datos personales:
  1. abrir el índice de producción en solo lectura (`?mode=ro&immutable=1`),
     como ya hace `test_buyer_actor_role_local_v1`;
  2. seleccionar el mínimo de filas por forma: homónimo exacto del comprador,
     persona cuyo nombre contiene tokens del comprador, prefijo personal, mismo
     nombre con dos documentos, nombre que también es tema, NIT/CC y forma
     jurídica;
  3. minimizar: remapear cada documento a un número sintético **consistente**
     (mismo tipo y longitud, misma colisión), y conservar el nombre normalizado
     sólo cuando es la colisión que se prueba;
  4. registrar en `manifest.json` el `built_at` y el hash de metadata del índice
     de origen, la política de minimización y `contains_personal_data`;
  5. el test del manifest impide editar el pack sin actualizar su hash.
- `make test-product-full-index` queda **diseñado, no implementado**: los
  targets de Makefile son F5.

### 3.4 Persistencia determinista

- `ProductHarness` crea `Settings` con feedback, SQLite, conversaciones e índice
  de actores bajo la raíz del escenario, `deepseek_api_key=None` y `today` fijo
  por escenario.
- `unconfined_paths()` revisa las rutas de `Settings` y todas las `Path` de los
  stores (feedback, radar, conversaciones). El constructor falla si alguna
  escapa.
- La comprobación es estructural y no mira los `mtime` de `data/`, porque el
  Streamlit de desarrollo escribe ahí y daría falsos positivos.

### 3.5 Corpus y cuarentena

- `scenarios.yaml` tiene los 20 casos en **DESIGN**. `TBD_RUNTIME` marca todo
  valor canónico que debe salir del runtime acreditado, y queda prohibido en
  READY.
- `quarantine.yaml` separa dos cosas que hasta ahora compartían `xfail`:

| Categoría | Qué es | Regla |
|---|---|---|
| `OPEN_PRODUCT_GAP` | el producto está mal (actor + región + comprador; `change_scope` pierde tema; SENA Chocó/Casanare; alias de hospital; OPP-003; REFERENCE-001) | backlog con reproducción RED; **nunca** cuenta como verde; se promueve a escenario sólo cuando se arregle la conducta |
| `HARNESS_DEBT` | el test está incompleto (6 xfail de los demos de Observer 2.0; `test_opportunity_followup_soql`) | `REPAIR_MOCKS` o `MOVE_TO_AUDIT`; no se conserva como xfail no estricto indefinido |

## 4. Qué hace F3 con esto

1. Integrar Lineage → refresh del inventario → rebase de este carril → revisar
   que los 20 siguen siendo los correctos.
2. Por escenario: fixtures reales mínimas → **RED primero** (mutación que
   reproduce la causa → FAIL; runtime actual → PASS) → quitar `TBD_RUNTIME` →
   pasar a `READY`.
3. Crear `test_product_contract_v1.py`, que ejecuta sólo los READY a través de
   `ProductHarness` y `Turn.violations(expect)`.

## 5. Hallazgos nuevos

- **H13 — Telemetría de Burr encendida por defecto.**
  `burr.telemetry.g_telemetry_enabled` es `True` y apunta a
  `https://app.posthog.com`. Nada en el repo la apaga (`dev.sh`, `.env*`,
  `app/`); sólo algunos harnesses de auditoría. **Sonda:** con la fuente
  congelada y la telemetría por defecto, **un turno** de `run_query` hizo **3
  intentos** `socket.create_connection(('app.posthog.com', 443))` desde
  `burr/telemetry.py:_send_event_json`. Implicación probable: el runtime
  productivo envía telemetría de uso de Burr a un tercero en cada turno, y los
  tests de workflow sin guard intentan esa salida. No se verificó el entorno de
  despliegue (fuera de alcance); es una decisión del usuario, no de este carril.
- **Semgrep no cubre `tests/`.** Su ignore por defecto excluye esa carpeta
  (11 archivos ignorados), así que `.semgrep/secoppal-laws.yml` no acredita
  nada sobre el harness. Los gates aplicables aquí son Ruff y pytest.

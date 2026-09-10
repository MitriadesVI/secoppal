# Contract Universe V1.1 — Integración R5
## Compatibilidad Entity Profile · clusters C03 + C04

**Veredicto: `ENTITY_PROFILE_CU_COMPATIBILITY_ADJUDICATED`**

Base `d37dfdd`. Rama `codex/contract-universe-v1-1-r5-profile-compatibility`.
Sin merge, push, deploy ni restart. Sin suite global.

Los cinco rojos quedan cerrados **sin una sola línea de runtime modificada**.
El diff son dos fixtures y un módulo de tests nuevo.

---

## 1. Preflight

| Verificación | Resultado |
|---|---|
| Base exacta | `d37dfdd` — `fix(contracts): separate state provenance and universe metadata` |
| `main` | `c52d7ed`, ancestro de la base; no avanzó |
| Worktree | nuevo y limpio; `.venv` enlazado al del repo principal, como los demás worktrees CU |
| R4 verde | `r4_state_metadata` 16/16 |
| Residuales | 10, exactamente los de `RESIDUAL_20_AFTER_R4.json`; clusters C01, C02, C03, C04, C24, C25, C26 |

Los cinco rojos se reprodujeron antes de tocar nada
(`c03_before.json`, `c04_before.json`).

---

## 2. C03 — el cero falso de Galapa

**`OBSOLETE_FIXTURE`.**

La hipótesis del encargo era que `SqlProfileClient` fabricaba filas sin
`estado_contrato`. Es correcta, y hay una segunda causa por delante: las cuatro
consultas contractuales ni siquiera llegaban al predicado. Morían antes con

```
OperationalError: no such function: date_extract_y
```

Por eso el síntoma no era «0 contratos» sino «la fuente no respondió»: el
transporte SQLite del fixture emula `unaccent` pero no el resto del dialecto
SoQL. Con `date_extract_y` emulado aparece el cero real, y ahí sí manda la
segunda causa: `estado_contrato` NULL en toda fila contractual, ninguna dentro
del universo celebrated.

El probe en copia desechable añadió exactamente dos cosas —
`register_dates(conn)` y `estado_contrato = "En ejecución"` en `jbjy-vk9h` — y
las nueve aserciones del test pasaron: 2 contratos, comprador correcto, sin
`contratista` ni `actor_scope`, 6 llamadas, y `900333444` ausente de todo el
SoQL. El runtime de producto no cambió. `FIXTURE_INTEGRITY.md` detalla qué se
tocó y qué no.

Las cinco contrapruebas del encargo están congeladas en
`tests/test_contract_universe_r5_profile_compatibility.py`. La más importante:
una fila Galapa `Cancelado` y una `Borrador` **no** aumentan el total, con el
mismo comprador y el mismo periodo. Contract Universe no se desactivó para que
nada pasara.

---

## 3. C04 — el follow-up de proveedores

**`INVALID_HISTORICAL_FIXTURE`.**

El test tomaba `first["resolved_params"]` de un perfil de **procesos** y le
sobrescribía `dataset="contratos"`. Ese dict entra a
`app/core/workflow/actions/planning.py`, donde `compatible(previous)` es falso
—dataset contractual sin `contract_universe_version`— y el turno sale por
`heuristic_only` con «La búsqueda contractual anterior requiere revisión de
universo».

La pregunta central del encargo era si ese estado persistido es válido. No lo es.

| | P1 perfil real (procesos) | P2 perfil real (contratos) | P3 fixture |
|---|---|---|---|
| `dataset` | procesos | **contratos** | **contratos** |
| `contract_universe` | — | `celebrated` | **ausente** |
| `contract_universe_version` | — | `contract-universe-1.1` | **ausente** |
| `compatible()` | True | True | **False** |
| `sources` confirmadas | BOTH | **CONTRACTS** | **BOTH** |
| `sources_provenance` | DEFAULT | **EXPLICIT** | DEFAULT |

P2 lo genera la autoridad productiva real con
`"analiza la alcaldia de galapa solo contratos"`. P3 es un híbrido en **dos**
ejes: sin sello y con las fuentes de un perfil de procesos.

Tres evidencias de que P3 no lo produjo ninguna versión soportada:

1. `apply_request` termina siempre en `contract_universe.stamp`, que sella todo
   `dataset ∈ {contratos, jbjy-vk9h}`. Es el punto único.
2. `P3_UNREACHABLE_SCAN.json`: 90 turnos de perfil, 63 con `dataset='contratos'`.
   Cero sin sello. Cero con `sources='BOTH'`.
3. El artefacto V1 **real** que el repo conserva
   (`docs/audits/contract_universe_v1_1_independent/v1_independent_artifacts.json`,
   emitido por los serializadores de `067b397`) sí lleva
   `contract_universe_version: contract-universe-1`. Un perfil histórico
   legítimo trae el sello; lo que cambia es el valor, no su presencia.

Por eso no se amplió el runtime para soportarlo. El test crea ahora su perfil
contractual con la autoridad productiva, y las cuatro consultas quedan en
`entity_profile_suppliers`, con Galapa, `PROFILE_ACTIVE_PARAM`, sólo
`jbjy-vk9h`, universo sellado y los dos rankings.

El caso que el fixture *decía* representar —un perfil viejo o sin sello— no se
perdió: se congeló aparte, y su respuesta correcta es revisión, no migración
silenciosa.

---

## 4. Tests de compatibilidad de persistencia

Los seis del encargo, en `tests/test_contract_universe_r5_profile_compatibility.py`:

| Requisito | Test |
|---|---|
| Perfil current contractual válido → funciona | `test_a_current_profile_continues_into_the_supplier_followup[contratos]` |
| Perfil current procesos → funciona | `…[procesos]` |
| `celebrated` + versión actual → funciona | `test_a_stamped_celebrated_profile_keeps_its_universe` |
| `source_records` explícito válido | `test_an_explicit_source_records_profile_executes_under_its_own_universe` |
| Versión vieja/incompatible → fail-closed | `test_an_old_or_unstamped_contractual_profile_is_sent_to_review` (`contract-universe-1` y sin versión) |
| Fixture imposible ≠ compatibilidad histórica | `test_no_profile_turn_persists_a_contract_dataset_without_its_universe` |

Una salvedad, declarada: el cuarto requisito pedía que un perfil
`source_records` **mantuviera** su universo a través del follow-up. El runtime
no lo mantiene — la vista de proveedores vuelve a `celebrated`. Lo hace
declarándolo en la respuesta («Contratos celebrados según las señales publicadas
en SECOP»), así que no hay cambio silencioso, y el turno no cae a revisión ni a
`heuristic_only`. La ruta tabular sí conserva `source_records`
(`test_complete_continuity_and_export`), de modo que la asimetría es entre rutas
y es **preexistente a R5**: no se tocó runtime. Se registra como deuda
`ENTITY_PROFILE_SOURCE_RECORDS_INHERITANCE_DEBT` y el test congela lo que sí es
exigible hoy: el turno continúa y la respuesta nombra el universo que aplica.
Cerrarla exigiría tocar el runtime de Entity Profile, fuera del alcance de R5.

---

## 5. Mutaciones

Las cuatro mueren por `assert`. Ninguna por error de importación ni timeout.
Baseline restaurada verde tras cada una (23 passed).

| | Mutación | Muerta por |
|---|---|---|
| M1 | Filas contractuales sin estado (fixture) | `assert 'Se observan 0 contratos' not in …` — vuelve el cero falso |
| M2 | `_merge_new_search` hereda `raw_params` del turno previo | `assert 'Se observan 2 contratos' in …` — el Actor previo reaparece y bloquea el perfil |
| M3 | `planning.py` deja de exigir `not compatible(previous)` | `assert 'heuristic_only' == 'entity_profile_suppliers'` — se rompe la continuidad de un perfil válido |
| M4 | `compatible()` devuelve `True` siempre | `assert False is True` — la versión vieja deja de ir a revisión |

M2 mata por la aserción de conteo y no por la de fuga del NIT: con el Actor
heredado el perfil ni siquiera se ejecuta («el conjunto de procesos no publica
quién contrata»). El mutante muere igual y por assertion; se anota tal cual.

---

## 6. Gates focales

| Gate | Esperado | Obtenido |
|---|---|---|
| C03/C04 | 5/5 | **5 passed** |
| Contract Universe auditor | 19/19 | **19 passed** |
| F2.2 | 34/34 | **34 passed** |
| Frozen | 43/43 | **43 passed** |
| Corpus | 89/89 | **89/89 PASS** |
| Buyer–Actor C05–C08 | 44/44 | **44 passed** |
| R3 SQL oracles | 34/34 | **34 passed** |
| R4 state metadata | 10/10 | **16 passed** (el archivo completo; los 10 nodeids R4 dentro) |
| Controles R5 nuevos | — | **14 passed** |
| Entity Profile + follow-up | — | **1074 passed** |

El último no lo exigía el encargo —no se tocó runtime de Entity Profile— pero
el módulo de tests modificado exporta constantes que otro módulo importa, así
que se corrió la familia completa.

---

## 7. Replay de los 10 residuales

`RESIDUAL_10_AFTER_R5.json`

```
before               10
now_pass              5
still_fail            5
errors                0
skipped               0
clusters_still_present  C01, C02, C24, C25, C26
```

Los cinco que pasan son exactamente C03 y C04. C01/C02 (Actor routing),
C24 (state catalog), C25 (arquitectura temporal) y C26 (export) siguen rojos y
no se tocaron.

---

## 8. Estáticos

| | Resultado |
|---|---|
| Ruff `app/ tests/ scripts/` | All checks passed |
| Semgrep `.semgrep/secoppal-laws.yml app/` | 10 hallazgos, **0 nuevos**, 0 desaparecidos; los 10 archivos idénticos byte a byte a `d37dfdd` |
| `make lint-core` | `app/core/ limpio` |
| Topes de LoC | 2 passed |
| `git diff --check` | limpio |
| Suite global | **no ejecutada**, por instrucción |

---

## 9. Limitaciones

- No se corrió la suite global. Los totales de la tabla de gates se solapan.
- `ENTITY_PROFILE_SOURCE_RECORDS_INHERITANCE_DEBT` queda abierta (§4).
- El MCP de codegraph no conectó en esta sesión; la exploración estructural se
  hizo con `rg` y lectura directa. No se afirma nada apoyado en el índice.
- `main_behavior` en la matriz no es una medición: `app/core/contract_universe.py`
  no existe en `c52d7ed`, así que estos cinco nodeids no tienen contraparte
  observable ahí. Se declara la ausencia en vez de inventar una comparación.

# DOGFOOD-001 — Presupuesto de ejecución y observabilidad del carril PROFILE

Base: `main @ ae5b608`. Alcance cerrado: presupuesto, observabilidad y lenguaje
de fallo. **No** se tocó Natural Alpha V3, Residual B, CapabilityCredential,
autoridades, resolución de comprador, semántica de ranking ni ningún timeout
aprobado. ADR-073 sin asignar.

## 1. Incidente congelado

Consulta real, canal `streamlit`:

```
cuales han sido los contratistas con mas contratos con la alcaldia de soledad en 2026?
```

Dos turnos de la misma consulta quedaron en `data/secoppal.db`, con seis minutos
de diferencia y por carriles distintos. La comparación es la evidencia:

| | `8eb7b3ba13fa` (20:22:08Z) | `d85de5f5773f` (20:28:44Z) |
|---|---|---|
| carril | legacy (perfil dentro de `execute_query`) | PROFILE (Frame) |
| `natural_alpha_status` | MATCHED · ANALYZE/SUPPLIERS | MATCHED · ANALYZE/SUPPLIERS |
| `natural_alpha_latency_ms` | 1 627,7 | 1 379,0 |
| `entity_profile_calls_total` | 2 | 2 |
| `entity_profile_total_latency_ms` | **8 691,2** | **6 411,4** |
| `entity_profile_accredited_metrics` | — | 0,0 |
| `results_count` | 0 | 0 |
| `total_ms` | 11 220,8 | `null` |

Todo lo lingüístico fue correcto en los dos: Natural Alpha, comprador, periodo,
SUPPLIERS, ranking, admisión y Frame. Los dos jobs `supplier_count` /
`supplier_value` **llegaron a ejecución y expiraron**.

### 1.1 Presupuesto restante al entrar a `execute_plan`

No está en la traza — ése es precisamente el segundo hallazgo. Se reconstruye de
forma determinista desde la aritmética del deadline y la latencia medida:

```
timeout_s = max(1, int(fetch_budget_s))
fetch_budget_s = min(ENTITY_PROFILE_TIMEOUT_PER_CALL, restante − CLOSING_RESERVE)
               = min(8, (15 − transcurrido) − 4)
```

* legacy midió 8 691 ms de socket ⇒ `timeout_s = 8` ⇒ ventana completa.
* PROFILE midió 6 411 ms ⇒ `timeout_s = 6` ⇒ `fetch_budget_s ∈ [6, 7)` ⇒
  transcurrido al entrar a `execute_plan` ∈ (4, 5] s.

Los ~4,5 s son Alpha (1,38 s) más resolución de comprador, admisión y
compilación. **La fase lingüística le estaba cobrando su latencia al socket de
SECOP: dos segundos menos de espera por llamada que el mismo turno por legacy.**

### 1.2 Por qué legacy no lo sufría

`legacy_budget(observed)` abre una ventana de deadline **nueva y completa** antes
de `handle_normal_flow`. La política existía; el carril PROFILE del Frame se
quedó fuera de ella por construcción, no por decisión.

### 1.3 Estados de los jobs: no observables

La traza del turno PROFILE **no** registraba, por job: si se intentó, con qué
estado terminó, cuántas filas volvieron ni su latencia. Con `results_count = 0`
y sin esos campos, «SECOP expiró en las dos consultas» y «este comprador no tiene
actividad» dejan exactamente la misma huella. Tampoco llegaban a `trace_json`
`credential_capability`, `authority_status`, `legacy_bypassed` ni el recibo del
verificador de residual: `log_trace` copia una lista explícita de campos y ésos
no estaban en ella.

## 2. Lo que se cambió

### A · Paridad de presupuesto

`app/core/natural_alpha/budget.py` deja de ser «el presupuesto de legacy» y pasa
a ser la política de ejecución, con los dos carriles pidiéndola por su nombre:

```
legacy_budget(observed)   → handle_normal_flow      (comportamiento idéntico)
profile_budget(observed)  → run_profile_route       (nuevo)
```

Misma condición (`observation is not None`), misma ventana (15 s), mismo
mecanismo de restauración. **No se aumentó el deadline del turno y no existe
ningún timeout especial para Soledad**: lo que cambia es que la ejecución del
Frame vuelve a recibir 8 s por llamada en vez de 6.

Invariante ejercitado:

```
Alpha tarda X · Residual B tarda Y · PROFILE entra a execute_plan
→ execution budget = política completa existente
```

### B · Observabilidad persistida

`app/core/entity_profile/execution_trace.py` (módulo nuevo, una
responsabilidad). Por job del Frame:

```
job_id · dataset_id · attempted · status · rows_received · latency_ms
```

`attempted=False` (BUDGET_EXHAUSTED) y `status=TIMEOUT` dejan huellas distintas:
una consulta que nunca salió no es una consulta que expiró. Los campos del turno
credenciado —`credential_capability`, `admission_reason`, `legacy_bypassed`,
`query_router_calls`, `authority_status`, `authority_reason`,
`residual_verifier_called`, `residual_verifier_status`, `residual_units`,
`residual_labels`, `weak_actor_guard_called`— pasan por el mismo validador y
llegan a `trace_json`.

Reglas heredadas del recibo A2: valores enumerados o numéricos, texto acotado,
**sin claves de API y sin SoQL**; un campo que el runtime no fijó está ausente,
no puesto por defecto.

### C · UX de fallo

`app/core/entity_profile/failure_language.py` (módulo nuevo). Tres causas, tres
frases:

```
TIMEOUT           SECOP no respondió a tiempo en las dos consultas sobre los proveedores.
ERROR             SECOP falló al responder las dos consultas sobre los proveedores.
BUDGET_EXHAUSTED  No alcancé a emitir las dos consultas sobre los proveedores:
                  el turno se quedó sin presupuesto antes de consultar a SECOP.
```

Un fallo mixto los enumera. En todos los casos sigue la cláusula que impide leer
el silencio como un cero. La advertencia de sección deja de decir «la fuente no
respondió» cuando la consulta nunca se emitió.

En la traza de la UI, `LLM: No` sigue diciendo la verdad sobre sí mismo (es el
fallback legacy de parseo) y ya no puede leerse como «aquí no intervino ningún
modelo»: se muestra aparte `Natural Alpha: Sí (MATCHED)` y la tabla de jobs con
su estado.

## 3. Gates

| Gate | Resultado |
|---|---|
| `query_router` en turno credenciado = 0 | verde (`test_natural_job_credentialed_admission`) |
| fallback a legacy = 0 | verde (idem) |
| semantic hard gates = 0 | verde |
| fallo de provider sigue fail-closed | verde |
| comportamiento legacy sin regresión | verde (`test_natural_alpha_deadline`, 4 casos) |
| caso Soledad deja evidencia inequívoca aunque SECOP expire | verde (`test_dogfood_001_execution_budget`) |
| suite global sin regresión | **0 rojos nuevos** |

Suite global, mismo entorno en los dos lados (el worktree de baseline recibió el
`data/` no rastreado; con eso los *skips* pasan a coincidir, 10 y 10):

```
baseline ae5b608   121 failed · 15 447 passed · 10 skipped · 23 xfailed · 2 errors
candidate          119 failed · 15 472 passed · 10 skipped · 23 xfailed · 2 errors
```

Los conjuntos se comparan como conjuntos, no por su número:

* **rojos nuevos: 0.**
* dos rojos del baseline no aparecen en el candidate —
  `test_contract_universe_independent_safety.py::test_ledger_rejects_swallowed_
  aggregate_attempt_outside_workflow[with_timeout|derived_for_timeout]`— y **no
  los arregla este cambio**. Copiados los archivos del candidate DENTRO del
  worktree, los dos siguen fallando allí: con el código constante, lo que decide
  es el entorno. El canario que el test lanza como subproceso no importa ninguno
  de los módulos tocados aquí, y `secop_client.py`, el offline guard y el propio
  test están intactos. La aserción es sobre el literal `'1 passed, 1 error'` y el
  worktree produce `'1 passed, 2 warnings, 1 error'`.

Los siete cambios se verificaron **por mutación**, no por que los tests pasaran:
quitar el wrapper de presupuesto, impedir que la ventana se abra, vaciar los
recibos, desconectar la persistencia, volver a la frase única, fijar la condición
del wrapper en `True` y esconder Alpha en la UI matan cada uno su test.

## 4. Deuda observada, NO tocada

* `log_trace` corre **antes** de `attach_telemetry` / `attach_a2_telemetry` /
  `attach_deadline_telemetry` en `_run_turn`. Por eso `total_ms`,
  `turn_deadline_ms` y el recibo A2 no llegan a `trace_json` en ningún carril.
  Es estructural y precede a este trabajo; se deja declarada como
  `TRACE_WRITTEN_BEFORE_TURN_TELEMETRY_DEBT`.
* Suite roja pre-existente: la suite global ya venía roja en `ae5b608`. La
  comparación se hace con el MISMO entorno en los dos lados (el worktree de
  baseline no traía `data/`, que no está rastreado, y eso por sí solo movía los
  *skips* de 19 a 10); los conjuntos de fallos se comparan como conjuntos, no
  por su número. Familia dominante: `no such function: date_extract_y`.
* Dos tests escriben dentro de ficheros RASTREADOS —
  `docs/audits/contract_universe_v1_1_f2_1_final/observations.jsonl` y
  `docs/audits/contract_universe_v1_1_f2_2/product_trace.jsonl`— así que toda
  corrida completa deja el repo sucio sin que nadie los edite. Pre-existente y
  fuera de alcance; queda anotado porque enturbia la propia distinción de
  «trabajo sucio pre-existente» que el cierre exige poder probar.

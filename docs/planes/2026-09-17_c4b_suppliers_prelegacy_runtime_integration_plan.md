# C4B — SUPPLIERS PRELEGACY RUNTIME INTEGRATION (plan de ejecución)

Base congelada: `mo/natural-job-prelegacy-v2` @ `20568fdc24b7e524820a47bba45193f400b47574`
Lab cerrado y adjudicado: B_LLM ganador (MATERIAL_FN=0, transfer hard gates=0),
`C_LOCAL`/spaCy REJECT, weak actor exact guard DESPUÉS de B.

Este documento es el plan de ejecución del checkpoint C4B. **No se tocó runtime**
en el commit que lo introduce: el objetivo es que la implementación se haga en una
ventana de contexto limpia, con el contrato y los gates ya escritos.

## Por qué no se ejecutó aquí

El C4B exige leer y modificar a la vez `assembly.py`, `guards.py`,
`admission.py`/`turn_coordination.py` y la ruta de perfil del Frame, más tests,
replay y suite global. Empezarlo sin poder cerrar la verificación dejaría el
runtime a medio integrar; se detiene antes de la primera edición.

## Contrato productivo (paso 1)

- `app/core/accreditation/residual_verifier.py` (nuevo):
  - `ResidualRoleBase V1`: dataclass con la instrucción semántica y la llamada al
    provider; **copia literal** del contenido de
    `experiments/natural_job_prelegacy_v2/residual_role_prompt_b.txt`.
  - `ResidualRoleContract V1` para SUPPLIERS: `human_job="ANALYZE/SUPPLIERS"`,
    input = `{raw_query, human_job, residual_units:[{id,text}]}`.
  - Salida cerrada: `JOB_LANGUAGE | MATERIAL | UNKNOWN`, una etiqueta por id.
  - Validación de host idéntica al spike (ids faltantes/nuevos/duplicados, label
    inválido, JSON malformado, error de provider, timeout) → todo CLARIFY.
  - Sin spans, sin SQL, sin QueryPlan, sin herramientas, sin params ejecutables.
- Drift test: `tests/test_residual_verifier_contract.py` compara el texto
  productivo contra el archivo congelado (sha256 756fe274…) y falla si difieren.

## Activación (paso 2)

Sólo con `credential == entity_profile.suppliers`, ensamblaje llegado al punto
residual y `residual_material != ()`. Residual vacío ⇒ **cero llamadas B**.

## Orden de autoridades (paso 3)

```
strong actor (detect_actor_name — NO se mueve)
→ autoridades deterministas (buyer, time, ranking, supplier request, source, modality, money, geography)
→ residual
→ B verifier
→ weak actor_exact_guard
```

- B devuelve MATERIAL/UNKNOWN ⇒ `CLARIFY` inmediato y **weak actor guard NO se
  ejecuta**.
- B aprueba (todas JOB_LANGUAGE) ⇒ se ejecuta `actor_exact_guard` (preserva
  A01/A02/H15; elimina los falsos bloqueos D01/H05, que quedan bloqueados por
  material, que es la razón correcta).
- No se toca `ActorNameResolver.exact_aliases_in`.

## Assembly (paso 4)

```
residual vacío                                   → READY
residual no vacío + B all JOB_LANGUAGE + weak actor clear → READY
residual no vacío + cualquier veto/error         → CLARIFY
```

Receipts nuevos: `residual_verifier_called`, `residual_verifier_contract`,
`residual_units`, `residual_labels`, `residual_verifier_status`,
`residual_verifier_latency_ms`, `weak_actor_guard_called`. Nunca API key.

## Admission (paso 5)

`AccreditationAssembly.READY` deja de terminar en `accredited_execution_pending`:
`AdmissionRoute.PROFILE`, `owner=ENTITY_PROFILE`, `params=accredited_params`,
credential/spec preservadas. CLARIFY y OTHER_OWNER siguen terminales fail-closed.
Credentialed: `QueryRouter calls = 0`, `legacy fallback = 0`.

## Frame (paso 6)

Reusar el runtime existente; **no** tercera ejecución. La ruta READY entrega
`accredited_params` al profile route y `resolve_profile_scope()` acepta el mismo
scope acreditado en C4A.

## Tests obligatorios (paso 7, sin red)

| caso | esperado |
|---|---|
| con quien ha contratado la alcaldia de galapa en 2025 | B llamado → READY |
| cuales son los proveedores con contratos mas grandes de alcaldia de barranquilla | residual limpio → READY |
| proveedores de educación de Secretaría de Educación de Cali | B MATERIAL → CLARIFY |
| proveedores de licitación pública de Galapa | modality guard → CLARIFY, B NO llamado |
| proveedores de Galapa por más de 500 millones | money guard → CLARIFY, B NO llamado |
| A01 / A02 (actor subject) | nunca READY |
| D01 obras civiles | B MATERIAL → CLARIFY por material, weak actor NO llamado |
| H05 "con qué empresas suele contratar…" | B UNKNOWN → CLARIFY, weak actor NO llamado |

## Replay y gates (paso 8)

Replay DEV + HOLDOUT grabados con el runtime candidato. Obligatorio en cero:
WRONG_CAPABILITY_READY, WRONG_BUYER, SILENT_FILTER_LOSS, ACTOR_STOLEN,
NEGATED_EXECUTED, UNSUPPORTED_CARDINALITY_EXECUTED, MATERIAL_FALSE_NEGATIVE,
QUERY_ROUTER_CREDENTIALED, LEGACY_FALLBACK_CREDENTIALED.

Comparar READY contra la simulación C4A.5 (`B_BEFORE_WEAK_ACTOR_GUARD` ≈ 15
supported READY, artefacto `residual_role_ordering_simulation.json`), explicando
caso por caso la diferencia.

## Gates de código (paso 9)

Focales C1–C4B, Frame/Profile, Natural Alpha, actor; Ruff; Semgrep
`.semgrep/secoppal-laws.yml`; y si pasan, suite global. Sin SECOP en vivo.

## STOP CONDITIONS

STOP si hiciera falta: QueryRouter para un turno credenciado, vocabulario nuevo
de verbos, Prompt V3 modificado, Prompt B tuneado, otro LLM, spaCy, buyer fuzzy,
tercera ruta de ejecución o fallback silencioso a legacy.

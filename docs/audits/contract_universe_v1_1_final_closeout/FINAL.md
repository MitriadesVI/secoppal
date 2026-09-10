# Contract Universe V1.1 — cierre final

Rama `codex/contract-universe-v1-1-r5-profile-compatibility`, base `d37dfdd`.

## R5_COMMIT

`0256677` — test(entity-profile): align Contract Universe profile fixtures

## Commits finales

| | |
|---|---|
| `0256677` | test(entity-profile): align Contract Universe profile fixtures |
| `e01a095` | fix(actor): preserve actor ownership for contracted-action intent |
| `c85dd08` | fix(state): make suspended contract semantics dataset-aware |
| `c3d5018` | refactor(truth-layer): use public temporal validation boundary |
| `d6e29c5` | test(export): use accredited Contract Universe metadata |
| `ded6ae4` | fix(contracts): keep the temporal boundary dormant and re-pin its consumers |

## Las cinco causas raíz

**C01 + C02 — propiedad del span del Actor.** No fue `contract_universe_intent`:
para ambos casos `source_occurrences = []` y `universe_request = None`. La
regresión venía del patrón nuevo `\b(?:que\s+)?(?:ha|han)\s+contratado\b` en
`INTENT_PATTERNS`, que consumía su marcador **antes** que las puertas de Actor.
Esas puertas casan un span ATÓMICO que incluye el marcador —
`detect_asserted_actor_name_before_document` devuelve
`"que ha contratado asilo de san antonio"` — así que el `replace` no encontraba
nada y el nombre sobrevivía: a `objeto`, y al gazetteer de compradores, que
resolvía "SAN ANTONIO" como E.S.E. HOSPITAL SAN ANTONIO. Medio nombre de un
actor se convertía en otro comprador real. El scrub del marcador se difiere
hasta después de las puertas, junto a la regla de framing que ya lo hacía por
la misma razón. `force_dataset` se aplica en la detección y no se toca: el
dataset sigue sin depender del flag.

**C24 — suspensión dataset-aware.** V1.1 añadió `contrato_suspendido` pero dejó
`Suspendido` también en `no_procede`, con un early return hardcodeado tapándolo.
Bajo V1.1 eso es una contradicción: un contrato suspendido es CELEBRADO, un
registro cancelado queda fuera del universo. La suspensión pasa a resolverse
por ciclo de vida en `DATASET_AWARE_FAMILY` y el caso especial desaparece. Los
resultados de proceso ("desierto", "revocado") ya no inventan lectura
contractual; su comportamiento en procesos es idéntico a main. El follow-up
tenía el mismo defecto en un segundo sitio: "y los suspendidos" se parsea sin
contexto, resuelve sobre el dataset por defecto y llegaba como `no_procede`,
que SÍ proyecta sobre contratos — por eso el rebase existente nunca corría y
el usuario recibía Cancelado. La transición relee las mismas palabras bajo el
dataset heredado. Cierra el límite que R4 documentó.

**C25 — frontera temporal.** Contract Universe y el formatter preguntaban si un
valor publicado es realmente una fecha importando un resolver privado del Truth
Layer. El guard no se relajó ni se duplicó el parser: la pregunta se hace una
vez, por la raíz del paquete. La paridad se midió antes del cambio sobre fechas
civiles, timestamps naive y con offset, texto libre, vacíos y calendarios
imposibles: idéntica en todos los casos. El import es diferido para que la capa
siga dormida en un import normal de la aplicación.

**C26 — metadatos de exportación.** Los `resolved_params` del fixture son
anteriores a Contract Universe, así que representaban una búsqueda contractual
que el runtime actual no puede producir. `build_results_xlsx` hacía bien en
rechazarla. El fixture se sella con `contract_universe.stamp`, la autoridad
productiva, y ahora se fijan los dos lados de la protección.

## Replay

```
residual 10 (los previos a R5)   10 passed   0 failed   0 errors   0 skipped
cinco bloqueadores                9 passed (incluye los hermanos parametrizados)
```

## Suite global

```
14349 passed, 19 skipped, 15 xfailed in 796.04s (0:13:16)
```

0 failed. Sin `-k`, `-m`, `--ignore`, `--deselect`, `-x` ni `maxfail`.

Primera pasada: 3 failed, todos causados por este cierre y todos cerrados en
este mismo worktree (`ded6ae4`) — la capa temporal despertando en el import, el
sha pineado de `formatter.py` y un fixture de migración cuya familia dejó de
declarar su propio valor.

## Estáticos

| | |
|---|---|
| Ruff | All checks passed |
| Semgrep | 10 heredados, **0 nuevos**, 0 desaparecidos (comparados por archivo+regla+texto casado) |
| lint-core | `app/core/ limpio` |
| size | 2 passed |
| diff-check | limpio |
| Frame | `secoppal_entity_profile_frame_enabled = False` |
| POLICY_VERSION | `contract-universe-1.1` |

## Integración

| | |
|---|---|
| main antes | `c52d7eda355f44278164d4cb9803a199eec83486` |
| HEAD final | `ded6ae4c09d3085b82c15c1945efcab5d446b73a` |
| main después | (ver POSTFLIGHT.json) |
| modo | `--ff-only`, sin merge commit, squash, rebase ni cherry-pick |
| push / deploy / restart | no |

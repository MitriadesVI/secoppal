# Authority Map — NATURAL_JOB_PRELEGACY_V2 (estado tras C1/C2)

Base: `main @ 6b30737` · Rama: `mo/natural-job-prelegacy-v2`
Este documento describe lo que está implementado, no lo que se planea. La
columna "C3" marca la frontera: nada de esa columna existe todavía.

## Dimensión → autoridad

| Dimensión | Autoridad | Input | Estado C1/C2 |
|---|---|---|---|
| Job | Natural Alpha (observación) | raw text | observada UNA vez por turno, antes de Admission; nadie la lee todavía |
| Credencial | `app/core/accreditation/credential.py` | observación aterrizada | contrato declarado, sin consumidor |
| Spec de capability | `app/core/accreditation/registry.py` | — | SUPPLIERS declarado como data; sin consumidor |
| Buyer | QueryRouter + EntityResolver (legacy) | raw text vía parser | sin cambios |
| Time | QueryRouter (legacy) | idem | sin cambios |
| Actor owner | autoridad Actor existente | raw text vía parser | sin cambios |
| Ranking / control | Control Language (params legacy) | idem | sin cambios |
| Residual | `capability_binding.residual` (sidecar A2) | params legacy | sin cambios |
| Source / universo | SoQL builder (legacy) | params | sin cambios |
| Cardinalidad | autoridad A2 existente | params + evidencia | sin cambios |
| Polaridad | autoridad A2 + sidecar existente | raw text | sin cambios |
| Frame | Frame V1.2 (`admission` + `run_profile_route`) | sobre + params | sin cambios |
| Executor | runtime de perfil / legacy | plan compilado | sin cambios |

Ninguna dimensión ejecutable ganó una segunda autoridad: C1/C2 no ejecutan nada
por la credencial.

## Qué cambió C2, exactamente

- Punto de observación: de "dentro de `_dispatch`, después de los controles
  estructurados" a "antes de `admit(...)`, después del gate `observes_turn`".
- Gate: reutiliza la taxonomía existente (`input_shape` para reset, refinement,
  pagination, ordinal y affirmative; el detector de radar; el de direct lookup) y
  los campos estructurados del contexto. No agrega gramática ni vocabulario.
- Invariante medido: el provider se llama **a lo sumo una vez por turno** en todas
  las rutas; los fast paths (reset, actor selection, radar, pagination,
  suggestion selection, direct lookup) siguen con **cero** llamadas.
- El testigo del seam (`note_seam`) sigue en el seam del dispatcher: las clases de
  cobertura no cambian de significado. Un turno que el carril Frame responde
  queda `PROFILE_BYPASS` aunque haya sido observado antes de la admisión.
- La Observation llega a `admit(observation=...)` como input inerte: probado que
  la decisión es idéntica con y sin ella.
- `QueryRouter` sigue parseando toda ruta: `admit()` no tiene carril credenciado.

## Frontera de C3 (NO implementado)

- `authority_assembler(raw_query, credential, capability_spec)` → `accredited_params`
  invocando las authorities, sin volver a interpretar dimensiones.
- Carril credenciado en Admission: `QUERY_ROUTER_CALLS_ON_CREDENTIALED_TURNS = 0` y
  `LEGACY_GRAPH_ENTERED_WITH_VALID_CREDENTIAL = 0`.
- Residual calculado sobre el raw query desde spans acreditados (plan V2 6 y 17),
  con el check de sobrealcance del `evidence_interval`.
- `CapabilitySpec` como control plane (EXECUTE / CLARIFY / OTHER_OWNER) y entrada
  al runtime Frame V1.2 sin el parser tabular.

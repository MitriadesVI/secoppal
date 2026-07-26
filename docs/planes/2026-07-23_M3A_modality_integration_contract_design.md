# M3A — Auditoría y diseño del contrato de integración de modalidades

> **DOCUMENT_STATUS:** `HISTORICAL_USEFUL`
> **CURRENT_AUTHORITY:** `2026-07-25_M3C_modality_conversation_closeout.md`
> Conserva el mapa previo de integración y el razonamiento histórico de M3A,
> pero no gobierna el comportamiento as-built. Ante cualquier contradicción,
> manda el closeout vigente.

> Ronda de solo-diseño. No se modifica código de producción. No hay commit,
> merge, rebase ni push. Único artefacto: este documento.

---

**MODEL_USED:** claude-opus-4-8 (Claude Opus 4.8)

**BRANCH:** `search/modality-contract-v1`

**HEAD:** `2bae6cb62de4d9e9ea36f7330ebdb4acba662352`
(esperado `2bae6cb…` ✓ — coincide)

**WORKTREE_STATUS:** limpio. Worktree dedicado en
`/Users/rodrigoortiz/Documents/secoppal-modality`. `git status --short` sin
cambios. La auditoría se realizó en este worktree; el worktree principal
(`radar/mvp-manual`) no fue tocado.

---

## FILES_INSPECTED

M2 (contrato nuevo de esta rama, congelado):
- `app/core/modality_resolution_models.py` — contrato de salida inmutable (leído completo)
- `app/core/modality_resolver.py` — fachada `resolve()` y orquestación (leído: 1–188)
- `app/data/modality_catalog_v1.yaml` — catálogo M1, campo `dataset_values`
- `tests/fixtures/modality_resolver_corpus_v1.yaml` — 301 casos oro (muestreado para trazado)

Pipeline existente (compartido, base `e3ab072`):
- `app/core/query_router.py` — parser `parse()`, pasos 0–12, `_extract_modality`, `MODALIDAD_KEYWORDS`, `ParsedQuery`
- `app/core/query_frame.py` — `QueryFrame`, `frame_from_params`, `params_from_frame`, `merge_frames`
- `app/core/followup_mutations.py` — `MUTATION_POLICIES`, `gate_delta`, `FILTER_FIELDS`
- `app/core/followup_engine.py` — `detect_and_merge` (orquestación de follow-up)
- `app/core/workflow/actions/parsing.py` — nodos Burr `parse_query` / `llm_parse` / `apply_context`
- `app/core/query_fingerprint.py` — huella sha256 canónica
- `app/core/soql_builder.py` — `DatasetSpec`, cláusula `modalidad_de_contratacion`
- `app/core/llm_handler.py` — schema LLM con `modalidad`
- Lectores secundarios: `formatter.py`, `suggester.py`, `opportunity_scope_policy.py`,
  `recall_audit.py`, `refinement_recovery.py`, `dialogue_policy.py`
- Tests índice: `test_query_router.py`, `test_query_frame.py`, `test_followup_*`,
  `test_soql_builder.py`, `test_query_fingerprint.py`, `test_frame_leak_canary.py`

---

## CURRENT_MODALITY_FIELDS

Hoy la modalidad es **un único string escalar** que viaja como
`params["modalidad"]` y, dentro de `QueryFrame`, como una entrada de
`modifiers["modalidad"]` (clave declarada en `_MODIFIER_KEYS`,
`query_frame.py:60`). Su valor es directamente el string oficial de
`modalidad_de_contratacion` (p. ej. `"Contratación directa"`), es decir,
**no hay separación entre alias, familia y valor SoQL**: el escalar ya es el
valor live. No existe representación de includes/excludes, ni de
procedure_ids, ni de familia vs. procedimiento, ni de issues.

## CURRENT_WRITERS

1. **Heurístico** — `query_router.py:670-672`:
   `modality = self._extract_modality(normalized); params["modalidad"] = modality`.
   `_extract_modality` (`:794`) es un match de substring sobre
   `MODALIDAD_KEYWORDS`, gana el alias más largo, retorna el string oficial.
   **No consume span**: no toca `scrubbed` ni `consumed_spans`.
2. **LLM** — `llm_handler.py:84`: `"modalidad": {"type": "string"}`. Segundo
   escritor independiente cuando el parser marca `needs_llm`.

## CURRENT_READERS

- `soql_builder.py:397-398` — **autoridad de ejecución**:
  `modalidad_de_contratacion = '{escape(params['modalidad'])}'` (igualdad exacta a un escalar).
- `soql_builder.py:37,256` — `modalidad` cuenta como predicado WHERE no vacío (evita `1=1`).
- `formatter.py:516` — `resolved_modality = (params.get("modalidad") or "").lower()` (display).
- `suggester.py:54-58` — sugiere concentrar en la modalidad dominante (escribe un `modified_params`).
- `opportunity_scope_policy.py:81` — `_truthy(params.get("modalidad"))` (presencia = filtro activo).
- `followup_mutations.py:26,79,89` — `modalidad` ∈ `FILTER_FIELDS`; limpiable por `replace_topic`/`contextual_requery`.
- `refinement_recovery.py:23`, `dialogue_policy.py:50,55`, `recall_audit.py:48` — mapeos de nombre/ayuda.
- `query_router.py:1210` — `_extract_object_terms` borra los alias de `MODALIDAD_KEYWORDS` del texto de objeto (parche que compensa la falta de scrub del paso 8).

## SERIALIZATION_PATHS

- **Dataclass, no Pydantic.** `QueryFrame` es un `@dataclass` mutable
  (`query_frame.py:20`); no tiene `to_dict`/`from_dict`/`__eq__`/`__hash__`
  propios. La representación canónica de transporte entre turnos y procesos es
  el **dict plano `params`**, no el frame.
- **Huella / hashing** — `query_fingerprint.py`: `sha256` de
  `json.dumps(_canonical(relevant), sort_keys=True, default=str)`. `_canonical`
  recorre dict/list/tuple/set pero **no** dataclasses. Un value object caería en
  `default=str` → depende de `repr()`. Además la huella incluye **toda** clave
  no transitoria y no vacía de `params`: cualquier diagnóstico que se cuele en
  `params` contaminaría la identidad de la consulta.
- **Persistencia de conversación** — el estado de contexto se guarda como
  `params` (`context_params`) que `frame_from_params` reconstruye. `params` debe
  permanecer **JSON-nativo**.
- **Igualdad** — el sistema no compara frames por `==`; compara por huella y por
  claves individuales (invariantes INV-1/2/4 en `followup_mutations.py`).

## FOLLOWUP_TOUCHPOINTS

- `workflow/actions/parsing.py:apply_context` → `followup_engine.detect_and_merge`.
- `detect_and_merge` (`followup_engine.py:787`) → `frame_from_params` +
  `gate_delta` (`followup_mutations.py:198`).
- El gate opera sobre **dict plano por nombre de clave**: `calculate_delta`
  filtra por truthiness (`value not in (None,[],"",{},False)`), `gate_delta`
  acepta/rechaza por pertenencia a `policy.allowed_set` y limpia por
  `allowed_clear` (por nombre de clave).
- `modalidad` está en `FILTER_FIELDS`: `refine_filter` la **setea**;
  `replace_topic` y `contextual_requery` la **limpian**; `remove_filter`
  **no** la limpia (hueco actual).
- `merge_frames`/`classify_turn` en `query_frame.py` están **DEPRECATED**; la ruta viva es `gate_delta`.

## BUILDER_TOUCHPOINTS

- `soql_builder.py:397-398` — único punto que traduce modalidad a WHERE.
- `DatasetSpec.modality = "modalidad_de_contratacion"` (`:79`) y el mismo campo en
  `select_fields` de ambos datasets (`:124`, `:161`).
- El **puente que M3B necesita** ya existe en el catálogo M1:
  `procedures[].dataset_values.{procesos|contratos}` mapea `procedure_id` → lista
  de strings raw de `modalidad_de_contratacion`, **por dataset**.

---

## RECOMMENDED_INVOCATION_POINT

**Único punto:** dentro de `QueryRouter.parse()`, reemplazando el paso 8
(`_extract_modality` → `modality_resolver.resolve(...)`). Una sola llamada por
turno:

```python
resolution = modality_resolver.resolve(user_query, dataset=params["dataset"])
```

Razones:
1. Es el único lugar que posee simultáneamente el texto del turno, el `dataset`
   ya seleccionado (paso 1) y el buffer `scrubbed` + la lista `consumed_spans`.
2. Mantiene la modalidad **determinista** y con un solo escritor. El escritor
   LLM (`llm_handler.py:84`) debe **retirarse** del schema (o quedar sin
   autoridad) para no violar el invariante de "no doble parsing".
3. El adaptador ResoluciónM2→params/envelope debe vivir en un módulo pequeño
   propio (`modality_frame_adapter.py`), no incrustado en `parse()` — política
   "no monolitos".

## RECOMMENDED_ORDER_IN_PIPELINE

Invocar **después de** *dataset selection* (paso 1: `resolve` necesita el
dataset para `DATASET_INAPPLICABLE`) y **después de** la familia de estado
(paso 7), y **antes de** amounts (9), contractor (10), city (10.5), phrase (11)
y object terms (12), de modo que los `consumed_spans` de modalidad se retiren de
`scrubbed` **antes** de la extracción de objeto. Es exactamente el slot actual
del paso 8 — cambia el motor, no la posición.

Relación con las otras etapas:
- vs. entity/temporal/amount: vocabularios disjuntos; el orden relativo no altera
  el resultado, pero modalidad debe correr antes que objeto.
- Se elimina la dependencia del parche `_extract_object_terms` (`:1210`) que hoy
  borra `MODALIDAD_KEYWORDS` del objeto: al consumir spans correctamente, ese
  parche deja de ser necesario (retirarlo es opcional en M3B, seguro en M3C).

## RECOMMENDED_OWNER_OF_CONSUMED_SPANS

**El parser (`QueryRouter.parse`)**, consistente con cómo ya scrubea estado,
montos y entidad. Concretamente:
- Recorre `resolution.includes + resolution.excludes` y los issues cuyos spans
  fueron consumidos, tomando `ModalityMatch.source_text` /
  `ResolutionIssue.source_text` como substring a retirar de `scrubbed`
  (`scrubbed = scrubbed.replace(source_text, " ")`), y los agrega a
  `consumed_spans`.
- **No** se reintroduce el mapa de offsets de M2 en el buffer `scrubbed` (que se
  muta por string, no por offset). Se usa `source_text`, coherente con el resto
  del parser. `resolution.consumed_spans`/`residual_text` quedan como diagnóstico
  del envelope, no como driver del scrub.

Nota del corpus: un issue de ambigüedad **sí** consume su span (caso
"licitación pública o subasta" consume `"subasta"`, residual `"o"`), así que el
texto ambiguo no se filtra a objeto aunque no produzca filtro.

---

## RECOMMENDED_CONTRACT

Se evaluó el `ModalityFilter` propuesto y **se acepta con una precisión sobre su
serialización**. El objeto **ejecutable** es:

```python
@dataclass(frozen=True)
class ModalityFilter:
    include_procedure_ids: tuple[str, ...] = ()   # ordenados, únicos
    exclude_procedure_ids: tuple[str, ...] = ()   # ordenados, únicos
    source_status: ResolutionStatus = ResolutionStatus.NO_MATCH

    def is_empty(self) -> bool:
        return not self.include_procedure_ids and not self.exclude_procedure_ids
```

Reglas (todas verificables):
- inmutable; IDs ordenados y únicos; includes ∩ excludes = ∅.
- vacío ⇔ ausencia de filtro (`NO_MATCH` → `ModalityFilter()` vacío).
- `procedure_ids` es **autoridad**; la familia textual **no** se reexpande aguas
  abajo (el builder mapea `procedure_id → dataset_values[dataset]`, nunca alias).

**Precisión clave sobre serialización** (respaldada por `query_fingerprint.py`):
NO se debe guardar el objeto `ModalityFilter` como valor dataclass dentro de
`params`, porque la huella cae en `default=str` (repr frágil) y el gate de
follow-up limpia por clave. El VO es un tipo **en memoria**; su proyección
persistida/huellada/al-builder es **JSON-nativa** bajo dos claves estables:

```
params["modalidad_include_procedure_ids"] = ["a", "b", ...]   # sorted, unique
params["modalidad_exclude_procedure_ids"] = ["c", ...]        # sorted, unique
```

Con esto la huella es determinista sin tocar `query_fingerprint.py`, y el gate
por-clave sigue funcionando. Adaptadores `to_params()` / `from_params()` en
`modality_frame_adapter.py`.

Los `issues` completos **NO** viven en `QueryFrame` (ver decisión siguiente).

## FRAME_VS_ENVELOPE_DECISION

**Recomendación: B (separación), en su variante híbrida mínima.**

- `QueryFrame` (y el `params` plano que alimenta huella + builder + gate) lleva
  **solo lo ejecutable**: las dos tuplas `modalidad_*_procedure_ids`
  (reemplazando el escalar `modifiers["modalidad"]`). Nada de issues, ambiguities,
  assumptions ni clarification.
- Un **envelope de interpretación separado** — `QueryInterpretation` — transporta
  `ModalityResolution` (status, issues, ambiguities, assumption_codes,
  needs_clarification/clarification_code, diagnostics) hacia las capas de
  diálogo/aclaración/narrador. **No** se huella, **no** pasa por `gate_delta`,
  **no** define identidad de consulta guardada.

Justificación anclada en el código:
1. `query_fingerprint.py` huella **toda** clave no vacía de `params`. Meter
   `issues`/`diagnostics` en el frame cambiaría la identidad de la consulta por
   ruido lingüístico (viola INV: dos consultas semánticamente iguales con
   distinta redacción tendrían huellas distintas).
2. `gate_delta` acepta/limpia por nombre de clave y por truthiness; los
   diagnósticos no tienen semántica de merge y no deben cruzar el gate.
3. El builder solo necesita procedure_ids. Mezclar diagnóstico y filtro
   aumentaría acoplamiento sin beneficio.

La Opción A (todo en QueryFrame) se rechaza: mezcla filtros ejecutables con
diagnóstico lingüístico e infla huella y persistencia.

## LEGACY_COMPATIBILITY_STRATEGY

El escalar `modalidad` no puede desaparecer de golpe (6 lectores). Estrategia por
capas:
1. **Autoridad de ejecución migra primero (M3B):** el builder deja de leer
   `params["modalidad"]` y lee `modalidad_include/exclude_procedure_ids`,
   expandiendo vía catálogo a `modalidad_de_contratacion IN (...)` /
   `NOT IN (...)`.
2. **Escalar como proyección de solo-lectura, transitoria:** el adaptador puede
   emitir un `modifiers["modalidad"]` derivado **solo cuando `include` es un
   único procedure_id** con `dataset_value` único, para que
   `formatter`/`suggester`/`opportunity_scope_policy` sigan funcionando sin
   cambio inmediato. Marcado como *legacy projection, read-only, deprecable*.
   Nunca es autoridad y nunca lo escribe el parser directamente.
3. **`opportunity_scope_policy._truthy`** pasa a mirar la presencia de
   `include/exclude_procedure_ids` (o el `ModalityFilter.is_empty()`).
4. **LLM:** retirar `modalidad` del schema (`llm_handler.py:84`) — único escritor
   que quedaría fuera del resolver.
5. **`_extract_object_terms` (`:1210`)** puede conservar el borrado de
   `MODALIDAD_KEYWORDS` como cinturón-y-tirantes en M3B; retirarlo en M3C tras
   confirmar que el scrub por span cubre todos los alias.

## NO_MODALITY_BEHAVIOR

Consulta sin modalidad ⇒ `resolve()` retorna `status = NO_MATCH`,
`includes=()`, `excludes=()`, `consumed_spans=()`. El adaptador produce
`ModalityFilter()` vacío → **no** escribe `modalidad_include/exclude_procedure_ids`
en `params` (se omiten claves vacías, igual que hoy `frame_from_params` descarta
`False/[]/""`). Resultado: `params`, `QueryFrame`, huella y SoQL son
**byte-idénticos** a los previos a M3, salvo un `QueryInterpretation` opcional con
`status=NO_MATCH` que ninguna capa ejecutable lee. Invariante 1 satisfecho.

## STATUS_HANDLING_MATRIX

| `ResolutionStatus` | Frame/params (ejecutable) | Envelope (diagnóstico) | Efecto en el turno |
|---|---|---|---|
| `NO_MATCH` | nada | `status` only | idéntico a pre-M3; el parser sigue con objeto/entidad/monto/fecha |
| `RESOLVED` | include (y/o exclude) procedure_ids | — | filtro aplicado |
| `RESOLVED_WITH_ASSUMPTION` | include procedure_ids | `assumption_codes` | filtro aplicado; nota opcional al usuario |
| `PARTIAL` | includes/excludes **sanos** que sí resolvieron | `issues` (ambigüedad local) | filtro sano se aplica; el issue no destruye el resto (invariante 6). Ej: "licitación pública o subasta" → aplica `licitacion_publica*`, reporta `BARE_SUBASTA_MULTIPLE_LEGAL_MEANINGS` |
| `AMBIGUOUS` | includes que resolvieron (posible vacío) | `issues`, `needs_clarification`, `clarification_code` | si no hay filtro ejecutable, la capa de diálogo puede pedir aclaración **sin** bloquear objeto/entidad/etc. |
| `CONFLICT` | **ningún** filtro contradictorio (no se emiten includes+excludes en conflicto) | `issues` (conflict) | no se produce WHERE contradictorio (invariante 7); se explica el conflicto |
| `NOT_SEARCHABLE` | nada (p. ej. `platform_test`) | `issues` (not_searchable) | no filtra por modalidad; resto de la consulta sigue |
| `DATASET_INAPPLICABLE` | nada | `issues` (dataset_inapplicable) | no filtra por modalidad; resto sigue |

Principio transversal: **un issue local nunca destruye filtros sanos ni el resto
del frame**; la aclaración es opcional, nunca obligatoria (filosofía de producto).

## FOLLOWUP_COMPATIBILITY_PLAN

Claves de follow-up (estables, por-nombre para el gate):
`modalidad_include_procedure_ids`, `modalidad_exclude_procedure_ids`.

- **Agregar modalidad** ("además subastas inversas"): `refine_filter` debe hacer
  **unión** de `include_procedure_ids`, no overwrite. Requiere merge tipado
  (pequeña excepción en el gate, que hoy sobrescribe). → M3C.
- **Reemplazar** ("mejor licitación pública"): `replace_topic`/`contextual_requery`
  ya limpian modalidad; el nuevo turno setea el include nuevo. Añadir las dos
  claves nuevas a los `allowed_clear` que hoy listan `"modalidad"`.
- **Excluir** ("pero no contratación directa"): hoy `exclude_topic` solo toca
  `objeto_exclude`. Se necesita permitir set de `modalidad_exclude_procedure_ids`
  vía `refine_filter`. → M3C.
- **Quitar filtro** ("sin filtro de modalidad"): `remove_filter.allowed_clear`
  **no** incluye modalidad hoy — hueco. Agregar ambas claves a su `allowed_clear`
  y a `requested_clear_fields`. → M3B/M3C.

Todas las políticas se actualizan por **nombre de clave**; ningún cambio
estructural en el motor de gate.

---

## PROPOSED_M3B_FILES

- **`app/core/modality_frame_adapter.py`** (NUEVO, pequeño): `ModalityResolution`
  → (`ModalityFilter` + claves `params` JSON-nativas) + `QueryInterpretation`
  envelope. Única responsabilidad: traducción de contrato. Sin lógica de negocio.
- **`app/core/modality_filter.py`** (NUEVO, pequeño): el value object
  `ModalityFilter` con invariantes y `to_params()`/`from_params()`. (Puede
  fusionarse con el adapter si queda trivial; preferible separado por
  responsabilidad.)
- `app/core/query_router.py`: paso 8 llama al resolver + adapter; scrub por
  `source_text`; escribe claves ejecutables + adjunta envelope a `ParsedQuery`.
  Retirar `_extract_modality`/`MODALIDAD_KEYWORDS` cuando el corpus lo respalde
  (posponer a M3C si hay riesgo).
- `app/core/soql_builder.py`: leer `modalidad_*_procedure_ids`, expandir vía
  catálogo M1 (`dataset_values[dataset]`) a `IN (...)` / `NOT IN (...)`.
- `app/core/query_frame.py`: reconocer las dos claves nuevas en `_MODIFIER_KEYS`
  (o un manejo estructural equivalente); retirar la clave `modalidad` legada de
  la ruta de autoridad.
- `app/core/llm_handler.py`: quitar `modalidad` del schema.
- `app/core/followup_mutations.py`: `FILTER_FIELDS`, `allowed_clear` de
  `remove_filter`/`replace_topic`/`contextual_requery`, unión en `refine_filter`
  (parte diferible a M3C).
- Shims de lectores: `opportunity_scope_policy.py`, `formatter.py`, `suggester.py`.
- `app/core/workflow/actions/parsing.py`: propagar el `QueryInterpretation`
  envelope al estado (nuevo `writes`) para la capa de aclaración/narrador.

## FILES_THAT_MUST_NOT_CHANGE

- **M2 congelado:** `modality_resolver.py`, `modality_resolution_models.py`,
  `modality_models.py`, `modality_composition.py`, `modality_alternatives.py`,
  `modality_scope.py`, `modality_text.py`, `modality_lexicon.py`,
  `modality_catalog_v1.yaml`, `modality_lexicon_v1.yaml`. M3 **consume**, no toca.
- **`query_fingerprint.py`**: no debe requerir cambios (la proyección JSON-nativa
  lo evita). Si algún diseño exige cambiarlo → señal de que el contrato se filtró
  al frame; revisar.
- **Radar/Warehouse:** ninguna dependencia nueva a M2 (invariante 12).
  `app/core/radar/*`, `warehouse/*` intactos.
- **Guards de ejecución** (`FollowupGuards.check_no_where_1_1`, `_has_nonempty_where_filter`):
  su lógica no cambia; solo reconocen las claves nuevas como predicado válido.

## TEST_PLAN

1. **Paridad no-modalidad (invariante 1):** para un set de consultas sin
   modalidad, `params`, `QueryFrame` y `query_fingerprint` idénticos pre/post M3.
2. **Integración resolver↔router:** los 8 casos de trazado (abajo) producen las
   claves ejecutables esperadas y scrub correcto (objeto/entidad/monto/fecha
   preservados).
3. **Builder:** include→`IN (...)`, exclude→`NOT IN (...)`, expansión por
   `dataset_values` correcta y dataset-aware; `procedure_ids` desconocidos no
   rompen el WHERE.
4. **Huella:** consulta con modalidad produce huella determinista y estable entre
   procesos; issues/diagnostics NO alteran la huella.
5. **Follow-ups:** agregar (unión), reemplazar (limpia+setea), excluir, quitar
   filtro — sobre las dos claves nuevas.
6. **Frame-leak canary:** extender `test_frame_leak_canary.py` con un vector de
   pivote de modalidad (turno 1 modalidad A → turno 2 modalidad B: A no
   sobrevive en el SoQL del turno 2).
7. **Envelope:** `PARTIAL`/`AMBIGUOUS`/`CONFLICT` propagan issues sin bloquear la
   consulta ni contaminar `params`.

## CASOS_DE_TRAZADO (grounded contra corpus M2)

1. `qué contratos de alimentación para adultos mayores … por más de 500 millones en 2025`
   → modalidad **ausente** (`NO_MATCH`); parser continúa con entidad/objeto/monto/fecha.
   Frame ejecutable idéntico a pre-M3. ✓
2. `qué selecciones abreviadas de alimentación … 2025` → familia
   `seleccion_abreviada` expande a procedure_ids exactos
   (`seleccion_abreviada_menor_cuantia`, `…_menor_cuantia_sin_manifestacion`,
   `…_subasta_inversa`); objeto y fecha preservados (corpus A04). ✓
3. `qué subastas inversas hizo Barranquilla` → procedimiento específico
   `seleccion_abreviada_subasta_inversa` (corpus, línea 194). ✓
4. `contratación directa con o sin ofertas de alimentación` → dos procedure_ids
   (`contratacion_directa`, `contratacion_directa_con_ofertas`), alternativa
   coordinada; objeto preservado (corpus M01). ✓
5. `licitación pública o subasta` → `PARTIAL`: include
   `licitacion_publica`+`licitacion_publica_obra`, issue
   `BARE_SUBASTA_MULTIPLE_LEGAL_MEANINGS`; **no** se pierde licitación; "subasta"
   se consume (residual "o") (corpus, línea 3248). ✓
6. `todas excepto contratación directa` → exclusión (`exclude` = contratación
   directa) sin inclusión positiva; `include=[]` (corpus "oportunidades excepto
   contratación directa", línea 1887). ✓
7. `acuerdos marco de precios de alimentación` → AMP explícito, familia propia
   `acuerdo_marco`, **no** selección abreviada (corpus A18/A19). ✓
8. `contratos de alimentación; no quiero excluir contratación directa` →
   anti-negación: resuelve como **include**, no como exclude (corpus "no quiero
   excluir mínima cuantía", línea 3208 → include). ✓

---

## RISKS

1. **Doble parsing / doble escritor (alto):** si el LLM conserva `modalidad`, dos
   fuentes escriben modalidad → viola invariante 8. Mitigación: retirar del
   schema LLM en M3B.
2. **Mapa de offsets M2 vs. scrub por string (medio):** `resolution.consumed_spans`
   son offsets sobre texto original; `scrubbed` se muta por reemplazo de
   substring y ya sufrió otros scrubs. Usar `source_text` (no offsets) para el
   scrub evita el desalineamiento; los offsets quedan como diagnóstico.
3. **Huella frágil por dataclass (medio):** guardar `ModalityFilter` como objeto
   en `params` haría depender la huella de `repr()`. Mitigación: proyección
   JSON-nativa (dos tuplas de strings).
4. **Contaminación de identidad por diagnóstico (medio):** issues en el frame
   cambiarían la huella por redacción. Mitigación: envelope separado.
5. **`procedure_id` → `dataset_value` dataset-dependiente (medio):** el mismo
   procedure_id mapea a strings distintos por dataset; el builder debe expandir
   con el dataset del turno, no del catálogo global. El catálogo ya lo modela
   (`dataset_values.{procesos|contratos}`).
6. **Follow-up "agregar" con overwrite (bajo-medio):** el gate hoy sobrescribe;
   "agregar modalidad" necesita unión. Diferible a M3C; documentar como known-gap.
7. **Lectores legacy silenciosos (bajo):** `suggester`/`formatter`/`opportunity_scope_policy`
   dependen del escalar; sin shim, degradan en silencio. Cubrir con la proyección
   de solo-lectura + tests.

## OPEN_QUESTIONS

1. ¿La proyección escalar legacy de solo-lectura debe emitirse siempre que haya
   exactamente un procedure_id, o nunca (y migrar los 3 lectores de una vez en
   M3B)? Recomendación tentativa: emitir solo-lectura en M3B, migrar lectores en M3C.
2. ¿"Agregar modalidad" (unión en `refine_filter`) entra en M3B o M3C? El resto
   de M3B no lo bloquea; propongo M3C.
3. ¿El `QueryInterpretation` envelope se persiste con la conversación (para
   re-explicar aclaraciones en turnos posteriores) o es efímero por turno?
   Recomendación: efímero por turno; la aclaración pendiente ya se modela aparte.
4. ¿`clarification_code` de modalidad se integra al mecanismo de aclaración
   existente (P15/dialogue_policy) o abre uno nuevo? Requiere leer P15 en M3B.
5. ¿Se retira `MODALIDAD_KEYWORDS`/`_extract_modality` en M3B o se deja como
   red de seguridad hasta M3C? Propongo dejarlo en M3B, retirar en M3C con corpus verde.

---

## FINAL_VERDICT

**M3A_DESIGN_READY_FOR_REVIEW**

Sin conflicto de contrato ni riesgo de compatibilidad legado que impida avanzar:
el escalar `modalidad` es proyectable desde el nuevo contrato, la huella se
mantiene estable con proyección JSON-nativa, y el envelope separado protege la
identidad de consulta. Diseño listo para revisión antes de implementar M3B.

*Detenido. No se implementa M3B. No se crea commit.*

# ACTOR_PENDING_EXPLICIT_DOCUMENT_SELECTION_V1

**CLOSED.** Una lista pendiente de candidatos de actor, respondida con el
documento de uno de esos candidatos, ahora **selecciona** ese candidato y
conserva íntegro el frame que produjo la lista. Un documento que no pertenece a
la lista sigue siendo una **búsqueda nueva** y no hereda nada. Sin merge ni
push.

- BASE: `17e01ef31bb55a186effb22ee4bf7541288d09ec` (main).
- BRANCH: `fix/actor-pending-explicit-document-v1`.
- ADR: **ninguno nuevo.** Este cambio no crea autoridad: alinea la selección por
  documento con la semántica de selección de candidato que ADR-060/061 ya
  fijaron. La única incorporación es un nombre de procedencia.
- Corrige deuda **previa a ADR-094**, congelada durante aquella entrega en
  `artifacts/audits/actor_exact_personal_prefix_collision_v1/existing_document_followup_baseline.json`.

## El defecto

Evidencia congelada en la entrega anterior:

```text
primera consulta:  exact_ambiguous_alias · fecha_desde = 2025-01-01
follow-up:         documento explícito
                   method = explicit_document
                   route  = actor_profile
                   fecha_desde = null
```

La identidad se resolvía y el año desaparecía. Reproducido antes de tocar
runtime sobre las dos ambigüedades vigentes —`exact_ambiguous_alias` y
`exact_prefix_ambiguous`— con índice sintético, sockets bloqueados y
persistencia temporal:

| Respuesta a la misma lista | método | base_number | fecha_desde |
|---|---|---|---|
| `2` | `user_selected_candidate` | 70000003 | 2025-01-01 |
| `CC 70000003` | `explicit_document` | 70000003 | **null** |

## ROOT_CAUSE

Un documento explícito **siempre** escribe `actor_scope` y `contratista` en el
frame parseado. `classify_actor_pending_turn` clasifica como `NEW_SEARCH`
cualquier turno cuyos params traigan esas claves, así que **toda** respuesta
documental era una búsqueda nueva por construcción: la lista pendiente nunca
llegaba a preguntarse si el documento era el de uno de sus propios candidatos.
El frame que la produjo —año, comprador, valor, tema— moría con ella.

No era un fallo de la gramática documental ni del resolver. Era una respuesta
correcta a la pregunta equivocada.

## La regla

Una respuesta documental a una lista pendiente significa exactamente una de dos
cosas, y adivinar entre ellas está prohibido:

| El documento | Qué es | Qué hereda |
|---|---|---|
| identifica exactamente un candidato de la lista | **selección** | todo `pending.original_params` |
| no identifica a nadie de la lista | **búsqueda nueva** | nada |

## Diseño

**No hay una segunda ruta de replay.** `handle_actor_selection` ya resolvía
`pending.original_params → candidate_from_payload → apply_actor_identity →
flujo normal`. Una respuesta documental entra por esa misma puerta resolviendo
un rango, de modo que las dos selecciones **son el mismo código** y no pueden
divergir.

**Toda la decisión vive en un módulo puro**, `app/core/actor_pending_document.py`
(≈100 LoC, sin reloj, sin store, sin recuperación):

- La gramática documental **no se reimplementa ni se amplía**: los params ya
  pasaron por el router, así que la identidad tipada viene de la misma autoridad
  que sirve `contratos del NIT X`.
- La comparación es de `IdentityKey` canónica —`document_type` + `base_number`—
  y de nada más. Nunca el nombre, el rango, el crudo del documento ni similitud.
  Por eso la selección es **independiente del orden** de la lista.
- Un número **sin marcador de tipo** queda `UNKNOWN` y no reclama un candidato
  tipado (`INV-ID-TYPE`): sigue como búsqueda explícita. Es la lectura
  conservadora; la UI muestra el documento con su tipo (`CC 70000003`), que es
  la forma que el usuario copia.
- Una respuesta que además pide otra cosa —otro año, otro dataset— **no** es una
  selección: seleccionarla obligaría a descartar en silencio lo que el usuario
  acaba de pedir.
- Dos candidatos con la misma `IdentityKey` **fallan cerrado**; no se elige el
  primero.

**La seguridad del pendiente no se debilita.** La selección por rango/clic
conserva `selection_token` y TTL. Un texto documental solo lee el
`actor_pending` vigente del último turno; con el pendiente vencido o de versión
incompatible el frame **no se hereda** y el documento continúa como la búsqueda
explícita que también es —no se revive un frame muerto—.

**Procedencia.** `explicit_document_pending_selection` es autoritativa y difiere
de `user_selected_candidate` en nada más que el nombre. La traza compacta la
expone sin inventar un segundo conteo de candidatos:

```json
{"actor_resolution_status": "RESOLVED",
 "actor_resolution_method": "explicit_document_pending_selection",
 "candidates_count": 0}
```

## Paridad rango ↔ documento

Oráculo central, sobre `contratos del proveedor X con alcaldía de barranquilla
en 2025`:

- `resolved_params` **idénticos** salvo procedencia (`actor_resolution`,
  `actor_resolution_method` y su espejo en `actor_scope.resolution_method`);
- **mismo universo SoQL**, comparado como multiconjunto porque los aspectos
  corren concurrentes y el orden de llegada no es el invariante;
- mismo número de llamadas a fuente.

## Casos

| Caso | Antes | Después |
|---|---|---|
| ambigüedad + año → `CC` candidato | año perdido | `fecha_desde=2025-01-01`, `fecha_hasta=2025-12-31` |
| ambigüedad + comprador → `CC` candidato | comprador perdido | `entidad_resolved` conservado |
| ambigüedad + comprador + año → `CC` candidato | ambos perdidos | ambos conservados |
| ambigüedad + valor → `CC` candidato | `valor_min` perdido | `valor_min` conservado |
| `exact_ambiguous_alias` (baseline congelado) → `CC` candidato | año perdido | año conservado |
| pendiente → documento ajeno | búsqueda nueva | **igual**: búsqueda nueva, nada heredado |
| pendiente vencido → documento | búsqueda nueva | **igual** |
| `INCONCLUSIVE` → documento (ADR-094) | frame conservado | **igual** |

## Un oráculo previo congelaba el defecto

`tests/test_actor_exact_prefix_workflow.py::test_document_followup_exits_clarification_without_another_challenger`
afirmaba `explicit_document` para las cuatro parametrizaciones. En la única en
la que el documento pertenecía a la lista (`AMBIGUOUS` + `CC 70000003`) eso era
afirmar el defecto. El test ahora **separa** los dos casos y afirma el frame en
ambas direcciones —conservado cuando selecciona, ausente cuando no—, de modo que
es más estricto que antes, no menos.

## Puertas

Evidencia en
[`artifacts/audits/actor_pending_explicit_document_selection_v1/`](../../artifacts/audits/actor_pending_explicit_document_selection_v1/).

- **Red first:** 24 fallando / 3 pasando antes del runtime; los tres que pasaban
  son las guardas de regresión de lo que no debía moverse (`red.json`).
- **Focales:** 27/27 verdes, incluidas tres propiedades Hypothesis —paridad de
  filtros rango/documento sobre frames arbitrarios, documento ajeno que nunca
  hereda, selección independiente del orden de candidatos—.
- **Dogfood sintético determinista:** cinco casos, `frame_preserved` en los
  cuatro que seleccionan, `soql_parity` en los cinco, nada heredado en el ajeno
  (`dogfood.json`, `scripts/audits/actor_pending_document_dogfood.py`).
- **Ruff:** `All checks passed!`
- **make lint-core:** `app/core/ limpio`.
- **Semgrep** (`app/`): 10 hallazgos, **delta 0** contra el baseline; ninguno en
  los archivos tocados.
- **git diff --check:** limpio.
- **Corpus de actor** (`actor_hygiene_v1`, sha `217df608`): 35/36. El único
  fallo, `AH-17-PENDING-PROCESSES`, se reproduce **idéntico en BASE `17e01ef`**
  desde un worktree separado; estados por caso iguales, delta 0. No toca
  respuestas documentales.
- **Corpus conversacional holístico** (195 casos, sha `a341405a`): 171/24, con
  estados por caso **idénticos a BASE**; los 24 son la deuda de oráculo ya
  registrada. Delta 0.
- **Suite completa**, una sola corrida en serie sobre `cf75cd6`:
  **11.791 passed, 2 skipped, 14 xfailed, 0 failed** (604 s).

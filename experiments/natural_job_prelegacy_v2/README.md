# NATURAL_JOB_PRELEGACY_V2 — CONTROL PLANE

> **Tesis:** el LLM acredita QUÉ trabajo quiere la persona; el host acredita CÓMO ejecutarlo.

El LLM no escribe SQL, no decide comprador, fechas, monto, dataset, modalidad ni
filtros ejecutables. El parser legacy no interpreta turnos que ya tienen una
credencial semántica válida.

Plan canónico: `docs/planes/2026-09-16_natural_job_prelegacy_v2_implementation_plan.md`.
Evidencia V1 que se preserva y **no** se mergea: `mo/natural-job-credential-v1 @ 411a03d`.

## Por qué existe V2

V1 midió el hueco real: el `evidence_quote` del modelo cubre siempre la oración
completa, así que no puede acreditar residuo; el extractor de tema del legacy
llama tema a un verbo (`vende`, `compra`, `gana`) y la paráfrasis muere una
authority más adelante. V2 cambia el orden de autoridad: authorities
independientes leen el **raw query**, no el intervalo del modelo.

## Invariante central

```text
QUERY_ROUTER_CALLS_ON_CREDENTIALED_TURNS = 0
LEGACY_FALLBACK_AFTER_VALID_CREDENTIAL = 0
```

## Qué se congela y dónde

| Artefacto | Ruta | Regla |
|---|---|---|
| Corpus DEV (38 NJC V1 + 4 controles V2) | `corpus_dev.json` | copiado sin edición + controles del plan §25; no es holdout |
| Prompt Natural Alpha V3 | `prompt_v3.frozen.txt` + `app/core/natural_alpha/prompt.py` | congelado antes de cualquier replay V3; no se reajusta viendo resultados |
| Manifest con hashes | `manifest.json` | todo hash se calcula en `freeze.py`, nunca se transcribe |
| Holdout independiente | `holdout.json` | lo escribe un agente en contexto fresco, sin leer el prompt V3 |

`freeze.py` reconstruye corpus + copia congelada + manifest, y falla si el corpus
V1 de la rama de evidencia no coincide con el sha que su manifest declaró.

## Reglas de la misión

1. **Prohibido** arreglar una paráfrasis añadiendo vocabulario al host
   (`con quien`, `a quien`, `quien le vende` como regex nuevas) o enumerando
   verbos en una whitelist. Si aparece, STOP.
2. El `evidence_interval` nunca autoriza residuo por sí solo: si invade spans
   host-owned, `evidence_consumes_residual = false` y el residuo se calcula
   sobre el raw query completo.
3. Una credencial válida **no** vuelve al legacy porque falte una palabra.
4. Ninguna dimensión ejecutable tiene dos autoridades simultáneas.
5. El control plane crece con otra `CapabilitySpec`, no con otro router.

## Gates y criterio de abandono

Hard gates y métricas viven en `manifest.json`. Sólo `PASS/PASS`
(clasificador + control plane) permite promoción. Si el control plane falla con
credentials correctas, el veredicto es `NO_GO_ARCHITECTURE` y esta vía se
abandona: no hay otro X4.

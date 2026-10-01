# FOUNDATION_ENTRY_ADMISSION — Paso 0 (diagnóstico, NO Gold)

Medición offline de la puerta de entrada de Foundation V3 (`eligible_first_search` +
`foundation_route_owner.specialized_owner`) sobre `8e0b9ca3`, 2026-09-30.

## Corpus

- 585 consultas distintas de `data/feedback.jsonl` (dogfood + piloto), etiquetadas a mano por
  intención por UN solo etiquetador (Claude), sin mirar antes la decisión de la puerta.
- 35 variantes sintéticas (`harness/synthetic.txt`).
- Las consultas reales contienen nombres y documentos de personas: **no se versionan**. Viven en
  `~/Documents/tools/secoppal-diagnostics/foundation_entry_m0/` (`wild_raw.json`, `labels.txt`,
  salidas `*_out.json`).

Etiquetas: `S` búsqueda, `SR` búsqueda con orden («más caros»), `A` actor/proveedor, `EP` perfil de
entidad, `SUP` ranking de proveedores, `AN` conteo/agregado, `F` fragmento de follow-up,
`C` control/protocolo, `R` Radar, `M` multi-job, `X|Y` ambigua.

## Resultados (585 reales)

| Clase | n | Puerta |
|---|---|---|
| S | 318 | ADMIT 228 · regex 88 (52 tema sin sustantivo de universo, 25 otro sustantivo, 11 ancla) · veto ACTOR 2 |
| SR | 20 | ADMIT 14 (Foundation no tiene orden; legacy sí: `ordering_signal`) · regex 6 |
| A | 114 | veto ACTOR 59 · regex 46 · **ADMIT 9** (nombre sin marcador; legacy también lo lee como tema) |

- 55/228 búsquedas admitidas llevan un rasgo que el host Foundation no resuelve (proxy por parse
  legacy): ciudad 17, mes/rango 21, comprador sin resolución exacta 11, región 3, otros 4.
  Verificado con propuestas a mano: ciudad y mes → `NEEDS_CLARIFICATION UNRESOLVED_HOST_DIMENSION:*`.
- Barranquilla: `_NON_ACTOR_SEARCH_LEADS` (`app/core/actor_conversation.py`) conoce «licitacion»
  pero no «licitaciones», «convocatorias», «selecciones» ni el artículo «las».
- Historial: `eligible_first_search` exige cero historial y `ConversationStore.get_history` no
  tiene TTL; en Telegram/WhatsApp (chat_id persistente) Foundation sólo ve el primer mensaje.

## Límites

Un solo etiquetador; corpus sesgado al dogfood; capacidad medida por proxy; Natural Alpha apagado
(en runtime sólo puede restar admisiones). Esto es diagnóstico: el Gold de M1 requiere un segundo
etiquetador independiente y acuerdo medido.

## Reproducir

```bash
CORP=~/Documents/tools/secoppal-diagnostics/foundation_entry_m0
SECOPPAL_ACTOR_INDEX=/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite \
  PYTHONPATH=. .venv/bin/python docs/audits/foundation_entry_admission_m0/harness/probe.py $CORP
PYTHONPATH=. .venv/bin/python docs/audits/foundation_entry_admission_m0/harness/legacy_oracle.py $CORP
PYTHONPATH=. .venv/bin/python docs/audits/foundation_entry_admission_m0/harness/capacity.py $CORP
.venv/bin/python docs/audits/foundation_entry_admission_m0/harness/analyze.py $CORP
```

Sin red (socket bloqueado), persistencia redirigida al directorio del corpus.

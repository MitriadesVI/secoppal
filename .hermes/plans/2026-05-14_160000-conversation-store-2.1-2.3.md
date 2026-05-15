# Plan: ConversationStore 2.1 + integración 2.2 + follow-ups 2.3

Fecha: 2026-05-14
Estado: PLAN — aprobado con ajustes del asesor
Tests baseline: 140/140 passing

---

## Goal

Dar memoria conversacional a SECOPPAL. Un usuario en Telegram puede preguntar
"procesos de pavimentacion en el Choco" y luego "y en Bolivar?" — y el sistema
entiende que sigue buscando procesos de pavimentacion.

Cubre tres tareas del roadmap:
  2.1 — ConversationStore en JSONL por chat_id
  2.2 — Integrar ConversationStore en el pipeline Burr
  2.3 — Deteccion de follow-ups contextuales

---

## Contexto y suposiciones

- SecopalWorkflow.run_query(user_query, channel) — firma actual, sin chat_id ni historia
- main.py Telegram ya tiene chat_id disponible pero no lo pasa al workflow
- main.py WhatsApp tiene sender (numero From) pero tampoco lo pasa
- Streamlit no tiene chat_id natural — session_id efimero via st.session_state
- FeedbackStore (data/feedback.jsonl) es el patron de referencia para JSONL stores
- Burr State es inmutable entre llamadas — no guarda contexto cross-turn por si solo

Suposiciones de diseno:
  - Guardar ultimos N=5 turnos por chat (configurable)
  - Un archivo JSONL por chat_id: data/conversations/<chat_id>.jsonl
  - Follow-up detection es heuristico (no LLM) — reglas explicitas abajo
  - Si detection falla silenciosamente → comportamiento degradado = query standalone
    (nunca romper)

---

## Archivos que cambian

NUEVOS:
  app/core/conversation_store.py     — clase ConversationStore + MERGE_RULES
  tests/test_conversation_store.py   — tests unitarios (~12 tests)

MODIFICADOS:
  app/core/orchestrator.py           — action apply_context, run_query recibe chat_id
  app/core/orchestrator v2.py        — copia legacy sincronizada
  app/main.py                        — pasa chat_id desde webhooks; maneja /reset

TESTS existentes que pueden romper:
  tests/test_orchestrator.py         — run_query() cambia firma, actualizar mock

---

## Diseno de ConversationStore

```python
# app/core/conversation_store.py

@dataclass
class Turn:
    turn_id: str        # uuid hex 12
    timestamp: str      # ISO UTC
    chat_id: str
    user_query: str
    response: str       # formatted_response del pipeline
    parsed_params: dict # snapshot de parsed_params para follow-ups
    result_ids: list    # IDs de contratos/procesos mostrados, en orden (prereq 2.9)
    trace_id: str       # link a FeedbackStore

class ConversationStore:
    base_path: Path     # default: data/conversations/

    def append_turn(chat_id, user_query, response, parsed_params, result_ids, trace_id) -> str
    def get_history(chat_id, last_n=5) -> list[Turn]
    def clear(chat_id) -> None   # usado por /reset y tests
```

Storage: data/conversations/<chat_id>.jsonl
Un archivo por chat. Append-only.
chat_id sanitizado para nombres de archivo:
  Telegram: "tg_<abs(id)>"   (puede ser negativo en grupos)
  WhatsApp: "wa_<digitos>"   (quitar el +, solo digitos)
  Streamlit: "st_<session_id>"

---

## MERGE_RULES (decision MVP — v1.2)

```python
# En app/core/conversation_store.py

MERGE_RULES = {
    "departamento_resolved": "replace",
    "departamento":          "replace",
    "entidad_resolved":      "replace",
    "entidad":               "replace",
    "fecha_desde":           "replace",
    "fecha_hasta":           "replace",
    "valor_min":             "replace",
    "valor_max":             "replace",
    "estado_family":         "replace",
    "objeto":                "replace",   # MVP: replace. Aditivo ("y tambien X") va en v1.3
    "dataset":               "keep_previous_unless_explicit",
}
```

Logica de merge en apply_context:
  merged = dict(context_params)  # base: turno anterior
  for key, new_val in parsed_params_new.items():
      if new_val is None:
          continue  # parser no detecto nada nuevo para este campo → conservar anterior
      rule = MERGE_RULES.get(key, "replace")
      if rule == "replace":
          merged[key] = new_val
      elif rule == "keep_previous_unless_explicit":
          if key in parsed_params_new and new_val:
              merged[key] = new_val
          # else: ya esta en merged del anterior
  return merged

---

## Heuristica de follow-up (2.3) — DECISION FINAL

Dos condiciones posibles para detectar follow-up.
Ambas requieren que el ultimo turno del chat_id tenga timestamp < 30 minutos.
Si pasaron >30 min → NO aplicar follow-up. Siempre tratar como nueva conversacion.

CONDICION A (continuation word):
  Query empieza con alguna de:
    ["y ", "ahora", "tambien", "también", "pero ", "solo ", "muéstrame", "muestrame",
     "dame", "ordena", "ordenalos", "ordénalos", "filtremos", "filtra"]
  → follow-up

CONDICION B (query corta sin verbo de nueva busqueda):
  len(query.split()) < 8
  Y query NO empieza con verbo de nueva accion:
    ["busca", "encuentra", "necesito", "quiero", "consulta", "muestra procesos",
     "muestra contratos", "listar", "ver"]
  → follow-up

Si ninguna condicion se cumple, o si el ultimo turno tiene >30 min → NO follow-up.

Implementado como funcion privada:
  _is_followup(query: str, last_turn: Turn | None) -> bool

---

## Comando /reset (2.3 extension)

En main.py, ANTES de llamar run_query, detectar comandos de reset:
  body.strip().lower() in {"/reset", "reset", "nueva", "limpiar", "nuevo"}

Si es reset:
  store.clear(chat_id)
  responder: "Conversacion reiniciada. Puedes empezar una nueva busqueda."
  NO llamar run_query

---

## Estructura Burr con apply_context

Nueva accion en el grafo (solo corre cuando followup=True):

  parse_query
    → apply_context  (si followup=True, via expr("followup"))
    → llm_parse      (si needs_llm)
    → resolve_entities
    → build_query
    → execute_query
    → degrade_query
    → format_response

  parse_query → llm_parse   (si not followup y needs_llm)
  parse_query → resolve_entities  (si not followup y not needs_llm)

apply_context:
  reads:  ["parsed_params", "context_params", "followup"]
  writes: ["parsed_params"]
  logica: merge segun MERGE_RULES

State inicial nuevo:
  followup=False
  context_params={}

---

## result_ids en cada turno

En format_response, extraer IDs de los resultados antes de formatear:
  result_ids = [r.get("referencia_del_proceso") or r.get("id_contrato") or "" for r in results[:10]]

Pasar al ConversationStore.append_turn().
No requiere cambios en el formatter — solo en orchestrator.run_query al final.

---

## Plan paso a paso

PASO 1 — ConversationStore + MERGE_RULES (2.1)
  Crear app/core/conversation_store.py
  Tests: tests/test_conversation_store.py
    - test_append_and_get_history
    - test_get_history_last_n_limita
    - test_sanitize_chat_id_telegram (negativo → "tg_123")
    - test_sanitize_chat_id_whatsapp ("+573..." → "wa_573...")
    - test_corrupt_line_skipped
    - test_clear_vacia_archivo
    - test_merge_replace_sobreescribe
    - test_merge_keep_previous_dataset
    - test_merge_campo_none_conserva_anterior
  pytest tests/ — todos passing

PASO 2 — Integrar en run_query sin follow-up aun (2.2)
  SecopalWorkflow.__init__: instanciar ConversationStore
  run_query(user_query, channel, chat_id=None):
    - al final: append_turn con result_ids
  Sin logica de follow-up aun
  Actualizar tests/test_orchestrator.py (firma nueva)
  pytest tests/ — todos passing

PASO 3 — Follow-up detection + apply_context (2.3)
  _is_followup() en orchestrator.py
  Accion apply_context en Burr
  Transiciones actualizadas
  run_query: si chat_id y followup → context_params en state
  Tests nuevos en tests/test_orchestrator.py:
    - test_followup_merge_departamento
    - test_followup_expira_30_min
    - test_no_followup_verbo_nueva_busqueda
    - test_reset_limpia_historial
  pytest tests/ — todos passing

PASO 4 — Webhooks + /reset (main.py)
  Telegram: pasar chat_id=str(chat_id); manejar reset antes de run_query
  WhatsApp: pasar chat_id=sender
  Streamlit: session_id en st.session_state
  pytest tests/

PASO 5 — Sincronizar legacy + decisions_log
  cp app/core/orchestrator.py "app/core/orchestrator v2.py"
  Crear docs/decisions_log.md con decisiones de diseno de esta sesion
  pytest tests/ final — baseline nuevo

---

## decisions_log.md (a crear al finalizar)

Decisiones a documentar:
  - objeto: "replace" en MVP (no aditivo). Razon: simplicidad. Aditivo en v1.3.
  - dataset: "keep_previous_unless_explicit". Razon: el usuario que dice "y en bolivar?"
    probablemente sigue en procesos, no en contratos.
  - Ventana temporal follow-up: 30 min. Razon: evitar reanudacion accidental al dia siguiente.
  - chat_id sanitization: prefijos tg_/wa_/st_ para evitar colision de IDs entre canales.
  - result_ids guardados desde 2.1 como prerrequisito de 2.9 (callbacks Telegram).
  - apply_context como action Burr separado (no modificar parse_query).

---

## Riesgos residuales

RIESGO 1: concurrencia en filesystem (dos requests mismo chat_id simultaneos)
  → Para v1.2 aceptable. JSONL append es atomico en la practica para archivos pequenos.
  → Fuera de scope hasta que haya evidencia de problema real.

RIESGO 2: data/conversations/ crece sin control en produccion
  → Fuera de scope. ADR pendiente para TTL o rotacion en v1.3.

RIESGO 3: Streamlit session_id no persiste entre recargas
  → Esperado. Streamlit es stateless por diseno. El usuario pierde contexto al recargar.
  → Aceptable en MVP.

---

## Definicion de done

  - pytest tests/ passing (todos, ~12 tests nuevos sobre baseline 140)
  - "y en bolivar?" tras query con departamento=Choco → departamento=Bolivar, tipo conservado
  - Turno con >30 min de antiguedad → NO aplica follow-up
  - /reset desde Telegram limpia historial y confirma al usuario
  - run_query(query, channel="telegram") sin chat_id sigue funcionando igual
  - legacy orchestrator v2.py sincronizado
  - docs/decisions_log.md creado

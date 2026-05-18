# CORPUS BACKLOG — SECOPPAL Query Corpus V1

Fallos conocidos del corpus, agrupados por severidad. NO editar parser/core para hacer pasar estos casos — son bugs reales que requieren features o fixes separados.

**Última actualización:** 2026-05-17 — CORPUS-002 cerró los 5 critical (commit 9d1660b). Corpus: 82/88 PASS.

## Critical (0) ✅

Los 5 critical anteriores (R005, R027, R020, R028, R029) fueron cerrados en CORPUS-002:
- `convocadas` → `oferta_abierta`
- `oferta_abierta + procesos` promueve a `opportunity_search`
- follow-up con topic nuevo ya no cae automáticamente como `refine_filter`

## High (8)

### CORPUS-R001 — "quiero ser proveedor de uniformes" no produce intent_type
- **Query:** "quiero ser proveedor de uniformes para el ejercito"
- **Expected:** `intent_type=bidder_intent` o `opportunity_search`, objeto=uniformes
- **Sintoma:** "ser proveedor" no esta en frases de deteccion
- **Sprint sugerido:** CORPUS-002

### CORPUS-R008 — "para presentarme" en parser sin workflow falla
- **Query:** "para presentarme en construccion de colegios"
- **Expected:** `intent_type=opportunity_search`, `dataset=procesos`
- **Sintoma:** enforcement funciona en workflow pero el parser aislado no setea intent_type
- **Sprint sugerido:** LOW (normal, requiere workflow)

### CORPUS-R018 — valor_min cuantia no setea valor_min
- **Query:** "contratos de cuantia superior a 500 millones"
- **Expected:** `valor_min=500000000`
- **Sintoma:** "cuantia" no se normaliza como "valor"
- **Sprint sugerido:** CORPUS-002

### CORPUS-R022 — objeto contiene tema cuando deberia omitirse
- **Query:** "temas de salud"
- **Expected:** `objeto=salud`, `objeto_not_contains=[tema]`
- **Sintoma:** "temas" es stopword structural que entra como objeto
- **Sprint sugerido:** CORPUS-002

### CORPUS-0062 — "suministro de alimentos primera infancia" no detecta bigram
- **Query:** "suministro de alimentos para primera infancia y adulto mayor"
- **Expected:** `objeto_contains=[alimentos, primera_infancia, adulto_mayor]`
- **Sintoma:** bigrams primera_infancia/adulto_mayor no se detectan consistentemente
- **Sprint sugerido:** CORPUS-002

### CORPUS-091 — timeout UX no retry con ordering_signal sin fecha
- **Query:** "los 20 contratos mas caros de colombia sin fecha"
- **Expected:** ordering_signal=valor_desc, response_not_contains=["Resultados: 0"]
- **Sintoma:** timeout sin retry automatico
- **Sprint sugerido:** CORPUS-002 (o ya cubierto por fix en orchestrator.py)

### CORPUS-0063 — "centros de vida y casas de la cultura" no detecta multi-bigram
- **Query:** "construccion de centros de vida y casas de la cultura"
- **Expected:** `objeto_contains=[centros de vida, casas de la cultura]`
- **Sintoma:** parser no maneja bien dos bigrams separados por "y"
- **Sprint sugerido:** CORPUS-002

### CORPUS-R023 — follow-up "y en ejecucion" hereda pero no aplica estado
- **Query:** "y en ejecucion" (tras contexto)
- **Expected:** `followup_intent_type=refine_filter`, estado=En ejecucion
- **Sintoma:** estado se hereda pero no se aplica en SoQL
- **Sprint sugerido:** CORPUS-002

## Medium (6)

### CORPUS-R004 — consultoria ambiental no setea departamento
- **Query:** "oportunidades de consultoria ambiental en antioquia 2025"
- **Expected:** `departamento_resolved=Antioquia`, `objeto_contains=[consultoria, ambiental]`
- **Sintoma:** departamento a veces no se resuelve con fecha presente
- **Sprint sugerido:** CORPUS-002

### CORPUS-R007 — "quiero ofertar" sin calificador de objeto falla silently
- **Query:** "quiero ofertar"
- **Expected:** `needs_clarification=true`
- **Sintoma:** no pide clarificacion, genera WHERE 1=1
- **Sprint sugerido:** CORPUS-002

### CORPUS-R012 — "mas de" con fecha no setea valor_min
- **Query:** "mas de 2025" en contexto de busqueda
- **Expected:** fecha no valor
- **Sintoma:** "mas de X" en contexto temporal no discrimina valor vs fecha
- **Sprint sugerido:** CORPUS-002

### CORPUS-R015 — follow-up "solo los de" pierde dataset
- **Query:** "solo los de 2026" (tras contexto contratos)
- **Expected:** conserva dataset=contratos
- **Sintoma:** follow-up pierde dataset_explicit
- **Sprint sugerido:** CORPUS-002

### CORPUS-R030 — "ver los de" tratado como new_search
- **Query:** "ver los de mayor valor" (tras contexto)
- **Expected:** `followup_intent_type=change_order`
- **Sintoma:** "ver los de" se clasifica como new_search
- **Sprint sugerido:** CORPUS-002

### CORPUS-R031 — "continua" sin contexto previo falla gracefully
- **Query:** "continua"
- **Expected:** `needs_clarification=true`
- **Sintoma:** intenta paginar sin contexto
- **Sprint sugerido:** CORPUS-002

## Low (3)

### CORPUS-R003 — "como empresa" no se elimina de objeto
- **Query:** "como empresa quiero ofertar en obras de acueducto en cali"
- **Expected:** `objeto_not_contains=[empresa]`
- **Sprint sugerido:** CORPUS-002

### CORPUS-0040 — "firmados en barranquilla" no fuerza dataset=contratos
- **Query:** "firmados en barranquilla"
- **Expected:** `dataset=contratos`, SIN estado
- **Sprint sugerido:** CORPUS-002

### CORPUS-0045 — "en aprobacion o enviados a proveedor" estado combinado
- **Query:** "en aprobacion o enviados a proveedor"
- **Expected:** `estado=[En aprobacion, enviado Proveedor]`
- **Sprint sugerido:** CORPUS-002

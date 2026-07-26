# M3C-B-R3′ — CLASIFICADOR DE CONTROL CONSCIENTE DE TARGETS M2

> **DOCUMENT_STATUS:** `HISTORICAL_DECISION_RECORD`
> **CURRENT_AUTHORITY:** `2026-07-25_M3C_modality_conversation_closeout.md`
> Documenta la causalidad y la geometría control↔target que produjeron R3. El
> contrato operacional consolidado y as-built está en el closeout vigente.

**MODELO RECOMENDADO:** GPT-5.6 Codex, razonamiento alto.
**ESTADO:** APPROVED_WITH_2_BLOCKING_AMENDMENTS incorporadas. Ejecutable.

## 0. OBJETIVO
Corregir el clasificador puro M3C-B para que un verbo de control (`incluye`,
`permite`, `usa`, `agrega`, `quita`, `excepto`, …) se interprete como operación
conversacional **solo cuando gobierna un target de modalidad producido por M2**,
y nunca cuando es un verbo descriptivo dentro de una cláusula relativa. No
modificar el álgebra del motor, ni la autoridad de M2, ni el enum, ni los códigos
de trace, ni los mensajes. Sin aliases jurídicos.

## 1. CONTEXTO
```
WORKTREE: /Users/rodrigoortiz/Documents/secoppal-modality
RAMA:     search/modality-contract-v1
HEAD:     33bbdd35d5c1f95e3cb9d598321a876ea7e3f431
```
El worktree contiene el overlay M3C-C **no commiteado** (parsing.py modificado +
wiring/tests untracked + los 4 `xfail` descriptivos). No eliminar, revertir,
mover, stashear ni commitear el overlay. No commit/merge/rebase/push durante R3′.

## 2. ARCHIVOS AUTORIZADOS
**Núcleo M3C-B:**
```
app/core/modality_followup.py
tests/test_modality_followup.py
```
**Plumbing quirúrgico (solo transportar spans; NO tocar el endurecimiento R1 de
deser/filtro estrictos):**
```
app/core/modality_followup_wiring.py     # solo classify_control: añadir target_spans
tests/test_modality_followup_wiring.py    # solo el call site + tests de plumbing/validación
```
**Overlay M3C-C (modificación condicional, solo al final):**
```
tests/test_modality_followup_pipeline.py  # retirar los 4 xfail(strict) una vez verdes;
                                          # convertir a tests normales; NO cambiar expectativas
```
Prohibido tocar cualquier otro archivo (`followup_engine.py`,
`followup_mutations.py`, `query_router.py`, `query_frame.py`, `query_plan.py`,
`planning.py`, `normal_flow.py`, `parsing.py`). Si alguno resulta necesario →
`STOPPED_M3C_B_R3_SCOPE_CONFLICT`.

## 3. PREMISA VERIFICADA (no re-derivar)
`ModalityInterpretation` (app/core/modality_frame_adapter.py:52-53) ya expone
`consumed_spans: tuple[tuple[int,int],...]` y `residual_text`. El wiring los pasa
**sin re-ejecutar M2**. El preflight (Apéndice A) confirma que los estados
bloqueados con evidencia traen spans.

**Comportamiento del clasificador (no promesa de M2):** todo candidato atómico —o
candidato `sin`— cuyo span esté DENTRO o SOLAPE un `consumed_span` se considera
texto del target y se descarta. Los controles válidos deben quedar FUERA de los
target spans.

## 4. CONTRATO DE CLASIFICACIÓN (única vía de detección atómica)
Reemplazar la búsqueda libre `finditer` por asociación candidato↔target. Para cada
candidato de señal atómica:
```
1. Rechazar si el span del verbo está DENTRO o SOLAPA un target M2.
2. Buscar el primer target M2 completamente a la DERECHA del candidato.
3. Si no existe target a la derecha: no es operación dirigida.
4. Si no existe target ANTERIOR: el candidato gobierna el target derecho.
5. Si existe target anterior: examinar el segmento previous_target.end → candidate.start.
6. Si en ese segmento hay una cláusula relativa ACTIVA (abierta por
   que/quien/quienes/cuyo/cuya/donde) sin frontera fuerte posterior:
   candidato DESCRIPTIVO → descartar.
7. Si no hay cláusula relativa activa: el candidato gobierna el target derecho.
8. Las conjunciones y/e/o/u NO abren ni cierran por sí solas una frontera.
9. Frontera fuerte (, ; :) puede cerrar una cláusula relativa antes de un nuevo control.
10. Agrupar candidatos válidos por operación:
      misma operación (aunque múltiples spans) → una instrucción;
      operaciones distintas → control_conflict.
```
`control_spans` sigue: ordenados, disjuntos, dentro del texto original, offsets
exactos, **sin** incluir separadores/relativizadores/target.

**Nota `sin`:** `sin` NO va por la tabla `_ATOMIC`, sino por `_sin_control_spans()`.
La comprobación de pertenencia a target_spans (paso 1) debe integrarse **dentro de
`_sin_control_spans()`**, aplicándose a cada candidato `sin` ANTES de evaluar su
polaridad. No basta con implementarla para los atómicos.

## 5. PRECEDENCIA DEL `sin` RESIDUAL (derivada de M2, no arbitraria)
```
El paso 1 (membresía de span) tiene precedencia absoluta.
Un "sin" DENTRO de un consumed_span es SIEMPRE jurídico (nunca control).
La regla de polaridad de "sin" solo evalúa un "sin" RESIDUAL, fuera de todo target
span, y conserva su condición actual: control ADD_EXCLUDE solo si el filtro M2 es
exclude-only.
```
Verificado (Apéndice A): "contratación directa sin ofertas" → "sin" dentro del span
→ jurídico; "sin mínima cuantía" → "sin" residual + exclude-only → ADD_EXCLUDE;
"contratación directa sin mínima cuantía" (mixto) → "sin" residual pero NO
exclude-only → no control. Las dos geometrías coinciden.

## 6. FALLBACK DE STATUS BLOQUEADO CON SPANS VACÍOS (extremadamente acotado)
```
Si resolution_status ∈ {PARTIAL, AMBIGUOUS, CONFLICT, NOT_SEARCHABLE,
                        DATASET_INAPPLICABLE}
   y target_spans == ()
   y hay un control imperativo al INICIO LÓGICO del turno:
      conservar la operación dirigida (para que el motor emita su bloqueo específico).
NO_MATCH NUNCA usa este fallback.
```
El fallback no ejecuta nada, no inventa targets: solo preserva la operación para que
el motor produzca `partial_target`/`ambiguous_target`/`conflicting_target`/
`non_searchable_target`/`dataset_inapplicable_target`. Preserva la invariante R1. Se
prueba con una `ModalityInterpretation` **construida** para `NOT_SEARCHABLE` (hoy
inalcanzable desde texto). Si no puede garantizarse limpiamente →
`STOPPED_M3C_B_R3_BLOCKED_TARGET_SPAN_GAP`.

## 7. CONSTRUCCIONES CERRADAS ANCLADAS
Regla definitiva (no "preferiblemente"): las construcciones cerradas (`cualquier
modalidad excepto X`, `no importa la modalidad, solo X`, …) se reconocen **solo al
inicio lógico del turno** (tras espacios/puntuación inicial). Una frase incidental
posterior no las activa.

## 8. PLUMBING DEL WIRING + VALIDACIÓN ESTRICTA DE TARGET_SPANS
Nueva firma pura (`target_spans` OBLIGATORIO, sin default):
```python
classify_modality_followup_control(
    text: str, *, current_filter: ModalityFilter,
    resolution_status: ResolutionStatus | str,
    target_spans: tuple[tuple[int, int], ...],   # OBLIGATORIO
    allow_implicit_replace: bool = False,
)
```
Callers a actualizar (conjunto cerrado, verificado):
```
app/core/modality_followup_wiring.py:187  classify_control → pasar target_spans(interpretation)
tests/test_modality_followup.py:64        helper → pasar spans realistas
tests/test_modality_followup_wiring.py:324 call site
```
Añadir helper `target_spans(interpretation)` → `interpretation.consumed_spans` (o
`()` si `interpretation is None`). `()` es legítimo para NO_MATCH/CLEAR/`sin`/
ausencia de mención; nadie obtiene el comportamiento viejo por olvido.

**VALIDACIÓN DE TARGET_SPANS (fail-closed, antes de clasificar):**
```
type(target_spans) is tuple
cada span es tuple de exactamente dos elementos
type(start) is int  and  type(end) is int      # rechaza bool/float/str/Decimal
0 <= start < end <= len(text)
spans ordenados y sin solaparse
```
Un span corrupto lanza `ValueError`; NUNCA degrada silenciosamente a clasificación
libre. Mismo principio fail-closed que el endurecimiento R1 de deserialización.
**Tests obligatorios:** bool como índice → ValueError; float → ValueError; string
numérico → ValueError; span invertido (start≥end) → ValueError; span fuera del texto
→ ValueError; spans desordenados → ValueError; spans solapados → ValueError;
contenedor no-tuple → ValueError.

## 9. PRESUPUESTO DE COMPLEJIDAD
```
NET_PRODUCTION_CHANGE_TARGET: <= 60 líneas netas
TOTAL_PRODUCTION_LINES_TARGET: <= 370
HARD_STOP:                     > 385  → STOPPED_M3C_B_R3_COMPLEXITY_BUDGET
```
Preferir un helper legible a un regex comprimido. Una sola vía de detección
atómica: no coexisten regex libres y acotadas.

## 10. GUARD ANTI-IMPROVISACIÓN
```
No añadir más reglas lingüísticas durante la implementación.
Si el corpus revela una geometría de control no contemplada por §4-§7:
    STOPPED_M3C_B_R3_UNMODELED_CONTROL_GEOMETRY
```
Prohibido `if "que"`, `if "quien"`, `if "y"` ad hoc reactivos.

## 11. CORPUS OBLIGATORIO (tests puros en test_modality_followup.py)
- **Descriptivos puros** (→ none sin implicit; REPLACE con implicit): `incluye`/
  `permite`/`usa`/`excluye`/`elimina`/`agrega` dentro de `que…/quien…/para quien…`.
  Nunca la op dirigida.
- **Comandos a media frase** (→ op): "de esos resultados quita contratación directa"
  → REMOVE_MEMBER; "en ese caso agrega mínima cuantía" → ADD_INCLUDE; "mejor usa
  subasta inversa" → REPLACE.
- **Control seguido de descripción** (→ solo el control anterior): "agrega
  contratación directa que incluye suministro" → ADD_INCLUDE; "ahora régimen
  especial que permite ofertas" → REPLACE.
- **Coordinación:** "agrega e incluye contratación directa" → ADD_INCLUDE (dos
  señales equiv.); "quita licitación pública y agrega mínima cuantía" →
  control_conflict; "régimen que permite ofertas y usa plataforma" → none.
- **`sin` ambas polaridades** (Apéndice A) + **cerradas ancladas** + **fallback
  bloqueado con `ModalityInterpretation` sintética NOT_SEARCHABLE** + **validación
  estricta de target_spans (§8)**.

## 12. VALIDACIÓN (una sola corrida de suite completa)
```
py_compile  modality_followup.py + modality_followup_wiring.py + tests tocados
pytest tests/test_modality_followup.py -q                       # puros primero
pytest [batería focal de 8 archivos] -q                         # con los 4 xfail → normales
# solo si todo verde:
pytest -q                                                       # suite completa, una vez
git diff --check ; git status --short ; git diff --name-only ; git diff --stat
```
Regresiones obligatorias verdes: 128 M3C-B originales, protección `sin` jurídico,
estados M2 bloqueados, filtros mixtos, RESTORE_INCLUDE abierto, conflictos de
control, cerradas, serialización/wiring M3C-C, evidencia SoQL M3C-C.

## 13. PROTECCIÓN DEL OVERLAY Y ESTRATEGIA DE COMMIT
Registrar hashes antes/después de `parsing.py`, `modality_followup_wiring.py`,
`tests/test_modality_followup_wiring.py`. Única modificación autorizada del overlay
durante R3′ = retirar los 4 xfail del pipeline.

**No hacer commit durante R3′.** Cuando se autorice el commit (tras revisión + suite
verde), usar DOS commits coherentes y autocontenidos — nunca commitear solo la firma
pura confiando en un caller untracked:

```
Commit 1 — primitivas + adaptador (estado válido: el módulo wiring existe aunque
           aún no esté enganchado en el grafo; parsing.py importa wiring, no al revés)
  app/core/modality_followup.py
  app/core/modality_followup_wiring.py
  tests/test_modality_followup.py
  tests/test_modality_followup_wiring.py
  msg: fix(modality): bind follow-up controls to resolved target spans

Commit 2 — integración M3C-C
  app/core/workflow/actions/parsing.py
  tests/test_modality_followup_pipeline.py
  (+ cualquier otro archivo M3C-C autorizado y realmente modificado)
  msg: feat(modality): wire conversational follow-up mutations
```
Alternativa igualmente válida: un único commit conjunto M3C-C + R3′.

## 14. SALIDA
Heredar el formato de M3C-B-R3 §15, añadiendo:
`CONTROL_CONTRACT_STEPS`, `TARGET_SPANS_STRICT_VALIDATION`, `SIN_PRECEDENCE`,
`SIN_MEMBERSHIP_IN_SIN_PATH`, `BLOCKED_EMPTY_SPAN_FALLBACK`,
`NOT_SEARCHABLE_SYNTHETIC_TEST`, `UNMODELED_GEOMETRY_GUARD`, `WIRING_PLUMBING_LINES`,
`COMMIT_STRATEGY`.
FINAL_VERDICT ∈ {`M3C_B_R3_READY_FOR_REVIEW`, `STOPPED_M3C_B_R3_SCOPE_CONFLICT`,
`…_WIRING_INTERACTION`, `…_BLOCKED_TARGET_SPAN_GAP`, `…_COMPLEXITY_BUDGET`,
`…_UNMODELED_CONTROL_GEOMETRY`, `…_FULL_SUITE_REGRESSION`}.

---

## Apéndice A — Preflight verificado (no re-derivar)
```
consumed_spans presentes en estados bloqueados con evidencia:
  PARTIAL "quita lic pública o subasta"        → [(6,24),(27,34)]
  AMBIGUOUS "además subasta"                    → [(7,14)]
  CONFLICT "agrega subasta inversa con precal." → [(7,42)]
  DATASET_INAPPLICABLE "agrega solicitud… contratos" → [(7,45)]
  NO_MATCH                                       → ()  (esperado; ya da none por has_evidence)
  NOT_SEARCHABLE                                 → inalcanzable desde texto
                                                   (platform_test→NO_MATCH); usar test sintético
sin:
  "contratación directa sin ofertas"        RESOLVED include → "sin" dentro span → jurídico
  "sin mínima cuantía"                       RESOLVED exclude → "sin" residual → ADD_EXCLUDE
  "contratación directa sin mínima cuantía"  RESOLVED mixto   → "sin" residual, no exclude-only → no control
```

# M3C — Contrato conversacional de modalidad (versión final corregida)

> **DOCUMENT_STATUS:** `HISTORICAL_DESIGN_BASELINE`
> **CURRENT_AUTHORITY:** `2026-07-25_M3C_modality_conversation_closeout.md`
> Este documento registra el diseño aprobado al inicio de M3C. La
> implementación final incorporó enmiendas posteriores y su contrato operacional
> consolidado está en el closeout as-built. En particular:
>
> - `RESTORE_INCLUDE` en universo abierto **NO crea whitelist**;
> - solo agrega el target a include cuando ya existía una whitelist;
> - M3C-D quedó diferido por la compuerta de producto;
> - ante cualquier contradicción, manda el closeout as-built.

> **Ronda de diseño.** Este documento congela la arquitectura y el plan de
> implementación de M3C. No modifica código de producción, tests ni contratos
> congelados de M1, M2 o M3B-2. No hay commit, merge, rebase ni push.

---

## 0. Metadatos y veredicto ejecutivo

**MODEL_USED:** GPT-5.6 Thinking
**MODELO RECOMENDADO PARA ESTA RONDA:** Claude Opus, razonamiento alto
**WORKTREE ESPERADO:** `/Users/rodrigoortiz/Documents/secoppal-modality`
**BRANCH ESPERADA:** `search/modality-contract-v1`
**HEAD ESPERADO:** `f94ffbb2f0ba65e6333efc1cb6eb940260ea6b67`
**BASE CONGELADA:** `feat(modality): integrate canonical query pipeline`
**PRECHECK LOCAL:** pendiente de ejecutar en el worktree real
**GO/NO-GO:** **GO para M3C-B**. M3C-D permanece sujeto a compuerta cuantitativa de producto.

### Decisión

M3C es la siguiente etapa correcta y tiene alto valor de producto. Convierte la
modalidad desde un filtro técnico de una sola consulta en una capacidad
conversacional visible: reemplazar, ampliar, excluir, retirar, restaurar y
limpiar criterios sin reconstruir toda la búsqueda.

La arquitectura se aprueba con estas correcciones incorporadas:

1. No hay precedencia numérica entre señales de control.
2. Las construcciones compuestas cerradas se reconocen antes que las señales
   atómicas.
3. Dos operaciones incompatibles no cubiertas por una construcción cerrada
   producen `control_conflict`.
4. Se agrega `RESTORE_INCLUDE` como operación distinta de `REMOVE_EXCLUDE`.
5. Los targets dirigidos se derivan de la unión del filtro canónico del turno,
   sin depender de la polaridad superficial con que M2 los produjo.
6. `sin modalidad` no significa `CLEAR`; requiere aclaración.
7. El motor de modalidad se aplica fuera del merge genérico.
8. La aclaración pendiente vive en un sidecar de `ConversationStore`, con TTL de
   30 minutos y consumo único.
9. M3C-D puede extender de forma controlada la interfaz de parseo mediante un
   override efímero de texto para M2; no cambia la semántica general de
   `QueryRouter`.
10. M3C-D solo se inicia después de medir la frecuencia real de estados M2 no
    ejecutables en follow-ups.
11. `RESTORE_INCLUDE` distingue entre whitelist y universo abierto: solo agrega
    targets a include cuando ya existe una whitelist; en modo abierto se limita
    a retirar la exclusión.
12. Un follow-up con target M2 ejecutable, sin control explícito y clasificado
    como follow-up usa `REPLACE` implícito.
13. Los procedure IDs pueden aparecer en logs técnicos locales; la telemetría
    externa conserva únicamente operación, status, códigos y conteos.
14. El sidecar guarda siempre `base_query_fingerprint`, agrega `base_turn_id`
    cuando exista y denomina `issue_target_ids` a la evidencia no ejecutable.

---

# 1. Alcance

## 1.1 Objetivo

Diseñar el contrato completo para que un follow-up pueda modificar el filtro
canónico de modalidad de forma:

- determinista;
- fail-closed;
- auditable;
- idempotente;
- ortogonal a topic, scope, fecha, estado, monto y orden;
- independiente del dataset hasta la proyección del builder;
- compatible con los contratos congelados de M1, M2 y M3B-2.

Casos principales:

```text
ahora licitación pública
cambia a mínima cuantía
solo subasta inversa
además contratación directa
también régimen especial
excepto contratación directa
sin mínima cuantía
quita contratación directa
elimina régimen especial
permite contratación directa
deja de excluir mínima cuantía
vuelve a incluir contratación directa
sin filtro de modalidad
cualquier modalidad
no importa la modalidad
```

## 1.2 Fuera de alcance

M3C no debe reabrir:

- catálogo M1;
- resolver, lexicón, composición o alternativas M2;
- precedencia lingüística M2;
- spell correction;
- proyección por dataset;
- semántica del SoQLBuilder;
- raws SECOP;
- Radar o Warehouse;
- ejecución live durante los bloques B y C.

M3C-D sí podrá ampliar **la interfaz** de parseo para una aclaración pendiente,
pero no podrá introducir una segunda semántica de modalidad.

---

# 2. Mapa actual del pipeline

## 2.1 Flujo vigente después de M3B-2

```text
user_query
  ↓
workflow.actions.parsing.parse_query
  ↓
QueryRouter.parse(user_query)
  ↓
resolve_parser_modality(...)
  ├─ M2 se invoca una sola vez
  ├─ produce ModalityInterpretation efímera
  ├─ proyecta solo executable_filter a params
  └─ enmascara spans consumidos antes de extraer topic
  ↓
parsed_params
  ├─ modalidad_include_procedure_ids
  └─ modalidad_exclude_procedure_ids
  ↓
apply_context, si followup=True
  ↓
FollowupEngine.detect_and_merge
  ↓
QueryFrame
  ├─ persiste únicamente include/exclude canónicos
  └─ no persiste ModalityInterpretation
  ↓
plan_query
  ↓
evaluate_modality_execution(ModalityInterpretation)
  ├─ NO_MATCH / RESOLVED / RESOLVED_WITH_ASSUMPTION → ejecutable
  └─ PARTIAL / AMBIGUOUS / CONFLICT / NOT_SEARCHABLE /
     DATASET_INAPPLICABLE → bloqueo y aclaración
  ↓
modality_dataset_projection
  ↓
raws exactos por dataset
  ↓
SoQLBuilder
```

## 2.2 Contrato ejecutable vigente

La única representación productiva de modalidad es:

```text
modalidad_include_procedure_ids
modalidad_exclude_procedure_ids
```

`ModalityFilter` ya garantiza:

- orden determinista;
- deduplicación;
- include/exclude disjuntos;
- proyección JSON-native;
- ausencia de claves vacías;
- rechazo del escalar legado `modalidad` en `QueryFrame`.

## 2.3 Comportamiento follow-up vigente

El merge genérico trata el cluster de modalidad de forma atómica. Cuando un
`refine_filter` acepta una de las dos claves, la transición limpia ambas y
escribe el cluster nuevo. Esto permite reemplazo básico, pero no expresa:

- agregar sin reemplazar;
- excluir conservando includes;
- retirar un miembro;
- retirar solo una exclusión;
- restaurar una inclusión;
- limpiar explícitamente el filtro;
- distinguir no-op de error;
- resolver conflictos entre verbos de control.

## 2.4 Persistencia vigente de aclaraciones

`ModalityInterpretation` viaja de forma efímera por el state del workflow.
`plan_query` bloquea estados no ejecutables y `resolve_entities` conserva los
`parsed_params` entendidos como `resolved_params`, pero no existe un sidecar que
persista:

- operación conversacional pendiente;
- status M2;
- issue codes;
- target text original;
- candidatos declarados;
- turno base;
- expiración.

Por tanto, el sistema puede preguntar, pero no puede completar de forma segura
un ciclo como:

```text
T1: subasta de alimentación
Sistema: ¿qué tipo de subasta?
T2: inversa
```

## 2.5 Autoridad duplicada

**DUPLICATED_AUTHORITY_FOUND: NO.**

No existe otra autoridad jurídica además de M2. Sí existe una deuda de
**transporte y mutación**: el merge genérico reemplaza el cluster de modalidad,
pero no conoce las operaciones conversacionales. M3C debe resolver esa deuda sin
convertir `FollowupEngine` en un segundo parser jurídico.

---

# 3. Principios e invariantes

## 3.1 M2 sigue siendo la única autoridad semántica

La capa conversacional puede reconocer solamente lenguaje de control:

```text
ahora
cambia a
solo
además
excepto
quita
permite
vuelve a incluir
limpia
```

No puede reconocer por su cuenta:

```text
licitación pública
mínima cuantía
subasta inversa
contratación directa
```

Los targets siempre provienen del filtro y el status producidos por M2.

## 3.2 Metadata efímera

La operación viaja como metadata tipada del turno. Nunca se agrega a `params`:

```text
modality_operation
modality_action
add_modalities
remove_modalities
clarification_status
```

Solo el filtro final entra en:

- `QueryFrame`;
- fingerprint;
- builder;
- persistencia del turno;
- ejecución.

## 3.3 Fail-closed

Si una operación dirigida requiere targets y M2 no produjo una interpretación
ejecutable:

- no aplicar miembros sanos de `PARTIAL`;
- no usar `candidate_filter`;
- no inferir a partir de `issues`;
- no ejecutar una búsqueda ampliada;
- no heredar silenciosamente una modalidad aproximada.

## 3.4 Independencia del dataset

Los `procedure_ids` sobreviven cambios de dataset. Solo el builder reproyecta.
Un include inaplicable mantiene su semántica y termina en `1=0`; no se limpia ni
se traduce a raws durante el merge.

## 3.5 Invariantes del resultado

Todo filtro final debe ser:

- ordenado;
- deduplicado;
- include/exclude disjunto;
- JSON-native;
- formado por IDs conocidos;
- independiente del orden de señales equivalentes;
- idempotente ante repetición de la misma operación.

---

# 4. Arquitectura recomendada

## 4.1 Módulo puro

Crear:

```text
app/core/modality_followup.py
```

Responsabilidades:

- enum cerrado de operaciones;
- clasificación de lenguaje de control;
- detección de construcciones cerradas;
- detección de conflicto de control;
- álgebra sobre `ModalityFilter`;
- gate por status M2;
- validación contra conjunto de IDs conocidos;
- decisión tipada y trace JSON-native.

No importar:

- `QueryRouter`;
- `FollowupEngine`;
- `SoQLBuilder`;
- `ConversationStore`;
- workflow handlers;
- Socrata o HTTP.

Puede importar únicamente contratos congelados:

- `ModalityFilter`;
- `ModalityInterpretation` o `ResolutionStatus`;
- helpers puros de normalización/masking ya existentes, si son necesarios.

## 4.2 Flujo de wiring

```text
parse_query
  ↓
QueryRouter + M2, exactamente una vez
  ↓
classify_modality_followup_operation
  ├─ solo lenguaje de control
  ├─ metadata efímera
  └─ spans de control
  ↓
apply_context
  ├─ reconstruye previous ModalityFilter
  ├─ reconstruye current ModalityFilter
  ├─ retira temporalmente las claves de modalidad del delta genérico
  ├─ enmascara spans de control para el follow-up genérico
  ├─ ejecuta detect_and_merge para topic/scope/fecha/estado/orden/dataset
  ├─ aplica el motor puro con previous + current + operation
  └─ escribe únicamente el filtro final en params mergeados
  ↓
QueryFrame persiste include/exclude finales
```

## 4.3 Por qué el motor queda fuera del merge genérico

Esto evita:

- condicionales de modalidad dentro de `FollowupEngine`;
- que `normalize_filter_transition` borre el filtro anterior antes del álgebra;
- que el trace genérico marque como rechazado un campo correctamente delegado;
- que una operación de modalidad borre topic, scope, fecha o orden;
- duplicar semántica jurídica dentro de las políticas de mutación.

El merge genérico permanece responsable de los dominios existentes. La modalidad
se aplica después, como una mutación ortogonal.

---

# 5. Modelo de operaciones

## 5.1 Enum cerrado

```python
class ModalityFollowupOperation(str, Enum):
    REPLACE = "replace"
    ADD_INCLUDE = "add_include"
    ADD_EXCLUDE = "add_exclude"
    REMOVE_MEMBER = "remove_member"
    REMOVE_EXCLUDE = "remove_exclude"
    RESTORE_INCLUDE = "restore_include"
    CLEAR = "clear"
```

No agregar:

- `INHERIT`: heredar es ausencia de operación;
- una operación por cada verbo superficial;
- `modify_modality` como intent global;
- `REPLACE_EXCLUDE`: una construcción como “cualquier modalidad excepto X” se
  representa mediante `REPLACE` y el filtro actual producido por M2.

## 5.2 Instrucción principal

Contrato orientativo:

```python
@dataclass(frozen=True)
class ModalityFollowupInstruction:
    status: str  # none | resolved | ambiguous | conflict
    operation: ModalityFollowupOperation | None
    control_code: str | None
    construction_id: str | None
    source_text: str
    control_spans: tuple[tuple[int, int], ...]
    target_required: bool
    explanation: str
```

`control_spans` permite ocultar verbos como `quita`, `agrega` o `solo` antes de
que el clasificador genérico procese el turno. No contiene spans jurídicos: esos
siguen siendo autoridad de M2.

## 5.3 Decisión de mutación

Contrato orientativo:

```python
@dataclass(frozen=True)
class ModalityMutationDecision:
    should_apply: bool
    final_filter: ModalityFilter
    reason: str
    needs_clarification: bool
    clarification_code: str | None
    no_op: bool
    trace: dict[str, object]
```

El trace debe ser JSON-native y no entrar en `params`.

---

# 6. Clasificador de control

## 6.1 Algoritmo

No usar una precedencia numérica del tipo `CLEAR > REMOVE > ADD`. El algoritmo
correcto es:

1. Normalizar una vista de control preservando offsets.
2. Reconocer primero construcciones compuestas cerradas.
3. Si una construcción cerrada coincide de forma única, emitir su operación.
4. Si no coincide, detectar todas las señales atómicas de control.
5. Si no hay señales y el turno trae un filtro M2 nuevo, preservar el
   comportamiento existente mediante `REPLACE` implícito.
6. Si todas las señales expresan la misma operación, emitirla.
7. Si aparecen operaciones distintas no cubiertas por una construcción cerrada,
   emitir `control_conflict`.
8. Si la forma es deliberadamente ambigua, emitir `ambiguous_control`.
9. Nunca elegir la primera regex encontrada.

## 6.2 Construcciones cerradas iniciales

| Construcción | Operación | Motivo |
|---|---|---|
| `cualquier modalidad excepto X` | `REPLACE` | M2 produce filtro solo-exclude |
| `todas las modalidades excepto X` | `REPLACE` | reemplazo completo por exclusión |
| `no importa la modalidad, solo X` | `REPLACE` | el target explícito domina el clear superficial |
| `no importa la modalidad, busca X` cuando X es modalidad resuelta | `REPLACE` | construcción de sustitución explícita |

No extender la lista sin corpus y test adversarial.

## 6.3 Señales atómicas iniciales

| Operación | Señales de control |
|---|---|
| `REPLACE` | `ahora`, `cambia a`, `usa`, `solo`, `únicamente` |
| `ADD_INCLUDE` | `además`, `también`, `agrega`, `incluye` |
| `ADD_EXCLUDE` | `excepto`, `excluye`, `sin X` cuando X fue resuelto por M2 |
| `REMOVE_MEMBER` | `quita`, `elimina`, `saca` |
| `REMOVE_EXCLUDE` | `permite`, `deja de excluir` |
| `RESTORE_INCLUDE` | `vuelve a incluir`, `reincorpora` |
| `CLEAR` | frases explícitas cerradas de limpieza |

Las señales se reconocen como lenguaje de control, no como aliases jurídicos.

## 6.4 CLEAR

CLEAR solo se activa con expresiones inequívocas:

```text
sin filtro de modalidad
cualquier modalidad
todas las modalidades
no importa la modalidad
quita el filtro de modalidad
elimina el filtro de modalidad
```

La frase:

```text
sin modalidad
```

no activa CLEAR. Se clasifica como `ambiguous_control`, porque puede significar:

1. quitar el filtro;
2. buscar registros cuyo dato de modalidad esté vacío o ausente.

Mensaje sugerido:

```text
¿Quieres quitar el filtro de modalidad o buscar registros sin ese dato?
```

---

# 7. Derivación de targets

Para toda operación dirigida distinta de `REPLACE`:

```text
targets = current.include ∪ current.exclude
```

La capa conversacional decide después si esos targets se agregan, excluyen,
retiran o restauran.

Esto evita depender de la polaridad superficial de M2. Ejemplos:

- `excepto contratación directa` puede producir el target en `current.exclude`;
- `quita contratación directa` puede producirlo en `current.include`;
- ambos deben dirigir la operación al mismo conjunto canónico.

Para `REPLACE`:

```text
final = current
```

No hay regla especial para `solo`: M2 ya entrega el filtro del turno y el
reemplazo elimina todo el filtro anterior.

Para `CLEAR`:

```text
targets = vacío
```

---

# 8. Álgebra de mutación

Notación:

```text
P+ = previous.include
P- = previous.exclude
C+ = current.include
C- = current.exclude
T  = C+ ∪ C-
F+ = final.include
F- = final.exclude
```

## 8.1 REPLACE

```text
F+ = C+
F- = C-
```

Requiere target ejecutable y filtro actual no vacío.

## 8.2 ADD_INCLUDE

```text
F+ = P+ ∪ T
F- = P- - T
```

Agregar un target previamente excluido lo retira de exclude.

## 8.3 ADD_EXCLUDE

```text
F- = P- ∪ T
F+ = P+ - T
```

Excluir un target previamente incluido lo retira de include.

## 8.4 REMOVE_MEMBER

```text
F+ = P+ - T
F- = P- - T
```

Retira el target del filtro completo; no lo convierte en exclusión.

## 8.5 REMOVE_EXCLUDE

```text
F+ = P+
F- = P- - T
```

`permite X` y `deja de excluir X` no fuerzan inclusión.

## 8.6 RESTORE_INCLUDE

`RESTORE_INCLUDE` debe respetar el modo semántico del filtro anterior.

```text
F- = P- - T

si P+ ≠ ∅:
    F+ = P+ ∪ T
si P+ = ∅:
    F+ = ∅
```

Interpretación:

- **Modo whitelist (`P+ ≠ ∅`)**: `vuelve a incluir X` retira X de exclude y lo
  agrega a la lista permitida.
- **Modo abierto (`P+ = ∅`)**: el filtro significa “todas excepto...”; restaurar
  X solo elimina su exclusión y nunca crea una whitelist accidental.

Casos canónicos:

```text
include=[A], exclude=[B], restore B
→ include=[A,B], exclude=[]

include=[], exclude=[B], restore B
→ include=[], exclude=[]

include=[], exclude=[], restore B
→ no-op; include=[], exclude=[]

include=[A], exclude=[], restore B
→ include=[A,B], exclude=[]
```

La operación continúa siendo distinta de `REMOVE_EXCLUDE` por intención,
mensajería, trace y telemetría, aunque ambas coincidan cuando el filtro previo es
abierto.

## 8.7 CLEAR

```text
F+ = ∅
F- = ∅
```

No requiere targets y funciona aunque M2 produzca `NO_MATCH`.

## 8.8 NONE / herencia

Si no existe una operación de modalidad:

- M3C no aplica mutación propia;
- el comportamiento genérico vigente permanece intacto;
- una operación anterior nunca se hereda como metadata;
- el filtro anterior se conserva o se limpia según las políticas genéricas ya
  congeladas para el intent global.

---

# 9. Matriz completa de álgebra

| # | P+ | P- | C+ | C- | Operación | F+ | F- | Aplicar | Resultado |
|---:|---|---|---|---|---|---|---|---|---|
| 1 | ∅ | ∅ | A | ∅ | REPLACE | A | ∅ | sí | reemplazo inicial |
| 2 | A | ∅ | B | ∅ | REPLACE | B | ∅ | sí | A desaparece |
| 3 | A | ∅ | B | ∅ | ADD_INCLUDE | A∪B | ∅ | sí | unión |
| 4 | A | ∅ | A | ∅ | ADD_INCLUDE | A | ∅ | sí | no-op idempotente |
| 5 | ∅ | A | A | ∅ | ADD_INCLUDE | A | ∅ | sí | retira exclusión |
| 6 | A | ∅ | A | ∅ | ADD_EXCLUDE | ∅ | A | sí | mueve a exclude |
| 7 | A∪B | ∅ | A | ∅ | REMOVE_MEMBER | B | ∅ | sí | retira A |
| 8 | ∅ | A∪B | A | ∅ | REMOVE_MEMBER | ∅ | B | sí | retira A de exclude |
| 9 | ∅ | A | A | ∅ | REMOVE_EXCLUDE | ∅ | ∅ | sí | permite A sin incluirlo |
| 10 | A | ∅ | ∅ | ∅ | CLEAR | ∅ | ∅ | sí | filtro limpio |
| 11 | ∅ | ∅ | ∅ | ∅ | CLEAR | ∅ | ∅ | sí | no-op exitoso |
| 12 | A | ∅ | candidato B | ∅ | ADD_INCLUDE + PARTIAL | A | ∅ | no | `partial_target` |
| 13 | A | ∅ | candidatos | ∅ | REPLACE + AMBIGUOUS | A | ∅ | no | `ambiguous_target` |
| 14 | A | ∅ | B | ∅ | ADD_INCLUDE + DATASET_INAPPLICABLE | A | ∅ | no | `dataset_inapplicable_target` |
| 15 | A | ∅ | unknown | ∅ | ADD_INCLUDE | A | ∅ | no | `unknown_procedure_id` |
| 16 | A | ∅ | B | ∅ | ADD + REMOVE simultáneos | A | ∅ | no | `control_conflict` |
| 17 | A | ∅ | A | ∅ | REMOVE_MEMBER | ∅ | ∅ | sí | filtro queda vacío válidamente |
| 18 | A | ∅ | ∅ | ∅ | change_dataset sin operación | A | ∅ | n/a | conserva IDs y reproyecta |
| 19 | A | B | B | ∅ | RESTORE_INCLUDE | A∪B | ∅ | sí | whitelist: restaura y agrega B |
| 20 | ∅ | B | B | ∅ | RESTORE_INCLUDE | ∅ | ∅ | sí | modo abierto: solo retira exclusión |
| 21 | ∅ | ∅ | B | ∅ | RESTORE_INCLUDE | ∅ | ∅ | sí | no-op; no crea whitelist |
| 22 | A | ∅ | B | ∅ | RESTORE_INCLUDE | A∪B | ∅ | sí | whitelist explícita: agrega B |
| 23 | A | B | B | ∅ | REMOVE_EXCLUDE | A | ∅ | sí | solo permite, no fuerza include |

Todo resultado se construye mediante `ModalityFilter`; nunca se mutan listas en
sitio.

---

# 10. Gate por status M2

## 10.1 Estados permitidos para operaciones dirigidas

```text
RESOLVED
RESOLVED_WITH_ASSUMPTION
```

## 10.2 Estados bloqueados

```text
PARTIAL
AMBIGUOUS
CONFLICT
NOT_SEARCHABLE
DATASET_INAPPLICABLE
```

## 10.3 NO_MATCH

| Situación | Política |
|---|---|
| CLEAR | permitido |
| operación dirigida | bloquear `target_missing` |
| sin operación | ausencia de mutación de modalidad |
| consulta inicial sin modalidad | búsqueda normal sin filtro |

## 10.4 Políticas específicas

- `PARTIAL`: no usar `candidate_filter`; aclarar.
- `AMBIGUOUS`: no escoger un candidato; aclarar.
- `CONFLICT`: no mutar; pedir reformulación.
- `NOT_SEARCHABLE`: no convertir el target en filtro.
- `DATASET_INAPPLICABLE`: no limpiar el filtro previo ni cambiar dataset
  automáticamente.

---

# 11. Precedencia y conflictos de control

## 11.1 Política

No existe ranking numérico entre operaciones.

```text
construcción cerrada única
  → aplicar

una sola operación semántica
  → aplicar

varias señales equivalentes de la misma operación
  → aplicar una vez

operaciones distintas sin construcción cerrada
  → control_conflict
```

## 11.2 Casos

| Texto | Decisión |
|---|---|
| `solo licitación pública` | REPLACE |
| `además licitación pública` | ADD_INCLUDE |
| `además, excepto contratación directa` | control_conflict |
| `quita y agrega contratación directa` | control_conflict |
| `sin modalidad excepto mínima cuantía` | ambiguous/control_conflict; no ejecutar |
| `no importa la modalidad, solo subasta inversa` | construcción cerrada REPLACE |
| `cualquier modalidad excepto directa` | construcción cerrada REPLACE con filtro solo-exclude |

## 11.3 Códigos estables

```text
target_missing
ambiguous_control
control_conflict
partial_target
ambiguous_target
conflicting_target
non_searchable_target
dataset_inapplicable_target
unknown_procedure_id
clear_applied
clear_noop
remove_absent_target
mutation_noop
```

El conflicto conversacional no se representa como `ResolutionStatus.CONFLICT` de
M2. Es un dominio distinto y usa `control_conflict`.

---

# 12. Integración con FollowupEngine

## 12.1 Estrategia de intent

Conservar:

```text
refine_filter
```

No crear `modify_modality`.

La operación concreta vive en `ModalityFollowupInstruction` y se transporta de
forma ortogonal al intent global.

## 12.2 Detección y transporte

`parse_query` debe escribir un nuevo campo efímero del state:

```text
modality_followup_instruction
```

Este campo:

- no entra en `parsed_params`;
- no entra en QueryFrame;
- no entra en fingerprint;
- no llega al builder;
- se consume en `apply_context`;
- se descarta al terminar el turno.

## 12.3 Aislamiento frente al merge genérico

En `apply_context`:

1. Extraer `previous_filter` desde `context_params`.
2. Extraer `current_filter` desde `parsed_params`.
3. Copiar `parsed_params` y retirar temporalmente:

```text
modalidad_include_procedure_ids
modalidad_exclude_procedure_ids
```

4. Enmascarar en `user_query` los `control_spans` de la instrucción.
5. Ejecutar `detect_and_merge` con el texto y delta genéricos.
6. Aplicar la mutación pura usando los filtros originales.
7. Retirar del merge cualquier clave de modalidad heredada que contradiga la
   decisión final.
8. Escribir `decision.final_filter.to_params()`.
9. Añadir un bloque `modality` al mutation trace.

Esto evita modificar el contrato central de `detect_and_merge` en M3C-C, salvo
que los tests demuestren una necesidad concreta.

## 12.4 REPLACE implícito

**Decisión cerrada: sí.** Un turno sin verbo explícito reemplaza la modalidad
anterior únicamente cuando se cumplen todas estas condiciones:

```text
turno clasificado como follow-up
+ M2 produjo target ejecutable
+ no existe control explícito
+ no existe control_conflict
+ el turno no fue clasificado como new_search
→ REPLACE implícito
```

Contrato:

```text
operation = REPLACE
control_code = implicit_target_replace
```

Ejemplo:

```text
T1: contratación directa de alimentación
T2: mínima cuantía
```

Esto centraliza la mutación de modalidad y evita mantener dos caminos —motor
puro y reemplazo atómico genérico— para el mismo dominio. Una consulta nueva que
contenga modalidad no se convierte automáticamente en follow-up.

---

# 13. Mutation trace

Formato sugerido:

```json
{
  "intent": "refine_filter",
  "requested_fields": ["modalidad_include_procedure_ids"],
  "accepted_fields": [],
  "delegated_fields": ["modalidad_include_procedure_ids"],
  "modality": {
    "instruction_status": "resolved",
    "operation": "add_include",
    "control_code": "add_include_ademas",
    "previous": {
      "include": ["licitacion_publica"],
      "exclude": []
    },
    "current_targets": ["contratacion_directa"],
    "final": {
      "include": ["contratacion_directa", "licitacion_publica"],
      "exclude": []
    },
    "reason": "applied",
    "no_op": false
  }
}
```

Requisitos:

- JSON-native;
- orden estable;
- sin raws SECOP;
- sin envelopes M2 completos;
- sin stack traces;
- no persistir la instrucción como params.

### Política de exposición

**Logs técnicos locales:** pueden incluir procedure IDs canónicos para depurar,
auditar y reproducir mutaciones.

**Telemetría externa:** no incluye IDs ni aliases. Solo conserva:

```text
operation
status
reason / clarification_code
previous_include_count
previous_exclude_count
target_count
final_include_count
final_exclude_count
no_op
```

Los raws SECOP nunca entran al trace conversacional ni a analytics.

---

# 14. Composición con otros deltas

## 14.1 Matriz

| Modificación de modalidad | Otro delta | Política |
|---|---|---|
| cualquiera | change_order | permitida |
| cualquiera | change_year | permitida |
| cualquiera | change_scope | permitida |
| cualquiera | change_dataset | permitida; IDs sobreviven |
| cualquiera | refine_topic | permitida si ambos dominios son claros |
| cualquiera | replace_topic | permitida; modalidad se aplica al final |
| cualquiera | exclude_topic | permitida |
| CLEAR | orden/fecha/scope/topic | permitida |
| mutación | pagination_more | prohibida; no paginar y cambiar filtro a la vez |
| mutación | suggestion_selection | manejar sugerencia antes; no combinar |
| mutación | reset/new_search explícito | reset/new_search domina y cancela metadata |
| dos operaciones de modalidad | cualquier delta | control_conflict salvo construcción cerrada |
| target M2 no ejecutable | cualquier delta | bloquear toda ejecución del turno |

## 14.2 Ejemplos

```text
además licitación pública en Antioquia
```

- modalidad: ADD_INCLUDE;
- scope: change_scope/refine correspondiente;
- topic anterior: conservado;
- ambos deltas se aplican.

```text
quita contratación directa y ordena por valor
```

- modalidad: REMOVE_MEMBER;
- orden: change_order;
- WHERE restante no se altera.

```text
solo subasta inversa de alimentación
```

- modalidad: REPLACE;
- topic: `alimentación`;
- no se hereda topic anterior si el clasificador global decide reemplazo de
  topic;
- ningún control token entra en objeto.

```text
ahora contratos y agrega solicitud de información a proveedores
```

- QueryRouter selecciona contratos antes de M2;
- M2 produce `DATASET_INAPPLICABLE`;
- M3C bloquea; no aplica cambio de dataset ni target parcial;
- mensaje concreto ofrece cambiar modalidad o dataset.

---

# 15. QueryFrame, fingerprint y builder

## 15.1 Persistencia

`QueryFrame` persiste únicamente:

```text
modalidad_include_procedure_ids
modalidad_exclude_procedure_ids
```

No se agregan claves nuevas a `_MODIFIER_KEYS`.

## 15.2 Fingerprint

La operación no afecta el fingerprint. Solo el filtro final lo hace.

Dos turnos que terminan en el mismo `ModalityFilter` deben producir la misma
huella, aunque uno haya usado `agrega` y otro `vuelve a incluir`.

## 15.3 Builder

**BUILDER_IMPACT: NONE.**

El builder continúa:

- recibiendo procedure IDs;
- proyectando por dataset;
- rechazando IDs desconocidos;
- emitiendo `1=0` cuando un include es inaplicable;
- sin conocer operaciones conversacionales.

---

# 16. Cambio de dataset

## 16.1 Follow-up exclusivo de dataset

```text
T1: subasta inversa de alimentación en procesos
T2: ahora contratos
```

Resultado:

- `procedure_ids` se conservan;
- dataset cambia;
- builder reproyecta;
- si el include es inaplicable, `1=0`;
- no se limpia modalidad.

## 16.2 Dataset y modalidad en el mismo turno

```text
ahora contratos y agrega solicitud de información a proveedores
```

Política:

- M2 usa el dataset efectivo del turno;
- `DATASET_INAPPLICABLE` bloquea la operación;
- no se aplica parcialmente `change_dataset`;
- no se ejecuta una consulta ampliada.

Este caso entra en M3C-C, no debe aplazarse si el wiring ya admite deltas
ortogonales.

---

# 17. Mensajes de usuario

| Código | Mensaje sugerido |
|---|---|
| `target_missing` | `¿Qué modalidad quieres modificar?` |
| `ambiguous_control` | `¿Quieres quitar el filtro de modalidad o buscar registros sin ese dato?` |
| `control_conflict` | `Veo dos instrucciones distintas sobre la modalidad. ¿Quieres reemplazarla, agregarla, excluirla o quitarla?` |
| `partial_target` | `Entendí una parte de la modalidad, pero necesito que aclares la otra antes de cambiar el filtro.` |
| `ambiguous_target` | `Esa modalidad puede significar varias opciones. ¿Cuál quieres usar?` |
| `conflicting_target` | `La modalidad indicada combina opciones incompatibles. ¿Puedes reformularla?` |
| `non_searchable_target` | `Esa opción no está disponible como filtro. ¿Quieres indicar otra modalidad?` |
| `dataset_inapplicable_target` | `Esa modalidad no está disponible en el conjunto de datos elegido. ¿Quieres cambiar el conjunto o la modalidad?` |
| `clear_applied` | `Quité el filtro de modalidad.` |
| `clear_noop` | `La búsqueda ya estaba sin filtro de modalidad.` |
| `remove_absent_target` | `Esa modalidad no estaba en el filtro; mantuve la búsqueda igual.` |
| `mutation_noop` | `El filtro de modalidad ya tenía ese ajuste.` |

Nunca mostrar procedure IDs ni nombres de enums internos.

---

# 18. M3C-D — Ciclo de aclaración pendiente

## 18.1 Compuerta de producto

M3C-B y M3C-C tienen valor inmediato. M3C-D es más costoso y solo debe comenzar
tras medir la frecuencia real de aclaraciones de modalidad.

### Telemetría mínima previa

Registrar durante dogfood:

```text
modality_followup_turns_total
modality_followup_blocked_total por status
modality_followup_control_conflict_total
modality_followup_noop_total
modality_followup_clear_total
modality_clarification_candidate_total
```

### Umbral cerrado

Evaluar una muestra mínima de **100 follow-ups reales de modalidad**. Iniciar
M3C-D si se cumple cualquiera:

- al menos **10** follow-ups quedan bloqueados por modalidad; o
- la tasa de bloqueo es **≥ 5 %** y existen al menos **5 casos bloqueados**.

Reportar por separado:

```text
PARTIAL
AMBIGUOUS
CONFLICT
NOT_SEARCHABLE
DATASET_INAPPLICABLE
target_missing conversacional
```

No todos justifican el mismo tipo de aclaración. Si no se cumple la compuerta,
aplazar D y cerrar B+C+E.

## 18.2 Modelo de sidecar

```python
@dataclass(frozen=True)
class PendingModalityClarification:
    operation: str
    resolution_status: str
    issue_codes: tuple[str, ...]
    issue_target_ids: tuple[str, ...]
    original_target_text: str
    dataset: str | None
    base_turn_id: str | None
    base_query_fingerprint: str
    created_at: str
    expires_at: str
    clarification_code: str
```

Notas:

- `issue_target_ids` conserva evidencia asociada a una ambigüedad, conflicto u
  otro issue; no implica que esos IDs sean ejecutables ni candidatos seguros.
- `issue_target_ids` nunca se convierte directamente en `ModalityFilter`.
- No persistir `candidate_filter` como filtro aplicable.
- No persistir diagnostics completos ni `ModalityResolution` entero.
- Las fechas se serializan en ISO-8601.

## 18.3 Ubicación

Crear un sidecar específico dentro de `ConversationStore`, análogo a un buffer
conversacional y separado de `Turn.params`.

API orientativa:

```text
set_pending_modality_clarification(chat_id, pending)
get_pending_modality_clarification(chat_id)
consume_pending_modality_clarification(chat_id)
clear_pending_modality_clarification(chat_id, reason)
```

No guardar el sidecar en:

- `QueryFrame`;
- params;
- fingerprint;
- Burr como autoridad persistente.

## 18.4 TTL y consumo

```text
TTL: 30 minutos
cardinalidad: máximo uno por chat
consumo: single-use
```

La lectura debe purgar expirados de forma determinista.

## 18.5 Cancelación

Cancelar por:

- resolución exitosa;
- expiración;
- reset;
- nueva búsqueda explícita;
- instrucción conversacional incompatible;
- reemplazo por otra aclaración pendiente más reciente.

No cancelar por un mero cambio ortogonal que pueda coexistir con la respuesta,
por ejemplo `inversa en Antioquia`, si la modalidad queda resuelta.

## 18.6 Excepción de interfaz para QueryRouter

M3C-D no puede cumplir simultáneamente:

- resolución contextual;
- una sola invocación M2;
- ausencia de contaminación del topic;

sin extender la interfaz de parseo.

### Contrato recomendado

Agregar un input efímero interno:

```python
@dataclass(frozen=True)
class ModalityParseOverride:
    context_prefix: str
    clarification_code: str
```

Firma:

```python
QueryRouter.parse(
    user_query: str,
    *,
    modality_override: ModalityParseOverride | None = None,
) -> ParsedQuery
```

Comportamiento:

1. Si no hay override, el flujo M3B-2 queda byte-for-byte equivalente.
2. Si hay override:

```text
resolution_text = context_prefix + " " + user_query
```

3. M2 se invoca una sola vez sobre `resolution_text`.
4. Los spans M2 se intersectan con el segmento correspondiente a `user_query`.
5. Solo la intersección se enmascara en el texto real del turno.
6. Los spans pertenecientes al prefijo no se proyectan al topic actual.
7. El resto del parser sigue procesando exclusivamente `user_query`.

Ejemplo:

```text
pending.original_target_text = "subasta"
user_query = "inversa en Antioquia"
resolution_text = "subasta inversa en Antioquia"
```

M2 consume el hull `subasta inversa`; al mapear al segmento actual solo se
enmascara `inversa`. `Antioquia` queda disponible para scope y el topic de T1 se
hereda.

Esta es una extensión de interfaz, no una reapertura de la semántica de
`QueryRouter`.

## 18.7 Referencia al turno base y flujo de resolución

El sidecar debe guardar:

```text
base_query_fingerprint: obligatorio siempre
base_turn_id: guardar cuando ConversationStore exponga un ID estable
```

Si existen ambos, deben ser coherentes. Un fingerprint cambiado, turno expirado
o referencia incompatible cancela la aclaración de forma fail-closed.

```text
T1 bloqueado
  ↓
guardar PendingModalityClarification
  ↓
T2 llega
  ↓
resolver si es respuesta, operación nueva o nueva búsqueda
  ↓
si respuesta:
    validar base_turn_id cuando exista
    validar siempre base_query_fingerprint
    construir ModalityParseOverride
    ejecutar QueryRouter/M2 una sola vez
    aplicar gate y operación originales
    recuperar contexto del turno base
    limpiar sidecar
    ejecutar
si no es respuesta:
    cancelar sidecar o pedir precisión, según clasificación
```

---

# 19. Plan de implementación por sub-bloques

## M3C-A — Diseño

**Estado:** este documento.
**Modelo recomendado:** Claude Opus, razonamiento alto.
**Producción:** sin cambios.

Entregable:

```text
docs/planes/2026-07-23_M3C_modality_conversation_contract.md
```

## M3C-B — Clasificador de control y motor puro

**Modelo recomendado:** GPT-5.6 Codex, razonamiento alto.

### Archivos

Crear:

```text
app/core/modality_followup.py
tests/test_modality_followup.py
```

No wiring.

### Contenido

- enum de siete operaciones;
- instrucción y decisión frozen;
- construcciones cerradas;
- conflicto de control;
- `sin modalidad` ambiguo;
- derivación de targets;
- álgebra completa;
- gate M2;
- validación de IDs conocidos por dependencia inyectada;
- trace JSON-native;
- property tests.

### Commit sugerido

```text
feat(modality): add pure conversational mutation algebra
```

## M3C-C — Wiring de operaciones resueltas

**Modelo recomendado:** GPT-5.6 Codex, razonamiento alto.

### Archivos esperados

Modificar:

```text
app/core/workflow/actions/parsing.py
```

Modificar solo con evidencia:

```text
app/core/followup_engine.py
app/core/followup_mutations.py
app/core/workflow/context.py
```

Tests:

```text
tests/test_modality_pipeline_integration.py
tests/test_followup_engine.py
tests/test_followup_mutations.py
tests/test_frame_leak_canary.py
tests/test_dialogue_e2e_invariants.py
```

No existe necesidad preliminar de modificar `FollowupEngine`: el aislamiento de
las claves de modalidad y la aplicación posterior pueden vivir en
`apply_context`. Cambiarlo exige una prueba que demuestre que el wrapper no
alcanza.

### Contenido

- instrucción efímera en Burr state;
- masking de control spans;
- extracción temporal de modalidad del delta genérico;
- motor puro posterior al merge;
- mutation trace delegado;
- CLEAR;
- operaciones con targets ejecutables;
- coexistencia con scope/fecha/orden/topic/dataset;
- frame leak canaries;
- una sola invocación M2.

### Commit sugerido

```text
feat(modality): wire conversational filter mutations
```

## Compuerta C→D

Ejecutar dogfood y medir. No comenzar D automáticamente.

## M3C-D — Sidecar y ciclo de aclaración

**Modelo recomendado:** Claude Opus para revisión de arquitectura y GPT-5.6
Codex, razonamiento alto, para implementación.

### Archivos probables

Crear:

```text
app/core/pending_modality_clarification.py
tests/test_pending_modality_clarification.py
```

Modificar:

```text
app/core/conversation_store.py
app/core/query_router.py
app/core/modality_parser_integration.py
app/core/workflow/actions/parsing.py
app/core/workflow/actions/planning.py
app/core/workflow/actions/rendering.py
app/core/workflow/handlers/normal_flow.py
app/core/workflow/handlers/refinement.py
```

Cada modificación debe justificarse tras auditoría del store y grafo reales.

Tests:

```text
tests/test_conversation_store.py
tests/test_modality_parser_integration.py
tests/test_modality_pipeline_integration.py
tests/test_dialogue_e2e_invariants.py
tests/test_frame_leak_canary.py
```

### Commit sugerido

```text
feat(modality): add pending clarification lifecycle
```

## M3C-E — Corpus, ataques, telemetría y cierre

**Modelo recomendado:** GPT-5.6 Codex, razonamiento medio/alto. GLM-5.2 puede
apoyar generación mecánica de casos, pero no decidir semántica.

### Archivos

```text
tests/fixtures/dialogue_e2e_corpus.yaml
tests/test_dialogue_e2e_invariants.py
tests/test_modality_pipeline_integration.py
docs/planes/2026-07-23_M3C_modality_conversation_closeout.md
```

### Contenido

- corpus multi-turn;
- ataques adversariales;
- paridad de consultas sin operación;
- telemetría de no-op/conflict/blocked;
- full suite;
- cierre y decisión M3D.

### Commit sugerido

```text
test(modality): close conversational follow-up contract
```

---

# 20. Matrices de pruebas

## 20.1 M3C-B — Unitarias puras

Cubrir:

- todas las operaciones;
- todas las construcciones cerradas;
- dos señales iguales;
- señales incompatibles;
- `sin modalidad` ambiguo;
- status M2 permitido/bloqueado;
- targets derivados de include, exclude y ambos;
- unknown IDs;
- no-op;
- filtro resultante vacío;
- orden/deduplicación/disjunción;
- conmutatividad de sets;
- serialización del trace.

Property tests:

```text
apply(op, same input) es determinista
ADD_INCLUDE nunca deja target en exclude
RESTORE_INCLUDE nunca deja target en exclude
RESTORE_INCLUDE con previous.include vacío nunca crea una whitelist
RESTORE_INCLUDE con previous.include no vacío agrega el target a la whitelist
ADD_EXCLUDE nunca deja target en include
REMOVE_MEMBER elimina target de ambos lados
CLEAR siempre produce filtro vacío
resultado siempre es ModalityFilter válido
```

## 20.2 M3C-C — Integración

- M2 se invoca una sola vez.
- operación no entra en params.
- operación no entra en QueryFrame.
- operación no entra en fingerprint.
- generic merge no rechaza modalidad delegada.
- topic/scope/date/order sobreviven.
- change_dataset conserva IDs.
- CLEAR no borra otros filtros.
- no-op produce trace sin error.
- status bloqueado impide ejecución completa.
- consultas sin operación conservan paridad M3B-2.

## 20.3 M3C-D — Aclaración

- guardar sidecar;
- TTL 30 minutos;
- expiración;
- single-use;
- cancelación por reset/new search;
- reemplazo de pending;
- `subasta` → `inversa`;
- `subasta` → `inversa en Antioquia`;
- M2 una sola vez;
- spans mapeados al segmento actual;
- no contaminación del topic;
- base turn correcto;
- issue target IDs nunca ejecutados directamente;
- sidecar no aparece en QueryFrame/fingerprint.

## 20.4 E2E obligatoria

### Reemplazo

```text
T1: contratación directa de alimentación en Barranquilla
T2: ahora licitación pública
```

Esperado:

- alimentación heredada;
- Barranquilla heredada;
- contratación directa eliminada;
- licitación pública único include.

### Solo

```text
T1: include A, exclude B
T2: solo subasta inversa
```

Esperado:

- include subasta inversa;
- exclude vacío.

### Agregar

```text
T1: licitación pública de mantenimiento
T2: además contratación directa
```

Esperado:

- ambas modalidades;
- mantenimiento heredado.

### Excluir

```text
T1: alimentación en procesos
T2: excepto contratación directa
```

Esperado:

- include vacío;
- exclude contratación directa;
- topic heredado.

### Retirar miembro

```text
T1: licitación pública o contratación directa de alimentación
T2: quita contratación directa
```

Esperado: solo licitación pública.

### Retirar exclusión

```text
T1: alimentación excepto contratación directa
T2: permite contratación directa
```

Esperado:

- exclude vacío;
- no se fuerza include.

### Restaurar inclusión — modo whitelist

```text
T1: incluye licitación y excluye contratación directa
T2: vuelve a incluir contratación directa
```

Esperado:

- contratación directa sale de exclude;
- entra en include junto con licitación.

### Restaurar inclusión — modo abierto

```text
T1: alimentación excepto contratación directa
T2: vuelve a incluir contratación directa
```

Esperado:

- contratación directa sale de exclude;
- include permanece vacío;
- la consulta vuelve al universo abierto sin exclusiones de modalidad.

### Limpiar

```text
T1: búsqueda con modalidad
T2: sin filtro de modalidad
```

Esperado:

- include/exclude vacíos;
- topic/scope conservados.

### `sin modalidad`

Esperado: aclaración; no CLEAR automático.

### No-op

```text
T1: licitación pública
T2: quita mínima cuantía
```

Esperado:

- filtro igual;
- trace no-op;
- no error.

### Cambio de dataset

```text
T1: modalidad en procesos
T2: ahora contratos
```

Esperado: IDs sobreviven y se reproyectan.

### Target ambiguo

```text
T1: contexto válido
T2: además subasta
```

Esperado:

- no mutación parcial;
- no ejecución ampliada;
- aclaración.

---

# 21. Ataques adversariales

| Texto | Política esperada |
|---|---|
| `además alimentación` | no operación de modalidad; `alimentación` es topic |
| `sin alimentación` | exclusión temática, no modalidad |
| `solo alimentación` | control de topic, no REPLACE de modalidad sin target M2 |
| `quita estado abierto` | mutación de estado, no modalidad |
| `permite procesos cerrados` | estado/dataset, no REMOVE_EXCLUDE de modalidad |
| `además subasta o licitación` | target M2 ambiguo/parcial; bloquear |
| `quita licitación y agrega mínima cuantía` | control_conflict |
| `sin modalidad de contratación directa` | ambiguous_control; no CLEAR |
| `cualquier modalidad excepto directa` | construcción cerrada REPLACE solo-exclude |
| `no importa la modalidad, busca licitación pública` | construcción cerrada REPLACE |
| `además licitación pública o contratación directa` | ADD_INCLUDE con unión M2 resuelta |
| `deja de excluir subasta` | target ambiguo; bloquear |
| `elimina todas` | target_missing, no CLEAR |
| `solo esas` | anáfora sin referente canónico; aclarar |
| `quita la anterior` | no resolver por posición; aclarar |

Las palabras de control nunca convierten un objeto, estado o anáfora sin
referente en operación de modalidad.

---

# 22. Telemetría y dogfood

El trace debe permitir calcular:

```text
turnos con operación por tipo
operaciones aplicadas
no-op por tipo
control_conflict
ambiguous_control
target_missing
bloqueos por status M2
CLEAR aplicado/no-op
restauraciones
filtros que quedan vacíos
```

Los procedure IDs se permiten en logs técnicos locales. La telemetría externa
solo registra operación, status, códigos, no-op y conteos; nunca IDs, aliases ni
raws SECOP.

Corpus mínimo M3C-E:

- 20 REPLACE;
- 15 ADD_INCLUDE;
- 15 ADD_EXCLUDE;
- 10 REMOVE_MEMBER;
- 10 REMOVE_EXCLUDE;
- 10 RESTORE_INCLUDE;
- 10 CLEAR;
- 15 conflictos/ambigüedades de control;
- 20 composiciones con otros deltas;
- 15 cambios de dataset;
- 20 casos sin operación para paridad;
- ataques adversariales anteriores.

---

# 23. Presupuesto de complejidad

Objetivos:

```text
modality_followup.py <= 300 líneas de producción
un enum de operaciones
una dataclass principal de instrucción
una función pública de clasificación
una función pública de aplicación
cero aliases jurídicos duplicados
cero raws SECOP
cero nuevas claves ejecutables
cero condicionales de modalidad en SoQLBuilder
```

Una segunda dataclass frozen para la decisión se admite si evita tuplas o dicts
sin contrato.

Evitar:

- regex distribuidas entre módulos;
- un intent global por verbo;
- mutaciones directas en QueryFrame;
- sidecar dentro de params;
- persistir envelopes M2 enteros;
- modificar M1/M2/M3B-2;
- un solo commit gigante.

---

# 24. Riesgos y mitigaciones

| Riesgo | Impacto | Mitigación |
|---|---|---|
| control words entran al topic | LIKE contaminado | control spans enmascarados antes del merge genérico |
| merge genérico borra modalidad | filtro incorrecto | retirar delta de modalidad y aplicar motor al final |
| dos operaciones en una frase | acción arbitraria | construcciones cerradas + `control_conflict` |
| `sin modalidad` limpia indebidamente | pérdida de intención | `ambiguous_control` |
| `permite` confirma sin efecto | UX engañosa | separar REMOVE_EXCLUDE de RESTORE_INCLUDE |
| RESTORE_INCLUDE crea whitelist en modo abierto | estrecha accidentalmente la búsqueda | semántica condicional según `previous.include` |
| PARTIAL ejecuta miembro sano | búsqueda ampliada | gate estricto; candidate_filter nunca ejecutado |
| IDs inventados | error tardío o consulta falsa | conjunto de IDs conocidos inyectado al motor |
| cambio de dataset limpia modalidad | pérdida de filtro | conservar procedure IDs; proyectar después |
| metadata fuga a frame | fingerprints inestables | frame leak canaries |
| aclaración contamina topic | resultados irrelevantes | override contextual con mapeo de segmentos |
| pending obsoleto | respuesta aplicada al turno equivocado | base fingerprint + TTL + single-use |
| M3C-D sobredimensionado | costo sin uso | compuerta de producto antes de D |

---

# 25. Decisiones cerradas

Antes de implementar M3C-B no quedan decisiones semánticas abiertas:

1. **REPLACE implícito: SÍ.** Solo para un turno clasificado como follow-up, con
   target M2 ejecutable, sin control explícito, sin conflicto y sin `new_search`.
2. **RESTORE_INCLUDE:** condicional al modo previo. Con whitelist agrega el
   target; en universo abierto solo retira la exclusión.
3. **Compuerta M3C-D:** muestra mínima de 100 follow-ups reales; GO con al menos
   10 bloqueados o tasa ≥5 % y al menos 5 bloqueados.
4. **Trace técnico local:** procedure IDs permitidos.
5. **Telemetría externa:** solo operación, status, códigos y conteos.
6. **Referencia base:** `base_query_fingerprint` siempre; `base_turn_id` cuando
   exista; guardar ambos y validar coherencia.
7. **Sidecar:** usar `issue_target_ids`; nunca tratarlos como filtro ejecutable.

Cualquier cambio a estas decisiones requiere una nueva ronda de diseño, no una
interpretación del implementador.

---

# 26. Criterios GO / NO-GO

## M3C-B — GO

- enum exacto de siete operaciones;
- no hay aliases jurídicos;
- álgebra completa y property tests;
- `sin modalidad` ambiguo;
- construcciones cerradas antes de señales atómicas;
- unknown IDs bloqueados;
- módulo dentro del presupuesto o exceso justificado;
- cero wiring.

## M3C-B — NO-GO

- ranking numérico de operaciones;
- primera regex gana;
- candidate_filter ejecutado;
- operación escrita en params;
- RESTORE_INCLUDE ausente o sin distinción entre whitelist y modo abierto.

## M3C-C — GO

- M2 exactamente una vez;
- modalidad delegada fuera del merge genérico;
- topic/scope/date/order no se borran;
- operation metadata no fuga;
- paridad sin operación;
- E2E resueltos verdes;
- full suite verde.

## M3C-C — NO-GO

- condicionales jurídicos en FollowupEngine;
- generic merge sigue reemplazando modalidad en paralelo;
- CLEAR acepta `sin modalidad`;
- target bloqueado muta parcialmente.

## M3C-D — GO

- compuerta de producto satisfecha;
- sidecar TTL/single-use;
- QueryRouter override limitado y testeado;
- una sola llamada M2;
- spans del segmento actual correctos;
- pending nunca entra en params/fingerprint;
- ciclos completos E2E verdes.

## M3C-D — NO-GO

- no hay frecuencia real que justifique el bloque;
- se requiere ejecutar M2 dos veces;
- se concatena texto sin mapa de offsets;
- se persiste `ModalityResolution` completo;
- una aclaración puede aplicarse a otro turno base.

## M3C-E — cierre

- corpus multi-turn congelado;
- ataques adversariales verdes;
- telemetría documentada;
- full suite verde;
- cero regresiones M1/M2/M3B-2;
- closeout y recomendación del bloque siguiente.

---

# 27. Salida contractual de M3C-A

```text
MODEL_USED: GPT-5.6 Thinking
BRANCH: search/modality-contract-v1 (expected, local precheck pending)
HEAD: f94ffbb2f0ba65e6333efc1cb6eb940260ea6b67 (expected)
PRECHECK: NOT_RUN_IN_LOCAL_WORKTREE

CURRENT_PIPELINE_MAP: DOCUMENTED
CURRENT_MODALITY_FOLLOWUP_BEHAVIOR: ATOMIC_GENERIC_REPLACEMENT_ONLY
CURRENT_CLARIFICATION_PERSISTENCE: NONE_FOR_MODALITY_ENVELOPE
DUPLICATED_AUTHORITY_FOUND: NO

RECOMMENDED_OPERATION_MODEL: CLOSED_CONSTRUCTIONS_THEN_SINGLE_OPERATION
OPERATION_ENUM: REPLACE, ADD_INCLUDE, ADD_EXCLUDE, REMOVE_MEMBER,
                REMOVE_EXCLUDE, RESTORE_INCLUDE, CLEAR
CONTROL_CLASSIFIER_LOCATION: app/core/modality_followup.py
MUTATION_ENGINE_LOCATION: app/core/modality_followup.py
EPHEMERAL_TRANSPORT: Burr state field modality_followup_instruction
PARAMS_ADDED: NONE

REPLACE_SEMANTICS: final=current
ADD_INCLUDE_SEMANTICS: previous.include ∪ targets; previous.exclude - targets
ADD_EXCLUDE_SEMANTICS: previous.exclude ∪ targets; previous.include - targets
REMOVE_MEMBER_SEMANTICS: remove targets from include and exclude
REMOVE_EXCLUDE_SEMANTICS: remove targets only from exclude
RESTORE_INCLUDE_SEMANTICS: always remove targets from exclude; add to include
                           only when previous.include is non-empty
CLEAR_SEMANTICS: empty ModalityFilter, explicit phrases only
NO_OP_SEMANTICS: success, unchanged filter, trace no_op=true

OPERATION_PRECEDENCE: NO_NUMERIC_PRECEDENCE
CONTROL_CONFLICT_POLICY: BLOCK_WITH_CONTROL_CONFLICT
M2_STATUS_GATE: RESOLVED / RESOLVED_WITH_ASSUMPTION ONLY FOR TARGETED OPS
PARTIAL_POLICY: BLOCK; DO_NOT_EXECUTE_CANDIDATE_FILTER
AMBIGUOUS_POLICY: BLOCK; ASK_CONCRETE_QUESTION
DATASET_SWITCH_POLICY: PRESERVE_PROCEDURE_IDS; PROJECT_LATER

FOLLOWUP_INTENT_STRATEGY: KEEP_REFINE_FILTER
FOLLOWUP_MUTATION_TRACE: LOCAL_TRACE_MAY_INCLUDE_PROCEDURE_IDS;
                         EXTERNAL_TELEMETRY_COUNTS_ONLY
QUERYFRAME_PERSISTENCE: FINAL_INCLUDE_EXCLUDE_ONLY
FINGERPRINT_IMPACT: FINAL_FILTER_ONLY
BUILDER_IMPACT: NONE

CLARIFICATION_STATE_MODEL: PendingModalityClarification with issue_target_ids,
                           base_query_fingerprint always, base_turn_id when available
CLARIFICATION_STORAGE_LOCATION: ConversationStore sidecar
CLARIFICATION_TTL: 30_MINUTES_SINGLE_USE
CLARIFICATION_CANCEL_POLICY: SUCCESS, EXPIRY, RESET, EXPLICIT_NEW_SEARCH,
                             INCOMPATIBLE_NEW_INSTRUCTION
CLARIFICATION_RESOLUTION_FLOW: CONTEXT_PREFIX_OVERRIDE, ONE_M2_CALL,
                               SEGMENT_OFFSET_MAPPING

M3C_B_FILES: modality_followup.py + test_modality_followup.py
M3C_C_FILES: parsing.py + integration/E2E/canary tests; other files by evidence
M3C_D_FILES: pending sidecar, ConversationStore, QueryRouter/parser integration,
             workflow lifecycle and tests
M3C_E_FILES: dialogue corpus, invariants, telemetry, closeout

COMPLEXITY_BUDGET: DEFINED
RISKS: DOCUMENTED
OPEN_DECISIONS: 0
GO_NO_GO: GO_FOR_M3C_B; M3C_D_REQUIRES_100_TURN_PRODUCT_GATE

FILES_CREATED:
- docs/planes/2026-07-23_M3C_modality_conversation_contract.md

FILES_MODIFIED: NONE
UNEXPECTED_FILES: NONE
PRODUCTION_CHANGED: NO
TESTS_CHANGED: NO
COMMIT_MERGE_PUSH: NO

FINAL_VERDICT:
M3C_DESIGN_FINAL_APPROVED
```

---

# 28. Control de revisión final

Enmiendas incorporadas:

```text
RESTORE_INCLUDE_OPEN_MODE: CORREGIDO
IMPLICIT_REPLACE: CERRADO_EN_SÍ_CON_GUARDS
M3C_D_GATE: 100_TURNS; >=10_BLOCKED OR >=5_PERCENT_AND_>=5_BLOCKED
LOCAL_TRACE_IDS: PERMITIDOS
EXTERNAL_TELEMETRY_IDS: PROHIBIDOS
BASE_QUERY_FINGERPRINT: OBLIGATORIO
BASE_TURN_ID: CUANDO_EXISTA
SIDECAR_TARGET_FIELD: issue_target_ids
OPEN_DECISIONS: 0
```

**FINAL_VERDICT:** `M3C_DESIGN_FINAL_APPROVED`
**NEXT_BLOCK:** `M3C-B_ONLY`

---

# 29. Orden de detención

Detenerse después de crear y revisar este documento.

No implementar M3C-B.
No modificar producción.
No modificar tests.
No crear commit.

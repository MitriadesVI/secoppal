# ADR-095 — Entity Profile Semantic Plan V1

**Fecha:** 2026-09-07. **Estado:** implementado en rama, sin merge ni push.
**Base:** `main` 7868664. **Rama:** `fix/entity-profile-semantic-plan-v1`.
**Informe y evidencia:** [`ENTITY_PROFILE_SEMANTIC_PLAN_V1.md`](../audits/ENTITY_PROFILE_SEMANTIC_PLAN_V1.md).

## Contexto

`"analizame que contratos firmo alcaldia de baranoa en agosto"` devolvía
0 procesos, 0 contratos y $0. El contraste en vivo el mismo día daba **47**
contratos firmados. El cero era del filtro, no de la fuente.

Tres defectos independientes lo producían, y ninguno era específico de esa
consulta: la acción contractual sobrevivía como tema y se proyectaba como
`LIKE '%firmo%'`; el overview consultaba seis fuentes fijas aunque la persona
hubiera nombrado una; y el evento temporal se confundía con el estado actual, lo
que sobre Baranoa reducía el universo de procesos de agosto de 48 a 2.

La auditoría encontró además una fuga bidireccional de predicados entre esquemas
—`estado_del_procedimiento` viajando a contratos y `estado_contrato` a
procesos— que nadie detectaba porque el transporte podía responder igual.

## Decisión

Entity Profile deja de operar sobre un diccionario de params compartido y pasa a
ejecutar **un plan semántico validado**:

```text
autoridades existentes
  → EntityProfileSemanticRequest
  → EntityProfileExecutionPlan
      ├─ ProcessSourcePlan
      └─ ContractSourcePlan
  → evidencia validada por métrica
  → EffectiveScopeSnapshot
  → render
```

La capa nueva **integra** autoridades y no vuelve a resolver texto, salvo las
dos decisiones que hoy no tenían dueño y que este ADR adjudica:

1. **`requested_sources`** — qué fuente nombró la persona. No es
   `params["dataset"]`: `_select_dataset` responde otra pregunta con otra forma,
   y se demostró que devuelve `contratos` para una consulta que dice `procesos`.
   `dataset_explicit` queda como señal auxiliar, nunca como autoridad. La
   ausencia de palabra de fuente significa que la persona no eligió: BOTH.
2. **`event`** — a qué hecho ancla el periodo. Sólo existe cuando hay periodo.
   Un hecho sin fecha productiva acreditada ("terminados en agosto") se nombra
   como no soportado y pide aclaración con cero llamadas, en vez de contestarse
   con la fecha de firma en silencio.

Reclamar un tramo como evento impide que ese mismo tramo filtre el estado
actual, por `INV-STRUCTURED-DIMENSION-NOT-TOPIC` extendida al estado. La
comprobación relee `estado_families` con el tramo enmascarado, de modo que un
estado nombrado con otra palabra sobrevive.

`effective_sources = requested ∩ capacidad(foco)`. El foco nunca reescribe la
fuente solicitada; si la intersección es vacía se pregunta y no se amplía.

Conteo y valor son métricas independientes del mismo agregado. Una fuente no
solicitada no es cero, no falló y no vuelve el perfil `partial`.

`INV-RENDERED-SCOPE-EQUALS-EXECUTED-SCOPE`: el bloque "qué analizamos" se deriva
del plan ejecutado y es su única fuente.

## Alternativas descartadas

- **Corregir el estado que activa `publicados` en `estado_families`.** Es la
  causa raíz global, pero cambiaría la semántica de `opportunity_search` para
  las consultas de oferente. Condición de STOP: *requiere reescribir QueryRouter
  de forma amplia*. Queda como `PUBLICATION_EVENT_VS_STATE_GLOBAL_DEBT`.
- **Corregir `_select_dataset`.** Misma razón; queda como
  `SCALAR_DATASET_SIGNAL_SOUP_DEBT`.
- **Cambiar la semántica global de fechas a `[inicio, siguiente)`.** Se auditó
  en vivo: ambas fechas se almacenan a `T00:00:00.000` y la diferencia entre
  `<=` y `<` es 0 sobre miles de filas. Sin evidencia, no se cambia.
- **Soportar `values` sobre procesos** presentando `precio_base`. El contrato de
  producto prohíbe presentar el presupuesto de un proceso como valor contratado.

## Consecuencias

Dos expectativas acreditadas se adjudican deliberadamente: una métrica ausente
deja de invalidar a su hermana, y el encabezado del perfil deja de ser
"comprador — periodo".

`jobs_for_focus` deja de ser autoridad de ejecución y pasa a ser el catálogo de
aspectos por foco. `QueryJob` transporta sus propios params ya proyectados, lo
que hace estructuralmente imposible volver a pasar un mismo dict a los dos
esquemas.

`firmar` entra en `PROCUREMENT_ACTION_VERB_FORMS` (commit global separado, con
corpus diferencial de 16 consultas: cambian sólo las dos objetivo). Quedan fuera
`firma` y los participios, por colisión con el sustantivo.

## Validación

Suite completa **12310 passed, 11 skipped, 16 xfailed, 0 failed**. Freeze del
incidente 22/22. Corpus sistémico 74 casos × 4 ejes. Propiedades P1–P12 verdes.
Ruff verde, `lint-core` limpio, Semgrep 10 → 10 en `app/` con delta 0.

Dogfood en vivo: la consulta del incidente pasa de `0 contratos · $0` a
`47 contratos · $911.5 millones`, con cero llamadas al dataset de procesos y sin
`firmo` en ningún predicado.

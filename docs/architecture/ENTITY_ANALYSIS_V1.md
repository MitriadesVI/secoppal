# Entity Analysis V1

**Fecha:** 2026-09-05. **Estado del núcleo:** implementado.
**Scope productivo:** `BUYER_UNIT`; `BUYER_GROUP` acotado a la capacidad declarada.
**Fuentes:** SECOP II Procesos `p6dx-8zbt` y SECOP II Contratos
`jbjy-vk9h`. **Warehouse / graph DB / LLM de cálculo:** no usados.

**Decisión territorial:**
[ADR-081](../decisions/2026-09-05_ADR-081_entity-analysis-territorial-metadata-consumer.md).

## Contrato de producto

Entity Analysis V1 responde una radiografía institucional breve. Un overview
ensambla como máximo ocho hechos y conserva denominadores distintos para
procesos y contratos. Cada cifra declara su dataset; un fallo de una fuente no
se transforma en cero ni elimina los demás aspectos.

La feature acepta exclusivamente un alcance que las autoridades productivas ya
resolvieron:

- `BUYER_UNIT`: una unidad compradora canónica de alta confianza;
- `BUYER_GROUP`: una referencia versionada de `buyer_groups.json`, cuya
  proyección existente no exceda 40 nombres físicos;
- `Territory`: nunca es alcance de Entity Analysis V1.

Por eso `analiza Baranoa` pide escoger entre la Alcaldía y el territorio
institucional, mientras `analiza la Alcaldía de Baranoa` sí construye el
perfil. El detector no convierte geografía en comprador y no captura
`analiza contratos de alimentación` cuando falta una entidad productiva.

## Precedencia y conversación

El enganche mínimo vive en la acción de ejecución existente, con este orden:

1. analyzer puntual ya acreditado;
2. overview o profundización de Entity Analysis;
3. Actor Profile;
4. consulta tabular normal.

Esto conserva, por ejemplo, la ruta puntual de `cuánto suma lo contratado`.
Dentro de un perfil activo, las preguntas completas de proveedores (incluidas
`quién tiene más contratos` y `quiénes contratan más con esa entidad`) se
interpretan como foco suppliers antes del ranking genérico de entidades.
Esta desambiguación contextual no captura suma, promedio ni contenido nuevo.
Entity Analysis sólo reconoce `analiza`, `analízame`, `cómo contrata`, `cómo ha
contratado`, `qué sabes de`, `perfil de` y `radiografía de`, y exige scope de
buyer. El marcador conversacional se guarda en los parámetros resueltos ya
existentes: proveedor, modalidad, tiempo y recientes pueden reutilizar buyer y
periodo. No se creó un segundo motor conversacional ni se modificaron
`query_plan.py` o `query_router.py`.

Una solicitud explícita que nombra el comprador, como `analízame la Alcaldía
de Galapa`, inicia `new_search`: no hereda al actor, tema o filtros de una
búsqueda anterior. Las preguntas de proveedores reutilizan buyer, periodo y
tema mediante `refine_filter`, tanto si el dataset persistido era procesos
como si era contratos. `_entity_profile_focus` conserva el foco actual;
`y por valor` después de suppliers vuelve a mostrar ambos rankings separados.
Los dos marcadores del perfil se retiran al abandonar esa ruta.

## Arquitectura

`app/core/entity_profile/` separa responsabilidades pequeñas:

| Módulo | Responsabilidad |
|---|---|
| `models.py` | scope, identidad, secciones, hechos y telemetría tipados |
| `scope.py` | aceptar BuyerUnit/BuyerGroup productivo y anexar metadata territorial |
| `turn.py` | intención, foco, aclaración Territory-vs-Buyer y follow-up seguro |
| `followup.py` | gramática acotada de preguntas contextuales de proveedores |
| `source_request.py` | qué FUENTE nombró la persona, y con qué procedencia |
| `temporal_event.py` | a qué HECHO ancla su periodo, y si ese hecho tiene fecha |
| `semantic_request.py` | la petición completa, JSON-safe, integrando autoridades |
| `source_plan.py` | proyectar la petición a params válidos para CADA esquema |
| `execution_plan.py` | fuentes efectivas, jobs, secciones y aclaración, en una pasada |
| `effective_scope.py` | lo EJECUTADO, en la forma en que el render puede decirlo |
| `query_runner.py` | presupuesto, concurrencia acotada y deadline |
| `aspects/` | construir y leer agregaciones por aspecto |
| `sections.py` | estado `SUPPORTED/PARTIAL/UNAVAILABLE` independiente |
| `evidence.py` | rechazar agregados inválidos o contradictorios para construir hechos |
| `assembler.py` | ensamblar el perfil sin cálculos de negocio propios |
| `render.py` | respuesta breve y etiquetada por fuente |

El resultado conserva referencias graph-ready: `institution_id`, BuyerUnit
canónica, DIVIPOLA, identidad documental tipada del proveedor, `id_del_proceso`
e `id_contrato`. Procurement Relationship Foundation V0 ya existe en main
([ADR-092](../decisions/2026-09-06_ADR-092_procurement-relationship-foundation-v0.md)).
Ambas capas comparten identidades conceptualmente compatibles, manteniendo
distintas BuyerUnit, territorio, cuenta SECOP y Actor. Una futura fase de
Competition/Relationships podrá usar ese contrato con sus gates propios.

Entity Analysis V1 no importa `app/core/procurement_relationships.py` ni
`scripts/procurement_layers/**`, no carga snapshots, consulta el graph offline,
hace traversal ni crea edges runtime. La Foundation es un contrato conceptual
futuro y no el motor del overview actual.

```text
PROCUREMENT_LAYERS_RUNTIME_DEPENDENCY = NONE
GRAPH_DATABASE = NONE
```

## Plan semántico V1 → V1.1 (ADR-095 y su enmienda)

> El cierre V1 fue refutado por revisión independiente. Lo que sigue describe el
> estado **V1.1**, con los siete P1 cerrados:
> [INDEPENDENT_REVIEW_REMEDIATION_V1_1](../audits/entity_profile_semantic_v1_1/INDEPENDENT_REVIEW_REMEDIATION_V1_1.md).
>
> Cambios de contrato respecto de V1: la ejecución y la presentación del perfil
> son **terminales** en el workflow; los hechos temporales son reclamaciones
> **por ocurrencia** y no un escalar; la fuente se lee del **núcleo de cada
> cláusula**; todo filtro material del WHERE se **declara** en el alcance
> (`INV-FILTER-IN-SQL-IMPLIES-FILTER-DECLARED`); una fuente **nombrada** se
> contesta entera o se pregunta (`EXPLICIT REQUEST COMPLETENESS`); y la
> conversación separa petición **confirmada** de **pendiente**.

El perfil no opera sobre un diccionario de params compartido: ejecuta **un plan
semántico validado**.

```text
autoridades existentes
  → EntityProfileSemanticRequest      (qué pidió la persona)
  → EntityProfileExecutionPlan        (qué se ejecuta, y qué se pregunta)
      ├─ ProcessSourcePlan
      └─ ContractSourcePlan
  → evidencia validada POR MÉTRICA
  → EffectiveScopeSnapshot            (qué se ejecutó)
  → render
```

La capa **integra** autoridades y no vuelve a resolver texto, salvo las dos
decisiones que no tenían dueño: **fuente solicitada** y **evento temporal**.

`params["dataset"]` no es autoridad de fuente. `_select_dataset` responde otra
pregunta —qué dataset escalar ejecuta la ruta tabular— y decide por sopa de
señales sin orden, así que `"analiza procesos de X con proveedores"` produce
`dataset=contratos, dataset_explicit=True`. La fuente se lee de las palabras de
la persona, con el nombre del comprador enmascarado; su ausencia significa que
no eligió, y una radiografía sin fuente elegida cubre las dos.

### Fuente solicitada ≠ fuente efectiva

`effective = requested ∩ capacidad(foco)`. El foco **nunca** reescribe la
solicitada; si la intersección es vacía se pregunta y se envían cero llamadas.

| foco | capacidad |
|---|---|
| overview, recent, temporal | ambas |
| suppliers, values | contratos |
| modalities | procesos |

`values` sobre procesos y `modalities` sobre contratos se adjudican como
aclaración: soportarlos exige capacidades acreditadas, no reinterpretar campos.

### Evento temporal ≠ estado actual

Un evento sólo existe cuando hay un periodo que anclar. Reclamar un tramo como
evento impide que ese MISMO tramo filtre el estado actual
(`INV-STRUCTURED-DIMENSION-NOT-TOPIC` extendida al estado); la comprobación
relee `estado_families` con el tramo enmascarado, así que
`"publicados en agosto y todavía abiertos"` conserva ambas dimensiones.

Un hecho sin fecha productiva acreditada —`terminados`, `liquidados`,
`iniciados`, `adjudicados`, `cedidos`— NO se traduce a firma ni a publicación:
pide aclaración con cero llamadas.

### Proyección segura por fuente

| | procesos | contratos |
|---|---|---|
| ancla temporal | `fecha_de_publicacion_del` | `fecha_de_firma` |
| campo de valor | `precio_base` | `valor_del_contrato` |
| estado prohibido | `estado_contrato*` | `estado_del_procedimiento`, `estado_de_apertura_del_proceso` |

`QueryJob` transporta sus propios params ya proyectados: es lo que hace
estructuralmente imposible volver a pasar un mismo dict a los dos esquemas.

### Verdad por métrica

`count(*)` no depende de la columna de valor. Un valor ausente deja esa métrica
no disponible y **no** invalida el conteo. Siguen siendo inservibles el agregado
malformado y el cero desmentido por su propia suma. Una fuente no solicitada no
es cero, no falló y no vuelve el perfil `partial`.

### Alcance renderizado == alcance ejecutado

`INV-RENDERED-SCOPE-EQUALS-EXECUTED-SCOPE`. El bloque "qué analizamos" se deriva
del plan ejecutado y es su única fuente. Un mes no se reduce a su año; un perfil
de contratos no menciona procesos ni ofrece profundizar en ellos.

Evidencia, corpus, dogfood y deuda declarada:
[`ENTITY_PROFILE_SEMANTIC_PLAN_V1.md`](../audits/ENTITY_PROFILE_SEMANTIC_PLAN_V1.md).

## Aspectos y denominadores

| Aspecto | Fuente | Semántica V1 |
|---|---|---|
| Actividad | ambas | `process_count` y `contract_count`, separados |
| Valores | contratos | suma y promedio contractuales; mediana `UNAVAILABLE_V1` |
| Modalidades | procesos | conteo y share; orden conteo desc/nombre asc |
| Proveedores | contratos | top por cantidad y top por valor, en listas distintas |
| Temporal | ambas | procesos, contratos y valor por año |
| Recientes | ambas | máximo tres por dataset; fecha desc/ID estable asc |

Todos los conteos y agregados se calculan server-side. Las consultas recientes
son bounded. Los proveedores se agrupan por documento y tipo documental; el
nombre publicado es etiqueta observada, no llave cuando existe documento.
Procesos y contratos nunca se equiparan 1:1. El presupuesto/base de un proceso
no se presenta como valor contratado.

## Presupuesto y honestidad parcial

El overview con ambas fuentes tiene un plan de seis llamadas:

1. actividad + modalidades de procesos;
2. actividad + valores de contratos;
3. proveedores por cantidad;
4. proveedores por valor;
5. procesos recientes;
6. contratos recientes.

Cuando la persona nombra UNA fuente, el plan ejecuta sólo las llamadas de esa
fuente y las demás secciones quedan `NOT_REQUESTED` —que no es cero ni fallo.
Los focos individuales usan entre una y dos llamadas. Antes de enviar una
llamada se comprueba el deadline total del turno; sin un segundo útil queda
registrada como `BUDGET_EXHAUSTED` y no se intenta. Cada llamada registra
dataset, aspecto, operación, estado, filas, latencia y SoQL; el perfil agrega
`calls_total`, llamadas y latencia por aspecto, y latencia total.

`TIMEOUT`, `ERROR`, `BUILD_ERROR` y `BUDGET_EXHAUSTED` nunca significan vacío.
Cada sección se degrada por separado y el render dice qué no pudo calcular. La
mediana no se simula: queda explícitamente no disponible en V1.

El scope mostrado debe coincidir con los parámetros ejecutables: Actor o
predicados alternativos de comprador bloquean el perfil antes de consultar.
Un agregado contractual vacío o malformado no confirma cero. Un conteo cero
contradicho por filas de otra llamada exitosa de contratos invalida los hechos
de actividad/valor contractual y explica la contradicción, conservando el
resultado original de transporte en SourceCalls.

## Territorial Knowledge: frontera del primer consumidor

Entity Analysis importa el loader de Territorial Knowledge sólo después de que
la resolución productiva haya entregado una BuyerUnit canónica. Hace un lookup
exacto por `product_buyer_unit` y acepta únicamente instituciones
`PRODUCT_RECOGNIZED` para anexar DIVIPOLA, territorio, familia institucional,
`institution_id` y procedencia.

El registro no resuelve texto, no crea buyer/alias/binding, no promueve
candidatos, no decide BuyerGroup y no modifica filtros SoQL. Los estados
`VERIFIED_LOGICAL_UNRECOGNIZED` y `ADJUDICATED_OFFLINE_CANDIDATE` no participan
en scope productivo.

## Limitaciones conocidas

- Territorial Analysis completo queda fuera de V1.
- La mediana contractual se difiere porque no existe una estrategia agregada
  acreditada y barata en este camino.
- El subset conversacional no introduce cambios en QueryPlan; operaciones más
  complejas siguen el sistema de follow-up existente o requieren una fase
  posterior.
- Para `entity_profile`, BuyerGroups conserva `group_id`, sus 38 miembros y
  scope estructurado, sin anteponer la declaración textual completa. Todas las
  demás rutas conservan `group_declaration()`.
- El detector AST compartido de `fac51c0` sigue enumerando todos los imports
  reales. `scope.py` es el único consumidor autorizado por ADR-081. El gate
  histórico `RUNTIME_IMPORTS = 0` cuenta consumidores **no autorizados**; un
  segundo consumidor falla cerrado, incluido Procurement Relationships. Sus
  strings de provenance no son imports. El JSON V0 no se modifica ni repinea;
  su estado histórico `SHADOW_NO_RUNTIME` se interpreta junto a ADR-081.

## Contratos de prueba

El corpus contiene 41 turnos simples y 20 conversaciones multiturno: conserva
los 40/15 originales y añade las regresiones beta de Galapa. Cubre Baranoa,
Galapa, Barranquilla, Bucaramanga, Yumbo, Gobernación del
Atlántico y Bogotá; overview, tiempo, modalidades, proveedores, valores,
recientes, Territory-vs-Buyer, BuyerGroup, fallo parcial y follow-up.

Los dogfoods y contrastes live se registran en
[`ENTITY_ANALYSIS_V1_DOGFOOD.md`](../audits/entity_analysis/ENTITY_ANALYSIS_V1_DOGFOOD.md).
El diagnóstico causal y la verificación beta posterior están en
[`BETA_ENTITY_ACTOR_HARDENING.md`](../audits/beta_entity_actor_v1/BETA_ENTITY_ACTOR_HARDENING.md).

## SUPPLIER_PLACEHOLDER_GROUPING_AND_TOPN_COVERAGE_V1 — 2026-09-06

Un documento placeholder no es un actor. `actor_identity` conserva la autoridad
cerrada existente y expone `actor_document_sentinels()` e
`is_actor_document_sentinel()` sin cambiar la normalización. Su proyección en
`actor_identity_soql` excluye sentinelas del documento mediante
`upper(trim(documento_proveedor)) NOT IN (...)` **antes de GROUP BY y LIMIT**.
El tipo documental no decide si un documento placeholder es atribuible: también
se excluye cuando la fuente lo publica con tipo NIT u Otro.

Ambos rankings mantienen `GROUP BY documento_proveedor, tipodocproveedor`,
`LIMIT 5`, desempates y métricas raw de las identidades válidas. No hay fallback
por nombre, hash, fuzzy ni inferencia de UT. `max(proveedor_adjudicado)` sólo
rotula grupos de documentos elegibles; no define la identidad. La validación
tipada fina sigue en `normalize_identity()`.

El foco suppliers mantiene dos llamadas, 8 s por llamada y 9 s globales; overview
mantiene seis llamadas. El dogfood conversacional de Barranquilla y Bucaramanga
2026 pasó de cuatro a cinco proveedores en ambos rankings; Galapa histórico
pasó de cuatro a cinco por cantidad y conservó cinco por valor. Se conservan los
timeouts iniciales de Galapa y los límites del audit, sin interpretarlos como
vacío. Los nombres territoriales solos siguen pidiendo aclaración institucional.

La cobertura `attributable_rows / unattributable_rows` se difiere: dos rankings
TOP5 no contienen denominadores completos y calcularlos exigiría otra consulta.
No se garantiza completar cinco ante documentos no-sentinel rechazados por otras
reglas del parser, ni se introduce refill. SoQL TRIM cubre espacios exteriores;
las demás variantes de whitespace siguen protegidas por el parser, sin afirmar
paridad total con `str.strip()`. No se amplía la autoridad por intuición.

Evidencia, SQL, filas raw, métricas parseadas, regresiones y puertas:
[`SUPPLIER_PLACEHOLDER_TOPN_V1.md`](../audits/entity_analysis/supplier_placeholder_topn_v1/SUPPLIER_PLACEHOLDER_TOPN_V1.md).

Cierre de validación: 334 focales verdes; Ruff verde; Semgrep 10 → 10, delta 0.
La suite completa terminó con 11.571 passed y un fallo nuevo en el replay de
Procurement Relationships: su registro congelado compara el hash literal de
`actor_identity.py`. El test pasa en la base; la API extraída cambia sólo ese
digest en el registro regenerado y el resto de artefactos coincide byte a byte.
No se repinea ni cambia ese carril. **Entrega PARTIAL**, pendiente de resolver
el contrato de replay antes de integración.

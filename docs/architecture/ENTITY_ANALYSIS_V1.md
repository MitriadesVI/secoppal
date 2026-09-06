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
| `query_runner.py` | plan fijo, presupuesto, concurrencia acotada y deadline |
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

El overview tiene un plan invariable de seis llamadas:

1. actividad + modalidades de procesos;
2. actividad + valores de contratos;
3. proveedores por cantidad;
4. proveedores por valor;
5. procesos recientes;
6. contratos recientes.

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

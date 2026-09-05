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
Entity Analysis sólo reconoce `analiza`, `analízame`, `cómo contrata`, `cómo ha
contratado`, `qué sabes de`, `perfil de` y `radiografía de`, y exige scope de
buyer. El marcador conversacional se guarda en los parámetros resueltos ya
existentes: proveedor, modalidad, tiempo y recientes pueden reutilizar buyer y
periodo. No se creó un segundo motor conversacional ni se modificaron
`query_plan.py` o `query_router.py`.

## Arquitectura

`app/core/entity_profile/` separa responsabilidades pequeñas:

| Módulo | Responsabilidad |
|---|---|
| `models.py` | scope, identidad, secciones, hechos y telemetría tipados |
| `scope.py` | aceptar BuyerUnit/BuyerGroup productivo y anexar metadata territorial |
| `turn.py` | intención, foco, aclaración Territory-vs-Buyer y follow-up seguro |
| `query_runner.py` | plan fijo, presupuesto, concurrencia acotada y deadline |
| `aspects/` | construir y leer agregaciones por aspecto |
| `sections.py` | estado `SUPPORTED/PARTIAL/UNAVAILABLE` independiente |
| `assembler.py` | ensamblar el perfil sin cálculos de negocio propios |
| `render.py` | respuesta breve y etiquetada por fuente |

El resultado conserva referencias graph-ready sin implementar un grafo:
`institution_id`, BuyerUnit canónica, DIVIPOLA, identidad documental tipada del
proveedor, `id_del_proceso` e `id_contrato`. Son suficientes para modelar en el
futuro `IN_TERRITORY`, `PUBLISHED` y `CONTRACTED_WITH`, pero V1 no añade
traversal, schema general ni base de grafos.

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
- El postprocesador compartido de BuyerGroups antepone hoy la lista completa de
  miembros. En Bogotá son 38 y rompe la brevedad de producto aunque el perfil y
  sus seis llamadas sean correctos. El carril no es dueño de
  `app/core/orchestrator.py`; por eso BuyerGroup se entrega `PARTIAL` hasta que
  el orquestador adjudique una presentación compacta.
- Dos canarios históricos de Territorial Knowledge exigen literalmente cero
  imports runtime: el inventario de consumidores y el gate del replay
  byte-exacto. ADR-081 sustituye esa premisa por un único consumidor acotado,
  pero el test y el builder históricos están fuera de la propiedad de este
  carril y requieren adjudicación del orquestador; el artefacto no se repinó.

## Contratos de prueba

El corpus congelado contiene exactamente 40 turnos simples y 15 conversaciones
multiturno. Cubre Baranoa, Barranquilla, Bucaramanga, Yumbo, Gobernación del
Atlántico y Bogotá; overview, tiempo, modalidades, proveedores, valores,
recientes, Territory-vs-Buyer, BuyerGroup, fallo parcial y follow-up.

Los dogfoods y contrastes live se registran en
[`ENTITY_ANALYSIS_V1_DOGFOOD.md`](../audits/entity_analysis/ENTITY_ANALYSIS_V1_DOGFOOD.md).

# Guía v1.4 — Nodos semánticos, Market Packs, warehouse y candidate pools asistidos en SECOPPAL

**Estado:** guía operativa v1.4 — final posterior al freeze de Adulto Mayor y puente a warehouse  
**Ámbito:** SECOPPAL Search, Warehouse, Domain Nodes, Market Packs, Advisor, Radar y dogfood runtime.  
**Uso recomendado:** documento base para diseñar, validar, integrar y retirar nodos semánticos/Market Packs sin degradar la búsqueda clásica.  
**Tesis central:** los nodos no reemplazan Classic Search ni Socrata. Los Market Packs estructuran el mercado, verifican qué se contrata, explican resultados y sirven como benchmark para el warehouse local. Cuando un Market Pack ya demostró valor semántico, debe congelarse como baseline y dejar de tunearse por inercia; el siguiente avance debe atacar el cuello real: recuperación, latencia, frescura o datos.  
**Principio rector:** ningún nodo entra al runtime porque “parece inteligente”; entra porque, comparado contra Classic Search y contra un benchmark frío, mejora una tarea real o permite navegar el mercado de forma útil y verificable. Primero anota, luego explica, después rescata candidate pools en shadow; si el bloqueo deja de ser semántico, se congela el nodo y se mueve la inversión a warehouse, frescura, latency o fuente de datos.

---

## 0. Resumen ejecutivo

SECOPPAL ya tiene una búsqueda clásica valiosa: parser, filtros, variantes, normalización, BM25/tier ranking, datasets SECOP y política de respuesta. Los nodos no deben destruir eso.

El aprendizaje acumulado de Adulto Mayor, W0S.4–W0S.10 y Fase 2C es:

```text
Classic Search encuentra candidatos cuando la consulta es directa o literal.
Market Packs entienden estructura contractual y convierten texto en mercado navegable.
Hybrid Search combina ambas: recupera con lo clásico, segmenta/filtra/reordena/explica con nodos.
Node-assisted candidate pool rescata queries composicionales cuando Classic falla por AND rígido, vocabulario literal o candidate pool irrelevante.
```

La mejora real no es “más palabras”. Es pasar de:

```text
aparece la palabra construcción/interventoría/mejoramiento
```

a:

```text
qué se está contratando realmente:
- ejecución física
- interventoría
- estudios y diseños
- suministro
- apoyo profesional
- mantenimiento
- operación de servicio
```

El benchmark A/B/C de Fase 2C dejó una señal fuerte:

| Tarea | Classic Search | Nodos / Hybrid | Lectura |
|---|---:|---:|---|
| Ejecución física real | ~25% precisión | ~94% precisión | Los nodos eliminan ruido de menciones no contractuales. |
| Interventorías | ~43% precisión | ~96–100% precisión | Separan función contractual. |
| Mantenimiento de malla vial urbana | ~38% precisión | ~100% precisión | Útil cuando se cruzan función + acción + activo. |
| Pavimento literal | ~93% precisión | ~93% precisión | Cuando lo literal ya funciona, los nodos solo empatan. |

Conclusión de arquitectura:

```text
No reemplazar Classic Search.
Activar Hybrid Search como modo experimental/dogfood.
Usar nodos primero para segmentar, explicar, reordenar y crear navegación; no para ocultar resultados de forma irreversible.
Solo explorar node-assisted candidate pool después de probar el Market Pack como anotador live, con guards negativos, query_signals separados de row_signals y qualifier_guard obligatorio.
```

---

## 1. Vocabulario base

### 1.1. Classic Search

La búsqueda actual de SECOPPAL:

- parser conversacional;
- extracción de filtros;
- variantes morfológicas/curadas;
- normalización de texto;
- ranking BM25/tier/frase;
- recuperación SECOP/Socrata;
- respuesta Advisor actual.

Es fuerte para:

- referencias exactas;
- nombres de entidades/procesos;
- términos explícitos;
- consultas específicas;
- palabras literales que ya discriminan bien, como `pavimento` en el benchmark.

### 1.2. Nodal Search

Búsqueda, filtro o segmentación por conceptos canónicos:

```json
{
  "market": "infraestructura_vial",
  "contract_function": "interventoria",
  "primary_action": "mantenimiento",
  "asset_or_component": "malla_vial_urbana"
}
```

Sirve para:

- distinguir función contractual;
- agrupar resultados;
- filtrar ruido semántico;
- navegar mercados;
- explicar por qué algo entra;
- alimentar Radar.

### 1.3. Hybrid Search

Diseño recomendado para runtime dogfood:

```text
Classic Search recupera candidatos.
Los nodos segmentan, filtran suavemente, reordenan, agrupan y explican.
```

Nunca debe empezar como exclusión dura. Debe conservar “Ver todo”.

### 1.4. Market Pack

Un Market Pack es un paquete versionado que define un mercado, sus submercados, dimensiones, valores, señales léxicas, reglas, relaciones, evidencias y política de runtime.

Ejemplos:

```text
market_pack_infrastructure_v0_1
market_pack_food_social_nutrition_v0_1
market_pack_software_v0_1
market_pack_social_services_v0_1
market_pack_dotacion_mobiliario_v0_1
```

### 1.5. Query signals

Señales derivadas de la consulta del usuario. Sirven para entender intención, modalidad solicitada, población, servicio y composición semántica. No prueban que una fila SECOP pertenezca al mercado.

Ejemplo:

```yaml
query: "alimentación para adultos mayores"
query_signals:
  population: adulto_mayor
  service: alimentacion
```

### 1.6. Row signals

Señales derivadas de una fila SECOP real: objeto, descripción, entidad, modalidad, tipo de contrato, ubicación y demás campos que el kernel use. Son la base de la clasificación real.

Ejemplo:

```yaml
row: "alimentación escolar PAE"
row_signals:
  service: alimentacion
  population: escolar
classification: no_aplica_para_adulto_mayor
```

Regla:

```text
query_signal no puede contaminar row_signal.
Lo que el usuario pidió no prueba lo que una fila contiene.
```

### 1.7. Node-assisted candidate pool

Búsqueda experimental asistida por un Market Pack. Solo se activa en shadow/dogfood cuando Classic Search devuelve 0, mucho ruido o un pool irrelevante, y cuando el Market Pack ya demostró precisión como anotador post-retrieval.

No reemplaza `QueryPlan`, `SoQLBuilder`, `opportunity_search` ni filtros duros. El nodo solo propone expansión blanda del mercado.

```text
Classic Search = base segura.
Market Pack = interpretación del mercado.
Node-assisted candidate pool = rescate controlado de recall.
```

### 1.8. Foreign-domain qualifier

Palabra o frase en la query que desplaza la intención hacia otro mercado.

Ejemplos:

```text
hogares geriátricos veterinarios
centros de bienestar laboral
centros de vida estudiantil
bienestar animal adulto mayor
```

Regla:

```text
Si hay un calificador fuerte de otro dominio, no activar node-assisted candidate pool automáticamente.
Usar Classic Search, pedir aclaración o marcar conflicto.
```

### 1.9. Actionability resolver

Capa que decide qué puede hacer el usuario con una fila: oportunidad accionable, verificar vigencia, referencia de mercado, histórico o descartado. No debe confundirse con pertenencia semántica.

```text
Pertenencia semántica responde: ¿qué es?
Accionabilidad responde: ¿qué puede hacer el usuario con esto?
```

---

## 2. Filosofía de nodos SECOPPAL

### 2.1. Un nodo no es una lista de palabras

Un nodo es:

```text
hipótesis de mercado
+ reglas de pertenencia
+ exclusiones
+ roles contractuales
+ evidencia
+ política de acción
+ contrato de renderizado
```

Ejemplo malo:

```text
adulto mayor = adulto mayor, ancianos, bienestar, vulnerabilidad
```

Ejemplo correcto:

```text
adulto_mayor_servicios_sociales:
  anchors fuertes:
    adulto mayor, personas mayores, centro vida, centro día, Ley 1276
  términos contextuales:
    atención integral, vulnerabilidad, bienestar, protección, alimentación
  blockers:
    bienestar animal, caninos, felinos, veterinaria, fauna
  roles:
    core_atencion_integral, direct_supply, operational_support, beneficiary_partial, excluded_noise
  render:
    directos, relacionados, separados, limitaciones técnicas
```

### 2.2. Un nodo maduro no es el que más trae

Un nodo maduro sabe:

1. qué trae;
2. por qué lo trae;
3. qué excluye;
4. qué no puede afirmar todavía;
5. qué puede hacer el usuario con eso.

### 2.3. Los nodos añaden inteligencia funcional

Los nodos no “piensan” en abstracto. Añaden estructura cuando el texto bruto es ambiguo:

```text
mercado = infraestructura vial
función = interventoría
acción = supervisión
activo = puente
```

Ese salto permite filtrar, comparar, contar, navegar, crear radares y explicar.

### 2.4. No todos los mercados usan las mismas dimensiones

No forzar una ontología universal. Cada mercado decide sus dimensiones útiles.

| Mercado | Dimensiones útiles iniciales |
|---|---|
| Infraestructura vial | función + acción + activo |
| Software | función + tipo de solución + etapa del ciclo |
| Alimentación | función + modalidad de entrega + población/programa |
| Servicios sociales | población + modalidad de atención + componente del servicio |
| Dotación/mobiliario | objeto/dotación + lugar destino + función contractual |

---

## 3. Gobernanza W0S: construir nodos sin autoengaño

### 3.1. Fases separadas

La construcción de nodos debe separar fases:

1. **Observación:** generar candidatos y review pack.
2. **Decisión:** documentar decisiones aprobadas por asesor/humano.
3. **Aplicación:** aplicar únicamente decisiones aprobadas.
4. **Reprobe:** medir antes/después.
5. **Evaluación humana:** etiquetar muestra revisable.
6. **Scoring:** calcular señales preliminares sin gate final.
7. **Shadow runtime:** anotar resultados reales sin alterar búsqueda.
8. **Advisor/render:** verificar si la clasificación puede explicarse al usuario.
9. **Dogfood:** activar navegación experimental con feature flags.
10. **Producción:** solo si hay evidencia suficiente.

Regla:

```text
No mezclar fases.
Una decisión documentada no prueba que el nodo funcione.
Una implementación que reduce ruido no reemplaza evaluación humana.
Un buen score preliminar no autoriza producción.
```

### 3.2. El agente no decide la ontología

Las decisiones de ontología son de producto/dominio:

- mantener anchor;
- degradar término;
- exigir coocurrencia;
- crear exclusión;
- dividir nodo;
- renombrar nodo;
- marcar overlap legítimo.

El agente puede estructurar, verificar y proponer observaciones, pero no mezclar juicios propios con reglas aprobadas.

Usar estados:

```text
approved_decision
proposed_followup_observation
implementation_correction
future_research
```

Solo `approved_decision` puede entrar a reglas.

### 3.3. No todo hallazgo se vuelve regla

Si durante un reprobe aparece un caso interesante, se registra como observación futura. No se aplica en la misma corrida salvo que sea corrección de fidelidad.

**Permitido:** corregir implementación para respetar una decisión aprobada.  
**No permitido:** agregar una regla nueva porque el resultado se ve mejor.

### 3.4. Una lista de anchors no siempre es exhaustiva

Si una decisión dice “mantener estos anchors fuertes”, no significa automáticamente “eliminar todos los demás anchors existentes”.

Antes de reemplazar cobertura previa, verificar si la decisión pretendía:

- preservar ejemplos principales;
- reemplazar completamente la lista anterior;
- agregar restricciones;
- o documentar evidencia representativa.

### 3.5. Todo YAML refinado debe ser trazable

Un YAML refinado debe incluir:

```yaml
source_decisions_artifact: artifacts/...
applied_decision_ids: []
not_applied_proposed_observations: []
previous_node_id: optional
implementation_correction_note: optional
created_at: ...
implementation_commit: ...
```

Nadie debería mirar una regla y preguntarse “¿de dónde salió esto?”.


### 3.6. Gates W0S para Market Packs

La experiencia Adulto Mayor deja una secuencia de gates que debe quedar como patrón para futuros Market Packs.

```yaml
wos_market_pack_gates:
  W0S.13_value_eval:
    pregunta: "¿el nodo aporta entendimiento, exploración o accionabilidad frente a Classic Search?"
    salida: keep | keep_with_limits | standby | drop

  W0S.13B_coverage_gate:
    pregunta: "¿qué buckets están cubiertos y cuáles siguen inconclusos?"
    buckets:
      - mercado_amplio
      - submercado_principal
      - submercado_secundario
      - servicios_componentes
      - negativos_ruido

  W0S.13C_negative_triage:
    pregunta: "¿los blockers y señales de ruido están separados de positive_subfacets?"
    salida:
      - positive_subfacets
      - noise_signals
      - exclusion_reasons

  W0S.14_compositional_offline:
    pregunta: "¿la ontología entiende población + modalidad + servicio + filtros?"

  W0S.14B_live_shadow_annotation:
    pregunta: "¿el Market Pack anota filas reales sin tocar runtime?"

  W0S.15_retrieval_gap:
    pregunta: "¿Classic falla por AND rígido, vocabulario literal o datos/Socrata?"

  W0S.15B_reliability:
    pregunta: "¿el shadow mejora recall con bajo ruido y hard filters preservados?"

  W0S.16A_builder_shadow_only:
    pregunta: "¿el builder sabe cuándo ayudar y cuándo callarse por qualifier conflict?"
```

Regla:

```text
Un gate positivo habilita el siguiente experimento; no habilita producción por sí solo.
```

### 3.7. Estados de decisión más precisos

```yaml
decision_states:
  keep_with_limits:
    meaning: "el nodo aporta, pero no está listo para usuarios"
  candidate_for_internal_streamlit_dogfood:
    meaning: "puede mostrarse internamente con flag y user_facing_ready=false"
  w0s16_justificado:
    meaning: "se justifica explorar candidate pool asistido, no activarlo en runtime público"
  production_ready:
    meaning: "usuarios reales, latencia, rollback, telemetría y explicación validados"
```
---

## 4. Anchors, contexto, blockers y ruido

### 4.1. Anchors necesarios, pero peligrosos

Los anchors activan o aportan señal fuerte:

```text
adulto mayor
centro vida
pavimentación de vías
programa de alimentación escolar
software de gestión documental
```

Riesgos:

- substring dentro de otro mercado;
- término genérico disfrazado de anchor;
- siglas ambiguas;
- contexto usado de forma incorrecta.

### 4.2. Términos contextuales no activan solos

Ejemplos:

```text
atención integral
protección
vulnerabilidad
bienestar
suministro
apoyo
mantenimiento
reparación
alimentación
```

Solo deben sumar señal si coocurren con anchor, programa, población, activo o función suficiente.

### 4.3. Blockers deben aplicarse también en gates

Aprendizaje de Adulto Mayor:

```text
centro de bienestar animal
```

No basta con impedir que active core. También debe impedir que `centro de bienestar` cuente como evidencia para activar `atención integral + protección + vulnerabilidad`.

Regla:

```text
Una señal bloqueada no debe servir como evidencia indirecta para otro gate.
```

### 4.4. Cada anchor tiene su propio contexto

No reutilizar una lista amplia de contexto para todos los anchors.

Ejemplo:

- Para `pavimentación`, puede tener sentido contexto como vía/calle/carrera/tramo/casco urbano/vereda.
- Para `mantenimiento`, esa lista es demasiado amplia: “mantenimiento en casco urbano” puede ser PTAP, cancha, cocina escolar o estación meteorológica.

Regla:

```text
Cada anchor genérico requiere coocurrencias más estrictas que un anchor específico.
```

### 4.5. Objeto de intervención vs ubicación

Muchos términos aparecen como lugar, no como objeto contractual.

Ejemplos:

```text
poda de árboles en vías
cocina escolar en vereda
malla eslabonada
actividad recreativa en cancha
```

Eso no es automáticamente infraestructura vial, construcción de cocina vial, malla vial ni obra deportiva.

Regla:

```text
Un término físico solo activa core si es objeto de intervención, no solo ubicación.
```


### 4.6. Blocker vs foreign-domain qualifier

No confundir blocker con qualifier.

| Concepto | Vive principalmente en | Ejemplo | Efecto |
|---|---|---|---|
| `blocker` | Row signal | `bienestar animal` en una fila SECOP | descarta o separa la fila |
| `foreign_domain_qualifier` | Query signal | `centros de bienestar laboral` | impide activar candidate pool asistido |

Regla:

```text
El blocker limpia filas.
El qualifier protege la intención del usuario.
```

### 4.7. Calificadores ajenos fuertes

Cada Market Pack debe tener un catálogo inicial de calificadores que pueden desplazar la consulta a otro mercado.

Para Adulto Mayor:

```yaml
foreign_domain_qualifiers:
  animal_veterinario:
    - animal
    - animales
    - veterinario
    - veterinaria
    - veterinarios
    - canino
    - caninos
    - felino
    - felinos
    - perro
    - perros
    - mascota
    - mascotas

  laboral:
    - laboral
    - bienestar laboral
    - trabajadores
    - empleados
    - talento humano
    - seguridad y salud en el trabajo
    - sst

  estudiantil_educativo:
    - estudiantil
    - estudiantes
    - universitario
    - escolar
    - bienestar universitario
    - bienestar estudiantil
```

Regla:

```text
Un Market Pack no puede ignorar silenciosamente un calificador que cambia el dominio de la consulta.
```

### 4.8. Términos explícitos de la query no se recortan

Si se construye `object_or_controlled`, los términos explícitos del usuario tienen prioridad sobre términos generales del catálogo.

Orden recomendado:

```yaml
object_or_priority:
  1_explicit_query_terms: "lo que el usuario dijo literalmente"
  2_modality_specific_terms: "términos de la modalidad detectada"
  3_service_specific_terms: "servicios transversales detectados"
  4_general_market_terms: "términos generales del mercado"
```

Regla:

```text
Nunca truncar fuera un término explícito de la query para meter un término genérico del catálogo.
```
---

## 5. Roles: pertenencia no es binaria

Un resultado puede pertenecer al nodo de varias maneras.

Roles base:

```text
core_direct
physical_execution
direct_supply
operation_service
operational_support
beneficiary_specific
beneficiary_partial
location_only
training_context
equipment_support
legitimate_overlap
unrelated_noise
technical_limitation
```

### 5.1. Core directo

El objeto principal pertenece claramente al mercado.

Ejemplo:

```text
operación de Centro Vida para personas mayores
construcción de placa huella
programa de alimentación escolar
implementación de software de gestión documental
```

### 5.2. Soporte operativo

Apoya la operación del mercado, pero el objeto principal pertenece a otro.

Ejemplo:

```text
mantenimiento de vehículos para transporte de adultos mayores
menaje para comedor comunitario
alquiler de maquinaria amarilla para mantenimiento vial
```

### 5.3. Beneficiario parcial

El mercado/población aparece junto a otras poblaciones.

Ejemplo:

```text
población vulnerable incluyendo niños, discapacidad y adultos mayores
```

### 5.4. Overlap legítimo

Un proceso puede pertenecer a varios nodos sin ser error.

Ejemplo:

```text
maquinaria amarilla para mantenimiento de vías
```

Puede ser:

```text
infraestructura_vial: physical_execution_support
parque_automotor_maquinaria: core
```

---

## 6. Relevancia semántica vs accionabilidad

Un resultado puede pertenecer al mercado y no ser accionable.

Ejemplo:

```text
contrato liquidado de atención integral a personas mayores
```

Pertenece a Adulto Mayor, pero para un proponente no es oportunidad. Es histórico o referencia de mercado.

### 6.1. Dos lecturas obligatorias

**Lectura semántica:**

```text
directo
relacionado
excluido
ruido
limitación técnica
```

**Lectura de acción:**

```text
accionable
referencia de mercado
histórico
descartado/no accionable
```

La primera responde “qué es”.  
La segunda responde “qué puede hacer el usuario con eso”.

### 6.2. Política de estado

Cada nodo debe interpretar estados sin cambiar pertenencia semántica.

Ejemplo base:

```yaml
actionable:
  - Publicado
  - Abierto
  - Convocatoria
  - Borrador
  - Presentación de ofertas
market_reference:
  - Seleccionado
  - Adjudicado
  - Celebrado
  - Firmado
historical:
  - Liquidado
  - Terminado
  - Cerrado
excluded_or_non_actionable:
  - Cancelado
  - Revocado
  - Suspendido
```

### 6.3. Proponente vs funcionario vs veeduría

Un mismo resultado puede tener distinto valor por audiencia.

| Audiencia | Prioridad |
|---|---|
| Proponente | accionable, fecha, valor, requisitos, cierre |
| Funcionario | referencias de mercado, históricos, comparables |
| Veeduría | trazabilidad, exclusiones, anomalías, concentración |


### 6.4. Actionability no es pertenencia semántica

Un proceso puede ser core del mercado y no ser oportunidad activa.

Ejemplo:

```text
Contrato seleccionado de atención integral a adultos mayores.
```

Clasificación correcta:

```yaml
semantic_membership: core_adulto_mayor
actionability: market_reference
reason: "estado seleccionado/adjudicado; no es oportunidad activa"
```

Regla:

```text
Market Pack no reemplaza opportunity_search.
Market Pack no decide por sí solo si algo está vigente.
```

### 6.5. Opportunity Search sigue siendo capa separada

`opportunity_search` conserva la política de oportunidad: estados abiertos, frescura, fecha de cierre, timeout, sugerencias de acotación y orden por fecha. El Market Pack solo aporta lectura de mercado.

```text
Core estructura la consulta.
Nodo entiende el mercado.
Opportunity policy decide si es oportunidad.
Market Pack explica el resultado.
Radar vigila la combinación.
```
---

## 7. Renderabilidad: de evaluación interna a Advisor

Un nodo no está listo para usuario solo porque clasifica bien.

### 7.1. Separar `internal_eval` de `user_response_model`

`internal_eval` puede contener:

```text
report_key
query_id
core
overlap
animal_guard
matched_terms
runtime_no_results_or_timeout
distribuciones técnicas
errores crudos
```

`user_response_model` nunca debe mostrar eso.

Traducción visible:

| Interno | Usuario |
|---|---|
| core | Directamente relacionado |
| overlap / adjacent | Posiblemente relacionado |
| animal_guard | Separado: bienestar animal |
| excluded / no_node_signal | Separado: baja señal del mercado |
| runtime_no_results_or_timeout | Limitación técnica |

### 7.2. Evidence quality

Antes de mostrar lista de resultados, calcular:

```text
user_facing_candidate
partial_evidence
insufficient_for_user_facing_result_list
```

Regla:

```text
Sin objeto + entidad, no hay lista usuario-final.
```

Campos mínimos deseables:

```text
result_key
dataset
object_text / title
entity_name
value
status
date
url/referencia verificable
source_report
query_id
user_query
shadow_label
matched_terms
exclusion_reason
technical_status
```

### 7.3. No decir “Encontré N procesos” sin dedupe

Si no hay llave estable:

```text
urlproceso
id_del_proceso
referencia_del_proceso
id_contrato
hash estable
```

no decir:

```text
Encontré 17 procesos
```

Decir:

```text
Evalué 17 coincidencias de prueba
```

### 7.4. Excluir también es valor

Ejemplo de render correcto:

```text
Separé resultados de bienestar animal para no inflar la búsqueda de adulto mayor.
```

Esto convierte un blocker en valor de Advisor.

### 7.5. Timeout no es cero resultados

Nunca convertir timeout en ausencia.

```text
No pudimos consultar SECOP para parte de la búsqueda. Esto no significa que no existan procesos.
```

---

## 8. Evaluación: Classic vs Nodal vs Hybrid

### 8.1. Ningún Market Pack entra sin benchmark A/B/C

Antes de runtime:

- **A = Classic Search:** SECOPPAL actual con parser, variantes, filtros y ranking.
- **B = Nodal Search:** clasificación/filtro por nodos.
- **C = Hybrid Search:** recuperación clásica + nodos para segmentar/reordenar/filtrar.

### 8.2. Métricas obligatorias

```text
TP
FP
FN
precision
recall
F1
Precision@10
candidatos a revisar
reducción de ruido
hallazgos exclusivos
pérdidas por método
errores por entidad/dataset
```

### 8.3. Taxonomía de errores

```text
mention_not_procurement
lexical_gap
explicit_asset
ambiguous_action
insufficient_text
semantic_misclassification
weak_search_query
technical_timeout
retrieval_noise
```

### 8.4. Decisión por dimensión

| Decisión | Significado |
|---|---|
| KEEP_PRIMARY | Aporta claramente; navegación principal. |
| KEEP_SECONDARY | Útil como refinamiento; no siempre supera búsqueda. |
| MERGE | Debe combinarse con Classic Search. |
| BROWSE_ONLY | Sirve para mapa/navegación, no para filtro duro. |
| DROP | Ambigua o no aporta suficiente. |

### 8.5. Primer gate: precisión y explicabilidad

No intentar medir recall perfecto al inicio.

Primero medir:

- ruido en core;
- calidad de excluded;
- utilidad de adjacent/subtypes;
- legitimidad de overlaps;
- si la clasificación es explicable.


### 8.6. Gate de valor

Preguntas obligatorias:

1. ¿El nodo mejora entendimiento?
2. ¿El nodo mejora exploración?
3. ¿El nodo mejora accionabilidad?
4. ¿El nodo separa mercado histórico de oportunidad?
5. ¿Qué buckets quedan inconclusos?

Regla:

```text
Nunca leer “0 falsos positivos” como éxito si los negativos evaluables fueron 0.
```

### 8.7. Gate de cobertura por bucket

Cada Market Pack debe evaluarse por buckets, no solo por promedio global.

```yaml
coverage_buckets:
  - mercado_amplio
  - modalidad_residencial_o_principal
  - modalidad_diurna_o_secundaria
  - servicios_transversales
  - negativos_ruido
```

Un buen promedio global no autoriza dogfood si los buckets críticos siguen ciegos.

### 8.8. Gate composicional offline

Antes de live, probar queries que crucen dimensiones:

```text
población + modalidad
población + servicio
servicio + modalidad
modalidad A + modalidad B
filtros duros + mercado
```

El objetivo es validar que la ontología entiende la composición, no recuperar filas todavía.

### 8.9. Live shadow eval

Prueba contra filas reales sin modificar runtime.

Métricas mínimas:

```text
queries_evaluadas
evaluables_vs_inconclusas
annotation_rate
regression_count
invariant_failures
noise_rate
core_rows
discarded_rows
explanations
```

Resultado posible:

```text
candidate_for_internal_streamlit_dogfood
```

Esto no significa producción; solo habilita dogfood interno con flag.

### 8.10. Retrieval Gap Analysis

Cuando Classic Search trae 0, trae basura o trae 80%+ descartado, no asumir que el mercado no existe.

Comparar:

```text
candidate pool clásico
candidate pool asistido por nodo
overlap
filas core nuevas
ruido introducido
filtros duros preservados
latencia
```

Diagnósticos mínimos:

```yaml
retrieval_gap_diagnoses:
  - fallo_por_and_demasiado_rigido
  - fallo_por_vocabulario_literal
  - fallo_por_datos_secop_socrata
  - fallo_por_timeout
  - fallo_por_filtro_duro_correcto
  - fallo_por_nodo_demasiado_amplio
```

### 8.11. Contrato de judge

Marcar `regresion` solo cuando:

- hay evidencia real de mercado en la fila;
- el Market Pack la clasifica mal;
- o introduce ruido como core.

No marcar regresión cuando:

- Classic trae filas irrelevantes;
- el Market Pack las descarta correctamente;
- el problema está en candidate pool.

### 8.12. Taxonomía de cero resultados

```yaml
zero_results:
  real_zero:
    meaning: "no hay filas razonables"
  retrieval_zero:
    meaning: "classic construyó mal el candidate pool"
  technical_zero:
    meaning: "timeout, throttling, datos.gov.co, falta SECOP_APP_TOKEN"
```

Regla:

```text
Timeout no es cero resultados. Candidate pool malo no es ausencia de mercado.
```

### 8.13. Invariantes deben auditar evidencia del kernel

Los tests de invariantes deben leer `matched_terms`, `matched_fields` o `evidence_fields` generados por el kernel.

No deben reimplementar un mini-parser con menos campos, porque produciría falsos incumplimientos.
---

## 9. Runtime dogfood

### 9.1. Nombre interno y nombre visible

Interno:

```text
classic_search
nodal_search
hybrid_search
```

Usuario:

```text
Buscar
Explorar mercado · Beta
```

No usar aún:

```text
Limitless Search
Búsqueda holística
```

“Limitless” promete demasiado; “holística” suena a consultoría con incienso.

### 9.2. Flujo recomendado

```text
consulta del usuario
    ↓
parser actual
    ↓
resolver mercado / Market Pack
    ↓
Classic Search recupera candidatos
    ↓
nodos solicitados/precalculados anotan
    ↓
segmentar / reordenar / explicar
    ↓
respuesta Advisor + Ver todo
```

### 9.3. Reglas duras para primera versión

1. Si no hay Market Pack, SECOPPAL funciona como hoy.
2. Los nodos no esconden resultados por defecto.
3. Siempre existe “Ver todo”.
4. Baja confianza no excluye silenciosamente.
5. Clasificación semántica debe ser precalculada o cacheada; no LLM por clic.
6. Guardar evidencia para “¿por qué entró?”.
7. Mantener búsqueda clásica y semántica como capas separadas.
8. Feature flags por mercado/dimensión.

### 9.4. Feature flags

```yaml
semantic_nodes_enabled: true
market_pack_infrastructure_enabled: true
market_pack_food_enabled: false
semantic_reranking_enabled: true
nodal_filter_hard_exclusion: false
show_explore_market_beta: true
show_why_this_result: true
```

### 9.5. Estados de madurez

```text
draft
benchmarking
dogfood
experimental
production
deprecated
```

### 9.6. Qué medir en dogfood

```text
modo usado
faceta abierta
nodo seleccionado
resultados antes/después
uso de Ver todo
reformulación de consulta
proceso abierto
radar creado
búsqueda abandonada
cero resultados
tiempo hasta click útil
quejas por etiqueta
```

Preguntas clave:

- ¿La gente entra a “Explorar mercado”?
- ¿Qué nodo selecciona?
- ¿Vuelve a “Ver todo” inmediatamente?
- ¿Abre más procesos útiles?
- ¿Crea radar desde una combinación?
- ¿Qué dimensión nadie usa?


### 9.7. Activación de node-assisted candidate pool

Solo se considera si el Market Pack ya pasó gates previos.

```yaml
activate_node_assisted_pool:
  market_pack_detected: true
  query_is_compositional: true
  hard_filters_parseable: true
  no_foreign_domain_conflict: true
  any:
    - classic_total == 0
    - classic_core_count == 0
    - classic_descartado_pct >= 0.80
    - classic_timeout_but_query_signals_sufficient == true
```

Si hay conflicto de calificador ajeno:

```yaml
foreign_domain_conflict:
  action: do_not_expand
  fallback:
    - classic_only
    - clarification
    - mark_as_ambiguous
```

### 9.8. Límites de OR controlado

El OR controlado mejora recall, pero puede destruir latencia si crece sin límites.

```yaml
node_assisted_limits:
  max_anchor_terms: 5
  max_facet_terms: 20
  max_object_or_terms: 12
  max_like_clauses: 60
  timeout_seconds: definido_por_entorno
  fallback: classic_only
```

Reglas:

```text
No buscar facetas transversales solas.
Siempre exigir anchor poblacional, programa, activo o modalidad suficiente.
Priorizar términos explícitos de la query antes de términos generales del catálogo.
```

### 9.9. Modo visible en dogfood interno

Si se muestra internamente:

```text
Classic Search trajo 50 filas, pero 0 parecen de adulto mayor.
Probé una búsqueda asistida por el nodo y encontré 44 coincidencias fuertes.
Vista experimental. No es producción.
```

Debe existir siempre:

```text
Ver búsqueda clásica
Ver descartados
Ver por qué entró
Ver por qué se bloqueó el shadow
```
---

## 10. Almacenamiento y arquitectura de datos

### 10.1. Estado actual

En pilotos, los nodos y clasificaciones viven como:

```text
JSON      → catálogo/valores/dimensiones
JSONL     → clasificación por proceso
Markdown  → guía, reglas, prompts, decisiones
Manifest  → trazabilidad, versión, hashes
CSV       → labels humanos
```

Ejemplos conceptuales:

```text
DAY4A_CATALOG_2C.json
DAY4A_CLASSIFICATION_2C.jsonl
DAY4A_CLASSIFICATION_GUIDE_2C.md
```

### 10.2. Estado recomendado para runtime

Fuente editorial versionada en Git:

```text
market_packs/*.json
```

Runtime:

```text
SQLite/Postgres
```

Tablas sugeridas:

```text
market_packs
semantic_nodes
node_aliases
node_relations
process_semantic_labels
classification_evidence
market_pack_versions
node_eval_runs
```

No usar base de grafos todavía. Tablas simples bastan.

### 10.3. Market Pack JSON mínimo

```json
{
  "market_id": "infrastructure",
  "version": "0.1.0",
  "status": "dogfood",
  "label": "Infraestructura",
  "description": "Procesos de obra pública, estudios, interventoría, mantenimiento, suministros y apoyo profesional asociados a infraestructura física.",
  "dimensions": {
    "contract_function": {
      "runtime_role": "primary_navigation",
      "values": [
        "physical_execution",
        "intervention",
        "studies_consulting",
        "supply",
        "professional_support"
      ]
    },
    "primary_action": {
      "runtime_role": "secondary_filter",
      "values": [
        "construction",
        "improvement",
        "rehabilitation",
        "maintenance_repair"
      ]
    },
    "asset_or_component": {
      "runtime_role": "optional_facet",
      "values": [
        "road",
        "urban_road_network",
        "bridge",
        "track_slab",
        "signage",
        "pavement"
      ]
    }
  },
  "runtime_policy": {
    "default_mode": "hybrid_search",
    "hard_exclusion_enabled": false,
    "always_show_view_all": true,
    "user_visible_name": "Explorar mercado · Beta"
  }
}
```

---

## 11. Morfología, derivaciones y aliases de dominio

### 11.1. Tres problemas distintos

No confundir:

| Capa | Ejemplo |
|---|---|
| Morfología | reparar, repara, reparó, reparando, reparado |
| Derivación | reparar, reparación, reparaciones |
| Alias de dominio | arreglo, parcheo, intervención correctiva, recuperación localizada |

### 11.2. El contexto decide la etiqueta

No hacer regla ciega:

```text
reparación = mantenimiento
```

Porque:

```text
reparación de baches → mantenimiento
reparación estructural integral → rehabilitación
demolición y reconstrucción → construcción
reparación y ampliación de capacidad → mejoramiento
```

### 11.3. Tres traducciones necesarias

```text
consulta del usuario → nodos canónicos
texto del contrato → nodos canónicos
nodos canónicos → filtros/navegación/respuesta
```

Ejemplo:

```text
Usuario: reparaciones de carreteras
Resolver consulta:
  market = infraestructura_vial
  action_candidates = mantenimiento, rehabilitación
  asset = vía/carretera

Proceso: recuperación de sectores críticos mediante parcheo
Clasificación:
  action = mantenimiento
  asset = vía/carretera

Coincidencia:
  nodos compatibles, aunque las palabras no sean iguales.
```

### 11.4. Señales léxicas en nodo

```json
{
  "node_id": "action.maintenance_repair",
  "preferred_label": "Mantenimiento y reparación",
  "lexical_signals": {
    "lemmas": ["mantener", "reparar", "conservar"],
    "derived_forms": ["mantenimiento", "reparación", "reparaciones", "conservación"],
    "domain_aliases": ["arreglo", "parcheo", "intervención correctiva"]
  },
  "classification_note": "Reparación estructural integral puede corresponder a rehabilitación; decidir por contexto."
}
```

Estas señales ayudan a recuperar y resolver consulta, pero no prueban clasificación.


### 11.5. Verbos de intención no son objeto contractual obligatorio

Palabras como:

```text
atender
operar
involucrar
prestar
brindar
desarrollar
garantizar
```

no deben convertirse automáticamente en filtros `LIKE` obligatorios.

En contratación pública, esas ideas pueden aparecer como:

```text
aunar esfuerzos
prestación de servicios
atención integral
apoyo logístico
operación del programa
fortalecimiento institucional
```

Regla:

```text
Los verbos de intención alimentan action_signal o query_signal, pero no necesariamente objeto AND.
```
---

## 12. Market Packs prioritarios

### 12.1. Infraestructura v0.1

No crear un único nodo plano `infraestructura`. Crear familia.

```text
infraestructura
├── infraestructura_vial
├── edificaciones_y_equipamientos
├── agua_y_saneamiento
├── espacio_publico_y_urbanismo
├── energia_y_alumbrado
├── mantenimiento_locativo
└── otra_no_determinada
```

Dimensión transversal:

```text
contract_function:
  - ejecución física
  - interventoría
  - estudios y diseños
  - suministro
  - mantenimiento
  - apoyo profesional
```

Profundidad especial inicial solo para vial:

```text
primary_action:
  - construcción
  - mejoramiento
  - rehabilitación
  - mantenimiento/reparación

asset_or_component:
  - vía
  - malla vial urbana
  - puente
  - placa huella
  - pavimento
  - señalización
```

No intentar desde el día 1 veinte categorías para agua, alumbrado y edificaciones. Primero market/submarket + función.

### 12.2. Alimentación social y nutricional

No crear nodo plano `alimentacion`. Crear familia:

```text
alimentacion_social_nutricional
├── alimentacion_escolar_pae
├── alimentacion_adulto_mayor
├── primera_infancia_icbf
├── comedores_comunitarios
├── ayuda_humanitaria_mercados
├── recuperacion_nutricional
├── alimentacion_institucional
├── refrigerios_logistica
└── alimentacion_animal_noise
```

Regla:

```text
Alimentación no es un mercado único.
Es una familia diferenciada por población, programa, modalidad de entrega y finalidad.
```

### 12.3. Software y tecnología

No empezar con “software” como lista de palabras. Usar dimensiones:

```text
software_tecnologia
├── desarrollo_software
├── licenciamiento_software
├── soporte_mantenimiento
├── infraestructura_tecnologica
├── ciberseguridad
├── conectividad_redes
├── datos_analitica
├── servicios_cloud
└── hardware_ti
```

Dimensiones útiles:

```text
solution_type:
  - software a medida
  - licencias
  - SaaS/cloud
  - soporte/mantenimiento
  - infraestructura TI
  - conectividad
  - seguridad

lifecycle_stage:
  - diseño
  - desarrollo
  - implementación
  - migración
  - soporte
  - renovación
  - auditoría
```

Blockers:

```text
infraestructura física
mantenimiento locativo
red vial
red eléctrica no TI
```

Cuidado: “infraestructura tecnológica” es TI; “infraestructura física” es obra pública.

### 12.4. Servicios sociales

No hacer un saco `servicios_sociales`. Es demasiado amplio.

Diseño recomendado:

```text
servicios_sociales
├── adulto_mayor
├── primera_infancia
├── infancia_adolescencia
├── mujer_genero
├── victimas_conflicto
├── discapacidad
├── etnias_comunidades
├── habitante_calle
├── migrantes
├── poblacion_vulnerable_general
└── familia_comunidad
```

Dimensiones:

```text
beneficiary_population
service_modality
service_component
operator_type
support_supply
```

#### Caso especial: Adulto Mayor como Market Pack piloto

Adulto Mayor debe modelarse por población + modalidad + servicio transversal.

```yaml
adulto_mayor_servicios_sociales:
  modalidades:
    residencial_larga_estancia:
      - centro de bienestar
      - centro de protección
      - asilo
      - hogar geriátrico
      - hogar gerontológico
      - comunidad de cuidado
      - larga estancia
      - atención residencial
      - cuidado 24/7

    diurna_comunitaria:
      - centro de vida
      - centro día
      - atención diurna
      - envejecimiento activo
      - actividades
      - recreación
      - actividad física
      - talleres
      - socialización

    ambas_modalidades:
      description: "cuando la query o fila cruza residencial y diurna"

  servicios_transversales:
    - alimentación
    - nutrición
    - atención integral
    - cuidado
    - psicosocial
    - recreación
    - actividad física
```

Regla:

```text
Comunidad de cuidado, asilo, hogar geriátrico, centro de protección y larga estancia no son equivalentes a centro día/centro de vida.
Son familias hermanas dentro de Adulto Mayor.
```

¿Dónde van animales?

No meter `animales` dentro de servicios sociales humanos. Crear Market Pack separado:

```text
bienestar_animal_proteccion_animal
```

Y usarlo como:

- nodo propio para quien busque bienestar animal;
- blocker/cross-node para Adulto Mayor y Alimentación;
- ejemplo de exclusión explicable.

### 12.5. Dotación y mobiliario

Debe ser Market Pack propio, no residuo de infraestructura.

```text
dotacion_mobiliario_equipamiento
├── mobiliario_oficina
├── mobiliario_escolar
├── dotacion_institucional
├── menaje_cocina
├── equipos_no_ti
├── dotacion_deportiva
├── dotacion_cultural
└── equipos_biomedicos_optional
```

Dimensiones:

```text
asset_type
recipient_space
contract_function
installation_required
cross_market
```

Cross-nodes:

```text
alimentacion_social_nutricional → menaje cocina / comedores
educacion → mobiliario escolar
salud → equipos biomédicos
deporte → dotación deportiva
software_tecnologia → hardware TI, si aplica
```

---

## 13. Cross-node graph

La madurez de SECOPPAL no estará en tener muchos nodos sueltos, sino un grafo de mercados contractuales donde un mismo contrato pueda tener varias lecturas:

```text
objeto principal
función contractual
población beneficiaria
activo/componente
soporte operativo
mercado proveedor
estado/accionabilidad
```

Ejemplo:

```text
Suministro de alimentos para adultos mayores del Centro Vida
```

Lecturas:

```yaml
alimentacion_social_nutricional:
  subtype: alimentacion_adulto_mayor
  role: direct_supply

adulto_mayor_servicios_sociales:
  subtype: alimentacion
  role: service_component
```

Ejemplo infraestructura + alimentación:

```text
adecuación de comedor comunitario
```

Lecturas:

```yaml
infraestructura_obra_publica:
  subtype: edificaciones_y_equipamientos
  role: physical_execution

alimentacion_social_nutricional:
  subtype: comedores_comunitarios
  role: operational_support
```

---

## 14. Clustering y presentación

### 14.1. Agrupar antes de mostrar

Un Advisor no debe listar 20 filas casi iguales si puede agrupar.

Agrupar por:

```text
entidad normalizada
objeto normalizado
subtipo/programa
estado
rango de valor
fecha/vigencia
```

Ejemplo:

```text
Servicios profesionales Centro Día — SDIS
4 procesos similares
Valor individual: $27.097.800
Fecha: 2026-07-04
Referencias: SDIS-CPS-332245-2026, SDIS-CPS-333145-2026, y 2 más.
```

Regla:

```text
Un Advisor que agrupa se siente analítico.
Un Advisor que lista todo se siente como tabla.
```

### 14.2. Procedencia de datos

Si un mock combina fixture, offline snapshot y live snapshot, debe marcarlo.

```yaml
source_mode:
  - fixture
  - offline_snapshot
  - live_snapshot
  - live_current
mixed_origin: true|false
```

No mezclar datos de prueba y reales sin advertencia.

---

## 15. Template de creación de Market Pack

### 15.1. Documento de scope

Cada Market Pack debe tener:

1. hipótesis de mercado;
2. usuarios objetivo;
3. preguntas que resuelve;
4. submercados;
5. dimensiones;
6. valores permitidos;
7. anchors fuertes;
8. términos contextuales;
9. blockers;
10. roles;
11. cross-nodes;
12. política de estado;
13. política de render;
14. métricas;
15. riesgos;
16. plan de benchmark A/B/C;
17. plan de dogfood.

### 15.2. JSON mínimo

```json
{
  "market_id": "string",
  "version": "0.1.0",
  "status": "draft|benchmarking|dogfood|experimental|production|deprecated",
  "display_name": "string",
  "market_hypothesis": "string",
  "submarkets": [],
  "dimensions": {},
  "lexical_signals": {},
  "blockers": [],
  "roles": [],
  "cross_nodes": [],
  "runtime_policy": {},
  "evaluation_policy": {},
  "render_policy": {}
}
```

### 15.3. Criterios para aceptar una dimensión

Una dimensión entra solo si:

1. divide el mercado en opciones comercialmente distintas;
2. el texto permite identificarla con consistencia;
3. el usuario puede navegar o filtrar con ella;
4. no duplica otra dimensión;
5. tiene población suficiente;
6. mejora A/B/C o aporta navegación clara.

---

## 16. Checklist antes de runtime

Un nodo/Market Pack no entra a dogfood si no cumple:

```text
[ ] scope documentado
[ ] catálogo versionado
[ ] corpus seed
[ ] review pack con texto legible
[ ] labels humanos o juicio independiente documentado
[ ] benchmark Classic/Nodal/Hybrid
[ ] taxonomía de errores
[ ] decisión por dimensión: KEEP/MERGE/BROWSE/DROP
[ ] render contract sin internals
[ ] evidence_quality suficiente o limitación explícita
[ ] dedupe estable o conteo honesto
[ ] feature flags definidos
[ ] Ver todo disponible
[ ] telemetría dogfood definida
[ ] rollback/disable fácil
[ ] query_signals separados de row_signals
[ ] positive_subfacets separados de noise_signals
[ ] actionability separada de pertenencia semántica
[ ] judge distingue retrieval_gap vs node_regression
[ ] invariantes usan matched_terms/evidence_fields del kernel
[ ] pruebas negativas offline
[ ] live shadow eval con filas reales
[ ] si aplica node-assisted, hard filters preservados
[ ] si aplica node-assisted, foreign_domain_qualifier_guard implementado
[ ] si aplica node-assisted, términos explícitos preservados antes de truncar
[ ] latencia shadow medida
[ ] timeout ≠ cero resultados

[ ] evidence serialization completa
[ ] grouped view por result_key
[ ] taxonomy equivalence map si hay dos vocabularios internos
[ ] live gate con timing por query
[ ] freeze decision si el bloqueo ya no es semántico
[ ] baseline exportable para warehouse/cold-start
[ ] fecha de corte y delta policy si usa corpus local
[ ] no declarar gold automático desde resultados del nodo
```

---

## 17. Guía de respuesta runtime experimental

Formato recomendado:

```text
Busqué "<consulta>".
Además de la búsqueda normal, organicé los resultados por mercado.

✅ Directamente relacionados (N)
Procesos donde se contrata claramente <función/mercado>.

🔗 Posiblemente relacionados (N)
Procesos donde el mercado aparece como soporte, beneficiario o componente.

🚫 Separados por baja señal (N)
El buscador los trajo por palabras parecidas, pero no parecen corresponder.

⚠️ Limitaciones técnicas
Si SECOP no respondió o el conteo es aproximado.

Acción sugerida: <una acción contextual>

Nota: clasificación experimental; no reemplaza revisión humana.
[Ver todo]
```

No mostrar:

```text
core
overlap
animal_guard
OPP-001
report_key
runtime_no_results_or_timeout
stack trace
```

---

## 18. Qué no hacer

1. No reemplazar Classic Search.
2. No activar exclusión dura en dogfood inicial.
3. No esconder resultados sin “Ver todo”.
4. No llamar LLM en cada interacción runtime.
5. No mezclar datos fixture/live sin marcar.
6. No presentar conteos no deduplicados como procesos.
7. No meter todos los mercados en un nodo saco.
8. No copiar dimensiones de vial a software/alimentación sin validar.
9. No convertir observaciones en reglas sin aprobación.
10. No afinar reglas mirando resultados sin nuevo ciclo de decisión.
11. No seguir tuneando un Market Pack congelado sin ADR nuevo.
12. No abrir Nodo 2 antes de W0D si el bloqueo real es fuente/corpus/latencia.
13. No tratar grupos estables como gold automático; son gold seed auditable.
14. No usar warehouse como excusa para clasificación masiva prematura.
15. No responder “hoy/abierto ahora” solo desde snapshot local sin delta/frescura visible.

---

## 19. Freeze de Market Pack

Un Market Pack puede cerrar su ciclo de validación semántica sin estar listo para usuario final. Esto no es fracaso: es una decisión de gobernanza para evitar tuning infinito.

Estado nuevo:

```yaml
market_pack_freeze:
  meaning: "el nodo demostró valor semántico suficiente; se congela como baseline y deja de tunearse por inercia"
  allowed:
    - usar_como_benchmark
    - usar_como_fixture
    - usar_como_gold_seed_auditable
    - usar_para_disenar_warehouse
    - usar_para_comparar_classic_vs_local_vs_semantic_verified
    - usar_para_revisar_recall_y_precision_en_frio
  forbidden:
    - agregar_vocabulario_sin_nuevo_ADR
    - mover_thresholds_para_promover
    - abrir_live_gates_repetidos_sin_hipotesis_nueva
    - activar_runtime_publico
    - mezclar_con_opportunity_search
    - usar_resultados_como_gold_automatico
```

Criterios mínimos para congelar:

```yaml
freeze_criteria:
  semantic_signal_confirmed: true
  stable_dedupe_available: true
  evidence_serialized: true
  grouped_view_available: true
  live_gate_executed: true
  divergence_rate_acceptable: true
  main_blocker_identified: true
  next_layer_clear: true
```

Regla:

```text
Cuando el bloqueo principal ya no es semántico sino infraestructura, fuente, frescura o latencia, no seguir afinando el nodo. Congelar y mover la inversión a la capa que desbloquea ese cuello.
```

### 19.1. Gold seed no es gold automático

Los grupos estables de un Market Pack congelado pueden servir como universo semilla para benchmark, pero no son verdad final automática.

```yaml
gold_seed_policy:
  stable_groups: "candidatos auditables"
  human_or_independent_review_required: true
  may_include:
    - accionable
    - mercado
    - verificar_vigencia
    - descartado
  must_not_call_all_positive: true
```

Regla:

```text
Un grupo estable prueba dedupe; no prueba relevancia positiva por sí solo.
```

---

## 20. Lecciones aprendidas de pruebas, gates y dogfood

Esta sección recoge el aprendizaje operativo de W0S.17–W0S.21, más las lecciones generales de Sprint A, phrase-aware retrieval, dual dataset, pagination buffer y gates de latencia. La finalidad es que el segundo nodo no repita errores ya pagados.

### 20.1. Los tests no son decoración

La cantidad de tests no importa sola. Importa qué contrato protegen.

Tipos de test que sí aportan:

```yaml
test_types_useful_for_nodes:
  unit_kernel:
    protects: "clasificación determinística y evidencia"
  invariants:
    protects: "reglas que no deben romperse aunque cambie implementación"
  regression:
    protects: "bug real ya observado"
  live_shadow:
    protects: "comportamiento sobre filas reales sin tocar runtime"
  gate_report:
    protects: "decisión reproducible con métricas"
  architecture_lock:
    protects: "no contaminar parser/ranking/runtime"
  smoke_live:
    protects: "token, fuente, timeout, schema y fricción real"
```

Regla:

```text
Cada test nuevo debe proteger una decisión, un bug real o una frontera arquitectónica. Si solo comprueba que el código actual hace lo que él mismo inventó, es test de cartón.
```

### 20.2. No reimplementar el kernel dentro del test

Los tests de invariantes deben leer evidencia generada por el kernel: `matched_terms`, `matched_fields`, `evidence_fields`, `technical_status`, `shadow_label`, `actionability`.

Mala práctica:

```text
El test vuelve a parsear el objeto con una regex parcial y decide que el kernel falló.
```

Buena práctica:

```text
El test inspecciona la evidencia oficial emitida por el kernel y verifica que la regla aprobada esté representada ahí.
```

### 20.3. Mapear taxonomías antes de comparar strings

W0S.19 encontró un bug del auditor: `shadow_label="excluded"` y `technical_status="no_signal"` eran equivalentes conceptuales, pero se comparaban como strings distintos y generaban divergencias falsas.

Regla:

```yaml
taxonomy_equivalence_map_required: true
example:
  excluded: no_signal
  core: core
  adjacent: adjacent
  animal_guard: animal_guard
```

Nunca comparar dos vocabularios internos por igualdad literal si nacieron en capas distintas.

### 20.4. Smoke pequeño antes de corrida grande

Antes de una corrida live completa, correr una query o muestra mínima para validar:

```yaml
live_smoke_checks:
  token_present: true
  source_responds: true
  report_writable: true
  schema_compatible: true
  no_secret_leak: true
  graceful_failure_if_no_token: true
```

Regla:

```text
No gastar 50 queries live si una sola ya muestra que token, schema o serialización están rotos.
```

### 20.5. Secretos nunca en prompts ni artifacts

```yaml
secrets_policy:
  use:
    - env_vars
    - local_dotenv_ignored
    - secret_manager_future
  never:
    - markdown
    - json_artifacts
    - tests
    - prompts
    - logs
    - screenshots
  verify:
    - grep_before_commit
    - no_token_in_artifacts
  rotate_if_exposed: true
```

### 20.6. Timeout no es fallo semántico

Si el nodo produce buenas clasificaciones, grupos estables y cero divergencias, pero el gate falla por timeout, el bloqueo está en fuente/runtime/infraestructura, no necesariamente en la ontología.

```yaml
timeout_interpretation:
  semantic_failure: false
  source_or_runtime_blocker: true
  next_layer:
    - deadline_policy
    - fallback
    - local_warehouse
    - delta_hot
```

Regla:

```text
No arreglar con vocabulario un problema que está en la fuente.
```

### 20.7. Latencia acotada protege experiencia; warehouse recupera amplitud

Deadline, count no bloqueante, fetch acotado y fallback honesto evitan que el usuario espere 60 segundos. Pero también pueden cortar queries que quizá completarían tarde. El warehouse local permite recuperar amplitud sin reabrir la cola de latencia.

```text
Deadline protege la experiencia.
Warehouse recupera amplitud.
Delta Socrata protege frescura.
Market Pack verifica sentido.
```

### 20.8. Agrupar no es opcional

Las variantes de query pueden recuperar el mismo proceso varias veces. Eso puede ser buena señal: robustez de recuperación. El error es mostrar cada fila como proceso distinto.

```yaml
grouping_required:
  key: result_key
  stable_quality_required_to_say: "proceso único"
  show:
    - rows_count
    - query_ids
    - labels
    - actionability_consolidada
    - warnings
    - divergence_flags
```

### 20.9. Actionability debe ser consolidada por grupo

Si varias filas del mismo proceso llegan por diferentes queries, el grupo debe consolidar `accionable`, `mercado`, `verificar_vigencia` o `descartado_no_aplica` con regla visible.

```yaml
actionability_group_policy:
  accionable: "si al menos una evidencia estable lo sostiene y no hay blocker fuerte"
  verificar_vigencia: "si hay señal de mercado pero estado/fecha no permiten afirmar oportunidad"
  mercado: "si pertenece pero no está activo o es referencia histórica"
  descartado_no_aplica: "si el grupo pertenece a dominio ajeno o no tiene señal suficiente"
```

### 20.10. No tocar runtime para probar inteligencia

Adulto Mayor avanzó porque se mantuvo la frontera:

```yaml
runtime_guardrails:
  user_facing_ready: false
  runtime_applied: false
  final_gate_evaluated: false
  decision_is_recommendation_only: true
  app_core_untouched: true
  opportunity_search_untouched: true
  query_router_not_contaminated: true
```

Regla:

```text
Primero probar, medir y cerrar gates. Integrar runtime solo después de demostrar valor y confiabilidad.
```

### 20.11. No tunear mirando el examen

Después de un live gate, no agregar vocabulario solo porque mejora ese reporte. Si hay nueva hipótesis, debe abrir ciclo nuevo:

```text
hipótesis → fixture → negativos → shadow → live → decisión
```

### 20.12. Un nodo puede pasar semántica y fallar producto

Estados separados:

```yaml
market_pack_gate_split:
  semantic_quality_gate:
    checks:
      - accionables
      - dedupe
      - divergencias
      - explainability
  runtime_reliability_gate:
    checks:
      - timeout_rate
      - p90
      - source_errors
      - freshness
  product_gate:
    checks:
      - user_value
      - UI clarity
      - rollback
      - telemetry
```

No mezclar esos gates.

---

## 21. Warehouse bridge para Market Packs

El aprendizaje de Adulto Mayor obliga a conectar la guía de nodos con el plan de warehouse local.

Los Market Packs no deben convertirse en clasificación masiva prematura. Deben aportar cuatro roles independientes:

```yaml
market_pack_roles_with_warehouse:
  query_resolution:
    description: "traducir lenguaje humano a conceptos de mercado"
  retrieval_expansion:
    description: "abrir rutas de recuperación ancladas sobre corpus local"
  semantic_verification:
    description: "verificar qué se contrata realmente en cada fila"
  navigation:
    description: "facetas, grupos, Radar y explicación"
```

### 21.1. Flujo objetivo

```text
consulta humana
→ QueryPlan / interpretación
→ expansión anclada por Market Pack
→ búsqueda rápida sobre DuckDB local
→ delta caliente en Socrata si aplica
→ unión y deduplicación
→ verificación semántica
→ actionability
→ ranking / agrupación
→ Advisor
```

### 21.2. Socrata sigue siendo fuente de verdad

DuckDB no debe vender frescura falsa. Para consultas como “hoy”, “abierto ahora”, “últimas oportunidades”, “recién publicado”, se requiere:

```yaml
freshness_policy:
  declare_local_cutoff: true
  hot_delta_required_for_recent: true
  fallback_to_socrata_if_stale: true
  warn_if_source_unavailable: true
```

### 21.3. Benchmark local mínimo para un Market Pack congelado

```yaml
warehouse_market_pack_benchmark:
  modes:
    - classic_socrata
    - classic_local
    - semantic_retrieval_local
    - semantic_verified_local
  metrics:
    - p50
    - p90
    - timeout_rate
    - false_zero_rate
    - precision_at_10
    - recall_at_20
    - actionable_groups
    - stable_group_rate
    - divergence_rate
    - explanation_complete_rate
```

### 21.4. Adulto Mayor como benchmark, no como runtime

```yaml
adulto_mayor_for_warehouse:
  role: benchmark
  not_role: production_runtime
  source: W0S20_full_corpus_live_run
  use_for:
    - baseline_socrata
    - local_vs_live_comparison
    - cold_start_seed
    - grouped_dedupe_validation
    - actionability_validation
    - semantic_verification_validation
```

---

## 22. Roadmap recomendado v1.4

### Ahora

1. Congelar Adulto Mayor como baseline de Market Pack.
2. No hacer más tuning semántico, vocabulario ni live gates Socrata para Adulto Mayor por inercia.
3. Usar Adulto Mayor W0S.20 como benchmark para el spike W0D de warehouse local.
4. Priorizar `warehouse/w0d-discovery-slice`.
5. Validar `classic_local` vs `classic_socrata`.
6. Medir recuperación local, dedupe, actionability, divergencias, falsos ceros y latencia.
7. Mantener `user_facing_ready=false` y `runtime_applied=false`.

### Después de W0D

1. Decidir si Adulto Mayor pasa de benchmark a Market Pack interno sobre warehouse.
2. Evaluar si el pipeline local permite abrir Nodo 2.
3. Diseñar Nodo 2 en papel usando esta guía.
4. Solo codificar Nodo 2 si W0D demuestra:
   - recuperación local rápida;
   - paridad razonable con Classic;
   - benchmark cold-start;
   - provenance de expansión;
   - verificación semántica auditable;
   - delta/frescura declarado.

### Solo con tracción

1. Radar automático por Market Pack.
2. Navegación nodal basada en memoria.
3. Clasificación persistente como cache analítica.
4. Histórico nacional ampliado.
5. Grafo de relaciones.
6. Embeddings o modelos semánticos solo si reglas/anclas no bastan.

---

## 23. Regla para Nodo 2

No codificar un segundo nodo antes de W0D.

Permitido antes de W0D:

```yaml
nodo_2_pre_w0d_allowed:
  diseno_en_papel: true
  analisis_de_mercado: true
  hipotesis_de_anchors: true
  negativos_probables: true
  benchmark_plan: true
  riesgos_y_blockers: true
```

Prohibido antes de W0D:

```yaml
nodo_2_pre_w0d_forbidden:
  implementar_kernel: true
  live_gate_socrata: true
  runtime_flag: true
  clasificacion_masiva: true
  merge_con_opportunity_search: true
```

Razón:

```text
Si el cuello actual es fuente/corpus/latencia, abrir otro nodo antes del warehouse repite la fricción. Primero se construye el sustrato local; luego se multiplican Market Packs.
```

### 23.1. Protocolo para diseñar Nodo 2 en papel

```yaml
node_2_design_doc:
  market_hypothesis: ""
  users:
    - proponentes
    - funcionarios
    - veedurias
    - ciudadanos
  value_over_classic: ""
  submarkets: []
  dimensions: []
  strong_anchors: []
  contextual_terms: []
  foreign_domain_qualifiers: []
  row_blockers: []
  actionability_policy: ""
  expected_data_fields: []
  benchmark_queries: []
  negative_queries: []
  warehouse_requirements: []
```

### 23.2. Recomendación de agentes/modelos

```yaml
agent_recommendation:
  design_review:
    model: Opus_4_8
    use_for: "revisar arquitectura, riesgos y autoengaños antes de codificar"
  implementation:
    model: Sonnet_5_High
    use_for: "kernel, gates, warehouse slice, tests e integración no trivial"
  small_patches:
    model: Sonnet_5_Medium
    use_for: "documentación, tests menores, fixes de loader"
  grep_docs_formatting:
    model: Haiku
    use_for: "inventarios, búsqueda, formato, limpieza"
```

---


## 24. Hipótesis comprobadas, refutadas y pendientes

Esta sección resume las hipótesis probadas durante el ciclo Adulto Mayor W0S.13–W0S.21 y las convierte en reglas operativas para futuros nodos. Su propósito es evitar que el segundo nodo repita errores ya pagados.

### 24.1. Hipótesis comprobadas

```yaml
hipotesis_comprobadas:
  classic_no_basta_en_queries_composicionales:
    estado: comprobada
    evidencia: "Classic puede fallar por AND rígido, vocabulario literal o candidate pool irrelevante."
    regla: "No declarar ausencia de mercado solo porque Classic trae cero o ruido."

  nodo_no_es_lista_de_palabras:
    estado: comprobada
    evidencia: "Adulto Mayor solo empezó a funcionar cuando se modeló como población + modalidad + servicio."
    regla: "Todo nodo debe nacer como hipótesis de mercado, no como lista de sinónimos."

  query_signal_no_es_row_signal:
    estado: comprobada
    evidencia: "Lo que el usuario pide no prueba que una fila pertenezca al mercado."
    regla: "Separar señales de consulta y señales de fila en toda arquitectura nodal."

  blocker_no_es_qualifier:
    estado: comprobada
    evidencia: "Bienestar animal puede bloquear filas; veterinario/laboral/estudiantil puede bloquear expansión de query."
    regla: "Blockers limpian filas; qualifiers protegen intención antes de expandir."

  actionability_no_es_pertenencia:
    estado: comprobada
    evidencia: "Contratos core pero liquidados sirven como mercado/histórico, no como oportunidad."
    regla: "Separar semantic_membership de actionability."

  evidence_serialization_es_obligatoria:
    estado: comprobada
    evidencia: "Sin objeto, entidad, valor, estado, fecha, URL/referencia y result_key no hay card auditable."
    regla: "Nodo que no puede producir evidencia visible no pasa a dogfood visual."

  grouping_por_result_key_es_obligatorio:
    estado: comprobada
    evidencia: "Variantes distintas pueden recuperar el mismo proceso; mostrar filas repetidas infla conteos."
    regla: "Agrupar antes de presentar y solo decir proceso único con llave estable."

  live_gate_puede_tener_bugs:
    estado: comprobada
    evidencia: "W0S.19 detectó falso positivo de divergencias por comparar excluded vs no_signal como strings distintos."
    regla: "Mapear equivalencias de taxonomía antes de comparar."

  sample_pequena_puede_subestimar_accionables:
    estado: comprobada
    evidencia: "W0S.19 tuvo 3 accionables; W0S.20 con corpus completo tuvo 9."
    regla: "No juzgar volumen accionable con muestra demasiado pequeña."

  timeout_no_prueba_fallo_semantico:
    estado: comprobada
    evidencia: "Adulto Mayor pasó semántica, dedupe y divergencias; falló gate formal por timeout Socrata."
    regla: "Si el bloqueo es fuente/latencia, congelar nodo y mover inversión a warehouse."

  freeze_evita_tuning_infinito:
    estado: comprobada
    evidencia: "W0S.21 cerró Adulto Mayor como benchmark en vez de seguir afinando vocabulario."
    regla: "Cuando el bloqueo ya no es semántico, no seguir tuneando el nodo."
```

### 24.2. Hipótesis refutadas o corregidas

```yaml
hipotesis_refutadas_o_corregidas:
  mas_vocabulario_siempre_mejora:
    estado: refutada
    razon: "Más términos pueden aumentar ruido, timeouts y overfitting."
    regla: "Todo vocabulario nuevo requiere hipótesis, negativos y gate."

  si_classic_no_encuentra_no_hay_mercado:
    estado: refutada
    razon: "Classic puede fallar por AND rígido o por forma de recuperación."
    regla: "Ejecutar retrieval gap antes de declarar ausencia."

  conteo_de_filas_equivale_a_conteo_de_procesos:
    estado: refutada
    razon: "La misma oportunidad puede aparecer muchas veces por variantes de query."
    regla: "Usar result_key y dedupe estable."

  divergencia_string_equivale_a_error_real:
    estado: refutada
    razon: "Dos capas pueden usar vocabularios distintos para la misma idea."
    regla: "Usar mapa de equivalencias."

  ui_interna_bonita_equivale_a_producto:
    estado: refutada
    razon: "La cabina existe para mostrar descartes, warnings, divergencias y límites."
    regla: "Dogfood interno no es user-facing."

  oportunidad_y_market_pack_son_lo_mismo:
    estado: refutada
    razon: "El Market Pack clasifica mercado; Opportunity Search decide vigencia/oportunidad."
    regla: "No fusionar capas prematuramente."

  warehouse_reemplaza_socrata:
    estado: refutada
    razon: "DuckDB da amplitud/velocidad, pero Socrata sigue como fuente de verdad y delta caliente."
    regla: "Declarar corte local y consultar delta para frescura."
```

### 24.3. Hipótesis pendientes para W0D

```yaml
hipotesis_pendientes_W0D:
  duckdb_reduce_timeout:
    pregunta: "¿El corpus local reduce timeout efectivo de Market Packs sin perder recall?"
    metrica: "p90 local, timeout_rate local, false_zero_rate"

  classic_local_parity:
    pregunta: "¿Classic local reproduce Classic Socrata en filtros estructurados?"
    metrica: "cero falsos ceros inexplicables"

  semantic_retrieval_local:
    pregunta: "¿La expansión anclada sobre corpus local encuentra relevantes exclusivos sin desbordar ruido?"
    metrica: "recall@20, precision@10, provenance completa"

  semantic_verified_local:
    pregunta: "¿La verificación semántica reduce ruido frente a semantic_retrieval?"
    metrica: "precision@10, descartes correctos, explanation_complete_rate"

  adulto_mayor_benchmark_transfer:
    pregunta: "¿Adulto Mayor congelado sirve como benchmark estable para comparar Socrata vs DuckDB?"
    metrica: "accionables, grupos estables, divergencias, actionability distribution"
```

Regla final:

```text
Una hipótesis comprobada se vuelve invariante. Una hipótesis refutada se vuelve guardrail. Una hipótesis pendiente se vuelve gate de W0D, no excusa para tunear el nodo.
```

---

## 25. Procedimiento recomendado para crear un nodo después de W0D

Este procedimiento aplica solo después de validar el warehouse discovery slice. Antes de W0D, un segundo nodo puede diseñarse en papel, pero no debe codificarse.

### Fase 0 — Elegir nodo con hipótesis de producto

No iniciar por vocabulario. Iniciar por valor.

```yaml
node_candidate_decision:
  market_name: ""
  user_value:
    proponentes: ""
    funcionarios: ""
    veedurias: ""
    ciudadanos: ""
  problem_classic_does_not_solve: ""
  expected_use:
    - search
    - radar
    - market_analysis
    - entity_profile
    - opportunity_discovery
  decision: go|no_go|paper_only
```

Gate:

```text
Si no se puede explicar qué mejora frente a Classic Search, no abrir nodo.
```

### Fase 1 — Scope y mapa de mercado

Definir el mercado antes de escribir código.

```yaml
market_scope:
  market_id: ""
  definition: ""
  submarkets: []
  modalities: []
  services_or_components: []
  contract_functions: []
  adjacent_markets: []
  excluded_markets: []
  likely_overlaps: []
```

Salida obligatoria:

```text
docs/market_packs/<market_id>_scope.md
```

### Fase 2 — Hipótesis léxica inicial

Separar anchors, contexto, blockers y qualifiers.

```yaml
lexical_hypothesis:
  strong_anchors: []
  contextual_terms: []
  explicit_user_terms_priority: true
  row_blockers: []
  foreign_domain_qualifiers: []
  risky_generic_terms: []
```

Regla:

```text
Un término contextual no activa el nodo solo.
Un qualifier fuerte puede bloquear la expansión.
Un blocker limpia filas.
```

### Fase 3 — Corpus mínimo y negativos

Construir corpus antes de kernel.

```yaml
minimum_corpus:
  core_positive: 10
  adjacent_or_overlap: 5
  negative_noise: 10
  compositional_queries: 10
  foreign_domain_queries: 5
  hard_filter_queries:
    - region
    - entity
    - value
    - year
    - status
```

Gate:

```text
Sin negativos no hay nodo.
Sin queries composicionales no hay node-assisted candidate pool.
```

### Fase 4 — Kernel determinístico

Implementar kernel puro, sin red, sin LLM y sin runtime.

```yaml
kernel_contract:
  functions:
    - normalize_text
    - build_query_signals
    - build_row_signals
    - detect_foreign_domain_qualifiers
    - classify_row_signal
    - resolve_actionability
    - build_evidence
  forbidden:
    - Socrata_calls
    - LLM_calls
    - runtime_flags
    - Opportunity_Search_mutation
```

Gate:

```text
El kernel debe explicar su decisión con evidencia.
```

### Fase 5 — Evidence serialization desde el primer día

No esperar al final.

```yaml
row_evidence_required:
  identity:
    - result_key
    - result_key_quality
    - stable_reference_candidates
    - dataset
    - query_id
  visible:
    - object_text_or_title
    - entity_name
    - value
    - status
    - date_field
    - url_or_reference
  semantic:
    - shadow_label
    - technical_status
    - matched_terms
    - positive_subfacets
    - noise_signals
    - exclusion_reason
  actionability:
    - actionability
    - actionability_reason
    - warnings
```

Gate:

```text
Si no se puede construir una card auditable, no pasar a UI interna.
```

### Fase 6 — Offline compositional eval

Probar composición antes de live.

```yaml
offline_eval:
  dimensions_crossed:
    - population_plus_modality
    - modality_plus_service
    - service_plus_value
    - market_plus_region
    - foreign_domain_conflict
  metrics:
    - false_positive_rate
    - false_negative_rate
    - evidence_complete_rate
    - qualifier_block_rate
```

Gate:

```text
El nodo debe saber cuándo ayudar y cuándo callarse.
```

### Fase 7 — Live shadow eval

Ejecutar contra filas reales sin tocar runtime.

```yaml
live_shadow_eval:
  mode: shadow
  user_facing_ready: false
  runtime_applied: false
  metrics:
    - rows_annotated
    - core_rows
    - adjacent_rows
    - discarded_rows
    - noise_rate
    - hard_filters_preserved
    - timeout_rate
    - runtime_errors
```

Gate:

```text
Live shadow positivo habilita dogfood interno, no producción.
```

### Fase 8 — Candidate pool asistido

Solo si el nodo ya pasó como anotador.

```yaml
node_assisted_pool:
  activate_when:
    - classic_zero
    - classic_core_zero
    - classic_noise_high
    - query_compositional
  must_preserve:
    - explicit_query_terms
    - hard_filters
    - dataset_scope
    - opportunity_policy_separation
  must_block_when:
    - foreign_domain_conflict
```

Gate:

```text
El assisted pool debe mejorar recall sin romper intención.
```

### Fase 9 — Dogfood interno con grouped view

La cabina debe mostrar lo incómodo.

```yaml
internal_dogfood:
  raw_rows_view: true
  grouped_by_result_key: true
  discarded_visible: true
  divergences_visible: true
  warnings_visible: true
  classic_visible: true
  assisted_visible: true
  exports: true
```

Gate:

```text
Si la cabina no puede explicar duplicados, descartes y divergencias, no sirve.
```

### Fase 10 — Live gate con decisión explícita

```yaml
live_gate:
  sample_min: 20
  preferred_sample: 50
  required_metrics:
    - total_queries
    - evaluable_queries
    - timeout_rate
    - raw_rows
    - stable_groups
    - actionable_groups
    - market_groups
    - discarded_groups
    - verify_validity_groups
    - duplicate_ratio
    - divergence_rate
    - p90_latency
  decisions:
    - promote_to_internal_market_pack
    - keep_internal_only
    - freeze_as_benchmark
    - fix_before_pilot
    - standby
    - drop
```

Regla:

```text
La decisión del gate es recomendación humana, no interruptor automático.
```

### Fase 11 — Freeze o siguiente capa

Si el nodo pasa semántica pero falla por fuente, frescura o latencia:

```yaml
freeze_and_bridge:
  freeze_node: true
  no_more_tuning: true
  next_layer:
    - warehouse
    - delta_hot
    - latency_policy
    - data_source_gap
```

Si el nodo falla semántica:

```yaml
semantic_failure:
  options:
    - fix_kernel
    - improve_negatives
    - split_node
    - standby
    - drop
```

Gate:

```text
No arreglar infraestructura con vocabulario.
No arreglar semántica con warehouse.
```

### Fase 12 — Integración runtime bajo flag

Solo después de pasar warehouse/local, dogfood y gate de producto.

```yaml
runtime_integration:
  flag_required: true
  rollback_required: true
  classic_visible: true
  no_hard_exclusion_initial: true
  telemetry_required: true
  advisor_copy_reviewed: true
  freshness_declared: true
```

Regla:

```text
Producción es la última fase, no el premio automático de un buen benchmark.
```

---

## 26. Tesis final v1.4

```text
SECOPPAL Search encuentra.
SECOPPAL Market Packs estructuran.
SECOPPAL Semantic Verification decide qué se contrata.
SECOPPAL Advisor explica.
SECOPPAL Warehouse conserva el universo.
SECOPPAL Radar vigila.
```

Los nodos no hacen que SECOPPAL “sepa más palabras”. Hacen que entienda mejor la estructura contractual de lo que se recupera.

La versión madura debe demostrar algo concreto:

```text
Cuando Classic Search trae ruido o se queda corto por lenguaje literal, el Market Pack ayuda a recuperar, verificar, agrupar y explicar qué se está contratando realmente.
```

Y la lección de Adulto Mayor queda congelada así:

```text
Adulto Mayor deja de ser experimento.
Ahora es benchmark.
```

---

## 27. Apéndice A — Caso Adulto Mayor W0S.13–W0S.21

### A.1. Hipótesis validada

Adulto Mayor no es una palabra clave; es un mercado compuesto por población, modalidad y servicios.

```text
adulto mayor + centro de vida + alimentación
adulto mayor + asilo + Antioquia
adulto mayor + nutrición + valor alto
adulto mayor + larga estancia + oportunidad
```

La hipótesis quedó viable como Market Pack determinístico, no como sinónimo plano.

### A.2. Gates W0S.13–W0S.16A

```yaml
W0S.13:
  result: keep_with_limits
  lesson: "el nodo aporta entendimiento y exploración, pero faltaban negativos y cobertura"

W0S.13B:
  result: partial_gate
  lesson: "promedio global no basta; bucket E sin evaluables no prueba seguridad"

W0S.13C:
  result: triage_passed
  lesson: "animal/funerario deben separarse como noise/adjacent, no positive_subfacets"

W0S.14:
  result: compositional_offline_passed
  lesson: "modalidad residencial y diurna son familias distintas"

W0S.14B:
  result: candidate_for_internal_streamlit_dogfood
  lesson: "el nodo anota filas reales; no castigar al nodo por retrieval malo"

W0S.15:
  result: retrieval_gap_validated
  lesson: "Classic puede fallar por AND rígido; shadow recupera core nuevo"

W0S.15B:
  result: w0s16_justificado
  lesson: "shadow funciona, pero puede ignorar calificadores de otro dominio"

W0S.16A:
  result: builder_shadow_only_passed
  lesson: "el nodo ya sabe cuándo ayudar y cuándo callarse; preservar términos explícitos antes del truncamiento"
```

### A.3. W0S.16B — Live shadow del candidate pool

Lección:

```text
El candidate pool asistido puede rescatar core cuando Classic falla, pero debe bloquearse ante calificadores de dominio ajeno y preservar filtros duros.
```

Reglas consolidadas:

```yaml
node_assisted_candidate_pool:
  explicit_terms_first: true
  hard_filters_preserved: true
  foreign_domain_conflict_blocks: true
  user_facing_ready: false
  runtime_applied: false
```

### A.4. W0S.17 — Evidence serialization

Lección: sin evidencia visible por fila no hay card auditable ni dogfood visual.

Campos mínimos consolidados:

```yaml
row_evidence_required:
  identity:
    - result_key
    - result_key_quality
    - stable_reference_candidates
    - dataset
    - query_id
  visible:
    - object_text_or_title
    - entity_name
    - value
    - status
    - date_field
    - url_or_reference
  semantic:
    - shadow_label
    - technical_status
    - matched_terms
    - positive_subfacets
    - noise_signals
    - exclusion_reason
    - modality
    - transversal_services
  actionability:
    - actionability
    - actionability_reason
    - runtime_status
    - warnings
```

Regla:

```text
Nodo que no puede producir cards auditables no está listo para dogfood visual.
```

### A.5. W0S.18 — Internal dogfood UI

Lección: una UI interna no debe vender producto; debe mostrar lo incómodo.

Debe permitir ver:

```yaml
internal_dogfood_ui:
  raw_rows: true
  grouped_results: true
  classic_visible: true
  assisted_visible: true
  discarded_visible: true
  divergences_visible: true
  warnings_visible: true
  exports: true
  user_facing_ready: false
```

Regla:

```text
La cabina no está para verse bonita; está para encontrar mentiras, duplicados y autoengaños.
```

### A.6. W0S.18B — Grouping por result_key

Lección: muchas filas repetidas no son necesariamente ruido. Pueden demostrar que distintas formas de preguntar recuperan el mismo proceso real.

Regla:

```text
No mostrar filas repetidas como procesos.
Agrupar por result_key.
Llamar “proceso único” solo si la llave es estable.
```

### A.7. W0S.19 — Live dogfood gate

Resultado corregido:

```yaml
w0s19_live_run:
  total_queries: 20
  evaluable_queries: 15
  timeout_rate: 0.25
  filas_crudas: 98
  grupos_estables: 70
  grupos_accionables: 3
  grupos_mercado: 14
  grupos_descartados: 42
  grupos_verificar_vigencia: 11
  divergencias: 0
  decision: keep_internal_only
```

Lección crítica:

```text
El gate también puede tener bugs. En W0S.19, el primer bloqueo por divergencias fue un falso positivo del auditor por comparar taxonomías equivalentes como strings distintos.
```

### A.8. W0S.20 — Full corpus live run

Resultado:

```yaml
w0s20_full_corpus_live_run:
  executed: true
  total_queries: 50
  evaluable_queries: 37
  fallos_timeout: 13
  timeout_rate: 0.26
  filas_crudas: 204
  grupos_totales: 112
  grupos_estables: 112
  stable_group_rate: 1.0
  grupos_accionables: 9
  grupos_mercado: 28
  grupos_descartados: 56
  grupos_verificar_vigencia: 19
  duplicate_ratio: 0.45
  divergencias_filas: 0
  grupos_con_divergencias: 0
  decision: keep_internal_only
  unico_hueco_para_promover: "timeout_rate 26% > 20%"
```

Lectura:

```text
Adulto Mayor pasó el gate semántico.
No pasó el gate de confiabilidad live por timeout Socrata.
```

### A.9. W0S.21 — Freeze + Warehouse Bridge

Decisión:

```yaml
adulto_mayor:
  status: frozen_baseline_for_warehouse
  more_semantic_tuning: false
  more_socrata_live_gates: false
  user_facing_ready: false
  runtime_applied: false
  next: warehouse/w0d-discovery-slice
```

Mantra:

```text
Adulto Mayor deja de ser experimento.
Ahora es benchmark.
```

### A.10. Decisión de viabilidad

```yaml
nodos_semanticos_secoppal:
  viables: true
  modalidad_correcta: Market_Packs_deterministicos
  no_viables_como:
    - reemplazo_total_de_Classic_Search
    - LLM_semantico_libre
    - expansion_OR_sin_controles
    - runtime_publico_inmediato
    - clasificacion_masiva_prematura
  viables_como:
    - anotador_post_retrieval
    - lente_de_mercado
    - agrupador_y_explicador
    - semantic_verifier
    - base_para_Radar
    - candidate_pool_asistido_en_shadow
    - benchmark_para_warehouse
```

---

## 28. Changelog v1.2

Cambios incorporados frente a v0.1/v1.1:

- Se añade contexto y filosofía de integración runtime.
- Se formalizan Classic Search, Nodal Search y Hybrid Search.
- Se incorporan aprendizajes de Fase 2C: los nodos brillan cuando separan mención de contratación.
- Se añade el gate obligatorio A/B/C antes de runtime.
- Se define el concepto de Market Pack.
- Se aclara almacenamiento actual y recomendado.
- Se separan morfología, derivación léxica y aliases de dominio.
- Se añade política de dogfood, feature flags y telemetría.
- Se incluye roadmap de Market Packs prioritarios: infraestructura, alimentación, software, servicios sociales y dotación/mobiliario.
- Se refuerza que los nodos no sustituyen la búsqueda clásica ni deben activar hard exclusions al inicio.

---

## 29. Changelog v1.3

Cambios incorporados frente a v1.2:

- Se actualiza la tesis: los nodos siguen sin reemplazar Classic Search, pero pueden explorar candidate pools asistidos en shadow cuando Classic falla por composición.
- Se añaden conceptos nuevos: `query_signals`, `row_signals`, `foreign_domain_qualifier`, `node-assisted candidate pool` y `actionability resolver`.
- Se formalizan gates W0S.13–W0S.16A para Market Packs.
- Se introduce la distinción crítica entre blocker y foreign-domain qualifier.
- Se añade la regla de preservación de términos explícitos antes de truncar `object_or_controlled`.
- Se refuerza que verbos de intención como atender/operar/involucrar no deben volverse filtros `LIKE` obligatorios.
- Se agrega taxonomía de cero resultados: real_zero, retrieval_zero y technical_zero.
- Se agrega contrato de `judge`: no marcar regresión cuando el nodo descarta correctamente basura traída por Classic Search.
- Se exige que los invariantes usen evidencia real del kernel (`matched_terms`, `evidence_fields`), no re-escaneo parcial.
- Se amplía el checklist antes de runtime con guards negativos, qualifier guard, live shadow eval, hard filters preservados y latencia.
- Se incorpora Apéndice A: Caso Adulto Mayor W0S.13–W0S.16A.
- Se actualiza roadmap inmediato: cerrar v1.3, consolidar Adulto Mayor, ejecutar W0S.16B y luego Streamlit interno con flag si pasa.

---

## 30. Changelog v1.4

Cambios incorporados frente a v1.3:

- Se actualiza la tesis central: los Market Packs estructuran y verifican, pero el warehouse local se vuelve el sustrato para recuperación amplia, rápida y auditable.
- Se incorpora el estado `market_pack_freeze` para cerrar ciclos semánticos sin activar producto público.
- Se declara Adulto Mayor como `frozen_baseline_for_warehouse` después de W0S.20–W0S.21.
- Se reemplaza el roadmap anterior: no más tuning Adulto Mayor; prioridad a `warehouse/w0d-discovery-slice`.
- Se agregan lecciones de W0S.17–W0S.21: evidence serialization, cabina interna, agrupación por `result_key`, live gate, bug del auditor y freeze.
- Se agrega política `gold_seed_no_gold_automatico`.
- Se formaliza el `warehouse_bridge` para Market Packs: query resolution, retrieval expansion, semantic verification y navigation.
- Se establece que Nodo 2 solo se diseña en papel antes de W0D; no se codifica antes de validar el corpus local.
- Se agregan lecciones generales de pruebas: no reimplementar kernel en tests, smoke antes de live grande, mapear taxonomías, no tunear mirando el examen, secretos fuera de artifacts y timeout como problema de fuente/infra cuando la semántica ya pasó.
- Se ajusta el Apéndice A para cubrir Adulto Mayor W0S.13–W0S.21.

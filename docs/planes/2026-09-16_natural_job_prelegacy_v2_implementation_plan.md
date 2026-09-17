# SECOPPAL — Natural Job Pre-Legacy Control Plane V2
## Plan de implementación corregido

**Estado:** autorizado para experimento arquitectónico, no para merge directo  
**Base:** `main @ 6b30737`  
**Evidencia a preservar, no mergear:** `mo/natural-job-credential-v1 @ 411a03d`

> **Tesis:** el LLM acredita QUÉ trabajo quiere la persona; el host acredita CÓMO ejecutarlo.

El LLM no escribe SQL, no decide comprador, fechas, monto, dataset, modalidad ni filtros ejecutables.
El parser legacy no interpreta turnos que ya tienen una credencial semántica válida.

---

## 1. Problema que corrige esta misión

Hoy, simplificando:

```text
Natural Alpha
  → propone ANALYZE / SUPPLIERS
  → registry reduce la observación a una capability
  → pipeline legacy interpreta toda la oración
  → QueryRouter crea params/objeto/residuos
  → A2 intenta limpiar o rechazar esos params
  → capability ejecuta o cae a legacy
```

Esto produce dos defectos estructurales:

1. el LLM entiende paráfrasis humanas como `con quién ha contratado`, pero el host vuelve a exigir vocabulario literal como `proveedor`;
2. el parser legacy puede contaminar la consulta antes de que la capability tome ownership (`vende`, `compra`, `quien`, etc. terminan como `objeto`).

**V2 no añade más vocabulario. Cambia el orden de autoridad.**

---

## 2. Arquitectura objetivo

```text
Usuario
  ↓
Controles estructurados preexistentes
  ↓
Natural Alpha
  ↓
CapabilityCredential válida?
  ├─ NO → legacy actual, sin cambios
  └─ SÍ
       ↓
  Semantic Admission
       ↓
  CapabilitySpec
       ↓
  Authorities independientes sobre RAW QUERY
       ├─ Buyer
       ├─ Time
       ├─ Actor ownership
       ├─ Ranking / Control
       ├─ Source
       ├─ Polarity / Cardinality cuando corresponda
       └─ Residual material
       ↓
  accredited_params
       ↓
  Frame V1.2 / Profile Route existente
       ↓
  compile → execute → publish
```

### Invariante central

```text
QUERY_ROUTER_CALLS_ON_CREDENTIALED_TURNS = 0
LEGACY_FALLBACK_AFTER_VALID_CREDENTIAL = 0
```

No se crea un tercer runtime de Entity Profile.
El carril nuevo entra por **Admission + Frame V1.2** ya existente.

---

## 3. Principios de autoridad

### 3.1 Natural Alpha es autoridad del JOB

Puede acreditar:

```text
ANALYZE / SUPPLIERS
ANALYZE / MODALITIES
ANALYZE / OVERVIEW
...
```

No puede acreditar:

```text
buyer
actor
fecha
monto
modalidad
estado
geografía
dataset
SQL
QueryPlan
limit
ordering ejecutable
```

### 3.2 Host es autoridad de parámetros

Cada dimensión ejecutable conserva un único dueño canónico:

- buyer → `EntityResolver` / autoridad institucional;
- tiempo → `resolve_temporal_scope`;
- actor → autoridad Actor Profile existente;
- ranking/control → Control Language / autoridad canónica;
- cardinalidad → autoridad cardinal existente;
- source → autoridad de fuente existente.

**Capability Binding no vuelve a reinterpretar esas dimensiones.**

### 3.3 Frame es autoridad del significado confirmado

Una vez admitido el turno, Frame conserva:

- owner;
- scope;
- claims acreditados;
- continuidad;
- plan;
- publicación.

---

## 4. Fase 0 — Corpus, prompt y baseline antes del runtime

### 4.1 Corpus DEV

Preservar los 38 casos de NJC V1 como DEV/regression.

Añadir incidentes reales ya observados:

- `con quien ha contratado la alcaldia de galapa en 2025`
- `con quien ha contratado mas la alcaldia de cartagena en 2026`
- `cuales son los proveedores con contratos mas grandes de alcaldia de barranquilla`

No usar este corpus como holdout de Prompt V3.

### 4.2 Prompt Natural Alpha V3

Congelar **antes de cualquier replay V3**.

Definiciones conceptuales:

```text
SUPPLIERS:
preguntas sobre qué personas o empresas reciben contratos,
venden, proveen o son adjudicatarias de una entidad.

CONTRACTS / SEARCH:
preguntas sobre qué contratos, objetos, adquisiciones o servicios
ha contratado una entidad.

TOP_ENTITIES:
comparaciones o rankings entre entidades compradoras.
```

#### Evidence quote

Pedir el fragmento contiguo mínimo que por sí solo demuestra el trabajo.

Idealmente no debe absorber buyer, fecha, monto, territorio ni topic.

**Esto es una preferencia del modelo, no una garantía de seguridad.**

Congelar prompt hash, modelo, temperatura y schema.

No modificar V3 después de ver resultados.

### 4.3 Holdout independiente

Después de congelar V3:

- otro agente/contexto fresco crea 15–20 casos;
- quien escribió V3 no modifica el prompt;
- incluir SUPPLIERS claros, near-misses y negativos;
- registrar `n` exacto.

---

## 5. CapabilityCredential

Reutilizar la idea validada en NJC V1:

```text
CapabilityCredential
  capability
  action
  aspect
  evidence_interval
  source = NATURAL_ALPHA
```

La credencial acredita **solamente el job**.

No acredita ninguna dimensión ejecutable.

Si el intervalo no corresponde al mismo raw text:

```text
credential invalid
```

---

## 6. Verificación determinista de sobrealcance de evidence

El plan no puede depender de que DeepSeek siempre cite el span mínimo.

Authorities leen siempre el RAW QUERY completo.

El `evidence_interval` sólo puede participar en el cálculo de residuo si no invade spans host-owned.

Comprobar overlap al menos con:

- buyer;
- actor;
- time;
- money;
- modality;
- geography;
- source;
- ranking/control;
- cardinality.

Si evidence está sobreextendido:

```text
credential sigue acreditando el JOB
evidence_consumes_residual = false
```

Nunca:

```text
overbroad evidence → silencio de filtro → execute
```

Caso obligatorio:

```text
"proveedores de galapa en obras civiles"
```

Si evidence traga toda la oración:

- SUPPLIERS puede seguir siendo el job;
- `Galapa` sigue siendo buyer/territory según authority;
- `obras civiles` sigue visible como residuo material;
- resultado: CLARIFY, no EXECUTE.

---

## 7. CapabilitySpec — control plane genérico

No escribir un handler gigante hardcoded de SUPPLIERS.

Ejemplo conceptual:

```text
CapabilitySpec(
    id = "entity_profile.suppliers",
    owner = ENTITY_PROFILE,
    focus = "suppliers",
    required = {BUYER},
    optional = {TIME, RANKING},
    supported_cardinality = {NONE, EXACT_5},
    unsupported = {
        MONEY,
        TOPIC,
        INDEPENDENT_GEOGRAPHY,
        ...
    }
)
```

El control plane:

1. recibe credential;
2. obtiene spec;
3. invoca authorities;
4. calcula residuo;
5. decide EXECUTE / CLARIFY / OTHER_OWNER.

La siguiente capability debe añadirse principalmente con otra `CapabilitySpec`, no con otro router.

---

## 8. Subir Natural Alpha antes de Admission

Conservar precedencia de los controles estructurados existentes:

- reset;
- actor selection;
- radar;
- pagination;
- suggestion selection;
- direct lookup.

Luego ejecutar Natural Alpha una sola vez:

```text
structured controls
  ↓
Natural Alpha
  ↓
credential
  ↓
Admission
```

No duplicar la llamada.
Un turno, una observation, un receipt.

---

## 9. Semantic Admission en Frame V1.2

Modificar Admission para aceptar:

```text
credential: CapabilityCredential | None
```

### Sin credential

Comportamiento legacy actual.

### Con credential

**Prohibido:**

```text
deps.query_router.parse(...)
```

En su lugar:

```text
authority_assembler(raw_query, credential, capability_spec)
```

produce sólo params acreditados.

Admission selecciona PROFILE / CLARIFY / OTHER_OWNER.

El carril credenciado debe entrar al runtime existente de Frame V1.2, incluso sin usar el parser tabular legacy.

No crear un tercer runtime.

---

## 10. Buyer Authority

Reutilizar la autoridad existente sobre raw text.

Preferir:

- `scan_entity()` exact/longest match;
- grupos institucionales declarados;
- aliases canónicos acreditados.

V2 inicial:

```text
exact / declared group → continuar
fuzzy / ambiguous / bare territory → CLARIFY
```

No buyer regex nueva.
No LLM buyer.

Debe conservar span/provenance para residual y claims.

---

## 11. Time Authority

Usar directamente:

```text
resolve_temporal_scope(raw_query)
```

Convertir a:

```text
fecha_desde
fecha_hasta
```

Ambigüedad/error → CLARIFY.

No QueryRouter.

---

## 12. Actor Ownership

Usar la autoridad Actor existente sobre raw/normalized text.

Si detecta actor como sujeto real:

```text
OTHER_OWNER → Actor Profile
```

SUPPLIERS no puede robarlo.

No duplicar gramática de Actor Profile.

Si la autoridad actor no puede invocarse limpiamente sin copiar grandes trozos:

```text
STOP
```

---

## 13. Ranking / Control

Un solo dueño canónico.

No repetir:

```text
Control Language = valor_desc
A2 vuelve a interpretar
→ contradicción
```

Para SUPPLIERS:

- ranking por cantidad;
- ranking por valor.

Ordering no soportado → CLARIFY.

No regex paralela de A2.

---

## 14. Source / Universe

La capability declara su universo soportado.

SUPPLIERS usa contratos.

Si el usuario expresa una fuente incompatible:

```text
CLARIFY
```

No dejar que legacy decida dataset desde residuos.

---

## 15. Polaridad

La credential acredita el JOB, nunca el signo.

### Con mención supplier literal

Usar autoridad determinista existente.

### Sin mención literal

Si existe negación con alcance no acreditable:

```text
CLARIFY
```

No asumir positivo porque Alpha dijo SUPPLIERS.

Supplier Semantics sidecar:

- no eliminar del código;
- no hacerlo requisito del happy path V2;
- no llamarlo si no existe mention localizada;
- no crear otro sidecar para salvar el caso principal.

---

## 16. Cardinalidad

Mantener:

```text
NONE
EXACT(5)
```

pueden ejecutar.

Cualquier otra → CLARIFY.

No nuevo schema.
No X3.
No vocabulario incremental.

---

## 17. Semantic Residual View

NO usar:

```text
objeto
objeto_or
objeto_phrases
```

producidos por QueryRouter.

El residual se calcula sobre RAW QUERY a partir de spans acreditados.

Se consumen sólo spans seguros:

- evidence span del job si pasa overreach check;
- buyer;
- time;
- actor;
- source;
- ranking/control;
- cardinalidad;
- demás dimensiones estructuradas;
- gramática funcional general cerrada ya existente.

Material significativo restante:

```text
CLARIFY
```

Nunca convertir automáticamente resto a topic SECOP.

### Invariante

No añadir:

```text
vende
compra
adjudica
trabaja
gana
...
```

a una whitelist para mejorar score.

Si hacen falta, el job/evidence/control plane está mal.

---

## 18. Reusar Frame V1.2

Con `accredited_params`, reutilizar:

```text
run_profile_route
run_profile_turn
collect_claims
compose_patch
compile_profile
execute_plan
publish
```

Adaptar sólo costuras que hoy suponen params legacy.

No duplicar:

- profile compiler;
- execution plan;
- rendering;
- publication;
- follow-up state.

Frame es owner terminal.

---

## 19. Follow-up

No construir otro sistema conversacional.

Un turno credenciado debe publicar suficiente ownership/frame para que el siguiente no caiga silenciosamente a legacy incompatible.

Para esta misión:

- first-turn completo;
- follow-up simple conserva owner o aclara;
- follow-up complejo puede CLARIFY honestamente.

No resolver toda la conversación todavía.

---

## 20. Dos bugs legacy independientes

### 20.1 `^con .+$`

No debe apropiarse de interrogativas:

```text
con quien...
con qué...
```

Fix gramatical/general, no SUPPLIERS-specific.

### 20.2 Trace UI

Mostrar:

```text
Natural Alpha:
  called / status / action-aspect

Semantic Credential:
  capability / evidence / overreach

Admission:
  route / owner

Authorities:
  buyer / time / actor / ranking / source

Frame:
  focus / decision

Supplier Semantics:
  called / not-called

Legacy:
  BYPASSED / USED
```

No usar `LLM: No` como resumen global.

---

## 21. Instrumentación obligatoria

Contadores reales:

```text
natural_alpha_calls
credential_created
credential_overreach
query_router_calls
legacy_llm_calls
legacy_graph_entered
authority_calls_by_dimension
source_calls
capability_execute
capability_clarify
other_owner
latency_total
latency_alpha
latency_authorities
latency_profile
```

Gate estructural:

```text
credential válido
→ query_router_calls == 0
→ legacy_graph_entered == 0
```

Mutation test:

si alguien vuelve a llamar QueryRouter en el branch credenciado, el gate debe morir.

---

## 22. Dos veredictos separados

### 22.1 CLASSIFIER VERDICT

Errores como:

```text
"que ha contratado galapa"
→ SUPPLIERS
```

son `CLASSIFIER_FAILURE`.

No falsan por sí solos el control plane.

Hard gate end-to-end:

```text
WRONG_CAPABILITY_EXECUTED = 0
```

Para medir control plane por separado, permitir replay con credentials oracle/inyectadas, claramente separado.

### 22.2 CONTROL PLANE VERDICT

Con credential correcta:

```text
WRONG_BUYER = 0
SILENT_FILTER_LOSS = 0
ACTOR_STOLEN = 0
NEGATED_JOB_EXECUTED = 0
UNSUPPORTED_CARDINALITY_EXECUTED = 0
LEGACY_FALLBACK_AFTER_VALID_CREDENTIAL = 0
QUERY_ROUTER_CALLS_ON_CREDENTIALED_TURNS = 0
LEGACY_GRAPH_ENTERED_WITH_VALID_CREDENTIAL = 0
```

Sólo PASS/PASS permite promoción end-to-end.

---

## 23. Métrica de utilidad

No usar `EXECUTE + CLARIFY = handled` como titular.

Separar:

```text
CLEAR_SUPPLIERS_EXECUTION_RATE
HONEST_CLARIFY_RATE
WRONG_EXECUTION_RATE
```

Sobre casos SUPPLIERS claros y soportados:

```text
CLEAR_SUPPLIERS_EXECUTION_RATE >= 90%
```

Reportar `10/11`, `11/11`, etc., no una falsa estadística poblacional.

---

## 24. Runs

- DEV/regression: corpus existente;
- HOLDOUT: 15–20 casos independientes;
- 2 corridas temp 0;
- reportar labels inestables;
- no majority vote para hard gates.

---

## 25. Incidentes obligatorios

Sin legacy:

```text
"con quien ha contratado la alcaldia de galapa en 2025"
→ SUPPLIERS / Galapa / 2025
```

```text
"con quien ha contratado mas la alcaldia de cartagena en 2026"
→ SUPPLIERS / Cartagena / 2026
```

```text
"cuales son los proveedores con contratos mas grandes de alcaldia de barranquilla"
→ SUPPLIERS / Barranquilla / ranking valor
```

Controles:

```text
"que ha contratado galapa este año"
→ NO SUPPLIERS
```

```text
"contratos de Fundacaribe"
→ Actor Profile / NOT SUPPLIERS
```

```text
"no me muestres con quien ha contratado Galapa"
→ CLARIFY / 0 source calls
```

```text
"proveedores de educación de Secretaría de Educación de Cali"
→ CLARIFY / filtro material visible
```

```text
multi-job
→ comportamiento seguro
```

---

## 26. Stop Conditions

STOP inmediato si:

- hay que copiar/reimplementar gran parte de QueryRouter;
- aparece una whitelist de verbos SUPPLIERS;
- se crea un tercer runtime de perfil;
- el LLM empieza a emitir parámetros ejecutables;
- hace falta Topic Evidence general nuevo;
- hace falta otro sidecar para el happy path;
- el control plane requiere reglas por paráfrasis en vez de authorities genéricas;
- Frame V1.2 necesita duplicarse en vez de adaptarse.

---

## 27. Criterio de abandono

Esta es la última prueba seria del enfoque.

### Arquitectura falsada si

Con credentials correctas:

```text
CONTROL_PLANE hard gates != 0
```

o:

```text
CLEAR_SUPPLIERS_EXECUTION_RATE < 90%
```

por defectos del control plane/residual/authorities.

Resultado:

```text
NO_GO_ARCHITECTURE
PARK / abandonar esta vía
```

No otra versión de routing.

### Si clasificador falla pero control plane pasa

Resultado:

```text
CONTROL_PLANE = PASS
CLASSIFIER = FAIL
```

Eso permite mejorar/reemplazar Natural Alpha sin tirar el nuevo cerebro host.

---

## 28. Gates de código

Sólo después de pasar la medición:

1. focales;
2. mutations;
3. Ruff;
4. Semgrep delta;
5. diff-check;
6. suite global vs `main`;
7. freeze SHA;
8. ZEUS fresco;
9. sólo con GO, integración.

No pagar suite global antes de saber que el diseño pasa semánticamente.

---

## 29. Authority Map entregable

| Dimensión | Autoridad | Input | Puede conceder | Puede vetar | Salida |
|---|---|---|---|---|---|
| Job | Natural Alpha | raw text | capability credential | sí | action/aspect/evidence |
| Buyer | Entity authority | raw text | buyer exact/group | sí | canonical buyer + span |
| Time | Temporal authority | raw text | period | sí | dates + span |
| Actor owner | Actor authority | raw text | other owner | sí | actor ownership |
| Ranking | Control authority | raw text | ordering supported | sí | canonical ordering |
| Residual | Admission | accredited spans | no | sí | clean/clarify |
| Capability | CapabilitySpec | accredited params | execution support | sí | focus |
| Frame | Frame V1.2 | claims | confirmed meaning | sí | frame/plan |
| Executor | Profile runtime | compiled plan | source calls | no semantic widening | evidence |

Ninguna dimensión ejecutable debe tener dos autoridades simultáneas.

---

## 30. Entrega final

Entregar:

- branch / SHA;
- Authority Map antes/después;
- Prompt V3 + hash;
- DEV corpus hash;
- HOLDOUT hash;
- CapabilitySpec(s);
- evidence-overreach policy;
- classifier verdict;
- control-plane verdict;
- rates con `n` exacto;
- 2-run stability;
- QueryRouter call counter;
- legacy graph counter;
- latencia comparativa;
- source calls;
- mutations;
- focales;
- global delta;
- ZEUS verdict.

Veredictos finales permitidos:

```text
GO_PRELEGACY_DOGFOOD
NO_GO_CLASSIFIER
NO_GO_CONTROL_PLANE
NO_GO_ARCHITECTURE
```

---

## 31. Estrategia de modelos

### Implementador

**DeepSeek sí puede implementar este plan**, pero como `writer`, no como arquitecto final ni auditor de sí mismo.

Requisitos:

- variante competente para código/razonamiento;
- acceso completo al repo/worktrees;
- capacidad de correr tests;
- commits frecuentes;
- STOP conditions obligatorias;
- no redefinir arquitectura por su cuenta.

No usaría una variante Flash con razonamiento OFF para liderar esta cirugía.

### Coordinación

Recomendado:

```text
GPT-5.6 Sol High
```

para preflight, freezes, adjudicación de desviaciones y control de STOP.

### Auditoría

ZEUS / Monte Olimpo en contexto fresco con:

```text
GPT-5.6 Sol High
```

es suficiente para la primera revisión independiente.

### Opus

Reservarlo para:

- revisión arquitectónica final si el candidate pasa;
- desempatar una duda causal seria;
- revisar un NO_GO discutible;
- revisar el Authority Map y fronteras antes del merge.

No gastaría Opus en implementación mecánica.

---

## 32. Checkpoints de implementación

```text
C0 — corpus/prompt freeze
C1 — credential + CapabilitySpec
C2 — Natural Alpha antes de Admission
C3 — credentialed Admission con QueryRouter=0
C4 — authorities limpias
C5 — Frame V1.2 end-to-end
C6 — DEV replay
C7 — blind holdout
C8 — gates de código
C9 — ZEUS
```

Commit en cada checkpoint.

Si aparece necesidad de:

- parser paralelo;
- vocabulario creciente;
- LLM params;
- tercer runtime;

STOP inmediato.

---

## 33. Resultado esperado si la tesis funciona

```text
"con quien ha contratado mas la alcaldia de cartagena en 2026"
```

debe verse así:

```text
Natural Alpha
  → ANALYZE / SUPPLIERS

Credential
  → valid

Admission
  → PROFILE / entity_profile.suppliers

Buyer Authority
  → Cartagena

Time Authority
  → 2026

Actor Authority
  → no competing owner

Ranking
  → supported

Residual
  → clean

QueryRouter
  → 0 calls

Legacy graph
  → BYPASSED

Frame
  → suppliers

Executor
  → SECOP contracts source
```

Ese es el producto que esta misión debe demostrar.

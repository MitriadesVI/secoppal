# Actor Profile V1 — guía técnica y operativa

**Estado:** entregado y vigente en `main` (`8acb21a`) · decisión canónica en
[`ADR-060`](../decisions_log.md) · **capacidad productiva ON por defecto**
**Última actualización:** 2026-09-06

> Esta guía es de **mantenimiento y operación**. La decisión y sus alternativas
> descartadas viven una sola vez, en `docs/decisions_log.md` (ADR-060). Aquí no
> se duplica el texto de la decisión: se explica cómo está construido, cómo se
> opera y cómo se extiende sin romper invariantes.

---

## 1. Propósito

Responder preguntas sobre **un actor contractual** (proponente / contratista):

- *¿qué contratos tiene X?*
- *¿qué ha hecho X en 2026?*
- *¿con qué comprador contrató X?*
- por **nombre**, por **NIT** o por **cédula**;
- desambiguando cuando el nombre corresponde a varios actores.

El problema que cierra no es de recall: es de **atribución**. Antes, el nombre
del actor entraba como objeto contractual y el sistema devolvía una lista con la
forma correcta y el contenido equivocado. Cuando lo que se afirma es sobre un
tercero identificable, **no responder es mejor que atribuir mal**.

---

## 2. Contrato de producto

Actor Profile V1 devuelve la **huella contractual observada en SECOP II**:

| Campo | Contenido |
|---|---|
| Nombre observado | escritura cruda publicada por la fuente |
| Documento tipado | `NIT` o `CC`, completo |
| `queried_at` | instante con zona en que se consultó |
| Conteo de contratos | filas observadas |
| Valor contractual reportado | suma publicada, no "valor ejecutado" |
| Rango de actividad | primer y último año observados |
| Actividad por año | distribución |
| Principales compradores | agregado |
| Modalidades | agregado |
| Territorios | agregado |
| Contratos recientes | muestra con enlaces a SECOP |

**Lo que NO es** — y el render lo declara explícitamente:

- no es un dossier legal;
- no es certificación RUP ni RUES;
- no es informe de riesgo;
- no es certificado de antecedentes;
- no emite juicios agregados ("proveedor riesgoso", "contratista frecuente") ni
  ordena actores en ninguna escala (`INV-NO-VERDICT`).

---

## 3. Arquitectura

```text
                         turno del usuario
                                │
                   ┌────────────▼─────────────┐
                   │ actor_conversation.py    │  operaciones y roles
                   │ (puro, sin identidades)  │  SET_ACTOR · REPLACE_ACTOR
                   └────────────┬─────────────┘  SELECT_ACTOR_CANDIDATE
                                │                SET_BUYER · SET_TERRITORY
                   ┌────────────▼─────────────┐
                   │ actor_intent.py          │  ¿es consulta de actor?
                   │ actor_arbitration.py     │  precedencia vs tema/geografía
                   └────────────┬─────────────┘
                                │
        ┌───────────────────────▼────────────────────────┐
        │ workflow/actions/actor_resolution.py           │  orquestación impura
        └───────┬────────────────────────────────┬───────┘
                │                                │
   ┌────────────▼───────────┐      ┌─────────────▼──────────────┐
   │ actor_identity.py      │      │ data/actor_alias_resolver  │
   │ IdentityKey tipada     │◄─────┤ SQLite local + FTS5 trigram│
   │ actor_document_types   │      │ (data/actor_alias_index)   │
   └────────────┬───────────┘      └─────────────┬──────────────┘
                │                                │
   ┌────────────▼────────────────────────────────▼──────────────┐
   │ actor_resolution.py — estado × método, predicado de        │
   │ autoridad (AUTHORITATIVE_ACTOR_METHODS)                    │
   └────────────┬───────────────────────────────────────────────┘
                │  identidad autoritativa
   ┌────────────▼───────────┐        ┌──────────────────────────┐
   │ actor_scope_policy.py  │        │ workflow/handlers/       │
   │ INV-NO-SILENT-DROP     │        │ actor_selection.py       │
   └────────────┬───────────┘        └──────────────────────────┘
                │
   ┌────────────▼──────────────────────────────────────────────┐
   │ actor_profile/  turn → assembler → aspects → models       │
   │                 → render (un texto para todos los canales) │
   └───────────────────────────────────────────────────────────┘
```

### Módulos runtime

| Archivo | Responsabilidad |
|---|---|
| `app/core/actor_identity.py` | `IdentityKey`, normalización, DV como evidencia |
| `app/core/actor_document_types.py` | compatibilidad tipo AP1 ↔ tipo publicado por la fuente |
| `app/core/actor_name.py` | normalización N2, `ActorResolutionResult/Candidate/Status`, formato de identidad para usuario y máscara diagnóstica |
| `app/core/actor_intent.py` | detección de intención de actor (no resuelve identidad) |
| `app/core/actor_arbitration.py` | arbitraje entre las dos puertas de nombre; evidencia temática vs. de entidad |
| `app/core/actor_resolution.py` | contrato puro estado × método + predicado de autoridad + token de lista pendiente |
| `app/core/actor_legal_forms.py` | equivalencias de cola `S.A.S.↔SAS`, `S.A.↔SA`, `E.S.E.↔ESE` |
| `app/core/actor_scope_policy.py` | capacidad de dataset para alcance de actor |
| `app/core/actor_conversation.py` | álgebra conversacional determinista |
| `app/core/actor_candidate_ui.py` | modelo de presentación de candidatos, neutral al transporte |
| `app/core/actor_profile/` | `turn` · `assembler` · `aspects` · `models` · `render` |
| `app/core/workflow/actions/actor_resolution.py` | orquestación impura hacia `QueryPlan` |
| `app/core/workflow/handlers/actor_selection.py` | reanudar una resolución pendiente sin perder el `QueryFrame` |
| `app/data/actor_alias_index.py` | builder del artefacto SQLite |
| `app/data/actor_alias_resolver.py` | resolver local precision-first |
| `app/data/actor_alias_source.py` | adquisición acotada desde SECOP II |
| `app/config.py` | flag y ruta del índice |

### Scripts

| Script | Uso |
|---|---|
| `scripts/build_actor_alias_index.py` | construir el directorio completo |
| `scripts/audits/actor_directory_coverage.py` | cobertura del artefacto |
| `scripts/audit_actor_legal_form_variants.py` | auditoría de equivalencias de forma jurídica |
| `scripts/actor_resolution_benchmark.py` · `scripts/benchmark_actor_alias_resolver.py` | benchmark de resolución/latencia |
| `scripts/audits/actor_profile_v1_adversarial.py` | corpus adversarial congelado |
| `scripts/run_actor_conversation_mutations.py` · `scripts/run_actor_candidate_ux_mutations.py` | campañas de mutación |

---

## 4. Modelo de identidad

```python
IdentityKey(document_type, base_number)   # NIT | CC en V1
```

La llave es lo que **une** fuentes. La procedencia (`IdentityMatch`) es lo que se
**audita**: crudo verbatim, origen, DV separado cuando se pudo, y por qué se
decidió el tipo.

**Invariantes:**

- `INV-ID-TYPE` — el mismo número bajo dos tipos declarados distintos **no** es
  automáticamente el mismo actor.
- `INV-NO-LENGTH` — la longitud **jamás** decide el tipo documental.
- `INV-PROVENANCE-SEPARATION` — la llave no lleva `source`, `raw_value`,
  `check_digit` ni `confidence`; si los llevara, dos vistas del mismo actor
  dejarían de comparar iguales.

**Dígito de verificación.** Es evidencia de **calidad del dato**, no autoridad de
tipo. Un DV válido no degrada una `CC` declarada a `NIT` (un número cualquiera
satisface el módulo 11 con probabilidad ≈1/11). Un DV inconsistente se anota como
`CHECK_DIGIT_MISMATCH` y **no** reescribe el número publicado:

```text
fuente publica  900123456-7
  ├─ identidad declarada → IdentityKey("NIT", "900123456")   (intacta)
  └─ DV declarado 7 vs calculado 8 → CHECK_DIGIT_MISMATCH    (calidad)
```

---

## 5. Capacidad de la fuente

La huella se construye sobre **SECOP II Contratos electrónicos, `jbjy-vk9h`**,
proyectando los campos reales de proveedor y documento.

El almacén histórico de **procesos no es verdad de Actor Profile**: no publica
autoridad equivalente de documento del proveedor por el mismo camino.

**Filtro por tipo de la fuente** (`actor_document_types.py`): se excluye solo lo
que **contradice** la identidad. `"No Definido"` y `"Otro"` se conservan siempre.
Un filtro de igualdad `tipodocproveedor = 'NIT'` cambiaría falsas atribuciones
por omisiones silenciosas — verificado: cuatro de los 4.230 contratos del NIT
860.002.400 están publicados como `"Otro"`, y hay 10.073 filas con documento
utilizable y tipo indeterminado.

**`INV-NO-SILENT-DROP`** (`actor_scope_policy.py`): si la consulta trae alcance de
actor y el dataset objetivo no puede proyectarlo, **no se ejecuta** — se pide
aclaración. El actor nunca se descarta para seguir con los filtros restantes.

---

## 6. Directorio local de aliases

Artefacto SQLite reproducible, construido desde `jbjy-vk9h` y reemplazado
atómicamente. Clasificaciones de build: `INDEXED_STABLE`, `INDEXED_AMBIGUOUS`,
`EXCLUDED_DIRTY` (los sucios viven solo en la tabla de auditoría; nunca entran a
`actor_aliases` ni a FTS, así que no pueden convertirse por accidente en
evidencia de auto-resolución exacta).

**Build completo aceptado (AP3-E / AP3-E1):**

```text
SOURCE_ROWS_CONSIDERED       1.235.591 grupos
OBSERVED_CONTRACT_ROWS       5.784.468
INDEXED_IDENTITIES           1.214.351   (CC 1.105.444 · NIT 108.907)
IDENTITY_COVERAGE            98,84%
CONTRACT_ACTIVITY_COVERAGE   98,13%      (5.676.542 / 5.784.468)
EXACT_AMBIGUOUS_ALIASES      14.305      (1,18%)
DIRTY_EXCLUDED               15.936      (1,30%)
INDEX_SIZE 662 MB · BUILD_DURATION 1.175 s · PEAK_MEMORY 3,02 GB
```

**Qué es y qué no es.** Es infraestructura de **recuperación de candidatos** y
búsqueda. **No** es verdad RUES, **no** es verdad RUP, **no** prueba existencia
legal y **no** certifica registro alguno.

El SQLite generado está **ignorado por git y nunca se commitea**.

---

## 7. Resolución por nombre

Orden de autoridad en `actor_alias_resolver.py`:

1. alias N2 exacto asociado a **una sola** `IdentityKey`: NIT conserva `RESOLVED`;
   CC requiere ausencia confirmada de extensiones N2 bajo otras CC (ADR-094);
2. alias N2 exacto asociado a **varias** `IdentityKey` → `AMBIGUOUS`;
3. sin exacto, FTS5 trigram/BM25 → **`CANDIDATES` solamente**.

No importa RapidFuzz, ni clientes HTTP, ni LLM, ni routing. Artefacto ausente,
corrupto o de esquema incompatible → `INDEX_UNAVAILABLE` **sin fallback**.

### Colisión de prefijo personal exacto — ADR-094

Un exacto único CC entre todos los tipos documentales deja de auto-resolver si
otra CC posee un alias N2 que empieza por `query + " "` y tiene tokens adicionales.
Todas las longitudes están cubiertas. La misma IdentityKey no desafía el exacto;
NIT y ambigüedad exacta anterior conservan su comportamiento.

`actor_exact_prefix.py` separa existencia, enumeración y presentación. Hay tres
resultados de existencia: ausencia confirmada, FOUND con testigo e INCONCLUSIVE.
Este último bloquea perfil, nuevas sondas y fuzzy. Tras FOUND se muestran exacto
original y hasta nueve challengers, deduplicados antes del cap y ordenados por
menor alias representante, tipo y número. La décima identidad prueba has_more;
un fallo conserva exacto + testigo y has_more desconocido/incomplete verdadero.
En conflicto actor/tema no se abre el nuevo pendiente prefix hasta que haya
intención de proveedor explícita; las ambigüedades anteriores conservan su UI.
No hay total exhaustivo. El pendiente transporta metadata y la traza conserva
`candidates_count`; documento o selección explícitos salen de la aclaración.

Esquema/N2 intactos y ambas SQL con rango nominal sobre autoindex existente.
Presupuestos 5/20 ms fijados desde BEFORE; VM y streaming usan deadline monotónico.
**Implementado en rama, promoción NO_GO** por la primera medición AFTER. Contrato,
evidencia y límites en [ADR-094](../decisions/2026-09-06_ADR-094_actor-exact-personal-prefix-collision.md)
y [el informe](../audits/ACTOR_EXACT_PERSONAL_PREFIX_COLLISION_V1.md).

### Variantes de forma jurídica

Equivalencias deterministas de **cola** (`actor_legal_forms.py`):
`S.A.S. ↔ SAS`, `S.A. ↔ SA`, `E.S.E. ↔ ESE`.

**N2 no se reconstruyó globalmente.** La capa está guardada en tiempo de consulta
y no reescribe tokens intermedios ni elimina la forma jurídica. Auditoría:
1.187.243 aliases · 36.965 clases de equivalencia · **77 equivalencias nuevamente
ambiguas** · **0 identidades equivocadas**. Las ambiguas **fallan cerrado**.
Método resultante: `exact_legal_form_variant`.

### Mención completa y geografía desnuda

- `INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION` — un fragmento más corto de una
  mención natural más larga **no** adquiere autoridad por ser alias exacto.
- `INV-BARE-GEOGRAPHY-PRECEDENCE` — *"contratos de Barranquilla"* sigue siendo
  interpretación geográfica cuando el contexto lo sostiene. Un alias de actor
  homónimo no secuestra la consulta. El lenguaje explícito de actor
  (*proveedor*, *contratista*, *qué ha contratado*) sí puede sobreponerse.

---

## 8. Modelo de autoridad

`actor_resolution.py` mantiene **dos dimensiones separadas**.

**Estado** — *qué ocurrió*:

| Estado | Significado |
|---|---|
| `RESOLVED` | una identidad determinada |
| `AMBIGUOUS` | alias exacto con varias identidades o colisión estructural CC |
| `CANDIDATES` | solo recuperación léxica |
| `UNRESOLVED` | nada utilizable |
| `INDEX_UNAVAILABLE` | artefacto ausente/incompatible |
| `INCONCLUSIVE` | no terminó el chequeo requerido de extensión exacta CC; no autoriza ejecución |

**Método** — *por qué una identidad es autoritativa*
(`AUTHORITATIVE_ACTOR_METHODS`):

| Método | Autoritativo |
|---|---|
| `explicit_document` | sí |
| `exact_unique_alias` | sí |
| `exact_legal_form_variant` | sí |
| `user_selected_candidate` | sí |
| `name_unresolved` | **no** |

**La recuperación difusa nunca es autoridad** (`INV-NAME-NO-GUESS`). El trigram
recupera; no identifica. Un candidato solo se vuelve autoritativo por **selección
explícita del usuario**.

---

## 9. Conversación natural de actor

`actor_conversation.py` reconoce **operaciones y roles, nunca identidades**. Un
nombre de actor sigue pasando por `ActorNameResolver`, y un comprador por
`EntityResolver`.

**Lenguaje soportado:** `busca proveedor X` · `busca contratista X` ·
`muestrame contratista X` · `información de X` · `qué contratos tiene X` ·
`qué ha hecho X` · `historial contractual de X` · `contratos de X con Y`.

**Operaciones:** `SET_ACTOR`, `REPLACE_ACTOR`, `SELECT_ACTOR_CANDIDATE`,
`SET_BUYER`, `SET_TERRITORY`.

**Precedencia (la aplican los callers, en este orden):**

1. selección de candidato pendiente;
2. `SET_ACTOR` / `REPLACE_ACTOR` explícito;
3. `SET_BUYER` / `SET_TERRITORY` contextual;
4. router genérico.

**Roles contextuales con actor activo:** `CON <entidad>` → comprador ·
`EN <lugar>` → territorio.

```text
FUNSOCOM
qué ha hecho en 2026
con Córdoba
  → actor = FUNSOCOM · año = 2026 · comprador = Departamento de Córdoba
```

Los filtros ortogonales (tiempo, valor, modalidad, orden) los sigue gobernando el
`QueryFrame` existente.

---

## 10. Selección de candidatos

**Documento completo, siempre.** En la lista de candidatos, NIT y CC se muestran
**completos**: el documento *es* la evidencia de desambiguación.

```text
1. JORGE VERBEL MARTELO      — CC 92550735
2. JORGE LUIS VERBEL GARCIA  — CC 1020727022
```

`format_actor_identity_for_user` es la primitiva de display para usuario;
`mask_identity_key` sigue intacta y se usa en reprs, logs, excepciones y
superficies diagnósticas no orientadas al usuario.

**Clic y texto van al mismo backend.** Streamlit renderiza controles nativos
desde `actor_candidate_options(resolved_params)`; el texto (`2`, `opción 2`,
`el segundo`) llega por `parse_actor_candidate_selection`. Ambos terminan en la
misma operación `SELECT_ACTOR_CANDIDATE`, servida por:

```python
SecopalWorkflow.select_actor_candidate(candidate_rank, pending_token, *,
                                       channel="whatsapp", chat_id=None,
                                       user_identity=None) -> dict
```

y resuelta en `handle_actor_selection`.

**La UI no puede inyectar identidad.** El payload de selección contiene solo el
rango canónico y un token opaco (`pending_selection_token`) que ata la selección
a la lista pendiente vigente: un callback demorado no puede seleccionar el mismo
rango después de otra consulta de actor. Procedencia resultante:
`user_selected_candidate`.

La operación es **independiente del transporte** y por eso sirve tal cual para un
futuro callback de Telegram. **Los botones de candidato en Telegram no están
entregados todavía.**

---

## 11. Huella contractual

`actor_profile/turn.py` decide si el turno es una consulta de huella y devuelve
`None` cuando no aplica —el flujo tabular sigue su camino sin enterarse—. Corre
**después** de los analyzers a propósito: *"cuánto suma lo contratado por el NIT
X"* tiene respuesta concreta; el perfil atiende la pregunta abierta.

`assembler.py` hace fan-out acotado con presupuesto por tarea y techo global,
reusando el patrón que `observer.py` ya corre en producción. `aspects.py` es un
**registro**, no un monolito: cada aspecto declara cómo construye su SoQL, cómo
lee la respuesta y con qué método de transporte se pide. Añadir un aspecto es un
`ProfileAspect` más en la lista.

**Reglas duras:**

- `queried_at` se toma **una vez** al empezar y es el mismo para todo el perfil.
- Ningún parser lanza: una respuesta con forma inesperada produce el valor vacío
  del aspecto, no una excepción que derribe el perfil.
- `INV-PARTIAL` — un agregado secundario que falla **no** destruye el perfil, y un
  perfil incompleto **no** se presenta como completo. El vocabulario de
  `outcome.py` separa el vacío confirmado de la fuente del fallo de la fuente:
  un timeout **no** es `EMPTY`.
- `INV-SCOPE-DECLARED` — el render declara fuente, fecha y qué **no** se consultó.
- `INV-NO-VERDICT` — la huella se describe; interpretarla es del lector.

---

### Scope aplicado visible — V1 (2026-09-06)

**ACTOR_PROFILE_APPLIED_SCOPE_VISIBILITY_V1 = CLOSED; GO en esta rama.**
Suite final: 11.672 passed, 11 skipped, 14 xfailed; sin merge ni push.

`ActorProfileResult.applied_scope` transporta un `ActorProfileAppliedScope`
inmutable. El ensamblador lo añade después del fan-out usando los mismos params
resueltos. No entra en las consultas, no almacena identidad, SQL, params completos
ni trazas. `scope.py` es una proyección pura; `scope_context.py` le entrega el grupo
y catálogo de modalidades ya cargados por las consultas. No llama al resolver de
actores, EntityResolver, índices ni bases de datos. El catálogo inyectado evita
convertir un label aportado por el usuario en autoridad.

`scope_render.py` produce un bloque cerca del conteo. Buyer + año ocupan tres
líneas, incluido el título. El perfil global conserva el texto anterior byte por
byte. Un cero confirmado y filtrado dice «0 contratos observados dentro de estos
filtros»; un fallo conserva `Outcome.ERROR`, conteo desconocido y su explicación.
El scope no transforma un fallo en vacío ni provoca una consulta global adicional.

| Dimensión / params efectivos | Alcance actual y presentación |
|---|---|
| `entidad_resolved` / `entidad_like` | Llegan en `resolved_params`. Se muestra el label acreditado; si hay `entidad_like`, prevalece como en SQL y se declara coincidencia. `exact` describe autoridad del label, no promete igualdad SQL si no existe binding físico. |
| `buyer_group` | Referencia id/versión validada por el registro existente. Label oficial; nunca label o miembros del payload. Buyer explícito prevalece sobre grupo. |
| `entidad_ecosystem`, `entidad_in_candidates`, `entidad_contains_all` | Planning puede aportar conjuntos y coincidencias institucionales. Se declaran alternativas o coincidencias conjuntas; ecosystem/candidates se omiten si SQL los suprime por buyer explícito. No se convierten en un BuyerGroup. |
| `departamento_resolved`, `departamentos_in`, `ciudad` | Departamento resuelto prevalece sobre región. Se muestran valores efectivos, sin volver a resolver spans; ciudad distingue igualdad municipal resuelta de coincidencia. |
| `fecha_desde`, `fecha_hasta` | Llegan desde parser/QueryFrame y selección pendiente. Año completo se resume como periodo; límites parciales se conservan. |
| `valor_min`, `valor_max` | Límites inclusivos efectivos, incluido cero; formateador monetario existente. No modifica interpretación ni aritmética monetaria. |
| `estado`, `estado_contrato`, `estado_contrato_adicional` | Los campos específicos suprimen el estado legacy, igual que `_build_where`; valores de cada familia son alternativas, familias distintas se conservan separadas. |
| `modalidad_include_procedure_ids`, `modalidad_exclude_procedure_ids` | Única modalidad ejecutable actual. Usa labels canónicos y proyección a contratos: exclusión sin raw no se muestra; inclusión sin raw explica el predicado `1=0`. `modalidad` legacy se rechaza en el builder existente. |
| `contract_type_include` | Llegan clases canónicas; labels de tipos físicos existentes y etiquetas de presentación para familias/composites. No se confunden con modalidad. |
| `objeto_exclude` | Puede entrar al perfil por sí solo: la puerta de listado no lo excluye. Se muestran las exclusiones realmente enviadas a SQL. |
| `objeto`, `objeto_or`, `objeto_phrases` | El builder/ensamblador los admiten, pero `is_actor_profile_turn` desvía temas positivos a listado. Se prueban por invocación directa, sin cambiar routing. Frases desconocidas no generan predicado ni label. Los términos se describen como búsqueda ampliada, sin afirmar una conjunción que el builder pueda relajar. |

La ejecución entrega `resolved_params` al perfil sin una lista nueva de filtros.
La puerta existente exige actor autoritativo, contratista y dataset compatible
(`jbjy-vk9h`), y excluye orden explícito y temas positivos. Analítica puntual y
Entity Profile tienen precedencia. Los estados de proceso y la política temporal
de oportunidades pertenecen a procesos: no se presentan como una capacidad nueva
del perfil de actor. Inyectarlos en el builder directo no acredita reachability
conversacional. Los tests del constructor tampoco acreditan cobertura universal de
lenguaje natural; sí acreditan los predicados de los params ya resueltos.

Evidencia: `tests/fixtures/actor_profile_scope_before.json` conserva SQL de
`f6fa8499e060294e47df25ea2c7ba3355c7a3638`: 23 casos × 8 aspectos, transporte y dataset.
El test compara consultas ejecutadas, no solo su cantidad. Las métricas se
congelaron ejecutando el ensamblador de ese commit con el doble determinista.
Las propiedades Hypothesis verifican inmutabilidad de params/SQL/métricas,
eliminación de filtros y separación EMPTY/ERROR. El catálogo completo de
modalidades se cruza con inclusión/exclusión y disponibilidad en contratos.

`ARCON_FULL_NAME = ALREADY_PASSING`; `PENDING_NEW_ACTOR = ALREADY_FIXED`.
Regresiones Burr con nombres del dogfood, identidades sintéticas y fuente vacía;
no afirman conteos actuales del índice productivo. TRANSPORTE conserva su colisión
exacta actor/tema. Cantillo Exact Prefix Challenger estaba pendiente al cierre de ese frente;
la decisión posterior ADR-094 se implementa en esta rama con promoción NO_GO.

---

## 12. Flag y despliegue

```bash
# default productivo EN CÓDIGO (app/config.py)
SECOPPAL_ACTOR_PROFILE_ENABLED=true
```

Actor Profile forma parte del beta productivo y ya no requiere activación manual.
`SECOPPAL_ACTOR_PROFILE_ENABLED=false` es el **rollback de emergencia explícito**;
`tests/test_actor_flag_off.py` verifica que restaura el comportamiento anterior.

El índice sigue siendo un **requisito operativo independiente del flag**:

```bash
SECOP_ACTOR_ALIAS_INDEX_PATH=<ruta a un actor_alias_index.sqlite completo>
```

**Requisito de despliegue:** debe existir un directorio de aliases **completo y de
esquema compatible** antes de operar. Sin artefacto válido el resolver devuelve
`INDEX_UNAVAILABLE` y no adivina. La ruta por defecto declarada es
`data/actor_alias_index.sqlite`, relativa al despliegue; ninguna ruta absoluta de
una máquina concreta es arquitectura.

El default ON no autoriza sustitutos: ante un índice ausente, corrupto o
incompatible no hay `LIKE` legacy, fuzzy autoritativo, identidad inventada ni
relajación global.

---

## 13. Semántica de fallo

| Situación | Comportamiento |
|---|---|
| Nombre sin resolver | `name_unresolved`; el perfil **no** se ejecuta; se pide aclaración |
| Alias exacto con varias identidades | `AMBIGUOUS` → lista de candidatos; nunca se elige por frecuencia |
| Solo similitud léxica | `CANDIDATES`; requiere selección del usuario |
| Índice ausente/incompatible | `INDEX_UNAVAILABLE`, sin fallback |
| Dataset sin capacidad de actor | no se ejecuta; aclaración explícita |
| Equivalencia de forma jurídica ambigua | falla cerrado |
| Agregado secundario caído | perfil parcial declarado como parcial |
| Timeout de fuente | estado de fallo, nunca `EMPTY` |

---

## 14. Evidencia de validación

Cierre de Actor Profile (`7f24517`): **8175 passed, 2 skipped, 24 xfailed** ·
corpus de actor **41/41** · corpus natural **6/6** · mutaciones AP3-G de UX de
candidatos **15/15**.

Auditoría adversarial congelada — **1.150 casos**:

```text
FROZEN_CORPUS_SHA256  a3db00d39cc8ed54811ac3ebfea2c05c6c1e275711171295e34015b5a8df7518
P0 0 · P1 0 · identidades equivocadas 0 · secuestros temáticos 0
pérdidas de actor 0 · auto-resoluciones difusas 0
```

Estado tras SEARCH-SH1 (`8acb21a`): suite **8250 passed, 2 skipped, 24 xfailed**;
centinelas focales de Actor Profile 8 passed, sin diferencias en ningún archivo
de Actor Profile.

Informes: `docs/audits/2026-08-18_ACTOR_PROFILE_DIAGNOSTIC.md` ·
`2026-08-18_ACTOR_NAME_RESOLUTION_BENCHMARK.md` ·
`2026-08-19_ACTOR_PROFILE_V1_ADVERSARIAL_AUDIT.md` ·
`2026-08-20_AP3E_PRODUCTION_ACTOR_DIRECTORY.md` ·
`2026-08-20_AP3E1_FULL_INDEX_REMEDIATION.md`.

---

## 15. Deuda conocida

| Ítem | Estado |
|---|---|
| Latencia trigram sobre índice completo: **287–857 ms** según caché (p95 caliente / p95 en frío) | diferido, no bloqueante |
| Pico de memoria del builder: **3,02 GB** | diferido; `bounded-memory` sigue siendo el objetivo |
| **77** equivalencias de forma jurídica ambiguas | fallan cerrado; comportamiento correcto, cobertura pendiente |
| Aprovisionamiento del índice como requisito de activación | operativo |
| Mismatch histórico de dialogue corpus `GOB-DEPTO-001` | preexistente, ajeno a este frente |
| Deuda de lint del repositorio | `make lint-core` limpio; deuda en superficies legacy |

---

## 16. Reglas de extensión segura

1. **No inferir tipo documental por longitud.** Nunca.
2. **No dejar que el DV reclasifique un tipo declarado.**
3. **No añadir métodos a `AUTHORITATIVE_ACTOR_METHODS`** sin una fuente de
   autoridad real; la similitud léxica no califica.
4. **No auto-seleccionar un candidato**, por alto que sea su BM25.
5. **No descartar el alcance de actor** para poder ejecutar el resto de filtros.
6. **No commitear el SQLite** del directorio.
7. **Un aspecto nuevo del perfil = un `ProfileAspect` más**, no una ruta de
   routing nueva.
8. **No convertir la huella en veredicto**: sin scores, sin rankings de actores,
   sin etiquetas de riesgo.
9. **Al ampliar la fuente**, declarar primero la capacidad del dataset; si no
   puede proyectar actor, debe fallar seguro.

---

## 17. Siguiente fase — Actor Relationships V1 (UT / Consorcios)

**No implementado.** Fuera de alcance de V1, junto con RUP, RUES, representante
legal, SIRI, enriquecimiento de responsabilidad fiscal, `ActorId` como grafo
entre fuentes, score de riesgo, Radar por actor y grafo profundo / Neo4j.

La operación siguiente probable es una **auditoría de fuente** contra *Grupos de
Proveedores — SECOP II* (dataset probable `ceth-n4bn`). Esa auditoría debe
validarse **antes** de fijar arquitectura: hoy la composición de uniones
temporales y consorcios es una hipótesis de fuente, no una capacidad entregada.

---

## Referencias

- Decisión canónica: `docs/decisions_log.md` → **ADR-060**
- Propuesta histórica (parcialmente superada):
  `docs/planes/2026-06-29_perfil-contratista-dossier.md`
- Estado técnico canónico:
  `docs/documento_tecnico_secoppal_arquitectura_bitacora.md`
- Dimensiones semánticas de búsqueda:
  `docs/architecture/SEMANTIC_QUERY_DIMENSIONS.md`

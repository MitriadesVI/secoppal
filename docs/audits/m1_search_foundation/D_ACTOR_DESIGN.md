# M1-D-ACTOR — Diseño de la autoridad ACTOR (proveedor/contratista como filtro de SEARCH)

Fecha: 2026-10-01. Autor: METIS (sólo lectura). Base: `integration/m1-wave1` @ `7bd671e6`
(worktree `secoppal-m1-int`). Autoridad: `C0_SPINE_V5_DESIGN.md` §5.5/§7/§10, `C1_1_METIS_REVIEW.md`,
ADR-103 (P9, enmienda 2), contrato M1.0 v1.5 (§4 `ACTOR is`, §6, C1, D5), ADR-060, ADR-094,
`A2_AUTHORITY_HEALTH.md` §2, `A3_GAP_MATRIX.md`, `A4_CAPABILITY_BOUNDARIES.md` §2.4/§6.

Lo que corrí (sólo lectura): `normalize_identity` sobre las formas de documento del Gold; consultas
read-only (`mode=ro&immutable=1`) al índice local `secoppal/data/actor_alias_index.sqlite`
(esquema 1, N2, construido 2026-08-20, 1.214.351 identidades); perfiles de columnas de
`artifacts/audits/relationship_foundation_v0/observations/schema_{p6dx-8zbt,jbjy-vk9h}.json` y
`tests/fixtures/actor_sources/secop2_contratos_source_contract.json`. No corrí tests.

> **Enmienda 2026-10-01 (contrato v1.7, Gold `m1-search-gold/1.7`).** La decisión de producto P-ACT-2
> cambió respecto de la recomendación original («bloquear»): una petición PROCESOS + ACTOR **se ejecuta
> sobre CONTRATOS y se anuncia**. Donde este documento diga que PROCESOS + ACTOR bloquea (§1.5, §2, §4.5,
> §5.4, INV-ACT-8, §9, §10, §11, §12), rige la **§14**.

---

## 0. Precondición

Sí hay decisión de arquitectura: **identidad de un tercero** (atribuir contratos a una persona o
empresa), **frontera de confianza** (la cita la propone el LLM; el directorio es infraestructura de
recuperación, no verdad registral), **datos personales** (82 % de los contratos de `jbjy-vk9h` son de
cédula: 4.875.277 de 5.958.553) y **contrato entre componentes** (`ActorValue`, proyección, frontera
con Actor Profile).

## 1. DECISIÓN (resumen)

1. **Autoridad nueva y pura, que compone sin modificarlas las primitivas vigentes**:
   `normalize_identity` (ADR-060) para documentos y `ActorNameResolver.resolve_exact` (ADR-060 + ADR-094)
   para nombres, más dos guardas derivadas del directorio (genericidad y conflicto de clase). **No se
   toca** `actor_identity.py`, `actor_alias_resolver.py` ni `actor_alias_index.py`, y **no se
   reconstruye** el índice. El impacto sobre legacy y Actor Profile es cero.
2. **Dos caminos cerrados, decididos sólo por la cita**: si la cita tiene forma de documento, va por el
   camino de documento, que no consulta el directorio. Si no, va por el camino de nombre. El marcador
   de tipo documental («NIT», «cédula», «C.C.») **forma parte de la cita** y el de rol («proveedor»,
   «contratista») no (aclaración de C1, §4.1). Un número sin marcador ni DV con guion **nunca** se
   resuelve.
3. **`GENERIC_IDENTITY:ACTOR` sale de un sidecar derivado del índice**, versionado (`actor-generic@1`)
   y ligado por digest a los metadatos del índice. Criterios cerrados: topónimo de catálogo, sólo forma
   jurídica o marcador de grupo, y cabeza de alta frecuencia en el directorio. No hay listas a mano.
4. **`ActorValue` basta para M1. El contrato C1.1 no cambia.** No se modela el grupo (consorcio/UT con
   integrantes). Se declara en el eco y se deja para `actor@2` como cambio aditivo.
5. **Proyección sólo sobre CONTRATOS** (`jbjy-vk9h`): `documento_proveedor` con las variantes de
   `document_variants` más la cláusula de tipo de ADR-060. Con **PROCESOS + ACTOR** el host ejecuta
   sobre CONTRATOS y lo anuncia (enmienda P-ACT-2, §14). Nunca cambia de dataset en silencio.
6. **Datos personales = P9 = ADR-060 tal cual**. El documento completo aparece sólo en la presentación
   intencional. En trazas y diagnósticos va enmascarado, y en recibos o ledger nunca hay números ni
   nombres.
7. **CUTOVER de ACTOR** exige, además de la enmienda 2 de ADR-103, un evaluador simétrico (hoy legacy
   puntúa «OK» por el mero hecho de detectar un actor), varias adjudicaciones del Gold y una decisión
   de producto sobre los candidatos (§11).

---

## 2. Decisiones vigentes respetadas y tensiones

| Vigente | Cómo se respeta | Tensión |
|---|---|---|
| ADR-060 §1 INV-ID-TYPE, INV-NO-LENGTH; el DV es calidad, no autoridad de tipo | Tipo sólo por marcador citado o por DV con guion; un número desnudo aclara | Legacy acepta un número desnudo cuyo último dígito valida el módulo 11 (≈1/11): Foundation es más estricto (§4.2) |
| ADR-060 §4 `AUTHORITATIVE_ACTOR_METHODS`, el trigram nunca autoselecciona | Camino de nombre: sólo `exact_unique_alias` y `exact_legal_form_variant` vía `resolve_exact` (sin FTS) | `user_selected_candidate` necesita estado (M2) |
| ADR-060 §5 mención completa, precedencia de la geografía desnuda | Guarda de completitud con el contexto (sólo retira autoridad) + topónimos genéricos | El marcador de rol no puede «rescatar» un topónimo, porque el contexto no crea autoridad (§4.3) |
| ADR-060 §2 la huella sale de `jbjy-vk9h`; INV-NO-SILENT-DROP | Proyección sólo en CONTRATOS; PROCESOS + ACTOR se redirige a CONTRATOS con aviso (§14) | Gold N073 (procesos + proveedor) dice EXECUTE: adjudicado en v1.7 con `host_universe_redirect` (§10, §14) |
| ADR-060 §9 documento completo en la UX; `mask_identity_key` para logs/diagnóstico | Igual (§6) | `log_trace` legacy ya persiste `resolved_params.contratista` en claro: deuda preexistente, no se toca |
| ADR-094 colisión de prefijo personal | La hereda `resolve_exact`. `AMBIGUOUS/INCONCLUSIVE` aclaran | `INCONCLUSIVE` depende de un presupuesto de reloj monotónico (determinismo limitado, §3) |
| C0 INV-C0-7/7b/8/9, P-7; C1.1 V5a (`excluded`) | Todas las reglas de contexto son existenciales (monótonas); polaridad NEGATIVE soportada | — |
| Contrato M1.0 C1 («cita sin marcador de rol») | Se aclara que el marcador de *tipo documental* es parte del identificador | Gold X083 cita «1000000001» sin «cédula»: adjudicación (§10) |
| D5 / §6 aceptación dual | §7 | — |
| §7 del diseño: D-ACTOR dueño de `actor_alias_resolver.py`, `actor_alias_index.py`, `actor_identity.py` | Se ejerce la propiedad **congelándolos** en M1 | Las correcciones de A2 a esos módulos quedan como deuda legacy (§12) |

---

## 3. Invariantes

### 3.1 Preservados

INV-ID-TYPE, INV-NO-LENGTH, INV-NAME-NO-GUESS, INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION,
INV-NO-SILENT-DROP, INV-TYPED-PROJECTION (ADR-060). INV-C0-7, -7b, -8, -9; INV-C1.1-EXCL; P-7
(ledger sin texto libre); invariante 6 (ninguna autoridad ve el texto crudo).

### 3.2 Introducidos

| Id | Invariante | Verificación |
|---|---|---|
| INV-ACT-1 | **Un RESOLVED de ACTOR sólo sale de uno de estos dos caminos**: (a) un documento con tipo citado o con DV con guion, normalizado con `normalize_identity`; (b) un nombre resuelto por `resolve_exact` con un método de la lista cerrada {`exact_unique_alias`, `exact_legal_form_variant`}. Cualquier otro estado o método da un resultado distinto de RESOLVED. | Tabla de decisión exhaustiva (Hypothesis, §8) + mutantes M1/M3/M9/M10/M17 |
| INV-ACT-2 | **El tipo documental nunca sale del contexto ni de la longitud.** | Monotonía del kit + propiedad «número desnudo ⇒ nunca RESOLVED» |
| INV-ACT-3 | **Ninguna cabeza genérica resuelve**: para todo alias del sidecar y todo nombre de los catálogos territoriales, el veredicto es `GENERIC_IDENTITY:ACTOR`. | Barrido exhaustivo sobre el fixture (CI) y sobre el índice real (local) |
| INV-ACT-4 | **Directorio rancio ⇒ BLOCK**: si el digest de los metadatos del índice no coincide con el del sidecar, o falta cualquiera de los dos, el camino de nombre da `AUTHORITY_UNAVAILABLE:ACTOR`. Nunca un vacío silencioso. | Test con un sidecar de otro índice; mutante M14 |
| INV-ACT-5 | **Conflicto de clase**: un nombre con forma de organización (n-grama de `org_tokens`) que resuelve a una CC no resuelve (ESP → persona natural, A2 -034). | Fixture + sonda A2 -034 sobre el índice real |
| INV-ACT-6 | **Recibos sin datos personales**: `AuthorityReceipt.rules` sólo usa tokens de un vocabulario cerrado; no contiene tramos de ≥5 dígitos ni tokens de la cita; `digests` sólo del índice y del sidecar, nunca de una `IdentityKey` (una CC de 10 dígitos se recupera por fuerza bruta a partir de su hash). | Propiedad sobre todos los veredictos de la suite; mutante M13 |
| INV-ACT-7 | **`display` es la cita del usuario** (NFC, recortada), nunca un nombre sacado del directorio. Así el valor no introduce texto nuevo y el único dato derivado es el documento. | Test de valor; mutante M15 |
| INV-ACT-8 | **ACTOR ⇒ CONTRATOS**: ningún plan ejecuta ACTOR sobre PROCESOS (una petición PROCESOS + ACTOR se redirige a CONTRATOS con el aviso `ACTOR_PROCESSES_TO_CONTRACTS`, política de vista, §14) y ninguna petición con ACTOR se ejecuta sin su cláusula (núcleo, E-ACTOR). | Test de proyección y de plan |
| INV-ACT-9 | **Un DV discordante nunca pasa en silencio**: un RESOLVED con DV declarado que no valida lleva la política `actor-dv-unverified@1`, y el eco del núcleo lo anuncia. | Test del autor + test de presentación (E-ACTOR) |

**Determinismo limitado (declarado).** `resolve_exact` hereda el presupuesto de ADR-094: con el índice
real, `INCONCLUSIVE` puede aparecer de forma no determinista. Sólo puede cambiar RESOLVED por CLARIFY
(falla cerrado), nunca al revés. En el kit (fixture pequeño) la tasa debe ser 0, y si no lo es: STOP.

---

## 4. Opciones comparadas

### 4.1 Dónde vive la lógica de identidad

| Opción | Fallos | Reversibilidad / migración | Decisión |
|---|---|---|---|
| A. Envolver `resolve_actor_params` | Lee texto crudo, instancia `QueryRouter`, lee `get_settings` (A2): viola el invariante 6 y la INV-C0-8 | — | No |
| **B. Autoridad nueva que compone `normalize_identity` + `resolve_exact` + guardas derivadas, sin tocar los módulos compartidos** | Foundation y legacy discrepan en los casos genéricos (Foundation aclara y legacy ejecuta, A2 -053): deuda declarada | Sólo archivos nuevos; legacy intacto; el sidecar se borra sin efecto | **Sí** |
| C. Arreglar los módulos compartidos (guarda en el resolver, rebuild con exclusión, DV en `actor_identity`) para ambos | Cambia Actor Profile y legacy en la misma ola. Rebuild de 660 MB; invalida las mediciones de ADR-094 | Baja; tests legacy en masa | No en M1 (ticket legacy aparte) |

### 4.2 Formas de documento (tabla de decisión cerrada; manda sobre el texto)

`marker` ∈ {NIT, N.I.T., NIT No., C.C., CC, cédula, cédula de ciudadanía, + «No./número/:»}, plegado y
cerrado en el módulo. Otros tipos (CE, pasaporte, TI…) → `UNRESOLVED_HOST_DIMENSION:ACTOR` con detalle
`ACTOR_DOCUMENT_KIND_UNSUPPORTED` (el índice y `IdentityKey` sólo conocen NIT/CC).

| Cita (tras el marcador) | Marcador | Resultado |
|---|---|---|
| dígitos con separadores de miles, `-d` que valida | NIT o ninguno | RESOLVED `NIT`, base sin DV |
| `-d` que no valida (Gold P114/P115/N063: DV inventado) | NIT o ninguno | RESOLVED `NIT` con la base declarada (ADR-060: el DV es calidad) + política `actor-dv-unverified@1`, anunciada (INV-ACT-9) |
| dígitos sin `-d` | NIT | `normalize_identity(document_type="NIT")` (regla vigente de ADR-060, incluida la separación por módulo 11) |
| dígitos sin `-d` | CC/cédula | RESOLVED `CC`, número entero |
| con `-d` | CC/cédula | CLARIFY `UNRESOLVED_HOST_DIMENSION:ACTOR` / `ACTOR_DOCUMENT_FORM_INVALID` (una cédula no tiene DV) |
| dígitos desnudos, sin `-d` | ninguno | CLARIFY `AMBIGUOUS_HOST_DIMENSION:ACTOR` / `ACTOR_DOCUMENT_TYPE_UNDETERMINED` («¿NIT o cédula?»), **aunque valide el módulo 11** (colisión 1/11 de una cédula de 10 dígitos) |
| base fuera de 5–11 dígitos, centinela | cualquiera | CLARIFY `UNRESOLVED…` / `ACTOR_DOCUMENT_OUT_OF_RANGE` (también el teléfono de A2 -044 con marcador; sin marcador ya cae en la fila anterior) |

Retiradas por contexto (existenciales, por tanto monótonas): un marcador de tipo **contrario** en
`left` → `AMBIGUOUS…` / `ACTOR_DOCUMENT_TYPE_CONFLICT`; `right[0]` de un solo dígito (DV no citado) →
`UNCITED_QUALIFIER:ACTOR`. El marcador presente en el contexto **nunca** suple al que falta en la cita
(INV-C0-7: con la ventana vacía el veredicto debe ser el mismo RESOLVED).

### 4.3 Cabezas genéricas

| Opción | Problema | Decisión |
|---|---|---|
| A. Lista a mano | Prohibida por el mandato; no cubre los 1.022 municipios ni las palabras temáticas | No |
| **B. Sidecar derivado del índice, fuera de línea, ligado por digest** | Hay que regenerarlo en cada rebuild (INV-ACT-4 lo exige) | **Sí** |
| C. Conteo en tiempo de consulta sobre FTS | Latencia y presupuesto no deterministas; FTS deja de ser «sólo sugerencias» | No |

Criterios de `actor-generic@1` (sobre aliases N2; cada entrada lleva su criterio):

- **G1 territorial**: el alias es igual a un nombre de municipio DIVIPOLA, de departamento o a un alias
  de región (catálogos de `municipal_territory` / `colombia_geography`, normalizados N2).
- **G2 sólo forma**: el alias se compone sólo de tokens de forma jurídica (`actor_legal_forms`) o de
  marcador de grupo (`union temporal|consorcio|ut`).
- **G3 cabeza frecuente**: alias de ≤2 tokens (sin la cola de forma jurídica) contenido como
  subsecuencia de tokens completos en los aliases de ≥ **T** identidades distintas.
- **`org_tokens`** (para INV-ACT-5): n-gramas de ≤3 tokens con soporte ≥ S identidades y una cuota NIT
  ≥ R.

Calibración medida (sonda read-only, identidades que contienen el alias):

| Cabeza | Identidades | Cabeza | Identidades |
|---|---|---|---|
| consorcio | 10.010 | cafam | 3 |
| salud | 1.339 | compensar | 3 |
| hospital | 1.007 | colsubsidio | 1 |
| construccion | 425 | caracol television s a | 1 |
| huila | 322 | seguros del estado s a | 7 |
| consorcio vial | 132 | | |
| barranquilla | 101 | | |

Cuota NIT de algunos `org_tokens`: acueducto 0,985, sociedad 0,99, ltda 0,99, «e s p» 0,99,
fundacion 0,987.

Propuesta inicial: **T = 20, S = 200, R = 0,95**. El script fija T, S y R en el sidecar e informa de
la distribución completa y de los proveedores grandes (≥100 ocurrencias) que quedan como genéricos.
Si no hay hueco entre las dos poblaciones, STOP (§9). El sidecar vive junto al índice y **no se
commitea** (contiene aliases de personas); se commitean el derivador, su test sobre el fixture y, en el
informe, el digest y los recuentos.

### 4.4 Grupo (consorcio/UT con integrantes, «Riesgo D»)

| Opción | Decisión |
|---|---|
| **A. `ActorValue` identifica la parte que contrata con su documento. El eco declara que no incluye consorcios o UT de los que X es integrante (la misma advertencia que ya da el render de Actor Profile)** | **Sí (M1)** |
| B. `actor@2` con `members`, desde `ceth-n4bn` | No hay autoridad de relaciones ingerida (`procurement_relationships` es un registro v0). Es M3 y aditivo por etiqueta (C1.1 §8) |

### 4.5 PROCESOS + ACTOR

`p6dx-8zbt` tiene `nit_del_proveedor_adjudicado` / `nombre_del_proveedor`, pero: 8.333.841 de
8.747.640 filas son «No Definido» (95,3 %), sólo 853.421 dicen `adjudicado = Si` (9,8 %) y **no hay
columna de tipo documental**, así que INV-ID-TYPE no se puede aplicar. `DatasetSpec` de procesos no
declara `contractor`. ~~**Decisión: BLOCK explícito con la acción «Ver contratos de X»**.~~
**[SUPERSEDED por la §14: decisión P-ACT-2 del usuario, 2026-10-01.]** Una petición PROCESOS + ACTOR
(cualquier vista, cualquier polaridad) **se ejecuta sobre CONTRATOS con aviso**; nunca se proyecta
`nit_del_proveedor_adjudicado`. Lo que no cambia es el motivo: la fuente de procesos no identifica al
proveedor.

---

## 5. Contratos

### 5.1 Autoridad (`app/core/host_authorities/actor.py`)

```text
ActorAuthority(DimensionAuthority):
  authority_id = "actor-authority"; version = "1"
  dimensions = {ACTOR}; operators = {"is"}
  __init__(directory: ActorDirectory | None)       # inyectado; None ⇒ el camino de nombre bloquea
  resolve(evidence, ctx) -> AuthorityVerdict       # puro; no lee ctx.datasets ni el reloj

factory(deps: AuthorityDeps) -> ActorAuthority     # ActorDirectory.from_resolver(deps.actor_name_resolver)
```

Orden de evaluación (el primero que dispara gana):

1. `quote is None` (segmentos distintos) → `RESIDUE_UNREAD:ACTOR`. Operador ≠ `is` → no RESOLVED.
2. Forma de documento → §4.2 (no se consulta el directorio).
3. Camino de nombre:
   - sin directorio, o sidecar o índice ausente o rancio → BLOCK `AUTHORITY_UNAVAILABLE:ACTOR`;
   - N2 ∈ sidecar → `GENERIC_IDENTITY:ACTOR` (detalle `ACTOR_GENERIC_{TERRITORY|FORM|HEAD}`);
   - `resolve_exact`: si es `RESOLVED` con un método autoritativo, se pasa a las guardas;
     `exact_ambiguous_alias` y `exact_legal_form_variant_ambiguous` → `AMBIGUOUS…` /
     `ACTOR_NAME_HOMONYM`; `exact_prefix_ambiguous` → `ACTOR_PREFIX_COLLISION`; `INCONCLUSIVE` →
     `ACTOR_PREFIX_CHECK_INCONCLUSIVE`; `UNRESOLVED` → `UNRESOLVED…` / `ACTOR_NAME_NOT_FOUND`;
     `INDEX_UNAVAILABLE` → BLOCK `AUTHORITY_UNAVAILABLE:ACTOR`; cualquier otro estado → no RESOLVED;
   - guardas: conflicto de clase (INV-ACT-5) → `AMBIGUOUS…` / `ACTOR_NAME_KIND_CONFLICT`;
     completitud (∃k: N2(`quote`+`right[:k]`) o N2(`left[-k:]`+`quote`) es un alias exacto de **otra**
     identidad) → `UNCITED_QUALIFIER:ACTOR` / `ACTOR_MENTION_INCOMPLETE`.
4. Polaridad: NEGATIVE resuelve el mismo valor con `excluded=True` y la política
   `actor-exclusion@1`. Nunca devuelve `NEGATION_UNSUPPORTED`.

Homónimos, varios NIT bajo un nombre y persona natural frente a razón social con el mismo alias son
siempre CLARIFY (la autoridad nunca colapsa al rango 1). Un nombre personal que resuelve a un NIT
(persona natural comerciante) es legítimo y resuelve.

### 5.2 Puerto de directorio (`app/core/host_authorities/actor_directory.py`)

```text
ActorDirectory(Protocol):
  exact(name: str) -> ActorResolutionResult    # = ActorNameResolver.resolve_exact; jamás resolve()
  generic(alias_n2: str) -> GenericCriterion | None
  org_shaped(alias_n2: str) -> bool
  binding_ok() -> bool                         # digest(metadatos del índice) == sidecar.index_digest; en cada llamada
  digests() -> ((“directory”, sha256), (“generic_heads”, sha256))
```

Sólo lectura. Abre SQLite por llamada (sin conexiones compartidas: seguro entre hilos). El sidecar es
un `frozenset` inmutable que se carga en el constructor.

### 5.3 `ActorValue` (sin cambios: `actor@1`)

`document` = `IdentityKey.base_number` (sin DV); `document_kind` ∈ {`NIT`, `CC`} (la autoridad no
emite otro); `display` = la cita (INV-ACT-7); `excluded` = `polarity is NEGATIVE`. La proyección
obtiene las variantes con `document_variants(IdentityKey(kind, document))`, que es la autoridad única
vigente; el DV no viaja en el valor.

**¿Cambio del contrato C1.1?** No para D-ACTOR. Hay una sola dependencia condicional: si P-ACT-1 exige
candidatos para el CUTOVER, se añade `alternatives: tuple[ExecutableValue, ...] = ()` al veredicto.
Ese campo ya está aprobado como aditivo en la revisión de C1.1 (§3 D-GEO) y no rompe a ningún carril.

### 5.4 Proyección (núcleo, E-ACTOR; la precisión es invariante)

Sólo `jbjy-vk9h`. Siendo V = `document_variants(id)` e I = `incompatible_source_types(kind)`:

- POSITIVE: `documento_proveedor IN (V) AND (tipodocproveedor IS NULL OR tipodocproveedor NOT IN (I))`.
- NEGATIVE (`actor-exclusion@1`, complemento con trato explícito de nulos):
  `(documento_proveedor IS NULL OR NOT (documento_proveedor IN (V) AND (tipodocproveedor IS NULL OR tipodocproveedor NOT IN (I))))`.
  Conserva las filas «No Definido» (2,9 %; proveedor desconocido no es X). La política lo declara.
- Petición con sujeto PROCESOS + ACTOR → ~~`UNSUPPORTED` con `ACTOR_DATASET_UNSUPPORTED`~~ **[v1.7:
  la política de vista la redirige a CONTRATOS con el aviso `ACTOR_PROCESSES_TO_CONTRACTS`; la
  proyección de este punto sólo ve contratos, y `ACTOR_DATASET_UNSUPPORTED` deja de existir (§14)]**.
  `UNSPECIFIED` ya va a CONTRATOS por PV3.
- Reglas entre condiciones (núcleo, después de las autoridades): dos ACTOR POSITIVE de identidades
  distintas bajo AND → CLARIFY `CONTRADICTORY_ACTOR` (un contrato tiene un único proveedor). ACTOR con un
  STATE previo a la adjudicación → ya da `STATE_NOT_IN_VIEW`.
- Límite que se anuncia: sólo SECOP II Contratos; contratos donde X figura como proveedor adjudicado
  con ese documento; no incluye UT o consorcios en los que X es integrante (§4.4) ni el documento
  alterno de una persona natural comerciante (CC frente a NIT: dos identidades, ADR-060).

### 5.5 Detalles (cerrados, `[A-Z][A-Z0-9_]`; el núcleo los traduce a mensajes)

`ACTOR_NAME_NOT_FOUND`, `ACTOR_NAME_HOMONYM`, `ACTOR_PREFIX_COLLISION`,
`ACTOR_PREFIX_CHECK_INCONCLUSIVE`, `ACTOR_NAME_KIND_CONFLICT`, `ACTOR_MENTION_INCOMPLETE`,
`ACTOR_GENERIC_TERRITORY`, `ACTOR_GENERIC_FORM`, `ACTOR_GENERIC_HEAD`,
`ACTOR_DOCUMENT_TYPE_UNDETERMINED`, `ACTOR_DOCUMENT_TYPE_CONFLICT`, `ACTOR_DOCUMENT_FORM_INVALID`,
`ACTOR_DOCUMENT_KIND_UNSUPPORTED`, `ACTOR_DOCUMENT_OUT_OF_RANGE`, `ACTOR_DIRECTORY_MISSING`,
`ACTOR_GENERIC_HEADS_STALE`.

---

## 6. Datos personales (P9 = ADR-060, sin política nueva)

| Superficie | Qué entra |
|---|---|
| Respuesta al usuario (eco, presentación intencional) | El documento completo («CC 12345678», `format_actor_identity_for_user`), igual que hoy en Actor Profile (ADR-060 §9) |
| `AuthorityReceipt` | Sólo `rules` de vocabulario cerrado (`path:document\|name`, `marker:nit\|cc\|none`, `dv:valid\|mismatch\|absent`, `method:<m>`, `generic:<criterio>`, `guard:<g>`), `candidates` (recuento) y `digests` del índice y del sidecar (INV-ACT-6) |
| Ledger (`HOST`) | `condition_id`, la decisión, la razón, el detalle y las reglas del recibo. Nunca `value.encode()` de `actor@1` |
| Diagnóstico (`foundation_trace_view`: resoluciones y SoQL del plan) | CC enmascarada con `mask_identity_key` (núcleo, E-ACTOR). Un NIT va en claro (como hoy) |
| Persistencia (`log_trace`, conversación) | Test de E-ACTOR: para un turno ACTOR-CC, ningún campo persistido contiene la base en claro. Legacy ya la persiste: deuda preexistente, se informa y no se toca |

Riesgo de homónimos de personas naturales:

- Alias exacto con varias CC → CLARIFY.
- Prefijo de otra CC (ADR-094) → CLARIFY.
- Nombre único en el directorio → RESOLVED a la única persona con contratos en SECOP II con ese nombre.
  El eco con el documento completo permite verificarlo. Riesgo residual declarado: una persona que el
  usuario tiene en mente y que nunca contrató es indistinguible.
- Nombre de forma organizativa con CC → CLARIFY (INV-ACT-5).

---

## 7. Frontera con Actor Profile

**Durante M1 (puerta y vetos puestos; aceptación dual, D5).** `actor_claims` sigue reclamando primero
(deuda de texto crudo hasta M3). Foundation sólo ve los turnos con actor que legacy declina (p. ej.
«contratos terminados de X», A4 §6.3). E-ACTOR los vuelve correctos en lugar de dejarlos sin resolver
o tratarlos como tema.

| Significado | Dueño aceptable en M1 |
|---|---|
| SEARCH cuyo único filtro es ACTOR | Foundation (lista) o Actor Profile (perfil) |
| SEARCH ACTOR + otra dimensión | Foundation; Actor Profile sólo si honra todo (falla con TOPIC, MODALITY, ORDER y STATE, A4 §6.3) |
| PROFILE_ACTOR | Actor Profile mediante HANDOFF (C1.4); SEARCH nunca lo captura |

La autoridad no lee `actor_pending` ni el estado de la conversación. La selección de candidatos
(capa 1) corre antes y no cambia.

**Exigencias de CUTOVER para ACTOR** (ADR-103 enmienda 2, tolerancia cero, Gold completo × 2 pasadas):

1. **Evaluador simétrico (HERA) antes de medir.** Hoy `gap_matrix.py` devuelve `L_OK` para ACTOR con
   sólo detectar el actor (línea 252), sin mirar la cita ni la polaridad. Definición común para ambos
   brazos:
   - OK = ACTOR propuesto, con la cita solapada a la del Gold y la misma polaridad;
   - y ejecución con la identidad oráculo, **o** aclaración `UNRESOLVED/AMBIGUOUS:ACTOR` cuando la
     identidad no está en el directorio (los actores del Gold son inventados, A4 §6.2);
   - nunca TOPIC, nunca descartado.
2. `L-only-OK(ACTOR) = 0` con ese evaluador, más el corpus de sensibilidad A4 §6.3 (14 formas × las
   identidades del fixture). En este corpus la identidad ejecutada debe ser exacta (V + cláusula de
   tipo en el SoQL).
3. 0 condiciones ACTOR descartadas o ensanchadas. 0 PROCESOS↔CONTRATOS cambiados en silencio.
   0 inversiones de polaridad (NEGATIVE incluido).
4. Barrido genérico sobre el índice real con 0 RESOLVED. Las sondas A2 -022…-034, -040, -044 y -053
   pasadas por la autoridad, con 0 FAIL_SILENT.
5. NEIGHBOR PROFILE_ACTOR (N056, N057, N059, N060, N064, N065, N067, N069): 0 secuestros y 0
   violaciones de conformidad (dependencia de C1.4/M1-I, no de D-ACTOR).
6. Test de datos personales en persistencia en verde (§6).
7. **P-ACT-1 resuelta.** La paridad de la métrica no equivale a la paridad de UX: legacy ofrece
   candidatos seleccionables y Foundation M1 sólo aclara.

---

## 8. Plan para el ARES del carril (un escritor, paquetes en serie, ownership exclusivo de archivos nuevos)

| Paquete | Archivos (nuevos) | Contenido |
|---|---|---|
| A1 | `app/core/host_authorities/actor_document.py` | Gramática cerrada de §4.2 → `IdentityMatch` vía `normalize_identity` + clasificación de la forma. Sin índice. |
| A2 | `app/data/actor_generic_heads.py`, `scripts/build_actor_generic_heads.py`, `tests/fixtures/actor_directory_m1/` (filas sintéticas → índice pequeño con el `build_actor_alias_index` existente) | Derivación G1–G3 + `org_tokens`, sidecar con `index_digest`, cargador y validador, informe de calibración |
| A3 | `app/core/host_authorities/actor_directory.py`, `app/core/host_authorities/actor.py` | Puerto + adaptador; autoridad y `factory` |
| A4 | `tests/test_host_authority_actor.py`, `tests/test_host_authority_actor_document.py`, `tests/test_actor_generic_heads.py`, `docs/audits/m1_c1/D_ACTOR_CANDIDATE.md` | Kit + dominio + mutantes + informe del slice |

**No se toca nada existente.** Gate: `git diff --stat 7bd671e6 -- app/ tests/ scripts/` sólo lista los
archivos nuevos de esta tabla. Registro, proyección, mensajes, eco, enmascarado de la traza y reglas
entre condiciones son del NÚCLEO (E-ACTOR).

**Tests**

- `assert_conforms(ActorAuthority(fixture_dir), cases)` con casos RESOLVED de ambos caminos,
  `monotone_chain=True`.
- **Barrido exhaustivo de documentos** (Hypothesis más enumeración desde el léxico, no a mano): marcador
  (lista cerrada ∪ ninguno, con variantes de mayúsculas y tildes) × separadores (ninguno, puntos, comas,
  espacios) × DV (ninguno, guion válido, guion inválido, espacio + dígito) × longitud 3–13 × centinelas.
  Oráculo = la tabla §4.2 escrita en el test con independencia del módulo.
- **Barrido de cabezas genéricas**: todo alias del sidecar del fixture; todos los nombres DIVIPOLA
  (1.022 municipios), departamentos y regiones; las 27 palabras temáticas de A2 -058; los tokens de
  forma jurídica y de grupo. Ninguno RESOLVED. Variante local sobre el índice real (marcador
  `requires_actor_index`, se omite en CI), con recuentos en el informe.
- Dominio: homónimo, prefijo ADR-094, variante de forma jurídica, ESP → CC, mención incompleta,
  índice ausente, sidecar rancio, NEGATIVE, sondas A2 reproducidas en el fixture.
- **Slice Gold ACTOR por `gold_id`** con IR oráculo:
  - SEARCH: P029, P034, P035, P109, P110, P111, P114, P115, P117, P119, P121, N001, N058, N061, N062,
    N063, N066, N068, N070, N071, N072, N073, X000, X006, X083. N073 conserva la IR oráculo
    `PROCESSES` (Foundation transcribe lo citado); el slice la evalúa ya redirigida a CONTRATOS (§14).
  - Fuera del slice: N040 es COUNT; N056, N057, N059, N060, N064, N065, N067 y N069 son
    PROFILE_ACTOR (entrega, no autoridad).
  - Dos corridas: fixture sembrado con los nombres sintéticos → RESOLVED exacto; directorio real →
    `ACTOR_NAME_NOT_FOUND`.
  - REAL por `gold_id`, sólo lo ejecuta HERA y no se siembra en ningún fixture commiteado.
- PII: propiedad INV-ACT-6 sobre todos los veredictos de la suite.

**Mutantes que deben morir.**

- M1: método FTS aceptado.
- M2: sin chequeo genérico.
- M3: número desnudo resuelto por módulo 11 o longitud.
- M4: marcador leído del contexto.
- M5: NEGATIVE ignorado.
- M6: sin la guarda de clase.
- M7: sin la guarda de completitud.
- M8: `INDEX_UNAVAILABLE` convertido en CLARIFY.
- M9: AMBIGUOUS colapsado al rango 1.
- M10: INCONCLUSIVE convertido en RESOLVED.
- M11: DV discordante sin política.
- M12: CC con DV aceptada.
- M13: dígitos o nombre en el recibo.
- M14: sidecar de otro índice aceptado.
- M15: `display` sacado del directorio.
- M16: centinela «no definido» resuelto.
- M17: `exact_prefix_ambiguous` tratado como RESOLVED.

**Impacto en legacy**: ninguno en runtime. Gate de regresión focal, sin modificar:
`tests/test_actor_identity.py`, `test_actor_name_resolver.py`, `test_actor_alias_index.py`,
`test_actor_exact_prefix*.py`, `test_foundation_authority_*.py`. «Corrí X, no la suite».

**Gates por paquete**: ruff sobre los archivos tocados → tests focales → mutantes (aplicar, correr,
restaurar) → `olympus-semgrep-delta --base 7bd671e6`. Antes de entregar: `git log` (commits ajenos).

## 9. Condiciones de STOP

- Hace falta cambiar la semántica de `actor_identity`, `actor_alias_resolver`, `actor_alias_index` o
  `resolve_exact`, o reconstruir el índice → METIS.
- Hace falta un campo nuevo en `ActorValue` o en el veredicto (incluido `alternatives`) → METIS.
- La calibración no muestra hueco: un proveedor legítimo y frecuente queda dentro de G3 y no se puede
  separar con T → informe + METIS. Prohibido ajustar a mano.
- `INCONCLUSIVE` aparece en el kit/fixture (no determinismo) → STOP.
- El slice REAL contiene números desnudos o conflictos de clase → se pide adjudicación del Gold
  (HERA/usuario). No se relaja la autoridad. (PROCESOS + ACTOR ya no es STOP: v1.7 lo adjudica como
  `host_universe_redirect`; el slice REAL tiene 0 casos.)
- Un mutante sobrevive dos rondas → se declara la garantía como limitada y se cierra.

## 10. Adjudicaciones del Gold que el CUTOVER necesita (nueva versión sellada; ADR-103 enm. 2)

- **X083** (y sus equivalentes REAL): la cita pasa a ser «cédula 1000000001» (el marcador de tipo forma
  parte del identificador). Con la cita actual, la IR oráculo aclara.
- **N073** («procesos del proveedor …»): ~~pasa a BLOCK con oferta de contratos (§4.5), salvo que P-ACT-2
  diga otra cosa~~. **Hecho en v1.7**: sigue `EXECUTE`, con `host_universe_redirect =
  {executed_universe: CONTRACTS, notice: ACTOR_PROCESSES_TO_CONTRACTS}` (§14).
- **P114, P115, N063, N064**: no cambian. El DV inventado resuelve con `actor-dv-unverified@1`.

## 11. Decisiones de producto pendientes (no bloquean D-ACTOR; sí el CUTOVER de ACTOR)

| Id | Pregunta exacta | Recomendación |
|---|---|---|
| P-ACT-1 | Cuando un nombre no tiene coincidencia exacta, Foundation ¿debe ofrecer candidatos seleccionables (como legacy/Actor Profile, ADR-060 §10) o basta con pedir el NIT o la razón social completa? | **Candidatos** antes de retirar el veto de ACTOR. Exige el campo aditivo `alternatives` y un evento de selección (patrón de F). Hasta entonces, el veto de ACTOR no se borra en M1-I aunque la métrica dé 0. |
| P-ACT-2 | «Procesos de/adjudicados a X»: ¿bloquear con la oferta de contratos, o proyectar `nit_del_proveedor_adjudicado` sólo para NIT y anunciar «sólo procesos adjudicados»? | ~~**Bloquear** en M1~~. **DECIDIDO (usuario, 2026-10-01, v1.7): ni una ni otra. Se ejecuta sobre CONTRATOS con aviso (§14).** |

## 12. Riesgos y qué haría cambiar la decisión

| Riesgo / señal | Respuesta |
|---|---|
| Atribuciones equivocadas en dogfood por un DV discordante que se resolvió | Una línea: `-d` inválido pasa a CLARIFY (y se adjudican P114, P115, N063, N064) |
| G3 deja fuera a proveedores reales frecuentes («Claro»…) | Recalibrar T con versión nueva del sidecar, o lista revisada y adjudicada con digest (sigue cerrada) |
| Sidecar o índice desincronizados tras un rebuild | INV-ACT-4 (BLOCK por llamada); regenerar el sidecar forma parte del runbook del índice |
| Legacy sigue resolviendo «proveedor Consorcio» (A2 -053) | Deuda legacy: un ticket aparte reutiliza el sidecar en `resolve_actor_params` (cambia legacy; fuera de M1-D) |
| Llega una autoridad de relaciones (UT, `ceth-n4bn`) | `actor@2` con `members` (aditivo) y proyección por integrantes, anunciada |
| El producto quiere procesos por proveedor | ~~P-ACT-2 → proyección NIT-only en procesos~~ Decidido en v1.7: no. «A qué procesos se presentó X» llegará con un dataset de ofertas o proponentes (fuera de M1); hasta entonces se ejecuta en contratos con aviso. |
| La ventana K=4 no ve el resto de una razón social larga | Falla hacia aclarar (completitud). Subir K es METIS (C0 §12) |

## 13. Non-goals

Grupos e integrantes; candidatos y selección en Foundation (M2/P-ACT-1); ACTOR ejecutado sobre procesos
(se redirige a contratos con aviso, §14; ofertas/proponentes = dataset futuro); ACTOR bajo
OR; roles distintos de contratista (comprador-actor, representante legal, supervisor); SECOP I; CE,
pasaporte y otros documentos; corregir legacy (A2 -053, el DV de `actor_identity`, el rebuild con
exclusión); reclamaciones de dueño (`actor_claims`, C1.4/M3); el registro, la proyección y los mensajes
(núcleo, E-ACTOR).

## 14. Enmienda P-ACT-2 (v1.7)

Fecha: 2026-10-01. Decisión de producto del usuario (no de este diseño). Contrato: `M1_0_SEARCH_CONTRACT.md`
§17; Gold `m1-search-gold/1.7`.

**Decisión.** A un proveedor o contratista se le buscan CONTRATOS, no procesos. Un request SEARCH cuyo
universo citado es PROCESOS (cualquier vista, incluida OPPORTUNITIES) y que lleva una condición ACTOR
(POSITIVE o NEGATIVE) **se ejecuta sobre CONTRATOS y el host lo anuncia** (`ACTOR_PROCESSES_TO_CONTRACTS`).
Foundation sigue transcribiendo fielmente lo citado (`universe: PROCESSES`); no se reescribe la IR. BUYER
sobre procesos no se ve afectado. «A qué procesos se presentó X» exige un dataset de ofertas o
proponentes y queda fuera de M1.

**Por qué no se cambia la autoridad ACTOR.** La autoridad resuelve *quién* (identidad) y es agnóstica del
universo: sólo ve una ventana de evidencia y nunca el dataset (invariante 6 y §5.1). Su veredicto es el
mismo en PROCESOS y en CONTRATOS. La enmienda decide *dónde* se ejecuta, y eso no es identidad.

**Dueño de la redirección: la política de vista del host (carril F, `app/core/product_view_policy.py` y
`product_view_announcement.py`), no la autoridad ACTOR.** Argumento (C0_SPINE_V5_DESIGN §4.4/§5.6): la
regla es de producto y de vista, no de identidad. F ya es el único módulo puro, posterior a las
autoridades y anterior a la proyección, que elige el dataset de una petición, lo registra en la
aceptación (`HostViewDecision`) y lo anuncia siempre (INV-C0-10); la autoridad ACTOR no puede ni leer
el universo ni anunciar.

**Dependencias.**

| Carril | Cambio que exige |
|---|---|
| Núcleo C1 / METIS | Enmienda de C0 §5.6: hoy la política sólo mira condiciones **POSITIVE** de peticiones `UNSPECIFIED` y «las peticiones con sujeto explícito no tienen decisión». Hace falta una regla nueva (no PV1–PV6; p. ej. `PV_ACTOR_PROCESSES`) que lea ACTOR de **cualquier** polaridad en peticiones `PROCESSES`, con `effect = APPROXIMATION` y `announced = True`. Es un cambio de contrato (`HostViewDecision`, validación de READY): decide METIS antes de que F escriba. |
| F (política y anuncio) | La regla y el aviso con la acción hacia la vista pedida. No consulta datos. |
| Núcleo E-ACTOR | INV-ACT-8 y la proyección ven sólo contratos; se elimina `ACTOR_DATASET_UNSUPPORTED` (§5.4). |
| D-ACTOR | Ningún cambio de código ni de contrato de la autoridad. La IR oráculo del slice N073 sigue en PROCESOS. |
| Evaluación (HERA) | `gap_matrix.py` puntúa `UNIVERSE_VIEW` igual en ambos brazos: CONTRATOS + aviso; ejecutar en procesos, bloquear donde el Gold dice EXECUTE o cambiar sin aviso son fallos. |

**Sigue abierto.** P034 (ACTOR con `UNSPECIFIED` y `product_view: HISTORICAL_PROCESSES`, porque PV2 gana a
PV3) también ejecuta un proveedor sobre procesos; la regla de arriba no lo cubre y el orquestador debe
decidir si se reordena PV2/PV3 o se readjudica.

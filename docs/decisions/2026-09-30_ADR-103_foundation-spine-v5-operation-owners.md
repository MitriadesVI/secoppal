# ADR-103 — Foundation spine v5: perfiles cerrados, enrutamiento por operación y contrato de autoridades

**Fecha:** 2026-09-30.
**Estado:** aceptada por el orquestador (HERA) con delegación del usuario; sin efecto en runtime
hasta que cada paso C1 se fusione con autorización expresa. Enmienda ADR-102 §2/§4 y resuelve la
tensión ADR-102 §9 vs D1.
**Misión:** `M1_SEARCH_FOUNDATION_V1`, ola C. Diseño completo:
`docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md` (METIS C0).
**Numeración:** máximo global en ramas y worktrees = ADR-102; 103 verificado libre.

## Contexto

La Fase A de M1 (Gold sellado de 999 consultas) mostró que Foundation pierde frente a legacy por
lo que su IR no puede expresar (ACTOR, universo omitido, mes/rango, inclusivos, orden), no por
entender mal; que el prompt v4.2 contiene convenciones normativas dañinas (Y1 «cita sólo el año»);
y que la puerta léxica es la única barrera que impide a SEARCH secuestrar capacidades vecinas
(25/58 sin ella).

## Decisión

1. **Perfiles cerrados.** `SECOPPAL_FOUNDATION_MODE ∈ {legacy, foundation_v3, foundation_v5}`.
   Un perfil es un paquete fijo productor+wire+H7+host; no hay selector suelto de productor. El
   prompt v4.2 queda byte-congelado y seleccionable como rollback.
   > **SUPERSEDED desde el CUTOVER por la Enmienda 4 (D8):** el conjunto pasa a
   > `{foundation_v5, foundation_v5_previous}` y `legacy`/`foundation_v3` mueren. Texto conservado; rige hasta el CUTOVER.
2. **IR v2 única en memoria** (`SCHEMA_VERSION = 2`), con `lift_v1` sin pérdida para el wire v4.2.
   El perfil fija el wire; ningún decoder infiere la versión.
3. **La operación gobierna el enrutamiento**, con frontera acotada: el LLM sólo decide a qué dueño
   de una tabla cerrada se *ofrece* el turno; el dueño debe reclamarlo con su propia admisión; sin
   segundo salto; a legacy sólo cruza la operación (nunca valores); la conformidad del ejecutor se
   mide y es 0 para el cutover.
4. **Autoridades por dimensión** como funciones puras sobre una ventana de evidencia (cita + ≤4
   tokens), contexto monótono (sólo retira autoridad), valor tipado con ida y vuelta, razones
   cerradas, registro con madurez `SHADOW → ACTIVE` y borrado de la rama en línea al activar.
5. **Lo normativo se cierra en el host** antes de escribir el prompt v5; Y1 se elimina.
6. **Product View Policy** como módulo puro tras la admisión; toda vista por defecto se anuncia.
7. **Caída del proveedor en `foundation_v5`: rige D1** («no pude interpretar ahora», sin legacy);
   ADR-102 §9 sigue rigiendo sólo `foundation_v3`.

## Decisiones de producto (P1–P9 del diseño)

Tomadas según la recomendación de METIS: P1 mensaje explícito con guía para operaciones sin dueño;
P2 MULTI con SEARCH → «por pasos» con un botón por job; P3 (enmendada, ver abajo) superlativo singular → ORDER + CARDINALITY 1; P4 activación por etapas con autorización expresa en cada una; P5 sí a perfiles
cerrados; P6 D1; P7 en M1 sólo botones de vista (período y orden en M2), opciones numeradas en
texto; P8 tope de top-N = 100 anunciado; P9 SEARCH por persona natural con la política de Actor
Profile (ADR-060).

## Consecuencias

- Cambian `interpretation_hash` y `plan_hash` de todo turno Foundation; `load_acceptance`/`lift_v1`
  permanecen hasta M4.
- El test que fija «un solo perfil» se reemplaza por uno de conjunto cerrado (cambio declarado).
- Las reclamaciones de dueños legacy siguen leyendo texto: deuda declarada que muere en M3.
- Activar v5 exige los criterios ACTIVATE_V5 (diseño §5.4.3) sobre la partición TEST del Gold
  (≥2 pasadas) y no regresión en el held-out DA-6 con custodio.

## Rollback

> **SUPERSEDED desde el CUTOVER por la Enmienda 4 (§4.6, D8):** hasta el CUTOVER rige este texto; después, el rollback
> de flag es `foundation_v5_previous` (degradado en follow-ups, sin legacy detrás). Texto conservado.

`foundation_v3` (v4.2 bajo el host v2) o `legacy` (paridad con `main`). Cada paso C1.x es un commit
revertible; C1.1 no cambia el runtime.

## Enmienda 1 (2026-09-30) — P3 vetada y corregida

La adopción inicial de P3 («superlativo singular → lista ordenada, sin límite») contradecía el Gold
sellado: N012 («el contrato más grande de la Alcaldía de Cali», doble etiqueta sin desacuerdo) y
R0507 (consulta REAL, «dame el contrato … de mayor cuantía») llevan `ORDER value_desc` con
cardinalidad 1. Veto del asesor aceptado; el Gold no se modifica.

Regla vigente:
- Superlativo **singular** («el/la <sustantivo> más …», «el de mayor valor») → `ORDER` +
  `CARDINALITY 1`.
- Superlativo **plural sin cifra** («los más caros») → lista ordenada, sin `CARDINALITY`.
- **Cifra explícita** («los 10 más caros») → `CARDINALITY N` (tope P8 = 100, anunciado).
La cardinalidad la decide el host a partir de la cita (número gramatical o cifra); el LLM no
entrega el valor (invariante 3).

## Enmienda 2 (2026-09-30) — dos gates distintos: ACTIVATE_V5 y CUTOVER

**ACTIVATE_V5** (dogfood experimental, la puerta sigue puesta, rollback inmediato) admite umbrales
estadísticos: los del diseño §5.4.3 (exactitud de operación ≥ 95 %, SEARCH→no-SEARCH ≤ 2 %, etc.).

**CUTOVER** (M1-I: se borran la puerta y el SEARCH legacy) es de tolerancia cero, sobre el Gold
completo (REAL + sintéticos) y con ≥ 2 pasadas, peor caso:

| Gate CUTOVER | Exigencia |
|---|---|
| Vecinos secuestrados por SEARCH (NEIGHBOR) | **0** |
| `L-only-OK` exclusivo (legacy acierta, Foundation no) | **0 en cada clase SEARCH** — incluye un SEARCH Gold que v5 envía a PROFILE/COUNT/otra operación |
| Ensanche silencioso (TIME, BUYER, VALUE, GEO, cualquier condición no ejecutada sin aviso) | **0** |
| Inversión de universo en casos CLEAR (procesos↔contratos, registros↔oportunidades) | **0** |
| Inversión de polaridad | **0** |
| Violaciones de conformidad de operación (INV-C0-6) | **0** |

No hay «diferencias firmadas» que eximan un `L-only-OK`: si el producto decide que lo correcto es
otra cosa, se corrige el **Gold** mediante adjudicación y una nueva versión sellada (con log), nunca
con una dispensa en el gate. Ningún umbral de CUTOVER se expresa en porcentaje.

## Enmienda 3 (2026-09-30) — P7 es postergación, no descarte

Los botones `[Elegir período]` y `[Mayor valor]` de la Product View están **postergados a M2**
(necesitan delta de IR), no descartados. M1 entrega `[Procesos históricos]`, `[Contratos]` (y
`[Oportunidades vigentes]` cuando la vista por defecto no lo es).

## Enmienda 4 (2026-10-01): un dueño por superficie, M2a antes del CUTOVER, rollback v5 y frontera de vocabulario

> **v3.** La v2 aplicó la revisión ZEUS (2 P1 y 9 P2) y las decisiones del usuario sobre OQ-1 a OQ-7
> (`USER_DECISIONS_2026-10-01.md`). La v3 aplica la verificación ZEUS de la v2 (N1–N7). Las versiones anteriores
> se conservan en `*_v1.md` y `*_v2.md`.
>
> **Numeración.** Los insumos asesores la llaman «enmienda 2». En este ADR ya existen las enmiendas 1–3, en `main` y
> en los 10 worktrees M1 que contienen el archivo, así que se anexa como **4**. Amplía la tabla de gates CUTOVER de
> la enmienda 2 sin modificar sus filas.
>
> **Autoridad.**
> - Decisiones del usuario del 2026-10-01: los puntos 1–12 del encargo y las respuestas a OQ-1 a OQ-7.
> - Adjudicación T3 (`ADJUDICATION_T3_2026-10-01.md` y `kit_v2_2/ADJUDICATED_OVERRIDES.md` A1–A7), que prevalece
>   sobre los insumos v2.1.
>
> **Evidencia y anexos.**
> - La evidencia se tomó sobre `main` = `88653752` y `secoppal-m1-c12` = `5b4b7848`.
> - El detalle de M2a está en `M2A_DESIGN_v2_1.md`, salvo donde T3 o esta enmienda lo corrigen.
> - Anexos: `C0_PATCHES`, `C1_2_TOPN_SPEC` y `C1_2_FRONTIER_SPEC`.
>
> **Pendientes de usuario:** ninguno. D1(d) quedó decidida después de la revisión: congelar.

### 4.1 Hechos verificados

| H | Hecho | Evidencia |
|---|---|---|
| H0 | **Línea base.** En las 449 consultas SEARCH REAL, legacy acierta 194 (43 %, no 53 %) y Foundation 101 (22 %). | `PHASE_A_CLOSE.md:23-26` |
| H1 | El turno Foundation persiste `parsed_params = {}`. `continues_foundation_turn` deja pasar las búsquedas completas y los turnos que reclama otro dueño. Todo lo demás que legacy fusionaría con el turno Foundation termina en `FOUNDATION_CONTINUATION_UNSUPPORTED`. | `foundation_runtime.py:221-246,256-257`; `foundation_route_owner.py:82-114` |
| H2 | **65 de los 585 registros REAL** son `FOLLOWUP_DELTA` (11,1 %). El archivo privado tiene 620 registros: 585 REAL y 35 SYNTHETIC. C0 §5.3.6.5 decía 64. | `gold_v1.jsonl` sha `6e470e89…` |
| H3 | El wire v4.2 no tiene ACTOR, y 119 de las 773 búsquedas del Gold lo llevan. `foundation_v3` sólo sirve de rollback mientras el veto entrega ACTOR a legacy, y C0 §5.3.6 borra ese veto. | `foundation_prompts.py:43-44`; `foundation_route_owner.py:74-79` |
| H4 | `temporal_semantics` era a la vez autoridad y escáner. Se cerró por paridad: pines de bytes, cierre transitivo de imports y una frontera perezosa. | `tests/test_m1_wave1_parity_pins.py`; `PARITY_TEMPORAL.md`; `PARITY_VALUE.md` |
| H5 | **C1.2 rompe los pines.** Legacy alcanza `interpretation_contract` por `workflow.deps → secop_client →(perezoso) foundation_guard`, y de ahí llega a `foundation_geo_level`, `foundation_operations` y `foundation_operator_table`. El cierre legacy contiene además `foundation_text_predicates` (`soql_builder.py:537`) y `foundation_trace_view` (`streamlit_app.py:375,487`); los dos son idénticos a `main` y no importan nada de `app`. Con la división de D13, simulada con el escáner del pin, el cierre queda en 386 módulos y sólo difieren de `88653752` `semantic_dimensions` y la hoja `foundation_guard`, ambas permitidas. | escáner del pin sobre c12 |
| H6 | `build_window` no ve otras dimensiones. El contexto sólo retira autoridad y el hueco no cambia el valor (INV-C0-7/7b). | `foundation_evidence_window.py:68-97` |
| H7 | En 1014 registros del Gold no hay ningún solape real de citas entre dimensiones. En X020 y X059 la GEO homónima está en *otra* ocurrencia; X038 es AMBIGUOUS y MULTI. | `adversarial.jsonl` |
| H7b | Solapes medidos en las propuestas v4.2 sobre REAL: 116 de 120 son contra UNIVERSE (BUYER+UNIVERSE 12, GEO+UNIVERSE 11, …). El oráculo sintético tiene 2 de 294 solapes BUYER+GEOGRAPHY, sin identificar. | `OMISSION_PROBE.md:220-222` |
| H8 | Cobertura sobre v4.2. Con clase cerrada aclara de más el 35–39 % de las propuestas correctas. Con `STOPWORDS` baja al 14 %, pero sólo ve el 46 % de las omisiones y el 0 % en STATE, MODALITY y mes. Hay 18 léxicos funcionales. Si cuentan los spans de petición, el chequeo queda vacío en el 53 %. | `OMISSION_PROBE.md` §1, §5, §6 |
| H9 | C1.1 contradice P8: el comentario de `values.py:33` y C0 §11 P8 dicen «aclarar». En el Gold, `limit_n` nunca pasa de 10. | `foundation_authority_values.py:33,523-542` |
| H10 | El catálogo SMMLV existe para 2024–2026. Sin año, legacy usa el año en curso. R0302 (SMMLV + 2026) y R0305 (SMMLV sin TIME) están sellados `EXECUTE CLEAR`. | `app/data/smmlv.py`; `query_router.py:1564-1591` |
| H11 | TIME, VALUE y GEO de wave 1 tienen firma `(quote, left, right)` y tipos de valor propios. | `value_authority.py:76,157`; `time_authority.py:125,327`; `geo_authority.py:39` |
| H12 | `QueryRouter.parse` es el lector de valores que comparten todos los dueños legacy. | `M3_CONSUMER_MAP.md` §1 (S1) |
| H13 | Hay dos enums de dimensión y dos `AuthorityReceipt`. `accreditation/authorities.py` está en el cierre legacy (`query_router → material_residual → accreditation.assembly`). | `accreditation/spec.py:32`; `assembly.py:87`; `foundation_authority_contract.py:209` |

### 4.2 Decisiones

**D1. Separación de gramáticas (INV-SEP).** La autoridad por cita (`host_authorities/*`, `foundation_authority_*`,
`time_grammar` con su cierre, D-MONEY y D-GEO) y los escáneres legacy (QueryRouter y los lectores de los dueños) son
gramáticas distintas.

- **(a) Pines de bytes.** `temporal_semantics.py` y `money_polarity.py` quedan idénticos a `main` byte a byte.
- **(b) Pin generado del cierre legacy (código y datos).**
  - Cada módulo `.py` del cierre transitivo desde las entradas legacy tiene el mismo sha256 que en el manifiesto
    de la base `88653752`.
  - **Todo archivo versionado que no sea `.py` y que lea el cierre legacy tiene también el mismo sha256 que en la
    base.** Estos datos se leen por ruta, no por import. Incluye:
    - el árbol completo `app/data/**` (por ejemplo `query_router.py:318`, `buyer_groups.py:79,82`,
      `buyer_source_names.py:41`);
    - los artefactos que lee `territorial_knowledge.py:43-53`: `artifacts/audits/municipal_m2a/{logical_units,physical_profiles}.json`,
      `municipal_m2b/*.json` y `municipal_m2b_1/*.json`.
  - **Casación por ruta resuelta.** Todo literal de un módulo del cierre que resuelva a un archivo versionado que no
    sea `.py` queda congelado. Las exenciones están en una lista explícita: estado de runtime (`pagination.json` /
    `.pagination.json` de `conversation_store`) y URLs (`/query.json` de `socrata_transport.soda3`).
  - Un archivo nuevo en `app/data/**` es violación.
  - La lista de divergencias permitidas es explícita y mínima: `semantic_dimensions` (aditivo, pin AST de lista
    blanca) y `foundation_guard` (hoja de D13).
  - `entity_resolver` ya es idéntico en esa base; su pin de wave 1 (`alias_owners`) sigue vigente.
  - Un módulo nuevo que entre en el cierre es violación.
  - Regenerar el manifiesto exige un motivo declarado en el commit y ZEUS.
  - Esto sustituye a cualquier lista prohibida mantenida a mano.
- **(c) Lo aprendido no entra por legacy.** Ninguna corrección de interpretación entra por legacy. Lo aprendido se
  guarda como fila del Gold o del corpus E-* de la dimensión.
- **(d) Datos de legacy congelados hasta el CUTOVER (DECIDIDO por el usuario, 2026-10-01).**
  - **Qué se congela.** Los datos que legacy consume quedan idénticos a `main` byte a byte, igual que el código:
    - alias y grupos de compradores (`aliases_db.json`, `buyer_source_names.json`, `buyer_groups.json`,
      `entity_source_aliases/`);
    - `territorial_knowledge_v0.json`;
    - DIVIPOLA y los datos municipales;
    - los catálogos (`smmlv.py`, `estados.py`, modalidad);
    - cualquier otro `app/data/*`;
    - los artefactos municipales versionados que lee `territorial_knowledge` (`artifacts/audits/municipal_m2a`,
      `municipal_m2b`, `municipal_m2b_1`).
  - **Dónde van las correcciones.** Las de las autoridades nuevas (D-BUYER: INVIAS→ANI y demás; el catálogo SMMLV de
    E-VALUE; regiones; etc.) viven en **archivos de datos propios de la ruta de cita**, en `app/host_data/<dim>/`,
    con el carril de cada dimensión como dueño.
  - **Pin de inalcanzabilidad.** Ningún módulo del cierre legacy contiene el literal `host_data` ni importa nada de
    `app.host_data`.
  - **Riesgo operativo declarado.** `SECOP_ALIAS_DB_PATH` (`config.py:32`) puede apuntar a otro archivo desde el
    entorno. Mientras dure el congelamiento, el despliegue no debe definirla.
  - Levantar el congelamiento tras el CUTOVER es una decisión expresa que se registra en el commit del CUTOVER.
- **(e) Frontera de vocabulario.**
  - Legacy sólo alcanza del lado Foundation estos módulos: la hoja `foundation_guard`, el enum `SemanticDimension`
    (crecimiento aditivo con pin de lista blanca) y los dos auxiliares sin vocabulario `foundation_text_predicates`
    y `foundation_trace_view` (cubiertos por el sha de (b)).
  - Ningún tipo de la IR es alcanzable desde legacy.
  - El cambio concreto está en `C1_2_FRONTIER_SPEC.md`.
- **(f) Frontera del CUTOVER** (para que el gate de separación se pueda verificar después del borrado).
  - **Conjuntos.**
    - F (Foundation): `foundation_*` salvo los tres de (e), `host_authorities.*`, `interpretation_contract`, los
      módulos de ruta de cita de wave 1 y los módulos M2a.
    - L (interpretación legacy superviviente): el cierre desde las entradas legacy.
  - **Entradas.** Son las `ENTRYPOINTS` del pin. Después del CUTOVER el corte deja de ser la frontera de modo y pasa
    a ser `DISPATCH_FRONTIER`: las aristas `orchestrator → F`. `orchestrator` es el único módulo que importa los dos
    lados: despacha HANDOFF y `LEGACY_FOLLOWUP` a `handle_normal_flow`.
  - **Aristas permitidas.**
    - L → F: sólo hacia los tres módulos de (e).
    - F → L: sólo `foundation_route_owner` hacia los detectores de reclamación (papel (i) de D12). Ningún otro
      módulo de F importa `query_router`, los detectores de dueño ni `followup_engine`.
    - **Uso inyectado.** El pin cubre también el **uso del atributo `.query_router`**, que se inyecta por
      `workflow.deps` sin import.
      - Dentro de F sólo lo usan `foundation_route_owner`, `foundation_shadow_hook` (`:20`) y, hasta el CUTOVER,
        `foundation_basic_fallback` (`:89`), que muere en el commit del CUTOVER.
      - **Destino de `foundation_shadow_hook`.** Es el instrumento de paridad SHADOW de M0. Es sólo observacional:
        no cambia el resultado y un fallo suyo se registra como `SHADOW_ERROR`. Sobrevive hasta M4, como fija el
        roadmap. No es un papel de interpretación (D12).
      - Su coste: tras el CUTOVER, `QueryRouter.parse` corre en cada turno Foundation sólo para el ledger. Si la
        latencia lo exige, apagarlo es una opción reversible que no requiere enmienda.
    - F → ejecutores compartidos (`secop_client`, `soql_builder`, transporte, índices): permitido.
  - **Pines.** El cierre desde las entradas, cortando `DISPATCH_FRONTIER`, no contiene F salvo (e). El conjunto de
    importadores de L dentro de F es `{foundation_route_owner}`. Los usuarios de `.query_router` dentro de F son
    `{foundation_route_owner, foundation_shadow_hook}` (+ `foundation_basic_fallback` hasta el CUTOVER). La frontera del CUTOVER sustituye a `MODE_FRONTIER`
    en el commit del CUTOVER. En C1.6, `MODE_FRONTIER` pasa a «modo ∈ perfiles Foundation».

**D2. Invariante 6 reformulado (INV-OWN).** Sustituye a «nadie relee el texto crudo» (roadmap, invariante 6; C0 §2).
La regla es: *nadie deriva significado positivo de texto que esté fuera de un span con dueño*.

Toda condición o valor ejecutable sale de una de estas cuatro fuentes:
1. un span con dueño del propio turno;
2. un valor tipado arrastrado de un turno aceptado (M2a);
3. un evento de protocolo que el host acredita (un botón o una opción);
4. **una política declarada del host** (`HostPolicy` o `HostViewDecision`) que no lee texto y lleva eco. Por
   ejemplo: PV1–PV6, la aproximación O1B o ACTOR×STATE, el año por reloj de SMMLV, el tope de top-N y la dirección
   de orden por defecto.

Sobre el texto sólo se permite verificar la literalidad y los offsets de una cita, construir la ventana y detectar
residuo. En un `HANDOFF`, el dueño legacy es dueño de toda la superficie del turno (C0 R2). Que la lea con su propia
gramática es deuda de M3 (D12).

**D3. Registro de ocurrencias por turno.** Es un módulo nuevo del núcleo. Tiene una entrada por superficie con dueño
del turno, con exactamente estos campos: `span`, dimensión, estado de autoridad (RESOLVED, CLARIFY, BLOCK o NOT_RUN),
valor canónico (el `encode()`, o nada) y `opaque`.

- **Vocabulario de dimensión.** Los tipos del registro usan `SemanticDimension`, el enum de la IR v2. El `span` es un
  `AccreditedSpan`, cuyo campo `dimension: str` lleva siempre `SemanticDimension.value`. Nunca lleva los valores de
  `accreditation.spec.Dimension`, como MONEY o RANKING. Un test lo comprueba.
- **Reutilizar el tipo sin importar el flujo.** En E-0, `AccreditedSpan` se mueve a la hoja `app/core/accredited_span.py`,
  sin imports de `app`, y `accreditation/authorities.py` la reexporta. La identidad no cambia:
  `accreditation.authorities.AccreditedSpan is accredited_span.AccreditedSpan`. El núcleo importa sólo la hoja.
  Importar `app.core.accreditation` ejecutaría su `__init__` (credential, registry, seam, spec → entity_profile) y
  `authorities.py` (capability_binding), es decir, el flujo.
- **Coste declarado.** Como `accreditation.authorities` está en el cierre legacy (H13), la lista de divergencias de
  D1(b) **se amplía en E-0** con `accreditation.authorities`, cuyo cambio es sólo la reexportación, y la hoja nueva.
  La alternativa, un tercer tipo de span, multiplica la deuda de H13.
- **Qué no guarda.** No guarda texto crudo y no se persiste: el ledger sólo lleva conteos y un digest. Lo que M2a
  arrastra no es ocurrencia de este turno. El núcleo no llama a `assemble`, a las autoridades de texto ni a
  `residual_verifier` (pin AST).
- **Residuo.** Se define igual que en `residual_of`, pero se calcula con el tokenizador de la ventana. `material_tokens`
  no se reutiliza (H8).

**D4. Opacidad y anidación (INV-OPQ).** Una ocurrencia acreditada y opaca no puede reinterpretarse positivamente
desde otra dimensión.

- **Qué es opaco.** Sólo tres casos: un BUYER RESOLVED exacto, un ACTOR RESOLVED autoritativo (ADR-060) y una
  GEOGRAPHY RESOLVED exacta. El host lo sabe por la regla de recibo cerrada que emite la autoridad: `buyer.exact`,
  `actor.authoritative` o `geo.exact`. TOPIC nunca es opaco, ni ninguna otra dimensión.
- **Reglas, siempre por offsets:**
  - **(a) Anidación.** La identidad opaca más larga posee la superficie. Cuando una ocurrencia propuesta está
    estrictamente dentro de una opaca:
    - Si la misma superficie (comparada plegada) **reaparece sin cubrir** en otra parte del turno, la propuesta se
      **re-ancla** a esa ocurrencia, con la regla `reanchored`. Si reaparece más de una vez, se aclara. Es el caso
      de X020 y X059.
    - Si no reaparece, no genera ni filtro ni significado, y se registra `nested_in_opaque:<DIM>` en el ledger.
      Ejemplo: «Soledad» dentro de «Alcaldía de Soledad».
    - Nunca se descarta en silencio una superficie que tiene otra lectura disponible.
  - **(b) Contenciones y solapes.**
    - Una condición que deriva su valor del segmento entero (TOPIC, por ejemplo) y contiene a una opaca o la solapa
      en parte ⇒ CLARIFY `OVERLAPPING_OCCURRENCE`.
    - La evidencia de universo o de operación puede contener a una opaca si el token que la funda está fuera de la
      opaca. Si sólo está dentro, se aplica (a). Sin esta regla, los solapes UNIVERSE+BUYER y UNIVERSE+GEO de H7b
      se aclararían de más.
  - **(c) Mismo span.** Si dos ocurrencias opacas de distinta dimensión tienen el mismo span, se aclara.
  - **(d) Solapes entre no opacas.** Se permiten.
- **Orden de admisión.** Primero las identidades, después el registro y la anidación, y al final el resto.
- **La ventana sigue ciega a propósito (H6).** No cambia su contrato. Coste declarado: tokens opacos vecinos pueden
  retirar autoridad.
- **Antes del sello del Gold de follow-ups,** HERA identifica y revisa los 2 solapes BUYER+GEOGRAPHY de H7b.
  Comprueba si son X020 y X059 (homónimos) o casos reales de anidación.

**D5. Cobertura de spans (OQ-2 = C).**

- **Definiciones.**
  - **Residuo:** el turno menos los spans con dueño. Son condiciones, universo, fuente, evidencia de operación del
    job (que incluye los verbos de petición citados) y lenguaje de control. Los `candidate_ids` de petición no
    cuentan como cobertura (H8).
  - **Material de vocabulario cerrado:** los tokens del residuo que reconoce el vocabulario de alguna autoridad de la
    ruta de cita: meses y días (TIME), estados (STATE), comparadores y negadores (VALUE), unidades (VALUE),
    modalidades (MODALITY) y sustantivos de universo (H7).
  - **Ese vocabulario es único por construcción.** El conjunto disparador se obtiene importando el vocabulario que
    cada autoridad publica (`TRIGGER_TOKENS`, un requisito aditivo para cada carril), nunca de una lista copiada. Un
    pin compara el conjunto con la unión de lo publicado.
  - No reimporta stopwords: no se ignora nada, se exige dueño para lo que sólo tiene sentido con dueño.
- **Gate duro del CUTOVER (OQ-2 = C):** material de vocabulario cerrado sin dueño ⇒ CLARIFY `UNCOVERED_MATERIAL`.
  Sobre el Gold, con capturas v5.1 y el peor de 2 pasadas:
  - el gate añade **0 `L-only-OK`**;
  - su sobre-aclaración se **mide y se publica, sin umbral**.
  - Ningún porcentaje entra en el CUTOVER (enmienda 2). En la v2 había un «≤ 5 %» por error del orquestador; queda
    retirado.
- **Resto de la cobertura** (clase abierta: TOPIC, nombres de ACTOR): queda en SHADOW con garantía limitada
  declarada. Activar la **cobertura completa** es otro paso, fuera del CUTOVER. Su criterio es que haya L-0 (léxico
  funcional único de la ruta de cita, sin dominio), ≤ 5 % de sobre-aclaración en los EXECUTE CLEAN con v5 (peor de 2
  pasadas) y los verbos de petición citados como evidencia de operación.
- **Esto reconcilia la decisión 4 con el punto 11.** La parte de vocabulario cerrado es estructural desde el CUTOVER;
  la de clase abierta queda declarada como limitada.

**D6. Principio del gate cero.** Los gates de tolerancia cero los sostienen garantías estructurales: opacidad,
cobertura de vocabulario cerrado, fail-closed de cada autoridad, tipos con ida y vuelta y eco verificable. El Gold
sólo las falsifica. Si un caso pasa sin una garantía que lo explique, se declara como garantía limitada.

**D7. M2a es precondición del CUTOVER M1-I.** Sustituye a C0 §5.3.6.5 (H1, H2).

- **Alcance, definido por exclusión.**
  - Dentro: la `AcceptedInterpretation` persistida, la `PendingClarification` opcional, los deltas sobre una
    petición SEARCH y el referente a resultados. Los deltas son SET, CLEAR o KEEP de una ranura (atómicos, F1),
    REPLACE de universo y SET_ORDER con cardinalidad. El alcance medible es el que el Gold 2.1 marca
    `m2a_scope = IN*`.
  - Fuera (M2b): el Frame de Entity Profile, las anáforas entre capacidades, las listas pendientes generales,
    Suppliers y MULTI.
- **Referentes (T3.6/A6).**
  - El referente semántico es la efectiva aceptada. Existe tras cualquier READY, aunque la fuente falle o devuelva
    cero filas, y se puede refinar.
  - El referente a resultados es el manifiesto de la llamada más el cursor, y sólo existe si la fuente respondió.
  - Si algo necesita filas y no hay referente a resultados ⇒ `INFORM` (o CLARIFY si hay algo que elegir).
- **Búsqueda nueva = consulta autosuficiente (A4).**
  - Decide el productor.
    > **SUPERSEDED por la Enmienda 5 (A9/A9.1):** decide el host por la forma de la interpretación; ver
    > `M2A_CONVERSATION_CONTRACT_V1.md` §7.2.
  - Suelo del host: con un ancla o una pendiente vigentes, una propuesta sin condición propia nunca es búsqueda
    nueva. Toda búsqueda nueva con ancla vigente lleva el eco `CONTEXT_RESET` con la acción «aplicar a la búsqueda
    anterior».
  - Es una garantía limitada declarada.
- **Aclaración pendiente (A7).**
  - Una respuesta compatible modifica el marco pendiente.
  - Una consulta autosuficiente la cancela.
  - «No, mejor X» con un fragmento cancela y aplica X sobre el marco.
  - Si sigue ambigua, `REASK`; F24 y F25 también dan `REASK`. Nunca se ejecuta un híbrido.
- **Relativos temporales (OQ-3 = b).** En un `FOLLOWUP_DELTA` gana el único referente temporal activo de granularidad
  compatible, con aritmética de periodo (predecesor o sucesor) y anuncio con fechas absolutas
  (`RELATIVE_WINDOW_RESOLVED`). El reloj sólo aplica a consultas autosuficientes. Si no hay referente activo, o hay
  varios ⇒ CLARIFY. Los relativos no temporales siguen F15: se aclaran.
- **VALUE por lado (A1; OQ-4 = a).** F26 queda retirada.
  - Rango sin interior ⇒ CLARIFY con dos opciones-evento: «sustituir el rango» y «mantener y ajustar».
    > **SUPERSEDED por la Enmienda 5 (A8/A8.1):** ver `M2A_CONVERSATION_CONTRACT_V1.md` §15.2.
  - La unidad sólo se hereda si hay una única unidad activa, SMMLV incluida.
  - Con SMMLV, si cambia el año se recalcula sobre la vigencia nueva (A2).
- **Sin entrega a legacy con ancla Foundation.** Otra operación, un referente de fila o Radar-SAVE ⇒ `INFORM`. En
  `FOLLOWUP_DELTA` decide el dueño del ancla: Foundation ⇒ M2a; dueño legacy ⇒ `LEGACY_FOLLOWUP`.
- **Persistencia.**
  - Un almacén por chat, append-only, con lector estricto.
  - Nada va a git, a la traza ni al ledger, y la retención es la del historial.
  - `Turn.parsed_params` sigue siendo `{}`, que es la barrera para los lectores legacy.
  - Vigencia de 30 minutos (F11).
- **Eco y hashes.** El eco usa la lista cerrada del Gold 2.1 §7; se guarda y se compara con lo que se ejecutó.
  `interpretation_hash` pasa a ser el digest de la efectiva; el `plan_hash` del primer turno no cambia.
- **Tensiones abiertas (OQ-5).** Los casos AMBIGUOUS quedan fuera de los gates de cero. Si un caso CLEAR depende de
  una tensión X7–X17 o de `UNIVERSE_FRAGMENT`, se adjudica por lote antes del sello; HERA adjunta los ids al cerrar
  M2A-0.

**D8. Perfiles y rollback tras el CUTOVER.**

- **Qué muere en el commit del CUTOVER:** `foundation_basic_fallback.py`, el perfil `foundation_v3` (v4.2 queda sólo
  como comparador en el harness) y el valor `legacy` como interruptor de SEARCH (roadmap, invariante 5).
- **Configuración.** El conjunto pasa a `{foundation_v5, foundation_v5_previous}`, con **default `foundation_v5`**.
  Un valor retirado en el entorno (`legacy`, `foundation_v3`) es un error de configuración al arrancar, nunca un
  fallback.
- **Rollback de flag (gate obligatorio): `foundation_v5_previous`.** Es el par (SHA del prompt, wire) de la última v5
  acreditada, corriendo bajo el host vigente. Tiene que ser **elegible**: pasa los gates CUTOVER de SEARCH con el
  host del commit del CUTOVER y trata el almacén M2a como ancla inválida (probado). Si no hay ninguna versión
  elegible, no hay CUTOVER.
- **Es un rollback DEGRADADO en follow-ups, y no hay legacy detrás.** Con `foundation_v5_previous` activo, todo
  fragmento pide la búsqueda completa. Afecta a los follow-ups de anclas Foundation: 65 de 585 en REAL (11,1 %). Es
  el comportamiento de hoy, pero sin ninguna ruta legacy que lo absorba.
- **Plan de contingencia (no es gate):** revertir el commit del CUTOVER. Restaura juntos la puerta, el veto, el SEARCH
  legacy y v3, y el almacén queda inerte para legacy.
- **Motivo (H3).** v4.2 no puede ser el rollback: con el veto borrado, regresaría las 119 búsquedas con ACTOR.

**D9. P8: tope de top-N.**

- **Regla.** Por encima de 100 se ejecutan 100 y se anuncia; nunca se aclara, salvo OQ-6 (≥ 10⁹, política de
  E-ORDER) (T3.3).
- **Cambio declarado al contrato C1.1, antes del freeze de C1.2** (`C1_2_TOPN_SPEC.md`):
  - `cardinality@2` con `n` (lo ejecutado, 1..100; la cota de `:529` se mantiene) y `n_requested`.
  - Se cumple `n == min(n_requested, 100)`; en un singular, los dos valen 1.
  - El **tipo** sólo tiene un límite de representación, `MAX_REQUESTED_TOP_N = 10**18 - 1`, que garantiza la ida y
    vuelta.
- **De dónde sale el anuncio.** Nace del valor tipado (`n_requested > n`). La autoridad devuelve `NARROWING`, la
  política `topn-cap@1` y la regla `topn.capped`, y el kit lo exige. La aceptación registra la `HostPolicy`, y la
  presentación emite `TOPN_CAPPED`, tanto en el primer turno como en M2a. Ninguna otra estructura duplica la cifra.
- **Política de E-ORDER/host (no del contrato; OQ-6):** una cifra ≥ 10⁹ no es un tamaño de lista y se aclara con
  `UNRESOLVED_HOST_DIMENSION:CARDINALITY` y detalle `CARDINALITY_FIGURE_OUT_OF_RANGE`.

**D10. SMMLV ejecutable (requisito de E-VALUE; OQ-1 = a, forzada por la evidencia de H10).**

- **Catálogo propio de la ruta de cita.** Vive en `app/host_data/value/` y parte de una copia de los años 2024–2026
  de `app/data/smmlv.py`, con los decretos citados. Lo mantiene D-MONEY, y tras E-VALUE el núcleo, que añade cada año
  en enero. Un test trampa falla en CI desde el 1 de enero si falta el año.
- **El catálogo de legacy (`app/data/smmlv.py`) queda congelado (D1(d)).** Si un año nuevo llega antes del CUTOVER,
  legacy aclara (`unsupported_year`, `query_router.py:1593-1600`): falla cerrado y no ensancha.
- **Fuentes del año** (exactamente una por petición), decididas por el host sin leer texto:
  - **año único autoritativo:** un `TimeValue` RESOLVED de la petición contenido en un solo año civil; regla
    `year.from_time`;
  - **año inferido por reloj:** la petición no tiene TIME. Se usa `ctx.local_date().year`, con la regla
    `year.inferred`, `year_source = CLOCK_INFERRED`, la `HostPolicy smmlv-clock-year@1` y el eco
    `YEAR_CLOCK_INFERRED`. Esto respeta R0305 sellado y es lo que hace legacy;
  - **año heredado (M2a):** `year.inherited`.
- **Aclarar.** Si el TIME abarca varios años o el año no está en el catálogo, se aclara con **`SMMLV_YEAR_AMBIGUOUS`**,
  el código de A2. Nunca se usa el salario de otro año. El Gold M1 no se re-sella.
- **Mecanismo.** `resolve` es puro y monótono y ante SMMLV devuelve CLARIFY. El año entra por una **entrada separada**
  que sólo el host puede llamar, con un valor tipado: el año de una de las tres fuentes anteriores. Esa entrada
  emite `unit.smmlv` y la regla del año, y lleva eco de la conversión con la cifra en pesos y la vigencia (aviso
  `SMMLV_CONVERTED`). Es la misma entrada que usa la herencia de M2a, así que hay **una** sola excepción declarada a
  INV-C0-7.
- **Lo que no cambia.** `AuthorityContext` no se toca.
- **Procedencia.** Se guardan la cantidad en SMMLV y la vigencia, para poder recalcular.
- **Redondeo.** Producto exacto; si no es entero, hacia el lado que no ensancha.

**D11. Adaptadores conformes (H11).**

- Cada E-step entrega `host_authorities/<dim>.py`, que implementa `DimensionAuthority` y sólo devuelve
  `ExecutableValue` del contrato. Los tipos propios del carril no cruzan el adaptador.
- **Gate:** `assert_conforms`, el slice Gold de la dimensión y las filas E-* cuyo consumidor es SEARCH.
- **Lo que no es gate de E:** las 365 filas E-TIME de dueños legacy son contrato de M3. Las 24 de paginación son de
  M2A-6.

**D12. Qué sobrevive de QueryRouter tras M1.**

- «Se borra la interpretación SEARCH legacy» significa: tras el CUTOVER, ninguna configuración alcanzable ejecuta un
  SEARCH sin ancla con parámetros de `QueryRouter` (INV-C0-17).
- Sobreviven **tres** papeles:

| Papel | Muere en |
|---|---|
| (i) Reclamaciones de dueños (C0 §5.3.3) | M3 |
| (ii) Intérprete de valores del dueño legacy en una entrega (C0 R2; H12) | M3, dueño por dueño |
| (iii) Parser de deltas con ancla de dueño legacy (`LEGACY_FOLLOWUP`; 9 de 65 REAL) | M2b/M3 |

- Fuera de la interpretación, `foundation_shadow_hook` sigue leyendo `QueryRouter.parse` como instrumento SHADOW
  observacional hasta M4 (roadmap; D1(f)).
- Mueren dentro de M1 el parser de deltas con ancla Foundation (lo sustituye M2a) y el camino SEARCH de
  `foundation_v3` y `foundation_basic_fallback` (en el CUTOVER).

**D13. División de `foundation_guard` (frontera de C1.2).**

- `foundation_guard.py` queda como hoja de frontera de fuente: los `ContextVar` y los seis ganchos, importando sólo la
  stdlib y `source_call_receipts`.
- `ExecutionGuard`, `FoundationGuardError`, `RequestExecution` y `_without_order` pasan a `foundation_execution_guard.py`,
  que es sólo de Foundation.
- `secop_client.py` sigue idéntico byte a byte a `main`.

**Opciones descartadas:**

| Opción | Por qué no |
|---|---|
| Partir `interpretation_contract` | El guard seguiría alcanzando la proyección |
| Imports perezosos | El escáner los cuenta |
| Ensanchar la frontera | Legacy alcanzaría vocabulario Foundation |

### 4.3 Cambios declarados al contrato congelado C1.1

- **Cambia:** `foundation_authority_values.py`, con `cardinality@2`, el comentario de `:33` y `MAX_REQUESTED_TOP_N`.
  El kit gana la comprobación INV-C1.2-CAP.
- **Aditivo:**
  - la entrada separada de insumos del host (D10 y M2a);
  - el registro de ocurrencias (D3);
  - `TRIGGER_TOKENS` publicados por autoridad (D5);
  - las reglas de recibo con efecto en el host, en la lista cerrada `HOST_SIGNIFICANT_RULES`: `buyer.exact`,
    `actor.authoritative`, `geo.exact`, `reanchored`, `topn.capped`, `unit.<escala>`, `unit.sibling`,
    `unit.inherited`, `unit.smmlv`, `year.from_time`, `year.inferred`, `year.inherited`,
    `order.direction_defaulted`, `buyer.territory.<código>` y `buyer.national`.
- **No cambian:** `EvidenceWindow`, `build_window`, `AuthorityContext`, `DimensionEvidence`, `AuthorityVerdict`,
  `AUTHORITY_REASONS` y `MAX_TOP_N`.

### 4.4 Orden (sustituye lo que toca de C0 §6; coherente con M2A-4 de `M2A_DESIGN_v2_1.md`)

| Paso | Cambio respecto de C0 |
|---|---|
| C1.2 | Antes de aplicar las specs se commitea el WIP de C1.2 tal cual (decisión del usuario). Después entran `C1_2_TOPN_SPEC.md` y `C1_2_FRONTIER_SPEC.md`. Los pines de paridad son gate. |
| C1.6.0 | La partición del Gold de SEARCH divide TEST en **TEST_A** (evaluación de v5) y **TEST_B** (evaluación de v5.1, que nadie ve antes). Se estratifica por slice y clase y se fija antes de escribir el prompt v5. |
| C1.6 | El prompt v5 va sin sección de estado. Criterios ACTIVATE_V5 sobre TEST_A (dos pasadas) y DA-6 con custodio. `MODE_FRONTIER` pasa a «modo ∈ perfiles Foundation». |
| C1.3–C1.5, C1.7, ACTIVATE_V5 | Sin cambio. |
| M2A-0 (HERA, desde ya) | Gold de follow-ups 2.1 con A1–A7 y las decisiones de OQ-1, OQ-3, OQ-4 y OQ-5. Revisión de los 2 solapes de H7b. Sello y partición DEV/TEST propia. |
| **E-0** (antes del primer ACTIVE) | Hoja `accredited_span` y registro de ocurrencias; fases de admisión; opacidad, anidación y re-anclaje; cobertura en SHADOW. |
| **L-0** (antes de E-TIME ACTIVE) | Léxico funcional único de la ruta de cita. |
| E-* | Cada uno con su adaptador (D11) y sus `TRIGGER_TOKENS` (D5). E-VALUE con SMMLV (D10). E-ORDER con el tope y la política ≥ 10⁹ (D9). E-BUYER, E-GEO y E-ACTOR emiten las reglas de exactitud. **E-ACTOR ACTIVE es obligatorio antes del CUTOVER.** |
| F, G | F espera a OQ-7. G integrado antes del CUTOVER. |
| M2A-1 → M2A-2 → M2A-3 → M2A-3b/3c | Núcleo en serie. M2A-1 introduce las directivas (`CLEAR`, `KEEP`, `CANCEL_PENDING`) en el wire 5.1. |
| **M2A-4 = v5.1** | Tarjeta de estado, ancla, fila `FOLLOWUP_DELTA`, eco, y **el prompt v5.1** (sección de estado). Hasta 3 iteraciones sobre DEV de los dos Gold. |
| M2A-5, M2A-6, M2A-8 | Pendiente, página y eventos (F), barrera de Radar (ARES no-núcleo). |
| **Activación de v5.1** (HERA) | Umbrales ACTIVATE_V5 de C0 §5.4.3 sobre **TEST_B**, sin regresión frente a v5 en TEST_B. Gates M2a de §4.5 en 0 sobre el TEST de follow-ups. DA-6 sin regresión, con custodio propio. Nunca se reutiliza TEST_A ni un DEV. |
| M2A-9 | Freeze, ZEUS fresco y la suite global pareada. |
| CUTOVER (M1-I) | Gates de §4.5 sobre los Gold completos, con al menos 2 pasadas. Borra la puerta, el veto, `continues_foundation_turn`, `last_foundation_turn`, `foundation_basic_fallback`, `foundation_v3` y `legacy`. Añade `foundation_v5_previous` (la v5 de C1.6). El default pasa a `foundation_v5`. `DISPATCH_FRONTIER` sustituye a `MODE_FRONTIER`. |

### 4.5 Gates CUTOVER añadidos (0 en todos, peor de al menos 2 pasadas, sin porcentajes en las filas de defecto)

| Gate | Exigencia |
|---|---|
| M2a (Gold de follow-ups 2.1 con A1–A7 sellado; se mide la efectiva resultante; sólo casos CLEAR) | 0 en: fragmento ejecutado solo; condición arrastrada perdida sin una operación citada; híbrido tras una pendiente, o respuesta elegida por el producto; composición sobre una base inválida; turno con ancla Foundation que llega a legacy; `OUT_*` o `INFORM` que llama a la fuente; relativo no temporal ejecutado; relativo temporal ejecutado sin referente único o sin eco; uso de filas sin referente a resultados; consulta autosuficiente y delta confundidos; follow-up que legacy acierta y M2a no, por clase; **0 discrepancias** de disposición, código y efectiva con la IR oráculo por el host. |
| Eco | 0 valores heredados, inferidos por reloj, puestos por defecto, recortados o convertidos sin su eco, en el primer turno y en follow-ups. |
| Opacidad | 0 significados derivados de una superficie anidada en una opaca. 0 descartes de una superficie que reaparece sin cubrir. X020 y X059 (CLEAR) dan su disposición Gold. X038 (AMBIGUOUS, MULTI) queda fuera de los gates de cero y sólo se informa. |
| Cobertura de vocabulario cerrado (D5) | El gate duro está activo y añade 0 `L-only-OK`. La sobre-aclaración está medida y publicada, sin umbral. |
| Persistencia | Todo turno Foundation escribe su registro. El lector estricto está probado. Paginación y opciones funcionan con el Frame apagado. |
| SEARCH legacy borrado | INV-C0-17: 0 SEARCH sin ancla con parámetros de `QueryRouter`, en todos los perfiles alcanzables. |
| Separación | Pin generado del cierre legacy (D1(b)) y pines de `DISPATCH_FRONTIER` (D1(f)) en verde. |
| Rollback | `foundation_v5_previous` **elegible** (D8). Sin alternativa. |
| VALUE/SMMLV | R0302 y R0305 dan su disposición sellada. |
| Top-N | 0 top-N por encima de 100 aclarados o ejecutados sin aviso. Lo garantizan el tipo, el kit y la presentación. |
| Precondiciones | OQ-7 resuelta y F integrado (por transitividad, sin F no hay CUTOVER). Pin de datos congelados (D1(b,d)) en verde. |

Queda sin efecto el inciso (g) de `M2A_DESIGN_v2_1.md` §7.2 (re-sello del Gold M1, por T3.2), igual que su regla
F26 y su `INHERITED_UNIT_IMPLAUSIBLE` (A1).

### 4.6 Consecuencias

- C0 se parchea según `C0_PATCHES`.
- El apartado «Rollback» de este ADR queda así:
  - hasta el CUTOVER, `foundation_v3` o `legacy`;
  - después, D8.
- En el contrato M1, D6 deja de valer para las anclas Foundation.
- Habrá dos evaluaciones de prompt, sobre particiones TEST disjuntas.
- Toda modificación de un módulo `.py` del cierre legacy exige regenerar el manifiesto con un motivo declarado. Es
  deliberado (D1(c)), y es una fricción para otros carriles que tocan legacy en `main`.
- Hasta el CUTOVER no se modifica ningún archivo de `app/data/**` (D1(d)).
- Las correcciones de datos se duplican en `app/host_data/`. Durante esa ventana legacy y Foundation pueden resolver
  distinto el mismo nombre; la diferencia se mide en la matriz de paridad.
- Tras el CUTOVER no queda ningún interruptor que reproduzca `main`. El rollback de flag degrada los follow-ups (D8).

### 4.7 Deuda con dueño y vencimiento

| Deuda | Dueño | Vence |
|---|---|---|
| Dos enums de dimensión y dos `AuthorityReceipt`. Mientras tanto no puede aparecer un tercero. | Núcleo | Cierre de M3 |
| Reexportación de `AccreditedSpan` en `accreditation.authorities` (divergencia declarada desde E-0) | Núcleo | Cierre de M3 |
| 18 léxicos funcionales (sólo se unifica la ruta de cita) | Núcleo (L-0) | Antes de activar la cobertura de clase abierta |
| QueryRouter: papeles (i), (ii) y (iii) | Núcleo | M3 / M2b |
| 365 filas E-TIME de dueños legacy | Dueño de cada manifiesto | M3 |
| Catálogo SMMLV anual (en `app/host_data/value/`) | D-MONEY, luego el núcleo | Cada enero |
| **Errores de datos conocidos de legacy (p. ej. INVIAS→ANI, `entity_resolver` exact)** que siguen hasta el CUTOVER por el congelamiento | D-BUYER (corrección en `host_data`); levantamiento en el CUTOVER | CUTOVER |
| Datos duplicados entre `app/data` y `app/host_data` | Núcleo con cada carril D | Commit del CUTOVER (se decide si se levanta el congelamiento) |
| `foundation_time_coverage` lee `raw` | D-TIME | E-TIME |
| `MODE_FRONTIER` y la guarda `!= "foundation_v3"` | Núcleo | C1.6; `DISPATCH_FRONTIER` en el CUTOVER |
| `semantic_dimensions` alcanzable desde legacy | Núcleo | M4 |
| `foundation_shadow_hook` ejecuta `QueryRouter.parse` en cada turno Foundation (sólo ledger) | Núcleo | M4 (roadmap) |
| Cambios de C0 §5.6 de Gold v1.7–v1.9 (OQ-7) | METIS | Antes de F; bloquea F y, por transitividad, el CUTOVER |

### 4.8 STOP y qué cambiaría estas decisiones

- **STOP:**
  - un E-step necesita un cambio de tipo que no esté en §4.3;
  - un adaptador sólo puede hacerse conforme cambiando un módulo legacy;
  - alguien propone corregir un lector legacy;
  - se integra C1.2 con los pines de paridad en rojo;
  - cualquier cambio en `app/data/**` antes del CUTOVER, o una corrección de datos de autoridad fuera de `app/host_data/`;
  - se sella el Gold de follow-ups sin haber revisado H7b;
  - no hay una `foundation_v5_previous` elegible;
  - la cobertura de vocabulario cerrado añade algún `L-only-OK`;
  - hay una violación de opacidad o un híbrido en dogfood: se vuelve a `foundation_v5_previous`.
- **Qué cambiaría las decisiones:**
  - Sobre-aclaración medible por tokens opacos vecinos ⇒ materializar la opacidad en la ventana (cambio de contrato).
  - La sobre-aclaración publicada de la cobertura de vocabulario cerrado resulta inaceptable para el producto ⇒
    el usuario decide (no hay umbral automático).
  - El productor no separa autosuficiente de fragmento tras 3 iteraciones ⇒ no hay CUTOVER.
  - Ninguna v5 elegible ⇒ el usuario decide si `foundation_v3` sobrevive un release (contradice el punto 7).

## Enmienda 5 (2026-10-01): autoridad normativa de M2a y cláusulas de D7 sustituidas

> **Numeración.** «5» verificado libre en `main` y en los 262 worktrees y todas las ramas locales (máximo previo: 4).
> **Autoridad.** Adjudicaciones del usuario del 2026-10-01 A8, A8.1, A9, A9.1 y G-FU-03
> (`docs/audits/m2a_advisory_2026-10-01/02_gold_followups/kit_v2_2/ADJUDICATED_OVERRIDES.md`, posteriores a la Enm. 4).
> El resto de la Enmienda 4 queda intacto.

1. **Autoridad de M2a.** `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` es la autoridad normativa única de M2a
   (tipos persistidos, `SemanticPatch`, búsqueda nueva frente a delta, `PendingClarification`, referentes, eco, gates
   M2a). D7 sigue vigente salvo lo que sustituye el punto 2; fuera de esas cláusulas el contrato precisa D7 sin
   cambiarlo, y una divergencia nueva con D7 exige otra enmienda.
2. **Cláusulas de D7 sustituidas (SUPERSEDED):**
   - «Rango sin interior ⇒ CLARIFY con dos opciones-evento» (viñeta «VALUE por lado»; OQ-4 = a) → contrato §15.2
     (A8/A8.1: un intervalo generado por un follow-up que queda vacío o degenerado a un punto sustituye la restricción
     VALUE completa y se anuncia; una igualdad pedida explícitamente sigue siendo válida).
   - «Búsqueda nueva = consulta autosuficiente (A4). Decide el productor.» → contrato §7.2 (A9/A9.1: con SEARCH activa,
     un turno que sólo aporta condiciones o modificadores, sin TOPIC ni ACTOR nuevos ni operación nueva, es delta; se
     decide por la forma de la interpretación, sin léxico).
   - Donde §4.4 (M2A-0), §4.5 (gate M2a) y C0 §5.3.6 punto 5(b) dicen «Gold de follow-ups 2.1 con A1–A7 sellado», léase
     `M2A_GOLD` sellado con A1–A9.1 (`docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json`: 49 REAL + 150 SYNTHETIC = 199).
3. **Regla de recibo añadida a la lista cerrada de §4.3** (aditiva, sin tipo nuevo): `time.relative_to_previous`, que
   emite la autoridad TIME por la entrada separada de insumos del host al resolver un relativo temporal respecto del
   único referente previo (OQ-3 = b, ya adoptado en D7).
4. **H7b revisado** antes del sello (§4.8): los dos solapes BUYER+GEOGRAPHY son X020 y X059 (homónimos; D4(a) los
   explica) y son independientes del Gold M2a (`docs/audits/m2a_conversation/M2A0_H7B_REVIEW.md`). Errata: «2 de 294» es «2 de 394».
5. **Deuda añadida a §4.7:**

| Deuda | Dueño | Vence |
|---|---|---|
| La exención `data/conversations/` del pin de frontera C1.2 deja 20 JSONL versionados fuera del congelamiento (P3; detalle en `docs/audits/m1_c1/C1_2_FRONTIER_PIN_DEBT.md`) | Núcleo | CUTOVER (decidir) |

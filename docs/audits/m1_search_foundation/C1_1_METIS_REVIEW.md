# M1-C1.1 — Revisión METIS del contrato de autoridades antes de congelarlo

Fecha: 2026-09-30. Revisor: METIS (sólo lectura). Candidato: rama `mission/m1-c1-spine`, worktree
`secoppal-m1-c1`, HEAD `880e5b73`, base `4ec99bc1`. Referencia: `C0_SPINE_V5_DESIGN.md` §3, §5.5, §6 C1.1,
§7, §10; ADR-103 con enmiendas 1–3. Carriles contrastados: D-TIME (`secoppal-m1-dtime` @ `24965fb6`,
`app/core/time_authority.py`), D-MONEY (`secoppal-m1-dmoney` @ `54fe83d2`, `app/core/value_authority.py`),
D-GEO (`secoppal-m1-dgeo` @ `01708198`, `app/core/geo_authority.py`); ADR-074 para BUYER.

Lo que corrí: `pytest tests/test_foundation_authority_contract.py tests/test_foundation_authority_kit.py`
sobre `880e5b73` (153 passed) y una sonda ad hoc de `build_window` y de los valores (abajo, §2.5). No corrí
la suite ni los mutantes del implementador.

## Veredicto

**FREEZE_WITH_CHANGES.** El implementador siguió §5.5 con fidelidad: casi todos los defectos que
aparecen son **del diseño C0** y salen a la luz al contrastarlo con los carriles reales. Los asumo
como enmienda de §5.5.1. Tres de ellos son caros si se congelan así:

1. La ventana no puede representar la condición S3 que hoy se ejecuta (ADR-100): es una regresión
   garantizada en E-STATE.
2. `StateValue` no puede expresar ni la negación que hoy se ejecuta («no terminados»,
   `celebrated-state-complement`) ni la proyección de procesos, que filtra por dos columnas.
3. `BuyerValue.owners == 1` cierra la puerta a los grupos declarados de ADR-074, y `GeoValue` no tiene
   sitio para el departamento de un municipio.

Los cambios forman una lista cerrada (§5). Ninguno toca el runtime. C1.1 sigue siendo solo de
archivos nuevos.

---

## 1. Fidelidad a §5.5 e INV-C0-7/8/9

| Punto | Estado | Desviación |
|---|---|---|
| `EvidenceWindow(spans, quote, left, right)`, `WINDOW_TOKENS=4` | Forma fiel | **D1.** §5.5.1 declaraba varias citas (`left` antes de la primera, `right` después de la última). El constructor reduce esto a una sola frase: delega en `material_phrase`, que rechaza las citas distintas no contiguas con un código fuera del conjunto cerrado (`MULTI_EVIDENCE_STATE_UNSUPPORTED`), y además rechaza la repetición (`DISJOINT_EVIDENCE`). Ver §2.5. |
| Constructor único después de `validate_raw` (§5.5.2) | Fiel | Tokenizador reimplementado porque H0 no está en la base (aceptado, §2 D3). **D2.** Una cita que empieza o termina dentro de una palabra («obras» en «sobras») deja el fragmento como token (`left=(…,'s')`). Ningún carril puede distinguir «s obras» de «sobras»: hay que rechazarla en el constructor. |
| `AuthorityContext(reference_instant, datasets)` | Fiel a la letra | **D3 (defecto de diseño).** «`datasets` vacío con UNSPECIFIED» hace que STATE/MODALITY no puedan producir literales por dataset antes de PV. Resultado: `STATE_NOT_IN_VIEW` siempre con UNSPECIFIED. **D4.** Cada carril convierte el instante en fecha por su cuenta. D-TIME necesita `today: date`; si un adaptador usa UTC y otro hora de Bogotá, «este año» cambia la noche del 31 de diciembre (misma familia que DEPLOY-TZ-001). |
| `DimensionEvidence` | Fiel | `level` sólo con GEOGRAPHY; el operador no se valida hasta C1.2 (aceptado). |
| `AuthorityVerdict` | Fiel, más estricto | Correcto. **D5.** Las razones son cerradas y gruesas, y no hay canal para el motivo fino que los tres carriles ya producen (`TIME_YEAR_NOT_STATED`, `VALUE_OPERATOR_BOUNDARY_MISMATCH`, `GEO_MUNICIPALITY_AMBIGUOUS`). Sin él, el núcleo pierde los mensajes de aclaración específicos que existen hoy. |
| `ExecutableValue` (11 tipos) | Fiel a la tabla de §5.5.1 | Los defectos están en la tabla misma: §3 (STATE, BUYER, GEO, negación, CARDINALITY). |
| `AUTHORITY_REASONS` + alias `VALUE_*` | Fiel | **D6.** Los alias no tienen sufijo, así que el kit los acepta en cualquier dimensión (una autoridad TIME puede devolver `VALUE_OPERATOR_AMBIGUOUS` y pasa). Además falta `GENERIC_IDENTITY:GEOGRAPHY`, que D-GEO ya emite para «Colombia». |
| Regla de polaridad | Fiel en el kit | Sin un campo de exclusión en los valores, la regla obliga a responder `NEGATION_UNSUPPORTED` en todas las dimensiones salvo MODALITY (ver §3). |
| INV-C0-7 | Cumple, más fuerte (cadena de truncados) | Correcto. Le falta la parte equivalente para el contenido **entre** citas, que hoy no existe porque la ventana lo prohíbe (§2.5). |
| INV-C0-8 | Cumple con límites declarados | No vigila la E/S de ficheros ni un reloj capturado en el constructor. D-GEO lee catálogos a través de funciones de módulo con caché (`get_municipal_territories`): el adaptador debe recibirlos en `AuthorityDeps.territories` (obligación del carril, no cambio de contrato). |
| INV-C0-9 | Cumple | Codificación etiquetada, versionada y canónica; `decode` estricto. Correcto. |
| Registro y madurez (§5.5.3) | Fiel | — |
| **Kit, comprobación de nivel** | **Desviación** | **D7.** `GeoValue.level is evidence.level` es falso por construcción cuando el productor dice `UNKNOWN`, porque un valor nunca lleva `UNKNOWN`. D-GEO resuelve `UNKNOWN` a un nivel concreto cuando la cita no es ambigua, así que el kit lo rechazaría siempre. |

## 2. Decisiones del implementador (§3 de `C1_1_CANDIDATE.md`)

| # | Decisión | Veredicto | Motivo |
|---|---|---|---|
| 1 | Valores en `foundation_authority_values.py`, reexportados | **Acepto** | Política de no monolitos; el contrato público no cambia. |
| 2 | `GeoLevel` en `foundation_geo_level.py` | **Acepto** | Evita una dependencia circular C1.1↔C1.2. Obligación de C1.2: importarlo, no redefinirlo. |
| 3 | `window_tokens` propio (H0 no está en la base) | **Acepto** | Obligación: cuando H0/D-TIME migren `foundation_time_coverage.py`, deben importar `window_tokens` y `WINDOW_TOKENS` del núcleo. D-TIME hoy duplica `WINDOW_TOKENS`, `fold` y `tokens_of` en `time_window.py`, y debe dejar de hacerlo al adaptarse. |
| 4 | `build_window` lanza `EvidenceWindowError(reason)` | **Acepto con cambio** | Lanzar está bien. Los códigos tienen que ser propios y cerrados; hoy se filtran los de `material_phrase` (`NO_MATERIAL_EVIDENCE:<D>`, `MULTI_EVIDENCE_<D>_UNSUPPORTED`). Ver el cambio W2. |
| 5 | **`DISJOINT_EVIDENCE`** | **Rechazo** | Ver §2.5. |
| 6 | La ventana se valida sola; `truncated/stripped` sólo quitan contexto; test de constructor único | **Acepto** | Se amplía a segmentos y huecos (W1). |
| 7 | `DimensionEvidence`: dimensión por nombre, `level` sólo con GEOGRAPHY, operador sin validar | **Acepto** | — |
| 8 | `AUTHORITY_REASONS` como producto código × dimensión; alias fijados a `KNOWN_CODES` | **Acepto con cambio** | Se añade `GENERIC_IDENTITY:GEOGRAPHY` y los alias quedan restringidos a VALUE en el kit (R1, K2). |
| 9 | Invariantes extra del veredicto (RESOLVED sin razón, no-RESOLVED ⇒ EQUIVALENT, efecto ≠ EQUIVALENT ⇒ política, UNAVAILABLE ⇒ BLOCK, `<id>@<v>`) | **Acepto** | Las cuatro reglas son correctas, y el núcleo traduce la *decisión* del veredicto (CLARIFY→NEEDS_CLARIFICATION, BLOCK→UNSUPPORTED), no la razón. Que la frase de §8 «la traducción CLARIFY/BLOCK del resto de razones queda al núcleo» no se lea como que el núcleo re-decide. |
| 10 | Campos de `AuthorityReceipt` | **Acepto** | Límite declarado: un identificador puede llevar texto del usuario. D-GEO hoy pone `name:<cita>`/`qualifier:<…>` en `evidence`: el adaptador debe usar digests, no nombres (obligación del carril; P-7). |
| 11 | `to_resolution(target_id)` | **Acepto** | Ata el valor codificado a `HostResolution.resolved_ids` sin cambiar su forma. |
| 12a | `BuyerValue.owners` exactamente 1 | **Rechazo** | Ver §3, BUYER. |
| 12b | `ActorValue.document_kind` identificador abierto, sin `UNKNOWN` | **Acepto** | Cédula y NIT se solapan numéricamente; filtrar por un documento de tipo desconocido mezclaría personas distintas. Fallar cerrado es lo correcto. |
| 12c | `GeoValue` sin `UNKNOWN`; DIVIPOLA de 2 o 5 dígitos; `members` sii REGION | **Acepto con cambio** | Falta `department` (§3, GEO). |
| 12d | `TimeValue` semiabierto, `precision` etiqueta abierta | **Acepto** | Con una regla: `precision` no es ejecutable (la proyección sólo lee `start`/`end_exclusive`; `precision` es para el eco y el recibo). |
| 12e | `MoneyValue.amount_cop ≥ 0` | **Acepto** | D-MONEY exige > 0, que es más estricto y compatible. |
| 12f | `StateValue.by_dataset` como tupla ordenada; 11 clases | **Rechazo en la forma** | Ver §3, STATE. |
| 12g | `ModalityValue` exige `procedure_ids` o `family_policy` | **Acepto** | Cubre M3/O5 (`modality-o6@1`, no filtra). |
| 12h | `ContractTypeValue.type_ids` no vacío; el residuo es deber de la autoridad | **Acepto** | — |
| 12i | `CardinalityValue.n ∈ 1..100` | **Acepto con cambio** | Falta la base (§3, CARDINALITY). |
| 13 | Kit más estricto que la letra | **Acepto con cambios** | K1–K5. |
| 14 | Ownership sin forzar el conflicto | **Acepto** | — |

### 2.5 `DISJOINT_EVIDENCE`: qué hace la ventana con varias citas

**Hechos.** Sonda sobre `880e5b73`:

- «procesos publicados o abiertos en Cali», con citas «publicados» y «abiertos»: devuelve
  `MULTI_EVIDENCE_STATE_UNSUPPORTED`. `material_phrase` lo rechaza antes de llegar a la comprobación
  de disjunción.
- «obras en Cali y obras en Bogota», con «obras» citado dos veces: devuelve `DISJOINT_EVIDENCE`.

El host vigente (`foundation_state_composition.composition_reason`) acredita el embudo S3 leyendo
precisamente el texto **entre** las dos citas (`raw[min:max]`, «o» desnudo). Así que la condición S3
(prompt v4.2, ADR-100) no tendría ninguna ventana posible: E-STATE regresaría de forma determinista.
G1/P-6 («toda cita se lee») acepta hoy la repetición.

**Decisión.** La ventana representa varias citas. Lo que hay entre ellas se expone de forma acotada,
y se trata como **evidencia estructural, no como contexto**:

```text
EvidenceWindow(frozen):
  spans:    tuple[RawSpan, ...]          # citas distintas, ordenadas (como hoy)
  segments: tuple[str, ...]              # NFC; un segmento por tramo máximo de citas contiguas
                                         # (solapadas o separadas sólo por espacios); 1..MAX_EVIDENCE_SEGMENTS
  gaps:     tuple[tuple[str, ...], ...]  # tokens plegados entre segmentos consecutivos;
                                         # len(gaps) == len(segments) - 1; cada uno ≤ 2·WINDOW_TOKENS
  left:     tuple[str, ...]              # ≤ K antes del primer segmento (como hoy)
  right:    tuple[str, ...]              # ≤ K después del último (como hoy)
  quote -> str | None  (propiedad)       # el segmento único si todos los segmentos son la misma frase
                                         # plegada (G1, repetición); None si son frases distintas
MAX_EVIDENCE_SEGMENTS = 4
```

Reglas:

1. **Por qué el tope de hueco es 2K.** Cada cita «ve» K tokens a cada lado. Un hueco de hasta 2K es
   exactamente la unión de la derecha de una cita y la izquierda de la siguiente, así que nada
   queda oculto. Un hueco mayor es `EVIDENCE_GAP_TOO_WIDE` y la petición se aclara. La ventana nunca
   elide el centro de un hueco, así que tampoco existe un hueco a medias. Lo que expone la ventana
   sigue acotado (≤ 2K·3 + 2K tokens fuera de la cita).
2. **Monotonía del hueco (INV-C0-7b, nueva).** El valor sólo puede derivarse de los `segments`. El
   contenido de un hueco sólo decide entre «RESOLVED(v)» y «no RESOLVED»: nunca cambia `v`. El
   embudo S3 cumple: con el hueco `("o",)` resuelve `open` con la política `ADR-100@1`; con cualquier
   otro hueco («y», una coma, más palabras) da CLARIFY `RESIDUE_UNREAD:STATE` con detalle
   `STATE_COMPOSITION_UNSUPPORTED`.
3. `truncated()`/`stripped()` conservan `segments` y `gaps`: el hueco no es contexto.
4. Las dimensiones de frase única (TOPIC, BUYER, GEO, TIME, VALUE, CONTRACT_TYPE, CARDINALITY) leen
   `quote`. Con `quote is None` (frases distintas) devuelven `RESIDUE_UNREAD:<D>`; es la misma
   disposición que hoy da `material_phrase`, ahora con una razón cerrada. STATE y MODALITY pueden leer
   `segments` y `gaps`.
5. La repetición de G1 («obras … obras») se acepta. El calificador del medio está en el hueco, a la
   vista de la autoridad. Esto resuelve la objeción que motivó `DISJOINT_EVIDENCE`.

## 3. ¿Encajan los carriles con adaptadores finos?

**D-TIME** (`resolve_time_condition(quote, left, right, operator, polarity, *, today)` → `TimeResolution`
con `decision/value/reason/detail`). Encaja: `TimeValue` coincide campo a campo; las razones que usa
(`NEGATION_UNSUPPORTED`, `OPERATOR_EVIDENCE_MISMATCH`, `UNCITED_QUALIFIER`, `UNRESOLVED_HOST_DIMENSION`)
están en el conjunto; `authority="time-authority@1"` cumple el formato.

- Le falta un canal para `detail`: cambio V3.
- Le falta una fecha local única para `today`: cambio V2.
- `precision` (year, month, day_range, open_from, bounded…) cabe en la etiqueta abierta.
- NEGATIVE → BLOCK está permitido.
- Con `quote is None` (W1) el adaptador devuelve `RESIDUE_UNREAD:TIME`.

**D-MONEY** (`resolve_value_condition(quote, operator, polarity, left, right)` → `ValueResolution`).
Encaja: el comparador se traduce a `Comparator`; `amount_cop > 0` cabe; `TARGET_REASON` cae en razones
del conjunto, y los cuatro `VALUE_OPERATOR_*` también son alias válidos. La negación dentro de la cita
se absorbe en el comparador con polaridad POSITIVE, y la NEGATIVE da BLOCK: conforme. Su codificación
propia (`("money@1", cmp, n)`) **no** es la del contrato. El adaptador debe usar
`foundation_authority_values.MoneyValue` y la propia queda como deuda del carril. No falta nada.

**D-GEO** (`resolve_geography(quote, level, polarity, *, department_hint, left, right)` → `GeoResolution`):

- **Falta** `GeoValue.department`. D-GEO lo mete hoy en `members=(departamento,)` para MUNICIPALITY, y
  el contrato lo rechaza (`INVALID_GEO_MEMBERS`, sondeado). La SoQL municipal necesita
  `departamento* = <dept> AND ciudad* IN (…)` (su CANDIDATE §Proyección). Cambio V5c.
- **Falta** `GENERIC_IDENTITY:GEOGRAPHY` («Colombia» no es un filtro). Cambio R1.
- **Kit**: un nivel `UNKNOWN` resuelto a uno concreto debe ser conforme. Cambio K1.
- **Sobra en el valor**: `secop_city_values` (las grafías de `ciudad` en SECOP) no va al valor. Es un
  binding de proyección indexado por DIVIPOLA, análogo a ADR-073 para compradores. Vive en los datos
  de D-GEO y lo consume la proyección en E-GEO; así el valor sigue siendo identidad y no literal de
  fuente.
- `department_hint` (información entre condiciones) no entra en `DimensionEvidence`: el adaptador pasa
  `None`. «Florencia, Caquetá» se cita como una sola frase.
- Los `candidates` para aclarar no se representan. Es un non-goal de C1.1; puede añadirse más tarde
  como campo con valor por defecto (`alternatives: tuple[ExecutableValue, ...] = ()`) sin romper a
  ningún carril.

**BUYER (ADR-074).** `owners: frozenset` de exactamente un elemento mezcla dos cosas: la ambigüedad
del alias, que es asunto de la autoridad (si hay más de un dueño, `AMBIGUOUS_HOST_DIMENSION:BUYER`,
nunca un valor), y la composición del comprador resuelto, que sí es asunto del valor. ADR-074 §4
exige BuyerGroup declarado y versionado («SENA» = todas sus regionales), con la unidad explícita por
encima del grupo y sin derivarlo de NIT ni de territorio. Con el contrato actual eso no se puede
expresar (`BUYER_OWNER_NOT_UNIQUE`, sondeado). Cambio V5b:

```text
BuyerValue(canonical, kind: BuyerKind{UNIT, GROUP}, members: tuple[str, ...],
           membership: str | None, excluded: bool)
  UNIT:  members == (canonical,), membership is None
  GROUP: len(members) ≥ 2, ordenados y distintos, canonical ∉ members,
         membership = "<group_id>@<versión>"  (membresía declarada, ADR-074 §4)
```

Los nombres de fuente por dataset siguen siendo el binding de ADR-073 (`project_buyer_names`), aplicado
miembro a miembro. Obligación de E-BUYER, no del contrato: proyectar **todos** los miembros o BLOCK.
Nunca el `LIKE` sobre el primero (ADR-074 §6); el grupo no toca `result_identity` (§5).

**ACTOR.** `ActorValue(document, document_kind, display)` sirve para M1 (P9). Le falta `excluded`
(V5a; ACTOR_NEGATION_COMPOSITION existe en legacy y CUTOVER exige `L-only-OK = 0`). Riesgo declarado:
`display` de una persona natural viaja en `resolved_ids` hacia la traza. El núcleo debe aplicar la
redacción de ADR-060 en el ledger y la traza antes de E-ACTOR. Rol de actor y miembros de una UT:
fuera de M1. Si el METIS de D-ACTOR los necesita, entran como `actor@2` con decodificación por
etiqueta (aditiva por construcción).

**STATE / MODALITY.**

- *STATE, forma de los literales.* `by_dataset: {dataset: literals}` pierde la **columna**. El embudo
  de procesos filtra `estado_del_procedimiento IN (…)` **y** `estado_de_apertura_del_proceso =
  'Abierto'`, dos columnas (`foundation_projection.py`, `estado_families.oferta_abierta`). Cambio V5d:
  `literals: tuple[(dataset, column, values), ...]`, ordenado por (dataset, column). Semántica: AND
  entre columnas del mismo dataset, OR dentro de `values`.
- *STATE, negación.* «no terminados» se ejecuta hoy con `celebrated-state-complement`. Sin un campo
  `excluded`, la autoridad sólo podría mentir en `canonical_class` o decir `NEGATION_UNSUPPORTED`, y
  esto último es una regresión. Cambio V5a.
- *STATE, clases.* `estado_families` tiene `contrato_suspendido` y `contrato_cedido`, que no caben en
  las 11 clases. Cambio V5d: se añaden `suspended` y `ceded`.
- *MODALITY.* Correcto tal cual; lo que no podía hacer era depender del dataset, y eso lo resuelve V2
  (`datasets` no vacío).

**CARDINALITY (P3 enmendada).** `n ∈ 1..100` cubre el singular (= 1). Falta registrar de dónde sale el
valor: «el contrato de mayor valor» y «el 1 de mayor valor» se anuncian distinto, y P8 sólo anuncia el
tope para cifras. Cambio V5e: `basis ∈ {figure, singular}`, con la regla `singular ⇒ n == 1`.

Obligación de **C1.2/C1.6** (no de C1.1): la tabla de operadores debe admitir que la evidencia de
CARDINALITY sea el sintagma singular («el contrato», «el de») y no sólo una cifra, aunque solape la
evidencia de universo de la petición. Si no, el contexto tendría que *crear* el 1 desde la cita de
ORDER, lo que viola INV-C0-7.

**ORDER / TOPIC / CONTRACT_TYPE.** Encajan tal cual. A TOPIC y CONTRACT_TYPE les falta `excluded`
(V5a). ORDER no admite negación (`NEGATION_UNSUPPORTED:ORDER`, ya en el conjunto).

**Regla de exclusión (V5a), en una línea.** Todo valor cuya proyección es pertenencia o intervalo
(TOPIC, BUYER, ACTOR, GEO, TIME, STATE, CONTRACT_TYPE; MODALITY ya lo tiene) lleva `excluded: bool`.
VALUE absorbe la negación en el comparador; ORDER y CARDINALITY no la admiten. Un RESOLVED con
`excluded=True` exige una política que declare el trato de nulos y de «No Definido» (ADR-064).

## 4. Invariantes

Se preservan: INV-C0-7/8/9, P-6 (G1: toda cita se lee), P-7 (sin texto libre; ver D-GEO), invariante 6
(ninguna autoridad ve el texto crudo; la ventana sigue acotada).

Se introducen:

- **INV-C0-7b.** El contenido de un hueco sólo retira autoridad o la condiciona, nunca cambia el valor.
- **INV-C1.1-EXCL.** En los tipos que tienen `excluded`, todo RESOLVED cumple `excluded == (polarity is
  NEGATIVE)`.
- **INV-C1.1-DS.** Las claves de dataset de `StateValue.literals` están contenidas en `ctx.datasets`.
- **INV-C1.1-TZ.** La fecha local de toda autoridad es `ctx.local_date()`.

## 5. Lista cerrada de cambios

Todos en el worktree `secoppal-m1-c1`. Los valores conservan la etiqueta `@1`: todavía no se ha
persistido nada.

| Id | Archivo | Cambio exacto |
|---|---|---|
| **W1** | `app/core/foundation_authority_contract.py` (`EvidenceWindow`) | Campos `spans, segments, gaps, left, right`; `quote` pasa a ser propiedad `str \| None` (§2.5); constante `MAX_EVIDENCE_SEGMENTS = 4`; validación: `1 ≤ len(segments) ≤ 4`, segmentos NFC no vacíos, `len(gaps) == len(segments) - 1`, cada hueco ≤ `2*WINDOW_TOKENS` tokens `[a-z0-9]+`; `truncated/stripped` conservan `segments` y `gaps`. |
| **W2** | `app/core/foundation_evidence_window.py` | `build_window(raw, spans)` sin el parámetro `dimension` y sin `material_phrase`. Agrupa en segmentos las citas solapadas o separadas sólo por espacios; los huecos se calculan con `window_tokens`. Códigos cerrados: `INVALID_RAW_TEXT`, `NO_MATERIAL_EVIDENCE`, `UNBOUND_RAW_SPAN`, `SPAN_INSIDE_WORD` (un borde de cita con un carácter alfanumérico a cada lado), `EVIDENCE_GAP_TOO_WIDE`, `TOO_MANY_EVIDENCE_SEGMENTS`. Se elimina `DISJOINT_EVIDENCE`. |
| **V2** | contrato (`AuthorityContext`) | `datasets` **no vacío**: con UNSPECIFIED el núcleo pasa el conjunto completo de datasets candidatos. Método `local_date() -> date` = `reference_instant.astimezone(REFERENCE_TZ).date()`, con `REFERENCE_TZ = timezone(timedelta(hours=-5))` (Colombia no tiene horario de verano). |
| **V3** | contrato (`AuthorityVerdict`) | Campo `detail: str \| None = None`: sólo en veredictos no RESOLVED, con forma `[A-Z][A-Z0-9_]{0,63}`. Es diagnóstico y permite un mensaje más fino en una tabla del núcleo; nunca cambia la decisión. |
| **R1** | contrato (`AUTHORITY_REASONS`) | Se añade `GENERIC_IDENTITY:GEOGRAPHY`. |
| **V5a** | `app/core/foundation_authority_values.py` | `excluded: bool` (último campo, codificado `0/1`) en `TopicValue, BuyerValue, ActorValue, GeoValue, TimeValue, StateValue, ContractTypeValue`. |
| **V5b** | valores (`BuyerValue`) | Se sustituye `owners` por `kind: BuyerKind{UNIT, GROUP}`, `members`, `membership`, con las reglas de §3 BUYER. |
| **V5c** | valores (`GeoValue`) | `department: str \| None`: obligatorio con MUNICIPALITY, `None` con DEPARTMENT y REGION. `members` sigue siendo exclusivo de REGION. |
| **V5d** | valores (`StateValue`, `STATE_CLASSES`) | Se sustituye `by_dataset` por `literals: tuple[tuple[str, str, tuple[str, ...]], ...]`, que son triples `(dataset, column, values)` ordenados y únicos por `(dataset, column)`. `of(class, {dataset: {column: values}})`; `literals_for(dataset) -> tuple[(column, values)] \| None`. Se añaden `suspended` y `ceded` a `STATE_CLASSES`. |
| **V5e** | valores (`CardinalityValue`) | `basis: CardinalityBasis{FIGURE, SINGULAR}`, con `SINGULAR ⇒ n == 1`. |
| **K1** | `tests/foundation_authority_kit.py` | Nivel: si la evidencia es `UNKNOWN`, se acepta cualquier nivel concreto; si no, deben ser iguales. |
| **K2** | kit | Los alias `VALUE_*` sólo valen con la dimensión VALUE. |
| **K3** | kit | INV-C0-7b: en cada caso RESOLVED con huecos, sustituir cada hueco por `()` y por `("x",)` da el mismo RESOLVED(v) o un no RESOLVED. |
| **K4** | kit | INV-C1.1-EXCL e INV-C1.1-DS. `default_context()` con `datasets` no vacío. |
| **K5** | `tests/test_foundation_authority_*.py` | Mutantes que deben morir: hueco ocultado (`gaps=()`); hueco sin tope; `quote` que une segmentos distintos; repetición rechazada; S3 rechazado; cita dentro de palabra aceptada; `excluded` ignorado; grupo con un solo miembro; municipio sin `department`; literal STATE sin columna; singular con `n=2`; `detail` en un RESOLVED; `local_date` en UTC; `datasets` vacío aceptado; alias VALUE en TIME. |
| **D0** | `docs/audits/m1_c1/C1_1_CANDIDATE.md` | Registrar esta enmienda. El coordinador sincroniza `C0_SPINE_V5_DESIGN.md` §5.5.1/§5.5.2, que no es del implementador. |

Gates del ciclo de cambios: ruff sobre los archivos tocados; los dos archivos de test; `pytest
tests/test_foundation_*.py` (paridad: siguen 0 imports desde runtime); `olympus-semgrep-delta --base
4ec99bc1`; mutantes K5 todos muertos; sonda §2.5 (S3 y repetición devuelven ventana, «sobras» devuelve
`SPAN_INSIDE_WORD`). Después, congelar el SHA y avisar a D-TIME, D-MONEY y D-GEO de que rebasen.

## 6. Non-goals

- Opciones de aclaración tipadas (`alternatives`); se pueden añadir después sin romper a nadie.
- Ancla temporal (publicación, firma o cierre), en `TimeValue` y en `OrderValue`.
- Rol de ACTOR y miembros de UT.
- Reglas entre condiciones: `department_hint`, `CONTRADICTORY_STATE`, la anulación de oportunidad.
  Siguen en el núcleo.
- Validar el operador contra la tabla (C1.2).

## 7. Condiciones de STOP

- Algún carril necesita un campo de valor o una razón que no esté en §5 → METIS antes de su paso E.
- Aplicar W1/W2 rompe un test `tests/test_foundation_*.py` existente → STOP. No debería pasar, porque
  nada del runtime importa estos módulos.
- Un mutante K5 sobrevive dos rondas → declarar la garantía como limitada y cerrar (hardening acotado).

## 8. Riesgos y qué haría cambiar la decisión

- **Huecos de más de 2K tokens frecuentes en el Gold** (citas dispersas legítimas) → revisar el tope
  (una ventana por segmento) antes de E-STATE. Fallan hacia aclarar, no hacia ensanchar.
- **K=4 insuficiente** en los slices de D-* → §12 del diseño. La alternativa es la cláusula entera, y
  sería METIS.
- **Varias autoridades necesitan información entre condiciones** → una etapa de política por petición
  (después de las autoridades, antes de PV), nunca ensanchar `DimensionEvidence`.
- **El METIS de D-ACTOR pide rol o relaciones** → `actor@2`, aditivo por etiqueta.
- **PII en las trazas** (`ActorValue.display`, recibos de D-GEO) → redacción en el núcleo antes de
  E-ACTOR/E-GEO. Si no se puede garantizar, `display` sale del valor y se resuelve en la presentación.

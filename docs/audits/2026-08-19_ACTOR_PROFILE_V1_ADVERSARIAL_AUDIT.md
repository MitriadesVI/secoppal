# ACTOR PROFILE V1 — ADVERSARIAL ACCEPTANCE AUDIT (APV1-AA)

**Veredicto: PARTIAL.** No hay falsas atribuciones materiales ni auto-resoluciones
inseguras, pero cuatro defectos P1 impiden recomendar activación o dogfood.

**Tipo:** auditoría adversarial de aceptación. Su objetivo era intentar demostrar
que Actor Profile **no** es seguro de activar, no confirmar que funciona.
**Producción no se modificó:** `git diff -- app/` vacío al cierre.

---

## 1. Estado verificado

```
BRANCH      feature/actor-profile-v1
HEAD        78943b6225613809a7e353aac7f053ee362c9666
MAIN        2403ebed12eeec243ff195e87496885fc6bdc8bd
```

Los números basales reportados se reverificaron y **coinciden**: 8032 passed,
2 skipped, 24 xfailed; query corpus 89/89; actor corpus 41/41 (con el flag
encendido); dialogue con `GOB-DEPTO-001` como único fallo, idéntico al basal;
`make lint-core` limpio.

---

## 2. Corpus congelado

Congelado **antes** de ejecutar SECOPPAL. Ninguna query se editó, sustituyó ni
retiró después.

```
FROZEN_CORPUS_SHA256   a3db00d39cc8ed54811ac3ebfea2c05c6c1e275711171295e34015b5a8df7518
  frozen_cases.jsonl     9cd4a21d7553b2d27f84654e0ea21f852adec8d5e3c076fd8326237583f35309
  sequences.jsonl        b8a46b0369877e0cba5029b3b0ed150928bc8966aa86468d43265520b40aaa2c
  source_evidence.jsonl  0869ce62d67d8dc7f73474e8421d20f23a227c5d6c782d1c4c2d81880db4aa6e
  metadata.json          4b67a70fa44857b4883007e9547df1b30c19963d20079592218ee30aad0ee0a2
```

1.150 casos · 38 secuencias · 385 grupos de evidencia · 211 documentos reales
únicos. Adquisición: muestra ancla de 50.000 filas `ORDER BY :id` (3 llamadas) +
agregación en lotes `IN` (6 llamadas). Sin descarga masiva.

| Estrato | n | Estrato | n |
|---|---|---|---|
| S1 STABLE_NIT | 120 | S9 MODIFIER_LIKE | 108 |
| S2 STABLE_CC | 96 | S10 BUYER_LIKE | 84 |
| S3 MULTI_ALIAS | 96 | S11 GROUP_LIKE | 72 |
| S5 CROSS_TYPE | 18 | S12 LOW_FREQUENCY | 72 |
| S6 DV_COLLISIONS | 84 | S_CC_ACCIDENTAL_DV | 48 |
| S7 LONG_NAMES | 120 | S8 NAME/DOC CONTRADICTION | 48 |
| S8 PREPOSITIONS | 108 | EXTERNAL_HOLDOUT (+contra) | 66 |
| S15 TOPIC | 10 | | |

---

## 3. Lo que resistió el ataque

Verificado sobre el corpus completo, no por muestreo:

| Gate | Resultado |
|---|---|
| Falsas atribuciones materiales | **0** — el oracle independiente coincidió en 19/20 perfiles |
| Documento de fuente no cubierto por las variantes | **0** de 803 resoluciones |
| Fuzzy elevado a autoridad | **0** (0 fuzzy RESOLVED, 0 CANDIDATES ejecutando) |
| Exact-ambiguous auto-seleccionado | **0** |
| Actor eliminado del SoQL | **0** |
| Topic hijack de temas conocidos | **0/10** |
| Cédula completa visible (datos reales) | **0** — se enmascara (`****8603`) |
| Failsafe del índice | **4/4** modos → bloqueo sin fallback |
| Pérdidas de contexto en follow-up | **0/38** secuencias |
| Purga al cambiar de actor | correcta en 6/6 |
| Proyección tipada NIT vs CC | predicados distintos en 3/3 cross-type |
| Flag por defecto | `False` real en proceso limpio |

El **oracle A** (SoQL propio, sin `SoQLBuilder`) reprodujo exactamente el conteo
del sistema en 19 de 20 identidades. La única divergencia es el defecto P1-03.

El **holdout externo** con marcador de rol se comportó bien: 5/6 → `CANDIDATES`,
1/6 → `UNRESOLVED`, **ninguno atribuyó nada**. El nombre largo se preservó
íntegro (`fundacion hogar de paso mis mejores anos`) y `BARRANQUILLA` dentro del
nombre no se consumió como geografía.

---

## 4. Hallazgos

### AP-P1-01 · El nombre del actor se degrada a tema (y a comprador) cuando coexiste con un documento

`SILENT_SCOPE_DROP` + `TOPIC_HIJACK`. **51/54** casos de contradicción dejaron el
nombre en `objeto`; **8/54** además lo convirtieron en entidad compradora.

```
contratos del proveedor ASILO DE SAN ANTONIO NIT 890.102.896-1
  → objeto: ['asilo']
  → entidad_resolved: E.S.E. HOSPITAL SAN ANTONIO      ← comprador no pedido
  → WHERE: nombre_entidad LIKE '%E.S.E. HOSPITAL SAN ANTONIO%'
           AND objeto LIKE '%asilo%' AND documento_proveedor = '890102896'
  → respuesta: "No encontre resultados exactos con esos filtros.
                La busqueda combinaba: - tema: asilo - entidad: E.S.E. ..."
```

Causa: el paso 10 del router sólo detecta menciones `if "contratista" not in
params and "actor_resolution" not in params`. Con documento presente el nombre
queda huérfano y lo recogen los extractores de objeto y entidad.

Gravedad: **no** hay falsa atribución —el sistema nunca afirma que el documento
sea del actor nombrado—, pero el resultado es **vacío presentado como ausencia de
actividad**, y el desglose de filtros **ni siquiera menciona el NIT** que el
usuario escribió. Es la forma de mentir que AP0 describió: no falla, no avisa, y
tiene forma de respuesta correcta. Viola `INV-NO-TOPIC-FALLBACK`.

Alcance real más allá del adversarial: `contratos del proveedor X NIT Y` es una
forma natural de preguntar, no sólo un ataque.

### AP-P1-02 · Un sufijo geográfico real del nombre se recorta y deja un filtro no solicitado

`CONTEXT_CORRUPTION` + `BOUNDARY_ERROR`. 4/216 menciones fuertes.

```
contratos del proveedor UNIVERSIDAD INDUSTRIAL DE SANTANDER
  → mención detectada : 'universidad industrial'        ← recortada
  → departamento_resolved: Santander                    ← filtro no pedido
  → status: CANDIDATES (fuzzy)
```

**Oracle B** (índice consultado directamente, sin el resolver) confirma que el
alias completo `universidad industrial de santander` **sí está indexado** con NIT
890201213, y que `universidad industrial` **no existe**. Es decir: un actor
presente en el índice deja de resolverse, y la consulta arrastra un
`departamento` que el usuario nunca pidió.

`scrub_strong_actor_leaks` existe para esto, pero recibe la mención **ya
recortada**, que ya no contiene la geografía. El orden de operaciones está
invertido.

Afecta a una clase entera y muy poblada en Colombia: ligas departamentales,
universidades, cámaras de comercio, ESEs.

### AP-P1-03 · Los documentos de 6–7 dígitos no se reconocen como documento

`SILENT_SCOPE_DROP`. Única divergencia del oracle.

```
contratos de la cédula 4265582
  sistema: 50 resultados   (búsqueda temática por la palabra "cedula")
  oracle:   9 contratos    (los de esa persona)
  respuesta: "📋 Contratos de cedula"
```

`_NIT_SHAPE_RE` exige 8–11 dígitos; `actor_identity` acepta 5–11. La cédula cae
al camino de nombre, y de ahí a tema. El dataset tiene **199.329** filas con
documento de 7 dígitos y **7.338** de 6 — ~3,5% del universo, cédulas de
numeración antigua, es decir personas mayores.

Efecto secundario: la clarificación imprime el número completo
(`"...la huella contractual de CEDULA 4265582..."`), porque lo trata como nombre.
No cuenta como fuga de CC del render —el número lo escribió el usuario y el
enmascarado real funciona— pero sí queda persistido en el historial.

### AP-P1-04 · Un nombre desnudo cae a tema cuando el índice no lo cubre

`TOPIC_HIJACK`. **39/120** menciones desnudas. Incluye **6/6** del holdout
externo, que era el caso obligatorio:

```
contratos de TUS ABUELOS BARRANQUILLA S.A.S.
  → objeto: ['abuelos']   ciudad: Barranquilla   ← geografía del nombre consumida
contratos de FUNDACION HOGAR DE PASO MIS MEJORES AÑOS
  → objeto: ['fundacion', 'vivienda', 'paso', 'anos']   ← "vivienda" es inventado
```

La expansión morfológica añade términos que el usuario nunca escribió. Con
marcador de rol el mismo actor se comporta bien; sin él, la política declarada de
AP3-C ("a miss preserves the exact baseline thematic frame") produce una búsqueda
temática cuyo resultado se presenta como respuesta, sin declarar que se
interpretó X como tema y no como actor.

Es comportamiento **baseline**, no una regresión de AP3 — pero es el
comportamiento que un usuario encontrará, y depende por completo de la cobertura
del índice.

### P2

- **Límite de 8 palabras** en `_clean_candidate_name`: 10/20 nombres largos no
  producen mención. El límite histórico subió de 4 a 8; no desapareció. Falso
  negativo seguro (cae a tema, con el efecto de P1-04).
- **Boundary**: `proveedor TUS ABUELOS BARRANQUILLA S.A.S. en Barranquilla`
  absorbe el sufijo → mención `'... s a s en barranquilla'`. Falso negativo seguro.
- **S6 / colisiones base·DV**: 21 pares reales donde ambas formas existen. Las
  variantes alcanzan a la pareja en 21/42 casos, pero la evidencia muestra que en
  todos son **el mismo NIT escrito distinto** (ICONTEC / `1c0nt3c`, UNAD /
  Universidad Nacional Abierta). `SOURCE_DATA_QUALITY`, no falsa atribución. El
  índice los excluye como `UNBRIDGED_DOCUMENT_FORM_COLLAPSE`.
- **Cross-type**: `13848603` → CC devuelve 192, NIT devuelve 1. Predicados
  distintos y correctos.

---

## 5. Índice: bloqueo de activación

**El índice configurado no existe en el repositorio** (`data/actor_alias_index.sqlite`,
en `.gitignore`). Sin él, toda resolución por nombre con marcador fuerte devuelve
`INDEX_UNAVAILABLE` y toda mención desnuda cae a tema.

Para poder auditar AP3 se construyó uno desde la muestra ancla. **No es un
directorio de producción**:

```
BUILD_MODE           SAMPLED (offline JSONL desde muestra ancla de 50.000 filas)
schema_version       1          normalization_version  N2
built_at             2026-08-20T03:32:30+00:00
source_max_date      2026-08-18
identity_count       474        alias_count            491
ambiguous_alias      2          excluded_dirty         45.487
```

De 45.646 grupos, **44.999 se excluyeron por `INSUFFICIENT_OCCURRENCES`**
(umbral 3). La cobertura medida sobre los actores del corpus está sesgada al alza
porque el índice se construyó desde la misma muestra: **sirve para medir la
calidad del resolver, no la cobertura de producción**.

`RESOLUTION QUALITY` y `INDEX COVERAGE` son cosas distintas y aquí sólo la
primera está medida. La segunda es desconocida.

---

## 6. Mutaciones

15 aplicadas, **13 detectadas**, todas restauradas byte a byte (sha256 verificado).

Detectadas: type predicate · fuzzy→RESOLVED · ambiguous por frecuencia · topic
conflict · cortar nombres en preposiciones · actor antes de dataset unsupported ·
colapso NIT/CC · flag por defecto · masking de CC · merge B/B+DV ·
`original_params` tras selección · contradicción nombre/documento.

**AUDIT_GAP · dos mutaciones no detectadas** (no se arregló producción):

- **M8** — renombrar `user_selected_candidate` a `exact_unique_alias` no rompe
  ningún test. La distinción de procedencia que §16 exige existe en el código
  pero **no está defendida**.
- **M10'** — eliminar la rama que bloquea ante `INDEX_UNAVAILABLE` no rompe
  ningún test. El failsafe **funciona** (verificado en vivo en los 4 modos de
  fallo) pero **ningún test lo protege**.

---

## 7. Salida obligatoria

```
ACTOR_PROFILE_V1_ADVERSARIAL_AUDIT: PARTIAL

MODEL_USED:  Claude Opus 5
BRANCH:      feature/actor-profile-v1
BASE_HEAD:   78943b6225613809a7e353aac7f053ee362c9666
FINAL_HEAD:  (ver commit de artefactos)

FROZEN_CORPUS:
  SHA256:               a3db00d39cc8ed54811ac3ebfea2c05c6c1e275711171295e34015b5a8df7518
  SOURCE_CASES:         1150
  UNIQUE_REAL_ACTORS:   211
  TOTAL_QUERY_VARIANTS: 1150
  FOLLOWUP_SEQUENCES:   38

STRATA: STABLE_NIT 120 · STABLE_CC 96 · CC_ACCIDENTAL_DV 48 · MULTI_ALIAS 96 ·
  EXACT_AMBIGUOUS 2 observados · CROSS_TYPE 18 · RAW_DV_COLLISIONS 84 ·
  LONG_NAMES 120 · PREPOSITION_NAMES 108 · MODIFIER_LIKE 108 ·
  BUYER_LIKE 84 · GROUP_LIKE 72 · LOW_FREQUENCY 72 · EXTERNAL_HOLDOUT 66

INDEPENDENT_ORACLE:
  DOCUMENT_CASES: 20   COUNT_MATCHES: 19   DIVERGENCES: 1 (AP-P1-03)

IDENTITY:
  FALSE_ATTRIBUTIONS: 0     NIT_CC_COLLAPSES: 0     DV_COLLAPSES: 0
  NAME_DOCUMENT_CONFLICTS: 54 (ninguno atribuye; 51 degradan el nombre a tema)
  SOURCE_CONTAMINATION_CASES: 21 pares base/DV + 5 cross-type, todos declarados

ROUTING:
  TOPIC_HIJACKS: 39 bare + 51 contradicción     ACTOR_DROPS: 0
  BARE_ACTOR_ERRORS: 39      BUYER_CONTRACTOR_CONFLICTS: 8
  BOUNDARY_ERRORS: 4 recortes + 10 no detectados por límite de 8 palabras

RESOLUTION:
  EXACT_UNIQUE: 423   EXACT_UNIQUE_WRONG: 0   EXACT_AMBIGUOUS: 2
  AMBIGUOUS_AUTO_SELECTED: 0   FUZZY_CASES: 175   FUZZY_AUTO_RESOLVED: 0
  UNRESOLVED_SAFE: 16

FOLLOWUP: SEQUENCES_PASS 38/38 · CONTEXT_LOSSES 0 · ACTOR_REPLACEMENT_ERRORS 0
DATASET_CAPABILITY: UNSUPPORTED_CASES 54 · SILENT_EXECUTIONS 0
FAILSAFE: INDEX_MISSING/CORRUPT/INCOMPATIBLE/EMPTY todos seguros · UNSAFE_FALLBACKS 0
PRIVACY: CC_VISIBLE_LEAKS 0 con datos reales
FLAG: DEFAULT_OFF sí · OFF_BASELINE preservado · ON_PATH ejercitado

INDEX: BUILD_MODE SAMPLED · COMPLETE no · IDENTITIES 474 · ALIASES 491
       REAL_SAMPLE_EXACT_COVERAGE sesgada al alza (índice derivado de la muestra)

MUTATIONS: TOTAL 15 · DETECTED 13 · UNDETECTED 2 (M8, M10')
SEVERITY:  P0 0 · P1 4 · P2 4 · INFO varios

FULL_SUITE 8032 passed/2 skipped/24 xfailed · QUERY_CORPUS 89/89 ·
ACTOR_CORPUS 41/41 · DIALOGUE 60/61 (GOB-DEPTO-001 idéntico) · LINT limpio
PRODUCTION_CODE_CHANGED: NO

MERGE_READY:                 YES
DOGFOOD_READY:               NO
PRODUCTION_ACTIVATION_READY: NO
UT_READY:                    NO

MERGE: NO   REBASE: NO   PUSH: NO
```

**Por qué MERGE_READY sí y DOGFOOD_READY no.** El código no rompe nada: suite
verde, corpus intactos, flag apagado por defecto, `app/` sin tocar. Fusionar es
seguro. Encender no lo es: cuatro P1 producen respuestas vacías o temáticas que un
usuario leería como "este actor no tiene contratos", y el índice de producción no
existe.

**BLOCKERS de activación:** AP-P1-01 · AP-P1-02 · AP-P1-03 · AP-P1-04 · índice
ausente/muestreado.

**AUDIT_GAPS:** M8 y M10' sin cobertura de test.

**INCIDENTAL_FINDINGS:** el umbral `minimum_total_occurrences = 3` descarta el
94% de los grupos de una muestra ancla — la cobertura del índice depende
críticamente del volumen de la fuente de construcción, no sólo de su alcance.

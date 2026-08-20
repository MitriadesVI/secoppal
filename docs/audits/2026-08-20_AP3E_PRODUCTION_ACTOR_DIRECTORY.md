# AP3-E — PRODUCTION ACTOR DIRECTORY & COVERAGE AUDIT

**Veredicto: PARTIAL.** El directorio de producción existe, está completo y
conciliado con la fuente, y su cobertura es del 98,84%. Pero esa densidad
destapó **tres falsas atribuciones materiales** que el índice muestreado no
podía revelar. No se arregló nada: §18 exige parar antes de tocar `app/core`.

```
BASE_HEAD   2614d02e846a44b98d7fdfd3108f92d76ceea4cc
FROZEN_CORPUS_SHA256 verificado: a3db00d3…a8df7518   (íntegro)
FLAG_DEFAULT_OFF: sí (Settings() real = False)
```

---

## 1. El builder no podía producir un directorio (§3)

`BUILD_MODE` previo = **B, muestra acotada**. La política se autodescribía
`BOUNDED_ANCHOR_THEN_TARGETED_SERVER_AGGREGATION` con `global_group_scan:
False`. Faltaban cinco capacidades: no había traversal (una sola llamada por
tipo, sin paginación), los topes duros lo impedían por construcción
(`_MAX_ANCHOR_PER_TYPE = 100.000` filas sobre 5,9 M; `_MAX_SELECTED_PER_TYPE =
10.000` identidades sobre 1,23 M), la selección estaba **ordenada por
frecuencia** —sesgo estructural hacia proveedores grandes—, sólo procesaba dos
tipos documentales, y no había ni checkpoint ni high-watermark.

Se reportó antes de construir, como exige §3, y se autorizó extender
`app/data/`. **`app/core` no se tocó.**

## 2. El directorio construido

```
BUILD_MODE            FULL_SERVER_SIDE_GROUP_TRAVERSAL
COMPLETE              sí — conciliado aritméticamente
SOURCE_DATASET        jbjy-vk9h        SOURCE_CUTOFF   2026-08-18
PAGES                 25 × 50.000      DUPLICATE_GROUP_KEYS   0
OBSERVED_CONTRACT_ROWS 5.784.468  ==  EXPECTED_CONTRACT_ROWS 5.784.468
BUILD_DURATION        1.175 s (19,6 min)   PEAK_MEMORY   3,02 GB
INDEX_SIZE            662 MB
```

**Completitud demostrada, no supuesta.** Contar duplicados no detecta huecos:
un grupo omitido no aparece en ninguna página. La verificación es aritmética —
la suma de `occurrences` de todos los grupos debe igualar el `count(*)` que la
fuente reporta bajo el mismo predicado. Cuadró exactamente.

### Cobertura

```
SOURCE_ROWS_CONSIDERED       1.235.591 grupos (tipo, documento, nombre)
ELIGIBLE_TYPED_IDENTITIES    1.228.645 documentos distintos en la fuente
INDEXED_IDENTITIES           1.214.351     (CC 1.105.444 · NIT 108.907)
IDENTITY_COVERAGE            98,84%
CONTRACT_ACTIVITY_COVERAGE   98,13%   (5.676.542 / 5.784.468 filas)
ALIASES                      1.217.203
EXACT_AMBIGUOUS_ALIASES      14.305   (1,18%)
DIRTY_EXCLUDED               15.936   (1,30%)
```

Las exclusiones son **todas** por calidad de evidencia, ninguna por frecuencia:

| razón | n |
|---|---|
| CROSS_TYPE_COLLISION | 4.968 |
| UNBRIDGED_DOCUMENT_FORM_COLLAPSE | 1.964 |
| SECONDARY_GROUP_ALIAS | 415 |
| LOW_DOMINANT_NAME_SHARE | 60 |
| EXTREME_ALIAS_CARDINALITY | 4 |
| **INSUFFICIENT_OCCURRENCES** | **0** |

## 3. Umbral de ocurrencias (§8)

Medido sobre 148.313 documentos / 869.309 filas antes de decidir:

| umbral | identidades | actividad |
|---|---|---|
| ≥1 | 100% | 100% |
| ≥2 | 72,1% | 95,2% |
| ≥3 (previo) | **56,1%** | 89,8% |

`RECOMMENDED_THRESHOLD = 1`, aplicado. El umbral 3 excluía al **43,9% de los
proveedores del país** mientras mantenía el 89,8% de la actividad — una cifra
que camuflaba el sesgo: el índice parecía sano y era un censo de proveedores
grandes.

Bajar a 1 significa exclusivamente que **la baja frecuencia no constituye
suciedad**. No convierte una observación en autoridad: una identidad con un
solo contrato y cualquier problema de evidencia sigue siendo `EXCLUDED_DIRTY`,
y hay test que lo congela (colisión cross-type con una observación por lado).

## 4. Holdouts

### Externo (§9) — fuente independiente, sin añadir aliases

| actor | estado | resuelto |
|---|---|---|
| ASILO DE SAN ANTONIO | **EXACT_RESOLVED** | 890102897 ✓ |
| ASILO HOGAR GRANJA SAN JOSE | **EXACT_RESOLVED** | 890102896 ✓ |
| FUNDACIÓN ANA DELIA | **EXACT_RESOLVED** | 901717025 ✓ |
| TUS ABUELOS BARRANQUILLA S.A.S. | CANDIDATES_SAFE | — |
| FUNDACION HOGAR DE SAN CAMILO | CANDIDATES_SAFE | — |
| FUNDACION HOGAR DE PASO MIS MEJORES AÑOS | CANDIDATES_SAFE | — |

**WRONG_IDENTITY = 0.** Los tres no resueltos **sí están en el directorio**
por NIT, con 11 contratos cada uno; lo que no coincide es la escritura:

```
fuente externa                        SECOP escribe
TUS ABUELOS BARRANQUILLA S.A.S.   →   TUS ABUELOS BARRANQUILLA SAS
FUNDACION HOGAR DE SAN CAMILO     →   fundación hogar san camilo   (sin "de")
FUNDACION HOGAR DE PASO MIS…      →   FHPM                          (sigla)
```

Dos de los tres aparecen como **candidato en rango 1** — un clic del usuario.
El tercero es una sigla, fuera del alcance de cualquier normalización.

`S.A.S.` vs `SAS` es sistemático y merece atención: N2 convierte la puntuación
en espacios, así que `s a s` y `sas` son aliases distintos. Es la forma
societaria más común de Colombia. **No se tocó** — cambiar N2 exigiría
reconstruir el directorio y §2 prohíbe reabrir decisiones de AP3-A.

### Aleatorio (§10) — 120 actores, semilla propia (20260820), muestreados de la
fuente en cinco tramos dispersos, sin que el índice participe en la selección

```
EXACT_RESOLVED   115  (95,8%)
AMBIGUOUS_SAFE     5  ( 4,2%)
WRONG_IDENTITY     0
```

## 5. Lo que la densidad destapó — P0

Tres casos de 1.150 (0,26%) en la re-auditoría adversarial donde una mención
**recortada** resuelve con `exact_unique_alias` —autoridad plena, sin pedir
aclaración— a una identidad **distinta**:

```
contratos del proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ en 2025
  mención detectada : 'e s'            ← tres caracteres
  índice            : 'e s' → E&S, NIT 900047333
  esperado          : NIT 890680027 (el hospital)
  resultado         : RESOLVED, exact_unique_alias, sin clarificación

contratos del proveedor ALCALDIA DE PUERTO SALGAR CUNDINAMARCA
  mención detectada : 'alcaldia de puerto salgar'   ← "CUNDINAMARCA" recortada
  índice            : → CC 1127389939 (una persona natural)
  esperado          : NIT 8999994138
```

Sin la variante `en 2025`, la misma consulta detecta el nombre completo y no
resuelve. El recorte lo dispara la cola de modificadores reconocidos.

**Por qué esto es nuevo y no una regresión del build.** El defecto de recorte
ya se conocía (AP-P1-02 de la auditoría anterior) y era un falso negativo
benigno: el fragmento no existía en un índice de 474 identidades. Con 1,2 M de
aliases, un fragmento de tres caracteres tiene alta probabilidad de coincidir
con algún actor real. **Aumentar la cobertura convirtió un falso negativo en
una falsa atribución.** Esa es la conclusión central de AP3-E.

## 6. Otros hallazgos

- **P1 · topónimos que son nombres de persona.** `Bogota` es alias exacto de
  dos cédulas de personas así llamadas, así que `contratos de Bogota` pasa de
  búsqueda territorial a `AMBIGUOUS`. Es seguro —pide aclaración— pero degrada
  una consulta frecuente y legítima. `has_strong_topic_evidence` cubre temas
  conocidos, no geografía.
- **P1 · latencia de recuperación difusa.** `trigram p95` pasó de **0,47 ms a
  287 ms** con el directorio completo (~600× sobre la referencia de AP3-B).
  Consume ~2% del presupuesto de 15 s del turno: material, no catastrófico.
- **P2 · memoria no acotada.** 3,02 GB de pico; §4 pide *bounded-memory*. El
  recorrido acumula 1,2 M de grupos en RAM antes de escribir.
- **P2 · fragilidad de red.** El primer build murió en la página 25 —tras 1,2 M
  de grupos— por un `ReadTimeout`. Se añadió reintento por página con backoff
  (justificado por medición, como pide §13) y timeout de adquisición de 180 s.
  **La atomicidad se verificó empíricamente**: el índice válido previo
  sobrevivió intacto y no quedaron temporales huérfanos.
- **P2 · dos tests dependen del índice presente en `data/`.**
  `test_an_unresolved_name_asks_for_the_document_instead_of_profiling` asumía
  que `FUNDACARIBE` nunca resolvería —existe: NIT 900429803— y
  `test_ecosystem_bogota_soql` asumía que `contratos de Bogota` emite SoQL. Son
  tests obsoletos, no regresiones de código; la suite cambia de resultado según
  qué índice haya en disco, y eso es higiene a corregir.

## 7. Salida

```
AP3E_PRODUCTION_ACTOR_DIRECTORY: PARTIAL

SOURCE_DATASET jbjy-vk9h · BUILD_MODE FULL_SERVER_SIDE_GROUP_TRAVERSAL
COMPLETE sí · SOURCE_CUTOFF 2026-08-18

SOURCE_ROWS_CONSIDERED      1.235.591 grupos / 5.784.468 filas
ELIGIBLE_TYPED_IDENTITIES   1.228.645
INDEXED_IDENTITIES          1.214.351
IDENTITY_COVERAGE           98,84%
CONTRACT_ACTIVITY_COVERAGE  98,13%
ALIASES 1.217.203 · EXACT_AMBIGUOUS 14.305 · DIRTY_EXCLUDED 15.936

OCCURRENCE_THRESHOLD_ANALYSIS  GE1 100%/100% · GE2 72,1%/95,2% · GE3 56,1%/89,8%
RECOMMENDED_THRESHOLD          1  (aplicado)

COVERAGE_NIT 108.907 · COVERAGE_CC 1.105.444
LOW_FREQUENCY_COVERAGE  completa (INSUFFICIENT_OCCURRENCES = 0)
GROUP_LIKE_COVERAGE     alias de grupo secundarios excluidos (415)
LONG_NAME_COVERAGE      sin límite en el índice; el límite vive en el parser

EXTERNAL_HOLDOUT   EXACT 3 · CANDIDATES 3 · WRONG_IDENTITY 0
RANDOM_HOLDOUT     SIZE 120 · EXACT 115 · AMBIGUOUS 5 · WRONG_IDENTITY 0

INDEX_SIZE 662 MB · BUILD_DURATION 1.175 s · PEAK_MEMORY 3,02 GB
RESUMABLE no (reintento por página, sin checkpoint) · IDEMPOTENT sí · ATOMIC sí (verificado)
INCREMENTAL_READY PARTIAL — hay cutoff por fecha de firma, pero la fuente no
  expone un high-watermark de actualización por fila; un rebuild completo de
  20 min es aceptable para V1.

EXACT_LATENCY   p50 0,10 ms · p95 0,12 ms
TRIGRAM_LATENCY p50 112 ms  · p95 287 ms      ← regresión material

RUNTIME_SMOKE  WRONG_IDENTITIES 3 · TOPIC_HIJACKS 0 · UNSAFE_FALLBACKS 0
  (sentinels 4/4: colisión tipada 13848603 NIT vs CC, CC corta 4265582,
   Universidad Industrial de Santander — los P1 previos siguen corregidos)

FLAG_DEFAULT_OFF sí
FULL_SUITE 8071 passed / 2 failed / 2 skipped / 24 xfailed
QUERY_CORPUS 89/89 · ACTOR_CORPUS 41/41 · DIALOGUE 60/61 (GOB-DEPTO-001 idéntico)
LINT limpio

PRODUCTION_DB_COMMITTED  NO
RUNTIME_CODE_CHANGED     NO (solo app/data/ builder + tests)

DOGFOOD_READY                NO
PRODUCTION_ACTIVATION_READY  NO
MERGE_READY                  YES (builder e índice; el runtime necesita el fix del recorte)
UT_READY                     NO
```

**BLOCKERS:** las tres falsas atribuciones por mención recortada. El fix vive en
`app/core/actor_intent.py` (frontera de mención) y §18 obliga a reportar antes
de tocarlo. Cualquier cambio ahí exige re-auditoría adversarial completa.

**NON_BLOCKING:** topónimo-como-persona · latencia trigram · memoria del build ·
`S.A.S.`/`SAS` en N2 · dos tests acoplados al índice de disco.

**INCIDENTAL:** el directorio ocupa 662 MB y no se versiona (`.gitignore`), así
que cualquier entorno sin build previo corre con resolución de nombres
inoperante — y ese es precisamente el estado en que se comporta de forma segura.

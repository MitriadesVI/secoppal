# CONTROL-NATIONAL-R2E — cobertura verificada del control nacional

**Tipo:** discovery + remediación · **Fecha:** 2026-09-01

**Rama:** `main` · **HEAD antes:** `e663a2c794d9d93535e5ab0c9c8f12258ac66b4f`

**Evidencia:**
[`control_national_r2e_candidates.json`](../../artifacts/audits/control_national_r2e_candidates.json)

**Reproductor:**
[`audit_control_national_r2e.py`](../../scripts/audits/audit_control_national_r2e.py)

**Autoridad arquitectónica:** ADR-072 — ownership exacto y elegibilidad de scan
son autoridades distintas. R2E no crea ADR nuevo.

## 1. Veredicto

```text
CONTROL_NATIONAL_R2E: PASS

UNIVERSE            6   (4 CONTROL_ROOTS + 2 RELATED_BUYERS)
MATRIX_BEFORE       PASS_EXACT 2 · WRONG_OWNER 4
MATRIX_AFTER        PASS_EXACT 6 · WRONG_OWNER 0 · FUZZY_ONLY 0 · AMBIGUOUS 0

NEW_SCAN_CROSS_OWNER      90 → 0
EXACT_MULTI_OWNER        173 → 172   (NEW_MULTI_OWNER_KEYS = 0)
TERRITORIAL_NON_HIJACK    7/8 secuestrados → 0/8
PARENT_RELATED_CANARIES   1/4 → 4/4
```

Los seis forman un universo operativo, no seis entidades constitucionalmente
equivalentes. El Fondo de Bienestar Social y el IEMP conservan NIT, nombre
físico y registro propios: no se fusionan con la CGR ni con la Procuraduría.

## 2. El baseline R2C se reprodujo, no se asumió

R2C se midió antes de D6/R2D.1, de R2D.3 y de ADR-072. La matriz se volvió a
correr contra el gazetteer congelado en `e663a2c`:

```bash
git show e663a2c:app/data/aliases_db.json > /tmp/aliases_pre.json
python scripts/audits/audit_control_national_r2e.py \
    --label PRE --gazetteer /tmp/aliases_pre.json --output /tmp/r2e_pre.json
```

| Entidad | R2C | HEAD `e663a2c` | Owner devuelto | Método |
|---|---|---|---|---|
| Contraloría General de la República | `PASS_EXACT` | `PASS_EXACT` | CGR-NIVEL CENTRAL | `exact` |
| Auditoría General de la República | `WRONG_OWNER` | `WRONG_OWNER` | BANCO DE LA REPUBLICA | `fuzzy(90)` |
| Procuraduría General de la Nación | `WRONG_OWNER` | `WRONG_OWNER` | (Secretaría Distrital de Integración Social) | `fuzzy(85)` |
| Defensoría del Pueblo | `PASS_EXACT` | `PASS_EXACT` | DEFENSORÍA DEL PUEBLO | `exact` |
| Fondo de Bienestar Social de la CGR | `WRONG_OWNER` | `WRONG_OWNER` | BANCO DE LA REPUBLICA | `fuzzy(90)` |
| Instituto de Estudios del Ministerio Público | `WRONG_OWNER` | `WRONG_OWNER` | SUBRED INTEGRADA … NORTE E.S.E. | `fuzzy(85)` |

```text
DRIFT_FROM_R2C: NONE
```

El smoke PRE mide el costo real del fail-open: la consulta natural
«Procuraduría General de la Nación» devolvía el `WHERE` de la Secretaría
Distrital de Integración Social —112.067 procesos y 96.080 contratos ajenos— y
«Auditoría General de la República» los 30.651 procesos del Banco de la
República.

## 3. Identidad SECOP II revalidada

Autoridad: procesos [`p6dx-8zbt`](https://www.datos.gov.co/resource/p6dx-8zbt.json)
y contratos [`jbjy-vk9h`](https://www.datos.gov.co/resource/jbjy-vk9h.json),
consultados **por NIT**, no por parecido textual. Ventana reciente
`2025-09-01 → 2026-09-01` (`fecha_de_publicacion_del` / `fecha_de_firma`).

| Código | NIT base | Nombre físico | Forma | Recientes P/C |
|---|---|---|---|---:|
| CGR | 899999067 | `CGR-NIVEL CENTRAL` | `MULTIPLE_PHYSICAL_SAME_IDENTITY` | 360 / 326 |
| AGR | 830065741 | `AUDITORIA GENERAL DE LA REPUBLICA` | `SINGLE_PHYSICAL_NAME` | 198 / 147 |
| PGN | 899999119 | `PROCURADURIA GENERAL DE LA NACION` | `SINGLE_PHYSICAL_NAME` | 534 / 364 |
| DP | 800186061 | `DEFENSORÍA DEL PUEBLO` | `SINGLE_PHYSICAL_NAME` | 2.098 / 9.609 |
| FBS | 800252683 | `FONDO DE BIENESTAR SOCIAL DE LA CONTRALORIA GENERAL DE LA REPUBLICA` | `SINGLE_PHYSICAL_NAME` | 113 / 97 |
| IEMP | 830015728 | `INSTITUTO DE ESTUDIOS DEL MINISTERIO PUBLICO.` | `SINGLE_PHYSICAL_NAME` | 22 / 20 |

Dos correcciones sobre la fotografía de R2C:

- **El IEMP publica su NIT con dígito de verificación.** `nit_entidad` vale
  `8300157281`; la identidad es `830015728-1`. La consulta por `830015728` a
  secas devuelve cero filas. El gazetteer guarda la base y el auditor prueba
  ambas variantes.
- **La CGR no tiene un solo nombre físico.** El NIT 899999067 publica **33**
  nombres: el nivel central, la Unidad de Seguimiento y Auditoría de Regalías y
  30 gerencias departamentales. R2C la vio `SINGLE_PHYSICAL_NAME` porque su
  sonda filtraba candidatos por puntaje de parecido; consultando por NIT la
  fragmentación aparece completa. Ver deuda §9.

## 4. Discovery y decisiones

| Entidad | Clasificación | Decisión | Aliases | Política de scan |
|---|---|---|---|---|
| CGR | `IDENTITY_PRESENT` + `OVER_GENERIC_ALIAS` + `MULTIPLE_PHYSICAL_SAME_IDENTITY` | `KEEP` + `KEEP_EXACT_DISABLE_SCAN` | 23 existentes, ninguno añadido | 4 formas cortas pierden scan |
| AGR | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` | 5 nuevos | todos elegibles |
| PGN | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` | 7 nuevos | todos elegibles |
| DP | `IDENTITY_PRESENT` + `PARENT_CHILD_COLLISION` | `KEEP_EXACT_DISABLE_SCAN` | 20 existentes, ninguno añadido | `defensoría` y `la defensoría` pierden scan |
| FBS | `IDENTITY_MISSING` + `PARENT_CHILD_COLLISION` | `ACCEPT_REGISTRY_VERIFIED` | 4 nuevos | todos elegibles |
| IEMP | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` | 3 nuevos | todos elegibles |

### 4.1 Contraloría General — auditar también el `PASS`

`PASS_EXACT` no significa seguro. Cruzando el catálogo físico de **27.286**
compradores SECOP II por secuencia completa de tokens:

| Alias | Compradores ajenos que lo contienen | Secuestros de scan PRE | Decisión |
|---|---:|---:|---|
| `contraloría` | 105 nombres / 92 identidades | **66** | `KEEP_EXACT_DISABLE_SCAN` |
| `contraloría general` | 28 / 25 | **14** | `KEEP_EXACT_DISABLE_SCAN` |
| `la contraloría` | 10 / 10 | **6** | `KEEP_EXACT_DISABLE_SCAN` |
| `la contraloría general` | 4 / 4 | **2** | `KEEP_EXACT_DISABLE_SCAN` |
| `contraloría general de la república` | 3 / 3 | 1 | `KEEP` |
| `la contraloría general de la república` | 1 / 1 | 1 | `KEEP` |
| `cgr` | 1 / 1 | 0 | `KEEP` |

Colombia tiene contralorías territoriales con NIT propio:
`CONTRALORIA GENERAL DE CALDAS`, `CONTRALORIA GENERAL DE SANTANDER`,
`CONTRALORÍA GENERAL DE MEDELLÍN`, `CONTRALORIA DEPARTAMENTAL DEL HUILA`,
`CONTRALORÍA MUNICIPAL DE PASTO` y decenas más. El fragmento `contraloría`
dentro del nombre de cualquiera de ellas sólo expresa la función de control, no
identifica a la CGR. Escritas solas como frase completa, en cambio,
`contraloría` y `contraloría general` sí nombran razonablemente al órgano
nacional. Por eso conservan ownership exacto y pierden scan: es exactamente la
separación que ADR-072 declaró.

Las dos formas largas conservan scan. `contraloría general de la república`
está contenida en el nombre del Fondo, y ahí el desempate correcto es
longest-match contra el alias específico del Fondo, no la ceguera del scan
(§4.5). Borrarlas habría dejado sin captura la frase canónica de la CGR.

### 4.2 Defensoría del Pueblo — auditar también el `PASS`

| Alias | Compradores ajenos | Secuestros PRE | Decisión |
|---|---:|---:|---|
| `defensoría` | 17 nombres / 14 identidades | 0 activos | `KEEP_EXACT_DISABLE_SCAN` |
| `la defensoría` | 4 / 2 | 0 activos | `KEEP_EXACT_DISABLE_SCAN` + colisión resuelta |
| `defensoría del pueblo` | 11 / 11 | 0 activos | `KEEP` |
| `defensoría pública` | 0 | 0 | `KEEP` |
| `dp` | 0 | 0 | `KEEP` (dos caracteres: nunca entra al scan) |

Los 11 «compradores ajenos» de `defensoría del pueblo` son registros con
**cédula** y el nombre de la propia institución, todos con cero procesos y cero
contratos: duplicados registrales, no dueños ajenos (§6).

Los dueños ajenos reales de `defensoría` sí existen: el
`DEPARTAMENTO ADMINISTRATIVO DE LA DEFENSORIA DEL ESPACIO PUBLICO` (activo, 4.930
procesos) y la `DEFENSORIA CIVIL DE NOBSA`. En PRE, la frase
«contratos de la defensoría civil de Nobsa» resolvía a DEFENSORÍA DEL PUEBLO
por el alias `la defensoria`. Hoy devuelve `None`.

**Colisión `la defensoría`.** Era una de las 173 claves multi-owner aceptadas:
la reclamaban la Defensoría del Pueblo y el DADEP, y por orden de carga ganaba
el DADEP. La consulta natural «la Defensoría» devolvía el departamento del
espacio público. La evidencia es asimétrica: para la Defensoría del Pueblo es
su nombre corto habitual; para el DADEP es un alias generado de más —su registro
conserva `defensoría del espacio público`, `la defensoría del espacio público` y
otras 38 formas específicas—. Se retira del DADEP, queda con dueño único y se
le desactiva el scan. La deuda multi-owner baja a 172; ninguna clave nueva.

### 4.3 Auditoría General de la República

Identidad ausente del gazetteer, no ambigua: un solo nombre físico, un solo NIT,
1.274 procesos y 1.092 contratos históricos. Ninguno de sus aliases candidatos
aparece en otro comprador físico del catálogo:

```text
auditoría general de la república        cross-owner 0
la auditoría general de la república     cross-owner 0
auditoría general                        cross-owner 0
la auditoría general                     cross-owner 0
agr                                      cross-owner 0
```

`auditoría` desnudo **no** se añade: aparece en
`CONTRALORIA GENERAL DE LA REPUBLICA - CGR - UNIDAD DE SEGUIMIENTO Y AUDITORIA
REGALIAS` y nombra una función, no una identidad. `AGR` no colisiona con ningún
comprador ni con ningún alias del gazetteer.

### 4.4 Procuraduría General de la Nación

Un solo nombre físico con actividad. Las procuradurías regionales y provinciales
que aparecen en el directorio (`PROCURADURIA PROVINCIAL DEL VALLE DE ABURRA`,
`PROCURADURIA`) comparten el NIT 899999119 y no publican por separado: son la
misma identidad, no compradores distintos. Las personerías municipales —el
control territorial de esta familia— **no** contienen la palabra «procuraduría»
en su nombre.

Por eso `procuraduría` y `la procuraduría` conservan scan, mientras
`contraloría` y `defensoría` lo pierden. La asimetría es empírica, no estética:
ADR-072 exige evidencia por alias y prohíbe derivar la elegibilidad de la forma.

`Ministerio Público` **no** se añade como alias. Es un concepto institucional que
cubre Procuraduría, Defensoría y personerías, y como secuencia de tokens sólo
aparece en el IEMP y en la `UNIVERSIDAD ESPECIAL DEL MINISTERIO PÚBLICO`
(mismo NIT del IEMP). Sin la puerta, el fuzzy la resolvía a INVÍAS en PRE y al
IEMP en POST —un error más creíble y por eso peor—. Queda declarada en
`resolution_blocked_aliases`: falla cerrada con petición de precisión.

### 4.5 Fondo de Bienestar Social de la CGR

Comprador separado con NIT propio, aunque su nombre contenga íntegramente el de
la Contraloría. El canario obligatorio:

```text
scan_entity("contratos del fondo de bienestar social de la
             contraloria general de la republica")
  PRE  → CGR-NIVEL CENTRAL           [alias 'la contraloria general de la republica']
  POST → FONDO DE BIENESTAR SOCIAL … [alias completo del Fondo]

scan_entity("licitaciones de la contraloria general de la republica")
  PRE  → CGR-NIVEL CENTRAL
  POST → CGR-NIVEL CENTRAL
```

Los aliases del Fondo llevan identidad suficiente —`de la república` o `cgr`—.
Se midieron y se rechazaron los genéricos:

| Frase | Compradores físicos distintos | Decisión |
|---|---:|---|
| `fondo de bienestar social de la contraloría` | 4 fondos territoriales | `RESOLUTION_BLOCK` |
| `fondo de bienestar social` | 9 | `RESOLUTION_BLOCK` |
| `bienestar social` | 12 | no se añade |
| `fondo contraloría` | 0 pero sin evidencia de uso | no se añade |

Los dos bloqueos son necesarios, no cosméticos: con el Fondo ya registrado, el
fuzzy resolvía `fondo de bienestar social` (95) al Fondo de la CGR y le habría
atribuido los fondos de bienestar de las contralorías de Atlántico, Bolívar,
Soledad y La Guajira.

### 4.6 Instituto de Estudios del Ministerio Público

Comprador separado de la Procuraduría, con 205 procesos y 213 contratos.

```text
scan_entity("contratos del instituto de estudios del ministerio publico")
  PRE  → None
  POST → INSTITUTO DE ESTUDIOS DEL MINISTERIO PUBLICO.
```

`IEMP` no colisiona con nada. `instituto de estudios` no se añade: es cabeza
genérica sin identidad. `ministerio público` tampoco, por §4.4.

## 5. Control territorial: la raíz nacional no secuestra

Ocho frases construidas sobre compradores físicos territoriales reales:

| Texto | PRE | POST |
|---|---|---|
| contraloría departamental de Bolívar | CGR (`la contraloria`) | `None` |
| contraloría general de Santander | CGR (`la contraloria general`) | `None` |
| contraloría general de Caldas | CGR (`la contraloria general`) | `None` |
| contraloría general de Medellín | CGR (`la contraloria general`) | `None` |
| contraloría municipal de Ibagué | CGR (`la contraloria`) | `None` |
| defensoría civil de Nobsa | DP (`la defensoria`) | `None` |
| personería municipal de Soledad | `None` | `None` |
| fondo de bienestar social de la contraloría departamental de Bolívar | CGR (`la contraloria`) | `None` |

```text
NATIONAL_ROOT_DOES_NOT_HIJACK_TERRITORIAL_ENTITY: PASS (0/8)
```

La contraloría de Bogotá conserva su identidad propia por su alias específico:
`scan_entity("contratos de la contraloria de bogota")` → `CONTRALORIA DE BOGOTA`,
y `resolve_entidad("Contraloría de Bogotá")` → el mismo owner por `exact`.

R2E no añade cobertura de contralorías territoriales ni de personerías: sólo
demuestra que las raíces nacionales dejaron de morderlas por scan. En el camino
de `resolve_entidad` el fuzzy sigue fallando abierto sobre entidades ausentes
del gazetteer —«Contraloría Departamental de Bolívar» sigue cayendo en la CGR—.
Eso es deuda preexistente, medida antes y después, sin regresión (§9).

## 6. Cómo se cuenta un dueño ajeno

El catálogo trae registros que llevan el nombre de una institución bajo un
documento de persona natural: once `DEFENSORIA DEL PUEBLO` con cédula, dos
`PROCURADURIA GENERAL DE LA NACION`, dos `CONTRALORÍA GENERAL DE LA REPÚBLICA`,
una `CGR`. Todos verificados con cero filas en procesos y cero en contratos.

```text
INACTIVE_REGISTRY_DUPLICATES   PRE 47 → POST 36
```

Un NIT sin actividad transaccional no puede secuestrar una consulta: no aparece
en ninguna respuesta. Por eso la puerta cuenta sólo compradores con actividad y
reporta los duplicados aparte, en vez de esconderlos o de contarlos como fallos.

## 7. Matriz AFTER

```text
INSTITUTIONAL_NAMES (6)      PASS_EXACT 6 · FAIL_CLOSED 0 · WRONG_OWNER 0 · FUZZY_ONLY 0 · AMBIGUOUS 0
HUMAN_FORMS (18)             PASS_EXACT 18   (PRE: 7 PASS · 11 WRONG)
ACRONYMS (5)                 PASS_EXACT 5    (PRE: 2 PASS · 1 WRONG · 2 MISSING)
```

Smoke de dataset con el `WHERE` que emite el builder —
`unaccent(UPPER(entidad)) LIKE unaccent(UPPER('%…%'))`:

| Frase humana | Owner | Procesos | Contratos |
|---|---|---:|---:|
| Contraloría General de la República | CGR-NIVEL CENTRAL | 5.654 | 5.424 |
| Auditoría General de la República | AUDITORIA GENERAL DE LA REPUBLICA | 1.274 | 1.092 |
| Procuraduría General de la Nación | PROCURADURIA GENERAL DE LA NACION | 2.224 | 1.549 |
| Defensoría del Pueblo | DEFENSORÍA DEL PUEBLO | 14.240 | 34.915 |
| Fondo de Bienestar Social de la CGR | FONDO DE BIENESTAR SOCIAL … | 1.104 | 782 |
| Instituto de Estudios del Ministerio Público | INSTITUTO DE ESTUDIOS … | 205 | 213 |

`ZERO_ROW = 0`. Cada conteo por `LIKE` coincide exactamente con el conteo por
NIT del nombre cubierto, de punta a punta.

Volumen reciente de 12 meses:

```text
por nombre físico cubierto      3.325 procesos · 10.563 contratos   ← reproduce R2C
por NIT (incluye 30 gerencias)  3.670 procesos · 10.835 contratos
```

## 8. Puertas

```text
EXACT_MULTI_OWNER            173 → 172
NEW_MULTI_OWNER_KEYS           0            PASS
NEW_SCAN_CROSS_OWNER          90 → 0        PASS
TERRITORIAL_NON_HIJACK        7 → 0         PASS
PARENT_RELATED_CANARIES     1/4 → 4/4       PASS
PROPOSED_REMOVALS (R1C)      0 → 4          ADJUDICADOS
```

`scripts/gazetteer_multi_owner_baseline.json` pierde `la defensoria`: la lista
es un techo de deuda aceptada y encogerla endurece la puerta, porque si esa
clave vuelve a tener dos dueños ahora falla.

### 8.1 Adjudicación de los 4 `PROPOSED_REMOVALS`

`audit_gazetteer_alias_ownership.py` pasa de 0 a 4 propuestas, todas sobre la
CGR y todas por la misma causa: el registro nuevo del Fondo contiene el nombre
de la Contraloría, así que su heurística `CROSS_ENTITY_HIJACK` marca cualquier
alias de la CGR contenido en el nombre de otra entidad del gazetteer.

| Alias propuesto para remoción | Adjudicación |
|---|---|
| `contraloria general` | **KEEP** — ya está en `scan_disabled_aliases`: no puede capturar texto ajeno por ninguna vía. |
| `la contraloria general` | **KEEP** — ídem. |
| `contraloria general de la republica` | **KEEP** — es el nombre canónico de la CGR. El desempate contra el Fondo lo hace longest-match sobre el alias específico del Fondo, verificado por canario (§4.5). |
| `la contraloria general de la republica` | **KEEP** — ídem. |

El auditor R1C es anterior a ADR-072 y no lee `scan_disabled_aliases`; por eso
dos de las cuatro propuestas ya estaban resueltas por metadata antes de que las
marcara. No se modifica el auditor en este lote: es herramienta compartida y
cambiar su semántica aquí mezclaría la evidencia de R2E con la de R1C.

Validación:

```text
tests/test_control_national_r2e.py     72 tests   (antes de los datos: 50 rojos, 22 verdes)
focales R2E+R2D.3+R2D.1+R2D+R2B+R1D+R1C          514 passed
corpus (scripts/run_query_corpus.py --mode all)   89/89 PASS
ruff check app/ tests/ scripts/                   PASS
make lint-core                                    PASS
suite completa            10.423 passed · 2 skipped · 15 xfailed · 0 failed
xfails                    15 — mismos que el baseline post-R2D.3; ninguno nuevo
```

No se modificó runtime: `entity_resolver.py`, `scan_alias_policy.py`,
`geography_alias.py` y `query_router.py` quedan intactos. R2E es datos, tests y
evidencia.

## 9. Deuda

**Nueva**

- `CGR_PHYSICAL_NAME_FRAGMENTATION_DEBT` — el NIT 899999067 publica 33 nombres
  físicos. El registro cubre `CGR-NIVEL CENTRAL` (5.654 procesos de 8.339); las
  30 gerencias departamentales y la unidad de regalías quedan fuera del `LIKE`.
  Resolverlo exige un buyer group, explícitamente prohibido en R2E.
- `BANREP_LA_REPUBLICA_OVERGENERIC_ALIAS_DEBT` — `la republica` es alias de
  `BANCO DE LA REPUBLICA` y aparece en 13 identidades distintas: Presidencia,
  Congreso, CGR, AGR y el Fondo entre ellas. Antes de R2E capturaba por scan la
  frase «contratos de la auditoría general de la república»; hoy pierde por
  longest-match frente a los aliases específicos, pero sigue viva:
  `resolve_entidad("Presidencia de la República")` → BANCO DE LA REPUBLICA.
  Fuera del universo R2E; no se tocó.
- `TERRITORIAL_CONTROL_FUZZY_FAIL_OPEN_DEBT` — `resolve_entidad` de una
  contraloría territorial sigue cayendo abierto en la CGR. Medido idéntico antes
  y después; cerrarlo requiere cobertura territorial, fuera de alcance.
- `REGISTRY_IDENTITY_NOISE` — 36 registros con documento de persona natural y el
  nombre de una institución de control, sin actividad. Ruido del directorio
  oficial, no del gazetteer.

**Intacta**

`SECOP_SYNTHETIC_URL_PROVENANCE_DEBT`, `CANONICAL_ENTITY_SOURCE_NAMES`,
`BUYER_CAPTURE_TRUNCATION` y el fuzzy sin puerta en hospital/ESE/universidad
heredado de R2D.

## 10. Límites

El cruce de seguridad usa el directorio oficial
[`SECOP II — Contacto Entidades y Proveedores`](https://www.datos.gov.co/resource/4ex9-j3n8.json)
filtrado por `es_entidad='Si'` (27.286 compradores) y contención por secuencia
completa de tokens: `valle` no coincide con `valledupar`, no hay `substring`
crudo. Un comprador ausente del directorio no queda medido; la ausencia de
candidato no prueba inexistencia jurídica.

R2E no crea buyer groups, no toca universidades, judicial, electoral,
contralorías territoriales, personerías ni la arquitectura de
`scan_alias_policy.py`.

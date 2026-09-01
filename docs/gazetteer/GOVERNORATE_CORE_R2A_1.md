# COMPOUND-BUYER-CAPTURE-R2A.1 — un nombre no se corta por su propia juntura

**Tipo:** corrección de gramática de captura · **Gazetteer:** sin cambios
**Fecha:** 2026-09-01 · **Rama:** `main`
**HEAD antes:** `f73ac749e18e49a42c90b324136e2c4548050734`
**Cierra:** `BUYER_CAPTURE_TRUNCATION_DEBT` (R2A §9)
**Evidencia:** [`artifacts/audits/governorate_core_matrix_r2a1.json`](../../artifacts/audits/governorate_core_matrix_r2a1.json)

---

## 1. Resumen ejecutivo

`de` y `del` articulan casi todo nombre propio colombiano. Las **siete**
familias de `ENTITY_REWRITES` los usaban como **frontera dura de captura**, con
la misma cadena de lookahead copiada siete veces. La gramática partía el nombre
justo por donde se articula.

El canario declarado era la contaminación territorial de Norte de Santander. Al
medir antes de tocar código apareció algo peor: la truncación también entregaba
**compradores equivocados con confianza media, en silencio**.

| Consulta | Antes | Después |
|---|---|---|
| `municipio de Villa de Leyva` | **ALCALDIA MUNICIPIO DE VILLAVICENCIO** | `FAIL_CLOSED` |
| `alcaldía de Villa de Leyva` | **ALCALDIA MUNICIPIO DE VILLAVICENCIO** | `FAIL_CLOSED` |
| `municipio de San Andrés de Sotavento` | **MUNICIPIO DE SAN JOSE DE CUCUTA** | `FAIL_CLOSED` |
| `alcaldía de San Juan de Pasto` | **MUNICIPIO DE SAN JOSE DE CUCUTA** | `FAIL_CLOSED` |
| `ESE Hospital San Rafael de Fusagasugá` | **…SAN RAFAEL DE FACATATIVA** | `ESE HOSPITAL SAN RAFAEL DE FUSAGASUGÁ` |
| `gobernación de Norte de Santander` | correcto **+ territorio `Santander`** | correcto, sin territorio |
| `universidad Nacional de Colombia` | correcto **+ `colombia` en el objeto** | correcto, objeto limpio |

Villa de Leyva y San Andrés de Sotavento **no están en el gazetteer**. La
respuesta honesta es pedir precisión, no elegir el vecino léxico más parecido.

---

## 2. Causa raíz

```text
CAPTURE_BOUNDARY (antes)
    (?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|sobre\b|$))
                                       ^^^^^^^^^^^^^
                                       conectores de nombre, no de cláusula
```

`para|donde|por|con|en|sobre` abren otra cláusula: son fronteras legítimas.
`de|del` **articulan el nombre**: Norte **de** Santander, Valle **del** Cauca,
Villa **de** Leyva, San Juan **de** Pasto, Universidad Nacional **de** Colombia,
Hospital San Rafael **de** Fusagasugá.

Con la frontera dura, `"gobernación de Norte de Santander"` capturaba
`"Norte"`, dejaba `"de Santander"` en el texto y el scan territorial lo leía
como el departamento de Santander. El comprador salía correcto **por
compensación**: el fuzzy encontraba `Gobernación Norte de Santander*` a partir
del fragmento.

### Familias afectadas

Las **siete**, porque las siete compartían la cadena: `alcaldía`,
`gobernación`, `gobierno departamental`, `ESE`, `hospital`, `universidad`,
`municipio`.

---

## 3. La corrección — tres piezas

### 3.1 Autoridad compartida, sin conectores

`CAPTURE_BOUNDARY` pasa a ser **una** constante que las siete familias
concatenan, y `de|del` sale de ella. La captura llega hasta el siguiente
conector que sí abre cláusula, y **quién decide dónde termina el nombre es el
resolver contra el gazetteer, no el regex**.

Siete copias eran siete sitios donde el defecto podía volver; ahora hay uno, y
un test lo fija.

### 3.2 `de|del` como punto de corte candidato

La escalera de `rewrite_entity` ya probaba spans del más largo al más corto
(ENTITY-TRAILING-001, para colas de basura como `"este mes"`). R2A.1 le añade
los cortes en cada `de|del`, **sin límite de profundidad**, porque el corte útil
puede estar más lejos que `_MAX_SHRINK_LEVELS`:

```text
"Medellín de obras de infraestructura de vías urbanas"
  escalera de un token  ->  llega sólo a 4 tokens del final
  cortes en de|del      ->  "Medellín"  ✓
```

Un prefijo que termine en conector (`"Departamento de Antioquia de"`) no se
propone: un nombre no acaba en "de".

### 3.3 Un fragmento no se acredita por parecido

Si la captura articula un nombre con `de|del`, **cualquier** prefijo más corto
es un trozo de ese nombre, y un trozo puede acreditarse por alias exacto pero
**nunca por fuzzy**. Es la misma enfermedad que R1D atajó para departamentos
(`San Andrés → San → Santander`), ahora tratada en general.

Sin conector interno no hay nombre compuesto que partir, así que la escalera de
basura trailing conserva su fuzzy y ENTITY-TRAILING-001 queda intacto.

### 3.4 Contención por tokens completos

`_official_matches_strict_rewrite` comparaba **substrings**: `"villa"` está
dentro de `"villavicencio"`, y por esa rendija `"Municipio de Villa"` acreditaba
a la alcaldía de Villavicencio. Ahora se comparan **secuencias de palabras**,
descartando el ruido de registro de SECOP (`ANTIOQUIA//`, `ATLANTICO**`,
`SOACHA..`) al tokenizar.

El ruido no siempre está en el borde del token, y ahí se escondía una trampa.
`"ALCALDIA LOCAL DE TEUSAQUILLO(oficial)"` pega el paréntesis **sin espacio**:
recortando extremos, `"teusaquillo(oficial"` seguía siendo un solo token y el
nombre se volvía **inalcanzable**. Se tokeniza por corridas alfanuméricas.

Ese fallo no lo vieron los tests focales ni el corpus: lo destapó el piso de
precisión de `tests/test_rated_eval.py` (RATED-040) en la suite completa. Es la
razón por la que "corrí dos archivos de test" no es validar.

### 3.5 Una institución sectorial no es un territorio

Capturar el nombre completo destapó un error de categoría que la truncación
tapaba. `"ESE Hospital Universitario de Santander"` no está en el gazetteer;
con el nombre entero en la mano, el fuzzy sin puerta de la familia ESE lo hizo
puntuar alto contra **`DEPARTAMENTO DE SANTANDER`** — una pregunta por un
hospital contestada con el comprador raíz del territorio.

Ninguna de las dos respuestas era correcta, pero ésa es peor **en clase**, y la
introdujo esta corrección. Las familias `hospital`, `ESE` y `universidad` se
marcan explícitas como sectoriales y no pueden resolver a un comprador raíz
departamental. Es una restricción de tipo, no un lexicón de entidades: la
cabeza sale de las formas canónicas que este mismo módulo produce.

### 3.6 Un exacto no pierde contra un fuzzy anterior

Dentro de un mismo span compiten formas canónicas rivales. El replacer de la
familia ESE ofrece `"Empresa Social del Estado X"` **antes** que `"ESE X"`, y la
primera hacía `fuzzy(95)` contra el hospital de Facatativá, tapando el alias
**exacto** del de Fusagasugá — que sí existe en el gazetteer. El orden del
replacer es una preferencia de redacción, no evidencia: ahora un exacto gana
dentro del span aunque llegue después.

---

## 4. La métrica corregida — dato ≠ ejecución

R2A reportó `ROOT_FUZZY: 0`. Era cierto **sobre el gazetteer** y falso sobre la
ejecución: Norte de Santander tenía su alias exacto y resolvía por `fuzzy(90)`.
Además el router escribía `method = "rewrite_gobernación"`, que nombra la
**gramática** que disparó, no cómo se acreditó la identidad — el fuzzy interno
quedaba invisible para cualquier auditoría.

R2A.1 separa los ejes y expone el método real en
`entidad_resolution.metadata.resolution_method`:

| Eje | Qué mide |
|---|---|
| `supporting_alias` | qué alias del gazetteer respalda la raíz — **dato** |
| `runtime_method` | cómo resolvió el runtime esta vez — **ejecución** |
| `residual_department` | territorio que la consulta filtra sin que nadie lo nombrara |

```text
                            PRE-R2A.1   POST-R2A.1
PASS                               29           29
FAIL_CLOSED                         3            3
RUNTIME_EXACT                      28           29
RUNTIME_FUZZY                       1            0
SUPPORTING_ALIAS_COUNT             29           29   <- no se movió
RESIDUAL_TERRITORY_ERRORS           1            0
```

`SUPPORTING_ALIAS_COUNT` idéntico en ambas columnas es la demostración de que
los dos ejes no son el mismo número. La sonda vive en
[`scripts/audits/governorate_core_matrix.py`](../../scripts/audits/governorate_core_matrix.py).

---

## 5. Coste medido y deuda que queda

### El guard cuesta dos municipios, y el remedio es un alias

Prohibir el fuzzy sobre fragmentos convierte un acierto-por-parecido en
`FAIL_CLOSED` cuando el nombre completo no está en el gazetteer **y** la forma
corta tampoco tiene alias exacto. Barrido sobre 26 municipios capitales, en la
construcción `"alcaldía de X de <tema>"`:

```text
pierden 2/26 — Medellín, Cali
```

Ambos porque su `official_name` es opaco (`DISTRITO ESPECIAL DE CIENCIA
TECNOLOGIA E INNOVACION DE MEDELLIN`) y `"alcaldía de Medellín"` sólo llegaba
por `fuzzy(90)`. Con una cola temporal (`"de 2025"`) no se pierden: la fecha se
consume antes de la captura. El remedio es un alias en `aliases_db.json`, no un
cambio de gramática — y R2A.1 tiene prohibido tocar el gazetteer.

### Latencia: +12 ms por consulta en la ruta de reescritura

Capturar el nombre completo alarga la escalera: donde antes se probaba un
candidato truncado, ahora se prueban el nombre entero y, si falla, sus cortes.
Cada intento que llega al Tier 3 es un fuzzy contra ~8 600 alias.

```text
antes    20,3 ms/consulta
después  32,7 ms/consulta      (mismo banco, 80 parses, mejor de 3)
```

Se midió también deduplicar las formas canónicas que las familias solapadas
(ESE y hospital) proponen dos veces: **no dio ganancia medible**, así que no se
incluyó — el coste no está en las repeticiones sino en los niveles extra de la
escalera.

12 ms de CPU son despreciables frente al presupuesto real de una consulta, que
lo domina la llamada a SECOP (p90 ≈ 11,6 s). Se deja registrado, no optimizado:
si algún día importa, es una tarea con su propia medición.

### `UNGATED_FUZZY_FOR_HEALTH_AND_EDU_NAMES` — acotada, no cerrada

```text
"hospital San Francisco de Villa de Leyva"
  antes  -> CORPORACIÓN INTERUNIVERSITARIA DE SERVICIOS - CIS   (fuzzy sobre el fragmento)
  ahora  -> SUBRED INTEGRADA DE SERVICIOS DE SALUD NORTE E.S.E. (fuzzy sobre el nombre completo)
```

La truncación se cerró — el span ya es el nombre entero y nada se derrama al
objeto — pero el resultado sigue siendo un comprador equivocado. La causa es
otra: las familias `hospital`, `ESE` y `universidad` **no tienen gate de
reescritura estricta**. `_official_matches_strict_rewrite` sólo cubre
`municipality` y `department`, así que un nombre ausente del gazetteer siempre
encuentra un vecino por encima del umbral.

§3.5 le pone un techo —esas familias ya no pueden responder con un comprador
raíz territorial— pero no la cierra: dentro del universo sectorial el fuzzy
sigue sin puerta. La evidencia en el cierre es el caso de arriba, verificado
sobre el código final: `fuzzy(85)` contra una ESE de Bogotá para un hospital de
Boyacá que no está en el gazetteer.

`"ESE Hospital Universitario de Santander"` acaba en el hospital de Neiva por
una causa **distinta**, que conviene no confundir: ahí el método es `exact`,
porque la frase genérica `"ESE Hospital Universitario"` es alias de ese
hospital concreto. Eso es ownership de alias —deuda de la familia R1C— y no se
arregla con una puerta de fuzzy.

No se corrige aquí: extender el gate estricto a esas familias rompería
resoluciones legítimas que hoy dependen del fuzzy (`"Empresa Social del Estado
Hospital San Rafael de Fusagasugá"` acierta por `fuzzy(95)`). Necesita su
propia medición.

---

## 6. Validación

```text
tests/test_compound_buyer_capture_r2a1.py            47 passed
tests R1D / R2A / cola territorial / R1A / geo       196 passed
foco router+entity+geo+gazetteer+plan             1 598 passed, 17 xfailed
corpus (scripts/run_query_corpus.py --mode all)      89/89 PASS
ruff · make lint-core                                limpios
aliases_db.json                                      SIN CAMBIOS
```

Cada una de las seis piezas se verificó **por mutación**: desactivada una a
una, cae el test que le corresponde y sólo ese grupo.

| Pieza desactivada | Tests que caen |
|---|---:|
| `de\|del` vuelve a ser frontera dura | 16 |
| sin guard de fragmento | 4 |
| sin exacto-gana-a-fuzzy | 1 |
| contención por substring | 1 |
| sin restricción sectorial | 1 |
| tokenizar recortando extremos en vez de por corridas | 2 |

### Barrido antes/después — 32 consultas reales

23 sin cambio. Las 9 que se movieron:

| Consulta | Antes | Después |
|---|---|---|
| `municipio de Villa de Leyva` | ALCALDIA MUNICIPIO DE VILLAVICENCIO | `FAIL_CLOSED` |
| `alcaldía de San Juan de Pasto` | MUNICIPIO DE SAN JOSE DE CUCUTA | `FAIL_CLOSED` |
| `municipio de San Andrés de Sotavento` | MUNICIPIO DE SAN JOSE DE CUCUTA | `FAIL_CLOSED` |
| `municipio de San Vicente del Caguán` | MUNICIPIO DE SAN JOSE DE CUCUTA | `FAIL_CLOSED` |
| `alcaldía de Villa del Rosario` | ALCALDIA MUNICIPIO DE VILLAVICENCIO | **MUNICIPIO DE VILLA DEL ROSARIO\*\*** |
| `ESE Hospital San Rafael de Fusagasugá` | …SAN RAFAEL DE FACATATIVA | **ESE HOSPITAL SAN RAFAEL DE FUSAGASUGÁ** |
| `hospital universitario de la Samaritana` | …HERNANDO MONCALEANO PERDOMO DE NEIVA | **HOSPITAL UNIVERSITARIO LA SAMARITANA** |
| `gobernación de Norte de Santander` | correcto + territorio `Santander` | correcto, sin territorio |
| `ESE Hospital Universitario de Santander` | EMPRESA SOCIAL DEL ESTADO HOSPITAL LOCAL | …HERNANDO MONCALEANO (sigue mal: §5) |

Dos son recuperación de identidad real —Villa del Rosario y La Samaritana
existen en el gazetteer y antes eran inalcanzables—; cuatro cambian un
comprador equivocado y confiado por una petición de precisión; una es lateral
dentro de la deuda abierta de §5.

---

## 7. Estado de deuda

| Deuda | Estado |
|---|---|
| `BUYER_CAPTURE_TRUNCATION_DEBT` | **CLOSED** |
| `UNGATED_FUZZY_FOR_HEALTH_AND_EDU_NAMES` | **NUEVA**, abierta (§5) — acotada por §3.5, no cerrada |
| `CANONICAL_ENTITY_SOURCE_NAMES_REQUIRED` | `OPEN_UNTOUCHED` |
| D6 | `OPEN_UNTOUCHED` |
| `SAME_LOGICAL_IDENTITY` (D5) | `OPEN_UNTOUCHED` |

No se tocó `aliases_db.json`, SoQL, Radar/SODA3/Actor, ministerios, la
agregación de Cundinamarca/Valle, ni Guainía. No se creó ecosystem
Gobernaciones. No se usó LLM.

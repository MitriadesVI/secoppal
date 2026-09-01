# GOVERNORATE-CORE-COVERAGE-R2A — identidad verificada de compradores raíz departamentales

**Tipo:** discovery registry-first + lote de gazetteer
**Fecha:** 2026-09-01 · **Rama:** `main`
**HEAD antes:** `1554886ffd2c2a473b979d03f787e62aa8ccd266`
**Evidencia cruda:** [`artifacts/audits/governorate_core_r2a_candidates.json`](../../artifacts/audits/governorate_core_r2a_candidates.json)

---

## 1. Resumen ejecutivo

R1D dejó 25/32 gobernaciones resueltas y 7 fallando cerrado. R2A no persiguió
32/32: persiguió **identidad demostrada**. El resultado es **PARTIAL**.

| Métrica | Antes | Después |
|---|---:|---:|
| `PASS` | 25 | **29** |
| `FAIL_CLOSED` | 7 | **3** |
| `WRONG_CHILD` | 0 | **0** |
| `ROOT_EXACT_ALIAS` | 25 | **29** |
| `ROOT_FUZZY` | 0 | **0** |
| `OTHER` | 0 | **0** |

Cuatro identidades se verificaron contra el registro y se aplicaron. Tres se
bloquearon: **no existen** como comprador raíz en SECOP II. Registrar cualquiera
de ellas habría producido una identidad inventada — exactamente lo que R1C y
R1D cerraron.

> **29 correctas + 3 honestamente desconocidas es el resultado bueno.**
> 32/32 sólo era alcanzable falsificando tres identidades.

---

## 2. El hallazgo que decide el lote

Las siete no eran el mismo problema. Se separan en tres mecanismos distintos, y
sólo uno era una laguna del gazetteer:

| Mecanismo | Jurisdicciones | Qué falta |
|---|---|---|
| **Identidad ausente del gazetteer** | Guaviare, Vaupés, Vichada | El registro SECOP II existe; el gazetteer no lo tenía. |
| **Identidad presente, frase natural huérfana** | San Andrés | El registro existe con un único alias: su propio nombre oficial. |
| **Raíz fragmentada: no existe nombre físico raíz** | Cundinamarca, Valle del Cauca | El comprador raíz **no tiene registro** en SECOP II. |
| **Sin evidencia SECOP II** | Guainía | La Gobernación no aparece en ningún dataset SECOP II. |

El tercero es el hallazgo estructural de R2A y se detalla en §6.

---

## 3. Discovery registry-first

Autoridad primaria, en este orden: procesos (`p6dx-8zbt`), contratos
(`jbjy-vk9h`), y el NIT de entidad. El SECOP Integrado (`rpmr-utcd`) se usó
**sólo** como evidencia auxiliar para corroborar identidad por NIT y para
explicar ausencias. Ninguna decisión se tomó por similitud de nombre.

| Jurisdicción | NIT | Nombre físico raíz (SECOP II) | Procesos | Contratos | Clasificación | Veredicto |
|---|---|---|---:|---:|---|---|
| Cundinamarca | 899999114 | — *(28 registros, ninguno raíz)* | — | — | `ROOT_IDENTITY_FRAGMENTED` | `BLOCK_NO_EVIDENCE` |
| Valle del Cauca | 890399029 | — *(29 registros, ninguno raíz)* | — | — | `ROOT_IDENTITY_FRAGMENTED` | `BLOCK_NO_EVIDENCE` |
| San Andrés | 892400038 | `GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA` | 31 349 | 34 240 | `IDENTITY_PRESENT_ALIAS_GAP` | `ACCEPT_ALIAS_RECONCILIATION` |
| Guainía | — | — | 0 | 0 | `NO_SECOP_II_EVIDENCE` | `BLOCK_NO_EVIDENCE` |
| Guaviare | 800103196 | `GOBERNACION DEL GUAVIARE` | 16 | 13 | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` |
| Vaupés | 845000021 | `GOBERNACION DEL VAUPES` | 2 | 1 | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` |
| Vichada | 800094067 | `DEPARTAMENTO DE VICHADA` | 507 | 513 | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` |

### Nombres físicos por dataset

Se verificó explícitamente, para cada jurisdicción, si el mismo nombre físico
funciona en procesos y en contratos. **Las siete son
`SAME_SOURCE_NAME_BOTH_DATASETS`.** El patrón que R1C encontró en Soacha
(`MUNICIPIO DE SOACHA..` en procesos, `MUNICIPIO DE SOACHA.` en contratos) no
reaparece aquí: `BLOCK_DATASET_NAME_SPLIT` queda sin instancias en este lote.

### El NIT como evidencia de identidad — Vichada

Vichada es el caso testigo de §6 de la tarea. Su nombre físico SECOP II es
`DEPARTAMENTO DE VICHADA`: no dice "gobernación". Lo que demuestra que **es** la
Gobernación no es el parecido del nombre, sino el NIT:

```text
SECOP II   DEPARTAMENTO DE VICHADA    nit 800094067    507 procesos / 513 contratos
SECOP I    VICHADA - GOBERNACIÓN      nit 800094067    14 389 filas (rpmr-utcd)
```

Misma cifra, misma autoridad territorial: misma identidad lógica. La misma
comprobación cruzada confirma Guaviare (`800103196`) y Vaupés (`845000021`).
El NIT se usó **sólo** para acreditar identidad; no se fusionó ningún registro
ni ecosistema por él.

---

## 4. Guainía — por qué la evidencia histórica no basta

Guainía es el caso donde la tentación de cerrar 32/32 era mayor, porque *sí*
hay evidencia — de la plataforma equivocada:

```text
rpmr-utcd   GUAINÍA - GOBERNACIÓN   origen=SECOPI   nit="NO DEFINIDO"   18 070 filas
p6dx-8zbt   (ninguna)
jbjy-vk9h   (ninguna)
```

Dos razones independientes para bloquear:

1. **No hay ancla de identidad.** El NIT es literalmente `NO DEFINIDO`. No hay
   nada que reconciliar contra un registro SECOP II porque no hay registro.
2. **Resolvería a cero.** El runtime filtra por el nombre físico en los datasets
   SECOP II. Un alias raíz apuntando a `GUAINÍA - GOBERNACIÓN` produciría un
   comprador que resuelve con confianza alta y devuelve cero filas — un fallo
   peor que el `FAIL_CLOSED` actual, porque el usuario leería "no hay contratos"
   en vez de "no conozco esa entidad".

`GUAINÍA - GOBERNACIÓN` es además una convención de nombres de SECOP I
(`DEPARTAMENTO - ENTIDAD`), no un nombre físico de SECOP II.

---

## 5. Cundinamarca y Valle — la raíz no existe

Ambas son canarios del lote y ambas siguen `FAIL_CLOSED`. La razón no es que el
gazetteer esté incompleto: es que **SECOP II no registra al comprador raíz**.

```text
Cundinamarca · NIT 899999114
  28 nombres físicos en procesos, 28 en contratos.
  Todos son secretarías, departamentos administrativos, oficinas
  o "GOBERNACIÓN DE CUNDINAMARCA - DESPACHO DEL GOBERNADOR" (9 procesos).
  SELECT ... WHERE entidad IN ('DEPARTAMENTO DE CUNDINAMARCA',
                               'GOBERNACION DE CUNDINAMARCA')  ->  0 filas

Valle del Cauca · NIT 890399029 / 8903990295 / 8903990291
  29 nombres físicos en cada dataset.
  Todos son secretarías, departamentos administrativos u oficinas.
  SELECT ... WHERE entidad IN ('DEPARTAMENTO DEL VALLE DEL CAUCA',
                               'GOBERNACION DEL VALLE DEL CAUCA',
                               'GOBERNACION DEL VALLE')        ->  0 filas
```

La contratación de estas dos gobernaciones ocurre **repartida** entre 28 y 29
registros que comparten NIT. No hay ninguno que las represente como un todo.

Elegir uno —la Secretaría de Salud por volumen, el Despacho del Gobernador por
nombre— sería precisamente el `WRONG_CHILD` que R1D cerró. Y elegir "todos"
excede el modelo actual: `official_name` es único por registro.

---

## 6. `CANONICAL_ENTITY_SOURCE_NAMES_REQUIRED`

**Estado: documentado, NO resuelto en R2A.**

R1C estableció que `LOGICAL_IDENTITY != SOURCE_PHYSICAL_NAME` con un caso de
*split entre datasets*. Cundinamarca y Valle muestran una segunda forma del
mismo hueco, más profunda:

```text
R1C (Soacha)   una identidad  ->  dos nombres físicos, uno por dataset
R2A (Cund/Val) una identidad  ->  N nombres físicos, ninguno raíz, mismo NIT
```

El modelo de un `official_name` por registro no puede representar un comprador
cuya contratación se distribuye entre N registros bajo un NIT común. Para
responder "contratos de la Gobernación de Cundinamarca" haría falta una noción
de **conjunto canónico de nombres físicos por identidad**, anclada en el NIT, y
un `WHERE` que admita ese conjunto.

Eso es un frente propio. R2A **no** lo diseña: cambiaría la forma del `WHERE` y
tocaría D5 (`SAME_LOGICAL_IDENTITY`), que queda `OPEN_UNTOUCHED`. Se propone
como ADR/frente separado con este documento como evidencia de entrada.

Mientras tanto, `FAIL_CLOSED` es la respuesta correcta: dice la verdad.

---

## 7. Aliases aplicados

Se prefirieron formas humanas de alta precisión sobre expansión mecánica. R1C
demostró que el exceso de aliases también genera deuda; además `normalize_text`
colapsa los acentos, de modo que registrar la variante sin tilde de cada frase
sólo duplica claves idénticas. Por eso los registros nuevos llevan **6 aliases**
y no las ~23 de los lotes antiguos.

| Entidad | Aliases |
|---|---|
| `GOBERNACION DEL GUAVIARE` | gobernación del Guaviare · gobernación de Guaviare · departamento del Guaviare · departamento de Guaviare · gobierno departamental del Guaviare · gobierno del Guaviare |
| `GOBERNACION DEL VAUPES` | gobernación del Vaupés · gobernación de Vaupés · departamento del Vaupés · departamento de Vaupés · gobierno departamental del Vaupés · gobierno del Vaupés |
| `DEPARTAMENTO DE VICHADA` | gobernación del Vichada · gobernación de Vichada · departamento del Vichada · departamento de Vichada · gobierno departamental del Vichada · gobierno del Vichada |
| `GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO…` | *(alias oficial preexistente)* + gobernación de San Andrés · gobernación de San Andrés y Providencia · gobernación del archipiélago de San Andrés · departamento de San Andrés · gobierno departamental de San Andrés |

**San Andrés no recibe el topónimo desnudo.** `san andrés` pertenece también a
la autoridad geográfica; darlo a un comprador reabriría el hijack que R1C
retiró del hospital de Tumaco. La entidad se reconcilia, no se recrea: no se
añadió ningún registro nuevo para el archipiélago.

---

## 8. Puerta de ownership endurecida

El gate `EXACT_MULTI_OWNER_BASELINE = 173` comparaba **cardinales**. Con
baseline `{A, B, C}`, un lote que salda `C` e introduce `D` deja 3 grupos, pasa
la puerta y crea una colisión nueva: un alias cuyo dueño no decide nadie.

R2A congela el **conjunto** de las 173 claves aceptado tras R1C/R1D
(`scripts/gazetteer_multi_owner_baseline.json`) y exige:

```text
NEW_MULTI_OWNER_KEYS = current_keys - baseline_keys = ∅
```

La deuda histórica puede encogerse (173 → 170 es válido); no puede renovarse
(173 → 173 con una clave nueva **falla**). El test de mutación
`test_validator_fails_on_substitution_at_equal_cardinality` fija exactamente el
caso que el cardinal no ve.

---

## 9. Defecto encontrado y corregido — cola territorial del comprador

La matriz de verificación destapó un fallo ajeno al gazetteer. La gramática de
comprador corta la captura en `" de/del "`:

```text
"gobernación del Valle del Cauca"
    consume  -> "gobernación del Valle"     (comprador, FAIL_CLOSED correcto)
    sobra    -> "del Cauca"                 -> departamento_resolved = 'Cauca'
```

Una pregunta por la Gobernación del **Valle del Cauca** emitía un `WHERE` por el
departamento del **Cauca** — un territorio que el usuario nunca nombró. Es un
*silent territorial fallback*, justo el invariante que R2A no podía sacrificar.

La corrección es un guard estricto y por tokens: se descarta el departamento
escaneado **sólo** cuando es parte del territorio que el propio comprador ya
declaró en su metadata. `"la gobernación del Cauca en Antioquia"` conserva la
intersección; `"contratos en el departamento del Cauca"` sigue siendo
territorio.

### Deuda hermana — `Norte de Santander` · **CERRADA en R2A.1**

El mismo corte produce un segundo caso, **fuera de las siete de R2A**:

```text
"gobernación de Norte de Santander"
    consume  -> "gobernación de Norte"      -> fuzzy -> 'Gobernación Norte de Santander*'
    sobra    -> "de Santander"              -> departamento_resolved = 'Santander'
```

Aquí el comprador acierta, pero **por fuzzy sobre una captura truncada**, y sin
`canonical_department` en la metadata el guard no tiene contra qué comparar.
Arreglarlo exige cambiar la gramática de captura en `entity_types.py`, que
afecta a todos los patrones de reescritura. Queda como deuda declarada:

```text
BUYER_CAPTURE_TRUNCATION_DEBT
  síntoma  : departamentos multipalabra con " de/del " se truncan al capturar
  víctimas : Norte de Santander (PASS con territorio contaminado)
  ámbito   : app/core/entity_types.py — gramática de captura, no gazetteer
  estado   : CLOSED por R2A.1 — ver GOVERNORATE_CORE_R2A_1.md
```

R2A.1 midió el alcance y resultó ser mucho mayor que este canario: las siete
familias de reescritura compartían la misma frontera, y la truncación no sólo
contaminaba con territorio residual — también entregaba **compradores
equivocados en silencio** (`municipio de Villa de Leyva` → Villavicencio).

---

## 10. Matriz 32/32 · POST-R2A

> **Nomenclatura corregida en R2A.1.** La columna `status` de esta tabla
> reporta `ROOT_EXACT_ALIAS` cuando existe un alias raíz que respalda la
> identidad: es una propiedad del **dato**, no del método que el runtime usó.
> Son dos ejes distintos, y confundirlos ocultó exactamente un caso —
> Norte de Santander tenía su alias exacto y resolvía por `fuzzy(90)`.
> La matriz con ambos ejes (`supporting_alias` · `runtime_method` ·
> `residual_department`) la produce
> [`scripts/audits/governorate_core_matrix.py`](../../scripts/audits/governorate_core_matrix.py).
> Los conteos de abajo se conservan como quedaron en R2A.

| natural_query | resolved_entity | supporting_alias | status *(soporte del dato)* |
|---|---|---|---|
| Gobernación del Amazonas | `GOBERNACION DEL AMAZONAS` | `gobernacion del amazonas` | `ROOT_EXACT_ALIAS` |
| Gobernación de Antioquia | `DEPARTAMENTO DE ANTIOQUIA//` | `gobernacion de antioquia` | `ROOT_EXACT_ALIAS` |
| Gobernación de Arauca | `DEPARTAMENTO DEL ARAUCA` | `gobernacion de arauca` | `ROOT_EXACT_ALIAS` |
| Gobernación del Atlántico | `DEPARTAMENTO DEL ATLANTICO**` | `gobernacion del atlantico` | `ROOT_EXACT_ALIAS` |
| Gobernación de Bolívar | `GOBERNACIÓN DE BOLÍVAR//` | `gobernacion de bolivar` | `ROOT_EXACT_ALIAS` |
| Gobernación de Boyacá | `GOBERNACIÓN DE BOYACÁ` | `gobernacion de boyaca` | `ROOT_EXACT_ALIAS` |
| Gobernación de Caldas | `GOBERNACION DE CALDAS` | `gobernacion de caldas` | `ROOT_EXACT_ALIAS` |
| Gobernación del Caquetá | `GOBERNACION DEL CAQUETA**` | `gobernacion del caqueta` | `ROOT_EXACT_ALIAS` |
| Gobernación de Casanare | `GOBERNACION DEL CASANARE` | `gobernacion de casanare` | `ROOT_EXACT_ALIAS` |
| Gobernación del Cauca | `GOBERNACIÓN DEL CAUCA` | `gobernacion del cauca` | `ROOT_EXACT_ALIAS` |
| Gobernación del Cesar | `GOBERNACION DEL DEPARTAMENTO DEL CESAR` | `gobernacion del cesar` | `ROOT_EXACT_ALIAS` |
| Gobernación del Chocó | `GOBERNACION DEL CHOCÓ.` | `gobernacion del choco` | `ROOT_EXACT_ALIAS` |
| **Gobernación de Cundinamarca** | — | — | **`FAIL_CLOSED`** |
| Gobernación de Córdoba | `DEPARTAMENTO DE CORDOBA` | `gobernacion de cordoba` | `ROOT_EXACT_ALIAS` |
| **Gobernación del Guainía** | — | — | **`FAIL_CLOSED`** |
| **Gobernación del Guaviare** | `GOBERNACION DEL GUAVIARE` | `gobernacion del guaviare` | **`ROOT_EXACT_ALIAS`** *(R2A)* |
| Gobernación del Huila | `GOBERNACION DEL HUILA*` | `gobernacion del huila` | `ROOT_EXACT_ALIAS` |
| Gobernación de La Guajira | `GOBERNACIÓN DE LA GUAJIRA` | `gobernacion de la guajira` | `ROOT_EXACT_ALIAS` |
| Gobernación del Magdalena | `GOBERNACION DEL MAGDALENA` | `gobernacion del magdalena` | `ROOT_EXACT_ALIAS` |
| Gobernación del Meta | `DEPARTAMENTO DEL META` | `gobernacion del meta` | `ROOT_EXACT_ALIAS` |
| Gobernación de Nariño | `GOBERNACION DE NARIÑO` | `gobernacion de narino` | `ROOT_EXACT_ALIAS` |
| Gobernación de Norte de Santander | `Gobernación Norte de Santander*` | `gobernacion de norte de santander` | `ROOT_EXACT_ALIAS` |
| Gobernación del Putumayo | `GOBERNACIÓN DEPARTAMENTO DEL PUTUMAYO` | `gobernacion del putumayo` | `ROOT_EXACT_ALIAS` |
| Gobernación del Quindío | `GOBERNACION DEL QUINDIO ARMENIA` | `gobernacion del quindio` | `ROOT_EXACT_ALIAS` |
| Gobernación de Risaralda | `GOBERNACIÓN DE RISARALDA**` | `gobernacion de risaralda` | `ROOT_EXACT_ALIAS` |
| **Gobernación de San Andrés** | `GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO…` | `gobernacion de san andres` | **`ROOT_EXACT_ALIAS`** *(R2A)* |
| Gobernación de Santander | `DEPARTAMENTO DE SANTANDER` | `gobernacion de santander` | `ROOT_EXACT_ALIAS` |
| Gobernación de Sucre | `GOBERNACION DE SUCRE` | `gobernacion de sucre` | `ROOT_EXACT_ALIAS` |
| Gobernación del Tolima | `GOBIERNO DEPARTAMENTAL` | `gobernacion del tolima` | `ROOT_EXACT_ALIAS` |
| **Gobernación del Valle del Cauca** | — | — | **`FAIL_CLOSED`** |
| **Gobernación del Vaupés** | `GOBERNACION DEL VAUPES` | `gobernacion del vaupes` | **`ROOT_EXACT_ALIAS`** *(R2A)* |
| **Gobernación del Vichada** | `DEPARTAMENTO DE VICHADA` | `gobernacion del vichada` | **`ROOT_EXACT_ALIAS`** *(R2A)* |

```text
MATRIX_AFTER (R2A — soporte del dato)
PASS:               29
FAIL_CLOSED:         3   (Cundinamarca, Valle del Cauca, Guainía)
WRONG_CHILD:         0
ROOT_EXACT_ALIAS:   29
ROOT_FUZZY:          0     <- medido sobre el gazetteer, NO sobre la ejecución
ROOT_OFFICIAL_ONLY:  0
OTHER:               0
```

```text
MATRIX_AFTER (R2A.1 — ejecución medida)
PASS:                       29
FAIL_CLOSED:                 3
RUNTIME_EXACT:              29
RUNTIME_FUZZY:               0     <- ahora sí, sobre el método real
SUPPORTING_ALIAS_COUNT:     29
RESIDUAL_TERRITORY_ERRORS:   0
```

Antes de R2A.1 la misma sonda daba `RUNTIME_EXACT 28 · RUNTIME_FUZZY 1 ·
RESIDUAL_TERRITORY_ERRORS 1` con `SUPPORTING_ALIAS_COUNT 29` intacto: la
diferencia entre las dos columnas es precisamente el defecto que se cerró.

---

## 11. Estado de deuda

| Deuda | Estado POST-R2A |
|---|---|
| D4 — identidad Alcaldía Local / FDL | `OPEN_UNTOUCHED` |
| D5 — `SAME_LOGICAL_IDENTITY` | `OPEN_UNTOUCHED` |
| D6 — regla `_is_unqualified_dept` | `OPEN_UNTOUCHED` · cerrada después en R2D.1 |
| `CANONICAL_ENTITY_SOURCE_NAMES_REQUIRED` | **NUEVA**, documentada §6, frente propio |
| `BUYER_CAPTURE_TRUNCATION_DEBT` | **CLOSED** por R2A.1 — [`GOVERNORATE_CORE_R2A_1.md`](GOVERNORATE_CORE_R2A_1.md) |
| `UNGATED_FUZZY_FOR_HEALTH_AND_EDU_NAMES` | **NUEVA**, abierta — ver R2A.1 §5 |

No se creó ecosystem Gobernaciones. No se añadieron ministerios, DAFP/DNI,
Alcaldía Mayor de Bogotá ni municipios. No se modificó SoQL. No se introdujo
fuzzy como solución en ningún caso.

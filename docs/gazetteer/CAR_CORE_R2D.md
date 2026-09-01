# CAR-CORE-R2D — núcleo verificado de 33 corporaciones ambientales

**Tipo:** expansión registry-first + reconciliación · **Fecha:** 2026-09-01

**Rama:** `main` · **HEAD antes:** `bcb8a5ff3e9f72ce8b5572ccd125974e1c7d217e`

**Evidencia:** [`car_core_r2d_candidates.json`](../../artifacts/audits/car_core_r2d_candidates.json)

**Reproductor:** [`audit_car_core_r2d.py`](../../scripts/audits/audit_car_core_r2d.py)

**Tests:** [`test_car_core_r2d.py`](../../tests/test_car_core_r2d.py)

## 1. Resultado

```text
CAR_CORE_R2D: PASS
MATRIZ (resolve_entidad, sigla + nombre institucional)
  PASS_EXACT   30
  FAIL_CLOSED   2   (CDA, CRA — sigla bloqueada)
  FUZZY_ONLY    1   (CRC — sigla bloqueada, nombre exacto)
  WRONG_OWNER   0
VOLUMEN CUBIERTO POR PASS_EXACT
  procesos  25.313 / 26.645 = 95,0 %
  contratos 22.719 / 24.213 = 93,8 %
```

No se buscó `PASS = 33`. Tres siglas quedaron fuera a propósito, y las tres
corporaciones entran igual por nombre institucional. Ninguna consulta natural
del universo resuelve hoy a un comprador ajeno.

## 2. El universo son 33, y Cormagdalena no es una de ellas

Autoridad: [Ministerio de Ambiente](https://www.minambiente.gov.co/eleccion-car/)
— 26 CAR y 7 corporaciones de desarrollo sostenible.

```text
CAR_CDS: 33
CORMAGDALENA: SPECIAL_AUTONOMOUS_CORPORATION / OUTSIDE_R2D_CORE
AUTORIDADES AMBIENTALES URBANAS: otro universo, no auditado aquí
```

Las 33 identidades quedaron verificadas contra SECOP II por NIT. **Cada NIT
devuelve exactamente un nombre físico, idéntico en procesos y en contratos**:
33/33 `SINGLE_PHYSICAL_NAME`, cero `SOURCE_NAME_SPLIT`, cero
`NO_SECOP_II_EVIDENCE`. Dos NIT merecen nota porque contradicen la intuición y
aun así son correctos: el del Quindío es `8900004478` —diez dígitos, con el
dígito de verificación pegado— y el del Sur de Bolívar publica bajo el nombre
físico `CSB`, acreditado por búsqueda de texto contra "SUR DE BOLIVAR".

## 3. Línea base

R2C se reprodujo contra `bcb8a5f` sin deriva: **11 `PASS_EXACT`, 22
`WRONG_OWNER`, drift 0**.

R2D usa un banco más exigente. R2C consultaba varias corporaciones por su
nombre largo incluso cuando tienen sigla propia; aquí se consulta **sigla y
nombre institucional para las 33**. Contra el mismo HEAD:

| Matriz | PASS_EXACT | WRONG_OWNER | FUZZY_ONLY |
|---|---:|---:|---:|
| R2C reproducido | 11 | 22 | 0 |
| R2D (sigla + nombre) | 9 | 23 | 1 |

La diferencia no es deriva: es superficie que R2C no probó. Dos casos que R2C
daba por buenos resultaron inseguros al preguntarlos por su sigla real —
`CORPONARIÑO` caía en la **Gobernación de Nariño** y `CRC` sólo llegaba por
fuzzy.

## 4. Causa raíz de las 24 resoluciones inseguras

| Causa | Casos | Entidades |
|---|---:|---|
| `IDENTITY_MISSING` | 17 | CAM, CARDIQUE, CARSUCRE, CDA, CODECHOCÓ, CORALINA, CORANTIOQUIA, CORPAMAG, CORPOCALDAS, CORPOCESAR, CORPOGUAJIRA, CORPOGUAVIO, CORPOMOJANA, CORPOURABÁ, CRA, CSB, CVS |
| `IDENTITY_PRESENT_ALIAS_GAP` | 6 | CORNARE, CORPONARIÑO, CORPONOR, CORPORINOQUIA, CORTOLIMA, CRC |
| `WRONG_ALIAS_OWNER` | 1 | CORPOCHIVOR |
| `SOURCE_NAME_SPLIT` | 0 | — |
| `MULTIPLE_PHYSICAL_SAME_IDENTITY` | 0 | — |
| `NO_SECOP_II_EVIDENCE` | 0 | — |

`ACRONYM_COLLISION`, `TERRITORY_HIJACK`, `PARENT_CHILD_HIJACK` y `FUZZY_HIJACK`
no son causas paralelas a éstas: son el **mecanismo** por el que se manifiestan.
Todas comparten un fail-open — cuando la identidad falta, el tier 3 del resolver
no se abstiene, sino que entrega el vecino léxico más cercano.

### 4.1 Dos aliases explicaban a quién secuestraba

El diagnóstico se pudo aislar quitando un alias a la vez:

- **`cor`**, alias de tres letras de CORPONARIÑO. `WRatio("corpocaldas", "cor")`
  = 90, y lo mismo para `corpamag`, `cornare`, `corponor`, `corpourabá`,
  `corpomojana`, `corporinoquia`. Ése era el destino de casi todas las siglas.
- **`corporación autónoma regional`**, alias de CORPONOR. Nombra la categoría
  entera, no a la entidad; `WRatio` contra cualquier "Corporación Autónoma
  Regional de X" da 95. Ése era el destino de casi todos los nombres largos.

Quitarlos **no cura el hijack**: al removerlos en una prueba aislada, las
consultas simplemente encontraron otros vecinos —`CORPOCALDAS` pasó a
*Municipio de Caldas (Antioquia)*, y el nombre del Tolima a *CVC*. Los aliases
basura deciden **a quién** se secuestra, no **si** se secuestra. La cura es la
identidad correcta: el tier 1 exacto gana antes de que el fuzzy opine. Las
remociones son defensa complementaria, y valen por sí solas porque un alias
exacto equivocado se entrega con `confidence: high`.

### 4.2 El daño por la vía del producto era mayor que el medido

La matriz mide `resolve_entidad`. El camino real del producto pasa antes por
`scan_entity`, y ahí el alias `corporación autónoma regional` ganaba como
subcadena más larga y devolvía CORPONOR con `method: scan_exact` y
`confidence: high` — **también para las 11 que la matriz daba por buenas**.

```text
ROUTER BEFORE: OK  5  WRONG 24  FAIL_CLOSED  4  territorio fabricado 20
ROUTER AFTER:  OK 14  WRONG  0  FAIL_CLOSED 19  territorio fabricado 19
```

Ninguna corporación empeoró. Los 19 que quedan cerrados se explican en §7.

## 5. Siglas: qué entró, qué no, y por qué

El riesgo de cada sigla se midió contra el **catálogo completo de compradores
SECOP II** (12.338 identidades `nit × nombre`) y contra el ownership del
gazetteer, no contra la intuición sobre su longitud.

**Admitidas (30).** Ninguna aparece como token en el nombre de otro comprador
ni tiene dueño ajeno en el gazetteer. Incluye siglas de tres letras —`CAM`,
`CSB`, `CVS`— porque la medición no encontró competencia y porque **no
agregarlas era la opción insegura**: `CAM` sin alias propio seguía resolviendo
a la *Cámara de Representantes*.

**Bloqueadas (3).**

| Sigla | Evidencia del bloqueo | Efecto |
|---|---|---|
| `CDA` | El token nombra a `SENA REGIONAL CUNDINAMARCA Grupo Administrativo CDA` (6.447 filas), y en Colombia designa a los centros de diagnóstico automotor | `FAIL_CLOSED` |
| `CRA` | Sigla normativa de la **Comisión de Regulación de Agua Potable y Saneamiento Básico**, compradora activa (3.453 filas); además abrevia *carrera* en direcciones | `FAIL_CLOSED` |
| `CRC` | Sigla normativa de la **Comisión de Regulación de Comunicaciones**, compradora activa (2.437 filas) | `FUZZY_ONLY` |

Ninguna de las tres pierde a su corporación: CDA, CRA y CRC entran por nombre
institucional con ownership exacto y único.

`CAR` merece mención aparte. Ya era alias exacto de la Corporación Autónoma
Regional de Cundinamarca —el uso colombiano corriente— y ahí se dejó. **No se
agregó ninguna forma `car de <departamento>` nueva**: seis departamentos tienen
más de una corporación (Antioquia tres; Bolívar, Boyacá, Cundinamarca,
Santander y Sucre dos cada uno), de modo que el territorio no identifica a la
corporación. La jurisdicción ambiental tampoco la convierte en gobierno
territorial, y los canarios lo verifican en ambos sentidos.

## 6. Cambios aplicados

```text
ENTITIES_ADDED:   18 (+1 defensiva, ver §6.3)
ALIASES_ADDED:    50
ALIASES_REMOVED:  13
RUNTIME_CHANGED:  NO
```

### 6.1 Retiros

Nueve aliases de **CORPOCHIVOR** estaban en poder de **CORPOBOYACA**, entre
ellos `corpochivor` y `corporación autónoma regional de chivor` — el nombre
institucional de una corporación asignado a su vecina, entregado por `exact`
con confianza alta. Se retiraron y se reasignaron a su dueña.

Se retiraron además `cor` (CORPONARIÑO) y los tres aliases de CORPONOR que
nombran la categoría y no la entidad: `corporación autónoma regional`,
`corporación regional`, `la corporación autónoma`.

### 6.2 Altas y completados

18 corporaciones sin registro entraron con su nombre físico SECOP II como
`official_name`, su NIT, su departamento sede y sus volúmenes reales. Cinco
registros existentes recibieron la forma que les faltaba: `cornare`,
`corponor`, `corporinoquia`, `corponariño` y el nombre institucional del
Tolima.

### 6.3 Cormagdalena — `DEFENSIVE_FIX`

Al registrar CORPAMAG, la frase *Corporación Autónoma Regional del Río Grande
de la Magdalena* empezó a caer en la CAR del Magdalena por `fuzzy(95)`: un
secuestro **causado por este lote**. Se aplicó la defensa mínima que no depende
del azar léxico — su identidad propia, verificada en SECOP II: NIT `829000127`,
nombre físico `CORPORACIÓN AUTÓNOMA DEL RÍO GRANDE DE LA MAGDALENA**`, 2.831
procesos y 2.601 contratos. **Sigue fuera de las 33** y así lo afirma el test.

## 7. Deuda: D6 bloquea a once corporaciones en el router

`_is_unqualified_dept` compara con `endswith` **sin frontera de palabra**. Por
eso `corpocaldas` "termina en" el departamento *Caldas* y el scan lo descarta
como topónimo desnudo; lo mismo `carsucre`, `codechocó`, `corantioquia`,
`corpoboyacá`, `corpocesar`, `corpoguajira`, `corponariño`, `cortolima`, y los
nombres institucionales que terminan en departamento.

```text
DEBT_D6_ACRONYM_ENDSWITH_DEPARTMENT
alcanzables por router: 22/33 — 78,8 % de procesos, 77,9 % de contratos
bloqueadas: CARSUCRE CODECHOCÓ CORANTIOQUIA CORPOBOYACÁ CORPOCALDAS
            CORPOCESAR CORPOGUAJIRA CORPONARIÑO CORTOLIMA CRA CRC
```

Las once resuelven correctamente por `resolve_entidad` y **ninguna devuelve un
dueño ajeno**: el modo de falla es cerrado. Arreglarlo es tocar D6, excluido
del alcance de R2D, y por eso se dejó congelado en un test que falla si el
conjunto crece o si alguna pasa a `WRONG_OWNER`.

## 8. Validación

```text
tests/test_car_core_r2d.py           231 passed
collision gate (R1C)                 NEW_MULTI_OWNER_KEYS = 0
                                     PROPOSED_REMOVALS    = 0 (antes y después)
                                     grupos multi-owner   173 → 173, por clave
dataset smoke                        34/34 resuelven al nombre físico
                                     ZERO_ROW_ANOMALIES = 0
ruff (archivos del lote)             PASS
make lint-core                       PASS
```

Se corrigió un oráculo de R2A: `test_the_archipelago_root_is_a_single_record`
medía la unicidad de la raíz territorial por substring `ARCHIPIELAGO` en el
nombre oficial, y CORALINA lleva el archipiélago en su nombre físico sin
competir por esa raíz. El test pasó a afirmar el invariante literal —evidencia
raíz y ownership de las frases raíz— y se le añadió un caso que exige que
CORALINA **no** sea la raíz. Es más estricto que antes, no menos.

### Nota sobre CORPOMOJANA

Publica un proceso y un contrato en todo el histórico de SECOP II, cero en los
últimos doce meses. No es una anomalía de resolución: su identidad es única,
su nombre físico existe y devuelve fila. Es escasez real de publicación, y se
registra como tal (`LOW_SECOP_II_PRESENCE`).

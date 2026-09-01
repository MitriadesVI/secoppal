# D6-DEPARTMENT-SUFFIX-R2D1 — geografía frente a institución que termina en geografía

**Tipo:** remediación de runtime · **Fecha:** 2026-09-01

**Rama:** `main` · **HEAD antes:** `5bddfdf90de83b0a647e8ba67cdfdbb781ecb6d0`

**Evidencia:** [`d6_department_suffix_r2d1.json`](../../artifacts/audits/d6_department_suffix_r2d1.json)

**Reproductor:** [`audit_d6_department_suffix_r2d1.py`](../../scripts/audits/audit_d6_department_suffix_r2d1.py)

**Tests:** [`test_d6_department_suffix_r2d1.py`](../../tests/test_d6_department_suffix_r2d1.py)

## 1. Resultado

```text
D6_DEPARTMENT_SUFFIX_R2D1: PASS
UNIVERSO
  aliases del gazetteer            8.645
  rechazados por la regla vieja    2.124
  liberados por la regla nueva     2.104
CAR/CDS POR LA RUTA DEL PRODUCTO (QueryRouter)
  alcanzables por nombre     14/33 → 33/33
  alcanzables por sigla      21/33 → 30/33   (CDA, CRA, CRC siguen bloqueadas)
  dueño ajeno                    0 → 0
  territorio fabricado           0 → 0
VOLUMEN RECIENTE ALCANZABLE
  procesos    78,8 % → 100 %
  contratos   77,9 % → 100 %
BARRIDO TERRITORIAL (330 consultas)
  capturas nuevas de comprador       0
  capturas perdidas                  0
```

## 2. La regla vieja preguntaba por el sufijo, no por la pertenencia

```python
for dept_key in DEPARTAMENTOS:
    if dept_key == alias_stripped or alias_stripped.endswith(dept_key):
        is_dept = True
```

`endswith` no tiene frontera de palabra. *corpocaldas* "termina en" **caldas**,
*carsucre* en **sucre**, *corponariño* en **nariño**. Y con frontera tampoco
bastaría: *Acueducto de Bogotá* y *Canal Teleantioquia* terminan igual de
literalmente en un departamento sin ser uno.

El propósito original era legítimo —impedir que "contratos en Antioquia" se
convirtiera en un comprador— pero el criterio elegido confundía **nombrar un
territorio** con **terminar en el nombre de un territorio**.

## 3. La igualdad estricta no era la respuesta, y hay evidencia

La hipótesis obvia —rechazar sólo cuando `alias == clave`— se probó y se
descartó. `DEPARTAMENTOS` no es una lista de 33 nombres canónicos sino un
catálogo de 122 formas geográficas que ya incluye sus envoltorios (`en
antioquia`, `del atlantico`, `departamento del atlantico`, `la capital`). Pero
no las incluye todas, y las que faltan empiezan por el artículo `el`:

```text
'licitaciones en el cauca'                       -> UNIVERSIDAD DEL CAUCA
'licitaciones en el distrito capital de bogota'  -> UNIVERSIDAD PEDAGÓGICA NACIONAL
'licitaciones en el valle del cauca'             -> GOBERNACION DEL VALLE DEL CAUCA - DADI
'licitaciones en el arauca'                      -> DEPARTAMENTO DEL ARAUCA
```

Los dos primeros son `territory → wrong buyer`: exactamente el fallo que R0/R1
cerraron con el caso San Andrés. La igualdad estricta cerraba D6 reabriendo
RC2, y por eso quedó **refutada**, no descartada por gusto.

## 4. La regla nueva: pertenencia, con despojo sólo en cabeza

Un alias es geografía cuando **él mismo, o lo que queda tras retirar wrappers
iniciales** (`el la los las de del en al`), pertenece al catálogo territorial.

```text
"el cauca"              → ["el cauca", "cauca"]                    → geografía
"en el meta"            → ["en el meta", "el meta", "meta"]        → geografía
"universidad del cauca" → ["universidad del cauca"]                → institución
"corpocaldas"           → ["corpocaldas"]                          → institución
"…alcaldía mayor de bogotá" → sin cambio: "de" no está en cabeza   → institución
```

La asimetría es el punto. Un envoltorio inicial no aporta identidad; un token
final sí: `de bogotá` al final de *Acueducto de Bogotá* es parte del nombre de
la empresa. Sin substring, sin sufijo textual, sin allowlist.

Vive en [`app/core/geography_alias.py`](../../app/core/geography_alias.py) —
puro, sin estado— y `_is_unqualified_dept` lo consulta en una línea. La
gramática de comprador raíz (`department_root_buyer_target`) conserva su
precedencia por delante.

## 5. Qué había realmente detrás de los 2.124 rechazos

| Clase | Casos | Ejemplo |
|---|---:|---|
| `LEGITIMATE_INSTITUTION_ENDING_IN_GEOGRAPHY` | 2.015 | `acueducto de bogota` |
| `TEXTUAL_SUFFIX_FALSE_POSITIVE` | 45 | `carsucre`, `corpocaldas` |
| `GEOGRAPHY_ALIAS_DEBT` | 44 | `ambiente bogota d.c.` |
| `BARE_GEOGRAPHY` | 15 | `antioquia` |
| `LEADING_WRAPPER_GEOGRAPHY` | 5 | `el cauca` |
| `OTHER` | 0 | — |

Sólo **20 de 2.124** eran geografía de verdad. El 99 % del rechazo era daño
colateral, y la parte que la intuición señalaba —las siglas tipo *corpocaldas*—
era la más pequeña de las dos causas: pesaban más los nombres institucionales
completos.

Los 153 aliases de comprador raíz (`departamento de X`, `gobernación de X`)
nunca entraron en el conteo: la precedencia los exime antes.

## 6. Lo que no cambió, medido y no supuesto

Las diez frases territoriales obligatorias siguen resolviendo a territorio con
`entidad_resolved = None`, incluidas las cuatro que la igualdad estricta
rompía. Los compradores raíz siguen siendo compradores. RC2 no se movió:

```text
"contratos en San Andrés"            → territorio (San Andrés, Providencia y Santa Catalina)
"Gobernación de San Andrés"          → GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO…
"contratos del Hospital San Andrés"  → HOSPITAL SAN ANDRES, sin territorio
```

El barrido sistemático de 330 consultas —cada departamento por diez plantillas—
produce el **mismo conjunto de capturas** antes y después: 0 nuevas, 0 perdidas.

## 7. La deuda no era de las CAR

D6 se descubrió midiendo corporaciones ambientales, pero las corporaciones eran
la minoría. Instituciones ajenas al universo CAR que la regla vieja bloqueaba y
la nueva alcanza, cada una verificada contra su registro real:

```text
secretaría general de la alcaldía mayor de Bogotá  → SECRETARÍA GENERAL DE LA ALCALDÍA MAYOR DE BOGOTÁ
Empresa de Acueducto y Alcantarillado de Bogotá    → EMPRESA DE ACUEDUCTO Y ALCANTARILLADO DE BOGOTÁ - E.S.P.
Instituto de Caminos y Construcciones de Cundinamarca → INSTITUTO … DE CUNDINAMARCA - ICCU
Canal Teleantioquia                                → SOCIEDAD TELEVISION DE ANTIOQUIA LTDA
Terminal de Transportes de Santa Marta Magdalena   → ALCALDÍA DISTRITAL DE SANTA MARTA
```

### 7.1 El caso de la ESE del Meta entregaba al comprador equivocado

`ESE del departamento de Meta` no era sólo un bloqueo. Con `endswith`, el scan
descartaba el alias propio de la empresa social y caía en el topónimo
calificado por "departamento de" — devolviendo **el departamento del Meta** con
`confidence: high`. Hoy devuelve `EMPRESA SOCIAL DEL ESTADO DEL DEPARTAMENTO
DEL META E.S.E`.

Por la ruta del router esa frase sigue cerrada como territorio: la detiene el
guard de comprador genérico (`generic_buyer_unqualified`), que es deuda
distinta de D6 y queda fuera de este lote. El modo de falla es cerrado, no un
dueño ajeno.

## 8. Siglas: la política de R2D no se movió

```text
CDA_NAME: REACHABLE     CDA_ACRONYM: BLOCKED
CRA_NAME: REACHABLE     CRA_ACRONYM: BLOCKED
CRC_NAME: REACHABLE     CRC_ACRONYM: BLOCKED
```

Las tres entran por su nombre institucional —que es lo que D6 les negaba— y
ninguna entra por su sigla. El conjunto de siglas bloqueadas tras el cambio es
exactamente `{CDA, CRA, CRC}`: el fix no abrió ninguna puerta trasera.

## 9. Un hallazgo lateral: la precedencia raíz quedó redundante

Bajo la regla nueva, **ningún alias del gazetteer depende ya** de que
`department_root_buyer_target` se evalúe primero: o no es geografía por
pertenencia, o empieza por un calificador de entidad. Con `endswith` sí era
load-bearing. Se conserva como autoridad semántica para aliases futuros, y el
test afirma el hecho medido en vez de simular una mutación que no muerde.

## 10. Mutaciones

| Mutación | Efecto exigido |
|---|---|
| restaurar `endswith` | caen las instituciones cuyo único alias termina en geografía, y CORPOCALDAS |
| igualdad estricta sin despojo | caen exactamente `el Cauca`, `el Valle del Cauca`, `el Distrito Capital de Bogotá`, `el Arauca` |

Las dos están congeladas como tests ejecutables, no como prosa.

## 11. Lo que sólo apareció al correr la suite completa

Nueve casos congelados como `xfail(strict=True)` en el lote 01 pasaron a
resolver bien y hubo que desmarcarlos: `icbf caldas`, `icbf cesar`,
`icbf cordoba`, `icbf magdalena`, `icbf norte de santander`, `sena antioquia`,
`sena caqueta`, `sena santander`, `sena regional cundinamarca`. Su razón de
xfail decía que la sigla ganaba —"'icbf' resolves to ICBF SEDE NACIONAL
first"— y era un diagnóstico equivocado: **tenían alias propio**, sólo que
terminaba en un departamento y D6 lo descartaba. Las dos que siguen en xfail,
`sena chocó` y `sena casanare`, no tienen alias en el gazetteer: ésas sí caen en
la sede nacional por la sigla, y son hueco de datos.

El golden de modalidad cambió por la misma causa. "Contratos de alimentación de
la regional SENA Bolívar" se partía en comprador genérico *SENA* más territorio
*Bolívar*; hoy nombra a la regional, que es lo que la frase pedía.

```text
antes:  entidad SENA            + departamento = 'Bolívar'
ahora:  entidad SENA REGIONAL BOLIVAR, sin territorio
```

Estos dos frentes son la mejor prueba de que la deuda no era de las CAR: no hay
ninguna corporación ambiental en ellos.

## 12. Deuda declarada, no resuelta

`GEOGRAPHY_ALIAS_REGISTRY_DEBT` — claves de `DEPARTAMENTOS` que no nombran a su
propio departamento y hoy sostienen decisiones geográficas:

```text
cucuta        → Norte de Santander     (es una ciudad)
cundi         → Cundinamarca           (fragmento)
bogota d.c. / bogota dc                (abreviatura no canónica)
departamento de atlantico / del atlantico / depto del atlantico
norte santander · san andres providencia · san andres y providencia
```

Limpiar `departamentos.py` queda fuera de R2D.1: cambiar el catálogo mueve la
resolución territorial de todo el producto y merece su propio lote con su
propia evidencia.

## 13. Validación

```text
tests/test_d6_department_suffix_r2d1.py      41 passed
tests/test_car_core_r2d.py                  250 passed
R2B · R2A · R2A.1 · R1D · R1C · geo/entity typed · qualified buyer   465 passed
collision gate R2A                            9 passed
suite completa                            10.294 passed · 2 skipped · 15 xfailed
suite completa en 5bddfdf (worktree)      10.216 passed · 11 skipped · 24 xfailed
ruff (archivos del lote)                    PASS
make lint-core                              PASS
aliases_db.json                             SIN CAMBIOS
```

Los xfail bajan de 24 a 15: son los nueve del lote 01 que dejaron de fallar. Los
skips difieren porque el baseline se corrió en un worktree temporal sin índice
local de actores (`test_actor_conversation_dogfood`, `test_actor_index_isolation`),
condición del entorno y no del cambio.

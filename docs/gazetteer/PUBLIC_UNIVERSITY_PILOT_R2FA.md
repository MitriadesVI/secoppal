# PUBLIC-UNIVERSITY-PILOT-R2FA

**Tipo:** auditoria supervisada + curacion acotada  
**Fecha:** 2026-09-02  
**HEAD PRE:** `8849f0d`

## PRE congelado antes de mutar datos

Se reprodujeron las 34 consultas naturales del universo R2C contra el resolver
actual y se revalido identidad con NIT, nombre fisico byte-for-byte y actividad
en `p6dx-8zbt` / `jbjy-vk9h`. La matriz independiente es:

```text
TOTAL         34
PASS_EXACT    19
FAIL_CLOSED   10
WRONG_OWNER    4
FUZZY_ONLY     1
LIKE           0
AMBIGUOUS      0
```

Diez `WRONG_OWNER` historicos ahora fallan cerrados por R2E.1/R2E.1A. El
undecimo drift es una correccion de identidad: UFPS Ocaña no comparte el NIT
de Cucuta. Ocaña es el comprador fisico
`UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA (ENTIDAD)`, NIT
`8001631300`; Cucuta publica como `UNIVERSIDAD FRANCISCO DE PAULA SANTANDER`,
NIT `890500622`.

No existe un `SOURCE_NAME_SPLIT` acreditado en las 34. La ausencia de filas de
contratos no es otro source name. El segundo nombre bajo el NIT de UTP es
`CCE ENTIDAD DE PRUEBAS (Entidad de Sistema)` y se clasifica `REGISTRY_NOISE`,
no segunda identidad universitaria.

## Seleccion del piloto

| Caso | NIT | PRE | Forma de identidad | Razon |
|---|---|---|---|---|
| Universidad de la Amazonia | 891190346 | FAIL_CLOSED | SINGLE_SOURCE_NAME | canario historico; antes caia en SDIS |
| Universidad de Antioquia | 890980040 | PASS_EXACT | SINGLE_SOURCE_NAME | control negativo, NO_CHANGE_REQUIRED |
| Universidad del Atlantico | 890102257 | WRONG_OWNER | SINGLE_SOURCE_NAME | mayor actividad entre los wrong con NIT |
| Universidad de Cordoba | 891080031 | FAIL_CLOSED | SINGLE_SOURCE_NAME | volumen reciente y evidencia dual |
| UFPS Cucuta | 890500622 | FUZZY_ONLY | SINGLE_SOURCE_NAME | unico fuzzy-only correcto actual |
| UFPS Ocana | 8001631300 | WRONG_OWNER | SEPARATE_BUYER | corrige mezcla historica de sedes/NIT |
| Universidad Internacional del Tropico Americano | 8440020714 | WRONG_OWNER | SINGLE_SOURCE_NAME | fuzzy hacia Universidad Nacional |
| Universidad Autonoma Indigena Intercultural | sin NIT | WRONG_OWNER | NO_SECOP_EVIDENCE | debe fallar cerrada, no inventar owner |

La seleccion contiene los cuatro `WRONG_OWNER`, el unico `FUZZY_ONLY`, dos
`FAIL_CLOSED` relevantes y un `PASS_EXACT` de abstencion. No se fuerza un caso
`SOURCE_NAME_SPLIT`: no existe evidencia para hacerlo.

## Decisiones congeladas antes de implementar

- Amazonia, Cordoba, UFPS Ocana y Unitropico: identidad propia acreditada.
- Atlantico: conservar canonical `UNIATLANTICO` y proyectar el nombre fisico;
  no renombrar el canonical para satisfacer Socrata.
- UFPS Cucuta/Ocana: compradores separados por NIT. El nombre no calificado
  de UFPS conserva ownership exacto para Cucuta, pero deja de ser elegible para
  scan porque aparece dentro del comprador fisico de Ocana.
- UAIIN: `RESOLUTION_BLOCK`; la busqueda registral no acredita comprador
  fisico SECOP y similarity no puede ser autoridad.
- Antioquia: `NO_CHANGE_REQUIRED`.

Los oraculos predictivos estan en
`tests/test_public_university_pilot_r2fa.py`. El cierre POST, gates y evidencia
reproducible esta en
[`public_university_pilot_r2fa.json`](../../artifacts/audits/public_university_pilot_r2fa.json),
generada por
[`audit_public_university_pilot_r2fa.py`](../../scripts/audits/audit_public_university_pilot_r2fa.py).

## Drift R2C -> PRE actual

R2C historico tenia 19 `PASS_EXACT`, 13 `WRONG_OWNER` y 2 `FUZZY_ONLY`.
La revalidacion actual cambia once casos:

| Caso | R2C | PRE actual | Causa |
|---|---|---|---|
| Amazonia | WRONG_OWNER | FAIL_CLOSED | fuzzy identity guard |
| Colegio Mayor de Cundinamarca | WRONG_OWNER | FAIL_CLOSED | fuzzy identity guard |
| Cordoba | WRONG_OWNER | FAIL_CLOSED | fuzzy identity guard |
| Cundinamarca | WRONG_OWNER | FAIL_CLOSED | fuzzy identity guard |
| UFPS Ocana | FUZZY_ONLY | WRONG_OWNER | NIT historico incorrecto; comprador separado |
| La Guajira | WRONG_OWNER | FAIL_CLOSED | territory/identity guard |
| Pacifico | WRONG_OWNER | FAIL_CLOSED | fuzzy identity guard |
| Popular del Cesar | WRONG_OWNER | FAIL_CLOSED | fuzzy identity guard |
| Sucre | WRONG_OWNER | FAIL_CLOSED | fuzzy identity guard |
| Tecnologica del Choco | WRONG_OWNER | FAIL_CLOSED | territory/identity guard |
| Tolima | WRONG_OWNER | FAIL_CLOSED | fuzzy identity guard |

Los otros 23 casos no cambian respecto de R2C. Amazonia ya no cae en la
Secretaria Distrital de Integracion Social: queda segura pero sin cobertura,
que es justamente la deuda curada por el piloto.

## Matriz POST del piloto

| Caso | NIT | PRE | Decision | Proyeccion | POST |
|---|---|---|---|---|---|
| Amazonia | 891190346 | FAIL_CLOSED | ADD_EXACT_AND_SCAN | si, `(Entidad)` exacto | PASS_EXACT |
| Antioquia | 890980040 | PASS_EXACT | NO_CHANGE_REQUIRED | no | PASS_EXACT |
| Atlantico | 890102257 | WRONG_OWNER | KEEP | si, canonical preservado | PASS_EXACT |
| Cordoba | 891080031 | FAIL_CLOSED | ADD_EXACT_AND_SCAN | si, grafia SECOP sin acento | PASS_EXACT |
| UFPS Cucuta | 890500622 | FUZZY_ONLY | ADD_EXACT; base exact sin scan | no | PASS_EXACT |
| UFPS Ocana | 8001631300 | WRONG_OWNER | SEPARATE_BUYER + ADD | si, `(ENTIDAD)` exacto | PASS_EXACT |
| Unitropico | 8440020714 | WRONG_OWNER | ADD_EXACT_AND_SCAN | no; canonical ya es fisico | PASS_EXACT |
| UAIIN | sin NIT | WRONG_OWNER | RESOLUTION_BLOCK | no | FAIL_CLOSED |

```text
PILOT PRE   PASS_EXACT 1  FAIL_CLOSED 2  WRONG_OWNER 4  FUZZY_ONLY 1
PILOT POST  PASS_EXACT 7  FAIL_CLOSED 1  WRONG_OWNER 0  FUZZY_ONLY 0
```

UAIIN no se fuerza a 8/8: la busqueda registral no devuelve un comprador
fisico propio ni NIT/actividad atribuible. El bloqueo evita que el fuzzy vuelva
a inventar Universidad Nacional como owner.

## Alias ownership y scan (ADR-072)

Se agregaron diez claves normalizadas elegibles para scan, contando las formas
curadas y colapsando duplicados por normalizacion. Cada una se contrasto por
secuencia completa de tokens contra el directorio `4ex9-j3n8`:

```text
NEW_SCAN_CROSS_OWNER     0
EXACT_MULTI_OWNER_BEFORE 172
EXACT_MULTI_OWNER_AFTER  172
NEW_MULTI_OWNER_KEYS     0
```

El alias exacto `universidad francisco de paula santander` se mantiene para
Cucuta, pero se declara `scan_disabled_aliases`: el nombre fisico de Ocana lo
contiene y no puede ser secuestrado por un fragmento. Las formas calificadas
con `Cucuta`, `Ocana` o `seccional Ocana` conservan scan. No se removieron
aliases.

## Source Name Projection (ADR-073)

Se agregaron cuatro bindings acumulativos; el sidecar conserva el binding
historico de DAPRE y ahora suma cinco. Su procedencia raiz apunta a este
artefacto, que contiene tanto DAPRE como la nueva evidencia.

| Canonical preservado | NIT | Procesos exacto (filas) | Contratos exacto (filas) |
|---|---|---|---|
| `UNIATLANTICO` | 890102257 | `Universidad del Atlántico` (4.592) | igual (4.546) |
| `UNIVERSIDAD DE LA AMAZONIA` | 891190346 | `UNIVERSIDAD DE LA AMAZONIA (Entidad)` (2.549) | igual (2) |
| `UNIVERSIDAD DE CÓRDOBA` | 891080031 | `UNIVERSIDAD DE CORDOBA` (3.708) | igual (2) |
| `UNIVERSIDAD ... SECCIONAL OCAÑA` | 8001631300 | nombre con `(ENTIDAD)` (1.509) | igual (1) |

Las grafias completas byte-for-byte, incluidos acentos, estan en el JSON. Los
ocho canarios dataset/binding prueban igualdad exacta, ausencia de `LIKE`,
`UPPER`, `unaccent` y wildcard, canonical estable y params inmutables.

## Replay de las 34 y no regresion

```text
POST TOTAL        34
POST PASS_EXACT   25
POST FAIL_CLOSED   9
POST WRONG_OWNER   0
POST FUZZY_ONLY    0
POST LIKE          0
POST AMBIGUOUS     0

NEW_WRONG_OWNER_OUTSIDE_PILOT 0
```

No se intentaron cerrar las otras ocho universidades que aun fallan cerradas.
La matriz completa, nombres registrales, nombres fisicos por dataset, cuatro
conteos de actividad, aliases existentes, owner/metodo PRE/POST y clasificacion
de identidad estan en el artefacto.

## Verificacion

| Gate | Resultado |
|---|---|
| Piloto + ADR-073 + R2E.1A/R2E.1/R2E/R2D.3/R1C/R1D + CAR + nacional + SENA/ICBF | 829 passed |
| Query corpus | 89/89 PASS, 0 FAIL |
| Ruff | PASS |
| `make lint-core` | PASS |
| Suite completa | 10.656 passed, 2 skipped, 15 xfailed |
| Nuevos xfails | 0 |

## Deuda

**Cerrada:** Amazonia, Atlantico, Cordoba, UFPS Cucuta, UFPS Ocana y
Unitropico pasan exactos; UAIIN deja de fallar abierta; la mezcla historica de
los NIT UFPS queda corregida.

**Nueva:** ninguna deuda de colision, scan, runtime o xfail. El `FAIL_CLOSED`
de UAIIN es una decision segura respaldada por ausencia de evidencia, no una
identidad pendiente inventada.

**No tocada:** ocho universidades fuera del piloto siguen `FAIL_CLOSED`; no se
crearon Buyer Groups, no se curaron privadas, CGR, Cundinamarca/Valle,
judicial, defensa, geografia, Radar, Actor ni semantica SODA3.

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
reproducible se incorporan al finalizar la curacion.

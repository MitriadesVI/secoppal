# PUBLIC-UNIVERSITY-COMPLETION-R2FB

**Tipo:** curacion acotada + auditoria supervisada
**Fecha:** 2026-09-02
**HEAD PRE:** `e33a97a`

R2F-A curo ocho universidades y dejo explicitamente sin tocar otras ocho que
seguian fallando cerradas. Este lote las cierra. El universo esta congelado en
esas ocho: no se toco ninguna de las que ya pasaban exacto, ni UAIIN salvo para
el replay.

## Revalidacion en vivo, no copia de R2F-A

La matriz PRE de R2F-A se reprodujo contra SECOP II antes de mutar nada. Para
cada caso se verificaron NIT, nombres registrales en `4ex9-j3n8`, nombres
fisicos en `p6dx-8zbt` y `jbjy-vk9h`, actividad total y de los ultimos doce
meses, y el resultado vivo del resolver y del scan.

Las ocho confirman la clasificacion congelada: `SINGLE_SOURCE_NAME`, un solo
nombre fisico por NIT, y `FAIL_CLOSED` en el resolver por el guard de identidad
fuzzy. Ninguna revela `BUYER_GROUP`, `MULTIPLE_PHYSICAL_UNITS` ni
`IDENTITY_AMBIGUOUS`, asi que ninguna se deja cerrada por falta de evidencia.

Dos hallazgos de ruido registral, ninguno de identidad:

- Bajo el NIT de la Universidad de Cundinamarca el directorio publica tambien
  `CENTROACADEMICODEPORTIVO`. No aparece como nombre fisico en ningun dataset:
  `REGISTRY_NOISE`, no segunda identidad.
- Bajo el NIT de La Guajira convive la grafia `UNIVERSIDA DE LA GUAJIRA`, un
  error de digitacion del registro que tampoco publica.

## Matriz del lote

| Caso | NIT | Nombre fisico (procesos) | Contratos | PRE | Decision | POST |
|---|---|---|---|---|---|---|
| Colegio Mayor de Cundinamarca | 800144829 | `Universidad Colegio Mayor de Cundinamarca` (16) | 0 | FAIL_CLOSED | ADD_EXACT_AND_SCAN + proyeccion solo procesos | PASS_EXACT |
| Cundinamarca | 890680062 | `UNIVERSIDAD DE CUNDINAMARCA` (3.551) | 10 | FAIL_CLOSED | ADD_EXACT_AND_SCAN | PASS_EXACT |
| La Guajira | 892115029 | `UNIVERSIDAD DE LA GUAJIRA` (3.828) | 0 | FAIL_CLOSED | ADD_EXACT_AND_SCAN | PASS_EXACT |
| Pacifico | 835000300 | `UNIVERSIDAD DEL PACIFICO//` (987) | 5 | FAIL_CLOSED | ADD_EXACT_AND_SCAN + proyeccion | PASS_EXACT |
| Popular del Cesar | 8923002856 | `UNIVERSIDAD POPULAR DEL CESAR.` (57) | 0 | FAIL_CLOSED | ADD_EXACT_AND_SCAN + proyeccion solo procesos | PASS_EXACT |
| Sucre | 892200323 | `UNIVERSIDAD DE SUCRE` (1.719) | 1 | FAIL_CLOSED | ADD_EXACT_AND_SCAN | PASS_EXACT |
| Tecnologica del Choco | 891680089 | `UNIVERSIDAD TECNOLOGICA DEL CHOCO` (2.257) | 1 | FAIL_CLOSED | ADD_EXACT_AND_SCAN + proyeccion | PASS_EXACT |
| Tolima | 890700640 | `UNIVERSIDAD DEL TOLIMA` (4.085) | 0 | FAIL_CLOSED | ADD_EXACT_AND_SCAN | PASS_EXACT |

```text
TARGET PRE   FAIL_CLOSED 8
TARGET POST  PASS_EXACT  8
```

## Cuando se declara un binding (ADR-073)

Un binding se declara solo cuando la identidad canonica difiere del nombre
fisico **byte-for-byte**. Cuatro lo requieren y cuatro no; declarar los otros
cuatro no cambiaria un caracter del filtro, y un binding que no separa nada es
ruido de auditoria con forma de evidencia.

| Canonical | Nombre fisico | Por que |
|---|---|---|
| `UNIVERSIDAD COLEGIO MAYOR DE CUNDINAMARCA` | `Universidad Colegio Mayor de Cundinamarca` | la caja del dataset no define el canonico |
| `UNIVERSIDAD DEL PACÍFICO` | `UNIVERSIDAD DEL PACIFICO//` | la doble barra es grafia de la fuente, no identidad |
| `UNIVERSIDAD POPULAR DEL CESAR` | `UNIVERSIDAD POPULAR DEL CESAR.` | el punto final es grafia de la fuente |
| `UNIVERSIDAD TECNOLÓGICA DEL CHOCÓ DIEGO LUIS CÓRDOBA` | `UNIVERSIDAD TECNOLOGICA DEL CHOCO` | la fuente publica el nombre corto y sin acentos |

El Choco es el caso que prueba que el binding no es cosmetico: el LIKE del
nombre legal completo no alcanza ni un solo comprador del directorio, ni
siquiera el propio. Sin proyeccion, la identidad correcta devolveria cero
filas.

Cundinamarca, La Guajira, Sucre y Tolima conservan su camino previo intacto:
su canonico ya es el nombre fisico exacto.

### Ausencia de contratos no es un split

Colegio Mayor y Popular del Cesar no publican un solo contrato bajo su NIT. Sus
bindings declaran unicamente `p6dx-8zbt`. Declarar ahi el nombre de procesos
afirmaria una observacion que nadie hizo; el dataset de contratos conserva su
proyeccion previa, que devuelve cero porque no hay nada que devolver.

### Paridad viva

Cada binding se contrasto contra el dataset con su propio filtro proyectado.
Las filas devueltas igualan exactamente las observadas por NIT: 16, 987/5, 57 y
2.257/1. `LIVE_PROJECTION_PARITY: PASS`.

## Alias ownership y scan (ADR-072)

Diez claves normalizadas nuevas, todas `EXACT_ELIGIBLE` y `SCAN_ELIGIBLE`.
Ninguna es un fragmento generico: son nombres institucionales completos o
formas calificadas por territorio.

Dos aliases nuevos contienen fragmentos que **ya tienen dueño**:

```text
"colegio mayor"           -> INSTITUCIÓN UNIVERSITARIA COLEGIO MAYOR DE ANTIOQUIA
"universidad tecnologica" -> UPTC
```

Los aliases nuevos son estrictamente mas largos y `scan_entity` devuelve el
match mas largo, asi que el fragmento no secuestra al comprador nuevo ni el
comprador nuevo le quita el fragmento a su dueño. Ambas direcciones estan
congeladas como oraculo. No se agrego ningun fragmento suelto: `universidad
tecnologica`, `universidad popular`, `universidad del pacifico` sin
calificador y `universidad de cundinamarca` como prefijo quedan fuera.

### Cross-owner: la auditoria se hizo mas severa

R2F-A audito el scan con `$q` acotado a cien resultados. Este lote pagina el
directorio **completo** —27.290 compradores— y compara por secuencia de tokens
en local. Es estrictamente mas exigente, y por eso encuentra tres homonimos que
un full-text acotado no habria mostrado:

| Alias | Homonimo | NIT | Procesos | Contratos |
|---|---|---|---|---|
| `universidad de la guajira` | `UNIVERSIDAD DE LA GUAJIRA` | 1010069431 | 0 | 0 |
| `universidad de la guajira` | `UNIVERSIDAD DE LA GUAJIRA` | 8929150294 | 0 | 0 |
| `universidad tecnologica del choco` | `UNIVERSIDAD TECNOLOGICA DEL CHOCO` | 35601815 | 0 | 0 |

Los tres son entradas registrales con NIT de persona natural o mal digitado, y
ninguno publica una sola fila en procesos ni en contratos. Un homonimo que no
publica no es un comprador fisico que el scan pueda secuestrar: no existe nada
suyo que atribuir mal. Se conserva el scan y se reporta la distincion en vez de
esconderla.

```text
NEW_SCAN_CROSS_OWNER                 0   (compradores fisicos)
REGISTRY_HOMONYMS_WITHOUT_ACTIVITY   3   (0 filas en ambos datasets)
EXACT_MULTI_OWNER_BEFORE           172
EXACT_MULTI_OWNER_AFTER            172
NEW_MULTI_OWNER_KEYS                 0
```

## Grafias publicadas y copy-paste

Una revision posterior al cierre pregunto por dos grafias concretas y destapo un
hueco sistematico: `normalize_text` conserva la puntuacion final, asi que
ninguna forma con puntuacion de fuente entra por la via exacta. Dos de las ocho
dependian de fuzzy para reconocer **su propio nombre fisico**, el que SECOP
publica y el que un usuario copiaria y pegaria.

| Grafia | Antes | Ahora | Evidencia |
|---|---|---|---|
| `UNIVERSIDAD DEL PACIFICO//` | fuzzy(96) | exact | 987 procesos + 5 contratos |
| `UNIVERSIDAD POPULAR DEL CESAR.` | fuzzy(98) | exact | 57 procesos |
| `UNIVERSIDA DE LA GUAJIRA` | FAIL_CLOSED | exact, sin scan | fila del directorio bajo NIT 892115029 |

Ninguna daba owner equivocado, asi que los gates del cierre seguian validos: el
hueco era de cobertura exacta, no de correccion.

El typo registral es `EXACT_ONLY`. Acredita a quien lo publica y nada mas: un
error de digitacion no es una forma con la que alguien nombre al comprador
dentro de texto libre, y darle scan seria inventarle un uso que la evidencia no
sostiene. Las dos grafias fisicas si conservan scan; sus tokens son identicos a
los de la forma limpia que ya pertenece al mismo owner, de modo que no abren
superficie nueva.

```text
NEW_SCAN_CROSS_OWNER   0
NEW_MULTI_OWNER_KEYS   0
```

### Una variante que NO se acredito

La misma revision aporto `UNIVERSIDAD TECNOLOGICA DEL CHOCO.`, con punto final,
desde una vista de usuarios de SECOP. No se promovio a alias: la forma con punto
devuelve **cero filas** por igualdad exacta en `p6dx-8zbt` y en `jbjy-vk9h`, y
tampoco aparece en el directorio. Un barrido `LIKE` sobre ambos datasets
confirma que existe **una sola grafia** para ese NIT, sin punto, con 2.257 y 1
filas: el binding no pierde ni una.

Queda registrada como `OFFICIAL_SURFACE_VARIANT` aportada externamente y no
reproducida en las fuentes que el runtime consulta. Sigue resolviendo al owner
correcto por fuzzy, y ese comportamiento esta congelado como oraculo. La
identidad esta acreditada; la grafia no, y evidencia externa sin reproducir no
compra ownership exacto.

## Replay de las 34

```text
POST TOTAL        34
POST PASS_EXACT   33
POST FAIL_CLOSED   1
POST WRONG_OWNER   0
POST FUZZY_ONLY    0
POST LIKE          0
POST AMBIGUOUS     0

NEW_REGRESSION_OUTSIDE_TARGET 0
```

El unico `FAIL_CLOSED` es UAIIN, por `NO_SECOP_EVIDENCE`: sigue sin comprador
fisico ni NIT atribuible, y su `RESOLUTION_BLOCK` de R2F-A se conserva. El
33/34 no se persiguio como meta; salio de que las ocho tenian identidad
acreditada. Si alguna hubiera fallado la evidencia viva, habria quedado
cerrada.

## Procedencia (schema 2)

El sidecar pasa de cinco a nueve bindings. Los cinco anteriores conservan su
`evidence` byte-for-byte: DAPRE sigue acreditado por
`source_name_projection_v1.json` y los cuatro del piloto por
`public_university_pilot_r2fa.json`. Los cuatro nuevos apuntan al artefacto de
este lote.

Cuatro tests de lotes anteriores afirmaban `len(bindings) == 5`. Ese assert no
es un oraculo de procedencia sino un censo global escrito desde un lote, y
caduca con cada lote siguiente. Se reescribieron como afirmaciones de
**pertenencia**, que es lo que esos tests querian proteger: los bindings
conocidos siguen ahi con su artefacto, ninguno ajeno se adjudica un artefacto
que no lo acredita, y los cuatro splits que V1 dejo en `REVIEW` —Soacha, Yopal,
FFDS y UAESP— siguen sin binding. La reescritura se verifico por mutacion:
readjudicar DAPRE al piloto, readjudicar un binding del piloto a este lote,
borrar un binding y proyectar Soacha fallan los cuatro.

## Verificacion

| Gate | Resultado |
|---|---|
| R2F-B + R2F-A + ADR-073 + R2E.1A/R2E.1/R2E/R2D.3/R1C/R1D/R2A + CAR + nacional + SENA/ICBF | 902 passed |
| Query corpus | 89/89 PASS, 0 FAIL |
| Ruff | PASS (27 hallazgos preexistentes en `.hermes/` y `docs/audits/evidence/`) |
| `make lint-core` | PASS |
| Suite completa | 10.720 passed, 2 skipped, 15 xfailed |
| Nuevos xfails | 0 |

**Modo de ejecucion LLM.** `DEGRADED_REGEX_ONLY`. El proveedor devuelve
`HTTP 402 Insufficient Balance`. Este lote no toca parser, LLM ni follow-ups,
asi que la suite es un gate de regresion valido para el, pero
`LLM_PROVIDER_LIVE_COVERAGE: NOT_VERIFIED`: no se puede afirmar cobertura
conversacional viva.

## Deuda

**Cerrada:** las ocho universidades publicas que R2F-A dejo abiertas resuelven
exacto. El universo de universidades publicas SECOP queda en 33/34, con el
unico cierre restante respaldado por ausencia de evidencia.

**Nueva:** la variante con punto del Choco queda sin acreditar hasta que aparezca en una fuente de proyeccion. Ninguna deuda de colision, scan, runtime ni xfail. Los tres homonimos
registrales sin actividad quedan documentados: si alguno empezara a publicar,
dejaria de ser ruido y el scan de esos dos aliases tendria que revisarse.

**No tocada:** UAIIN salvo replay, privadas, Buyer Groups, CGR, judicial,
defensa, parser, LLM, follow-ups, geografia, Radar, Actor y SODA3.

Evidencia reproducible en
[`public_university_completion_r2fb.json`](../../artifacts/audits/public_university_completion_r2fb.json),
generada por
[`audit_public_university_completion_r2fb.py`](../../scripts/audits/audit_public_university_completion_r2fb.py).
Oraculos en `tests/test_public_university_completion_r2fb.py`.

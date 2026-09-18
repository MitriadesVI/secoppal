# Cobertura ADR-073 para los compradores del dogfood

**Fecha de observación:** 2026-09-17 · **Dominio:** `www.datos.gov.co`
**Artefacto:** `artifacts/audits/dogfood_buyer_coverage_adr073.json`
**Reproductor:** `scripts/audits/audit_dogfood_buyer_coverage_adr073.py` (read-only)

Sin cambios de runtime. `SoQLBuilder` y `buyer_source_names.py` quedan
byte-idénticos: lo único que cambia es el sidecar de datos.

## Por qué

`project_buyer_names` devuelve `None` sin binding, y el builder cae entonces a
`unaccent(UPPER(campo)) LIKE '%…%'`. Ninguno de los compradores del dogfood
tenía binding, así que todas sus consultas salían por el fallback. El incidente
DOGFOOD-001 mostró el síntoma —dos jobs de proveedores de Soledad expirando— y
su corrección devolvió al carril PROFILE los 8 s de socket que le correspondían,
pero **no** tocó la causa: la consulta misma.

## Orden de acreditación

El artefacto guarda cada paso, de modo que ningún nombre físico entra «porque
parece correcto»:

```
canónico del resolver
  → NIT que el dataset asocia a ESE nombre exacto (sin fuzzy)
  → TODAS las formas físicas que ese NIT publica
  → qué universo devuelve HOY el fallback LIKE, con sus NITs
  → LIKE contra binding exacto: mismas filas, misma latencia medida
```

Los predicados no se reconstruyen: se piden a `SoQLBuilder`, que es quien los
emite en producción. Reconstruirlos habría medido una consulta que el producto
nunca manda.

## Identidades acreditadas

| canónico | NIT | procesos | contratos |
|---|---|---|---|
| `MUNICIPIO DE SOLEDAD` | 890106291 | 10.302 | 12.087 |
| `MUNICIPIO DE GALAPA.` | 890102472 | 4.460 | 4.314 |
| `DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA` | 890102018 | 17.166 | 49.061 |
| `ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS` | 890480184 | 49.678 | 48.301 |

El NIT coincide entre datasets en los cuatro, y el canónico del resolver es una
forma física viva en ambos: no hubo que elegir nada. `MUNICIPIO DE GALAPA.`
lleva punto final y se congela con él — puntuación, acentos y mayúsculas son
parte del binding, no ruido a normalizar.

## Medición

Mediana de 3 repeticiones por celda, `count(*)` sobre el predicado real.

| canónico | dataset | LIKE (ms) | exacto (ms) | | filas |
|---|---|---|---|---|---|
| MUNICIPIO DE SOLEDAD | procesos | 4.344 | 1.013 | 4,3× | 10.302 |
| MUNICIPIO DE SOLEDAD | contratos | 6.614 | 980 | 6,8× | 12.087 |
| MUNICIPIO DE GALAPA. | procesos | 3.804 | 738 | 5,2× | 4.460 |
| MUNICIPIO DE GALAPA. | contratos | 3.291 | 674 | 4,9× | 4.314 |
| BARRANQUILLA | procesos | 3.734 | 773 | 4,8× | 17.166 |
| BARRANQUILLA | contratos | 3.933 | 870 | 4,5× | 49.061 |
| CARTAGENA | procesos | 5.188 | 776 | 6,7× | 49.678 |
| **CARTAGENA** | **contratos** | **17.429** | **1.132** | **15,4×** | 48.301 |

**Mismo universo en las ocho celdas**, y ningún NIT ajeno alcanzado por el
fallback. Cartagena por contratos tardaba más que el deadline completo del turno
(17,4 s contra 15 s): ninguna corrección de presupuesto podía salvar esa
consulta. Soledad emite DOS jobs de proveedores a la vez, cada uno a ~6,6 s: por
eso 6 s de socket no alcanzaban y 8 s apenas.

Un intento previo de medir con tope de 90 s murió por timeout de lectura. El
reproductor ahora registra eso como medición y deja `same_universe` en `None`,
nunca en `True`: un LIKE que no contestó no acredita equivalencia.

## Cartagena: cuatro nombres, un NIT

Bajo `890480184` publican también:

```
DEPARTAMENTO ADMINISTRATIVO DE TRÁNSITO Y TRANSPORTE DATT
ALCALDIA DE LA LOCALIDAD INDUSTRIAL Y DE LA BAHIA
ALCALDÍA DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS - FONDO ESPECIAL DE BOMBEROS
```

Son cuerpos distintos, no variantes ortográficas del mismo. El fallback **no
alcanza ninguno** —el del Fondo ni siquiera lleva el «DEL» del canónico—, así
que el binding tampoco: quedan **declarados en el artefacto y fuera del
binding**. Incorporarlos ampliaría el universo en silencio, que es una decisión
de producto (`BUYER_GROUP`), no de proyección de nombres. Es el mismo patrón que
ADR-073 ya declaró para la CGR.

## Fuera de la cohorte

**MUNICIPIO DE MALAMBO.** No tiene identidad canónica y por tanto no hay clave
para un binding:

```
'municipio de malambo'  → None, rewrite_unresolved, low
'malambo'               → ESE HOSPITAL LOCAL DE MALAMBO SANTA MARÍA MAGDALENA, fuzzy(90), medium
```

«Malambo» a secas se lo lleva hoy el hospital por fuzzy sin puerta. Eso es deuda
de gazetteer/identidad (familia M1A), no de ADR-073: escribir aquí un nombre
físico para Malambo sería exactamente lo que esta capa prohíbe.

## Verificación

`tests/test_dogfood_buyer_coverage_adr073.py` ata cada binding a su artefacto:
identidad, formas por dataset, equivalencia medida (`is True`, nunca `None`),
ausencia de NITs ajenos, y que el binding **no exceda** lo que el fallback ya
alcanzaba. Cuatro mutaciones lo confirman: absorber DATT en el binding, apuntar
la evidencia a otro lote, declarar equivalencia sin medirla y fingir un NIT
ajeno matan cada una su caso.

Consumidores del sidecar y del builder: 793 pasan, 0 fallan.

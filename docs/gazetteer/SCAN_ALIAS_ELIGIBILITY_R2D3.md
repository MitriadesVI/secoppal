# SCAN-ALIAS-ELIGIBILITY-R2D3 — exactitud y elegibilidad de scan

**Tipo:** discovery y remediación · **Fecha:** 2026-09-01

**Rama:** `main` · **HEAD de discovery:** `db8329802339d672ee15b01dac7c98ecbb4b8720`

**Evidencia de entrada:**
[`freed_alias_safety_r2d2.json`](../../artifacts/audits/freed_alias_safety_r2d2.json)

## 1. Hipótesis medida

R2D.2 demostró que un mismo string ejercía dos autoridades distintas:

```text
ALIAS_OWNERSHIP != SCAN_ALIAS_ELIGIBILITY
```

Los nueve aliases confirmados siguen resolviendo por Tier 1 al owner que figura
en el gazetteer. Los nueve también entran a `_scan_index`, y allí capturan 17
compradores físicos distintos. La hipótesis exacto-vs-scan queda confirmada,
pero no autoriza a conservar los nueve en exacto: esa pregunta se decidió
individualmente con el owner, la forma física SECOP, los compradores ajenos y
la semántica de la frase.

## 2. Discovery por alias

| Alias | Owner actual | Tokens | Cabeza institucional | Location-like | Compradores ajenos | Resultado exacto PRE | Decisión |
|---|---|---:|---|---|---:|---|---|
| `candelaria valle` | ALCALDÍA MUNICIPAL DE CANDELARIA VALLE | 2 | no | sí | 3 | owner actual · `exact` | `KEEP_EXACT_DISABLE_SCAN` |
| `ciudad bolivar` | ALCALDIA LOCAL DE CIUDAD BOLIVAR // | 2 | no | sí | 11 | owner actual · `exact` | `REMOVE_ALIAS_COMPLETELY` |
| `corporacion quindio` | CORPORACION AUTONOMA REGIONAL DEL QUINDIO | 2 | `corporacion` | no | 1 | owner actual · `exact` | `REMOVE_ALIAS_COMPLETELY` |
| `francisco jose de caldas` | UNIVERSIDAD DISTRITAL FRANCISCO JOSE DE CALDAS | 4 | no | no | 36 | owner actual · `exact` | `REMOVE_ALIAS_COMPLETELY` |
| `puerto boyaca` | ALCALDIA MUNICIPAL DE PUERTO BOYACA | 2 | no | sí | 9 | owner actual · `exact` | `KEEP_EXACT_DISABLE_SCAN` |
| `salud arauca` | UNIDAD ADMINISTRATIVA ESPECIAL DE SALUD DE ARAUCA | 2 | no | no | 1 | owner actual · `exact` | `REMOVE_ALIAS_COMPLETELY` |
| `salud narino` | INSTITUTO DEPARTAMENTAL DE SALUD DE NARIÑO | 2 | no | no | 1 | owner actual · `exact` | `REMOVE_ALIAS_COMPLETELY` |
| `santa marta magdalena` | ALCALDÍA DISTRITAL DE SANTA MARTA | 3 | no | sí | 0 literal + canario | owner actual · `exact` | `REMOVE_ALIAS_COMPLETELY` |
| `soledad atlantico` | MUNICIPIO DE SOLEDAD | 2 | no | sí | 2 | owner actual · `exact` | `KEEP_EXACT_DISABLE_SCAN` |

`CURRENT_SCAN_HIJACKS` es el conjunto congelado de 17 compradores de §3. En
todos, el scan PRE devolvía el owner actual mediante el alias de la tabla.

### 2.1 Por qué exacto sí en tres

`candelaria valle`, `puerto boyaca` y `soledad atlantico` son formas humanas
ciudad/municipio + departamento. Escritas solas, identifican razonablemente la
administración municipal registrada. Dentro del nombre de un hospital, un
concejo, una personería, una institución educativa o el SENA, sólo expresan
jurisdicción: no identifican a la alcaldía. Por eso conservan ownership exacto
y pierden scan.

### 2.2 Por qué exacto no en seis

- `ciudad bolivar` no distingue la localidad 19 de Bogotá del municipio
  homónimo de Antioquia; el directorio contiene ambas alcaldías.
- `corporacion quindio` es también el comienzo literal de CORPORACION QUINDIO
  CONVENTION BUREAU. Falta `autonoma regional` o la sigla `CRQ`.
- `francisco jose de caldas` es un nombre propio compartido por al menos 36
  compradores físicos, no una identificación de la Universidad Distrital.
- `salud arauca` y `salud narino` expresan función + territorio y no distinguen
  instituto/unidad administrativa de una unidad prestadora.
- `santa marta magdalena` es ciudad + departamento, sin cabeza institucional.

Retirar estos aliases del ownership no basta por sí solo si el fuzzy vuelve a
fabricar el mismo dueño. La remediación debe bloquear esas seis formas como
referencias completas antes del fuzzy, con metadata declarativa y resultado
fail-closed.

## 3. Los 17 compradores físicos secuestrados

| Alias | Comprador físico SECOP distinto | NIT publicado |
|---|---|---|
| `candelaria valle` | INSTITUTO MUNICIPAL DEL DEPORTE Y LA RECREACION DE CANDELARIA VALLE | 81500395 |
| `ciudad bolivar` | ASOCIACION DE JUNTAS DE ACCION COMUNAL DE LA LOCALIDAD 19, CIUDAD BOLIVAR | 900072146 |
| `ciudad bolivar` | COLEGIO CEDID CIUDAD BOLIVAR INSTITUCION EDUCATIVA DISTRITAL | 830000758 |
| `ciudad bolivar` | COLEGIO CIUDAD BOLIVAR ARGENTINA IED | 830066973 |
| `ciudad bolivar` | JUNTA DE ACCION COMUNAL DEL BARRIO SUBNORMAL LA PLAYA DE LA LOCALIDAD 19 CIUDAD BOLIVAR | 830091314 |
| `corporacion quindio` | CORPORACION QUINDIO CONVENTION BUREAU | 901068969 |
| `francisco jose de caldas` | CENTRO EDUCATIVO FRANCISCO JOSE DE CALDAS | 901004821 |
| `puerto boyaca` | CAMARA DE COMERCIO DE LA DORADA, PUERTO BOYACA, PUERTO SALGAR Y ORIENTE DE CALDAS | 8908011567 |
| `puerto boyaca` | CONCEJO MUNICIPAL DE PUERTO BOYACA | 820000370 |
| `puerto boyaca` | ESTABLECIMIENTO PENITENCIARIO DE MEDIANA SEGURIDAD Y CARCELARIO DE PUERTO BOYACA | 820000992 |
| `puerto boyaca` | INSTITUTO MUNICIPAL DE DEPORTE Y RECREACION DE PUERTO BOYACA | 820000484 |
| `puerto boyaca` | PERSONERIA MUNICIPAL DE PUERTO BOYACA | 800255019 |
| `puerto boyaca` | SENA PUERTO BOYACA | 899999034 |
| `salud arauca` | UNIDAD PRESTADORA DE SALUD ARAUCA | 9008012094 |
| `salud narino` | UNIDAD PRESTADORA DE SALUD NARIÑO | 900807338 |
| `santa marta magdalena` | CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA. | 891701694 |
| `soledad atlantico` | INSTITUCION EDUCATIVA POLICARPA SALAVARRIETA SOLEDAD ATLANTICO | 802003453 |

## 4. Alternativas

### A. Política derivada para los 2.104

Descartada. El runtime no posee la evidencia de compradores físicos ajenos que
hizo decisivos estos nueve casos. Reglas como «dos tokens», «sin cabeza» o
«ciudad + departamento» también expulsarían regionales SENA/ICBF y otros
aliases buenos que D6 acaba de liberar. R2D.3 no amplía su universo por
intuición.

### B. Metadata explícita de scan

Elegida para los tres aliases buenos en exacto. El registro conserva el alias
en `aliases` y declara la misma clave normalizada en
`scan_disabled_aliases`. El loader mantiene Tier 1 y la omite al construir
`_scan_index`.

### C. Eliminar el alias

Elegida para los otros seis. Se retiran del conjunto `aliases`, se declaran en
`resolution_blocked_aliases` para impedir que fuzzy reabra el owner y tampoco
entran al scan. Las formas institucionales específicas de cada owner siguen
disponibles.

La opción combinada B/C es la mínima que generaliza el principio sin una
heurística global. Como persiste dos autoridades en el schema del gazetteer,
la decisión arquitectónica se registra en ADR-072.

## 5. Terminal Santa Marta

R2D.2 acreditó dos hechos independientes:

1. `santa marta magdalena` no puede capturar una frase mayor para la Alcaldía;
2. CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA. existe como comprador físico
   SECOP II, código `724398862`, NIT base `891701694`, activo en Santa Marta.

R2D.3 retirará el alias territorial y registrará la identidad específica con
aliases que incluyan la cabeza `terminal`/`central`. El match específico debe
ganar por longitud, pero la seguridad del resto no dependerá de longest-match.

## 6. Baseline antes de remediar

```text
ALIASES_AUDITED:                    9
KEEP_EXACT_DISABLE_SCAN:            3
REMOVE_ALIAS_COMPLETELY:            6
ADD_SPECIFIC_IDENTITY:              1 decisión independiente
NEW_WRONG_OWNER_FROM_FREED_ALIASES: 17
ADVERSARIAL_LIVE:
  CORRECT_OWNER:                   14
  WRONG_OWNER:                     36
  FAIL_CLOSED:                      2
PREEXISTING_WRONG_OWNER:            19
```

Los resultados POST, gates de ownership, mutaciones y validación se añadirán
sin reescribir esta evidencia PRE.

---

## 7. POST_REMEDIATION

**HEAD auditado:** `debf2e3489572368ab564cfc6b8decb2f10def6b`

**Commits de implementación:**

```text
1e84e82  docs(audit): classify freed scan aliases
bd7867e  fix(entity): separate exact alias from scan eligibility
debf2e3  feat(gazetteer): add verified Santa Marta transport buyer
```

**Artefacto POST:**
[`scan_alias_eligibility_r2d3.json`](../../artifacts/audits/scan_alias_eligibility_r2d3.json)

### 7.1 Matriz adversarial POST

Replay de las 52 sondas HIGH/MEDIUM congeladas por R2D.2 contra el runtime
actual.

```text
                                     PRE   POST
CORRECT_OWNER                         14     15
WRONG_OWNER                           36     20
FAIL_CLOSED                            2     17

NEW_WRONG_OWNER_FROM_FREED_ALIASES    17      0
PREEXISTING_WRONG_OWNER               19     20
TOTAL                                 52     52
```

El gate del lote es causal, no absoluto: `NEW_WRONG_OWNER = 17 → 0`. Los 20
`WRONG_OWNER` que quedan son deuda subyacente anterior a D6 — ninguno se activa
por un alias liberado, y R2D.3 no los aborda. Los 15 hijacks que se convierten
en `FAIL_CLOSED` son la consecuencia esperada de bloquear seis aliases: el
sistema deja de nombrar un owner equivocado y pide desambiguación.

### 7.2 Las dos autoridades

Núcleo de ADR-072, verificado contra el runtime:

```text
resolve_entidad("puerto boyaca")                      → ALCALDIA MUNICIPAL DE PUERTO BOYACA   (exact)
scan_entity("Concejo Municipal de Puerto Boyacá")     → None

resolve_entidad("candelaria valle")                   → ALCALDÍA MUNICIPAL DE CANDELARIA VALLE (exact)
scan_entity("Hospital de Candelaria Valle")           → None

resolve_entidad("soledad atlantico")                  → MUNICIPIO DE SOLEDAD                   (exact)
scan_entity("Concejo Municipal de Soledad Atlántico") → None
```

La abreviatura municipal sobrevive donde el usuario la escribe como nombre
completo; muere donde sólo era la jurisdicción de otra institución.

### 7.3 Aliases bloqueados

Los seis retirados no reabren por fuzzy:

```text
ciudad bolivar            method=alias_resolution_blocked  confidence=none  clarification=True
corporacion quindio       method=alias_resolution_blocked  confidence=none  clarification=True
francisco jose de caldas  method=alias_resolution_blocked  confidence=none  clarification=True
salud arauca              method=alias_resolution_blocked  confidence=none  clarification=True
salud narino              method=alias_resolution_blocked  confidence=none  clarification=True
santa marta magdalena     method=alias_resolution_blocked  confidence=none  clarification=True
```

`FUZZY_RESURRECTIONS = 0`.

### 7.4 Terminal Santa Marta

```text
IDENTITY:      CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA.
NIT:           891701694    SECOP_CODE: 724398862
```

```text
"Terminal de Transportes de Santa Marta"            → CENTRAL DE TRANSPORTE …  (exact)
"Central de Transporte de Santa Marta"              → CENTRAL DE TRANSPORTE …  (exact)
"Central de Transporte de Santa Marta Ltda"         → CENTRAL DE TRANSPORTE …  (fuzzy 98)
"Terminal de Transportes de Santa Marta Magdalena"  → CENTRAL DE TRANSPORTE …  + departamento Magdalena
```

La frase adversarial de R2D.2 ya no alcanza a la Alcaldía Distrital por ninguna
vía: el alias territorial está bloqueado y la identidad específica gana por
nombre propio, no por longest-match.

Smoke sobre el dataset, con el `WHERE` que emite el builder
(`UPPER(entidad) LIKE UPPER('%…%')`, insensible a mayúsculas):

```text
PROCESS_ROWS   (p6dx-8zbt, por NIT 891701694)   227
PROCESS_ROWS   (p6dx-8zbt, WHERE del runtime)   227
CONTRACT_ROWS  (jbjy-vk9h, por NIT 891701694)     0
```

Los 0 contratos son evidencia conocida del comprador, no una anomalía
`ZERO_ROW`: la identidad está acreditada y los procesos existen y son
alcanzables de punta a punta.

### 7.5 No regresión D6

```text
CAR/CDS por nombre institucional        33/33
SENA/ICBF regionales, fail-closed        0/8
Canarios no-CAR (EAAB, ICCU,
  Teleantioquia, Secretaría General)      4/4
```

`app/core/geography_alias.py` no se modificó en este lote (último cambio:
`cafe9fc`, previo a R2D.3).

### 7.6 Collision gate

```text
EXACT_MULTI_OWNER baseline   173
EXACT_MULTI_OWNER actual     173
NEW_MULTI_OWNER_KEYS           0   → PASS
```

### 7.7 R2D.2 POST-aware

`scripts/audits/audit_freed_alias_safety.py` se hizo POST-aware: reconstruye el
universo D6 sumando los aliases todavía activos y los explícitamente retirados
en `resolution_blocked_aliases`, de modo que la invariante de universo sigue
siendo verificable después de remediar.

```text
FREED_BY_D6 reconstruidos activos          2.098
FREED_BY_D6 retirados por política             6
TOTAL                                      2.104   ✓

NEW_WRONG_OWNER_CONFIRMED                      0
SANTA_MARTA_TERMINAL verdict   REMEDIATED_SPECIFIC_IDENTITY
R2D1_VERDICT                   PASS_CONFIRMED
```

Artefacto de la corrida POST:
[`freed_alias_safety_r2d2_post.json`](../../artifacts/audits/freed_alias_safety_r2d2_post.json).
El artefacto histórico `freed_alias_safety_r2d2.json` no se reescribió.

### 7.8 Validación

```text
tests/test_scan_alias_eligibility_r2d3.py         15 tests PASS
focales del lote (R2D.3 + R2D.1 + CAR R2D)      348 passed
corpus (scripts/run_query_corpus.py --mode all)   89/89 PASS
ruff check app/ tests/ scripts/                   PASS
make lint-core                                    PASS
suite completa                                    10.351 passed · 2 skipped · 15 xfailed · 0 failed
xfails                                            15 — mismos que el baseline post-D6; ninguno nuevo
```

**R2D1_FINAL_VERDICT = PASS_CONFIRMED**

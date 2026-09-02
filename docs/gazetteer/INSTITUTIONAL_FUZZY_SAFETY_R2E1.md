# INSTITUTIONAL-FUZZY-SAFETY-R2E.1 — dos dueños ajenos, dos causas

**Tipo:** discovery + remediación · **Fecha:** 2026-09-01

**Rama:** `main` · **HEAD antes:** `fb205a778d254670d915ab59570740693963e11f`

**Evidencia:**
[`institutional_fuzzy_safety_r2e1.json`](../../artifacts/audits/institutional_fuzzy_safety_r2e1.json)

**Reproductor:**
[`audit_institutional_fuzzy_safety_r2e1.py`](../../scripts/audits/audit_institutional_fuzzy_safety_r2e1.py)

**Autoridad arquitectónica:** ADR-072 —ownership exacto y elegibilidad de scan
son autoridades distintas— y ADR-069 —`INV-NO-SILENT-SEMANTIC-FALLBACK`—.
R2E.1 no crea ADR nuevo: aplica los dos que ya existen a un tercer mecanismo.

## 1. Veredicto

```text
INSTITUTIONAL_FUZZY_SAFETY_R2E1: PASS

BANREP_CROSS_OWNER                     2 → 0
CONTROL_TERRITORIAL_WRONG_OWNER       14 → 0
BANREP_POSITIVE_LOST                   0 → 0
NATIONAL_SIX_OK                      6/6 → 6/6
PARENT_RELATED_OK                    4/4 → 4/4
TERRITORIAL_SCAN_HIJACKS               0 → 0
LITERAL_PROJECTION_REACHES_OWN_BUYER      13/14
NEW_MULTI_OWNER_KEYS                       0
```

Las dos causas se remediaron por separado, con un commit cada una. No hay una
regla única detrás.

## 2. El PRE se mide, no se cita

`--baseline` reconstruye el estado anterior **contra el código vivo**: devuelve
el alias retirado al registro en memoria y neutraliza la puerta del fuzzy. Así
la matriz PRE es una medición reproducible, no una transcripción de R2E.

```bash
python scripts/audits/audit_institutional_fuzzy_safety_r2e1.py \
    --label PRE --baseline --skip-network --output /tmp/r2e1_pre.json
python scripts/audits/audit_institutional_fuzzy_safety_r2e1.py --label POST
```

El artefacto POST lleva la matriz PRE completa bajo la clave `pre`.

## 3. Caso A — `la república` no nombra al Banco

### 3.1 Por qué vía ocurría el secuestro

Las dos, y por mecanismos distintos:

| Frase | Vía | Alias | Owner PRE |
|---|---|---|---|
| `resolve_entidad("Presidencia de la República")` | `fuzzy(90)` | `la republica` | BANCO DE LA REPUBLICA |
| `scan_entity("contratos de Presidencia de la República")` | `scan_exact` | `la republica` | BANCO DE LA REPUBLICA |

En el fuzzy el alias empataba a 90.0 con las dos formas propias del DAPRE y
ganaba por orden de índice; en el scan capturaba directamente. Un mismo alias
sobre-genérico alimentando dos mecanismos: exactamente la asimetría que ADR-072
describe, esta vez sin que ninguna de las dos vías fuera segura.

### 3.2 La pregunta que decide

*¿`la república`, escrita sola, identifica razonablemente al Banco de la
República?* La respuesta es **no**, y no por estética: la frase no contiene
cabeza institucional —el `banco` es lo que identifica al emisor— y «República»
es componente de una decena larga de instituciones nacionales.

Cruce por secuencia completa de tokens contra el directorio oficial
[`4ex9-j3n8`](https://www.datos.gov.co/resource/4ex9-j3n8.json)
(`es_entidad='Si'`, 27.286 compradores):

```text
la república    53 nombres físicos · 16 NIT publicados
```

| NIT publicado | Nombre físico |
|---|---|
| `860005216` | BANCO DE LA REPUBLICA |
| `8999990839` | PRESIDENCIA DE LA REPÚBLICA |
| `899999067` | CONTRALORIA GENERAL DE LA REPUBLICA |
| `830065741` | AUDITORIA GENERAL DE LA REPUBLICA |
| `899999103` | CONGRESO DE LA REPÚBLICA - HONORABLE SENADO DE LA REPÚBLICA |
| `8999991031` | SENADO DE LA REPUBLICA |
| `8999990980` | CONGRESO DE LA REPUBLICA DE COLOMBIA CAMARA DE REPRESENTANTES |
| `800252683` | FONDO DE BIENESTAR SOCIAL DE LA CONTRALORIA GENERAL DE LA REPUBLICA |
| `899999734` | FONDO DE PREVISION SOCIAL DEL CONGRESO DE LA REPUBLICA |
| `8999990830` | FONDO DE PROGRAMAS ESPECIALES … PRESIDENCIA DE LA REPÚBLICA (FONDO PAZ) |
| `900272394` | INSTITUCIÓN EDUCATIVA BICENTENARIO DE LA INDEPENDENCIA DE LA REPÚBLICA DE COLOMBIA |
| … | (5 NIT más, entre ellos duplicados registrales con cédula) |

El Banco es **uno** de dieciséis. La decisión es `REMOVE_ALIAS_COMPLETELY`.

### 3.3 Por qué además se bloquea

Retirar el alias no basta. Sin él, `resolve_entidad("la República")` no queda
en silencio: el fuzzy global lo entrega al `INSTITUTO COLOMBIANO AGROPECUARIO -
ICA` con puntaje 90. Un dueño ajeno nuevo, más difícil de detectar que el
anterior porque nadie lo espera.

Por eso la frase pasa a `resolution_blocked_aliases`: `resolve_entidad` falla
cerrado **antes** del fuzzy y pide precisión. No es conservar el alias en otra
lista —no queda en `scan_disabled_aliases`, porque tampoco es segura en
exacto—: es dejar la evidencia negativa visible en datos, como ADR-072 previó.

```json
{
  "official_name": "BANCO DE LA REPUBLICA",
  "aliases": ["banco de la república", "banrep", "banco central de colombia", "…"],
  "resolution_blocked_aliases": ["la república"]
}
```

### 3.4 Banco adversarial

`BANREP_CROSS_OWNER = 0`. Ninguna institución que lleve «de la República»
termina en el Banco:

| Frase | PRE | POST |
|---|---|---|
| Presidencia de la República | BANCO DE LA REPUBLICA `fuzzy(90)` | DAPRE `fuzzy(90)` |
| Vicepresidencia de la República | BANCO DE LA REPUBLICA `fuzzy(90)` | `None` (`like`) |
| Auditoría General de la República | AGR `exact` | AGR `exact` |
| Contraloría General de la República | CGR `exact` | CGR `exact` |
| Senado de la República | CONGRESO/SENADO `exact` | CONGRESO/SENADO `exact` |
| Congreso de la República | CONGRESO/SENADO `exact` | CONGRESO/SENADO `exact` |
| Cámara de Representantes | CÁMARA `exact` | CÁMARA `exact` |
| Fondo de Bienestar Social de la CGR | FBS `exact` | FBS `exact` |

«Presidencia de la República» aterriza en su **propia identidad**: el NIT
`899999083` publica «PRESIDENCIA DE LA REPÚBLICA» y «DAPRE» como nombres
físicos del mismo comprador. Resolver a la ficha del DAPRE no es un
compromiso; es el mismo NIT. Sobre la proyección de esa ficha, §6.

«Vicepresidencia de la República» no existe como comprador con NIT propio en el
directorio: sin identidad que acreditar, el resolver deja de afirmar una y cae
al filtro literal.

### 3.5 Canarios positivos

`BANREP_POSITIVE_LOST = 0`. Las ocho formas que sí nombran al Banco siguen
resolviendo por `exact`:

```text
Banco de la República · Banrep · Banco Central de Colombia · Banco emisor
BR · banrepública · el emisor · Banco de la República de Colombia
```

`BR` conserva ownership exacto y sigue **fuera** del índice de scan, igual que
antes: dos caracteres dentro de texto libre no acreditan a nadie. Se verificó
por separado en las dos vías, porque el ownership exacto y la elegibilidad de
scan no se derivan el uno del otro.

## 4. Caso B — el fuzzy no puede rellenar una identidad ausente

### 4.1 La hipótesis del enunciado se quedó corta

R2E dejó anotado que «Contraloría Departamental de Bolívar» caía en la CGR. La
medición sobre un banco de catorce contralorías territoriales muestra que la
raíz nacional es **una** de cuatro familias de dueño ajeno, no la causa:

| Familia del dueño ajeno PRE | Casos |
|---|---:|
| Raíz nacional (`CGR-NIVEL CENTRAL`) | 5 |
| Gobernación / departamento | 5 |
| Municipio | 2 |
| Otra contraloría territorial (`CONTRALORIA DE BOGOTA`) | 1 |
| Secretaría distrital ajena | 1 |

Una puerta que sólo prohibiera «territorial pedido → raíz nacional» habría
puesto el contador en 0 dejando nueve dueños ajenos vivos. Habría sido una
puerta cosmética.

### 4.2 La causa medida

Todos los casos —los catorce territoriales y los dos del Banco— comparten un
mecanismo: **`WRatio` puntúa 90 la contención parcial**, y en la dirección
peligrosa. El candidato ganador era un *fragmento* de lo que el usuario
escribió:

```text
"contraloria departamental de bolivar"   ← ganó el alias 'contraloria'          (CGR)
"contraloria general de caldas"          ← ganó el alias 'contraloria general'  (CGR)
"contraloria departamental del huila"    ← ganó el alias 'huila'                (Gobernación)
"contraloria municipal de bucaramanga"   ← ganó el alias 'bucaramanga'          (Municipio)
"contraloria distrital de cartagena"     ← ganó el alias 'contraloria distrital'(C. de Bogotá)
"presidencia de la republica"            ← ganó el alias 'la republica'         (Banrep)
```

Es la semántica de `scan_entity` —«cualquier aparición del alias identifica al
dueño»— colada en Tier 3. ADR-072 ya la había falsificado para el scan; el
fuzzy la seguía aplicando porque `scan_disabled_aliases` sólo gobierna el
`_scan_index`.

La dirección contraria sí es legítima: cuando la frase del usuario está
contenida en el alias —«Banrep» dentro del nombre del emisor— el parecido
parcial expresa una abreviatura, no una pérdida.

### 4.3 La regla, y por qué no enumera instituciones

Se descartó, medida contra el banco, la reescritura estricta por familia
«contraloría»: enumerar cabezas institucionales en runtime es el antipatrón que
este repo ya identificó —vocabulario cerrado más fail-open— y no cubre las
gobernaciones ni los municipios que aparecieron en la medición.

La regla implementada es una puerta sobre los tiers **inferenciales**:

> Un candidato del fuzzy debe conservar la **cabeza** que el usuario escribió y
> cada **territorio** que el usuario nombró. Si no queda ningún candidato que
> los conserve, el resolver no afirma identidad.

`app/core/fuzzy_identity_guard.py` — función pura, sin nombres de instituciones
y sin catálogo propio: recibe el catálogo territorial de quien la llama
(`DEPARTAMENTOS` ∪ `GAZETTEER_CIUDADES`, las autoridades que ya existen) y
opera sobre tokens. El alias que hizo el match y el nombre físico cuentan
**juntos** como evidencia: una ficha puede llevar la cabeza en el alias y el
territorio en el nombre oficial.

El enganche son dos líneas, en Tier 2 y Tier 3 de `resolve_entidad`. **Tier 1
no la cruza.** El ownership exacto es autoridad declarada, no inferencia: si la
puerta actuara sobre él, `BR` —que no comparte un solo token con «BANCO DE LA
REPUBLICA»— dejaría de resolver. Hay un test que lo fija.

### 4.4 Banco adversarial territorial

Catorce contralorías territoriales, cada una verificada por NIT contra
`p6dx-8zbt` / `jbjy-vk9h`. Ninguna está en el gazetteer: R2E.1 **no** añade
cobertura territorial.

| Consulta natural | NIT | Nombre físico SECOP | P/C | PRE | POST |
|---|---|---|---:|---|---|
| Contraloría Departamental de Bolívar | `890480306` | CONTRALORIA DEPARTAMENTAL DE BOLIVAR | 126/100 | CGR | `FAIL_CLOSED_LITERAL` |
| Contraloría General de Caldas | `800077897` | CONTRALORIA GENERAL DE CALDAS | 182/148 | CGR | `FAIL_CLOSED_LITERAL` |
| Contraloría General de Santander | `890201705` | CONTRALORIA GENERAL DE SANTANDER | 108/92 | DEPARTAMENTO DE SANTANDER | `FAIL_CLOSED_LITERAL` |
| Contraloría Municipal de Ibagué | `890706867` | CONTRALORIA MUNICIPAL DE IBAGUE | 622/619 | CGR | `FAIL_CLOSED_LITERAL` |
| Contraloría General de Medellín | `811026988` | CONTRALORÍA DISTRITAL DE MEDELLÍN | 375/321 | CGR | `FAIL_CLOSED_LITERAL` |
| Contraloría Departamental del Huila | `800169790` | CONTRALORIA DEPARTAMENTAL DEL HUILA | 340/253 | GOBERNACION DEL HUILA* | `FAIL_CLOSED_LITERAL` |
| Contraloría Municipal de Pasto | `800156682` | CONTRALORÍA MUNICIPAL DE PASTO | 125/118 | CGR | `FAIL_CLOSED_LITERAL` |
| Contraloría Departamental del Atlántico | `890103037` | CONTRALORIA DEPARTAMENTAL DEL ATLANTICO | 161/130 | DEPARTAMENTO DEL ATLANTICO** | `FAIL_CLOSED_LITERAL` |
| Contraloría Distrital de Cartagena | `800194000` | CONTRALORIA DISTRITAL DE CARTAGENA DE INDIAS | 651/639 | CONTRALORIA DE BOGOTA | `FAIL_CLOSED_LITERAL` |
| Contraloría Municipal de Bucaramanga | `890211142` | CONTRALORIA MUNICIPAL DE BUCARAMANGA | 65/63 | MUNICIPIO DE BUCARAMANGA | `FAIL_CLOSED_LITERAL` |
| Contraloría Departamental del Valle del Cauca | `800090735` | CONTRALORIA DEPARTAMENTAL DEL VALLE DEL CAUCA | 269/233 | GOBERNACION DEL VALLE … | `FAIL_CLOSED_LITERAL` |
| Contraloría Distrital de Barranquilla | `800022773` | CONTRALORIA DISTRITAL DE BARRANQUILLA | 89/63 | CONTRALORIA DE BOGOTA | `FAIL_CLOSED_LITERAL` |
| Contraloría Departamental de Nariño | `800157830` | CONTRALORIA DEPARTAMENTAL DE NARIÑO | 161/213 | GOBERNACION DE NARIÑO | `FAIL_CLOSED_LITERAL` |
| Contraloría Municipal de Neiva | `800095147` | CONTRALORIA MUNICIPAL DE NEIVA. | 132/125 | MUNICIPIO DE NEIVA | `FAIL_CLOSED_LITERAL` |

La Contraloría de Bogotá conserva su identidad propia por alias exacto:
`resolve_entidad("Contraloría de Bogotá")` → `CONTRALORIA DE BOGOTA` por
`exact`, antes y después.

### 4.5 Qué significa `FAIL_CLOSED_LITERAL`

No es silencio. El resolver deja de **afirmar** un dueño y cae al Tier 4, que
filtra por las palabras del propio usuario:
`unaccent(UPPER(entidad)) LIKE unaccent(UPPER('%CONTRALORIA DEPARTAMENTAL DE BOLIVAR%'))`.

```text
LITERAL_PROJECTION_REACHES_OWN_BUYER   13/14
```

Trece de las catorce alcanzan a su comprador real sin que el gazetteer los
conozca. La equivalencia `Ñ → N` de `unaccent` se verificó en vivo: la consulta
literal de Nariño devuelve los 161 procesos del NIT `800157830`.

La excepción es Medellín: SECOP publica `CONTRALORÍA DISTRITAL DE MEDELLÍN` y
la consulta natural dice «General» —así aparece también en el directorio de
compradores—. El filtro literal devuelve cero filas. Cero filas es una respuesta
vacía honesta; atribuirle a esa consulta los 5.654 procesos de la CGR no lo era.

## 5. Preservación de R2E

Medido en el mismo artefacto, no asumido:

```text
NATIONAL_6            6/6 PASS_EXACT   (CGR · AGR · PGN · Defensoría · FBS · IEMP)
PARENT_RELATED        4/4
TERRITORIAL_SCAN      0 secuestros / 8 textos
NEW_MULTI_OWNER_KEYS  0
NEW_SCAN_CROSS_OWNER  0   (no se añadió ningún alias)
```

La política exacto/scan de ADR-072 queda intacta: R2E.1 no toca
`scan_alias_policy.py`, no mueve ninguna clave entre `aliases` y
`scan_disabled_aliases`, y no añade aliases nuevos a ningún registro.

## 6. Deuda

**Cerrada**

- `BANREP_LA_REPUBLICA_OVERGENERIC_ALIAS_DEBT` (R2E §9) — §3.
- `TERRITORIAL_CONTROL_FUZZY_FAIL_OPEN_DEBT` (R2E §9) — §4.

**Nueva**

- `DAPRE_STALE_PHYSICAL_NAME_DEBT` — la ficha
  `DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA` guarda 16.817
  procesos, pero hoy el NIT `899999083` publica un solo nombre físico: `DAPRE`.
  La consulta por el nombre largo devuelve **cero filas** en `p6dx-8zbt` y en
  `jbjy-vk9h`; SECOP renombró al comprador después de construirse el gazetteer.
  Es preexistente y afecta a toda consulta por el DAPRE, no sólo a
  «Presidencia»: R2E.1 corrige a qué identidad apunta la frase, no cómo se
  proyecta esa ficha. Pertenece a la familia `CANONICAL_ENTITY_SOURCE_NAMES` y
  se cierra con el modelo de nombres físicos, no con un alias.
- `PRESIDENCIA_SCAN_NOT_CAPTURED` — `scan_entity` ya no secuestra la frase
  «Presidencia de la República» dentro de texto libre, pero tampoco la captura:
  el DAPRE sólo tiene la forma `presidencia de la república departamento
  administrativo`. Añadir la forma corta tiene cross-owner 0 —los dos nombres
  físicos que la contienen comparten el NIT `899999083`—, pero es cobertura
  nueva y no un secuestro: se documenta, no se ejecuta aquí.

**Intacta, por decisión explícita**

- `CGR_PHYSICAL_NAME_FRAGMENTATION_DEBT` — las 33 formas físicas bajo el NIT
  `899999067` siguen sin cubrir. Requiere Buyer Group / modelo de nombres de
  origen, prohibido en este lote.
- `REGISTRY_IDENTITY_NOISE` — los registros con cédula y cero actividad siguen
  fuera. No se limpiaron.
- `CANONICAL_ENTITY_SOURCE_NAMES`, `SECOP_SYNTHETIC_URL_PROVENANCE_DEBT` y el
  fuzzy sin puerta en hospital/ESE/universidad heredado de R2D.

## 7. Corrección documental

`CONTROL_NATIONAL_R2E.md` §9 listaba `BUYER_CAPTURE_TRUNCATION` como deuda
intacta. La autoridad histórica dice otra cosa:
[`GOVERNORATE_CORE_R2A_1.md`](GOVERNORATE_CORE_R2A_1.md) §7 la declara `CLOSED`
y [`GOVERNORATE_CORE_R2A.md`](GOVERNORATE_CORE_R2A.md) §9 lo registra así. Era
arrastre de redacción. Corregido; ninguna deuda cerrada se revive.

## 8. Validación

```text
tests/test_institutional_fuzzy_safety_r2e1.py   93 tests
                                               PRE: 40 rojos · 53 verdes
                                               POST: 93 verdes
focales entidad/gazetteer (15 archivos)        763 passed
corpus (scripts/run_query_corpus.py --mode all)  89/89 PASS
ruff check app/ tests/ scripts/                  PASS
make lint-core                                   PASS
suite completa            10.516 passed · 2 skipped · 15 xfailed · 0 failed
xfails                    15 — los mismos del baseline post-R2E; ninguno nuevo
```

Los 40 rojos previos a la mutación son la congelación del PRE: 9 del alias
sobre-genérico, 28 del control territorial, 3 del módulo que aún no existía.

`semgrep --config .semgrep/secoppal-laws.yml` **no se corrió**: el binario no
está instalado en este entorno. La ley que vigila —`Settings()` fuera de pytest
sin redirigir la persistencia— no aplica a este lote: ni el auditor ni los tests
construyen `Settings` ni `SecopalWorkflow`; sólo `EntityResolver`, que recibe la
ruta del gazetteer por argumento.

## 9. Límites

- El cruce de seguridad usa el directorio oficial `4ex9-j3n8` filtrado por
  `es_entidad='Si'` (27.286 compradores) y contención por secuencia completa de
  tokens. Un comprador ausente del directorio no queda medido.
- El banco territorial son catorce contralorías, no todas: R2E.1 mide una
  muestra suficiente para separar causas, no cobertura.
- La puerta del fuzzy se validó contra el banco adversarial, los focales de
  entidad, el corpus y la suite completa. No hay medición de su efecto sobre
  consultas fuera de ese conjunto.
- No se tocaron universidades, personerías, Buyer Groups, las gerencias de la
  CGR, SODA3, Radar ni Actor.

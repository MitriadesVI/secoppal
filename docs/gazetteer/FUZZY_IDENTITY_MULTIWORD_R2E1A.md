# FUZZY-IDENTITY-MULTIWORD-R2E.1A — el territorio es una frase

**Tipo:** auditoría + corrección · **Fecha:** 2026-09-02

**Rama:** `main` · **HEAD antes:** `bc13e609a06f3a2bb8a45ae80e5126de7d2299b8`

**Evidencia:**
[`fuzzy_identity_multiword_r2e1a.json`](../../artifacts/audits/fuzzy_identity_multiword_r2e1a.json)

**Reproductor:**
[`audit_fuzzy_identity_multiword_r2e1a.py`](../../scripts/audits/audit_fuzzy_identity_multiword_r2e1a.py)

**Antecedente:** [`INSTITUTIONAL_FUZZY_SAFETY_R2E1.md`](INSTITUTIONAL_FUZZY_SAFETY_R2E1.md)

El principio de R2E.1 no se cuestiona: un candidato del fuzzy no puede perder la
cabeza institucional ni el territorio que el usuario pidió. Lo que se audita es
si la implementación reconocía los territorios de más de una palabra.

## 1. Veredicto

```text
FUZZY_IDENTITY_MULTIWORD_R2E1A: PASS
CURRENT_GUARD_VERDICT (R2E.1): INCOMPLETE

MULTIWORD_TERRITORY_LOSS        5 → 0
BANK_FALSE_REJECTIONS           0 → 0
NEW_FALSE_REJECTIONS                0
REAL_SURFACE_VERDICT_CHANGES        0
```

## 2. La discordancia era real

`TERRITORY_TOKENS` se construía como `frozenset(DEPARTAMENTOS) |
frozenset(GAZETTEER_CIUDADES)`: las **claves** del catálogo. Pero
`candidate_keeps_requested_identity` iteraba **tokens sueltos** de la petición.
Las claves multiword nunca podían coincidir con un token.

Clasificación de las 171 claves:

| Catálogo | Claves | `SINGLE_TOKEN` | `WRAPPER_FORM` | `MULTIWORD` | `WRAPPER_MULTIWORD` |
|---|---:|---:|---:|---:|---:|
| `DEPARTAMENTOS` | 122 | 33 | 76 | 10 | 3 |
| `GAZETTEER_CIUDADES` | 49 | 45 | 0 | 4 | 0 |

Trece núcleos multiword distintos. Doce de los trece tenían al menos un
componente invisible para la regla token a token:

| Núcleo | Tokens invisibles | Efecto |
|---|---|---|
| `santa marta` | `santa`, `marta` | **invisible entero** |
| `puerto carreño` | `puerto`, `carreño` | **invisible entero** |
| `san andres` | `san`, `andres` | **invisible entero** |
| `san andres providencia` | `san`, `andres`, `providencia` | **invisible entero** |
| `san andres y providencia` | idem | **invisible entero** |
| `distrito capital` | `distrito`, `capital` | **invisible entero** |
| `norte de santander` | `norte` | **confundido con Santander** |
| `norte santander` | `norte` | idem |
| `distrito capital de bogota` | `distrito`, `capital` | parcial (`bogota` sí se veía) |
| `bogota d.c.` / `bogota dc` | `d`, `c`, `dc` | parcial |
| `valle del cauca` | — | único núcleo cubierto |

`valle del cauca` funcionaba por accidente: `valle` y `cauca` son, cada uno,
clave del catálogo por separado. Ese accidente es lo que hacía pasar el caso E
del enunciado y lo que hacía parecer sana a la regla.

Los dos modos de fallo no son el mismo. Un territorio **invisible** deja la
petición sin requisito: el candidato puede perderlo entero. Un territorio
**confundido** es peor: `norte de santander` quedaba reducido a `santander`, y
el candidato «CONTRALORIA GENERAL DE SANTANDER» —otro NIT, otro departamento—
lo satisfacía.

## 3. Banco adversarial

Oráculos de la **puerta**, no del resolver: dicen si el candidato conserva la
identidad pedida, no qué comprador debe salir.

| Caso | Consulta | Candidato | Correcto | R2E.1 | R2E.1A |
|---|---|---|---|---|---|
| A | contraloría departamental de **norte de santander** | CONTRALORIA GENERAL DE SANTANDER | `False` | ✗ `True` | `False` |
| B | contraloría de **san andrés** | CGR (alias `contraloria`) | `False` | ✗ `True` | `False` |
| C | contraloría de **puerto carreño** | CGR (alias `contraloria`) | `False` | ✗ `True` | `False` |
| D | contraloría de **santa marta** | CONTRALORIA GENERAL DEL MAGDALENA | `False` | ✗ `True` | `False` |
| E | contraloría departamental del **valle del cauca** | CONTRALORIA GENERAL DEL CAUCA | `False` | `False` | `False` |
| F | contraloría de **santander** | Gobernación Norte de Santander* | `False` | `False` | `False` |
| G | contraloría de **san andrés y providencia** | CGR (alias `contraloria general`) | `False` | ✗ `True` | `False` |
| H | contraloría del **distrito capital de bogotá** | UNIVERSIDAD PEDAGÓGICA NACIONAL | `False` | `False` | `False` |

Canarios positivos —el candidato sí conserva el territorio, aunque lo escriba de
otra forma— y casos que decide la regla de cabeza: 9/9 iguales antes y después.

```text
MULTIWORD_TERRITORY_LOSS   R2E.1 = 5   ·   R2E.1A = 0
BANK_FALSE_REJECTIONS      R2E.1 = 0   ·   R2E.1A = 0
```

## 4. Cuatro estrategias, medidas

`spans_in` + formas equivalentes no se eligió por elegancia. Se compararon
cuatro reglas sobre el banco adversarial y sobre **toda** la superficie real del
gazetteer —cada nombre oficial y cada alias como consulta, contra los tres
primeros candidatos del fuzzy—.

| Estrategia | Pérdidas multiword | Falsos rechazos medidos | Problema |
|---|---:|---:|---|
| **A** — token a token (R2E.1) | 5 | 0 | no ve la frase; confunde vecinos |
| **B** — todos los tokens de todas las claves | 0 | 0 en el gazetteer real | vuelve territoriales `departamento`, `distrito`, `san`, `santa`, `norte`, `capital`, `puerto` — vocabulario institucional que llevan **99 de las 591** fichas |
| **C** — spans, coincidencia literal | 0 | **1 real** | «Contraloría del Distrito Capital de Bogotá» perdía CONTRALORIA DE BOGOTA: la misma frase en otro orden deja de coincidir |
| **D** — spans + formas del mismo canónico | 0 | 0 | la elegida |

B es el atajo que el enunciado advertía y la medición confirma el riesgo: no
falla hoy sobre el gazetteer, pero deja el significado equivocado —«santa» no es
un territorio— y su seguridad depende de que esas consultas no lleguen al fuzzy.

C falla de verdad. `contraloría del distrito capital` y `contraloría de bogotá`
nombran al mismo comprador sin compartir una sola palabra.

D absorbe ese caso porque canonicaliza: reconoce el tramo territorial más largo
y acepta cualquier forma que nombre **el mismo territorio canónico**.

## 5. La regla

`app/core/territory_phrase_index.py` — módulo propio, una responsabilidad:
reconocer qué territorio nombra una petición y enumerar sus formas
equivalentes. No define geografía: recibe `DEPARTAMENTOS` y
`GAZETTEER_CIUDADES`, que ya son autoridad, y el conjunto de conectores.

```text
RULE_BEFORE  cada token de la petición que sea clave del catálogo debe
             aparecer en la evidencia
RULE_AFTER   cada TRAMO territorial de la petición —el más largo, sin
             solapar— debe aparecer en la evidencia como secuencia
             contigua, en esa forma o en otra que nombre el mismo
             territorio canónico
```

`fuzzy_identity_guard` sigue siendo el mismo mecanismo con la misma regla de
cabeza y el mismo alcance: **sólo Tier 2 y Tier 3**. Tier 1 no la cruza.

### 5.1 Dos filtros que la evidencia obligó a poner

**Formas equivalentes, no territorios equivalentes.** El catálogo incluye
atajos que sirven para filtrar y no para nombrar: `cucuta` resuelve a Norte de
Santander y `cundi` a Cundinamarca. Tomarlos como evidencia habría dejado que un
comprador de Cúcuta acreditara una petición por el departamento. Una forma sólo
cuenta si comparte al menos un token con el **nombre del territorio canónico**.
Se mide contra el canónico y no contra la frase pedida a propósito: es lo que
permite que `bogotá` valga por `distrito capital de bogotá`.

**El despojo de conectores no puede inventar topónimos.** Retirar el artículo de
`la capital` producía el tramo `capital`, que es vocabulario institucional
—CANAL CAPITAL, SENA REGIONAL DISTRITO CAPITAL—. Una forma de un solo token sólo
entra al índice si el catálogo ya la reconoce sin conectores. `el valle` sigue
valiendo porque `valle` es clave por sí sola; `la capital` no, porque `capital`
no lo es. Era el único tramo que el despojo fabricaba.

## 6. Falsos rechazos: cómo se midieron y qué no prueban

```text
superficie real     10.844 consultas (todo nombre oficial y todo alias)
triples evaluados   30
cambios de veredicto 0    ·   nuevos falsos rechazos 0   ·   nuevas aceptaciones 0
```

**Treinta.** Ése es el dato incómodo y hay que decirlo: casi todo alias del
gazetteer resuelve por Tier 1, así que la puerta apenas se ejerce sobre la
superficie propia. Por eso el corpus 89/89 y la suite completa dieron idéntico
resultado con las cuatro estrategias: **no discriminan**, y presentarlos como
prueba de que el cambio es seguro sería presentar una puerta que no se abrió.

La discriminación vino del banco adversarial y de la superficie real medida
triple a triple, no de las suites.

## 7. Preservación

```text
R2E.1  territorial 14/14 · BANREP 8/8 exact · nacional 6/6 · padre-relacionado 4/4
R2E    6/6 · R2D.3 · R2D · R2D.1 · R2A · R2A.1 · R2B · R1C · R1D · SENA/ICBF
       gobernaciones · San Andrés · multi-regional
       923 tests focales · 0 fallos
```

`tests/test_institutional_fuzzy_safety_r2e1.py` cambia en tres puntos: las
pruebas unitarias de la puerta pasan el índice en vez del conjunto de tokens.
Las aserciones de comportamiento son las mismas.

## 8. DAPRE — reclasificación

Separado de la puerta. R2E.1 abrió `DAPRE_STALE_PHYSICAL_NAME_DEBT` afirmando
que «SECOP renombró al comprador después de construirse el gazetteer». **Esa
afirmación no tenía respaldo**: la evidencia disponible es un corte, no una
serie temporal. Las tres autoridades, congeladas por NIT base `899999083`:

| Autoridad | NIT publicado | Nombre |
|---|---|---|
| `REGISTRY_NAME` (`4ex9-j3n8`, `es_entidad='Si'`) | `899999083` | `DAPRE` |
| `REGISTRY_NAME` | `8999990839` | `PRESIDENCIA DE LA REPÚBLICA` |
| `REGISTRY_NAME` | `8999990830` | `FONDO DE PROGRAMAS ESPECIALES PARA LA PAZ DEL DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA — FONDO PAZ` |
| `PROCESS_SOURCE_NAMES` (`p6dx-8zbt`) | `899999083` | `DAPRE` (17.128 filas) |
| `CONTRACT_SOURCE_NAMES` (`jbjy-vk9h`) | `899999083` | `DAPRE` (4.351 filas) |

El `official_name` del gazetteer,
`DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA`, **no aparece
como nombre en ninguna de las tres superficies**. En el registro sólo existe
incrustado dentro del nombre del Fondo Paz; no hay una sola fila cuyo nombre sea
esa frase.

```text
CLASSIFICATION: SOURCE_NAME_SPLIT
OLD_DEBT_NAME:  DAPRE_STALE_PHYSICAL_NAME_DEBT
NEW_DEBT_NAME:  DAPRE_SOURCE_NAME_SPLIT   familia CANONICAL_ENTITY_SOURCE_NAMES
```

Un mismo NIT base sostiene tres superficies de nombre —una de ellas repetida en
procesos y contratos— y el gazetteer guarda una cuarta que no es ninguna. No se
modificó `official_name`, no se cambió a `DAPRE` y no se tocó la proyección: no
existe todavía una abstracción de nombres de origen que lo resuelva sin abrir
alcance nuevo.

## 9. Validación

```text
tests/test_fuzzy_identity_multiword_r2e1a.py   45 tests
focales entidad/gazetteer (20 archivos)        923 passed
corpus (scripts/run_query_corpus.py --mode all)  89/89 PASS
ruff check app/ tests/ scripts/                  PASS
make lint-core                                   PASS
suite completa            10.561 passed · 2 skipped · 15 xfailed · 0 failed
xfails                    15 — los mismos del baseline post-R2E.1; ninguno nuevo
```

`semgrep` no se corrió: el binario no está instalado en este entorno. Su ley
—`Settings()` fuera de pytest sin redirigir la persistencia— no aplica: ni el
auditor ni los tests construyen `Settings` ni `SecopalWorkflow`.

## 10. Límites

- El banco adversarial son quince casos construidos sobre topónimos reales del
  catálogo, no una muestra de consultas de usuarios.
- La superficie real ejerce la puerta 30 veces. Un cambio de comportamiento
  fuera de esos 30 triples y del banco no queda medido.
- Los tramos territoriales se reconocen sobre el catálogo actual. Un topónimo
  ausente de `DEPARTAMENTOS` y `GAZETTEER_CIUDADES` sigue siendo invisible para
  la puerta; R2E.1A no amplía catálogos (prohibido en el lote).
- No se tocaron universidades, Buyer Groups, la proyección del DAPRE, el grupo
  físico de la CGR, los catálogos de geografía, el fuzzy de hospital/ESE/
  universidad, SODA3, Radar ni Actor.

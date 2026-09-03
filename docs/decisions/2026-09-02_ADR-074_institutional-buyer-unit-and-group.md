# ADR-074 — El comprador es una unidad lógica acreditada; el NIT no es su clave y el grupo se declara

**Fecha:** 2026-09-02
**Estado:** Accepted — principios. Runtime **sin cambios** en esta aceptación.

**Evidencia:**
[`INSTITUTIONAL_BUYER_MODEL_P01.md`](../gazetteer/INSTITUTIONAL_BUYER_MODEL_P01.md)
y [`institutional_buyer_model_p01.json`](../../artifacts/audits/institutional_buyer_model_p01.json);
[`BUYER_PUNCTUATION_SURFACE_P0.md`](../gazetteer/BUYER_PUNCTUATION_SURFACE_P0.md).

El draft de este ADR vive en §16 del informe P0.1. Se acepta **modificado**: las
diferencias entre aquel draft y esta decisión están enumeradas al final, en
*Qué cambió respecto del draft*.

## Contexto

El censo P0 dejó 20 superficies de comprador sin adjudicar. La readjudicación
P0.1 las reprodujo las 20 contra fuente viva y falsificó la hipótesis que
sostenía el modelo implícito del gazetteer:

- el NIT distrital de Bogotá (`899999061`, con cuatro grafías publicadas)
  sostiene **41 unidades compradoras transaccionales** y **107 nombres** de
  directorio, incluidos la Personería, la Veeduría, la Policía Nacional y tres
  personas naturales;
- el territorio `Distrito Capital de Bogotá` tiene 2.584.635 procesos y la
  administración distrital aporta el **14,5 %**;
- `890503373` (Villa del Rosario, ~90.000 habitantes) publica **tres** unidades
  bajo un NIT: municipio, concejo y personería.

Y encontró dos compradores reales hoy inalcanzables desde lenguaje natural, los
dos por la misma causa —una unidad repartida en varios canonicals— y **ninguno**
por puntuación: MinJusticia (0 de 14.517 filas, porque todo alias humano resuelve
a `MINJUSTICIA.`, etiqueta que SECOP no publica) y Yopal (0 de 19.039 contratos,
porque el alias resuelve a la grafía de procesos).

## Decisión

### 1. `BUYER_UNIT_IS_PRIMARY_RUNTIME_IDENTITY`

Una **BuyerUnit** es una unidad compradora **lógica acreditada**, representada
por 1..n nombres físicos publicados. **No** es "el nombre que SECOP publica":
Yopal prueba que una misma unidad se publica con dos grafías, una por dataset, y
MinJusticia que un canonical puede no existir en ninguna fuente.

```
BuyerUnit
  canonical_name                # identidad y display humano
  aliases[]                     # ADR-072: exact / scan son autoridades distintas
  source_names{dataset: [...]}  # ADR-073: byte-for-byte, igualdad exacta
  fiscal_identity               # NIT + grafías observadas; evidencia, NO clave
  territory
```

La BuyerUnit es la identidad primaria de runtime y sigue siéndolo. Nada en este
ADR la sustituye.

Corolario operativo: **una BuyerUnit no se parte porque procesos y contratos
usen strings distintos**. Ese caso es un binding de proyección (ADR-073), no una
unidad nueva.

### 2. `NO_NIT_AUTO_MERGE`

El NIT es evidencia fiscal. Identifica una persona jurídica, no una unidad
compradora. Un mismo NIT admite cuatro lecturas, y las cuatro se observaron en
el mismo lote:

| Lectura | Ejemplo observado |
|---|---|
| `SAME_BUYER_UNIT` | una unidad, dos grafías, un dataset cada una |
| `SHARED_NIT_DISTINCT_BUYER_UNIT` | Bogotá (41), Villa del Rosario (3) |
| `SHARED_NIT_BUYER_FAMILY` | las 20 alcaldías locales de Bogotá |
| `REGISTRY_NOISE` | personas naturales y Policía Nacional bajo el NIT distrital |

**`same NIT ⇒ auto merge` queda prohibido, sin excepciones.** Fusionar dos
canonicals en una BuyerUnit exige el predicado completo de cinco condiciones
(§9 del informe P0.1): NIT acreditado con equivalencia base/DV acotada;
procedencia de dataset **disjunta**; grafías iguales tras quitar exactamente un
signo terminal de la más larga; ningún tercer canonical reclama ninguna de las
dos cadenas; nombre institucional idéntico en tokens y sin puntuación terminal.

El predicado acertó 5 de 5 y produjo 0 falsos positivos sobre los 50 NIT más
afectados del censo P0. Si falla una sola condición, el caso es
`KEEP_SEPARATE` o `REVIEW`, nunca merge.

### 3. `TERRITORY_IS_SEPARATE`

Territorio y comprador son dimensiones independientes. `en Bogotá` no es
`de la Alcaldía de Bogotá`. Ninguna pertenencia, ninguna membresía y ningún
filtro de comprador se deriva de un campo geográfico.

Esto ya es ADR-040 (resolución geo tipada). Lo que P0.1 añade es la magnitud:
el 85,5 % de la contratación del territorio de Bogotá no es la administración
distrital.

### 4. `GROUP_MEMBERSHIP_DECLARED` — invariante `INV-BUYER-GROUP-MEMBERSHIP-IS-DECLARED`

Un **BuyerGroup** es una agrupación **declarada** de BuyerUnits. No es alias, no
es NIT, no reemplaza a la BuyerUnit.

- referencia BuyerUnits; no las copia ni las redefine;
- **jamás** se deriva de un NIT ni de un campo geográfico;
- cada miembro lleva su propia procedencia acreditada, y la membresía se versiona
  —igual que los bindings de `buyer_source_names.json` en schema 2—;
- un grupo no es alias de un hijo: `alcaldía de bogotá` no puede resolver a la
  Secretaría General ni a ninguna otra unidad concreta;
- **una BuyerUnit explícita siempre gana al grupo.**

`BUYER_GROUP_REQUIRED: YES`. Sin grupos, `alcaldía de Bogotá` sólo puede fallar
cerrado —que es lo que hoy hace, y es correcto—.

### 5. `INV-BUYER-GROUP-MEMBERSHIP-DOES-NOT-DEFINE-RESULT-IDENTITY`

**Un BuyerGroup determina qué BuyerUnits se consultan. Nada más.**

Un grupo no participa en `result_identity`, no deduplica por NIT, no deduplica
entre BuyerUnits, no modifica la `stable_key` de procesos ni de contratos, y no
colapsa hechos contractuales.

Canario normativo:

```
BuyerUnit A y BuyerUnit B comparten NIT X
A publica el contrato C1; B publica el contrato C2
BuyerGroup(A, B)  ⇒  logical_contracts = 2      # NUNCA 1
Si C1 tiene dos filas físicas: physical_rows = 3, logical_contracts = 2
```

Las dos métricas se conservan por separado. Llamar "contratos" a un `count(*)`
físico sin etiquetarlo queda prohibido.

La autoridad de deduplicación sigue siendo, y sólo, `result_identity`.

### 6. `NO_SILENT_RELAXATION` (herencia de ADR-069)

Ante ambigüedad de comprador: `FAIL_CLOSED` o clarificación. Nunca relajar el
filtro, la identidad ni la membresía en silencio.

Esta decisión tiene una consecuencia inmediata y no hipotética. El camino de
ejecución que hoy sostiene los grupos en embrión (ICBF, SENA) corta en 15
candidatos, y al pasarse **no trunca a 15: emite un único `LIKE` sobre
`entidad_in_candidates[0]`** (`app/core/soql_builder.py:401-411`). El comentario
del código anuncia un fallback a `contains_all` "o los primeros 15", y no ocurre
ninguna de las dos cosas. Un grupo de 39 miembros consultaría una unidad y
respondería como si fueran 39.

**Ese fallback silencioso queda prohibido por este ADR con independencia de que
BuyerGroup llegue a existir.** Es la misma familia que
`INV-NO-SILENT-SEMANTIC-FALLBACK`: un vocabulario acotado más una degradación
que no se declara.

## Qué este ADR NO decide

Deliberadamente, para que la implementación pueda contradecirlo sin reabrirlo:

- **El mecanismo de ejecución no se congela.** `entidad_in_candidates` queda
  registrado como `CURRENT_EXECUTION_CANDIDATE` —es el mecanismo vivo, disparado
  por nombre, con el guard `not entidad_value` que ya garantiza la precedencia de
  la unidad explícita— y **no** como arquitectura definitiva. La implementación
  de BuyerGroup puede sustituirlo por `OR` exacto con proyección, batching,
  agrupación por token común o cualquier capacidad de transporte que se
  demuestre segura. La decisión de estrategia es un artefacto posterior
  (`BUYER_GROUP_EXECUTION_STRATEGY`), no este ADR.
- **La semántica por defecto de "alcaldía de Bogotá" no es ley universal.**
  `B_CENTRAL_PLUS_LOCAL` (19 centrales + 20 locales = 39 unidades, 374.302
  procesos) es la **política de producto del registro de Bogotá**, versionada con
  ese registro. El modelo admite varios grupos y subgrupos por territorio —
  `bogota_central`, `bogota_local_mayoralties`, `bogota_executive`— y otro
  territorio puede declarar una política distinta sin tocar este ADR. Lo que sí
  es normativo: la respuesta **declara su membresía**, no la aplica en silencio.
- **El universo municipal.** M0 es descubrimiento posterior. Lo único que este
  ADR le impone es que la cadena `DIVIPOLA → municipio → NIT → comprador único`
  está falsificada: por municipio hay 1 territorio, 1..n grafías de identidad
  legal, 1..n BuyerUnits y 0..1 grupos.
- **Qué hacer con las 190 superficies de puntuación del censo P0.** Este ADR no
  autoriza aplicarlas en lote.

## Consecuencias

- El gazetteer gana una capa de grupos; **no pierde unidades**.
- Los cinco duplicados canónicos deben fusionarse **antes** de que ADR-073 les
  cree bindings de proyección. Hoy el `LIKE` los enmascara —una grafía corta
  alcanza a la larga por substring—; un binding convierte la comparación en
  igualdad exacta y el enmascaramiento en **cero silencioso**. Merge y binding
  son un solo cambio, y los `source_names` de la unidad superviviente deben
  cubrir todos los datasets donde transacciona, verificado en vivo.
- `entity_ecosystem.json` (disparo **geográfico**, inyección post-query) queda
  formalmente desaconsejado frente al disparo por nombre. Ya está declarado
  estado derivado de ejecución y no viaja a persistencia, paginación ni radares;
  este ADR añade que tampoco es el camino para grupos.
- El fallback a `entidad_in_candidates[0]` debe cerrarse.
- Una consulta de grupo queda ligada al query **ejecutado**. Afirmar en la
  respuesta un filtro que el fetch no aplicó viola esta decisión.

## Deuda registrada por esta aceptación

| Deuda | Qué falta |
|---|---|
| `BUYER_GROUP_CANDIDATE_CAP` | el tope de 15 no alcanza para ningún grupo real de Bogotá, y al pasarse colapsa a un miembro |
| `ECOSYSTEM_SOQL_DISPLAY_DIVERGENCE` | el SoQL exhibido puede llevar un `OR` de compradores que las filas mostradas nunca aplicaron |
| `GAZETTEER_PHANTOM_NIT_DEBT` | Ibagué lleva `800133897` (cero filas) donde publica `800113389`; falta el censo |
| `GAZETTEER_SYNTHETIC_CANONICAL_DEBT` | `MINJUSTICIA.` prueba que hay canonicals que SECOP no publica; falta el censo |
| `MUNICIPAL_REWRITE_HEAD_COMPATIBILITY` | `_official_matches_strict_rewrite` aceptaría `PERSONERIA MUNICIPIO DE X` para `alcaldía de X`; latente hasta que entren personerías |

## Alternativas descartadas

1. **Normalizar la puntuación de comprador.** Ya vetada por P0
   (`KEEP_EXPLICIT_ALIASES`): crea colisiones y claves multi-owner. P0.1 añade
   que tampoco ataca la causa — MinJusticia no se arregla quitando puntos.
2. **Fusionar por NIT.** Falsificada: fusionaría 41 unidades de Bogotá en una y
   arrastraría la Policía Nacional y tres personas naturales.
3. **Definir la BuyerUnit como el string publicado.** Es el modelo implícito
   actual y es el que produjo los dos compradores inalcanzables: una unidad
   quedó repartida en varios canonicals porque el gazetteer se construyó por
   dataset.
4. **Derivar la membresía de un grupo del NIT o de la geografía.** Refutada por
   los mismos datos: el NIT distrital incluye órganos de control y personas
   naturales; el territorio de Bogotá incluye la Universidad Nacional, INVIAS y
   Ecopetrol.

## Qué cambió respecto del draft de §16

| Draft P0.1 | Decisión aceptada |
|---|---|
| BuyerUnit = "el nombre de comprador que SECOP publica" | unidad **lógica acreditada**, representada por 1..n nombres físicos |
| `BOGOTA_DEFAULT_GROUP_SEMANTICS: B_CENTRAL_PLUS_LOCAL` como recomendación del ADR | política de producto del **registro de Bogotá**, versionada allí; el ADR admite varios grupos y subgrupos |
| BuyerGroup "sobre el mecanismo existente `entidad_in_candidates`" | `entidad_in_candidates` es `CURRENT_EXECUTION_CANDIDATE`, no arquitectura; la implementación puede sustituirlo |
| — | añade `INV-BUYER-GROUP-MEMBERSHIP-DOES-NOT-DEFINE-RESULT-IDENTITY` y su canario de NIT compartido |
| el tope de 15 como restricción a resolver | el colapso a `candidates[0]` como **fallback silencioso prohibido**, con o sin BuyerGroup |

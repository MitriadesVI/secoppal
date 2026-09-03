# INSTITUTIONAL BUYER MODEL — P0.1

**Tipo:** discovery + adjudicación read-only

**Fecha de observación:** 2026-09-02

**HEAD auditado:** `7fa44a30fd92af6176ff9a85f73cd4f7696527e7`

**Resultado:** `PASS`

**Decisión:** `BUYER_UNIT_IS_PRIMARY` + `BUYER_GROUP_REQUIRED` + `NO_NIT_MERGE`

**ADR propuesto:** ADR-074 (estado `DRAFT`, no aceptado)

`PASS` significa que los 20 casos del universo P0.1 se readjudicaron con un
predicado verificable y que los 20 predicados se reprodujeron contra las tres
fuentes vivas. No significa que la superficie esté sana: la auditoría encontró
dos compradores reales que hoy son inalcanzables desde lenguaje natural.

---

## 1. Resultado ejecutivo

| Medida | Resultado |
|---|---:|
| Casos de entrada (`WRONG_OWNER` + `NEEDS_REVIEW`) | 20 |
| Predicados reproducidos en vivo | 20 / 20 |
| Unidades compradoras bajo el NIT distrital de Bogotá (transaccionales) | 41 |
| Grafías del mismo NIT distrital observadas | 4 |
| Nombres distintos en el directorio bajo ese NIT | 107 |
| Procesos en el territorio de Bogotá | 2.584.635 |
| Procesos de la administración distrital (política recomendada) | 374.302 |
| Canarios semánticos que pasan | 2 de 5 |

Dos hallazgos nuevos, ambos verificados contra fuente viva:

- **MinJusticia es inalcanzable.** Toda frase humana (`minjusticia`,
  `ministerio de justicia`, `ministerio de justicia y del derecho`) resuelve al
  canonical `MINJUSTICIA.`, una etiqueta que SECOP **no publica en ningún
  dataset**. El `LIKE` resultante alcanza 0 de 7.595 procesos y 0 de 6.922
  contratos. 14.517 filas invisibles.
- **Yopal pierde sus contratos.** `alcaldía de yopal` resuelve a
  `MUNICIPIO DE YOPAL...`, la grafía de *procesos*. Contratos publica
  `MUNICIPIO DE YOPAL..`. El `LIKE` alcanza 0 de 19.039 filas.

Ninguno de los dos es un problema de puntuación. Los dos son el mismo defecto:
**una unidad compradora repartida en varios canonicals**.

---

## 2. El modelo institucional

Cuatro conceptos separados. Ninguno se deriva de otro.

### TERRITORY

Campos geográficos (`departamento_entidad`/`ciudad_entidad` en procesos,
`departamento`/`ciudad` en contratos). Ubicación de la compra o del comprador.
**Jamás implica un comprador.**

Evidencia: el territorio `Distrito Capital de Bogotá` tiene 2.584.635 procesos.
La administración distrital aporta 374.302 — el **14,5 %**. Los mayores
compradores del territorio son nacionales con domicilio en Bogotá: Universidad
Nacional (110.849), Agencia Nacional de Tierras (53.402), INVIAS (44.047),
Ecopetrol (37.929), Banco de la República (30.659), Cámara de Representantes
(20.186). El inventario clasifica 127 unidades como residuo geográfico:
1.389.701 procesos que están en Bogotá y no son la Alcaldía.

### LEGAL / FISCAL IDENTITY

El NIT. Identifica una persona jurídica, **no una unidad compradora**.

Un mismo NIT puede significar cuatro cosas distintas, y en Bogotá significa las
cuatro a la vez (§3).

Además la cadena del NIT es sucia por sí misma. El Distrito Capital se publica
como `899999061`, `8999990619` (base + DV 9), `8999990615` (base + DV 5) y
`0899999061` (cero guía) — cuatro grafías, una identidad. Y hay NIT fantasma:
el gazetteer atribuye a Ibagué el NIT `800133897`, que **no tiene una sola fila**
en procesos ni en contratos; Ibagué publica bajo `800113389`, con los dígitos
transpuestos.

### BUYER UNIT

El nombre de comprador que SECOP publica (`entidad` / `nombre_entidad`).
**Es la identidad primaria de runtime y debe seguir siéndolo.**

Conserva identidad individual aunque comparta NIT. `Secretaría Distrital de
Integración Social` (112.072 procesos) y `Alcaldía Local de Chapinero` (3.006)
comparten NIT y son consultables por separado — los canarios A y D lo exigen.

### BUYER GROUP

Agrupación **declarada** de BuyerUnits. Nunca inferida de NIT ni de geografía.

No es alias, no es NIT, no reemplaza a la BuyerUnit. Un grupo referencia
unidades; no las duplica.

---

## 3. `SAME_NIT ⇒ SAME_BUYER` queda falsificado

El NIT distrital de Bogotá sostiene, sólo en datos transaccionales,
**41 unidades compradoras distintas**: 19 de la administración central,
20 alcaldías locales / fondos de desarrollo local, y 2 órganos que **no son la
administración** (Personería de Bogotá, Veeduría Distrital). En el directorio,
el mismo NIT sostiene 107 nombres distintos en 132 filas, e incluye:

- `POLICÍA NACIONAL DE COLOMBIA` — entidad nacional;
- `SUBRED INTEGRADA DE SERVICIOS DE SALUD NORTE` — ESE con NIT propio
  (`900971006`);
- `CONCEJO DE BOGOTA` — corporación político-administrativa;
- tres personas naturales (`DERLY YURANY AREVALO CARDENAS`,
  `ROBERT NICOLAS NEIRA DIAZ`, `SALMA VALENTINA PALACIOS MERCHAN`).

Un NIT igual, por tanto, no acredita ni siquiera pertenencia al mismo grupo.

El caso recíproco también aparece en el lote: `890503373` (Villa del Rosario)
publica **tres** unidades compradoras — `MUNICIPIO DE VILLA DEL ROSARIO**`
(5.693/5.501), `CONCEJO MUNICIPAL DE VILLA DEL ROSARIO.` (44/45) y
`PERSONERIA MUNICIPIO DE VILLA DEL ROSARIO` (22/23). No es un fenómeno de
ciudades grandes: pasa en un municipio de Norte de Santander.

---

## 4. Causa raíz de los duplicados: la fuente pierde un carácter

Los cinco pares duplicados del lote comparten una firma exacta y verificable:
**procesos publica una grafía con un signo terminal más que contratos**, para el
mismo NIT.

| NIT | Procesos publica | Contratos publica |
|---|---|---|
| `891855017` | `MUNICIPIO DE YOPAL...` | `MUNICIPIO DE YOPAL..` |
| `800094755` | `MUNICIPIO DE SOACHA..` | `MUNICIPIO DE SOACHA.` |
| `8002469532` | `FONDO FINANCIERO DISTRITAL DE SALUD..` | `FONDO FINANCIERO DISTRITAL DE SALUD.` |
| `900457461` | `MINISTERIO DE JUSTICIA Y DEL DERECHO..` | `MINISTERIO DE JUSTICIA Y DEL DERECHO.` |
| `900126860` | `UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS..` | `UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS.` |

Contraste sobre los 50 NIT más afectados del censo P0: **45 publican la misma
cadena en ambos datasets, 4 difieren, y los 4 difieren en la misma dirección**
(`PROC_LONGER_BY_1`). Cero contraejemplos. Un sexto caso aparece fuera del lote:
`.SECRETARÍA DISTRITAL DE SALUD..` en procesos frente a
`.SECRETARÍA DISTRITAL DE SALUD.` en contratos.

La puntuación **sí es texto publicado**, no un sufijo sintético del gazetteer:
`MUNICIPIO DE YOPAL...` con tres puntos es lo que SECOP devuelve para
`nit_entidad='891855017'`. Lo que es sintético es el efecto: el gazetteer se
construyó por dataset, y cada dataset produjo un canonical.

### Por qué tres de los cinco no rompen nada y dos sí

`_entity_clause` (`app/core/soql_builder.py:733`) usa `LIKE '%valor%'` cuando el
canonical no tiene binding de proyección. Un `LIKE` con la grafía **corta**
alcanza también la larga, porque es su substring. Con la larga, no.

| Alias resuelve a | Dirección | Efecto |
|---|---|---|
| `MUNICIPIO DE SOACHA.` (corta) | contratos → procesos | funciona **por accidente** |
| `FONDO FINANCIERO DISTRITAL DE SALUD.` (corta) | contratos → procesos | funciona por accidente |
| `UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS.` (corta) | contratos → procesos | funciona por accidente |
| `MUNICIPIO DE YOPAL...` (larga) | procesos ↛ contratos | **0 de 19.039 contratos** |
| `MINJUSTICIA.` (inexistente) | ninguna | **0 de 14.517 filas** |

MinJusticia es el caso extremo: tiene **cuatro** canonicals para una unidad.
Dos son etiquetas sintéticas (`MINJUSTICIA.`, `MINJUSTICIA..`) que concentran
todos los alias humanos y que SECOP no publica —`entidad like 'MINJUSTICIA%'`
devuelve vacío en procesos y en contratos—; los otros dos son los nombres reales
publicados, pero sólo llevan alias puntuados que nadie escribe
(`ministerio de justicia y del derecho..`).

### Consecuencia para ADR-073

Hoy el `LIKE` **enmascara** tres de los cinco duplicados. Crear un binding de
Source Name Projection para esos canonicals convierte la comparación en igualdad
exacta y transforma el enmascaramiento en cero silencioso. El docstring de
`_entity_clause` ya nombra el caso Soacha por su nombre.

> **No crear bindings de proyección para los cinco pares antes de fusionarlos.**

---

## 5. Los cinco canarios de Bogotá

Ejecutados por el pipeline real (`QueryRouter`) con persistencia aislada.

| Canario | Esperado | Observado | Veredicto |
|---|---|---|---|
| **A** `integracion social de bogota ... adulto mayor 2026` | BuyerUnit = SDIS | territorio Bogotá + `objeto=[integracion, social, adulto_mayor]`, **sin comprador** | `FAIL` |
| **B** `contratos recientes de la alcaldia de bogota por mas de 1000 millones` | BuyerGroup | `entidad='Municipio de bogota'` → `rewrite_alcaldía_unresolved`, pide precisión | `FAIL_CLOSED` |
| **C** `contratos en bogota por mas de 1000 millones` | territorio | territorio `Distrito Capital de Bogotá`, sin restricción de comprador | `PASS` |
| **D** `contratos de la alcaldia local de chapinero` | BuyerUnit = FDL Chapinero | `scan_exact` → `ALCALDÍA LOCAL DE CHAPINERO - FONDO DE DESARROLLO LOCAL` | `PASS` |
| **E** `contratos de las alcaldias locales de bogota` | BuyerGroup | territorio Bogotá + `objeto=[alcaldia, locales]` | `FAIL` |

### A y E: el comprador se convierte en tema

El mismo mecanismo destruye los dos. El escaneo geográfico consume `de bogota` y
lo convierte en `departamento_resolved`; lo que queda —`integracion social`,
`alcaldias locales`— no es reconocido como comprador y cae en `objeto`. El
usuario pidió un comprador y recibe territorio más una sopa de términos.

El resolver aislado **sí** acierta en A: `resolve_entidad("integracion social de
bogota")` devuelve `(Secretaría Distrital de Integración Social)` con
`fuzzy(95)`. El pipeline nunca llega a preguntárselo. Es el patrón
`NORMALIZATION_ERASES_USER_INTENT` ya registrado en HCA-R2.

Lo mismo le pasa a `procesos de la alcaldia mayor de bogota`: territorio +
`objeto`, comprador borrado, aunque el resolver aislado lo lleve a
`SECRETARÍA GENERAL DE LA ALCALDÍA MAYOR DE BOGOTÁ` con `fuzzy(90)`.

### B: falla cerrado, que es lo correcto — y por eso falta el grupo

`alcaldía de X` se reescribe a `Municipio de X` (`_strict_rewrite_target`,
`app/core/entity_resolver.py:673`). Para Bogotá no hay ningún canonical
`Municipio de Bogotá`, así que el resolver pide precisión en vez de adivinar.
Ese es el comportamiento correcto y **no debe tocarse**: lo que falta no es
relajar la puerta, es que exista un BuyerGroup que la consulta pueda nombrar.

### C: el territorio ya está limpio en el fetch

`entidad_ecosystem` —el catálogo curado de `app/data/entity_ecosystem.json`— se
inyecta **después** de ejecutar (`normal_flow.py:193`) y está declarado estado
derivado de ejecución (`app/core/derived_execution_state.py`), por lo que no
viaja a la persistencia, la paginación ni los radares. Las filas que el usuario
ve en C son territoriales, sin restricción de comprador. `PASS`.

Con una salvedad que conviene registrar: ese bloque **sí reconstruye el SoQL**
que se muestra en la respuesta asesora, sin volver a consultar. El SoQL exhibido
lleva un `OR` de hasta 15 compradores que las filas mostradas nunca aplicaron.
Es una divergencia de procedencia entre lo que se ve y lo que se dice; queda
anotada como deuda, fuera del alcance de P0.1.

---

## 6. Qué existe ya en el repo — no duplicar arquitectura

Hay **dos** cosas distintas llamadas «ecosystem». Conviene no confundirlas.

| Pieza | Disparo | Estado | Sirve para BuyerGroup |
|---|---|---|---|
| `entidad_in_candidates` (`soql_builder.py:398`) | por **nombre** institucional | activo, en el fetch | **sí — es el mecanismo** |
| `entidad_contains_all` | por nombre | activo | sí, para grupos con token común |
| `icbf_resolver` / `sena_resolver` / `entity_ecosystem_router` | por nombre | activo | sí — son BuyerGroups en embrión |
| `entidad_ecosystem` (`entity_ecosystem.json`) | por **geografía** | post-query, en cuarentena | **no — es el antipatrón** |

El mecanismo de ejecución de un BuyerGroup **ya existe**: `entidad_in_candidates`
emite un `OR` de `LIKE` sobre el campo de comprador, guardado por
`not entidad_value` para que una unidad explícita siempre gane al grupo. ICBF y
SENA ya lo usan. Lo que falta es un registro de grupos declarados que no sean
ICBF/SENA, disparado por nombre y nunca por territorio.

**Restricción medida:** ese camino corta en 15 candidatos y, al pasarse, cae al
primer elemento de la lista. Bogotá tiene 20 alcaldías locales y 19 unidades
centrales. **El tope de 15 no alcanza para ningún grupo real de Bogotá** y hay
que resolverlo antes de implementar (agrupar por token común con
`entidad_contains_all`, subir el tope, o paginar el `OR`).

---

## 7. Triage de los 20

Cada caso lleva un predicado que el auditor comprueba en vivo. Si el predicado
no se reproduce, el caso degrada a `REVIEW`. Se reprodujeron los 20.

| Causa | Casos |
|---|---:|
| `DUPLICATE_CANONICAL_SAME_BUYER_UNIT` | 5 |
| `REGISTRY_NOISE` | 6 |
| `SOURCE_NAME_VARIANT_SAME_BUYER_UNIT` | 4 |
| `SHARED_NIT_DISTINCT_BUYER_UNIT` | 2 |
| `OVERGENERIC_ALIAS` | 2 |
| `WRONG_ALIAS_OWNER` | 1 |

### Detalle

| # | Grafía física | NIT | Causa | Remediación |
|---:|---|---|---|---|
| 1 | `ALCALDIA DE IBAGUE -` | 800133897 | `REGISTRY_NOISE` | `NO_CHANGE` |
| 2 | `ALCALDIA DE IBAGUe+` | 800133897 | `REGISTRY_NOISE` | `NO_CHANGE` |
| 3 | `ALCALDIA*` | 891280000 | `OVERGENERIC_ALIAS` | `RESOLUTION_BLOCK` |
| 4 | `CENAC PUENTE ARANDA+` | 830039548 | `WRONG_ALIAS_OWNER` | `ADD_EXACT_ONLY` |
| 5 | `E.S.E BELLO SALUD` | 800174995 | `REGISTRY_NOISE` | `NO_CHANGE` |
| 6 | `HOSPITAL BENJAMIN BARNEY GASCA E.S.E` | 891380055 | `SOURCE_NAME_VARIANT` | `ADD_EXACT_ONLY` |
| 7 | `E.S.E. HOSPITAL LOCAL PUERTO ASIS *` | 846000253 | `SOURCE_NAME_VARIANT` | `ADD_EXACT_ONLY` |
| 8 | `E.S.E  HOSPITAL SAGRADO CORAZoN DE JESuS` | 846000471 | `SOURCE_NAME_VARIANT` | `ADD_EXACT_ONLY` |
| 9 | `E.S.E  HOSPITAL SAGRADO CORAZÓN DE JESÚS` | 846000471 | `SOURCE_NAME_VARIANT` | `SOURCE_NAME_BINDING` |
| 10 | `E.S.E` | 891201845 | `OVERGENERIC_ALIAS` | `RESOLUTION_BLOCK` |
| 11 | `FONDO FINANCIERO DISTRITAL DE SALUD.` | 8002469532 | `DUPLICATE_CANONICAL` | `MERGE` |
| 12 | `SECRETARIA DE SALUD DEPARTAMENTAL+` | 892115015 | `SHARED_NIT_DISTINCT_UNIT` | `KEEP_SEPARATE` |
| 13 | `MINISTERIO DE JUSTICIA Y DEL DERECHO.` | 900457461 | `DUPLICATE_CANONICAL` | `MERGE` |
| 14 | `ALCALDIA DE SOACHA*/*` | 800094755 | `REGISTRY_NOISE` | `NO_CHANGE` |
| 15 | `MUNICIPIO DE SOACHA.` | 800094755 | `DUPLICATE_CANONICAL` | `MERGE` |
| 16 | `ALCALDIA DE SOACHA//` | 8000947557 | `REGISTRY_NOISE` | `NO_CHANGE` |
| 17 | `SECRETARiA DE AMBIENTE, MINAS Y DESARROLLO RURAL` | 8000947557 | `SHARED_NIT_DISTINCT_UNIT` | `KEEP_SEPARATE` |
| 18 | `AGENCIA NACIONAL DE CONTRATACION PUBLICA- COLOMBIA COMPRA EFICIENTE` | 8905033730 | `REGISTRY_NOISE` | `NO_CHANGE` |
| 19 | `MUNICIPIO DE YOPAL..` | 891855017 | `DUPLICATE_CANONICAL` | `MERGE` |
| 20 | `UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS.` | 900126860 | `DUPLICATE_CANONICAL` | `MERGE` |

### Notas de adjudicación

**Caso 4 — único con daño transaccional por owner equivocado.** `CENAC PUENTE
ARANDA+` es un comprador real y activo: 2.967 procesos y 1.454 contratos bajo
`830039548`. El resolver lo manda a `CENTRAL ADMINISTRATIVA Y CONTABLE CENAC
PERSONAL`, que es NIT `900385079` — otra identidad legal (una tercera,
`CENAC AVIACION`, es `830039207`). El canonical correcto ya existe en el
gazetteer con el mismo NIT y sin actividad registrada.

**Casos 8 y 9 se remedian juntos.** `normalize_text` pliega acentos, así que
`CORAZoN DE JESuS` y `CORAZÓN DE JESÚS` colapsan a la misma clave: un solo
alias exacto cubre las dos. El caso 9 es además la grafía **publicada** en
procesos (5.184 filas) y no coincide con el canonical, lo que lo convierte
también en un caso de proyección ADR-073.

**Casos 12 y 17 son unidades distintas, no duplicados.** `SECRETARIA DE SALUD
DEPARTAMENTAL+` cuelga del NIT base de La Guajira, cuya Gobernación publica bajo
`8921150151` mientras el NIT base publica `ADMINISTRACIÓN TEMPORAL PARA EL
SECTOR AGUA POTABLE Y SANEAMIENTO BÁSICO`: tres unidades, una identidad fiscal.
`SECRETARiA DE AMBIENTE, MINAS Y DESARROLLO RURAL` es la secretaría sectorial de
Soacha bajo el NIT del municipio. Ninguna de las dos lleva territorio en el
nombre, así que **no pueden resolverse solas** aunque se mantengan separadas.

**Los 6 `REGISTRY_NOISE` no aparecen como comprador en ninguna fuente
transaccional.** Cada uno de sus NIT publica exactamente un nombre distinto del
disputado. Son filas de registro del directorio, no unidades compradoras.

---

## 8. Puerta de comprador genérico: la puntuación la abre

Hallazgo colateral, verificado:

| Entrada | `normalize_text` | ¿bloqueada como genérica? |
|---|---|---|
| `alcaldia` | `alcaldia` | **sí** |
| `ALCALDIA*` | `alcaldia*` | **no** |
| `ALCALDIA.` | `alcaldia.` | **no** |
| `gobernacion` | `gobernacion` | **sí** |
| `GOBERNACION+` | `gobernacion+` | **no** |

`is_unqualified_buyer_name` compara contra un vocabulario enumerado; un solo
signo terminal deja pasar la cadena al fuzzy global, que le encuentra vecino a
cualquier cosa. Así `ALCALDIA*` termina en `(Secretaría Distrital de Integración
Social)` con `fuzzy(84)`.

**Alcance honesto:** el defecto es **del resolver, no del pipeline**. El router
normaliza el signo antes de capturar la entidad, y
`contratos de ALCALDIA*` llega a `resolve_entidad` como `alcaldia`, que sí se
bloquea. No se reprodujo un caso de usuario. Sigue siendo un fail-open para
cualquier llamador que invoque `resolve_entidad` directamente —follow-ups,
radar, auditorías— y es la misma familia que
`INV-NO-SILENT-SEMANTIC-FALLBACK` (ADR-069): vocabulario enumerado más
fallback silencioso.

También queda **latente, no reproducido**: `_official_matches_strict_rewrite`
acepta la reescritura `alcaldía de X` si el nombre oficial contiene el token X y
alguno de `municipio|alcaldia|distrito`. `PERSONERIA MUNICIPIO DE VILLA DEL
ROSARIO` satisface las dos condiciones. Hoy no ocurre porque ni Personerías ni
Concejos están en el gazetteer; ocurrirá el día que se agreguen.

---

## 9. `SHARED_NIT_POLICY`

**Prohibido:** `same NIT → auto merge`. Sin excepciones.

Un NIT igual admite cuatro lecturas, y las cuatro se observaron en este lote:

1. `SAME_BUYER_UNIT` — la misma unidad publicada con dos grafías;
2. `SHARED_NIT_DISTINCT_BUYER_UNIT` — unidades operativas distintas bajo una
   identidad fiscal (Bogotá: 41; Villa del Rosario: 3);
3. `SHARED_NIT_BUYER_FAMILY` — unidades distintas que además forman un grupo
   consultable (las 20 alcaldías locales);
4. `REGISTRY_NOISE` — filas de registro sin unidad detrás (personas naturales,
   Policía Nacional bajo el NIT del Distrito).

### Evidencia que permite fusionar dos canonicals en una BuyerUnit

Deben cumplirse **todas**:

1. mismo NIT acreditado, con equivalencia base/DV acotada (no se borran dígitos
   de forma global);
2. **procedencia de dataset disjunta** — un canonical sólo tiene `procesos`, el
   otro sólo `contratos`;
3. las grafías publicadas son iguales tras quitar **exactamente un** signo
   terminal de la más larga;
4. ningún tercer canonical reclama ninguna de las dos cadenas;
5. el nombre institucional, en tokens y sin puntuación terminal, es idéntico.

Este predicado acertó 5 de 5 en el lote y produjo 0 falsos positivos en los 50
NIT más afectados del censo P0.

### Evidencia que obliga a mantener separadas dos unidades

Cualquiera de estas basta:

- ambas transaccionan en el **mismo** dataset con nombres institucionales
  distintos;
- naturaleza institucional distinta (control, corporación, descentralizada)
  aunque compartan NIT;
- territorio distinto;
- el nombre de una es prefijo genérico de una clase (`E.S.E`, `ALCALDIA`) y no
  identifica unidad alguna.

### Pertenencia a un grupo

`Buyer Group membership` **nunca** se infiere de NIT ni de geografía. Se declara,
se acredita con el NIT observado en vivo por miembro, y se versiona.

---

## 10. Bogotá — inventario e inventario de exclusiones

Clasificación por **acreditación**, no por defecto: lo que no está acreditado
como distrital no es miembro, aunque su domicilio sea Bogotá.

| Clase | Unidades | Procesos | Contratos |
|---|---:|---:|---:|
| `ADMINISTRACION_CENTRAL_CANDIDATE` | 19 | 287.491 | 257.304 |
| `LOCAL_MAYORALTY_FDL_CANDIDATE` | 20 | 86.811 | 85.546 |
| `DECENTRALIZED_DISTRICT_ENTITY` | 20 | 228.749 | 178.847 |
| `DISTRICT_COMPANY` | 9 | 39.583 | 29.137 |
| `DISTRICT_HEALTH_ENTITY` (Subredes ESE) | 4 | 156.884 | 253.310 |
| `DISTRICT_CONTROL_OR_CORPORATE_BODY` | 3 | 15.018 | 15.311 |
| `NOT_BOGOTA_ADMINISTRATION` (residuo geográfico) | 127 | 1.389.701 | 954.997 |
| `REVIEW_DIRECTORY_ONLY_SHARED_NIT` | 79 | 0 | 0 |

Las 20 alcaldías locales cubren **las 20 localidades** de Bogotá: el conjunto
está completo, no es una muestra.

Los tres órganos de control/corporativos —Contraloría de Bogotá, Personería de
Bogotá, Veeduría Distrital— se excluyen de la administración por naturaleza
institucional, **no** por NIT: Personería y Veeduría comparten el NIT del
Distrito. Es exactamente el caso que prohíbe usar el NIT como lista de miembros.

Los 79 nombres `REVIEW_DIRECTORY_ONLY_SHARED_NIT` son filas del directorio bajo
el NIT distrital que nunca transaccionan. Incluyen variantes ortográficas de
unidades ya inventariadas (`ALCALDIA LOCAL DE ENGATIVa`), la cadena desnuda
`ALCALDIA` y `ALCALDIA DE BOGOTA`. No se adjudican como unidad ni se descartan
como ruido sin revisión individual.

---

## 11. «Alcaldía de Bogotá»: tres políticas

| | A. Central | B. Central + locales | C. Ecosistema distrital |
|---|---|---|---|
| **Incluye** | 19 secretarías, departamentos administrativos, UAE, Secretaría General | A + 20 alcaldías locales / FDL | B + 20 descentralizadas + 9 empresas + 4 subredes ESE |
| **Excluye** | alcaldías locales, descentralizadas, empresas, ESE, control | descentralizadas, empresas, ESE, control | control (Contraloría, Personería, Veeduría, Concejo) |
| **Unidades** | 19 | 39 | 72 |
| **Procesos** | 287.491 | 374.302 | 799.518 |
| **Contratos** | 257.304 | 342.850 | 804.144 |
| **Riesgo de sorpresa** | omite un tercio de la contratación del ejecutivo distrital | bajo: todo lo incluido se llama «alcaldía» o es secretaría del alcalde | alto: devuelve EAAB, Transmilenio, Metro, Lotería y las subredes hospitalarias |
| **Explicable en UI** | «la administración central» | «la administración distrital y sus 20 alcaldías locales» | requiere enumerar tres familias heterogéneas |
| **Subfiltros** | no los necesita | `solo central` / `solo alcaldías locales` / una localidad | necesarios y numerosos |

### Recomendación: `B_CENTRAL_PLUS_LOCAL` como semántica por defecto

Tres razones, en orden de peso:

1. **A oculta el 23 % de la contratación del ejecutivo distrital.** Las
   alcaldías locales suman 86.811 procesos y ejecutan presupuesto del alcalde a
   través de los fondos de desarrollo local. Un usuario que pide «la alcaldía» y
   no los recibe no tiene forma de saber que faltan.
2. **C mezcla naturalezas que el usuario consulta por nombre propio.** Nadie
   busca «alcaldía de Bogotá» esperando contratos del Acueducto o de la Subred
   Norte —que sola aporta 72.364 procesos, más que 18 de las 19 secretarías—.
   Esas entidades ya son BuyerUnits consultables y deben seguir siéndolo.
3. **Ninguna incluye órganos de control.** Personería, Veeduría, Concejo y
   Contraloría de Bogotá no son la administración, aunque tres de ellos
   compartan su NIT.

Dos condiciones para que la recomendación sea aceptable:

- la respuesta debe **declarar su membresía** («39 unidades: 19 secretarías y
  departamentos administrativos + 20 alcaldías locales»), no aplicarla en
  silencio;
- deben existir subfiltros (`solo central`, `solo alcaldías locales`, una
  localidad concreta) antes de activar el grupo.

**No se implementa en P0.1.**

---

## 12. Modelo de datos candidato

```
BuyerUnit                       # identidad primaria de runtime; ya existe
  canonical_name                # official_name en aliases_db.json
  aliases[]                     # ADR-072: exact / scan por separado
  source_names{dataset: [...]}  # ADR-073: byte-for-byte, igualdad exacta
  fiscal_identity               # NIT + grafías observadas; NO es la clave
  territory

BuyerGroup                      # nuevo; NO duplica BuyerUnits
  id
  display_name
  aliases[]                     # dispara por NOMBRE, jamás por geografía
  members[]                     # referencias a BuyerUnit.canonical_name
  subgroup_members[]            # p. ej. "Alcaldías Locales" dentro de "Distrital"
  provenance                    # artefacto + fecha que acredita CADA miembro
  version
```

Principios obligatorios, todos derivados de evidencia de esta auditoría:

- un BuyerGroup **referencia** BuyerUnits; no las copia ni las reemplaza;
- un grupo **no es alias de un hijo**: `alcaldía de bogotá` no puede resolver a
  la Secretaría General ni a ninguna otra unidad;
- **un NIT no es una lista de miembros** — el NIT distrital incluye la Policía
  Nacional y tres personas naturales;
- una BuyerUnit explícita **siempre gana** al grupo (el guard
  `not entidad_value` ya lo garantiza en `soql_builder`);
- la membresía se versiona y cada miembro lleva su propia procedencia, igual que
  los bindings de `buyer_source_names.json` en schema 2.

Antes de implementar hay que resolver el tope de 15 candidatos (§6).

---

## 13. `MUNICIPAL_M0_IMPACT`

La cadena futura **no puede** ser:

```
DIVIPOLA → municipio → NIT → comprador único
```

Villa del Rosario la rompe con 3 unidades bajo un NIT; Bogotá con 41.

M0 debe poder representar cuatro cosas por municipio:

| Capa | Qué es | Cardinalidad |
|---|---|---|
| `MUNICIPAL_TERRITORY` | DIVIPOLA + campos geográficos | 1 |
| `MUNICIPAL_LEGAL_IDENTITY` | NIT y sus grafías (base, DV, cero guía) | 1..n grafías |
| `MUNICIPAL_BUYER_UNIT` | nombres publicados por ese NIT | 1..n |
| `MUNICIPAL_BUYER_GROUP` | agrupación declarada, sólo si n > 1 | 0..1 |

Para un municipio pequeño puede haber una sola BuyerUnit y ningún grupo. Para
una ciudad grande, decenas.

### Qué debe detectar M0 automáticamente

1. **Todas las grafías del NIT** — base, base+DV y variantes con cero guía. Que
   Bogotá se publique de cuatro formas no es un caso raro.
2. **Todas las unidades compradoras por NIT y por dataset**, con conteos, sin
   deduplicar por similitud.
3. **Divergencias procesos/contratos** que satisfagan el predicado de fusión de
   §9. Si lo satisfacen, proponer `MERGE`; si no, `KEEP_SEPARATE`.
4. **Naturaleza de cada unidad**: `EXECUTIVE` (alcaldía, secretarías),
   `CORPORATE` (concejo), `CONTROL` (personería), `DECENTRALIZED` (institutos,
   empresas, ESE). `alcaldía de X` debe devolver sólo `EXECUTIVE`.
5. **NIT fantasma**: NIT atribuido con cero filas transaccionales. Ibagué prueba
   que existen y que el gazetteer los transporta.
6. **Ruido de registro**: personas naturales y entidades ajenas colgadas del NIT
   municipal.

M0 **no** debe: fusionar por NIT, inferir grupo por DIVIPOLA, ni aceptar
similitud como acreditación de identidad.

---

## 14. Dedup guard revisado

| Condición | Veredicto |
|---|---|
| mismo NIT + misma unidad institucional (predicado §9 completo) | `EXISTING_SAME_BUYER_UNIT` → `MERGE` |
| mismo NIT + variación de grafía de la fuente | `SOURCE_NAME_VARIANT` → binding ADR-073, sin canonical nuevo |
| mismo NIT + unidad institucional distinta | `SHARED_NIT_DISTINCT_BUYER_UNIT` → `KEEP_SEPARATE` |
| mismo nombre + NIT distinto | `REVIEW` — nunca auto-merge (CENAC ×3) |
| sin NIT, sólo similitud | `NO_AUTO_ACCEPT` — falla cerrado |
| NIT con cero filas transaccionales | `PHANTOM_NIT` → `REVIEW`, no acredita identidad |
| pertenencia a un grupo | **nunca** por NIT ni por geografía |

---

## 15. Plan de remediación — NO APLICADO

Orden por daño medido, no por volumen de superficies.

### P1 — compradores inalcanzables (14.517 + 19.039 filas)

1. `MERGE` MinJusticia: 4 canonicals → 1. Los alias humanos deben quedar en el
   canonical cuyos `source_names` sean los nombres realmente publicados.
2. `MERGE` Yopal: 2 canonicals → 1, conservando **ambas** grafías publicadas
   como source names.

### P2 — owner equivocado con filas reales (4.421 filas)

3. `ADD_EXACT_ONLY` `CENAC PUENTE ARANDA+` al canonical del NIT `830039548`.
   Elegibilidad de scan por separado, según ADR-072 / R2D.3.

### P3 — duplicados enmascarados por el `LIKE`

4. `MERGE` Soacha, FFDS y UAESP. No urgentes en resultados, **bloqueantes** para
   ADR-073: no crear bindings antes de fusionar.
5. Revisar el sexto par fuera del lote: `.SECRETARÍA DISTRITAL DE SALUD..` / `.`.

### P4 — alias exactos seguros

6. `ADD_EXACT_ONLY` en los casos 6, 7 y 8/9 (un solo alias cubre 8 y 9).
7. `SOURCE_NAME_BINDING` para el caso 9: `E.S.E  HOSPITAL SAGRADO CORAZÓN DE
   JESÚS` en `p6dx-8zbt`.

### P5 — fail-closed

8. `RESOLUTION_BLOCK` para `alcaldia*` y `e.s.e`, y **cerrar la puerta de §8**:
   la guarda de comprador genérico debe evaluarse sobre la clave sin puntuación
   terminal, no sobre la cadena cruda.

### Sin cambio

9. Los 6 `REGISTRY_NOISE` y los 2 `SHARED_NIT_DISTINCT_BUYER_UNIT`: `NO_CHANGE`
   y `KEEP_SEPARATE`.

### Deuda abierta que esta auditoría no cierra

- `GAZETTEER_PHANTOM_NIT_DEBT` — Ibagué tiene NIT transpuesto en el gazetteer;
  hace falta un censo de NIT sin filas.
- `GAZETTEER_SYNTHETIC_CANONICAL_DEBT` — `MINJUSTICIA.` prueba que existen
  canonicals que SECOP no publica. Falta el censo completo.
- `ECOSYSTEM_SOQL_DISPLAY_DIVERGENCE` — el SoQL mostrado en la respuesta
  asesora puede llevar el `OR` del ecosistema territorial que las filas no
  aplicaron (§5).
- `BUYER_GROUP_CANDIDATE_CAP` — el tope de 15 de `entidad_in_candidates` no
  alcanza para Bogotá.

---

## 16. ADR-074 — `DRAFT`, no aceptado

**Título:** El comprador es la unidad publicada; el NIT no es su clave y el grupo
se declara.

**Estado:** `PROPOSED / DRAFT`. La aceptación se decide después de revisar este
resultado.

**Contexto.** El censo P0 dejó 20 superficies sin adjudicar y reveló que un NIT
puede cubrir varias unidades compradoras. La revalidación en vivo muestra que el
NIT distrital de Bogotá cubre 41 unidades transaccionales y 107 nombres de
directorio, incluidos órganos de control y personas naturales; que el 85,5 % del
territorio de Bogotá no es la administración distrital; y que dos compradores
reales son hoy inalcanzables porque una unidad quedó repartida en varios
canonicals.

**Opciones consideradas.**

1. *Normalizar la puntuación.* Vetada ya por P0 (`KEEP_EXPLICIT_ALIASES`): crea
   colisiones y claves multi-owner. Esta auditoría añade que tampoco atacaría la
   causa —MinJusticia no se arregla quitando puntos—.
2. *Fusionar por NIT.* Falsificada: fusionaría 41 unidades de Bogotá en una y
   arrastraría la Policía Nacional y tres personas naturales.
3. *BuyerUnit primaria + BuyerGroup declarado.* Recomendada.

**Recomendación.**

- `BUYER_UNIT_IS_PRIMARY_RUNTIME_IDENTITY: YES`.
- `BUYER_GROUP_NEEDED: YES`, disparado por nombre, con membresía acreditada y
  versionada, sobre el mecanismo existente `entidad_in_candidates`.
- `SHARED_NIT_POLICY`: prohibido el auto-merge por NIT; fusión sólo con el
  predicado de cinco condiciones de §9.
- `BOGOTA_DEFAULT_GROUP_SEMANTICS`: `B_CENTRAL_PLUS_LOCAL`, con membresía
  declarada en la respuesta y subfiltros.
- Invariante propuesta: **`INV-BUYER-GROUP-MEMBERSHIP-IS-DECLARED`** — ninguna
  pertenencia a un grupo puede derivarse de un NIT ni de un campo geográfico.

**Consecuencias.**

- El gazetteer gana una capa de grupos; no pierde unidades.
- Los 5 duplicados deben fusionarse **antes** de que ADR-073 les cree bindings,
  o el `LIKE` que hoy los enmascara se convierte en cero silencioso.
- `entity_ecosystem.json` (disparo geográfico) queda formalmente desaconsejado
  frente a `entidad_in_candidates` (disparo por nombre).
- Hay que subir o rodear el tope de 15 candidatos antes de declarar el primer
  grupo de Bogotá.

---

## 17. Reproducción y límites

```bash
.venv/bin/python scripts/audits/audit_institutional_buyer_model_p01.py
```

El auditor consulta las tres fuentes vivas, reintenta y falla si Socrata no
responde; no convierte errores de red en ceros. Cada adjudicación declara un
predicado (`PHANTOM_NIT`, `NOT_PUBLISHED_AS_BUYER`, `PUBLISHED_SAME_NIT`,
`DUPLICATE_PAIR`) que se comprueba en vivo; si no se reproduce, el caso degrada
a `REVIEW` y el estado global baja a `PARTIAL`. Los canarios corren por el
`QueryRouter` real con persistencia redirigida a un temporal
(`isolated_runtime_stores`).

**Límites declarados.**

- El inventario de Bogotá se acredita sobre nombres conocidos por el gazetteer
  más una lista distrital verificada; no es un censo exhaustivo del universo
  distrital.
- El bucket `NOT_BOGOTA_ADMINISTRATION` es un residuo geográfico, no una
  adjudicación entidad por entidad.
- La firma `PROC_LONGER_BY_1` se contrastó sobre los 50 NIT más afectados del
  censo P0, no sobre los 603 canonicals.
- El bypass de puntuación de §8 y el riesgo de reescritura municipal no se
  reprodujeron a nivel de pipeline; se reportan como defectos del resolver.

**No se modificaron** `aliases_db.json`, `buyer_source_names.json`,
`normalize_text`, `EntityResolver`, `SoQLBuilder` ni ningún archivo de runtime.

| | |
|---|---|
| `RUNTIME_CHANGED` | NO |
| `ALIASES_CHANGED` | NO |
| `SOURCE_BINDINGS_CHANGED` | NO |

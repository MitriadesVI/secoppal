# Semántica de negación dimensional

**Estado:** diseño aprobado · binding **R4-S1 implementado** · OBJECT end-to-end
**R4-S2 implementado** · safe policies **R4-S3 implementadas**; lifecycle diferido a
R4-S4 · decisión canónica en
[`ADR-064`](../decisions_log.md)
**Rama de diseño:** `design/hca-negation-semantics` (hija de
`fix/hca-normalization-contract`, `9ecf3be`)
**Última actualización:** 2026-08-25

> Esta guía conserva el diagnóstico y contrato de diseño originales. R4-S1
> implementó la detección compartida, `NegationRequest`, aislamiento de spans y
> transporte efímero fuera de `QueryFrame`; no implementó las políticas de
> exclusión listadas para los sprints posteriores. La evidencia conversacional
> viene de
> [`docs/audits/2026-08-22_CONVERSATIONAL_SYSTEM_HOLISTIC_AUDIT.md`](../audits/2026-08-22_CONVERSATIONAL_SYSTEM_HOLISTIC_AUDIT.md)
> (HCA-1) y de sondeos directos al parser reproducidos abajo. El corpus congelado
> `tests/fixtures/audits/conversation_holistic_v1.yaml`
> (`sha256 262c225c…99ffba`) no se toca.
>
> R4-S2 ejecutó únicamente OBJECT mediante la autoridad existente
> `objeto_exclude`, corrigió su proyección NULL-safe y añadió contradicción
> fail-closed. Evidencia de cierre:
> [`2026-08-25_HCA_R4_S2_OBJECT_NEGATION.md`](../audits/2026-08-25_HCA_R4_S2_OBJECT_NEGATION.md).
>
> R4-S3 convirtió las dimensiones no soportadas a `CLARIFY`, sin llamadas a
> SECOP ni side effects de actor/entidad, y volvió transaccional la aclaración
> de follow-up. DEPARTMENT sólo ejecuta por resta de un `departamentos_in`
> finito y canónico ya existente; no se añadió predicado negativo al builder.
> Evidencia de cierre:
> [`2026-08-25_HCA_R4_S3_SAFE_NEGATION_POLICIES.md`](../audits/2026-08-25_HCA_R4_S3_SAFE_NEGATION_POLICIES.md).

---

## 1. El problema no es "no soportamos negación"

SECOPPAL **sí** ejecuta negación: la modalidad tiene un subsistema de exclusión
completo y correcto. El problema es que ese subsistema es el **único** que
entiende que un marcador negativo pertenece a una dimensión, y todas las demás
dimensiones tratan el marcador como ruido léxico.

El resultado no es "la exclusión se ignora". Es peor: el valor negado sobrevive
como **filtro positivo**, porque los extractores estructurados corren antes que
cualquier lectura de polaridad y reclaman el valor sin mirar qué lo gobierna.

Sondeo directo sobre `QueryRouter.parse` (HEAD `9ecf3be`, sin red):

```text
"contratos de aseo que no sean de barranquilla"
  → ciudad=Barranquilla
    entidad_resolved=DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA
    objeto=[aseo]
```

La negación no solo se invierte: activa además el mapeo `_CITY_TO_ENTITY`
(`app/core/query_router.py:323`), así que el usuario que pidió *excluir*
Barranquilla recibe una consulta **restringida al comprador de Barranquilla**.
La inversión es doble.

```text
"contratos de aseo en atlantico menos barranquilla"
  → departamento_resolved=Atlántico
    ciudad=Barranquilla
    entidad_resolved=DISTRITO ESPECIAL … DE BARRANQUILLA
```

El usuario pidió *Atlántico sin Barranquilla* y recibe *exactamente Barranquilla
dentro de Atlántico*: el complemento exacto de su intención.

```text
T1 "contratos de aseo en atlantico"      → departamento_resolved=Atlántico
T2 "menos barranquilla"                  → intent=change_scope
                                            ciudad=Barranquilla
                                            entidad_resolved=DISTRITO … BARRANQUILLA
                                            (Atlántico DESAPARECE)
```

En seguimiento el daño es máximo: la exclusión se clasifica como *cambio de
alcance* usando como evidencia el valor que el usuario acababa de rechazar, y
`normalize_scope_transition` borra el alcance positivo legítimo.

```text
T1 "contratos de alimentacion en barranquilla"
T2 "sin alimentacion"    → intent=contextual_requery, frame IDÉNTICO al de T1
```

Aquí no hay inversión sino **no-op silencioso**: el turno del usuario no cambia
nada y nadie se lo dice.

```text
"contratos donde el proveedor no sea union temporal aseo"
  → {dataset: contratos}     ← todo lo demás desaparece
```

Drop total: la consulta ejecuta *todos los contratos de Colombia*.

**Conclusión:** la negación falla de tres maneras distintas —inversión, no-op y
drop— y las tres comparten la misma raíz: **no existe un paso que ate un
marcador negativo a una dimensión antes de decidir qué hacer con él.**

---

## 2. Capacidades negativas existentes

Inventario verificado sobre HEAD `9ecf3be`.

| DIMENSION | POSITIVE_FIELD | NEGATIVE_FIELD_EXISTS | CURRENT_RUNTIME_SUPPORT | PROJECTION_SUPPORT | FOLLOWUP_SUPPORT | NOTES |
|---|---|---|---|---|---|---|
| **OBJECT/TOPIC** | `objeto`, `objeto_or`, `objeto_phrases` | **sí** — `objeto_exclude` | **parcial**: solo la regex `\b(?:excluye\|excluir\|excepto)\s+(.+?)\s*$` anclada al final (`query_router.py:929`) | **sí** — `NOT (… OR …)` con expansión morfológica (`soql_builder.py:429`) | **sí** — intent `exclude_topic` + `MutationPolicy({objeto_exclude})` | no reconoce `sin`; no reconoce subjuntivo (`excluyan` → fuga a `objeto`); `NOT(...)` no es null-safe; no detecta contradicción |
| **CITY** | `ciudad` | no | no | `unaccent(UPPER(ciudad)) LIKE '%x%'` (subcadena) | `SCOPE_FIELDS` → *reemplaza* | sin gazetteer municipal; sin canonicalización; acoplada a `_CITY_TO_ENTITY` |
| **DEPARTMENT** | `departamento_resolved`, `departamentos_in` | no | no | `departamento = 'Atlántico'` (igualdad, vocabulario cerrado) | `SCOPE_FIELDS` | 34 valores catalogados, incluido el centinela `No Definido` |
| **BUYER ENTITY** | `entidad_resolved`, `entidad_like` | no | no | igualdad / LIKE sobre alias | `SCOPE_FIELDS` | homónimos y ecosistemas de entidad |
| **ACTOR/PROVIDER** | `contratista` | no | no | igualdad o OR sobre `documento_proveedor` | `FILTER_FIELDS` | hoy: drop total del turno |
| **MODALITY** | `modalidad_include_procedure_ids` | **sí** — `modalidad_exclude_procedure_ids` | **completo** | `modalidad_de_contratacion NOT IN (…)` (`soql_builder.py:694`) | **sí** — álgebra propia (`modality_followup.py`: `ADD_EXCLUDE`, `REMOVE_EXCLUDE`) | **AUTORIDAD DE REFERENCIA** |
| **CONTRACT TYPE** | `contract_type_include` | no | no | cláusula propia | `CONTRACT_TYPE_FIELDS` | catálogo cerrado ⇒ barato de añadir después |
| **STATE** | `estado_*` (4 campos) | no | no | igualdad / `IN` | `STATE_FIELDS` | catálogo cerrado |
| **TIME** | `fecha_desde`, `fecha_hasta` | no | no | rango cerrado único (`>= AND <=`) | `change_year` | un solo rango **no puede** expresar un hueco |
| **MONEY** | `valor_min`, `valor_max` | **no aplica** | **sí, por polaridad** (ADR-063) | `>=` / `<=` | `MONEY_FIELDS` | `no mayores de 500 millones` → `valor_max`, correcto hoy |

### 2.1 Por qué la modalidad sí funciona

`procesos de obra sin contratacion directa` produce hoy
`modalidad_exclude_procedure_ids=[contratacion_directa, contratacion_directa_con_ofertas]`
y `objeto=[obra]`. Correcto. Tres propiedades lo explican, y las tres son
reutilizables:

1. **Vocabulario cerrado.** La modalidad se resuelve contra
   `modality_catalog_v1.yaml`. Una exclusión es un conjunto de `procedure_id`,
   no una cadena.
2. **Disciplina de gap.** `has_external_negative_operator`
   (`modality_alternatives.py:106`) evalúa marcadores negativos **solo en el
   texto entre spans ya resueltos**, nunca dentro del alias. Por eso
   `"contratación directa sin ofertas"` —alias real,
   `modality_lexicon_v1.yaml:137`— resuelve a *inclusión* y no a exclusión.
3. **Fail-closed ante contradicción.** `ModalityFilter.__post_init__` **lanza
   `ValueError`** si `include ∩ exclude ≠ ∅`. La contradicción es un error, no
   un resultado vacío.

`objeto_exclude` tiene (1) a medias —vocabulario abierto—, no tiene (2) y **no
tiene (3)**.

### 2.2 La arbitración actual es accidental

`contratos de vigilancia excepto minima cuantia` no produce
`objeto_exclude=[minima, cuantia]` únicamente porque la modalidad se resuelve en
el paso **1.c** del parser y consume su span antes de que la regex de exclusión
corra en el paso **11**. Nadie decidió esa prioridad como contrato; es el orden
del archivo.

La prueba de que es frágil está en el mismo pipeline: la ciudad se detecta en el
paso **10.5** y también consume su span, así que
`"contratos de aseo excepto en barranquilla"` deja la cola de `excepto` vacía y
el resultado es `ciudad=Barranquilla` **positivo**. El mismo mecanismo que salva
la modalidad rompe la geografía.

---

## 3. Principio central

### `INV-NEGATION-PRESERVES-DIMENSION`

> Un marcador de exclusión debe resolverse a una **petición de negación tipada**
> antes de cualquier decisión sobre ejecutabilidad. La petición conserva cuatro
> cosas: **dimensión** negada, **valor** negado, **alcance** dentro del cual se
> niega, y **veredicto de ejecutabilidad**.

Forma canónica propuesta (aún no es código):

```text
NegationRequest(
    dimension:   OBJECT | CITY | DEPARTMENT | BUYER | ACTOR | MODALITY | CONTRACT_TYPE | STATE | TIME
    value_span:  el texto gobernado por el marcador, sin normalizar todavía
    scope_anchor: el filtro positivo que contiene la exclusión (o None)
    marker:      el operador literal encontrado ("sin", "que no sean", "menos", …)
    verdict:     EXECUTABLE | CLARIFY | CONTRADICTORY
)
```

Consecuencias que el invariante prohíbe explícitamente:

- `"sin alimentación"` **no** puede volverse una lista genérica de términos
  negativos sin saber que pertenece a OBJECT.
- `"que no sean de Barranquilla"` **no** puede volverse `objeto_exclude`.
- Ninguna dimensión puede recibir un valor **positivo** derivado de un span
  gobernado por un marcador negativo.

### `INV-NO-SILENT-NEGATION-DROP`

> Si el usuario usa un marcador inequívoco de exclusión sobre un span resoluble
> y SECOPPAL no puede representar esa exclusión, el turno **no se ejecuta como
> si el marcador no existiera**. Se aclara o se explica. Nunca se convierte en
> positivo; nunca se descarta en silencio.

Ejecutar la parte positiva e ignorar la exclusión **no cumple** el invariante:
devuelve un superconjunto que contiene precisamente las filas que el usuario
rechazó, sin decirlo. Es la falla que HCA-1 reportó como P1.

Hermano de `INV-NORMALIZATION-PRESERVES-INTENT` (ADR-063): allí la normalización
monetaria borraba roles direccionales; aquí la extracción estructurada borra
polaridad. Mismo patrón raíz —
`NORMALIZATION_ERASES_USER_INTENT_BEFORE_POLICY` — distinta dimensión.

---

## 4. Taxonomía de negaciones

Seis clases del encargo, más una séptima que la evidencia obliga a separar.

### A. `VALUE_EXCLUSION` — vocabulario abierto

`sin alimentación`. La dimensión existe y admite negar un valor, pero el valor
no pertenece a ningún catálogo: se proyecta por coincidencia de subcadena. La
exclusión es tan difusa como la inclusión.

Dimensión afectada: **OBJECT**.

### B. `SET_EXCLUSION` — vocabulario cerrado

`excepto contratación directa`. Se retira un miembro de una familia finita y
enumerable. La exclusión es exacta, completa y verificable contra el catálogo.

Dimensiones afectadas: **MODALITY** (ya soportada), **CONTRACT TYPE**,
**STATE**, **DEPARTMENT**.

### C. `SCOPE_EXCLUSION` — resta territorial

`que no sean de Barranquilla`. Niega un ámbito. Su significado depende de si
existe un alcance positivo que la contenga:

- **con ancla** (`en Atlántico menos Barranquilla`) → resta acotada, resultado
  finito y comprensible;
- **sin ancla** (`que no sean de Barranquilla`) → complemento nacional. No es
  una consulta: es "todo Colombia menos una ciudad", con el mismo problema de
  amplitud que P15 ya bloquea para oportunidades.

Dimensiones afectadas: **CITY**, **DEPARTMENT**, **BUYER**.

### D. `TEMPORAL_EXCLUSION` — hueco

`menos los de 2025`. No es un rango: es un rango **con un agujero**. Ver §8.

### E. `CONTRADICTORY_NEGATION`

`alimentación pero sin alimentación`. El mismo valor aparece positivo y negativo
en la misma dimensión. Hoy el resultado depende del orden de los parsers, que es
exactamente lo que `INV-NORMALIZATION-PRESERVES-INTENT` prohíbe.

### F. `UNSUPPORTED_NEGATION`

La fuente o la proyección no pueden expresar la exclusión con seguridad. Debe
aclarar o explicar; nunca ignorar.

### G. `POLARITY_INVERSION` — **no es una exclusión**

`no mayores de 500 millones` → `valor_max=500000000`. Las dimensiones ordenadas
niegan **invirtiendo el comparador**, no excluyendo un conjunto. Ya funciona
(HCA-NEG-004 PASS) y ADR-063 lo protege.

Separarla es un requisito de seguridad, no una sutileza taxonómica: el marcador
`menos` es compartido. `por menos de 500 millones` (comparador, HCA corpus
línea 441, PASS) y `menos los de 2025` (exclusión) usan la misma palabra. Un
detector de negación que capture `menos` sin respetar spans ya consumidos
**rompe el filtro monetario**.

---

## 5. Caso OBJECT

```text
"contratos de la gobernacion de sucre sin alimentacion"
  → entidad_resolved=GOBERNACION DE SUCRE   ✅
    objeto=[alimentacion]                   ❌ debía ser objeto_exclude
```

### 5.1 Estado de la ruta

`objeto_exclude` **ya llega hasta SoQL y ya sobrevive al follow-up**. La cadena
completa existe y está verificada:

```text
parser (regex "excluye|excluir|excepto" al final)
  → params["objeto_exclude"]
  → frame_from_params / params_from_frame        (query_frame.py:79,108)
  → classify_control_intent → "exclude_topic"    (followup_control_tokens.py:78)
  → MutationPolicy({objeto_exclude})             (followup_mutations.py:100)
  → _merge_refine_filter (unión con el previo)   (followup_engine.py:703)
  → SoQL: NOT (… OR …)                           (soql_builder.py:429)
```

Verificado en vivo: T1 `contratos de alimentacion en barranquilla` + T2
`excluye alimentacion` → `intent=exclude_topic`, `objeto_exclude=[alimentacion]`
persistido.

**Solo falta el disparador lingüístico.** Ese es el arreglo mínimo.

### 5.2 Los tres huecos reales

**(a) Vocabulario del marcador.** La regex solo acepta `excluye|excluir|excepto`
y solo anclada al final. No acepta `sin`, `que no sean`, `salvo`, `excluyendo`,
ni el subjuntivo:

```text
"contratos que excluyan alimentacion" → objeto=[excluyan, alimentacion]
```

El verbo fuga al objeto (familia `HCA-TOPIC-VERB-001`) y la exclusión se pierde.

**(b) `NOT (…)` no es null-safe.** La cláusula generada es:

```sql
NOT (unaccent(UPPER(objeto_del_contrato))    LIKE '%comedor%'
  OR unaccent(UPPER(descripcion_del_proceso)) LIKE '%comedor%'
  OR … )        -- 6 variantes morfológicas × 2 campos = 12 LIKE
```

Con lógica de tres valores, si uno de los dos campos es NULL y el otro no
coincide, la disyunción vale `NULL`, `NOT NULL` vale `NULL`, y **la fila se
elimina aunque no contenga el término excluido**. Es un defecto latente del
mecanismo *actual*, no del propuesto. La forma correcta es explícita:

```sql
NOT ( COALESCE(unaccent(UPPER(objeto_del_contrato))    LIKE '%x%', false)
   OR COALESCE(unaccent(UPPER(descripcion_del_proceso)) LIKE '%x%', false) )
```

HCA-R4.1 verificó esta forma directamente en SoQL. En vivo, los cuatro campos
reales tienen 6 NULL en total —todos en Procesos— y 0 vacíos: la forma actual
borró las 6 filas para un término ausente; `COALESCE` preservó 6/6 y siguió
excluyendo los controles que sí contenían el término.

**(c) La exclusión hereda la expansión morfológica.** `objeto_exclude=[alimentacion]`
excluye también `comedor`, `nutricion`, `alimentos`. Para una inclusión eso es
recall deseable; para una exclusión es un borrado más amplio que lo pedido. La
decisión de diseño es **conservar la expansión** (simetría con el positivo, y el
usuario que dice "sin alimentación" no quiere ver comedores escolares) pero
**declararla en la traza y en la respuesta**: "excluí alimentación (y sus
variantes: comedor, nutrición, alimentos)". Silencio aquí sería otra forma de
drop.

### 5.3 Ruta mínima de diseño

```text
lenguaje crudo
  → resolución de dimensiones cerradas primero (modalidad, tipo, estado,
    depto, ciudad, entidad, fechas, dinero) — consumen su span
  → detección de marcadores SOLO en el texto residual (disciplina de gap)
  → binding marcador → dimensión del span que gobierna
  → residuo libre gobernado por marcador ⇒ dimensión = OBJECT
  → NegationRequest(OBJECT, span, …)
  → objeto_exclude
  → SoQL NOT(...) null-safe
```

**OBJECT es el destino por defecto del residuo, no el destino por defecto de la
negación.** La diferencia es todo el invariante.

### 5.4 Estado de implementación R4-S2

La ruta mínima anterior está ejecutada. Todos los marcadores OBJECT resueltos
por S1 se proyectan a `objeto_exclude`; `NegationRequest` se descarta como
metadata efímera y no se añadió ninguna representación runtime paralela. La
exclusión usa `coalesce(<LIKE>, false)` por campo antes del `NOT`. Un overlap
entre la proyección positiva y negativa existente produce aclaración antes de
consultar SECOP. CITY, TIME, BUYER y ACTOR continúan fail-closed sin ejecución.

---

## 6. Caso GEOGRAPHY

```text
"contratos de aseo que no sean de barranquilla"
```

Esto **no** equivale a `NOT objeto LIKE '%barranquilla%'` —esa fue la tentación
que HCA-1 documentó. Pero tampoco equivale de forma segura a
`ciudad != 'Barranquilla'`.

### 6.1 Lo que realmente proyecta el runtime

```sql
-- positivo, hoy
unaccent(UPPER(ciudad)) LIKE unaccent(UPPER('%Barranquilla%'))
```

**Subcadena, no igualdad.** Y no hay gazetteer municipal en el repo:
`app/data/colombia_geography.py` solo tiene departamentos y regiones; el
reconocimiento de ciudad es una regex de ~25 nombres literales
(`query_router.py:328`). No existe `ciudad_resolved` canónico.

La asimetría es decisiva:

- un LIKE positivo demasiado ancho devuelve filas de más — el usuario las ve y
  las descarta;
- un `NOT LIKE` demasiado ancho **borra filas correctas de forma invisible**.

Con el vocabulario de ciudades que el parser ya reconoce, el riesgo es concreto:
`NOT LIKE '%cali%'` elimina también Calima, San Calixto y California;
`NOT LIKE '%san andres%'` elimina `San Andrés Sotavento` y todos los valores
exactos `San Andrés`, incluidos los que la fuente diferencia solo por
departamento (Archipiélago, Santander y Antioquia). `NOT LIKE '%cartagena%'`
elimina además `Cartagena Del Chairá`.

### 6.2 ¿`NOT ciudad = 'Barranquilla'` debe incluir filas con ciudad NULL?

No se decide por intuición. HCA-R4.1 midió directamente los campos CITY de la
fuente en vivo. NULL y vacío fueron 0; la ausencia se codifica exactamente como
el centinela `No Definido`:

| dataset | campo | valor centinela | filas | % del total vivo |
|---|---|---|---|---|
| `p6dx-8zbt` (procesos) | `ciudad_entidad` | `No Definido` | 2 154 643 | **23,79 %** |
| `jbjy-vk9h` (contratos) | `ciudad` | `No Definido` | 1 286 689 | **21,52 %** |

No aparecieron otras variantes de centinela entre los valores agrupados. Como
referencia separada, DEPARTMENT dio 582 818 (6,44 %) y 120 469 (2,01 %): la
cifra de diseño 6,61 % / 2,01 % solo se reproduce, al redondear, en Contratos.

Consecuencias, y son opuestas entre sí:

- **NULL**: `NOT (x LIKE …)` con `x` nulo ⇒ `NULL` ⇒ **fila eliminada**. Falso
  negativo silencioso.
- **`No Definido`**: es una cadena real, no coincide con el patrón, ⇒ **fila
  incluida**. El usuario recibe filas cuya ubicación se desconoce y que
  *podrían* ser justo las que excluyó.

**Política documentada (no ejecutada en V1):** una exclusión geográfica debe
declarar su tratamiento del desconocido, y el default correcto es
**incluir el desconocido y decirlo**: "excluí Barranquilla; N resultados tienen
territorio sin definir y se conservaron". Excluir el desconocido convertiría
una resta de ciudad en la resta pedida más **21–24 %** del universo; para
DEPARTMENT, el riesgo medido es 2–7 %. Ambos son exactamente el drop silencioso
que el invariante prohíbe.

### 6.3 `en Atlántico menos Barranquilla`

Combina alcance positivo de departamento con exclusión de ciudad. Es la forma
*más* natural y la que hoy produce el peor resultado (§1). El ancla positiva
resuelve el problema de amplitud, pero **no** resuelve el de proyección: la
resta sigue siendo un `NOT LIKE` sobre una columna no canonicalizada.

### 6.4 Departamento sí es distinto

`departamento_resolved` proyecta **igualdad sobre vocabulario cerrado**
(`departamento = 'Atlántico'`, 34 valores catalogados). Y cuando el alcance
positivo es un conjunto explícito (`departamentos_in`, búsqueda regional), la
exclusión ni siquiera necesita un predicado negativo: se resuelve **restando de
la lista positiva antes de construir el SoQL**.

```text
"en la costa caribe menos atlántico"
  → departamentos_in = <caribe> − {Atlántico}
  → departamento IN ('Bolívar','Cesar',…)      -- sigue siendo positivo
```

Cero riesgo de NULL, cero riesgo de subcadena, cero cambio en `SoQLBuilder`.
**Es la exclusión más barata y más segura del sistema.**

---

## 7. Caso MODALITY

El mecanismo existente **es autoridad reutilizable, pero solo en parte.**

**Reutilizable — el vocabulario y la disciplina.** `_EXTERNAL_NEGATIVE_OPERATORS`
y `_ANTI_NEGATION_PATTERNS` (`modality_alternatives.py:29-37`) no tienen nada
específico de modalidad: son español. Deben extraerse a un módulo compartido
—`app/core/negation_markers.py`, pequeño y de una sola responsabilidad— que
`modality_alternatives` importe **sin cambio de comportamiento**. Duplicar esa
lista sería garantizar deriva entre dimensiones.

La lista de anti-negación necesita crecer con evidencia de dominio:

```text
"contratos con entidades sin animo de lucro"
  → objeto=[entidades, animo, lucro]
```

`sin ánimo de lucro` es terminología jurídica colombiana (ESAL), no una
exclusión. Un detector ingenuo de `sin` produciría
`objeto_exclude=[animo, lucro]`: mal en la dimensión y mal en el valor. Otras
del mismo tipo, presentes en objetos reales de SECOP: `sin fórmula de reajuste`
(`secop_field_values.json:930`), `sin perjuicio de`.

**No reutilizable — el álgebra.** `modality_followup.py` (≈490 líneas) opera
sobre conjuntos de `procedure_id` con `ADD_EXCLUDE`/`REMOVE_EXCLUDE`,
reexpansión de familias y detección de `sin` como token de control según el
estado del filtro previo. Eso es específico de un catálogo jerárquico. OBJECT no
lo necesita: su merge ya funciona vía `_merge_refine_filter`.

**Veredicto: `MODALITY_NEGATION = EXISTING`.** No se toca su comportamiento. La
única intervención admisible es mover la lista de marcadores a un módulo
compartido, con pruebas que fijen equivalencia exacta.

**Lo que sí debe copiarse a OBJECT es el fail-closed:** `ModalityFilter` lanza
`ValueError` ante `include ∩ exclude ≠ ∅`; `objeto` + `objeto_exclude` con el
mismo valor produce hoy, en silencio, una consulta insatisfacible (§12).

---

## 8. Caso TIME

```text
T1 "contratos de aseo en valledupar"
T2 "menos los de 2025"   → intent=change_year
                            fecha_desde=2025-01-01
                            fecha_hasta=2025-12-31
```

El sistema devuelve **exactamente el complemento** de lo pedido, y el título de
la respuesta lo afirma.

### A. ¿Un solo rango puede representar "todos menos 2025"?

**No.** `fecha_desde`/`fecha_hasta` describen un intervalo convexo. El
complemento de un intervalo dentro de otro intervalo es una **unión de dos
intervalos**. Es una limitación de la representación, no de la fuente.

### B. ¿Debe existir `temporal_exclude`?

Sería representable en SoQL:

```sql
(fecha_de_firma < '2025-01-01' OR fecha_de_firma > '2025-12-31')
```

Pero el propio `SoQLBuilder` documenta la razón para no hacerlo a la ligera
(`soql_builder.py:505-508`): una `OR` entre columnas de fecha en `p6dx-8zbt`
tiene *"severe planner penalty"*, y por eso la política de oportunidades se
reescribió como un único `case(...)`. El gate vigente de Etapa 6 es
**p90 < 15 s**; **11,6 s** fue el resultado observado en la corrida de cierre,
no el umbral. Introducir disyunciones temporales sin medir es arriesgar ese
presupuesto de latencia por una construcción de uso minoritario.

Además el coste de diseño escala mal:

```text
"desde 2024 menos 2025"           → 2 intervalos
"entre 2024 y 2026 excepto 2025"  → 2 intervalos
"los de 2026 menos enero"         → 2 intervalos, granularidad mes
"menos 2025 y 2023"               → 3 intervalos
```

Sostener eso correctamente es un **álgebra de intervalos**: normalización,
fusión, granularidad mixta, vacío, y su presentación al usuario. HCA-1 ya
reporta que el parser temporal falla en casos *positivos* mucho más simples
(`HCA-TIME-001`: rango de meses colapsa al último mes; `HCA-TIME-002`:
relativos civiles descartados en silencio; `HCA-TIME-004`: `desde 2025` ≡
`en 2025`). **Construir un álgebra de huecos sobre un parser de rangos que aún
no es correcto es invertir en el orden equivocado.**

### C. ¿Aclarar y fallar cerrado en V1?

Sí.

**`TEMPORAL_NEGATION_V1 = CLARIFY_UNSUPPORTED`.**

El arreglo mínimo obligatorio no es soportar el hueco: es **dejar de producir el
complemento exacto**. Un marcador de exclusión que gobierna un span temporal
debe impedir que ese span se convierta en `fecha_desde`/`fecha_hasta` positivos,
y el turno debe aclarar. El oráculo del corpus congelado ya pide exactamente
eso, y nada más: `{q: "menos los de 2025", absent: [fecha_desde]}`.

Precondición para reabrir: cerrar `HCA-TIME-001/002/003/004` y medir el coste
real de una disyunción de fechas contra el gate de latencia.

---

## 9. BUYER / ACTOR

Son dimensiones distintas y no admiten un `entity_exclude` genérico.

```text
"contratos que no sean de la alcaldia de barranquilla"
  → entidad_resolved=DISTRITO ESPECIAL … DE BARRANQUILLA    ❌ invertido
    (objeto vacío: la consulta queda = "todo lo de la entidad excluida")

"contratos donde el proveedor no sea union temporal aseo"
  → {dataset: contratos}                                     ❌ drop total
```

**BUYER** (quién compra) y **ACTOR** (quién vende) tienen resolutores, riesgos y
proyecciones diferentes:

| | BUYER | ACTOR |
|---|---|---|
| campo | `nombre_entidad` / `entidad` | `proveedor_adjudicado`, `documento_proveedor` |
| resolución | gazetteer de alias + ecosistemas | índice FTS5 + NIT/DV (ADR-042, ADR-060) |
| identidad | ecosistema: excluir "Alcaldía de Barranquilla" ¿excluye sus secretarías? | UT/consorcios: excluir un integrante ¿excluye la UT? (Riesgo D abierto) |
| NULL | raro | frecuente: procesos no adjudicados |

Un `entity_exclude` común colapsaría dos preguntas de identidad no resueltas.
La de actor está explícitamente abierta en el perfil de contratista (integrantes
de UT). **Ambas se difieren.**

Recomendación adicional, independiente del soporte: **desacoplar
`_CITY_TO_ENTITY` de la detección de ciudad cuando hay marcador negativo**. Ese
mapeo convierte una exclusión geográfica en una restricción de comprador y
duplica el daño. Es de una línea y no depende de que se implemente exclusión
alguna.

---

## 10. Alcance de la negación (binding)

No se intenta análisis sintáctico universal. Se define una regla de asociación
suficiente para no negar la dimensión equivocada.

**Regla de gap (heredada de la modalidad):** un marcador gobierna el span
resuelto o el residuo que lo *sigue inmediatamente*, sin cruzar otro span ya
resuelto.

| frase | lectura | resultado esperado |
|---|---|---|
| `contratos de alimentación sin transporte` | positivo + exclusión | `objeto=[alimentacion]`, `objeto_exclude=[transporte]` |
| `contratos sin alimentación ni transporte` | dos exclusiones (`ni` propaga) | `objeto_exclude=[alimentacion, transporte]` |
| `sin alimentación en Barranquilla` | exclusión + positivo | `objeto_exclude=[alimentacion]`, `ciudad=Barranquilla` |
| `no de alimentación sino de transporte` | negación + reemplazo | `objeto=[transporte]`, `objeto_exclude=[alimentacion]` |
| `contratación directa sin ofertas` | alias interno | inclusión de modalidad, **no** exclusión |

Dos reglas explícitas:

1. **`ni` propaga el marcador anterior** a los términos siguientes de la misma
   dimensión. Sin ella, `sin alimentación ni transporte` excluye solo el primero.
2. **`sino` cierra la negación y abre un positivo.** El span tras `sino` es
   positivo aunque siga a un marcador.

**Estas dos formas producen hoy params idénticos** —`objeto=[alimentacion,
transporte]` en ambos casos— y ese es el test anti-metamórfico N5≠N6 (§15): dos
consultas de significado opuesto no pueden colapsar al mismo frame.

Fuera de alcance V1: negación de constituyentes coordinados complejos,
negación con cuantificadores, doble negación (`no sin`).

---

## 11. Política de follow-up

**Regla estructural, la más importante del bloque:**

> Un valor gobernado por un marcador negativo **nunca** puede servir de evidencia
> para clasificar la intención del turno.

Hoy `menos barranquilla` se clasifica `change_scope` **usando Barranquilla como
el nuevo alcance**, y `normalize_scope_transition` borra Atlántico. La
clasificación debe correr sobre el **residuo positivo**; si el residuo positivo
está vacío, el turno es de exclusión, no de cambio de alcance.

Comportamientos definidos:

| T1 | T2 | Hoy | Diseño V1 |
|---|---|---|---|
| `alimentación en Barranquilla` | `sin alimentación` | `contextual_requery`, frame idéntico (**no-op silencioso**) | **CONTRADICCIÓN** → aclarar, no ejecutar. El único tema positivo es el que se niega: `objeto ∧ ¬objeto` es insatisfacible. Pregunta: *"¿quieres cambiar de tema, o quitar alimentación de lo que ya buscaste?"* |
| `aseo en Atlántico` | `menos Barranquilla` | `change_scope` → Atlántico se pierde, ciudad=Barranquilla | **CLARIFY**: la exclusión de ciudad no es ejecutable (§6). El frame previo **se conserva intacto**; nunca se reemplaza el alcance por el valor negado |
| `aseo en Valledupar` | `menos los de 2025` | `change_year` → complemento exacto | **CLARIFY** (§8). Sin filtro de fecha nuevo; el frame previo se conserva |
| `vigilancia` | `excepto mínima cuantía` | `refine_filter` + `modalidad_exclude` ✅ | **sin cambio** |
| `alimentación en Barranquilla` | `excluye alimentación` | `exclude_topic`, `objeto` y `objeto_exclude` con el mismo valor → **0 filas garantizadas, en silencio** | **CONTRADICCIÓN** → aclarar |

Reglas fail-closed adicionales:

1. **Una exclusión no ejecutable en seguimiento no es un no-op.** Produce un
   turno de aclaración y deja el frame anterior sin tocar. Hoy N10 no cambia
   nada y no avisa: viola `INV-NO-SILENT-NEGATION-DROP`.
2. **Una política de mutación de exclusión solo puede tocar su campo negativo.**
   `MutationPolicy({objeto_exclude})` ya lo hace. Si más adelante llega
   `exclude_scope`, mismo patrón: nunca campos positivos. Esa restricción es la
   que impide estructuralmente el desastre de `menos barranquilla`.
3. **Una exclusión sin alcance positivo previo ni en el turno** (`menos
   Barranquilla` como primer turno) es demasiado amplia. Precedente en el
   código: P15 ya bloquea oportunidades sin alcance.
4. **Las exclusiones deben poder retirarse.** `refinement_recovery.py:25` ya
   etiqueta `objeto_exclude` como *"exclusión"* de cara al usuario, pero
   `requested_clear_fields` (`followup_control_tokens.py:60`) no tiene slot para
   ella: hoy una exclusión heredada **no se puede quitar** por lenguaje. Añadir
   `quita la exclusión` es barato y cierra el ciclo de vida.

---

## 12. Conflictos

```text
alimentación   +  sin alimentación
Barranquilla   +  no Barranquilla
2025           +  menos 2025
```

**Política: `CONTRADICTION → CLARIFY`, fail-closed, en V1.**

Definición precisa: hay contradicción cuando el **mismo valor**, en la **misma
dimensión**, aparece simultáneamente en el conjunto positivo y en el negativo,
tras normalización — incluida la expansión morfológica, para que
`alimentación` + `sin comedor` se evalúe con el mismo criterio.

Justificación de que no la decida el orden de los parsers:

- hoy `contratos de alimentacion pero sin alimentacion` →
  `objeto=[alimentacion, pero]`: el segundo `alimentacion` se deduplica, la
  contradicción desaparece y hasta el conector `pero` fuga al objeto;
- hoy `objeto=[alimentacion]` + `objeto_exclude=[alimentacion]` genera
  `(… LIKE '%alimentacion%' …) AND NOT (… LIKE '%alimentacion%' …)`: cero filas
  garantizadas, presentadas como "no encontré resultados".

Un cero *fabricado* es indistinguible de un cero *real* para el usuario. Es la
misma clase de daño que el P0 de `direct_lookup` que R1 corrigió: una negación
confiada sobre algo que el sistema nunca consultó.

**Precedente que se replica:** `ModalityFilter.__post_init__` ya trata el
solapamiento como error. La diferencia entre modalidad y objeto no debe ser el
azar de qué subsistema se escribió con más cuidado.

**Qué NO se hace en V1:** adivinar cuál de las dos lecturas quiso el usuario
(*revisión de tema* vs *exclusión*). La respuesta a la aclaración es la mejor
telemetría posible para decidirlo después con datos en lugar de intuición.

---

## 13. Fuente y proyección

Que el `QueryFrame` pueda almacenar una exclusión no prueba nada. La matriz
evalúa la **proyección real**.

| DIMENSION | REPRESENTABLE | SOQL_FORM | NULL_SEMANTICS | SAFE | DECISION |
|---|---|---|---|---|---|
| **OBJECT** | sí | `NOT (f₁ LIKE '%x%' OR f₂ LIKE '%x%' …)` | 3VL: un campo NULL ⇒ **fila borrada** si el otro no coincide. En vivo: 6 filas de Procesos afectables; 0 de Contratos. `COALESCE(…, false)` verificado | sí, **con el fix null-safe** | **SUPPORTED** |
| **MODALITY** | sí | `modalidad_de_contratacion NOT IN (…)` sobre raws de catálogo | NULL ⇒ fila borrada (medir; en producción hoy) | sí | **EXISTING** |
| **DEPARTMENT (resta de conjunto)** | sí | `departamento IN (<positivos − excluidos>)` | **no aplica**: predicado positivo | sí | **SUPPORTED** |
| **DEPARTMENT (complemento abierto)** | sí | `departamento NOT IN (…)` | `No Definido` (6,44 % / 2,01 % en HCA-R4.1) queda **incluido** | ambiguo, decidible | **CLARIFY_ONLY** |
| **CITY** | técnicamente sí | `NOT (unaccent(UPPER(ciudad)) LIKE '%x%')` | NULL/vacío 0 %, pero `No Definido` es 23,79 % / 21,52 %; **colisión de subcadena** (Calima⊃cali, San Andrés Sotavento) | **no** | **CLARIFY** |
| **TIME** | sí, con disyunción | `(fecha < a OR fecha > b)` o `case(…)` | NULL ⇒ borrada | transporte exploratorio SAFE; parser/representación siguen abiertos | **CLARIFY_ONLY** |
| **BUYER** | sí | `nombre_entidad != '…'` / `NOT LIKE` | homónimos, ecosistemas | no | **CLARIFY_ONLY** |
| **ACTOR** | sí | `proveedor_adjudicado NOT LIKE` / `documento_proveedor NOT IN` | NULL frecuente (no adjudicados); UT | no | **CLARIFY_ONLY** |
| **CONTRACT TYPE** | sí | `tipo_de_contrato NOT IN (…)` | cerrado | no autorizado en V1 | **CLARIFY_ONLY** |
| **STATE** | sí | `estado_* NOT IN (…)` | cerrado | no autorizado en V1 | **CLARIFY_ONLY** |
| **MONEY** | n/a | comparador invertido | n/a | sí | **POLARITY**, no exclusión |

`SOQL_REPRESENTABILITY: PARTIAL` — toda dimensión es *expresable*; no toda es
*segura*. La frontera V1 la traza la seguridad, no la expresividad.

**`SoQLBuilder` no se modifica en este bloque.**

---

## 14. Alcance V1

### SUPPORTED

1. **OBJECT exclusion** — vocabulario de marcadores ampliado (`sin`, `que no
   sean`, `salvo`, `excluyendo`, subjuntivo), binding por gap, lista de
   anti-negación, `NOT(...)` null-safe, expansión morfológica declarada en la
   respuesta.
2. **MODALITY exclusion** — existente, comportamiento intacto. Solo se extrae el
   vocabulario de marcadores a un módulo compartido.
3. **DEPARTMENT set-exclusion con alcance positivo explícito** — resta sobre
   `departamentos_in` antes del SoQL. Sin predicado negativo, sin semántica de
   NULL, sin tocar el builder.

### CLARIFY (fail-closed, no ejecuta)

4. **CITY exclusion**, con o sin ancla positiva. Precondición para reabrir:
   gazetteer municipal (DIVIPOLA) + `ciudad_resolved` con proyección por
   igualdad. La medición de NULL/centinela ya cerró en HCA-R4.1 y refuerza esta
   frontera. Ese trabajo también mejora la **precisión positiva** de ciudad, así
   que se paga dos veces.
5. **TEMPORAL exclusion**. La disyunción pasó el benchmark exploratorio de
   transporte de HCA-R4.1, pero no activa soporte. Precondición restante: cerrar
   `HCA-TIME-001/002/003/004` y diseñar el álgebra de intervalos.
6. **Geografía negativa sin alcance positivo alguno** — complemento nacional.
7. **BUYER exclusion** y **ACTOR exclusion** — identidad no resuelta
   (ecosistemas, UT).
8. **CONTRACT TYPE** y **STATE exclusion** — reconocidas, pero sin autoridad de
   ejecución negativa autorizada en V1.

### Frontera V1 divergente respecto a la propuesta inicial, y por qué

El encargo sugería *"CITY exclusion cuando existe un alcance positivo mayor"*.
La evidencia lo desaconseja: el alcance positivo resuelve la **amplitud** pero
no la **proyección**. Mientras `ciudad` se proyecte por subcadena sobre una
columna sin canonicalizar, `en Atlántico menos Barranquilla` sigue borrando
municipios por colisión y sin que el usuario lo vea. La sustitución propuesta
—**DEPARTMENT set-exclusion**— entrega la misma clase de utilidad ("resta dentro
de un alcance") con riesgo nulo, porque nunca genera un predicado negativo.

CITY es la exclusión más pedida y la única que en V1 se aclara sin ejecutar. Eso
es deliberado: **aclarar es estrictamente mejor que devolver el complemento
exacto**, que es lo que ocurre hoy.

### Fuera de objetivo, explícitamente

El objetivo no es soportar toda la negación del español. Es **dejar de ejecutar
lo contrario de lo que pidió el usuario**. Las tres fallas de HCA-1 quedan
cubiertas: OBJECT se ejecuta bien, CITY y TIME dejan de invertirse.

---

## 15. Matriz de pruebas pre-registrada

Pre-registrada **antes** del código, en
[`tests/fixtures/audits/negation_focal_v1.yaml`](../../tests/fixtures/audits/negation_focal_v1.yaml).

Ese archivo **no se ejecuta**: ningún test lo carga por glob; el runner de HCA
solo lee el corpus que recibe por `--corpus`. Es un artefacto de diseño hasta que
el bloque de implementación lo conecte.

| ID | consulta | expected_dimension | expected_positive | expected_exclusion | expected_policy | must_not_silently_drop |
|---|---|---|---|---|---|---|
| N1 | `contratos sin alimentación` | OBJECT | — | `alimentacion` | EXECUTE | sí |
| N2 | `contratos de aseo que no sean de barranquilla` | CITY | `objeto=[aseo]` | `ciudad=Barranquilla` | CLARIFY | sí |
| N3 | `contratos en atlántico menos barranquilla` | CITY dentro de DEPARTMENT | `departamento=Atlántico` | `ciudad=Barranquilla` | CLARIFY (conserva Atlántico) | sí |
| N4 | `excepto contratación directa` | MODALITY | — | `contratacion_directa*` | EXECUTE (existente) | sí |
| N5 | `contratos de alimentación sin transporte` | OBJECT | `alimentacion` | `transporte` | EXECUTE | sí |
| N6 | `sin alimentación ni transporte` | OBJECT | — | `alimentacion`, `transporte` | EXECUTE | sí |
| N7 | `alimentación pero sin alimentación` | OBJECT | — | — | CLARIFY (contradicción) | sí |
| N8 | `menos los de 2025` | TIME | — | año 2025 | CLARIFY | sí |
| N9 | `en 2025 menos 2025` | TIME | — | — | CLARIFY (contradicción) | sí |
| N10 | T2 `sin alimentación` sobre tema `alimentación` | OBJECT | — | — | CLARIFY (contradicción) | sí |
| N11 | T2 `menos Barranquilla` sobre `Atlántico` | CITY | `departamento=Atlántico` intacto | `ciudad=Barranquilla` | CLARIFY | sí |
| N12 | `contratos de alimentación en Barranquilla` | — (control) | `objeto=[alimentacion]`, `ciudad=Barranquilla` | ninguna | EXECUTE | n/a |

Añadidos por evidencia del sondeo (no estaban en el encargo):

| ID | consulta | por qué |
|---|---|---|
| N13 | `contratos de vigilancia por menos de 500 millones` | control anti-regresión: `menos` **no** es marcador aquí; protege ADR-063 |
| N14 | `contratos con entidades sin ánimo de lucro` | anti-negación de dominio (ESAL) |
| N15 | `contratación directa sin ofertas` | `sin` dentro de un alias de modalidad ⇒ inclusión |
| N16 | `contratos que excluyan alimentación` | subjuntivo; hoy el verbo fuga a `objeto` |
| N17 | `contratos de aseo excepto en barranquilla` | span de ciudad consumido antes ⇒ hoy invierte |
| N18 | `contratos que no sean de la alcaldía de barranquilla` | BUYER; hoy invierte y deja la consulta sin tema |
| N19 | `contratos donde el proveedor no sea X` | ACTOR; hoy **drop total** |

Y dos anti-metamórficas, que son el mejor detector de regresión disponible
porque no dependen de valores concretos:

- **N5 ≢ N6** — significados opuestos no pueden producir el mismo frame (hoy lo
  producen: `objeto=[alimentacion, transporte]` en ambos).
- **N2 ≢ N12** — una consulta y su negación no pueden producir el mismo frame
  (hoy difieren solo en `objeto`, no en polaridad).

---

## 16. Mediciones preimplementación — cerradas por HCA-R4.1

El precheck en vivo del 2026-08-24 cerró las tres mediciones obligatorias. La
evidencia, queries y rondas completas están en
[`2026-08-24_HCA_R4_1_NEGATION_LIVE_SOURCE_PRECHECK.md`](../audits/2026-08-24_HCA_R4_1_NEGATION_LIVE_SOURCE_PRECHECK.md)
y en su artefacto JSON.

1. **OBJECT NULL.** Procesos: `nombre_del_procedimiento` 5 NULL
   (0,00005521 %) y `descripci_n_del_procedimiento` 1 (0,00001104 %), sin
   solape ni vacíos; Contratos: 0 NULL/vacíos en ambos campos. La forma actual
   borró las 6 filas NULL ante un término ausente; la forma con `COALESCE`
   preservó 6/6 y siguió excluyendo controles positivos.
2. **CITY unknown.** NULL/vacío = 0 en ambos datasets. El único centinela
   relevante observado fue exactamente `No Definido`: 2 154 643/9 055 609
   (23,79346326 %) en Procesos y 1 286 689/5 980 312 (21,51541592 %) en
   Contratos.
3. **TIME transport.** 8 rondas alternadas por variante de fetch y 4 de count,
   sin timeouts. La disyunción tuvo p90 0,4582 s (fetch) y 2,5275 s (count), por
   debajo del gate orientativo de 15 s. Es fuente-a-fuente y exploratorio: no
   levanta `TEMPORAL_NEGATION_V1 = CLARIFY_UNSUPPORTED`.

---

## 17. Regla final

> `no X` no significa `buscar X`.
> Y no todas las exclusiones significan lo mismo.
>
> Primero se preserva **la dimensión que el usuario negó**; solo después se
> decide si SECOPPAL puede ejecutar esa exclusión.

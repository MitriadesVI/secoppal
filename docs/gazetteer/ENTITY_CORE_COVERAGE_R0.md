# ENTITY-CORE-COVERAGE-R0 — Cobertura conversacional de compradores institucionales

**Tipo:** auditoría read-only · **Cambios de runtime:** 0
**Fecha:** 2026-08-31 · **Revisión:** R0.1 (cierre documental)
**HEAD:** `bfda67df4f252e9cdc569301bfcede12479e2a4c` · **Rama:** `main`
**Worktree (tracked):** limpio
**Evidencia cruda:** [`artifacts/audits/entity_core_coverage_r0.json`](../../artifacts/audits/entity_core_coverage_r0.json)

> **R0.1 — qué cambió respecto de R0.** Solo el *conteo del universo* y una
> evidencia que faltaba. Ninguna observación, veredicto o sonda de R0 fue
> alterada.
>
> 1. **Ministerios: 18 activos, no 19.** El Ministerio de Igualdad y Equidad se
>    reclasifica a `HISTORICAL_IN_LIQUIDATION` y sale del universo activo. Se
>    conserva su sonda y su resultado, ahora en categoría aparte.
> 2. **Departamentos administrativos: 6, no 5.** Faltaba la **Dirección Nacional
>    de Inteligencia**, no probada en R0. Sondeada en R0.1 → `MISSING`.

---

## 1. Resumen ejecutivo

Se midió qué pasa cuando un usuario nombra en lenguaje natural a los compradores
institucionales prioritarios. El veredicto de cada caso sale de lo que el runtime
**resuelve**, no de que una cadena exista en `aliases_db.json` (gate 4 de la tarea).

| Universo | Total | PASS | PARTIAL | MISSING | WRONG_BUYER |
|---|---:|---:|---:|---:|---:|
| A. Gobernaciones | 32 | **25** | 0 | 0 | **7** |
| B. Ministerios *(activos)* | 18 | **11** | 4 | 3 | 0 |
| B′. Ministerios *(histórico, en liquidación)* | 1 | 0 | 0 | 1 | 0 |
| C. Deptos. administrativos | 6 | **4** | 0 | 2 | 0 |
| D. Fiduprevisora | 1 | **1** | 0 | 0 | 0 |
| E. Baranoa | 2 | 0 | 0 | 1 | 1 |

**Veredicto: PARTIAL.** La cobertura básica es mejor de lo que sugería
`gap_20260526`, pero el modo de fallo dominante no es la ausencia: es que **una
identidad ausente o mal poblada no se reporta como ausente, se degrada en
silencio a otro filtro**. Siete gobernaciones producen hoy un `WHERE` que el
usuario no pidió y ninguna dice que no sabe.

### Los cuatro mecanismos detrás de todos los hallazgos

No son 12 bugs sueltos. Son cuatro causas, cada una verificada con sondas
dirigidas (sección 7).

| # | Mecanismo | Efecto observado |
|---|---|---|
| **RC1** | `geo_mention_resolution()` consulta ciudades **antes** que departamentos, y el patrón territorial exige `kind=="department"` | 4 departamentos con capital homónima nunca activan el gate territorial: `arauca`, `bogota`, `cucuta`, `san andres` |
| **RC2** | `_is_unqualified_dept()` habilita un alias territorial desnudo en cuanto hay un cualificador delante, **sin verificar que la entidad dueña pertenezca a ese territorio** | "departamento de San Andrés" → `HOSPITAL SAN ANDRES` (que es de **Tumaco, Nariño**) |
| **RC3** | Si la gobernación no está en el gazetteer, `scan_entity` falla y `scan_departamento` captura el token | "Gobernación del Vichada" → `departamento='Vichada'`: se pidió **un comprador** y se devuelve **todo el territorio** |
| **RC4** | Aliases genéricos puros sobreviven en el gazetteer (prohibidos por la regla 11.1 de la guía) | "gobierno departamental del Cauca" → comprador del **Tolima** ∧ territorio **Cauca** |

RC3 es una instancia exacta de `INV-NO-SILENT-SEMANTIC-FALLBACK` (ADR-069): el
defecto no es el hueco de cobertura, es que el hueco falla abierto.

### Estado de los gates críticos

| Gate | Resultado |
|---|---|
| 1. Gobernación no puede resolver a subentidad | **FAIL** — 2 casos (Cundinamarca, Valle del Cauca) |
| 2. "departamento de X" sigue siendo territorial | **FAIL** — 30/32; fallan Arauca y San Andrés (RC1) |
| 3. "Gobernación de X" produce comprador | **FAIL** — 4 casos degradan a territorio (RC3) |
| 4. No dar PASS por existir la cadena | Aplicado; reclasificó Tolima y MinCultura |
| 5. Detectar DUPLICATE_IDENTITY | **6 grupos detectados**, 5 con rama muerta alcanzable |
| 6. NIT no fusiona ecosistemas | Respetado — MinTIC y FonTIC quedan separados |

### Tests

```
pytest tests/test_geo_vs_entity_typed.py tests/test_geo_mention_resolution.py \
       tests/test_entity_resolver*.py tests/test_entity_ecosystem*.py \
       tests/test_gazetteer_*.py tests/test_entity_multi_regional.py \
       tests/test_entity_trailing_token_robustness.py
→ 183 passed, 13 xfailed
```

Baseline verde. **Ningún defecto de este informe está cubierto por los tests
actuales**: pasan y los defectos existen. `test_geo_vs_entity_typed.py` prueba
Atlántico, Nariño, Antioquia y Valle-territorio — justamente las que funcionan.

---

## 2. Gobernaciones — 32/32

Sondas por entidad: nombre natural, forma corta, sin tildes (cuando aplica),
consulta completa `"contratos de <entidad> en 2025"`.

| # | Departamento | Status | Comprador resuelto | Nota |
|---|---|---|---|---|
| 1 | Amazonas | PASS_EXACT | `GOBERNACION DEL AMAZONAS` | |
| 2 | Antioquia | PASS_EXACT | `DEPARTAMENTO DE ANTIOQUIA//` | nombre SECOP no evidente |
| 3 | Arauca | PASS_EXACT | `DEPARTAMENTO DEL ARAUCA` | ⚠ rompe el gate territorial (RC1) |
| 4 | Atlántico | PASS_EXACT | `DEPARTAMENTO DEL ATLANTICO**` | canario ADR-040 ✅ |
| 5 | Bolívar | PASS_EXACT | `GOBERNACIÓN DE BOLÍVAR//` | |
| 6 | Boyacá | PASS_EXACT | `GOBERNACIÓN DE BOYACÁ` | |
| 7 | Caldas | PASS_EXACT | `GOBERNACION DE CALDAS` | |
| 8 | Caquetá | PASS_EXACT | `GOBERNACION DEL CAQUETA**` | |
| 9 | Casanare | PASS_EXACT | `GOBERNACION DEL CASANARE` | |
| 10 | Cauca | PASS_EXACT | `GOBERNACIÓN DEL CAUCA` | |
| 11 | Cesar | PASS_EXACT | `GOBERNACION DEL DEPARTAMENTO DEL CESAR` | |
| 12 | Chocó | PASS_EXACT | `GOBERNACION DEL CHOCÓ.` | `departamento="No Definido"` en gazetteer |
| 13 | Córdoba | PASS_EXACT | `DEPARTAMENTO DE CORDOBA` | |
| 14 | **Cundinamarca** | **WRONG_BUYER** | `DEPARTAMENTO DE CUNDINAMARCA- SECRETARIA DE SALUD` | **subentidad** — no existe la raíz |
| 15 | **Guainía** | **WRONG_BUYER** | *(ninguno)* → `departamento='Guainía'` | canario ✅ detectado — RC3 |
| 16 | **Guaviare** | **WRONG_BUYER** | *(ninguno)* → `departamento='Guaviare'` | canario ✅ detectado — RC3 |
| 17 | Huila | PASS_EXACT | `GOBERNACION DEL HUILA*` | |
| 18 | La Guajira | PASS_EXACT | `GOBERNACIÓN DE LA GUAJIRA` | |
| 19 | Magdalena | PASS_EXACT | `GOBERNACION DEL MAGDALENA` | |
| 20 | Meta | PASS_EXACT | `DEPARTAMENTO DEL META` | |
| 21 | Nariño | PASS_EXACT | `GOBERNACION DE NARIÑO` | canario ADR-040 ✅ |
| 22 | Norte de Santander | PASS_EXACT | `Gobernación Norte de Santander*` | |
| 23 | Putumayo | PASS_EXACT | `GOBERNACIÓN DEPARTAMENTO DEL PUTUMAYO` | |
| 24 | Quindío | PASS_EXACT | `GOBERNACION DEL QUINDIO ARMENIA` | |
| 25 | Risaralda | PASS_EXACT | `GOBERNACIÓN DE RISARALDA**` | |
| 26 | **San Andrés** | **WRONG_BUYER** | `DEPARTAMENTO DE SANTANDER` / `HOSPITAL SAN ANDRES` | **institución ajena** — RC2 |
| 27 | Santander | PASS_EXACT | `DEPARTAMENTO DE SANTANDER` | |
| 28 | Sucre | PASS_EXACT | `GOBERNACION DE SUCRE` | |
| 29 | Tolima | PASS_ALIAS | `GOBIERNO DEPARTAMENTAL` | reclasificado — ver 2.1 |
| 30 | **Valle del Cauca** | **WRONG_BUYER** | `GOBERNACION DEL VALLE DEL CAUCA - DADI` | **subentidad** |
| 31 | **Vaupés** | **WRONG_BUYER** | *(ninguno)* → `departamento='Vaupés'` | canario ✅ detectado — RC3 |
| 32 | **Vichada** | **WRONG_BUYER** | *(ninguno)* → `departamento='Vichada'` | canario ✅ detectado — RC3 |

Los siete canarios de gobernación pedidos (Atlántico, Nariño, Cundinamarca,
Guainía, Guaviare, Vaupés, Vichada) quedaron cubiertos: dos PASS y cinco fallos
reales, ninguno silenciado.

### 2.1 Tolima — corrección de un falso positivo propio

La heurística inicial marcó Tolima como WRONG_BUYER porque el `official_name`
no contiene "TOLIMA" ni un marcador de raíz. Al verificar el registro:

```
official_name : GOBIERNO DEPARTAMENTAL
departamento  : Tolima
total_procesos: 27 694   ·  total_contratos: 27 203
aliases       : 'gobernación del tolima', 'gobierno departamental de tolima', …
```

Es el comprador **real** de la Gobernación del Tolima en SECOP, con el mismo
patrón que ADR-040 documentó para Atlántico: el nombre registrado no se parece a
la frase humana. Reclasificado a **PASS_ALIAS**. El defecto que sí queda es su
alias genérico (finding WB-04).

### 2.2 Gate territorial: 30/32

Se probó `"contratos en el departamento de <X>"` para las 32.

| Departamento | Esperado | Obtenido |
|---|---|---|
| Arauca | `departamento='Arauca'` | `nombre_entidad='DEPARTAMENTO DEL ARAUCA'` ❌ |
| San Andrés | `departamento='San Andrés…'` | `nombre_entidad='HOSPITAL SAN ANDRES'` ❌ |

Los otros 30 pasan. Ambos fallos son RC1 + RC2, no fallos del patrón territorial
en sí: con Antioquia, Atlántico y Valle el patrón dispara correctamente.

---

## 3. Ministerios — 18 activos + 1 histórico

| Ministerio | Status | Registro en gazetteer | Nota |
|---|---|---|---|
| Interior | PASS_EXACT | `MINISTERIO DEL INTERIOR` (25 alias) | |
| **Relaciones Exteriores** | **MISSING** | — | identidad ausente; "Cancillería" tampoco |
| Hacienda y Crédito Público | PASS_EXACT | 24 alias | |
| Justicia y del Derecho | PASS_EXACT | `MINJUSTICIA..` | ⚠ **4 registros** — ver §9 |
| **Defensa Nacional** | **MISSING** | — | ✅ confirma `gap_20260526` |
| Agricultura y Desarrollo Rural | PASS_EXACT | 25 alias | |
| Salud y Protección Social | PASS_EXACT | `MINSALUD` (20 alias) | |
| Trabajo | PASS_EXACT | 20 alias | |
| Minas y Energía | PASS_EXACT | 42 alias | |
| Comercio, Industria y Turismo | PARTIAL | `…- MINCIT` (22 alias) | falla la sigla "Mincomercio" |
| Educación Nacional | PASS_EXACT | `… (MEN)` (23 alias) | |
| Ambiente y Desarrollo Sostenible | PARTIAL | **1 alias** | falla "Minambiente" |
| Vivienda, Ciudad y Territorio | PARTIAL | `MINVIVIENDA`, **1 alias**, `dep="No Definido"` | canario ✅ — falla el nombre completo |
| **Tecnologías de la Información (MinTIC)** | **MISSING** | — | existe `FONDO UNICO DE TIC` (14 873 proc.): **entidad distinta, no fusionar** |
| Transporte | PASS_EXACT | 26 alias | |
| Cultura | PARTIAL | `Ministerio de las Culturas, las Artes y los Saberes` (32 alias) | identidad presente, falta el nombre histórico |
| Deporte | PASS_EXACT | 17 alias | |
| Ciencia, Tecnología e Innovación | PASS_EXACT | `MINCIENCIAS` (19 alias) | |

**Universo activo: 18.** PASS 11 · PARTIAL 4 · MISSING 3.

### 3.1 Fuera del universo activo (R0.1)

| Ministerio | Categoría | Status | Nota |
|---|---|---|---|
| Igualdad y Equidad | `HISTORICAL_IN_LIQUIDATION` | MISSING | Probado en R0 y contado allí como activo. R0.1 lo saca del universo de 18. Su ausencia del gazetteer **no es deuda de cobertura prioritaria**; se conserva la sonda por trazabilidad. |

Los dos canarios pedidos: **MinVivienda** = PARTIAL (1 alias),
**MinDefensa** = MISSING (confirma la evidencia histórica).

MinCultura ilustra el gate 4 en acción: la cadena existe y la entidad tiene 32
aliases, pero la frase que la gente usa —"Ministerio de Cultura"— no resuelve.
Es deuda de alias, no de identidad; por eso PARTIAL y no MISSING.

---

## 4. Departamentos administrativos nacionales — 6

| Entidad | Status | Registro |
|---|---|---|
| DAPRE — Presidencia de la República | PASS_EXACT | 16 alias |
| DANE | PASS_EXACT | `DEPARTAMENTO ADMINISTRATIVO NACIONAL DE ESTADISTICA (DANE)` |
| DNP — Planeación Nacional | PASS_EXACT | 25 alias · canario ✅ |
| **DAFP — Función Pública** | **MISSING** | — · canario ✅ confirma `gap_20260526` |
| Prosperidad Social | PASS_EXACT | 24 alias · canario ✅ |
| **Dirección Nacional de Inteligencia** | **MISSING** | — · **añadida en R0.1** |

**DNI (evidencia nueva, R0.1).** No fue probada en R0. Sondeada ahora con el
mismo protocolo de 4 sondas: 0 registros en `aliases_db.json`, **4/4 sondas sin
resolución**.

| Sonda | Resultado |
|---|---|
| "Dirección Nacional de Inteligencia" | *(ninguno)* |
| "DNI" | *(ninguno)* |
| "Direccion Nacional de Inteligencia" | *(ninguno)* |
| "contratos de Dirección Nacional de Inteligencia en 2025" | *(ninguno)* |

A diferencia de las gobernaciones ausentes, la DNI **no degrada a territorio**:
falla en blanco, sin filtro fabricado. Es el modo de fallo menos dañino de los
observados. Si se agrega en un lote futuro, cuidado con la sigla: `DNI` está
sobrecargada (documento de identidad) y un alias desnudo repetiría el patrón
RC2 de `HOSPITAL SAN ANDRES`.

**Nota DANE (REVIEW, no duplicate-identity):** el gazetteer tiene 9 registros
DANE — 1 nacional + 6 direcciones territoriales (Centro, Norte, Suroccidente,
Noroccidente, Centro Oriente, Centro Occidente) + 2 del fondo rotatorio
(`FONDO ROTATORIO DEL DANE`, `FONDANE - DIRECCION TERRITORIAL CENTRO`). No
son grafías rivales de una misma identidad: son unidades territoriales reales,
la misma forma que ICBF y SENA. Si el DANE territorial se vuelve consultable, la
vía es el **ecosystem resolver** existente (ADR-020), no más aliases. No es
prioridad de este ciclo.

---

## 5. Fiduprevisora

| Sonda | Resultado |
|---|---|
| "Fiduciaria La Previsora" | `FIDUCIARIA LA PREVISORA S.A.- FIDUPREVISORA S.A.` |
| "Fiduprevisora" | idem |
| "contratos de Fiduprevisora en 2025" | idem |

**PASS_EXACT**, 21 aliases, sin duplicados. Único universo enteramente limpio.

---

## 6. Baranoa — canario municipal

| Sonda | Resultado | Lectura |
|---|---|---|
| "Alcaldía de Baranoa" | clarificación: *"No encontré… ¿cuál es el nombre oficial?"* | **fail-closed correcto** |
| "Municipio de Baranoa" | clarificación | **fail-closed correcto** |
| "Alcaldia de Baranoa" (sin tildes) | clarificación | **fail-closed correcto** |
| "Baranoa Atlántico" | `departamento='Atlántico'` | ❌ degrada a territorio (RC3) |

Baranoa no está en el gazetteer. Lo que hay que subrayar: **3 de 4 sondas se
comportan bien**. El `_strict_rewrite_target` de `entity_resolver.py` reconoce
"municipio/alcaldía de X", no encuentra la entidad y **pide clarificación en vez
de inventar** — exactamente lo que el resto del sistema debería hacer.

El contraste es el hallazgo: para un municipio ausente el sistema falla cerrado;
para una gobernación ausente (Vichada) falla abierto y devuelve el departamento
entero. La diferencia no es de diseño deliberado: es que "alcaldía/municipio de
X" tiene una regla de reescritura estricta y "Gobernación de X" no la tiene
cuando la entidad no existe.

---

## 7. Wrong-buyer findings

| ID | Caso | Resuelve a | Causa |
|---|---|---|---|
| **WB-01** | Gobernación de Cundinamarca | `DEPARTAMENTO DE CUNDINAMARCA- SECRETARIA DE SALUD` | La raíz no existe. El gazetteer tiene 3 secretarías de Cundinamarca (Salud 16 alias, Educación 1, Gobierno 1) y ninguna entidad madre. `rewrite_gobernación` acepta la primera con `confidence=high`. |
| **WB-02** | Gobernación del Valle del Cauca | `GOBERNACION DEL VALLE DEL CAUCA - DADI` | **Cinco** registros "GOBERNACIÓN DEL VALLE…", todos subentidades (DADI, Sec. Educación, Sec. Salud, Sec. Desarrollo Social, Sec. Desarrollo Rural) y ninguna raíz. Gana DADI por longitud de alias. |
| **WB-03** | Gobernación de San Andrés | `DEPARTAMENTO DE SANTANDER` (frase larga) / `HOSPITAL SAN ANDRES` (corta) | Doble: la gobernación del archipiélago existe (30 752 procesos) pero con **1 solo alias** = su nombre oficial completo. Sin alias natural, el fuzzy la confunde con Santander. Y `HOSPITAL SAN ANDRES` —de **Tumaco, Nariño**— lleva los aliases desnudos `san andres` / `san andrés` (RC2). |
| **WB-04** | "gobierno departamental del Cauca" | `GOBIERNO DEPARTAMENTAL` (Tolima) **+** `departamento='Cauca'` | Alias genérico puro `gobierno departamental`, prohibido por la regla 11.1. Produce un `WHERE` contradictorio: comprador del Tolima ∧ territorio del Cauca. |
| **WB-05** | Gobernaciones de Guainía, Guaviare, Vaupés, Vichada | `departamento='<X>'` | RC3: identidad ausente → degradación silenciosa a territorio. |
| **WB-06** | "departamento de Arauca" (territorial) | `nombre_entidad='DEPARTAMENTO DEL ARAUCA'` | RC1: `arauca` está en `GAZETTEER_CIUDADES`, `geo_mention_resolution` devuelve `kind="city"`, el patrón territorial exige `"department"` y no dispara. |
| **WB-07** | "UAESP" | `SANTIAGO DE CALI … UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PUBLICOS` | La sigla de Bogotá resuelve a la entidad homónima de **Cali**. Fuera de los universos A–E; anotado por proximidad. |

**Alcance exacto de RC1** — colisiones ciudad/departamento en el código actual:

| clave | ciudad | departamento |
|---|---|---|
| `arauca` | Arauca | Arauca |
| `bogota` | Bogotá | Distrito Capital de Bogotá |
| `cucuta` | Cúcuta | Norte de Santander |
| `san andres` | San Andrés | San Andrés, Providencia y Santa Catalina |

Son 4 y están enumeradas: es una lista cerrada, no un riesgo difuso.

---

## 8. Duplicate-identity findings

Detectados normalizando puntuación decorativa (`.`, `..`, `**`, `//`) del
`official_name`. **Seis grupos**, y el patrón es constante: SECOP registra dos
grafías, una acumula los procesos y la otra queda en `total_procesos = 0` —
pero **la rama muerta suele tener más aliases**, así que el runtime la prefiere.

| ID | Identidad | Variantes | Rama viva | Rama muerta | ¿El runtime cae en la muerta? |
|---|---|---|---|---|---|
| **DI-01** | Ministerio de Justicia | 4 registros | `MINJUSTICIA..` (7 449) · `MINISTERIO DE JUSTICIA Y DEL DERECHO..` (7 548) | `MINJUSTICIA.` (0, 20 alias) · `… DERECHO.` (0) | Sí — 2/8 sondas |
| **DI-02** | Fondo Financiero Distrital de Salud | 2 | `…SALUD..` (23 264, **1 alias**) | `…SALUD.` (0, **52 alias**) | **Sí — 6/8 sondas**, y la frase natural "Fondo Financiero Distrital de Salud" también |
| **DI-03** | Municipio de Soacha | 2 | `…SOACHA..` (20 463) | `…SOACHA.` (0, 17 alias) | **Sí — 7/7 sondas**, incluidas "contratos del municipio de Soacha" y "alcaldía de Soacha" |
| **DI-04** | UAESP | 2 | `…PÚBLICOS..` (6 029, 1 alias) | `…PÚBLICOS.` (0, 24 alias) | Sí — 4/8 sondas |
| **DI-05** | Municipio de Yopal | 2 | `…YOPAL...` (16 779) | `…YOPAL..` (0) | No en las sondas naturales |
| **DI-06** | Ministerio de Justicia (par largo) | incluido en DI-01 | | | |

DI-02 y DI-03 son los graves: la consulta **más natural posible** resuelve a la
grafía de cero procesos. En runtime eso es un `WHERE nombre_entidad = '…SOACHA.'`
que devuelve cero filas — sin error, sin aviso.

> **Limitación honesta:** `total_procesos` es el conteo capturado al construir el
> gazetteer, no una consulta a SECOP de hoy. Que sea 0 es evidencia fuerte de
> rama muerta, no prueba definitiva. Verificar en vivo es el primer paso del
> lote A, antes de borrar nada.

Ninguno se resolvió eligiendo una grafía arbitrariamente: quedan todos
reportados como DUPLICATE_IDENTITY, según el gate 5.

---

## 9. Partial-alias findings

| ID | Entidad | Aliases | Falla |
|---|---|---|---|
| **PA-01** | `MINVIVIENDA` | **1** | "Ministerio de Vivienda, Ciudad y Territorio" no resuelve. Además `departamento="No Definido"`. |
| **PA-02** | `MINISTERIO DE AMBIENTE Y DESARROLLO SOSTENIBLE` | **1** | "Minambiente" no resuelve. |
| **PA-03** | `Ministerio de las Culturas, las Artes y los Saberes` | 32 | El nombre histórico "Ministerio de Cultura" / "Mincultura" no resuelve. |
| **PA-04** | `MINISTERIO DE COMERCIO INDUSTRIA Y TURISMO - MINCIT` | 22 | "Mincomercio" no resuelve. |
| **PA-05** | `GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO DE SAN ANDRES…` | **1** | Causa directa de WB-03. |
| **PA-06** | `GOBERNACIÓN DEL VALLE… SECRETARÍA DE DESARROLLO RURAL` | **1** | Ya en `lote_01_debt.md`. |
| **PA-07** | `DEPARTAMENTO DE CUNDINAMARCA - SECRETARIA DE EDUCACION` | **1** | Ya en `lote_01_debt.md`. |
| **PA-08** | `DEPARTAMENTO DE CUNDINAMARCA -SECRETARIA DE GOBIERNO` | **1** | Ya en `lote_01_debt.md`. |

PA-01, PA-02, PA-03, PA-04 y PA-05 son **deuda nueva**, no listada en
`lote_01_debt.md`. La regla de ese documento ("el número de entidades con <3
aliases NO debe aumentar") se refería a las entidades del lote; estas venían de
antes y nunca se contaron.

---

## 10. Propuesta de lotes siguientes — no implementada

Tres grupos, en orden de prioridad. Ninguno se ejecuta en esta fase.

### A. RECONCILIATION — entidades existentes pero incorrectas o duplicadas

*Tesis: corregir lo que hoy responde mal es más urgente que agregar lo que hoy
no responde. Una respuesta incorrecta con `confidence=high` es peor que un
"no sé".*

| # | Caso | Acción propuesta |
|---|---|---|
| A1 | Gobernación de Cundinamarca (WB-01) | Verificar en SECOP si existe el comprador raíz. Si existe → agregarlo. Si **no** existe (SECOP solo registra secretarías) → documentarlo y decidir política; no elegir una secretaría por defecto. |
| A2 | Gobernación del Valle (WB-02) | Igual que A1. Hoy DADI gana por accidente de longitud de alias. |
| A3 | Gobernación de San Andrés (WB-03 / PA-05) | Poblar aliases de la gobernación del archipiélago **y** acotar los aliases desnudos `san andres`/`san andrés` de `HOSPITAL SAN ANDRES` (Tumaco) a formas que incluyan "hospital"/"tumaco". |
| A4 | Alias genérico `gobierno departamental` (WB-04) | Acotar a `gobierno departamental del tolima` y variantes con token territorial. Regla 11.1. |
| A5 | Duplicados DI-01…DI-05 | Verificar en vivo cuál grafía es la viva; consolidar aliases en ella. **DI-02 y DI-03 primero.** |
| A6 | Aliases pobres PA-01…PA-04 | Enriquecer MinVivienda (+ corregir `departamento`), MinAmbiente, MinCultura (nombre histórico), MinComercio (sigla). |

**Tamaño:** ~15 casos → cabe en un lote de reconciliación (guía §9: 4–15).
Sugerencia: partir en A-1 (A1–A4, semántica de gobernaciones) y A-2 (A5–A6,
duplicados y aliases), para no mezclar dos tesis.

### B. CORE EXPANSION — identidades institucionales faltantes

| # | Entidades | n |
|---|---|---|
| B1 | Gobernaciones de **Guainía, Guaviare, Vaupés, Vichada** | 4 |
| B2 | Ministerios activos de **Defensa Nacional, Relaciones Exteriores, TIC** | 3 |
| B3 | **DAFP — Función Pública** y **Dirección Nacional de Inteligencia** | 2 |

**Tamaño:** 9 entidades. Registry-first: NIT + conteos Socrata antes de aliases.
(El Ministerio de Igualdad y Equidad **no** entra: está en liquidación, §3.1.)

Tres precauciones:
- **B2/MinTIC:** existe `FONDO UNICO DE TECNOLOGÍAS DE LA INFORMACIÓN` (14 873
  procesos). Es el patrimonio autónomo, **no** el ministerio. Gate 6: no fusionar
  por NIT ni por parecido de nombre.
- **B1:** verificar primero que esas cuatro gobernaciones tengan actividad
  contractual registrada. Su ausencia puede ser un vacío real de SECOP, no del
  gazetteer — en cuyo caso el arreglo es el mensaje al usuario, no un alias.
- **B3/DNI:** no agregar `DNI` como alias desnudo. La sigla está sobrecargada
  (documento de identidad) y repetiría el patrón RC2.

### C. MUNICIPAL COVERAGE — Baranoa y la estrategia general

Baranoa es un caso; municipios hay ~1 100. Agregarlos de a uno no escala.

| # | Acción |
|---|---|
| C1 | Agregar Alcaldía/Municipio de Baranoa (Atlántico) por la vía normal de lote. |
| C2 | **Antes de C3**, decidir la estrategia: el gazetteer no puede crecer a 1 100 municipios sin multiplicar las colisiones tipo WB-03. Opciones a evaluar, no a implementar aquí: (a) cobertura por demanda (los que los usuarios piden), (b) top-N por volumen contractual, (c) resolución municipal derivada de DIVIPOLA en vez de aliases. |
| C3 | Lote municipal según lo decidido en C2. |

C2 es una decisión de arquitectura y merece su propio documento. **No debería
resolverse dentro de un lote de aliases.**

### Sobre el ecosystem resolver para gobernaciones

**No se recomienda.** La evidencia no lo sostiene:

- 25 de 32 gobernaciones ya resuelven con **comprador simple**, incluidas las de
  nombre SECOP no evidente (Atlántico, Tolima, Antioquia).
- De los 7 fallos: 4 son identidad ausente (B1), 2 son raíz ausente con
  subentidades presentes (A1, A2) y 1 es alias pobre + alias veneno (A3).
  **Ninguno es un problema de estructura multi-entidad.**
- ICBF y SENA necesitaron ecosistema porque una consulta regional debe abarcar
  varias entidades operativas bajo un mismo NIT. Una gobernación es 1:1 con su
  departamento: no hay nada que abarcar.

Cundinamarca y Valle son los únicos candidatos concebibles —SECOP parece
registrar por secretarías—, pero eso se decide **después** de A1/A2, cuando se
sepa si la raíz existe. Hasta entonces sería construir un ecosistema para
esconder un hueco de datos.

El `ENTITY_ECOSYSTEM_QUERY_BRIDGE.md` §6 lista "Gobernaciones — prioridad media,
complejidad baja". Esta auditoría es la evidencia que faltaba para bajarlo:
**el comprador simple sí basta.**

---

## 11. Método y limitaciones

**Cómo se midió.** Harness read-only sobre
`QueryRouter().parse(q).params`, 4 sondas por entidad más un gate territorial por
gobernación. Sin red: no se consultó SECOP. La persistencia de `Settings` se
redirigió por entorno a un directorio temporal antes de importar `app.*` (ley
`settings-sin-redirigir-persistencia`); se verificó que `data/feedback.jsonl` y
`data/secoppal.db` conservan su timestamp previo.

**Limitaciones que el lector debe tener presentes:**

1. **Sin verificación en vivo contra SECOP.** Que una entidad falte en el
   gazetteer no prueba que falte en SECOP; que `total_procesos = 0` es evidencia
   de rama muerta, no prueba. Toda acción de borrado en el lote A debe verificarse
   antes.
2. **El universo B es una lista declarada, no derivada.** R0 auditó 19
   ministerios; R0.1 corrigió el universo a **18 activos + 1 en liquidación**
   (§3.1) y el de departamentos administrativos a **6** (§4). Un universo
   declarado se desactualiza cuando cambia la estructura del Estado: es la
   limitación estructural de esta auditoría, y la razón de esta misma revisión.
3. **La clasificación agregada usa la sonda más débil.** Una entidad con 3 sondas
   PASS y 1 WRONG_BUYER se reporta WRONG_BUYER. Es deliberado —el usuario que
   escribe la cuarta forma recibe la respuesta mala— pero infla la severidad
   frente a una lectura por sonda. El JSON trae las sondas individuales.
4. **Una reclasificación manual** (Tolima) y una de identidad (MinCultura), ambas
   documentadas con su evidencia en §2.1 y §3.

---

## 12. Salida

```
ENTITY_CORE_COVERAGE_R0: PARTIAL   (revisión R0.1 — cierre documental)

HEAD:            bfda67df4f252e9cdc569301bfcede12479e2a4c
WORKTREE_STATUS: limpio en archivos tracked

UNIVERSE_ACCOUNTING (R0.1):
  ACTIVE_MINISTRIES_TOTAL:      18
  HISTORICAL_MINISTRIES_TESTED: 1    (Igualdad y Equidad — en liquidación)
  ADMIN_DEPARTMENTS_TOTAL:      6    (DAPRE, DNP, DAFP, Prosperidad Social,
                                      DNI, DANE)

GOBERNACIONES:
  PASS:        25/32   (24 exact + 1 alias)
  PARTIAL:     0
  MISSING:     0       (4 identidades ausentes se reportan como WRONG_BUYER
                        porque degradan a territorio, no a "no sé")
  WRONG_BUYER: 7       Cundinamarca, Valle del Cauca, San Andrés,
                        Guainía, Guaviare, Vaupés, Vichada
  GATE_TERRITORIAL: 30/32  (fallan Arauca y San Andrés)

MINISTERIOS (activos = 18):
  PASS:      11/18
  PARTIAL:   4    Comercio, Ambiente, Vivienda, Cultura
  MISSING:   3    Defensa Nacional, Relaciones Exteriores, TIC
  DUPLICATE: 1    Justicia y del Derecho (4 registros / 2 pares)

MINISTERIOS (histórico, en liquidación = 1):
  Igualdad y Equidad → MISSING · HISTORICAL_IN_LIQUIDATION
  (probado, fuera del universo activo — no es deuda prioritaria)

DEPARTAMENTOS_ADMIN (= 6):
  PASS:    4/6   DAPRE, DANE, DNP, Prosperidad Social
  PARTIAL: 0
  MISSING: 2     DAFP — Función Pública
                 Dirección Nacional de Inteligencia (añadida en R0.1;
                 0 registros, 4/4 sondas sin resolver, NO degrada a territorio)

FIDUPREVISORA: PASS_EXACT  (21 aliases, sin duplicados)
BARANOA:       MISSING     (3 de 4 sondas fallan CERRADO — pide clarificación;
                            "Baranoa Atlántico" degrada a territorio)

TOP_FINDINGS:
  1. RC3 — Una gobernación ausente NO dice que falta: degrada en silencio a
     territorio. Guainía, Guaviare, Vaupés y Vichada devuelven el departamento
     entero cuando se pide el comprador. Instancia de INV-NO-SILENT-SEMANTIC-
     FALLBACK (ADR-069).
  2. DI-02/DI-03 — Duplicados con rama muerta preferida: "contratos del municipio
     de Soacha" y "Fondo Financiero Distrital de Salud" resuelven a la grafía con
     total_procesos = 0 → cero filas, sin aviso.
  3. WB-01/WB-02 — Cundinamarca y Valle resuelven a una SUBENTIDAD con
     confidence=high porque la raíz no existe en el gazetteer.
  4. RC2 — HOSPITAL SAN ANDRES (Tumaco, NARIÑO) lleva el alias desnudo
     "san andres" y captura al archipiélago, incluso en consultas territoriales.
  5. RC1 — 4 departamentos con capital homónima (arauca, bogota, cucuta,
     san andres) nunca activan el gate territorial: geo_mention_resolution
     resuelve ciudad antes que departamento.
  6. Los 183 tests relevantes pasan. Ninguno cubre estos casos.

NEXT_RECOMMENDED_LOT:
  Lote A-1 — RECONCILIATION de gobernaciones (A1–A4, ~4 casos).
  Corregir lo que responde mal antes de agregar lo que no responde.
  B1/B2/B3 (9 entidades) va después, registry-first.
  NO se recomienda ecosystem resolver para gobernaciones: el comprador simple
  basta en 25/32 y ningún fallo es de estructura multi-entidad.

RUNTIME_FILES_CHANGED: 0
```

---

## 13. Estado post-remediación

Las métricas y hallazgos anteriores se conservan como baseline histórico de
R0/R0.1. Esta tabla registra únicamente el cierre posterior de sus causas raíz.

| Causa raíz | Estado | Remediación |
|---|---|---|
| RC1 | **CLOSED** — `8cecc69` | `ENTITY-TYPED-GEO-R1B`: el contexto explícito `departamento de/del X` consulta la autoridad de departamentos sin cambiar la precedencia ciudad-first de menciones desnudas. |
| RC2 | **PARTIAL** — `GAZETTEER-ALIAS-OWNERSHIP-R1C` | Retirados los 14 aliases territoriales desnudos en manos de entidades de otro territorio, `san andres` incluido: ninguna entidad no territorial los posee ya. **Residuo:** `resolve_entidad("san andrés")` sigue devolviendo `HOSPITAL SAN ANDRES` por `fuzzy(95)` contra su propio `official_name`, que entra siempre a `choice_to_official`. No es corregible con datos (deuda D1 de R1C). El daño está contenido: por RC1 el router devuelve el departamento. |
| RC3 | **CLOSED** — `4d82210` | Compradores calificados ausentes fallan cerrado y no degradan a territorio. |
| RC4 | **CLOSED** — `ROOT-BUYER-EVIDENCE-R1D` | R1C retiró los aliases genéricos; R1D separa `SOURCE_PHYSICAL_NAME` de `SCAN_ALIAS`, de modo que `GOBIERNO DEPARTAMENTAL` ya no captura cualquier territorio. La forma calificada `gobierno departamental de/del X` usa ownership raíz único: Cauca → su Gobernación; Tolima → `GOBIERNO DEPARTAMENTAL`. |

Detalle y evidencia en
[`GAZETTEER_ALIAS_OWNERSHIP_R1C.md`](GAZETTEER_ALIAS_OWNERSHIP_R1C.md). Ese
bloque además **refuta** la lectura de "rama muerta" de §8: los 6 grupos son
pares complementarios por dataset (una identidad lógica, dos nombres físicos),
no una grafía viva y una muerta.

Post-R1B, el gate territorial tipado cubre las cuatro colisiones enumeradas en
R0 (`arauca`, `bogota`, `cucuta`, `san andres`). La semántica no tipada y la
resolución institucional de `Gobernación de/del X` permanecían sin cambios.

Post-R1D, la matriz de gobernaciones queda en **25 compradores raíz con alias
exacto + 7 fail-closed**, sin `WRONG_CHILD`, fuzzy de raíz ni resolución basada
sólo en `official_name`. Cundinamarca y Valle dejan de promover secretarías;
Tolima se recupera por sus aliases territoriales; San Andrés deja de encogerse
a `San` y resolver lateralmente a Santander. Las cuatro gobernaciones ausentes
continúan fail-closed. La evidencia completa y la matriz 32/32 están en
[`GAZETTEER_ALIAS_OWNERSHIP_R1C.md`](GAZETTEER_ALIAS_OWNERSHIP_R1C.md#11-post-r1d--evidencia-institucional-para-compradores-raíz).

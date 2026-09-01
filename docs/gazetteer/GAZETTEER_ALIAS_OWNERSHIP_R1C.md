# GAZETTEER-ALIAS-OWNERSHIP-R1C — propiedad de aliases del gazetteer

**Tipo:** auditoría + remediación de datos · **Cambios de runtime Python:** 0
**Fecha:** 2026-08-31 · **Rama:** `main`
**HEAD de la auditoría:** `8cecc69c86b5820b108cf348d03bec7ac3510623`
**Auditor:** [`scripts/audits/audit_gazetteer_alias_ownership.py`](../../scripts/audits/audit_gazetteer_alias_ownership.py)
**Evidencia cruda:** [`artifacts/audits/gazetteer_alias_ownership_r1c.json`](../../artifacts/audits/gazetteer_alias_ownership_r1c.json)

> R1C **no es un lote de expansión**. No se agregan gobernaciones, ministerios,
> DAFP, DNI, Baranoa ni alcaldías nuevas. Primero se limpia el mapa
> institucional existente.

---

## 1. El principio

Un alias no significa *"esta frase podría tener alguna relación con esta
entidad"*. Significa:

> **cuando el usuario usa esta frase como comprador, esta entidad es una
> interpretación institucionalmente válida.**

De ahí la regla que ordena todo el bloque: **la cobertura no justifica un
ownership incorrecto**. Cuando el comprador raíz no existe en el gazetteer, la
respuesta correcta al retirar el alias es `FAIL_CLOSED`. Es preferible

> *"No tengo resuelta la Gobernación de Cundinamarca"*

a

> *"la Gobernación de Cundinamarca es un hospital de Sopó"*.

Es la misma tesis de `INV-NO-SILENT-SEMANTIC-FALLBACK` (ADR-069) aplicada al
dato en vez de al código: el defecto no es el hueco, es la degradación
silenciosa que lo tapa.

---

## 2. Universo medido

| Métrica | Valor |
|---|---:|
| Entidades | 559 |
| Aliases (crudos) | 10 840 |
| Aliases normalizados únicos | 8 609 |
| Grupos con dueño múltiple exacto | **217** |
| Grupos de posible misma identidad lógica | 6 |

La normalización es la misma de `EntityResolver` (`normalize_text`), de modo
que lo que aquí se llama colisión es exactamente lo que el runtime ve.

### Hallazgos por clasificación

| Clasificación | Hallazgos | Acción |
|---|---:|---|
| `PARENT_TO_CHILD_HIJACK` | 91 | `REMOVE_WRONG_ALIAS` |
| `TERRITORY_AS_ENTITY_HIJACK` | 14 | `REMOVE_WRONG_ALIAS` |
| `GENERIC_STRUCTURAL_ALIAS` | 9 | 6 `REMOVE`, 3 `MANUAL_REVIEW` |
| `CROSS_ENTITY_HIJACK` | 3 | `REMOVE_WRONG_ALIAS` |
| `POSSIBLE_SAME_LOGICAL_IDENTITY` | 9 | `SAME_IDENTITY_REVIEW` |
| `MANUAL_REVIEW` | 166 | — |
| **Remociones propuestas** | **114** | sobre **28** entidades |

---

## 3. Cómo se decide, y por qué así

El auditor no juzga "parecido": juzga **lo que el alias declara** contra **lo
que la entidad es**. Cinco reglas deterministas, todas verificables sobre el
JSON.

### 3.1 El alias declara un tipo institucional que el dueño no es

`alcaldía de Sopó` declara un comprador municipal; el dueño es una E.S.E. Un
prestador de salud no es un gobierno municipal. → `PARENT_TO_CHILD_HIJACK`.

### 3.2 El alias es la frase DESNUDA del comprador raíz

`gobernación del valle` no contiene nada que distinga a `… - DADI` de la
Gobernación entera. Basta **un** token propio para que el alias deje de ser la
frase del padre: `gobernación del valle del cauca **dadi**` se conserva, igual
que `alcaldía de Medellín **- inder**` o `alcaldía de Bogotá **uaesp**`.

Para saber si un token es un lugar o una materia —"sopó" sí, "movilidad" no— el
auditor **deriva un vocabulario de topónimos del propio gazetteer**: toda frase
`municipio de X` / `alcaldía de X` / `gobernación de X` declara que X es un
lugar. El repositorio no tiene lista de municipios; esta es evidencia interna,
no una lista escrita a mano.

### 3.3 Territorio desnudo en manos de quien no lo gobierna

`san andres` perteneciendo a `HOSPITAL SAN ANDRES` (de **Tumaco, Nariño**).
`atlantico` a una E.S.E. `bogota d.c.` a cuatro entidades a la vez.

### 3.4 El nombre inequívoco de otra institución

`ministerio de minas` perteneciendo a `ANM`, existiendo
`MINISTERIO DE MINAS Y ENERGIA`. También el territorio ajeno:
`gobernación del cauca` perteneciendo al DADI **del Valle**.

### 3.5 Alias sin identidad ni territorio

`gobierno departamental`, `gobernación departamental`, `el distrito capital`:
sólo tokens estructurales. Capturan la frase de cualquier territorio.

### Lo que el auditor deliberadamente NO toca

| Caso | Por qué se deja en `MANUAL_REVIEW` |
|---|---|
| `la distrital`, `la nacional` (3 casos) | Con artículo es el apodo real de una universidad. Sin artículo no lo es: `distrital` a secas capturaba "gobierno distrital de Cali" y se retira. |
| Fondos de Desarrollo Local de Bogotá (116 casos) | En SECOP la alcaldía local y su FDL son el mismo comprador. Retirarle al fondo la frase de "su" alcaldía no corrige un ownership ajeno. |
| Dueños con nombre opaco y sin raíz alternativa (24) | `DILOF` ← "alcaldía de Fontibón". Sin comprador raíz demostrable, la remoción no está respaldada. |
| Cabezas nacionales desnudas y sin candidato (50) | `contraloría` a secas no pertenece a nadie en particular. |

Un dueño opaco **sí** se corrige: `UPTC` ← "gobernación de Boyacá" se retira
porque `GOBERNACIÓN DE BOYACÁ` existe. Y también cuando **no** existe raíz
alternativa —`PROSPERIDAD SOCIAL` ← "gobernación de Bogotá"—: una entidad que no
es un gobierno de ningún tipo no puede poseer la frase desnuda de un gobierno, y
el resultado correcto es `FAIL_CLOSED`. Es la misma regla que
`TERRITORY_AS_ENTITY_HIJACK`, un nivel más arriba.

---

## 4. Los cinco casos que motivaron el bloque

### A. Cundinamarca / Sopó

`E.S.E. HOSPITAL DIVINO SALVADOR DE SOPO` (Cundinamarca, 4 696 procesos) posee
`gobernación de cundinamarca`, `gobierno de cundinamarca`,
`departamento de cundinamarca`, `alcaldía de sopó` y `municipio de sopó`.
Es hijack del padre **departamental** y del **peer municipal** a la vez.

Se conservan sus 18 aliases hospitalarios.

### B. Valle / DADI y sus hermanas

Cuatro registros comparten el prefijo `GOBERNACION DEL VALLE DEL CAUCA` y
**ninguno es la raíz**. La frase `gobernación del valle` está disputada por los
cuatro: gana quien quede último en el JSON, no quien tenga derecho.

Se conserva todo alias que nombre la unidad (`dadi`, `… secretaría de
educación`, `… infraestructura`).

### C. Hospital San Andrés

`HOSPITAL SAN ANDRES` es de **Tumaco (Nariño)** y lleva los aliases territoriales
desnudos `san andres` / `san andrés`, que capturan al archipiélago.

Se conservan `hospital san andres`, `hospital san andres de tumaco`,
`ips san andres`, `hsa` y las 17 formas restantes.

### D. Tolima

`GOBIERNO DEPARTAMENTAL` (Tolima, 27 694 procesos) es el comprador **real** de
la Gobernación del Tolima: SECOP registra ese nombre. El problema es su alias
`gobierno departamental` sin territorio, que captura "gobierno departamental del
Cauca" y produce un `WHERE` contradictorio —comprador del Tolima ∧ territorio del
Cauca (WB-04 de R0)—.

Se conservan `gobierno departamental del tolima`, `gobierno departamental de
tolima`, `gobernación del tolima`, `departamento del tolima` y las demás formas
con territorio.

### E. ANM / MinMinas

`ANM` posee `ministerio de minas`, `ministerio de minas y energia` y
`ministerio de minas y energía`, existiendo `MINISTERIO DE MINAS Y ENERGIA`
(11 151 procesos, 42 aliases). Se conservan `anm`, `agencia nacional de
minería`, `autoridad minera` y el resto de su identidad real.

---

## 5. Hallazgo nuevo: el gate `strict_rewrite` estaba tapando dos hijacks

R0 reportó Atlántico y Boyacá como `PASS_EXACT`. Medido sobre `alias_index`, la
resolución **exacta** de ambas frases está secuestrada:

| Frase | Dueño exacto (antes de R1C) | Lo que devuelve el runtime |
|---|---|---|
| `gobernación del atlántico` | `EMPRESA SOCIAL DEL ESTADO UNIVERSITARIA DEL ATLANTICO//` | `DEPARTAMENTO DEL ATLANTICO**` por `fuzzy(97)` |
| `gobernación de boyacá` | `UPTC` | `GOBERNACIÓN DE BOYACÁ` por `fuzzy(95)` |

Aciertan **por accidente**: `_strict_rewrite_target` rechaza el match exacto
porque el nombre oficial de la ESE/universidad no contiene un marcador
departamental, y el fuzzy que sigue da con la entidad correcta. El canario
ADR-040 del Atlántico pasa sin que el dato lo sostenga. Si mañana el nombre
oficial de la ESE cambiara —o el cutoff del fuzzy—, el canario caería sin que
ningún test lo viera venir.

Es la razón por la que los tests de R1C se escriben sobre `alias_index` y no
sólo sobre `resolve_entidad`: **el ownership es una propiedad del dato**, y una
capa de código que lo compensa no lo corrige.

---

## 6. Identidad lógica ≠ nombre físico — los 6 grupos NO son ramas muertas

R0 describió las variantes con `total_procesos = 0` como posibles "ramas
muertas". **La evidencia lo refuta.** Los seis grupos tienen exactamente el
mismo patrón:

| Identidad | Nombre físico A | Nombre físico B |
|---|---|---|
| Municipio de Soacha | `MUNICIPIO DE SOACHA..` — 20 463 proc / 0 contr · `["procesos"]` | `MUNICIPIO DE SOACHA.` — 0 proc / 20 145 contr · `["contratos"]` |
| Municipio de Yopal | `MUNICIPIO DE YOPAL...` — 16 779 / 0 · `["procesos"]` | `MUNICIPIO DE YOPAL..` — 0 / 18 386 · `["contratos"]` |
| Fondo Financiero Distrital de Salud | `…SALUD..` — 23 264 / 0 · `["procesos"]` | `…SALUD.` — 0 / 21 057 · `["contratos"]` |
| Ministerio de Justicia (largo) | `…DERECHO..` — 7 548 / 0 · `["procesos"]` | `…DERECHO.` — 0 / 6 769 · `["contratos"]` |
| MinJusticia (sigla) | `MINJUSTICIA..` — 7 449 / 0 · `["procesos"]` | `MINJUSTICIA.` — 0 / 6 763 · `["contratos"]` |
| UAESP | `…PÚBLICOS..` — 6 029 / 0 · `["procesos"]` | `…PÚBLICOS.` — 0 / 5 715 · `["contratos"]` |

**6 de 6** son pares complementarios: `source_datasets` disjuntos, volúmenes del
mismo orden de magnitud, cero solapamiento. No hay una rama viva y una muerta:
hay **una identidad lógica con dos nombres físicos, uno por dataset SECOP**.

```
LOGICAL_IDENTITY      Municipio de Soacha
SOURCE_PHYSICAL_NAME  "MUNICIPIO DE SOACHA.."   (procesos,  p6dx-8zbt)
SOURCE_PHYSICAL_NAME  "MUNICIPIO DE SOACHA."    (contratos, jbjy-vk9h)
```

Un `total_procesos = 0` no prueba inactividad: prueba que **ese nombre no existe
en ese dataset**. Borrar, fusionar o elegir una variante por el conteo produciría
un `WHERE` que devuelve cero filas en la mitad de las consultas.

`action = SAME_IDENTITY_REVIEW` para los seis. **R1C no los toca**, no consolida
identidades y no cambia SoQL ni la resolución dataset-aware. Si el patrón se
confirma sistémico, merece un frente propio: la arquitectura correcta es
`canonical_entity` + `source_names` por dataset, y esa es una decisión de diseño,
no un lote de aliases.

MinJusticia aparece además entre los hallazgos de alias (9 casos, `ministerio de
justicia` en la sigla): quedan como `SAME_IDENTITY_REVIEW`, **no** como
cross-entity hijack, porque `MINJUSTICIA` es una contracción del nombre largo, no
una entidad ajena.

---

## 7. Deuda que R1C deja abierta a propósito

| # | Deuda | Por qué no se cierra aquí |
|---|---|---|
| D1 | `GOBIERNO DEPARTAMENTAL` **se llama así**. Retirado el alias, el `official_name` sigue en el índice de scan y "gobierno departamental del Cauca" lo seguirá encontrando. | Arreglarlo exige tocar `EntityResolver` (rechazar nombres oficiales genéricos sin calificador), no el gazetteer. |
| D2 | "Gobernación de Cundinamarca" seguirá resolviendo a `DEPARTAMENTO DE CUNDINAMARCA- SECRETARIA DE SALUD`. | Esa secretaría **no tiene** el alias: llega por `fuzzy` Tier-3 sobre su `official_name`, que satisface la reescritura estricta. Es un defecto del resolver, no de ownership. |
| D3 | `resolve_entidad("gobernación del Tolima")` devuelve `rewrite_unresolved` pese a que el ownership es correcto. | `_strict_rewrite_target` exige que el `official_name` contenga el territorio; `GOBIERNO DEPARTAMENTAL` no lo contiene. Preexistente. |
| D4 | 116 aliases de Fondos de Desarrollo Local de Bogotá. | Requiere decidir si la alcaldía local y su FDL son un comprador o dos. |
| D5 | Los 6 grupos de identidad lógica. | Frente propio (§6). |
| D6 | `_is_unqualified_dept` rechaza **todo** alias que *termine* en un nombre de departamento (`endswith`), sea o no un departamento desnudo. "secretaría general de la alcaldía mayor de **bogotá**" y "ese del departamento de **meta**" quedan rechazados pese a nombrar la entidad. | Preexistente y de runtime. R1C lo hace visible porque cambia a qué cae el scan cuando el hijack desaparece. |

D1, D2 y D3 comparten causa: **el runtime compensa con fuzzy lo que el dato no
sostiene**. R1C limpia el dato y deja los tres visibles.

---

## 8. Prevención de regresión

El `validate` existente **ya detecta** esta clase de fallo, pero no estaba
gateada: `--full-scan` recorre cada alias de cada entidad y reporta cuando
`scan_entity` devuelve un dueño distinto del esperado. Lo que no tenía era una
puerta que distinguiera **ownership múltiple exacto** (dos entidades reclamando
la misma frase, donde el runtime sólo puede honrar a una) del ruido histórico.

Se añade a `do_validate` un chequeo offline de `EXACT_MULTI_OWNER` que:

* reporta **siempre** el conteo, y
* **falla** sólo si el número crece respecto del baseline declarado (217).

No se convierte el backlog histórico en un fallo global —rompería el baseline
por deuda conocida— ni se crea una allowlist que esconda casos: la deuda queda
contada y cualquier entrada nueva que la aumente falla.

---

## 9. Estado post-remediación — lo que cambió, medido

**114 aliases retirados** (145 cadenas crudas, contando grafías con y sin tilde)
de **28** entidades. Ninguna quedó por debajo de 3 aliases; ninguna perdió su
nombre oficial. Aliases: 10 840 → 10 695. Grupos con dueño múltiple: 217 → **173**.

### Canarios

| # | Sonda | Antes | Después |
|---|---|---|---|
| A | `gobernación de cundinamarca` (ownership) | E.S.E. Hospital Divino Salvador de Sopó | *(nadie)* → `FAIL_CLOSED` |
| A | `hospital divino salvador de sopó` | hospital | **hospital** (sin cambio) |
| B | `gobernación del valle` | Secretaría de Infraestructura / DADI | *(nadie)* → `FAIL_CLOSED` |
| B | `dadi valle` | DADI | **DADI** (sin cambio) |
| C | `san andrés` (ownership) | `HOSPITAL SAN ANDRES` | *(nadie)* |
| C | `hospital san andrés` | hospital | **hospital** (sin cambio) |
| D | `gobierno departamental` (ownership) | `GOBIERNO DEPARTAMENTAL` (Tolima) | *(nadie)* |
| D | `gobierno departamental del tolima` | Tolima | **Tolima** (sin cambio) |
| E | `ministerio de minas` | **`ANM`** | **`MINISTERIO DE MINAS Y ENERGIA`** |
| E | `agencia nacional de minería` | `ANM` | **`ANM`** (sin cambio) |

### Dos canarios que dejaron de aprobar por accidente

| Sonda | Antes (exacto) | Después (exacto) |
|---|---|---|
| `gobernación del atlántico` | `EMPRESA SOCIAL DEL ESTADO UNIVERSITARIA DEL ATLANTICO//` | **`DEPARTAMENTO DEL ATLANTICO**`** |
| `gobernación de boyacá` | `UPTC` | **`GOBERNACIÓN DE BOYACÁ`** |

Ambas ya devolvían la entidad correcta, pero por `fuzzy`. Ahora también por
ownership: el canario ADR-040 pasa a estar sostenido por el dato (§5).

### Compradores correctos que estaban tapados

Retirar el hijack no siempre deja un hueco: en tres casos destapó a la entidad
que sí existía y perdía por longitud de alias.

| Sonda | Antes | Después |
|---|---|---|
| `alcaldía de fontibón` | `DILOF` | **`ALCALDIA LOCAL DE FONTIBON`** |
| `municipio de fontibón` | `DILOF` | **`ALCALDIA LOCAL DE FONTIBON`** |
| `alcaldía de armenia` | `GOBERNACION DEL QUINDIO ARMENIA` | **`MUNICIPIO DE ARMENIA QUINDIO`** |
| `departamento de meta` | `EMPRESA SOCIAL DEL ESTADO DEL DEPARTAMENTO DEL META E.S.E` | **`DEPARTAMENTO DEL META`** |

### Cobertura perdida, declarada

De 519 sondas (los aliases retirados más los conservados de las entidades
tocadas), **48 pasan a `FAIL_CLOSED`**. Todas eran frases de comprador raíz en
manos ajenas. Las que más pesan:

* **Bogotá.** `alcaldía de bogotá`, `municipio de bogotá`, `alcaldía mayor
  bogotá` y `la alcaldía mayor` no resuelven a nada. El gazetteer **no tiene**
  la Alcaldía Mayor de Bogotá: la frase pertenecía a tres secretarías
  distritales a la vez. Es la pérdida más visible y el mejor argumento para un
  lote de expansión que la agregue —fuera del alcance de R1C—.
* **Valle del Cauca.** Ninguna de las cuatro subentidades responde ya por la
  Gobernación. Cuatro registros con el mismo prefijo y ninguna raíz.
* **Municipios sin comprador raíz**: Sopó, Yarumal, Remedios, Fusagasugá. Sus
  hospitales dejaron de hacer de alcaldía.

Tres sondas empeoran por la deuda **D6**, no por el dato: `ese del departamento
de meta` y dos variantes resuelven ahora a `DEPARTAMENTO DEL META` en vez de a
la E.S.E., porque `_is_unqualified_dept` rechaza el alias largo de la E.S.E. por
terminar en "meta". El alias sigue siendo suyo y la resolución exacta lo honra;
es el `scan` el que no.

### Validación

```
tests/test_gazetteer_alias_ownership_r1c.py   44 passed   (18 rojos antes)
tests focales del gazetteer y el resolver     123 passed, 13 xfailed
suite completa                                9781 passed, 2 skipped, 24 xfailed
scripts/run_query_corpus.py --mode all        89/89 PASS
gazetteer_manager.py validate                 0 errores · 173/173 multi-owner
make lint-core                                app/core/ limpio
ruff check app/ tests/ scripts/               All checks passed
```

Un test existente cambió de sujeto:
`test_v3_scan_entity_keeps_qualified_alcaldia` usaba "alcaldía de Bogotá" para
comprobar que `_is_unqualified_generic` no rechaza un alias con calificador
territorial. Como esa frase ya no tiene dueño válido, el sujeto pasa a
"alcaldía de Cartagena". Lo que el test verifica no cambió.

---

## 10. Salida

```
GAZETTEER_ALIAS_OWNERSHIP_R1C — evidencia congelada

ENTITIES:                     559
TOTAL_ALIASES:                10840
UNIQUE_NORMALIZED_ALIASES:    8609
EXACT_MULTI_OWNER_GROUPS:     217
PARENT_CHILD_HIJACKS:         91
CROSS_ENTITY_HIJACKS:         3
GENERIC_STRUCTURAL:           9   (6 remove / 3 manual review)
TERRITORY_HIJACKS:            14
SAME_LOGICAL_IDENTITY_REVIEW: 6 grupos (9 hallazgos de alias)
MANUAL_REVIEW:                166
PROPOSED_REMOVALS:            114 sobre 28 entidades
```

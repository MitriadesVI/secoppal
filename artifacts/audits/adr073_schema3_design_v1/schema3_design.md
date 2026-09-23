# ADR-073 Schema 3 — Diseño (D1)

**Estado:** diseño, NO activado. `app/data/buyer_source_names.json` sigue en
Schema 2 y el runtime no cambió.
**Base:** `main` @ `c9bac8a9` · **Corrida:** 2026-09-23
**Evidencia:** `artifacts/audits/adr073_schema3_design_v1/`
(`schema3_consumer_inventory.json`, `schema2_to_schema3_parity.json`,
`schema3_adversarial_cases.json`, `schema3_draft.json`,
`admissible_570_matrix.jsonl`) y `artifacts/audits/adr073_d0_canary_autopsy_v1/`.

---

## 0. El problema que Schema 3 resuelve

Schema 2 guarda **una** llave por binding:

```json
"identity_key": {"type": "NIT", "value": "899999061"}
```

y `parse_registry` exige unicidad de esa llave (`identity_key duplicada`). Eso
funciona mientras la llave sea estructural (`BUYER_UNIT`) o el NIT sea único
por identidad — 18 bindings hoy. Deja de funcionar en el momento en que dos
BuyerUnits legítimamente distintas comparten NIT, que es el caso medido del
censo:

```text
SENA      899999034  → 39 BuyerUnits
Bogotá    899999061  → 39
ICBF      899999239  → 34
Cali      890399011  → 19
Valle     890399029  →  7
DANE      899999027  →  7
```

Schema 3 no "hace el NIT repetible" dentro de la misma semántica: **saca la
evidencia fiscal de la identidad**. Tres dimensiones, tres autoridades:

```text
BuyerUnit          identidad lógica de comprador
Fiscal evidence    evidencia sobre NIT/documento (0..n, puede repetirse)
Source projection  nombres físicos exactos por dataset
```

Invariantes (ADR-074 sigue siendo la autoridad normativa):

```text
BuyerUnit != NIT
same NIT != same BuyerUnit
directory NOT_FOUND != identity disproved
directory evidence != source projection
directory evidence != BuyerGroup membership
BuyerGroup membership jamás se deriva del NIT
```

---

## 1. D1.1 — Inventario de consumidores (medido, no supuesto)

`schema3_consumer_inventory.json` recorre 26 archivos declarados y clasifica
cada ocurrencia de `identity_key`, `_parse_identity_key`,
`BuyerSourceNameRegistry`, `load_registry`, `parse_registry`,
`project_buyer_names`, `BuyerSourceBinding`, `names_for`, `binding_for`.

| categoría | archivos | lectura |
|---|---|---|
| RUNTIME_PROJECTION | `app/core/soql_builder.py`, `app/core/analytics/contract_activity/admission.py`, `app/core/buyer_groups.py` | llaman `project_buyer_names(...)` y usan la tupla resultante |
| RUNTIME_LOADER | `app/core/buyer_source_names.py` | define y valida el contrato |
| AUDIT_ONLY | auditores del carril ADR-073 | leen `binding.identity_key` como autoridad de auditoría |
| TEST_ONLY | tests del carril | fijan el contrato |
| PRODUCT_DATA | `app/data/buyer_source_names.json` | declara `identity_key` (18 NIT + 575 BUYER_UNIT) |
| OTHER | `app/core/actor_name.py`, `app/core/actor_identity.py` | dominio homónimo (actor), no buyer source names |

### La pregunta del encargo, contestada con el código

> ¿El runtime de ADR-073 necesita realmente `identity_key` para proyectar nombres?

**NO.** Cadena completa de la proyección productiva:

```text
soql_builder._entity_clause            → project_buyer_names(valor, dataset_id)
admission._source_names                → project_buyer_names(buyer, "jbjy-vk9h")
buyer_groups.BuyerGroup.names_for      → project_buyer_names(member, dataset_id)
        ↓
BuyerSourceNameRegistry.project(canonical, dataset)
        ↓  binding_for(canonical)  → índice por normalize_text(canonical_name)
        ↓  binding.names_for(dataset) → sólo lee `sources`
```

`identity_key` no aparece en ningún punto de esa cadena. Su único uso en el
loader es la validación de unicidad al cargar (`_parse_identity_key` + set de
duplicados). **Es una llave de auditoría, no un insumo de proyección**: mover la
evidencia fiscal a una dimensión propia no cambia la API productiva ni un solo
`WHERE`.

El inventario lo verifica además al revés: `identity_key_readers.runtime_outside_loader == []`.

---

## 2. D1.2 — Contrato Schema 3

### 2.1 Forma

```json
{
  "schema_version": 3,
  "authority": "ADR-073",
  "buyer_units": [
    {
      "canonical_name": "DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA",
      "source_names": {
        "p6dx-8zbt": ["DAPRE"],
        "jbjy-vk9h": ["DAPRE"]
      },
      "projection_evidence": {
        "observed_at": "2026-09-02",
        "evidence": "artifacts/audits/source_name_projection_v1.json",
        "observed_rows": {"p6dx-8zbt": 17128, "jbjy-vk9h": 4351}
      },
      "fiscal_evidence": [
        {
          "scheme": "NIT",
          "base_number": "89999908",
          "raw_forms": ["899999083"],
          "check_digit": {"declared": "3", "computed": 3, "split_validated": true},
          "shared": false,
          "kind": "ACCREDITATION",
          "source": "buyer_source_names.json#schema2:identity_key",
          "observed_at": "2026-09-02",
          "evidence": "artifacts/audits/source_name_projection_v1.json"
        },
        {
          "scheme": "NIT",
          "base_number": "89999908",
          "raw_forms": ["899999083"],
          "check_digit": {"declared": "3", "computed": 3, "split_validated": true},
          "shared": false,
          "kind": "SOURCE_OBSERVATION",
          "source": "SECOP_DIRECTORY",
          "observed_at": "2026-09-23",
          "evidence": "artifacts/audits/adr073_directory_identity_attestations_v1/directory_identity_attestations.jsonl",
          "outcome": "FOUND_UNIQUE",
          "outcome_note": null
        }
      ]
    }
  ]
}
```

Cambios frente al Schema 2:

| Schema 2 | Schema 3 | por qué |
|---|---|---|
| `identity_key` (único global) | **no existe** | una llave fiscal no puede ser la identidad de una BuyerUnit |
| `sources` | `source_names` | mismo contenido, nombre que dice lo que es |
| `observed_at` / `evidence` / `observed_rows` del binding | `projection_evidence` | eran procedencia de la PROYECCIÓN; ahora se declara como tal |
| — | `fiscal_evidence[]` | 0..n atestaciones, repetibles entre BuyerUnits |
| — | `kind` / `outcome` | distingue acreditación de observación de fuente |

### 2.2 Identidad

Una BuyerUnit se identifica por `canonical_name`. La unicidad que el loader
exige es la del **canónico normalizado** (`normalize_text`), exactamente la que
ya usa el índice `_by_canonical` del registro actual. No hay segunda llave.

### 2.3 Evidencia fiscal: 0..n, repetible

- Cada entrada es UNA atestación: `(scheme, base_number, raw_forms, source,
  observed_at, evidence)` más `kind` y, si es observación, `outcome`.
- Una BuyerUnit puede tener **0..n** entradas. Sin NIT es válida (caso B).
- Un mismo `base_number` puede aparecer en **1..n** BuyerUnits: no hay
  restricción de unicidad sobre la evidencia fiscal (caso A). En el draft, las
  bases `899999061` (Bogotá) y `899999239` (ICBF) aparecen en 37 y 32 BuyerUnits
  respectivamente sin colisión de loader.
- `kind`:
  - `ACCREDITATION` — una autoridad acredita el documento.
  - `SOURCE_OBSERVATION` — una fuente lo observó (o no lo encontró). No
    acredita.
- `shared` es una **observación derivada** (misma base en ≥2 BuyerUnits del
  registro), recomputable, y jamás un insumo de proyección. En el draft:
  `false` para las 18 acreditaciones (bases distintas) y `true` sólo donde el
  censo lo declaró compartido.
- Base/DV: la separación sólo ocurre si el módulo 11 oficial
  (`app/core/actor_identity.compute_check_digit`) la valida. Un DV que no cuadra
  se anota en `check_digit` y **la identidad no se reescribe** (caso D).

### 2.4 Proyección

La API productiva **no cambia**:

```python
project_buyer_names(canonical_name, dataset_id) -> tuple[str, ...] | None
```

`None` = sin binding (el llamador conserva su camino previo). La tupla conserva
los nombres byte-for-byte: puntuación, acentos y mayúsculas son parte del
binding. El prototipo `Schema3Registry.project` implementa exactamente esta
semántica y la paridad se mide contra el registro v2 (§3).

### 2.5 Procedencia: tres cosas que no se mezclan

| pregunta | dónde se contesta |
|---|---|
| ¿qué artefacto acredita los nombres físicos de esta BuyerUnit? | `projection_evidence` |
| ¿qué autoridad acredita el documento fiscal? | `fiscal_evidence[kind=ACCREDITATION].source` + `.evidence` |
| ¿qué observó una fuente externa sobre ese documento? | `fiscal_evidence[kind=SOURCE_OBSERVATION].source` + `.outcome` |

Un `evidence` de raíz no existe (igual que en Schema 2): un campo de raíz sólo
podría contestar "¿qué lote tocó el archivo al final?", que no acredita nada.

### 2.6 Ausencia

`directory_not_found` se representa como **ausencia de atestación de esa
fuente**:

```json
{"kind": "SOURCE_OBSERVATION", "source": "SECOP_DIRECTORY", "outcome": "NOT_FOUND",
 "outcome_note": "ausencia de atestación de ESA fuente; no es evidencia negativa ni invalida acreditaciones de otras autoridades"}
```

No invalida una acreditación de otra autoridad (caso E, y los 6 canarios de D0
en el draft real). `outcome` admite `FOUND_UNIQUE`, `FOUND_MULTI`, `NOT_FOUND`,
`PARTIAL`, `INCONCLUSIVE`, `NO_OBSERVED_NIT`.

### 2.7 Qué rechaza el loader (prototipo)

- `schema_version != 3`; `identity_key` en raíz o en cualquier unidad;
- `buyer_units` vacío; canónico duplicado tras normalizar;
- dataset desconocido, lista de nombres vacía o con duplicados normalizados;
- `projection_evidence` sin `observed_at` ISO o sin `evidence`;
- entrada fiscal sin `scheme`/`base_number` numérica/`raw_forms`/`shared`
  booleano/`source`/`observed_at`/`evidence`;
- `kind` fuera de `{ACCREDITATION, SOURCE_OBSERVATION}`;
- `SOURCE_OBSERVATION` sin `outcome` válido.

---

## 3. D1.3 — Migración offline y paridad

`scripts/audits/adr073_schema3_migration_v1.py` migra el sidecar vigente a un
**draft** en `artifacts/audits/adr073_schema3_design_v1/schema3_draft.json`. El
sidecar no se toca (`sha256_before == sha256_after`).

Gate de paridad (byte-for-byte, sobre proyección exacta), en **dos bloques
explícitos** más un barrido de control:

```text
positivo      bindings v2 × dataset              593 × 2 = 1186 comparaciones · 0 diffs
complemento   censo SIN binding × dataset        518 × 2 = 1036 comparaciones · 0 diffs
barrido       censo completo × dataset          1111 × 2 = 2222 comparaciones · 0 diffs
consistencia  1186 + 1036 = 2222  ·  593 + 518 = 1111
verdict       PASS
```

`project_v2(canonical, dataset) == project_v3(canonical, dataset)` para todo
binding y todo dataset declarado, y `None == None` para toda identidad del censo
sin binding. Además, la migración no movió los INSUMOS por debajo
(`structural_diff`): `canonical_changes = 0`, `source_name_changes = 0`,
`dataset_changes = 0`, `projection_provenance_changes = 0`, `total_changes = 0`.
Ningún nombre físico, dataset, canónico ni fecha cambió.

**Corrección de una etiqueta falsa (antes/después).** La primera versión del
harness pasaba el censo ENTERO al bloque de complemento y el contador se
publicaba como `identities_without_binding_compared = 2222`. Ese número era —y
es— el barrido del censo completo (1111 × 2 datasets), no el complemento. No
hubo doble cómputo: hubo una etiqueta que afirmaba algo que el dato no era. El
complemento real es `1111 − 593 = 518` identidades → **1036** comparaciones, y
ahora se DERIVA dentro de `parity_report` (censo − bindings por canónico
normalizado) para que la etiqueta no pueda volver a desprenderse del dato. El
artefacto conserva el número anterior en `labeling_correction` con su
significado real, y el test
`test_identity_complement_is_the_set_difference_not_the_sweep` fija la
regresión (`complement_comparisons != census_sweep_comparisons`).

Composición del draft:

```text
buyer_units                        593
fiscal_evidence entries            612
units_with_accreditation            18
units_with_observation_only        575
units_without_fiscal_evidence        0
observations_by_outcome   FOUND_UNIQUE 383 · FOUND_MULTI 150 · NOT_FOUND 61
bases en ≥2 BuyerUnits    899999061 → 37 · 899999239 → 32
identity_key en unidades             0
```

---

## 4. D1.4 — Casos adversariales

`schema3_adversarial_cases.json` (todos PASS):

| caso | expectativa | resultado |
|---|---|---|
| A `shared_nit` | el loader acepta dos BuyerUnits con el mismo NIT | 2 unidades, base `899999061` declarada por ambas |
| B `no_nit` | BuyerUnit sin evidencia fiscal es válida | `fiscal_evidence == []` y proyección intacta |
| C `multiple_raw_forms` | base `890399011` + raw `8903990113` (DV 3 válido) | ambas formas conservadas, sin reescribir la fuente |
| D `invalid_dv` | DV que no valida no colapsa al base | `8999990615` se conserva íntegro, `split_validated=false` |
| E `directory_not_found` | no invalida otra autoridad | acreditación + observación `NOT_FOUND` coexisten |
| F `buyer_group` | mismo NIT ≠ un miembro | `BuyerGroup(members=(A,B))` mantiene 2 miembros distintos |
| G `projection_split` | nombres distintos por dataset | `MUNICIPIO DE SOACHA..` (procesos) vs `MUNICIPIO DE SOACHA.` (contratos) |

---

## 5. D1.5 — Los 570 ADMISSIBLE

`admissible_570_matrix.jsonl` (570 filas) declara por identidad: canónico,
`current_binding`, attestation del directorio, estado de evidencia fiscal,
`shared_nit`, cardinalidad de cuentas, relación de nombres, candidato de
proyección y `requires_gazetteer_reaudit = yes`.

```text
census_class      ALREADY_BOUND_VERIFIED 319 · NO_STABLE_IDENTITY 251
current_binding   yes 319 · no 251
fiscal_status     NIT_FOUND_UNIQUE_ACCOUNT 570
name_relation     EQUAL_TOKENS 570
shared_nit        NOT_SHARED 570
promotion_status  NOT_EVALUATED 570     (no existe READY_TO_PROMOTE)
```

**Ninguno se promueve.** La matriz es medición: falta la corrida
Gazetteer-Wide con estas attestations y una decisión humana.

### Sobre los 424 REVIEW

No se reducen a dos `if`: la matriz preserva por separado `name_relation`,
`fiscal_identity_evidence`, `shared_nit` y `directory_account_count`, y
`shared_nit = true` **no** implica binding inadmisible — puede existir
perfectamente `BuyerUnit A ↔ NIT X` acreditada junto a `BuyerUnit B ↔ NIT X`
acreditada con `A != B`. Schema 3 sólo tiene que poder representarlo.

---

## 6. Plan de implementación (misiones siguientes, NO ahora)

1. **SCHEMA 3 IMPLEMENTATION** (runtime, revisable y atómica):
   - `app/core/buyer_source_names.py`: `SCHEMA_VERSION = 3`; `BuyerSourceBinding`
     pierde `identity_key` y gana `fiscal_evidence`; la unicidad queda sólo en el
     canónico; el loader rechaza `identity_key`.
   - Los tres consumidores de proyección **no cambian** (inventario D1.1).
   - Tests: los adversariales A–G como tests de contrato del loader real.
   - Gate: la misma paridad positiva 1186/1186 **más** el complemento 1036/1036
     (barrido del censo 2222/2222 como control, nunca como complemento).
2. **GAZETTEER-WIDE REAUDIT** con las attestations del directorio.
   **Requisito explícito: la próxima Gazetteer-Wide debe ser SCHEMA-3-AWARE.**
   No puede reutilizar sin cambios la semántica `binding.identity_key → Identity`.
   Hoy el auditor vigente hace, en `audit_gazetteer_source_bindings_v1.py`:

   ```python
   "identity_key": binding.identity_key,                       # línea 962 (_binding_payload)
   type_, _, value = binding.identity_key.partition(":")
   record.identity = Identity(type_, value, ...)               # línea 998 (build_inventory)
   ```

   Bajo Schema 3 eso ya no compila ni es correcto: `identity_key` no existe, la
   identidad de la BuyerUnit es `canonical_name` y la evidencia fiscal es 0..n
   atestaciones (varias fuentes, mismo `base_number`, `shared` derivado). El
   re-audit debe:

   - **sustituir** `Identity(type_, value)` por la lista `fiscal_evidence[]`
     (0..n), sin elegir una "principal" por orden ni por frecuencia;
   - **no** derivar identidad de la evidencia fiscal: la unidad ya está
     identificada por su canónico;
   - conservar en el registro de salida la separación
     `BuyerUnit / fiscal_evidence / source_names`, sin volver a aplanarla a un
     único `identity_key`;
   - tratar `NOT_FOUND` como ausencia de atestación de esa fuente (nunca como
     evidencia negativa), igual que el resto del carril.

   Los puntos exactos que leen ese campo están inventariados en
   `schema3_consumer_inventory.json` →
   `gazetteer_wide_reaudit_requirement` (con archivo y línea), y los tests
   `test_dogfood_buyer_coverage_adr073.py` / `test_gazetteer_source_binding_audit.py`
   tendrán que fijar el contrato nuevo en lugar del `identity_key`.
3. **PROMOTION BATCH** independiente, con revisión humana de las dos políticas
   pendientes (a) `oficial ⊂ canónico` (35 casos), (b) multi-cuenta
   (260 + 290 compartidos).

---

## 7. Política de evidencia cruda (205 MB)

El corpus HTML del censo **no se commitea**. Se versiona lo que hace
reproducible el resultado sin cargar el repositorio:

```text
versionado:
  REPORT.md · summary.json · manifest.json
  directory_identity_attestations.jsonl (entregable plano)
  official_responses.sha256 (índice SHA256 de cada respuesta cruda)
  progress.jsonl (ledger de consultas)
  fixtures raw representativos (respuestas completas, no recortes)

fuera de Git (salvo autorización explícita):
  official_responses/*.html  (~205 MB)
  records/*.json             (1111 registros completos)
```

Fixtures representativos obligatorios, uno por clase:
`unique exact` · `shared NIT` · `multi-account` · `DV válido` · `DV inválido` ·
`directory not found` · `name subset` · `name intersection` · los 18 canarios.
Se eligen con la regla declarada en `artifacts/audits/adr073_raw_evidence_policy_v1/selection.json`
y se copian byte-for-byte con su SHA256 (el original permanece en el artefacto).

Un clon sin los HTML puede reconstruir toda la capa determinista desde
`progress.jsonl` + el índice: si un HTML falta, el rebuild lo declara como
problema en vez de inventar el resultado.

# HCA Final P1 — remediación de runtime

**Fecha:** 2026-08-26
**Rama:** `feature/hca-final-p1-runtime-fixes`
**Base:** `main = eb8cfe1ed72ba5c9df0ab9c0ead5089ff5d09368`
**Alcance:** los seis `REAL_RUNTIME_BUG` confirmados por
`docs/audits/2026-08-26_HCA_ORACLE_REVIEW.md`. Nada más.

```text
HCA_FINAL_P1_FIX_SPRINT: PASS
REAL_RUNTIME_BUGS_CLOSED: 6 / 6
HCA: 177/195 → 182/195   ·   rotos: 0
CORPUS_SHA256: 262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba
               (idéntico antes y después — el corpus no se editó)
```

## Los seis defectos

| Caso | Defecto | Estado |
|---|---|---|
| HCA-MONEY-020 | monto explícito < 1M descartado | cerrado |
| HCA-MONEY-026 | singular `millon` no admitido | cerrado |
| HCA-STATE-005 | `publicados` ensanchado al funnel completo | cerrado |
| HCA-STATE-007 | `contrato_activo` truncado en la fuente | cerrado |
| HCA-ENTITY-005 | comprador genérico convertido en tema | cerrado |
| HCA-FOLLOWUP-016 | ordenar cambiaba el `WHERE` | cerrado (ver §4) |

---

## 1. MONEY — la magnitud no es la evidencia

### Causa raíz

Dos síntomas, una raíz: el router juzgaba **la cifra** en vez de **la evidencia
que la acompaña**.

- Un piso duro de `1_000_000` COP en los ocho sitios donde el extractor acepta
  un monto. Existe por una razón real —evitar que "100 procesos", "2025" o un
  NIT se conviertan en dinero— pero estaba aplicado al número, así que también
  mataba montos declarados: `"más de 100 mil pesos"` resolvía a 100.000 y se
  descartaba.
- El catálogo de unidades del router escribía `millones?`, que **no acepta el
  singular**; la alternativa `mil`, colocada después, se comía el prefijo de
  `millon`. `"más de 1 millon"` valía 1.000 y también caía bajo el piso.

Además, el span de polaridad capturaba `"100 mil"` pero dejaba `"pesos"` fuera,
de modo que el marcador de moneda escrito por el usuario era invisible para
cualquier política de admisión.

### Fix

- `app/utils/money.py` publica la **gramática canónica de unidades**, pegada a
  `money_to_cop`, que es la capa que sabe escalarlas. El router deja de
  mantener su catálogo divergente. El lookahead de frontera va *dentro* del
  grupo: los consumidores escriben `{MONETARY_UNIT_PATTERN}?` y si quedara
  fuera, el `?` haría opcional la frontera en vez de la unidad.
- `app/core/money_admission.py` decide qué monto puede ser filtro:
  - **INV-EXPLICIT-MONEY-IS-NOT-DROPPED-BY-MAGNITUDE** — con unidad monetaria
    declarada (marcador de moneda o escala ≥ millón) se admite cualquier
    magnitud positiva. El piso anti-ruido sobrevive solo para números desnudos,
    que es contra lo que protegía.
  - **INV-NO-SILENT-EXPLICIT-MONEY-DROP** — un comparador con unidad declarada
    que no se puede convertir pide precisión en lugar de ejecutar sin cota.
- Los spans de polaridad capturan `pesos` con o sin `de`.

`"100 mil"` sin marcador de moneda **no** cuenta como evidencia: es tan
compatible con una cantidad de ítems como con un monto, y el módulo prefiere no
producir filtro a producir uno equivocado.

### Evidencia

```text
más de 100 mil pesos   → valor_min = 100000        (antes: sin filtro)
menos de 750 mil pesos → valor_max = 750000        (antes: sin filtro)
más de 1 millon        → valor_min = 1000000       (antes: sin filtro)
más de 1 millón        → valor_min = 1000000
más de 2 millones      → valor_min = 2000000       sin cambio
más de 500 millones    → valor_min = 500000000     sin cambio
```

Anti-confusión, todas verificadas sin cambio respecto de `eb8cfe1`: año 2025,
`NIT 900739049`, `100 procesos`, SMMLV con metadata indexada, rango simétrico
`entre 100 y 500 millones`, rango con guión `1000-3000 millones`, herencia de
unidad entre lados y rango contradictorio preservado para P8.

**Hallazgo del adversarial.** Con la gramática ya corregida quedó al descubierto
un caso que el otro defecto tapaba: `"más de un millón y medio de pesos"` pasó a
resolverse como 1.000.000. `money_to_cop` se niega a interpretar fracciones a
medias, pero solo puede negarse si las ve — y `"y medio"` quedaba fuera del
span. La fracción entra ahora en el span y el caso termina en pregunta:

```text
más de un millón y medio de pesos → CLARIFY, 0 source calls
más de 1.5 millones               → valor_min = 1500000   (sin cambio)
```

**Fuera de alcance, registrado.** `"contratos referencia CO1.PCCNTR.1234567"`
produce `valor_max = 1234567`. Verificado **idéntico en `eb8cfe1`**: es
preexistente, no lo introduce este sprint y no es uno de los seis casos.

---

## 2. STATE — exacto frente a familia

Decisión completa en **ADR-067**.

### Causa raíz

Nada garantizaba que la forma de **resolver** un estado y la forma de
**proyectarlo** coincidieran.

- `publicados` nombra un valor literal del dataset y se resolvía como familia →
  la fuente recibía `IN ('Publicado','Borrador','En aprobación','Aprobado','Abierto')`.
- `en ejecución` pide una familia (ADR-003) y se proyectaba como escalar. La
  familia la declaraba `intent_vocabulary`, que no conoce el dataset y por tanto
  nunca escribió filtros; el span quedaba consumido, `detect_estado_family` no
  lo volvía a ver y el extractor legacy dejaba `estado_contrato = 'En ejecución'`.

### Fix

`LEXICAL_EXACT_STATE` en `estado_families.py`, con precedencia sobre
`NATURAL_TO_FAMILY` y derivando la ubicación del literal de `ESTADO_FAMILIES` en
vez de duplicarla. `estado_projection.py` aplica
**INV-STATE-FAMILY-PROJECTS-COMPLETE** sobre los params ya resueltos, así que
cubre cualquier ruta que declare familia; **rellena y nunca ensancha**.

### Evidencia a nivel de fuente

```text
procesos publicados de aseo en cesar
  antes:  estado_del_procedimiento IN ('Publicado','Borrador','En aprobación','Aprobado','Abierto')
  ahora:  estado_del_procedimiento = 'Publicado'

contratos en ejecucion de la alcaldia de medellin
  antes:  estado_contrato = 'En ejecución'
  ahora:  estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')

oportunidades de aseo en cesar                      ← policy O1B, SIN CAMBIO
  estado_del_procedimiento IN ('Publicado','Abierto') + ventana de descubrimiento
```

Diferencial de SoQL sobre **50 consultas adversariales** contra `eb8cfe1`: 9
cambian, y los cambios de filtro de estado son exactamente los previstos —
`publicados`, `en borrador`, las tres formas de `en ejecución`, y un caso extra
descrito abajo. Las familias de un solo valor (`cerrado`, `terminado`,
`cedido`), `no_procede` sobre contratos y `en_evaluacion` quedan intactas.

**Efecto colateral deseado.** `"procesos desiertos"` declaraba
`estado_family = no_procede` y consultaba la fuente **sin ningún filtro de
estado** — el mismo defecto de HCA-STATE-007 en otra familia. Ahora proyecta
`IN ('Cancelado', 'Suspendido')`.

**Liquidación intacta.** `"contratos liquidados de obra en 2025"` y
`"procesos liquidados de obra en cauca"` siguen exactamente como estaban
(sin `estado_contrato`, dataset `contratos`). `STATE_LIQUIDATION_BEHAVIOR_CHANGED = NO`.

---

## 3. GENERIC BUYER — un comprador a medias no es un tema

### Causa raíz

La defensa existía a medias. `EntityResolver.scan_entity` ya rechazaba el alias
desnudo —por eso no se elegía una entidad arbitraria— pero el token sobrante
caía en la extracción de objeto y salía como `LIKE '%alcaldia%'`. Ese LIKE busca
la palabra dentro del objeto del contrato, que es otra pregunta, y de paso
**satisfacía el guard de alcance**: un comprador incompleto desbloqueaba la
ejecución haciéndose pasar por tema.

Al rutearlo a `entidad` apareció el segundo agujero, hasta ahora invisible:
`resolve_entidad` **no** tenía la defensa de `scan_entity`, y sus tiers de fuzzy
global resolvían con confianza alta:

```text
alcaldia    → (Secretaría Distrital de Integración Social)
gobernacion → DEPARTAMENTO DEL META
municipio   → ALCALDÍA DEL DISTRITO ... DE CARTAGENA DE INDIAS
ministerio  → INVIAS
```

### Fix

`app/core/generic_buyer.py` reúne el vocabulario repartido entre
`EntityResolver._GENERIC_ENTITY_TOKENS` y un literal suelto de `QueryRouter`, y
añade lo que faltaba: decidir si la mención está **calificada**. Un genérico
seguido de `de`, `del`, `local`, `regional`, `distrital`… abre un nombre propio
y se resuelve normalmente; uno desnudo pide precisión.
**INV-INCOMPLETE-BUYER-CANNOT-BECOME-TOPIC.**

### Evidencia

13 genéricos desnudos (`alcaldia`, `gobernacion`, `municipio`, `ministerio`,
`secretaria`, `instituto`, `agencia`, `fondo`, `corporacion`, `empresa`,
`entidad`, `distrito`, `departamento`) → **CLARIFY, 0 source calls, 0 fugas a
`objeto`**.

10 compradores completos siguen resolviendo igual: alcaldía de Medellín,
alcaldía local de Teusaquillo, gobernación del Atlántico, ministerio de
Educación Nacional, secretaría de Educación de Bogotá, ICBF, ANI, EAAB, distrito
de Barranquilla, municipio de Soacha. `"que contrató el departamento del
Atlántico"` (HCA-ENTITY-004, fuera de alcance) sin cambio.

**Fuera de alcance, registrados.** Dos comportamientos verificados **idénticos
en `eb8cfe1`**, preexistentes y ajenos a los seis casos:

- `"contratos del fondo nacional de vivienda"` no resuelve — falta en el
  gazetteer;
- `"contratos del proveedor LMP Ingeniería con la alcaldía"` deja `alcaldia` en
  `objeto`. El guard solo reconoce la mención tras `de/del/para/en/por`, y aquí
  la introduce `con` dentro de una construcción de relación actor-comprador.
  Ampliarlo tocaría la ruta de actor (R7), que este sprint no debe mover.

---

## 4. FOLLOW-UP — ordenar no cambia el universo

Decisión completa en **ADR-068**.

### Causa raíz

`entidad_ecosystem` es estado **derivado de ejecución**: `normal_flow` lo
inyecta después de consultar, reconstruye la SoQL para la respuesta asesora y
**no vuelve a consultar** — las filas que el usuario vio nunca lo aplicaron. Al
persistirse en el turno, el siguiente follow-up lo heredaba como filtro pedido.

Tres capas ya se defendían por su cuenta (buffer de paginación, repesca de
página 2, guardado de radares). Faltaba la cuarta: la persistencia
conversacional, que es por donde se propaga.

### Fix

`app/core/derived_execution_state.py` declara la categoría una sola vez. El
filtro se aplica en `ConversationStore.append_turn`, no en los cuatro handlers
que escriben turnos: el invariante es del historial — lo que entra ahí es
exactamente lo que el próximo follow-up hereda.

### Evidencia — source calls, no frames

```text
T1  contratos de obra en cali por mas de 300 millones
    WHERE ciudad LIKE '%Cali%' AND valor_del_contrato >= 300000000
          AND (tipo_de_contrato = 'Obra')
    ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC

T2  los mas antiguos primero
    WHERE ciudad LIKE '%Cali%' AND valor_del_contrato >= 300000000
          AND (tipo_de_contrato = 'Obra')          ← idéntico
    ORDER BY fecha_de_firma ASC, valor_del_contrato ASC, id_contrato ASC
```

`tests/test_ordering_followup_source_universe.py` fija el invariante sobre 10
casos (territorio municipal y departamental, comprador explícito, dinero,
tiempo, estado, negación, modalidad). **Verificado por mutación:** sin el fix, 4
de ellos fallan.

El ecosistema que el usuario declara sobrevive: `"contratos del ICBF en
Antioquia"` + `"ordénalos por mayor valor"` conserva `ICBF` en la fuente. No
viaja por esta clave, y `should_expand_ecosystem` ni se activa cuando hay
entidad.

### Re-adjudicación de HCA-FOLLOWUP-016

```text
WHERE preserved:                       YES  (idéntico entre turnos)
ordering correct:                      YES  (fecha_asc)
obra preservada como contract_type:    YES  (contract_type_include=['obra'],
                                             tipo_de_contrato = 'Obra')

PRIMARY_RUNTIME_BUG:      CLOSED
SECONDARY_ORACLE_DEBT:    CONFIRMED  — objeto_has:[obra]
```

El caso sigue rojo en el HCA bruto **solo** porque el oráculo exige `obra` en
`objeto`, mientras SEARCH-SH1 la representa estructuralmente como tipo
contractual. El fix de runtime **no** reinserta `obra` en `objeto` para poner el
caso en verde. Va al Oracle Patch, con diff separado.

---

## 5. Verificación contra la fuente

Socrata sana. Consultas de solo lectura sobre `jbjy-vk9h` y `p6dx-8zbt`:

```text
100.000 ≤ valor_del_contrato ≤ 999.999      5 filas    ← el rango que el piso
                                                          de 1M hacía inalcanzable
misma cota, contradictoria                  0 filas    ← el comparador discrimina
estado_del_procedimiento = 'Publicado'      3 filas
estado_del_procedimiento = 'Borrador'       3 filas
ambos a la vez                              0 filas    ← valores reales y disjuntos
estado_contrato = 'En ejecución'            3 filas
estado_contrato = 'Modificado'              3 filas
estado_contrato = 'Prorrogado'              3 filas    ← el truncamiento perdía filas
```

Los defectos no eran teóricos: el ensanchamiento traía filas que nadie pidió y
el truncamiento ocultaba filas legítimas, con datos reales.

---

## 6. No regresión

```text
HCA congelado          177/195 → 182/195   ·  rotos: 0  ·  sha del corpus intacto
corpus focal P1        40/40               ·  make audit-hca-final-p1
make audit-negation-v1        22/24        ·  idéntico a eb8cfe1 (verificado en
                                              worktree del commit base:
                                              NEG-N15 y NEG-ANTI-002 son basales)
make audit-time-v2            32/32
make audit-control-language-v1 42/42
make audit-actor-hygiene-v1    36/36
suite pytest completa   8657 passed · 2 skipped · 24 xfailed · 0 failed
query corpus            89/89
dialogue corpus         60/61   ·  único FAIL GOB-DEPTO-001, basal: verificado
                                   60/61 con el mismo caso en eb8cfe1
make lint-core          app/core/ limpio
ruff check              limpio en app/ tests/ scripts/ con ruff.toml del repo
semgrep                 NO EJECUTADO — no está instalado en este entorno. La
                        ley `settings-sin-redirigir-persistencia` se verificó a
                        mano sobre el diff: la única construcción nueva fuera de
                        tests usa `Settings(**overrides)`, que es la excepción
                        declarada por la propia regla
data/ real              sin escrituras del sprint — el mtime de feedback.jsonl
                        lo mueve el Streamlit de dev del usuario; la última
                        entrada es anterior a la sesión
git diff --check        limpio
query_plan.py           299 LoC, intacto — NEXT_TOUCH_RULE no se invocó
```

### Sentinels de alcance

```text
INSUFFICIENT_EVIDENCE — sin cambio deliberado
  HCA-ENTITY-004   "que contrató el departamento del Atlántico"   FAIL → FAIL
  HCA-STATE-003    "contratos liquidados"                         FAIL → FAIL
  HCA-SOURCE-002   "procesos liquidados"                          FAIL → FAIL

KNOWN_DEFERRED — siguen fail-safe
  HCA-MODALITY-002 · HCA-MODALITY-012 · HCA-TYPO-003              FAIL → FAIL

CONFIRMED_ORACLE_DEBT — intactos
  HCA-TOPIC-007 · HCA-ENTITY-001 · HCA-FOLLOWUP-007 · HCA-META-001B · HCA-ACTOR-004

SAFE_UNSUPPORTED_CAPABILITY
  HCA-TOPIC-012                                                   FAIL → FAIL
```

Ningún fixture de auditoría fue editado: `conversation_holistic_v1.yaml`,
`negation_focal_v1.yaml`, `time_v2_focal.yaml`, `control_language_v1.yaml` y
`actor_hygiene_v1.yaml` conservan su contenido.

### Tres aserciones de test actualizadas

Ninguna es un oráculo de auditoría; las tres fijaban el comportamiento
defectuoso que este sprint corrige:

1. `tests/test_plan_query_node.py` — exigía el escalar truncado
   `estado_contrato = 'En ejecución'`. Lo que el test comprueba (hay `WHERE` de
   estado pero no basta para ejecutar) no cambia.
2. `tests/test_pagination_buffer.py` — exigía `entidad_ecosystem` **en** el
   historial. Ahora exige lo contrario, y un espía sobre la expansión mantiene
   el test no-vacuo: sin él, la aserción que protege pasaría trivialmente.
3. `tests/test_modality_pipeline_integration.py` — golden de paridad cuyo
   marcador era el escalar truncado. Mismo criterio que la nota `AP2-B` ya
   presente en ese archivo: cambia el filtro, no la interpretación de modalidad.

---

## 7. Estado semántico del corpus

```text
182  runtime PASS
  5  CONFIRMED_ORACLE_DEBT     (el runtime es correcto, el oráculo no)
  1  SAFE_UNSUPPORTED          (HCA-TOPIC-012: pedir precisión es lo correcto)
  1  FOLLOWUP-016              (runtime cerrado, oráculo secundario obsoleto)
────
189 / 195 semánticamente sostenidos

Los 6 restantes:
  3  capacidades deliberadamente diferidas  (ONLY_MODALITY_SCOPE_POLICY)
  3  casos sin autoridad suficiente         (liquidación, comprador vs territorio)
```

Ninguna incertidumbre queda maquillada.

---

## 8. Siguiente

```text
Final P1 Runtime Fixes  ←  este documento
        ↓
Integration Review + ff
        ↓
Oracle Patch        (incluye la deuda secundaria de HCA-FOLLOWUP-016)
        ↓
Final HCA
```

# GAZETTEER-WIDE SOURCE BINDING AUDIT V1 — cobertura ADR-073 sobre todo el gazetteer

**Tipo:** auditoría read-only + artefacto reproducible
**Fecha:** 2026-09-18 · **Rama:** `audit/adr073-gazetteer-wide-v1` · **HEAD antes/después:** `0ca6469`
**Artefacto:** [`artifacts/audits/gazetteer_source_bindings_v1/`](../../artifacts/audits/gazetteer_source_bindings_v1/)
**Reproductor:** [`scripts/audits/audit_gazetteer_source_bindings_v1.py`](../../scripts/audits/audit_gazetteer_source_bindings_v1.py)
**Tests:** [`tests/test_gazetteer_source_binding_audit.py`](../../tests/test_gazetteer_source_binding_audit.py)

## 1. Veredicto

```text
ADR073_GAZETTEER_WIDE_V1: COMPLETE
  1134 / 1134 identidades cerradas · 2312 llamadas · 0 timeouts · 0 ERROR · 0 drift
  593 bindings vigentes re-observados (593 verificados) · 2 candidatos · 4 grupos READY_TODAY
RUNTIME MODIFICADO: NO · DATOS DE PRODUCTO MODIFICADOS: NO · PROMOCIÓN: NO
```

**La cifra viva es `summary.json`**, reconstruible desde `progress.jsonl` con
`--summary-only` sin volver a tocar la red. Este documento congela el cierre del
lote; si el gazetteer cambia, el lote se repite y el documento se re-sincroniza.

Nota de procedencia: `started_at`/`finished_at` del manifiesto son de una
reconstrucción de reportes posterior, no de la ventana de medición —la corrida
duró 6.213,5 s según su propio log, y el manifiesto lo declara así en
`batch_totals.timing_note`. La telemetría del lote está recompuesta desde
`progress.jsonl` y marcada como recompuesta: no se disfraza de original.

## 2. Qué se midió y qué no

Se recorrió **el gazetteer completo** (1134 identidades: 1111 BuyerUnits + 23
entradas de ecosistema) y, para cada una, se preguntó lo mismo:

```text
A. ¿Tiene identidad estable acreditable?          → identity / identity_provenance
B. ¿Cómo aparece físicamente en p6dx-8zbt?        → datasets[p6dx-8zbt]
C. ¿Cómo aparece físicamente en jbjy-vk9h?        → datasets[jbjy-vk9h]
D. ¿Qué alcanza HOY el fallback LIKE productivo?  → like_universe / forms_reached
E. ¿Qué alcanzaría una igualdad exacta?           → equivalence.exact_where
F. ¿Son ambos universos equivalentes?             → equivalence.same_universe
G. ¿El LIKE alcanza identidades/NIT ajenos?       → nits_reached / foreign
H. ¿Hay formas reales que el LIKE NO alcanza?     → forms_not_reached_by_like
I. ¿Pertenece a un Buyer Group declarado?         → declared_groups
J. ¿Es un ecosistema institucional complejo?      → ecosystem
K. ¿Puede proponerse un binding sin cambiar semántica? → status + candidate
```

No se midió —ni se autoriza con este lote— nada de: agregación por NIT,
composición de unidades distintas, corrección de nombres, aliases nuevos,
fuzzy, geografía, Radar, Actor ni transporte de runtime.

## 3. Autoridades e inventario

Sólo las cinco autoridades declaradas alimentan identidad y membresía:

| Autoridad | Papel en el inventario |
|---|---|
| `app/data/aliases_db.json` | fuente principal de BuyerUnits canónicas (1096 fichas) |
| `app/data/buyer_source_names.json` | identity ya acreditada + canónicos que sólo existen ahí |
| `app/data/buyer_groups.json` | membresía DECLARADA (no se infiere de NIT ni de nombre) |
| `app/data/icbf_regionals.json` | unidad declarada por catálogo institucional |
| `app/data/sena_units.json` | unidades nacionales declaradas + ecosistemas (padre, regionales, macro) |

Ninguna identidad se inventó y ningún NIT observado en la fuente se usó como
acreditación: un NIT publicado es **evidencia**, no llave. Por eso el inventario
tiene 596 identidades con llave acreditada (593 con binding vigente + 3 unidades
SENA del catálogo) y el resto queda con la identidad declarada como ausente.

## 4. Método

1. **Preclasificación** por identidad: binding vigente, membresía declarada,
   ecosistema, identidad acreditada.
2. **Fase A (todo el gazetteer):** universo LIKE actual por dataset — grupos
   (forma física, NIT) —, formas de la identidad acreditada cuando el NIT está
   acreditado, y re-observación del universo de cada binding vigente.
3. **Fase B (sólo lo plausiblemente bindable):** conteo del universo LIKE,
   conteo del candidato exacto y comparación `same_universe`.
4. Los predicados **no se reconstruyen**: salen de
   `SoQLBuilder._accent_fold_like(...)` y `SoQLBuilder._exact_entity_clause(...)`,
   que son las primitivas del runtime. El auditor mide la consulta que el
   producto manda, no una versión equivalente escrita a mano.

**Reglas duras aplicadas:**

```text
un timeout   NO es equivalencia      → same_universe = null y clase INCONCLUSIVE
sin llave    NO se inventa identidad → NO_STABLE_IDENTITY / AMBIGUOUS_IDENTITY
mismo NIT    NO iguala misma unidad  → MULTI_NAME_REVIEW / BUYER_GROUP_CANDIDATE
fallback     NO alimenta el candidato: el candidato son las formas que el LIKE
             YA alcanza, y sólo si no alcanza identidades ajenas
```

**Checkpointing:** cada identidad cierra con una línea atómica en
`progress.jsonl` y su evidencia completa en `records/<hash>.json`. Un fallo
posterior no borra trabajo previo y `--resume` salta lo cerrado.

### Transporte: la decisión y su prueba

Las sondas de latencia se contradijeron entre ventanas (el mismo `GROUP BY` con
`unaccent(UPPER()) LIKE` medido entre 4 y 126 s según el momento). La medición
que decidió es la consulta que este auditor más emite —la igualdad exacta sobre
`jbjy-vk9h`—: 63,9 s en `classic` contra 2,6 s en `soda3`, y un LIKE que
expiraba a los 120 s en `classic` contra 50,7 s en `soda3` (18-09, ventana de la
corrida). Se usa `soda3`, que es además el transporte del runtime (ADR-070).

Lo que hace válido el resultado no es la velocidad sino esto: **ambos transportes
devuelven el mismo universo** para estos predicados. Fingerprints idénticos en
4/4 casos, [`transport_parity.json`](../../artifacts/audits/smoke_adr073_v1/transport_parity.json).
Cada registro guarda además su propio `transport`, para que un lote reanudado
con otro transporte no quede invisible.

## 5. Validación antes del lote completo

El smoke exigido por el encargo (§26) corrió contra la fuente viva, en
`artifacts/audits/smoke_adr073_v1/` (22 identidades, no versionado como
artefacto del lote), con sus gates en `smoke_gates.json`:

| Gate | Resultado |
|---|---|
| Los 4 bindings del dogfood se reproducen (Soledad, Galapa, Barranquilla, Cartagena) | PASS — nombres y clasificación `ALREADY_BOUND_VERIFIED`, conteos vivos coherentes con el sidecar |
| DAPRE reproduce el source-name split | PASS — el canónico largo no es el nombre físico (`DAPRE`), declarado en `evidence_flags` |
| Yopal reproduce el split ENTRE datasets | PASS — `MUNICIPIO DE YOPAL...` / `MUNICIPIO DE YOPAL..` |
| SENA no se aplana | PASS — padre y ecosistema regional `INSTITUTIONAL_ECOSYSTEM`, sin binding, decenas de unidades físicas bajo un solo NIT |
| ICBF se mide como composición, no como unidad | PASS — 33 nombres oficiales, 3 formas de NIT |
| Buyer Groups no se mezclan con bindings | PASS — ningún grupo es identidad; los miembros se auditan individualmente |
| Cero ERROR / cero INCONCLUSIVE en el smoke | PASS |

## 6. Resultados del lote completo (1134 / 1134)

| Clase | N | Qué significa aquí |
|---|---:|---|
| `ALREADY_BOUND_VERIFIED` | 593 | binding vigente re-observado sin drift |
| `NO_STABLE_IDENTITY` | 497 | una sola forma física y un solo NIT **observado**, sin acreditación |
| `INSTITUTIONAL_ECOSYSTEM` | 23 | SENA nacional/regionales/macro e ICBF nacional: composiciones, no BuyerUnits |
| `AMBIGUOUS_IDENTITY` | 14 | el LIKE del canónico alcanza ≥2 identidades fiscales distintas |
| `MULTI_NAME_REVIEW` | 3 | ≥2 formas token-distintas bajo un NIT observado, sin acreditación |
| `EXACT_BINDABLE` | 2 | unidad declarada por catálogo con proyección exacta y mismo universo |
| `BUYER_GROUP_CANDIDATE` | 1 | el universo alcanzado tiene forma de raíz + unidad hija |
| `INCONCLUSIVE` | 1 | nombre declarado que la fuente no publica (0 filas, no hubo fallo) |
| `BINDING_DRIFT_REVIEW` · `FOREIGN_IDENTITY_COLLISION` · `NOT_EQUIVALENT` · `SOURCE_NAME_SPLIT` · `BUYER_GROUP_MEMBER` · `ERROR` | 0 | nada que reportar |

```text
bindings vigentes re-observados: 593 / 593 verificados, 0 con drift, 0 modificados
candidatos producidos:            2   (ver §8)
llamadas a la fuente:             2312 · timeouts 0 · reintentos 4 · ~5,4 s por llamada
duración del lote:                6213,5 s (~1h 44m) con workers=2 sobre SODA3
```

Los 593 bindings del sidecar se re-observaron contra la fuente (formas declaradas
presentes, filas de hoy, ausencia de NIT ajeno). La verificación **no** tocó el
sidecar: lo auditó. Ningún binding vigente se modificó ni se propuso modificar, y
el hash del archivo es el mismo al cerrar el lote.

## 7. Casos peligrosos (no se esconden para mejorar el porcentaje)

**Colisiones de LIKE (`AMBIGUOUS_IDENTITY`, 14)** — el nombre canónico, tal como
lo emite hoy el fallback, alcanza identidades fiscales distintas:

| Canónico | NITs alcanzados | Filas | Lectura |
|---|---:|---:|---|
| `ANI` | **197** | 214.307 | el acrónimo es substring de agencias, municipios y personas; el universo llegó al tope de 200 grupos, así que 197 es un **piso**, no el total |
| `ESE CENTRO` | 56 | 54.323 | «centro» de salud: decenas de ESE distintas comparten el nombre |
| `EMPRESA SOCIAL DEL ESTADO HOSPITAL LOCAL` | 10 | 20.884 | idem |
| `E.S.E. HOSPITAL SAN ANTONIO` | 5 | 11.711 | idem |
| `ADRES` | 4 | 9.655 | — |
| `DEPARTAMENTO DE CORDOBA` / `DEPARTAMENTO DEL META` | 3 / 3 | 21.767 / 58.801 | el nombre del departamento no identifica una sola unidad |
| `DANE - DIRECCION TERRITORIAL CENTRO` | 2 | 54.089 | la dirección territorial no es una identidad fiscal |
| `ANM` · `ENTerritorio S.A` · `HOSPITAL SAN ANDRES` · `INSTITUTO COLOMBIANO AGROPECUARIO - ICA` · `UNIVERSIDAD DEL VALLE` · `UNIVERSIDAD FRANCISCO DE PAULA SANTANDER` | 2 cada uno | 8.033 – 58.811 | misma raíz, varias unidades |

Ninguna se resolvió: se declaran. Son el argumento más fuerte del lote para no
promover bindings por parecido de nombre, y también son defectos que el producto
ya está ejecutando hoy.

**Splits observados en bindings VIGENTES** (`evidence_flags.source_name_split_observed`, 11):
`FONDO FINANCIERO DISTRITAL DE SALUD` (45.137) · `MUNICIPIO DE SOACHA` (44.331) ·
`MUNICIPIO DE YOPAL` (37.137) · `DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA
REPÚBLICA` (21.516, el nombre físico es `DAPRE`) · `MINISTERIO DE JUSTICIA Y DEL
DERECHO` (14.522) · `UAESP` (12.129) · `UNIATLANTICO` (9.227) · `UNIVERSIDAD DE LA
AMAZONIA` (2.560) · `UNIVERSIDAD TECNOLÓGICA DEL CHOCÓ` (2.258) · `UFPS SECCIONAL
OCAÑA` (1.530) · `.SECRETARÍA DISTRITAL DE SALUD.` (1.134). Es el fenómeno que
ADR-073 vino a resolver, medido sobre los bindings que ya lo resuelven.

**Composición y multi-nombre (4):** `SENA REGIONAL ATLANTICO` (27.436,
`BUYER_GROUP_CANDIDATE`: el universo alcanzado tiene forma de raíz + unidades
hija) y `FONDANE - DIRECCION TERRITORIAL CENTRO` (14.521), `GOBERNACION DE
CALDAS` (24.737) y `SENA REGIONAL CUNDINAMARCA Grupo Administrativo CDA`
(11.453) como `MULTI_NAME_REVIEW`. En los cuatro casos hay más de una forma
token-distinta bajo un mismo NIT observado, sin autoridad declarada que diga que
son la misma unidad — o que son varias.

**Cobertura desbalanceada (62):** un dataset alcanza <1% del otro, lo que
significa que el nombre canónico no viaja igual entre procesos y contratos.
Mayores: `UNIVERSIDAD NACIONAL DE COLOMBIA` (112.154 / **14**) · `LA CORPORACION
COLOMBIANA DE INVESTIGACION AGROPECUARIA` (52.822 / 1) · `UNIVERSIDAD DE
ANTIOQUIA` (44.658 / 34) · `ECOPETROL` (38.093 / 74) · `ESE HOSPITAL MANUEL
URIBE ANGEL` (35.384 / 5). Es un umbral de reporte, no una clasificación.

## 8. Candidatos (`bindings_ready.jsonl`)

```text
candidatos al cierre: 2
  SENA SECRETARIA GENERAL                                                    11.022 / 10.762
  SENA DIRECCIÓN GENERAL Dirección de Promoción y Relaciones Corporativas     5.559 /    249
```

Ambos son unidades nacionales **declaradas por el catálogo** `sena_units.json`:
una BuyerUnit concreta, no la composición del padre. Proyección exacta, mismo
universo verificado en los dos datasets (`same_universe: true`), sin identidad
ajena y sin formas fuera del alcance. Los dos son `EXACT_BINDABLE`.

No hay más candidatos, y la razón no es el auditor: sólo 18 identidades del
inventario tienen un NIT acreditado por una autoridad declarada, y las 18 ya
tienen binding. Sin llave acreditada no hay `identity_key`, y sin `identity_key`
no hay candidato que pueda escribirse sin inventar la identidad — que es
exactamente lo que el encargo prohíbe.

**Worklist de acreditación: 490 identidades** de las 497 que quedaron sin llave.
Son casos cuyo universo LIKE alcanza **una sola forma física y un solo NIT** en
ambos datasets, con universo completo y sin identidad ajena — es decir, donde la
acreditación de esa llave bastaría para escribir un binding.

```text
mayores por filas alcanzadas (p6dx-8zbt + jbjy-vk9h):
  INSTITUCIÓN UNIVERSITARIA ITM .................................... 144.444
  SUBRED INTEGRADA DE SERVICIOS DE SALUD NORTE E.S.E. (OFICIAL) ... 137.461
  UNIVERSIDAD NACIONAL DE COLOMBIA ................................. 112.168
  SUBRED INTEGRADA DE SERVICIOS DE SALUD SUR E.S.E.** ............. 104.531
  AGENCIA NACIONAL DE TIERRAS - ANT ................................ 102.154
  MUNICIPIO DE PEREIRA- OFICIAL ..................................... 87.705
  SUBRED INTEGRADA DE SERVICIOS DE SALUD SUR OCCIDENTE ESE. ......... 87.485
```

Acreditar esos NIT es una decisión de producto con revisión humana; con la
acreditación en mano, cada uno se convierte en `EXACT_BINDABLE` con su universo
ya medido en `records/<hash>.json`. El auditor no da ese paso solo.

Forma de un candidato:

```json
{
  "canonical_name": "...",
  "identity_key": {"type": "NIT", "value": "..."},
  "sources": {"p6dx-8zbt": ["..."], "jbjy-vk9h": ["..."]},
  "observed_rows": {"p6dx-8zbt": 0, "jbjy-vk9h": 0},
  "classification": "EXACT_BINDABLE|SOURCE_NAME_SPLIT",
  "semantics_preserved": true,
  "foreign_identities_reached": [],
  "promotion_status": "CANDIDATE_NOT_PROMOTED",
  "evidence_record": "records/<hash>.json"
}
```

Ese `promotion_status` es deliberado: el archivo NO tiene la forma de
`buyer_source_names.json` y dice en cada línea que no es un binding productivo.
Promover un candidato es otro lote, con revisión humana independiente.

## 9. Buyer Groups y ecosistemas

**Los cuatro grupos declarados quedan `READY_TODAY`**: todos sus miembros tienen
binding verificado y la proyección cabe en el tope de 40 nombres.

| Grupo | Miembros | Bindeados | Nombres proyectados (proc./contr.) | Estado |
|---|---:|---:|---:|---|
| `bogota_central` | 18 | 18 | 18 / 18 | `READY_TODAY` |
| `bogota_local_mayoralties` | 20 | 20 | 20 / 20 | `READY_TODAY` |
| `icbf_national` | 33 | 33 | 33 / 33 | `READY_TODAY` |
| `bogota_executive` | 38 | 38 | 38 / 38 | `READY_TODAY` |

Ningún grupo se colapsó dentro de un binding de su padre, y ningún candidato
—que no es un binding productivo— se contó como miembro listo.

**Ecosistemas institucionales (23 entradas):** SENA (padre, 15 regionales y 6
macro-regiones) e ICBF nacional, clasificados `INSTITUTIONAL_ECOSYSTEM` y medidos
por su proyección declarada, sin aplanarse:

- `ECOSYSTEM:ICBF:national`: 33 nombres oficiales, **121.936 filas** en procesos y
  120.869 en contratos, con tres formas de NIT publicadas (`899999239`,
  `8999992391`, `8999992392`) — la misma advertencia del catálogo, medida.
- `ECOSYSTEM:SENA:regional:Antioquia`: decenas de nombres físicos (centros, grupos
  administrativos, intercentros) bajo un solo NIT `899999034`.
- `sena_ecosystem_coverage.json`: 22 entradas de catálogo, 3 unidades nacionales
  declaradas (2 con candidato, 1 sin evidencia física en la fuente). **No se creó
  ningún Buyer Group nuevo** ni se modificó la membresía declarada.

## 10. Reproducción

```bash
# lote completo (resumible: repetirlo continúa donde quedó)
.venv/bin/python scripts/audits/audit_gazetteer_source_bindings_v1.py --resume --workers 2

# reconstruir todos los reportes desde progress.jsonl, sin tocar la red
.venv/bin/python scripts/audits/audit_gazetteer_source_bindings_v1.py --summary-only

# una identidad concreta, en un directorio aparte
.venv/bin/python scripts/audits/audit_gazetteer_source_bindings_v1.py \
    --only "MUNICIPIO DE SOACHA" --output artifacts/audits/mi_prueba
```

`manifest.json` congela HEAD, rama, sha256 del script, sha256 de las cinco
autoridades, sha256 de los módulos de runtime leídos, datasets, transporte,
workers, presupuesto, política de reintentos y total de identidades. Sin esos
hashes una corrida no es reproducible, sólo recordada.

## 11. Límites y deuda abierta

- **490 identidades esperan acreditación de identidad.** Sólo 18 del inventario
  tienen NIT acreditado, y las 18 ya tienen binding: `EXACT_BINDABLE` **no puede**
  producir más candidatos mientras la acreditación siga siendo la actual. Eso es
  cobertura de identidad, no un fallo del auditor, y el worklist de §8 es el
  camino.
- **Las 14 colisiones son defecto del fallback, no del binding.** `ANI` (197 NIT)
  o `ESE CENTRO` (56) alcanzan identidades distintas HOY, en producción. Ningún
  binding ADR-073 las arregla: eso es admisión/routing, otra decisión y otro lote.
- **62 identidades con cobertura desbalanceada** entre procesos y contratos
  (UNAL: 112.154 / 14): el nombre canónico no viaja igual en los dos datasets.
  Cada una puede necesitar su propio binding por dataset, y eso no se resuelve
  por parecido.
- **1 sin evidencia física:** `SENA DIRECCIÓN GENERAL Dirección Jurídica` está
  declarada en el catálogo y no se publica con ese nombre (0 filas en ambos
  datasets). El catálogo y la fuente discrepan; el auditor lo declara.
- **1 universo truncado** (`ANI`: el `GROUP BY` llegó al tope de 200 grupos), así
  que su cuenta de 197 identidades es un piso.
- Un timeout se registra como `INCONCLUSIVE` y nunca se resuelve por
  conveniencia. En este lote no hubo ninguno: por eso la comparación de universos
  está completa en todas las identidades medibles.
- Nota de procedencia del artefacto: la telemetría está recompuesta desde
  `progress.jsonl` y la ventana temporal del manifiesto es la de una
  reconstrucción de reportes posterior (§1). El defecto quedó corregido en el
  script: una reconstrucción ya no pisa la procedencia del lote.

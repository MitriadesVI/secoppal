# GAZETTEER-WIDE SOURCE BINDING AUDIT V1 — cobertura ADR-073 sobre todo el gazetteer

**Tipo:** auditoría read-only + artefacto reproducible
**Fecha:** 2026-09-18 · **Rama:** `audit/adr073-gazetteer-wide-v1` (`c7beacd`) + remediación en `fix/adr073-gazetteer-wide-v1-remediation`
**Lote:** medido sobre `06bd882` · **Cierre del lote:** `0ca6469` → `c7beacd` · **Revisión independiente:** `fabda711` (§12)
**Artefacto:** [`artifacts/audits/gazetteer_source_bindings_v1/`](../../artifacts/audits/gazetteer_source_bindings_v1/)
**Reproductor:** [`scripts/audits/audit_gazetteer_source_bindings_v1.py`](../../scripts/audits/audit_gazetteer_source_bindings_v1.py)
**Tests:** [`tests/test_gazetteer_source_binding_audit.py`](../../tests/test_gazetteer_source_binding_audit.py) ·
[`tests/test_zeus_adr073_adversarial.py`](../../tests/test_zeus_adr073_adversarial.py) ·
[`tests/test_zeus_adr073_recheck.py`](../../tests/test_zeus_adr073_recheck.py) ·
[`tests/test_gazetteer_source_binding_audit_regressions.py`](../../tests/test_gazetteer_source_binding_audit_regressions.py)

## 1. Veredicto

```text
ADR073_GAZETTEER_WIDE_V1: COMPLETE (con remediación de la revisión independiente, §12)
  1134 / 1134 identidades cerradas · 2312 llamadas en 1137 intentos
  (2298 en los 1134 registros vigentes + 14 en los 3 reprocesos declarados)
  0 timeouts · 0 ERROR · 0 drift
  593 bindings vigentes re-observados: 593 con liveness de nombre verificada,
  y el chequeo de identidad fiscal ajena —que exige NIT acreditado— aplicó a 18
  2 candidatos · 4 grupos READY_TODAY
RUNTIME MODIFICADO: NO · DATOS DE PRODUCTO MODIFICADOS: NO · PROMOCIÓN: NO
```

**La cifra viva es `summary.json`**, reconstruible desde `progress.jsonl` con
`--summary-only` sin volver a tocar la red. Este documento congela el cierre del
lote; si el gazetteer cambia, el lote se repite y el documento se re-sincroniza.

Procedencia, sin ambigüedad: el manifiesto separa la historia del LOTE
(`batch`, inmutable) de la última invocación (`last_invocation`) y del rebuild
(`report_rebuilt_at`). `batch.head` es el HEAD sobre el que se MIDIÓ
(`06bd882`); `head` en el nivel raíz es el HEAD de la última invocación. La
ventana de medición y los conteos de `batch` se **derivan** de
`progress.jsonl` (`06:20:28 → 08:05:54 UTC`, 6325,3 s); ninguna cifra del
manifiesto se recompone a mano ni se copia de un log externo.

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

El smoke exigido por el encargo (§26) corrió contra la fuente viva y quedó en
`artifacts/audits/smoke_adr073_v1/` (22 identidades), con su paridad de
transportes en `transport_parity.json`. Los gates se leen de
`smoke_adr073_v1/progress.jsonl` —que es la evidencia cruda de esa corrida— y de
`transport_parity.json`; no existe ningún `smoke_gates.json`:

| Gate | Resultado | Dónde se ve |
|---|---|---|
| Los 4 bindings del dogfood se reproducen (Soledad, Galapa, Barranquilla, Cartagena) | PASS — los cuatro cierran `ALREADY_BOUND_VERIFIED` | `smoke_adr073_v1/progress.jsonl` (12 `ALREADY_BOUND_VERIFIED`) |
| SENA no se aplana | PASS — padre y regional `INSTITUTIONAL_ECOSYSTEM`, sin binding | `progress.jsonl` (3 ecosistemas: SENA nacional, SENA Antioquia, ICBF nacional) |
| ICBF se mide como composición, no como unidad | PASS — 33 formas físicas, 121.936 / 120.869 filas | `progress.jsonl` (`ECOSYSTEM:ICBF:national`) |
| Buyer Groups no se mezclan con bindings | PASS — ningún grupo es identidad; los miembros se auditan individualmente | `summary.json` / `buyer_group_readiness.json` |
| Cero ERROR / cero INCONCLUSIVE en el smoke | PASS — 12 + 3 + 7, sin `ERROR` ni `INCONCLUSIVE` | `progress.jsonl` |
| Ambos transportes devuelven el mismo universo | PASS — `same_universe_all: true`, fingerprints idénticos en 4/4 casos | `transport_parity.json` |

Dos salvedades de esa carpeta, declaradas en vez de tapadas: su `summary.json`
es un corte **parcial** de 8 identidades de una ventana anterior (su
`progress.jsonl` tiene 22), y ese directorio está fuera del alcance de esta
misión, así que no se reescribió. Los gates de arriba se tomaron de la evidencia
cruda, no del resumen parcial. El split de DAPRE y el split entre datasets de
Yopal se verifican en el lote completo (`evidence_flags.source_name_split_observed`,
§7), que es donde esos dos bindings quedaron medidos.

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
bindings vigentes re-observados: 593 / 593 con liveness de nombre verificada, 0 con drift,
                                 0 modificados · identidad fiscal ajena chequeada en 18 (NIT
                                 acreditado) · 575 verificados sólo estructuralmente (§8)
candidatos producidos:            2   (ver §8)
llamadas a la fuente:             2312 en 1137 intentos (2298 en los 1134 vigentes + 14
                                  en los 3 reprocesos declarados) · timeouts 0 · reintentos 4
ventana de medición del lote:     6325,3 s (06:20:28 → 08:05:54 UTC) con workers=2 sobre SODA3
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

**Splits observados en bindings VIGENTES** (`evidence_flags.source_name_split_observed`, 11).
Los nombres son los canónicos del artefacto, no los alias con que se los conoce en
el habla: `FONDO FINANCIERO DISTRITAL DE SALUD` (45.137) · `MUNICIPIO DE SOACHA` (44.331) ·
`MUNICIPIO DE YOPAL` (37.137) · `DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA
REPÚBLICA` (21.516, el nombre físico es `DAPRE`) · `MINISTERIO DE JUSTICIA Y DEL
DERECHO` (14.522) · `UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS` (12.129,
el nombre físico es `UAESP`) · `UNIATLANTICO` (9.227) · `UNIVERSIDAD DE LA
AMAZONIA` (2.560) · `UNIVERSIDAD TECNOLÓGICA DEL CHOCÓ DIEGO LUIS CÓRDOBA` (2.258) · `UNIVERSIDAD FRANCISCO DE PAULA
SANTANDER SECCIONAL OCAÑA` (1.530) · `.SECRETARÍA DISTRITAL DE SALUD.` (1.134). Es el fenómeno que
ADR-073 vino a resolver, medido sobre los bindings que ya lo resuelven.

**Composición y multi-nombre (4):** `SENA REGIONAL ATLANTICO` (27.436,
`BUYER_GROUP_CANDIDATE`: el universo alcanzado tiene forma de raíz + unidades
hija) y `FONDANE - DIRECCION TERRITORIAL CENTRO` (14.521), `GOBERNACION DE
CALDAS` (24.737) y `SENA REGIONAL CUNDINAMARCA Grupo Administrativo CDA`
(11.453) como `MULTI_NAME_REVIEW`. En los cuatro casos hay más de una forma
token-distinta bajo un mismo NIT observado, sin autoridad declarada que diga que
son la misma unidad — o que son varias.

**Cobertura desbalanceada (81):** un dataset alcanza <1% del otro, lo que
significa que el nombre canónico no viaja igual entre procesos y contratos. Son
81 y no 62 porque un dataset en CERO cuenta como desbalance, no como dato
ausente: con `if value` el cero desaparecía del vector y 19 casos con 0 filas en
contratos quedaban invisibles (`EMPRESAS PUBLICAS DE MEDELLIN E.S.P.`:
26.067 / **0**; `UNIVERSIDAD DEL TOLIMA`: 4.130 / **0**). Mayores: `UNIVERSIDAD NACIONAL DE COLOMBIA` (112.154 / **14**) · `LA CORPORACION
COLOMBIANA DE INVESTIGACION AGROPECUARIA - AGROSAVIA` (52.822 / 1) · `UNIVERSIDAD DE
ANTIOQUIA` (44.658 / 34) · `EMPRESA COLOMBIANA DE PETROLEOS` (38.093 / 74) · `ESE HOSPITAL MANUEL
URIBE ANGEL` (35.384 / 5). Es un umbral de reporte, no una clasificación: no cambia
la clase de ninguna identidad ni habilita candidatos.

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

### 8.1 Qué se comprobó en cada binding vigente (sin inflarlo)

`593 / 593 verificados` aplanaba dos comprobaciones distintas en una cifra. El
resumen las separa:

```text
existing_bindings_total:            593
source_name_liveness_verified:      593     (formas declaradas presentes y con filas
                                             hoy, en los dos datasets)
foreign_identity_check:
  NIT_authoritative_checked:         18     (identidad NIT acreditada: se comprobó que
                                             el universo declarado no alcanza un NIT ajeno)
  BUYER_UNIT_structural_only:       575     (identidad BUYER_UNIT: no tiene NIT propio,
                                             así que NO acredita nada sobre identidad ajena)
```

Los 575 no están menos verificados: están verificados **estructuralmente**, que es
todo lo que su identidad declarada permite afirmar. Llamarlos «identity-safe
verified» sería afirmar algo que la evidencia no sostiene.

### 8.2 Intentos y supersession (1137 asientos, 1134 identidades)

`progress.jsonl` es append-only y el lote tiene 1137 asientos para 1134
identidades: tres unidades SENA se reprocesaron. Cada asiento se identifica
(`record_id` = `<canonical_hash>:<attempt>`) y un reproceso declara a cuál
reemplaza; los tres históricos lo declaran en `progress_supersessions.jsonl`,
porque el auditor de la época no escribía esos campos. El reporte **exige** esa
declaración y valida que el sucesor sea posterior y corresponda al intento
anterior:

```text
progress_lines:        1137
canonical_unique:      1134
superseded_records:       3
active_records:        1134
undeclared_duplicates:    0
```

Los dos reemplazos que importan cambiaron el RESULTADO: `SENA SECRETARIA GENERAL`
y `SENA DIRECCIÓN GENERAL Dirección de Promoción y Relaciones Corporativas`
quedaron con `candidate = null` en el primer intento y produjeron su candidato en
el reproceso. Antes, `done[canonical_hash] = record` dejaba ganar al último sin
dejar rastro; ahora el reemplazo es explícito, con el `sha256` de los dos asientos.
Un duplicado sin declarar **falla** el reporte y el canónico queda sin resultado
final: el auditor no elige por orden de línea.

### 8.3 Integridad de la evidencia raw

`records.sha256` se genera con el pipeline (una entrada por registro activo,
`path` + `sha256`) y se **verifica antes** de construir el resumen: un
`records/<hash>.json` alterado, ilegible o ausente deja el reporte en
`evidence_integrity.ok = false`, con el detalle, y la corrida termina con código
de salida ≠ 0. El archivo de hashes **no** se regenera sobre evidencia alterada:
regenerarlo lavaría la alteración. La evidencia que sostiene los dos candidatos
existe y está verificada en el commit (`records/ce230b16c71c8ffb.json`,
`records/c16129a97681d1e0.json`), junto con los 1134 registros.

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

El estado de un grupo no depende sólo de que sus miembros estén bindeados. Es
`BLOCKED` —aunque todos sus miembros estén listos— si la proyección **no cabe**
(`> 40` nombres, tras adjudicar candidatos incluidos) o si **dos miembros
distintos reclaman el mismo nombre físico**, que es la condición que
`buyer_groups.BuyerGroup.names_for` rechaza en producción. El reporte publica
`duplicated_source_names` por dataset para que eso se vea en vez de esconderse
detrás del conteo: sumar con `extend()` deja los dos nombres en la lista y el
total sube como si todo estuviera bien. En este lote no hay colisión ninguna
(0 en los cuatro grupos) y por eso los cuatro son `READY_TODAY`.

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

# adoptar la evidencia de un directorio heredado que no tiene índice (ver §11)
.venv/bin/python scripts/audits/audit_gazetteer_source_bindings_v1.py \
    --summary-only --adopt-evidence-index --output <directorio_heredado>
```

El índice de evidencia (`records.sha256`) no se crea nunca por su cuenta. Si falta
y hay evidencia esperada, el reporte lo declara como problema y termina con código
≠ 0: sin línea base no hay nada contra qué verificar, y regenerarlo desde el disco
certificaría a ciegas cualquier alteración previa. `--adopt-evidence-index` es la
única forma de fijar esa línea base sobre evidencia que no se pudo verificar, deja
`last_invocation.evidence_index_adopted = true` en el manifiesto y es una decisión
deliberada, no un efecto colateral.

`manifest.json` describe el lote y la invocación en bloques que no se pisan:

```text
head / branch          HEAD de ESTA invocación (aquí, la rama de remediación)
batch                  procedencia del LOTE original, inmutable:
                       head 06bd882 (el HEAD sobre el que se MIDIÓ), rama, sha256 del
                       script y de las cinco autoridades y de los módulos de runtime
                       leídos, transport, workers, timeout, retry_budget, y —derivados
                       de progress.jsonl— source_calls 2312, retries 4, records 1137 y
                       la ventana de medición 06:20:28 → 08:05:54 UTC (6325,3 s)
last_invocation        qué hizo esta corrida: mode (measure | recheck | rebuild),
                       started_at, finished_at, elapsed_seconds, selected, pending,
                       processed, batch_records, options, sha256 del script y de los
                       runtime files LEÍDOS AHORA, y su propia source_telemetry
report_rebuilt_at      cuándo se reconstruyeron los reportes
progress               progress_lines / canonical_unique / superseded_records /
                       active_records / undeclared_duplicates
report_status          ok + problemas concretos (integridad y supersession)
```

Sin esos hashes una corrida no es reproducible, sólo recordada. Y con un solo
bloque no había forma de distinguir «el lote midió en `06bd882`» de «esta
reconstrucción corrió en otro HEAD»: por eso el HEAD del lote y el HEAD de la
invocación son campos distintos, y `batch` no lo reescribe ni `--summary-only` ni
`--only` ni un rebuild.

La corrida sale con código 0 sólo si el reporte se puede leer como resultado
cerrado; con evidencia alterada o una repetición sin declarar sale ≠ 0.

## 11. Límites y deuda abierta

- **490 identidades esperan acreditación de identidad.** Sólo 18 del inventario
  tienen NIT acreditado, y las 18 ya tienen binding: `EXACT_BINDABLE` **no puede**
  producir más candidatos mientras la acreditación siga siendo la actual. Eso es
  cobertura de identidad, no un fallo del auditor, y el worklist de §8 es el
  camino.
- **Las 14 colisiones son defecto del fallback, no del binding.** `ANI` (197 NIT)
  o `ESE CENTRO` (56) alcanzan identidades distintas HOY, en producción. Ningún
  binding ADR-073 las arregla: eso es admisión/routing, otra decisión y otro lote.
- **81 identidades con cobertura desbalanceada** entre procesos y contratos
  (UNAL: 112.154 / 14), 19 de ellas con 0 filas en contratos: el nombre canónico
  no viaja igual en los dos datasets. Cada una puede necesitar su propio binding
  por dataset, y eso no se resuelve por parecido.
- **1 sin evidencia física:** `SENA DIRECCIÓN GENERAL Dirección Jurídica` está
  declarada en el catálogo y no se publica con ese nombre (0 filas en ambos
  datasets). El catálogo y la fuente discrepan; el auditor lo declara.
- **1 universo truncado** (`ANI`: el `GROUP BY` llegó al tope de 200 grupos), así
  que su cuenta de 197 identidades es un piso.
- **Los 575 bindings de identidad `BUYER_UNIT` no acreditan identidad fiscal.**
  Su verificación es de presencia y filas de las formas declaradas. Afirmar
  «identity-safe» sobre ellos exigiría una identidad fiscal acreditada que no
  existe (§8.1).
- **La evidencia de un intento reemplazado vive sólo en `progress.jsonl`.**
  `records/<hash>.json` guarda el intento VIGENTE —es lo que
  `bindings_ready.jsonl` referencia—, así que el reemplazo es auditable por su
  asiento y su `sha256` (declarados en `progress_supersessions.jsonl`) pero la
  medición completa del intento anterior no se conserva en `records/`. Cambiar el
  esquema de nombres tocaría las rutas que los candidatos ya declaran: es un paso
  de otro lote.
- **`smoke_adr073_v1/summary.json` es un corte parcial de 8 de sus 22
  identidades** (§5). Ese directorio quedó fuera del alcance de esta misión y no
  se reescribió; los gates se leen de su `progress.jsonl`.
- **Reconstruir un directorio con evidencia y SIN índice ahora falla.** Antes el
  índice se regeneraba desde el disco y el reporte salía `ok`; eso convertía un
  `rm records.sha256` en una forma de certificar evidencia alterada. El
  comportamiento nuevo es el correcto —y el único seguro—, pero es un cambio
  hacia afuera: un directorio heredado sin índice (por ejemplo
  `smoke_adr073_v1/`, o los `..._attempt*` de las primeras corridas) necesita
  `--adopt-evidence-index` para volver a ser verificable, y esa adopción queda
  marcada en el manifiesto. El artefacto de este lote **no** está en ese caso:
  su índice está en el commit y se verifican 1134/1134 con el rebuild.
- Un timeout se registra como `INCONCLUSIVE` y nunca se resuelve por
  conveniencia. En este lote no hubo ninguno: por eso la comparación de universos
  está completa en todas las identidades medibles. La telemetría y la ventana del
  lote se derivan de `progress.jsonl`; no hay ninguna cifra recompuesta a mano
  ni copiada de un log externo.

## 12. Remediación de la revisión independiente (ZEUS)

La revisión independiente (`audit/adr073-gazetteer-wide-v1-independent`,
`fabda711`) devolvió `REMEDIATION_REQUIRED` con 3 P1, 6 P2 y 2 P3, y cinco tests
adversariales rojos (`z08`, `z09b`, `z11`, `z12`, `z13`). Esos cinco casos se
portaron **verbatim** a `tests/test_zeus_adr073_adversarial.py` y hoy pasan; las
expectativas no se rebajaron. La re-revisión (`2e2c9a9`) agregó `z16` —el
fail-open del índice de hashes—, portado igual de verbatim a
`tests/test_zeus_adr073_recheck.py`, más los findings P1-4 y P3-3 de la tabla.
Las regresiones propias quedaron en
`tests/test_gazetteer_source_binding_audit_regressions.py`.

| Finding | Qué era | Qué quedó |
|---|---|---|
| P1-1 | 1134 raw records en disco, 384 trackeados; la evidencia de los 2 candidatos SENA no estaba en el commit | 1134/1134 trackeados, con test que lo mide (`git ls-files`) |
| P1-2 | `done[canonical_hash] = record`: el último intento ganaba en silencio | supersession explícita (auto o por ledger validado); duplicado sin declarar → reporte falla y el canónico queda sin resultado |
| P1-3 | `--summary-only`/`--only` reescribían `started_at`/`finished_at` del lote | `batch` separado e inmutable; `last_invocation` propio; `report_rebuilt_at` |
| P2-1 | «593 verificados» aplanaba liveness con chequeo de identidad fiscal | `source_name_liveness_verified` + `foreign_identity_check` (18 NIT / 575 estructural) |
| P2-2 | telemetría recompuesta a mano y `batch_totals` alimentado desde un log en `/tmp` | telemetría derivada de `progress.jsonl`, separando todos los intentos (2312) de los vigentes (2298); claves viejas absorbidas y declaradas |
| P2-3 | `records.sha256` existía pero nadie lo generaba ni lo validaba | lo genera el pipeline y se verifica **antes** del resumen; no se regenera sobre evidencia alterada; exit ≠ 0 |
| P2-4 | `READY_AFTER_REVIEW` le ganaba a `would_exceed_after_review` | `over_after` → `BLOCKED` (tope `> 40`, como producción) |
| P2-5 | dos miembros con el mismo nombre físico no se detectaban | `duplicated_source_names` por dataset y `BLOCKED`, hoy y tras revisión |
| P2-6 | `if value` hacía invisible un dataset en 0 | `value is not None`: 62 → 81 casos; no cambia clase ni elegibilidad |
| P3-1 | el documento usaba alias humanos como nombres canónicos (`ECOPETROL`, `CORPOICA`, `UAESP`, `UFPS`) | nombres canónicos del artefacto (`EMPRESA COLOMBIANA DE PETROLEOS`, `… - AGROSAVIA`, `UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS`, `UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA`) |
| P3-2 | `smoke_gates.json` citado e inexistente; HEAD del manifiesto sin contexto; `icbf_units` 34 con 33 unidades | gates reanclados a la evidencia cruda; `batch.head` vs HEAD de la invocación; `icbf_units` cuenta unidades declaradas (33) |
| P1-4 (re-revisión) | borrar `records.sha256` antes de un rebuild hacía que el pipeline lo regenerara desde el disco y certificara `ok: true` con un registro alterado (reproducido: exit 0 contra exit 2 con el índice presente) | el índice no se crea solo: su ausencia con evidencia esperada es un problema y la corrida sale ≠ 0; fijar una línea base sobre un directorio heredado exige `--adopt-evidence-index` y queda marcado en el manifiesto. Test: `test_z16_deleting_the_hash_index_must_not_launder_altered_evidence` (portado verbatim de `2e2c9a9`) |
| P3-3 (re-revisión) | la rama «evidencia sin entrada en el índice» no tenía regresión propia | test directo (`test_index_without_entry_for_an_active_record_is_undeclared`): índice presente y **sin** la línea de un registro activo → `undeclared` + exit ≠ 0. Y sin índice no se enumera esa rama: una causa raíz, un problema, no 1134 líneas repitiendo lo mismo |

Lo que esta remediación **no** hizo, por estar fuera de su alcance: no volvió a
ejecutar el lote (2312 llamadas) —todo el trabajo es offline sobre la evidencia
existente—, no promovió ningún candidato, no tocó `app/**` ni los datos de
producto, no modificó los 1137 asientos de `progress.jsonl` y no cambió ningún
resultado observado para hacer pasar un test. La deuda de producto que la
revisión dejó explícitamente aparte (ownership de alias ANI/INVÍAS, las 14
`AMBIGUOUS_IDENTITY`, el worklist de acreditación, `SENA REGIONAL ATLANTICO` y
los bindings de entidades nuevas) sigue abierta y sigue siendo otra misión.

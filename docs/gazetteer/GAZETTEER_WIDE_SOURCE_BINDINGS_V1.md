# GAZETTEER-WIDE SOURCE BINDING AUDIT V1 — cobertura ADR-073 sobre todo el gazetteer

**Tipo:** auditoría read-only + artefacto reproducible
**Fecha:** 2026-09-18 · **Rama:** `audit/adr073-gazetteer-wide-v1` · **HEAD antes/después:** `0ca6469`
**Artefacto:** [`artifacts/audits/gazetteer_source_bindings_v1/`](../../artifacts/audits/gazetteer_source_bindings_v1/)
**Reproductor:** [`scripts/audits/audit_gazetteer_source_bindings_v1.py`](../../scripts/audits/audit_gazetteer_source_bindings_v1.py)
**Tests:** [`tests/test_gazetteer_source_binding_audit.py`](../../tests/test_gazetteer_source_binding_audit.py)

## 1. Veredicto

```text
ADR073_GAZETTEER_WIDE_V1: PARTIAL
  corte de este documento: 385 / 1134 identidades cerradas (2026-09-18 01:54, America/Bogota)
  0 ERROR · 0 INCONCLUSIVE · 0 drift · 0 candidatos
  el lote sigue corriendo; se reanuda con --resume y la cifra viva es summary.json
RUNTIME MODIFICADO: NO · DATOS DE PRODUCTO MODIFICADOS: NO · PROMOCIÓN: NO
```

Este documento describe el método y congela un corte. **La cifra viva es
`summary.json`**, que se reconstruye desde `progress.jsonl` con `--summary-only`
sin volver a tocar la red. Un corte parcial se declara como tal: `summary.json`
expone `uncovered_selected`, y ese número es la parte del gazetteer que este
corte NO cubre.

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

## 6. Resultados del corte (385 / 1134, 2026-09-18 01:54)

Clases al corte — la tabla completa y viva está en `summary.json`:

| Clase | N | Qué significa aquí |
|---|---:|---|
| `ALREADY_BOUND_VERIFIED` | 243 | binding vigente re-observado sin drift |
| `NO_STABLE_IDENTITY` | 137 | una sola forma física y un solo NIT **observado**, sin acreditación |
| `AMBIGUOUS_IDENTITY` | 5 | el LIKE del canónico alcanza ≥2 identidades distintas |
| `BINDING_DRIFT_REVIEW` · `FOREIGN_IDENTITY_COLLISION` · `NOT_EQUIVALENT` · `INCONCLUSIVE` · `ERROR` | 0 | nada que reportar en este corte |

```text
bindings vigentes re-observados: 243 / 243 verificados, 0 con drift
candidatos nuevos:                0   (ver §8: por cobertura de identidad, no por el auditor)
```

Los 593 bindings del sidecar se re-observan contra la fuente (formas declaradas
presentes, filas de hoy, ausencia de NIT ajeno). La verificación no toca el
sidecar: lo audita. **Ningún binding vigente se modificó ni se propuso modificar.**

## 7. Casos peligrosos (no se esconden para mejorar el porcentaje)

**Colisiones de LIKE (`AMBIGUOUS_IDENTITY`)** — el nombre canónico, tal como lo
emite hoy el fallback, alcanza identidades fiscales distintas:

| Canónico | Filas alcanzadas | NITs alcanzados | Lectura |
|---|---:|---:|---|
| `ANI` | 214.307 | **197** | el acrónimo es substring de agencias, municipios y personas: el LIKE por acrónimo no acota nada |
| `DANE - DIRECCION TERRITORIAL CENTRO` | 54.089 | 2 | la dirección territorial no es una sola identidad fiscal |
| `ANM` | 21.826 | 2 | acrónimo compartido |
| `DEPARTAMENTO DE CORDOBA` | 21.767 | ≥2 | el nombre del departamento no identifica una sola unidad compradora |
| `ADRES` | 9.655 | 4 | — |

**Splits observados en bindings VIGENTES** (`evidence_flags.source_name_split_observed`):
`DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA` (21.516 filas;
el nombre físico publicado es `DAPRE`, el canónico largo no devuelve una sola
fila) y `.SECRETARÍA DISTRITAL DE SALUD.` (1.134). Es evidencia del fenómeno que
ADR-073 vino a resolver, no una reclasificación de esos bindings.

**Pendiente de revisión por composición:** al corte, **1** identidad acreditada
mostró señal de composición (un NIT que publica más de una unidad token-distinta),
**0** formas reales quedaron sin alcanzar por el LIKE y **4** identidades tienen
cobertura desbalanceada entre datasets (un dataset alcanza <1% del otro). El
detalle con nombres está en `summary.json → top_lists`. El caso
CGR/Cundinamarca/Valle del discovery de ADR-073 **todavía no apareció en el tramo
procesado**: no se declara ausencia, se declara cobertura.

## 8. Candidatos (`bindings_ready.jsonl`)

```text
candidatos al corte: 0
```

No es un fallo del auditor ni un problema de la fuente: es el resultado de la
**cobertura de identidad actual**. Sólo 18 identidades del inventario tienen un
NIT acreditado por una autoridad declarada, y las 18 ya tienen binding. Sin
llave acreditada no hay `identity_key`, y sin `identity_key` no hay candidato que
pueda escribirse sin inventar la identidad — que es exactamente lo que el encargo
prohíbe.

Lo accionable de este corte es el **worklist de acreditación**: identidades cuyo
universo LIKE alcanza **una sola forma física y un solo NIT** en ambos datasets,
con universo completo y sin identidad ajena.

```text
worklist al corte: 137 identidades   (summary.top_lists.no_stable_identity_with_single_observed_nit)
mayores: ANT 102.154 · ALCALDIA DISTRITAL BARRANCABERMEJA 59.608 · ALCALDIA MUNICIPIO DE IBAGUE* 56.651
         ALCALDIA DE PASTO 49.802 · DEFENSORÍA DEL PUEBLO 49.235 · CAR CUNDINAMARCA 46.123
```

Acreditar esos NIT es una decisión de producto con revisión humana; con la
acreditación en mano, cada uno pasa a `EXACT_BINDABLE` con el candidato ya
medido en su `records/<hash>.json`. El auditor no da ese paso solo.

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

Los grupos se leen en `buyer_group_readiness.json`; **los candidatos no son
bindings productivos**, así que `READY_AFTER_REVIEW` sólo significa «si los
candidatos individuales se adjudican y promueven, el grupo quedaría completo».
Un miembro sin evidencia en el corte aparece como bloqueado con esa razón
escrita, no como cumplido.

Los ecosistemas institucionales quedaron probados en el smoke (§5) y se miden
sin aplanarse:

- `ECOSYSTEM:ICBF:national`: 33 nombres oficiales declarados, **121.936 filas**
  en procesos y 120.869 en contratos, y tres formas de NIT publicadas
  (`899999239`, `8999992391`, `8999992392`) — la misma nota del catálogo que
  advierte verificar regional por regional.
- `ECOSYSTEM:SENA:regional:Antioquia`: **decenas de nombres físicos** (centros,
  grupos administrativos, intercentros) bajo un solo NIT `899999034`. SENA no es
  un mapa 1:1 y el auditor no lo trató como tal.
- `sena_ecosystem_coverage.json` lleva la cobertura por regional y por dataset,
  con `units_total`, identidad, bindings y revisión. **No se creó ningún Buyer
  Group nuevo** ni se modificó la membresía declarada.

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

- Sólo 18 identidades del inventario tienen NIT acreditado hoy, y las 18 ya
  tienen binding: la clase `EXACT_BINDABLE` **no puede** producir candidatos
  nuevos mientras la acreditación siga siendo la actual. Eso es un resultado de
  la cobertura de identidad, no un fallo del auditor.
- Un NIT observado y no acreditado queda registrado como evidencia
  (`no_stable_identity` con su valor). Acreditar esos NIT es una decisión de
  producto con revisión; el auditor sólo deja el worklist.
- Las formas físicas que el LIKE no alcanza quedan declaradas y fuera de todo
  candidato. Incorporarlas ampliaría el universo consultado y eso es una
  decisión de Buyer Group, no de proyección de nombres.
- Un timeout se registra como `INCONCLUSIVE`; nunca se resuelve por
  conveniencia. Reprocesar lo inconcluso es parte del lote, no una excepción.

# HCA Oracle Patch V1 — Integration Review y reparación de evidencia canónica

**Fecha:** 2026-08-26. **HCA_ORACLE_PATCH_INTEGRATION_REVIEW: PASS**.
**HCA_ORACLE_PATCH_V1: INTEGRATED**. **NEXT: READY_FOR_HCA_FINAL_AUDIT**.

Se reparó primero la omisión documental del review runtime y después se integró
el patch autorizado. No se editó runtime, runner, tests de producto ni ninguna
expectativa adicional. Los seis oráculos se incorporaron sin modificación respecto
al commit fuente; los siete FAIL restantes no se tocaron ni readjudicaron.

## 1. Cadena de integración

| Hito | Commit / resultado |
| --- | --- |
| Main antes | `8833ba286d5679926493efed4287cdbb425ff276` |
| Fuente Oracle Patch V1 | `9bd014c0d11c0acb353006eddd4421078c1bba0a` |
| Reparación documental en main | `25d9eac05eb14e56ed2f8b9c9ddb6b670c0f1092` |
| Main tras cherry-pick del patch | `f074402c5147e98ede373d09341874df2c428aaf` |
| Método | `git cherry-pick 9bd014c0d11c0acb353006eddd4421078c1bba0a` |
| Conflictos / merge commits / push | `0 / 0 / NO` |

El cierre que contiene este informe y la actualización de bitácora es otro commit
exclusivamente documental posterior a `f074402`; no cambia el árbol ejecutable ni
el corpus que se verificaron. Su SHA se obtiene del historial de este informe.

El preflight encontró HEAD en `audit/hca-oracle-patch-v1`, refs coincidentes con
la solicitud, cero cambios tracked y 250 archivos untracked: 59 de evidencia
omitida y 191 ajenos. Se conservaron los worktrees existentes y se añadió un
worktree detached de `9bd014c` para validar el patch con el entorno Python ya
instalado. No hubo reset, clean, rebase, instalación ni intervención en servicios.

Tras reparar la evidencia, `merge-base(main, 9bd014c)` siguió siendo `8833ba2`,
pero `merge-base --is-ancestor main 9bd014c` devolvió 1:
**CLEAN_DOC_DIVERGENCE**, no un fallo. No se intentó fast-forward ni se reescribió
el commit fuente. La rama `audit/hca-oracle-patch-v1` conserva su SHA original.

## 2. Omisión documental: procedencia y selección explícita

**MISSING_RUNTIME_REVIEW_EVIDENCE: found=YES, legitimate=YES**.
**Clasificación: CANONICAL_EVIDENCE_OMISSION**.

Se versionaron exactamente el [informe original de integración Final P1](2026-08-26_HCA_FINAL_P1_RUNTIME_INTEGRATION_REVIEW.md)
y sus **58 archivos de evidencia**, sin alterar sus bytes: **59 archivos** en
`25d9eac`, mensaje `docs(audit): track final HCA runtime integration evidence`.
El [inventario por archivo](evidence/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW/omission-inventory.tsv)
registra PATH, TYPE, SIZE, PURPOSE, REFERENCED_BY_TRACKED_DOC,
BELONGS_TO_RUNTIME_INTEGRATION_REVIEW, estado untracked inicial y SHA256.

La bitácora de `8833ba2` ya enlazaba el informe omitido; el informe del patch
también lo referencia. Los 57 hashes del manifiesto original coinciden y no hay
archivos extra en la carpeta. Los JSON, corpus, commits y resultados se cotejaron
con el historial y entre sí. El contenido confirma:

- `MAIN_BEFORE=eb8cfe1`, `MAIN_AFTER=dec589a`, siete commits y fast-forward.
- HCA `177/18/0 → 182/13/0`, comparación de los 195 IDs sin regresiones.
- Seis defectos runtime cerrados y `HCA_FINAL_P1_RUNTIME: CLOSED / INTEGRATED`.
- Suite autoritativa histórica: **8657 passed, 2 skipped, 24 xfailed, 0 failed**.

El log previo sin índice y el control negativo de mutación pertenecen al mismo
review y están explicados por su informe: no se presentan como baseline final,
experimento ajeno o nueva conclusión. No se incorporó contenido de otras carpetas
untracked. Los seis `.py` del cierre anterior son reproductores históricos bajo
`docs/audits/evidence/`, no runtime, runner ni tests de producto. Algunos requieren
el scratch y los worktrees originales; esa dependencia queda registrada y no se
afirma que todos sean ejecutables desde el archivo canónico sin preparación.

Además, 55 archivos coinciden byte a byte con el scratch histórico disponible.
`final-status.txt` sólo perdió un espacio inicial al archivarse; su hash archivado
coincide con el manifiesto. Se conserva sin edición y se interpreta como snapshot
legible, no como porcelain para parsear columnas del índice.

**Advertencia de formato, preservada explícitamente:** `git diff --cached --check`
de la reparación devolvió **2**, con **34 avisos de trailing whitespace únicamente
en el `mutation.log` histórico**, cuya salida de pytest contiene espacios finales.
No se presenta ese chequeo como limpio. Se preservaron los bytes y el SHA original;
no se alteró la configuración de whitespace ni se reescribió la evidencia.
El [diagnóstico exacto](evidence/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW/repair-whitespace-check.json)
está archivado como JSON. No existe duda de procedencia ni cambio semántico.

El stage usó una lista explícita de los 59 archivos, nunca `git add -A`.
**Reparación: RUNTIME=0, TESTS=0, FIXTURES=0, RUNNER=0**.

## 3. Alcance y minimalidad del patch

| Clasificación de `8833ba2..9bd014c` | Archivos |
| --- | ---: |
| HCA_FIXTURE | 1 |
| DOC | 1 |
| EVIDENCE | 9 |
| BITACORA | 1 |
| RUNNER / RUNTIME / OTHER_TEST / OTHER | 0 / 0 / 0 / 0 |

Los dos `.py` del patch son sondas documentales ya incluidas en `9bd014c`.
**Python inesperado=0**; este cierre no añade scripts Python. Los árboles `app/`
y `scripts/` son idénticos entre base, fuente y main integrado.

La comparación estructural independiente y el gate existente confirman exactamente:

```text
HCA-ACTOR-004
HCA-TOPIC-007
HCA-ENTITY-001
HCA-FOLLOWUP-007
HCA-META-001B
HCA-FOLLOWUP-016
```

**195 IDs y orden preservados; UNEXPECTED=0**. Se conservaron todas las consultas,
clocks, categorías, severidades y primeros turnos de ambos followups. Fuera de los
casos, el YAML parseado conserva sus metadatos; sólo hay enmienda de comentario.

El diff binario de `8833ba2..9bd014c` es exactamente igual al de
`25d9eac..f074402`, SHA256
`0d9b1c991ef9821e323f7a0b96ece21d4ca180b71a7f3e0620cf42606d2a85be`.
En el momento de integrar, la única diferencia entre main y el árbol fuente eran
los 59 archivos reparados. No se retocaron expectativas durante la integración.

## 4. No vacuidad y límites observables

Se reejecutó el probe existente sobre registros reales de esta nueva corrida.
Cada registro real pasa y su mutación conceptual falla: **NONVACUOUS=6/6**.

| Caso | Contrato que sigue protegido | Mutación detectada |
| --- | --- | --- |
| ACTOR-004 | Candidatos no autorizan identidad; block, actor_pending, CANDIDATES, candidato tipado y ausencia de actor_scope/contratista | Auto-resolución fuzzy; 5 checks fallan |
| TOPIC-007 | Concepto `alimentacion_escolar`, territorio y alias fuente; sin `%pae%` | Pérdida del tema; 2 checks |
| ENTITY-001 | Comprador real `DEPARTAMENTO DEL ATLANTICO**`, fechas y ausencia de territorio | Comprador convertido en territorio; 3 checks |
| FOLLOWUP-007 | Obra estructurada y territorio; modalidad eliminada | Pérdida de Obra al quitar modalidad; 2 checks |
| META-001B | Distinción legítima comprador/territorio, tema y tiempo | Colapso de las dos dimensiones; 2 checks |
| FOLLOWUP-016 | Dinero, Cali, Obra, orden ascendente y ausencia de compradores derivados | Reintroducción del ecosistema derivado; 2 checks |

Ninguno se reduce a execute, clarify, dataset o un campo trivial. Se conservaron
[siete registros reales sin editar](evidence/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW/nonvacuity-records.jsonl)
para reproducir el [resultado del probe](evidence/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW/nonvacuity.json).

**FOLLOWUP-016:** el DSL HCA no compara WHERE(T1) con WHERE(T2). La evidencia
externa del Final P1, ahora versionada, sí captura las llamadas reales al stub:

```sql
-- WHERE1 y WHERE2, idénticos
unaccent(UPPER(ciudad)) LIKE unaccent(UPPER('%Cali%')) AND valor_del_contrato >= 300000000 AND (tipo_de_contrato = 'Obra')
```

Mismo dataset; sólo cambia ORDER BY de DESC a ASC. LIMIT permanece en 50, sin
cursor. Se extrajo el WHERE completo, sin quitar compradores ni otros filtros.
**FILTERS(WHERE1)==FILTERS(WHERE2)**.
**FOLLOWUP_016_SOURCE_UNIVERSE_EVIDENCE: EXTERNAL_AND_CANONICAL**.
No se añadió DSL. Las consultas fuente completas están en la
[validación de procedencia](evidence/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW/omission-validation.json).

**ACTOR-004:** el fixture tampoco observa directamente llamadas al stub. Su
oracle es fuerte dentro de la DSL mediante block + pending + CANDIDATES +
ausencia de identidad; no se creó un proxy de source calls. El registro externo
del review runtime demuestra cero llamadas y permanece **complementario**, no
se atribuye esa aserción al fixture.

## 5. Corridas completas y siete FAIL restantes

| Corrida nueva | Ref / árbol | PASS | FAIL | ERROR |
| --- | --- | ---: | ---: | ---: |
| Baseline, después de reparar docs | `25d9eac` | 182 | 13 | 0 |
| Patch, antes de integrar | `9bd014c` detached | 188 | 7 | 0 |
| Main después del cherry-pick | `f074402` | 188 | 7 | 0 |

Comparados los 195 IDs: **PASS→FAIL=0; FAIL→PASS=exactamente los seis autorizados**.
Los 195 veredictos completos postintegración son idénticos a los de la corrida
del patch. Los estados del baseline también coinciden con la evidencia runtime
canónica y la evidencia pre-patch. Todos los isolation.json declaran
`outside_sandbox=[]`.

| Clasificación conservada | IDs | Cantidad |
| --- | --- | ---: |
| SAFE_UNSUPPORTED | HCA-TOPIC-012 | 1 |
| KNOWN_DEFERRED | HCA-MODALITY-002, HCA-MODALITY-012, HCA-TYPO-003 | 3 |
| INSUFFICIENT_EVIDENCE | HCA-ENTITY-004, HCA-STATE-003, HCA-SOURCE-002 | 3 |
| REAL_RUNTIME_BUG | Ninguno entre los FAIL adjudicados restantes | 0 |
| CONFIRMED_ORACLE_DEBT | Ninguno entre los FAIL adjudicados restantes | 0 |

Las clasificaciones se cotejaron con `adjudication.json` del Oracle Review
original, no se dedujeron del nuevo score ni se cambiaron por conveniencia.

## 6. Regresiones focales y verificación final

| Comando ejecutado sobre `9bd014c` | Resultado |
| --- | --- |
| `make audit-hca-final-p1` | 40/40 |
| `make audit-actor-hygiene-v1` | 36/36 |
| `make audit-control-language-v1` | 42/42 |
| `make audit-time-v2` | 32/32 |
| `make audit-negation-v1` | 22/24; mismos NEG-N15 y NEG-ANTI-002 |

Los cinco fixtures focales conservan sus SHA. Se usó `PYTHONHASHSEED=0` y
`AUDIT_OUT` separado por corrida; no se cambiaron fixtures ni recursos del usuario.
Los 250 archivos inicialmente untracked conservaron su contenido SHA256: 59
pasaron a tracked por reparación y los **191 ajenos siguen untracked e intactos**.

`git diff --check` del working tree postintegración y del diff del cherry-pick:
**PASS**. El diff acumulado de la reparación conserva los 34 avisos históricos
de `mutation.log` declarados en §2; no se afirma que ese diff esté libre de avisos.
**FULL_PYTEST: NOT_REQUIRED / NOT_EXECUTED**. No se ejecutó `make lint-core`:
no hubo Python inesperado ni cambios en `app/`. La suite 8657 es evidencia
histórica validada, no una corrida atribuida a esta integración.

## 7. Baseline canónico y cierre

```text
FINAL_P1_RUNTIME: INTEGRATED
ORACLE_PATCH_V1: INTEGRATED
RAW_HCA: 188 / 195
SEMANTICALLY_SUPPORTED: 189 / 195
COMPOSITION_OF_RAW_FAILS: 1 SAFE + 3 DEFERRED + 3 INSUFFICIENT
HCA_SHA_HISTORICAL: 262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba
HCA_ORACLE_PATCH_V1_SHA: a341405a79d74cdb43872cf937cd2b58f4ebb736afe4a2135ced9b6d389c352e
RUNTIME_CHANGED: 0
RUNNER_CHANGED: 0
MERGE_COMMIT: NO
PUSH: NO
NEXT: READY_FOR_HCA_FINAL_AUDIT
```

189/195 significa 188 PASS observados más un SAFE sostenido por la adjudicación;
no es el score del runner. El nuevo SHA reemplaza deliberadamente al histórico
como baseline del Final HCA. Se conservan el SHA anterior, el Oracle Review,
el Oracle Patch original y todo el historial de adjudicación.

Evidencia nueva: [directorio y manifiesto SHA256](evidence/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW/manifest.json),
[gates posteriores](evidence/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW/postintegration-gates.json)
y [comparación de los 195 IDs](evidence/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW/hca-status-diff.json).
Los comandos reproducibles son el runner HCA y los cinco targets anteriores,
el `corpus_gate.py --base 8833ba2 --json <salida>` existente y
`mutation_probe.py --results <nonvacuity-records.jsonl> --json <salida>` existente.
Usar salidas nuevas fuera de la evidencia histórica; no sobrescribir archivos
canónicos para repetir la auditoría.

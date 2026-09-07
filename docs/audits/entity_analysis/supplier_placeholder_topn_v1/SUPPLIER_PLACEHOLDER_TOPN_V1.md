# SUPPLIER_PLACEHOLDER_TOPN_V1

Actualización posterior: véase «Procurement Foundation frozen replay fix» al final.
El diagnóstico y veredicto PARTIAL del cierre supplier original se conservan
como evidencia histórica; la nueva validación se registra por separado.

## Cambio y base

Se restaura una invariante existente: placeholder no es actor. El filtro se
aplica a documentos antes de agrupar y limitar, en cantidad y valor, reutilizando
Actor Identity. No modifica Aggregate Value Quality, ADR-005, value_sanity,
Offers, Competition, UT/consorcios, Gazetteer ni GOB-DEPTO-001.

- BASE: `911683b504baacc24feabc2c7302adf3a2ef5d1c`, HEAD real de main al inicio.
- BRANCH: `fix/entity-supplier-placeholder-topn-v1`.
- WORKTREE: `/Users/rodrigoortiz/Documents/secoppal-supplier-placeholder-topn-v1`.
- HEAD de implementación probado: `da19819bd61d3c85339f19dfdb0de74849e9aa00`.
- AVQ no se integró para preparar esta tarea; se partió del main observado.
- Sin merge, push, arranque de app, repins ni cambios en persistencia de main.

## Root cause y autoridad

`WHERE documento_proveedor IS NOT NULL AND documento_proveedor != ''` dejaba
pasar `No Definido`. GROUP BY fusionaba nombres, `max(proveedor_adjudicado)`
rotulaba el grupo artificial y LIMIT 5 lo seleccionaba antes de que
`normalize_identity()` lo descartase. El parser ya era seguro; sólo corregirlo
no podía recuperar el puesto consumido.

`actor_document_sentinels()` ofrece una tupla estable de la autoridad privada
existente; `is_actor_document_sentinel()` comparte exactamente su strip/uppercase
con `_clean_raw`. No se añadieron sentinelas ni reglas de normalización.
`actor_identity_soql.exclude_actor_document_sentinels()` proyecta esa autoridad,
valida el identificador de columna y duplica comillas en literales SoQL.

QUERY_BEFORE (fragmento tras el buyer scope):

```sql
AND documento_proveedor IS NOT NULL AND documento_proveedor != ''
GROUP BY documento_proveedor, tipodocproveedor
ORDER BY contract_count DESC, tipodocproveedor ASC, documento_proveedor ASC LIMIT 5
```

QUERY_AFTER (mismo scope y mismo orden; valor usa total_contract_value DESC):

```sql
AND documento_proveedor IS NOT NULL AND documento_proveedor != ''
AND upper(trim(documento_proveedor)) NOT IN ('-', 'N/A', 'NA', 'NO APLICA', 'NO DEFINIDA', 'NO DEFINIDO', 'NODEFINIDO', 'NONE', 'NULL', 'SIN DATO', 'SIN INFORMACION', 'SIN INFORMACIÓN')
GROUP BY documento_proveedor, tipodocproveedor
ORDER BY contract_count DESC, tipodocproveedor ASC, documento_proveedor ASC LIMIT 5
```

La forma UPPER y el escape de comillas están documentados en las fuentes
oficiales [UPPER](https://dev.socrata.com/docs/functions/upper) y
[Text](https://dev.socrata.com/docs/datatypes/text). La aceptación de TRIM se
verificó en el endpoint SODA3 real: `soql_trim_probe.json` y los dogfoods.

## Audit bounded y oráculos vivos

Los probes iniciales se ejecutaron antes del fix, con SODA3 y timeout 8 s.
Frecuencias de documentos/tipos: LIMIT 1000, orden determinista, sin contratos
crudos. Barranquilla y Bucaramanga alcanzaron el cap: sólo es una muestra de los
grupos frecuentes, no un censo. Galapa histórico agotó el timeout en ese probe;
`rejected: []` en ese recibo **no** demuestra ausencia. Los rankings y probes
focalizados por nombres/tipos sí respondieron. El único placeholder observado en
las respuestas exitosas fue `No Definido`; no se afirma exclusividad nacional.

| Buyer / periodo | Filas del grupo tipo No Definido | Nombres detrás de ese grupo | Placeholder total, todos los tipos | Cantidad antes → después | Valor antes → después |
|---|---:|---:|---:|---|---|
| Barranquilla / 2026 | 11 | 9 | 14 | 4 → 5 | 4 → 5 |
| Bucaramanga / 2026 | 11 | 11 | 14 | 4 → 5 | 4 → 5 |
| Galapa / histórico | 65 | 8 | 66 | 4 → 5 | 5 → 5 |

El desglose tipado resolvió la aparente contradicción entre ranking y nombres:
Barranquilla y Bucaramanga también tienen 2 filas placeholder con tipo NIT y 1
con tipo Otro; Galapa tiene otra con tipo NIT. No se convierten esos nombres en
actores ni se infiere pertenencia UT. `audit_names_typed_*.json` conserva las
respuestas, sin cap alcanzado (LIMIT 100).

Las filas válidas del TOP5 anterior son exactamente el prefijo del ranking
posterior, incluidas métricas y orden, en las seis comparaciones. El quinto
puesto recuperado no reordena ni recalcula valores de las identidades válidas.
La comprobación está resumida en `summary.json`; SQL completo, raw rows y
SupplierMetrics están en `before_*.json`, `after_*.json` y `dogfood_*.json`.

El primer probe posterior de Galapa agotó 8 s en ambos rankings. El dogfood
posterior vía SecopalWorkflow completó overview y follow-up sin errores en
suppliers, con cinco métricas por ranking. Las latencias del follow-up fueron:
Barranquilla 0,176/0,421 s, Bucaramanga 0,140/0,512 s y Galapa 0,431/0,417 s
(cantidad/valor). Esto acredita esos recorridos; no es un benchmark de latencia
fría ni una garantía de disponibilidad del servicio.

Los prompts exactos `analiza barranquilla en 2026` y
`analiza bucaramanga en 2026` producen la aclaración institucional existente.
Se continuó con la Alcaldía explícita y después
`cuales son sus mayores proveedores`. Galapa usó el prompt solicitado histórico.
Los tres follow-ups ejecutaron **dos llamadas** y renderizaron diez entradas
(cinco por ranking). Settings y conversaciones se aislaron en un temporal; el
narrador se desactivó en el harness, sin cambiar código productivo.

## Pruebas y puertas

Red first: commit de regresión anterior al fix, dos fixtures sintéticos mínimos
con procedencia explícita (no fingidos snapshots live). La consulta generada se
ejecuta sobre SQLite como oráculo relacional de WHERE/GROUP BY/LIMIT; el
transporte/dialecto se verifica aparte con SODA3 real. No se simula el filtro
mediante el mismo helper que se está probando.

Salida literal inicial (`red_literal.json`; copia legible `red.txt`):

```text
7 failed, 16 passed in 1.51s
```

Incluye grupo artificial de 11 filas, etiqueta max(nombre), cuatro visibles
antes, cinco después y orden/métricas intactos. Se añaden pruebas de escape,
identificador SQL, separación CC/NIT, no-sentinel inválido, presupuesto y render.
Hypothesis 6.165.10 comprueba inserción de sentinelas arbitrarios del vocabulario
con variaciones de caso/espacios, métricas y multiplicidades arbitrarias de
actores válidos, y paridad helper/parser con whitespace. Propiedades sin red,
deterministas; no se alteró el parser tipado.

Preflight: Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10; Codegraph 0.9.6.
`codegraph status` informó índice actualizado de main en la base inspeccionada
(808 archivos, 18.853 nodos). El MCP estuvo cargado y se usó context + impact
para build_supplier_query/normalize_identity; impact identificó 61 símbolos,
incluidos alias source, alias index, actor intent y suppliers. Se confirmaron
manualmente los archivos y consumidores relevantes de esa misma base en el
worktree. No se afirma que el índice de main cubra los archivos nuevos.

Comando focal:

```sh
.venv/bin/python -m pytest tests/test_actor_identity*.py tests/test_actor_alias_source.py tests/test_actor_source_contract.py tests/test_actor_profile*.py tests/test_entity_profile*.py tests/test_module_size_limits.py tests/test_plan_query_node.py -q
```

Salida literal (`focal.txt`):

```text
334 passed in 13.49s
```

Incluye corpus Entity Profile de turnos y conversaciones, y corpus query V1
por test_plan_query_node. Corrí esos tests focales, no la suite en esa etapa.

Puertas de estilo y leyes (salidas literales completas adjuntas):

```text
All checks passed!
app/core/ limpio
```

`ruff.txt`: `.venv/bin/ruff check app/ tests/ scripts/`.
`lint-core.txt`: `make lint-core` (no sustituye Ruff).
`semgrep_literal.json` conserva stdout/stderr exactos (incluidos espacios de
formato); `semgrep.txt` sólo recorta espacios finales para git diff --check.
Comando: `.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off`.
Salida literal resumida: `Ran 3 rules on 264 files: 10 findings.`
Semgrep baseline 10; posterior 10; delta 0, sin hallazgos nuevos ni removidos.
El scan posterior incluye el nuevo módulo ya rastreado por Git. Baseline crudo y
delta: `semgrep-baseline.json`, `semgrep_delta.json`.

FULL_SUITE: ejecutada una vez en background, tras pasar focales y comprobar con
pgrep que no existía otro pytest. La solicitud específica autorizó esta ejecución;
no se ejecutó simultáneamente con dogfood. Salida literal (`full-suite_literal.json`;
copia legible `full-suite.txt`):

```text
1 failed, 11571 passed, 11 skipped, 14 xfailed in 512.39s (0:08:32)
```

Falla `tests/test_procurement_relationships.py:186`,
`test_complete_offline_replay_canaries_and_runtime_isolation`. Se ejecutó sólo ese
test sobre un worktree temporal del SHA BASE, sin cambios; salida literal en
`full-failure-base.txt`:

```text
1 passed in 0.49s
```

Por tanto **es un fallo nuevo inducido por esta entrega**, no un baseline rojo.
El replay recalcula hashes de archivos fuente del worktree y compara contra el
registro congelado. La única diferencia del registro es
`source_authorities.local_digests.app/core/actor_identity.py`: extraer la API
pública solicitada cambia el hash literal, aunque conserva la semántica de la
autoridad. Diagnóstico offline sin escrituras a evidencia ni red en
`full_failure_diagnosis.json`:

- Base/registro: `ca0c436eecea67fbbb30d5397249b193112cc15f0bf9f50444ae3ef35ae9b725`.
- Actual: `f23d9c6a3f15fdbfcee788a567260463a484777687cda3bddfda544acaf7bbaf`.
- Diferencias en el resto de artefactos de Foundation: ninguna (byte exact).
- Guardas calculadas: PASS; identidad/efectos/runtime imports: todos cero.

No se cambia `build_registry.py`, el test de replay ni el registro de Procurement
Relationships, por la exclusión explícita del carril. El bloqueo de integración
requiere una decisión del orquestador sobre el replay histórico: revisar su uso
de bytes del checkout frente a los blobs de la entrega congelada. No se propone
repinear los hashes para esconder el cambio.

## Alcance, pendientes y riesgos

- GROUP_BY_CHANGED: NO. LIMIT_CHANGED: NO. CALL_BUDGET_CHANGED: NO.
- NAME_FALLBACK: NO. VALID_RANKING_CHANGED: NO.
- Los diez hallazgos Semgrep preexistentes están fuera del carril y no se tocan.
- El replay congelado de Procurement Relationships queda rojo por el nuevo hash
  fuente; el diagnóstico y la comparación contra BASE se entregan sin modificarlo.
- No se añade supplier_identity_coverage: los TOP5 no dan denominadores globales
  y obtenerlos requeriría otra llamada.
- Documentos no-sentinel rechazados por otras reglas pueden aún reducir el TOP5;
  no se inventa identidad, validación SQL tipada ni refill. Normalizaciones que
  colapsen distintas escrituras raw tampoco se reagrupan en este cambio.
- SQL TRIM cubre espacios exteriores. No se declara equivalencia universal con
  `str.strip()` para tabs/otros whitespace; el parser sigue rechazando esas
  formas. No se amplía una lista de escrituras por intuición.
- Audit parcial por caps/timeouts; observación viva sin snapshot transaccional.
- No se cambia autoridad de identidad ni se crea ADR.

COMMITS (sin squash):

1. `ab25016a47ebeb933bbb2298b48054426c0d8a7a` — test(entity-profile): freeze supplier placeholder topn regression
2. `da19819bd61d3c85339f19dfdb0de74849e9aa00` — fix(entity-profile): exclude identity sentinels before supplier ranking
3. Cierre documental de esta evidencia; SHA final en la entrega y en git log.

GIT_DIFF_CHECK: sin salida, exit 0, para cambios staged y BASE..HEAD.

FILES_CHANGED: actor_identity.py; nuevo actor_identity_soql.py; suppliers.py;
tests de sentinelas y regresión supplier; fixture mínimo; ENTITY_ANALYSIS_V1.md;
bitácora técnica; este directorio de recibos y salidas.

VERDICT: **PARTIAL**. Fix focal y dogfood acreditados; integración bloqueada por
el replay de hash descrito. Sin merge ni push.

## Reproducción

Los harnesses exactos usados están archivados como `probe_harness.py.txt` y
`dogfood_harness.py.txt`. Ejecutar desde el worktree con `PYTHONPATH=.` y el Python
del entorno del proyecto. El probe `before` corresponde a la base anterior al
fix: usar ese código/base para reproducirlo; ejecutarlo sobre el fix generaría
queries corregidas. No sobrescribir los recibos históricos. El dogfood puede
escribir nuevos recibos en un directorio separado, manteniendo Settings aislado.


## Procurement Foundation frozen replay fix

Continuación autorizada en la misma rama desde
`5b5aef187b2c9e379e69b2d2be0df15542008dd6`. Se restaura la intención de ADR-092,
sin crear ADR, repinear el registro ni cambiar los tres commits supplier.

La causa se acreditó **antes de editar** para las siete fuentes locales:
[frozen_replay_fix/provenance_before.json](frozen_replay_fix/provenance_before.json).
Los siete digests del registro coinciden con los blobs Git de
`FOUNDATION_HEAD = d608336a32553ac11a69924d663491ce852e1446`.
Sólo Actor Identity difiere en el checkout posterior:

```text
FROZEN_REGISTRY_DIGEST: ca0c436eecea67fbbb30d5397249b193112cc15f0bf9f50444ae3ef35ae9b725
FOUNDATION_BLOB_DIGEST: ca0c436eecea67fbbb30d5397249b193112cc15f0bf9f50444ae3ef35ae9b725
CURRENT_CHECKOUT_DIGEST: f23d9c6a3f15fdbfcee788a567260463a484777687cda3bddfda544acaf7bbaf
```

`build_registry.sources()` ahora calcula exclusivamente `local_digests` desde
los blobs congelados. `frozen_delivery.py` centraliza BASE, FOUNDATION_HEAD y
LOCAL_SOURCES; su helper admite sólo esas rutas y usa `git show` con argumentos,
sin shell ni ref configurable por el usuario. Los errores Git se propagan:
no existe fallback al archivo actual. Los hashes de recibos/schema conservan
su procedencia anterior. Nodos, edges, cardinalidades, estados y límites de
cobertura permanecen intactos.

Historical replay describe los bytes usados por la entrega congelada. Current
compatibility/isolation sigue inspeccionando el checkout actual y atribuyendo
los efectos históricos a `git diff BASE FOUNDATION_HEAD`. La prueba que inserta
un import runtime prohibido en un árbol temporal conserva la detección y el
rechazo; sólo su spy de Git admite ahora las siete lecturas históricas exactas.

Red first, sin cambiar implementación: [red.json](frozen_replay_fix/red.json)
conserva salida literal y [red.txt](frozen_replay_fix/red.txt) su copia legible:

```text
6 failed, 39 passed in 1.77s
```

Las nuevas regresiones comprueban las siete procedencias, lectura de la ref
exacta, independencia ante bytes arbitrarios posteriores, ausencia de fallback
ante errores Git y rutas no acreditadas. Dos pruebas con Git real demuestran
rechazo de ref inexistente y de un archivo que existe actualmente pero no en la
entrega histórica. Hypothesis usa doce ejemplos deterministas de bytes; modifica
sólo la lectura simulada de archivos actuales, sin red ni escritura de fuentes.

Comandos ejecutados después del fix, en este orden:

```sh
.venv/bin/python -m pytest -q tests/test_procurement_relationships.py tests/test_procurement_foundation_frozen_delivery.py
.venv/bin/python -m pytest -q tests/test_actor_identity.py tests/test_actor_identity_sentinels.py tests/test_entity_profile_supplier_placeholders.py
.venv/bin/ruff check app/ tests/ scripts/
make lint-core
.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
git diff --check
.venv/bin/python scripts/audits/audit_relationship_foundation_v0.py --check
```

Salidas literales resumidas (stdout/stderr íntegros en los JSON del subdirectorio):

```text
48 passed in 2.99s
175 passed in 0.64s
All checks passed!
app/core/ limpio
Ran 3 rules on 264 files: 10 findings.
{"byte_exact": true, "graph_database": "NONE", "runtime_effect": "ZERO", "verdict": "PASS"}
```

Diff-check: sin salida, exit 0. Semgrep conserva exactamente los mismos diez
hallazgos baseline, comparados por regla, archivo y rango; nuevos 0, removidos 0,
errores de análisis 0. Ver [semgrep_delta.json](frozen_replay_fix/semgrep_delta.json).
Corrí los 48 tests Foundation y los 175 focales supplier, no la suite en esa etapa.

El manifiesto previo incluye 107 archivos del registro, Foundation y Layers.
[replay_verification.json](frozen_replay_fix/replay_verification.json) comprueba
cero cambios en esos archivos, diez artefactos reconstruidos byte-exactos,
registro byte-exacto, todas las guardas de identidad/efectos en cero y ninguna
modificación adicional en `app/`. REPLAY_TEST, EXACT_DELIVERY_TEST y TAMPER_TEST
pasan; no se desactiva ni debilita la comparación byte por byte.

La revalidación supplier usa el código actual contra los recibos live preservados;
**no es un dogfood nuevo contra SECOP**. Compara el SQL actual completo con el
SQL capturado, vuelve a parsear las filas, comprueba métricas/orden del prefijo
válido y las dos consultas por follow-up. Evidencia:
[supplier_receipt_recheck.json](frozen_replay_fix/supplier_receipt_recheck.json).

| Buyer | Cantidad antes → después | Valor antes → después |
|---|---|---|
| Barranquilla | 4 → 5 | 4 → 5 |
| Bucaramanga | 4 → 5 | 4 → 5 |
| Galapa | 4 → 5 | 5 → 5 |

GROUP_BY_CHANGED: NO. LIMIT_CHANGED: NO. CALL_BUDGET_CHANGED: NO.
NAME_FALLBACK: NO. VALID_RANKING_CHANGED: NO.

Instrumentación: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10 y Codegraph
0.9.6. MCP cargado y consultado para status; su índice correspondía a main.
Se inicializó y sincronizó un índice propio del worktree (813 archivos,
18.921 nodos, estado actualizado). CLI query/impact acreditó la cadena
read_source_blob → sources → build. `affected` no recorrió dependientes ni
identificó tests; se completó con `rg` e inspección manual de `run()` y sus tests.
No se interpreta esa limitación como ausencia de consumidores. Salidas:
[tooling.json](frozen_replay_fix/tooling.json).

Los diez hallazgos Semgrep ajenos al fix no se modificaron. Los límites supplier
originales sobre caps/timeouts, documentos no-sentinel y whitespace continúan.
El replay necesita acceso local a los objetos Git históricos; sin ellos falla
explícitamente. No se ejecutaron captura nueva, arranque de app, merge ni push.


Suite completa posterior al fix: ejecutada una sola vez en background con
`.venv/bin/python -m pytest -q`, después de las puertas y de comprobar que no
existía otro pytest (`full-suite-preflight.json`). La orden específica autoriza
esta ejecución. Salida literal final:

```text
11586 passed, 11 skipped, 14 xfailed in 524.62s (0:08:44)
```

Exit 0; **cero failures**, incluidos replay histórico, aislamiento y supplier.
[full-suite.json](frozen_replay_fix/full-suite.json) conserva el output literal;
[full-suite.txt](frozen_replay_fix/full-suite.txt) es la copia legible.
Por tanto ahora sí corrí la suite completa, además de los focales citados.

La verificación final conserva los 107 digests previos; registry repinned: NO;
frozen artifacts changed: NO; current checkout used for frozen digest: NO.
La rama conserva los tres commits supplier y añade uno separado:
`fix(audit): replay procurement foundation from frozen source blobs`.
HEAD_BEFORE: `5b5aef187b2c9e379e69b2d2be0df15542008dd6`;
HEAD_AFTER corresponde a este commit de cierre y se entrega con su SHA en el
reporte final y `git log`. Sin squash, merge ni push.

FINAL_BRANCH_VERDICT: **GO**. El fallo de replay que bloqueaba el cierre
supplier queda resuelto. Rama validada para fast-forward a main, cuya relación
de ancestro se comprobó al cierre; la integración no se ejecuta en esta tarea.
No queda trabajo pendiente dentro del alcance solicitado.

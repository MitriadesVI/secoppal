# VALUE-SANITY-REACHABILITY-V1

Se restaura la defensa ADR-005 / VALUE-SANITY-002A en búsquedas ordinarias de
contratos: `valor_facturado` viaja en el SELECT de `jbjy-vk9h` y se convierte
mediante `SecopClient._safe_money` antes de llegar al formatter. El detector ya
existía, pero la proyección omitía su denominador; incluso añadir solo el SELECT
dejaba un string que el detector rechazaba por no ser numérico.

## Base y alcance

- Base exacta: `b123c5ac7e97870bbbc220ea9f0405f2001b7a21`.
- Rama: `fix/value-sanity-reachability-v1`.
- Worktree: `/Users/rodrigoortiz/Documents/secoppal-value-sanity-reachability-v1`.
- Preflight: `main` coincidía con la base y no tenía cambios versionados; sí
  tenía archivos sin seguimiento. No se borraron ni copiaron al worktree propio.
- Leídos `AGENTS.md`, `docs/orquestacion/CARRILES.md`, ADR-005 y la auditoría
  `docs/audits/entity_profile_value_quality_v1/ENTITY_PROFILE_VALUE_QUALITY_AUDIT.md`
  desde la rama `audit/entity-profile-value-quality-v1` en `6ff45a6`.
- Runtime propio: únicamente `app/core/soql_builder.py` y
  `app/core/secop_client.py`. Test nuevo: `tests/test_value_sanity_reachability.py`.
  Pin actualizado de forma acotada en `tests/test_truth_layer_public_parity.py`.
- Se notificó la edición compartida de `docs/decisions_log.md`, autorizada por
  este encargo. Sin nuevo ADR, sin merge, sin push.

## Evidencia funcional

El test no devuelve campos que el SELECT enviado al transporte no solicite.
Ejecuta `SoQLBuilder.build` → payload fuente proyectado → `SecopClient.query`
→ normalización real → `Formatter.to_rows` → detector real. También prueba
directamente la normalización, para detectar por separado el defecto de tipo.

| Caso offline | Resultado |
|---|---|
| `CO1.PCCNTR.9281738`, `CD-58-2026-6993`, EDUBAR; `344398000000 / 2108000000` | `suspect`, ratio ≈163,38; principal `$344,398,000,000`; referencia `2108000000.0` |
| `30000000 / 10692000` | Sin anomalía por ratio ≈2,81; principal `$30,000,000` |
| Facturado `0`, null, negativo, ausente o no numérico | Sin excepción ni clasificación afirmativa CLEAN |
| Facturado entero, decimal o representación monetaria ya aceptada por `_safe_money` | Misma conversión numérica que el valor principal; no se modifica el payload original |
| Frontera generada `100 × facturado + delta`, delta ∈ {-1,0,1} | Hypothesis confirma la frontera inclusiva 100 y conserva principal/referencia |

`RATIO_SUSPECT_THRESHOLD = 100` y `detect_value_anomaly` permanecen intactos.
El caso normal solo acredita ausencia de anomalía por este ratio; no certifica
calidad económica. Los no evaluables no reciben `value_quality="clean"`.

La fila suspect contiene `value_quality`, `value_warning`, `value_reference` y
conserva el valor fuente principal. WhatsApp y Telegram generan la advertencia
por tarjeta y la referencia facturada; `format_streamlit` genera la advertencia
existente de cabecera cuando el primer resultado es suspect. Se verificaron
textos producidos, sin arrancar la app ni inspeccionar una sesión de navegador.

## Consumidores y límites

Codegraph MCP `status`, `context` e `impact` se consultaron antes de editar.
CLI Codegraph 0.9.6: `codegraph status` informó índice actualizado en main
(807 archivos, 18.818 nodos, 42.109 aristas). Se confirmó que las cuatro
autoridades de runtime consultadas coincidían con la base. El índice de main
sirvió para impacto previo; no se afirmó que indexara el worktree modificado.
Los accesos dinámicos a `select_fields` se completaron con `rg` y lectura directa.

- SELECT: los 15 campos previos se conservan y se añade solo `valor_facturado`.
  Regresión de igualdad de consulta retirando esa única columna; count, WHERE,
  ORDER BY y LIMIT se mantienen.
- `direct_lookup`: referencia exacta transporta el denominador y llega a suspect;
  los tests existentes cubren también las rutas por identificador y el scope.
- Radar: búsqueda usa `SoQLBuilder`; follows usa `spec.select_fields`. Se prueba
  el fetch real con transporte falso y la conservación de clave, campos
  persistidos y hash de estado/valor. El facturado no decide identidad ni eventos.
- XLSX: conserva las nueve columnas existentes y exporta `344398000000` como
  principal. No añade una columna facturada ni de diagnóstico.
- Entity Profile: su proyección de recientes comparte `select_fields` y recibe
  el campo aditivo. No se modificó código del perfil ni agregados.
- Procesos: no se añade el campo a su SELECT ni se amplía su normalización.

Fuera del carril permanecen los 10 hallazgos Semgrep preexistentes y las causas
de agregados/rankings descritas por la auditoría de origen. Esta corrección no
adjudica esos valores ni incorpora una política de agregación nueva.

## Puertas ejecutadas

Entorno canónico `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/`, ejecutado
desde el worktree propio. Versiones: Ruff 0.16.4, Semgrep 1.176.0,
Hypothesis 6.165.10, Codegraph 0.9.6. Disponibilidad no se usa como validación.

Regresión antes del fix, salida literal final: `13 failed, 5 passed in 1.05s`.
[Log completo](red.log.gz). Commit `267b95a` congela ese rojo; commit `9a16f98`
restaura transporte y conversión.

Focal: corrí los 17 módulos listados a continuación, no la suite en esa puerta.
Después se lanzó la suite completa expresamente solicitada, en serie, tras
comprobar que no había otro pytest activo.

```text
$ .venv/bin/ruff check app/ tests/ scripts/
All checks passed!
$ .venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off --json --output /tmp/value-sanity-reachability-v1/semgrep-final.json
Ran 3 rules on 263 files: 10 findings.
$ .venv/bin/python -m pytest tests/test_value_sanity.py tests/test_value_sanity_reachability.py tests/test_formatter_no_results.py tests/test_url_repair.py tests/test_socrata_transport_classic.py tests/test_socrata_transport_soda3.py tests/test_soda3_transport_selection.py tests/test_direct_lookup.py tests/test_direct_lookup_entity_scope.py tests/test_radar_keys.py tests/test_radar_follows.py tests/test_radar_runner.py tests/test_radar_builder.py tests/test_export_xlsx.py tests/test_soql_builder.py tests/test_module_size_limits.py tests/test_truth_layer_public_parity.py -q
453 passed in 4.59s
$ make lint-core
app/core/ limpio
```

[Ruff literal](ruff.log) · [Semgrep literal completo](semgrep.log.gz) ·
[Focal literal completo](focal.log) · [lint-core literal](lint-core.log).

Semgrep global sigue rojo: 10 findings blocking antes y después, cero nuevos,
cero eliminados, cero en los dos archivos runtime propios, cero errores de
escaneo. Comparación por multiconjunto `(check_id, path, extra.message)`:
[delta](semgrep-delta.json), [base](semgrep-base.json.gz),
[final](semgrep-final.json.gz). El exit code 0 del CLI sin `--error` no significa
cero hallazgos. El scan excluye el archivo >1 MB que reporta su salida literal.

## Pin de paridad y suite completa

La primera suite completa terminó con esta salida literal:

```text
FAILED tests/test_truth_layer_public_parity.py::test_m2_modified_files_match_their_pinned_state
1 failed, 11526 passed, 11 skipped, 14 xfailed in 477.65s (0:07:57)
```

[Primera ejecución completa](full-suite-first.log.gz). No era un fallo de la
base: el nuevo SELECT cambia el hash físico del builder y el pin acreditaba
los bytes anteriores. Los otros 11 tests de paridad pasaron. El contrato vigente
`docs/orquestacion/CARRILES.md` permite repins deliberados, documentados y
acotados por pruebas. Se notificó y actualizó exclusivamente este digest:

- Previo: `0cbc0f1944914b34a195b608c1b7dfe17d2465d0b8fe31aa561d897a61da1b39`.
- Actual: `b64a3f1bd7bf22c77c6cca36bf094176af6f47b61cfebc2c4e61819f7eae7a46`.

La prueba nueva congela los 15 campos anteriores y restaura la consulta al
retirar exclusivamente `valor_facturado`. Se verificó además que, restaurando
solo ese digest en memoria, el AST del test de paridad vuelve a ser idéntico al
de la base: no se cambia ninguna aserción ni otro pin/oráculo histórico.
[Comprobación del pin y delta Semgrep](pin-check.log).

Se corrigió asimismo la ficha del testigo offline para conservar sus roles
acreditados: comprador Distrito de Barranquilla, proveedor EDUBAR S.A.; se
verifica que ambos llegan sin cambios a la tarjeta. Después se repitieron Ruff,
Semgrep y los 453 focales (incluidos los 12 de paridad), todos conformes a sus
puertas, y se relanzó la suite completa en serie sobre el código final.

Salida literal final de la segunda ejecución:

```text
$ .venv/bin/python -m pytest -q
11527 passed, 11 skipped, 14 xfailed in 458.33s (0:07:38)
```

[Suite final completa](full-suite.log.gz). Exit code 0. Se ejecutó la suite
completa sin exclusiones adicionales; los 11 skips y 14 xfails son los que
reporta pytest. El testigo de reachability se reprodujo offline; no se repitió
la auditoría viva ni se arrancó la aplicación.

`git diff --check` y `git diff --check b123c5a..HEAD`: salida vacía, código 0.
La revisión final confirma que detector, formatter y Entity Profile conservan
sus bytes respecto de la base. La rama contiene los tres commits solicitados.

Nada queda pendiente dentro del alcance de restauración. Los 10 hallazgos
Semgrep preexistentes se conservan y no constituyen delta de este carril. No se
cambiaron agregados ni se hicieron merge/push, conforme al encargo.

VERDICT: GO

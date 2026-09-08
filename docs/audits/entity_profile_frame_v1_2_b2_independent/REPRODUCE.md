# Reproducción de la revisión B.2

Candidato exacto: `5d715ba9e7d67aa05f703d2a280fdf9e2efde3b1`. Worktree de auditoría: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b2-independent`. Rama: `audit/entity-profile-frame-v1-2-b2-independent`. El commit final sólo añade este directorio; app/tests/Streamlit del candidato conservan bytes. Para reproducir los tests propios se necesita ese commit de auditoría, no sólo el candidato que aún no los contiene.

Los comandos siguientes se ejecutan desde este worktree. Intérprete canónico externo, cwd del candidato. El runner fija `PYTHONPATH` con raíz/tests/plugin, `PYTEST_ADDOPTS='-p offline_guard'`, `PYTHONDONTWRITEBYTECODE=1` y Frame OFF por defecto. Las fixtures integradas lo encienden mediante entorno/configuración reales. `tests/conftest.py` y el conftest local de auditoría redirigen feedback, SQLite, conversaciones e índice Actor a temporales. Las mutaciones/exportaciones nunca se hacen sobre este runtime ni sobre otro worktree.

Los logs `.txt.gz` y JUnit `.xml.gz` son salidas literales comprimidas sin pérdida. `literal_log_manifest.json` conserva longitud descomprimida y SHA256. Al repetir se generan nuevos `.txt` propios. La primera línea de cada log incluye el argv exacto, salvo salidas directas de helpers/documentadas aquí. El exit del runner no resume la validez de todos sus subprocesses: leer `EXIT_CODE` y XML de cada target.

## Preflight y preservación

```bash
git rev-parse --show-toplevel
git rev-parse HEAD
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py preserve
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/verify_audit.py
```

`preserve` coteja los 2.550 archivos del candidato con sus blobs Git y hashes iniciales. `verify_audit` verifica ascendencia B.1/base/probado/candidato, árboles probados→final, 2.457 archivos congelados ajenos al delta y 89 logs vinculados. También exporta la base en un temporal con raíz Git aislada para el Semgrep comparable y el contraejemplo del reloj. No construye Settings fuera de pytest.

Codegraph se probó con Node predeterminado (rechazado por la herramienta) y con el Node 22 instalado:

```bash
PATH=/opt/homebrew/opt/node@22/bin:$PATH codegraph --version
PATH=/opt/homebrew/opt/node@22/bin:$PATH codegraph status
PATH=/opt/homebrew/opt/node@22/bin:$PATH codegraph query publication_io
```

0.9.6; `Not initialized`; query no disponible. MCP ausente. No se creó un índice ni se usó el de otro checkout. Fallback explícito a rg/manual, `consumers.txt.gz`. Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10, Python 3.14.7. Disponibilidad no equivale a validación.

## Puertas y targets congelados

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py gates
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py acceptance
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py historical_current
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py regressions
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py originals tests/test_v12b_independent*.py --tb=short
```

`gates` ejecuta Ruff global app/tests/scripts, Semgrep global app con las leyes del proyecto (texto y JSON), lint-core y diff-check de base→HEAD. Semgrep no se reduce a conteos: `semgrep_delta.json` compara regla, ruta y fragmento completo contra un scan nuevo de la base. Baseline 10, actual 10, añadidos 0, retirados 0. Su exit 0 no significa cero findings.

`acceptance` reproduce el argv literal de `final_acceptance.txt.gz` del autor, redirigiendo únicamente XML, flags y salida a este directorio. Contiene los cinco tests B.1, los tres B.2 y `test_entity_profile_metric_evidence.py`.

`historical_current` usa los 70 node IDs vigentes, cuatro controles verdes y todos los ports B.1. `regressions` extrae los seis comandos pytest documentados en el REPRODUCE B: tamaño, target amplio, adyacentes, corpus, utilidad y núcleo/A2/propiedades. `originals` ejecuta los 84 históricos intactos. No hay deselect introducidos en estas corridas.

| Log | Resultado observado |
|---|---|
| `acceptance.txt.gz` | 130 passed |
| `historical_current.txt.gz` | 105 passed |
| `originals.txt.gz` | 5 failed, 79 passed; cinco montajes adjudicados |
| `regressions_4.txt.gz` | 2 passed; tamaño |
| `regressions_5.txt.gz` | 3 failed, 1902 passed; dos expectativas adjudicadas y deuda del reloj |
| `regressions_6.txt.gz` | 472 passed, 2 xfailed; Actor preexistentes |
| `regressions_7.txt.gz` | 276 passed |
| `regressions_8.txt.gz` | 15 passed |
| `regressions_9.txt.gz` | 1 failed, 407 passed; misma deuda del reloj |

Los JSON `*_flags.json` muestrean el flag antes de ejecutar el cuerpo de cada caso. No abarcan cambios posteriores dentro del test: los seis originales de overflow activan Frame dentro de su cuerpo. El archivo propio final tiene 51 muestras ON. El target amplio mezcla API de núcleo, controles OFF y pruebas integradas ON; no se afirma ON global.

## Pruebas propias y reproducción focal de los hallazgos

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py own_verified \
  docs/audits/entity_profile_frame_v1_2_b2_independent/test_publication_sequences.py \
  docs/audits/entity_profile_frame_v1_2_b2_independent/test_record_buyer.py \
  docs/audits/entity_profile_frame_v1_2_b2_independent/test_supplier_topologies.py -s
```

Salida literal final: `4 failed, 47 passed in 21.87s`. No errores de colección/setup ni timeouts. Los cuatro rojos son exactamente los dos F4 encadenados y dos de ranking. Los dos casos de PublicationRecoveryError entre los verdes son observaciones de estado/autoridad, no pruebas de seguridad.

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py focal_f4 \
  docs/audits/entity_profile_frame_v1_2_b2_independent/test_publication_sequences.py::test_rollback_of_retry_never_promotes_an_earlier_failed_turn -s
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py focal_supplier \
  docs/audits/entity_profile_frame_v1_2_b2_independent/test_supplier_topologies.py::test_ranking_claim_and_published_warning_are_consistent -s
```

Los comandos focales equivalentes están preparados para reproducir los mismos node IDs ejecutados en `own_verified`; no se cuentan como corridas adicionales ya realizadas. F4 conserva controles con prefijo publicado, lee desde otro proceso, comprueba el rollback byte a byte y publica otro chat durante el fallo. Proveedores observa las secciones reales mediante wrapper, la respuesta y el registro publicados, los conteos, las identidades y el siguiente turno. Sólo clientes/narración son sintéticos.

`PublicationRecoveryError` interrumpe la llamada, pero en el fallo de reapertura para truncar el lector nuevo admite el sobre íntegro sin barrera. Esta observación no se confunde con un resultado False ni con una garantía de seguridad. No hay simulación de pérdida eléctrica; los subprocesses prueban reinicio de proceso/visibilidad.

## Mutaciones, inventario y salida final

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/audit_runner.py mutate
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_independent/build_manifest.py
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check docs/audits/entity_profile_frame_v1_2_b2_independent
```

La campaña carga las nueve mutaciones documentadas del autor, cambia sólo OUT y fija la exportación al candidato exacto. Cada baseline se ejecuta antes de su mutante; cada archivo se restaura en finally y todos los hashes app se cotejan tras cada caso. `mutations.json` y los 18 logs control/mutante registran nueve detecciones conductuales revisadas manualmente. Ningún montaje/import/timeout cuenta como detección. La campaña histórica no se repite: conserva su adjudicación de 25 conductuales y dos de etiqueta.

`build_manifest` reconstruye resultados por node ID desde los XML, comprueba los once residuales, 45 controles y cinco fallos históricos exactos, y produce el mapa para el coordinador. La unión es inventario, nunca suma de garantías.

Antes de entregar se cotejan hashes del candidato y alcance de paths; `git diff --check af3d0c7..HEAD`, `git diff --check`, `git diff --cached --check`, `git status --short` y `git log` verifican la entrega. El HEAD final está en la respuesta final; el commit sólo incluye este directorio.

## Intentos iniciales y límites honestos

- `sequences.txt.gz`: primera reproducción F4, 2 failed/4 passed; se conserva, no se suma al resultado final.
- `own.txt.gz`: 25 failed/26 passed en el primer montaje propio. Veinte fallos eran un oráculo demasiado estricto que exigía JSONL completamente idéntico después de un rechazo; la aclaración puede añadirse sin reparar el prefijo. Se corrigió a preservación byte a byte del prefijo dañado, manteniendo rechazo y cero consultas. Otro caso exigía recuperar una tabla sin reset pese a la admisión conservadora; ahora verifica el bloqueo sin consultas y la recuperación con reset. Ninguno se convierte en hallazgo F5/F9 ni exige debilitar esas guardas.
- `own_final.txt.gz`: 4 failed/47 passed tras corregir esos oráculos. `own_verified.txt.gz` repite esos mismos resultados después de ajustar únicamente la emisión de evidencia para Ruff.
- El primer helper de verificación suponía que ambos manifiestos históricos usaban las mismas claves (`uncompressed_bytes` frente a `bytes/archive`); se corrigió el lector propio y la verificación posterior acreditó todos los logs. Un intento de importar el runner desde stdin omitió el path del helper de salida; no llegó a ejecutar pruebas. Se usó después el entrypoint de script documentado. Estos errores de scripts no son resultados del producto.
- `own_ruff.txt.gz` conserva siete T201 propios iniciales. Se sustituyó la emisión de evidencia por `sys.stdout.write` en un helper local; Ruff final está verde. Ruff global estuvo verde desde el primer scan.
- El primer diff-check staged señaló whitespace literal de las trazas JUnit; se conserva en `xml_whitespace_initial.txt.gz`. Se comprimieron los XML byte a byte, sin limpiar ni editar sus fallos, y el diff-check final pasó.
- Los tiempos dependen de la máquina y del día. El rojo temporal del 8 de septiembre se reproduce en base y candidato; no se oculta detrás del total antiguo del autor.

**Corrí estos targets, no la suite.** No app ni SECOP/LLM live, datos del usuario, mensajes o radares reales. Sin merge, push, reconciliación efectiva o activación. Fuera del carril quedan Semgrep baseline, xfails Actor y el montaje histórico del reloj. Los dos hallazgos nuevos requieren remediación separada antes de una nueva revisión de cierre y del gate del coordinador.

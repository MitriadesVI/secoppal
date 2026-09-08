# Reproducción offline B.3

Ejecutar desde el worktree B.3. Entorno canónico:
`/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.
`run_checks.py` carga el runner histórico sin modificarlo y redirige OUT a B.3.
Añade al PYTHONPATH raíz/tests/plugin/helper histórico/helper propio;
PYTEST_ADDOPTS carga `offline_guard`, Frame OFF en entorno y las fixtures
integradas lo activan por caso. `conftest.py` local redirige feedback JSONL,
SQLite, conversaciones e índice Actor a tmp_path. Los flags guardados se miden
al entrar a cada test, no son una afirmación global de Frame ON.

No ejecutar los runners históricos directamente: escribirían su evidencia.
Los comandos exactos y tiempos de cada corrida están dentro de sus `.txt.gz`;
JUnit `.xml.gz` conserva node IDs y trazas literales. La compresión es reversible,
sin limpiar trazas ni whitespace. `literal_log_manifest.json` registra bytes
originales y SHA256 para comprobarlo.

## Reproducción inicial y originales intactos

Antes de editar se ejecutó, como target `baseline`:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/run_checks.py baseline \
  docs/audits/entity_profile_frame_v1_2_b2_independent/test_publication_sequences.py \
  docs/audits/entity_profile_frame_v1_2_b2_independent/test_record_buyer.py \
  docs/audits/entity_profile_frame_v1_2_b2_independent/test_supplier_topologies.py -s
```

La repetición después se llama `after_originals` y usa los mismos paths. Ambas
corridas producen cuatro rojos, con causas diferentes documentadas en REMEDIATION.
No se equiparan esos contadores ni se modifican los originales para ocultarlos.

## Aceptación B.3 y controles

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/run_checks.py own_verified \
  docs/audits/entity_profile_frame_v1_2_b3_remediation/test_publication_ports.py \
  docs/audits/entity_profile_frame_v1_2_b3_remediation/test_publication_chains.py \
  docs/audits/entity_profile_frame_v1_2_b3_remediation/test_supplier_ports.py \
  docs/audits/entity_profile_frame_v1_2_b3_remediation/test_ranking_matrix.py -s --tb=short
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/run_checks.py final_ports \
  docs/audits/entity_profile_frame_v1_2_b3_remediation/test_publication_ports.py -s --tb=short
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/run_checks.py acceptance
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/run_checks.py historical_current
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/run_checks.py regressions
```

`acceptance` reproduce los 130 node IDs B.1/B.2/metric_evidence congelados.
`historical_current` reproduce los 70 vigentes, cuatro controles y ports B.1.
`regressions` reproduce los seis comandos dirigidos del REPRODUCE B, expandiendo
sus globs: tamaño, amplio, adyacentes, corpus, utilidad y núcleo/A2/propiedades.
No invoca pytest global. Los tres rojos amplios se conservan sin deselect;
la deuda del reloj aparece también en núcleo y se reproduce en la base exportada.
No se suman targets superpuestos ni se cuentan observaciones como garantías.

## Puertas, mutaciones y preservación

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/run_checks.py gates
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check docs/audits/entity_profile_frame_v1_2_b3_remediation
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/verify.py base
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/mutate.py
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/verify.py preserve
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/build_manifest.py
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/verify.py archive
git diff --check a7e12ebf037f962d356bb34130e180e97f97aa0e..HEAD
git diff --check
git diff --cached --check
```

Las mutaciones exportan el HEAD al entrar al script; nunca escriben este runtime.
Cada control debe estar verde antes de aplicar su mutante, y se restauran y
comprueban todos los hashes app tras cada cambio. El runner acepta uno o varios
nombres para repetir sólo esos mutantes, conservando el resto del manifiesto.
Después de ajustar el oráculo del port se ejecutó:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b3_remediation/mutate.py B3-lost-prefix-nonconfirmation
```

`verify.py base` exporta la auditoría A7, crea una raíz Git temporal para el scan
Semgrep comparable y ejecuta exclusivamente el contraejemplo histórico del reloj.
Compara regla/ruta/fragmento, no sólo conteos. `preserve` usa cat-file --batch
para comparar cada blob congelado fuera de las tres modificaciones autorizadas;
comprueba el alcance de archivos nuevos y los hashes originales B.2.
`build_manifest` sólo propone mappings, no edita tests ni configura colección.

## Iteraciones iniciales y adjudicación

- `new_first`: 4 failed/247 passed. Los cuatro errores del montaje exigían
  llamadas al paginar una página que ya estaba en buffer. Se mantiene entrega
  de filas distintas, dataset contractual, export y ausencia del universo fallido.
  La prueba nueva se corrigió para permitir el buffer válido; ningún runtime
  de paginación cambió.
- `ranking_first`: 28 failed/42 passed. Veintiséis casos esperaban que el NIT
  600123456 conservara su dígito verificador dentro de la base; se fijó el valor
  esperado 60012345 del normalizador existente, sin tocar identidad. Dos casos
  de fallo total exigían mensajes de cada consulta; la respuesta real ya dice
  que ninguna fuente respondió y que no equivale a ausencia. Se acreditan
  ambos estados de consulta y ese mensaje. No se alteró runtime para ellos.
- Primer diff-check staged: detectó una línea vacía extra al final del helper
  port. Se retiró antes del commit, sin cambio de comportamiento.
- `initial-B3-lost-prefix-nonconfirmation-*`: control verde, mutante detenido
  antes por igualdad física del helper. No se cuenta como detección conductual.
  Se mejoró el port para conservar los bytes y dejar la decisión funcional al
  test histórico; su repetición falla en autoridad admitida, con consultas de B.
- Primera ejecución de `build_manifest` trató residual_acceptance como lista de
  strings, pero el manifiesto histórico contiene objetos con nodeid. Se corrigió
  el lector propio; no se ejecutaron tests ni cambió el producto en ese intento.

No hay Settings fuera de pytest ni fuentes/narración live. Los procesos nuevos
leen sólo almacenamiento temporal. Frame permanece OFF en configuración.

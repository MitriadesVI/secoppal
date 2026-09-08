# Resultado y reproducción B.2

**V1_2_B2_READY_FOR_INDEPENDENT_REVIEW**. F4/F5/F9 residual y overflow de
proveedores corregidos dentro del alcance. Los once originales de aceptación
están verdes, con controles positivos conservados. No es GO del coordinador ni
PRODUCTION_GO. Frame OFF por defecto, sin merge/push.

HEAD probado: `aaa9168b75a5633469af507a1cf7c1c2e2c1ce00`.
El HEAD final es el commit de evidencia identificado en la respuesta final del
carril (también `git rev-parse HEAD` en esta rama). El delta probado→final queda
limitado a este directorio documental: app/tests idénticos, comprobados mediante
los árboles Git de HEADS.json y `git diff`.

Las tres puertas, salida literal:

```text
All checks passed!
Ran 3 rules on 325 files: 10 findings.
130 passed in 41.60s
```

Ruff global app/tests/scripts; Semgrep global app, baseline rojo 10 y delta 0
por regla/ruta/fragmento (semgrep_delta.json); pytest dirigido con plugin offline.
La salida íntegra, incluidos comandos, está preservada en gzip sin pérdida;
literal_log_manifest.json acredita bytes descomprimidos y SHA256. Ruff adicional
sobre scripts de evidencia también verde. lint-core: `app/core/ limpio`.
Diff-check del intervalo BASE→código y del staged de evidencia: sin salida,
exit 0. No se modificaron guardas de tamaño ni sus límites.

| Conjunto | Salida literal | Log |
|---|---|---|
| Aceptación residual original + B.2 + consumidor monetario | `130 passed in 41.60s` | [final_acceptance.txt.gz](final_acceptance.txt.gz) |
| Históricos vigentes + cuatro controles verdes + ports B.1 | `105 passed in 30.40s` | [historical_current.txt.gz](historical_current.txt.gz) |
| Target amplio intacto | `2 failed, 1903 passed in 138.48s (0:02:18)` | [final_main_5.txt.gz](final_main_5.txt.gz) |
| Adyacentes finales | `472 passed, 2 xfailed in 13.94s` | [final_adjacent.txt.gz](final_adjacent.txt.gz) |
| Corpus dirigido (también incluido en target amplio final) | `276 passed in 1.16s` | [regression_7.txt.gz](regression_7.txt.gz) |
| Utilidad dirigida (también incluida en target amplio final) | `15 passed in 4.06s` | [regression_8.txt.gz](regression_8.txt.gz) |
| Núcleo/A2/propiedades (también incluido en target amplio final) | `408 passed in 2.03s` | [regression_9.txt.gz](regression_9.txt.gz) |
| Guardas de tamaño (también incluidas en adyacentes finales) | `2 passed in 0.17s` | [regression_4.txt.gz](regression_4.txt.gz) |
| Ejemplos de respuesta y registro de proveedores | `2 passed in 1.64s` | [supplier_examples.txt.gz](supplier_examples.txt.gz) |

Los conjuntos se superponen: no sumarlos. **Corrí estos targets, no la suite**.
El target amplio sigue ejecutando los dos originales con expectativas
contractuales reemplazadas; son exactamente:

- `tests/test_entity_profile_frame_b_publication.py::test_a_record_from_another_catalog_does_not_govern_the_next_turn`.
- `tests/test_entity_profile_frame_b_semantics.py::test_a_forged_execution_record_does_not_change_the_next_turn`.

Exigen continuar después de corrupción, mientras la adjudicación F5 obliga a
rechazar antes de consultas. No se tocaron ni se añadieron deselect para hacerlos
pasar. Sus reemplazos/controles siguen verdes en la aceptación independiente.
El único rojo nuevo del primer target amplio fue la frase de monto ausente;
se corrigió preservando «valor no disponible» y el control original pasa en la
aceptación final y el target amplio final. No hay regresiones nuevas ni xfails
nuevos en estos targets; los dos xfails Actor adyacentes son el baseline intacto.

## Mutaciones conductuales

Nueve baselines verdes y nueve retiradas detectadas por aserciones de producto,
con todos los archivos de app restaurados en la exportación temporal después
de cada mutación. HEAD y cada anchor/reemplazo/node ID en mutations.json.

| Mutación | Producto observado |
|---|---|
| F4-partial-recovery | Publicación declarada exitosa no recarga si se retira el separador de cola parcial |
| F4-confirmation-recovery | Turno fallido vuelve a gobernar al retirar el rearme después de unlink visible |
| F4-reader-fence | JSON de turno declarado no publicado adquiere autoridad tras fsync/write fallidos |
| F5-multiplicity | Copia de observación autoriza consultas; propiedad de unicidad falla |
| F9-buyer-consumers | Assembler/renderer mezclan Galapa con hechos y SQL de Baranoa |
| F9-buyer-snapshot | Snapshot de comprador alterado, con digest recalculado, admite consultas |
| supplier-unsafe-float | Infinity/1e999 abortan la respuesta y pierden hechos |
| supplier-false-record-accreditation | 1e999 sigue contado como métrica válida en el registro |
| supplier-value-ranking | Render afirma principal por valor con métricas no acreditadas |

No hay import errors, setup errors ni timeouts contados como detecciones.
`mutation_initial/` conserva la primera campaña: tres salidas conductuales se
clasificaron REVIEW por no reconocer `E assert` / `DID NOT RAISE`. Se comprobó
su aserción, se corrigió el clasificador y se repitió toda la campaña sobre el
HEAD final de código, incluyendo la nueva reparación de cola parcial.
La adjudicación congelada anterior sigue siendo **25 detecciones conductuales y
dos diferencias de route_reason**. No se volvió a presentar como 27 garantías
ni se sumó a esta campaña. No hubo mutantes neutralizados por otra defensa en
la campaña nueva; ese límite anterior permanece documentado.

## Reproducción

Desde este worktree, con el intérprete canónico:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_remediation/run_checks.py final_main
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_remediation/run_checks.py historical_current
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_remediation/mutate.py
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_b2_remediation/verify.py
```

El comando exacto de aceptación está en la primera línea descomprimida de
final_acceptance.txt.gz; ejecuta los cinco independientes, los tres B.2 y el
consumidor test_entity_profile_metric_evidence.py. `run_checks.py regressions`
reproduce todos los targets dirigidos congelados, nunca pytest global.
Los logs se leen con `gzip -dc <ruta>.txt.gz`. El plugin offline_guard se
inyecta por PYTHONPATH/PYTEST_ADDOPTS en los runners. Todas las conversaciones,
Settings y efectos sintéticos usan temporales de pytest. No SECOP/LLM live.

Los outputs antes/después originales, el primer error de montaje offline y los
seis errores del oráculo propio de paginación quedan preservados; no se cuentan
como comportamiento corregido del producto. La paginación válida puede usar el
buffer sin llamadas nuevas, por lo que el oráculo final exige avance de filas y
el dataset correcto. La inyección real de publicación no se debilitó.

## Pendiente y preservación

2.457 archivos congelados preservados por hash; los diez findings Semgrep
ajenos, los dos xfails Actor, originales, pins/guardas y documentos compartidos
siguen intactos. Diez rutas runtime en el delta (ocho modificaciones y dos
extracciones pequeñas); tres archivos nuevos de tests. No parser/QueryRouter,
query_plan, transporte, identidad, territorio, gramáticas monetarias, catálogos,
ranking general, dependencias ni activación.

No quedan pendientes de implementación dentro de los cuatro bloqueos. Quedan
la revisión independiente, la reconciliación activa definitiva propuesta en
test_reconciliation_proposed.json y el gate global del coordinador. El límite
físico de publicación y el rechazo conservador de snapshots antiguos sin vínculo
de comprador se declaran en REMEDIATION.md; no se promete rollback con un binario
antiguo ni autenticidad contra reescritura coherente del almacén.

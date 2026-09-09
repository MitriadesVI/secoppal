# Contract Universe V1.1 — Integration R4

**STATE_UNIVERSE_METADATA_COMPATIBILITY_ADJUDICATED**

Los diez nodeids C10–C13 quedan cerrados: seis oráculos obsoletos de presentación
y cuatro regresiones de provenance/traza. Base:
`e23753cc6f37401ac1e9b1a5f9d89809c3e4daf6`. Rama:
`codex/contract-universe-v1-1-r4-state-metadata`. No merge, push, deploy o restart.

C10 ya mostraba «Contratos celebrados según las señales publicadas en SECOP.»
en los seis perfiles completos. P17 examinaba solo las líneas de filtros y
confundía el predicado de universo con estado explícito. Ahora verifica el label
y examina los filtros restantes después de separar el predicado de universo.
Se prohíbe inventar Estado actual cuando no queda filtro explícito. Los seis
SQL observados son idénticos antes y después; render productivo no se modifica.

C11/C13 conservan metadata compatible de estado usando el catálogo existente,
sin alterar `estado_contrato` ni su WHERE. Las familias que no equivalen al
filtro exacto no se conservan. El consumidor de recuperación de refinamientos
tras reload prueba utilidad productiva, además de QueryFrame y migración.
C12 conserva el universo en el merge, pero separa `derived_fields` de intención
aceptada; un source_records explícito sí se registra como decisión contractual.

## Evidencia y adjudicación previa

`C10_C13_MATRIX.json` contiene un registro por nodeid, propiedad, consumidor,
veredicto y evidencia. Se escribió la adjudicación inicial antes de editar P17.
`before_10.txt`: **10 failed in 1.94s**. El `main` histórico
`c52d7eda355f44278164d4cb9803a199eec83486` se reejecutó en un snapshot aislado de
Git: **10 passed in 3.05s**, no se reutilizó solo el resultado histórico R1.
`after_10.txt`: **10 passed in 1.26s**.

`base_full_pipeline.jsonl`, `candidate_full_pipeline.jsonl` y
`main_full_pipeline.jsonl` guardan parse, consultas del builder, plan de perfil,
render y traza donde el test los recorre. Las pruebas que no compilan SQL no
simulan haberlo ejecutado. `base_observations.jsonl` conserva también locales
de las fallas originales. Los controles de estado guardan after_parse,
after_plan, SQL diagnóstico y params recargados; `SQL_PARITY.json` prueba nueve
comparaciones byte a byte. Las observaciones de workflow distinguen SQL real
del ledger frente a compilación diagnóstica.

## Validación focal, sin sumar suites solapadas

| Grupo | Resultado literal / evidencia |
|---|---|
| Diez originales | `10 passed in 1.26s` — after_10.txt |
| Auditor Contract Universe | `19 passed in 3.49s` |
| F2.2 | `34 passed in 12.00s` |
| Frozen | `43 passed in 5.45s` |
| Corpus | `Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)` |
| Buyer–Actor C05–C08 | `44 passed in 9.47s` |
| R3 SQL oracles | `34 passed in 1.76s` |
| Consumidores estado | `181 passed in 10.98s` |
| Controles R4 finales | `16 passed in 2.30s` — r4_controls_final_v2.txt |
| Size | `2 passed in 0.11s` |

Corrí esos grupos focales, **no la suite**. `state_consumers.json` conserva el
comando exacto: followup_mutations, query_frame, conversation_store,
contextual_suggestions, invariants_characterization, firmado_signed_semantics,
entity_profile_frame_migration, entity_profile_frame_b_integration y
entity_profile_remediation_properties. Frozen incluye Frame OFF/ON; controles
adicionales repiten la composición activa bloqueada en ambos flags. Hypothesis
usa 40 ejemplos deterministas para familia incompatible, sin I/O externo.

Puertas literales, archivos completos adjuntos:

```text
All checks passed!
Ran 3 rules on 333 files: 10 findings.
app/core/ limpio
2 passed in 0.11s
```

Semgrep sigue rojo semánticamente: **10 heredados, 0 nuevos**. Que el CLI salga
con código 0 no significa cero findings. Todos sus archivos con findings son
idénticos a e23753c; los cuatro módulos propios figuran entre los escaneados y
ninguno tiene hallazgos. Ruff y Semgrep finales incluyen los módulos nuevos
staged. Codegraph no tiene índice: fallback explícito documentado en el mapa.
Diff-check se conserva al cierre en `diff_check.txt`/JSON.

## Mutaciones

M1 añade Estado actual al render sin filtro: falla el control visible.
M2 borra metadata de Cerrado: fallan consumidor de reload y C13.
M3 retiene familia incompatible: Hypothesis encuentra contradicción.
M4 vuelve a atribuir universo/version a accepted_fields: fallan C12 y controles.
Las cuatro tienen control positivo previo verde, salidas y JUnit. No se cuentan
imports ni timeouts. La primera evaluación del harness M2 rechazó contabilizar
KeyError de claves ausentes; se conserva como `M2_initial.*`. Se reforzó el
control nuevo con `.get` para observar AssertionError, y se repitió con éxito.

## Replay y límites que no se corrigen

`RESIDUAL_20_AFTER_R4.json`: **before=20, now_pass=10, still_fail=10,
errors=0, skipped=0**. Permanecen C01, C02, C03, C04, C24, C25, C26. Sus tests y
runtime específicos no se modifican.

Hay dos límites de estado heredados medidos durante las contrapruebas:

1. «contratos firmados en ejecución de mantenimiento» ya aclara y emite cero
   SQL productivo, en ambos flags. La ruta «firmados que estén en ejecución»
   conserva los tres miembros. No se amplía el adaptador escalar de composición.
2. «y los suspendidos» ya se rebasa a `[Cancelado, Suspendido]` en el helper de
   followup. R4 no arregla esa superficie adicional. El reemplazo explícito
   «y los contratos suspendidos» es exacto y se verifica junto con terminado.

Los intentos iniciales de controles que exigían soporte inexistente quedan en
`base_new_controls.*`, `r4_controls_final.*` y
`new_controls_before_followup_adjudication.py.txt`; la evidencia base prueba
que son límites heredados, no rojos ocultados ni tests originales repineados.
Su alcance preciso consta en `STATE_METADATA_CONTRACT.md`. Este veredicto
cierra los diez residuales asignados, no acredita soporte general de toda
composición de estados ni una puerta global de integración.

## Reproducción

En este worktree, el entorno `.venv` apunta al entorno canónico. El runner
conservado en `runner.py.txt` registra comando, cwd, exit code, duración,
stdout/stderr y JUnit. Usa `BURR_TELEMETRY_ENABLED=false` y Node 22 para preflight.
Los nodeids exactos están en `nodeids_10.json` y `nodeids_20.json`. Las puertas
auditor/F2.2/frozen conservan los plugins offline y SQL dates del carril previo.
No ejecutar la suite global para reproducir este lote.

`cu_r4_capture_runs.py.txt`, `cu_r4_observe.py.txt`, `cu_r4_historical.py.txt`,
`cu_r4_mutations.py.txt` y `cu_r4_report.py.txt` conservan los procedimientos de
captura y adjudicación. Las sustituciones temporales se restauran en finally;
ningún otro worktree se modifica. La evidencia generada por auditor/F2.2 se
copia a este directorio y los archivos históricos se restauran byte a byte.

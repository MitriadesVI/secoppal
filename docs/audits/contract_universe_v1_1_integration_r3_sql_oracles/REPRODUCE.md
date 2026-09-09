# Reproducción R3

Usar este worktree con el Python canónico de SECOPPAL. Los comandos exactos están
junto a cada salida en los JSON; los nodeids congelados en nodeids_34.json y
nodeids_54.json. Activar BURR_TELEMETRY_ENABLED=false. No ejecutar suite global.

Los scripts cu_r3_*.py.txt son copias exactas del tooling de auditoría (no runtime).
Copiarlos a /tmp con nombre .py; cu_r3_run.py declara W/E/P y run/pytest. Adaptar
W sólo si cambia el worktree. Usar el adaptador explícito
-p tests.contract_universe_sql_dates para los replays heredados.

Para el control main: git archive c52d7ed en una carpeta desechable
/tmp/cu-r3-main. cu_r3_fingerprints.py usa cu_r3_capture.py como plugin para
capturar params reales dentro de pytest (evita Settings ad hoc fuera de pytest).
cu_r3_seeded_sql.py fija PYTHONHASHSEED=0 y usa cu_r3_sql_capture.py. Si se repite,
retirar sólo los logs recién generados de la ejecución propia para no concatenarlos.

No ejecutar cu_r3_edit.py sobre los tests ya adjudicados: documenta la transición
original y exige encontrar las expectativas anteriores. Para reproducir el antes,
extraer cb02e08 a otro directorio desechable y ejecutar los nodeids_34.

cu_r3_validate.py repite auditor/frozen/corpus/R2B/replay54/módulos acotados y
estáticos; preserva los bytes históricos de outputs fijos de tests heredados.
cu_r3_mutations.py crea copias desechables desde cb02e08, superpone los tests del
carril y ejecuta cada mutación por separado. Sus siete JUnit acreditan kills.
cu_r3_report.py verifica evidencia SQL, resultados por nodeid y manifiesto runtime.

Al reproducir sobre un commit cerrado, el overlay de cu_r3_mutations.py debe
obtener archivos con git diff --name-only cb02e08, en lugar de sólo el diff sin
commit; se conservan también los dos archivos nuevos explícitos del script.

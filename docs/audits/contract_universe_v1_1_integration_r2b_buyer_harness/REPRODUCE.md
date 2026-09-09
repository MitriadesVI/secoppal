# Reproducir R2B

Los comandos exactos, cwd, stdout/stderr, duración y exit code están en cada JSON.
Usar un worktree propio con el candidato; Python es el de .venv canónico manteniendo
cwd del candidato. No construir Settings fuera de pytest ni arrancar la app.

Para C05–C08 y replay 98, cargar nodeids_44.json / nodeids_98.json como lista de
argumentos, preservando los escapes literales de los parámetros, y ejecutar:

```text
python -m pytest -p tests.contract_universe_sql_dates <nodeids exactos> -q --tb=short
```

Las contrapruebas son tests/test_buyer_actor_harness_compatibility.py. El plugin
de fechas es infraestructura de test preexistente; no desactiva Contract Universe.

Los runners cu_r2b_*.py.txt se pueden copiar a un directorio temporal como .py.
Ajustar W/P a las rutas locales en cu_r2b_run.py. Ejecutar primero cu_r2b_run.py,
luego cu_r2b_mutations.py y cu_r2b_focal.py. Las mutaciones crean copias desechables,
no modifican el candidato. mutation_edits.json registra los reemplazos exactos.
Focal preserva las trazas generadas dentro de R2B y restituye la evidencia histórica
que esos tests escriben por defecto, exclusivamente en el worktree propio.

No suite global. No usar los 44 verdes como autorización de merge ni reparar
los 54 residuales fuera del alcance asignado.

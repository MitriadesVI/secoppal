# Oráculos congelados V1.1

Autorización: instrucción explícita del usuario y adjudicación independiente en
`../contract_universe_v1_1_independent/FROZEN_TEST_ADJUDICATION.md`.

Se conserva el fixture, filas, helpers y todas las aserciones semánticas.
Únicamente cambian los números adjudicados y el comentario de Aprobado:
24 → 22 en búsqueda y SUM/COUNT, 26 → 24 en Actor, Radar 24/23 → 22/21.
El control source_records conserva 94 y su suma original.
`oracle_integrity.json` verifica igualdad AST salvo constantes enteras.
`frozen_oracle_patch.json` permite revisar cada modificación exacta.

`frozen_before.json` conserva stdout/stderr literal y los diez nodeids fallidos.
`focal_static_literal.json` y `frozen_after.txt` conservan todos los nodeids posteriores.
Solo cambia el sufijo parametrizado `celebrated-24` a `celebrated-22`.
Se utiliza el plugin de dialecto SQL `tests.contract_universe_sql_dates`,
que registra funciones temporales SQLite; no se carga `tests.cu_v1_policy_shim`.
No hay xfail, skip ni deselect añadidos.

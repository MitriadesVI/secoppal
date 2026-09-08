# Reconciliación y gate del coordinador

Rama: `integration/entity-profile-frame-v1-2-coordinator-gate`.
Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-coordinator-gate`.
Base: `616826231d5d2bf25c47640e5dcf4f1bbc491326`.
Runtime auditado: `4edc248e60610efdbeebb3cb8010bc7b59445cad`.

Desde la raíz del worktree, el runner usa el intérprete canónico y comandos
explícitos, ejecutados en serie. No requiere activar un entorno ni copiar .env.

```sh
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_coordinator_gate/run_gate.py full
```

Se permite una ejecución `full`, que rechaza sobrescribir su evidencia. Para
reproducir posteriormente, exportar el GATE_CODE_HEAD registrado en
`full_refs.json` a otro worktree aislado; sus resultados todavía no existían en
ese commit. No borrar resultados de una corrida fallida para repetirla.

`collect` ejecuta colección global y tres bloques complementarios. `focal`
recibe targets, y `--run-id` distingue diagnósticos sin sobrescribir resultados.
Los logs contienen el comando literal, salida y código de retorno; los JSON
por componente contienen colección, resultado por fase, skips/xfails, flags
antes/después del cuerpo de cada test y conexiones HTTP reales observadas.
El flag se observa después de fixtures; tests que lo cambian dentro de su
cuerpo conservan ese control. No se impone OFF sobre las fixtures.

Los bloques docs usan las copias activas de B.2 y B.3, sus conftest originales,
PYTHONPATH explícito y offline_guard. Ningún directorio histórico se colecta.
Las dependencias tienen nombres distintos entre los tres directorios salvo
conftest, cargados por pytest según la ruta de cada bloque. Las pruebas con
subprocesos conservan su PYTHONPATH y persistencia temporal heredada.

La suite global inicia con Frame OFF. Sus fixtures pueden activarlo. Los
transportes HTTP reales sólo permiten lecturas públicas de datos.gov.co,
incluido POST de consultas SODA3; se serializan con un lock. Otros destinos y
operaciones fallan explícitamente. No se copian credenciales del usuario. Los
transportes sintéticos de las pruebas permanecen intactos. `offline_guard`
bloquea sockets IP en la aceptación Frame, además de la guarda HTTP.

Toda persistencia por defecto se redirige antes de colección a un directorio
temporal. Los conftest establecen aislamiento por test. Los JSON de límites
se escriben en COORDINATOR_OUTPUT; el cambio de destino no altera el oráculo
ni parchea Path/open. El corpus adicional sólo usa parsing y follow-up puro.

Semgrep se ejecuta sobre exportación Git de BASE y sobre el candidato con el
mismo binario/reglas. La evaluación compara multiconjuntos de regla, ruta y
fragmento, registra desplazamientos de línea y exige escaneo completo, sin
errores ni hallazgos nuevos. Findings heredados se registran como baseline rojo.

Los tamaños de módulos están en `tests/test_module_size_limits.py`, incluido
en el bloque global. Los corpus semánticos, utilidad, propiedades, F1–F10,
residuales, Actor/Radar/lookup/paginación y regresiones B están en tests/;
`collection_manifest.json` acredita su colección. `scripts/run_query_corpus.py`
es el componente adicional E; las mutaciones históricas permanecen como
evidencia independiente congelada B.3, no se modifican ni se atribuyen nuevas
corridas de mutaciones a este gate de reconciliación.

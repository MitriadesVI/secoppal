# AGENTS.md — SECOPPAL

Bootstrap para agentes que trabajan en este repositorio. La política multiagente
canónica es [`docs/orquestacion/OLYMPUS_POLICY.md`](docs/orquestacion/OLYMPUS_POLICY.md).
Léela para tareas `COMPLEX` o `HIGH_RISK` y cuando haya más de un actor. La
misión, el worktree, el ownership, el scope y el candidate vigentes delimitan
cada paquete; este archivo conserva sólo leyes duras del producto y del repo.

## Leyes duras

### Nunca levantes la app desde un worktree

`dev.sh` tiene `PROJECT_DIR` hardcodeado al checkout principal y hace `pkill`
global de `uvicorn` y `streamlit`. Ejecutar `./dev.sh`, `make dev` o
`make restart` desde un worktree mata procesos ajenos y escribe en el `data/`
de `main`. Está prohibido. El orquestador controla cualquier ejecución local.

### Redirige toda persistencia fuera de pytest

Toda construcción de `Settings()` fuera de pytest, incluidos snippets, debe
redirigir `secop_feedback_path` / `feedback_db_path`. De lo contrario escribe en
datos reales. `.semgrep/secoppal-laws.yml` lo detecta, pero el agente sigue
siendo responsable.

### Git y trabajo concurrente

- Trabaja en el worktree y branch asignados; confirma el repo root antes de
  escribir.
- Reclama ownership de los paths antes de editarlos. Un solo writer puede ser
  dueño de archivos solapados.
- La suciedad preexistente pertenece al usuario: nunca stash, reset, clean,
  checkout destructivo ni sobrescritura.
- Revisa el estado y el log antes de cerrar. No hagas merge, push, deploy o
  restart sin autorización explícita.
- Un hallazgo fuera de scope se reporta; no autoriza ampliar el paquete.

### `NEXT_TOUCH_RULE`

`tests/test_module_size_limits.py` impone topes de LoC. Si un cambio necesita
tocar un módulo que alcanzó su tope, primero extrae una responsabilidad a un
módulo pequeño y después realiza el cambio funcional. El tope no se sube.

### No monolitos

Prefiere módulos pequeños de una responsabilidad con el enganche mínimo. No
apiles funciones nuevas en archivos ya cargados.

## Verificación

Nunca declares que algo pasa sólo porque leíste el código. Los gates dependen
del destino y blast radius, según la política canónica.

- Si cambia Python, ejecuta Ruff sobre el scope afectado y los tests focales.
- Si cambia runtime, scripts o tests, ejecuta Semgrep con
  `.semgrep/secoppal-laws.yml` y separa baseline de delta.
- Usa Hypothesis cuando cambien parsers, resolvers, normalizadores, aritmética
  de rangos, máquinas de estado u otra lógica combinatoria con invariantes.
- Una entrega documental usa comprobaciones estructurales; no fabrica tests.
- `make lint-core` sólo busca archivos prohibidos: no sustituye a Ruff.
- No corras la suite completa desde un worker. El coordinador decide una única
  corrida global antes de integrar cuando el destino la requiera.

El reporte siempre enumera los comandos y dice literalmente: “corrí X, no la
suite” cuando no ejecutó la suite completa. Baselines rojos conocidos no se
ocultan ni se arreglan fuera de scope; se informa `BASELINE`, `CANDIDATE` y
`DELTA`.

## Herramientas y ADR

Descubre disponibilidad y operaciones antes de confiar en herramientas
auxiliares. Para navegación estructural usa Graft o tgrep sólo cuando estén
disponibles y sean adecuados; los fallbacks son `rg`, `git grep`, lectura focal
y `git log`/`git blame`. Semgrep verifica leyes del repo, Hypothesis invariantes
y pytest comportamiento ejecutado.

El número de un ADR sale del máximo global. Ningún worker asigna números en
paralelo: HERA serializa la asignación final, salvo que exista y se use un
mecanismo mecánico de reserva. La bitácora canónica es
`docs/decisions_log.md`; los ADR viven en `docs/decisions/`.

## Entrega

Entrega commits revisables en la rama asignada y reporta: tesis del cambio;
diff por archivo; gates literales y lo no ejecutado; findings fuera de scope;
limitaciones, riesgos y acciones aún no autorizadas. Candidate freeze,
auditoría y promoción son decisiones separadas.

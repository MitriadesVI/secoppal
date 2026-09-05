# AGENTS.md — SECOPPAL

Contrato para agentes que trabajan en este repositorio. Codex lee este archivo;
`.claude/CLAUDE.md` contiene el mismo criterio para Claude Code. Si los dos se
contradicen, gana este archivo y se reporta la contradicción.

Trabajas en **un carril**, dentro de **un worktree propio**. No eres el único
agente activo. Todo lo que sigue existe porque dos agentes en paralelo se pisan
de formas que no fallan ruidosamente.

---

## 1. Leyes duras (violarlas corrompe datos o tumba al otro agente)

### 1.1 Nunca levantes la app desde tu worktree

`dev.sh` tiene `PROJECT_DIR` hardcodeado al worktree principal
(`/Users/rodrigoortiz/Documents/secoppal`) y hace `pkill` global de `uvicorn` y
`streamlit`. Si corres `./dev.sh`, `make dev` o `make restart` desde tu worktree:

- matas los procesos del otro agente, y
- escribes en el `data/` de `main`, no en el tuyo.

**Prohibido.** Si necesitas ver la app corriendo, pídelo al orquestador.

### 1.2 Toda construcción de `Settings()` fuera de pytest redirige persistencia

Incluye snippets ad hoc de un solo uso. Sin redirigir `secop_feedback_path` /
`feedback_db_path`, escribes en los `data/` reales del usuario. Esto ya está
codificado como ley en `.semgrep/secoppal-laws.yml` — la puerta lo detecta, pero
no dependas de la puerta: es tu responsabilidad.

### 1.3 Git: no salgas de tu worktree

- Nunca `git stash`, `git reset`, `git checkout` sobre otro worktree.
- Verifica `git rev-parse --show-toplevel` antes de cualquier comando que escriba.
- Otro agente puede haber commiteado en el repo mientras trabajabas: revisa
  `git log` antes de declarar cerrado.
- "Eso ya estaba roto antes" se **prueba** con `git stash` local o `git log`, no
  se asume.

### 1.4 `NEXT_TOUCH_RULE`

`tests/test_module_size_limits.py` impone topes de LoC. `app/core/query_plan.py`
está a **299/300**. Si tu cambio necesita tocarlo, primero **extraes una
responsabilidad a un módulo nuevo**, y después haces el cambio funcional. El
tope no se sube. Nunca.

### 1.5 No monolitos

Módulo pequeño de una responsabilidad + enganche mínimo. No apiles funciones
nuevas en archivos ya cargados.

---

## 2. La puerta de verificación

**Nunca declares que algo pasa porque leíste el código.** El orden es:

```bash
.venv/bin/ruff check app/ tests/ scripts/                        # debe decir "All checks passed!"
.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
.venv/bin/python -m pytest tests/<los de tu carril> -q           # SOLO los tuyos
```

**No corras la suite completa.** Son ~11.161 tests y hacen red real contra
SECOP; dos agentes corriéndola en paralelo se rate-limitean y producen rojos
falsos. La suite entera la corre **el orquestador**, una sola vez, antes de
fusionar. Tú corres `ruff`, `semgrep` y los tests de tu carril.

Dos advertencias que vienen de fallos reales aquí:

- **`make lint-core` NO revisa estilo.** Solo busca archivos prohibidos (`.bak`,
  copias, binarios) en `app/core/`. Que pase no dice nada. El linter es `ruff`.
- **Correr los tests de tu carril no es "validar" el repo.** Un refactor rompe
  importadores que no tocaste. Por eso **declara siempre qué corriste**, con
  estas palabras: *"corrí X, no la suite"*. El orquestador asume que no la
  corriste; lo que no puede asumir es qué sí.

**Estado real de las puertas al 2026-09-05:** `ruff` verde. `semgrep` está
instalado y su baseline global es **rojo con 10 hallazgos** de
`except-exception-que-silencia`. No la ignores y no la "arregles" fuera de tu
lote: el conteo baja solo por lotes asignados. Invoca el ejecutable como
`.venv/bin/semgrep`; `python -m semgrep` no es la comprobación de disponibilidad.

`ruff.toml` selecciona solo reglas donde `app/` ya está limpio, para que la
puerta nazca verde. El backlog conocido (C901 81, BLE001 54, S608 22, S110 10)
está listado ahí y se limpia **por lotes asignados**, nunca de golpe y nunca
fuera de tu carril.

### 2.1 Instrumentación obligatoria

En cada tarea haz un preflight de disponibilidad de `ruff`, `semgrep`,
`hypothesis` y `codegraph`. No declares una herramienta activa por la mera
presencia de archivos: registra versión y, para Codegraph, `codegraph status`.

- **Ruff:** toda entrega que cambie Python ejecuta
  `.venv/bin/ruff check app/ tests/ scripts/` y conserva la salida literal.
- **Semgrep:** toda entrega que cambie runtime, scripts o tests ejecuta
  `.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off`.
  Mientras exista baseline rojo, la condición del carril es cero hallazgos
  nuevos en archivos propios; el reporte separa baseline y delta.
- **Codegraph:** es el primer instrumento para consumidores, callers/callees,
  impacto y tests afectados. Sincroniza o comprueba el índice antes de confiar
  en él. Si el MCP no está cargado, usa el CLI (`query`, `callers`, `callees`,
  `impact`, `affected`); si tampoco está disponible, declara el fallback y usa
  `rg` más verificación manual. Nunca lo omitas en silencio.
- **Hypothesis:** al crear o modificar parsers, resolvers, normalizadores,
  aritmética de rangos, máquinas de estado o cualquier lógica con invariantes y
  espacio combinatorio, añade o amplía pruebas de propiedades. Las propiedades
  deben ser deterministas, sin I/O externo y expresar una invariante real. Un
  cambio sólo documental o mecánico no justifica una prueba ornamental.

El reporte final dice qué instrumento se usó, con qué alcance y qué no se
ejecutó. Disponibilidad no equivale a validación y un índice presente no
equivale a un MCP cargado en la sesión.

---

## 3. Propiedad de archivos

Tu carril es dueño de un conjunto de archivos. **No edites archivos del otro
carril, ni siquiera para un arreglo obvio.** Si encuentras un defecto fuera de
tu carril: documéntalo en tu reporte final y sigue. El orquestador lo enruta.

Los carriles activos y sus fronteras viven en
[`docs/orquestacion/CARRILES.md`](docs/orquestacion/CARRILES.md). Léelo antes de
tocar código y decláralo leído en tu primer mensaje.

Archivos **compartidos** (cualquier carril puede necesitarlos) — si tienes que
tocar uno, **avisa antes de editar**: `docs/decisions_log.md`, `Makefile`,
`ruff.toml`, `.semgrep/`, `AGENTS.md`.

---

## 4. ADR y bitácora

- El número de ADR sale del **máximo GLOBAL sobre todos los worktrees**, no del
  de tu rama. Al 2026-09-04 el máximo es **ADR-080**.
- Cada carril tiene un **rango reservado** en `CARRILES.md`. Usa solo el tuyo.
- La bitácora canónica es `docs/decisions_log.md`. Los ADR viven en
  `docs/decisions/`.

---

## 5. Herramienta correcta por tipo de pregunta

| La pregunta | El instrumento |
|---|---|
| "¿Dónde se usa X?" / "¿qué rompo?" | Codegraph (`query` / `callers` / `impact` / `affected`); si no está disponible, declarar fallback a `rg` + verificación |
| "¿Cuándo apareció esto?" | `git log -S` / `git blame` |
| "¿Violé una ley del proyecto?" | `.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off` |
| "¿La regla se mantiene para entradas no enumeradas?" | Hypothesis + ejemplos focales de regresión |
| "¿Cómo funciona la API X?" | leer el código de la dependencia — `burr` y el SDK de `openai` se mueven rápido, no cites de memoria |

---

## 6. Cómo entregas

Commits en tu rama, en tu worktree. **No hagas merge a `main`** — eso lo hace el
orquestador. **No hagas push** salvo petición explícita.

Tu reporte final debe decir, en este orden:

1. Qué cambiaste y por qué (la tesis, no la lista de diffs).
2. **Salida literal** de las tres puertas. Si no corriste alguna, dilo.
3. Qué encontraste fuera de tu carril y no tocaste.
4. Qué quedó sin hacer y por qué.

Reportar "listo" con una puerta sin correr es el único error que no se perdona
aquí: hace que el orquestador fusione a ciegas.

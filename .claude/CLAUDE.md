<!-- CODEGRAPH_START -->
## CodeGraph

This project has a CodeGraph MCP server (`codegraph_*` tools) configured. CodeGraph is a tree-sitter-parsed knowledge graph of every symbol, edge, and file. Reads are sub-millisecond and return structural information grep cannot.

### When to prefer codegraph over native search

Use codegraph for **structural** questions — what calls what, what would break, where is X defined, what is X's signature. Use native grep/read only for **literal text** queries (string contents, comments, log messages) or after you already have a specific file open.

| Question | Tool |
|---|---|
| "Where is X defined?" / "Find symbol named X" | `codegraph_search` |
| "What calls function Y?" | `codegraph_callers` |
| "What does Y call?" | `codegraph_callees` |
| "How does X reach/become Y? / trace the flow from X to Y" | `codegraph_trace` (one call = the whole path, incl. callback/React/JSX dynamic hops) |
| "What would break if I changed Z?" | `codegraph_impact` |
| "Show me Y's signature / source / docstring" | `codegraph_node` |
| "Give me focused context for a task/area" | `codegraph_context` |
| "See several related symbols' source at once" | `codegraph_explore` |
| "What files exist under path/" | `codegraph_files` |
| "Is the index healthy?" | `codegraph_status` |

### Rules of thumb

- **Answer directly — don't delegate exploration.** For "how does X work" / architecture questions, answer with 2-3 codegraph calls: `codegraph_context` first, then ONE `codegraph_explore` for the source of the symbols it surfaces. For a specific **flow** ("how does X reach Y") start with `codegraph_trace` from→to — one call returns the whole path with dynamic hops bridged — then ONE `codegraph_explore` for the bodies; don't rebuild the path with `codegraph_search` + `codegraph_callers`. Codegraph IS the pre-built index, so spawning a separate file-reading sub-task/agent — or running a grep + read loop — repeats work codegraph already did and costs more for the same answer.
- **Trust codegraph results.** They come from a full AST parse. Do NOT re-verify them with grep — that's slower, less accurate, and wastes context.
- **Don't grep first** when looking up a symbol by name. `codegraph_search` is faster and returns kind + location + signature in one call.
- **Don't chain `codegraph_search` + `codegraph_node`** when you just want context — `codegraph_context` is one call.
- **Don't loop `codegraph_node` over many symbols** — one `codegraph_explore` call returns several symbols' source grouped in a single capped call, while each separate node/Read call re-reads the whole context and costs far more.
- **Index lag — check the staleness banner, don't guess a wait.** When a codegraph response starts with "⚠️ Some files referenced below were edited since the last index sync…", the listed files are pending re-index — Read those specific files for accurate content. Files NOT in that banner are fresh and codegraph is authoritative for them. `codegraph_status` also lists pending files under "Pending sync".

### If `.codegraph/` doesn't exist

The MCP server returns "not initialized." Ask the user: *"I notice this project doesn't have CodeGraph initialized. Want me to run `codegraph init -i` to build the index?"*
<!-- CODEGRAPH_END -->

## Política de selección de herramientas

Cada clase de incertidumbre tiene su instrumento. Usar el equivocado no solo es
lento: da una respuesta que *parece* válida. Antes de investigar, identifica qué
tipo de pregunta es.

| La pregunta | El instrumento | Por qué ese |
|---|---|---|
| "¿Dónde se usa X?" / "¿qué rompo si lo cambio?" | `codegraph_*` | grafo AST preindexado; grep da falsos positivos |
| "¿Cuándo apareció este comportamiento?" | `git log/blame/bisect` vía Bash | el historial, no el estado actual |
| "¿Esta propiedad se cumple SIEMPRE?" | Hypothesis | los tests de ejemplo solo cubren lo que alguien imaginó |
| "¿Introduje un antipatrón de Python?" | `ruff` | rápido, con autofix |
| "¿Violé una ley de ESTE proyecto?" | `semgrep --config .semgrep/` | reglas propias que ruff no puede conocer |
| "¿Qué rompí?" | `pytest` + `codegraph_impact` | radio de impacto y evidencia |
| "¿Cómo funciona la API X en la versión que usamos?" | leer el código de la dependencia | `burr` y el SDK de `openai` se mueven rápido; no cites de memoria |

### La capa de verificación manda sobre la lectura

**Nunca declares que algo pasa porque leíste el código.** El orden es
`ruff → pytest → semgrep`, y cada uno puede decir NO de forma objetiva.

Dos reglas que vienen de fallos reales en este repo:

- **Correr dos archivos de test no es "validar".** Una extracción o un refactor
  puede romper importadores que no tocaste; eso solo lo ve la suite completa.
  Si no la corriste, dilo: "corrí X, no la suite".
- **`make lint-core` NO revisa estilo.** Solo busca archivos prohibidos (`.bak`,
  copias, binarios) en `app/core/`. Que pase no dice nada sobre la calidad. El
  linter real es `ruff`.

### Puertas de este repo

- `ruff check` — `ruff.toml` selecciona **solo reglas donde `app/` ya está
  limpio**, para que la puerta nazca en verde y cualquier regresión falle. El
  backlog conocido (`BLE001` 57, `S110` 11, `S608` 20, `C901` 80) está listado
  ahí y se limpia por lotes, no de golpe.
- `semgrep --config .semgrep/secoppal-laws.yml` — leyes arquitectónicas. La
  primera: `Settings()` fuera de pytest sin redirigir `secop_feedback_path`
  escribe en los `data/` reales.
- `tests/test_module_size_limits.py` — topes de LoC. `query_plan.py` opera a
  299/300: la `NEXT_TOUCH_RULE` exige extraer una responsabilidad **antes** de
  cualquier cambio funcional, como hizo R5 con `temporal_gate.py`. El tope no
  se sube.

Estas herramientas viven en la rama `chore/agent-tooling` hasta que se integre.


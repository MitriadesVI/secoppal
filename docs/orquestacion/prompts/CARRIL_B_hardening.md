# Carril B — HARDENING · Piloto

Trabajas en `/Users/rodrigoortiz/Documents/secoppal-codex-hardening`, rama
`hardening/silent-fallback-workflow-v1`. **Ábrelo como raíz del proyecto. No
trabajes en `/Users/rodrigoortiz/Documents/secoppal`** — hay otro agente activo
en un tercer worktree.

## Paso 0 — obligatorio antes de tocar código

Primero verifica dónde estás. Corre esto y pega la salida:

```bash
pwd && git rev-parse --show-toplevel && git branch --show-current
```

Debe decir `.../secoppal-codex-hardening` y `hardening/silent-fallback-workflow-v1`. **Si dice otra cosa, detente y avísame** —
estarías a punto de escribir en el worktree de otro agente.

Después lee `AGENTS.md` y `docs/orquestacion/CARRILES.md`. Confirma que los
leíste y **enumera los archivos que tienes prohibidos**. Si no puedes
enumerarlos, no empieces.

## El trabajo — y por qué no es cosmético

Hay 6 violaciones `S110` (`try`/`except`/`pass`) en tu carril:

```
app/core/workflow/actions/execution.py:356
app/core/workflow/handlers/direct_lookup.py:375
app/core/workflow/handlers/normal_flow.py:178
app/core/workflow/handlers/pagination.py:346
app/core/workflow/handlers/radar.py:111
app/core/workflow/handlers/refinement.py:98
```

Esto **no es una tarea de linter**. Este repositorio tiene una invariante,
`INV-NO-SILENT-SEMANTIC-FALLBACK` (ADR-069), que nació de una auditoría donde
siete defectos distintos resultaron ser **un solo mecanismo**: vocabularios
enumerados que fallaban abierto. La conclusión fue literal: *el defecto era el
fallback, no el hueco*.

`try/except/pass` es la forma sintáctica exacta de ese defecto.

Tu pregunta no es "¿cómo silencio a ruff?". Es:

> **Para cada uno de los 6: ¿qué se traga este `except`, y puede tragarse un
> fallo que cambie lo que ve el usuario sin que nadie se entere?**

## Secuencia

1. **Clasifica los 6 antes de editar ninguno.** Para cada sitio: qué excepción
   ocurre realmente, qué estado queda cuando ocurre, y si el usuario podría
   recibir una respuesta materialmente distinta. Usa `git log -S` / `git blame`
   para saber por qué se escribió así — algunos `pass` son deliberados y
   correctos.
2. **Separa en dos grupos:**
   - **Benigno:** el `except` es legítimo. Arreglo = capturar la excepción
     concreta (no `Exception` desnudo) + log. Sin cambio de comportamiento.
   - **Fallback silencioso:** tragarse ese error **sí** puede alterar la
     respuesta. Esto es un **hallazgo**, no un cambio. Documéntalo con el
     escenario concreto (entrada → salida equivocada) y **no lo arregles**:
     cambiar comportamiento observable es del carril de features.
3. **Prueba lo que endureces.** Si un `except` benigno pasa a loggear, un test
   debe demostrar que la ruta de excepción se ejerce.
4. **Cierra el lote.** Si los 6 quedan limpios, actualiza el conteo del backlog
   en `ruff.toml` (`S110 10` → lo que quede). `ruff.toml` es zona compartida:
   **avisa antes de editarlo**.

## Dato que cambia el marco

La ley `except-exception-que-silencia` de `.semgrep/secoppal-laws.yml` **marca
exactamente estos mismos 10 sitios**. Estaba escrita pero nunca había corrido:
el binario `semgrep` no estaba instalado en el entorno (se instaló el
2026-09-04). Es decir, la puerta más específica del proyecto llevaba tiempo en
rojo sin que nadie lo viera.

Tu lote de 6 la baja de 10 a 4. Los 4 restantes (`formatter.py`,
`response_policy.py`) quedan para un lote posterior, **no para ti**. Reporta el
conteo antes y después con la salida literal de semgrep.

## Fronteras

- Tuyo: los 6 archivos de arriba y sus tests.
- **Prohibido:** `formatter.py` y `response_policy.py`. Tienen 4 `S110` más y
  los verás en la salida de ruff. **Quedan fuera del lote a propósito** — el
  otro agente los está editando ahora mismo.
- **Prohibido cambiar comportamiento observable.** Si endurecer un `except`
  cambia una respuesta, eso se reporta, no se hace.

## Cierre

ADR disponibles: **086–090** (el máximo global del repo es ADR-080).

**No corras la suite completa** (`pytest tests/`). Son ~11k tests con red real
contra SECOP y hay otro agente activo: en paralelo se rate-limitean y dan rojos
falsos. La suite la corre el orquestador antes de fusionar.

Tú corres:

```bash
source .venv/bin/activate
ruff check app/ tests/ scripts/
semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
python -m pytest tests/<solo los de tus 6 handlers> -q
```

Reporta con: la clasificación de los 6 (benigno vs hallazgo, con el
razonamiento), la **salida literal** de las tres, el conteo de semgrep antes y
después, **la lista exacta de archivos de test que corriste**, y lo que dejaste
sin hacer. No hagas merge a `main` ni push.

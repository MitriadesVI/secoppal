# Carril A — FEATURES · Piloto

Trabajas en `/Users/rodrigoortiz/Documents/secoppal-codex-features`, rama
`feature/money-polarity-fix-v1`. **Ábrelo como raíz del proyecto. No trabajes en
`/Users/rodrigoortiz/Documents/secoppal`** — ese es el worktree principal y hay
otro agente activo en un tercero.

## Paso 0 — obligatorio antes de tocar código

Primero verifica dónde estás. Corre esto y pega la salida:

```bash
pwd && git rev-parse --show-toplevel && git branch --show-current
```

Debe decir `.../secoppal-codex-features` y `feature/money-polarity-fix-v1`. **Si dice otra cosa, detente y avísame** —
estarías a punto de escribir en el worktree de otro agente.

Después lee `AGENTS.md` y `docs/orquestacion/CARRILES.md`. Confirma que los
leíste y **enumera los archivos que tienes prohibidos**. Si no puedes
enumerarlos, no empieces.

## El problema

Existe una inversión de polaridad en los filtros de dinero. La hipótesis de
trabajo, **derivada de una investigación previa y no verificada en este árbol**,
es esta:

> Cuando el usuario pide un umbral monetario, el parser cae a un fallback que
> **inventa un `valor_max`**. El efecto es asimétrico: quien pide *"menos de X"*
> obtiene lo correcto por accidente; quien pide **"más de X"** recibe justamente
> el complemento de lo que pidió. Hay indicios de una consulta que pedía 18
> resultados y devolvió 2.766.

Trátala como **hipótesis a falsificar, no como diagnóstico**. Tu primer
entregable es evidencia, no un fix.

## Secuencia

1. **Reproduce y congela.** Antes de tocar runtime, escribe un test que falle
   por la razón correcta (`xfail(strict=True)` o fixture de regresión, según el
   patrón que ya use el repo). Un fix sin bug congelado no se acepta.
2. **Localiza la autoridad real.** El punto de partida es
   `app/core/money_admission.py` y `app/core/query_plan.py`. Determina **quién
   decide la polaridad** y dónde se pierde. Puede no estar donde parece.
3. **Arregla en la autoridad**, no en el borde. Si el arreglo natural cae en
   `query_plan.py`, **para**: ese archivo está a 299/300 LoC y la
   `NEXT_TOUCH_RULE` te obliga a extraer una responsabilidad a un módulo nuevo
   *antes* del cambio funcional. No subas el tope.
4. **Verifica que no invertiste el signo del otro lado.** El caso *"menos de X"*
   funcionaba por accidente; asegúrate de que sigue funcionando por diseño.

## Fronteras

- Tuyo: `money_admission.py`, `query_plan.py`, `formatter.py`,
  `response_policy.py`, y los tests de dinero.
- **Prohibido:** todo `app/core/workflow/**` — otro agente está ahí ahora mismo.
- **No limpies lint fuera del fix.** Verás `except Exception` y `try/except/pass`
  en tus archivos. Déjalos. Ese lote es del otro carril. Repórtalos.

## Cierre

ADR disponibles: **081–085** (el máximo global del repo es ADR-080).

**No corras la suite completa** (`pytest tests/`). Son ~11k tests con red real
contra SECOP y hay otro agente activo: en paralelo se rate-limitean y dan rojos
falsos. La suite la corre el orquestador antes de fusionar.

Tú corres:

```bash
source .venv/bin/activate
ruff check app/ tests/ scripts/
semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
python -m pytest tests/<solo los de dinero/polaridad> -q
```

Reporta con: la tesis, la **salida literal** de las tres, **la lista exacta de
archivos de test que corriste**, lo que viste fuera de tu carril, y lo que
dejaste sin hacer. No hagas merge a `main` ni push.

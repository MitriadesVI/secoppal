# Carriles de trabajo paralelo

**Actualizado:** 2026-09-04 · **Orquestador:** Claude Code en el worktree principal.

Dos agentes Codex trabajan en paralelo. Este archivo es la única autoridad sobre
quién puede tocar qué. Si un carril necesita cruzar la frontera, **pide primero**.

---

## Regla de partición

La frontera **no es solo por archivo, también por tiempo del código**:

- **HARDENING** opera sobre lo que **ya está fusionado en `main`**. Endurece
  código estable.
- **FEATURES** opera sobre lo **nuevo**. Escribe comportamiento.

Sin esta regla, hardening endurece un blanco móvil y el merge lo paga el humano.

---

## Carril A — FEATURES

| | |
|---|---|
| **Worktree** | `/Users/rodrigoortiz/Documents/secoppal-codex-features` |
| **Rama** | `feature/money-polarity-fix-v1` |
| **Base** | `main` @ 413b363 |
| **ADR reservados** | **081 – 085** |

### Dueño de

```
app/core/money_admission.py
app/core/query_plan.py            (¡NEXT_TOUCH_RULE — 299/300!)
app/core/formatter.py
app/core/response_policy.py
tests/**  (solo tests de dinero/polaridad)
```

### Prohibido

- `app/core/workflow/**` — es del carril B.
- Limpiar lint fuera del alcance del fix. Si ves un `except Exception` feo en un
  archivo tuyo, **déjalo** y repórtalo: el lote de limpieza es del carril B.

---

## Carril B — HARDENING

| | |
|---|---|
| **Worktree** | `/Users/rodrigoortiz/Documents/secoppal-codex-hardening` |
| **Rama** | `hardening/silent-fallback-workflow-v1` |
| **Base** | `main` @ 413b363 |
| **ADR reservados** | **086 – 090** |

### Dueño de

```
app/core/workflow/actions/execution.py
app/core/workflow/handlers/direct_lookup.py
app/core/workflow/handlers/normal_flow.py
app/core/workflow/handlers/pagination.py
app/core/workflow/handlers/radar.py
app/core/workflow/handlers/refinement.py
tests/**  (solo tests de los handlers anteriores)
```

### Prohibido

- `formatter.py` y `response_policy.py` — **tienen S110 pero son del carril A en
  este ciclo.** Sus 4 violaciones quedan fuera del lote a propósito.
- Cambiar comportamiento observable. Si endurecer un `except` cambia una
  respuesta al usuario, eso es un hallazgo para reportar, no un cambio para hacer.

---

## Zona compartida (avisar antes de editar)

```
docs/decisions_log.md
Makefile
ruff.toml
.semgrep/
AGENTS.md
docs/orquestacion/CARRILES.md
```

---

## Recursos únicos — no son de nadie

| Recurso | Regla |
|---|---|
| Puertos 8000 / 8501 | Solo el worktree principal. Ningún carril levanta la app. |
| `data/` de `main` | Ningún carril escribe ahí. `Settings()` siempre redirigido. |
| Red SECOP | Dos suites completas en paralelo pueden rate-limitearse. Si ves timeouts raros, repórtalo antes de "arreglarlo". |

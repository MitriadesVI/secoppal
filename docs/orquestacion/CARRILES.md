# Carriles de trabajo paralelo

**Actualizado:** 2026-09-05 (Entity Analysis / Competition Sources epoch)
**Orquestador:** agente coordinador en el worktree principal.

Este archivo es la autoridad sobre propiedad, fronteras y recursos compartidos
del epoch. Los agentes trabajan en worktrees separados y no pueden ampliar su
carril por cuenta propia.

---

## Base común del epoch

- Base histórica solicitada: `main` @ `0d6fd55`.
- Base funcional preparada: `main` @ `f43ac30` (incluye el guard M1B corregido).
- HEAD de lanzamiento: el commit de `main` que contiene esta revisión de
  `CARRILES.md`; el orquestador entrega su SHA completo a ambos agentes.
- Warehouse local: **PROHIBIDO**.
- Neo4j / Memgraph: **PROHIBIDO**.
- Push y merge a `main`: **PROHIBIDOS** durante los carriles.

Los commits `111c4f1` y `21e5ffb` no forman parte de la base ni pueden
reutilizarse.

---

## Carril A — ENTITY ANALYSIS V1

| | |
|---|---|
| **Worktree** | `/Users/rodrigoortiz/Documents/secoppal-codex-entity` |
| **Rama** | `feature/entity-analysis-v1-core` |
| **Tipo** | Product / architecture / implementation |
| **ADR reservados** | **081–085** |

### Propiedad exclusiva

```text
app/core/entity_profile/**
app/core/workflow/actions/entity_profile.py       (si se crea)
app/core/workflow/actions/execution.py            (sólo wiring mínimo)
app/core/analytics/**                              (sólo cambio exigido por reutilización)
tests/test_entity_profile*.py
tests/fixtures/entity_profile*
docs/architecture/ENTITY_ANALYSIS_V1.md
docs/audits/entity_analysis/**
docs/decisions/ADR del rango 081–085
```

Puede leer las autoridades existentes de Territorial Knowledge, BuyerUnits,
BuyerGroups, source names, routing, planning, follow-up, SoQL y analytics.

### Prohibido sin autorización nueva

- Editar `app/core/query_plan.py`. Si resulta necesario: **STOP** y propuesta
  previa de extracción por `NEXT_TOUCH_RULE` (299/300).
- Editar `app/core/query_router.py`. Debe documentar por qué el scope productivo
  existente no basta y pedir autorización.
- Resolver texto, crear BuyerUnits, promover candidatos, crear aliases o
  bindings, inferir BuyerGroups o convertir Territory en BuyerUnit mediante
  Territorial Knowledge.
- Usar outputs del carril B, warehouse, DuckDB, Parquet, graph DB o LLM para
  cálculos.

`docs/decisions_log.md` sólo puede solicitarlo A y debe avisar primero al
orquestador. El orquestador decide y edita la zona compartida.

---

## Carril B — COMPETITION INTELLIGENCE SOURCE AUDIT V1

| | |
|---|---|
| **Worktree** | `/Users/rodrigoortiz/Documents/secoppal-codex-competition` |
| **Rama** | `audit/competition-intelligence-sources-v1` |
| **Tipo** | Data / source research; cero runtime |
| **ADR reservados** | **086–090**, sin escribir ADR en este epoch |

### Propiedad exclusiva

```text
scripts/audits/competition/**
scripts/audits/audit_competition_sources_v1.py
artifacts/audits/competition_sources_v1/**
docs/audits/COMPETITION_INTELLIGENCE_SOURCE_AUDIT_V1.md
tests/test_competition_source_audit*.py
```

### Prohibido

```text
app/**
tests de Entity Analysis
QueryRouter
workflow
analytics
Territorial Knowledge
BuyerGroups
aliases
bindings
docs/decisions_log.md
docs/decisions/**
```

B sólo produce evidencia acotada y reproducible. No acepta fuentes como
autoridad productiva, no cambia comportamiento y no hace snapshots nacionales.

---

## Zona compartida — sólo orquestador

```text
docs/orquestacion/CARRILES.md
AGENTS.md
Makefile
ruff.toml
.semgrep/
docs/decisions_log.md
```

Ningún agente edita esos archivos. Si necesita un cambio, se detiene y lo pide.

---

## Guardas y autoridades

| Guarda | Contrato |
|---|---|
| `tests/test_municipal_m1b.py` | Los 1.013 digests de `gates.json` se verifican contra los blobs del commit congelado `51a5ad9`; editar runtime no repinea evidencia. |
| `tests/test_truth_layer_public_parity.py` | Los repins sólo son deliberados, documentados y acotados por pruebas. Nunca se desactiva globalmente. |
| ADR-080 / Territorial Knowledge V0 | `SHADOW_NO_RUNTIME` salvo el consumo acotado que A someta a gates: sólo metadata después de resolver una BuyerUnit productiva `PRODUCT_RECOGNIZED`; nunca resolución ni scope. |

`gates.json`, evidencia histórica, aliases, bindings, BuyerGroups y
`proposals.json` no se reescriben ni repinean durante este epoch.

---

## Recursos únicos y validación

- Ningún carril ejecuta `dev.sh`, `make dev` o `make restart`, ni mata procesos.
- Ningún carril escribe en `data/` de `main`.
- Cualquier `Settings()` fuera de pytest redirige toda persistencia.
- A y B pueden ejecutar pruebas focales en paralelo.
- Sólo el orquestador corre la suite completa, una vez y en serie, cuando B ya
  no esté haciendo llamadas intensivas a SECOP.
- Las consultas SECOP de A y B deben ser acotadas; timeout/403/transporte no es
  evidencia afirmativa.

Cada carril ejecuta y reporta literalmente:

```text
ruff check app/ tests/ scripts/
semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
python -m pytest tests/<tests de su carril> -q
git diff --check
```

Semgrep parte de diez hallazgos conocidos. La condición del epoch es **NO NEW
SEMGREP FINDINGS IN OWNED FILES**, no cero absoluto. Cada agente debe decir
“corrí X, no la suite”.

---

## Carriles anteriores en cuarentena

| Commit | Rama | Estado |
|---|---|---|
| `111c4f1` | `feature/money-polarity-fix-v1` | **CUARENTENA**: no merge, repin, push ni reutilización. |
| `21e5ffb` | `hardening/silent-fallback-workflow-v1` | **CUARENTENA**: cambia comportamiento observable; no merge, repin, push ni reutilización. |

Sus worktrees no se borran, resetean ni reciclan para el epoch nuevo.

---

## Cierre

Los carriles son independientes: A no espera outputs de B y B puede cerrar
`PARTIAL` o `NO_GO` sin bloquear un `PASS` válido de A. El orquestador revisa
propiedad, diffs, uso de recursos y commits por separado antes de cualquier
decisión de integración. **No hay merge automático ni push.**

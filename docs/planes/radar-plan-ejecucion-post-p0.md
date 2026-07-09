# Radar — Plan de ejecución post-P0 (para implementar por etapas)

> **Estado base:** rama `radar/mvp-manual`, HEAD `50b7672`. Suite de radar en verde
> (121 tests `test_radar*`, 215 con `-k "radar or formatter or phrase"`).
> **Regla estratégica:** Verdad → Operabilidad → Push → Valor. No se construye push
> sobre un diff con ruido. Cada etapa cierra con suite completa verde antes de commit.

---

## 0. Cómo trabajar (léelo antes de tocar código)

- **Rama:** trabaja SIEMPRE sobre `radar/mvp-manual`. NO fusiones ni cherry-pickees
  `codex/radarpolish` (contiene `a3875e7`, una re-implementación *alternativa* de P0A/E
  que ya está reescrita aquí como `9795bb9` → duplicaría semántica).
- **Python:** `.venv/bin/python`. Tests: `.venv/bin/python -m pytest ...`.
- **Un módulo, una responsabilidad.** No apiles funciones nuevas en `store.py`/`runner.py`.
  Push, targets, attempts, locks, retención y deadlines van en **módulos pequeños nuevos**
  con enganche mínimo (patrón `follows.py`). Ver el mapa de módulos en cada etapa.
- **Higiene de tests:** ningún test ni script standalone puede escribir en `data/secoppal.db`.
  Construye `RadarStore(tmp_path / "radar.db")` en fixtures; cualquier `Settings()`/workflow
  standalone debe redirigir `feedback_path`/`feedback_db_path` a `tmp_path`.
- **No agregar al índice:** `codegraph`, `semantic_pilot_v1/` (untracked en el worktree; que
  sigan en `.gitignore`).
- Cada etapa: implementar → tests de la etapa → `pytest tests/test_radar*.py -q` →
  `pytest -q` completo → `git diff --check` → commit.

---

## Estado VERIFICADO del núcleo — NO re-implementar

Una auditoría externa reciente reportó 5 "críticos" que **ya están cerrados** en este HEAD
(la auditoría se hizo sobre un snapshot pre-P0; sus números de línea no coinciden con el
código actual). **No los vuelvas a "arreglar":**

| Reportado | Estado | Dónde vive hoy |
|---|---|---|
| `disappeared` = ruido de ventana | ✅ Cerrado `9795bb9` | `diff.classify` separa `window_churn` de `disappeared`; `disappeared` sólo con `confirmed_disappeared_keys` (el runner no las pasa → vacío). `has_novelty` **no** incluye `window_churn`. |
| Ciego a cambios en resultados viejos | ✅ Cerrado `9795bb9` | `runner._refresh_seen_rows` + `follows.fetch_current_rows_by_keys` (barrido por clave fuera de ventana). |
| Salta validación de frase | ✅ Cerrado `93206f8` | `runner._validated_rank_and_dedupe` usa `validate_rank_and_dedupe` y excluye cuarentena. |
| timeout ≡ vacío, sin backoff | ✅ Cerrado `50b7672` | `_RadarFailure` + outcomes tipados + `store.record_failure(next_run_after, permanent, pause_after)`. |
| `ordering_signal` rompe ventana | ✅ Cerrado | `builder.RADAR_SAVE_DENY_KEYS` (save) + `runner._apply_window_policy` (run). |

### Invariantes del núcleo (NO romper en ninguna etapa)

- **INV-R1** — `disappeared` sólo con claves verificadas por lookup. Ausencia de ventana = `window_churn`, nunca desaparición.
- **INV-R2** — `has_novelty` = `new ∪ changed_state ∪ changed_value ∪ disappeared`. **Nunca** incluye `window_churn`.
- **INV-R3** — El runner usa **exclusivamente** `validate_rank_and_dedupe`. Candidatos en cuarentena no entran a diff, eventos ni snapshots. Prohibido reintroducir `formatter.rank_and_dedupe` en el radar.
- **INV-R4** — timeout/transient: no borra `seen`, no emite eventos, no marca disappeared, aplica `next_run_after`. `radar_empty` no incrementa `failure_count`.
- **INV-R5** *(nuevo, E1)* — La primera corrida (baseline) **nunca** genera push, aunque `has_novelty=True`.
- **INV-R6** *(nuevo, E5/E6)* — Sin target activo no hay push. Quiet hours bloquea. Un mismo digest no se envía dos veces.
- **INV-R7** *(nuevo, E2)* — Una corrida por radar a la vez.
- **INV-R8** *(nuevo, E3)* — Unicidad `(owner, name)` case-insensitive.

---

# BLOQUE A — Cierre y Verdad restante

## E0 — Cierre P0 documental + encuadre de rama

**Objetivo:** dejar registro de que P0 está cerrado y corregir el encuadre de merge.

**Tareas**
1. Escribir nota de cierre técnico (no ADR salvo cambio de arquitectura) mapeando
   auditor↔commits (la tabla de arriba). Ubicar en `docs/operativo/` o al pie de este plan.
2. Corregir el encuadre: `main` está **detrás** de `radar/mvp-manual` (no hay nada que
   integrar); el riesgo de duplicación es `codex/radarpolish`, no `main`.
3. Verificación de rama (NO merges destructivos):
   ```bash
   git log --oneline main..radar/mvp-manual   # debe mostrar 9795bb9, 93206f8, 50b7672
   git log --oneline radar/mvp-manual..main    # debe estar VACÍO
   ```

**Permitido:** docs. **Prohibido:** tocar código del radar, fusionar ramas.
**GO:** nota escrita; verificación de rama confirmada.

---

## E1 — Marcador de baseline (★ único hueco de Verdad que queda)

**Problema:** en la primera corrida `seen_map` está vacío → todo cae en `diff.new` →
`has_novelty=True` con `outcome=RADAR_OK`. El feed es silencioso (evento `baseline`), pero
el **digest/gate `has_novelty` no lo es**. Si el notifier (E6/E7) se conecta a `has_novelty`,
el primer ciclo de cada radar empuja TODO el backfill como "nuevos". Esto es precondición
dura del push (INV-R5).

**Archivos permitidos:** `app/core/radar/runner.py`, `app/core/radar/scheduler.py`,
`tests/test_radar_runner.py`, `tests/test_radar_scheduler.py`.
**Prohibido:** tocar `diff.py`, `digest.py` (más allá de leer), red SECOP.

**Cambios concretos**
1. `runner.run_radar`: calcular `is_first_run = not seen_map` **justo después** de
   `seen_map = store.get_seen(radar.id)` (hoy la variable se recalcula más abajo en línea
   ~443; súbela, no la dupliques).
2. Inyectar en el digest OK: `digest["is_baseline"] = is_first_run` (junto a `outcome`/`reason`).
   En los `_error_digest` dejar `is_baseline=False` (ya no tienen novedad).
3. `RadarRunResult`: nuevo campo `is_baseline: bool = False`; setéalo en el return OK.
4. `scheduler.RadarOutcome`: nuevo campo `is_baseline: bool = False`; propágalo desde
   `result.is_baseline` en el brazo `status="ran"`. (El notifier de E6 leerá esto.)
5. **No** cambiar `has_novelty` ni la lógica de diff. El gate anti-baseline es responsabilidad
   del notifier: `should_notify = outcome.has_novelty and not outcome.is_baseline`.

**Tests mínimos** (`tests/test_radar_runner.py`, `tests/test_radar_scheduler.py`)
1. Primera corrida con N resultados → `result.is_baseline is True` y `digest["is_baseline"] is True`.
2. Segunda corrida con un nuevo real → `is_baseline is False`, `has_novelty True`.
3. `RadarOutcome.is_baseline` refleja el de la corrida.
4. Un helper `should_notify(outcome)` (o el gate inline) devuelve `False` en baseline aunque
   `has_novelty True`, y `True` en la segunda corrida con novedad.

**Validación**
```bash
.venv/bin/python -m pytest tests/test_radar_runner.py tests/test_radar_scheduler.py -q
.venv/bin/python -m pytest -q
```
**Commit:** `fix(radar): mark baseline run so push suppresses backfill`
**GO:** INV-R5 testeado; suite verde.

---

# BLOQUE B — Operabilidad (correctitud que ya afecta hoy)

## E2 — Lock por radar (P1D)

**Problema:** scheduler (systemd) y "ejecutar radar X" (chat/UI) pueden correr el mismo radar
a la vez; ambos leen `seen_map` antes del upsert → eventos "nuevo" duplicados. WAL +
`busy_timeout` **no** cubren este read-then-write entre procesos.

**Módulo nuevo:** `app/core/radar/locks.py` (no metas esto en `store.py`).
**Archivos permitidos:** `locks.py` (nuevo), `runner.py` (adquirir/soltar), `store.py` (sólo
si el lock se persiste como columnas), `tests/test_radar_locks.py` (nuevo).

**Diseño (SQLite local → simple):** columnas en `radars`:
```
running_since   TEXT   -- ISO8601; NULL = libre
running_owner   TEXT   -- identificador del proceso/pasada
```
(usa el patrón `_ensure_columns` para migración aditiva; NO recrear tablas).

**API sugerida (`locks.py`)**
```python
LOCK_TTL_MINUTES = 30
def try_acquire(store, radar_id, *, owner) -> bool   # atómico vía UPDATE ... WHERE running_since IS NULL OR running_since < ttl_cutoff
def release(store, radar_id) -> None
@contextmanager
def radar_lock(store, radar_id, *, owner) -> Iterator[bool]   # yield adquirido; release en finally
```
El `UPDATE` condicional debe ser **una sola sentencia** (atómico), no SELECT-luego-UPDATE.

**Enganche:** `run_radar` (o su caller en scheduler/handler) entra por `radar_lock`. Si no
adquiere → salta con outcome `skipped`/`locked` (no error). `release` SIEMPRE en `finally`.

**Tests mínimos** (`tests/test_radar_locks.py`)
1. Segunda adquisición del mismo radar con lock activo → `False`.
2. Lock más viejo que `LOCK_TTL_MINUTES` → se puede readquirir (proceso muerto no bloquea para siempre).
3. `release` tras éxito libera.
4. `release` en `finally` tras excepción libera.
5. Dos radares distintos adquieren en paralelo (sin bloqueo cruzado).

**Commit:** `fix(radar): prevent concurrent runs per radar`
**GO:** INV-R7 testeado; suite verde.

---

## E3 — Unicidad case-insensitive (P2D)

**Problema:** `radars` tiene `UNIQUE(owner, name)` (case-sensitive) pero
`get_radar_by_name` busca con `COLLATE NOCASE`. Hoy puedes crear `PAE` y `pae` (el
`IntegrityError` de `create_radar` no salta) y el lookup devuelve uno arbitrario.

**Archivos permitidos:** `store.py`, `tests/test_radar_store.py`.
**Prohibido:** cambiar la firma pública de `create_radar`/`get_radar_by_name`.

**Diseño**
1. Columna `name_norm TEXT` = `lower(trim(name))` (migración aditiva vía `_ensure_columns`,
   backfill de filas existentes en la misma migración).
2. Índice único: `CREATE UNIQUE INDEX IF NOT EXISTS ux_radars_owner_namenorm ON radars(owner, name_norm)`.
3. `create_radar`: computar y persistir `name_norm`; el `IntegrityError` ahora atrapa `PAE`/`pae`.
4. **Migración de duplicados existentes:** antes de crear el índice único, detectar colisiones
   `(owner, name_norm)` y renombrar con sufijo determinista (` (2)`, ` (3)`) o reportar; NO
   perder radares. Documentar la política elegida en el docstring de la migración.
5. `get_radar_by_name`: puede seguir con `COLLATE NOCASE` o cambiar a `WHERE name_norm = lower(trim(?))`
   (preferible: consistente con el índice).

**Tests mínimos** (`tests/test_radar_store.py`)
1. Crear `PAE` y luego `pae` para el mismo owner → `RadarExistsError`.
2. Mismo nombre para owners distintos → permitido.
3. Lookup por nombre con distinto casing sigue encontrando el radar.
4. Migración con duplicados preexistentes: no borra radares; aplica sufijo/reporte.

**Commit:** `fix(radar): enforce case-insensitive radar names`
**GO:** INV-R8 testeado; migración probada sobre DB con duplicados; suite verde.

---

# BLOQUE C — Digest accionable

## E4 — Digest con links (P1A)

**Por qué antes del push:** para WhatsApp/Telegram el link ES la acción. `urlproceso` ya se
persiste en `radar_events` y `radar_seen_results`; el texto del digest no lo usa.

**Archivos permitidos:** `app/core/radar/digest.py`, `tests/` de digest/radar.
**Prohibido:** WhatsApp real, notification prefs, identidad, pliegos, nodos.

**Reglas** — cada ítem relevante del digest incluye: título/objeto corto, entidad, valor si
existe, estado si existe, fecha relevante si existe, **link público si existe**, o la marca
"link no disponible" si no. Nota: ambos datasets (procesos/contratos) exponen `urlproceso`,
pero puede venir vacío → la rama "no disponible" debe testearse por dataset, no sólo procesos.

**Cambios concretos**
1. `_format_item`: añadir `Link: {urlproceso}` cuando exista; si no, `Link: no disponible`.
2. Mostrar también `changed_value` en el texto (hoy sólo se cuenta): bloque con `prev_value → value`
   + link.
3. `changed_state`: añadir link al bloque existente.
4. Decidir explícitamente el tope: hoy es "top 3". Para push accionable, mantén top-N por
   sección con link, y deja el resto al conteo.

**Tests mínimos**
1. Digest de `new` incluye link si existe.
2. Digest de `changed_state` incluye link si existe.
3. Digest de `changed_value` muestra valor anterior y nuevo.
4. Sin URL → "link no disponible", no rompe.
5. `window_churn` no aparece en el texto (INV-R2).
6. Candidatos en cuarentena no aparecen (INV-R3).

**Commit:** `feat(radar): include actionable links in digest`
**GO:** suite verde; checklist de reglas cubierto.

---

# BLOQUE D — Push (no arrancar hasta E1–E4 cerradas)

> **Precondición de bloque:** E1 (baseline) + E2 (lock) + E3 (unicidad) + E4 (links) cerradas.

## E5 — Notification targets (P1B)

**Objetivo:** saber a quién notificar y por dónde. Sin esto no hay push fiable.

**Decisión de modelado (obligatoria antes de escribir schema):** hoy `radars.channel` guarda
un canal por radar. A partir de aquí, `radars.channel` es **default de presentación**; el
**enrutamiento real de push** vive en `radar_notification_targets` (por owner). Documenta que
`radars.channel` deja de decidir el push.

**Módulo nuevo:** `app/core/radar/notify_store.py` (targets + attempts; NO en `store.py`).
**Archivos permitidos:** `notify_store.py` (nuevo), `tests/test_radar_notify_store.py` (nuevo).

**Schema (idempotente, misma DB)**
```
radar_notification_targets (
  id, owner, channel,           -- whatsapp|telegram|email|web
  destination,                  -- chat_id|phone|email
  enabled, quiet_hours_start, quiet_hours_end, timezone,
  created_at, updated_at,
  UNIQUE(owner, channel, destination)
)
```
Si ya existe tabla de usuarios/preferencias, reutilízala en vez de duplicar.

**Reglas**
- Un owner puede tener varios canales; un canal puede estar deshabilitado.
- Sin target activo → nadie recibe (INV-R6).
- No asumir web-owner = WhatsApp-owner sin vínculo explícito.
- Primera versión: alta manual/admin, sin UI completa.

**Tests mínimos**
1. Owner sin target → no elegible.
2. Owner con target deshabilitado → no elegible.
3. Owner con target activo → elegible.
4. Quiet hours activa → bloquea (usa `timezone` del target).
5. `UNIQUE(owner, channel, destination)` no duplica el mismo destino.

**Commit:** `feat(radar): add notification targets`
**GO:** suite verde.

---

## E6 — Notifier dry-run (P1C)

**Objetivo:** producir "lo que se enviaría" y auditarlo. **Cero envío real.**

**Módulo nuevo:** `app/core/radar/notifier.py` (orquestación pura). Persistencia de attempts en
`notify_store.py` (E5).
**Archivos permitidos:** `notifier.py` (nuevo), `notify_store.py` (attempts), `scheduler.py`
(sólo enganche opcional), `tests/test_radar_notifier.py` (nuevo).

**Schema attempts** (en `notify_store.py`)
```
radar_notification_attempts (
  id, owner, radar_id, channel, destination_hash, digest_hash,
  dry_run, status,              -- pending|skipped|would_send|failed|sent
  skip_reason, created_at
)
```

**Flujo**
```
outcomes del scheduler
→ filtrar should_notify = has_novelty AND NOT is_baseline   (INV-R5)
→ excluir radar_empty / radar_timeout / next_run_after activo / needs_attention
→ agrupar por owner
→ aplicar notification_targets (E5) + quiet hours
→ construir digest (E4)
→ registrar attempt dry-run (would_send/skipped) con digest_hash
→ NO enviar
```

**Reglas**
- Notificar sólo si `should_notify` (baseline suprimida — INV-R5).
- No notificar `radar_empty` ni `radar_timeout`.
- No notificar radares en `next_run_after` (backoff) ni `needs_attention` (salvo flujo admin futuro).
- Agrupar por owner (anti-spam).
- **Idempotencia:** el mismo `digest_hash` no se re-registra como `would_send` si se corre dos
  veces sin cambios (INV-R6).

**Tests mínimos** (`tests/test_radar_notifier.py`)
1. Radar con novedad (no baseline) → attempt `would_send`.
2. Radar en baseline con `has_novelty` → attempt `skipped` (reason baseline).
3. Radar sin novedades → no attempt.
4. timeout → no attempt de push.
5. Quiet hours → `skipped`.
6. Backoff (`next_run_after`) → `skipped`.
7. Varios eventos del mismo owner → un digest agrupado.
8. Correr dos veces sin cambios → no duplica el mismo `digest_hash`.

**Commit:** `feat(radar): add notification dry run`
**GO:** dry-run produce exactamente lo que se enviaría; INV-R5/R6 testeados; suite verde.

---

## E7 — Push real por un canal (P1E)

**Precondiciones:** E1–E6 cerradas + dry-run probado.
**Canal primero:** **Telegram** (más fácil de estabilizar). WhatsApp/OpenWA después.
**Reusar** el path de envío existente en el borde (`app/main.py`/webhooks); NO reinventar cliente.

**Módulo:** `app/core/radar/channels/telegram.py` (o reuso del cliente existente) + wiring en
`notifier.py`. `scripts/run_radar_scheduler.py` pasa a invocar el notifier en modo real
detrás de un flag (`--send` / env), con dry-run como default.
**Archivos permitidos:** `notifier.py`, `channels/`, `scripts/run_radar_scheduler.py`,
`deploy/` (si el timer necesita el flag), tests.

**Reglas**
- Sólo digest agrupado; máximo una notificación por radar/owner/ciclo.
- Respetar quiet hours; registrar `sent`/`failed`.
- No reintentar en loop infinito; canal desactivable.

**Tests mínimos**
1. Envío OK → attempt `sent`.
2. Error de canal → `failed` (con mensaje).
3. Quiet hours → bloquea.
4. Digest ya enviado no se re-envía.
5. Owner con dos radares → un digest agrupado (o dos bloques en un mensaje, según diseño).

**Commit:** `feat(radar): send notification digests`
**GO:** checklist "listo para push beta" (abajo) completo.

---

# BLOQUE E — Valor incremental

## E8 — Fechas críticas / closing_soon (P2A)

**Problema:** `snapshot_hash` sólo cubre `(estado, valor)`; cambios de **fecha de cierre** —
la señal más accionable para un proponente — son invisibles.

**Archivos permitidos:** `keys.py` (extraer fecha), `diff.py` (nuevos kinds), `store.py`
(estado de alerta), `digest.py`, `runner.py`, tests correspondientes.

**Diseño (v1 acotada a `changed_deadline` + `closing_soon`)**
1. `keys.extract_fields`: añadir fecha de cierre/recepción (lee el campo del `SoQLBuilder.SPECS`
   por dataset; NO hardcodear nombre).
2. `keys.snapshot_hash`: incluir la fecha de cierre en el payload → un cambio de fecha produce
   `changed_state`/nuevo kind. Añadir kind `changed_deadline` en `diff` (misma clave, cambió fecha).
3. `closing_soon`: el runner calcula días-a-cierre; emite alerta si ≤ umbral (p. ej. 5 días).
   **Anti-repetición:** columna nueva en `radar_seen_results`
   (`closing_soon_notified_at TEXT`) para no re-alertar cada corrida (migración aditiva).
4. Proceso ya cerrado → no `closing_soon`.

**Tests mínimos**
1. Cambio de fecha de cierre → `changed_deadline`.
2. Mismo deadline → no duplica evento.
3. Cierra en N días → `closing_soon`.
4. `closing_soon` ya notificado → no se repite (usa `closing_soon_notified_at`).
5. Proceso cerrado → no `closing_soon`.

**Commit:** `feat(radar): track deadline changes and closing-soon alerts`
**GO:** suite verde.

## E9 — Creación declarativa (P2B)

**Problema:** hoy crear radar exige "guardar la última búsqueda"; "crea un radar de PAE en
Atlántico" sin búsqueda previa devuelve `radar_save_no_context`.

**Archivos permitidos:** `commands.py`, `handlers/radar.py`, `builder.py`, tests.

**Flujo:** mensaje declarativo → parse/query_plan → validar scope/topic (`has_scope_or_topic`)
→ `sanitize_for_save` (misma ruta que guardar) → `create_radar` → **primera corrida baseline
silenciosa** (INV-R5: no genera N novedades de backfill ni push).

**Reglas:** no crear sin scope/topic; confirmar nombre sugerido; si ambiguo, pedir aclaración;
`ordering_signal` no se guarda (ya lo hace `sanitize_for_save`).

**Tests mínimos**
1. "crea radar de PAE en Atlántico" → crea radar.
2. Query sin scope/topic → no crea.
3. Primera corrida no emite N "nuevos" al feed (baseline).
4. `ordering_signal` no persiste.
5. Usa la misma sanitización que guardar última búsqueda.

**Commit:** `feat(radar): create radars from natural language`

## E10 — Edición de radar (P2C)

**MVP:** pausar / activar / borrar / renombrar / cambiar frecuencia. Editar filtros después.
**Archivos permitidos:** `commands.py`, `handlers/radar.py`, `store.py`, tests.

**Tests mínimos**
1. Renombrar respeta unicidad case-insensitive (INV-R8).
2. Pausar excluye del scheduler.
3. Activar re-incluye si no tiene `needs_attention`.
4. Cambiar frecuencia recalcula `next_run_at`/equivalente.
5. No permite `PAE`/`pae` para el mismo owner.

**Commit:** `feat(radar): edit saved radars`

## E11 — Retención y poda (P2E)

**Módulo nuevo:** `app/core/radar/retention.py` + hook en `scripts/run_radar_scheduler.py`.
**Política:** eventos 180 días, runs 90 días, `seen_results` mientras el radar exista; al
borrar radar → cascada (FK ya `ON DELETE CASCADE`) o tombstone documentado.

**Tests mínimos** (`tests/test_radar_retention.py`)
1. Poda eventos antiguos; 2. no poda recientes; 3. poda runs antiguos; 4. no toca `seen`
activos; 5. delete radar limpia dependencias.

**Commit:** `feat(radar): add retention cleanup`

## E12 — Flujo needs_attention (P2F)

**Objetivo:** listar radares en `needs_attention`, ver último error/reason, editar/borrar,
reactivar. Reactivar limpia `needs_attention` + `failure_count`/`next_run_after` (ya lo hace
`update_last_run`; expón un comando/endpoint), sin borrar historial.
**Archivos permitidos:** `handlers/radar.py`, `commands.py`, `store.py`, tests.

**Tests mínimos**
1. Radar `needs_attention` aparece en listado dedicado.
2. Reactivar limpia el flag, conserva historial.
3. Reactivar sin cambios permite la próxima corrida.
4. Si vuelve a fallar, incrementa de nuevo.

**Commit:** `feat(radar): manage radars needing attention`

## E13 — Higiene: frame_version + limpieza de duplicados

Tres deudas detectadas por la auditoría, no en el plan original:
1. **`frame_version`** en el QueryFrame guardado (radares pre-Etapa-7/geo-tipada replayean con
   semántica nueva sin aviso). Añadir campo + migración explícita; al leer un frame de versión
   vieja, registrar/avisar en vez de mutar silencioso.
2. **Unificar detectores de anomalía:** `digest._atypical_threshold` convive con
   `app/utils/value_sanity.py::detect_value_anomaly`. Reusar el existente; no dos rutas.
3. **`relevance_label`** (columna en `radar_seen_results`) es hoy columna muerta (el runner no
   la escribe): o la cableas (semilla de P3A) o la documentas como reservada.

**Commit:** `chore(radar): frame version + dedupe anomaly detection`

---

# BLOQUE F — Diferencial (post-push, capa semántica madura)

## E14 — Radares por nodo semántico (P3A)
Precondición: E1–E7 + nodos maduros (p. ej. adulto mayor). Guardar `radar_type = query|node`,
`node_id`, `node_version`, `facets_enabled`. Cada evento explica `matched_node/faceta/terms/
relevance_type/confidence`. Cambio de versión de nodo no altera silenciosamente un radar viejo.
El fix INV-R3 (validación de frase en runner) es la costura por donde entra esto.
**Commit:** `feat(radar): support semantic node radars`

## E15 — Digest asesor Nivel 2 (P3B)
De conteos a lectura breve (reglas + signals, sin LLM caro): priorizar "cierra pronto",
mencionar mayor cuantía, agrupar por faceta, no inventar si faltan datos, mantener links.
Reusar el detector de anomalía unificado de E13.
**Commit:** `feat(radar): add advisory digest summary`

## E16 — Documentación final
1. **Marcar superseded** `docs/planes/radar-produccion-scheduler-push.md` (propone APScheduler
   in-FastAPI, ya descartado; systemd `deploy/secoppal-radar.timer` es lo vigente).
2. Doc operativo del Radar; runbook del scheduler; tabla de comandos usuario; matriz de eventos
   (`new`/`changed_state`/`changed_value`/`changed_deadline`/`closing_soon`/`baseline`/
   `window_churn` interno).
3. Nota de honestidad: el "digest asesor" fue plantilla de conteos hasta E15.
**Commit:** `docs(radar): update production plan after P0 fixes`

---

# Orden de ejecución

```
BLOQUE A  E0  cierre P0 + encuadre
          E1  marcador de baseline           ★ único Verdad restante
BLOQUE B  E2  lock por radar
          E3  unicidad case-insensitive
BLOQUE C  E4  digest con links
BLOQUE D  E5  notification targets
          E6  notifier dry-run
          E7  push real (Telegram)           ← gate "push beta"
BLOQUE E  E8  fechas críticas / closing_soon
          E9  creación declarativa
          E10 edición
          E11 retención
          E12 needs_attention
          E13 frame_version + higiene
BLOQUE F  E14 nodos semánticos
          E15 digest asesor
          E16 docs
```

---

# Checklist — "Radar listo para push beta" (gate de E7)

```
[ ] E1 baseline: primera corrida no empuja backfill (INV-R5).
[ ] No falsos disappeared por ventana (INV-R1).
[ ] has_novelty excluye window_churn (INV-R2).
[ ] Refresh por clave detecta cambios en vistos antiguos.
[ ] Phrase validation en runner; cuarentena fuera del diff (INV-R3).
[ ] timeout/transient con backoff; radar_empty no incrementa fallos (INV-R4).
[ ] Radar roto → needs_attention / auto-pausa.
[ ] Lock evita doble corrida (INV-R7).
[ ] Unicidad case-insensitive (INV-R8).
[ ] Digest trae links.
[ ] Owner tiene canal explícito (notification_targets).
[ ] Dry-run produce exactamente lo que se enviaría.
[ ] Push respeta quiet hours.
[ ] No se duplica un digest ya enviado (INV-R6).
[ ] Attempts auditados.
[ ] pytest completo verde.
```

# Checklist — "Radar comercialmente interesante"

```
[ ] Alertas con links.
[ ] Cierra pronto / cambio de fecha (E8).
[ ] Crear radar en lenguaje natural (E9).
[ ] Editar/pausar/reactivar fácil (E10/E12).
[ ] Radares por nodo/mercado (E14).
[ ] Digest asesor breve (E15).
[ ] Explicación de por qué entró cada resultado (E14).
```

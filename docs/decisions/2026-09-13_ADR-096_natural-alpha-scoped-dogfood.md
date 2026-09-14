# ADR-096 — Natural Alpha: dogfood acotado al seam

**Fecha:** 2026-09-13. **Estado:** alcance aceptado; preparación en rama, sin activación.
**Decisión humana:** `ACCEPT_SCOPED_DOGFOOD`.
**Misión:** `NATURAL_ALPHA_SCOPED_DOGFOOD`.
**Base:** `a3a9cfd58e26a31d3a748fabe33dc2aae1b9215b`.
**Rama:** `codex/natural-alpha-v1`.

## Contexto y autoridad

Natural Alpha observa únicamente el seam posterior a los handlers explícitos y
anterior a `normal_flow`. La admisión de Profile y los fast paths pueden atender
un turno antes de llegar allí. El alcance aceptado reconoce esa cobertura
parcial: no existe una garantía global de detección o bloqueo multi-job.

La exclusión de Profile es provisional y medible. Bajo esta decisión, su bypass
no es un defecto del candidate: es un gap de cobertura que debe aparecer en la
evaluación. Los fast paths quedan excluidos intencionalmente. Esta decisión no
extiende el seam ni cambia el orden o las autoridades de routing.

Este ADR registra una decisión ya tomada. La documentación, el commit y el
reporte estadístico no activan Alpha, no autorizan merge y no sustituyen gates
ni revisión independiente.

## Alcance aceptado

El dogfood se limita a Rodrigo autenticado en Streamlit, con email allowlisted
y receipt de identidad emitido desde la cuenta vigente. Sólo los turnos que
alcanzan el seam pueden entrar al carril Alpha. `/chat`, una identidad JSON,
una copia de la identidad o el email por sí solo no acreditan ese origen.

Dentro de ese carril, una propuesta válida con al menos dos jobs produce la
clarificación por pasos. Una propuesta single, vacía, inválida o con error del
proveedor mantiene la delegación al runtime legacy. La propuesta no crea ni
modifica params, QueryPlan, SoQL, dataset, backend o efectos. Los handlers
explícitos, Profile, QueryRouter, QueryPlan y SoQL conservan su autoridad.

El bloqueo multi-job se garantiza sólo para propuestas multi-job validadas
dentro del carril Alpha; no se extiende a todos los mensajes de Streamlit ni a
los turnos excluidos. El timeout propio de Alpha y los 15 segundos completos
posteriores para legacy mantienen el contrato ya aprobado.

## Dos denominadores y un checkpoint

El reporte distingue los turnos observados de Streamlit para el email y periodo
elegidos de los que efectivamente alcanzaron el seam:

| Medida | Definición |
| --- | --- |
| `eligible_turns_window` | Traces Streamlit del email normalizado desde `since`, incluidos los que no alcanzaron Alpha. |
| `reached_seam` | Turnos de esa ventana con modo `alpha`, elegibilidad registrada como `1` y status presente. |
| `coverage_gap` | `eligible_turns_window - reached_seam`. |
| `coverage_rate` | `reached_seam / eligible_turns_window`; `null` sin denominador. |
| `excluded_by_route` | Conteos de rutas host exactas para los turnos fuera del carril; no interpreta la consulta. |

La ventana se ordena cronológicamente por timestamp y después por `trace_id`.
Se extiende hasta el turno número **50 que alcanzó Alpha**, inclusivamente.
Puede contener más de 50 turnos Streamlit por las exclusiones intermedias. Los
turnos posteriores al corte no entran a ese checkpoint. Si todavía no existen
50 turnos Alpha, se usan todos los disponibles y `checkpoint_ready=false`.
El reporte expone el `trace_id` y timestamp de `window_end` para revisar el corte.

`checkpoint_ready` expresa volumen observado, no aprobación del dogfood. Los
criterios del checkpoint de 50 turnos se aplican sólo al carril Alpha: utilidad
percibida de al menos 80%, paridad single-job de 100%, efectos incorrectos cero,
multi-job validado ejecutado silenciosamente cero y llamadas Alpha fuera de
elegibilidad cero. La cobertura de los turnos generales se reporta por separado.

Las valoraciones y la utilidad del reporte corresponden sólo a turnos Alpha:
`utility_rate = positivos / valorados`. Los no valorados no son negativos ni
positivos; se informa también cobertura de rating. Sin valoraciones, utilidad
es `null`. La paridad, la corrección semántica y los efectos requieren revisión
con evidencia; el reporte no los infiere del rating o de la ausencia de errores.

## Instrumentación y límites

`scripts/report_natural_alpha_dogfood.py` recibe explícitamente `--db`, `--email`
y `--since` con zona horaria; `--checkpoint` permite un tamaño distinto para
pruebas o revisiones. Nunca abre el origen con SQLite: crea una snapshot de
bytes de DB y WAL, si existe, en un directorio temporal privado. No copia SHM.
Antes y después de copiar deben coincidir el conjunto DB/WAL, sus hashes
SHA-256 calculados por streaming y sus testigos de device, inode, tamaño,
mtime y ctime; los hashes de las copias deben coincidir con los del origen.
Si aparece o desaparece WAL o cambia un testigo, repite desde un temporal
nuevo, con un máximo de tres intentos. La inestabilidad persistente falla como
`SNAPSHOT_UNSTABLE`; no se entrega un reporte de una copia dudosa. La presencia
de cualquier rollback journal se rechaza conservadoramente, incluso si parece
inactivo: el reporte no intenta recuperar ni interpretar ese journal.

Sólo la copia verificada se abre mediante URI `mode=ro`; SQLite puede crear su
propio SHM dentro del temporal. El directorio y sus sidecars se eliminan al
terminar, tanto en éxito como en error. Ninguna conexión SQLite recibe el path
origen, ni se crean o modifican sus DB/WAL/SHM. Verifica las columnas necesarias
y falla explícitamente si faltan. No construye Settings, no migra la DB, no
escribe un reporte en disco y no selecciona texto de consultas. Esto corrige
la premisa insuficiente de que `mode=ro` por sí solo impedía efectos sobre SHM.

La salida incluye status counts, bloqueos, inválidos, errores, ratings y media /
p95 de latencias observadas en el carril Alpha. El p95 usa rango más cercano.
Los valores ausentes no se convierten en cero. La cola opcional `--review-queue`
contiene sólo trace ID, timestamp, route, status y rating. No incluye email ni
raw query; el default tampoco incluye la cola.

El receipt queda ligado al objeto emitido mediante su identidad local, sin
registry ni estado persistente nuevo. Las copias no heredan autoridad. Un
`PublicationRecoveryError` se traduce a `NaturalAlphaPublicationError`
conservando su causa: no hay fallback legacy ni éxito Alpha ante ese fallo. Esto
no repara ni redefine el mecanismo general de recuperación de publicación.
`_continuation_owner=NONE` sigue siendo metadata interna, no una referencia
reutilizable.

El caso C06 del smoke live sigue siendo un límite conocido: el modelo propuso
`FOLLOW/RUN` donde el gold provisional esperaba `FOLLOW/SAVE`. Se conserva la
evidencia y no se ajustan prompt, gold, corpus, manifest ni reportes congelados.
El resultado live histórico acredita ese experimento, no una cobertura global.

## Activación y rollback

La activación local para Rodrigo sigue siendo una acción operativa explícita y
separada, posterior a los gates y la revisión que HERA adjudique. No se cambian
flags, no se reinicia la app y no se ejecuta proveedor live en esta misión.

Rollback: `SECOPPAL_NATURAL_ALPHA_MODE=off`.

Ante fallo del checkpoint, se apaga el flag y se reevalúa fuera de producción.
Una futura inclusión de Profile o de otros turnos excluidos exige una decisión
de alcance y evidencia propias.

[Implementación, corpus y uso del reporte](../../experiments/natural_alpha_v1/README.md).

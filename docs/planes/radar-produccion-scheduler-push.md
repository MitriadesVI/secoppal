# Radar — Roadmap a producción: scheduler push

> Objetivo de producto: **hacerle la vida fácil a la gente**. Que el usuario no
> tenga que pulsar "Actualizar": los radares corren solos y le llega el digest
> de novedades por su canal, sin abrir la app.

## Dónde estamos (v1.0 manual + feed pull)

Ya implementado y en producción de código:

- Modelo de datos completo: `radars` (con `frequency`, `channel`, `last_run_at`,
  `last_error_*`), `radar_runs` (auditoría: soql/params/execution_ms), `radar_seen_results`,
  `radar_events` (feed).
- Motor: `run_radar()` reutiliza el núcleo puro `execute_resolved_params` (sin
  contaminar conversación), diffea, persiste seen + eventos, genera digest.
- UX: feed de movimientos (todos los radares, cronológico) + **pull-to-refresh**
  ("🔄 Actualizar" corre los radares activos). Crear radar desde chat y desde
  formulario.

**Lo único que falta para "no pulsar nada": que algo corra `run_radar()` solo.**

## Restricción técnica clave

**Streamlit no puede correr trabajos en background** — solo ejecuta código cuando
hay una sesión con la página abierta. Por tanto el scheduler **no** vive en
`streamlit_app.py`. Vive en el backend FastAPI (`app/main.py`) o como proceso/cron
externo. El feed de Streamlit solo *lee* lo que el scheduler ya computó.

## Diseño propuesto

### Fase A — Scheduler in-process (APScheduler en FastAPI)

1. Añadir `APScheduler` (`BackgroundScheduler`) al arrancar `app/main.py`.
2. Un job periódico (p. ej. cada 15 min) que:
   - lee `radars` con `enabled=1`,
   - filtra los **vencidos** según `frequency` vs `last_run_at`
     (`manual` → nunca auto; `diaria` → >24h; `semanal` → >7d; `cada_6h` → >6h),
   - corre `run_radar(deps, store, radar)` para cada uno (secuencial o pool chico),
   - respeta un rate-limit hacia SECOP (no martillar datos.gov.co).
3. Idempotencia: el propio `last_run_at` evita doble corrida; un lock por radar
   (columna `running_since` o un `SELECT … FOR UPDATE` equivalente en SQLite con
   `BEGIN IMMEDIATE`) evita solapamiento si un job se atrasa.

`frequency` y `last_run_at` **ya existen** en el esquema — Fase A no requiere
migración de datos, solo cablear el job.

### Fase B — Notificación push (digest al canal)

Reusar la infraestructura de canales del proyecto (Telegram/WhatsApp/email; ver
`app/main.py` webhooks y `project_secoppal_canales_webhooks`). Tras cada corrida
automática con `digest["has_novelty"]`:

1. Resolver el destino desde `radar.channel` + identidad del `owner`.
2. Enviar el `digest["text"]` (ya formateado, con top 3 + banderas de riesgo).
3. Registrar el envío (evitar reenviar el mismo `run_id`).

Regla de oro: **solo notificar si hay novedad** (`has_novelty`), nunca "sin
novedades", para no quemar la atención del usuario.

### Fase C — Escala

- Mover SQLite → Postgres cuando crezcan usuarios/radares (el `RadarStore` ya
  encapsula el acceso; cambiar la conexión es local).
- Sustituir el scheduler in-process por una cola de trabajos (RQ/Celery/Arq) +
  workers cuando un solo proceso no alcance.
- Particionar corridas por `frequency` y barajar horarios para aplanar la carga
  contra SECOP.
- Backpressure y reintentos: si SECOP responde lento/timeout, degradar a menor
  frecuencia y marcar `last_error_*` (ya existe).

## Decisiones abiertas (resolver al arrancar Fase A)

- Frecuencias ofrecidas al usuario: ¿`diaria` / `semanal` / `cada_6h`?
  (la columna `frequency` ya las soporta como string).
- Selector de canal por radar en la UI (hoy `channel` se setea pero no se elige).
- Ventana horaria de notificación (no mandar a las 3am).
- ¿Push también al feed con badge "nuevo desde tu última visita"?

## Riesgos

- **Carga a SECOP**: muchos radares × frecuencia alta = muchas queries. Mitigar
  con rate-limit, ventana de revisión (`WINDOW_LIMIT=100`) y horarios barajados.
- **Ruido de notificaciones**: solo `has_novelty`, y agrupar varios radares en un
  digest diario por usuario si hace falta.
- **Coste de cómputo**: cada corrida ~5–10s contra SECOP; con pool y colas se
  controla en Fase C.

## Resumen

El motor (`run_radar`) y los datos (`frequency`, `last_run_at`, `radar_events`,
`channel`) ya están listos. Producción = **Fase A** (un job que llama `run_radar`
a los radares vencidos) + **Fase B** (mandar el digest cuando hay novedad). Eso
convierte el pull-to-refresh actual en push real: el usuario no toca nada.

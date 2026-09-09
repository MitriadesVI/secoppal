# Aprobado V1.1: adjudicación independiente

**ACREDITADO.** La política declarada está implementada, es coherente entre
Python y SQL, y la fuente real acepta el predicado que la expresa. La
`INCONCLUSIVE` de la primera revisión queda resuelta: V1.1 dejó de inferir
inclusión sin fecha, que era el extremo no acreditado.

## Contrato adjudicado

| Señal publicada | Regla V1.1 | Comprobado |
|---|---|---|
| Aprobado + fecha ISO utilizable | incluido | sí |
| Aprobado sin fecha, inválida, centinela textual o no observable | indeterminado, **no** incluido | sí |
| En ejecución, Cerrado, Modificado, terminado, cedido, Suspendido, Prorrogado | incluidos con o sin fecha | sí |
| Cancelado, Borrador, enviado Proveedor, En aprobación | excluidos incluso con fecha | sí |
| Estado desconocido, vacío o ausente | no incluido | sí |
| Inferencia de firma, fecha o año a partir del estado | prohibida | sí |

## Matriz independiente Python ↔ SQL

[`tests/test_cu_v11_audit_aprobado.py`](../../../tests/test_cu_v11_audit_aprobado.py):
**302 casos verdes**. Quince estados (los siete posteriores, Aprobado, los
cuatro excluidos, `None`, cadena vacía, `ESTADO NUEVO`, y los señuelos
`Firmado`/`Celebrado`) cruzados con nueve formas de fecha: ISO con hora, fecha
civil, ISO con offset, `2026-02-30`, `2026-13-01`, `pendiente`, cadena vacía,
espacios y `None`.

Para **cada** combinación, `eligible()` en Python y el predicado emitido por
`predicate()` ejecutado en SQL deciden lo mismo. No hay una sola divergencia.
La propiedad se repite con Hypothesis en
[`test_cu_v11_audit_properties.py`](../../../tests/test_cu_v11_audit_properties.py),
que además comprueba que el clasificador no muta la fila.

`contract_signature.py`: Aprobado sin fecha pasa a `signature_status='unknown'`
y `signature_date_status='missing'`; los otros siete conservan su inferencia.
No se inventan umbrales de centinela (1900/1970): el catálogo vigente no los
declara para `fecha_de_firma` y V1.1 no los introduce.

## Dialecto Socrata: no sólo la emulación

La emulación SQLite (`tests/contract_universe_sql_dates.py`) registra
`date_extract_y` con `datetime.fromisoformat`; extiende el dialecto y nada más,
sin política de elegibilidad ni allowlist de estados. Por sí sola no acredita
que Socrata acepte el predicado, así que se agotaron las vías del encargo en
orden.

**Vía 1 — evidencia versionada del repo.** `docs/secop_integrado/SCHEMA_AUDIT.md:131`
y `docs/datasets/SECOP_HISTORICAL_DATASET_COMPARISON.md:365-375` conservan una
consulta real a `rpmr-utcd` con
`date_extract_y(fecha_de_firma_del_contrato) = 2026` en el `WHERE` y su
resultado agregado (`3155203479424`). Acredita que la función existe en SoQL y
opera sobre una columna de fecha de firma de un dataset SECOP, en `WHERE`.
No acredita `jbjy-vk9h` ni la forma `IS NOT NULL`.

**Vía 2 — documentación.** POLICY_DELTA enlaza `dev.socrata.com` para la
función y para Floating Timestamp. Es documentación, no observación.

**Vía 3 — cuatro lecturas públicas acotadas, read-only, sin credenciales.**
Registradas íntegras en [socrata_dialect_reads.json](socrata_dialect_reads.json).

| # | Qué | Resultado |
|---|---|---|
| 1 | `api/views/jbjy-vk9h.json`, tipos de columna | HTTP 200. `fecha_de_firma` es `calendar_date` (timestamp tipado); `estado_contrato` es `text`. Ver [jbjy_vk9h_metadata_columns.json](jbjy_vk9h_metadata_columns.json) |
| 2 | `$where=upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL`, `$limit=1` | HTTP 200. Aceptado. Devuelve `CO1.PCCNTR.1432296` / `Aprobado` / `2020-03-09T00:00:00.000` |
| 3 | la variante `IS NULL`, `$limit=2` | **timeout de cliente a los 20 s**. Un timeout no es ausencia: esta lectura no acredita nada sobre la existencia ni el número de filas Aprobado sin fecha |
| 4 | el predicado **completo** que emite `predicate()`, restringido a ese id, `$limit=1` | HTTP 200. La fuente acepta la consulta real verbatim y **admite** la fila Aprobado con fecha |

Presupuesto agotado: 4 de 4. Dos filas observadas en total. Sin barrido, sin
conteos, sin afirmación nacional.

## Qué queda acreditado y qué no

Acreditado: la fuente real acepta `date_extract_y(fecha_de_firma)` dentro del
predicado de producción sobre `jbjy-vk9h`; una fila Aprobado con fecha entra; la
columna es un timestamp tipado, de modo que no puede contener texto arbitrario
que la función deba interpretar.

No acreditado, y no se afirma: cuántas filas Aprobado sin fecha existen, su
proporción o su distribución — la lectura 3 venció. Que la regla sea
jurídicamente correcta para cada expediente. Que otros dialectos o datasets se
comporten igual. La mitad negativa del contrato («Aprobado sin fecha no entra»)
está acreditada por la matriz de paridad y por la semántica del predicado sobre
una columna tipada, no por una observación en vivo.

Mutaciones que confirman que la regla es load-bearing: quitar la exigencia de
fecha en `eligible()` mata 12 casos; quitarla del SQL mata 10, incluida la
matriz de producto. Ninguna muere por error de importación.

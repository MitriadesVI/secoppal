# Iteraciones del arnés independiente

Las primeras ejecuciones no se presentan como regresiones del producto.
Se preservan sus stdout/stderr y trazas completas para hacer auditable cada
ajuste; ninguna de ellas se usa como baseline de una mutación.

- `independent-first`: SQLite inicialmente no permitía agregados en threads,
  faltaban columna documental/función temporal y el test confundía filas
  formateadas con filas físicas. El observer también derivaba un cliente de
  fuente fuera de la simulación; los sockets quedaron bloqueados.
- `independent-second`: el arnés ya ejecutaba agregados, pero exigía llamadas
  nuevas para páginas almacenadas y filas tabulares en un perfil Actor. Se
  cambió la verificación al contrato real: IDs publicados/disjuntos, respuesta
  de perfil y filas físicas de consultas de contratos recientes.
- `independent-third` y `network-diagnostic`: las 64 aserciones pasaban; los
  errores de teardown eran telemetría Burr. Se identificó el stack exacto y
  se configuró BURR_TELEMETRY_ENABLED=false antes de las importaciones,
  conforme al código instalado de burr/telemetry.py. No se ignoró la guarda.
- `independent-boundaries-first`: la expectativa de Bogotá asumía campo ciudad,
  aunque la autoridad vigente representa Distrito Capital como departamento;
  se corrigió al valor canónico explícito. La exclusión temática se expresa
  como `NOT (COALESCE(... LIKE ...))`, no `NOT LIKE`; se conserva la comprobación
  del efecto incorrecto sobre proveedor, que se reproduce también en BASE.
- `independent-final`: 95 passed con guarda de red y recorder externo de
  excepciones. Los cuatro locales propios pasan aparte. Las cuatro mutaciones
  tienen baseline verde en el arnés corregido y fallan por producto.

Los dos errores estrictos heredados de tests del autor son distintos de estos
ajustes del arnés propio. La reproducción con telemetría OFF y contra BASE está
documentada en INDEPENDENT_REVIEW.md. No se modificaron esos tests.

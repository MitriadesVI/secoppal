# Contrato previo al cambio funcional B.2

Base af3d0c7a26182fc26599a4b764ec60fe600b17f8; ancestro candidato
af8f0a3407102dcb807089e87e470d754b835d61. Diff de auditoría sólo A;
ningún runtime ni test previo modificado. Worktree/rama nuevos solicitados,
un escritor, sin modificar otros worktrees, main ni datos. AGENTS/CARRILES y
arquitectura congelada leídos. Esta asignación específica B.2 fija la propiedad.

| Hallazgo | Causa | Contrato | Archivos / pruebas |
|---|---|---|---|
| F4 | JSON completo visible antes de terminar fsync | Sobre sin transacción pendiente y fsync completados; fallo nunca gobierna recarga | conversation_store, workflow/publication_io (extracción); once originales y nuevos fallos de confirmación |
| F5 | set de observaciones elimina multiplicidad | una observación por job, incluso no intentado; sin deduplicar | record_validation; original integrado + Hypothesis puro |
| F9 | referencia y predicados no incluyen comprador | BuyerScope completo canónico e inmutable en plan y snapshot, igualdad estructural | frame_compiler, record_validation; ambos consumidores, round-trip/grupos |
| Overflow | float acepta inf y desborda Decimal finito | monto ausente/inválido explícito, no cero; conservar documentos/conteos; ranking por valor no acreditado si hay hueco | supplier_numbers, suppliers, models, execution_record, frame_sections, render; mezcla de proveedores, seis semillas y propiedades |

## Publicación: visibilidad, durabilidad y resultado

Una pequeña barrera por chat y época se crea y sincroniza ANTES del append.
Mientras exista, ninguna línea del sobre acredita continuidad, ni historial,
ni buffer/exportación. No contiene semántica: sólo bloquea lectura durante una
transacción incompleta. El lock existente serializa lectores/escritores; la
época y el CAS se conservan. Historial/buffer privados se preparan como antes.
La confirmación elimina la barrera únicamente después del flush, fsync de
sobre y fsync del directorio. No hay operación de persistencia posterior a
esa eliminación en la ruta normal. No se suprime ningún fsync existente.

Si falla preparar archivos privados, no hay append ni barrera nueva: sigue
la autoridad anterior. Si falla crear/sincronizar barrera, queda autoridad
anterior o continuidad inválida. Si falla append (parcial/completo), flush,
fsync de sobre/directorio: la barrera existente deja continuidad inválida.
Si falla eliminar barrera, incluso después de su visibilidad, se vuelve a
crear/sincronizar la barrera antes de declarar no publicado. Si tampoco puede
rearmarse, se trunca el append de ESTA transacción bajo lock y se sincroniza
el sobre, conservando el prefijo anterior. Si ambas reparaciones físicas
fallan se lanza un error de almacenamiento no recuperable: no se devuelve
éxito ni False acreditado. Este límite no es una modalidad de éxito incierto.
No se promete supervivencia ante todos los fallos físicos simultáneos.

La barrera incluye época en el nombre: reset avanza época y hace inertes las
barreras previas sin borrar generaciones. Una nueva consulta explícita puede
publicar después de una barrera incompleta si puede completar I/O; una
continuación rechaza antes de consultar. El siguiente append usa la revisión
física, por lo que la cola fallida no se promueve. Otro chat queda intacto.
Una caída tras eliminar la barrera pero antes de que su borrado sea durable
puede volver a bloquear un éxito: recuperación conservadora, nunca promoción
de un False. Las pruebas distinguirán errores reales, post-visibilidad y
fallos dobles; no se contabilizan import errors ni timeouts.

## Instrumentación y reproducción inicial

Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10.
Codegraph 0.9.6 con Node 22: Not initialized; MCP ausente. Fallback rg/manual.
Primer montaje pytest falló al importar offline_guard; preservado separado.
Con PYTHONPATH del plugin correcto: 11 failed, 45 passed in 17.54s.
Node IDs y trazas literales en before.txt. Todos los originales quedan intactos.

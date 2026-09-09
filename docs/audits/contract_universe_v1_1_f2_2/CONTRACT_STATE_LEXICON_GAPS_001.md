# CONTRACT_STATE_LEXICON_GAPS_001

Deuda separada para el coordinador. **No corregida en F2.2.**

Casos preexistentes confirmados por la revisión F2.1 (`8dfd574`):

- `contratos prorrogados de la Alcaldía de Cartagena`.
- `contratos aprobados de la Alcaldía de Cartagena`.

Las superficies aisladas `prorrogados` y `aprobados` no producen una resolución
contractual en `resolve_estado(surface, 'contratos')`: devuelve `None`. Pueden
terminar en `objeto` en lugar de en un filtro de estado. La documentación del
probe F2.1 y la revisión final acreditan su preexistencia; `lexicon_gaps.json`
conserva la resolución actual y las observaciones de base de esa revisión.

F2.2 bloquea la composición incompleta, sin introducir vocabulario nuevo.
`app/core/estado_families.py` conserva exactamente los bytes de `b2fb22e`.
No cambiar los oráculos de Aprobado ni su política para cubrir esta deuda:
la admisión de una fila Aprobado y reconocer una superficie lingüística son
responsabilidades diferentes.

Recomendación: tratar el vocabulario en un carril propio, con pares de flexiones,
pruebas de filtro ejecutado y pruebas que impidan atribuir palabras de Actor u
objeto a un estado. No hay autorización de implementación en esta entrega.

# Contrato B.3 previo a cambios funcionales

Base auditada a7e12ebf037f962d356bb34130e180e97f97aa0e; candidato ancestro
5d715ba9e7d67aa05f703d2a280fdf9e2efde3b1. Delta exclusivamente A bajo auditoría
B.2. Rama/ruta propuestas libres, creadas sin sobrescribir. AGENTS, CARRILES,
diseño congelado, contrato/cierre B.2 y revisión/protocolo/reproducción/
reconciliación independientes leídos. La orden B.3 asigna este carril explícito.
Un escritor, sin delegación. Main tracked limpio, untracked preexistentes
preservados; existen otros procesos de agentes, ningún pytest observado.
El worktree nuevo empieza limpio y nadie más fue asignado a él. Esto acredita
el aislamiento observado, no ausencia universal de escritores futuros.

Instrumentos: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10.
Codegraph: Node predeterminado 26 rechazado; con Node 22, versión 0.9.6,
status Not initialized y query falla por falta de índice. MCP ausente.
Fallback explícito rg + lectura de store, staging, coordinación, publicador,
aspecto proveedores, assembler, renderer y registro. Disponibilidad no valida.

| Hallazgo | Invariante | Archivos previstos | Aceptación |
|---|---|---|---|
| B2-I-F4 | Un False posterior no promueve un False anterior; prefijo físico no acredita publicación | workflow/publication_io.py; pruebas/helper propios | perfil/tabla, A válido, prefijo válido/fallido, tres fallos, proceso nuevo, historial/export/cursor/siguiente turno, otro chat, éxito posterior/reset/huérfanos |
| B2-I-SUP | Cada TOP se acredita según su propia consulta y métricas de orden; toda presentación consume esa decisión | entity_profile/supplier_rankings.py (extracción pequeña), frame_sections.py, render.py; pruebas propias | overview/suppliers, cuatro daños, listas disjuntas/coincidentes parcialmente, monto ausente/inválido, consulta fallida/vacía, publicación/registro/recarga, values positivo |

## Decisión mínima de recuperación

Se elige CONSERVAR EL BLOQUEO, sin reconstruir una frontera confirmada ni borrar
los intentos anteriores. Distinciones:

- Final físico: offset para delimitar únicamente el append propio bajo lock.
- Última publicación acreditada: el sobre admisible antes de iniciar; ni JSON
  válido ni revisión/longitud bastan cuando una barrera lo niega.
- Sufijo no confirmado: puede preceder al offset y pertenece a otros intentos.
- Barrera: su presencia al entrar acredita que el prefijo está bloqueado.
- Época: barrera por época existente; reset invalida el archivo y avanza época.

Se captura la presencia de la barrera antes de rearmar. Si unlink ya fue visible
y el rearme falla, se reemplaza SOLAMENTE el append propio. Con prefijo bloqueado,
se escribe y sincroniza una marca de recuperación terminal que NO es un sobre
válido (`publication_incomplete`), conservando todos los bytes anteriores.
El lector estricto existente rechaza esa cola: continuidad inválida, sin volver
a B. Con prefijo previamente admisible se mantiene el rollback B.2 byte a byte.
Un prefijo previamente corrupto también sigue corrupto al restaurar sus bytes.
No se añaden lectores semánticos, backend, búsqueda hacia atrás ni limpieza.

| Inicio | Fallo | Fin admisible |
|---|---|---|
| A confirmado | append/fsync B | barrera; ninguna continuidad |
| A+B bloqueado | confirmación C visible, rearme falla, recuperación funciona | A+B intacto + marca terminal sincronizada; ninguna continuidad |
| A admisible | mismo fallo C | A intacto y admisible |
| cola con marca | otro fallo de confirmación y rearme | marca previa conservada; ninguna continuidad |
| cualquier cola bloqueada | publicación nueva exitosa D | D con generación propia; jamás historial/filtros de B/C |
| cualquier estado | reset | época nueva; generaciones/barreras antiguas inertes |
| cualquier estado | también falla recuperación física | PublicationRecoveryError; observar archivos/lector, no inferir bloqueo |

La marca usa el rechazo de sobres incompletos ya existente. La revisión sigue
siendo ordinal físico para CAS; no se usa para inferir confirmación. La marca
puede consumir una línea/revisión, nunca un resultado/capacidad. No se suprimen
fsync ni excepciones, ni se cambia False por True. Proceso nuevo no equivale a
caída eléctrica. Límites físicos múltiples conservan su adjudicación explícita.

## Decisión de producto por ranking

Representación inmutable mínima: respuesta disponible, vacío confirmado,
conteos válidos, montos válidos y acreditación del orden solicitado. Cantidad
requiere conteos; valor requiere montos representables. Un monto inválido en
cantidad hace PARTIAL la sección y advierte en ese TOP, conservando su ranking
por conteo y el TOP por valor válido. Un fallo de consulta no es un vacío.
No cruza montos entre consultas, reordena filas ni cambia SQL/presupuesto.
Sección y render comparten la decisión; el registro existente conserva métricas
por call_id y validez/ausencia, y las pruebas cotejan su coherencia.

## Originales y ports previstos

Los originales se conservan byte a byte y se ejecutan antes/después.
F4 exige igualdad del prefijo físico como mecanismo; el port conserva todo
el prefijo y permite únicamente la marca terminal de no confirmación.
El inyector real después de unlink y rearme fallido se mantiene.
SUP exige advertencia contra valor ante cualquier daño; el port conserva la
prohibición de contradicción y cambia esa expectativa sólo para daño en cantidad,
según la decisión explícita del usuario. No se presenta ese rojo mecánico como
vulnerabilidad restante ni como verde funcional. Cada port debe matar un mutante.
No se reabren F5/F9, ports B.1, expectativas sustituidas ni deuda del reloj.

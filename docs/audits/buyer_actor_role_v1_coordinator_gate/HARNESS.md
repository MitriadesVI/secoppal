# Corrección acotada de los dos montajes

No hay escritor concurrente en los dos archivos en ninguno de los worktrees
registrados, ni rama/worktree Test Safety accesible ni commit equivalente en
su historia global. Véase preflight.json. Los cambios se hacen sólo en este
worktree, sin incorporar hooks o trabajo de otro carril.

Antes: corpus sustituía query/count, pero aggregate y clientes derivados
alcanzaban SECOP. Render sustituía aggregate en la instancia, pero
with_timeout creaba otro cliente real. El producto absorbía las excepciones.
La reproducción estricta dio `2 passed, 2 errors`, sin permitir la salida.
Burr se desactiva por BURR_TELEMETRY_ENABLED=false antes de importar, mecanismo
verificado en burr/telemetry.py del entorno canónico.

El primer diagnóstico con el runner Frame y la guarda socket dio dos passes:
la guarda HTTP del runner interceptaba antes del socket. Se conserva esa
salida. gate_checks añade comprobación posterior del registro HTTP externo;
las violaciones de colección también cambian el exit final. La guarda del
auditor sigue comprobando sockets fuera del producto en la aceptación.

Después: una fixture explícita, usada sólo por los dos tests, instala la
frontera SQL sintética ya auditada. Query, aggregate, count, with_timeout y
derived_for_timeout comparten esa fuente. Dos contratos por Actor, 2025/2026,
600/700 millones; SQL real evaluado en SQLite. Las aserciones originales de
operaciones, identidad, tiempo y comprador se conservan. El caso de suma
además exige la respuesta real de 1.3 mil millones en dos registros. No se
simulan parser, arbitraje, admisión, plan ni render. Los errores SQL y sockets
se registran fuera del workflow y se comprueban al finalizar la fixture.

La pasada de ambos módulos, paridad y tamaño dio `53 passed in 4.95s`.
El control en copia temporal APFS dio dos passes y cero intentos; retirar
aggregate/with_timeout volvió a detectar ambos tests: 701 intentos HTTP
bloqueados, exit 1. Ver harness_removal_clone/result.json. El archivo mutado
se restauró antes de eliminar la copia. No son campañas runtime nuevas.

No se actualizó ninguna huella: los guards focales pasan sin adjudicación.
La copia inicial mediante git archive falló por ENOSPC antes de ejecutar
tests; storage_incident.json conserva ese incidente de entorno.

No se añaden skips, xfails, deselects ni expectativas que acepten defectos.
La caracterización de «sin proveedor FUNDACARIBE» sigue siendo deuda conocida,
separada de la aceptación de negación explícita soportada.

# Capacidad de disco (round 2)

El gate anterior abortó por `OSError: [Errno 28] No space left on device` durante
la suite global, con unos 438 MiB disponibles. Antes de autorizar otra corrida se
midió la necesidad en vez de estimarla.

## Medición

Un bloque real de 101 casos pesados en persistencia (`test_actor_conversation_corpus`,
`test_actor_profile_render`, `test_buyer_actor_independent_product`) se ejecutó
muestreando el scratch del runner cada 2 s. El pico fue **15,5 MiB y crece de forma
monótona**: los temporales no se liberan hasta que termina el componente. Extrapolado
a los 13.312 casos globales da entre 0,5 GiB (ritmo implícito en el 80% que alcanzó
la corrida abortada) y 2 GiB (ritmo lineal de esta muestra, sesgada hacia lo pesado).

## Temporales propios del gate liberados

Sin autorización adicional, por ser copias desechables de ESTE carril, sin procesos
con handles abiertos (`lsof`), sin `.git` y con su evidencia ya preservada en
`h4_base_direct/` y `h4_base_diagnostic/`:

| Ruta | Recuperado |
|---|---|
| `$TMPDIR/buyer-role-base-j4swahmy` | 188 MiB |
| `$TMPDIR/buyer-role-pre-frame-n7kxlw97` | 126 MiB |
| **Total** | **318 MiB** (2,47 → 2,79 GiB) |

También se eliminó, ya cumplida su función y tras extraer su evidencia, la copia
`base-runtime` del scratchpad usada para el comparativo H4 y las dos mutaciones.

## Lo que no alcanzó, y la autorización

Durante la sesión el volumen perdió ~2,3 GiB por causas ajenas a este gate (sin
archivos grandes nuevos atribuibles; ningún proceso del gate activo). Quedaron
776 MiB. Todos los volúmenes montados comparten el mismo contenedor APFS
(`disk3s*`, 460 GiB): **no había otro volumen disponible**.

Con 776 MiB no se lanzó la corrida: la orden prohíbe empezar "para ver si alcanza".
Se entregó al usuario un inventario de caches regenerables, ninguna usada por
SECOPPAL, y autorizó **exclusivamente** borrar un modelo:

| Ruta | Liberado |
|---|---|
| `~/.cache/huggingface/hub/models--yuxinlu1--gemma-4-12B-coder-fable5-composer2.5-v1-GGUF` | 9,1 GiB |

Resultado: **9,88 GiB disponibles**, entre 5x y 20x la necesidad estimada.

No se tocaron datos del usuario, índices, `.env`, evidencia histórica, untracked
preexistentes, otros worktrees ni metadatos Git. Los otros cuatro modelos de esa
cache (docling ×2, whisper-tiny, all-MiniLM-L6-v2; 664 MiB) quedaron intactos, y
no se autorizó ni ejecutó limpieza de uv, pip, Homebrew ni npm.

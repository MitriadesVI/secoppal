# Buyer–Actor V1: gate del coordinador, ronda 2

**Veredicto: BUYER_ACTOR_V1_GO_FOR_MERGE.** Los tres bloqueos de la ronda 1 se
resolvieron —capacidad, adjudicación H4 y aislamiento del opt-in del autor— y la
corrida completa terminó en verde: **17/17 componentes**, cero fallos inesperados,
cero deselects, cero intentos de red no autorizados. El runtime auditado no se tocó.

La ronda 1 (`GATE_INCONCLUSIVE`) se conserva íntegra en el directorio padre. Esta
ronda escribe exclusivamente en `round2/`.

## 1. Referencias

| Referencia | SHA |
|---|---|
| BASE / MAIN_BEFORE | `52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386` |
| Runtime probado | `aab5d41d45e58f21b727929d4ec454f8fbbbf9cd` |
| Candidato auditado | `e06c60482756e2d4238052818117568f9cd4e946` |
| Auditoría GO | `7c8cd9be7e071924763d887e8edf14c8804f5b18` |
| GATE_CODE_HEAD ronda 1 | `9f68ed90007153fcbdf4f5aaebf7eec7fbc3fd54` |
| Entrega ronda 1 | `4741f9e6b945d40b73e868afabf8de30d811f8eb` |
| **GATE_CODE_HEAD ronda 2** | **`f864b3b25e46943f79a247bf3f1314db359cdd1d`** |

Ascendencia lineal comprobada; `main` es ancestro del candidato y no divergió.
El delta ronda 1 → ronda 2 toca **sólo** dos tests, un helper de tests y el adapter
del gate. `git diff e06c604..HEAD -- app/ streamlit_app.py` está **vacío**.

## 2. Resultados literales

| Componente | Salida |
|---|---|
| Ruff `app/ tests/ scripts/` | `All checks passed!` |
| Ruff directorio del gate | `All checks passed!` |
| Ruff Python activos Frame | `All checks passed!` |
| Semgrep BASE | `Ran 3 rules on 326 files: 10 findings.` |
| Semgrep candidato | `Ran 3 rules on 327 files: 10 findings.` |
| **Suite global `tests/`** | **`13278 passed, 19 skipped, 15 xfailed in 762.72s (0:12:42)`** |
| Frame B.2 independiente | `47 passed in 17.97s` |
| Frame B.3 remediación | `303 passed in 42.66s` |
| Frame B.3 independiente | `115 passed in 35.73s` |
| Aceptación Buyer–Actor OFF/ON | `95 passed in 29.14s` |
| Opt-in autor + auditor | `8 passed in 1.99s` |
| Corpus adicional | `Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)` |
| lint-core | `app/core/ limpio` |
| diff-check | salida vacía, `EXIT_CODE=0` |
| Tamaño de módulos | dentro de la colección global (`2 passed`) |

`execution_manifest.json` agrega los diecisiete con `pass: true`. Una sola corrida
`full`, cero reruns. La suite global produjo resumen final, JSON y JUnit completos.

## 3. Colección y excepciones, por caso

Colección idéntica a la ronda 1, comparada nodo a nodo: 13.312 globales,
47 + 303 + 115 complementarios, 95 de aceptación, 8 opt-in. **Cero añadidos,
cero desaparecidos.** Retirar un marcador xfail no cambia la colección.

Fases globales: `setup:passed 13309`, `setup:skipped 3`, `call:passed 13278`,
`call:skipped 31`, `teardown:passed 13312`. **Cero fallos de call, setup o teardown**
en todos los componentes, y cero deselects.

**15 xfails**, todos deuda preexistente ajena a este carril: dos de
`test_actor_hardening_v2_1_remediation`, uno de `test_control_language`, seis de los
tres `test_demo_*` (insights de concentración y modalidad), tres de
`test_gazetteer_lote_01` y tres de `test_opportunity_queries`. La ronda 1 tenía 16:
la diferencia es exactamente el caso H4 adjudicado en §4.

**19 skips**, identificados por procedencia y ninguno una pérdida de cobertura:

- 8 en `test_actor_conversation_dogfood` — *production actor index is not available*.
- 8 de los opt-in (`test_buyer_actor_role_local_v1` ×4, `test_buyer_actor_independent_local` ×4)
  — el índice se habilita **sólo** en el componente `buyer_local`, donde los ocho
  corrieron y pasaron. No se pierden: se ejecutan en su componente dedicado.
- 2 en `test_dogfood_regressions` — recall vivo, no asertable sin red.
- 1 en `test_actor_index_isolation` — sin directorio local no hay nada que aislar.

Semgrep: **cero nuevos**, cero removidos, cero desplazamientos, cero errores de
scanner, ambos escaneos completos. Los diez son el baseline rojo heredado
`except-exception-que-silencia`; exit 0 no significa limpio.

## 4. H4: adjudicado como comportamiento correcto

`tests/test_h4_fallback.py::test_new_topic_should_clear_inherited_ordering_signal`
reprodujo XPASS(strict) frente a XFAIL en BASE. Se verificó el comportamiento, no la
ausencia del campo, ejecutando los dos turnos con transporte sintético sobre **ambos**
runtimes (`round2/h4_adjudication/`):

| | BASE `52a6bcc` | Candidato |
|---|---|---|
| Turno 1 `resolved_params` | `ordering_signal=valor_desc`, `objeto=['adulto_mayor']`, `ciudad=Cartagena` | idéntico |
| Turno 2 `resolved_params` | `{ciudad: Cali, objeto: ['adulto_mayor'], ordering_signal: valor_desc, ...}` | `{dataset, ciudad: Cali, limit}` |
| Turno 2 `ORDER BY` | `valor_del_contrato DESC, …` heredado | `fecha_de_firma DESC, …` por defecto |

BASE arrastraba a una búsqueda de **educación en Cali** tanto el orden como el tema
del turno anterior. El candidato entrega exactamente los params que produce la frase
aislada: un control de un solo turno en workflow limpio devuelve `{ciudad, dataset,
limit}` **en los dos runtimes**, de modo que la no captura de «educación» como tema es
deuda preexistente del parser, no ejecución perdida por este cambio.

El turno 2 se ejecutó de verdad: `needs_clarification=False`, `query_error=''`, una
fila, una consulta emitida sobre Cali. La aserción no puede pasar por accidente.

Controles inversos conservados, idénticos en BASE y candidato: «ahora los mas caros»
como refinamiento fija `valor_desc`; una búsqueda nueva que pide orden explícito lo
conserva. No se eliminaron todos los órdenes.

Se retiró **sólo** ese marcador. `strict=False` no se tocó, el caso no se retiró, el
runtime no se modificó, y no se declara resuelta la familia H4: `test_control_language::
test_scope_change_carrying_a_new_topic_keeps_that_topic` sigue xfail.

Mutación de control: el caso reescrito **falla** contra el runtime BASE, en la
aserción de contaminación de contexto. Ver `round2/mutation/MUTATION.md`.

## 5. Opt-in del autor: frontera externa cerrada

Causa confirmada: `source_for` sustituía métodos en la instancia, pero
`app/core/observer.py:214` agrega sobre `secop_client.with_timeout(...)`, que
construye un `SecopClient` nuevo. `derived_for_timeout` ya devuelve `self` al detectar
métodos mockeados; `with_timeout` no. Cinco agregados por caso salían a la red:
`8 passed, 4 errors`, veinte intentos POST bloqueados.

Se reutilizó la solución de `tests/buyer_actor_independent_support.py`: atar
`with_timeout` y `derived_for_timeout` al mismo cliente, y hacer la fuente SQLite
compartible entre hilos con lock, porque el observer agrega en cinco hilos.

No se simuló nada del trabajo bajo prueba. Los agregados **entran** ahora a la fuente
sintética: **6 llamadas evaluadas en SQLite por caso en vez de 1**, `violations: null`,
y cada agregado conserva comprador y monto —la guarda estricta del fixture cubre cinco
consultas más, no menos—. Se conservan índice en solo lectura, las dos menciones
autorizadas, las comprobaciones de tamaño/mtime/inode, SQL, comprador, monto, respuesta
y flags efectivos, y el registro externo comprobado después del producto.

Los ocho opt-in: `8 passed`, cero errores de teardown, cero intentos externos.
Consumidores focales del helper compartido repetidos: `135 passed` sobre
`test_buyer_actor_role_arbitration_v1`, `…_controls_v1`, `…_properties_v1`,
`…_local_v1`, `test_buyer_actor_independent_local` y `test_h4_fallback`.

Mutación de control: retirar las dos líneas reproduce `4 passed, 4 errors` con veinte
intentos bloqueados. Los cuerpos siguen pasando porque el observer absorbe la
excepción; lo que falla es el registro externo. La detección no fue desactivada.

No se incorporó el carril Test Safety ni se instalaron hooks.

## 6. Entorno, red y preservación

Frame OFF y Actor ON como entorno inicial, dejando a las fixtures cambiarlos: en la
suite global el lector Actor fue `True` en los 13.309 casos que llegaron a `call`, y
el de Frame `False` en 13.044 y `True` en 265 por fixtures. La aceptación Buyer–Actor
registra dentro de workflows reales **81 trazas con Frame efectivo False y 81 con
True**, todas con Actor True: las comprobaciones OFF/ON se conservan. No se activó
el default ON ni el botón del Frame; ese cambio sigue separado.

Red: **216 peticiones, todas autorizadas, cero denegadas**, exclusivamente `POST` a
`www.datos.gov.co` en `/api/v3/views/jbjy-vk9h/query.json` y
`/api/v3/views/p6dx-8zbt/query.json` —las lecturas públicas SODA3 previstas por la
suite histórica—. Los 465 complementarios, los 95 de aceptación y los 8 opt-in
registraron **cero** conexiones. Burr apagado antes de importar, secretos retirados,
persistencia JSONL/SQLite, conversaciones, índice ambiental, Hypothesis y temporales
redirigidos. Sin LLM de pago, mensajes, radares reales, app, reinicios ni despliegue.

Preservación: 1.521 archivos vigilados, hashes **idénticos** antes y después, cero
`original_failures`. `code_frozen` verde: el código no cambió durante la corrida.
Postflight: `MAIN_AFTER` seguía en `52a6bcc` al terminar el gate, `HEAD` en
`f864b3b`, `tracked_status` vacío.

## 7. Capacidad

Ver `round2/CAPACITY.md`. Se liberaron 318 MiB de temporales propios de este carril;
el volumen perdió después ~2,3 GiB por causas ajenas al gate, y con 776 MiB **no se
lanzó la corrida**. Tras la autorización específica del usuario para borrar un único
modelo regenerable, se dispuso de 9,88 GiB. La corrida consumió un pico modesto y
terminó con 9,79 GiB libres. No se tocaron datos del usuario, índices, `.env`,
evidencia histórica, untracked preexistentes, otros worktrees ni metadatos Git.

## 8. Límites y deudas abiertas

- El baseline Semgrep sigue rojo: diez `except-exception-que-silencia` heredados.
- Los 15 xfails y los 19 skips listados en §3 siguen abiertos; ninguno pertenece a
  este carril salvo el H4 adjudicado, que dejó de serlo.
- «Educación» no se captura como tema en el parser, en BASE y en el candidato por
  igual. Deuda preexistente, fuera del alcance de este encargo.
- `with_timeout` no preserva métodos mockeados como sí hace `derived_for_timeout`.
  Aquí se compensó desde el test; el asimetría en `app/core/secop_client.py` sigue
  ahí y volverá a morder a cualquier fixture que parchee la instancia.
- Siguen fuera y separados: negación corta, ortografía de nombres, ranking fuzzy,
  Radar, contador de traza, P2 de publicación, y el botón/default ON del Frame.
- No se emite PRODUCTION_GO. Este gate acredita la integración, no la activación.

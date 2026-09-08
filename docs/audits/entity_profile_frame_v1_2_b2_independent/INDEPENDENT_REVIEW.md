# SECOPPAL — revisión independiente de cierre V1.2-B.2

**Veredicto: REMEDIATION_REQUIRED.** Los once contraejemplos residuales originales están verdes, pero el protocolo de publicación y la honestidad del ranking de proveedores conservan brechas reproducidas. F5 y F9 cierran en el alcance exigido. No se concede `V1_2_B2_GO_FOR_COORDINATOR_GATE` ni autorización productiva.

| Referencia | SHA |
|---|---|
| Candidato auditado | `5d715ba9e7d67aa05f703d2a280fdf9e2efde3b1` |
| HEAD probado declarado | `aaa9168b75a5633469af507a1cf7c1c2e2c1ce00` |
| Base / auditoría B.1 | `af3d0c7a26182fc26599a4b764ec60fe600b17f8` |
| Runtime B.1 | `af8f0a3407102dcb807089e87e470d754b835d61` |

Rama nueva `audit/entity-profile-frame-v1-2-b2-independent`; worktree nuevo `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b2-independent`. El HEAD final es el commit que contiene esta entrega, consignado completo en la respuesta final y obtenible con `git rev-parse HEAD`. No se incluye una autorreferencia imposible dentro del propio commit.

Sólo se añadieron pruebas, helpers y evidencia en este directorio. Un auditor, sin delegación. Se leyeron AGENTS, CARRILES, diseño congelado, revisión/reconciliación B.1, contrato/resultados/remediación B.2, los cinco tests independientes B.1 y los tres B.2. Se comprobaron ascendencia, delta, consumidores y evidencia vinculada. `integrity.json` verifica 89 logs históricos comprimidos y 2.457 archivos de la base ajenos al delta runtime. `candidate_sha256.json` verifica los 2.550 archivos del candidato contra sus blobs Git. Probado→final sólo añade evidencia B.2: árboles app/tests y blob Streamlit idénticos. La auditoría no edita originales, catálogos, configuración, guardas, documentos compartidos ni datos del usuario.

## Hallazgo B2-I-F4 — P1 — rollback de un reintento promueve un turno ya declarado no publicado

**Requisito:** un turno con `continuation_published=False` no puede gobernar Frame, historial, cursor, exportación ni la siguiente continuación; el rollback limitado al append propio debe conservar ese invariante.

**Reproducción integrada, sin mocks de publicación, almacenamiento o validación:**

1. A publica un resultado válido: perfil de Baranoa/agosto o tabla contractual de obra/Baranoa.
2. B ejecuta otro universo: perfil de Galapa/julio o procesos de alimentación/Bogotá. Se permite el append completo real y se lanza `OSError` en el fsync del descriptor del sobre, identificado por inode. B devuelve False y el aviso «No pude guardar el contexto». La barrera existe; otro proceso no admite B ni su historial.
3. C intenta una publicación explícita. Se permite crear/sincronizar la barrera y escribir/sincronizar su sobre. `Path.unlink` retira realmente la barrera y después informa error. Reabrir la barrera para rearmarla falla antes de crearla.
4. La recuperación abre realmente el sobre en `r+b`, trunca sólo C al offset anterior y sincroniza correctamente. Los bytes quedan exactamente iguales al prefijo A+B. C también devuelve False. Un segundo chat publica durante esa frontera y conserva su resultado.
5. Una instancia y un proceso nuevos recargan B como publicación vigente. El siguiente turno actúa desde B.

**Esperado:** conservar A si aún es admisible, o bloquear continuidad; nunca admitir B. **Observado:** B pasa a OK porque su revisión 2 coincide con las dos líneas restantes y ya no existe barrera. En perfil, «proveedores» hace dos consultas con GALAPA y julio. En tabla, el historial vuelve a incluir alimentación/Bogotá, `export_params` devuelve ese universo y «más» hace seis llamadas sobre sus procesos; en la corrida final termina en `pagination_exhausted`, sin filas. La afirmación es ejecución/capacidad del universo fallido, no entrega de filas inexistentes ni creación de un Radar real.

**Evidencia:** `test_publication_sequences.py::test_rollback_of_retry_never_promotes_an_earlier_failed_turn[True-False]` y `[True-True]`; orden de parámetros: `failed_prefix`, `profile`. `own_verified.txt.gz` conserva IDs, lectura del proceso, historial, export y llamadas. `[False-False]` y `[False-True]` son controles verdes con prefijo confirmado. No hay import errors, fallos de montaje ni timeouts en estos cuatro casos finales.

**Causa y alcance:** `app/core/workflow/publication_io.py:38` toma el tamaño físico, que puede incluir un append anterior no confirmado. La recuperación `:59–69` elimina sólo el append propio sin preservar el estado de no confirmación del prefijo. `conversation_store.py:435` acepta después la última línea y su revisión. La localidad del truncate se cumple; la localidad no acredita que ese prefijo esté publicado. Es una brecha residual en la nueva recuperación B.2, demostrada en el workflow y otra lectura de proceso. No depende de pérdida eléctrica, de que falle el truncate ni de `PublicationRecoveryError`.

## Hallazgo B2-I-SUP — P2 — una respuesta afirma y niega la acreditación del ranking por valor

**Requisito:** conservar hechos y proveedores válidos, identificar los montos no acreditables y no presentar como acreditado un ranking que la propia evidencia declara no acreditado.

**Reproducción integrada:** perfil contractual Baranoa/agosto; el TOP por cantidad devuelve NIT 900123456, tres contratos y monto `Infinity`. El TOP por valor devuelve un proveedor distinto, NIT 800123456, un contrato y monto 700.000.000. Los TOP5 son consultas independientes y no tienen que incluir los mismos proveedores. El resto de fuentes conserva sus respuestas sintéticas válidas.

**Esperado:** el juicio sobre acreditación del ranking debe ser consistente dentro de la respuesta publicada, manteniendo identidad, conteos y montos válidos. **Observado:** overview publica «Principal contratista por valor: PROVEEDOR VALOR ($700 millones)» y también «el ranking por valor no está acreditado». Suppliers publica «Principales contratistas por valor contratado» y la misma advertencia negativa. La sección es PARTIAL; el registro conserva un monto de proveedor VALID y el otro INVALID/None. Se conservan presupuestos 4/2, publicación y siguiente turno tras recarga; no hay aborto ni cambio de universo.

**Evidencia:** `test_supplier_topologies.py::test_ranking_claim_and_published_warning_are_consistent[count-overview]` y `[count-suppliers]`, en `own_verified.txt.gz`. Los controles sin daño, daño sólo en TOP por valor y daño en ambos TOP pasan. La prueba observa el assembler real mediante un wrapper que ejecuta su implementación; los registros y respuestas son los publicados realmente.

**Causa y alcance:** `frame_sections.py:283–291` declara no acreditado el ranking completo al ver cualquier monto ausente en cualquiera de las dos consultas. `render.py:84` y `:117` comprueban sólo los miembros del TOP por valor para hacer la afirmación positiva. La nueva advertencia y las guardas de B.2 no expresan la misma condición. El hallazgo se limita a esa contradicción explícita introducida por el delta; no adjudica un orden alternativo, no calcula un nuevo ranking ni exige un techo monetario de negocio.

## Cuatro cierres y utilidad

| Cierre | Evidencia favorable reproducida | Adjudicación |
|---|---|---|
| F4 publicación | Fronteras originales; barrera write/fsync/directorio; unlink antes/después; rearme fallido con prefijo válido; recuperación API de cola completa/parcial; lectura de proceso; aislamiento por chat; reset/épocas | **PARCIAL**, B2-I-F4 bloqueante. Protocolo y límites en `PUBLICATION_PROTOCOL.md`. |
| F5 observaciones | Cardinalidad+membresía; exact copy, distinta observación del mismo job, reemplazo por duplicado, ausente, ajena y jobs duplicados aun con digest recalculado; propiedad determinista; registros reales válidos | **CERRADO** en las variantes exigidas. Rechazo previo a consultas, sin deduplicar/reparar el prefijo. |
| F9 comprador | Assembler/renderer con comprador ajeno e IDs iguales; BuyerUnit/BuyerGroup, todos sus campos y kind; copia serializada equivalente; snapshot alterado con digest recalculado; ausencia del vínculo | **CERRADO**. No re-resolución ni recompilación en la validación de recarga. Consistencia, no autenticidad criptográfica. |
| Proveedores | Infinity/1e999 dejan de abortar; -Infinity, NaN, ausencia, vacío, malformado, underflow; identidad/conteos/montos válidos; registro honesto; values; montos finitos incluidos 1e100 y 1e308 | **PARCIAL**: overflow cerrado; honestidad del ranking incompleta por B2-I-SUP. |

F5 conserva publicaciones ejecutadas, con fallo parcial y total, pending inicial y pending con ejecución conservada. Una ejecución real `BUDGET_EXHAUSTED` no intentada publica cuatro observaciones y recarga; no se equipara ausencia de intento a registro incompatible. Las dos propiedades deterministas de duplicación vuelven a pasar. Los daños propios se aplican después de una publicación válida y conservan el digest coherente cuando corresponde. Una aclaración nueva puede añadirse al JSONL; el prefijo dañado queda intacto, sin convertirlo en evidencia reparada.

F9 compara el BuyerScope estructural completo: kind, display_name, canonical_buyers, authority, authority_version y group_id. Los 14 daños de snapshot propios se rechazan, incluidos los dos registros sin `buyer_scope`. No se exige recuperar esos snapshots antiguos. `validate_record` no recibe builder ni resolver; la inspección de su flujo y consumidores confirma que coteja el vínculo persistido. Una reescritura coherente de Frame, plan, evidencia y digest del almacén completo queda fuera de la promesa.

El resto de regresiones se detalla en `CLOSURE_MATRIX.md`. F1/F2/F3/F6/F7/F8/F10 conservan los controles vigentes, incluidos Actor/token, UI, migración/rollback, exclusión entre procesos y Radar SAVE/LIST/RUN sintéticos. No se reabren las tres sustituciones B.1 ni las dos expectativas del autor que F5 reemplazó.

## Resultados independientes y deudas

| Target | Salida literal |
|---|---|
| Aceptación original + B.2 + consumidor monetario | `130 passed in 41.03s` |
| Históricos vigentes + cuatro controles + ports | `105 passed in 33.83s` |
| Históricos intactos | `5 failed, 79 passed in 33.59s` |
| Pruebas propias finales | `4 failed, 47 passed in 21.87s` |
| Amplio intacto | `3 failed, 1902 passed in 138.86s (0:02:18)` |
| Adyacentes | `472 passed, 2 xfailed in 14.98s` |
| Corpus | `276 passed in 1.19s` |
| Utilidad | `15 passed in 3.89s` |
| Núcleo/A2/propiedades | `1 failed, 407 passed in 2.21s` |

Los tiempos literales deben consultarse también en cada log; `reconciliation_manifest.json` fija node IDs y estados desde XML. No se suman targets superpuestos. La unión de 2.654 node IDs es inventario de ejecución, no 2.654 protecciones verdes. Dos casos propios de `PublicationRecoveryError` son observacionales; de los 47 verdes propios, 45 son controles de producto y dos describen el límite. Los cinco probes históricos también permanecen separados.

**Ajuste del manifiesto del autor:** hoy el target amplio tiene un tercer rojo histórico, además de las dos expectativas adjudicadas. `test_entity_profile_frame_integration_contract.py::test_a_relative_period_does_not_get_reinterpreted_on_reload` espera `2026-09-07`, pero observa `2026-09-08`. El mismo test intacto falla igual en una exportación de la base B.1 (`clock_base.txt.gz`). El helper fija la fecha para QueryRouter, pero omite `today` al llamar `collect_claims`, mientras su aserción exige la fecha antigua. No es una regresión B.2 ni una nueva falla del reloj inyectado del workflow: `test_the_injected_clock_is_the_one_that_resolves_relative_periods` pasa. La deuda del montaje se entrega al coordinador, sin editarlo, archivarlo como caso reemplazado ni ocultarlo en deselect. Por tanto, la afirmación literal «los únicos rojos históricos son los dos explicados» ya no es cierta en la fecha de esta auditoría.

Nueve mutaciones nuevas: nueve baselines verdes, nueve cambios conductuales detectados, hashes app restaurados tras cada mutación. El auditor revisó las aserciones: autoridad recargada, consultas tras corrupción, rechazo de consumidores, aborto de respuesta, estado de métricas y ranking visible. `mutations.json` fija candidato, anchors, reemplazos, targets y hashes. No se contaron import errors, montajes fallidos ni timeouts. La adjudicación histórica sigue siendo 25 detecciones conductuales y dos diferencias de etiqueta; no se repitió esa campaña ni se sumó a la nueva.

## Puertas y preservación final

Salidas literales de las tres puertas:

```text
All checks passed!
Ran 3 rules on 325 files: 10 findings.
4 failed, 47 passed in 21.87s
```

Ruff 0.16.4 global app/tests/scripts, más Ruff del directorio propio. Semgrep 1.176.0: baseline global rojo diez, candidato diez, delta cero por regla/ruta/fragmento completo; ambos scans ejecutados independientemente y guardados en `semgrep*.json` y `semgrep_delta.json`. No se confunde su exit 0 con una puerta global verde. lint-core: `app/core/ limpio`. Tamaño: `2 passed in 0.17s`. Diff-check: sin salida, exit 0. No se modificaron límites.

Hypothesis 6.165.10 ejecutó propiedades de núcleo/B/B.1/B.2, duplicación intentada/no intentada y representación monetaria. Codegraph 0.9.6 con Node 22: `Not initialized`; query no disponible y MCP ausente. Fallback explícito `rg` y lectura manual, con mapa guardado en `consumers.txt.gz`. Disponibilidad e índice son hechos separados.

El flag Frame productivo sigue OFF. Los escenarios integrados usan la configuración real ON por fixture; otros controles verifican OFF y rollback. Los JSON `*_flags.json` muestrean el flag al entrar al cuerpo del test, no cada cambio posterior: los seis originales de overflow lo encienden dentro de su cuerpo. No se atribuye ON global al target amplio.

**Corrí esos targets, no la suite.** No app, pytest global, SECOP/LLM live, mensajes, radares reales, datos del usuario, merge, push ni activación. Persistencia temporal, fuentes/narración sintéticas y plugin offline que prohíbe sockets IP. Fuera de carril quedan los diez findings Semgrep, dos xfails Actor y la deuda del reloj histórico. Las primeras iteraciones de pruebas propias y de scripts se conservan y adjudican en `REPRODUCE.md`; no se cuentan como defectos del producto.

Quedan por remediar B2-I-F4 y B2-I-SUP, corregir por el coordinador el montaje temporal ajeno y ejecutar la reconciliación activa/gate global después de una nueva revisión. Esta auditoría entrega el mapa exacto, no efectúa esas modificaciones ni propone un backend alternativo.

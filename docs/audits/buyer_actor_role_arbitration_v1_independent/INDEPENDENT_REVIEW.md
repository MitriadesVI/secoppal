# Revisión independiente focal Buyer–Actor V1

**Veredicto: BUYER_ACTOR_V1_GO_FOR_COORDINATOR_GATE.**

Las dos puertas restauran el comprador, contratos y monto, con respuesta y
continuidad acreditadas. No encontré una regresión bloqueante del delta en los
contrastes ejecutados. Este veredicto no es PRODUCTION_GO ni acredita la suite
global. Se entregan dos defectos heredados del arnés al carril Test Safety y la
negación corta incorrecta al coordinador; no se corrigen en esta auditoría.

## 1. Objeto auditado y preservación

- Candidato: `e06c60482756e2d4238052818117568f9cd4e946`.
- Código probado declarado: `aab5d41d45e58f21b727929d4ec454f8fbbbf9cd`.
- BASE: `52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386`.
- Referencia pre-Frame: `7868664757b28bed16c583ea50327d669f1501e3`.
- Rama propia: `audit/buyer-actor-role-arbitration-v1-independent`.
- Worktree propio: `/Users/rodrigoortiz/Documents/secoppal-buyer-actor-role-arbitration-v1-independent`.

BASE y probado son ancestros del candidato. Probado→final sólo cambia
documentación/evidencia: cero cambios en `app/`, `tests/` y `scripts/`.
Los 14 hashes del manifiesto del autor coinciden con los archivos auditados.
`preservation-before.json` congela todos los archivos versionados del candidato;
`preservation-final.json` verifica que sus bytes siguen intactos. El HEAD de
entrega se obtiene con `git rev-parse HEAD` y se declara en el mensaje final;
los commits propios sólo añaden
pruebas/helpers/scripts independientes y este directorio.

Leídos AGENTS.md, CARRILES.md, REPORT, scope, commands, adjudicación, manifiestos,
evidencia antes/después e historia. No se modificaron main, el implementador,
Test Safety, runtime, tests históricos, catálogos, índices ni configuración.
Main y el implementador conservaban sus SHAs y tracked/staged limpios en el
postflight; los untracked preexistentes de main no se eliminaron.

## 2. Evidencia de producto independiente

El harness propio usa QueryRouter, resolución de comprador, builder y resolver
SQLite de aliases, admisión, workflow, planificación, constructor SQL y
ConversationStore reales. Únicamente se simulan las fronteras de fuente y
narración externa. Cada turno reconstruye el workflow y recarga persistencia.
La fuente sintética evalúa el SQL en SQLite; contiene señuelos de comprador,
monto, año y documento. También ejecuta los agregados y el conteo, con soporte
para las funciones SoQL empleadas. La consulta de una página puede servirse
desde el buffer publicado: no se exige artificialmente una nueva llamada.

El fixture registra excepciones fuera del workflow y las comprueba después de
`run_query`. Se verifica el WHERE, no la mera presencia de documento_proveedor
en SELECT. Los resultados formateados se contrastan con los resultados físicos
y los IDs; un perfil Actor se verifica por respuesta/agregados/contratos
recientes, no suponiendo que tenga filas tabulares.

| Garantía | Resultado independiente |
|---|---|
| Cartagena: «contratos de alcaldia de cartagena por mas de 500 millones» | 5 estados del índice × OFF/ON; comprador canónico, contratos, mínimo 500000000, sin Actor/pending/contratista, WHERE enviado correcto |
| Barranquilla: «que ha contratado alcaldia de barranquilla por mas de 2000 millones» | Mismas 10 combinaciones; mínimo 2000000000, sin tema espurio «contratado» |
| Contexto y continuidad de ambas semillas | En las 20 combinaciones: semilla → solo 2026 → muéstrame más, workflow nuevo por turno, mismo comprador/monto, año correcto y páginas disjuntas |
| Actor legítimo | FUNDACARIBE bare/verbal/rol explícito, documento, único/ambiguo, relación con Cartagena, institución proveedora UIS/Cartagena y nombre largo con alias institucional parcial |
| Nueva consulta tras Actor | Actor confirmado y pendiente × ambas semillas × OFF/ON; consulta nueva, año y paginación sin heredar documento |
| Refinamiento dependiente | «solo 2026» y «con la alcaldia de cartagena» conservan el Actor confirmado; también reproducido en BASE |
| Perfil institucional | Tras FUNDACARIBE, «analiza contratos de alcaldia de cartagena en 2026» conserva análisis institucional; Frame ON publica propietario ENTITY_PROFILE |
| Variantes | Artículos, tildes, mayúsculas, software+año, orden por valor, Galapa y Baranoa con expectativas canónicas explícitas |
| Autoridad local por mención | Nombre largo con prefijo de Cartagena y UNIVERSIDAD SOLANO con evidencia fuzzy conservan Actor/aclaración; veto global mutado cambia producto y es detectado |
| Ciudad | «contratos de barranquilla» parte de city_to_entity real y termina sin comprador/Actor; la variante verbal explícita conserva Actor. Bogotá conserva territorio, sin comprador solicitado |
| Tema y negación soportada | Homónimo software conserva aclaración; «contratos no de alcaldia de cartagena» y «donde el proveedor no sea FUNDACARIBE» no ejecutan |
| Hypothesis | 35 ejemplos deterministas propios: tipos documentales, cardinalidad, orden, frecuencia e identidades ajenas. Comprador, dataset, monto y WHERE positivos fijos; no se usa la salida inicial como único oráculo |

`MATRIX.md` y `matrix.json` separan resultado, resolución, SQL, respuesta y
continuidad. Las trazas completas se conservan en archivos tar.gz por ejecución.

### Flags efectivos e índice real

El literal `actor=True` de `flow_for` no se usa como evidencia. El harness propio
invoca `actor_profile_enabled()` y `entity_profile_frame_enabled()`, limpia la
caché de Settings y registra sus valores junto con Settings. Los controles
positivos resuelven el documento 71000001 y ejecutan WHERE documental; no
podrían pasar con Actor apagado. Las pruebas históricas de kill-switch también
se ejecutaron en el manifiesto focal.

Pasaron por separado las cuatro pruebas opt-in del autor. Otras cuatro propias
verificaron las dos menciones locales, los lectores efectivos, producto y
preservación de tamaño/mtime_ns/inode. Sólo se hicieron lookups exactos acotados
de Cartagena y Barranquilla; no se exportaron identidades, no se recorrió ni
reconstruyó el índice. Devuelve 20 y 4 candidatos respectivamente, lo cual no
afirma el total de identidades. **Índice real + transporte sintético; no SECOP
live.** Los casos reparados no necesitan una resolución de proveedor.

## 3. Falsificación y reproducción histórica

Cada mutación se ejecutó sobre una exportación desechable del candidato, después
de pasar exactamente los mismos targets sin mutación. Ningún kill se atribuye a
setup, imports, timeout o sólo route_reason.

| Mutación | Baseline | Mutante | Diferencia observable |
|---|---|---|---|
| Retirar protección verbal | 2 passed | 2 failed | Barranquilla pasa a selección Actor ambigua, cero SQL de la búsqueda |
| Retirar protección bare | 2 passed | 2 failed | Cartagena pasa a selección Actor ambigua, cero SQL de la búsqueda |
| Sustituir autoridad por mención por veto global entidad_resolved | 4 passed | 4 failed | Pierde aclaración/Actor y ejecuta búsqueda tabular por comprador parcial/fuzzy |
| Retirar separación de contexto en parsing y external_admission | 8 passed | 4 failed, 4 passed | Después de Actor confirmado conserva FUNDACARIBE y ejecuta su WHERE documental; casos de Actor pendiente sobreviven por otras guardas |

`mutations.json` registra comandos, salidas y hashes restaurados. Todos los
archivos mutados se restauraron y verificaron antes de eliminar las copias.

Reproducción propia pre-Frame: 10 combinaciones OFF; BASE: 20 OFF/ON. Con alias
único ambas semillas ejecutan el perfil/documento indebido; con varios exactos
piden selección y no ejecutan. Barranquilla también aclara con índice vacío,
ausente o incompatible. Cartagena sin homónimo/índice sí entrega comprador,
pero consulta innecesariamente el directorio. Por eso los 20/10 tests rojos
históricos **no equivalen a 20/10 secuestros**: BASE tiene 14 casos de rol/ejecución
incorrectos y 6 de lookup innecesario; pre-Frame, 7 y 3 respectivamente.
Los cuatro módulos Actor responsables no cambian entre pre-Frame y BASE. No
apareció una contradicción que justificara recorrer los commits de agosto.

## 4. UIS y negación corta

El cambio de expectativa UIS está justificado por el contrato del encargo:
«qué ha contratado» no acredita por sí solo el rol proveedor. La única rama
nueva del test histórico corresponde al par textual UIS/verbo. El fixture YAML
de identidad permanece idéntico a BASE y el hash original del test coincide con
la adjudicación del autor. Se ejecutó el resto del corpus y un control integrado
positivo de UIS explícitamente proveedora, único y ambiguo, OFF/ON. Evidencia:
`uis-preservation.json`, corpus focal y pruebas propias de Actor.

**Negación heredada incorrecta, no aceptación:** «contratos sin proveedor
FUNDACARIBE» produce `objeto_exclude=['fundacaribe']` tanto en BASE como en el
candidato, OFF/ON. Su WHERE es una negación de LIKE sobre objeto_del_contrato y
descripcion_del_proceso; no excluye documento ni nombre del proveedor. La
fuente sintética demuestra que siguen apareciendo contratos de FUNDACARIBE
cuyo objeto dice «software audit». No es una regresión introducida ni agravada
por este delta. Requiere adjudicación/corrección separada y no se oculta tras
un verde de las dos semillas.

## 5. Puertas reproducidas y límites

Preflight: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10, Codegraph 0.9.6
ejecutado con Node 22. `codegraph status`: **Not initialized**. MCP no cargado;
fallback declarado a rg/Git/lectura manual para los dos helpers, QueryRouter,
Actor resolution, parsing, external_admission y consumidores focales. No se
modificó ni generó un índice Codegraph.

Salidas literales principales:

```text
All checks passed!
Ran 3 rules on 327 files: 10 findings.
1718 passed, 5 skipped, 2 xfailed in 130.22s (0:02:10)
530 passed, 2 xfailed in 80.26s (0:01:20)
4 passed in 1.61s
95 passed in 30.18s
4 passed in 1.11s
Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
2 passed in 0.12s
app/core/ limpio
```

La salida final propia y el opt-in propio se encuentran en `validation-summary.json`
y sus stdout íntegros. Corrí los manifiestos focales documentados, las pruebas
propias de producto/fronteras/Hypothesis/locales y tamaño, **no la suite**. Los
targets se solapan y sus totales no se suman. Diff-check se conserva vacío y
con exit 0. Ruff se ejecutó globalmente también después de crear los helpers.

Semgrep conserva **10 hallazgos globales heredados**, iguales por regla, ruta y
fragmento, **cero nuevos y cero removidos**, según `semgrep-delta.json`. BASE
escanea 326 archivos y candidato 327 por el módulo nuevo. Una primera ejecución
en un export sin raíz Git produjo un falso delta por la exclusión de
app/config.py; se preservó esa salida y se repitió en un repositorio temporal
para igualar las condiciones. No se atribuye esa diferencia de instrumento al
código. Exit 0 sin --error no significa scan limpio.

Skips focales: uno por ausencia del directorio en el worktree aislado y cuatro
opt-in locales, posteriormente ejecutados aparte. Xfails: los dos estrictos
preexistentes de Actor+región+comprador, sin nuevos xfails, skips ni deselect
introducidos para esconder fallos. Los deselect de mutaciones/historia sólo
acotan targets anunciados; no son gates completos.

### Hallazgo para Test Safety

El plugin del autor lanza AssertionError al intentar red, pero no comprueba
fuera del producto si la excepción fue absorbida. Una pasada estricta añadió
4 errores de teardown a los 1718 passed. Al desactivar la telemetría de Burr
**antes de importar**, se identificaron dos incidentes de telemetría y dos
defectos reproducibles del arnés:

1. `test_actor_conversation_corpus.py::test_all_expanded_sequences_mutate_real_query_frames`:
   agregados de Actor llegan al cliente SECOP real y sus excepciones se absorben.
2. `test_actor_profile_render.py::test_a_specific_aggregate_question_still_gets_its_specific_answer`:
   una consulta de agregado también intenta la frontera de red.

Con telemetría OFF, los dos se reproducen en BASE: `2 passed, 2 errors`;
en el subconjunto candidato de cuatro tests: `4 passed, 2 errors`. No son
regresiones de este delta. La guarda independiente bloqueó los sockets y
conservó stacks completos. El arnés propio simula consulta, agregado, conteo y
clientes derivados, y comprueba las violaciones después del workflow. Mapa para
Test Safety en REPRODUCE.md; no se alteró su infraestructura ni sus worktrees.

## 6. Qué quedó fuera

Sin remediación runtime, negaciones, ortografía, ranking fuzzy, Radar, contador
de trazas ni P2 de publicación. Sin pytest global, SECOP live, LLM, mensajes,
radares reales, arranque/reinicio de app, activación, merge o push. La puerta
global única y la decisión de integración corresponden al coordinador. Las
garantías aquí se limitan a los casos y mutaciones enumerados; no se convierten
en una afirmación universal sobre gramática o proveedores.

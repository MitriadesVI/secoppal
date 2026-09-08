# Buyer–Actor V1: gate del coordinador

**Veredicto: GATE_INCONCLUSIVE. No GO para merge.** La única corrida global
abortó por ENOSPC sin resumen final. Además, se reprodujeron un XPASS estricto
nuevo de H4 y un montaje local del autor que todavía intenta agregados externos.
La aceptación independiente Buyer–Actor y los 465 complementarios Frame pasan,
pero no sustituyen la acreditación global ni el componente local completo.

## 1. Cambio y alcance

Se corrigieron exclusivamente los dos montajes autorizados:

- `tests/test_actor_conversation_corpus.py::test_all_expanded_sequences_mutate_real_query_frames`.
- `tests/test_actor_profile_render.py::test_a_specific_aggregate_question_still_gets_its_specific_answer`.

Ambos usan una fixture explícita con fuente SQLite sintética, consulta,
agregado, conteo y clientes derivados. Se preservan sus aserciones funcionales;
el render también acredita 1.3 mil millones en dos registros sintéticos.
El registro externo y los errores SQL se comprueban después del producto.
No se simulan parser, arbitraje, admisión, planificación ni render.
HARNESS.md contiene antes/después y la retirada controlada del aislamiento.

Antes, la guarda estricta reprodujo `2 passed, 2 errors`. Después, los dos
módulos, paridad y tamaño dieron `53 passed in 4.95s`. En copia temporal:
baseline dos passes y cero intentos; retirar aggregate/with_timeout produjo
701 intentos HTTP bloqueados, errores en los dos casos y exit 1. El helper
temporal se restauró. No se ejecutaron nuevas campañas de mutación runtime.
Otros guards acotados dieron `40 passed in 6.32s`.
**No fue necesario actualizar huellas.** No se cambió ningún skip/xfail/deselect.

Leídos AGENTS.md, CARRILES.md y los informes del autor/auditor. No apareció
un worktree/rama Test Safety accesible, modificación concurrente de los dos
tests en los worktrees registrados ni commit equivalente en la historia global.
No se incorporaron hooks ni trabajos de ese carril.

## 2. Referencias y preservación

| Referencia | SHA |
|---|---|
| BASE / MAIN_BEFORE / MAIN_AFTER | `52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386` |
| Runtime probado | `aab5d41d45e58f21b727929d4ec454f8fbbbf9cd` |
| Candidato auditado | `e06c60482756e2d4238052818117568f9cd4e946` |
| Auditoría GO | `7c8cd9be7e071924763d887e8edf14c8804f5b18` |
| GATE_CODE_HEAD | `9f68ed90007153fcbdf4f5aaebf7eec7fbc3fd54` |

La ascendencia completa está comprobada. Main no avanzó ni diverge; sería
compatible por ascendencia, pero **no se acredita un merge**. El delta de la
auditoría sólo añadió pruebas/helpers/scripts/evidencia. De sus 3.335 archivos,
sólo difieren los dos tests autorizados; app/, Streamlit, informes originales y
runner Frame siguen idénticos. audited_preservation.json y postflight.json
acreditan hashes antes/después. No se tocaron otros worktrees ni sus untracked.

Rama `integration/buyer-actor-role-v1-gate`, worktree
`/Users/rodrigoortiz/Documents/secoppal-buyer-actor-role-v1-gate`.
HEAD final es el commit documental de entrega, resuelto con `git rev-parse
HEAD` en esa rama y declarado con SHA completo en la entrega. Su delta respecto
a GATE_CODE_HEAD sólo agrega evidencia/documentación, sin cambiar código.

## 3. Resultados literales

| Componente | Salida / estado |
|---|---|
| Ruff global app/tests/scripts | `All checks passed!` |
| Ruff directorio nuevo y Python activos Frame | `All checks passed!` en ambos |
| Semgrep completo candidato | `Ran 3 rules on 327 files: 10 findings.` |
| Suite global tests/ | **INCOMPLETA**: `OSError: [Errno 28] No space left on device` |
| Frame B.2 independiente | `47 passed in 16.88s` |
| Frame B.3 remediación | `303 passed in 39.86s` |
| Frame B.3 independiente | `115 passed in 34.09s` |
| Buyer–Actor independiente OFF/ON | `95 passed in 27.78s` |
| Opt-in autor + auditor | `8 passed, 4 errors in 1.79s` |
| Corpus adicional | `Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)` |
| lint-core | `app/core/ limpio` |
| Tamaño | incluido en los 53 focales verdes |
| diff-check | salida vacía, exit 0 |

Los logs preservan el literal completo, comandos y códigos de salida.
execution_manifest.json incluye **todos** los componentes: global incompleta y
local rojo mantienen el resultado agregado en false. El exit del último
subprocess no reemplaza el estado del conjunto.

La corrida global se lanzó una vez; no se repitió. Su salida termina en el
progreso parcial, alrededor del 80%; no se generaron JSON final de pytest ni
JUnit ni totales definitivos. No se cuentan puntos como passes ni se atribuyen
todos los fallos a disco. global_abort.json conserva el error del runner.
Después de su abort, los componentes pendientes se completaron **en serie**,
con temporales separados por componente, sobre el mismo código congelado.

Semgrep: BASE escanea 326 archivos; candidato 327. Diez findings en ambos,
cero errores de scanner, cero nuevos, cero removidos y cero desplazamientos.
Comparación por regla/ruta/fragmento en semgrep_delta.json. Son diez hallazgos
heredados `except-exception-que-silencia`, baseline rojo; exit 0 no es limpio.

## 4. Colección, excepciones y flags

Colección automática: 13.312 casos globales, 47 + 303 + 115 complementarios,
95 independientes Buyer–Actor y 8 opt-in. No desapareció ningún caso del
manifiesto reconciliado; los tres bloques complementarios coinciden exactamente.
Los 228 añadidos frente al manifiesto inicial son 214 Buyer–Actor y 14 controles
Frame ya integrados en MAIN_BEFORE. affected_classification.json clasifica sólo
esos módulos y los dos montajes modificados.

Los 95 independientes incluyen **dos caracterizaciones de negación corta**:
no acreditan excluir al proveedor FUNDACARIBE. El resto conserva las garantías
positivas y negativas aprobadas. No se suman los totales focales solapados.

En los componentes completados: cero skips, xfails o deselects; cero fallos de
call/setup. Hay cuatro errores de teardown en los opt-in del autor. Global:
colección conocida, resultados finales/skips/xfails/deselects no disponibles por
el abort. El gate anterior tenía 11 skips y 16 xfails, pero no se extrapolan esos
totales a esta corrida incompleta.

Frame OFF y Actor ON se fijan por entorno antes de importar/colección, dejando
a las fixtures cambiar los valores. La aceptación registra lectores efectivos
Actor=True y Frame=False/True dentro de workflows reales; los controles Actor
positivos ejecutan predicados documentales. gate_plugin registra Frame después
de fixtures y gate_checks ambos lectores; las trazas de aceptación contienen la
observación durante el cuerpo. El registro global final de flags se perdió con
el abort: no se presenta el entorno inicial como observación de cada test.

Burr OFF antes de importar, secretos del entorno retirados, persistencia JSONL,
SQLite, conversaciones, índice ambiental, Hypothesis y temporales redirigidos.
Suite histórica: sólo lecturas públicas SECOP previstas, GET/HEAD y POST SODA3,
bajo la política/lock del gate Frame. Resto offline con registro externo HTTP
y guarda socket. No hubo intentos en los 465 Frame, los 95 independientes ni
los cuatro locales independientes. Los 20 del autor fueron bloqueados.

Índice local: sólo dos menciones exactas autorizadas y metadata/stat; las
aserciones de tamaño/mtime/inode pasaron en los ocho cuerpos. Sin reconstruir,
barrer ni exportar identidades. No LLM de pago, mensajes, radares reales, app,
reinicios, activación, despliegue, merge o push.

## 5. Bloqueos y hallazgos fuera del montaje autorizado

1. **Entorno: disco agotado.** Antes del gate, una exportación temporal completa
   ya había fallado por ENOSPC; el control focal se pudo completar con clon APFS.
   Con unos 347 MiB disponibles al iniciar la corrida global no hubo capacidad
   suficiente. El runner no pudo finalizar sus logs/reportes. Los temporales se
   limpiaron automáticamente al abortar. La salida parcial se conserva, sin
   reconstruir totales ausentes. Hace falta resolver capacidad antes de otra
   corrida; este gate no autoriza borrar datos del usuario para obtener espacio.
2. **H4: XPASS estricto reproducido antes de los errores masivos.** El test
   `test_new_topic_should_clear_inherited_ordering_signal` no cambia respecto a
   BASE. En copia temporal con runtime BASE: `1 xfailed in 0.87s`; candidato:
   `1 failed in 0.82s`, motivo `XPASS(strict)`. La aserción de limpiar ordering
   heredado ahora pasa. Es una expectativa de defecto que requiere adjudicación,
   no evidencia de que esa aserción funcional haya regresado. No se retiró el
   marcador: la orden sólo permite dos montajes y adjudicar huellas. El primer
   intento del comparativo BASE falló en el snapshot del runner por el módulo
   nuevo ausente en BASE; se conserva como fallo de instrumento, y el comando
   directo aislado produjo el resultado válido en h4_base_direct/.
3. **Montaje local del autor fuera de alcance.** Cuatro casos de
   test_buyer_actor_role_local_v1.py dejan escapar cinco agregados cada uno del
   observer mediante with_timeout. source_for sólo sustituye métodos en la
   instancia. El registro HTTP captura 20 intentos bloqueados y cuatro errores
   de teardown, aunque sus aserciones de cuerpo pasan. No se confunden con
   telemetría ni con los cuatro opt-in independientes verdes. No se corrige
   silenciosamente este tercer montaje ni se rebaja la guarda para aceptar el
   resultado previo del auditor. Su fixture offline sólo lanza excepciones de
   socket sin un ledger; el registro HTTP del gate añade evidencia externa.

No se adjudican los demás F/E de la salida truncada sin reportes de fase. Los
errores masivos coinciden con agotamiento de recursos, pero no prueban que cada
caso sea exclusivamente ambiental. Otra corrida requiere causa resuelta,
alcance autorizado para los montajes/expectativas restantes y candidato nuevo
claramente identificado; no se ejecuta hasta obtener un verde.

## 6. Instrumentos y pendiente

Ruff 0.16.4, Semgrep 1.176.0 y Hypothesis 6.165.10 disponibles. Codegraph 0.9.6
con Node 22: `Not initialized`; MCP no cargado. Fallback explícito rg/Git/manual
para clientes derivados, consumidores y hashes. Se ejecutaron las propiedades
existentes de aceptación independiente y complementarios Frame; no se cambió
lógica combinatoria runtime ni se añadió una propiedad ornamental.

Corrí las puertas estáticas, la suite global **sin poder completarla**, los
complementarios, aceptación y diagnósticos indicados. No declaro validada la
suite ni el repositorio. Integración y dogfood quedan pendientes por los tres
bloqueos anteriores. Siguen abiertos y separados negación corta, ortografía,
ranking fuzzy, creación de Radar, contador de traza y P2 de recuperación Frame.
No se emite PRODUCTION_GO ni BUYER_ACTOR_V1_GO_FOR_MERGE.

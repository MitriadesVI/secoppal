# SECOPPAL V1.2-B.3 — publicación encadenada y ranking coherente

**V1_2_B3_READY_FOR_INDEPENDENT_REVIEW.** B2-I-F4 y B2-I-SUP corregidos en el
alcance probado. Frame sigue OFF por defecto. No es GO del coordinador ni
PRODUCTION_GO. Sin merge, push, activación o reconciliación activa.

Base: `a7e12ebf037f962d356bb34130e180e97f97aa0e`, que contiene el candidato
`5d715ba9e7d67aa05f703d2a280fdf9e2efde3b1`. Su delta sólo añade la auditoría B.2.
Runtime probado congelado: `70f7e54b0a6b6a2f6d8f3fb98525a0b699d59691` (SHA completo en `mutations_initial.json`).
HEAD probado con port final: `b28a01a09697206dd5c67642cecab71e63742496` (SHA completo en `tested_tree.json`).
Entre esos dos commits sólo cambia el oráculo del helper de recuperación;
app/tests son idénticos. El HEAD final es el commit que contiene esta evidencia,
obtenible con `git rev-parse HEAD` y consignado completo en la entrega. El delta
probado→final sólo contiene evidencia/helpers de este directorio; no cambia
runtime ni los tests de producto. Esto evita una autorreferencia de commit.

Rama: `fix/entity-profile-frame-v1-2-b3-final-residuals`.
Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b3-remediation`.
Un solo escritor, sin delegación. Se preservaron main y los worktrees anteriores.
AGENTS/CARRILES, diseño congelado y documentos B.2 leídos. El contrato previo,
las autoridades y transiciones están en `REPAIR_CONTRACT.md`.

## Reparación de publicación

El offset sigue delimitando el append propio bajo la exclusión por chat; ya no
se trata como certificado de publicación del prefijo. Antes de armar la nueva
barrera se captura si el prefijo estaba bloqueado. Si el unlink fue visible y
el rearme fracasa, el rollback conserva todos los bytes previos y reemplaza sólo
su propio append. Cuando el prefijo estaba bloqueado, ese sufijo propio contiene
una marca terminal de recuperación, sincronizada con el archivo y directorio.
El lector estricto existente rechaza esa marca porque no es un sobre completo.
No se agrega un lector, un almacén o una autoridad semántica.

A+B queda intacto; la marca no acredita B ni contiene Frame, filtros, historial
o capacidades. El fallo C sigue devolviendo False. Otro fallo de recuperación
preserva la marca o vuelve a dejar barrera: jamás promueve B/C. Una publicación
D exitosa puede ser autoridad propia con la generación que el staging preparó
desde el estado bloqueado, sin copiar el historial fallido. La revisión conserva
su función de ordinal físico/CAS; una línea o revisión no acredita confirmación.

Con prefijo confirmado, el rollback conserva A byte a byte y A continúa. No se
borran publicaciones confirmadas ni intentos anteriores, no hay búsqueda hacia
atrás, restauración global o limpieza de generaciones. Reset/época y locks/CAS
no cambiaron. La publicación tabular después de contexto inválido conserva la
admisión F5: puede aclarar y requerir reset; no se debilitó para este cierre.
El almacenamiento admite una publicación nueva real sin reset, y el workflow de
perfil también lo acredita después de las tres fallas.

### Evidencia antes/después por contraejemplo

Los cuatro originales dieron `4 failed, 47 passed in 21.51s`. Los 47 incluyen
**dos observaciones**, no 47 garantías. El mismo target intacto después dio
`4 failed, 47 passed in 20.89s`; los cuatro rojos cambian de causa mecánica y no
se ocultan ni se presentan como verdes. Sus ports acreditan el comportamiento.

| Original B.2 | Antes | Después y port |
|---|---|---|
| `test_rollback_of_retry_never_promotes_an_earlier_failed_turn[True-False]` | B tabular admite alimentación/Bogotá, historial/export y siguiente turno | Original exige archivo idéntico A+B y rechaza la marca adicional. Port `test_failed_prefix_port[True-False]`: bloqueo, sin historial/export ni consultas desde B; otro chat conserva su búsqueda. |
| Mismo test `[True-True]` | B perfil gobierna Galapa/julio en otro proceso y siguiente suppliers | Port `[True-True]`: A+B preservado, continuidad bloqueada, no consultas de Galapa/julio desde B. |
| `test_ranking_claim_and_published_warning_are_consistent[count-overview]` | Afirma principal por valor y niega acreditación de ese ranking | Original ya supera la prohibición de contradicción y falla porque exige advertencia negativa de valor. Port del mismo nombre conserva valor acreditado y advierte monto de cantidad. |
| Mismo test `[count-suppliers]` | Título positivo de valor y advertencia negativa simultáneos | Port conserva título válido de valor, cantidad por conteos y advertencia limitada a monto de cantidad. |

`original_sha256.json` conserva los hashes originales; `preservation.json`
comprueba también todos los blobs históricos fuera del delta propio.
`reconciliation_proposal.json` mapea cada node ID original a su port y resultado.
No se editó ningún original o helper de la auditoría anterior.

El helper port de publicación conserva el inyector real de unlink visible,
rearme fallido y rollback. Permite un sufijo de recuperación propio y verifica
preservación del prefijo; la prueba histórica verifica autoridad, historial,
exportación, proceso nuevo y continuación. El control con prefijo confirmado
conserva igualdad completa. El port de proveedores cambia sólo la expectativa
de advertencia en daño de cantidad, según la decisión de producto explícita
B.3; mantiene la prohibición de contradicción y sus controles positivos.

### Secuencias y límites

- 216 combinaciones exhaustivas de tres fallos: append parcial/completo, fsync
  de sobre, unlink antes/después de visibilidad y rearme fallido. Cada frontera
  se acredita por operaciones reales, inode/eventos y fsync de recuperación.
  Oracle externo a la implementación: tras cada fallo, sólo A o bloqueo.
- Propiedad Hypothesis determinista: 40 ejemplos de 2–5 fallos, directorio fresco
  por ejemplo, contra ConversationStore real. No se prueba un modelo contra sí
  mismo. Cada secuencia incluye éxito posterior, preservación del prefijo,
  reset, avance de época y rechazo de reserva antigua.
- Ocho trayectos integrados: perfil/tabla × cuatro fallos iniciales, seguidos
  por dos fallos de confirmación/rearme. Lectura de proceso en cada frontera,
  historial, export con contratos fallidos, ausencia de cursor disponible,
  siguiente turno, generaciones huérfanas, publicación simultánea de otro chat,
  éxito explícito de perfil y recuperación por reset de ambos productos.
- Dos secuencias adicionales abren un proceso nuevo tras cada fallo crítico.
  Los controles B.2 conservan A admisible cuando el prefijo estaba confirmado.

`PublicationRecoveryError` sigue siendo un límite de recuperación física,
no un certificado de bloqueo. Los dos probes originales permanecen intactos y
verdes como **observaciones**. Si además falla abrir el archivo para rollback,
el sobre nuevo íntegro puede quedar sin barrera y el lector admite Galapa/julio,
aunque la llamada terminó en excepción sin True/False acreditado. El caso
principal B.3 tiene truncate y sincronización exitosos y False explícitos: ya
no reproduce esa promoción. Tampoco se afirma inmunidad a pérdida eléctrica,
remonte o combinación arbitraria de fallos de dispositivo. Proceso nuevo sólo
acredita lectura desde el filesystem por otro intérprete.

## Acreditación independiente de rankings

`RankingAccreditation` es una decisión pequeña e inmutable por TOP: disponibilidad,
vacío confirmado, validez de conteos/montos y acreditación del orden solicitado.
El assembler la coloca en la sección y usa esa misma decisión para advertencias
y estado PARTIAL. El resumen y los títulos consumen esa decisión explícita.
La ruta legacy reutiliza la misma política para sus hechos existentes; no se
crea otra interpretación del Frame. No hay cambio de SQL, orden, límites,
identidades, filtros, catálogo o número de llamadas.

| TOP cantidad | TOP valor | Ranking cantidad | Ranking valor | Sección |
|---|---|---|---|---|
| Ambos válidos | Válido | Acreditado | Acreditado | SUPPORTED |
| Monto inválido/ausente, conteos válidos | Válido | Acreditado; advertencia de monto en cantidad | Acreditado, con título/resumen positivo | PARTIAL |
| Válido | Monto inválido/ausente | Acreditado | Sin afirmación de principal/ranking acreditado | PARTIAL |
| Monto inválido/ausente | Monto inválido/ausente | Conteos válidos conservados | No acreditado | PARTIAL |
| Consulta falla | Válido | No disponible, no vacío | Acreditado | PARTIAL |
| Válido | Consulta falla | Acreditado | No disponible, no vacío | PARTIAL |
| Respuesta vacía exitosa | Válido/vacía exitosa | Vacío confirmado, sin principal inventado | Sólo según su respuesta propia | SUPPORTED |

La matriz nueva ejecuta 68 variantes: overview/suppliers × listas disjuntas o
parcialmente coincidentes × 17 combinaciones. Incluye Infinity, None, columna
omitida, consulta fallida, consulta vacía y conteo inválido. En las listas
coincidentes, una identidad tiene monto inválido en un TOP y válido en el otro;
ningún monto se rellena cruzando consultas. El primer proveedor válido de valor
no habilita una afirmación de principal si otra métrica necesaria del mismo TOP
no se acredita. Conteos dañados no invalidan automáticamente un TOP por valor
con montos válidos, aunque la sección informa el daño.

Se verifican respuesta completa, encabezado, hechos, títulos, advertencias,
sección, métricas persistidas por call_id, round-trip del registro, publicación
y siguiente turno con buyer/periodo intactos. Los 20 casos portados conservan
las ocho topologías y doce controles de montos finitos. Values/montos conserva
la comparación textual positiva B.2 y presupuesto de una llamada. Los ejemplos
reales de ocho respuestas completas con su registro están en
`response_record_examples.json`, extraídos de la salida literal, no reconstruidos.

## Puertas y regresiones ejecutadas

Salidas literales de las tres puertas principales:

```text
All checks passed!
Ran 3 rules on 326 files: 10 findings.
321 passed in 48.16s
```

La prueba del helper de recuperación cambió después de esa corrida para
priorizar aserciones funcionales: sus cuatro escenarios se repitieron en el
HEAD probado final (`final_ports.txt.gz`). Runtime idéntico en ambas corridas.

| Target dirigido | Salida literal |
|---|---|
| Baseline de auditoría B.2 | `4 failed, 47 passed in 21.51s` |
| Originales B.2 después, intactos | `4 failed, 47 passed in 20.89s` |
| Propias B.3, incluidos ports | `321 passed in 48.16s` |
| Ports de publicación finales | `4 passed in 3.98s` |
| Aceptación B.1/B.2 + consumidor monetario | `130 passed in 39.91s` |
| Históricos vigentes + cuatro controles + ports B.1 | `105 passed in 29.38s` |
| Guardas de tamaño | `2 passed in 0.12s` |
| Amplio intacto | `3 failed, 1902 passed in 129.24s (0:02:09)` |
| Adyacentes | `472 passed, 2 xfailed in 13.64s` |
| Corpus | `276 passed in 1.05s` |
| Utilidad | `15 passed in 3.58s` |
| Núcleo/A2/propiedades | `1 failed, 407 passed in 1.92s` |
| Reloj en exportación de la base | `1 failed in 0.47s` |

No sumar campañas/targets superpuestos como garantías. La matriz JSON mantiene
cada node ID/resultado y el inventario unido sólo para reconciliación. La
aceptación verifica individualmente los once residuales y 45 controles B.1.
Se conservan F5/F9 y F1/F2/F3/F6/F7/F8/F10, A2/R3d/13 contraejemplos, R1/R2/R4,
ocho escenarios B, seis focos/presupuestos, CLEAR, exclusiones, reloj, Actor/token,
Radar sintético, UI, paginación, rollback y exclusión entre procesos.

Los tres rojos amplios son exactamente los dos registros cuya expectativa de
continuar fue sustituida por F5 y el test temporal
`test_a_relative_period_does_not_get_reinterpreted_on_reload`. El tercero vuelve
a fallar en una exportación de la base: esperaba 2026-09-07, recibió 2026-09-08.
No se cambió runtime, fecha del sistema o expectativa. Las pruebas nuevas llaman
el harness que pasa TODAY explícito a run_query; no invocan capas temporales
sin su parámetro. No apareció un cuarto rojo amplio ni un nuevo xfail.

Ruff 0.16.4 global app/tests/scripts y directorio propio verde. Semgrep 1.176.0:
scan global actual y scan nuevo de base exportada, diez por lado y delta cero
por regla/ruta/fragmento completo, en `semgrep_delta.json`. Exit 0 de Semgrep no
convierte su baseline rojo en verde. Sin supresiones nuevas. `lint-core`:
`app/core/ limpio`; diff-check de commits/staged/working tree sin hallazgos.
Hypothesis 6.165.10 ejecutó propiedades de publicación, números y controles
históricos. Codegraph 0.9.6 con Node 22: `Not initialized`, query no disponible,
MCP ausente; fallback declarado rg/manual, mapa en `consumers.txt.gz`.

## Mutaciones y examen de sus aserciones

Nueve mutaciones B.2 repetidas y cuatro B.3: **13 controles verdes y 13 cambios
conductuales detectados**, con hashes app restaurados tras cada mutante temporal.
La única mutación B.2 que cambia anchor es `supplier-value-ranking`: pasa de
la condición local de montos a la decisión compartida; el mutante sigue forzando
la afirmación de principal por valor y el mismo control B.2 la rechaza.

| Mutación B.3 | Diferencia conductual observada |
|---|---|
| Perder el bloqueo previo | El port llega a la aserción histórica: un turno expresamente no publicado se vuelve autoridad; proceso y consultas de Galapa/julio. |
| Recuperar sólo por longitud física | La secuencia integrada tabular admite el sobre/historial fallido en otro proceso. |
| Generalizar advertencia desde cantidad | La misma respuesta vuelve a afirmar y negar el ranking por valor. |
| Render vuelve a decidir por montos visibles | Ignora la decisión negativa suministrada por el assembler y publica principal por valor. Es un control del contrato entre consumidores, no un incidente live adicional. |

Las dos primeras mutaciones atacan pasos del mismo defecto; no son garantías
independientes. La campaña B.2 detecta autoridad incorrecta, consultas después
de daño de registro, falta de rechazo del comprador, aborto por Infinity,
métrica falsamente VALID y afirmación indebida de principal. No cuentan
import errors, setup, timeout ni diferencias sólo de route_reason.

`mutations.json` conserva cada anchor/reemplazo/target/HEAD y restauración.
`mutations_initial.json` adjudica como MECHANISM_ONLY_NOT_COUNTED la primera
corrida del mutante de prefijo, que falló antes en igualdad de bytes. Se ajustó
el oráculo del port para que el fallo llegue a autoridad/historial/continuación;
control y mutante repetidos quedan en los logs finales de ese nombre. No se
contó dos veces. La adjudicación histórica permanece **25 conductuales y dos
de etiqueta**; no se suma a estas 13 ni se reejecutó esa campaña antigua.

## Hallazgos ajenos, recomendaciones y pendiente

Fuera del carril, sin tocar: diez findings Semgrep, dos xfails Actor y montaje
histórico del reloj. No se reabren los tres ports B.1 ni las dos expectativas
sustituidas. Recomendación fundada en la colección: el coordinador debe incluir
explícitamente los tests de docs/audits con su aislamiento, acreditar los ports
B.3 y corregir el montaje del reloj antes del gate global. Un glob sólo sobre
tests/ omite todos los escenarios nuevos de esta reparación.

La propuesta conserva los originales por hash y distingue pruebas funcionales,
observaciones y expectativas mecánicas cambiadas. No se efectuó reconciliación
activa. Pendientes por alcance: revisión independiente B.3, reconciliación y
gate global del coordinador; cualquier decisión de promoción es posterior.

**Corrí los targets indicados, no la suite.** No app, pytest global, SECOP/LLM
live, datos del usuario, mensajes, radares reales, merge, push o activación.
Las fuentes/narración son sintéticas y la persistencia temporal; offline_guard
prohíbe sockets IP en pytest. Mutaciones exclusivamente en exportaciones temporales.

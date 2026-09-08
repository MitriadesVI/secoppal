# SECOPPAL V1.2-B.3 — revisión independiente focal de cierre

**Veredicto: `V1_2_B3_GO_FOR_COORDINATOR_GATE`.**

B2-I-F4 y B2-I-SUP están cerrados en el alcance reparado, los dos ports son
válidos y no se demostró ninguna regresión bloqueante. No es `PRODUCTION_GO`.
Frame sigue OFF por defecto. Sin merge, push, activación ni reconciliación
activa. No se corrigió runtime durante esta auditoría.

SHA auditado: `4edc248e60610efdbeebb3cb8010bc7b59445cad`.
HEAD final de auditoría: consignado en la entrega al coordinador.
Rama: `audit/entity-profile-frame-v1-2-b3-independent`.
Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b3-independent`.

## 1. Preflight, autoridades y alcance

Ascendencia verificada: `a7e12ebf` (base/auditoría B.2) → `f5d9dac` → `70f7e54`
→ `b28a01a` (HEAD probado) → `4edc248e` (candidato), todos descendientes de
`main`. `5d715ba9` (runtime B.2) es ancestro del candidato. El worktree del
implementador estaba limpio en el SHA candidato.

**Delta probado→final** (`b28a01a` → `4edc248`), comprobado sobre *todos* los
paths y no sólo `app/` y `tests/`: 95 archivos, **todos `A` (añadidos)**, todos
dentro de `docs/audits/entity_profile_frame_v1_2_b3_remediation/`. Cero
modificaciones a runtime, a tests de producto y a helpers de auditoría previos
—incluidos `conftest.py`, `run_checks.py`, `publication_port.py` y el runner
histórico—. La declaración del implementador es exacta.

**Delta funcional** (`a7e12ebf` → `4edc248e`), fuera de `docs/audits/`: cuatro
archivos, 81 inserciones y 12 supresiones.

| Archivo | Cambio |
|---|---|
| `app/core/workflow/publication_io.py` | +8: captura `blocked_prefix` y marca terminal en el rollback |
| `app/core/entity_profile/supplier_rankings.py` | nuevo, 53 líneas: `RankingAccreditation` |
| `app/core/entity_profile/frame_sections.py` | ±16: publica la decisión como hecho y deriva advertencias |
| `app/core/entity_profile/render.py` | ±16: consume la decisión en overview y listados |

`git diff a7e12ebf 4edc248e -- docs/audits/entity_profile_frame_v1_2_b2_independent/
tests/ .semgrep/ ruff.toml Makefile pytest.ini AGENTS.md` está **vacío**: los
originales B.2, los tests del proyecto, los catálogos y las guardas están
intactos byte a byte.

**Herramientas.** Ruff 0.16.6 y Semgrep disponibles en `.venv`; Hypothesis
6.165.10; pytest 9.0.2 sobre Python 3.14.7. **Codegraph: el servidor MCP falló
al conectar (`CONNECTION_CLOSED`)**; se declara fallback a `rg`/lectura manual
para el mapa de consumidores, coherente con el `Not initialized` que reportó el
implementador.

**Alcance propio.** Sólo se añadió este directorio. No se editó runtime,
configuración, tests del implementador, originales, catálogos, guardas ni
documentos compartidos. Las mutaciones se aplicaron exclusivamente sobre una
exportación desechable en scratchpad, con restauración comprobada por SHA256 y
verificación de que el worktree quedó sin cambios.

## 2. Reproducción sobre el SHA final

La advertencia del encargo era pertinente y se atendió: la corrida histórica
`321 passed` antecede al ajuste del helper (`b28a01a`). **No se extrapoló.** El
conjunto completo se reejecutó con el helper definitivo sobre `4edc248e`.

| Target | Declarado | Reproducido |
|---|---|---|
| Propias B.3 completas (4 archivos, helper definitivo) | `321 passed` | **`321 passed in 49.57s`** |
| Ports de publicación finales | `4 passed` | **`4 passed in 3.81s`** |
| Originales B.2 intactos | `4 failed, 47 passed` | **`4 failed, 47 passed in 20.44s`** |
| Aceptación B.1/B.2 + consumidor monetario | `130 passed` | **`130 passed`** |
| Históricos vigentes + controles + ports B.1 | `105 passed` | **`105 passed`** |
| Guardas de tamaño | `2 passed` | **`2 passed`** |
| Amplio | `3 failed, 1902 passed` | **`3 failed, 1902 passed`** |
| Adyacentes | `472 passed, 2 xfailed` | **`472 passed, 2 xfailed`** |
| Corpus | `276 passed` | **`276 passed`** |
| Utilidad | `15 passed` | **`15 passed`** |
| Núcleo/A2/propiedades | `1 failed, 407 passed` | **`1 failed, 407 passed`** |

Los cuatro rojos de los originales B.2 son exactamente los cuatro node IDs
declarados, y sólo en los casos con prefijo bloqueado (`[True-*]`): los
`[False-*]`, que son el control de continuidad legítima, siguen verdes con el
original intacto.

Estos targets **se solapan** (los ports finales están contenidos en las propias
B.3; el corpus y la utilidad reaparecen en el amplio). No se suman como
garantías independientes.

Se distinguen cuatro niveles de evidencia: lectura estática del diff; API
aislada (`append_confirmed` + `ConversationStore` reales); workflow integrado
(harness de Frame con `chat.say`); y lectura entre procesos (`subprocess` con un
intérprete nuevo sobre el mismo almacenamiento). Los probes observacionales de
límite se reportan por separado y no cuentan como garantías.

## 3. Adjudicación de los ports B.3

### Publicación — **PORT VÁLIDO**

El original exigía `target.read_bytes() == before` ("only own append may be
removed"), y por eso moría **antes** de sus propias aserciones funcionales. El
port sustituye únicamente ese oráculo del helper por `after.startswith(before)`
más igualdad estricta cuando el prefijo no estaba bloqueado, y **delega la
discriminación a la prueba histórica completa**, que invoca sin modificarla
(`historical.test_rollback_of_retry_never_promotes_an_earlier_failed_turn`).

Verificado:

- Las inyecciones siguen alcanzando `['unlink_visible', 'rearm_open_failed',
  'rollback_open']` con `Path.unlink`/`Path.open` reales; el helper comprueba
  que el unlink fue **realmente visible** (`assert not barrier.exists()`).
- **No se simula** publicación, lector ni validador: se usa el `ConversationStore`
  real, el lector estricto existente y un proceso nuevo real.
- A+B se preserva byte a byte (`startswith`), y con prefijo confirmado la
  igualdad completa se mantiene.
- B/C fallidos no gobiernan: la prueba histórica comprueba `usable`/`result_id`,
  historial, exportación, cursor, siguiente turno y lectura desde otro proceso.
- Otro chat permanece intacto: `other.say(...)` publica **durante** el fallo y
  `other.say('más')` conserva su dataset.
- Con prefijo confirmado (`failed_prefix=False`) el control exige
  `not after.clarifies`: la continuidad legítima se conserva.

El ajuste `b28a01a` es exactamente la corrección correcta: el oráculo anterior
verificaba **presencia de la marca** (igualdad con `before + marca`), lo que el
propio implementador adjudicó como `MECHANISM_ONLY_NOT_COUNTED`. El oráculo
final discrimina autoridad. Lo comprobé por mutación: con el fix revertido, la
aserción que muere es `assert not after['usable'], 'B was promoted to current
authority'` — **autoridad admitida, no bytes ni longitud**.

### Proveedores — **PORT VÁLIDO**, con decisión de producto trazada

Este **no es un cambio mecánico**. La decisión autorizada en B.3 —acreditar cada
ranking según su consulta y sus métricas necesarias— es una decisión de producto
explícita, y queda trazada aquí.

El original fallaba en una sola línea:
`assert profile.suppliers.status == SectionStatus.PARTIAL and warning`, con
`warning = 'ranking por valor no está acreditado' in response`. Con daño de
montos **sólo en el TOP por cantidad**, el estado ya era `PARTIAL` y la
prohibición de contradicción (línea 65) **ya pasaba**; lo que fallaba era la
exigencia de una advertencia negativa **sobre el ranking por valor**, que es
válido e independiente. Ésa es la expectativa global incompatible.

El port conserva íntegras las líneas 34–67 y 70 del original y sustituye esa
única expectativa por cuatro más estrictas:

```python
assert profile.suppliers.status == SectionStatus.PARTIAL
assert warning == (bad_ranking in ('value', 'both'))
assert positive_claim == (bad_ranking == 'count')
if bad_ranking == 'count':
    assert 'montos de proveedores no acreditables en el TOP por cantidad' in result.response
```

Se conservan: prohibición de contradicción; advertencia sobre el dato realmente
inválido; identidades, conteos y montos válidos; presupuesto de llamadas (2/4);
publicación; siguiente turno con buyer/periodo intactos; y los controles
positivos (`bad_ranking is None` → `SUPPORTED`) y negativos, incluido el control
de doce montos finitos.

La decisión es coherente con lo que la respuesta **afirma**: `_supplier_lines`
imprime sólo conteos en el TOP por cantidad y sólo montos en el TOP por valor.
Un monto dañado en el TOP por cantidad no se muestra ni se afirma; se advierte.

## 4. F4 — validación de fallos encadenados

Propiedad auditada: *ningún intento posterior fallido convierte en confirmado un
registro anterior declarado no publicado*.

### Mecanismo, verificado por lectura y por ejecución

`blocked_prefix = barrier.exists()` se captura **antes** de `_arm`. En el
rollback, tras `truncate(offset)`, si el prefijo estaba bloqueado se escribe
`\n{"publication_incomplete":true}\n` y se sincroniza archivo y directorio.

No se añade lector ni autoridad: la marca la rechazan **dos defensas ya
existentes** en `ConversationStore._read_envelope`, que sólo admite `lines[-1]`
y exige `envelope.revision == len(lines)`. El defecto original era justamente
que, tras truncar a `offset = |A+B|`, el archivo `A+B` satisfacía ambas.

### Reproducción independiente (8 pruebas propias, `test_f4_chained_failures.py`, `test_f4_property.py`)

A válido → B fallido → C fallido, perfil y tabla, con prefijo confirmado y no
confirmado, sobre almacenamiento real y con lectura desde otro proceso:

| Pregunta del encargo | Observado |
|---|---|
| ¿Cuándo se escribe la marca? | Sólo con `blocked_prefix`, tras el truncate, en el propio sufijo |
| ¿Qué ve otro proceso? | `usable=False` en todas las fronteras bloqueadas (intérprete nuevo) |
| ¿Inerte en fallos sucesivos? | Sí: D y E fallidos no la duplican ni la borran (`after == marked`) |
| ¿Publicación nueva legítima? | **Recupera sin reset**: D es autoridad y no hereda de B |
| ¿Reset y época? | La marca no se limpia sola; tras reset una publicación nueva es autoridad |
| Prefijo bloqueado por barrera | Bloquea; con `blocked_prefix=True` se instala la marca |
| Prefijo bloqueado por marca terminal | Bloquea; `lines[-1]` no es un sobre → `CORRUPT` |
| Prefijo bloqueado por cola parcial | **Bloquea igual, sin barrera y sin marca**: "sin barrera" ≠ confirmado |

Controles positivos obligatorios, todos verdes: publicación normal y recarga;
recuperación soportada sin reset; recuperación mediante reset; chat distinto que
sigue trabajando; ausencia de herencia desde generaciones fallidas.

Una petición nueva y exitosa produce su propio resultado aunque repita palabras;
lo prohibido —promover el registro fallido o heredar de él— no ocurre.

### Prueba de que el cierre es real (mutación)

El cierre **no descansa en excluir la secuencia del hallazgo**. Con el fix
revertido y **recuperación y fsync exitosos y False explícito** —la secuencia
exacta del hallazgo—, mi prueba independiente falla con `B was promoted to
current authority` (`usable=True`). Con el fix, no. El defecto es reproducible
por un camino independiente y está cerrado.

### Límites — observaciones, no certificados

`PublicationRecoveryError` sigue siendo una observación de límite. Se documenta
lo que **queda admitido**, no se declara seguro por haber lanzado:

| Escenario (3.º fallo simultáneo) | Excepción | Qué queda admitido |
|---|---|---|
| No se puede **abrir** el archivo para rollback | `PublicationRecoveryError` | El sobre nuevo íntegro (C), sin barrera — límite ya declarado |
| Falla el **flush** de la marca | `PublicationRecoveryError` | Nada: la marca alcanzó el archivo; sigue bloqueado |
| Falla la **escritura** de la marca | `PublicationRecoveryError` | **B**, un registro anterior explícitamente no publicado (`usable=true`, 2 líneas) |

El tercer caso es una **ampliación del límite declarado que REMEDIATION no
documenta**: la causa es que `truncate(offset)` se ejecuta *antes* de escribir la
marca, de modo que si la escritura falla, la cola que bloqueaba ya se eliminó.
No es una regresión (en la base ese escenario admitía el registro anterior
incluso con recuperación **exitosa**) ni reabre F4 (aquí la recuperación
**fracasa** y no hay False acreditado), y no se exige inmunidad a fallos físicos
simultáneos. Se eleva como **P2 de documentación de límite**, con su relación
con la promesa de producto: en esa cadena de tres fallos, la promesa "un turno
no publicado nunca gobierna" no se sostiene y la respuesta correcta del producto
sería el reset. Evidencia: `limit_marker_write.json`, `limit_marker_flush.json`.

No se confunde proceso nuevo con pérdida eléctrica: los probes entre procesos
sólo acreditan lectura desde el filesystem por otro intérprete.

## 5. Rankings — una decisión, todos sus consumidores

`RankingAccreditation` es un `dataclass` inmutable por TOP. `frame_sections`
lo publica **incondicionalmente como hecho** para ambos rankings y deriva de él
el estado de sección y las advertencias; `render` lo consume en el overview y en
los dos listados.

Matriz propia (`test_sup_ranking_matrix.py`): 5 estados × 5 estados × 2 focos ×
listas disjuntas/coincidentes = **100 combinaciones**, más 4 pruebas focales:
**104 passed**. Estados: válido, montos inválidos (`Infinity`), conteos
inválidos, consulta fallida y respuesta vacía exitosa.

Verificado en cada combinación:

- `available` y `accredited` siguen **su propia consulta**, nunca la otra.
- Ninguna respuesta publicada afirma y niega el mismo ranking (en ambos sentidos).
- Una afirmación positiva exige la acreditación **de ese** ranking y sus filas.
- La advertencia recae sobre el dato realmente inválido, con exclusividad
  bidireccional (montos y conteos, cantidad y valor).
- Un monto inválido en el TOP por cantidad **no** invalida un TOP por valor
  independiente y válido; la sección es `PARTIAL` mientras ese ranking es válido.
- Listas coincidentes: el monto dañado de una identidad compartida **no se
  rellena** desde la otra consulta (`total_contract_value is None` en el TOP
  dañado, válido en el otro), y los rankings no se reconstruyen ni reordenan.
- No hay cambio de SQL, filtros, límites ni presupuesto: `len(result.calls)`
  sigue siendo 2 (suppliers) y 4 (overview).
- Vacío confirmado, dato inválido y fuente fallida siguen distinguidos:
  `empty`→`SUPPORTED` sin principal inventado; `query_failure` no se interpreta
  como vacío; ambas consultas fallidas → `UNAVAILABLE`.
- Publicación y siguiente turno tras recarga: `continuation_published` y SQL con
  `BARANOA`/`2026-08` intactos.
- Corrección del overflow y `values`/montos conservados como control.

**Sobre la compatibilidad legacy.** `section_ranking` sólo recalcula si el hecho
no está o no es un `RankingAccreditation`. En la ruta Frame el hecho **siempre**
está: mi prueba comprueba identidad de objeto
(`section_ranking(...) is fact`), de modo que el trayecto nuevo consume la
decisión explícita y no una reinterpretación. El fallback existe únicamente para
el assembler legacy de `sections.py`, que no publica el hecho; allí aplica la
misma política sobre sus hechos existentes. Cuando la sección es
`unavailable_section`, el fallback devuelve `accredited=False`: falla cerrado.

## 6. Regresiones y mutaciones

Controles conservados sin reconstruir sus soluciones: F1/F2/F3/F5/F6/F7/F8/F9/F10,
A2/R3d, trece contraejemplos, R1/R2/R4, ocho escenarios B, seis focos, CLEAR,
exclusiones y reloj inyectado — reproducidos en `acceptance` (130),
`historical_current` (105) y los seis targets de regresión.

### Los tres rojos amplios son PREEXISTENTES (probado, no asumido)

Los tres node IDs se ejecutaron sobre la base `a7e12ebf` exportada: **fallan
idénticamente**. No hay regresión introducida por B.3.

- `test_a_record_from_another_catalog_does_not_govern_the_next_turn` y
  `test_a_forged_execution_record_does_not_change_the_next_turn`: las dos
  expectativas de ejecución tras corrupción sustituidas por F5 en la
  adjudicación anterior. No se reabren.
- `test_a_relative_period_does_not_get_reinterpreted_on_reload`: deuda del reloj
  del helper, `assert '2026-09-08' == '2026-09-07'`. No es una falla productiva
  nueva. **No se corrigió el reloj ni se cambió la fecha del sistema.**

No apareció un cuarto rojo ni un nuevo xfail.

### Mutaciones

Se examinaron las **trece** del manifiesto del implementador
(`mutations.json`, HEAD `b28a01a`, todas `BEHAVIOR_DETECTED` con `restored:true`
y SHA256 por archivo) y se ejecutaron **seis propias** sobre una exportación
desechable:

| Mutación propia | Objetivo | Resultado |
|---|---|---|
| `drop-blocked-prefix-capture` | `blocked_prefix = False` | **MUERTA**: 5/7 F4 y la propiedad Hypothesis; muere en `usable` |
| `marker-without-trailing-newline` | marca vacía | **MUERTA**: mismas 5 |
| `marker-before-arm-instead-of-after` | capturar tras `_arm` | **MUERTA**: 3/7, incluido el **control positivo** |
| `ranking-shared-amount-condition` | `counts and amounts` | **MUERTA**: 36/104 |
| `ranking-warning-generalised-to-value` | etiqueta fija `valor` | **MUERTA**: 40/104 |
| `ranking-render-recomputes-from-amounts` | render re-acredita | **SOBREVIVE** a las 104 |

Adjudicación de las aserciones, según lo exigido:

- **Una diferencia de bytes no acredita seguridad.** Ninguna detección propia se
  apoya en bytes: mueren en `usable`, `query`, historial o en el texto publicado.
- **Retirar un mismo mecanismo de dos maneras no son dos garantías
  independientes.** `drop-blocked-prefix-capture` y
  `marker-without-trailing-newline` —equivalentes a `B3-lost-prefix-nonconfirmation`
  y `B3-physical-length-only-recovery` del implementador— atacan el mismo paso.
  Cuentan como **una**. El implementador lo declara igual.
- **Un mutante neutralizado por otra defensa se declara como tal.**
  `B3-render-reaccredits` **no produce diferencia conductual en el flujo real**:
  sobrevive a mis 104 casos porque, con datos reales, la condición del render y
  la decisión del assembler coinciden por construcción. Sólo muere ante
  `test_render_consumes_the_assembler_decision`, que inyecta una decisión
  discrepante. Es un **control del contrato assembler→renderer, válido como
  tal**, y así lo presenta el implementador; no es un incidente espontáneo del
  workflow ni acredita una defensa observable en producto.
- `marker-before-arm` mata el control positivo de continuidad legítima: ese
  control **detecta**, no es decorativo.
- No se contaron import errors, timeouts, fallos de montaje ni diferencias de
  `route_reason`. Restauración comprobada por SHA256 tras cada mutante y
  `git diff --quiet` sobre el worktree.

`mutations_initial.json` adjudica `MECHANISM_ONLY_NOT_COUNTED` la primera corrida
del mutante de prefijo, que moría por igualdad física. Verifiqué que el oráculo
final ya no lo hace: muere en autoridad admitida. La adjudicación es correcta y
no se contó dos veces.

## 7. Puertas

| Puerta | Resultado |
|---|---|
| Ruff global `app/ tests/ scripts/` | `All checks passed!` |
| Ruff directorio propio | `All checks passed!` |
| Semgrep `app/` con `.semgrep/secoppal-laws.yml` | `Ran 3 rules on 326 files: 10 findings` |
| Semgrep base `a7e12ebf` (raíz Git temporal) | `Ran 3 rules on 325 files: 10 findings` |
| **Delta Semgrep por regla, ruta, línea y fragmento** | **cero** (`only_in_head: []`, `only_in_base: []`) |
| `make lint-core` | `app/core/ limpio` |
| `git diff --check` (commits, working tree, staged) | sin hallazgos |
| Guardas de tamaño | `2 passed` |
| Hypothesis (40 ejemplos, derandomize) | `1 passed`, y muere ante la mutación |

Los diez findings de Semgrep son `except-exception-que-silencia` en handlers y
formatter, **idénticos en base y candidato**: preexistentes y ajenos al carril.
Un exit 0 de Semgrep no convierte ese baseline en verde. Sin supresiones nuevas.

No se ejecutó pytest global, ni app, ni SECOP/LLM live, ni datos del usuario,
mensajes o radares reales. Persistencia redirigida a `tmp_path` por `conftest.py`
propio; `offline_guard` activo; Frame OFF en entorno.

## 8. Manifiesto para la reconciliación (no ejecutada)

Detalle completo en `reconciliation_manifest.json`. Puntos que el coordinador
debe decidir:

1. **`pytest tests/` omite todas las garantías nuevas.** Comprobado: la
   colección por defecto recoge 13 089 tests de `tests/` y **cero** de los 321
   de B.3 (`pytest.ini` fija `testpaths = tests`). Un glob sobre `tests/` deja
   fuera el cierre completo de B.3. La recomendación del implementador está
   fundada.
2. **No ejecutar indiscriminadamente todo `docs/audits/`.** Vigentes y sus
   condiciones de aislamiento: los cuatro archivos de
   `entity_profile_frame_v1_2_b3_remediation/` (321) y los siete míos (115),
   ambos con su `conftest.py` local, el plugin `offline_guard` y el
   `PYTHONPATH` que incluye raíz, `tests/`, el plugin y los directorios de
   auditoría. Los originales B.2 sustituidos **no** deben ejecutarse como gate.
3. **Originales sustituidos por port** (conservados por hash, no borrados):
   `test_rollback_of_retry_never_promotes_an_earlier_failed_turn[True-False]`
   y `[True-True]` → `test_failed_prefix_port[True-False]`/`[True-True]`;
   `test_ranking_claim_and_published_warning_are_consistent[count-overview]` y
   `[count-suppliers]` → el port homónimo de `test_supplier_ports.py`.
   Equivalencia acreditada en la sección 3.
4. **Controles positivos que deben conservarse**: los casos `[False-*]` del test
   histórico de publicación; `bad_ranking is None` → `SUPPORTED`; el control de
   doce montos finitos; mis controles de prefijo confirmado y cola parcial.
5. **Probes observacionales, separados de las garantías**: los dos de
   `PublicationRecoveryError` del implementador y mis tres de límite
   (`test_f4_limits.py`).
6. **Adjudicación anterior preservada**: tres ports B.1; dos expectativas de
   ejecución tras corrupción sustituidas por F5; deuda del reloj del helper.
7. **Deuda del reloj, reparación determinista pendiente** (no ejecutada aquí):
   `today` debe llegar a **todas** las capas del helper que lo reciben, de modo
   que la expectativa no dependa de la fecha del sistema. No se corrigió el
   reloj ni se cambió la fecha.

## 9. Hallazgos y observaciones

No se abrió ningún frente por preferencia de implementación. No hay hallazgos P0
ni P1.

**P2-1 — Límite de recuperación más amplio de lo documentado.**
*Requisito*: la sección 4 exige que ningún intento fallido promueva un registro
anterior no publicado. *Escenario*: A confirmado → B bloqueado por barrera → C
con unlink visible + rearme fallido + **fallo de escritura de la marca**.
*Esperado según REMEDIATION*: en el límite queda admitido "el sobre nuevo
íntegro". *Observado*: queda admitido **B**, el registro anterior no publicado
(`usable=true`, `query='consulta B NO publicada'`, 2 líneas), también desde otro
proceso. *Causa*: `truncate(offset)` precede a la escritura de la marca.
*Impacto*: sólo bajo tres fallos simultáneos y con `PublicationRecoveryError`
lanzado; no es regresión ni reabre F4. *Acción*: documentar el límite con
precisión antes del gate; una reparación opcional sería escribir la marca antes
de truncar, o truncar a `offset` sólo tras confirmar la escritura.

**Observación A — El reset desactiva la barrera (preexistente).** Con
`['barrier', 'reset']`, `advance_epoch` renombra la barrera y un turno no
publicado se vuelve legible. Verificado **idéntico en la base `a7e12ebf`** y en
el candidato (`probe_reset.py`): comportamiento previo y deliberado ("old epochs
are inert after reset"), fuera del alcance de B.3, que no tocó reset/época.

**Observación B — `B3-render-reaccredits` no acredita defensa en producto.**
Sobrevive a 104 casos de flujo real; sólo muere ante el control de contrato.
Coincide con la propia calificación del implementador.

**Observación C — Codegraph no disponible.** El servidor MCP falló al conectar;
se declaró fallback a `rg`/manual.

## 10. Veredicto

`V1_2_B3_GO_FOR_COORDINATOR_GATE`.

- **B2-I-F4: CERRADO** en la secuencia del hallazgo —recuperación y fsync
  exitosos con False explícito—, verificado por reproducción independiente del
  defecto vía mutación, ocho pruebas de cadena y una propiedad Hypothesis.
- **B2-I-SUP: CERRADO**, verificado sobre 104 combinaciones con la decisión
  consumida por todos los consumidores y sin contradicción publicada.
- **Ports: ambos válidos**, con la decisión de producto de rankings trazada.
- **Sin regresiones bloqueantes demostradas**: los tres rojos amplios son
  preexistentes y se probó su identidad en la base.

Condicionado a que el coordinador incluya explícitamente los tests de
`docs/audits/` con su aislamiento, registre el P2-1 y la deuda del reloj.
No se emite `PRODUCTION_GO`. Sin merge, push ni activación.

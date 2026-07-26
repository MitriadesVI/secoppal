# Radar — Closeout de integración técnica sobre `main`

- `DOCUMENT_STATUS: CURRENT`
- `AUTHORITY_KIND: INTEGRATION_RECORD`
- Fecha: 2026-07-26
- Rama de integración: `integration/radar-mvp-main`
- Rama fuente: `radar/mvp-manual` · HEAD `e3ab0727f35620d0973274033900afdda582f380`
- Base de integración: `main` en `3762cb4dcc10dbbbf27b624368e94a058beda776`
  (ya contenía el contrato conversacional de modalidad, ADR-054)

Este documento registra **qué código de Radar entró a `main` y en qué estado
operativo queda**. No es un cierre de producto.

## Estado

```text
RADAR_CODE_IN_MAIN:        pendiente hasta el fast-forward final
AUTO_DELIVERY_DEFAULT:     OFF
PILOT_MODE:                supervised
FIRST_NATURAL_PREVIEW:     sin evidencia en el repositorio
FIRST_SUPERVISED_SEND:     sin evidencia en el repositorio
P1B_6_PRODUCT_GATE:        NOT_COMPLETE
AUTOMATIC_SCHEDULING:      NOT_ENABLED
RADAR_BRANCH_DELETION:     NOT_AUTHORIZED
PUSH:                      NOT_PERFORMED
```

`FIRST_NATURAL_PREVIEW` y `FIRST_SUPERVISED_SEND` se declaran como *sin
evidencia*: el código de los pilotos está integrado, pero el árbol no contiene
registro de ninguna ejecución real, y esta integración no ejecutó ninguna. Que
el código exista no significa que el hito operativo haya ocurrido.

## Alcance trasladado

23 commits exclusivos de `radar/mvp-manual`, desde el merge-base
`1402051a85f541b824f07e3538a99b8e14c17111`, aplicados uno por uno en orden
cronológico. Ninguno se saltó, duplicó ni reordenó; los mensajes originales se
conservan íntegros.

| # | integrado | origen | mensaje |
| --: | --- | --- | --- |
| 1 | `06d4c1b` | `9795bb9` | fix(radar): make diff window-safe and refresh seen results |
| 2 | `a8b7829` | `93206f8` | fix(radar): use canonical phrase validation in runner |
| 3 | `95397e2` | `50b7672` | fix(radar): add run outcomes and failure backoff |
| 4 | `3891b8f` | `f405986` | docs(radar): plan de ejecución post-P0 (E0-E16) |
| 5 | `eec1a43` | `80aac0e` | fix(radar): prevent concurrent runs with fenced leases |
| 6 | `7e11be7` | `02ffcfd` | fix(radar): track consecutive permanent failures |
| 7 | `d555c18` | `0d34b7c` | fix(radar): materialize run state atomically |
| 8 | `0915a36` | `28f3e94` | fix(radar): make follows refresh atomic and fenced |
| 9 | `5792982` | `a06e782` | fix(radar): preserve attention after successful runs |
| 10 | `329943d` | `7ed03a7` | feat(radar): add notification ownership and archival controls |
| 11 | `30e38c9` | `32cb28e` | feat(radar): add idempotent notification outbox |
| 12 | `8ed5f54` | `c8c59a9` | feat(radar): add deterministic notification selection |
| 13 | `527e787` | `c63e0ce` | feat(radar): add worker transactional core |
| 14 | `bf83219` | `0beecc9` | fix(radar): enforce notification preference integrity |
| 15 | `227c166` | `3bf1795` | feat(radar): freeze useful event metadata |
| 16 | `c8bda71` | `f9a77e4` | feat(radar): add versioned notification payloads |
| 17 | `190edca` | `87fe8af` | feat(radar): enforce transactional delivery windows |
| 18 | `7284609` | `2516f69` | feat(radar): add provider-safe delivery worker |
| 19 | `7be896b` | `ce70de4` | feat(radar): add supervised Telegram pilot |
| 20 | `050a658` | `29f0c97` | feat(radar): add isolated Telegram demo workflow |
| 21 | `b3f8f53` | `5883aa8` | fix(secop): update process response date field |
| 22 | `4a55137` | `9e79d06` | feat(radar): add notification payload v3 object excerpts |
| 23 | `c7479d8` | `e3ab072` | feat(radar): add supervised real Telegram pilot |

## Los dos conflictos, y por qué no costaron código

Solo dos cherry-picks conflictuaron —`87fe8af` y `2516f69`— y en ambos el único
archivo en conflicto fue `docs/decisions_log.md`. La causa es de numeración, no
de lógica: ambos commits añaden una entrada (ADR-052 y ADR-053) justo después de
ADR-041, que es donde `main` acababa de colocar ADR-054.

En los dos casos se conservó la versión del log de la rama de integración y se
verificó, por comparación de listas, que el resto del commit entraba completo:

- `87fe8af`: 9 archivos de código y tests, paridad exacta.
- `2516f69`: 14 archivos de código y tests, paridad exacta.

**Ningún archivo de código o test se descartó.** Las decisiones tampoco: ADR-052
y ADR-053 se reconciliaron después en `docs/decisions_log.md`, con su texto
commiteado original y una nota que explica el orden de aparición. En su rama de
origen ambas existían únicamente como entradas del log, no como archivos ADR
separados; se conservan en esa misma forma.

El ADR-051 provisional **no se integró**: solo existe como cambio sin commitear
en el worktree operativo, nunca fue un commit formal y no se auditó como
vigente.

## Los tres archivos solapados

`main` (modalidad) y `radar/mvp-manual` tocaban tres archivos en común:

| archivo | resolución |
| --- | --- |
| `docs/decisions_log.md` | conflicto diferido y reconciliado en el commit documental |
| `app/core/formatter.py` | combinación verificada |
| `app/core/soql_builder.py` | combinación verificada |

Para los dos archivos de código se comprobó la combinación en ambas direcciones:
el diff contra `radar/mvp-manual` es *exactamente* el delta de modalidad, y el
diff contra `main` es *exactamente* el delta de Radar. Es decir, el archivo
integrado es la suma de ambos, sin pérdida por ningún lado.

Los hunks no se solapan y responden a propósitos distintos:

- `formatter.py`: Radar corrige una sola línea —el nombre de columna
  `fecha_de_recepcion_de_respuestas_al_in` → `fecha_de_recepcion_de`— dentro de
  la tabla de etiquetas de fechas. Modalidad toca imports, el mensaje de
  ausencia de resultados y las etiquetas de ordenamiento.
- `soql_builder.py`: Radar añade `closing_date`/`signature_date` a `DatasetSpec`
  y sus valores por dataset. Modalidad toca imports, `NONEMPTY_WHERE_KEYS` y los
  bloques de construcción del `WHERE`.

Los 66 archivos restantes gobernados por Radar son idénticos byte a byte a la
rama fuente.

## Artefactos excluidos

Los 23 commits tocan 69 archivos, todos bajo `app/`, `tests/`, `docs/` o
`scripts/`. La auditoría previa confirmó **cero** archivos fuera de esas rutas y
cero artefactos operativos: sin `.env`, sin bases de datos, sin WAL/SHM, sin
tokens, sin PID, sin estado de runtime. El worktree operativo sucio no se tocó
en ningún momento, y nada de su contenido sin commitear entró a la integración.

## Configuración segura por defecto

Verificado por auditoría del código integrado:

- ningún módulo ejecuta trabajo al importarse: `worker.py`, `scheduler.py`,
  `telegram_provider.py` y `notify/__init__.py` solo definen constantes y
  símbolos;
- ningún archivo de cron, launchd o systemd entra con esta integración, por lo
  que el merge no habilita ninguna programación automática;
- `TelegramProvider` toma el token de la variable de entorno
  `TELEGRAM_BOT_TOKEN` y falla con `TelegramConfigurationError` si no existe;
  también exige una allowlist de chats;
- el envío real está detrás de un flag literal `--confirm-send` **y** de la
  palabra literal `SEND`; ninguna operación acepta el token como argumento;
- `preview` abre la base en modo estrictamente de solo lectura (`mode=ro`) y no
  envía nada;
- los pilotos **rechazan explícitamente** `data/secoppal.db`; el piloto real
  exige la base canónica `data/telegram_p2_real.db`;
- no hay token ni chat ID real commiteado: el único par que aparece
  (`999999:super-secret-token` / `-1001234567890`) es fixture sintética en
  `tests/test_notify_telegram_provider.py`.

## Qué NO afirma este documento

Integrar el código no cierra P1B-6 ni autoriza beta. En particular:

- no se declara `P1B-6 COMPLETE`;
- no se declara `beta GO`;
- no se habilita entrega automática;
- no se autoriza borrar `radar/mvp-manual` ni la rama de integración;
- no se hizo push.

El estado correcto es:

```text
TECHNICALLY_INTEGRATED
OPERATIONALLY_SUPERVISED
PRODUCT_GATE_PENDING
```

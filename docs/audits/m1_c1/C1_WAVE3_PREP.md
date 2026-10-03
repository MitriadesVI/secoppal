# C1 Wave 3 — preparación de ownership y frontera

Base leída: `main` `5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f`.
Paquete docs/tests; **cero cambios app**, sin implementación C1.3/C1.4,
sin freeze, auditoría, promoción ni activación. Este documento propone el reparto
para adjudicación de HERA; no cambia ADR-103 ni concede permisos de runtime.

Autoridad leída: C0 §5.1–5.3 y filas C1.3/C1.4 de §6; ADR-103 enm. 4
D1(b/e/f), D3 y D13; C1.2 candidato y deuda de frontera; IR v2 real
`interpretation_contract.py`, `foundation_operations.py`, `foundation_lift_v1.py`.
D1/D3 de C1.2 quedan cerrados documentalmente por la orden humana que HERA
transmitió en esta misión, conforme al microgate separado
`outputs/C1_D1_D3_MICROGATE.md` (`GO_CON_MATIZ`, base anterior). Ese resultado
no era un postimage del hook; el GO_SPEC posterior de §F concreta esa frontera. Este prep no cambia código ejecutable.

- D1: jobs vacío válido sólo sin requests; requests presentes exigen al menos
  un job y pertenencia exacta de cada request a uno. No es bicondicional:
  un job no-SEARCH acreditado puede existir sin requests según sus reglas.
  La pareja vacía v1 conserva `NO_ACTION/NO_REQUEST`, sin sintetizar SEARCH.
  La fila jobs de C0 §5.1.2 se alinea por la ampliación documental expresa;
  una restricción adicional del wire v2 se debe declarar en C1.3.
- D3: rechazo de operación/sujeto ajenos al vocabulario del wire elegido y
  pares dimensión-operador fuera de su tabla, sin inferencia, rescate ni
  ejecución alternativa. `UNSUPPORTED` explícito conserva operador libre y
  label; no rescata un par inválido. Se preservan las conversiones deterministas
  v1 acreditadas (jobs por operación, COUNT/SUM_VALUE→TOTAL,
  GEOGRAPHY→DEPARTMENT), sin convertirlas en defaults implícitos del wire v2.

La evidencia del microgate es externa y read-only; el cambio C0 sólo registra
la excepción D1 ya aceptada, no rediseña el contrato ni concede implementación.

## A. C1.3: decoder y perfiles

Scope propuesto exclusivo:

- Nuevo `app/core/foundation_decoder_v2.py`.
- `app/core/foundation_producers.py`: registro cerrado `PROFILES` y
  `LinguisticProducer(profile)`; preservar el perfil v3 y sus callers.
- Nuevo `app/core/foundation_prompts_v5.py`: sólo lo necesario para perfil
  inactivo; freeze del prompt corresponde a C1.6.
- `app/core/foundation_universe_kind.py`: admitir versión 5 según C0 §5.2;
  sin default `universe_kind` para unspecified.
- Tests propios de wires cruzados y forma documentada, con paths disjuntos de C1.4.

El perfil fija prompt + decoder, nunca sniffing del payload. Wire 4.2 por decoder
v2 y wire 5 por decoder v1 deben fallar sin propuesta ejecutable ni llamada a
fuente. Wire v2 no hereda los defaults de lift v1. C1.3 no selecciona v5 desde
config/runtime: esa selección pertenece a C1.6. `foundation_prompts.py` permanece
byte-congelado. Compatibilidad de `LinguisticProducer(provider)` y los callers
existentes requiere test específico; una firma propuesta no permite romperlos.

## B. C1.4: operación y handoff

Scope propuesto exclusivo:

- Nuevos `app/core/foundation_operation_host.py` (decisión pura) y
  `app/core/foundation_operation_owners.py` (tabla cerrada).
- `app/core/foundation_route_owner.py`: reclamaciones del dueño y
  `executed_owner`, como puente autorizado a detectores legacy.
- `app/core/foundation_runtime.py`: `OperationHandoff` y despacho de disposición.
  Su ubicación final se cierra antes de escribir; no introducir ciclos.
- `app/core/foundation_admission.py` y `app/core/foundation_projection.py`:
  retirar los dos rechazos `UNSUPPORTED_OPERATION` sólo al sustituirlos por la
  decisión autorizada, no dejando un hueco de admisión.
- `app/core/foundation_messages.py`: terminales del contrato C0.
- `app/core/orchestrator.py`: **exclusivamente C1.4**, único hook delimitado en §F,
  limitado al postimage de §F y a la divergencia estrecha de §E. No abrir todo `_dispatch`.
- `app/core/turn_ledger.py`: **exclusivamente C1.4**, sólo el postimage exacto
  `Stage.to_dict` de §F. Necesidad demostrada por METIS: el ledger base redacta
  `operation-owners@1`, autoridad literal de C0 §5.3.5. La orden humana original
  permitió ledger si necesario; HERA acredita esa condición y autoriza preparar
  el pin, **no escribir app en este prep**. `CAPABILITY` ya existe; no cambiar
  `_TOKEN`, `_clean`, `_token`, otros campos ni crear otro ledger.


Tests de C1.4: operación × reclamación, MULTI/PROTOCOL terminal, ausencia de dueño
sin segundo salto, ctx/deps idénticos y cero llamadas a fuente durante handoff;
sonda de conformidad R3 observacional. Handoff sólo transfiere operation, aspect,
owner, job_id, table_version; nunca condiciones/spans/params. La misión actual
no toca R3, Gold, sello, oráculo, `foundation_accepted.py` ni kit congelado.

## C. Compartidos y paralelismo

No hay archivo app compartido autorizado entre los dos scopes propuestos.
`interpretation_contract.py`, `foundation_operations.py`, `foundation_operator_table.py`,
`foundation_lift_v1.py`, `foundation_acceptance_loader.py`, contrato C1.1 y
`foundation_universe_policy.py` se consumen en lectura; no se amplían por conveniencia.
Si cualquiera necesita escribirlos, se vuelve al prep y se serializa con scope
nuevo. **Si preparar el compartido exige app/, STOP: este prep no lo autoriza.**

C1.3/C1.4 sólo pueden correr en paralelo con app disjunto, tests propios y
ownership explícito. Un helper/fixture compartido se prepara antes; nunca dos
writers del mismo path. C1.4 usa el perfil existente hasta integración secuencial;
no modifica producers. C1.3 no modifica runtime/orchestrator. Un test de C1.4 que
requiera wire 5 espera a C1.3 o usa IR oráculo construida por contrato vigente.

## D. Dependencias observadas

Graft callers y lectura de fuente acreditan:

| Consumidor | Dependencia actual / consecuencia |
|---|---|
| `configured_producer`, callers y tests de v3 | `LinguisticProducer(provider)`; C1.3 debe preservar comportamiento v3 |
| `LinguisticProducer.produce` | `host_candidates`, prompt v4.2, `decode_proposal`; perfil elige las tres piezas coherentemente |
| `decode_proposal` | IR v2 mediante lift, vocabulario y tabla actuales; no requiere C1.4 |
| `foundation_runtime.run_foundation_search` | productor, H7, admisión, proyección; C1.4 consume la IR ya disponible |
| `orchestrator._dispatch` | import perezoso y llamada a `run_foundation_search`, luego retorno dict; es la única frontera productiva observada |
| `foundation_route_owner` | parse y detectores legacy; conservar puente único, no mover parsing al host puro |
| `turn_ledger` | API de stages disponible, pero `_token` redacta autoridad pública; excepción exacta de serialization requerida |
| INV-SEP legacy | manifiesto 88653752 + `test_m1_wave1_parity_pins.py`; hook nuevo no está en su whitelist actual |

D1/D3 están cerrados con los matices anteriores. C1.4 no depende de que el proveedor
produzca wire 5 para probar IR oráculo. C1.6 y CUTOVER siguen fuera de alcance:
MODE_FRONTIER no se convierte aquí en DISPATCH_FRONTIER. Graft actualizado al
leer (grafo externo a repo); la fuente manda sobre topología indexada.

## E. Divergencias legacy estrechas preparadas

La whitelist histórica conserva sus tres entradas previas y añade únicamente:
`app.core.orchestrator` (hook AST exacto) y `app.core.turn_ledger`
(`Stage.to_dict` exacto). No se regeneran manifiesto ni hashes base.
`tests/test_conversation_store_clear_delta.py` registra correspondencia exacta
con `NARROWING_PINS`, comprueba que cada uno rechace un adversario y ejecuta
`frozen_module_problems` del pin canónico invoca directamente ambos
pines concretos sobre **bytes reales** o overrides in-memory;
`c1_narrowed_legacy_problems` comprueba ese mismo gate, no una exención aparte. Las entradas no
exoneran todo el módulo. Cada módulo actual y cada postimage acreditado pasa;
una mutación fuera del slot falla y retirar un narrow pin o volverlo siempre
verde rompe el meta-pin. Los tests propios añaden negativos dentro de los slots.

## F. Postimages exactos, sin implementación

METIS `outputs/C1_4_HOOK_POSTIMAGE_SPEC.md`: **GO_SPEC** sobre `75acaa76`,
con 49 controles externos por assert (no casos pytest). Espec derivados de
C0/ADR y acreditados por HERA; no derivados de código candidato futuro. Los
excerpts se conservan en `tests/fixtures/c1_wave3_hook_expected.py.txt` y
`tests/fixtures/c1_wave3_ledger_method_expected.py.txt`. No son módulos runtime.

`tests/test_c1_wave3_prep_boundary.py` compara el AST efectivo de Python leyendo
**bytes** (respeta coding cookie/BOM). Ignora posiciones/comentarios, sin pin
byte general; el mutante latin1 demuestra que cambiar codificación que altera
literales sí falla. Los hashes base anteriores y manifiesto no cambian.

- Hook: único bloque `_dispatch` desde import `foundation_runtime` hasta antes
  del `with legacy_budget` existente. Todo AST exterior conserva
  `5ebeb7a8b5399e7d73be2466a27dd27c5917ebc9e9edb16ca8a97b6b79d10738`.
  Baseline hook `0c8fa739026c676625c81498b304bae9a554e698db0b72be002c402ebf956983`
  y postimage `9b4b69fe3c3984aa252f83fbfe8a7f9843623dfa1bf1e4ce50166aa51ac8a2e4`
  son los únicos aceptados. Postimage registra handoff previo, entra una vez al
  mismo legacy budget/observed_deps/ctx y registra el resultado después; el
  retorno dict/None conserva las ramas existentes. Los helpers propuestos viven
  en runtime de C1.4, no se implementan aquí ni se importan en tests synthetic.
- Ledger: módulo base AST
  `b70abcca6ecc1c07f9f90212a2b80aab1c645e9bd84a0175f022cfba269e448f` o módulo
  postimage `80667853385c8970e3bf6143ab66e8085c46384b7e22bc4a9f2479fdb625449c`.
  Único cambio: cuerpo de `Stage.to_dict`, método postimage
  `8472007529bd89e57e782d9e2414e1ac88f2b8dadc60448bb4984945926f3d8e`.
  El exterior (firma incluida, cuerpo sustituido por Pass) conserva
  `5f0a57c8571fb42e8837e3fa7f2cbf5f9e254c6a91f610cab3e8ad884d947eb1`.
  Sólo CAPABILITY + authority de **tipo str exacto**, igual a
  `operation-owners@1`, puede publicar ese literal. Sólo su top-level detail con
  key `table_version` y value del mismo literal, ambos tipo str exacto, recibe
  la excepción. Nunca aliases, subclases, versiones, prefijos, otro stage/field,
  nested, IDs privados ni strings arbitrarios. El filtro genérico queda intacto.

Positivos prueban baseline y postimages in-memory; ledger synthetic serializa
realmente ambos campos, y los negativos mantienen la redacción base. Hook
skeleton prueba dict/None/handoff con todos los helpers stubbed: orden,
identidad de ctx/deps, un legacy call y preservación de estado. No certifica
runtime ni cero fuentes de la implementación futura. Mutantes dentro/fuera
fijan que la whitelist no permita reescritura amplia, import adicional,
segundo salto, payload/ctx cambiado, records omitidos/reordenados, regex u otro
método alterado. Nunca calcular expected desde el candidate en el test.

El pin permite sólo esos AST ya acreditados; no concede freeze/promoción ni
certifica C1.4 implementado. HERA congela el nuevo SHA y ZEUS fresco revisa prep
y permiso propuesto antes de habilitar ejecución del paquete C1.4. Un cambio de
postimage exige decisión/revisión nueva; no reseal automático.

## G. Manifiesto congelado y stops

El sha256 literal del `legacy_closure_manifest_88653752.json` permanece
`33e81b8eb80c4655c7ccccfd23af489ab5ad7951233d7dd3ffbb8ca2aaf32a09`.
El test nunca lo genera ni consulta Git. **Está prohibido regenerarlo para
legitimar un cambio de legacy.** Una divergencia requiere razón, pin exacto,
autoridad y ZEUS fresco; no reseal del manifiesto original.

STOP ante app compartido en prep, delta de producto no aprobado, relajación de
INV-SEP, postimage distinto al exacto acreditado, cambio R3/Gold/kit, selección v5 anticipada,
modificación de contrato C1.1/IR fuera del microgate o ampliación de turn_ledger
fuera del postimage acreditado. Ownership por cápsula de misión; `olympus-state own claim`
se intentó y quedó sin respuesta, terminándose sólo ese proceso por instrucción
de HERA: `GAP_REPORT=STATE_UNAVAILABLE`. Ningún cambio preexistente se sobrescribió.

La evidencia de comandos y gates vive fuera del producto en
`work/gates/c1-wave3-prep/` del hilo. Freeze, ZEUS, readiness final e integración
corresponden al root; este prep no los emite.

## Gates de primera entrega (antes de ampliación GO_SPEC)

- `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -q tests/test_c1_wave3_prep_boundary.py`: **8 passed in 0.20s**.
- `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -q tests/test_c1_wave3_prep_boundary.py tests/test_m1_wave1_parity_pins.py tests/test_m1_wave1_legacy_mode_runs_no_foundation.py`: **95 passed in 10.22s**. Corrí esos tests focales/expandidos, no la suite.
- `ruff check tests/test_c1_wave3_prep_boundary.py`: **All checks passed!**
- `semgrep --config .semgrep/secoppal-laws.yml --json --exclude tests/test_c1_wave3_prep_boundary.py app tests scripts`: **BASELINE=27**, errors 0. Base app/tests/scripts sin modificaciones, salvo el test nuevo excluido.
- `semgrep --config .semgrep/secoppal-laws.yml --json app tests scripts`: **CANDIDATE=27**, errors 0; comparación por rule/path/line/message: **NEW_DELTA=0**. Los 27 findings de baseline no se repararon fuera de scope.
- `git diff --check`: exit 0.
- `git diff --exit-code 5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f -- app`: exit 0, **APP_DELTA=0**.

Rondas de reparación: **0/4**. No suite global, providers, fuentes, ejecución app,
freeze ni autoauditoría. Estos gates acreditan el prep y preservación actual;
no acreditan C1.3/C1.4 implementados. La ampliación GO_SPEC posterior fija postimages sin ejecutarlos.

Ampliación documental final autorizada por HERA: sólo fila jobs de C0 §5.1.2,
conservando la implicación D1 y todos los demás bytes de C0. Comprobación
estructural por diff de esa ruta: una fila retirada y una añadida, ambas
`InterpretationProposal`, con regla de requests y no bicondicional:
**DOC_STRUCTURAL=PASS; ONLY_C0_JOBS_ROW_CHANGED=YES; NO_BICONDITIONAL=YES**.
Tras el delta documental corrí `pytest -q tests/test_c1_wave3_prep_boundary.py`
con el Python acreditado: **8 passed**, no la suite. `git diff --check` y ausencia
de app delta siguen exit 0. Expanded y Semgrep no se repitieron: no cambió Python.
Claim ampliado volvió a timeout 10s; no hay baseline del state API acreditado
para este worker: **GAP_REPORT=STATE_UNAVAILABLE**, sin bloquear la entrega.

## Gates finales de ampliación GO_SPEC

Failure→adjudicación: el primer expanded dio **476 passed, 1 failed** en
`test_the_allowed_divergences_are_real_and_inside_the_closure`: exigía que toda
entrada ya difiriera de la base, incompatible con preparar permiso estrecho y
APP_DELTA=0. HERA adjudicó **CONTRACT_APPLICATION**, autorizando sólo los dos
staged names literales con baseline/postimage exacto y closure obligatorio;
todas las otras divergencias conservan `sha != manifest`. Autorizó también
la rama mínima del gate canónico para ejecutar los narrow pins directamente,
incluidos overrides. No se cambió app para volver verde el test ni se tocó
hash/manifiesto. Controles negativos invocan ahora el gate canónico, no sólo
una comprobación independiente de metadata.

- Focal nuevo: `python -m pytest -q tests/test_c1_wave3_prep_boundary.py`:
  **47 passed** (Python acreditado `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`).
- Focal pin/metadata canónico: ese Python con
  `-m pytest -q tests/test_c1_wave3_prep_boundary.py tests/test_conversation_store_clear_delta.py tests/test_m1_wave1_parity_pins.py`:
  **179 passed in 8.69s**.
- Expanded afectado: ese Python con
  `-m pytest -q tests/test_c1_wave3_prep_boundary.py tests/test_conversation_store_clear_delta.py tests/test_m1_wave1_parity_pins.py tests/test_m1_wave1_legacy_mode_runs_no_foundation.py tests/test_m2a_boundaries.py`:
  **481 passed in 13.58s**. Corrí esos tests, no la suite.
- `ruff check tests/test_c1_wave3_prep_boundary.py tests/test_conversation_store_clear_delta.py tests/test_m1_wave1_parity_pins.py`:
  **All checks passed!**
- `semgrep --config .semgrep/secoppal-laws.yml --json app tests scripts`:
  **BASELINE=27, CANDIDATE=27, NEW_DELTA=0, ERRORS=0** frente al baseline
  capturado al inicio sobre los mismos app/tests/scripts (nuevo test excluido).
- `git diff --check`: exit 0. `git diff --exit-code 5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f -- app`: exit 0.
- `git diff --exit-code 75acaa76d4af168c074721d9460be6fd33765577 -- tests/fixtures/legacy_closure_manifest_88653752.json docs/audits/m1_search_foundation/C0_SPINE_V5_DESIGN.md`: exit 0; esta ampliación no cambia manifiesto ni C0 ya alineado.

Dos iteraciones de reparación del paquete de tests: ajuste de instrumentación
al mask contractual del ledger/anchors de mutantes; después aplicación del
contrato staged metadata adjudicada por HERA. **2/4**, cero reparaciones producto.
No freeze, auditoría, suite global, activación ni implementación. Evidence logs:
`work/gates/c1-wave3-prep/postimage-*` externo al producto. SHA final lo reporta
ARES y lo congela HERA; los postimages no autorizan ejecutar Foundation nuevo
bajo la prohibición humana vigente.

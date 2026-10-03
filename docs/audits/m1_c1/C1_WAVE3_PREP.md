# C1 Wave 3 — preparación de ownership y frontera

Base leída: `main` `5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f`.
Paquete docs/tests; **cero cambios app**, sin implementación C1.3/C1.4,
sin freeze, auditoría, promoción ni activación. Este documento propone el reparto
para adjudicación de HERA; no cambia ADR-103 ni concede permisos de runtime.

Autoridad leída: C0 §5.1–5.3 y filas C1.3/C1.4 de §6; ADR-103 enm. 4
D1(b/e/f), D3 y D13; C1.2 candidato y deuda de frontera; IR v2 real
`interpretation_contract.py`, `foundation_operations.py`, `foundation_lift_v1.py`.
D1/D3 de C1.2 siguen siendo decisiones del microgate separado. Su resultado
`GO_CON_MATIZ` no es un postimage autorizado del hook. Este prep no cambia jobs
vacíos, rescate de vocabulario, UNSUPPORTED explícito ni conversiones v1.

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
  condicionado al postimage y permiso de divergencia. No abrir todo `_dispatch`.
- `app/core/turn_ledger.py`: **no permitido por ahora**. Sólo si una prueba
  demuestra insuficiencia del API existente, con delta exacto y autorización
  separada. `StageName.CAPABILITY` ya existe; `Stage.decision` es `str` y
  `record(Stage)` está disponible. No crear otro ledger.

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
| `turn_ledger` | `CAPABILITY`, `Stage`, `record`; insuficiencia aún no demostrada |
| INV-SEP legacy | manifiesto 88653752 + `test_m1_wave1_parity_pins.py`; hook nuevo no está en su whitelist actual |

D1/D3 se adjudican antes de habilitar C1.3. C1.4 no depende de que el proveedor
produzca wire 5 para probar IR oráculo. C1.6 y CUTOVER siguen fuera de alcance:
MODE_FRONTIER no se convierte aquí en DISPATCH_FRONTIER. Graft actualizado al
leer (grafo externo a repo); la fuente manda sobre topología indexada.

## E. Divergencia legacy requerida, aún no concedida

La whitelist vigente contiene `semantic_dimensions`, hoja `foundation_guard`
y `conversation_store` (delta M2A-2). **No contiene orchestrator.** C1.4 debe
acreditar una entrada estrecha de `ALLOWED_DIVERGENCE` para
`app.core.orchestrator`, ligada al pin AST de §F y al permiso explícito de HERA.
No se añade en este prep: el pin histórico está protegido. No se exonera todo
el módulo ni toda la función por nombre. `turn_ledger` necesitaría la misma
prueba/autoridad si llegara a tocarse.

## F. Pin AST cerrado

`tests/test_c1_wave3_prep_boundary.py` pinea el AST completo de orchestrator salvo
un único bloque de statements en `_dispatch`, delimitado por:

1. Import directo existente `from app.core.foundation_runtime import run_foundation_search`.
2. El `with legacy_budget(...)` existente, que queda **fuera** del bloque permitido.

El bloque actual comprende el import, assignment `foundation = ...` y
`if foundation is not None`. La frontera debe aparecer exactamente una vez.
Todo AST fuera del bloque conserva digest literal de la base. El bloque también
conserva digest literal: `APPROVED_C1_4_HOOK_AST` es vacío. Por tanto **hoy no hay
ningún cambio runtime permitido**, incluido dentro del slot. No es readiness GO.

Para abrir C1.4 falta un postimage completo acreditado (árbol exacto, sin wildcard),
el hash literal de ese postimage y la entrada estrecha de §E. Eso requiere gate
y revisión del nuevo candidate; jamás calcular el hash esperado desde el código
candidate dentro del test ni actualizarlo automáticamente para conseguir verde.
El postimage debe implementar la disposición de C0 §5.3 sin cambiar ctx/deps ni
los handlers precedentes. Si exige más imports fuera del slot, cambia el contrato
del pin y vuelve a HERA antes de editar. El pin ignora comentarios y posiciones;
no acredita paridad de bytes, runtime ni R3 por sí solo.

Los controles negativos mutan ctx dentro del slot, alteran una llamada previa,
cambian fallback, añaden import fuera del slot y duplican/borran el delimitador.
Así se demuestra que la exención no acepta reescritura amplia de dispatch.

## G. Manifiesto congelado y stops

El sha256 literal del `legacy_closure_manifest_88653752.json` permanece
`33e81b8eb80c4655c7ccccfd23af489ab5ad7951233d7dd3ffbb8ca2aaf32a09`.
El test nunca lo genera ni consulta Git. **Está prohibido regenerarlo para
legitimar un cambio de legacy.** Una divergencia requiere razón, pin exacto,
autoridad y ZEUS fresco; no reseal del manifiesto original.

STOP ante app compartido en prep, delta de producto no aprobado, relajación de
INV-SEP, postimage desconocido, cambio R3/Gold/kit, selección v5 anticipada,
modificación de contrato C1.1/IR fuera del microgate o necesidad de turn_ledger
sin reproducción. Ownership por cápsula de misión; `olympus-state own claim`
se intentó y quedó sin respuesta, terminándose sólo ese proceso por instrucción
de HERA: `GAP_REPORT=STATE_UNAVAILABLE`. Ningún cambio preexistente se sobrescribió.

La evidencia de comandos y gates vive fuera del producto en
`work/gates/c1-wave3-prep/` del hilo. Freeze, ZEUS, readiness final e integración
corresponden al root; este prep no los emite.

## Gates de esta entrega

- `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -q tests/test_c1_wave3_prep_boundary.py`: **8 passed in 0.20s**.
- `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -q tests/test_c1_wave3_prep_boundary.py tests/test_m1_wave1_parity_pins.py tests/test_m1_wave1_legacy_mode_runs_no_foundation.py`: **95 passed in 10.22s**. Corrí esos tests focales/expandidos, no la suite.
- `ruff check tests/test_c1_wave3_prep_boundary.py`: **All checks passed!**
- `semgrep --config .semgrep/secoppal-laws.yml --json --exclude tests/test_c1_wave3_prep_boundary.py app tests scripts`: **BASELINE=27**, errors 0. Base app/tests/scripts sin modificaciones, salvo el test nuevo excluido.
- `semgrep --config .semgrep/secoppal-laws.yml --json app tests scripts`: **CANDIDATE=27**, errors 0; comparación por rule/path/line/message: **NEW_DELTA=0**. Los 27 findings de baseline no se repararon fuera de scope.
- `git diff --check`: exit 0.
- `git diff --exit-code 5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f -- app`: exit 0, **APP_DELTA=0**.

Rondas de reparación: **0/4**. No suite global, providers, fuentes, ejecución app,
freeze ni autoauditoría. Estos gates acreditan el prep y preservación actual;
no acreditan C1.3/C1.4 implementados ni autorizan el postimage pendiente.

# C1.3 — wire 5 y perfiles explícitos, v3 activo preservado

Base `524e709a5f99080e773ff39d55cc6eb9142147cf`; branch `mission/c1-3-wire-v2`.
Orden humana actual transmitida por HERA: C1.3/C1.4 continúan en paralelo.
Autoridad: C0 §5.1/§5.2, ADR-103, cierre D1/D3 y ownership prep §A.
Este paquete no implementa C1.4, selección runtime C1.6, activación ni M2A-5/6.
Candidate SHA es el commit que contiene este documento; root hace freeze/ZEUS.

## Cambio por archivo

- `app/core/foundation_decoder_v2.py` nuevo: wire 5 con `jobs` obligatorio,
  vocabulario v2 exacto, evidencia host B o spans C sólo harness/replay, listas y
  claves cerradas. Reutiliza IR/validaciones actuales, sin importar lift v1.
  No sniffing, corrección heurística, operación en requests, valores ejecutables,
  subject/geo/aspect inferidos ni `universe_kind` por defecto. La ausencia de
  campos obligatorios rechaza toda propuesta.
- `app/core/foundation_producers.py`: `PROFILES` inmutable de dos perfiles,
  cada uno liga prompt + decoder + metadata. `LinguisticProducer(provider,
  *, profile="foundation_v3")` conserva la firma histórica de un argumento;
  nombres ajenos se rechazan. `configured_producer` queda igual y no lee un
  selector nuevo: siempre usa default v3. La telemetry identifica el perfil
  efectivamente empleado, nunca la versión adivinada del payload.
- `app/core/foundation_prompts_v5.py` nuevo: draft mínimo inactivo de arm B y
  wire documentado, hash para telemetry; no freeze del prompt C1.6 ni evaluación
  de calidad lingüística. El prompt v3 permanece byte-idéntico.
- `app/core/foundation_universe_kind.py`: versiones 4.2/5; argumento de versión
  interno explícito, default 4.2 preservado. En wire 5 unspecified prohíbe
  `universe_kind`; no genera claim/evidence de universo. Universo especificado
  mantiene kind obligatorio y expansión basada únicamente en su evidencia.
- `tests/test_c1_3_wire_v2.py` nuevo: 46 casos sintéticos, incluidos dos tests
  Hypothesis (50/60 ejemplos). Cruzados y empty-cross no producen propuesta,
  vocabulario completo y malformado, claves ejecutables ajenas, operadores
  fuera de tabla, D1 vacío/sin requests/request sin jobs, ausencia de campos,
  evidencia foreign/malformada, referent único, unspecified, geolevel explícito,
  None no convertido a TOTAL, UNSUPPORTED explícito y no rescate TIME/before.
  Guard autouse comprueba cero query/count/aggregate durante todos estos casos.

## Invariantes y límites

D1: `jobs=[]/requests=[]` conserva `NO_ACTION/NO_REQUEST`. Requests requieren
jobs; non-SEARCH acreditado puede no tener requests. No bicondicional.
D3: vocabulario según perfil; rechazo íntegro sin propuesta ni source call.
COUNT sin aspect conserva **None**; COUNT con TOTAL explícito conserva TOTAL.
V2 no hereda lift/defaults v1. IR, ownership C1.4 y pines prep no se modificaron.

Un único fallo focal nuevo fue un **ORACLE_PROBLEM** adjudicado por HERA:
mi test suponía que COUNT sin aspect debía rechazarse. `job_shape_error` real
permite None o TOTAL y C0 define aspect opcional; corregí test/prompt propuesto
para respetar esa autoridad. No cambié contrato/IR ni fabriqué TOTAL.
Rondas de reparación: **1/4**, expectativa nueva y su prompt, sin parche shared.

## Gates ejecutados

Python acreditado en todos los comandos pytest:
`/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.

- `python -m pytest -q tests/test_c1_3_wire_v2.py`: **46 passed in 0.39s**.
- `python -m pytest -q tests/test_foundation_prompts.py tests/test_foundation_ir_v2.py`: **92 passed in 1.51s**.
- `python -m pytest -q tests/test_c1_3_wire_v2.py tests/test_foundation_prompts.py tests/test_foundation_ir_v2.py tests/test_foundation_runtime.py tests/test_foundation_basic_fallback.py tests/test_foundation_route_ownership.py tests/test_m1_wave1_parity_pins.py tests/test_m1_wave1_legacy_mode_runs_no_foundation.py tests/test_m2a_boundaries.py tests/test_c1_wave3_prep_boundary.py tests/test_conversation_store_clear_delta.py tests/test_module_size_limits.py`:
  **718 passed in 35.82s**. Corrí esos tests afectados, no la suite.
- `ruff check app/core/foundation_decoder_v2.py app/core/foundation_producers.py app/core/foundation_prompts_v5.py app/core/foundation_universe_kind.py tests/test_c1_3_wire_v2.py`: **All checks passed!**
- `semgrep --config .semgrep/secoppal-laws.yml --json app tests scripts` antes
  de cambios y sobre candidate: **BASELINE=27 CANDIDATE=27 NEW_DELTA=0 ERRORS=0**,
  comparación rule/path/line/message. Findings base no reparados fuera de scope.
- `git diff --check`: exit 0.
- Diff contra base sobre IR, operations, accepted, runtime, orchestrator, ledger,
  config, prompt v3 y pines prep: exit 0. Gold/oráculo/kit no tocados.

Graft callers de LinguisticProducer y lectura de fuente acreditan default
`configured_producer`; tgrep para prompt/versiones. Evidencia externa:
`work/gates/c1-3-wire-v2/` del hilo. Claim Olympus se intentó, timeout 10s;
ownership se mantuvo por cápsula exclusiva de HERA: `GAP_REPORT=STATE_UNAVAILABLE`.
Sin suciedad preexistente, otros writers ni subagentes en este worktree.

## Pendientes

Root freeze y ZEUS fresco. Prompt v5 congelado/calidad productiva, selección de
perfil runtime, handoff operativo y activación no están certificados ni dentro
del paquete. Los tests sintéticos acreditan contrato mecánico y compatibilidad
activa, no exactitud semántica del proveedor real. No suite global, llamadas a
proveedores/fuentes, ejecución app, main/push/deploy/restart ni promoción.

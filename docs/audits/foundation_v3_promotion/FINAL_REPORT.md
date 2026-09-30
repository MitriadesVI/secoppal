# Informe final — FOUNDATION_V3_SELECTIVE_PROMOTION_V1

## Candidato

- Base: `main` = `680907653b190a277993ce813b070b1984aee2b5` (no avanzó durante la misión).
- Rama: `feature/foundation-v3-promotion-v1`, worktree `../secoppal-foundation-v3-promotion-v1`.
- Código congelado para gates: `0dc5985a` (último commit con cambios en `app/` o `tests/`). El
  HEAD final añade sólo documentación e instrumentos de `scripts/audits/`; ver `git log`.
- Sin merge, push, deploy, restart, activación, red ni gasto. Árbol limpio al cierre.

## Commits (en orden)

| Commit | Contenido |
|---|---|
| `7a99874a` | plan, preflight e inventario selectivo (antes de copiar código) |
| `89a74384` | nodeids de la baseline de `main` |
| `4035226e` | trasplante del core: contrato, admisión, proyección, guard, H7, tokens, recibos; prompt recortado a B v4.2; productor B único; un flag; opt-in O1B en el builder |
| `2a9159be` | integración después de los dueños vigentes (`foundation_route_owner`), traza y guarda de continuación |
| `e3121e4d` | G1/T4: todas las citas de TOPIC/BUYER/GEOGRAPHY |
| `637ff179` | G2: la ocurrencia del universo nunca es tema |
| `b83161c2` | composición de estados (sólo el embudo ADR-100/S3) |
| `1d1069d1` | dirección y límite del operador monetario |
| `49da6fc4` | mensajes humanos deterministas |
| `bcb21746` | contrato del fallback (antes de implementarlo) |
| `b469886d` | fallback rotulado por caída del proveedor |
| `7cae662b` | puertas de producto y corpus de regresión |
| `0dc5985a` | propiedad de turnos Suppliers con forma de compuerta, kit de mutaciones y red bloqueada |
| HEAD | ADR-100 (aclaración), ADR-101, ADR-102, bitácora, gates, delta, mutaciones, handoff, paridad |

## Diff runtime real (`app/`, frente a `main`)

Nuevos: `foundation_admission`, `foundation_basic_fallback`, `foundation_evidence`,
`foundation_guard`, `foundation_messages`, `foundation_producers`, `foundation_projection`,
`foundation_prompts`, `foundation_route_owner`, `foundation_runtime`,
`foundation_state_composition`, `foundation_text_predicates`, `foundation_token_plan`,
`foundation_universe_kind`, `foundation_universe_policy`, `foundation_value_direction`,
`interpretation_contract`, `source_call_receipts`.
Modificados: `config.py` (2 flags), `orchestrator.py` (ciclo de recibos + entrada en
`_dispatch`), `secop_client.py`, `soql_builder.py`, `observer.py`, `feedback.py`,
`semantic_dimensions.py`, `workflow/helpers.py`, `workflow/actions/execution.py`,
`workflow/dual_dataset.py`. Rechazados: `foundation_tender_complement.py`, el hook previo a
`coordinate_turn`, los selectores de productor y de H7 (`SELECTIVE_INVENTORY.md`).

## Resultados

- **Focales Foundation:** 1855 passed (detalle en `COMPONENT_GATES.md`).
- **Preservación** (Suppliers, A2 dogfood, Contract Activity replay, follows, Entity Profile
  acciones, actor flag off, paridad Truth Layer, product contract): verdes.
- **Corpora de auditoría:** 174 casos, veredictos idénticos a `main` caso a caso.
- **Mutaciones:** 20/20 muertos por aserción del test de la propiedad (`MUTATION_REPORT.json`).
- **Suite global** (`GLOBAL_DELTA.json`): base 119 failed + 2 errors; candidato 120 failed +
  2 errors, 18 366 passed (+1855 tests nuevos). `BASE_ONLY = 0`, los 120 nodeids rojos de la
  base siguen rojos, sin errores nuevos, sin pasados perdidos, sin cuelgues. Un nodeid exclusivo
  (`test_supplier_request_authority::test_arbitrary_digit_boundaries…`) es un defecto latente de
  `main` que Hypothesis muestrea según la semilla: `--hypothesis-seed=19` falla idéntico en base y
  candidato, el módulo no está en el diff. Se propuso como tarea aparte.
- **Paridad flag OFF:** 785 consultas reales de 12 corpus × 2 perfiles (defaults; Frame+Actor ON):
  idénticas en ruta, params, SoQL, llamadas (multiconjunto), total, respuesta, dueño de
  continuación, claves del resultado y traza persistida, tras enmascarar sólo reloj
  («consultado el …», `_opportunity_reference_instant`). 0 claves Foundation con flag OFF.
- **Primera infancia:** A–F verdes por la ruta real (`test_foundation_product_gates.py`).
- **Estáticos:** ruff sólo con el F841 preexistente; semgrep delta efectivo 0 (hallazgo
  preexistente desplazado 2 líneas); `lint-core` y `diff --check` limpios.

## Contrato y resultado del fallback

Contrato en `BASIC_FALLBACK_CONTRACT.md` (commiteado antes). Sólo `PROVIDER_ERROR`; parser
determinista vigente sin `llm_parse`; lista blanca cerrada (dataset, tema, comprador y
departamento resueltos, fechas); rótulo «Búsqueda básica — modo degradado»;
`second_llm_calls = 0` probado con un `llm_handler` espía. Fuera del contrato: cero llamadas y
reintento. Nunca por `INVALID_PROPOSAL` ni por otro terminal.

## Veredictos

| Componente | Veredicto |
|---|---|
| FOUNDATION_CORE_TRANSPLANT | GO |
| PROMPT_V42_INTEGRITY | GO (`a5867d40…` recalculado y fijado en test) |
| H7_MANDATORY_PROFILE | GO |
| ROUTE_OWNER_PRESERVATION | GO |
| NATURAL_ALPHA_SUPPLIERS_PRESERVATION | GO |
| HISTORICAL_TENDER_COMPLEMENT_EXCLUSION | VERIFIED |
| HOST_SILENT_PATH_HARDENING | GO |
| HUMAN_CLARIFICATION_UX | GO |
| BASIC_PROVIDER_FALLBACK | GO (acotado; ver límites) |
| FIRST_INFANCY_END_TO_END | GO (offline, propuestas del productor fijadas en el corpus) |
| FLAG_OFF_PARITY | GO |
| GLOBAL_DELTA | ZERO_CANDIDATE_ONLY (1 nodeid adjudicado como muestreo Hypothesis de un defecto de `main`) |
| FOUNDATION_PROMOTION_CANDIDATE | GO_INDEPENDENT_REVIEW |

`GO_INDEPENDENT_REVIEW` no es merge ni producción.

## Hallazgos nuevos (para el usuario)

1. **Paridad rota en el laboratorio:** `f703504b` relajaba para todo el producto la guarda
   anti-consulta-global con O1B; acotado a Foundation.
2. **Productor por defecto equivocado en el laboratorio:** brazo C, no el B medido.
3. **Continuación silenciosa:** tras un turno Foundation, un fragmento legado perdía el tema;
   ahora aclara.
4. **Frases de comprador leídas como actor por el parser legado** («busca licitaciones de
   alcaldía de barranquilla en 2024»): Foundation respeta al dueño ACTOR; pérdida de cobertura
   para `FOUNDATION_ENTRY_ADMISSION_V1`.
5. **Defecto de `main` fuera de alcance:** «001 proveedores» se autoriza como ejecutable en la
   autoridad de solicitudes de proveedores.
6. `count` de `SecopClient` reintenta con espera un error de contrato
   (`FOUNDATION_COUNT_PAYLOAD_INVALID`); deuda, no se tocó la política compartida.

## Deudas y límites

- Sin medición viva: latencia y costo del productor no se repitieron (sin red); la telemetría
  para dogfood está instrumentada (`foundation_telemetry`: versión, SHA del prompt, modelo
  reportado, latencia, error del proveedor, validez, código interno, autoridad/reglas/desacuerdo
  H7, llamadas, latencia total, fallback). Las razones de declinación de ruta no se persisten
  en la traza legada.
- El fallback usa la semántica legada de `objeto` (palabras con AND, no frase) y un parser con
  bugs conocidos; por eso su lista blanca excluye valor, estado, modalidad y negación.
- G2 pide aclaración cuando el productor cita «contratos de primera infancia» como un solo span;
  no hay recorte léxico. Frecuencia real desconocida sin llamadas vivas.
- Operadores inclusivos («al menos», «hasta») se aclaran: la proyección sólo tiene > y <.
- Rollback: un turno Foundation guardado deja contexto vacío para un follow-up legado.
- Límites DA-6 heredados (ADR-102); el paquete no se copia a CI (opción B); deuda de
  `page_ids`/orden para un paquete futuro.
- Siguiente misión ya decidida: `FOUNDATION_ENTRY_ADMISSION_V1`.

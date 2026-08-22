# SEARCH-SH1 — Semantic Dimension + Monetary Follow-up Hardening

Date: 2026-08-22  
Branch: `fix/search-semantic-hardening-v1`  
Base: `7f245177227636b488254ff4782835b23725fcce` (`main`)  
Verified code head: `c4d1984b8818412abd1c452962e4dc4f5c351c3f`

## Verdict

`SEARCH_SH1: PASS`

The two reported defects are fixed without changing Actor Profile files or the
existing modality resolver/catalog/lexicon. Live SECOP replay returned the
original 46-row Atlántico base universe, a four-row `>= $100M` refinement with
zero rows below the threshold, and a correctly exhausted filtered pagination
universe. The bounded mutation campaign detected 15/15 mutants and restored all
temporarily changed sources byte-for-byte.

## Frozen reproductions and diagnosis

The four regressions were frozen before runtime changes in
`tests/test_search_sh1_dogfood_regressions.py`.

### R3 boundary trace

Raw turns:

- Base: `contratos adulto mayor gobernacion de atlantico en 2026`
- Follow-up: `por mas de 100 millones en 2026`

The parser was not the loss point. Before and after the fix it emitted
`valor_min=100000000` for the follow-up.

| Boundary | Before fix | After fix |
|---|---|---|
| Parsed base | contracts; `adulto_mayor`; `DEPARTAMENTO DEL ATLANTICO**`; 2026 | unchanged |
| Parsed follow-up | processes default, non-explicit; 2026; `valor_min=100000000` | unchanged |
| Previous `QueryFrame` | contracts + topic + buyer + 2026 | unchanged |
| Current `QueryFrame` | no topic/scope; 2026 + `valor_min=100000000` | unchanged |
| `followup_intent_type` | `change_year` | `refine_filter` because money is an orthogonal delta |
| Mutation request | dates + `valor_min` | dates + `valor_min` |
| Gate result | dates accepted; `valor_min` rejected as `field_not_allowed_for_intent` | dates and `valor_min` accepted; no rejected fields |
| Merged frame | prior topic/buyer/dataset + dates; no money | prior topic/buyer/dataset + dates + `valor_min` |
| `params_from_frame` | no `valor_min` | `valor_min=100000000` |
| SoQL field | none | `valor_del_contrato >= 100000000` |
| Stored context | unfiltered params/fingerprint | filtered params/fingerprint |

`MONEY_LOSS_STAGE: FollowupClassifier(change_year) -> followup_mutations.gate_delta(change_year allowed_set)`

The exact rejected field was `valor_min`. Because it disappeared before frame
serialization, the resulting query fingerprint did not represent a new value
universe and the old result sidecar remained eligible for pagination.

### Current deterministic trace

The final trace reports:

```text
intent = refine_filter
requested_fields = [fecha_desde, fecha_hasta, valor_min]
accepted_fields = [fecha_desde, fecha_hasta, valor_min]
rejected_fields = []
merged dataset = contratos
merged topic = [adulto_mayor]
merged buyer = DEPARTAMENTO DEL ATLANTICO**
merged dates = 2026-01-01 .. 2026-12-31
merged valor_min = 100000000
```

The final predicate is:

```sql
valor_del_contrato >= 100000000
```

### R4 pagination root cause and fix

There were two coupled failure modes:

1. R3 lost the value delta, so its fingerprint remained equivalent to the old
   unfiltered query.
2. A non-pagination semantic turn did not unconditionally discard all cached
   result groups before executing the new universe.

The fix now:

- clears `offset`, `_display_offset`, and `_shown_total` for every accepted
  semantic mutation;
- deletes all pagination-buffer groups at the normal semantic-turn boundary;
- leaves conversation history intact;
- saves the new filtered query parameters/fingerprint for subsequent paging.

This also handles zero-result, clarification, timeout, and dual-dataset sibling
cases without allowing a stale sidecar to survive.

## Source audit

Official sources audited live on 2026-08-22:

- SECOP II processes, `p6dx-8zbt`:
  <https://www.datos.gov.co/resource/p6dx-8zbt.json>
- SECOP II electronic contracts, `jbjy-vk9h`:
  <https://www.datos.gov.co/resource/jbjy-vk9h.json>

### Structured fields

| Dataset | Contract type field | Modality field | Relevant justification field |
|---|---|---|---|
| Processes | `tipo_de_contrato` | `modalidad_de_contratacion` | `justificaci_n_modalidad_de` |
| Contracts | `tipo_de_contrato` | `modalidad_de_contratacion` | `justificacion_modalidad_de` |

Processes also exposes `subtipo_de_contrato`, but all 9,040,457 observed rows
were `No Definido`; it is not usable for this layer.

### Frequent type evidence

| Source value | Processes | Contracts |
|---|---:|---:|
| Prestación de servicios | 5,840,731 | 5,119,097 |
| Decreto 092 de 2017 | 1,264,820 | 210,122 |
| Suministros | 733,445 | 148,790 |
| Otro | 371,909 | 216,266 |
| Compraventa | 333,511 | 118,136 |
| Obra | 143,108 | 52,297 |
| Arrendamiento de inmuebles | 46,271 | 47,599 |
| Interventoría | 28,636 | 13,066 |
| Consultoría | 24,060 | 11,477 |
| Arrendamiento de muebles | 8,838 | 2,292 |
| Acuerdo de cooperación | 27 | 22 |

No exact `tipo_de_contrato = Convenio` source value was found.

### Relevant justification evidence

| Source value/family | Processes | Contracts |
|---|---:|---:|
| Contratos o convenios Interadministrativos (valor cero) | 81,023 | 82,693 |
| Contratos o convenios Interadministrativos (con valor) | 54,909 | 41,130 |
| Convenios Solidarios Regimen articulo 92 Ley 2166 de 2021 | 2,208 | 2,342 |
| Law2166of2021_Article95_SolidarityAgreements | not observed | 6,558 |
| Contratos interadministrativos | 562 | 406 |

The interadministrative combined family is mostly stored as contract type
`Otro`, proving that `tipo_de_contrato` alone cannot represent natural
`convenio` language.

### Term classification and projection policy

Counts in composite rows are candidate-family counts, not an assertion that
every row in the family is a convenio. Controlled text evidence is deliberately
required where the source value conflates contracts and agreements.

| Natural term | Classification | Evidence-backed policy |
|---|---|---|
| convenio / convenios | `COMPOSITE` | solidarity justification family OR combined interadministrative family + `convenio` text OR Decreto 092 + `convenio` text OR exact `Acuerdo de cooperación` type |
| convenio interadministrativo | `COMPOSITE` | combined interadministrative justification + controlled `convenio` text |
| convenio de asociación | `COMPOSITE` | Decreto 092 type/justification + controlled `convenio` text |
| convenio solidario | `STRUCTURED_FAMILY` | the Spanish and English solidarity justification values |
| contrato interadministrativo | `COMPOSITE` | exact `Contratos interadministrativos` justification OR combined family + exact phrase text |
| prestación de servicios | `STRUCTURED_EXACT` | `tipo_de_contrato = 'Prestación de servicios'` |
| obra | `STRUCTURED_EXACT` | `tipo_de_contrato = 'Obra'`; plural `obras` remains thematic for compatibility |
| suministro | `STRUCTURED_EXACT` | `tipo_de_contrato = 'Suministros'` |
| arrendamiento | `STRUCTURED_FAMILY` | inmueble + mueble source types (55,109 process; 49,891 contract rows) |
| compraventa | `STRUCTURED_EXACT` | `tipo_de_contrato = 'Compraventa'` |

Representative positive records:

- Process `CO1.REQ.1673022`: combined interadministrative justification,
  `CONVENIO SOLIDARIO LA PLAYA`, type `Obra`.
- Process `CO1.REQ.7235254`: explicit solidarity justification, type `Obra`.
- Process `CO1.REQ.233217`: exact `Contratos interadministrativos`
  justification, type `Otro`.
- Contract `CO1.PCCNTR.6103113`: combined interadministrative justification,
  object begins `CONVENIO DE TRANSFERENCIA DE RECURSOS`, type `Otro`.
- Contract `CO1.PCCNTR.6685652`: explicit solidarity justification, type `Obra`.
- Contract `CO1.PCCNTR.208804`: exact `Contratos interadministrativos`
  justification, type `Otro`.
- Contract `CO1.PCCNTR.999428`: Decreto 092 and an explicit `CONVENIO DE
  ASOCIACIÓN` object.

Representative false-positive evidence explains why raw `%convenio%` cannot be
treated as equivalent to contractual nature:

- Process `CO1.REQ.9999985` is a service contract supporting execution of a
  separately referenced convenio.
- Contract `CO1.PCCNTR.997722` is an `Obra` under a separately referenced
  convenio.
- Contract `CO1.PCCNTR.997401` is an `Interventoría` over works derived from
  convenios.

## Semantic design

`SemanticDimension` formalizes:

```text
TOPIC
MODALITY
CONTRACT_TYPE_OR_NATURE
ACTOR
BUYER
GEOGRAPHY
TIME
VALUE
STATE
CONTROL_LANGUAGE
```

The small `ContractTypeInterpretation` is independent from modality and exposes
canonical IDs, classifications, consumed spans, and residual text. The router
runs it after modality masking and before catch-all topic extraction. The
invariant is therefore structural:

`INV-STRUCTURED-DIMENSION-NOT-TOPIC`

Projection remains explicit per dataset because justification field names and
object/name fields differ. Modality procedure IDs are never used as contractual
type IDs.

## Modality audit

The existing modality resolver/catalog/lexicon files were not modified.

| User language | Procedure IDs | Consumed/residual/topic | Processes/contracts |
|---|---|---|---|
| licitación / licitación pública | `licitacion_publica`, `licitacion_publica_obra` | consumed; empty residual; no leak | both |
| selección abreviada | `seleccion_abreviada_menor_cuantia`, `seleccion_abreviada_menor_cuantia_sin_manifestacion`, `seleccion_abreviada_subasta_inversa` | consumed; empty residual; no leak | both |
| selección abreviada de menor cuantía | `seleccion_abreviada_menor_cuantia` | consumed; empty residual; no leak | both |
| subasta inversa | `seleccion_abreviada_subasta_inversa` | consumed; empty residual; no leak | both |
| mínima cuantía | `minima_cuantia` | consumed; empty residual; no leak | both |
| concurso de méritos | `concurso_meritos_abierto`, `concurso_meritos_precalificacion` | consumed; empty residual; no leak | both |
| contratación directa | `contratacion_directa`, `contratacion_directa_con_ofertas` | consumed; empty residual; no leak | both |
| régimen especial | `regimen_especial`, `regimen_especial_con_ofertas` | consumed; empty residual; no leak | both |
| régimen especial con ofertas | `regimen_especial_con_ofertas` | consumed; empty residual; no leak | both |
| régimen especial sin ofertas | `regimen_especial` | consumed; empty residual; no leak | both |
| acuerdo marco / AMP | `acuerdo_marco_precio_secop` | consumed; empty residual; no leak | both |
| enajenación de bienes | `enajenacion_sobre_cerrado`, `enajenacion_subasta` | consumed; empty residual; no leak | both |

The frozen abbreviated-selection control still expands to exactly three
children and excludes AMP. `convenio` + `régimen especial` produces two
independent dimensions.

## Money and mutation semantics

All requested minimum, maximum, and range phrases survive parser, frame,
intent gate, merge, params, SoQL, and stored context.

- Repeated year + new money becomes a composable refinement, not a year-only
  mutation.
- Scope + money and modality + money are accepted together.
- A complete new range replaces both endpoints.
- A new minimum clears an inherited maximum only when the bounds conflict;
  the symmetric rule applies to a new maximum.
- Contracts use `valor_del_contrato`; processes use `precio_base`.
- Every accepted semantic delta clears pagination cursors.

Mutation campaign:

```text
M01..M15 detected
TOTAL 15
DETECTED 15
RESTORED_BYTE_FOR_BYTE YES
```

The executable harness is `scripts/run_search_sh1_mutations.py`.

## Live dogfood replay

The production workflow was run with isolated conversation storage and real
official SECOP endpoints.

| Case | Dataset | Live result | Semantic verification |
|---|---|---:|---|
| A base Atlántico adult senior contracts, 2026 | contracts | 46 | topic, buyer, year preserved |
| A `por mas de 100 millones en 2026` | contracts | 4 | `valor_min=100000000`; minimum raw value `$348,747,143`; rows below threshold `0` |
| A `más resultados` | contracts | filtered total 4, exhausted | retained `valor_min`; no stale rows or IDs |
| B Barranquilla convenios, 2026 | processes | 2 | `contract_type_include=[convenio]`; no `objeto` leak |
| C convenio + special regime + Barranquilla + `>2B` + 2026 | processes | 0 | valid source-backed intersection; both dimensions, buyer, time and money present; no topic leak |
| D abbreviated selection Barranquilla, 2026 | contracts | 19 | exact three-child family preserved |
| E adult senior Cartagena `>1B`, 2025 | contracts | 5 | thematic `adulto_mayor` preserved |

One initial A fetch timed out at the workflow's bounded eight-second SECOP
budget; the immediate mandatory replay succeeded with the exact 46/4 counts.
The configured narrator account also reported insufficient balance during one
turn, but deterministic parsing, SECOP retrieval, counts, and validation were
unaffected.

## Verification gates

| Gate | Result |
|---|---|
| Frozen R1–R4 | pass |
| Contractual/adversarial focused tests | pass |
| Modality resolver/dialogue/E2E tests | 3,327 passed |
| Query corpus | 89/89 passed |
| Dialogue policy corpus | 60/61; pre-existing `GOB-DEPTO-001` mismatch in unchanged `query_plan` |
| Money/follow-up/pagination focused tests | 364 passed |
| Actor Profile focal sentinels | 8 passed |
| Mutation campaign | 15/15, byte-restored |
| Final full suite | 8,250 passed, 2 skipped, 24 expected xfails |
| `make lint-core` | pass |
| Ruff on all new SEARCH-SH1 files | pass |
| Repository-wide `make lint` | baseline debt: 269 findings across legacy app/tests/scripts |

The dialogue-corpus mismatch and repository-wide lint debt are non-blocking:
neither failing surface is changed by SEARCH-SH1. No Actor Profile path and no
existing modality resolver/catalog/lexicon file differs from base.

## Commit chain

```text
126bcaa test(search): freeze semantic and money dogfood regressions
010a896 fix(search): preserve monetary followup mutations
0cefcd4 fix(search): invalidate pagination after filter refinement
dea338e feat(search): classify contractual query dimensions
aaa5410 test(search): add contractual dimension and modality adversarials
706818c test(search): add semantic hardening mutation campaign
0ce925d fix(search): preserve thematic compatibility controls
b84a088 fix(search): complete interadministrative projection
c4d1984 test(search): make money mutants deterministic
```

No merge, rebase, or push was performed.

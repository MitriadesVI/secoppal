# NATURAL_ALPHA_CAPABILITY_COVERAGE_V1

Base `0e5c7420` (main). Rama `feature/natural-alpha-capability-coverage-v1`.

Pregunta: de los 24 trabajos humanos del catálogo (`app/core/natural_alpha/catalog.py`),
¿cuáles llegan hoy a un runtime real, con dueño y contrato de entrada?

## Método (evidencia, no nombres)

1. Lectura del dispatcher (`orchestrator._dispatch`), del seam Alpha
   (`workflow/handlers/natural_alpha.py`), de los dos planos de binding
   (`capability_binding/registry.py` en el dispatcher; `accreditation/*` en la
   admisión Frame) y de cada runtime candidato.
2. **Sonda offline** del runtime actual: workflow real, proveedor Alpha simulado
   que etiqueta el par, fuente grabadora, red prohibida, Frame OFF (default de
   producción). Scripts en el scratchpad de la sesión (`probe_legacy.py`,
   `probe_pairs.py`, `probe_journeys.py`); no se versionan.
3. **Dogfood real**: 51 turnos observados por Alpha en `data/secoppal.db`
   (copia de solo lectura).

Hecho estructural que decide la mitad de la matriz: radar, paginación, lookup y
reset son *fast paths* que corren **antes** del seam Alpha. Sólo reclaman la
frase literal (`mis radares`, `muéstrame más`, `CO1.NTC.…`). Cualquier paráfrasis
cae al seam, Alpha la entiende, ningún binding la recoge y termina en la búsqueda
tabular **como tema**:

| Frase | Alpha | Qué ejecuta hoy (sonda) |
|---|---|---|
| qué búsquedas tengo guardadas | FOLLOW/LIST | «Procesos de busquedas, tengo, guardadas en Atlántico» |
| enséñame los que siguen | RESULT_ACTION/PAGINATE | «Procesos de ensename, siguen en Atlántico» |
| guárdame esta búsqueda | FOLLOW/SAVE | «Procesos de guardame, busqueda en Atlántico» |
| pásamelo a Excel | RESULT_ACTION/EXPORT_XLSX | «Procesos de pasamelo, excel en Atlántico» |
| qué modalidades usa la Alcaldía de Soledad | ANALYZE/MODALITIES | «Procesos de modalidades de MUNICIPIO DE SOLEDAD» |
| montos de la Alcaldía de Soledad | ANALYZE/VALUES | «Procesos de montos de MUNICIPIO DE SOLEDAD» |
| cuánta plata movió la Alcaldía de Soledad en 2025 | ANALYZE/TOTAL_VALUE | «Procesos de plata, movio …» |

(«en Atlántico» es herencia de follow-up del turno anterior de la sonda.)

## Matriz canónica (24 pares)

Binding: `BOUND` · `SPECIAL_BRIDGE` · `LEGACY_ONLY` (el dueño legacy ya lo
atiende sin Alpha) · `NONE`.

| Par | Runtime | Dueño / ruta | Contrato de entrada | Binding | Tier | Bloqueo / qué falta |
|---|---|---|---|---|---|---|
| SEARCH/PROCESSES | YES | normal_flow (Burr) → `heuristic_only` | parser legacy + autoridades | LEGACY_ONLY | — (no hace falta) | La búsqueda ES el default. Dogfood: 20/51 turnos, bien atendidos. Alpha no aporta dataset (fuente = `source_request_occurrences`). |
| SEARCH/CONTRACTS | YES | ídem | ídem | LEGACY_ONLY | — | ídem |
| SEARCH/PROCESSES_AND_CONTRACTS | YES | `dual_dataset` | ídem | LEGACY_ONLY | — | Sonda: «procesos y contratos de aseo en Soledad» → dual OK. |
| SEARCH/OPEN_OPPORTUNITIES | YES | Search + `opportunity_policy` | Alpha intent bridge | SPECIAL_BRIDGE / PENDING_MAIN | B (en curso) | `feature/opportunity-alpha-intent-bridge-v1-1` (7afc716, apilado sobre 17c1294) **no está en main**. Dogfood: 6 turnos → `heuristic_only`. No se duplica. |
| ANALYZE/OVERVIEW | YES | Entity Profile `overview` | regex `_OVERVIEW` + comprador | LEGACY_ONLY | — | «analiza la alcaldía de X» ya entra por detección de perfil (dogfood y sonda). |
| ANALYZE/VALUES | YES | Entity Profile `values` | `bound_focus` + comprador | NONE | B | Paráfrasis sin «analiza» → búsqueda de «montos». Falta autoridad de superficie del foco (ver §Plano de perfil). |
| ANALYZE/VALUE_STATS | YES | analytics `value_stats` | regex del analyzer | LEGACY_ONLY | C | Sólo la frase regex. «valor promedio …» OK. Qué estadística se pide no está definido. |
| ANALYZE/TOTAL_VALUE | YES (dos) | analytics `aggregate_sum` y perfil `values` | regex | NONE | C | Dos runtimes candidatos; «cuánto ha contratado en total» cayó a búsqueda en la sonda. Hay que decidir el dueño. |
| ANALYZE/SUPPLIERS | YES | Entity Profile `suppliers` | `supplier_contract.decide` | BOUND (flag A2) | — | Hecho, dogfood 13 turnos. No se reconstruye. Refinamiento en follow-up roto (J5). |
| ANALYZE/MODALITIES | YES | Entity Profile `modalities` | `bound_focus` + comprador | NONE | B | «qué modalidades usa la Alcaldía de Soledad» → búsqueda de «modalidades». Mismo bloqueo que VALUES. |
| ANALYZE/RECENT | YES | Entity Profile `recent` | `bound_focus` + comprador + tiempo | NONE | C | Dogfood: «qué ha contratado … estos dos últimos meses» → búsqueda con fecha, que es una respuesta razonable. Perfil-reciente vs búsqueda-por-fecha no está decidido. |
| ANALYZE/TEMPORAL | YES | analytics `temporal` / perfil `temporal` | regex | LEGACY_ONLY | C | Dos runtimes; regex actual lo atiende en la frase canónica. |
| ANALYZE/EXECUTION | YES | analytics `execution_status` | regex | LEGACY_ONLY | C | Qué es «ejecución» (estado reportado vs avance) no está definido. |
| ANALYZE/TOP_ENTITIES | YES | analytics `ranking` | regex | NONE (vetado) | E | El registry lo veta: Alpha confundió proveedores con ranking de entidades. Se conserva. |
| ANALYZE/CONTRACT_COUNT | YES | Contract Activity | `ContractActivityBinding` + unidad | BOUND (flag CA) | — | No se reconstruye. «fuente sana» (jbjy truncado). |
| ANALYZE/CONTRACTS_BY_MODALITY | YES | Contract Activity | ídem | BOUND | — | Follow-up «y por modalidad?» → clarify (dogfood y sonda): falta continuation (J6). |
| ANALYZE/CONTRACTS_BY_STATE | YES | Contract Activity | ídem | BOUND | — | ídem |
| FOLLOW/SAVE | YES | Radar `_do_save` | **nombre obligatorio** + búsqueda previa exportable | NONE | B | El store exige un nombre que la persona no dio. Nombrar es decisión de producto. Hoy: búsqueda basura (J2). |
| FOLLOW/LIST | YES | Radar `_do_list` | sólo `radar_owner` | **BOUND (este trabajo)** | A | Implementado. |
| FOLLOW/RUN | YES | Radar `_do_run` | nombre de radar | **BOUND (este trabajo)** | A | Implementado; nombre acreditado por el store del dueño. «¿hay novedades?» sin nombre → lista, no ejecuta. |
| RESULT_ACTION/PAGINATE | YES | `handle_pagination` + contrato TABULAR | cursor admitido | NONE | B | Las palabras de continuación son el trabajo mismo; ninguna autoridad determinista las separa de un tema nuevo («dame más pero de Cali»). Bindearlo haría que la quote consuma residual. |
| RESULT_ACTION/EXPORT_XLSX | PARTIAL | `export_xlsx.build_results_xlsx` (UI) | filas ya mostradas | NONE | B | Existe sólo como botón Streamlit. No hay entrega de archivo por conversación (ni en WhatsApp/Telegram). |
| LOOKUP/SECOP_REFERENCE | YES | `direct_lookup` | detector de id/URL/ref | LEGACY_ONLY | — (no bindear) | El detector es la autoridad del identificador y pre-reclama el turno. Sin identificador Alpha no puede aportar el input («proceso número 2345 …» → búsqueda). |
| OTHER/OTHER | — | — | — | NONE | E | Por definición. |

## Resumen por tier

- **TIER A (bindable now): 2** — FOLLOW/LIST, FOLLOW/RUN. Implementados.
- **TIER B (contrato pequeño): 6** — FOLLOW/SAVE, RESULT_ACTION/PAGINATE,
  RESULT_ACTION/EXPORT_XLSX, ANALYZE/MODALITIES, ANALYZE/VALUES,
  SEARCH/OPEN_OPPORTUNITIES (bridge ya escrito, pendiente de main).
- **TIER C (semántica de producto): 5** — TOTAL_VALUE, VALUE_STATS, RECENT, TEMPORAL, EXECUTION.
- **TIER D (no implementado): 0** — todos los pares tienen algún runtime.
- **TIER E (no bindear): 2** — TOP_ENTITIES, OTHER.
- **Ya cubiertos, sin tier nuevo: 9** — SEARCH ×3 y OVERVIEW (legacy es el dueño),
  LOOKUP (detector determinista), SUPPLIERS y CA ×3 (bound).

## Plano de perfil (por qué MODALITIES/VALUES no son TIER A)

El runtime y el mecanismo existen (`classify_entity_profile_turn(..., bound_focus=…)`).
Lo que falta es el contrato: `supplier_contract.decide` y
`accreditation/assembly.assemble` son de forma *proveedor* (`supplier_guard`,
`SUPPLIER_MENTION` como superficie propia del trabajo). Para otro foco, «modalidades»
o «cómo contrata» quedan como residual material y el turno se rechaza o, peor,
se ejecuta con el tema tirado. El contrato pequeño que falta es **una autoridad de
superficie por foco** (la generalización de `supplier_guard`), y con ella MODALITIES
y VALUES pasan a ser declaración de spec. Además el plano `accreditation` sólo vive
con Frame ON, que no es el default.

## Journeys

| Journey | Estado | Evidencia (sonda offline + dogfood) |
|---|---|---|
| J1 OPEN_OPPORTUNITIES → PAGINATE | PARTIAL | «muéstrame más» → `pagination_more` OK. «enséñame los que siguen» → búsqueda de «ensename, siguen». Primer paso sin el bridge (7afc716 fuera de main). |
| J2 OPEN_OPPORTUNITIES → SAVE | BROKEN | Pasos: «guárdala» → búsqueda de «guardala». Mismo turno: MULTI_JOB_BLOCKED → mensaje por pasos (honesto, no ejecuta sólo el primero). Sólo funciona la frase literal «guardar esta búsqueda como radar X». |
| J3 PROCESSES → SAVE → LIST → RUN | PARTIAL (antes BROKEN) | SAVE natural roto; LIST y RUN naturales ya llegan al Radar con este binding. |
| J4 CONTRACTS → EXPORT_XLSX | PARTIAL | Botón UI funciona. «descárgalo en Excel» → búsqueda de «descargalo, excel» (`heuristic_plus_llm`). |
| J5 SUPPLIERS → refinamiento | BROKEN | Paso 1 `entity_profile_suppliers`. «y solo los 3 más grandes» → búsqueda de contratos (`heuristic_only`): el ranking se pierde. Medido con Frame OFF (default); Frame ON no se midió. |
| J6 CONTRACT_COUNT → BY_MODALITY | PARTIAL | Aislado funciona (dogfood `analytics_contract_activity`). «y por modalidad?» → `analytics_contract_activity_clarify` (dogfood real y sonda): falta continuation del comprador. |

## Implementación TIER A

`FOLLOW/LIST` y `FOLLOW/RUN` → comandos Radar existentes.

- Flag propio `SECOPPAL_NATURAL_FOLLOW_BINDING_ENABLED` (default OFF). Ni A2 ni CA lo abren.
- `capability_binding/registry.py`: los dos pares, con su propia línea de rollout.
- `workflow/handlers/natural_follow.py`: llama `_do_list` / `_do_run` del handler
  Radar. Sin `chat_id` o sin `radar_store` devuelve `None` → cadena legacy intacta.
- `radar/name_mentions.py`: autoridad del nombre = el store del dueño; nombre
  completo, palabra completa, sin tildes ni mayúsculas; sin alias ni fuzzy.
  Cero o varios nombres → responde con la lista de radares y no ejecuta nada.
- Los comandos literales siguen en su fast path sin llamar a Alpha.
- Un turno bound deja una traza con la observación Alpha (`REACHED_ALPHA`); los
  comandos literales siguen sin traza, como antes.

Garantía acotada, declarada: LIST y RUN no toman dimensiones del texto. Si la
persona añade un filtro («revisa PAE Caribe pero sólo Cali»), el radar corre con
su propio frame y el filtro no se aplica. La respuesta nombra el radar que corrió.

### R2 (remediación del NO_GO de integración)

- La watchlist sintética (`frequency='follows'`, «📌 Procesos seguidos») no es un
  radar de búsqueda: nunca es candidata de FOLLOW/RUN natural. Antes, «revisa mis
  procesos seguidos» la ejecutaba con su frame vacío y cada turno contaba un
  fallo permanente; al tercero quedaba `needs_attention=1, enabled=0` y el
  scheduler dejaba de refrescarla. LIST no cambia.
- Controles nuevos que matan mutantes que antes sobrevivían: cabeza de nombre
  («revisa PAE» con «PAE Caribe» → nada corre) y alcance de owner en el
  call-site (radar homónimo de otro owner). Matriz M1–M8 (+M4b): 9/9 muertos.
- Nota: la identidad cruzada ya estaba protegida en segunda línea (`_do_run`
  re-resuelve por `(owner, name)`), así que sin el scoping un radar ajeno no se
  ejecutaba; el turno terminaba en ambigüedad o `radar_run_not_found`.
- Fuera de alcance, preexistente en main: el comando literal «revisa radar 📌
  procesos seguidos» alcanza la watchlist por `NOCASE` y la pausa igual al
  tercer intento.

## Siguientes 5 capacidades, por palanca de producto

1. **FOLLOW/SAVE natural** — cierra J2/J3 y es la función de retención. Contrato:
   nombre por defecto = la consulta literal del turno buscado (no inventado),
   o pedir el nombre con un pendiente. Decisión de producto pequeña.
2. **SEARCH/OPEN_OPPORTUNITIES a main** — ya escrito (17c1294 → 7afc716). Es el
   trabajo más frecuente después de SEARCH en el dogfood.
3. **Autoridad de superficie por foco de perfil** → MODALITIES y VALUES como specs.
   Convierte «cómo contrata / qué modalidades usa / montos de X» en perfil.
4. **PAGINATE natural** — contrato de «continuación pura» en la admisión (dueña
   del cursor), con Alpha como testigo, no como autoridad.
5. **Continuation de Contract Activity y del ranking de proveedores** (J5/J6):
   heredar el comprador acreditado al follow-up en vez de clarificar.

EXPORT_XLSX conversacional queda detrás: la UI ya lo resuelve con un clic.

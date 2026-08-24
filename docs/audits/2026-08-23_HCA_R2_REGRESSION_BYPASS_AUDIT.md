# HCA-R2 — Regression / bypass audit: TIME P5 + MONEY P8

**Fecha de auditoría:** 2026-08-23

**Base auditada:** `0d194f3cbd6284bbbcd5f15a9f44b46b7e0a2ef3` (`fix/hca-direct-lookup-gate`)

**Rama de auditoría:** `audit/hca-r2-regression-bypass`

**Corpus congelado:** `tests/fixtures/audits/conversation_holistic_v1.yaml`

**SHA-256 verificado:** `262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`

**Alcance:** diagnóstico documental; no se modificó runtime, parser, policy, builder ni oracle HCA.

## 1. Resumen ejecutivo

La contradicción TIME no es una regresión única. Los casos históricamente implementados en su forma literal (`en <mes>`, `el mes pasado`, `este mes`, mes+año y año cerrado) siguen funcionando. Los fallos HCA se dividen en: formas lingüísticas nunca soportadas, expresiones complejas reducidas por un `search()` al último mes+año reconocible y un intervalo abierto (`desde 2025`) cerrado por una segunda etapa genérica del mismo `QueryRouter`. El cierre histórico de `TEMPORAL-MONTH-PARSING-001` fue válido para sus tres formas enumeradas, pero incompleto como garantía temporal general.

La contradicción MONEY sí elude una guarda existente. El extractor entiende correctamente los roles lingüísticos de `más de 500 millones` y `menos de 100 millones`, pero inmediatamente ordena ambos números con `min()`/`max()`. P8 recibe `valor_min=100M` y `valor_max=500M`, por lo que ya no puede observar la contradicción original. P8 funciona con parámetros construidos manualmente, pero está cegada en el workflow de lenguaje natural que debía proteger. La cronología es más severa de lo que sugiere la palabra "bypass": la ordenación `min()`/`max()` fue introducida por `2479d853` el 2026-05-15 y ya estaba presente en el mismo árbol que creó P8 (`9aa5f81c`, 2026-05-21). **P8 no fue cegada por un cambio posterior: nació ciega**, y nunca protegió esta ruta ni un solo día.

No se encontró un commit posterior que haya roto soporte previamente operativo para los diez casos TIME ni para P8. Sí se encontró un patrón común y previo: **`NORMALIZATION_ERASES_USER_INTENT_BEFORE_POLICY`**. En TIME el patrón explica la reducción de estructuras complejas y el cierre de un intervalo abierto; en MONEY explica por completo el bypass de P8. Las formas temporales que nunca generan estructura siguen siendo, separadamente, gaps de cobertura gramatical.

## 2. Método y evidencia runtime

Se inspeccionaron `git log`, `git show`, `git blame`, ADRs, backlog, planes y tests vivos. Las trazas se ejecutaron con `today=2026-08-23` para coincidir con el oracle HCA. Todos los casos mínimos son turnos iniciales; por tanto `followup merge` no participa y no hay evidencia de `FOLLOWUP_ONLY_BUG`.

Validación dirigida, sin ejecutar la suite completa:

- 52 tests históricos/dirigidos pasaron: fechas del router, P5/P8 de `QueryPlan`, workflow temporal, caracterización relativa y rangos monetarios.
- Los 5 casos temporales del corpus de `DialoguePolicy` pasaron.
- Bucket HCA TIME congelado: 8/15 pasan, 7/15 fallan, 0 errores. Fallan `HCA-TIME-003`, `004`, `008`, `010`, `012`, `014` y `015`.
- Caso focal `HCA-MONEY-021`: 0/1; esperaba bloqueo y el workflow ejecutó.
- Control directo de P8 con `{valor_min: 500_000_000, valor_max: 100_000_000}`: bloquea con `invalid_value_range`.

**Re-verificación independiente (2026-08-24), sobre el mismo commit base:** se reejecutó el runner del corpus congelado y se reprodujeron los mismos números — TIME 8 PASS / 7 FAIL / 0 error, fallando exactamente `HCA-TIME-003`, `004`, `008`, `010`, `012`, `014`, `015`; `HCA-MONEY-021` 0/1, con `final_frame` `valor_min=100000000`, `valor_max=500000000` y ejecución en lugar de bloqueo. Se confirmaron además, uno a uno, los cinco anclajes de línea citados en este informe, el orden de ejecución `_extract_dates()` (línea 565) antes del barrido genérico de años (líneas 577-584), las tres regex R4, las cuatro citas documentales y el paso de los seis tests históricos invocados. La traza aislada de T8 reproduce `_extract_dates → 2025-01-01..None` y `parse() → 2025-01-01..2025-12-31`, evidenciando el cierre del intervalo abierto fuera del extractor.

`PASS` en el veredicto final significa que la auditoría obtuvo evidencia suficiente y clasificó todo el alcance; no significa que los comportamientos HCA estén sanos.

## 3. Autoridades históricas

| Commit | Autoridad / decisión declarada | Promesa real y tests introducidos | Estado actual |
|---|---|---|---|
| `66e6242c9947d7cc55cb0ea2558411c8432a66b1` (2026-04-15) | Checkpoint previo al parser de mes | `_extract_dates()` ya soportaba `en 20xx`, `desde 20xx` y `hasta 20xx`; no había soporte de día, relativo ni rango de meses. | Las regex siguen vivas. `desde` produce correctamente solo el inicio dentro de `_extract_dates()`, pero el parse completo lo cierra después. |
| `506cdce91552616cdf60f25c5cf98eb4bce9568a` (2026-04-15) | “month dates” | Añadió mes+año y `tests/test_query_router.py::{test_month_year_extraction,test_month_year_del_mes}`. En la misma versión existía la extracción genérica posterior de cualquier año, hoy en `query_router.py:577-584`. | Los dos tests pasan. La regla de mes+año y la finalización genérica de año no han sufrido regresión; su interacción explica T8 desde entonces. |
| `2479d8534e4b0bee38737edf42e5f9075edeb1c4` (2026-05-15) | Snapshot pre-audit P0; **origen real de la ordenación monetaria** | Introdujo `params["valor_min"] = min(amount_lo, amount_hi)` / `max(...)` en la ruta de rango compuesto. No añadió test adversarial de roles. | Vivo y sin cambios por `git blame` (líneas 1266-1268). Es **anterior en seis días** a la creación de P8. |
| `1f73198aac16f5c5e5e4fdaf32773140f519495c` (2026-05-21) | Sprint 2A B1, `DialoguePolicy` | R4 se especificó como `en <mes>`, `este mes`, `el mes pasado`, `hace N meses`; tests unitarios de señales. No incluía `de <mes>`, `del mes pasado` ni `año pasado`. | Regex sin cambios por `git blame`; las formas exactas siguen pasando. |
| `9aa5f81caa0086617bf0dd16891bf93c8b70521e` (2026-05-21) | Sprint 2A B2, `QueryPlan` | P5 convierte `temporal_month` en rango mensual. P8 bloquea si los parámetros estructurados cumplen `valor_min > valor_max`. Añadió `tests/test_query_plan.py`, incluido `test_invalid_value_range_blocks`. | Ambos guards siguen vivos y sus unit tests pasan. P8 nunca recibió garantía E2E desde texto. |
| `21b1aef1cb15cbca6d45009455967c7439e3b848` (2026-05-21) | Sprint 2A B4.3, activación P5/P6/P7 | Añadió workflow tests para `aseo putumayo en mayo` y `contratos este mes`. | Pasan. Cubren exactamente el léxico de R4, no variantes preposicionales ni granularidad de día/rango. |
| `93df2458bafdc96899f4ad48865d576afd7eff4d` (2026-05-21) | Sprint 2A B4.4, clarificación | El docstring de `tests/test_plan_query_clarification.py:3-7` reconoce que P8 es solo safety net y que el router hace auto-swap antes del pipeline. No añadió test E2E P8. | La limitación documentada continúa exactamente igual. |
| `290b66af8ad95d39a94ffdeb1e7f962f59aa20bd` (2026-05-21) | Corpus `DialoguePolicy` de 60 casos | Cinco ejemplos temporales, todos con formas R4 canónicas. El runner usa parámetros curados. | 5/5 pasan, pero no ejercitan el parser ni la proyección SoQL reales. |
| `d37d48cf96f4f881f8207b27f3c9d543d28a0673` (2026-05-21) | Corrección documental Sprint 2A | `docs/CORPUS_BACKLOG.md:239-244` registra D3: P8 es código muerto E2E por auto-swap; D4: corpus unit-level. | Es la descripción histórica más precisa del alcance real. |
| `ecf94de0bd978861eb1bd6baadc335039ffa665a` (2026-05-28) | Hardening de rangos monetarios | Añadió el rango de polaridad compuesto y 21 tests en `tests/test_value_ranges.py`; todos los ejemplos son rangos coherentes. Endureció herencia de unidad y scrub alrededor de la ordenación ya existente; **no introdujo** el `min()`/`max()`, que ya venía de `2479d853`. | Tests pasan; ninguna prueba adversarial verifica que los roles min/max sobrevivan. Refuerza la ruta exacta HCA, no es una regresión de P8. |
| `4ac717c067885287c18ab1257a5883d440d005a8` (2026-06-05) | ADR-025 / P5b | Añadió hoy, ayer, semana actual/anterior y últimos N días; el ADR declara P5 intacta. | Los tests dirigidos pasan. No prometió día calendario explícito, año pasado ni rango de meses/días. |

Las declaraciones históricas relevantes son:

- `docs/CORPUS_BACKLOG.md:104`: cierra `TEMPORAL-MONTH-PARSING-001` enumerando exactamente `en mayo`, `el mes pasado`, `este mes`.
- `docs/decisions_log.md:241-244,262`: describe P5 con “etc.” y cierra el bug; describe P8 como bloqueo de `min > max`.
- `docs/decisions_log.md:979-1017`: ADR-025 limita expresamente P5b a semana/día relativos y preserva P5.
- `docs/plans/queryplan_invariants_consolidation.md:57-62`: afirma que `valor_min ≤ valor_max` ya está consolidado y que P8 “ya está bien”. Esa conclusión contradice D3 y el workflow real.

Conclusión histórica: TIME es **VALID_BUT_INCOMPLETE**, no `PARSER_REGRESSION`. MONEY es una garantía conocida como no-E2E en 2026-05-21 y luego sobredeclarada como invariante consolidada: `P8_BLINDED_BY_NORMALIZATION` + `P8_INCOMPLETE` + `P8_WRONG_ASSUMPTION`, sin evidencia de `P8_REGRESSION`.

## 4. TIME — traza por capa

Convenciones: `{}` significa que la capa no contiene `fecha_desde` ni `fecha_hasta`; `FD/FH` son esos dos campos. `Frame` muestra `QueryFrame.modifiers`. La proyección final usa `fecha_de_firma` para estas consultas de contratos. Los valores no temporales se omiten para mantener visible la transformación auditada.

| Caso y raw query | `_extract_dates()` | `parsed_params` | `QueryFrame` | `DialoguePolicy` → `QueryPlan` | Filtro SoQL / resultado |
|---|---|---|---|---|---|
| T1 `contratos de aseo en junio de 2026` | FD `2026-06-01`; FH `2026-06-30` | mismos valores | mismos modificadores | señal `temporal_month(6,2026)` redundante; P5 conserva fechas y añade `temporal_conflict` | `>= '2026-06-01' AND <= '2026-06-30'`; correcto |
| T2 `contratos de aseo de junio en cucuta` | `{}` | `{}` | `{}` | sin señal (R4 exige `en`) → sin fechas | sin filtro temporal; ejecuta con scope omitido |
| T3 `contratos de aseo del mes pasado en cucuta` | `{}` | `{}` | `{}` | sin señal (`\bel mes pasado\b` no acepta `del`) → sin fechas | sin filtro temporal; ejecuta con scope omitido |
| T4 `contratos de aseo del año pasado en cucuta` | `{}` | `{}` | `{}` | sin señal temporal; el marcador solo evita promoción a oportunidad | sin filtro temporal; ejecuta con scope omitido |
| T5 `contratos de aseo entre enero y marzo de 2026` | FD `2026-03-01`; FH `2026-03-31` | mismos valores | mismos modificadores | sin señal adicional; plan conserva marzo | solo marzo; pierde enero-febrero |
| T6 `contratos de aseo entre el 1 de febrero y el 15 de marzo de 2026` | FD `2026-03-01`; FH `2026-03-31` | mismos valores | mismos modificadores | sin señal adicional; plan conserva marzo | marzo completo; pierde inicio y día 15 final |
| T7 `contratos de aseo del 15 de junio de 2026` | FD `2026-06-01`; FH `2026-06-30` | mismos valores | mismos modificadores | señal `temporal_month(6,2026)` redundante; P5 conserva parser y marca conflicto | junio completo; pierde el día 15 |
| T8 `contratos de aseo desde 2025` | FD `2025-01-01`; sin FH | FD `2025-01-01`; **FH `2025-12-31`** | intervalo cerrado 2025 | sin señal; P5/P5b no participan | `>= '2025-01-01' AND <= '2025-12-31'`; semánticamente igual a T9 |
| T9 `contratos de aseo en 2025` | FD `2025-01-01`; FH `2025-12-31` | mismos valores | mismos modificadores | sin señal; plan conserva año cerrado | año 2025 cerrado; correcto |
| T10 `contratos de aseo de este mes en cucuta` | `{}` | `{}` | `{}` antes del plan | señal `temporal_month(8,2026)`; P5 crea FD `2026-08-01`, FH `2026-08-31` | agosto 2026; correcto |

Puntos exactos de pérdida:

1. T2-T4 no llegan a producir una representación temporal. No existe una capa posterior que pueda recuperar la intención.
2. T5-T7 llegan a `month_year_re.search()` (`query_router.py:1021-1031`). El `search()` acepta un fragmento interno y lo normaliza inmediatamente a mes completo, sin saber que había días o dos extremos.
3. T8 sale correctamente abierto de `_extract_dates()` (`query_router.py:1055-1057`). Después, el barrido genérico de años de `parse()` (`query_router.py:577-584`) añade por `setdefault` el final del año. No sobrescribe una clave, pero sí sobrescribe la **semántica** de intervalo abierto mediante finalización posterior.
4. `QueryFrame` transporta sin reinterpretar `fecha_desde/fecha_hasta` como modificadores (`query_frame.py:54-64`) y `SoQLBuilder` los proyecta literalmente (`soql_builder.py:515-518`). No son capas causales.
5. Todos los casos son first-turn. Followup merge no aplica; ninguna clasificación es `FOLLOWUP_ONLY_BUG`.

## 5. TIME root cause matrix

| Caso | Historical support claim | Actual failure layer | Classification | Regression commit if known | Fix surface recommended |
|---|---|---|---|---|---|
| T1 `en junio de 2026` | Mes+año desde `506cdce`; `en <mes>` desde P5 | Ninguna; solo flag redundante `temporal_conflict` | **WORKING** | N/A | Test de integración que además controle ausencia/presencia intencional del flag |
| T2 `de junio` | Cierre P5 puede leerse amplio, pero regex/test prometían `en <mes>` | `DialoguePolicy` R4 / ausencia de parser de mes sin año | **PARSER_NEVER_SUPPORTED_FORM; NEW_UNCOVERED_FORM; WRONG_HISTORICAL_CLAIM** si “etc.” se interpreta como cobertura gramatical | Ninguno | Contrato temporal léxico antes de P5, con variantes preposicionales explícitas |
| T3 `del mes pasado` | Backlog cerró literalmente `el mes pasado`, no `del` | Regex exacta de `DialoguePolicy` | **PARSER_NEVER_SUPPORTED_FORM; INCOMPLETE_PRIOR_FIX; NEW_UNCOVERED_FORM** | Ninguno; regex original `1f73198` | Gramática de relativo mensual que preserve la frase consumida |
| T4 `del año pasado` | P5 no lo prometió; ADR-025 tampoco | No existe detector temporal de año relativo | **PARSER_NEVER_SUPPORTED_FORM; NEW_UNCOVERED_FORM** | Ninguno | Tipo temporal de año relativo, separado de heurística de oportunidad |
| T5 rango de meses | Solo existía mes+año singular y rango año-año | `QueryRouter._extract_dates`, partial match al segundo mes | **PARSER_NEVER_SUPPORTED_FORM; GRANULARITY_COLLAPSE; NEW_UNCOVERED_FORM** | Ninguno; comportamiento deriva de `506cdce` | Parser de intervalo con dos extremos antes de fallback singular |
| T6 rango de días | No hubo promesa/test histórico | `QueryRouter._extract_dates`, partial match `marzo de 2026` | **PARSER_NEVER_SUPPORTED_FORM; GRANULARITY_COLLAPSE; NEW_UNCOVERED_FORM** | Ninguno | Parser de fechas/dos extremos con granularidad y procedencia |
| T7 día exacto | No hubo promesa/test histórico de día explícito | `QueryRouter._extract_dates`, partial match de mes+año | **PARSER_NEVER_SUPPORTED_FORM; GRANULARITY_COLLAPSE; NEW_UNCOVERED_FORM** | Ninguno | Fecha exacta tipada; impedir que fallback mensual consuma una expresión más específica |
| T8 `desde 2025` | `_extract_dates` prometía intervalo abierto desde `66e6242` | Segunda etapa genérica de año dentro de `QueryRouter.parse` | **LATER_LAYER_OVERWRITE; GRANULARITY_COLLAPSE; WRONG_HISTORICAL_CLAIM** | No es regresión; interacción presente al menos desde `506cdce` | Contrato de intervalo abierto que prohíba completar el extremo ausente |
| T9 `en 2025` | Soporte explícito desde `66e6242` | Ninguna | **WORKING** | N/A | Mantener oracle E2E de año cerrado |
| T10 `este mes` | P5 / `TEMPORAL-MONTH-PARSING-001` | Ninguna | **WORKING** | N/A | Fortalecer E2E para ambos extremos y SoQL |

`SILENT_SCOPE_DROP` describe el resultado observable de T2-T4 (el sistema ejecuta sin el alcance temporal y sin aclarar), pero no se usa como causa primaria en la matriz porque esas formas no generan siquiera una señal parcial. En T5-T8 la señal sí se conserva, pero con semántica reducida; la categoría más precisa es `GRANULARITY_COLLAPSE`.

## 6. Test archaeology — TIME

| Capability | TEST_EXISTS | TEST_PATH | ASSERTION_STRENGTH | CURRENTLY_PASSES | EXERCISES_REAL_WORKFLOW |
|---|---|---|---|---|---|
| Mes explícito con año | YES | `tests/test_query_router.py::{test_month_year_extraction,test_month_year_del_mes}` | Fuerte para ambos extremos, débil para interacción con policy/SoQL | YES | NO, parser-only. HCA `TIME-002` sí confirma workflow |
| Mes sin año | YES, solo `en` | `tests/test_query_plan.py::test_temporal_month_uses_injected_today`; `tests/test_plan_query_scrub_and_temporal.py::test_temporal_month_en_mayo_e2e` | Fuerte en FD/FH para la forma exacta | YES | YES para `en mayo`; no para `de junio` |
| Mes pasado | YES, solo `el mes pasado` | `tests/test_query_plan.py::test_temporal_month_pasado`; corpus `TEMPORAL-SYN-003` | Débil: solo FD/prefijo; params curados | YES | NO para parser/workflow; no prueba `del mes pasado` |
| Año pasado | Test adyacente, no de fecha | `tests/test_plan_query_implicit_opportunity.py::test_explicit_historical_not_promoted` | Muy débil: solo afirma que no se promueve oportunidad; permite perder toda fecha | YES | YES, pero no protege la capability temporal |
| Este mes | YES | `tests/test_plan_query_scrub_and_temporal.py::test_temporal_month_este_mes_e2e`; corpus `TEMPORAL-SYN-002` | Media: E2E afirma solo FD; corpus usa params curados | YES | YES para FD; HCA `TIME-009` añade oracle congelado |
| Día exacto | NO antes de HCA | — | Ninguna | N/A | NO |
| Rango multi-mes | NO antes de HCA | — | Ninguna | N/A | NO |
| Rango con días explícitos | NO antes de HCA | — | Ninguna | N/A | NO |
| `desde <año>` abierto | NO desde lenguaje natural | `tests/test_response_policy.py::TestDateScope` usa params ya construidos; no protege parse | Nula para preservación del extremo abierto | YES | NO |
| Año cerrado | YES | `tests/test_query_router.py::test_year_extracted_as_date_range` | Fuerte en FD/FH, parser-only | YES | NO en ese test; HCA `TIME-001` cubre workflow |
| Semana/día relativos P5b | YES | `tests/test_temporal_relative_characterization.py` | Fuerte dentro de su catálogo | YES | Parcial; no incluye día calendario ni año relativo |

### Por qué 8.000+ tests no detectaron HCA-TIME

La cantidad total no equivale a cobertura del contrato lingüístico. La protección estaba particionada por capas: parser-only para mes+año/año, `QueryPlan` con params preconstruidos para P5, y un corpus de diálogo explícitamente unit-level. Las pruebas de workflow usaban solo las formas canónicas que copiaban las regex. Varias aserciones verificaban únicamente `fecha_desde`, de modo que un extremo incorrecto podía sobrevivir. El único test E2E con `año pasado` comprobaba otra política (no promover oportunidad), por lo que pasa mientras la fecha desaparece. No existía un oracle adversarial que comparara la estructura expresada —día, intervalo abierto o dos extremos— con los parámetros y el SoQL final.

Esto ya estaba parcialmente admitido por D3/D4 en `docs/CORPUS_BACKLOG.md`; el error posterior fue tratar el paso de tests de capa como garantía de workflow y de cobertura gramatical.

## 7. MONEY — traza real de HCA-MONEY-021

Raw exacto: `contratos de aseo en cali por mas de 500 millones y menos de 100 millones`.

| Stage | Input | Output | Intent preserved | Notes |
|---|---|---|---|---|
| Raw query | `más de 500M` + `menos de 100M` | Dos comparadores con roles opuestos | YES | La contradicción es explícita: mínimo 500M y máximo 100M |
| Compound money match | grupos `lo_text=500 millones`, `hi_text=100 millones` | `amount_lo=500_000_000`, `amount_hi=100_000_000` | YES | `_compound_range_re`, `query_router.py:1235-1265` reconoce correctamente polaridad y unidades |
| Normalización dentro del extractor | `500_000_000`, `100_000_000` | `valor_min=min(...)=100_000_000`; `valor_max=max(...)=500_000_000` | **NO** | Pérdida exacta en `query_router.py:1266-1268`, antes de `parsed_params` y P8 |
| `parsed_params` | rango ya ordenado | `valor_min=100M`, `valor_max=500M` | NO | Ya no existe provenance, comparador original ni orden declarado |
| `QueryFrame.modifiers` | params ordenados | `valor_min=100M`, `valor_max=500M` | NO | Transporte fiel; no causa el swap (`query_frame.py:54-64`) |
| `DialoguePolicy` | raw + params ordenados | sin señal de contradicción monetaria | NO | No reconstruye los roles monetarios; P8 no depende de una señal |
| `QueryPlan` / P8 | `vmin=100M`, `vmax=500M` | `100M > 500M` es falso; `should_execute=True`, sin `invalid_range` | NO | P8 participa formalmente, pero está **blinded by normalization** |
| Scope / execution policy | tema `aseo`, ciudad `Cali`, rango válido aparente | permite ejecución | NO | El scope es sustancial; ninguna policy restante tiene motivo para bloquear |
| SoQL planning | rango ordenado | `valor_del_contrato >= 100000000 AND valor_del_contrato <= 500000000` | NO | `soql_builder.py:447-450` proyecta fielmente el estado ya corrompido |

El swap ocurre **ANTES de P8**, dentro de money extraction/normalization. No ocurre en P8 ni después de P8. P8 sí se invoca en el plan, pero no participa epistemológicamente: solo ve una representación que borró la contradicción.

### Autoridad P8 — respuestas obligatorias

1. **Contradicción prometida:** `valor_min > valor_max`, que debía bloquear ejecución y pedir aclaración.
2. **Objeto/capa:** `QueryPlan`, sobre `working_params` (`query_plan.py:248-264`).
3. **¿Recibe valores originales?** No en la ruta de lenguaje natural; recibe valores ya ordenados.
4. **¿El parser ya los ordenó?** Sí, con `min(amount_lo, amount_hi)` y `max(...)`.
5. **¿El test histórico usa valores estructurados?** Sí, y de forma activamente engañosa. `test_invalid_value_range_blocks` (`tests/test_query_plan.py:159-167`) inyecta `{"valor_min": 1_000_000_000, "valor_max": 5_000_000}` ya contradictorios y **además transporta el raw `"mantenimiento entre 1000M y 5M"` como segundo argumento**. Ese string nunca se parsea: es inerte. Su presencia hace que el test *parezca* cubrir lenguaje natural, lo que explica en buena parte por qué el gap sobrevivió a las revisiones posteriores.
6. **¿Protege workflow real?** Solo protege llamadas que construyan parámetros contradictorios directamente o una ruta futura que conserve roles. No protege hoy el rango contradictorio expresado en lenguaje natural.

**Cronología decisiva (verificada por `git blame` y por lectura del árbol histórico):** el `min()`/`max()` fue introducido por `2479d853` el 2026-05-15. P8 fue creada por `9aa5f81c` el 2026-05-21. Inspeccionando `git show 9aa5f81c:app/core/query_router.py` se comprueba que la ordenación **ya estaba presente en el mismo árbol que introdujo P8** (líneas 741-742 de esa versión). Por tanto P8 no fue cegada después: **nació ciega**. Nunca protegió, ni un solo día, el rango contradictorio expresado en lenguaje natural. Esto cierra `P8_REGRESSION` de forma definitiva y convierte la ceguera en congénita, no adquirida.

Clasificación final P8:

- `P8_REGRESSION`: **NO**. El guard no fue roto y su unit test sigue pasando.
- `P8_BYPASSED`: **YES**, en sentido funcional E2E.
- `P8_BLINDED_BY_NORMALIZATION`: **YES**, causa primaria.
- `P8_INCOMPLETE`: **YES**, porque valida solo la forma estructurada posterior.
- `P8_WRONG_ASSUMPTION`: **YES**, para la afirmación posterior de que la invariante ya estaba consolidada; D3 ya había documentado lo contrario.

## 8. MONEY root cause matrix resumida

| Stage | Input | Output | Intent preserved | Notes |
|---|---|---|---|---|
| Linguistic extraction | lower-bound 500M; upper-bound 100M | roles y montos correctos | YES | Evidencia contradictoria todavía disponible |
| Canonical ordering | `(500M,100M)` | `(min=100M,max=500M)` | **NO** | Primera y decisiva pérdida |
| Frame / plan input | rango aparente 100M..500M | mismo rango | NO | No hay metadata para auditar intención |
| P8 | 100M ≤ 500M | permite ejecutar | NO | Guard correcto sobre input insuficiente |
| SoQL | min=100M; max=500M | rango ejecutable 100M..500M | NO | Proyección fiel, no causa raíz |

## 9. Principio epistémico común

**Patrón encontrado: `NORMALIZATION_ERASES_USER_INTENT_BEFORE_POLICY`.**

La arquitectura reduce temprano una expresión lingüística rica a dos escalares canónicos y descarta la evidencia que permitiría decidir si esa normalización es lícita:

```text
MONEY:  lower-bound(500M) + upper-bound(100M)
        -> sort(100M, 500M)
        -> P8 observa un rango válido

TIME:   exact-day / two-ended-range
        -> search(month+year) dentro de la expresión
        -> mes completo sin granularidad ni extremos originales

TIME:   open-since(2025)
        -> fecha_desde=2025-01-01
        -> bare-year completion añade fecha_hasta
        -> intervalo cerrado indistinguible de "en 2025"
```

El patrón común no implica que todos los TIME sean el mismo bug: T2-T4 fallan antes, por falta de representación. Pero T5-T8 y MONEY comparten la pérdida irreversible de rol, granularidad o apertura antes de que una policy pueda validar contradicción/fidelidad.

## 10. Superficies de remediación, sin código

### TIME_REMEDIATION_SURFACE

- `app/core/query_router.py`: establecer precedencia de expresiones temporales específicas sobre fallbacks; distinguir fecha exacta, mes cerrado, año cerrado, intervalo abierto y rango de dos extremos.
- `app/core/dialogue_policy.py`: ampliar el contrato lingüístico de relativos/mes sin año sin duplicar una segunda semántica incompatible.
- `app/core/query_frame.py`: transportar tipo de intervalo, granularidad, extremos presentes y provenance; los dos strings FD/FH no bastan para distinguir “desde” de “en”.
- `app/core/query_plan.py` P5/P5b: validar conflictos semánticos sobre la representación tipada y no convertir una detección parcial en certeza.
- `app/core/soql_builder.py`: proyectar únicamente intervalos aceptados; no intentar reparar lenguaje en esta capa.
- Tests: matriz gramatical completa raw → params → frame → plan → SoQL, con oracles negativos que prohíban inventar extremos o ampliar granularidad. Incluir variantes `de/del/en`, día exacto, rango mes-mes, rango día-día, año relativo, abierto `desde/hasta` y ambos datasets.

### MONEY_REMEDIATION_SURFACE

- `app/core/query_router.py`: conservar comparador/rol declarado, orden textual y montos antes de cualquier canonicalización.
- `app/core/query_frame.py`: transportar intervalo monetario tipado o metadata de provenance/contradicción; no solo `valor_min/valor_max` ya ordenados.
- `app/core/query_plan.py` P8: validar los roles semánticos originales antes de permitir ordenación; definir de forma explícita cuándo un rango reversible sí puede canonicalizarse.
- `app/core/soql_builder.py`: recibir únicamente un intervalo validado y no inferir ni corregir roles.
- Tests: mantener el unit test estructurado, añadir E2E desde lenguaje natural para contradicciones en ambos órdenes y para rangos sanos equivalentes; afirmar bloqueo, ausencia de consulta SECOP y ausencia de SoQL normalizado silenciosamente.

No se propone aún un refactor global. El siguiente bloque debe definir primero el contrato mínimo de preservación de intención compartido por ambos dominios.

## 11. Veredicto obligatorio

```text
HCA_R2_AUDIT:
PASS

TIME_PRIOR_FIX_STATUS:
VALID_BUT_INCOMPLETE

MONEY_P8_STATUS:
BLINDED_BY_NORMALIZATION

COMMON_ARCHITECTURAL_PATTERN:
NORMALIZATION_ERASES_USER_INTENT_BEFORE_POLICY

TIME_CASES_CLASSIFIED:
10/10

MONEY_CONTRADICTION_TRACED:
YES

HISTORICAL_TEST_GAPS_IDENTIFIED:
YES
```

## 12. NEXT DECISION

```text
READY_FOR_HCA_NORMALIZATION_CONTRACT_FIX
```

Razón: continuar con negación dejaría intacta una falla transversal de contrato; y un fix temporal aislado resolvería solo parte de TIME mientras P8 seguiría ciega. El siguiente trabajo debe ser acotado a preservar rol, granularidad y apertura hasta la capa de validación, antes de reparar las gramáticas concretas.

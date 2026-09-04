# Municipal M1A.2 — identidad completa protegida; gate global PARTIAL por REVIEW

**MUNICIPAL_M1A_2: PARTIAL. M1A_RETRY_2_READY: NO. PRODUCTION_PROMOTED: 0. PUSH: NO.**

La corrección elimina los wrong owners observados del barrido municipal y pasa
los gates de las 503 propuestas cargadas juntas. No se declara readiness: cinco
compradores existentes carecen de afiliación municipal acreditada por M0 y
producen **20 consultas REVIEW por catálogo**. REVIEW no cuenta como PASS ni
como wrong owner sin evidencia. Resolver esa deuda requiere adjudicación de
fuentes; no se eliminaron aliases legítimos para ocultarla.

Rama `main`; HEAD inicial `32c27cbcf1235fb045bb7e8c76054197a357f232`.
`23bdd40` congela discovery, tests y resultados PRE antes del fix.
`940a495` implementa el guard y sus pruebas. Un commit documental posterior
conserva el cierre. Los archivos locales ajenos a esta tarea se conservan.

## Discovery congelado

La autoridad sigue siendo `MunicipalTerritoryRegistry`, sobre
`app/data/municipal_territories_v1.json`. No hay catálogo municipal nuevo.

| Universo derivado | Conteo |
|---|---:|
| Municipios DIVIPOLA | 1.103 |
| Nombres distintos con más de una palabra | 279 |
| Subconjunto con conector interno `de/del` | 93 |
| Pares de nombres con prefijo completo igual a otro municipio | 46 |
| Grupos de nombres homónimos | 64 |

Los conteos no se ajustaron a las aproximaciones de la tarea. El artefacto
`municipal_identity_matrix.json` contiene definiciones, SHA-256 de la autoridad
y todos los nombres/códigos. Se congelaron 1.916 consultas predictivas por
catálogo sobre 406 municipios compuestos u homónimos; los tests iniciales
registran **12 fallos esperados y 8 pases**, sin introducir xfails.

## Política y separación de autoridades

`app/core/compound_identity_guard.py` decide NONE, identidad municipal única o
ambigua. La primera identidad municipal completa de cada escalera fija el
mínimo span que puede consumirse. Una captura con tokens trailing puede
encogerse hasta ese span, pero no atravesarlo.

El guard exige ownership exacto único y familia institucional municipal; una
jurisdicción departamental contradictoria invalida incluso un alias exacto.
Esto cierra también la colisión entre un municipio y una alcaldía local
homónima. DIVIPOLA aporta restricciones negativas: no crea BuyerUnits,
miembros de grupos, aliases ni bindings, y no usa NIT. El guard existente de
`proper_names` sigue protegiendo nombres institucionales más largos.

Un homónimo municipal desnudo siempre requiere departamento, aunque sólo
exista un comprador conocido. Una forma cualificada puede resolver por su
alias completo acreditado. Si no existe esa evidencia, se conserva la identidad
y se pide aclaración. La excepción geográfica dentro de la grafía oficial
`BOGOTÁ, D.C.` se reconoce como nombre completo antes de interpretar la coma
como cualificador.

Cuando no se acredita el comprador, se consume toda la identidad para evitar
el rescaneo de sus fragmentos. La resolución conserva códigos y span protegidos,
sin buyer resuelto. El plan impide ejecutar, incluso con las reglas opcionales
desactivadas, y el builder rechaza también llamadas directas con objeto,
fechas o `allow_global=True`.

Se extrajeron los gates iniciales del plan a `query_plan_preflight.py` antes de
modificar su comportamiento, conforme a NEXT_TOUCH_RULE. `query_plan.py` queda
en **276 LoC**, bajo el límite de 300. ADR-069, ADR-072 y ADR-074 cubren la
política; no se crea un ADR.

`entity_types.py` permanece byte-idéntico. La escalera sintáctica R2A.1 sigue
intacta. Antioquia con `de 2025` resuelve como antes. Medellín con
`de obras de infraestructura` **ya fallaba en el HEAD inicial**: sólo había
fuzzy para las variantes originales. Tras agotar esas variantes, el guard puede
consultar la grafía genérica `Distrito especial de <captura completa>` únicamente
como alias exacto existente, con los mismos controles de ownership, familia y
departamento. Así alcanza el alias ya acreditado de Medellín sin relajar fuzzy.
La prueba de Barranquilla con `pavimentacion` conserva comprador y objeto.

## Matriz exhaustiva y oráculo

Se ejecutaron las cuatro formas pedidas sobre los 1.103 municipios y dos formas
cualificadas adicionales sobre los 146 municipios que integran grupos homónimos:
**4.412 + 292 = 4.704 consultas por catálogo; 9.408 en total**.

| Resultado | Baseline | Candidato completo | Total |
|---|---:|---:|---:|
| CORRECT_BUYER | 244 | 2.112 | 2.356 |
| CLARIFICATION | 4.440 | 2.572 | 7.012 |
| FAIL_CLOSED sin aclaración | 0 | 0 | 0 |
| REVIEW | 20 | 20 | 40 |
| WRONG_BUYER | 0 | 0 | 0 |
| WRONG_DIVIPOLA | 0 | 0 | 0 |

El oráculo une propuestas M0 con códigos DIVIPOLA, usa `recognized_units` para
los compradores existentes y permite sólo evidencia de fuentes físicas exactas
con afiliación acreditada y única en los estratos M0. Las resoluciones de grupo
se verifican miembro a miembro: el grupo declarado de Bogotá tiene 38 miembros,
todos acreditados para DIVIPOLA 11001. No se adjudica un grupo por su etiqueta.
Ningún CORRECT_BUYER tuvo un builder bloqueado.

Los cinco REVIEW, con cuatro consultas cada uno, son:

| Comprador existente | Evidencia insuficiente |
|---|---|
| ALCALDIA DE TURBACO BOLIVAR | Sin nombre físico exacto en actividad/directorio M0; la fuente acreditada contiene `MUNICIPAL` |
| ALCALDIA MUNICIPAL DE OCAÑA | M0 marca UNRESOLVED_AFFILIATION |
| ALCALDIA MUNICIPIO DE CALARCA | M0 marca UNRESOLVED_AFFILIATION |
| ALCALDIA DISTRITAL BARRANCABERMEJA | M0 marca UNRESOLVED_AFFILIATION |
| MUNICIPIO DE YUMBO VALLE | M0 marca UNRESOLVED_AFFILIATION |

`review_adjudication.json` conserva las filas M0 y del directorio pertinentes.
Ni el parecido del nombre, ni una ubicación del directorio, ni el NIT sustituyen
la conciliación ausente. El gate retorna PARTIAL y código de salida 1 por estos
REVIEW; **no** es una declaración de 1.103 municipios completamente aprobados.

**Sotavento:** PRE candidato seleccionaba `ALCALDIA SAN ANDRES`, 68669,
Santander. POST conserva 23670, San Andrés de Sotavento, Córdoba, no selecciona
BuyerUnit y bloquea ejecución con aclaración. El artefacto
`sotavento_canary.json` conserva ambas fases. Susacón y Susa mantienen sus
identidades separadas en el barrido completo.

## Gates del candidato y fallos del Retry #1

| Gate | Resultado |
|---|---|
| Propuestas cargadas juntas | 503; 498 nuevas + 5 ampliaciones, sólo temporales |
| Aliases por parser → plan → SoQL | 1.888/1.888; 0 fallos y 0 wrong owners nuevos |
| Cross-interaction anterior | 3.071; PASS, 0 wrong owners nuevos, 0 owners correctos perdidos |
| Nuevas claves multi-owner | 0 |
| Nuevos cruces de scan | 0; 42.790 superficies |
| NO_NIT_MERGE / unidades y bindings preservados | PASS |
| Transformación repetida | Idempotente y equivalente al snapshot completo del Retry #1 |
| Proyección de fuentes | 1.006 comprobaciones, 503 propuestas, 0 fallos |
| Geografía independiente | 2.206 consultas, 0 fallos |
| Archivos de producción, M0 y evidencia histórica | SHA-256 conservados |

Baranoa, Nuevo Colón y Villa de Leyva tienen afiliación/proyección acreditadas
en el candidato. En baseline requieren aclaración; después resuelven al
comprador correcto sin objeto espurio, control de orden ni scope geográfico.
Son **oráculos de ausencia prepromoción**, no tres bugs nuevos. Los tests de
ausencia se ligan al baseline congelado; las pruebas candidatas comprueban las
resoluciones y fuentes nuevas antes de cambiar dichas expectativas.

Belmira demuestra un **assert de schema obsoleto**: `scan_disabled` puede contener
el canonical propio además de aliases. Su lookup exacto sigue disponible y el
scan de una personería que contiene ese canonical devuelve None. Se corrige la
aserción preservando este contrato. `adjudication.json` contiene la evidencia
separada de cada caso. No se modificó el verificador histórico del Retry #1.

## Validación y rendimiento

- Focales: **493 passed, 1 xfailed histórico**.
- Compatibilidad con el catálogo candidato en copia aislada: **299 passed, 1 xfailed histórico**.
- Corpus canónico: **89/89**, sin cambios de fixtures.
- Ruff, `make lint-core`, module-size y `git diff --check`: PASS.
- Semgrep: configuración presente, ejecutable/paquete no disponibles; no ejecutado.
- Suite completa final: **11.013 passed. 2 skipped. 14 xfailed históricos**; `validation/full_suite_final.log`.
- No nuevos xfails. La primera suite terminó con 11.011 pases y dos fallos de
  expectativas: confianza `none` de un comprador no acreditado y pin del builder.
  Las 46 pruebas focales posteriores verifican el bloqueo de identidad y la
  paridad de consultas ordinarias antes de actualizar esas expectativas.

Benchmark emparejado, mismos catálogos y ocho consultas, calentamiento y nueve
rondas, sin tests ni gates concurrentes. No se modifica `scan_entity`.

| Pipeline (ms) | PRE p50 / p95 | POST p50 / p95 |
|---|---:|---:|
| Baseline | 54,579 / 131,377 | 23,924 / 124,514 |
| Candidato | 0,725 / 155,082 | 0,750 / 145,009 |

**PERFORMANCE: ACCEPTABLE.** Delta candidato p50 +0,024 ms, p95 −10,073 ms;
no regresión material observada. Son canarios offline, no un SLA de tráfico
real. La precompilación de regex sigue siendo trabajo separado.

## Reproducción

```bash
.venv/bin/python scripts/audits/gate_divipola_buyer_matrix.py --discovery
.venv/bin/python scripts/audits/gate_divipola_buyer_matrix.py
.venv/bin/python scripts/audits/verify_municipal_m1a_retry2.py
.venv/bin/python scripts/audits/adjudicate_municipal_m1a2.py
.venv/bin/python scripts/audits/check_municipal_m1a2_candidate_tests.py
.venv/bin/python scripts/audits/benchmark_municipal_m1a2.py post
.venv/bin/python scripts/run_query_corpus.py --mode all
.venv/bin/python -m pytest tests/ -q
.venv/bin/ruff check app/ tests/ scripts/
make lint-core
```

`--pre` sólo reproduce evidencia PRE si se ejecuta en el commit predictivo
`23bdd40`; no debe sobrescribir esa evidencia con runtime corregido. Todos los
artefactos nuevos están en `artifacts/audits/municipal_m1a_2/`. Los verificadores
históricos, proposals.json y los catálogos de producción permanecen intactos.
La promoción y el push no se ejecutan.

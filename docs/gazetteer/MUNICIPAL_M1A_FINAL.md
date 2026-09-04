# Municipal M1A FINAL — promoción de las 503 unidades ejecutivas municipales

**MUNICIPAL_M1A_FINAL: PASS. PRODUCTION_PROMOTED: 503. PUSH: NO.**
Rama `main`; `HEAD_BEFORE` `67281fcb240f2844df0af78b1b83443800bbdff9`, árbol
seguido limpio. Se escriben únicamente `app/data/aliases_db.json` y
`app/data/buyer_source_names.json`.

## Autoridad de igualdad: el baseline actual, no el snapshot del Retry #1

El Retry #1 declaraba equivalencia contra `municipal_m1a_retry/promoted_snapshot`.
Ese snapshot se regeneró desde un baseline que **todavía contenía el canonical
duplicado de Turbaco**, eliminado después por M1A.2A. Reutilizarlo como autoridad
habría comparado la promoción contra un árbol que ya no existe. Aquí la igualdad
exigida es otra:

```text
transform(baseline_actual, 503 propuestas) == candidato auditado en dry run
transform(candidato, 503 propuestas)       == candidato        (idempotencia)
```

Ningún conteo se hereda: cross-interaction, gazetteer y aliases se derivan de los
archivos bajo prueba. `verify_municipal_m1a_final.py` y
`municipal_m1a_final_gates.py` son nuevos; `verify_municipal_m1a_retry.py`,
`verify_municipal_m1a_retry2.py` y la evidencia histórica no se modifican.

### El promotor no se pudo ejecutar por su `main`, y por qué

`promote_municipal_m1a.py --apply` **no es ejecutable sobre este HEAD**. Su `main`
lee el baseline congelado de M1A (599 registros, con el duplicado de Turbaco) y
llama `validate()` con él; sobre el árbol corregido eso levanta
`KeyError: 'ALCALDIA DE TURBACO BOLIVAR'` antes de escribir nada. Además su `main`
sobrescribe artefactos históricos de M1A y del retry. Se reutiliza por tanto su
función pura `transform`, **sin modificarla**, invocada desde el verificador final
con el baseline correcto. Los bytes escritos son los que produce `encode()` del
promotor. `promote_municipal_m1a.py` queda byte-idéntico.

## Delta real reproducido desde `67281fc`

No se asumen las métricas del Retry #1: Turbaco ya fue consolidado.

| Métrica | Retry #1 (histórico) | **Esta promoción (reproducido)** |
|---|---:|---:|
| Propuestas de entrada | 503 | **503** |
| BuyerUnits nuevas | 498 | **498** |
| Unidades existentes ampliadas | 5 | **5** |
| Rechazos por evidencia | 0 | **0** |
| Aliases añadidos (neto) | 1.883 | **1.883** |
| Bindings de fuente añadidos | 503 | **503** |
| Gazetteer | 599 → 1.097 | **598 → 1.096** |
| Aliases totales | 10.802 → 12.685 | **10.798 → 12.681** |
| Bindings totales | 85 → 588 | **86 → 589** |

Los totales bajan en una unidad y suben en un binding respecto al histórico por la
corrección de M1A.2A, no por la promoción.

**El delta neto de 1.883 se verificó de nuevo, y su causa se precisa.** El retry lo
atribuía a cinco duplicados preexistentes eliminados por `dict.fromkeys`. Esto se
reprodujo y se localizó: el duplicado **no** es una intersección entre lo propuesto
y lo existente —esa intersección es **vacía en las cinco ampliaciones**— sino una
repetición **interna** de la lista de aliases ya almacenada, una en cada unidad
ampliada (`municipio de manizales`, `municipio de tocancipa`, `municipio de
sabaneta`, `municipio de los patios`, `municipio de bucaramanga`). Medido sobre
pares distintos `(owner, alias)`: **10.516 → 12.404, cero perdidos, +1.888
ganados, cero owners perdidos**. Los 1.888 strings propuestos entran completos; el
conteo bruto sube 1.883 porque se colapsan cinco repeticiones que ya existían.

## Gates del dry run y de producción

La batería se corre dos veces: contra el candidato (antes de escribir) y contra la
**producción real** ya expandida, en un intérprete nuevo, sin proyector parcheado
(`temporary_candidate_or_projector_patch: false`). Ambas dan el mismo resultado.

| Gate | Resultado |
|---|---|
| Aliases M0 por parser → plan → SoQL exacto | **1.888/1.888**; 0 fallos; 0 wrong owners |
| Matriz DIVIPOLA completa (2 catálogos × 4.704) | **9.408**; REVIEW **0**, WRONG_BUYER **0**, WRONG_DIVIPOLA **0**, FAIL_CLOSED **0** |
| Cross-interaction | **3.068** derivadas; 0 nuevos wrong owners; 0 owners correctos perdidos |
| Nuevas claves multi-owner | **0** |
| Nuevos cruces de scan | **0** |
| `NO_NIT_MERGE` / unidades y bindings preservados | **PASS** |
| Proyección de fuentes ADR-073 | 503 bindings × 2 datasets = **1.006**; 0 fallos |
| Geografía DIVIPOLA con y sin departamento | **2.206** consultas; **0** fallos |
| Identidad compuesta derivada | 279 compuestos, **93** de/del, **46** colisiones de prefijo, **64** grupos homónimos; **411** códigos relevantes, **0** sin cubrir por la matriz |
| Idempotencia de la transformación | **PASS** |

**Cross-interaction: 3.068, no 3.071.** La diferencia se localizó exactamente: las
tres consultas que sólo aportaba el canonical duplicado eliminado
(`contratos de ALCALDIA DE TURBACO BOLIVAR` y sus dos aliases cercanos). El barrido
se deriva del catálogo y de los aliases, nunca de un conteo fijado.

La matriz DIVIPOLA usa el oráculo de M1A.2 sin modificar, más la evidencia
afirmativa ya adjudicada y congelada por M1A.2A para Ocaña, Calarcá,
Barrancabermeja y Yumbo. Esa evidencia se **lee** de
`municipal_m1a_2a/adjudication.json`; `adjudicate()` no se reejecuta porque
reescribiría la evidencia de M1A.2A. La inyección sólo puede acreditar un canonical
que ya existe y que aún no tenía código; nunca crea owners. Turbaco queda cubierto
por la BuyerUnit que M0 ya reconocía.

### Canarios de identidad

`San Andrés de Sotavento` era el bloqueo del Retry #1. En producción promovida:

| Consulta | Resultado en producción |
|---|---|
| `contratos del municipio de San Andrés de Sotavento` | Sin BuyerUnit, sin objeto, no ejecuta, pide aclaración |
| `municipio de San Andrés de Sotavento` | Idéntico |
| `alcaldía de San Andrés, departamento Santander` | Sin BuyerUnit; no se fabrica el ejecutivo de 68669 |

`sotavento` nunca se convierte en objeto contractual y `ALCALDIA SAN ANDRES` nunca
reclama 23670. Susacón resuelve a `ALCALDIA MUNICIPAL SUSACON BOYACA` y Susa a
`ALCALDIA DE SUSA`: identidades separadas. El canario de Susacón se formula como
invariante —«si hay owner, es de Susacón y no es el de Susa»— y no como oráculo de
presencia, porque exigir presencia lo habría vuelto una aserción que sólo puede
cumplirse después de promover.

### Baranoa

| Consulta | Resultado |
|---|---|
| `contratos de la alcaldia de baranoa` | `ALCALDIA MUNICIPAL DE BARANOA` |
| `procesos del municipio de baranoa` | misma BuyerUnit |
| `contratos en baranoa` | Territorio Baranoa, Atlántico (08078), **sin** BuyerUnit |

Ninguna de las dos formas de comprador produce objeto, orden ni scope geográfico.
La paridad sobre los snapshots M0 congelados es **380 procesos únicos** y **374
contratos únicos**, cubiertos al 100%. No hubo consulta live; si el dato vivo
difiere, es un valor nuevo, no una regresión.

## Segunda aplicación

`PRODUCTION_DELTA = 0` y bytes idénticos, comparados contra la **primera escritura
real** de esta tarea, no contra un baseline histórico:
`{buyer_units: 0, aliases: 0, source_bindings: 0}`.

## Cobertura M0 reproducida

M0 se reejecutó offline sobre la producción expandida, con las fuentes congeladas.

| Métrica y denominador | Valor |
|---|---:|
| Territorios reconocidos / 607 acreditados activos | **571/607 = 94,0692%** |
| Territorios reconocidos / 1.103 DIVIPOLA | **571/1.103 = 51,7679%** |
| Procesos únicos / cohorte acreditada | 323.960/339.745 = **95,3539%** |
| Contratos únicos / cohorte acreditada | 318.904/331.185 = **96,2918%** |
| BuyerUnits municipales reconocidas | 119 → **622** |
| Territorios añadidos / perdidos | **501 / 0** |
| Reclamaciones de fuente en disputa | **0** |

Las 503 propuestas añaden 501 territorios: Manizales (17001) y El Cerrito (76248)
ya tenían otra unidad reconocida. Los porcentajes de volumen describen la cohorte
acreditada, no el universo de municipios de Colombia. El denominador global de
BuyerUnits municipales sigue **pendiente de adjudicación M2**.

## Transición de oráculos pre/post promoción

Un único test focal falló tras la promoción, y era el oráculo de ausencia previsto:
`test_compound_identity_guard.py::test_retry_absence_and_scan_contracts_have_evidence`.
`adjudicate_municipal_m1a2.py` tomaba **producción viva** como rol «baseline» y el
snapshot del Retry #1 como «candidate». Producción ya no es un baseline.

El cambio aplicado no relaja ni elimina ninguna aserción:

- El rol de ausencia se liga al **baseline congelado pre-promoción**
  (`municipal_m1a_final/baseline/`), que es la política que M1A.2 ya declaraba.
  Baranoa, Nuevo Colón y Villa de Leyva siguen exigiendo ausencia de comprador y
  bloqueo de ejecución allí.
- Se añade un rol `production` que exige la **resolución correcta** de esas tres
  unidades y conserva el contrato de `scan_disabled` verificado en Belmira
  (lookup exacto disponible, scan de una personería homónima devuelve `None`,
  el canonical propio puede figurar en `scan_disabled_aliases`).
- La salida deja de escribir en `municipal_m1a_2/`, para no sobrescribir evidencia
  histórica.

No se tocó ningún otro test, ni el parser, ni el guard, ni los oráculos.

## Validación

- Focales municipales, compuestos, DIVIPOLA, ADR-072, familia institucional,
  BuyerGroups, SDIS, gobernaciones, universidades, ICBF/SENA, `result_identity`,
  `result_grouping`, Turbaco y M1A.2A, más `test_module_size_limits`: **850 passed**
  tras la transición del oráculo, sin fallos.
- Corpus canónico: **89/89**.
- Ruff, `make lint-core`, `test_module_size_limits`, `git diff --check`: **PASS**.
- Suite completa: **11.032 passed, 2 skipped, 14 xfailed históricos** en 508,68 s.
  **Nuevos xfails: 0. Tests modificados: 0.**
- `LLM_EXECUTION_MODE: DETERMINISTIC_OFFLINE_NO_LLM`;
  `LLM_PROVIDER_LIVE_COVERAGE: NOT_VERIFIED`.

Archivos protegidos alterados durante la promoción: **ninguno**. DIVIPOLA,
BuyerGroups, propuestas M0, `result_identity`, `result_grouping`, el parser y la
evidencia histórica de M0/M1A/M1A.1/M1A.2/M1A.2A/retry conservan su SHA-256.

## Reproducción

```bash
.venv/bin/python3 scripts/audits/verify_municipal_m1a_final.py preflight
.venv/bin/python3 scripts/audits/verify_municipal_m1a_final.py dry-run
.venv/bin/python3 scripts/audits/verify_municipal_m1a_final.py apply
.venv/bin/python3 scripts/audits/verify_municipal_m1a_final.py second-apply
.venv/bin/python3 scripts/audits/verify_municipal_m1a_final.py gates
.venv/bin/python3 scripts/audits/verify_municipal_m1a_final.py coverage
.venv/bin/python3 scripts/run_query_corpus.py --mode all
.venv/bin/python3 -m pytest tests/ -q
.venv/bin/ruff check app/ tests/ scripts/
make lint-core
```

`preflight` exige `main` en `67281fc` con árbol seguido limpio y congela el
baseline; `apply` se niega a escribir si los bytes del candidato no coinciden con
los del dry run auditado. Todos los artefactos viven en
`artifacts/audits/municipal_m1a_final/`.

## Deuda abierta

La promoción no cierra M2. Siguen abiertos: la adjudicación del universo municipal
completo, el denominador global de BuyerUnits municipales, los 36 territorios
acreditados activos aún no reconocidos, los conflictos SIGEP conservados de Ocaña
(NIT) y Calarcá (código 63002 frente a 63130), y la cobertura con proveedor LLM
vivo. La seguridad de nombres compuestos se comprueba sobre el universo DIVIPOLA
derivado (411 códigos relevantes); no se extrapola a compradores ajenos a él.

Commits: `feat(gazetteer): promote verified municipal executive buyer units` y
`docs(audit): close municipal M1A final promotion`. **PUSH: NO.**

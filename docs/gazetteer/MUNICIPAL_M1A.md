# Municipal M1A — promoción detenida por canarios predictivos

**Estado: BLOCKED. Producción: 0 promociones. PUSH: NO.**

La entrada M0 coincide con los 503 ejecutivos autorizados: 498 unidades nuevas y cinco ampliaciones de aliases. El catálogo temporal acredita los 1.888 aliases por lookup exacto y conserva la proyección física en ambos datasets. Sin embargo, el pipeline real falla 30 consultas en nueve municipios e introduce cuatro resoluciones nuevas de **Susacón a Susa**. Se activó el **STOP GLOBAL** solicitado para wrong owners sistémicos. No se excluyeron unos municipios para publicar el resto: incorporar Susa también altera consultas por Susacón, por lo que la independencia del lote no está demostrada.

`app/data/aliases_db.json` y `app/data/buyer_source_names.json` permanecen byte por byte iguales al HEAD inicial `9fb6375a3d23158b44ca9ad14bf1624c2495d1fc`, en `main`. Las métricas de `preview/` describen exclusivamente un candidato no aplicado. El manifiesto de la raíz registra 503 propuestas diferidas y ninguna promovida.

## Evidencia del bloqueo

El gate ejecuta los **1.888 aliases de M0** por `QueryRouter → compute_query_plan → SoQLBuilder`, con un catálogo temporal y su registro de proyección. Las formas de alcaldía prueban contratos y las formas de municipio prueban procesos. No usa LLM ni llamadas a Socrata. En cada fallo conserva parámetros BEFORE/AFTER, comprador esperado, resolución observada y SoQL candidato.

| DIVIPOLA | Ejecutivo esperado | Fallos | Comportamiento observado |
|---|---|---:|---|
| 05148 | El Carmen de Viboral | 4 | `el carmen` se corta como `el carm`; alcaldía termina en El Espinal, ya ocurría BEFORE |
| 05856 | Valparaíso, Antioquia | 2 | `valparaiso` se corta como `val`; no resuelve |
| 13244 | El Carmen de Bolívar | 4 | El mismo recorte; alcaldía termina en El Espinal, ya ocurría BEFORE |
| 15087 | Belén, Boyacá | 2 | `belen` se corta como `bel`; no resuelve |
| 15494 | Nuevo Colón | 4 | Se pierde `nuevo`, aparece orden por fecha y búsqueda de objeto `alcaldia/municipio` + `colon` |
| 15774 | Susacón | 4 | `susacon` se corta como `susa`; el candidato resuelve exactamente a **ALCALDIA DE SUSA**, BEFORE no tenía comprador |
| 18860 | Valparaíso, Caquetá | 2 | `valparaiso` se corta como `val`; no resuelve |
| 25486 | Nemocón | 4 | `nemocon` se corta como `nemo`; no resuelve |
| 66088 | Belén de Umbría | 4 | Se corta como `bel`; no resuelve |

La frontera compartida `CAPTURE_BOUNDARY` de `app/core/entity_types.py` admite `\s*` y alternativas como `con\b`, `en\b` y `para` sin exigir comienzo de palabra. El recorte dentro del nombre ocurre **antes** de consultar el alias completo. En Susacón, acreditar por exactitud el fragmento equivocado sigue siendo un wrong owner. Nuevo Colón presenta además una interferencia de extracción de ordenación que permanece pendiente. No se modificaron estas autoridades ni `query_router.py` durante M1A.

La detección inicial de El Carmen se atribuyó provisionalmente a la política de scan. La comprobación completa refutó esa explicación: sus aliases no estaban deshabilitados y el fallo ya existía en BEFORE. El gate conserva ambas ejecuciones.

### Geografía y Baranoa

Los 16 canarios de comprador —dos por Baranoa, Sabaneta, Puerto Gaitán, Magangué, Los Patios, Tocancipá, Aguazul e Ipiales— pasan en el **candidato**. Baranoa no tiene rama especial en el promotor: DIVIPOLA 08078, cuenta 701706467, evidencia fiscal 890112371, mismo nombre físico en ambos datasets. La proyección candidata alcanza 381 filas / **380 procesos únicos** y **374 contratos únicos** del snapshot M0.

El canario obligatorio `contratos en baranoa` falla la semántica geográfica tanto BEFORE como en el candidato: se convierte en objeto `baranoa`, sin comprador y sin filtro de ciudad. No basta afirmar que no se convirtió en buyer. De ocho canarios `en X`, sólo Ipiales llega correctamente a territorio tras el plan; Sabaneta, Los Patios y Tocancipá conservan capturas legacy de comprador, y los otros cuatro quedan como objeto. No se atribuye esta deuda histórica a M1A ni se declara cerrada.

En producción Baranoa continúa sin cobertura acreditada: **0/380 procesos y 0/374 contratos**. La paridad positiva corresponde sólo a la simulación y no implica promoción.

## Promotor y trazabilidad

`scripts/audits/promote_municipal_m1a.py` verifica el conteo exacto, los hashes de capturas M0 y la evidencia histórica y reciente por cuenta/nombre/NIT/dataset. La transformación conserva DIVIPOLA, municipio, departamento, fecha de acreditación, códigos SIGEP, base de afiliación, aliases originales, source names byte por byte, volúmenes y regla de autorización. El NIT es evidencia, nunca criterio de merge ni identidad del binding. Los bindings candidatos usan IDs `BUYER_UNIT:municipal_m0:<DIVIPOLA>` del lote acreditado.

Las cinco ampliaciones mantienen su canonical: Manizales, Tocancipá, Sabaneta, Los Patios y Bucaramanga. Los bindings preexistentes se conservan íntegros. M0 acredita 503 bindings completos nuevos en el candidato; ninguno se escribe a producción.

El promotor valida primero en memoria, repite la transformación y ejecuta los gates. `--apply` sale con código **1** ante el bloqueo y **no escribe los datos de producción**. Dos ejecuciones de `--apply` generan los mismos bytes de evidencia y conservan los hashes de producción. La idempotencia positiva de la transformación se prueba también antes de llegar al gate que bloquea.

### Scan ADR-072

Se auditan **42.790 superficies** del directorio congelado, incluidas cuentas inactivas, y toda la actividad, no sólo la cohorte municipal. Sólo se exime el nombre físico exacto junto con su cuenta y evidencia fiscal acreditadas. No se supone que dos instituciones sean la misma por NIT o ubicación.

El candidato declara **409 superficies EXACT_ONLY** —incluidos nombres canónicos automáticos— y no añade aliases para deshabilitarlas. El ajuste de runtime se limita a permitir que `scan_disabled_aliases` nombre el canonical propio, además de aliases propios. Los nombres ajenos siguen causando error de carga. Ningún registro actual necesita esta nueva posibilidad; los datos de producción siguen iguales.

Los gates de lookup, identidad canónica, aliases multi-owner y proyección no muestran nueva deuda en el candidato. Esto **no** equivale a un gate global verde: el pipeline introduce cuatro wrong-owner queries. No se modifica ningún umbral de deuda histórica.

## Conteos: producción y simulación separados

| Métrica | BEFORE | Producción AFTER | Candidato bloqueado |
|---|---:|---:|---:|
| Registros gazetteer | 599 | 599 | 1.097 |
| Aliases almacenados | 10.802 | 10.802 | 12.685 |
| Nuevas BuyerUnits | — | 0 | 498 |
| Unidades existentes ampliadas | — | 0 | 5 |
| Aliases adicionales | — | 0 | 1.883 |
| Bindings completos adicionales | — | 0 | 503 |
| Rechazos individuales por evidencia | — | 0 | 0 |
| Propuestas diferidas por STOP global | — | 503 | 503 |

M0 propone 1.888 aliases; cinco ya existen byte por byte en las ampliaciones, por lo que la suma incremental sería 1.883. No se inventaron aliases ni se regeneró el gazetteer legacy.

## Cobertura vigente

El replay offline de M0 contra producción es **idéntico al análisis M0 completo**, incluidas filas, unidades y afiliaciones. Como no hubo promoción, no existe un replay POST de producción expandida. Las proyecciones 571/607, ~95,35% de procesos y ~96,29% de contratos continúan siendo potencial M0, no cobertura alcanzada.

| Métrica | Numerador / denominador | Cobertura real |
|---|---:|---:|
| COVERAGE_IN_ACCREDITED_ACTIVE_COHORT | 70 / 607 territorios | 11,5321% |
| COVERAGE_OVER_DIVIPOLA_1103 | 70 / 1.103 territorios nominales | 6,3463% |
| PROCESS_UNIQUE_VOLUME_COVERAGE | 197.352 / 339.745 procesos | 58,0883% |
| CONTRACT_UNIQUE_VOLUME_COVERAGE | 199.963 / 331.185 contratos | 60,3780% |

El denominador global de BuyerUnits municipales sigue sin adjudicarse. Ningún porcentaje de volumen acredita cobertura del mismo porcentaje de municipios colombianos.

## Rendimiento del candidato

Medición emparejada en el mismo proceso, siete cargas y listas de consultas idénticas con calentamiento; tiempos de pared en milisegundos. El lookup sólo incluye consultas exactas en ambos catálogos. El archivo conserva consultas y número de muestras; no se interpreta como benchmark de tráfico real.

| Operación | BEFORE p50 / p95 | Candidato p50 / p95 |
|---|---:|---:|
| Carga EntityResolver | 92,327 / 97,515 | 115,366 / 125,269 |
| Lookup exacto | 0,0088 / 0,1454 | 0,0087 / 0,1380 |
| Scan | 6,800 / 31,643 | 7,733 / 52,975 |
| Fuzzy | 15,953 / 18,200 | 21,086 / 28,937 |

El lookup permanece estable; el scan p95 aumenta ~67% en esta muestra pequeña y merece revisión antes de promover. No se introdujo optimización para sortear el STOP. Los tiempos AFTER de esta tabla son del candidato, no de producción.

## Validación y reproducción

- Baseline: 29 tests focales; corpus 89/89; validate sin errores.
- M1A: 19 tests focales; junto con ADR-072, **76 passed**. Incluyen rechazo localizado, idempotencia, preservación de aliases y bindings, familia institucional, política canónica de scan y gate de bloqueo. No se añaden xfails para aceptar el defecto de captura.
- Corpus final: 89/89.
- Ruff: PASS; `make lint-core`: PASS; validate: 0 errores, 67 warnings legacy y 141 claves multi-owner frente al tope vigente de 173.
- Suite completa: **10877 passed, 2 skipped, 14 xfailed**, sin nuevos xfails; 487.22s (0:08:07).
- LLM_EXECUTION_MODE: `DETERMINISTIC_OFFLINE_NO_LLM` para los canarios municipales.
- LLM_PROVIDER_LIVE_COVERAGE: `NOT_VERIFIED`.

```bash
# Esperado: exit 1, STOP GLOBAL, ningún cambio a los datos de producción.
.venv/bin/python scripts/audits/promote_municipal_m1a.py --apply

# Replay de la producción sin promoción; preserva M0 original.
.venv/bin/python scripts/audits/audit_municipal_m0.py \
  --output artifacts/audits/municipal_m1a/unchanged_runtime_replay.json.gz
.venv/bin/python scripts/audits/verify_municipal_m1a.py
.venv/bin/python scripts/audits/benchmark_municipal_m1a.py
.venv/bin/python -m pytest tests/test_municipal_m1a.py -q
```

`artifacts/audits/municipal_m1a/` contiene baseline comprimido, manifiesto de no promoción, BEFORE/AFTER real, rechazos/diferimientos, canarios, cobertura vigente, benchmark, prueba de idempotencia y gates. `preview/` contiene el manifiesto candidato derivado, su delta, los gates de datos y los 30 fallos del pipeline completo. El benchmark es reproducible en método, no byte-idéntico en tiempos; los artefactos de transformación sí son deterministas.

## Deuda y commits

**Cerrado:** mecanismo reproducible de simulación/promoción con gate previo de escritura; trazabilidad por propuesta; pruebas de idempotencia; auditoría de scan sobre evidencia congelada; demostración de que exact ownership aislado no basta para promover este lote.

**Abierto:** promoción de las 503 propuestas; captura de nombres completos y precedencia de ordenación en Nuevo Colón; geografía explícita `en X`; medición/revisión de rendimiento del candidato; afiliación residual M0 y denominador global; proveedor LLM vivo no verificado. Resolver estos bloqueos requiere un trabajo semántico previo a reintentar M1A, sin añadir excepciones por municipio.

Commit funcional: `5a222b1`. El commit documental registra este cierre BLOCKED; ninguno promueve datos y no se hace push.

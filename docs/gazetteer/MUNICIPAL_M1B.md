# Municipal M1B — residuo acreditado y señal para M2

**MUNICIPAL_M1B: PASS. DISCOVERY / READ-ONLY. PUSH: NO.**

Rama `main`, baseline `51a5ad9`. **607 − 571 = 36**, derivado por diferencia de conjuntos; la lista no está hardcodeada. Los 36 contienen **39 perfiles físicos** `(account, physical_source_name)` en **38 cuentas**, con **2,571 filas físicas de procesos / 2,513 procesos únicos** y **1,711 filas físicas de contratos / 1,711 contratos únicos**.

Hay **23 perfiles ejecutivos en 22 cuentas** y **16 perfiles de otras instituciones**. Angelópolis publica dos grafías por una cuenta; no se fusionan en el inventario ni se confunden 36 territorios con 36 alcaldías faltantes.

## Alcance, fuentes y autoridades

Se leen M1A FINAL, M0 V1, ADR-072/073/074, coverage_post, analysis, proposals, directory, SIGEP, procesos, contratos, ownership histórico y adjudicación/gates M1A.2A. La ventana de actividad es `[2025-09-03, 2026-09-03)`. El script verifica el manifiesto SHA-256 de M0 y reejecuta `audit_municipal_m0.py` con salida temporal. El resultado debe ser idéntico al post-analysis congelado de M1A FINAL y sus volúmenes deben coincidir con coverage_post.

El denominador se conserva: 339.745 procesos únicos y 331.185 contratos únicos de la cohorte municipal acreditada. Una afiliación nueva de directorio no se suma al numerador de ese denominador sin ampliar y volver a conciliar la cohorte. Ubicación de directorio o geografía de la fuente sirve únicamente para seleccionar contexto.

El inventario añade **249 perfiles de contexto** con actividad y **262 cuentas registradas sin actividad observada en la ventana**. Se conservan separadas de los 39 perfiles que explican la deuda. Son señales de ubicación, afiliación no municipal o afiliación pendiente; no se convierten en pertenencia municipal. Las cuentas sin actividad no se declaran ruido, inactivas ni SECOP I por ausencia de datos.

Cada perfil conserva estratos y referencias a las fuentes, cuenta, nombre físico, grafías fiscales, filas/IDs por dataset, directorio, SIGEP, coincidencias exactas del catálogo, bindings existentes y conflictos históricos/recientes. Las coincidencias de alias sin adjudicación se etiquetan como observaciones, no owners acreditados. No se hace fuzzy ni se deduplica por NIT. La equivalencia fiscal sólo elimina puntos/espacios y un DV separado para anotar pares.

## Impacto y denominadores

| Corte | Δ procesos únicos | Δ contratos únicos | Δ pp procesos | Δ pp contratos | Cobertura procesos | Cobertura contratos |
|---|---:|---:|---:|---:|---:|---:|
| ACTUAL | 0 | 0 | 0.0000 | 0.0000 | 95.3539% | 96.2918% |
| TOP_5 | 1805 | 1162 | 0.5313 | 0.3509 | 95.8851% | 96.6427% |
| TOP_10 | 2437 | 1686 | 0.7173 | 0.5091 | 96.0712% | 96.8009% |
| TOP_20 | 2499 | 1706 | 0.7356 | 0.5151 | 96.0894% | 96.8069% |
| ALL_36 | 2513 | 1711 | 0.7397 | 0.5166 | 96.0935% | 96.8084% |
| ALL_SAFE | 0 | 0 | 0.0000 | 0.0000 | 95.3539% | 96.2918% |
| EVIDENCIA LISTA, PIPELINE PENDIENTE | 1483 | 1045 | 0.4365 | 0.3155 | 95.7904% | 96.6073% |

Top 5/10/20/36 son **techos condicionales**: suponen resolver todos los perfiles acreditados de esos territorios. No son una promesa de APPLY. Los candidatos sin pipeline no se cuentan como seguros.

El resto de la deuda de cobertura está en los **571 territorios ya reconocidos**: 13,499 filas físicas / **13,272 procesos únicos**, 10,570 filas físicas / **10,570 contratos únicos**. La cobertura nominal se obtiene con al menos una unidad; no acredita todo su ecosistema.

Reconciliación exacta: **323.960 + 2.513 + 13.272 = 339.745** procesos; **318.904 + 1.711 + 10.570 = 331.185** contratos. La CAS de San Gil aporta 1.602 procesos y 1.495 contratos de `ACCREDITED_OTHER_AFFILIATION`: queda en contexto. Filtrar sólo por `municipality` la incluiría erróneamente y daría 4.115/3.206.

Los agregados capturados no contienen valor contractual ni listas de IDs. Se informa valor `null`, no cero ni estimado. La suma de DISTINCT por estrato está habilitada por la reconciliación global M0; en contratos se acreditó igualdad entre filas físicas y IDs distintos. Se mantienen ambas métricas y no se extrapola esa igualdad a otras ventanas.

## Ranking completo y ejecutivo frente a ecosistema

Orden: procesos únicos descendentes, contratos únicos descendentes y DIVIPOLA como desempate. El score adicional es la suma de ambos conteos, sin valor monetario. `HAS_EXECUTIVE` indica existencia institucional por SIGEP/autoridad acotada; no prueba actividad reciente de una alcaldía. `OTHER` cuenta cuentas no ejecutivas acreditadas como aproximación explícita, no como censo lógico de BuyerUnits.

| # | DIVIPOLA | Municipio / departamento | Proc. filas / únicos | Contr. filas / únicos | HAS_EXECUTIVE | Ejecutivo reconocido | OTHER |
|---:|---|---|---:|---:|---|---|---:|
| 1 | 76892 | YUMBO / VALLE DEL CAUCA | 457 / 457 | 443 / 443 | YES | YES | 2 |
| 2 | 52573 | PUERRES / NARIÑO | 418 / 418 | 62 / 62 | YES | NO | 1 |
| 3 | 68081 | BARRANCABERMEJA / SANTANDER | 374 / 372 | 334 / 334 | YES | YES | 1 |
| 4 | 05660 | SAN LUIS / ANTIOQUIA | 359 / 337 | 323 / 323 | YES | NO | 0 |
| 5 | 23168 | CHIMÁ / CÓRDOBA | 221 / 221 | 0 / 0 | YES | NO | 1 |
| 6 | 25658 | SAN FRANCISCO / CUNDINAMARCA | 212 / 204 | 177 / 177 | YES | NO | 0 |
| 7 | 05036 | ANGELÓPOLIS / ANTIOQUIA | 205 / 194 | 163 / 163 | YES | NO | 0 |
| 8 | 76834 | TULUÁ / VALLE DEL CAUCA | 164 / 160 | 130 / 130 | YES | NO | 1 |
| 9 | 15516 | PAIPA / BOYACÁ | 55 / 55 | 54 / 54 | YES | NO | 1 |
| 10 | 68266 | ENCISO / SANTANDER | 19 / 19 | 0 / 0 | YES | NO | 0 |
| 11 | 66045 | APÍA / RISARALDA | 16 / 16 | 0 / 0 | YES | NO | 1 |
| 12 | 68679 | SAN GIL / SANTANDER | 12 / 12 | 12 / 12 | YES | YES | 1 |
| 13 | 50689 | SAN MARTÍN / META | 8 / 8 | 0 / 0 | YES | NO | 1 |
| 14 | 73861 | VENADILLO / TOLIMA | 7 / 7 | 0 / 0 | YES | NO | 0 |
| 15 | 52250 | EL CHARCO / NARIÑO | 6 / 4 | 0 / 0 | YES | NO | 0 |
| 16 | 52520 | FRANCISCO PIZARRO / NARIÑO | 4 / 4 | 0 / 0 | YES | NO | 1 |
| 17 | 05890 | YOLOMBÓ / ANTIOQUIA | 3 / 3 | 3 / 3 | YES | NO | 1 |
| 18 | 52254 | EL PEÑOL / NARIÑO | 3 / 3 | 3 / 3 | YES | NO | 1 |
| 19 | 13894 | ZAMBRANO / BOLÍVAR | 3 / 3 | 0 / 0 | YES | NO | 0 |
| 20 | 05142 | CARACOLÍ / ANTIOQUIA | 3 / 2 | 2 / 2 | YES | NO | 1 |
| 21 | 05101 | CIUDAD BOLÍVAR / ANTIOQUIA | 1 / 1 | 1 / 1 | YES | NO | 1 |
| 22 | 15377 | LABRANZAGRANDE / BOYACÁ | 1 / 1 | 1 / 1 | YES | NO | 1 |
| 23 | 25483 | NARIÑO / CUNDINAMARCA | 2 / 1 | 1 / 1 | YES | NO | 0 |
| 24 | 05411 | LIBORINA / ANTIOQUIA | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 25 | 13212 | CÓRDOBA / BOLÍVAR | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 26 | 23068 | AYAPEL / CÓRDOBA | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 27 | 52786 | TAMINANGO / NARIÑO | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 28 | 54125 | CÁCOTA / NORTE DE SANTANDER | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 29 | 70670 | SAMPUÉS / SUCRE | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 30 | 73671 | SALDAÑA / TOLIMA | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 31 | 85225 | NUNCHÍA / CASANARE | 3 / 1 | 0 / 0 | YES | NO | 0 |
| 32 | 85279 | RECETOR / CASANARE | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 33 | 86755 | SAN FRANCISCO / PUTUMAYO | 1 / 1 | 0 / 0 | YES | NO | 0 |
| 34 | 99624 | SANTA ROSALÍA / VICHADA | 6 / 1 | 0 / 0 | YES | NO | 0 |
| 35 | 25805 | TIBACUY / CUNDINAMARCA | 0 / 0 | 1 / 1 | YES | NO | 0 |
| 36 | 70771 | SUCRE / SUCRE | 0 / 0 | 1 / 1 | YES | NO | 0 |

## Los 39 perfiles que explican exactamente los 36 territorios

La naturaleza jurídica SIGEP es la primera autoridad institucional. Los establecimientos públicos se refinan con su nombre oficial. Las clasificaciones basadas sólo en nombre quedan identificadas como provisionales en el inventario de contexto. La deuda principal usa precedencia explícita: conflicto de fuente, variante de fuente, falta de dataset ejecutivo, NIT compartido entre instituciones distintas, unidad no ejecutiva y demás casos. Los demás impedimentos siguen como flags.

| DIVIPOLA | Cuenta | Nombre físico | Familia | Deuda | Proc. filas / únicos | Contr. filas / únicos | Disposición |
|---|---|---|---|---|---:|---:|---|
| 05036 | 700774284 | ALCALDIA MUNICIPIO DE ANGELOPOLIS. | MUNICIPAL_EXECUTIVE | SOURCE_NAME_VARIANT | 0 / 0 | 163 / 163 | MANUAL_REVIEW |
| 05036 | 700774284 | ALCALDIA MUNICIPIO DE ANGELOPOLIS.. | MUNICIPAL_EXECUTIVE | SOURCE_NAME_VARIANT | 205 / 194 | 0 / 0 | MANUAL_REVIEW |
| 05101 | 725814933 | PERSONERIA DE CIUDAD BOLIVAR | PERSONERIA | NON_EXECUTIVE_MUNICIPAL_UNIT | 1 / 1 | 1 / 1 | KEEP_SEPARATE |
| 05142 | 721709038 | CONCEJO DE CARACOLI | CONCEJO | SHARED_NIT_DISTINCT_UNIT | 3 / 2 | 2 / 2 | KEEP_SEPARATE |
| 05411 | 702916602 | ALCALDIA MUNICIPIO DE LIBORINA | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 05660 | 702596610 | MUNICIPIO DE SAN LUIS | MUNICIPAL_EXECUTIVE | REVIEW | 359 / 337 | 323 / 323 | KEEP_SEPARATE |
| 05890 | 725533046 | PERSONERÍA DE YOLOMBÓ | PERSONERIA | SHARED_NIT_DISTINCT_UNIT | 3 / 3 | 3 / 3 | KEEP_SEPARATE |
| 13212 | 704439827 | ALCALDIA MUNICIPAL DE CORDOBA | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 13894 | 702668120 | ALCALDIA MUNICIPAL DE ZAMBRANO | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 3 / 3 | 0 / 0 | MANUAL_REVIEW |
| 15377 | 729459636 | ASOCIACION DE MUNICIPIOS DEL PIE DE MONTE ORIENTAL | OTHER_DECENTRALIZED | NON_EXECUTIVE_MUNICIPAL_UNIT | 1 / 1 | 1 / 1 | KEEP_SEPARATE |
| 15516 | 705857084 | INSTITUTO TERMAL DE PAIPA | INSTITUTE | NON_EXECUTIVE_MUNICIPAL_UNIT | 55 / 55 | 54 / 54 | KEEP_SEPARATE |
| 23068 | 709519680 | MUNICIPIO DE AYAPEL | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 23168 | 703041020 | EMPRESA SOCIAL DEL ESTADO CAMU CHIMA | ESE_HEALTH | NON_EXECUTIVE_MUNICIPAL_UNIT | 221 / 221 | 0 / 0 | KEEP_SEPARATE |
| 25483 | 704985506 | MUNICIPIO DE NARIÑO | MUNICIPAL_EXECUTIVE | REVIEW | 2 / 1 | 1 / 1 | KEEP_SEPARATE |
| 25658 | 704081157 | MUNICIPIO DE SAN FRANCISCO | MUNICIPAL_EXECUTIVE | REVIEW | 212 / 204 | 177 / 177 | KEEP_SEPARATE |
| 25805 | 704079102 | ALCALDIA MUNICIPAL DE TIBACUY | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 0 / 0 | 1 / 1 | MANUAL_REVIEW |
| 50689 | 723109732 | INSTITUCION EDUCATIVA IRACA | OTHER_DECENTRALIZED | NON_EXECUTIVE_MUNICIPAL_UNIT | 8 / 8 | 0 / 0 | KEEP_SEPARATE |
| 52250 | 710575226 | ALCALDÍA MUNICIPIO DE EL CHARCO | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 6 / 4 | 0 / 0 | MANUAL_REVIEW |
| 52254 | 732295738 | CONCEJO DE EL PEÑOL | CONCEJO | NON_EXECUTIVE_MUNICIPAL_UNIT | 3 / 3 | 3 / 3 | KEEP_SEPARATE |
| 52520 | 713333268 | CENTRO DE SALUD SEÑOR DEL MAR | ESE_HEALTH | NON_EXECUTIVE_MUNICIPAL_UNIT | 4 / 4 | 0 / 0 | KEEP_SEPARATE |
| 52573 | 704434299 | CENTRO HOSPITAL NUESTRO SEÑOR DE LA DIVINA MISERICORDIA PUERRES E.S.E. | ESE_HEALTH | NON_EXECUTIVE_MUNICIPAL_UNIT | 417 / 417 | 62 / 62 | KEEP_SEPARATE |
| 52573 | 723887113 | ALCALDIA MUNICIPIO DE PUERRES | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 52786 | 702896176 | ALCALDIA MUNICIPAL DE TAMINANGO - NARIÑO | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 54125 | 704930403 | MUNICIPIO DE CACOTA | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 66045 | 705796597 | EMPRESAS PUBLICAS MUNICIPALES DE APIA | PUBLIC_COMPANY | NON_EXECUTIVE_MUNICIPAL_UNIT | 16 / 16 | 0 / 0 | KEEP_SEPARATE |
| 68081 | 703022061 | INSPECCION DE TRANSITO Y TRANSPORTE DE BARRANCABERMEJA | OTHER_DECENTRALIZED | NON_EXECUTIVE_MUNICIPAL_UNIT | 374 / 372 | 334 / 334 | KEEP_SEPARATE |
| 68266 | 704322999 | ALCALDIA DE ENCISO SANTANDER | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 19 / 19 | 0 / 0 | MANUAL_REVIEW |
| 68679 | 722364247 | TERMINAL DE TRANSPORTES DE SAN GIL | PUBLIC_COMPANY | NON_EXECUTIVE_MUNICIPAL_UNIT | 12 / 12 | 12 / 12 | KEEP_SEPARATE |
| 70670 | 700043086 | ALCALDIA MUNICIPAL DE SAMPUES SUCRE | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 70771 | 705855385 | SUCRE - ALCALDIA MUNICIPIO DE SUCRE | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 0 / 0 | 1 / 1 | MANUAL_REVIEW |
| 73671 | 704584382 | MUNICIPIO DE SALDAÑA | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 73861 | 703840447 | MUNICIPIO DE VENADILLO | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 7 / 7 | 0 / 0 | MANUAL_REVIEW |
| 76834 | 711601906 | UNIDAD CENTRAL DEL VALLE DEL CAUCA | INSTITUTE | NON_EXECUTIVE_MUNICIPAL_UNIT | 164 / 160 | 130 / 130 | KEEP_SEPARATE |
| 76892 | 704247097 | INSTITUTO MUNICIPAL DE CULTURA DE YUMBO | INSTITUTE | NON_EXECUTIVE_MUNICIPAL_UNIT | 416 / 416 | 405 / 405 | KEEP_SEPARATE |
| 76892 | 704247162 | CONTRALORIA MUNICIPAL DE YUMBO | CONTRALORIA | NON_EXECUTIVE_MUNICIPAL_UNIT | 41 / 41 | 38 / 38 | KEEP_SEPARATE |
| 85225 | 702736570 | ALCALDIA MUNICIPAL DE NUNCHIA | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 3 / 1 | 0 / 0 | MANUAL_REVIEW |
| 85279 | 703709261 | ALCALDÍA MUNICIPAL DE RECETOR | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 1 / 1 | 0 / 0 | MANUAL_REVIEW |
| 86755 | 712078021 | MUNICIPIO DE SAN FRANCISCO | MUNICIPAL_EXECUTIVE | REVIEW | 1 / 1 | 0 / 0 | KEEP_SEPARATE |
| 99624 | 713103521 | ALCALDIA MUNICIPAL DE SANTA ROSALIA | MUNICIPAL_EXECUTIVE | MISSING_DATASET_EVIDENCE | 6 / 1 | 0 / 0 | MANUAL_REVIEW |

## Decisiones de identidad y límites de promoción

- **Angelópolis:** misma cuenta y dos grafías, `...ANGELOPOLIS..` en procesos y `...ANGELOPOLIS.` en contratos. El histórico observado es disjunto por dataset y cambia un signo terminal. Es candidato a adjudicar fuente/identidad; no se crean dos BuyerUnits ni se aplica automáticamente una consolidación de canonicals inexistentes.
- **San Luis, San Francisco y Nariño:** las fuentes físicas tienen otros propietarios por cuenta (incluida evidencia histórica para Nariño). Un binding exacto sólo por nombre puede seleccionar otra unidad; quedan separados y sin alias o identidad automáticos.
- **Ejecutivos con un solo dataset reciente:** se informa la evidencia histórica de cada dataset, incluso los conjuntos vacíos. No se confunde cero actividad reciente con SECOP I ni se oculta un contrato histórico bajo la etiqueta de ausencia total.
- **Instituciones no ejecutivas:** concejos, personerías, contraloría, salud, institutos, empresas y otros establecimientos son unidades independientes. `KEEP_SEPARATE` permite proponerlas individualmente en M2; no es autorización para un grupo.

| Disposición | Perfiles del residuo | Perfiles de contexto |
|---|---:|---:|
| AUTO_SAFE | 0 | 0 |
| MANUAL_REVIEW | 19 | 183 |
| KEEP_SEPARATE | 20 | 64 |
| SOURCE_BINDING_ONLY | 0 | 2 |
| ALIAS_ONLY | 0 | 0 |
| DO_NOT_ADD | 0 | 0 |

**AUTO_SAFE = 0**. Todos los ejecutivos residuales tienen conflicto de nombre, variante por dataset o evidencia reciente incompleta; no hay un lote ejecutivo equivalente a M1A. Hay **12 perfiles con afiliación/familia/fuentes históricas exclusivas listos para evaluar pipeline**. El techo condicional de esa selección figura arriba, sin promoverla a AUTO_SAFE. No se construyó ni mutó un catálogo candidato, no se añadieron aliases ni bindings. `ALIAS_ONLY` y `DUPLICATE_BUYER_UNIT` no se declaran por similitud.

## SECOP_DIRECTORY_EXACT_DIVIPOLA

Se revalida el contrato M1A.2A sobre las mismas capturas: **646 filas**, **616 códigos municipales**, **616 acuerdos primarios**, **0 contradicciones**, **606 corroboraciones independientes** por municipio/departamento y **10 ambiguas**. No se cuentan las ambiguas como acuerdos.

**San Gil (68679)** es la nueva afiliación adjudicable: canonical existente, nombre físico byte-idéntico en ambos datasets, cuenta única `700043011`, directorio exacto y departamento compatible. **Barrancabermeja (68081) y Yumbo (76892)** conservan su adjudicación congelada M1A.2A; no se vuelven a acreditar desde códigos de departamento de M0. Total: **1 nuevo caso / 3 ejecutivos existentes** en los 36.

Esto añade **0 perfiles y 0 volumen al residuo municipal acreditado fijo**: los perfiles residuales de estos territorios son otras instituciones. Los nuevos ejecutivos físicamente observados de Caracolí, Apía y San Martín, por ejemplo, no reciben afiliación automática por esta autoridad: todavía no son unidades existentes con el contrato completo.

Los seis controles del pipeline actual aciertan el owner existente. **Eso no equivale a pipeline seguro de promoción**: los tres carecen de binding exacto ADR-073. El primer alias exacto observado de Barrancabermeja (`alcaldía distrital del municipio de barrancabermeja`) además deja `alcaldia`/`distrital` como objeto en la consulta. Se conserva el SoQL como diagnóstico, sin modificar runtime. Por ello Barrancabermeja requiere revisión y los otros dos se señalan para binding de fuente; ninguno incrementa ALL_SAFE.

## Señal para M2 y siguiente paso

| Familia | Perfiles | Procesos únicos | Contratos únicos |
|---|---:|---:|---:|
| MUNICIPAL_EXECUTIVE | 23 | 781 | 666 |
| CONCEJO | 2 | 5 | 5 |
| PERSONERIA | 2 | 4 | 4 |
| CONTRALORIA | 1 | 41 | 38 |
| ESE_HEALTH | 3 | 642 | 62 |
| INSTITUTE | 3 | 631 | 589 |
| PUBLIC_COMPANY | 2 | 28 | 12 |
| FUND | 0 | 0 | 0 |
| OTHER_DECENTRALIZED | 3 | 381 | 335 |
| REGISTRY_NOISE | 0 | 0 | 0 |
| UNKNOWN_REVIEW | 0 | 0 | 0 |

Por número de perfiles el residuo **todavía es mayoritariamente ejecutivo: 58.97%**, frente a **41.03%** de otras instituciones. Por volumen se invierte: los ejecutivos aportan **31.08%** de los procesos y **38.92%** de los contratos del residuo. No se afirma que el residuo sea principalmente no ejecutivo por conteo de perfiles.

**NEXT: GO_DIRECT_TO_M2.** Priorizar instituciones no ejecutivas de Yumbo, Puerres, Barrancabermeja, Chimá, Tuluá y Paipa; adjudicar por separado la pareja de Angelópolis y los homónimos antes de plantear un M1B APPLY acotado. La mayor deuda de volumen permanece dentro de territorios nominalmente reconocidos. El denominador global de BuyerUnits municipales sigue sin adjudicar.

## Gates, validación y reproducción

Se protegen **1013 archivos** del baseline mediante hash: runtime, datos productivos, tests/scripts previos, autoridades y evidencia municipal histórica. `WRONG_OWNER=0` se refiere a los owners afirmados en las comprobaciones existentes, no a una garantía de toda consulta futura. `NO_NIT_MERGE`, `NO_RUNTIME_MUTATION`, `NO_GAZETTEER_MUTATION`, `NO_SOURCE_BINDING_MUTATION` y `NO_HISTORICAL_EVIDENCE_MUTATION`: **PASS**. Live calls: **0**.

- `focal_tests`: **PASS**. Evidencia: `artifacts/audits/municipal_m1b/validation/focal_tests.log`.
- `corpus_89`: **PASS**. Evidencia: `artifacts/audits/municipal_m1b/validation/corpus_89.log`.
- `ruff`: **PASS**. Evidencia: `artifacts/audits/municipal_m1b/validation/ruff.log`.
- `lint_core`: **PASS**. Evidencia: `artifacts/audits/municipal_m1b/validation/lint_core.log`.
- `diff_check`: **PASS**. Evidencia: `artifacts/audits/municipal_m1b/validation/diff_check.log`.
- Replay independiente: **PASS**, seis artefactos iguales salvo metadata de validación; informe idéntico antes de adjuntar los logs.

Suite completa no requerida: sólo docs/audit/tests nuevos, sin cambios productivos. `LLM_EXECUTION_MODE=DETERMINISTIC_OFFLINE_NO_LLM`; proveedor LLM vivo no verificado.

```bash
.venv/bin/python3 scripts/audits/audit_municipal_m1b.py --validate
# Replay en otra carpeta sin sobrescribir evidencia histórica:
.venv/bin/python3 scripts/audits/audit_municipal_m1b.py --output /tmp/municipal_m1b_replay
```

Artefactos: `residual_territories.json`, `buyer_profiles.json`, `impact_ranking.json`, `classification.json`, `promotion_candidates.json`, `gates.json`, bajo `artifacts/audits/municipal_m1b/`. Commits exclusivamente docs/audit. **PUSH: NO.**

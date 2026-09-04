# Municipal M2B — adjudicación de identidad institucional

**MUNICIPAL_M2B: PARTIAL. Integridad: PASS. PUSH: NO. Aplicaciones: 0.**

Se adjudican offline **55 perfiles / 55 BuyerUnits candidatas**; permanecen **876 identidades pendientes**. PARTIAL expresa deuda residual, no un gate de seguridad fallido.

## Baseline y universo

Autoridad congelada: M2A, HEAD inicial `e98bf39`, rama `main`. La afiliación no se recalcula. El universo exacto es 931 perfiles, 347.163 procesos únicos y 117.277 filas físicas de contratos. Se separan 370 perfiles ya lógicos sin reconocimiento de perfil y 628 reconocidos. Los 1.929 perfiles territoriales contienen 994 unidades lógicas iniciales; los granos no se suman.

## Taxonomía reproducible

La clase primaria es una partición. Los indicadores secundarios se solapan y quedan por perfil. Precedencia explícita: H → B → I → A → D → E → G → F → J → C → K. Una señal de abreviatura describe deuda; no acredita su expansión.

| Clase | Causa | Perfiles | Procesos únicos | Contratos físicos |
|---|---|---:|---:|---:|
| A | SAME_ACCOUNT_MULTIPLE_SOURCE_NAMES | 0 | 0 | 0 |
| B | SIGEP_ENTITY_MULTIPLE_ACCOUNTS | 0 | 0 | 0 |
| C | EXACT_DIRECTORY_ENTITY_WITHOUT_LOGICAL_UNIT | 29 | 15945 | 565 |
| D | CROSS_DATASET_SOURCE_VARIANT | 0 | 0 | 0 |
| E | PUNCTUATION_ONLY_VARIANT | 0 | 0 | 0 |
| F | ABBREVIATED_INSTITUTIONAL_NAME | 444 | 168407 | 28436 |
| G | SHARED_NIT_DISTINCT_BUYER_UNIT | 0 | 0 | 0 |
| H | OWN_SIGEP_ENTITY_BUT_IDENTITY_CONFLICT | 59 | 2646 | 2412 |
| I | MULTIPLE_ACCOUNTS_SAME_INSTITUTION_CANDIDATE | 20 | 16935 | 1852 |
| J | NO_INDEPENDENT_IDENTITY_EVIDENCE | 379 | 143230 | 84012 |
| K | OTHER_REVIEW | 0 | 0 | 0 |

## Prioridad exacta por familia M2A

Orden: procesos únicos, contratos físicos, perfiles, número seguro adjudicable; desempate estable. La familia congelada se conserva para reconciliar el denominador. Una naturaleza propia SIGEP puede refinar UNKNOWN en el candidato sin reescribir M2A.

| Familia | Perfiles | Procesos únicos | Contratos físicos | Perfiles resueltos |
|---|---:|---:|---:|---:|
| ESE_HEALTH | 415 | 216290 | 41154 | 51 |
| UNKNOWN | 191 | 81154 | 48003 | 4 |
| PUBLIC_COMPANY | 76 | 20185 | 2825 | 0 |
| INSTITUTE | 38 | 12566 | 13325 | 0 |
| MUNICIPAL_EXECUTIVE | 19 | 6869 | 6229 | 0 |
| PUBLIC_SERVICES | 66 | 6273 | 1731 | 0 |
| CONCEJO | 66 | 2202 | 2217 | 0 |
| PERSONERIA | 48 | 813 | 1056 | 0 |
| FUND | 8 | 560 | 535 | 0 |
| CONTRALORIA | 2 | 159 | 145 | 0 |
| EDUCATION | 2 | 92 | 57 | 0 |

## Contraste de las cuatro vías

| Vía | Pool con evidencia | Aceptados con cadena completa | Procesos del pool |
|---|---:|---:|---:|
| VIA_I | 0 | 0 | 0 |
| VIA_II | 0 | 0 | 0 |
| VIA_III | 931 | 55 | 347163 |
| VIA_IV | 162 | 55 | 61564 |

Los pools se solapan: III es corroboración de IV, nunca ganancia adicional.

- **I:** los 931 carecen de SIGEP_ENTITY_MATCH propio en M2A; no quedaron fuera por múltiples cuentas de un código propio ya acreditado.
- **II:** ninguna cuenta de los 931 tiene otra superficie en la captura completa. SAME_BUYER_UNIT y SOURCE_VARIANT por esta vía: 0. No se fabrica un lote de variantes.
- **III:** el directorio acredita una observación propia por cuenta/nombre byte-exacto y territorio concordante; por sí solo no concede identidad ni naturaleza jurídica.
- **IV:** se consulta el índice global SIGEP por nombre institucional, sin seleccionar por NIT. Se contrastan igualdad de tokens/plantilla ADR-075 y la nueva regla ESE. Las contradicciones fiscales bloquean; nunca seleccionan una unidad.

### Regla ESE probada

Se retira **a lo sumo una etiqueta legal completa de un extremo**: ESE, E.S.E. o EMPRESA SOCIAL DEL ESTADO (también el plural literal). Todos los tokens del núcleo institucional deben coincidir en orden con la fila propia SIGEP. Sólo la naturaleza jurídica propia `Empresas Sociales del Estado` permite la operación. Se permite etiqueta presente/ausente o en el otro extremo; no se eliminan palabras del núcleo, topónimos, niveles hospitalarios ni abreviaturas arbitrarias. Un municipio solo no acredita identidad.

El conjunto oficial debe identificar un código SIGEP único, territorio acreditado único y familia compatible. Se contrastan todas las cuentas observadas, cuentas adicionales del directorio, reclamaciones canónicas y vecinos fiscales de familia compatible. Otra cuenta no explicada bloquea aunque no transaccione en la ventana. Los IDs se forman con código SIGEP y cuenta; el NIT no es la llave. No se consolidan canonicals.

No hay acreditación de siglas opacas como HMI, fuzzy, substring genérico ni corrección de errores ortográficos. El matching por tokens y el guard de municipio compuesto reutilizan ADR-075. Las superficies físicas byte-exactas permanecen intactas.

### Razones de rechazo de IV (pueden solaparse)

| Razón | Perfiles |
|---|---:|
| COMPETING_DIRECTORY_ACCOUNTS | 24 |
| COMPETING_OBSERVED_ACCOUNTS | 2 |
| EXISTING_CANONICAL_CLAIM_REQUIRES_BINDING_REVIEW | 10 |
| OFFICIAL_FISCAL_CONTRADICTION | 59 |
| UNEXPLAINED_DIRECTORY_FISCAL_PEER_ACCOUNT | 55 |
| UNEXPLAINED_SAME_FAMILY_FISCAL_PEER_ACCOUNT | 1 |

## Salud

El subaudit contiene **439 perfiles**, 231281 procesos únicos y 41709 contratos físicos. Incluye señales ESE que M2A etiquetaba UNKNOWN; ese alcance no altera la familia histórica.

Hospital explícito: 275; centro/puesto: 106; una cuenta con varias grafías: 0. Centro/puesto sólo obtiene naturaleza institucional mediante su propia fila SIGEP.

Se publican 12 controles con varias observaciones de salud en un territorio. Territorio y familia compartidos no fusionan hospitales. Cada rechazo permanece en el ledger y en el delta.

## NIT compartido

120 perfiles pendientes tienen vecinos fiscales observados. Todos conservan KEEP_SEPARATE, incluidos los que logran acreditar su propia identidad. Otros cruces de directorio se enumeran aparte: 364 perfiles con otra cuenta registral bajo su evidencia fiscal; ambos conjuntos se solapan. La clase I exige una coincidencia institucional estricta con otra cuenta del directorio; un vecino fiscal sin esa evidencia no basta para asignarla. **NIT_AUTO_MERGES = 0**.

## Batches

Los cinco batches particionan 931 perfiles. Un batch puede contener revisión y ganancia segura; expected_gain cuenta sólo candidatos adjudicados.

| Batch | Perfiles | Candidatos lógicos | Procesos del batch | Contratos físicos | Ganancia procesos |
|---|---:|---:|---:|---:|---:|
| BATCH_A_HIGH_CONFIDENCE | 0 | 0 | 0 | 0 | 0 |
| BATCH_B_SOURCE_VARIANTS | 0 | 0 | 0 | 0 | 0 |
| BATCH_C_HEALTH_IDENTITY | 397 | 55 | 203296 | 39297 | 21055 |
| BATCH_D_MULTI_ACCOUNT_REVIEW | 61 | 0 | 29287 | 3617 | 0 |
| BATCH_E_MANUAL | 473 | 0 | 114580 | 74363 | 0 |

## Impacto hipotético dentro de la afiliación verificada

| Escenario | Perfiles | Nuevas unidades | Procesos | Contratos físicos | Cobertura procesos territorial |
|---|---:|---:|---:|---:|---:|
| TOP_SAFE_50 | 50 | 50 | 20892 | 664 | 53.60% |
| TOP_SAFE_100 | 55 | 55 | 21055 | 668 | 53.62% |
| ALL_RULE_BASED | 55 | 55 | 21055 | 668 | 53.62% |

ALL_RULE_BASED resuelve **6.06%** del volumen de procesos pendiente y **0.57%** de sus filas de contratos. TOP_SAFE_100 agota el pool si hay menos de 100; no se rellena con casos REVIEW. **Cambio de cobertura productiva SECOPPAL: 0.**

## Los 370 ya lógicos

| Disposición | Perfiles | Procesos | Contratos físicos |
|---|---:|---:|---:|
| READY_FOR_BUYER_PROMOTION | 0 | 0 | 0 |
| READY_FOR_REGISTRY_ONLY | 314 | 16011 | 14079 |
| SOURCE_BINDING_ONLY | 0 | 0 | 0 |
| KEEP_SEPARATE | 28 | 2477 | 1751 |
| MANUAL_REVIEW | 28 | 1132 | 824 |

No requieren adjudicar identidad. Las disposiciones M1B y conflictos congelados se conservan. Preparar un registro de conocimiento no equivale a aprobar aliases o promoción. READY_FOR_BUYER_PROMOTION exige un payload y gates posteriores, que M2B no fabrica.

## Delta territorial

| Municipio | Unidades actuales | Candidatas nuevas | Identidad pendiente |
|---|---:|---:|---:|
| BARRANQUILLA | 2 | 0 | 6 |
| BARANOA | 1 | 0 | 1 |
| BOGOTÁ, D.C. | 38 | 0 | 0 |
| BUCARAMANGA | 5 | 0 | 4 |
| BARRANCABERMEJA | 3 | 0 | 5 |
| YUMBO | 3 | 0 | 5 |
| ABEJORRAL | 1 | 0 | 1 |
| ALEJANDRÍA | 2 | 0 | 1 |
| AMAGÁ | 2 | 0 | 0 |
| AMALFI | 1 | 0 | 0 |
| ANDES | 1 | 0 | 2 |

Los municipios pequeños son controles de cardinalidad institucional baja, no de población. Los 1.103 territorios conservan sus unidades actuales y referencias de contexto pendientes M2A. Bogotá conserva exclusivamente su autoridad de registry declarado; nuevas identidades M2B: 0.

## Gates, reproducción y siguiente paso

Archivos protegidos: 576; cambios: 0. Incluye runtime completo y evidencia histórica M0/M1A/M1B/M2A, además del código M2A. El build bloquea conexiones de socket. LIVE_CALLS=0, NIT_AUTO_MERGES=0, AUTO_MERGES_BY_TERRITORY=0, FUZZY_IDENTITY_MATCHES=0.

```bash
.venv/bin/python3 scripts/audits/audit_municipal_m2b.py
.venv/bin/python3 scripts/audits/audit_municipal_m2b.py --output /tmp/municipal_m2b_replay
.venv/bin/python3 scripts/audits/audit_municipal_m2b.py --validate
```

Pruebas adversariales, corpus 89, Ruff, lint-core, diff y reproducción independiente quedan en `validation.json` y `validation/`. Si cambia un byte protegido se exige la suite completa y el audit falla cerrado.

**ADR_REQUIRED: YES.** La nueva regla ESE sólo se acepta tras la prueba offline. El barrido global y el ADR se referencian en `adr_scan.json`. No se cambia runtime.

**NEXT: M2B_1_TARGETED_ADJUDICATION.** Priorizar abreviaturas opacas de alto volumen, cuentas adicionales de salud y conflictos oficiales; los ya lógicos preparados para registro son un lote separado para M2C. No se ha aprobado ni aplicado ninguna promoción. **PUSH: NO.**

## Límites

La evidencia es la captura congelada M0, no una consulta vigente. La ausencia de otra cuenta se limita a las observaciones y al directorio capturados. Los controles adversariales no constituyen una estimación de precisión contra un oráculo completo. El veto fiscal conserva contradicciones y sacrifica recall; no rehabilita el NIT como autoridad de identidad. Las familias distintas y nombres distintos no prueban por sí solos SAME_BUYER_UNIT ni su negación.

## Cierre verificado

- **56 pruebas PASS:** 35 M2B y 21 regresiones M2A.
- **Corpus 89/89 PASS**, Ruff (`app/ tests/ scripts/`), `make lint-core` y `git diff --check`: PASS.
- **13 artefactos reproducidos byte a byte**, incluidos los 12 JSON del build y su informe generado.
- **576 archivos protegidos, 0 cambios** antes/después de la validación. Suite completa no requerida por la condición explícita de esta tarea.
- **ADR-076** documenta exclusivamente la regla de identidad ESE probada. El barrido previo cubre 29 worktrees; máximo ADR-075.
- **PUSH: NO.** Commit limitado a scripts, tests y documentación/evidencia M2B; producción intacta.

[ADR-076](../decisions/2026-09-04_ADR-076_municipal-own-sigep-ese-identity.md) ·
[Validación](../../artifacts/audits/municipal_m2b/validation.json) ·
[Gates](../../artifacts/audits/municipal_m2b/gates.json) ·
[Candidatos](../../artifacts/audits/municipal_m2b/adjudicated_candidates.json)

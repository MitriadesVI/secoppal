# Municipal M2B.1 — adjudicación dirigida de identidad de alto volumen

**MUNICIPAL_M2B_1: PARTIAL. Integridad: PASS. PUSH: NO. Aplicaciones: 0.**

M2B.1 adjudica **50 perfiles**: 43 unidades lógicas candidatas nuevas y 7 perfiles enlazados a canonicals ya declarados. La ganancia es **22183 procesos únicos** y **1752 filas físicas de contratos**. Los 55 candidatos M2B permanecen congelados y no se readjudican.

## Reconciliación del universo

| Clase M2B | Perfiles | Procesos únicos | Contratos físicos | Perfiles nuevos auditables |
|---|---:|---:|---:|---:|
| F | 444 | 168407 | 28436 | 406 |
| C | 29 | 15945 | 565 | 12 |
| I | 20 | 16935 | 1852 | 20 |
| H | 59 | 2646 | 2412 | 59 |

Los conteos F/C/I/H reproducen byte por byte la taxonomía M2B. Suman 552 perfiles; 55 pertenecen al lote ya aceptado, por lo que el delta dirigido contiene 497. Los 379 perfiles J completan los 876 pendientes y no se atacan como lote.

## F — patrones de abreviación

| Patrón | Perfiles | Procesos | Contratos físicos | Expansión oficial disponible | Delta aceptado |
|---|---:|---:|---:|---:|---:|
| HOSPITAL_OR_ESE_LEGAL_ABBREVIATION | 287 | 143971 | 25198 | 75 | 32 |
| PUBLIC_COMPANY_LEGAL_FORM_ABBREVIATION | 157 | 24436 | 3238 | 22 | 16 |

Estas clases son descriptivas. ESE, ESP, una sigla o un nombre corto no acreditan identidad por sí mismos. Los errores, truncamientos y ruido permanecen visibles; no se corrigen por similitud.

## Siglas opacas

Se examinan los 876 perfiles todavía pendientes tras M2B. El inventario contiene 63 ocurrencias estructurales: OFFICIALLY_EXPANDED=0, MULTIPLE_EXPANSIONS=0, UNRESOLVED_ACRONYM=63, CONFLICTING_EXPANSION=0.
Una expansión sólo es elegible si la propia fuente oficial imprime literalmente la sigla y la cadena completa de territorio, familia, cuenta y competidores pasa. Derivar iniciales de un nombre está prohibido. HMI, INDER o IMDER no se expanden por conocimiento común ni por parecido.

## C — directorio exacto

Los 29 perfiles se clasifican como DIRECTORY_PLUS_OWN_SIGEP_ENTITY=29. Disposiciones: KEEP_REVIEW=10, M2B_ACCEPTED_BASELINE=17, M2B1_DELTA_EXISTING_CANONICAL=2.
No se añadió un registro externo ni una llamada viva. DIRECTORY_ONLY concede cero identidades. El enlace a un canonical existente exige además identidad propia ADR-076 y ausencia de cuentas competidoras.

## I — cuentas múltiples

Clasificación: SAME_ENTITY_HISTORICAL_ACCOUNT=0, DISTINCT_BUYER_UNITS=0, REGISTRY_ONLY_ACCOUNT=20, UNRESOLVED=0.
REGISTRY_ONLY_ACCOUNT significa que la cuenta aparece en el directorio y no en la ventana transaccional congelada. No se la llama histórica: la captura agregada no prueba sucesión temporal. Fusiones productivas: 0.

## H — conflictos oficiales

Tipos observados, con solapamiento: FISCAL_CONFLICT=59, NAME_CONFLICT=1, ACCOUNT_CONFLICT=5. Resueltos: 0; KEEP_REVIEW: 59. No apareció una autoridad independiente superior; ninguna contradicción se ignora o decide por mayoría.

## Reglas contrastadas

| Regla | Matched | Aceptados | Rechazados | Procesos ganados | Contratos físicos | Resultado |
|---|---:|---:|---:|---:|---:|---|
| M2B1_R0_EXISTING_CANONICAL_ADR076 | 10 | 5 | 5 | 6274 | 1435 | COMPOSICIÓN ADR EXISTENTE |
| M2B1_R1_OFFICIAL_LOCATION_QUALIFIER | 72 | 28 | 44 | 7659 | 162 | ACEPTADA |
| M2B1_R2_TRAILING_CARE_LEVEL | 1 | 1 | 0 | 2643 | 1 | ACEPTADA |
| M2B1_R3_ESP_LEGAL_DESIGNATOR | 22 | 16 | 6 | 5607 | 154 | ACEPTADA |
| M2B1_R4_OFFICIALLY_PRINTED_ACRONYM | 0 | 0 | 0 | 0 | 0 | SIN ADJUDICACIÓN |

Cada trial agrega como máximo una regla nueva después del manejo legal ESE ya aceptado por ADR-076:

1. **R0** enlaza un perfil byte-exacto a su canonical declarado cuando la identidad propia ADR-076 y la cuenta del directorio están completas. No agrega normalización.
2. **R1** trata el municipio completo acreditado sólo como calificador terminal. Tras retirarlo debe quedar un núcleo institucional distintivo con secuencia exacta de tokens; territorio y familia nunca son el núcleo.
3. **R2** permite únicamente NIVEL I/II/III/1/2/3 al final del nombre sanitario oficial. No elimina niveles internos ni otros calificadores.
4. **R3** permite únicamente ESP terminal cuando la propia naturaleza SIGEP es una empresa de servicios públicos domiciliarios. No equipara SA, SAS o LTDA.
5. **R4** exige una sigla impresa literalmente y delimitada en el nombre oficial. No obtuvo ninguna adjudicación.

Todas exigen un único código SIGEP, territorio congelado, familia propia compatible, cuenta/nombre byte-exacto en directorio, NIT concordante sólo como veto y explicación independiente de cuentas vecinas. NIT no escoge identidad ni ID lógico. Un segundo código, cuenta, canonical o conflicto conserva REVIEW.

## Salud y canarios

El subaudit de salud cubre 260 perfiles del delta dirigido. Hospital local, razón social larga, calificador territorial, nivel y sigla se publican como hipótesis separadas. No se combinaron dos relajaciones nuevas. Dos hospitales del mismo territorio o NIT conservan unidades distintas.

## Concentración de volumen

| Corte | Procesos | Share del target | Share de los 876 pendientes | Aceptados en el corte |
|---|---:|---:|---:|---:|
| TOP_25 | 64386 | 35.21% | 19.74% | 3 |
| TOP_50 | 91009 | 49.76% | 27.91% | 7 |
| TOP_100 | 122809 | 67.15% | 37.66% | 11 |

## Impacto y stop condition

El acumulado M2B+M2B.1 es **105 perfiles**, 98 unidades candidatas nuevas más 7 enlaces a unidades existentes, y 826 pendientes. Resuelve 43238 procesos (12.45% del pool M2B original) y 2420 filas (2.06%). La cobertura productiva cambia 0.

**M2B_IDENTITY_DIMINISHING_RETURNS = NO.** El delta alcanza exactamente 50 perfiles y 6.39% del volumen original. Cumple la rama de 50 perfiles del umbral OR, aunque no la rama de 10% de volumen.

**Sugerencia para los edges:** no abrir M2B.2 como barrido general. Sólo justificarlo con una fuente oficial con cuentas fechadas o expansiones institucionales explícitas. En paralelo, los 314 registry-only ya lógicos son mejor insumo para TERRITORIAL_KNOWLEDGE_FOUNDATION que nuevas reglas nominales.

**NEXT: M2B_2_ONLY_IF_JUSTIFIED.** ADR_REQUIRED: YES. R1, R2 y R3 son semánticamente distintas y se documentan por separado después del barrido global.

## Reproducción y límites

```bash
.venv/bin/python3 scripts/audits/audit_municipal_m2b1.py
.venv/bin/python3 scripts/audits/audit_municipal_m2b1.py --output /tmp/municipal_m2b1_replay
.venv/bin/python3 scripts/audits/audit_municipal_m2b1.py --validate
```

El ejecutor bloquea sockets y no invoca LLM. Los ceros de WRONG_IDENTITY, fuzzy y merge indican que ningún candidato aceptado viola los canarios ejecutables; no son una estimación poblacional contra un oráculo completo. La ausencia de actividad no fecha una cuenta. La captura congelada puede estar incompleta, por lo que la falta de expansión oficial queda como deuda, no como evidencia negativa.

## Decisiones y cierre de validación

El barrido posterior al experimento cubrió los 29 worktrees y encontró ADR-076
como máximo global. Las reglas aceptadas se documentan de forma independiente:
[ADR-077](../decisions/2026-09-04_ADR-077_municipal-official-location-qualifier.md),
[ADR-078](../decisions/2026-09-04_ADR-078_municipal-trailing-care-level.md) y
[ADR-079](../decisions/2026-09-04_ADR-079_municipal-esp-legal-designator.md).

Validación final: 89 tests focales PASS; corpus 89/89 PASS; Ruff, lint-core,
diff y staged-diff PASS; los 13 artefactos reproducibles son byte-exactos. Los
612 archivos protegidos de producción, runtime y evidencia M0→M2B no cambiaron,
por lo que la suite completa no era requerida. Detalle ejecutable:
[`validation.json`](../../artifacts/audits/municipal_m2b_1/validation.json).

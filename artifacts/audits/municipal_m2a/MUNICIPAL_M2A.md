# Municipal M2A — universo institucional y afiliación

**MUNICIPAL_M2A: PASS. PUSH: NO. Promociones: 0.**

SECOPPAL reconoce **625 de 994 BuyerUnits municipales VERIFICADAS**, con un pool no resuelto de **10168 perfiles** (afiliación o identidad).

La cifra pertenece al denominador conservador M2A; no representa todos los compradores municipales del país.

## Granos y denominadores

| Grano | Total |
|---|---:|
| PHYSICAL_STRATUM | 15494 |
| PHYSICAL_PROFILE | 11298 |
| LOGICAL_BUYER_UNIT | 994 |

Recognized logical + unrecognized logical = verified logical units. Recognized profiles + unrecognized profiles + identity-unresolved profiles = verified territorial cohort. Verified territorial + affiliation-unresolved + nonmunicipal = all physical profiles. Never add unlike grains.

Los perfiles se reconcilian exclusivamente por (cuenta, nombre físico byte-exacto), entre datasets. Las unidades lógicas usan autoridad canónica congelada o identidad SIGEP independiente con cuenta observada única. Una afiliación territorial verificada sin identidad institucional resuelta permanece fuera del denominador lógico. Los NIT compartidos sólo anotan vecinos fiscales; ninguna propuesta se aplica.

| Cohorte | Perfiles | Procesos físicos | Procesos únicos | Contratos físicos | Contratos únicos derivados |
|---|---:|---:|---:|---:|---:|
| all_observed | 11298 | 1885602 | 1819018 | 1095013 | 1095013 |
| affiliated_verified | 1929 | 724289 | 703120 | 465445 | 465445 |
| recognized | 628 | 354187 | 336337 | 331514 | 331514 |
| unrecognized_verified_logical | 370 | 19861 | 19620 | 16654 | 16654 |
| verified_affiliation_identity_unresolved | 931 | 350241 | 347163 | 117277 | 117277 |
| unresolved_affiliation_pool | 9237 | 1015199 | 973700 | 567673 | 567673 |
| non_municipal_verified | 132 | 146114 | 142198 | 61895 | 61895 |

**Contratos únicos: DERIVED_FROM_GLOBAL_IDENTITY.** El SoQL de contratos congelado mide count(*), no DISTINCT por perfil. La igualdad global 1.095.013 = 1.095.013 permite derivar esa dimensión sólo en la ventana [2025-09-03, 2026-09-03). Cada volumen derivado lleva su provenance.

Cobertura del volumen territorial verificado: procesos **47.83%**, contratos **71.23%**.

## Vías de afiliación y falsificación

Vía A compone la evidencia fiscal del perfil con una afiliación territorial ya acreditada y verifica la cardinalidad antes de usarla. Sola produce PROBABLE. VERIFIED requiere una fila SIGEP propia emparejada por identidad institucional o el registro propio byte-exacto de directorio con código concordante; la segunda evidencia proviene del perfil observado.

Vía B distingue el territorio fiscal de la identidad. Su pool se reproduce seleccionando las filas SIGEP Municipal/Distrital con DIVIPOLA único. Para aplicar la vía se exige además que todas las filas fiscales tengan alcance único, código municipal válido y ausencia de Bogotá. VERIFIED territorial requiere cruce con directorio propio de cuenta/nombre byte-exacto/NIT y código municipal único. Ese cruce no se convierte en identidad lógica: la acreditación de BuyerUnit requiere identidad independiente o autoridad congelada previa.

El emparejamiento usa tokens institucionales idénticos o plantilla estricta de rol + municipio completo con calificadores departamentales. No usa distancia, similitud ni subconjuntos arbitrarios. Los topónimos compuestos pasan por `decide_identity` de M1A.2, sobre spans completos de mayor longitud. La naturaleza jurídica sólo se toma de la fila emparejada. La familia inferida por nombre es PROVISIONAL.

| Vía | Pool perfiles | Pool strata | Territorios pool | Aplicados | VERIFIED | PROBABLE |
|---|---:|---:|---:|---:|---:|---:|
| via_a | 232 | 458 | 176 | 230 | 199 | 31 |
| via_b | 1814 | 2989 | 826 | 1779 | 1153 | 617 |

Cruce propio con directorio en el pool B: **1111/1814 (61.25%)**; con identidad propia SIGEP: **383**; VERIFIED tras el cruce, entre aplicaciones B: **1109**.

Los pools A/B se solapan; sus cifras nunca se suman para construir el universo.

**Contradicción reproducida:** el pool de 1.814 perfiles incluye Personería Municipal de Venecia Antioquia porque descarta la fila fiscal de Asamblea Departamental de Córdoba. El NIT 890980764 tiene ambas jurisdicciones SIGEP: la vía fiscal B se cierra para ese perfil. Una fila propia independiente se evalúa por separado; el conflicto fiscal queda visible. Los códigos fuera del DIVIPOLA municipal vigente también se excluyen de la aplicación, conservando el pool hipotético.

Canario fiscal (presencia de filas propias no equivale a match):

```json
{
  "pool_with_alcaldia_strata": 868,
  "council_personeria_strata": 538,
  "council_personeria_profiles": 273,
  "own_role_row_present_strata": 444,
  "own_entity_matched_profiles": 147,
  "classified_executive": 0
}
```

Los contadores anteriores miden contradicciones observadas y controles de seguridad; no son una tasa de precisión poblacional: las capturas no contienen un oráculo completo de afiliaciones negativas. El matching conservador sacrifica recall y publica la deuda sin forzar PASS por entidad.

## Bogotá y deudas heredadas

M0 contiene 174 strata/92 perfiles con código 11001: 76 strata/39 perfiles por el registro ejecutivo declarado, y 98 strata/53 perfiles por SIGEP. La orden exige exclusivamente la primera autoridad. M2A conserva los 92 como censo histórico y sólo afirma los 39 del registro declarado. Otros perfiles con señales de Bogotá se muestran como contexto, sin membresía ni reinterpretación del modelo productivo. Esta restricción impide comparar directamente el nuevo denominador con los 622 reconocidos del baseline sin consultar el ledger de cobertura anterior. Los bytes del modelo, bindings y grupos no cambian.

Ocaña mantiene la deuda de NIT SIGEP; Calarcá mantiene 63002 frente a 63130. Se reutilizan las adjudicaciones M1A.2A y se conservan las contradicciones; esta auditoría no las resuelve.

## Reconciliación del residuo acreditado anterior

**2.513 + 13.272 = 15.785 procesos únicos**, 138 perfiles/96 territorios. **1.711 + 10.570 = 12.281 filas de contratos**. El primer sumando corresponde a 39 perfiles/36 territorios residuales; el segundo a 99 perfiles en 60 territorios ya reconocidos. El ledger en coverage.json enumera cada perfil y explica el segundo sumando por familia, sin atribuir toda la actividad territorial a su ejecutivo.

| Familia del residuo en territorios reconocidos | Perfiles | Procesos únicos | Contratos físicos |
|---|---:|---:|---:|
| CONCEJO | 4 | 62 | 58 |
| CONTRALORIA | 15 | 497 | 480 |
| EDUCATION | 1 | 8 | 0 |
| ESE_HEALTH | 1 | 360 | 1 |
| FUND | 1 | 125 | 114 |
| INSTITUTE | 21 | 4959 | 4860 |
| OTHER_DECENTRALIZED | 16 | 3070 | 2928 |
| PERSONERIA | 8 | 76 | 73 |
| PUBLIC_COMPANY | 26 | 2623 | 815 |
| PUBLIC_SERVICES | 1 | 111 | 0 |
| UNKNOWN | 5 | 1381 | 1241 |

## Delta M1B y familias

Se consumen por referencia los 288 perfiles y 262 señales sólo de registro de M1B; no se vuelve a ejecutar su descubrimiento. Las 39 filas residuales conservan deuda y disposición. El único cambio taxonómico permitido separa EDUCATION de OTHER_DECENTRALIZED, explicitado por perfil en family_mapping_m1b.json. Las asociaciones municipales permanecen OTHER_DECENTRALIZED.

| Familia | Unidades lógicas verificadas | Perfiles con afiliación verificada | Pool no resuelto |
|---|---:|---:|---:|
| MUNICIPAL_EXECUTIVE | 611 | 633 | 224 |
| CONCEJO | 124 | 190 | 260 |
| PERSONERIA | 121 | 169 | 260 |
| CONTRALORIA | 21 | 23 | 58 |
| ESE_HEALTH | 8 | 423 | 817 |
| INSTITUTE | 28 | 66 | 290 |
| PUBLIC_COMPANY | 27 | 103 | 246 |
| FUND | 6 | 14 | 72 |
| FIRE_DEPARTMENT | 1 | 1 | 1 |
| PUBLIC_SERVICES | 1 | 67 | 173 |
| EDUCATION | 3 | 5 | 5439 |
| OTHER_DECENTRALIZED | 20 | 20 | 14 |
| UNKNOWN | 23 | 215 | 2314 |

## Controles territoriales

| DIVIPOLA | Municipio | Familias lógicas observadas | Reconocidas | Sin reconocer | Contexto pendiente |
|---|---|---|---:|---:|---:|
| 05036 | ANGELÓPOLIS | MUNICIPAL_EXECUTIVE, PERSONERIA | 0 | 2 | 2 |
| 05142 | CARACOLÍ | CONCEJO, PERSONERIA | 0 | 2 | 4 |
| 08001 | BARRANQUILLA | CONTRALORIA, UNKNOWN | 1 | 1 | 128 |
| 08078 | BARANOA | MUNICIPAL_EXECUTIVE | 1 | 0 | 7 |
| 11001 | BOGOTÁ, D.C. | FIRE_DEPARTMENT, FUND, MUNICIPAL_EXECUTIVE, UNKNOWN | 38 | 0 | 34 |
| 13836 | TURBACO | MUNICIPAL_EXECUTIVE | 1 | 0 | 4 |
| 52573 | PUERRES | ESE_HEALTH, MUNICIPAL_EXECUTIVE | 0 | 2 | 2 |
| 54498 | OCAÑA | MUNICIPAL_EXECUTIVE | 1 | 0 | 15 |
| 63130 | CALARCÁ | MUNICIPAL_EXECUTIVE | 1 | 0 | 7 |
| 68001 | BUCARAMANGA | CONTRALORIA, MUNICIPAL_EXECUTIVE, OTHER_DECENTRALIZED | 1 | 4 | 66 |
| 68081 | BARRANCABERMEJA | MUNICIPAL_EXECUTIVE, OTHER_DECENTRALIZED, PERSONERIA | 1 | 2 | 23 |
| 76892 | YUMBO | CONTRALORIA, INSTITUTE, MUNICIPAL_EXECUTIVE | 1 | 2 | 18 |
| 85279 | RECETOR | MUNICIPAL_EXECUTIVE | 0 | 1 | 1 |

Baranoa es el TerritoryProfile 08078; su ejecutivo es un elemento de executive[]. Cualquier otra unidad acreditada observada tiene identidad propia. El territorio no se reemplaza por la Alcaldía.

Se publican los 1.103 TerritoryProfiles. Sus perfiles pendientes son referencias de contexto y pueden aparecer en varios territorios candidatos; no son un denominador sumable ni membresía afirmada.

## Acciones futuras y reproducibilidad

Los rankings son deterministas: métrica descendente, DIVIPOLA ascendente, ID lógico estable. La complejidad usa número de familias y luego número de unidades. No se usa valor monetario. READY_FOR_REGISTRY indica preparación de una ficha de conocimiento; no autoriza promoción. READY_FOR_BUYER_PROMOTION requiere adjudicación/pipeline posterior y permanece vacío. Los candidatos M1B retienen su disposición por referencia.

Los artefactos cubren perfiles, unidades, cadenas de afiliación, vías, familias, mapeo M1B, territorios, cobertura, pendientes, rankings, hashes y gates. No hay nuevas fuentes de red.

```bash
.venv/bin/python3 scripts/audits/audit_municipal_m2a.py
.venv/bin/python3 scripts/audits/audit_municipal_m2a.py --output /tmp/municipal_m2a_replay
```

**LLM_EXECUTION_MODE: DETERMINISTIC_OFFLINE_NO_LLM.** El ejecutor bloquea conexiones de socket antes de leer insumos. Los archivos productivos y autoridades históricas llevan SHA-256 antes/después en byte_identity.json. La validación se adjunta en validation.json; si algún byte protegido cambia la suite completa es obligatoria.

**ADR_REQUIRED: YES.** Las cadenas aceptadas constituyen una autoridad reusable para auditoría offline, con límites de identidad y Bogotá explícitos. El ADR se escribe después de contrastar el experimento y verificar el máximo global de los worktrees. Véase [ADR-075](../decisions/2026-09-04_ADR-075_municipal-territorial-affiliation-evidence-chains.md) y el barrido retenido en adr_scan.json. **NEXT: M2B_ADJUDICATION.**

## Incertidumbre de volumen

Procesos municipales: cota inferior 703,120; superior 1,676,820. Contratos únicos derivados: 465,445 a 1,033,118.

La cota superior aumenta frente a M0 porque M2A deja de usar el orden registral como autoridad para excluir perfiles nacionales y aplica la restricción de Bogotá. No se atribuye todo ese pool al universo municipal. La cota inferior incorpora sólo afiliaciones verificadas, incluyendo las que todavía requieren acreditar identidad lógica.

El principal trabajo de M2B será adjudicar identidad propia de salud y otros descentralizados: un código territorial concordante no resuelve nombres oficiales abreviados ni múltiples cuentas. No se relaja el matching para aumentar ese denominador.

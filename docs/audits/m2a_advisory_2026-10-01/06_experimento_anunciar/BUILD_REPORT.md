# BUILD_REPORT: experimento H1 (anunciar en vez de aclarar), preparación offline

Sin texto de consultas: sólo ids y conteos. Generado por `build_announce_cases.py`.

## 1. Reproducción de la selección E3

- Registros seleccionados: **58** (cifra de HADES: 58).
- El script original de E3 no está guardado; lo reconstruí con `gap_matrix.evaluate` (Gold sellado `sealed_v1/gold_v1.jsonl`, capturas Fase A `captures.jsonl`) y estos criterios, que son mi interpretación de la definición:
  1. población = SEARCH + EXECUTE + no AMBIGUOUS = 446 (coincide);
  2. Foundation no ejecuta (`final_status != READY`) = 301 (coincide con E1);
  3. toda fila con fallo intrínseco es «conocida por el host»: `F_PROPOSED_HOST_UNRESOLVED`, `HOST_RESOLVED_PROJECTION_UNSUPPORTED`, o proxy/propuesta como UNSUPPORTED (`F_PROXY:UNSUPPORTED`, `PROPOSED_AS:UNSUPPORTED`), y ninguna es ancla dura (BUYER/ACTOR/TOPIC/universo);
  4. sin condición espuria de Foundation de dimensión distinta de UNSUPPORTED (el host no sabe que falló).

Embudo:

| paso | n |
|---|---:|
| population | 446 |
| foundation_not_executing | 301 |
| all_failures_known_and_not_hard_anchor | 59 |
| dropped_spurious_non_UNSUPPORTED_foundation_condition | 1 |
| no_intrinsic_failure(gate_only) | 6 |

- Diferencia: el total coincide (58). El desglose por dimensión **no** coincide exactamente con el de HADES (GEO 18/STATE 15/VALUE 6/ORDER 5/TIME 5/OR-NOT 3/MODALITY 3 en HADES; abajo mis cifras por registro). Causa probable: HADES cuenta por dimensión «principal» o con otro emparejamiento de proxies; no puedo distinguirlo sin su script. Sin la regla 4 salen 59; sin proxies UNSUPPORTED salen 53. La selección es defendible, no idéntica verificada.

## 2. Distribución por dimensión no aplicada

Por registro (un registro cuenta una vez por dimensión):

| dimensión | n |
|---|---:|
| GEOGRAPHY | 20 |
| STATE | 16 |
| ORDER | 6 |
| VALUE | 6 |
| TIME | 5 |
| MODALITY | 5 |
| LOGIC_OR_NOT | 3 |

Por condición no aplicada (un grupo OR de estados o de temas cuenta como una condición):

| dimensión/nivel | n |
|---|---:|
| STATE | 16 |
| GEOGRAPHY/MUNICIPALITY | 16 |
| ORDER | 6 |
| VALUE | 6 |
| TIME | 5 |
| MODALITY | 5 |
| LOGIC_OR_NOT | 3 |
| GEOGRAPHY/REGION | 3 |
| GEOGRAPHY/DEPARTMENT | 1 |

Condiciones no aplicadas en total: 61; registros con 1 condición: 55, con 2: 3, con más: 0.

## 3. Envolvente: UNIQUE / MULTIPLE / NONE

Por condición:

| tipo | n |
|---|---:|
| UNIQUE | 34 |
| MULTIPLE | 6 |
| NONE | 21 |

Por registro (UNIQUE sólo si todas las condiciones lo son; NONE si alguna es NONE):

| tipo | n |
|---|---:|
| UNIQUE | 31 |
| MULTIPLE | 6 |
| NONE | 21 |

Tipo por dimensión (condiciones):

| dimensión | UNIQUE | MULTIPLE | NONE |
|---|---:|---:|---:|
| GEOGRAPHY/DEPARTMENT | 0 | 0 | 1 |
| GEOGRAPHY/MUNICIPALITY | 13 | 3 | 0 |
| GEOGRAPHY/REGION | 2 | 1 | 0 |
| LOGIC_OR_NOT | 0 | 0 | 3 |
| MODALITY | 5 | 0 | 0 |
| ORDER | 0 | 0 | 6 |
| STATE | 11 | 1 | 4 |
| TIME | 2 | 0 | 3 |
| VALUE | 1 | 1 | 4 |

## 4. Qué tan ancho es el envolvente (dato adicional, no pedido)

`exactness` por condición: LOSSLESS = el envolvente equivale a lo pedido con datos del repo (no hay ensanche, el hueco es de autoridad del host); WIDER = contiene lo pedido y más; UNPROVEN = candidato del repo sin contención probada; DROP = se quita la condición; LOSSLESS_DROP = quitarla equivale a lo pedido (lo implica el universo o el orden por defecto).

| exactness (condición) | n |
|---|---:|
| WIDER | 21 |
| LOSSLESS | 17 |
| DROP | 17 |
| LOSSLESS_DROP | 4 |
| UNPROVEN | 2 |

| dimensión | LOSSLESS | WIDER | UNPROVEN | DROP | LOSSLESS_DROP |
|---|---:|---:|---:|---:|---:|
| GEOGRAPHY/DEPARTMENT | 0 | 0 | 0 | 1 | 0 |
| GEOGRAPHY/MUNICIPALITY | 0 | 16 | 0 | 0 | 0 |
| GEOGRAPHY/REGION | 3 | 0 | 0 | 0 | 0 |
| LOGIC_OR_NOT | 0 | 0 | 0 | 3 | 0 |
| MODALITY | 3 | 2 | 0 | 0 | 0 |
| ORDER | 0 | 0 | 0 | 5 | 1 |
| STATE | 10 | 1 | 1 | 1 | 3 |
| TIME | 0 | 2 | 0 | 3 | 0 |
| VALUE | 1 | 0 | 1 | 4 | 0 |

Por registro: NO_WIDENING=19, DROPS_A_CONDITION=17, WIDENING=22. NO_WIDENING = ninguna condición se ensancha: el registro sólo carece de autoridad en el host.

- Registros UNIQUE con ensanche real (todas WIDER o LOSSLESS, alguna WIDER): **16**.
- Registros UNIQUE sin ningún ensanche (todas LOSSLESS): **15**.
- Registros sin ensanche alguno (UNIQUE LOSSLESS + todo LOSSLESS_DROP/LOSSLESS): **19**.

## 5. Primer criterio de muerte (¿al menos 30 de 58 con envolvente único?)

- Registros con envolvente **UNIQUE**: **31** de 58. Umbral: 30. **NO se cumple: H1 sobrevive a este criterio** (margen +1).

- Sensibilidad (un nombre coloquial de municipio con dos homónimos DIVIPOLA, tratado como el municipio principal en lugar de ambiguo): 3 registro(s) pasarían de MULTIPLE a UNIQUE (R0259, R0327, R0339); UNIQUE sería 34.

- Lectura estricta: de los 31 UNIQUE, sólo 16 muestran algo más ancho que lo pedido; 15 son equivalentes (no requieren aviso de ensanche: son una autoridad que falta). Con la regla literal del encargo cuentan como UNIQUE.

- Fragilidad del margen: UNIQUE depende de supuestos no verificados para estos registros:
  - región con varios departamentos (host no verificado): R0237, R0239
  - año inferido / ventana móvil (ya hospedado en m1-int-par, reloj de hoy): R0228, R0540
  - vista OPPORTUNITIES con estado de evaluación (tensión Gold/vista): R0298
  - Si se excluyen todos (5), UNIQUE = 26.

- El segundo criterio (más de un tercio con condición decisiva) lo deciden los jueces; no se evalúa aquí.

Ids por tipo de registro:

- UNIQUE: R0006, R0050, R0082, R0090, R0102, R0151, R0152, R0153, R0154, R0163, R0174, R0178, R0190, R0217, R0225, R0228, R0237, R0239, R0244, R0258, R0260, R0298, R0302, R0353, R0354, R0429, R0462, R0495, R0535, R0540, S013
- MULTIPLE: R0176, R0259, R0305, R0327, R0339, R0352
- NONE: R0009, R0028, R0067, R0084, R0110, R0114, R0117, R0124, R0144, R0175, R0177, R0218, R0219, R0230, R0297, R0368, R0370, R0444, R0517, R0578, R0580

## 6. Hallazgos al construir (hechos, con su fuente)

- Varias condiciones «no aplicadas» ya tienen resolución exacta en datos del repo (estados con `resolve_estado`, SMMLV con `SMMLV_COP_BY_YEAR`, familia selección abreviada del catálogo, un departamento de `DEPARTAMENTOS`, una región de `ZONES_BY_NAME`): la causa es autoridad no cableada en el host de la captura, no ausencia de dato.
- La autoridad temporal del worktree `secoppal-m1-int-par` ya hospeda mes sin año (año más reciente ≤ hoy) y ventanas móviles; en la captura v4.2 no estaban. Esos casos (TIME/during_month, relative_period) pueden ya no ser casos de aviso.
- `resolve_estado('adjudicados','contratos')` devuelve la familia `en_evaluacion` de contratos (estados pre-firma: Borrador, enviado Proveedor, En aprobación, Aprobado). No contiene «contrato adjudicado»; no se usó como envolvente. Posible defecto del léxico, no investigado.
- Un nombre de municipio coloquial (3 registros) no es alias DIVIPOLA exacto y es prefijo de dos municipios de departamentos distintos: por eso MULTIPLE.

## 7. No verificado

- No hay conteos de filas (sin red ni warehouse): el tamaño real de cada superconjunto es «no medible»; sólo hay factores de calendario o de nº de municipios.
- No ejecuté el host: «aplicable hoy» sale de las filas de `gap_matrix` sobre la captura v4.2 (producer congelado), no de una corrida actual.
- Que el host acepte un filtro con varios departamentos (regiones) o varias familias no está verificado.
- Que el usuario lea el aviso no está medido (no hay telemetría).
- La exactitud de la reconstrucción de E3 (ver §1) y el criterio de «conocido por el host» son interpretación mía.
- El reloj de los envolventes temporales es 2026-10-01 (fecha del experimento), no la fecha original de cada consulta REAL.
- Los textos de aviso son propuestas mías, no el copy de producto.
- La lectura de «qué intención hay tras el estado» (hint del Gold -> literal) es manual y está en `env_state`; los jueces deberían poder discrepar.


# NATIONAL-CORE-COVERAGE-R2B — núcleo nacional de la Rama Ejecutiva

**Tipo:** discovery registry-first + reconciliación de gazetteer
**Fecha:** 2026-09-01 · **Rama:** `main`
**HEAD antes:** `2791fa9cf80721e13f6204390c4a876a947da2b7`
**Evidencia congelada antes de mutar:**
[`national_core_r2b_candidates.json`](../../artifacts/audits/national_core_r2b_candidates.json)

---

## 1. Resultado de discovery

Las nueve deudas de R0 tienen evidencia suficiente para corregirse. Cinco son
identidades físicas ausentes del gazetteer y cuatro son identidades ya
presentes con un hueco de alias. Ningún caso exige inventar un comprador,
agrupar unidades ni cambiar runtime.

```text
ACCEPT_REGISTRY_VERIFIED      5  Relaciones, Defensa, MinTIC, DAFP, DNI
ACCEPT_ALIAS_RECONCILIATION   4  MinCIT, MinAmbiente, MinVivienda, Cultura
BLOCKED                       0
SOURCE_NAME_SPLIT             0
ENTITIES_TO_ADD               5
```

El baseline reproducido contra el HEAD actual es más severo que R0. Las cinco
identidades ausentes no siempre fallaban en blanco: el fuzzy directo las
entregaba a otros compradores. La matriz pre-R2B queda así:

| Universo | PASS | PARTIAL | MISSING | WRONG_OWNER |
|---|---:|---:|---:|---:|
| Ministerios activos (18) | 11 | 4 | 0 | 3 |
| Departamentos administrativos (6) | 4 | 0 | 0 | 2 |

`EXACT_MULTI_OWNER_GROUPS=173`; `NEW_MULTI_OWNER_KEYS=0`.

## 2. Autoridad del universo

El universo vigente sale del [Manual de Estructura del Estado — Rama Ejecutiva,
orden nacional](https://www.funcionpublica.gov.co/gestor-normativo/manual-estructura-del-estado/rama-ejecutiva-orden-nacional):
18 ministerios activos y 6 departamentos administrativos. El Ministerio de
Igualdad y Equidad permanece fuera de los 18 como
`HISTORICAL_IN_LIQUIDATION`; no se elimina su evidencia histórica.

El Manual sólo define el universo. No confiere identidad de compra ni genera
aliases. Para eso la autoridad es, en orden:

1. [Procesos SECOP II `p6dx-8zbt`](https://www.datos.gov.co/resource/p6dx-8zbt.json).
2. [Contratos SECOP II `jbjy-vk9h`](https://www.datos.gov.co/resource/jbjy-vk9h.json).
3. NIT/documento.
4. Nombre físico observado en cada dataset.

## 3. Evidencia SECOP e identidad

| Caso | NIT | Nombre físico en procesos y contratos | Proc. | Contr. | Clasificación | Decisión |
|---|---|---|---:|---:|---|---|
| Relaciones Exteriores | `899999042` | `MINISTERIO DE RELACIONES EXTERIORES` | 84 | 71 | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` |
| Defensa Nacional | `899999003` | `MINISTERIO DE DEFENSA NACIONAL` | 3 592 | 2 586 | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` |
| MinTIC | `899999053` | `MINISTERIO DE TECNOLOGÍAS… DE COLOMBIA.` | 359 | 314 | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` |
| MinCIT | `830115297` | `MINISTERIO DE COMERCIO… - MINCIT` | 4 319 | 3 641 | `IDENTITY_PRESENT_ALIAS_GAP` | `ACCEPT_ALIAS_RECONCILIATION` |
| MinAmbiente | `830115395` | `MINISTERIO DE AMBIENTE Y DESARROLLO SOSTENIBLE` | 10 506 | 10 159 | `IDENTITY_PRESENT_ALIAS_GAP` | `ACCEPT_ALIAS_RECONCILIATION` |
| MinVivienda | `900463725` | `MINVIVIENDA` | 10 661 | 10 591 | `IDENTITY_PRESENT_ALIAS_GAP` | `ACCEPT_ALIAS_RECONCILIATION` |
| Cultura | `830034348` | `Ministerio de las Culturas, las Artes y los Saberes` | 28 284 | 11 860 | `IDENTITY_PRESENT_ALIAS_GAP` | `ACCEPT_ALIAS_RECONCILIATION` |
| DAFP | `899999020` | `DEPARTAMENTO ADMINISTRATIVO DE LA FUNCIÓN PÚBLICA ENTIDAD` | 2 764 | 2 391 | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` |
| DNI | `900475460` | `DIRECCION NACIONAL DE INTELIGENCIA` | 742 | 408 | `IDENTITY_MISSING` | `ACCEPT_REGISTRY_VERIFIED` |

En los nueve casos hay un solo nombre físico por NIT y el mismo nombre funciona
en ambos datasets. No aparece `SOURCE_NAME_SPLIT`,
`MULTIPLE_PHYSICAL_SAME_IDENTITY` ni `ROOT_IDENTITY_FRAGMENTED`.

## 4. Separaciones que el lote preserva

### Relaciones Exteriores

El Ministerio (`899999042`) y el Fondo Rotatorio (`8605110716`) son compradores
distintos. La propia [página de transparencia de Cancillería](https://cancilleria.gov.co/transparencia-y-acceso-informacion-publica)
publica ambos NIT por separado. El sitio oficial usa “Cancillería” para el
Ministerio, de modo que el alias tiene autoridad institucional; no se transfiere
ninguna frase del Fondo al Ministerio.

El registro físico del Fondo ya existe como `FONDO ROTATORIO**`. Se reconcilió
la frase completa del Fondo con ese registro para que el nuevo alias largo del
Ministerio nunca capture al comprador distinto por substring.

### Defensa Nacional

La raíz sí existe físicamente en SECOP II. No se crea grupo Defensa ni se
fusionan Ejército, Armada, Fuerza Aeroespacial, Policía, Agencia Logística,
Hospital Militar, fondos, cajas o unidades sectoriales.

### MinTIC

El Ministerio (`899999053`) y el Fondo Único de TIC (`8001316486`) son
identidades distintas. La existencia del Fondo no se usó como evidencia del
Ministerio; cada una tiene su propio nombre físico y NIT.

### Cultura

SECOP muestra una sola identidad: `Ministerio de las Culturas, las Artes y los
Saberes`, NIT `830034348`, en ambos datasets. “Ministerio de Cultura” es alias
histórico/humano del registro existente. No se crea duplicado.

### DAFP y DNI

DAFP compra con el literal observado terminado en `ENTIDAD`. El registro
homónimo de Cundinamarca tiene otro NIT (`899999114`) y no se fusiona. El
[Manual de Función Pública](https://www.funcionpublica.gov.co/eva/gestornormativo/manual-estado/estructura-estado.php?id=507)
respalda las formas institucionales DAFP y Departamento Administrativo de la
Función Pública.

La DNI compra con NIT `900475460`. El [Decreto 4179 de
2011](https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=44666)
crea el departamento administrativo que se denomina Dirección Nacional de
Inteligencia. La sigla desnuda `DNI` no se incluyó: es polisémica y los
canarios adversariales exigen que no capture intenciones documentales.

## 5. Política de aliases

El lote usa formas institucionales de alta precisión y no genera variantes que
`normalize_text` ya colapsa. No entran aliases genéricos como `ministerio`,
`defensa`, `cultura`, `ambiente` o `departamento administrativo`.

Las frases predictivas quedaron escritas en
[`test_national_core_r2b.py`](../../tests/test_national_core_r2b.py) antes de
modificar `aliases_db.json`. En el baseline fallaron exactamente por las deudas
descritas; los canarios adversariales de `DNI` ya pasaban.

## 6. Post-remediación

Las nueve deudas quedan cerradas sin cambiar runtime. Las cinco identidades
nuevas usan literalmente el nombre físico observado; las cuatro existentes se
reconciliaron sin duplicarlas.

| Métrica | Antes | Después |
|---|---:|---:|
| Entidades | 562 | **567** |
| Aliases crudos | 10 718 | **10 749** |
| Aliases normalizados únicos | 8 577 | **8 608** |
| `EXACT_MULTI_OWNER_GROUPS` | 173 | **173** |
| `NEW_MULTI_OWNER_KEYS` | 0 | **0** |
| Remociones propuestas por auditor R1C | 0 | **0** |

Se añadieron 31 aliases: 23 en las cinco entidades nuevas, 7 para reconciliar
las cuatro parciales y 1 defensivo en el Fondo Rotatorio. Los aliases mínimos
que cierran deuda son:

| Identidad | Aliases nuevos |
|---|---|
| Relaciones Exteriores | `ministerio de relaciones exteriores`; `cancillería`; `cancillería de colombia`; `ministerio de exteriores`; `ministerio de relaciones exteriores de colombia` |
| Defensa Nacional | `ministerio de defensa nacional`; `ministerio de defensa`; `mindefensa`; `mindefensa colombia` |
| MinTIC | nombre institucional; nombre físico con `de Colombia`; `mintic`; `ministerio tic`; `ministerio de tic`; `ministerio de las tic` |
| DAFP | nombre institucional; `función pública`; `dafp`; `función pública de colombia`; `departamento de la función pública` |
| DNI | nombre institucional y dos formas largas con `departamento administrativo`; **no** `DNI` |
| MinCIT | `mincomercio` |
| MinAmbiente | `minambiente`; `ministerio de ambiente` |
| MinVivienda | nombre institucional; `ministerio de vivienda` |
| Cultura | `ministerio de cultura`; `mincultura` |
| Fondo Rotatorio | `fondo rotatorio del ministerio de relaciones exteriores` |

El auditor R1C pasa sin remociones. Sus dos `MANUAL_REVIEW` adicionales son las
formas humanas de `MINVIVIENDA`: el nombre físico opaco no contiene la palabra
“ministerio”, pero la reconciliación por NIT `900463725` está congelada en el
artefacto de discovery. No son colisiones ni ownership ajeno.

### Matriz nacional

| Universo | PASS | PARTIAL | MISSING | WRONG_OWNER |
|---|---:|---:|---:|---:|
| Ministerios activos (18) | **18** | **0** | **0** | **0** |
| Departamentos administrativos (6) | **6** | **0** | **0** | **0** |

El Ministerio de Igualdad y Equidad sigue fuera de la primera fila como
`HISTORICAL_IN_LIQUIDATION`.

### Dataset smoke

Cada identidad nueva recorrió la cadena
`frase humana → resolver exacto → nombre físico → filas SECOP`:

| Comprador resuelto | Procesos | Contratos |
|---|---:|---:|
| `MINISTERIO DE RELACIONES EXTERIORES` | 84 | 71 |
| `MINISTERIO DE DEFENSA NACIONAL` | 3 592 | 2 586 |
| `MINISTERIO DE TECNOLOGÍAS… DE COLOMBIA.` | 359 | 314 |
| `DEPARTAMENTO ADMINISTRATIVO DE LA FUNCIÓN PÚBLICA ENTIDAD` | 2 764 | 2 391 |
| `DIRECCION NACIONAL DE INTELIGENCIA` | 742 | 408 |

`ZERO_ROW_ANOMALIES=0`. Procesos y contratos usan el mismo nombre físico en
los cinco casos.

### Validación

```text
tests/test_national_core_r2b.py                         66 passed
R2B + R2A.1 + R2A + R1D + R1C + manager/resolver/router
                                                        373 passed
scripts/run_query_corpus.py --mode all                  89/89 PASS
scripts/gazetteer_manager.py validate                   0 errores
audit_gazetteer_alias_ownership.py                      0 remociones
ruff check app/ tests/ scripts/                         All checks passed
make lint-core                                          app/core/ limpio
suite completa                                          9993 passed,
                                                        2 skipped, 24 xfailed
```

Los xfails permanecen en 24: aumento 0.

## 7. Fuera de alcance

No se tocan agencias, superintendencias, CAR, Rama Judicial, Congreso,
organismos de control, universidades, alcaldías, municipios, ecosistema de
Fuerza Pública, Cundinamarca/Valle como grupos, Guainía, D6,
`UNGATED_FUZZY_HEALTH_EDU`, `SAME_LOGICAL_IDENTITY`, SoQL, SODA3, Radar ni Actor
Profile. No se crea ADR porque discovery no encontró una limitación nueva del
modelo.

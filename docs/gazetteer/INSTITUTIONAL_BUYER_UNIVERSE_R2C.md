# INSTITUTIONAL-BUYER-UNIVERSE-R2C — mapa de expansión del gazetteer

**Tipo:** discovery / auditoría read-only · **Fecha:** 2026-09-01

**Rama:** `main` · **HEAD:** `affac04d61619b3a60c6d801ab10f91bf7931964`

**Evidencia:** [`institutional_buyer_universe_r2c.json`](../../artifacts/audits/institutional_buyer_universe_r2c.json)

**Reproductor:** [`audit_institutional_buyer_universe.py`](../../scripts/audits/audit_institutional_buyer_universe.py)

No se modificó `aliases_db.json`, runtime, `EntityResolver`, `QueryRouter` ni
`entity_types`.

## 1. Decisión

```text
NEXT_RECOMMENDED: R2D_CAR_CORE
SECOND_PRIORITY:  R2E_CONTROL_NATIONAL
THIRD_PRIORITY:   R2F_PUBLIC_UNIVERSITY_PILOT
```

CAR/CDS gana por la combinación, no por el tamaño aislado:

- universo oficial acotado de **33** compradores;
- sólo **11/33 PASS_EXACT** y **22/33 WRONG_OWNER** en el banco conservador de
  consultas naturales;
- **26.645 procesos y 24.213 contratos** publicados/firmados en los últimos 12
  meses;
- las 33 identidades tienen evidencia SECOP II y, en esta fotografía, un solo
  nombre físico por NIT;
- el hueco es mayoritariamente reconciliación de identidades simples, no diseño
  de un buyer group ni agregación de unidades.

El segundo lote debe ser organismos de control: es pequeño (6), verificable,
presenta 4 resoluciones inseguras y conserva identidades separadas. Las
universidades quedan terceras pese a su volumen: mezclan un universo oficial
nacional de 16 con 34 nodos públicos SUE/SECOP, tienen más splits físicos y casi
todo su volumen visible está en procesos, no en contratos.

## 2. Método y lectura correcta de los números

Autoridad estructural: [Manual de Estructura del Estado de Función
Pública](https://www.funcionpublica.gov.co/eva/gestornormativo/manual-estado/index.php).
Autoridad de identidad de compra: [procesos SECOP II
`p6dx-8zbt`](https://www.datos.gov.co/resource/p6dx-8zbt.json) y [contratos
SECOP II `jbjy-vk9h`](https://www.datos.gov.co/resource/jbjy-vk9h.json), en ese
orden, reconciliados por NIT y nombre físico.

La medida reciente cubre `2025-09-01` a `2026-09-01`. En procesos usa
`fecha_de_publicacion_del`; en contratos, `fecha_de_firma`. Los conteos se
mantienen separados. `UNKNOWN` significa que la búsqueda no produjo una
identidad/NIT suficientemente confiable; no se reemplaza por cero.

La clasificación es deliberadamente conservadora:

- `PASS_EXACT`: la consulta está respaldada por ownership exacto y llega al
  nombre físico correcto.
- `FUZZY_ONLY`: llega al comprador correcto sólo por fuzzy; no es cobertura
  segura.
- `WRONG_OWNER`: alguna consulta natural del caso llega a otro comprador. Si
  otra variante sí funciona, el caso sigue contando inseguro.
- `MISSING`: hay identidad SECOP II pero el resolver falla cerrado.
- `NO_SECOP_II_EVIDENCE`: la sonda registral no acredita comprador físico.
- `SOURCE_NAME_SPLIT` y `FRAGMENTED_BUYER` son ejes de identidad adicionales,
  no excusas para fusionar entidades.

El conteo `NO SECOP` de la matriz es un eje de evidencia: puede coexistir con
`WRONG_OWNER`, porque una alta corte sin comprador físico independiente puede
ser, al mismo tiempo, enviada por fuzzy a una entidad ajena. Esto preserva la
lección R2B: `MISSING != WRONG_OWNER`.

## 3. Matriz ejecutiva

Ordenada por recomendación. `Recent P/C` = procesos / contratos de 12 meses.

| Grupo | Universo | Pass | Partial | Missing | Wrong | Fuzzy | No SECOP | Recent P | Recent C | Complejidad | Valor | Prioridad |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| CAR/CDS core | 33 | 11 | 0 | 0 | 22 | 0 | 0 | **26.645** | **24.213** | baja-media | alta | **P0** |
| Control nacional | 6 | 2 | 0 | 0 | 4 | 0 | 0 | 3.325 | **10.563** | baja | alta | **P1** |
| Universidades públicas SECOP | 34 | 19 | 0 | 0 | 13 | 2 | 1 | **97.753** | 959 | alta | alta | **P1 / piloto** |
| Descentralizadas — inventario | 19 | 9 | 0 | 0 | 7 | 1 | 7 | 33.999 | 28.075 | alta/no cerrado | alta | DEFER |
| Autoridades ambientales urbanas | 7 | 0 | 0 | 0 | 5 | 2 | 0 | 6.931 | 6.061 | media | media-alta | P2 separado |
| Defensa / Fuerza Pública | 9 | 4 | 0 | 0 | 5 | 0 | 2 | 3.857 | 2.003 | alta | alta | P2 |
| Rama Judicial — raíces | 8 | 1 | 0 | 0 | 7 | 0 | 5 | 3.031 | 1.213 | alta | media | P2 |
| Judicial — seccionales físicas | 21 | 0 | 0 | 0 | 21 | 0 | 1 | 1.995 | 1.490 | alta | media | P2 |
| Organización electoral | 3 | 0 | 0 | 0 | 3 | 0 | 1 | 523 | 434 | media | media | P2 |
| Cormagdalena — separada | 1 | 0 | 0 | 0 | 1 | 0 | 0 | 759 | 667 | baja | media | P2 separado |
| Rama Legislativa | 2 | **2** | 0 | 0 | 0 | 0 | 0 | 5.748 | 5.381 | baja | media | DEFER |
| Banco de la República + CNSC | 2 | **2** | 0 | 0 | 0 | 0 | 0 | 5.044 | 612 | baja | media | DEFER |

`COVERAGE_PRECISION_SAFE` / `UNSAFE_RESOLUTION_RATE` de los tres candidatos:

| Grupo | Precision safe | Unsafe rate |
|---|---:|---:|
| CAR/CDS | 33,33% | **66,67%** |
| Control nacional | 33,33% | **66,67%** |
| Universidades públicas | 55,88% | **44,12%** |

No hay `MISSING` en estos tres grupos porque el fuzzy tiende a fallar abierto.
Ese cero no es una buena noticia: explica el volumen de `WRONG_OWNER`.

## 4. Universo ambiental reconciliado

### 4.1 CAR_CORE_COUNT = 33

MinAmbiente declara [33 CAR y corporaciones de desarrollo
sostenible](https://www.minambiente.gov.co/eleccion-car/): 26 CAR y 7 CDS. La
lista operativa auditada contiene exactamente esas 33.

Función Pública muestra 34 bajo el bloque ambiental porque incluye a la
Corporación Autónoma Regional del Río Grande de la Magdalena. La [Ley 161 de
1994](https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=68533)
la define como ente corporativo especial del orden nacional que opera como
Empresa Industrial y Comercial del Estado.

```text
CAR_CORE_COUNT = 33
CORMAGDALENA_CLASSIFICATION = SPECIAL_NATIONAL_CORPORATE_ENTITY_EICE
FINAL_AUDIT_UNIVERSE = 33 CAR/CDS + 1 Cormagdalena separada + 7 AAU
```

Cormagdalena tiene identidad SECOP II única, 759 procesos y 667 contratos
recientes, pero su consulta natural resuelve a dueño ajeno. Debe reconciliarse
separada; no altera la cardinalidad CAR.

### 4.2 Resultado 33/33

| Clasificación | Entidades |
|---|---|
| `PASS_EXACT` (11) | CAR Cundinamarca, CARDER, CAS, CDMB, Cormacarena, Corpoamazonia, Corpoboyacá, Corponariño, CRC, CRQ, CVC |
| `WRONG_OWNER` (22) | CAM, Cardique, Carsucre, CDA, Codechocó, Coralina, Corantioquia, Cornare, Corpamag, Corpocaldas, Corpocesar, Corpochivor, Corpoguajira, Corpoguavio, Corpomojana, Corponor, Corporinoquia, Corpourabá, Cortolima, CRA, CSB, CVS |

Todas tienen evidencia SECOP II y `SINGLE_PHYSICAL_NAME` en la fotografía. El
caso de Corpochivor es especialmente demostrativo: la consulta exacta cae en
Corpoboyacá, por lo que no puede contarse como ausencia.

### 4.3 Autoridades ambientales urbanas = categoría separada

MinAmbiente enumera [7 autoridades ambientales urbanas](https://www.minambiente.gov.co/wp-content/uploads/2023/07/DS-A-SCD-10_V3_copia_controlada.pdf):
SDA Bogotá, DAGMA Cali, AMVA, Barranquilla Verde, EPA Cartagena, DADSA Santa
Marta y EPA Buenaventura.

- SDA y DAGMA: `FUZZY_ONLY` en al menos una forma natural.
- Las otras cinco: `WRONG_OWNER`.
- Volumen reciente conjunto: 6.931 procesos / 6.061 contratos.

No entran en `R2D_CAR_CORE`: comparten función ambiental, no tipo institucional.

## 5. Rama Judicial — las altas cortes no son el centro de compra

El [Manual de la Rama Judicial](https://www.funcionpublica.gov.co/eva/gestornormativo/manual-estado/rama-judicial.php/index.php)
distingue altas cortes, Consejo/Comisión, DEAJ, direcciones seccionales,
Fiscalía e Instituto de Medicina Legal.

Resultado de discovery:

| Comprador lógico | Resultado | Evidencia SECOP II | Recent P/C |
|---|---|---|---:|
| Corte Constitucional | `WRONG_OWNER` | no comprador físico independiente acreditado | UNKNOWN |
| Corte Suprema de Justicia | `WRONG_OWNER` | no acreditado | UNKNOWN |
| Consejo de Estado | `WRONG_OWNER` | no acreditado | UNKNOWN |
| Comisión Nacional de Disciplina Judicial | `WRONG_OWNER` | no acreditado | UNKNOWN |
| Consejo Superior de la Judicatura | `WRONG_OWNER` | no acreditado | UNKNOWN |
| DEAJ | `WRONG_OWNER` | nombre físico único | 479 / 268 |
| Fiscalía General | `WRONG_OWNER` | nombre físico único | 845 / 464 |
| Medicina Legal | `PASS_EXACT` | nombre físico único | 1.707 / 481 |

Se detectaron además 21 direcciones seccionales físicas; 20 tienen evidencia
por nombre/NIT y las 21 consultas naturales resuelven a dueño ajeno. Su volumen
conjunto reciente es 1.995 procesos / 1.490 contratos. La compra judicial está
concentrada en DEAJ/seccionales y entidades operativas, no en registros físicos
independientes de las altas cortes.

Por eso `Rama Judicial` es `BUYER_GROUP_CANDIDATE`, no entidad ni siguiente
lote plano. No se crea buyer group en R2C.

## 6. Organismos de control

El [universo nacional de Función Pública](https://www.funcionpublica.gov.co/eva/gestornormativo/manual-estado/organismos-control.php)
produce 6 compradores auditables, preservando dos separaciones obligatorias.

| Entidad | Estado | Recent P/C |
|---|---|---:|
| Contraloría General — nivel central | `PASS_EXACT` | 360 / 326 |
| Auditoría General | `WRONG_OWNER` | 198 / 147 |
| Procuraduría General | `WRONG_OWNER` | 534 / 364 |
| Defensoría del Pueblo | `PASS_EXACT` | 2.098 / 9.609 |
| Fondo de Bienestar Social de la Contraloría | `WRONG_OWNER` | 113 / 97 |
| Instituto de Estudios del Ministerio Público | `WRONG_OWNER` | 22 / 20 |

El Fondo no se fusiona con la CGR y el IEMP no se fusiona con Procuraduría.
Contralorías territoriales y personerías quedan como `FUTURE_UNIVERSE`.

## 7. Legislativa, electoral y autónomos

La [Rama Legislativa](https://www.funcionpublica.gov.co/eva/gestornormativo/manual-estado/rama-legislativa.php)
es bicameral. SECOP II acredita dos compradores físicos:

- `CONGRESO DE LA REPÚBLICA - HONORABLE SENADO DE LA REPÚBLICA`;
- `CAMARA DE REPRESENTANTES`.

Ambos son `PASS_EXACT`. No aparece un tercer comprador físico agregado
`Congreso de la República`; esa frase es `BUYER_GROUP_CANDIDATE`.

En [organización electoral](https://www.funcionpublica.gov.co/eva/gestornormativo/manual-estado/organizacion-electoral.php),
Registraduría, CNE y Fondo Rotatorio se mantienen separados: 0/3 pass, 3
`WRONG_OWNER`. La sonda no acreditó una raíz física independiente para
Registraduría, mientras CNE y Fondo sí tienen identidades SECOP II.

Banco de la República y CNSC son 2/2 `PASS_EXACT`. No existe deuda que justifique
un lote. No se mezclan con universidades para priorización.

## 8. Universidades públicas

Dos universos, no uno:

```text
UNIVERSITY_OFFICIAL_UNIVERSE = 16
  Entes universitarios autónomos nacionales del Manual de Función Pública.

SECOP_PUBLIC_UNIVERSITY_UNIVERSE = 34
  Nodos públicos SUE/SECOP; UFPS Cúcuta y Ocaña se miden como compradores físicos.
```

El [listado nacional de Función Pública](https://www.funcionpublica.gov.co/eva/gestornormativo/manual-estado/organos-autonomos.php)
no equivale a todas las universidades públicas que contratan. El Ministerio de
Educación reconoce además el conjunto más amplio de [entes universitarios
autónomos](https://www.mineducacion.gov.co/1759/w3-printer-89243.html).

Resultado del universo SECOP: 19 `PASS_EXACT`, 13 `WRONG_OWNER`, 2
`FUZZY_ONLY`; 97.753 procesos recientes pero sólo 959 contratos. Los splits de
nombre y el patrón de publicación justifican un piloto, no un lote de 34.

Canarios pedidos:

| Entidad | Estado |
|---|---|
| Universidad Nacional | `PASS_EXACT` |
| Universidad de Antioquia | `PASS_EXACT` |
| Universidad del Valle | `PASS_EXACT` |
| Universidad del Atlántico | `WRONG_OWNER` (nombre físico `UNIATLANTICO`) |
| Universidad Distrital | `PASS_EXACT` |
| UPTC | `PASS_EXACT` |
| Universidad de Cartagena | `PASS_EXACT` |

Un futuro `R2F` debe seleccionar un piloto registry-first de universidades con
alto volumen, `WRONG_OWNER` y nombre físico simple; no implementar el concepto
agregado “universidades públicas”.

## 9. Defensa / Fuerza Pública

Se midieron nueve raíces/relacionadas sin construir ecosistema:

| Estado | Entidades |
|---|---|
| `PASS_EXACT` | Fuerza Aeroespacial, Agencia Logística, Fondo Rotatorio de la Policía, CREMIL |
| `WRONG_OWNER` | Ejército, Armada, Policía, Hospital Militar, CASUR |

Ejército y Policía representan intención institucional que puede abarcar
múltiples unidades físicas; Agencia Logística, Hospital Militar, fondos y cajas
son compradores independientes. La [estructura sectorial](https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=72035)
también distingue comandos, sanidades y direcciones. `Fuerza Pública` queda
`BUYER_GROUP_CANDIDATE` y el bloque se difiere por complejidad.

## 10. Grandes descentralizadas — inventario, no universo final

La muestra sectorial de 19 produce 9 pass, 7 wrong, 1 fuzzy y 2 sin evidencia
acreditada por la sonda; volumen reciente conocido de 33.999 procesos / 28.075
contratos. Destacan:

- pass: INVÍAS, ANI, SuperTransporte, Prosperidad Social, ICA, ADR, ANT, AUNAP,
  ANM;
- inseguras: ANSV, ANLA, Parques (fuzzy), Aerocivil, UPRA, DIAN;
- SENA/ICBF son ecosistemas preexistentes y no deben convertirse en aliases
  raíz simples.

La muestra demuestra alto valor de producto, pero no autoriza una lista manual
final: debe derivarse del Manual por sector y reconciliarse completa antes de
competir como lote.

## 11. Buyer group candidates

| Consulta natural | Clasificación | Razón |
|---|---|---|
| Rama Judicial | `BUYER_GROUP_CANDIDATE` | DEAJ + seccionales + entidades operativas |
| Congreso de la República | `BUYER_GROUP_CANDIDATE` | Senado y Cámara compran separados |
| Fuerza Pública | `BUYER_GROUP_CANDIDATE` | fuerzas y unidades físicas distintas |
| CAR de la Costa | `BUYER_GROUP_CANDIDATE` | subconjunto geográfico, no comprador |
| universidades públicas | `BUYER_GROUP_CANDIDATE` | 34 nodos físicos auditados |
| Gobernación de Cundinamarca | `BUYER_GROUP_CANDIDATE` | R2A probó raíz fragmentada |

## 12. Límites y siguiente gate

El artefacto JSON conserva, por entidad, `logical_name`, fuente institucional,
NIT cuando fue acreditado, nombres físicos de procesos/contratos, registros y
aliases existentes, método real, owner resuelto, clasificación y cuatro
conteos de volumen.

Las búsquedas registrales se hicieron contra la API pública sin token. Una
ausencia de candidato queda `UNKNOWN/NO_SECOP_II_EVIDENCE`, no prueba jurídica
de inexistencia. Antes de aplicar R2D deben reconfirmarse los 33 NIT y nombres
físicos, congelar el baseline, escribir canarios predictivos y dividir el lote
si la puerta recomendada de 10 entidades de la guía lo exige.

R2C no añade aliases ni cambia el comportamiento del producto.

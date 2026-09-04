# Municipal M1A.2A — adjudicación de los cinco REVIEW

**MUNICIPAL_M1A_2A: PASS. M1A_RETRY_2_READY: YES.**
**PRODUCTION_PROMOTED: 0. PUSH: NO.** Rama `main`; HEAD inicial
`bd60e399a0b73c66fad60299b992729ae7b850fa`.

## Alcance y decisiones

Se adjudican exclusivamente las cinco BuyerUnits pendientes de M1A.2. El guard,
`classify`, `evaluate`, DIVIPOLA, M0 y los resultados históricos de M1A.2 se
conservan byte por byte. No se introduce `UNCHANGED_BASELINE_REVIEW`. La única
mutación productiva es la corrección independiente del duplicado de Turbaco.

| Caso | DIVIPOLA | Cuenta SECOP | NIT físico | Decisión |
|---|---|---|---|---|
| ALCALDIA DE TURBACO BOLIVAR | 13836 | 702768342 | 890481149 | DUPLICATE_BUYER_UNIT; consolidado en ALCALDIA MUNICIPAL DE TURBACO BOLIVAR |
| ALCALDIA MUNICIPAL DE OCAÑA | 54498 | 703478552 | 890501102 | VERIFIED_MUNICIPAL_AFFILIATION |
| ALCALDIA MUNICIPIO DE CALARCA | 63130 | 704291491 | 890000441 | VERIFIED_MUNICIPAL_AFFILIATION |
| ALCALDIA DISTRITAL BARRANCABERMEJA | 68081 | 702270372 | 890201900 | VERIFIED_MUNICIPAL_AFFILIATION |
| MUNICIPIO DE YUMBO VALLE | 76892 | 702384066 | 890399025 | VERIFIED_MUNICIPAL_AFFILIATION |

Cada caso conserva nombres físicos por dataset, cuenta, evidencia fiscal,
fuentes, discrepancias, decisión, razón y tipo de autoridad en
[`adjudication.json`](../../artifacts/audits/municipal_m1a_2a/adjudication.json).
Acreditar afiliación no significa resolver todas las discrepancias fiscales de
un registro ni convertir un NIT en identidad de comprador.

## Turbaco: corrección manual acreditada

El canonical corto existía desde `66e6242`; `7781d6d` añadió el canonical físico
con aliases humanos compartidos. La similitud y sus contadores históricos de
catálogo no se usan como prueba de identidad.

Las consultas SECOP sin ventana temporal observaron **5.247 filas de procesos**
y **4.912 de contratos**, todas del ejecutivo bajo cuenta `702768342`, NIT
`890481149` y nombre exacto `ALCALDIA MUNICIPAL DE TURBACO BOLIVAR`.
Son filas físicas publicadas, no una suma ni una deduplicación de contratos por
NIT. El canonical corto no aparece en la historia consultada, M0 o directorio
actual. M0 ya reconoce la unidad física en DIVIPOLA 13836; SIGEP 1213 acredita
la alcaldía central municipal correspondiente.

Se verificaron además dos cuentas de directorio: `706283421`,
`ALCALDIA MUNICIPAL DE TURBACO 1`, NIT `890481149`; y `708966437`,
`ALCALDiA DE TURBACO 2`, NIT `8904811490`. Las consultas por **ambas cuentas**, en
ambos datasets sin filtro temporal, devolvieron cero filas. No se incorporan a
la BuyerUnit, ni se equipara el NIT terminado en cero. Su clasificación es
registro sin actividad física observada; no se afirma que nunca hayan tenido
actividad en versiones antiguas no publicadas.

Ningún tercer canonical reclama los aliases afectados o los nombres fuente.
Se elimina sólo el registro sintético A y se conserva B con sus metadatos;
los contadores no se suman. Las 27 superficies humanas distintas y el canonical
sobreviviente tienen owner exacto único (**28 comprobaciones**). Desaparecen
cuatro claves multi-owner. Conforme a ADR-072, dos claves normalizadas que
podían capturar las cuentas numeradas pierden elegibilidad de scan, conservando
su lookup exacto. El chequeo de aristas usa las **42.790 superficies** de M0.

ADR-073 se aplica mediante un binding explícito de B al mismo nombre físico
exacto en ambos datasets. Su clave es `BUYER_UNIT`, no NIT. El registro sintético
sobrevive como alias humano; no se proyecta como string SECOP.

**El predicado automático de cinco condiciones de ADR-074 no aplica**: los
nombres difieren por el token `MUNICIPAL`, y no tienen datasets disjuntos.
La instrucción de esta tarea autoriza esta adjudicación manual con evidencia
física. No se amplía el predicado automático ni se añade una regla de similitud.
El script de corrección está limitado a este par concreto, exige la evidencia
congelada y es idempotente; por defecto sólo hace dry run.

## Segunda vía: SECOP_DIRECTORY_EXACT_DIVIPOLA

La cohorte se define antes de inspeccionar concordancia: todas las filas del
directorio con **nombre físico exacto + cuenta** en estratos M0 ya clasificados
`ACCREDITED_MUNICIPAL_AFFILIATION`. No se incluyen los casos pendientes para
hacer pasar su propia validación. Se cuenta una fila del directorio una vez,
aunque aparezca en procesos y contratos.

| Métrica | Filas |
|---|---:|
| ROWS_TESTED | 646 |
| ROWS_WITH_MUNICIPAL_LOCATION_CODE | 616 |
| MATCH | 616 |
| CONTRADICTION | 0 |
| AMBIGUOUS | 0 |
| Sin código municipal exacto | 30 |

El subconjunto de BuyerUnits ya reconocidas aporta 63 coincidencias y 27 filas
sin código; el de ejecutivos, 507 coincidencias y 15 sin código.

**Límite metodológico:** M0 ya utilizaba códigos del directorio para seleccionar
buena parte de su cohorte; el 616/616 tiene sesgo de incorporación y no estima
la precisión del directorio entero. Por eso se añade un contraste que ignora
tanto ese código como los códigos SIGEP seleccionados por M0: vincula evidencia
institucional/fiscal con el **nombre de municipio y departamento publicado por
SIGEP**, y lo resuelve en DIVIPOLA. Resultado: **606 MATCH, 0 CONTRADICTION,
10 AMBIGUOUS** entre las 616 filas con código. Las diez ambigüedades quedan
enumeradas; nunca cuentan como coincidencias. Incluyen nombres alternativos
sin lookup y Barbosa con evidencia SIGEP para dos departamentos.

Política explícita y acotada: al menos 100 filas, concordancia primaria completa,
cero ambigüedades primarias, al menos 95% de corroboración del segundo contraste
y cero contradicciones en ambos. La cohorte cumple; el veredicto es
**ACCEPT_SCOPED_AUTHORITY**. No se declara validez para cualquier comprador ni
para filas futuras sin revalidación.

La función general exige conjuntamente:

1. BuyerUnit existente con nombre físico exacto observado en ambos datasets.
2. Una sola cuenta física y la misma cuenta en el directorio.
3. Nombre de directorio exacto, sin filas contradictorias para esa cuenta.
4. Código `CO-XXX-DDMMM` único y perteneciente al registro municipal DIVIPOLA.
5. Departamento compatible en el catálogo y el directorio.
6. Familia institucional municipal executive.

La función no contiene nombres ni códigos de Ocaña/Calarcá y puede reutilizarse
en una adjudicación posterior de M2. No entra al runtime y no crea BuyerUnits,
aliases, bindings ni grupos desde geografía.

Ocaña y Calarcá cumplen también frente al directorio actual. Se conservan dos
conflictos de SIGEP: Ocaña publica NIT `800099250` frente a `890501102` en SECOP;
Calarcá publica código `63002`, ausente del registro municipal, frente a `63130`
en SECOP. La vía nueva acredita la afiliación de las unidades físicas SECOP;
no declara equivalentes los NIT ni modifica el código SIGEP. Estos casos eran
**pendientes**, no contradicciones ocultas dentro de la cohorte acreditada.

## Barrancabermeja y Yumbo: evidencia oficial adicional

El directorio actual sigue publicando `CO-SAN` y `CO-VAC`, respectivamente, y
ciudad `No Provisto`. La vía de código exacto los rechaza correctamente.

Se intentó SIGEP live: requests falló por cadena de certificados; el curl del
sistema, con validación TLS activa, obtuvo HTTP 200. Se conserva la página del
reporte, **no se presenta esa página como una exportación nueva de entidades**.
Las páginas institucionales consultadas devolvieron HTTP 403 y no se usan como
evidencia afirmativa; los snippets de buscador tampoco son autoridad.

Se recuperó además el [registro oficial de Función Pública de 2018](https://www1.funcionpublica.gov.co/documents/28587425/35001556/6.pdf/fa670b2f-68e3-7206-e7d7-0bd3a7f8b96f?t=1544538435512),
anexo 4 de la quinta convocatoria. Las páginas **5 y 35**, inspeccionadas
visualmente, identifican las alcaldías, su naturaleza jurídica, departamento,
municipio y NIT. El archivo completo, SHA-256 y las filas extraídas quedan
congelados. Esta es corroboración histórica, no prueba de actividad en 2026.

La conciliación manual combina esa versión con la exportación oficial SIGEP
M0 de 2026: entidades **0329 y 0511**, suborden Municipal, nivel Central,
naturaleza Alcaldía, NIT explícito con DV separado y códigos **68081 y 76892**.
La actividad reciente y el directorio actual vinculan cada canonical físico
exacto a su cuenta SECOP. Las variantes de nombre municipal/distrital y
municipio/alcaldía quedan adjudicadas explícitamente entre registros; no se
crean o fusionan cuentas. La autoridad es
`MANUAL_SIGEP_AND_OFFICIAL_ENTITY_REGISTER`, no nombre + NIT + departamento.

## Matriz posterior y validación

| Resultado | Baseline | Candidato | Total |
|---|---:|---:|---:|
| TOTAL | 4.704 | 4.704 | 9.408 |
| CORRECT_BUYER | 264 | 2.132 | 2.396 |
| CLARIFICATION | 4.440 | 2.572 | 7.012 |
| REVIEW | 0 | 0 | 0 |
| WRONG_BUYER | 0 | 0 | 0 |
| WRONG_DIVIPOLA | 0 | 0 | 0 |
| FAIL_CLOSED sin aclaración | 0 | 0 | 0 |

Las únicas diferencias de owner/clasificación respecto a la matriz histórica
son sus **40 REVIEW → CORRECT_BUYER**. Ocho consultas de Turbaco cambian de A a B;
los otros 32 casos conservan comprador y ganan evidencia de afiliación.

La reproducción usa el
mismo `buyer_oracle`, `evaluate` y `classify` de M1A.2, añadiendo sólo evidencia
afirmativa de los cuatro casos acreditados. Turbaco queda cubierto por la
BuyerUnit que M0 ya reconocía. Un owner sin afiliación sigue devolviendo REVIEW.

El candidato se regenera temporalmente desde el baseline corregido mediante
las mismas 503 propuestas. El snapshot histórico de Retry #1 no se sobrescribe:
contiene la deuda A/B anterior y no sería un candidato representativo del nuevo
baseline. Se prueban idempotencia, ownership, scan y recorridos completos.

Los gates del candidato pasan: **503 propuestas temporales**, **1.888 consultas
de aliases**, **3.068 comprobaciones de interacción**, cero owners correctos
perdidos, **0 new wrong owners**, **0 new multi-owner**, **0 new scan cross-owner**
y **NO_NIT_MERGE: PASS**. El barrido de interacción tiene tres consultas menos
que el histórico (3.071): el catálogo ya no contiene el registro duplicado A;
las consultas se derivan del catálogo y de los aliases, no de un conteo fijado.
Los SHA-256 protegidos confirman que M0, el guard, el catálogo territorial,
los resultados históricos de M1A.2 y el snapshot de Retry #1 permanecen intactos.


## Reproducción

```bash
.venv/bin/python3 scripts/audits/adjudicate_municipal_m1a2a.py
.venv/bin/python3 scripts/audits/consolidate_turbaco_m1a2a.py
# --apply sólo para reproducir la corrección manual ya autorizada de Turbaco.
.venv/bin/python3 scripts/audits/verify_municipal_m1a2a.py
.venv/bin/python3 -m pytest tests/test_municipal_m1a2a.py tests/test_turbaco_m1a2a.py -q
.venv/bin/python3 scripts/run_query_corpus.py --mode all
.venv/bin/python3 -m pytest tests/ -q
.venv/bin/ruff check app/ tests/ scripts/
make lint-core
```

Los metadatos de cada captura conservan URL/SoQL, fecha UTC, estado, duración y
SHA-256 cuando hubo respuesta. Las consultas SECOP de historia no tienen filtro
de fecha y están por debajo del límite de 10.000 estratos; no se toma un timeout
como conjunto vacío. Se conserva el timeout inicial de contratos y el reintento
acotado que sí terminó. Todas las respuestas definitivas se reproducen offline.

## Commits y controles de cambio

- `99a5210`: discovery, adjudicación, evidencia congelada y pruebas de autoridad.
- `ee03ab4`: corrección funcional aislada de Turbaco y cuatro regresiones.
- El commit de cierre conserva matriz, gates, informe y logs finales.

Pruebas predictivas: 13 pases de autoridad y cuatro fallos que reproducían el
canonical sintético de Turbaco. Después de la corrección y las regresiones de scan: 19/19 pruebas nuevas
y **277 focales**. Corpus **89/89**, Ruff y `make lint-core`: PASS.
La primera suite registró **11.029 pases, un fallo, 2 skipped y 14 xfailed**.
El fallo era el contrato anterior de `scan_entity("alcaldia de turbaco")` en
Lote 01. Ese alias ahora conserva lookup exacto y pierde scan por la cuenta
`ALCALDiA DE TURBACO 2`, acreditada en el directorio. Se conserva la prueba
positiva de scan con la forma cualificada `alcaldia de turbaco bolivar` y se
añaden dos regresiones que exigen lookup exacto de las formas cortas y rechazo
de scan de las cuentas numeradas. **39 passed, 3 xfailed históricos** en ese
rechequeo. No se elimina el alias ni se cambia el guard/oráculo para pasar tests.

**Suite completa: 11032 passed, 2 skipped, 14 xfailed históricos**, en 518.48s (0:08:38).
No hay nuevos xfails. `gates.json` incluye los resultados y hashes de los logs;
`evidence_manifest.json` identifica las capturas exactas. No queda ningún REVIEW.
La readiness autoriza considerar Retry #2; esta tarea no ejecuta la promoción.

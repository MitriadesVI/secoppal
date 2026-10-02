# OMISSION_PROBE: chequeo de omisión/cobertura de spans sobre propuestas Foundation v4.2

Medición offline, sólo lectura. Script: `omission_probe.py` (+ `lexicons.py`, `make_tables.py`). Salidas:
`omission_probe_results__res_cond.json` (variante primaria C), `..__res.json` (A, literal), `..__res_B.json` (B),
`per_record_counts__*.json` (ids + conteos, sin texto). Sin red, sin LLM, sin pytest, sin `Settings()`; sólo funciones puras
importadas del worktree `secoppal-m1` (`host_candidates`, `decode_proposal`, `normalize_text`, `residual_of`,
`material_tokens`, `gap_matrix.match_request`). Cero escrituras fuera del scratchpad (`git status` de secoppal-m1 limpio).

## 0. Insumos encontrados y qué quedó fuera

| Insumo | Ruta | n |
|---|---|---|
| Gold privado sellado (v1.6, texto REAL) | `~/Documents/tools/secoppal-diagnostics/m1_search_gold/sealed_v1/gold_v1.jsonl` | 620 = **585 REAL + 35 SYNTHETIC** |
| Propuestas crudas v4.2, pasada 1 | `.../captures/producer_raw.jsonl` (`raw_content`, ids `span-N`) | 620 (620 PROPOSAL, 0 errores de decodificación) |
| Segunda pasada | `.../captures/producer_raw_rerun.jsonl` | **40** ids (muestra de estabilidad) |
| Gold sintético en git | `secoppal-m1/docs/audits/m1_search_foundation/gold/{neighbor,minimal_pair,adversarial}.jsonl` | 111 + 191 + 92 = 394 (v1.9) |
| Held-out DA-6 con v4.2 (3 pasadas, 45 casos inventados) | `~/Documents/secoppal-evidence/bc_h7_heldout/v42_s{1,2,3}` | 3 x 45 |
| Corpus de regresión yaml | `secoppal/tests/fixtures/foundation_v3_regression_corpus.yaml` | propuestas escritas a mano, NO capturas LLM: no usado |

Fuera de la medición:
- **NEIGHBOR / MINIMAL_PAIR / ADVERSARIAL no tienen propuesta capturada** (la Fase A sólo corrió REAL+35 sintéticos). Sólo
  tienen el "oráculo" (spans = citas del Gold), que es una propuesta perfecta hipotética, no una propuesta real.
- No existe el adaptador Gold -> IR en `scripts/audits/m1_search/`; el oráculo lo construí yo (spans = `quote` de cada condición Gold +
  `universe_quote`; no hay cita de verbos porque el Gold no los cita).
- Los 585+35 son el sello v1.6; los +15 pares MINIMAL_PAIR de v1.8/1.9 no tienen propuesta.
- Población de las tablas de detección/coste: operación Gold = SEARCH, `ambiguity != AMBIGUOUS` (los 170 registros no-SEARCH
  —FOLLOWUP_DELTA, PROTOCOL, PROFILE_*, RANK_*, COUNT...— y los AMBIGUOUS quedan fuera).
- Las propuestas se miden **antes** del host (H7 / universe_kind): la condición `RETRIEVAL_INTENT` derivada se excluye (no la cita el LLM).
  Verificado: las condiciones decodificadas por mí coinciden con las capturadas (0 discrepancias tras excluir la derivada).
- Un `candidate_id` de v4.2 no incluye texto fuera del turno; la decodificación es la del producto (misma función).

## 1. Hallazgo previo que condiciona toda cifra: la cobertura "contando TODOS los spans" es vacía

El prompt v4.2 pide `candidate_ids` a nivel de petición "covering the request". En **326/620 propuestas (297/585 REAL, 29/35 SYN) ese span
de petición es exactamente el turno completo**. Con la definición literal (todos los spans) el residuo desaparece en ese 52,6 %.
Por eso reporto tres variantes de cobertura:

- **A** = todos los spans (literal de la orden).
- **B** = A menos spans que cubren >=90 % del texto.
- **C** = sólo condiciones + universo + fuente (excluye `candidate_ids` de petición). **Es la primaria**: cualquier chequeo real tendría que ser C o B.

| Cobertura de tokens (n = 620) | media | mediana | p10 | p90 | % con cobertura = 1,0 |
|---|---|---|---|---|---|
| A: todos los spans | 0,793 | 1,000 | 0,00 | 1,00 | 56,8 |
| B: A sin spans >=90 % | 0,587 | 0,667 | 0,00 | 0,86 | 6,8 |
| C: cond+universo+fuente | 0,613 | 0,667 | 0,00 | 1,00 | 12,7 |

(REAL n=585: A 0,786 / C 0,617; SYN n=35: A 0,916 / C 0,550.) Con A y L3, 80/620 propuestas cambian de veredicto respecto de C.
Un solo turno de <=4 tokens (150/620) puede quedar cubierto por una ventana candidata de 4 palabras.
`omissions` (canal declarativo del LLM) sólo aparece en 1/620 propuestas.

## 2. Léxicos de palabras funcionales (item 2)

18 léxicos cargados por AST (no se importaron los worktrees): `STOPWORDS` (380), `RESIDUAL_FUNCTION_WORDS` (28),
`foundation_time_coverage._GLUE` (49) y `_TRAILING_GLUE` (53) [worktree c12], `buyer._FUNCTION_WORDS` (23) [dbuyer, sin commit],
`legacy_shadow._FUNCTION_WORDS` (6), `fuzzy_identity_guard.FILLER_TOKENS` (10), `dialogue_policy._COURTESY_TOKENS` (7) y `_FILLER_PREFIXES` (5 de 7; 2 multi-palabra descartadas),
`control_language._LEFT_GLUE` (8), `_LEFT_FILLER` (21), `_CONTROL_TURN_FILLER` (42), `generic_buyer._ARTICLES` (6), `actor_intent._LEADING_STOPWORDS` (20),
`entity_profile.turn._PROFILE_NOISE` (24), `source_occurrence._HEAD_FILLERS` (57), `actor_arbitration._NAME_CONNECTORS` (13),
`actor_conversation._SEARCH_LEAD_ARTICLES` (8). **No cargó** `spell_correction.PROTECTED_FUNCTIONAL_VOCABULARY` (no evaluable estáticamente).
No se agotó la búsqueda de otros léxicos (grep por nombre de variable; pueden existir más). Matriz completa de intersecciones en el JSON (`lexicons.pairwise_intersection`).

Hechos de la tabla:
- Casi nada es comparable por intersección: `RESIDUAL_FUNCTION_WORDS` y `STOPWORDS` tienen intersección **0**; `_GLUE`, `buyer._FUNCTION_WORDS`,
  `legacy_shadow`, `generic_buyer._ARTICLES`, `fuzzy FILLER` son casi subconjuntos de `STOPWORDS` (de/del/el/la/los/las...), pero los de c12 añaden ordinales/cantidades.
- Palabras únicas de un solo léxico: `STOPWORDS` 326, `RESIDUAL_FW` 17, `_CONTROL_TURN_FILLER` 5, `source_occurrence._HEAD_FILLERS` 4, `_FILLER_PREFIXES` 3, `_LEFT_GLUE` 2, `_COURTESY_TOKENS` 1; el resto 0.
- **`STOPWORDS` no es de clase cerrada**. De sus 380 entradas: 12 meses, 30 palabras de tiempo (año, mes, semestre, hoy, último...), 27 de estado
  (abierto, cerrado, adjudicado, firmado, vigente, disponible, publicados...), 30 comparadores/negadores (no, ni, sin, excepto, salvo, mayor, menor,
  menos, mas, mínimo, máximo, entre, superior, superen...), 29 de dinero/valor (millones, pesos, smmlv, cuantía, valor, total, barato/caro...),
  13 sustantivos de universo, 9 de actor/geo/sector (proveedor(es), departamento(s), provincia, área, sector, oficial) y 65 verbos de petición.
  13 de ellas (no, ni, sin, excepto, salvo, entre, mas, menos, mayor, menor, mínimo, máximo, superior) son negadores/comparadores; `RESIDUAL_FW` aporta además `desde`/`hasta`.
- Variantes L usadas (negadores/comparadores PROTEGIDOS se quitan de todas, y se reportan aparte): L0 vacío (0); L1 clase cerrada mínima art/prep/conj (30);
  L1b L1 + clíticos/posesivos/cópula-aux/interrogativos/cuantificadores (75); L2 STOPWORDS (367 tras quitar protegidos); L3 STOPWORDS ∪ RESIDUAL_FW (393);
  L4 unión de los 18 léxicos ∪ L1b (469); **L3x** = L3 tal cual el producto (408; comparadores ignorables), sólo referencia.
- Mi tokenización `[a-z0-9]+` plegada reproduce el veredicto de `material_tokens(residual_of(...))` en **620/620** propuestas con L3x (con L3, 615/620; difieren por los protegidos).

## 3. Población y definiciones

Registros SEARCH no ambiguos (n = 446 EXECUTE + otros): por tipo, contra el Gold (n por slice: REAL / SYN):

| Tipo | n total | REAL | SYN | Definición |
|---|---|---|---|---|
| CLEAN_CORRECT | **208** | 187 | 21 | todas las condiciones Gold dentro del vocabulario IR v4.2 y emparejadas sin diferencia (polaridad, dirección, valor, dimensión); sin condición espuria; sujeto del universo correcto |
| CORRECT_UNIVERSE_UNCITED | 43 | 43 | 0 | igual, pero el usuario no cita sustantivo de universo (UNSPECIFIED) |
| IR_LIMIT | 147 | 141 | 6 | alguna condición Gold fuera del vocabulario IR (ACTOR, mes/rango, inclusivos, orden) |
| PROPOSAL_WRONG | 48 | 45 | 3 | omisión, mala dimensión/polaridad/valor o condición espuria sin ser IR_LIMIT |
| NOT_EXECUTE (CLARIFY) | 3 | | | |

Emparejamiento: `gap_matrix.match_request` (solape de raíces de 5 letras), el mismo de la matriz A3. Se **omite la comprobación de lógica OR/NOT** y de vista (oportunidad).

## 4. Detección: ¿el residuo no vacío cubre las omisiones? (item 3a)

Dos definiciones de "omisión", porque la primera tiene n muy pequeño:

**(i) Por emparejamiento** (condición Gold sin ninguna condición propuesta con solape; n = 28 condiciones en 28 registros, de 1000 condiciones Gold). Celdas = `residuo no vacío / residuo contiene un token de la propia cita omitida` (variante C):

| Dimensión (n) | L0 | L1 | L1b | L2 | L3 | L4 | L3x |
|---|---|---|---|---|---|---|---|
| ACTOR (7) | 7/7 | 7/7 | 7/7 | 7/7 | 7/7 | 7/7 | 7/7 |
| VALUE (7) | 7/4 | 7/4 | 7/4 | 3/2 | 3/2 | 3/2 | 2/1 |
| STATE (4) | 4/3 | 4/2 | 4/2 | 1/0 | **0/0** | 0/0 | 0/0 |
| TOPIC (3) | 3/2 | 3/1 | 2/1 | 2/1 | 2/1 | 2/1 | 1/1 |
| MODALITY (3) | 3/0 | 3/0 | 3/0 | **0/0** | 0/0 | 0/0 | 0/0 |
| CONTRACT_TYPE (3) | 2/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| ORDER (1) | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 | 0/0 |
| **Total (28)** | 27/17 | 25/15 | 24/15 | 14/11 | 13/11 | 13/11 | 10/9 |
| recall (any) % | 96,4 | 89,3 | 85,7 | 50,0 | 46,4 | 46,4 | 35,7 |

IC 95 % Wilson aprox.: 13/28 -> 30-64 %; 24/28 -> 68-94 %. BUYER, TIME, GEO: 0 omisiones por emparejamiento.
Las celdas "any" sobreestiman: un residuo no vacío por cualquier otra causa (verbo de petición) basta; la columna "attrib" (segundo número) es el chequeo honesto.

**(ii) Geométrica, independiente del emparejador**: para cada condición Gold (EXECUTE, SEARCH; n=1000) se mira cuántos tokens de su cita quedan sin cubrir
por spans (C). `partial` = cita recortada; `full` = ningún token cubierto. Celdas de detección = `partial / full` señalados por el residuo bajo L (sin L0: con L0 se señalan los 83 y 11):

| Dimensión Gold | n | citada entera | recorte parcial | sin citar | L1 | L1b | L3 | L4 |
|---|---|---|---|---|---|---|---|---|
| BUYER | 149 | 149 | **0** | **0** | | | | |
| GEOGRAPHY | 148 | 143 | 5 | **0** | 0/0 | 0/0 | 0/0 | 0/0 |
| TOPIC | 245 | 226 | 18 | 1 | 13/1 | 13/1 | 12/1 | 11/1 |
| TIME (año 130 / mes 18 / entre fechas 13 / relativo 2) | 163 | 138 | 25 (mes 13, rango 12) | 0 | 18/0 | 18/0 | 12/0 | 10/0 |
| VALUE (>=:12, <=:14, >:61, <:13) | 100 | 77 | 23 | 0 | 19/0 | 19/0 | 13/0 | 11/0 |
| STATE | 61 | 50 | 9 | 2 | 1/2 | 1/2 | 2/0 | 0/0 |
| ACTOR | 79 | 70 | 2 | **7** | 2/7 | 2/7 | 2/7 | 2/7 |
| ORDER | 23 | 22 | 0 | 1 | 0/1 | 0/1 | 0/1 | 0/1 |
| MODALITY / CONTRACT_TYPE | 11 / 15 | todas | 0 | 0 | | | | |
| **Total** | **1000** | 906 | **83 (8,3 %)** | **11 (1,1 %)** | 53/11 | 53/11 | 42/9 | 34/9 |

Lectura: en este corpus y con v4.2 la **omisión total** de una condición es rara (11/1000; 0/149 comprador, 0/148 geografía; 7 son ACTOR, que la IR no puede expresar).
Lo frecuente es el **recorte** (83): 25 TIME no-año (convención Y1), 23 VALUE (inclusivos / «al menos»), 18 TOPIC. STATE: L3 detecta 0/2 omisiones completas porque `STOPWORDS` contiene los estados.
Un truncamiento tipo «alcaldía de Soledad» -> «Soledad» (BUYER recortado) no ocurre en las 149 de este corpus.

**Errores invisibles al chequeo de cobertura** (condición Gold cubierta por un span, pero con otra dimensión/polaridad/valor): ACTOR cita como otra dimensión **72/79**; ORDER 22/23;
TIME no-año como `during_year` (IR_PROXY) 30; VALUE inclusivo/estricto 18; BUYER 8; CONTRACT_TYPE 11; STATE 6; MODALITY 3; POLARIDAD 1.

## 5. Coste: aclaraciones de más (item 3b)

Falsos positivos = propuesta correcta (CLEAN_CORRECT, n = 208) cuyo residuo es no vacío. Variante C (cond+universo+fuente):

| L | FP | FP % | de ellos con token protegido | de ellos sólo por recorte de cita Gold | FP% (B) | FP% (A literal) |
|---|---|---|---|---|---|---|
| L0 vacío | 195 | 93,8 | 2 | 0 | 93,3 | 48,6 |
| L1 art/prep/conj | 82 | **39,4** | 2 | 9 | 36,1 | 2,9 |
| L1b + clít./aux/interrog./cuant. | 72 | **34,6** | 2 | 10 | 31,2 | 2,9 |
| L2 STOPWORDS | 34 | 16,3 | 2 | 8 | 15,4 | 2,4 |
| L3 STOPWORDS ∪ RESIDUAL_FW | 29 | **13,9** | 2 | 8 | 13,0 | 1,9 |
| L4 unión de todos | 18 | **8,7** | 2 | 7 | 7,7 | 1,0 |
| L3x (producto tal cual) | 28 | 13,5 | 0 | 8 | 12,5 | 1,9 |

Con n=208 el error de muestreo es de unos +-5 pp. Otros grupos (variante C):
- CORRECT_UNIVERSE_UNCITED (n=43): FP L1 2,3 %, L1b 2,3 %, L2-L4 0 %.
- IR_LIMIT (n=147): FP L1 49,7 %, L1b 48,3 %, L3 25,9 %, L4 22,4 %. De ellos, residuo **sólo** de tokens dentro de una cita TIME del Gold (Y1): 13 (L1), 16 (L1b), 12 (L3); sólo recortes de cita Gold: 28-32.
- Oráculo (propuesta perfecta, mínimo teórico, NO propuestas reales): sobre los mismos 208 ids FP = L1 35,6 %, L1b 30,3 %, L3 10,1 %, L4 5,3 %. Sobre los 446 SEARCH/EXECUTE de REAL+35: L1 42,4, L1b 37,9, L3 9,0, L4 4,9.
  Sintéticos (oráculo, sin propuesta real): NEIGHBOR (46) L1 30,4 / L3 10,9; MINIMAL_PAIR (170) L1 15,3 / L3 14,7; ADVERSARIAL (78) L1 37,2 / L3 39,7 (residuo de `no`, `ni`, `excepto`, `y`, `ordenados`, `si`: el Gold no cita el negador en varias citas).
  Es decir, **la propuesta real de v4.2 está a ~3-5 pp del mínimo que impone el propio Gold** (que no cita verbos): el coste no viene de errores del LLM sino de lo que ningún span cita.
- Held-out DA-6 v4.2 (45 casos inventados, estilo con verbos «Busca...», «Muéstrame...»; correctas = `MATCH` y esperado READY): n=32/33/34 por pasada;
  FP L1 24/25/26 (75-76 %), L1b 23/24/25, L3 5/5/6 (15,6-17,6 %), L4 1/1/1. **Peor caso sobre las 3 pasadas** (n=32 correctas en las tres): L1 24, L1b 23, **L3 5 (15,6 %)**, L4 1 (3,1 %).

### Tokens del residuo en falsos positivos (CLEAN, variante C)

L1b (72 registros con FP, 114 ocurrencias, 51 tokens distintos; 25 ocurrencias singleton de clase abierta suprimidas por privacidad; el resto aparece a cualquier frecuencia):

| Categoría (heurística por regex/listas, declarada) | registros | ocurrencias | tokens (n) |
|---|---|---|---|
| Verbo de petición | 38 | 48 | busca 14, muéstrame 11, quiero 5, ver 4, presentarme 4, licitar 3, busco 2, buscar, buscate, interesan, quisiera, necesito |
| Resto de span mal recortado (token dentro de cita Gold sin citar) | 13 | 18 | pesos 3, convocatoria 1 (+ singletons suprimidos) |
| Muletilla / cuantificador | 11 | 11 | algún 6, alguna 5 |
| Cópula/aux conector | 7 | 7 | tiene 6, estén 1 |
| Sustantivo de universo | 7 | 7 | procesos 3, proceso 3, licitaciones 1 |
| Cortesía | 6 | 6 | hola 5, holaa 1 |
| Otros | 14 | 15 | valor 2, ahora 2 (+ suprimidos) |
| Protegidos (comparador) | 2 | 2 | mas 2 |

L3 (29 registros, 36 ocurrencias, 26 distintos; 22 suprimidas): **coordinador `y`/`o` entre spans citados** 10 registros (o 7, y 3); recortes de cita Gold 9 registros (14 ocurrencias); otros 9; verbo 1 (interesan); protegido 2 (mas).
L4: 18 registros: recortes 8, otros 8, protegido 2, verbo 1. No hay listado de 40 tokens porque con L3/L4 sólo quedan 26/23 tokens distintos.

## 6. Sensibilidad (item 4)

Curva (variante C; recall sobre las 28 omisiones por emparejamiento; FP sobre CLEAN n=208; IR_LIMIT n=147):

| L | tamaño | recall any % | recall attrib % | FP clean % | FP IR_LIMIT % |
|---|---|---|---|---|---|
| L0 | 0 | 96,4 | 60,7 | 93,8 | 91,8 |
| L1 | 30 | 89,3 | 53,6 | 39,4 | 49,7 |
| L1b | 75 | 85,7 | 53,6 | 34,6 | 48,3 |
| L2 | 367 | 50,0 | 39,3 | 16,3 | 29,9 |
| L3 | 393 | 46,4 | 39,3 | 13,9 | 25,9 |
| L4 | 469 | 46,4 | 39,3 | 8,7 | 22,4 |

Respuestas a las preguntas:
- **Clase cerrada sin vocabulario de dominio ni verbos (L1/L1b)**: FP 35-39 % en las propuestas reales (30-36 % incluso con el oráculo): no hay FP aceptable sin cubrir otras categorías. Mantiene recall 86-89 %.
- **Léxicos con vocabulario de dominio (L2-L4)**: FP 9-16 %, pero el recall cae al 46-50 % porque `STOPWORDS` contiene meses, estados, modalidad, comparadores y unidades; el recall de STATE y MODALITY por omisión completa es 0 con L3/L2.
  Es decir, el FP aceptable se compra con ceguera exactamente en las dimensiones que más importa vigilar.
- **Subida de cobertura por categoría citable** (ignorar la categoría; voraz; CLEAN n=208). Partiendo de L1: +verbos de petición 39,4 -> 29,3 %; +aux 22,1; +"otros" 16,8; +recortes de cita 12,0; +muletillas 8,2; +cortesía 5,3; +sustantivos de universo 2,4; +interrogativos 1,0; (1,0 % restante = 2 registros con `mas`, protegido).
  Partiendo de L1b: verbos 34,6 -> 24,0; +otros 18,8; +recortes 13,5; +muletillas 9,6; +universo 6,2; +aux 3,8; +cortesía/Y1/números/interrog. 1,0.
  Las categorías cuyo FP dependen en exclusiva (L1b): verbos de petición 22 registros, recortes de cita 10, "otros" 7, aux 5, muletillas 4, universo 3, cortesía 0.
  Con L3 el único bloque grande es el coordinador `y`/`o` (10 registros; sin él 9,6 %), luego los recortes (12 -> 5,8 %).
- Los verbos de petición (≈ 50-60 % del FP en L1b) no son citables en v4.2: ninguna dimensión los cubre (CONTROL_LANGUAGE existe en `SemanticDimension` pero no está en el prompt). El Gold tampoco los cita (el oráculo los deja en residuo).
- Y1: dentro de CLEAN el FP debido a Y1 es **0** (por construcción, meses/rangos son IR_LIMIT). En IR_LIMIT, residuo exclusivamente de tokens de citas TIME Gold: 12-16 de 147 (L3: 12; L1b: 16); tokens TIME en residuo (L1b): entre 7, agosto 6, desde 3, junio 2, hasta 2, números de día. 25 de 33 condiciones TIME no-año quedan recortadas.
  Con L2/L3 los meses desaparecen (`STOPWORDS` los contiene): durante_mes recortado 13, señalado 0 con L3/L4, 8 con L1b.

## 7. IR_CANNOT_EXPRESS (item 3c)

147 registros SEARCH/EXECUTE (n por clave Gold fuera de vocabulario; un registro puede tener varias). Residuo no vacío con la propuesta real (v4.2) frente al oráculo (cita perfecta, como lo haría una IR que sí exprese todo):

| Clave | registros | v4.2 L3 | oráculo L3 | v4.2 L1b | oráculo L1b | v4.2 L4 | oráculo L4 |
|---|---|---|---|---|---|---|---|
| ACTOR | 79 | 10 | 6 | 22 | 58 | 9 | 2 |
| ORDER/value_desc | 22 | 4 | 2 | 7 | 8 | 2 | 0 |
| TIME/during_month | 18 | 2 | 2 | 16 | 9 | 2 | 2 |
| TIME/between_dates | 13 | 13 | 0 | 11 | 3 | 11 | 0 |
| VALUE >= | 12 | 7 | 4 | 11 | 7 | 6 | 3 |
| VALUE <= | 14 | 9 | 3 | 13 | 6 | 8 | 2 |
| relativo/date_desc | 3 | 1 | 1 | 2 | 2 | 1 | 1 |
| **Cualquiera (147)** | 147 | **38** | **15** | **71** | **85** | **33** | **8** |

Estado de las 147: ACTOR 72/79 citado como otra dimensión (BUYER/UNSUPPORTED/TOPIC...) y 7 omitido; ORDER 21 como otra dimensión y 1 omitido; TIME no-año 30 como `during_year` (proxy); VALUE inclusivo 18 como estricto; VALUE <= 7 omitido y 1 polaridad.
Con la IR actual el residuo señala el problema en 31/147 registros (L3, residuo con un token de la propia condición IR); con una IR v2 ideal, el residuo mínimo L3 baja a 15/147 (L4: 8/147), es decir, "hasta 3x menos ruido", pero el chequeo de cobertura **no ve** la mayor parte del problema IR, que es mala dimensión (ACTOR, ORDER, TIME, VALUE), no omisión.

## 8. Solapes y ventana ciega (item 5)

Spans de dimensiones distintas solapados (REAL n=585, pasada 1):
- 120 propuestas (20,5 %) solapan algún par de dimensiones **distintas**, pero **116 son contra UNIVERSE** (UNIVERSE+UNSUPPORTED 47, TOPIC+UNIVERSE 41, BUYER+UNIVERSE 12, GEO+UNIVERSE 11, UNIVERSE+VALUE 11, TIME+UNIVERSE 8, STATE+UNIVERSE 5, MODALITY+UNIVERSE 5). Excluyendo UNIVERSE: **4** (TOPIC+VALUE 1, TOPIC+UNSUPPORTED 1, BUYER+UNSUPPORTED 1, STATE+TOPIC 1). SYN35: 1 (UNIVERSE+UNSUPPORTED).
- Oráculo (citas Gold): 0/446 solapes, así que los 4 de v4.2 son del LLM, no del Gold. Oráculo sintético: 2/294 (BUYER+GEOGRAPHY).

Ventana ciega: TIME con >=1 token vecino (<=4 por lado) dentro de un span BUYER/ACTOR/GEOGRAPHY de la misma propuesta (v4.2 no emite ACTOR):

| Conjunto | propuestas con TIME | citas TIME | vecino <=4 en BUYER/GEO | adyacente (distancia 1) |
|---|---|---|---|---|
| REAL v4.2 | 176 | 184 | 132 citas / 126 propuestas (71,6 % de propuestas con TIME) | 10 |
| SYN35 v4.2 | 14 | 14 | 13 | 0 |
| Oráculo Gold REAL+35 | 163 | 163 | 134 | 20 |
| Oráculo sintéticos | 58 | 60 | 36 (35 prop.) | 5 |

La ventana de <=4 vecinos es casi siempre no vacía (72 %) porque los turnos son cortos; adyacencia real: 10/176. Medida sólo geométrica: no distingue tokens funcionales intermedios.

## 9. Segunda pasada (item 6)

Sólo hay 40 ids repetidos (30 REAL + 10 SYN; 34 SEARCH no ambiguos; CLEAN_CORRECT 23 en ambas). Tipo de registro: 0 cambian entre pasadas. FP sobre CLEAN (n=23): L1 56,5 % (13), L1b 56,5 %, L2 13,0 %, L3 **8,7 % (2)**, L4 8,7 %; **peor caso por registro = igual a cada pasada, 0 registros cambian de estado FP**.
Detección en esas 40: 2 omisiones por pasada (`any`: L0 2, L1/L1b 1, L2-L4 0). Con n=23 y 2 no hay poder estadístico; la estabilidad del residuo en pasadas repetidas es compatible con "estable" en esta muestra (propuestas canónicas cambian 7,5 % según Fase A, en otra definición; aquí la clase y el residuo no cambian).

## 10. Lo que estos datos permiten y no permiten concluir

Permiten:
1. Un chequeo "propuesta cubre todo el texto" con **todos los spans** es vacío en el 53 % de las propuestas v4.2 (span de petición = turno completo). Cualquier diseño debe excluir los `candidate_ids` de petición (variante C).
2. En 1000 condiciones Gold de 620 propuestas reales v4.2, la omisión total fue 1,1 % (11: 7 ACTOR, 2 STATE, 1 TOPIC, 1 ORDER) y el recorte 8,3 % (83: TIME no-año 25, VALUE 23, TOPIC 18, STATE 9). Ningún BUYER (149) ni GEO (148) quedó sin citar o recortado. El caso «de la Alcaldía de Soledad» no aparece en este corpus; el hueco es real en teoría y no fue observado aquí.
3. Un residuo con clase cerrada (L1/L1b, sin vocabulario de dominio ni verbos) detecta 86-89 % de las omisiones pero aclara de más en 35-39 % de las propuestas correctas; el piso teórico con el oráculo del Gold es 30-36 %. Con el léxico del producto (L3) el coste baja a 14 % (10 % oráculo) pero sólo ve 46 % de las omisiones y 0 % en STATE/MODALITY/mes porque `STOPWORDS` contiene ese vocabulario. L4 (unión) mantiene el mismo recall que L3 con 8,7 %.
4. El coste de L1b se debe sobre todo a verbos de petición (38 de 72 registros), luego muletillas/cuantificadores, aux, recortes de cita y universo; para ≤5 % hay que cubrir (o ignorar) verbos, aux, muletillas, cortesía y universo. Ninguna de esas categorías es citable hoy.
5. Los errores más frecuentes del intérprete (ACTOR como otra dimensión 72/79, ORDER 22/23, TIME/VALUE proxy 48) son **invisibles** a un chequeo de cobertura: el span está citado.
6. Hay 5 léxicos con intersección mínima; `STOPWORDS` y `RESIDUAL_FW` son disjuntos; ninguno es de clase cerrada pura salvo `_ARTICLES`/`legacy_shadow`.

No permiten:
- Estimar la tasa de omisión en NEIGHBOR / MINIMAL_PAIR / ADVERSARIAL (sin propuestas); ni en prompt v5 u otro modelo; ni con el host/H7 aplicado.
- Un recall fiable: 28 omisiones (IC ~30-64 % para L3) o 11 completas; los recorte(83) sí tienen n útil. Todo recall por dimensión tiene n<=7.
- Concluir sobre el coste en turnos con verbos (el held-out DA-6 con «Busca...» da 15,6 % con L3 y 72-77 % con L1b; el REAL terso da 13,9 % y 35-39 %); el coste depende del registro del corpus.
- Atribuir causa a los FP más allá de la clasificación heurística por regex/listas (verbo, cortesía, muletilla...): las listas son mías, no una autoridad.
- Evaluar `emitir CONTROL_LANGUAGE`, la cita de verbos o evidencia de operación: no hay propuestas con esas citas; sólo se simula "ignorar la categoría" (cota inferior del coste si fuera citable perfectamente).
- Medir el efecto sobre el producto real: mi "clarificar" es residuo no vacío; el producto tiene otras barreras (H7, guardas, host).
- Generalizar la pasada repetida: sólo 40 ids (23 correctos).
- Cobertura de los 18 léxicos: puede haber más; `spell_correction.PROTECTED_FUNCTIONAL_VOCABULARY` no se cargó.
- Mi población "correcta" omite la lógica OR/NOT y la vista de oportunidad; el emparejador heurístico (raíces de 5 letras) puede mal-clasificar casos límite.

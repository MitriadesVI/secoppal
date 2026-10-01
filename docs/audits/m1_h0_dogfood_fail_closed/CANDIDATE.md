# M1-H0 — Dogfood fail-closed (candidate)

Rama `fix/m1-h0-dogfood-fail-closed`, base `4ec99bc1`, código en `c5fbd5c5`. Sin merge ni push.

## Tesis
Con `SECOPPAL_FOUNDATION_MODE=foundation_v3` el host ejecutaba dos significados equivocados sin avisar
(ADR-069). H0 los convierte en `NEEDS_CLARIFICATION`; no amplía capacidades y no resuelve meses ni rangos.

## Qué cambió
| Archivo | Cambio |
|---|---|
| `app/core/entity_resolver.py:488` | `EntityResolver.alias_owners(normalized) -> frozenset[str]`, consulta pura sobre `owners_by_alias` (que ya existía y es no-lossy). `alias_index` y todo el indexado quedan intactos. |
| `app/core/foundation_admission.py:109-111` | BUYER: si `resolve_entidad` dice `exact` y el alias tiene >1 dueño, `NEEDS_CLARIFICATION` con `AMBIGUOUS_HOST_DIMENSION:BUYER` (constante `AMBIGUOUS_BUYER`, :24). |
| `app/core/foundation_admission.py:117` | TIME: `during_year` sólo resuelve si además `not uncited_time_qualifier(raw, condition.evidence)`; si no, el `UNRESOLVED_HOST_DIMENSION:TIME` existente. Enganche de una línea. |
| `app/core/foundation_time_coverage.py` (nuevo, 82 LoC) | Chequeo de cobertura de frontera con léxico cerrado. Sin parser temporal. |
| `app/core/foundation_messages.py` | Mensaje propio para `AMBIGUOUS_HOST_DIMENSION:BUYER` (entra a `KNOWN_CODES` vía `_EXACT`); el texto de `TIME` ahora dice "un año completo, sin meses ni rangos" (antes sugería que el problema era sólo "más de un año"). |

### Elección del código de razón
BUYER usa un código nuevo `AMBIGUOUS_HOST_DIMENSION:BUYER` (como pedía la misión) porque distingue en traza
"no resolvió" de "resolvió a varios dueños"; se le añadió su mensaje exacto. TIME reutiliza
`UNRESOLVED_HOST_DIMENSION:TIME`, tal como se pidió.

### Diseño del chequeo TIME
Desde cada año citado dentro de las spans, se camina por el texto crudo hacia atrás y hacia delante sobre
*palabras pegamento* (de, del, el, la, año, y, o, a, al…; hacia delante también en, hacia, para, hasta). La
primera palabra del léxico cerrado (meses y abreviaturas con/sin tilde, periodos parciales como semestre/
trimestre/finales de, cotas desde/hasta/entre/antes/después/tras/a partir/previo/posterior/anterior; hacia
delante: en adelante, atrás, posteriores, anteriores, a la fecha, hasta hoy) u otro año => el año está
calificado. Cualquier otra palabra detiene la marcha. Ventana: 4 tokens contando el marcador (la cadena más
larga legítima, «a partir de el año 2023», usa 3-4). La regla "sólo pegamento" es lo que mantiene limpios
«hasta 500 millones en 2025» o «contratos 2025 mayores a 2000 millones».
- `2025-2026`, `2025/2026`, `2025 a 2026`, `2023 y 2025`: dos años => se pregunta (el productor los reducía a uno).
- Números que no son años (`3025`, `12025`, `1899`): no califican.
- Extensión sobre lo pedido (declarada): cotas posteriores al año («2023 en adelante», «y posteriores») y año vecino.

## Pruebas
Tests nuevos: `tests/test_foundation_h0_buyer_ambiguity.py` (147) y `tests/test_foundation_h0_time_coverage.py` (212).
- BUYER: los 137 aliases multi-dueño del índice real (136 `exact`) nunca salen READY; los 136 dan `AMBIGUOUS_HOST_DIMENSION:BUYER`;
  `alcaldia de barranquilla` y `gobernacion del huila` siguen READY con su dueño único; muestra de ~100+ aliases de dueño único; sin llamada a fuente (harness).
- TIME: exhaustivo sobre listas escritas de forma independiente del módulo (25 meses x 3 capitalizaciones x 8 formas, 18 periodos x 4 formas, 16 cotas previas x mayúsculas x 6 formas, 14 cotas posteriores), más Hypothesis (qualifier inmediato siempre visto; sin qualifier nunca) y negativos (año solo, vigencia, año, importes, no-años).
- Frontera de ventana y bloqueo por palabra no-pegamento fijados.

## Gates (comandos literales y resultados)
- `ruff check` sobre los 6 archivos tocados/nuevos: `All checks passed!`
- `pytest tests/test_foundation_*.py tests/test_procurement_foundation_frozen_delivery.py tests/test_entity_resolver*.py -q`: **2369 passed** (incluye los 2 tests nuevos y `test_foundation_product_gates`, el replay del corpus congelado).
- `pytest tests/test_natural_alpha_evaluation.py test_text_lineage*.py test_truth_layer_public_parity.py test_procurement_layers_sources.py test_gazetteer_source_binding_audit_regressions.py test_qualified_buyer_fail_closed.py test_module_size_limits.py test_query_router.py test_residual_verifier_contract.py`: **380 passed**. Los tests que fijan bytes/SHA no cambiaron; ningún validador relajado.
- `olympus-semgrep-delta --config .semgrep/secoppal-laws.yml --base 4ec99bc1 <6 archivos>`: TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0.
- Corrí los focales y los de guardas, no la suite global.
- `olympus-state own claim` respondió CLAIMED (tardó más de 120 s; el hang conocido de la baseline de main). No se pudo liberar ownership de forma fiable al cierre: ver "Pendiente".

## Mutantes (ejecutados; cada uno aplicado al archivo, test corrido, archivo restaurado)
Muertos (23/23): quitar el enganche BUYER; `alias_owners` vacío; umbral `>2`; quitar el enganche TIME; léxico previo vacío;
léxico posterior vacío; sin meses (previo, posterior, ambos); sin cotas previas; sin cotas posteriores; sin periodos; sin
`sept`; sin `hasta`; sin `partir`; sin `adelante`; sin `semestre`; ventana 3; ventana 6; ignorar otro año; sin pegamento;
sin lado posterior; sin parada en palabra no-pegamento. Los mutantes de léxico también matan sin el test de contención de léxico
(109 fallos de comportamiento al quitar los meses previos). Primera tanda: dos mutantes mal construidos (B4/B5) sobrevivieron por mi error de construcción; se rehicieron correctamente y mueren.

## Paridad legacy
`alias_index` es idéntico al índice last-writer recomputado desde `aliases_db.json` (test). Comparación base `4ec99bc1` (export `git archive`) vs candidato sobre
11.665 claves (aliases + nombres físicos: `resolve_entidad` valor+método) y 620 textos del gold (`scan_entity`) más el `alias_index` completo:
hash SHA-256 idéntico `59957bfb884c51d27c1e9b0e40a43bca904e3132758f5c9f11c7f6d8db6d8ea2` en ambos. La ruta legacy no importa los módulos tocados salvo el método añadido.

## Replay de impacto (capturas A3, host sólo admisión, sin red; únicamente gold_id)
El "antes" reproduce 620/620 las admisiones guardadas en `host.jsonl` (217 READY). Después: 195 READY. 22 pasan de READY a NEEDS_CLARIFICATION; ninguno cambia de otra forma.
- **TIME (13)** `UNRESOLVED_HOST_DIMENSION:TIME`: R0194, R0333, R0376, R0463, R0476, R0479, R0494 (los 7 pedidos, clase mes), más R0474, R0546 (mes) y R0210, R0287, R0372 (cota «entre» + dos años), R0316 (dos años sin cota). Verificado por clase de marcador que todos son mes o rango; ninguno es falso positivo de la lista.
- **BUYER (9)** `AMBIGUOUS_HOST_DIMENSION:BUYER`: R0058, R0145, R0146, R0147, R0148, R0149, R0552, S008, S021.
- READY->CLARIFY fuera de las clases mes/rango/multi-dueño: **ninguno**.

## Riesgos y fuera de alcance
1. **Coste de recall en BUYER:** siglas de uso masivo (ICBF 12 dueños, SENA, INVIAS) siempre piden aclaración; decisión de producto (p. ej. grupo canónico nacional) pendiente. 9/217 READY del gold.
2. **Legacy sigue con el defecto**: `resolve_entidad("invias")` devuelve `ANI`, `resolve_entidad("icbf")` `ICBF REGIONAL TOLIMA`, `resolve_entidad("sena")` una regional; `scan_entity` (la ruta principal) sí elige otro dueño. No se cambió (fuera de alcance, paridad demostrada).
3. Falsos positivos conservadores de TIME: un mes usado como nombre propio sin «en» delante del año («Avenida Primero de Mayo 2025»), siglas coincidentes con abreviaturas (SEP, MAR) cuando preceden al año. Piden aclaración, nunca ejecutan mal. CORRECCIÓN R1: la versión R0 de este informe y el docstring afirmaban que «Primero de Mayo en 2025» pregunta; es falso (da READY porque «en» no es pegamento previo al año). Ver R1.
4. Falsos negativos TIME: calificadores fuera de léxico o a más de 4 tokens de pegamento (p. ej. «navidad de 2024»). («Q1 2025» quedó cubierto en R1.)
5. El host sólo mira el texto crudo alrededor de la cita (no hizo falta releer fuera de la ventana ni cambiar arquitectura).
6. Mensaje TIME cambiado (público); cualquier otro TIME no resuelto lo comparte.
7. Sin ZEUS fresco ni congelación del candidato: decisión del orquestador.

---

# R1 — reparación tras ZEUS (GO_WITH_FIXES)

Rama `fix/m1-h0-dogfood-fail-closed`, base `4ec99bc1`, R0 en `fb34c4aa`. Una sola ronda de reparación. Sin merge ni push.

## P1-A TIME: un token numérico u ordinal ya no corta el recorrido
`app/core/foundation_time_coverage.py`:
- Pegamento en ambas direcciones (CORRECCIÓN R2: no es «ambos órdenes sin excepciones»; ver «Límites congelados de TIME» en R2): ordinales y cuantificadores (`ORDINALS`: primer(o/a/os/as)… cuarto, último, un, dos…doce) y números de 1-3 dígitos («15 de marzo», «primeros 100 días»). Cubre «2025 segundo semestre», «2025, segundo semestre», «2025 primer trimestre», «los dos primeros trimestres de 2025».
- Plurales de periodo (semestres, trimestres, meses, semanas, días…), `dia/dias`, `fin`, `cierre` («fin de año 2025», «cierre de 2025»).
- Cotas posteriores: `antes, despues, ahora, actualidad, presente, aca` («2025 o antes», «hasta ahora», «a la actualidad», «al presente», «para acá», «al día de hoy»); cotas previas `pre`, `post`.
- Fechas numéricas leídas de los caracteres crudos pegados al año: `15/03/2025`, `2025-03`, `2025/03`, `12/2025`, `2025-1`, `2025-I`; y cuartos compactos (`Q1`, `T1`, `H1`, `1T`, `2S`).
- Docstring: ahora declara que es una GUARDA DE FRONTERA con garantía limitada (no un parser temporal) y corrige la afirmación falsa sobre «Primero de Mayo en 2025».

Pruebas (`tests/test_foundation_h0_r1_time_property.py`): léxicos independientes comparados por IGUALDAD con el módulo (quitar una sola palabra falla), comportamiento por palabra (cada mes/periodo/cota sola, cada palabra de pegamento), periodo ANTES y DESPUÉS del año por propiedad (35 ordinales x 7 periodos x 6 formas; 7 números x 4 periodos x 4 formas), Hypothesis en ambos órdenes (sólo para las formas del léxico escrito, no para toda forma natural; ver R2), y negativos (años con importes/cifras/no-años siguen ejecutando).

Mutantes: los 3 supervivientes de ZEUS (quitar previa/previos/posteriores/anteriores, quitar `anos`, quitar `e`/`u`) mueren; 14 mutantes TIME nuevos (ordinales, números, fecha antes/después, cuartos, plurales, antes/ahora/actualidad, fin/cierre, dia/dias, pre/post, paseo posterior, ventana 3, romano) y 6 de R0 repetidos: todos muertos (23/23 TIME).

## P1-B BUYER: la aclaración nombra candidatas y no es un bucle
Nuevo módulo `app/core/foundation_buyer_choices.py` (una responsabilidad); `foundation_messages.user_message(..., entity_resolver=None)` lo usa para `AMBIGUOUS_HOST_DIMENSION:BUYER`; `foundation_runtime.py` pasa `workflow.entity_resolver` (1 línea). El código de razón no cambia.
- Candidatas = dueños del alias (`alias_owners`), con sede «nacional/central» primero, resto ordenado, máx. 6 y «y N más». `choices` lleva las mismas.
- Sólo se ofrece un nombre si el host lo acepta de verdad: el nombre oficial lo reclama exactamente esa entidad, `resolve_entidad` lo da `exact` y la fuente tiene nombre físico para cada dataset del request (si no, la proyección responde `UNSUPPORTED BUYER_SOURCE_PROJECTION_REQUIRED`).
- Resultado con el resolver real: «ICBF» → «ICBF SEDE NACIONAL; ICBF REGIONAL ANTIOQUIA, ATLANTICO, CAUCA, CUNDINAMARCA, GUAJIRA y 6 más». «ICBF REGIONAL BOGOTA» NO se ofrece (exacta y de un solo dueño, pero sin nombre físico en la fuente: UNSUPPORTED). Cada candidata mostrada de los 137 aliases multi-dueño pasa admisión READY y tiene nombre físico en contratos (`project_buyer_names`); la proyección completa se ejercita de punta a punta con las 6 de ICBF.
- **Límite declarado, no resuelto:** 117 de los 137 aliases multi-dueño (entre ellos `sena`, `invias`, `ids`, hospitales «san juan de dios/san rafael», `cenac`, `fondane`) no tienen ningún dueño verificable, porque la fuente no tiene nombre físico para ninguno (ni siquiera el nombre inequívoco sería ejecutable en el Foundation). Para esos el mensaje deja de pedir «el nombre completo» (era el bucle) y dice con honestidad que no hay nombre exacto que confirmar, sugiriendo otra entidad o el territorio. Es deuda de D-BUYER / vinculación de nombres de fuente, no de H0.
- Nota de arquitectura (hecho verificado): el Foundation sólo reclama el PRIMER turno de una conversación (`INELIGIBLE_HISTORY`). La respuesta del usuario en la misma conversación la resuelve el legacy: probé que escribir cada candidata mostrada de ICBF en la misma conversación da `entidad_resolved` igual a esa candidata. El READY del Foundation se verifica en una conversación nueva.

Pruebas (`tests/test_foundation_h0_r1_buyer_choices.py`, 144): orden/tope/«y N más» de ICBF, nombre compartido o sin proyección nunca ofrecido, SENA sin candidatas sin bucle, propiedad sobre los 137 aliases (cada candidata mostrada resuelve a exactamente ella y es READY; sin candidata sólo si ningún dueño es verificable), y punta a punta con harness: «contratos del ICBF en 2025» → aclaración con 6 candidatas y 0 llamadas a la fuente → «contratos de <candidata> en 2025» → READY con `nombre_entidad = '<candidata>'` (6/6), y misma conversación → legacy resuelve a la misma entidad. 9 mutantes BUYER (tope, sede primero, proyección, dueño único, conteo, texto «y N más», runtime sin resolver, orden, choices): 9/9 muertos (total 32/32 con los de TIME).

## Límites / P2 aceptados (no se reparan en esta ronda)
1. Sobre-aclaraciones fuera de las clases mes/rango (fail-closed, no ejecutan mal): «acta de inicio de 2025», «interventoría final 2025», «liquidación final del 2025», «Hospital 20 de Julio 2025», «Colegio 7 de Agosto 2025», «Batallón Mayo 2025», «SEP/MAR 2025», «dragado del mar 2025». Recuento sobre las sondas de falsos positivos de ZEUS (45 frases): 30 preguntan, idéntico a R0; R1 no añade falsos positivos en ellas. En las 620 capturas del gold: **0 READY->CLARIFY nuevos frente a `fb34c4aa`** (195 READY antes y después; sólo los 22 de R0, todos de clase mes/rango/multi-dueño).
2. «INVIAS» pregunta porque ANI reclama ese alias en los datos (legacy: invias→ANI, preexistente). Deuda D-BUYER.
3. Dos años sin cota («2023 y 2025», R0316) pasan de READY a CLARIFY: ampliación declarada fuera de la letra «mes, cota o rango».
4. «Primero de Mayo en 2025» NO pregunta (corrección de la documentación de R0); «Avenida Primero de Mayo 2025» sí.
5. La cobertura TIME es una guarda de frontera con garantía limitada: sigue sin ver calificadores fuera del léxico («Semana Santa», «navidad de 2025»), a más de 4 tokens, o separados del año por una palabra no-pegamento («agosto del ICBF en 2025»). La autoridad temporal completa la entrega la misión D-TIME.

## Gates R1 (corrí estos, no la suite global)
- `pytest tests/test_foundation_*.py tests/test_procurement_foundation_frozen_delivery.py tests/test_entity_resolver*.py -q`: **3092 passed** (incluye los 2 archivos nuevos).
- Guardas de bytes/SHA y vecinos (`test_natural_alpha_evaluation`, `test_text_lineage*`, `test_truth_layer_public_parity`, `test_procurement_layers_sources`, `test_gazetteer_source_binding_audit_regressions`, `test_qualified_buyer_fail_closed`, `test_module_size_limits`, `test_query_router`, `test_residual_verifier_contract`, `test_legacy_shadow`): **456 passed**; ningún validador relajado.
- Paridad legacy: mismo cálculo de hashes (11.665 claves + 620 textos + `alias_index`): `59957bfb884c51d27c1e9b0e40a43bca904e3132758f5c9f11c7f6d8db6d8ea2`, idéntico al de base y R0.
- Replay 620 capturas (sólo gold_id): 0 cambios de estado frente a `fb34c4aa`.
- `ruff check` sobre los 6 archivos tocados/nuevos: `All checks passed!`
- `olympus-semgrep-delta --config .semgrep/secoppal-laws.yml --base 4ec99bc1` (10 archivos): TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0.

---

# R2 — micro-ronda tras ZEUS R1 (GO_WITH_FIXES): sólo opciones de comprador

Rama `fix/m1-h0-dogfood-fail-closed`, base `4ec99bc1`, R1 en `c25f98ee`. Una sola ronda. Sin merge ni push.

## P1: una lista parcial ya no se presenta como la elección completa
Causa (ZEUS): `shown` era el subconjunto tecleable y podía excluir justo la entidad nombrada («IDRD» ofrecía la Secretaría de Cultura; «ICBF Bogotá» ofrecía la Sede Nacional y Cundinamarca) y el mensaje lo llamaba «Opciones».
`app/core/foundation_buyer_choices.py` ahora contabiliza cada dueño exactamente una vez: ofrecido (`shown`, máx. 6), sobre el tope (`hidden`, conteo real) o `unavailable` (el host no lo toma por nombre: nombre compartido o sin nombre físico en la fuente). Regla de redacción (`foundation_messages.py`), la opción más honesta:
1. Todos ofrecibles: «Opciones: …» + «y N más» (N real) + botones (`choices`). Hoy: `sdh`, `la sdh`.
2. Algún dueño no ofrecible: el texto dice «Algunas de las entidades que usan ese nombre: …», «Esta lista es parcial: hay K más que no puedo consultar por nombre todavía (nombres, hasta 4)», y NO lleva botones. Un botón sobre una lista parcial sería presentarla como la elección completa.
3. Un dueño distintivo no ofrecible («icbf bogota» -> ICBF REGIONAL BOGOTA; «idrd» -> IDRD): el texto dice que lo más probable es que se refiera a él y que todavía no se puede consultar por nombre; no ofrece hermanos ni botones.
4. Ningún dueño ofrecible (117 de 137 aliases, p. ej. SENA/INVIAS): texto genérico de R1, sin opciones.
Dueño distintivo = aquel cuyo nombre contiene todas las palabras citadas y que es una minoría (<= la mitad de los dueños): «ICBF Bogotá» -> 1/12, «IDRD» -> 1/2; «ICBF» (11/12) no promueve a nadie. Si es ofrecible va primero (incluso pasado el tope y la regla de sede); si no, regla 3. Heurística de palabras, límite declarado: una cita que no repite palabras del nombre oficial («instituto distrital de recreación y deporte» -> IDRD) cae en la regla 2, que igualmente nombra a IDRD como no consultable y no ofrece botones.
Resultado sobre los 137 aliases multi-dueño (136 los pregunta el guard): 117 sin ofrecibles, 16 parciales, 2 con dueño distintivo no consultable (icbf bogota, idrd), 2 completos.

## P1 negación
Con polaridad NEGATIVE en el BUYER («que no sean del ICBF») no se ofrece ninguna opción ni se nombra ningún dueño (elegir uno reduciría la exclusión a una sola entidad): «Pides excluir «X», un nombre que corresponde a más de una entidad compradora… ¿Puedes decir de qué entidad concreta se trata, o buscar sin esa exclusión?». La traza (`AMBIGUOUS_HOST_DIMENSION:BUYER`) no cambia.

## P2
- Mutantes del delta R1: añadidos tests que matan los 6 supervivientes de ZEUS (mensaje sin `shown`, sin comprobar la dimensión, `owners<2`->`<1`, `_SMALL_NUMBER` 1-4/`\d+`, `_DATE_BEFORE` `\d{2}`, `typable` sin value/exact con resolvers falsos) y los de R2 (nuevo código). **61/61 muertos** (cubre los 26 de ZEUS, los de TIME de R1 y los nuevos de BUYER/mensaje/runtime de R2; ninguno quedó sin aplicar). Antes del test nuevo sobrevivían 3 de R2 (dataset por request, material_phrase, «y N más» en lista completa): ya muertos.
- `material_phrase`: `buyer_choices` lee la frase con `material_phrase(condition.evidence, raw, "BUYER")`, la misma autoridad del host, no con `" ".join(quotes)`. Para ello `user_message(..., raw=)` recibe el texto crudo (`foundation_runtime` pasa `ctx.user_query`, el mismo que usó la admisión); sin `raw` no hay opciones (texto genérico).

## Límites congelados de TIME (NO reparados; regla de hardening acotado)
La guarda TIME es de frontera con garantía limitada; su autoridad completa es de D-TIME. Siguen READY (no regresión, fail-open frente a base):
- «2025 2do semestre», «2025 1er semestre», «del 2025 3er trimestre» (ordinal abreviado con sufijo);
- «semestre II de 2025», «2025 II semestre», «2025 I» (romanos);
- «2025 2ª mitad» frente a «la 2ª mitad de 2025» (asimetría de orden), «2025 durante el segundo semestre».
Por tanto la frase de R1 «ambos órdenes» valía para las formas del léxico escrito, NO para toda forma natural. Sobre-aclaraciones por duraciones («2025 de 6 meses», «2025 a 12 meses», «hasta 300 2025»): fail-closed, no se reparan. Los límites 1-5 de R1 siguen vigentes.

## Archivos R2
`app/core/foundation_buyer_choices.py`, `app/core/foundation_messages.py`, `app/core/foundation_runtime.py` (+1 kwarg), `tests/test_foundation_h0_r1_buyer_choices.py` (ajustado al nuevo contrato: ICBF es parcial y sin botones), `tests/test_foundation_h0_r2_buyer_options.py` (nuevo, 164+1 skip), `tests/test_foundation_h0_r2_time_boundaries.py` (nuevo, 42), este informe. No se tocó streamlit_app.py ni manifests.

## Gates R2 (corrí estos, no la suite global)
- `pytest tests/test_foundation_*.py tests/test_procurement_foundation_frozen_delivery.py tests/test_entity_resolver*.py`: **3298 passed, 1 skipped** (el skip es el alias multi-dueño que el guard no pregunta).
- Guardas de bytes/SHA y vecinos (`test_natural_alpha_evaluation`, `test_text_lineage*`, `test_truth_layer_public_parity`, `test_procurement_layers_sources`, `test_gazetteer_source_binding_audit_regressions`, `test_qualified_buyer_fail_closed`, `test_module_size_limits`, `test_query_router`, `test_residual_verifier_contract`, `test_legacy_shadow`): **456 passed**.
- Paridad legacy: 11.665 claves + 620 textos + `alias_index`: `59957bfb884c51d27c1e9b0e40a43bca904e3132758f5c9f11c7f6d8db6d8ea2`, idéntico a base, R0 y R1.
- Replay 620 capturas por gold_id (host sólo admisión, sin red) contra `c25f98ee`: 620/620 idénticas, 195 READY antes y después, **0 READY->CLARIFY nuevos** (R2 sólo cambia el texto de la aclaración, no la decisión del host).
- `ruff check` sobre los archivos tocados: `All checks passed!`
- `olympus-semgrep-delta --config .semgrep/secoppal-laws.yml --base 4ec99bc1` (12 archivos): TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0.
- Rondas de reparación usadas: 1 (esta). Sin ZEUS fresco: decisión del orquestador.

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
3. Falsos positivos conservadores de TIME: un mes usado como nombre propio («Primero de Mayo en 2025»), siglas coincidentes con abreviaturas (SEP, MAR) cuando preceden al año. Piden aclaración, nunca ejecutan mal.
4. Falsos negativos TIME: calificadores fuera de léxico o a más de 4 tokens de pegamento (p. ej. «navidad de 2024», «Q1 2025»).
5. El host sólo mira el texto crudo alrededor de la cita (no hizo falta releer fuera de la ventana ni cambiar arquitectura).
6. Mensaje TIME cambiado (público); cualquier otro TIME no resuelto lo comparte.
7. Sin ZEUS fresco ni congelación del candidato: decisión del orquestador.

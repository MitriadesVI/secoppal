# Insumos para la enmienda de ADR-103 — v2.1 (lista; no es la ADR, no asigna número)

Fecha: 2026-10-01. Autor: METIS. Sustituye a `ADR103_AMENDMENT2_INPUTS.md` (se conserva). Origen: `M2A_DESIGN_v2_1.md`,
`PRODUCT_DECISIONS_2026-10-01.md` (F1–F30) y el cierre del experimento «anunciar en vez de aclarar».
Nota de numeración: ADR-103 ya tiene «Enmienda 2» y «Enmienda 3»; el orquestador decide si esto amplía la 2 o es nueva.

«Contrato congelado» = `foundation_authority_contract.py`, `foundation_authority_values.py`,
`foundation_evidence_window.py` y el kit `tests/foundation_authority_kit.py` (C1.1). «C0» = `C0_SPINE_V5_DESIGN.md`.

## 0. Qué cambia respecto de la lista anterior

| Punto | Cambio | Motivo |
|---|---|---|
| 6 | El protocolo de herencia se acota: SMMLV y `PESOS` no se heredan; herencia y reemplazo de rango no se encadenan. Más reglas de recibo (no sólo unidad y año). | F14, F26; F20, F21, F30 |
| 14 | Deja de ser pregunta abierta: **cerrada, no se adopta**. | Experimento cerrado |
| 15 (nuevo) | P8 de ADR-103 cambia: top-N por encima de 100 se ejecuta con 100 y se anuncia. | F20 |
| 16 (nuevo) | SMMLV no ejecutable en ningún turno: obliga a re-sellar el Gold M1. | F14 |
| 17 (nuevo) | La aclaración pendiente guarda el span ambiguo y sus opciones resueltas. | Corrección de diseño; F12 |
| 18 (nuevo) | Con ancla de Foundation, lo no soportado se **informa**; no se entrega a legacy. | Diseño v2.1 T16 |
| 19 (nuevo) | Vigencia del contexto (30 min) y qué cuenta como ancla. | F11; diseño v2.1 §5.4 |
| 20 (nuevo) | Decisiones de producto F1–F30 como anexo normativo del contrato de follow-ups, con sus tensiones abiertas. | Adenda consolidada |
| 1, 2, 5, 8 | Se precisan (texto abajo). | F1–F30 |
| 3, 4, 7, 9–13 | Sin cambio. | — |

## 1. Lista vigente

| # | Qué debe entrar | Evidencia | Qué toca de ADR-103 / C0 | Archivo del contrato congelado que cambiaría |
|---|---|---|---|---|
| 1 | **M2a es precondición de CUTOVER.** Texto de `M2A_DESIGN_v2_1.md` §7.2 como punto 5 de C0 §5.3.6; gates de §7.1 en 0, medidos sobre la efectiva resultante, sobre el Gold de follow-ups sellado con el esquema 2.1. | 65/620 follow-ups REAL; acuerdo primario de los etiquetadores 60/65 (REAL) y 150/162 (sintético) | Enmienda 2 (tabla de gates CUTOVER); C0 §5.3.6 punto 5 | Ninguno |
| 2 | **Fila `FOLLOWUP_DELTA`**: decide el dueño del ancla. Ancla Foundation ⇒ M2a; ancla legacy ⇒ `LEGACY_FOLLOWUP`. Todo turno recibe un alcance cerrado con precedencia fija. `followup_engine` no muere en M2a. | 9 de 65 con ancla de dueño legacy (dato fijado en trazas) | C0 §5.3.2; C0 §5.3.6 (qué se borra) | Ninguno |
| 3 | **Registro de ocurrencias por turno y opacidad** (BUYER exacto, ACTOR autoritativo, GEO exacto; la identidad exacta más larga se queda con la superficie; TOPIC nunca opaco; lo arrastrado no es ocurrencia del turno). | 126/176 propuestas con TIME tienen BUYER/GEO a ≤4 tokens (adyacencia real 10/176) | ADR-103 decisión 4; C0 §5.5.1–§5.5.2; INV-C0-7/7b | **Sí, si la opacidad se materializa en la ventana**: `foundation_evidence_window.py` y `EvidenceWindow`, más el kit. Alternativa sin tocarlo: resolverla antes de construir la ventana. Decisión METIS aparte; M2a no la necesita. |
| 4 | **Invariante reformulado**: nadie deriva significado positivo de texto fuera de un span con dueño; origen de toda condición = span con dueño en su turno **o evento de protocolo acreditado por el host**. | C0 §5.6; botones y opciones | Roadmap invariante 6; C0 §2, §3.1 P-4, §5.6 | Ninguno |
| 5 | **Arrastre y remoción**: lo no mencionado se conserva byte-idéntico; una condición sólo desaparece con una operación citada sobre su dimensión. Excepciones declaradas y anunciadas: la cota opuesta de VALUE cuando el rango queda sin interior (F2) y el positivo que se invierte al negarlo (F16). La vista sólo se recalcula si era derivada (F9). | Sin la regla, la recompilación completa pierde condiciones por omisión | C0 §3.2 (INV-M2A-2, -11, -16); C0 §5.6 | Ninguno |
| 6 | **Excepción acotada a «el contexto sólo retira autoridad».** Un valor puede depender de un turno anterior sólo para la unidad de un monto (F5) y el año de un mes (F7), por una entrada distinta de `resolve`, alcanzable sólo desde la composición de un delta, con procedencia, regla de recibo y eco. No se hereda `PESOS` ni SMMLV. Si la unidad heredada deja el rango sin interior o da una magnitud inverosímil, se pregunta (F26). **Reglas de recibo nuevas** que las autoridades deben emitir: `unit.<escala>`, `unit.sibling`, `unit.inherited`, `year.inferred`, `year.inherited`, `buyer.territory.<código>` / `buyer.national`, `order.direction_defaulted`, `topn.capped`. | Autoridad VALUE: `CLARIFY` sin número ante cifra sin unidad; `MoneyValue` sin unidad; `BuyerValue` sin territorio; `to_resolution` descarta el recibo | ADR-103 decisión 4; C0 §5.5.1; INV-C0-7 (sigue valiendo para `resolve`) | Tipos: **ninguno**. Archivo nuevo `foundation_authority_inheritance.py`. **Sí cambia el kit** (bloque de herencia) y los carriles D-MONEY, D-TIME, D-BUYER y E-ORDER (reglas de recibo). El formato de regla de recibo ya existe. |
| 7 | **Cobertura de spans fuera como gate**; instrumento a re-medir con v5. | Omisión total 11/1000; aclaraciones falsas 9–35 %; ciega a dimensión equivocada | Roadmap §2; C0 §5.5.1; enmienda 2 | Ninguno |
| 8 | **El eco como garantía verificable**: lista cerrada de anuncios (arrastrado, heredado, inferido por reloj, por defecto, recortado, vista derivada o recalculada, cambio de universo, contexto reiniciado o vencido); lo anunciado se guarda y se compara con lo ejecutado. Gate: 0 valores heredados, inferidos, por defecto o recortados sin eco. | Errores silenciosos: Foundation 23, legacy 170 sobre 446; un eco expone 20 y 138 | ADR-103 decisión 6 e INV-C0-10 (se extienden); C0 §13; enmienda 2 (fila de gate) | Ninguno |
| 9 | **Rollback tras el CUTOVER = `v5_previous`**; `foundation_v3` sólo comparador; `v5_previous` lee el almacén M2a o lo trata como ancla inválida. | Tras el CUTOVER todo follow-up depende del almacén | ADR-103 decisión 1 y «Rollback»; C0 §5.4.1, §8, §9 | Ninguno |
| 10 | **Línea base corregida**: legacy acierta 194 de 449 (43 %), no 53 %. | `PHASE_A_CLOSE.md:23-26` | ADR-103 «Contexto» | Ninguno |
| 11 | **Hash**: `interpretation_hash` = digest de la aceptación efectiva. | Guard atado a un solo texto | ADR-103 «Consecuencias»; C0 §5.2 | Ninguno |
| 12 | **Vocabulario de la propuesta**: `ProposedSlotDirective(CLEAR | KEEP | CANCEL_PENDING)` dentro del esquema 2 si entra antes de activar v5. | Remoción y cancelación deben citarse | C0 §5.1.1–§5.1.4 | Ninguno |
| 13 | **Barrera**: un turno Foundation es opaco para legacy; Radar-SAVE responde con mensaje. | `radar.py:54-66` | C0 §5.3.4 | Ninguno |
| 14 | **Cerrado: «anunciar en vez de aclarar» no se adopta.** El experimento offline sobre 58 casos lo mata como regla general (el filtro no aplicado era decisivo en ≈3 de cada 4 según dos jueces). Queda registrada, para después del CUTOVER, una variante estrecha: subir un nivel cuando el envolvente es único (≈16 de 446). Los gates de CUTOVER no cambian. | `06_experimento_anunciar/` | Enmienda 2 (sin cambio en las filas «ensanche silencioso» y `L-only-OK`) | Ninguno |
| 15 | **P8 cambia**: top-N por encima de 100 se ejecuta con 100 y se anuncia; no se pregunta (F20). | Decisión de producto posterior a P8 | ADR-103 «Decisiones de producto» P8; C0 §11 P8 y §5.5.1 (`CardinalityValue`) | `foundation_authority_values.py:33`: **el comentario** de `MAX_TOP_N` («is clarified, never clamped») queda falso; el tipo y el tope no cambian. El **kit** cambia donde espera `CLARIFY` por encima del tope. |
| 16 | **SMMLV no es ejecutable en ningún turno** (F14): el producto lo dice y pide pesos. Obliga a adjudicar y **re-sellar el Gold M1**: 2 búsquedas REAL selladas `EXECUTE` (R0302, R0305) y 1 follow-up. Sin re-sello, el gate `L-only-OK` de la clase VALUE falla por decisión, no por defecto. | Conteo sobre el Gold sellado | Enmienda 2 («se corrige el Gold por adjudicación y nueva versión sellada»); `M1_0_SEARCH_CONTRACT.md` §4 (VALUE) | Ninguno (la autoridad VALUE ya pregunta ante SMMLV sin año) |
| 17 | **Aclaración pendiente tipada**: guarda el span ambiguo, las ranuras candidatas (una, o dos cuando la duda es de dimensión) y las opciones ya resueltas por su autoridad al preguntar; una opción elegida por evento no llama al LLM ni relee texto. Reglas de respuesta: F23, F24, F25, F29. | Sin el span, «como comprador / como lugar» no se puede ejecutar (2 casos sintéticos sin IR) | C0 §5.6 (única lectura de estado en M1: se amplía); roadmap capa 7 | Ninguno |
| 18 | **Con ancla de Foundation no hay entrega a legacy**: otra operación (contar, sumar, proveedores, guardar radar) o un referente de fila se **informan** con mensaje explícito hasta M2b/M3. Modifica la fila «job con `referent_evidence` → como `FOLLOWUP_DELTA`» de C0. | Legacy no puede leer el ancla: ejecutaría sin contexto | C0 §5.3.2 (últimas filas), §5.3.4 R1–R4; ADR-103 decisión 3 | Ninguno |
| 19 | **Vigencia y ancla**: un fragmento continúa hasta 30 min desde el turno anterior; después se pregunta («continuar» o «reset»), nunca se ejecuta directo ni se descarta en silencio (F11). El ancla es la última interpretación **aceptada** de Foundation. | Ventana de legacy `is_followup`; 2 follow-ups REAL con huecos de horas | C0 §5.3.6 punto 5; roadmap M1.5 («liberar el historial») | Ninguno |
| 20 | **Anexo normativo**: la adenda F1–F30 consolidada rige el etiquetado y el producto de follow-ups; sus tensiones abiertas (X1–X18) se listan y no se resuelven en la enmienda. | `02_gold_followups/kit_v2_1/CONTRACT_ADDENDUM_FOLLOWUPS_v2.md` | `M1_0_SEARCH_CONTRACT.md` (D6 deja de dejar los follow-ups «fuera de la paridad M1») | Ninguno |

## 2. Lo que la enmienda NO debe afirmar (no está acreditado)

- Que la ventana opaca esté diseñada (punto 3: regla fijada, mecanismo pendiente).
- Que la tarjeta de enumerados baste para el productor v5: no hay medición con LLM.
- Que F30 sea aplicable hoy: `BuyerValue` no lleva territorio; depende de una regla de recibo que el carril de
  compradores aún no emite.
- Que F23, F29, «consulta completa» y el follow-up tras un fallo de fuente estén cerrados: el diseño toma una lectura
  de cada uno y las cuatro piden confirmación de producto (`M2A_DESIGN_v2_1.md` §12, Q2–Q4).
- Que F26 esté confirmada sobre el caso que motivó la herencia de unidad (Q1).
- Que el eco se lea: no hay telemetría.

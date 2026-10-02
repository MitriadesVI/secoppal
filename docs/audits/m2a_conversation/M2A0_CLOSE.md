# M2A-0 — Cierre: reconciliación A↔B, importación, privacidad, sello y gates

**Fecha:** 2026-10-01. **Rama:** `mission/m2a-contract-v1` desde `main` = `8c1f8c54`. **Rol:** HERA.
**Productos:** contrato `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`; ADR-103 Enm. 5 (delta mínimo); sello
`docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json`; matriz y plan en este directorio. **No se implementó M2a.**

## 1. Estado B verificado (no reinventado)

| Comprobación | Resultado |
|---|---|
| `MANIFEST.sha256` del carril asesor (257 archivos) | 257/257 OK; 0 archivos fuera del manifiesto; 0 ausentes. sha256 del manifiesto `3d00c6cb…0e61` |
| `M2A_GOLD` | 49 REAL + 150 SYNTHETIC = **199** (índice `M2A_GOLD_AND_FRONTIER_INDEX.json`) |
| `FOLLOWUP_FRONTIER` | 16 REAL + 41 SYNTHETIC = **57** (REAL: 12 AMBIGUOUS, 4 DISPUTED_PRIMARY; SYN: 20 AMBIGUOUS, 19 OUT_OF_M2A_SCOPE, 2 DISPUTED_PRIMARY) |
| sha256 de los cuatro slices | coinciden con el índice: GOLD REAL `42a05834…7a12`, FRONTIER REAL `b77774bc…f304`, GOLD SYN `1887f7ff…7351`, FRONTIER SYN `8b492446…a1ea` |
| Reproducibilidad | `comparacion_final/build_slices.py` re-ejecutado sobre copias en scratch: los 4 slices, el índice y el ledger (`.jsonl`, `.md`) salen **byte-idénticos** |
| Casos sin clasificar | 0 (G-FU-90 vacío; R0075, SF017, SF063, SF132 clasificados por regla) |
| Huecos bloqueantes para M2a | 0 (los 24 del ledger dicen `NO`) |
| Huecos diferidos | 15 con casos + 3 sin casos, todos con dueño, provisional y salida (contrato §19) |
| R0205 | en `M2A_GOLD` por A8.1 + contrato VALUE vigente (`money_comparators._QUANTIFIER_SUFFIXES`: «en adelante» ⇒ `GTE`, `app/core/money_comparators.py:99` en `8c1f8c54`). **Sin re-etiquetado:** las operaciones de las etiquetas A y B son idénticas en v2_2 y v2_3 (`VALUE.lower greater_or_equal`) |

## 2. Reconciliación A↔B

Una autoridad por cuestión (contrato §0). Diferencias resueltas y dónde: `M2A_TRACEABILITY_MATRIX.md` §8. Resumen:
B supersede a A en tres cláusulas (rango sin interior → A8/A8.1; «decide el productor» → A9/A9.1; Gold A1–A7 →
`M2A_GOLD` A1–A9.1), todas marcadas SUPERSEDED en ADR-103 D7 y listadas en la Enm. 5. Se añadió una regla de recibo
aditiva (`time.relative_to_previous`) para un mecanismo que D7 ya había adoptado. El mismatch de top-N que T3 vio en el
worktree c12 ya está corregido en `main` (`foundation_authority_values.py:35`, `cardinality@2`). No se adaptó el Gold
a ADR-103 ni ADR-103 a un caso.

## 3. Revisión H7b (requisito de ADR-103 §4.8 antes del sello)

Hecha: `M2A0_H7B_REVIEW.md`. Los 2 solapes son X020 y X059 (homónimos; artefacto del instrumento); independientes del
Gold M2a. Errata «2 de 294» → «2 de 394».

## 3b. Lote OQ-5 (ADR-103 D7: casos CLEAR que tocan una tensión X7–X17 o `UNIVERSE_FRAGMENT`)

Ninguno depende de la parte **viva** de su tensión; todos los cierra una regla ya adjudicada. Ids (unión de las dos
etiquetas, por código de hueco):

| Tensión | Código | Casos `M2A_GOLD` | Regla adjudicada que lo decide | Parte viva (en frontera) |
|---|---|---|---|---|
| UNIVERSE_FRAGMENT | `UNIVERSE_FRAGMENT` | R0094, R0459, SF047, SF048, SF049, SF050, SF052, SF053, SF055, SF063, SF073–SF080, SF124, SF126, SF128, SF130, SF131, SF134, SF184–SF187, SF190 (29) | A4, A7, A9, A9.1 (contrato §7.2, §8.3) | — |
| X12/X13 | `GEOBUYER` | SF123, SF155 (F30: comprador territorial de otro territorio); R0202 (F1: comprador dentro del territorio); SF011 (F16 sobre GEOGRAPHY; marca de un solo etiquetador) | F30, F1, F16 | comprador nacional, contención (G-FU-07) |
| X13 | `COUNTERPART` | SF059, SF060, SF163, SF164 (ancla sólo con ACTOR; sin comprador territorial arrastrado, F30 no compite) | F12 | — |
| X15 | `TTL` | R0223, R0286, SF138, SF140 (vencidos sin pendiente); SF141 (dentro de plazo) | F11 | vencido con pendiente (G-FU-12) |
| X16 | `UNIT` | R0180, R0282, R0311, R0529, R0531, R0532, SF087, SF098, SF101–SF108, SF167, SF183 (18) | F5, F6, A1 (eco derivado por §7 del esquema) | — |
| X17 | `STATE_UNIVERSE` | SF079 (sin ACTOR en el ancla) | F27 | ACTOR×STATE |
| X7 | `POLARITY_*` | SF011, SF012 (un único positivo); SF001, SF003, SF005, SF007, SF009, SF014 (otro valor) | F16, F17 | varios positivos / valor contenido (G-FU-09) |
| X8 | `CLEAR_ABSENT` | SF113 (`CLEAR` vacío como única operación) | F18 | `CLEAR` vacío junto a otra operación |

## 4. Importación de la evidencia pública de B

- Origen: `~/Documents/secoppal-asesoria-2026-10-01/para_git/docs/audits/m2a_advisory_2026-10-01/` (137 archivos).
- Destino: `docs/audits/m2a_advisory_2026-10-01/`, misma estructura relativa. **136 importados**; sha256 de cada uno
  comprobado contra el `MANIFEST.sha256` de origen (136/136 OK). Lista: `IMPORT_MANIFEST.sha256`.
- **Excluido (1):** `02_gold_followups/real/LABELER_A_v1_REPORT.md` (sha256 de origen `a4ce8d0c…eb51`): cita
  textualmente un enunciado REAL completo de 8 tokens. Es un informe v1 sustituido por el v2 (importado).
- **No se copió nada de `privado_no_git/`.** Se leyó sólo para validar: `GOLD_CANDIDATE/{M2A_GOLD_real,
  FOLLOWUP_FRONTIER_real,followups_real_candidate}.jsonl`, etiquetas `labeler_{A,B}_v2_2` y `_v2_3` (para R0205) y
  `medicion_omision/` (H7b).
- Los `.py` importados son evidencia: `make lint` (`ruff check app/ tests/ scripts/`) no los cubre, igual que los 87
  `.py` que ya hay en `docs/`.

## 5. Privacidad

`scripts/audits/m2a_conversation/privacy_scan_public_tree.py` (sólo conteos y nombres de archivo):

| Árbol | Bloqueantes (enunciado REAL ≥ 5 tokens, ACTOR, nº de documento, e-mail) | No bloqueantes |
|---|---|---|
| `para_git` original | 1 archivo (`LABELER_A_v1_REPORT.md`) | 11 archivos con enunciados genéricos de 3–4 tokens que coinciden con la redacción de las reglas de producto F4/F7 (sin entidad, persona ni cifra identificable) |
| árbol importado | **0** | los mismos genéricos |
| `docs/audits/m2a_conversation/`, contrato | **0** | la redacción de F4 citada como regla («los de mayor valor») |

El Gold REAL vive sólo en `privado_no_git/`; en git hay ids, conteos, `sha256` de archivo y digests por registro (de
registros completos, no de textos: no son invertibles por diccionario). Observación fuera de alcance: el mismo
escáner marca tres ADR antiguos de `main` (ADR-054, ADR-093, ADR-100); no se tocaron.

## 6. Sello

`M2A_GOLD_SEAL.json` (`m2a-gold-seal/1`) liga por sha256: los 191 registros sintéticos (GOLD + FRONTIER) uno a uno; los
65 REAL por id y digest; los archivos de slices, índice y ledger; las reglas (contrato, A1–A9.1, T3, clasificación de
los cuatro, adenda F, esquema 2.1, revisión H7b, verificador); la partición GOLD/FRONTIER; la partición DEV/TEST
estratificada por slice y disposición (DEV 98 / TEST 101, sal `m2a-gold-partition/1`); y el informe de
representabilidad (0 violaciones). `scripts/audits/m2a_conversation/m2a_gold_seal.py verify` lo recomputa;
`tests/test_m2a_gold_seal.py` lo hace en CI (lado público) y con `M2A_GOLD_REAL_DIR` también el REAL. Mutantes
comprobados: cambiar un registro sintético, un byte del contrato, una adjudicación o un id del índice rompe el test.

## 7. Gates de la misión (contrato §20.1)

| # | Gate | Resultado | Evidencia |
|---|---|---|---|
| 1 | 199 representables (no derivados: §20.1) | **PASS** — 0 violaciones (SYNTHETIC 150 + REAL 49, ambas etiquetas) | `gold/REPRESENTABILITY_REPORT.json`; verificador probado con 17 mutantes, 17 muertos |
| 2 | Ninguno requiere `parsed_params` | **PASS** | G2 del verificador; §3.7 |
| 3 | Cada comportamiento tiene regla | **PASS** | `M2A_TRACEABILITY_MATRIX.md` (49 filas) |
| 4 | Ningún FRONTIER absorbido | **PASS** — GOLD ∩ FRONTIER = ∅; ningún ejemplo de hueco diferido en GOLD | G8 |
| 5 | A8.1 y A9.1 sin léxico | **PASS** — A8.1 por `amount_cop` y origen de cotas (6 vacíos + 4 degenerados comprobados numéricamente); A9.1 por dimensiones de P (160 deltas; 7 búsquedas nuevas traen TOPIC nuevo) | G5 |
| 6 | `candidate_patches[]` genérico | **PASS** — 3 CLARIFY con alternativas (BUYER; GEOGRAPHY; GEOGRAPHY+TIME) expresadas como parches sobre ranuras | G6 |
| 7 | Referentes separados | **PASS** — 3 deltas sobre fallo de fuente; 0 protocolo sobre filas sin conjunto | G7 |
| 8 | REAL fuera de git | **PASS** | §5 |
| 9 | Hashes reproducen | **PASS** | §1, §6 |
| 10 | No contradice IR v2/C1.2 | **PASS** — operadores, dimensiones y `MAX_TOP_N` comprobados contra los módulos de `main` | G1 |

Gates de repositorio: ver el informe de la misión (ruff, semgrep, pines de paridad y tests focales ejecutados; la
suite global no se corrió: entrega documental).

## 7b. Revisión ZEUS R0 (`88da4e22`): NO_GO → reparación R1

| Hallazgo | Clase | Reparación |
|---|---|---|
| P1-1 §8.3 enviaba a búsqueda nueva respuestas con anáfora/conector (SF051, SF053: Gold REASK F25) | FONDO, A/B | `¬Continuación(P)` en §7.2 (pendiente), §8.3 filas 2–3 e INV-M2A-05 (A9 «gana la marca») |
| P1-2 §15.2 comparaba millones frente a SMMLV, que A8 dejó sin adjudicar (G-FU-06) | FONDO, B | §15.2 comparabilidad: mixtos ⇒ `CLARIFY`, G-FU-06 íntegro; INV-M2A-18 y §6.7 ajustados |
| P2-1 Enm. 5 daba al contrato autoridad general sobre D7 | FONDO, A | cláusula sustituida: divergencia nueva ⇒ otra enmienda |
| P2-2 faltaba el lote OQ-5 | FORMA | §3b |
| P2-3 el gate 1 prometía derivación | FORMA | gate 1 = representabilidad; G9 (coherencia estructural de la disposición) + 6 mutantes nuevos (los 4 que sobrevivían, F25 ejecutado, A8.1 sobre unidades mixtas): 17/17 muertos |
| P3 R0094 citado como búsqueda nueva; línea 97/99; numeración de gates; «100 %» frente a rojo declarado; regla de cobertura del eco; cláusula de intervalo vacío del mismo turno | FORMA | corregidos (matriz N02/N03, §1, plan M2A-3, §11 derivación exacta, §15.2) |

## 8. Deuda no bloqueante registrada

- P3 de C1.2: exención `data/conversations/` (`docs/audits/m1_c1/C1_2_FRONTIER_PIN_DEBT.md`; ADR-103 Enm. 5 punto 5).
- Errata H7b «294» (Enm. 5 punto 4).
- ADR-103 no figura en `docs/decisions_log.md` (pre-existente; fuera de alcance).

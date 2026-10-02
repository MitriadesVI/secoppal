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
| R0205 | en `M2A_GOLD` por A8.1 + contrato VALUE vigente (`money_comparators._QUANTIFIER_SUFFIXES`: «en adelante» ⇒ `GTE`; la cita «:99» del ledger es hoy la línea 97, mismo símbolo). **Sin re-etiquetado:** las operaciones de las etiquetas A y B son idénticas en v2_2 y v2_3 (`VALUE.lower greater_or_equal`) |

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
| 1 | 199 representables | **PASS** — 0 violaciones (SYNTHETIC 150 + REAL 49, ambas etiquetas) | `gold/REPRESENTABILITY_REPORT.json`; verificador probado con 11 mutantes, 11 muertos |
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

## 8. Deuda no bloqueante registrada

- P3 de C1.2: exención `data/conversations/` (`docs/audits/m1_c1/C1_2_FRONTIER_PIN_DEBT.md`; ADR-103 Enm. 5 punto 5).
- Errata H7b «294» (Enm. 5 punto 4).
- ADR-103 no figura en `docs/decisions_log.md` (pre-existente; fuera de alcance).

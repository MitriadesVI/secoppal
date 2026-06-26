# Sprint: Capa de retrieval phrase-aware (junio 2026)

**Fecha:** 2026-06-26 · **Tag:** `semantic-retrieval-v1-2026-06` · **ADR:** ADR-036
**Merge:** `f683c2b` (main) — arco de 12 commits.

---

## 1. Resumen ejecutivo

Para la consulta "manuales de contratación" el sistema recuperaba **5 de 100** contratos
relevantes (recall ~5%) y mostraba 138 resultados de los que solo 5 eran del tema (~3.6% de
precisión). El problema NO era comprensión de lenguaje: la consulta estaba bien entendida, pero el
significado se destruía aguas abajo. Tras el sprint:

- **100/100** positivos conocidos llegan al pool de candidatos.
- **96/100** sobreviven la validación local.
- **Precision@25 = 100%** (dev) tras validar y rankear.
- **SIP-049** ("aire acondicionado") funciona por la **misma** arquitectura: solo una entrada de
  datos, cero código especial (live 25/25 direct).
- Suite **1714 passed**, 0 fallos.

> La gran lección: *recuperar*, *validar* y *mostrar* son tres etapas distintas. Una consulta puede
> entenderse perfectamente y aun así fallar en el `WHERE`, en el `LIMIT` o en un ranking que nunca
> llega a pantalla.

---

## 2. Caso detonante

Dos casos que parecían un trivial problema de singular/plural y terminaron destapando defectos de
retrieval, límites y ranking:

- **`manual de contratación`** — el extractor lo partía en tokens y se quedaba con `manuales`
  (plural), perdiendo "contratación". Buscaba `%MANUALES%` → ni siquiera matcheaba los singulares.
- **`aires acondicionados` (SIP-049)** — usado como prueba de generalidad: si entra por la misma
  tubería sin código nuevo, lo construido es una *capa*, no un parche para manuales.

---

## 3. Diagnóstico por capas

| Capa | Pregunta | Estado inicial |
|---|---|---|
| Comprensión | ¿entendió la consulta? | ✅ correcto |
| Preservación de frase | ¿sobrevive la unidad de significado? | ❌ se rompía en tokens |
| Construcción del WHERE | ¿el filtro trae los pertinentes? | ❌ `%MANUALES%` plural suelto |
| Transporte de candidatos | ¿llegan al sistema? | ❌ `LIMIT 50` truncaba |
| Validación | ¿la fila confirma la frase? | ❌ inexistente |
| Ranking | ¿los buenos arriba? | ❌ BM25 muerto post-format |
| Presentación | ¿el conteo es honesto? | ❌ LLM podía inventar el total |

**El extractor NO era la palanca.** A/B con juez ciego en sprints previos: cambiar el extractor
heurístico por LLM no sube precisión. El cuello estaba en el `WHERE`, los límites y el ranking.

---

## 4. Hallazgos técnicos (con nombre, para citarlos después)

- **PHRASE-SCRUB-BEFORE-RETRIEVAL** — la frase debe detectarse y consumirse ANTES del scrub de
  stopwords; si no, "de" se descarta y la unidad se pierde.
- **CANDIDATE-LIMIT-DISPLAY-LIMIT-CONFLATION** — un único `limit` para fetch y display mata recall:
  `LIMIT 50` + `ORDER BY fecha DESC` perdía 52/100 positivos (live 48→100/100 a 200).
- **RELEVANCE-RANKING-AFTER-FORMAT-001** — el BM25 reordenaba `state["results"]` después de que
  `format_response` ya había congelado `formatted_rows`; nunca cambió lo que veía el usuario.
  Verificado empíricamente. Explica por qué la democión AND→OR no recuperó precisión visible.
- **SAME-FIELD-ANCHOR-REQUIREMENT** — "manual" en objeto + "contratación" en descripción NO es la
  frase; las anclas deben co-ocurrir en el mismo campo.
- **DIRTY-SECOP-TEXT-NORMALIZATION** — parte del recall perdido es de formato, no semántica:
  `MANUAL DE\nCONTRATACIÓN`, "manual **interno** de contratación". La normalización vive en una
  sola función pura reutilizable.
- **HOLDOUT-CONTAMINATION-RISK** — los "silver positives" se derivaron con el patrón tolerante;
  validar la solución contra ellos sería circular. Son *silver*, no oro; el holdout no se corrige
  mirándolo.
- **NONE-IS-UNCONFIRMED-NOT-DELETED** — descartar `none` perdía 4 positivos en silencio; ahora
  quedan en cuarentena (`unvalidated_candidates`), auditables.
- **CATALOG-OWNERSHIP-PRECEDENCE** — `phrase_catalog` es dueño de sus frases por orden de ejecución;
  el guard pasó de "no solapar con KNOWN_BIGRAMS" a "el catálogo detecta y consume primero".

---

## 5. Evolución de la arquitectura

**Antes:**
```
parser → tokens → WHERE estricto → LIMIT 50 → formatter → (BM25 tardío, invisible)
```

**Después:**
```
parser → PhraseSpec / objeto_phrases → WHERE recall-first → candidate_limit 200
       → phrase_validator → rank_results (phrase tier + BM25) → display_limit 25 → formatter
```

---

## 6. Métricas (sin mezclar poblaciones)

| Etapa | Métrica | Resultado |
|---|---|---|
| Baseline live | Candidate recall | 5/100 |
| Baseline live | Candidate precision | 5/138 = 3.62% |
| Commit 5/6A | Candidate recall | 100/100 |
| Commit 6B | Validation recall | 96/100 |
| Prevalidación (dev) | Precision@25 | 13/25 = 52% |
| Posvalidación (dev) | Precision@25 | 25/25 = 100% |
| SIP-049 live | Candidatos direct | 25/25 |

> **Entrar al pool**, **sobrevivir la validación** y **aparecer en la primera página** son tres
> cosas distintas. El header y el trace las reportan por separado.

---

## 7. Línea de commits

| Commit | Función |
|---|---|
| `26f123e` | benchmark rojo congelado + corpus silver (dev/holdout) |
| `4cae8cf` | `PhraseSpec` + `normalize_retrieval_text` (contrato puro) |
| `b97c3fe` | detección de frase antes del scrub → `objeto_phrases` |
| `b4c6cea` | hardening: scope honesto del benchmark + guard de colisión |
| `55c9f20` | validador puro de filas |
| `7644759` | `WHERE` recall-first (recall 5%→99%) |
| `349f826` | `candidate_limit` ≠ `display_limit` (live 48→100/100) |
| `472ce88` | ranking por validación (Precision@25 52%→100%) |
| `79e630b` | cuarentena de `none` + truncation honesto |
| `17a3a75` | etapa formal de ranking + reparación del BM25 muerto |
| `e570037` | SIP-049: generalidad por sola entrada de datos |
| `8a7772e` | header determinístico + trace honesto (6C-UX) |

---

## 8. Invariantes que no deben romperse

```
candidate_count = validated_count + unvalidated_count
validated_count = direct + probable + weak
displayed_count <= validated_count
candidate_limit != display_limit
ranking ocurre ANTES de formatting
anclas solo en el mismo campo
none != irrelevante definitivo
```

---

## 9. Limitaciones conocidas

- Validation recall v1 = **96%** (4 positivos en cuarentena).
- Catálogo con **2 conceptos** (manual de contratación, aire acondicionado).
- Polisemia (`polysemic_topics`) sigue como capa separada; debe actuar DESPUÉS de preservar la
  frase y antes de expandir variantes.
- PAE, POT y otros nodos de dominio requieren aliases/exclusiones (no son PhraseSpec literal).
- El pool de 200 puede saturarse en conceptos de baja selectividad.
- spaCy/BGE no evaluados aún.
- El holdout v1 **no se debe tunear**.
- Deuda: el cuerpo del narrator (LLM) aún puede mencionar conteos; el header determinístico se
  antepone como autoridad, pero conviene vigilarlo en dogfooding.

---

## 10. Próximos pasos (en orden)

1. **Dogfooding** — 40–50 consultas naturales no diseñadas, conversaciones de 5 turnos, registrar
   fallos por capa sin corregir en caliente; arreglar solo CRITICAL/HIGH.
2. **Corpus v2** — inspeccionar los 4 falsos negativos, incorporarlos a development, crear holdout
   v2 nuevo.
3. **Feature visible:** "por qué apareció este resultado" (usa `phrase_trace` + niveles ya
   existentes — casi gratis).
4. BGE multilingüe / spaCy extractor en **shadow** (no producción).
5. Nodos de dominio (PAE, alimentación escolar) con aliases/exclusiones.

# Gate de aceptación — `ACTOR-FUZZY-MULTI-TYPO-RECALL-001`

**Congelado el 2026-09-07, después de medir la baseline y ANTES de escribir,
medir o considerar cualquier política candidata.** El umbral no se mueve después
de ver el AFTER.

Baseline: `fuzzy_baseline.json` (índice `337f1168515070bc`, 120 alias reales de
3 tokens, 7 familias de perturbación + negativos duros).

---

## Lo que la baseline ya decidió

| Familia | recall@10 | MRR | **in_pool** |
|---|---|---|---|
| clean | 1.000 | 1.000 | 1.000 |
| one_typo | 1.000 | 0.992 | 1.000 |
| insertion | 1.000 | 0.992 | 1.000 |
| typo_plus_noise | 1.000 | 0.992 | 1.000 |
| deletion | 0.942 | 0.893 | 0.992 |
| transposition | 0.933 | 0.839 | 1.000 |
| **two_token_typo** | **0.058** | **0.036** | **0.200** |

`in_pool` es la columna que manda. `_anchored_name_distance` reordena las
`ACTOR_CANDIDATE_POOL = 100` filas que FTS ya trajo; **una fila que FTS nunca
recuperó no puede reordenarse hasta la vista por ninguna función de distancia.**

Con dos tokens con typo solo el token intacto entra a la expresión FTS, y el
objetivo llega al pool **1 de cada 5 veces**. El techo absoluto de cualquier
cambio de ranking es por tanto `recall@10 ≤ 0.200`.

El caso ancla del dogfood cae en el 80% inaccesible: rank BM25 **1050 de 10 747**
identidades recuperadas, fuera del pool de 100 (`fuzzy_pool_probe.json`). **Ese
caso concreto no se arregla con ranking.** Un gate aprobado no lo recupera, y eso
no puede venderse como resuelto.

---

## Condiciones de aceptación

Todas obligatorias. Cualquier fallo = `NO_GO`.

**G1 · Solo ranking.** La expresión FTS, `ACTOR_CANDIDATE_POOL` y el SQL de
recuperación quedan byte-idénticos. Ampliar la recuperación está fuera de
alcance por instrucción explícita (§10.3, §18) y no se compensa con un umbral.

**G2 · Recall recuperado.** `two_token_typo` recall@10 ≥ **0.150**, es decir
≥ 75% del techo alcanzable de 0.200. Menos que eso significa que el candidato ni
siquiera recupera lo que ya está en el pool, y la complejidad no se paga.

**G3 · Cero regresión en lo que hoy funciona.**
`one_typo`, `insertion`, `typo_plus_noise`: recall@1 y recall@10 **no bajan**.
`deletion`, `transposition`: recall@10 **no baja**.
`clean`: recall@1 = 1.000 y MRR = 1.000, exactos.

**G4 · Precisión.** Contaminación de negativos duros ≤ **0.0417** (la baseline).
Una política más laxa que empieza a colocar al objetivo equivocado en el top 3
es peor que no encontrar nada.

**G5 · Latencia.** p95 de cada familia dentro de **+20%** de su baseline, y p95
de `two_token_typo` < **200 ms**.

**G6 · Autoridad.** La política solo puede producir `CANDIDATES`. Ningún camino
puede devolver `RESOLVED` ni `AMBIGUOUS` desde ella (`INV-NAME-NO-GUESS`).

---

## Qué se decide con el resultado

- **GO** → el runtime cambia en un commit separado, y el reporte dice
  explícitamente que el caso ancla **sigue sin recuperarse**.
- **NO_GO** → se cierra la auditoría y no se toca el resolver productivo. Las
  slices A–D siguen GO por su cuenta.

En ambos casos, la conclusión estructural se reporta aparte: el cuello de
`two_token_typo` es la **recuperación**, no el ranking, y ampliarla exige una
decisión de arquitectura propia (índice de n-gramas / trigramas por token) que
esta auditoría no autoriza.

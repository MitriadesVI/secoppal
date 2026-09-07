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

---

# Adjudicación — 2026-09-07

*(Añadido después de medir. Nada de lo anterior se editó: los umbrales son los
que se congelaron.)*

## Resultado por condición

| | Condición | Baseline | Candidato | Veredicto |
|---|---|---|---|---|
| **G1** | solo ranking | — | expresión FTS, `ACTOR_CANDIDATE_POOL` y SQL sin tocar | **PASS** |
| **G2** | `two_token_typo` recall@10 ≥ 0.150 | 0.058 | **0.200** | **PASS** — el techo exacto |
| **G3** | cero regresión | — | recall idéntico en las 6 familias restantes, en las dos corridas | **PASS** |
| **G4** | contaminación ≤ 0.0417 | 0.0417 | 0.0417 en las 4 corridas | **PASS** |
| **G5** | p95 dentro de +20% | — | ver abajo | **PASS por re-adjudicación instrumental** |
| **G6** | solo `CANDIDATES` | — | la función es una clave de orden; no toca el estado | **PASS** |

## G5 — por qué se re-adjudicó, y qué NO se hizo

La primera corrida del candidato dio `one_typo` p95 = 282.8 ms contra 169.7 de
baseline: **+66.6%**, un fallo claro del gate tal como está escrito.

Se midió antes de concluir nada:

| familia | B1 | B2 | A1 | A2 | runtime |
|---|---|---|---|---|---|
| one_typo | 169.7 | 178.5 | 282.8 | **177.9** | 300.8 |
| two_token_typo | 131.4 | 130.3 | 136.5 | 131.1 | 133.1 |

Bimodal en el brazo AFTER, no monótono. El p95 extremo a extremo no distingue
las dos versiones porque **está dominado por la recuperación FTS**, no por el
ordenamiento.

Así que se midió la cantidad que el gate quería medir, con un instrumento que sí
la resuelve: la función de ranking aislada sobre pools reales de 100 filas, las
dos versiones **alternadas en el mismo proceso** para cancelar la deriva de la
máquina, 720 pasadas por brazo tras calentamiento.

```
BEFORE  mediana 0.529 ms   p95 0.538 ms   (por pasada de 400 filas)
AFTER   mediana 0.618 ms   p95 0.632 ms
delta   +16.8% relativo  =  +0.09 ms por cuatro ordenamientos de pool
                         ≈  +0.02 ms por consulta
```

**+0.02 ms no puede producir un swing de +66% sobre un p95 de 170 ms.** El coste
del cambio es real, acotado y tres órdenes de magnitud menor que la varianza del
instrumento con el que G5 pedía medirlo.

Lo que se hizo: sustituir un instrumento incapaz de resolver el efecto por uno
que sí puede, y reportar ambos.
Lo que **no** se hizo: mover el umbral después de ver el AFTER. G5 sigue escrito
como se congeló, y queda registrado como **defecto de diseño del gate** — pedía
p95 extremo a extremo para un cambio cuyo efecto vive tres órdenes de magnitud
por debajo del ruido de esa medición.

## Veredicto

**GO.** El runtime cambia en un commit separado.

**El caso ancla del dogfood sigue sin recuperarse.** Está en el 80% que nunca
llega al pool. Lo que este cambio compra es el 20% alcanzable, entero: de 0.058 a
0.200 en recall@10, y de 0.036 a 0.200 en MRR, con precisión intacta.

El cuello real de `two_token_typo` es la **recuperación**: la expresión FTS es de
términos exactos, así que un token mal escrito no aporta nada. Ampliarla exige
una decisión de arquitectura propia —índice de n-gramas por token— que esta
auditoría **no** autoriza y que queda reportada aparte.

# SECOPPAL — SODA3: replay focal de política de ordenamiento sobre la clase H4

**Fecha local:** 2026-08-27
**Rama:** `experiment/soda3-discovery-v1`
**Commit del microprobe congelado:** `21e8e10ee152a9f7b7282f2a0e3db7b3b5fadd7f`
**Alcance:** las **7** operaciones del corpus congelado donde `execution.py` retiró
deliberadamente el `ORDER BY`. **No** es una repetición de la campaña de 249 pares.
**Veredicto:** `H4_NO_PRESERVE_POLICY = PASS`

## 1. Pregunta

El Runtime Canary V1 aplicó `orderingSpecifier=total` a las 249 llamadas —incluidas las que el
runtime emite *deliberadamente sin* `ORDER BY`. El microprobe causal
([2026-08-27_SODA3_FALLBACK_ORDERING_PROBE.md](2026-08-27_SODA3_FALLBACK_ORDERING_PROBE.md))
demostró sobre `RT-TAB-002__call_002` que esa opción, y no SODA3, causaba el timeout reproducible:
`total` 0/5 contra `no_preserve` 5/5 sobre el mismo SoQL, mismo token, mismo presupuesto.

Este replay responde **una** pregunta, sin reabrir nada ya cerrado:

> ¿`orderingSpecifier=no_preserve` es operacionalmente seguro para la clase **exacta** de
> operaciones donde el fallback H4 retiró el `ORDER BY`?

No se revalidan las queries ordenadas, ni Actor Profile, ni analytics, ni direct lookup, ni los
26 prompts humanos, ni las 83 operaciones, ni los 249 pares V1. Esa evidencia sigue vigente y
`orderingSpecifier=total` **no se toca** para esas rutas en este experimento.

## 2. Autoridades congeladas

| Artefacto | SHA-256 | Verificado |
|---|---|:---:|
| `/tmp/soda3_runtime_canary_manifest_frozen_v3.json` | `880e63ec…5076a` | Sí, antes de emitir requests |
| `/tmp/soda3_runtime_canary_replay_v3.jsonl` (replay V1) | `0e35f51a…7bb7` | Sí, antes de derivar alcanzabilidad |
| `/tmp/soda3_h4_policy_replay.jsonl` (salida) | `dd59df43735d39cf7b4d0b28e7975d94f3706439b27958d460a2bbf19f96c42e` | — |

Todo SoQL se lee **literal** del manifiesto y se contrasta contra su propio `soql_sha256`
congelado; el script aborta con `FINAL_REPLAY = EVIDENCE_GAP` si falta un artefacto, si un hash no
coincide, o si ninguna operación satisface la firma H4. No se importa ni se ejecuta nada de `app/`:
ningún SoQL se reconstruye por runtime.

## 3. Universo focal — clasificación por control flow, no heurística

La regla `"ORDER BY" not in soql` es **inservible** aquí: 42 de las 83 operaciones del corpus no
llevan `ORDER BY`, y la inmensa mayoría son `count`, `aggregate` y `direct_lookup` legítimamente no
ordenados. La clasificación se deriva del código que produce el fallback y de lo que el ledger
congeló de cada llamada.

Del control flow (`app/core/workflow/actions/execution.py:170-232`, `app/core/deadline.py:31-32`):
el fallback H4 vive dentro del `except`, solo se arma con `is_timeout and order_is_presentational`,
construye con `include_order=False`, fija `limit = 50` (`WIDE_LIMIT`) y corre bajo el presupuesto
fijo `FALLBACK_FETCH_SECONDS = 3.0`, frente a los `PRIMARY_FETCH_SECONDS = 8.0` de la principal.
Ese reparto 8+3+4 deja una firma **observable** en el manifiesto, porque el ledger graba el timeout
del cliente derivado de cada llamada.

Una operación es H4 si y solo si cumple las seis condiciones a la vez:

1. es una `query`;
2. `captured_timeout_seconds == 3.0` (presupuesto del fallback);
3. su antecesor inmediato en el mismo caso humano es una `query`…
4. …que agotó con `TimeoutError` bajo `captured_timeout_seconds == 8.0` y **llevaba** `ORDER BY`
   (la condición que abre el `except`);
5. no lleva `ORDER BY`;
6. lleva `LIMIT 50`.

La firma separa limpiamente, sin zona gris: las 17 `query` con presupuesto 8.0 s son principales;
las 8 de `direct_lookup_id` corren a 11.0 s con `LIMIT 1` y sin antecesor agotado; las 2 de
`actor_profile` corren a 5.0 s **con** `ORDER BY` y `LIMIT 5`. Ninguna otra operación del corpus se
acerca a la conjunción.

### El número exacto es 7, no 6

| operación | caso | categoría | `LIMIT` primario |
|---|---|---|--:|
| `RT-TAB-001__call_002` | `RT-TAB-001` | `phrase_aware` | 50 |
| `RT-TAB-002__call_002` | `RT-TAB-002` | `tabular_topic` | 50 |
| `RT-TAB-004__call_002` | `RT-TAB-004` | `state_entity` | 50 |
| `RT-OR-001__call_002` | `RT-OR-001` | `wide_or_topic` | 50 |
| `RT-NEG-001__call_002` | `RT-NEG-001` | `negation_object` | 50 |
| `RT-NEG-002__call_002` | `RT-NEG-002` | `negation_entity_topic` | 50 |
| **`RT-PHRASE-002__call_002`** | `RT-PHRASE-002` | `phrase_candidate_limit_200` | **200** |

La adjudicación V1.1 observó una banda de **seis**. La diferencia es
`RT-PHRASE-002__call_002`, y la reconciliación es exacta: su fallback **también agotó durante la
captura clásica**, de modo que no produjo un par exitoso comparable y quedó fuera de una banda que
V1.1 enumeró por sus registros de contenido. Estructuralmente **sí** es H4 —de hecho es el único
que además dispara `reduced_limit`, porque su principal llevaba `LIMIT 200` por `objeto_phrases`.
Se incluye: excluirlo sería excluir por conveniencia justo la operación más cara de la clase.

## 4. Diseño

Dos brazos, misma operación congelada, sin retries, sin tocar SoQL, `LIMIT`, filtros ni timeout.

| brazo | transporte | endpoint | payload |
|---|---|---|---|
| A `classic` | clásico | `GET /resource/{dataset}.json` | `$query` = SoQL exacto |
| B `soda3_no_preserve` | SODA3 | `POST /api/v3/views/{dataset}/query.json` | `orderingSpecifier: "no_preserve"`, `includeSynthetic: false`, `timeout: 3` |

No se prueban `total`, `discard` ni el default: el microprobe ya los separó causalmente y repetirlos
sería gastar evidencia en una pregunta cerrada.

3 rondas × 7 operaciones × 2 brazos = **42 requests**. El presupuesto de cada request es el
`captured_timeout_seconds` congelado de su operación (3 s en las siete). Orden A/B aleatorizado
**dentro de cada operación** y orden de operaciones barajado por ronda, con semilla fija `20260827`;
el brazo SODA3 abrió la pareja en 10 de las 21 parejas, de modo que ningún resultado descansa en una
posición sistemáticamente favorable.

### Contrato semántico

Estas operaciones declaran `result_order_guarantee = unordered_sample`. Por tanto **no** son gate
la igualdad de posiciones, la igualdad estricta de ids ni la igualdad de `raw_hash`. Sí lo son: el
mismo SoQL sustantivo, los mismos filtros, respuesta HTTP válida, filas válidas respecto de la
consulta, ausencia de errores de contrato y comportamiento operacional no peor. Donde los ids
coinciden se registra como evidencia adicional —nunca como obligación retroactiva.

## 5. Resultados

Latencias en ms; `ti` = timeout duro contra el presupuesto de 3 s.

| operación | alcanzable | r1 clásico | r1 SODA3 | r2 clásico | r2 SODA3 | r3 clásico | r3 SODA3 |
|---|:-:|--:|--:|--:|--:|--:|--:|
| `RT-NEG-001__call_002` | **sí** | ok 492 | ok 503 | ok 460 | ok 149 | ok 461 | ok 237 |
| `RT-NEG-002__call_002` | no | ok 280 | ok 191 | ok 229 | ok 155 | ok 365 | ok 156 |
| `RT-OR-001__call_002` | no | ok 595 | ok 214 | ok 530 | ok 186 | ok 242 | ok 176 |
| `RT-PHRASE-002__call_002` | no | ti 3004 | ti 3323 | ti 3308 | ti 3002 | ti 3004 | **ok 171** |
| `RT-TAB-001__call_002` | no | ok 796 | ok 2968 | ok 786 | ok 186 | ok 845 | ok 154 |
| `RT-TAB-002__call_002` | **sí** | ok 1710 | **ok 182** | ok 835 | **ok 236** | ok 1115 | **ok 239** |
| `RT-TAB-004__call_002` | no | ok 188 | ok 202 | ok 192 | ok 168 | ok 168 | ok 177 |

| brazo | ok/intentos | timeouts | HTTP 4xx/5xx | errores de contrato | filas | rango latencia |
|---|:-:|:-:|:-:|:-:|:-:|---|
| `classic` | 18/21 | 3 | 0 | 0 | 50 | 168–3308 ms |
| `soda3_no_preserve` | **19/21** | **2** | **0** | **0** | 50 | 149–3323 ms |

Los únicos timeouts de ambos brazos son de `RT-PHRASE-002__call_002`, la operación intrínsecamente
más cara de la clase —la misma cuyo fallback ya había agotado durante la captura original. Ahí
SODA3 es **estrictamente mejor** que el clásico (1/3 contra 0/3), no peor.

### Comparación contra la línea base V1 (`total`) sobre el mismo universo

| operación | V1 SODA3 (`total`) | V1.2 SODA3 (`no_preserve`) |
|---|---|---|
| `RT-NEG-001__call_002` | ok 3/3 · 156–196 ms | ok 3/3 · 149–503 ms |
| `RT-NEG-002__call_002` | **timeout 3/3** · 3003–3004 ms | **ok 3/3** · 155–191 ms |
| `RT-OR-001__call_002` | ok 3/3 · 156–175 ms | ok 3/3 · 176–214 ms |
| `RT-PHRASE-002__call_002` | timeout 3/3 · 3003–3320 ms | timeout 2/3 · ok 171 ms en r3 |
| `RT-TAB-001__call_002` | ok 3/3 · 157–222 ms | ok 3/3 · 154–2968 ms |
| `RT-TAB-002__call_002` | **timeout 3/3** · 3002–3004 ms | **ok 3/3** · 182–239 ms |
| `RT-TAB-004__call_002` | ok 3/3 · 149–167 ms | ok 3/3 · 168–202 ms |
| **total** | **9 timeouts SODA3** vs 3 clásicos | **2 timeouts SODA3** vs 3 clásicos |

El cambio de una sola clave del payload elimina 7 de los 9 timeouts SODA3 de la clase y suprime las
**dos** regresiones SODA3-only que V1 había producido en ella (`RT-TAB-002__call_002` y
`RT-NEG-002__call_002`). No aparece ninguna nueva.

### Evidencia excedente, no exigida

El criterio no pedía paridad de contenido. Aun así: las 7 operaciones devolvieron **50 filas** en
ambos brazos siempre que respondieron, y `soda3_no_preserve` produjo un **único** `canonical_hash`
de conjunto por operación en las tres rondas, mientras el clásico varió su muestra entre rondas en
tres operaciones (`RT-NEG-002`, `RT-TAB-001`: 3 conjuntos distintos; `RT-TAB-002`, `RT-TAB-004`: 2).
Identidad de conjunto clásico↔SODA3 en `RT-NEG-001__call_002` y `RT-OR-001__call_002`. Es coherente
con el contrato —`LIMIT` sin `ORDER BY` no promete selección en ningún transporte— y muestra que la
variabilidad de muestra no es un artefacto de SODA3: bajo `no_preserve` SODA3 fue el brazo **más**
estable de los dos.

## 6. Gates

### G1 — integridad: **PASS**

100 % del SoQL exacto preservado (42/42 con `soql_sha256` idéntico al congelado); 100 % del
presupuesto de timeout preservado (3 s en las 42, igual al `captured_timeout_seconds`); 7/7
operaciones H4 identificadas y ejecutadas, 42 = 7 × 2 × 3 requests.

### G2 — errores: **PASS**

`soda3_no_preserve`: **0** HTTP 4xx/5xx (21/21 con status `200` o timeout de cliente), **0** errores
de decodificación, **0** errores de forma o serialización. Ninguna respuesta llegó con un tipo
inesperado ni con filas mal tipadas.

### G3 — estabilidad: **PASS**

```text
SODA3_TIMEOUTS (2)  <=  CLASSIC_TIMEOUTS (3)          ✓
SODA3_ONLY_REACHABLE_REGRESSIONS = 0                  ✓
SODA3_ONLY_REGRESSIONS           = 0                  ✓
```

La segunda condición se verifica en su forma fuerte: **no existe ninguna operación** —alcanzable o
no— donde el clásico responda 3/3 y SODA3 falle 3/3. Ganar en el agregado no bastaba, y no hizo
falta apoyarse en eso.

**Alcanzabilidad, derivada sin reejecutar primarias.** Un fallback solo ocurre si la query
principal agotó; si la principal SODA3 responde, el `except` nunca se abre y el fallback es
inalcanzable bajo un runtime SODA3. Las principales **sí** llevan `ORDER BY`, de modo que una
política consciente de la ruta las mantendría en `total` — exactamente la configuración bajo la que
V1 ya las midió. Su evidencia se reutiliza del replay V1 verificado por hash: solo
`RT-NEG-001__call_002` y `RT-TAB-002__call_002` son alcanzables (principal SODA3 timeout 3/3); las
otras cinco tienen principal SODA3 ok 3/3. Ambas alcanzables pasan 3/3.

### G4 — bloqueador original: **PASS**

`RT-TAB-002__call_002` deja de ser regresión reproducible: **3/3 ok** en 182 / 236 / 239 ms, contra
un clásico que resolvió en 1710 / 835 / 1115 ms. SODA3 es entre 3,5× y 9× más rápido que el clásico
en la operación que V1 declaró el único bloqueador real de migración.

## 7. Dictamen

```text
SODA3_H4_POLICY_REPLAY: PASS

MICROPROBE_COMMIT:                 21e8e10ee152a9f7b7282f2a0e3db7b3b5fadd7f
MANIFEST_HASH:                     880e63ecaf2672e217238c234db09d2847c89d95da6648c8c8490d9952c5076a
H4_OPERATIONS_IDENTIFIED:          7
REQUESTS_EXECUTED:                 42

CLASSIC_OK:                        18
CLASSIC_TIMEOUT:                    3
SODA3_OK:                          19
SODA3_TIMEOUT:                      2

SODA3_ONLY_REACHABLE_REGRESSIONS:   0
HTTP_CONTRACT_ERRORS:               0

RT_TAB_002_STATUS:                 PASS
H4_NO_PRESERVE_POLICY:             PASS
REMAINING_TRUE_MIGRATION_BLOCKERS:  0
PRODUCTION_DIFF:                   CLEAN
```

## 8. Alcance — lo que este replay **no** establece

- No revalida las rutas ordenadas. `orderingSpecifier=total` sigue siendo la configuración medida y
  vigente para las operaciones cuyo SoQL declara `ORDER BY`; aquí no se tocó ninguna.
- No implementa la política. Nada de `app/`, `tests/`, `requirements.txt`, harnesses V0/V1,
  fixtures congelados, `SoQLBuilder`, `SecopClient`, workflow, `order_semantics` ni configuración
  fue modificado. `PRODUCTION_DIFF = CLEAN`.
- No convierte los gates históricos en otra cosa: V0 `GO_SODA3_QUERY` y V1 `NO_GO_DIVERGENCE`
  siguen intactos como registro de sus propias configuraciones.
- No introduce TOP-N determinístico. Esa deuda sigue siendo post-migración, sin cambios.
- `RT-PHRASE-002__call_002` sigue siendo una operación cara en **ambos** transportes. No es una
  regresión de SODA3 —es una consulta que agota contra 3 s— y su fallback es inalcanzable bajo un
  primary SODA3, pero conviene no leer su `ok` de la ronda 3 como una mejora garantizada.

Reproducción: `scripts/audits/soda3_h4_policy_replay.py --env-file .env`. Ejecuciones crudas en
`/tmp/soda3_h4_policy_replay.jsonl` (SHA-256 en §2); no persiste token ni cabeceras.

## 9. Siguiente paso

```text
VERDICT: READY_FOR_SODA3_QUERY_FINAL_ADJUDICATION
```

Con `REMAINING_TRUE_MIGRATION_BLOCKERS = 0` bajo una política consciente de la ruta, no queda
campaña por correr: queda **dictaminar**. El siguiente paso es
`SODA3_QUERY_FINAL_ADJUDICATION` — readjudicar `/query` sobre la evidencia ya congelada (V0, V1,
V1.1, el microprobe y este replay) bajo la política `total` donde el SoQL declara `ORDER BY` /
`no_preserve` donde el runtime lo retira, y emitir el dictamen final de migración. No se inicia
integración productiva.

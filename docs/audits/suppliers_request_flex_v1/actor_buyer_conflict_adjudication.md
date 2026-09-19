# ACTOR_BUYER_SPAN_CONFLICT — adjudicación (SUPPLIERS 1.1 closeout)

Consulta reproducida, literal:

```
quién ha contratado por mayor valor con la Alcaldía de Barranquilla
```

## Resultado por lado (mismas condiciones: harness del probe, `frame_on`,
provider Natural Alpha scripted con la misma quote, verificador de residual
aprobando, sin índice de actores salvo donde se dice)

| | BASE `795b628` | CANDIDATO `c532f63` |
|---|---|---|
| route | `admission_ACTOR_BUYER_SPAN_CONFLICT` | idéntico |
| assembly reason | `ACTOR_BUYER_SPAN_CONFLICT` | idéntico |
| source calls | 0 | 0 |
| response | aclaración | idéntica |

Evidencia cruda: `/tmp/base_v06.jsonl` y `/tmp/cand_v06.jsonl` (probe v06), más
`docs/audits/suppliers_request_flex_v1/dogfood.jsonl` (caso `s04`).

## Evidencia de las autoridades (idéntica en ambos lados)

```
buyer: OK buyer_exact  span (40, 67) = "la Alcaldía de Barranquilla"
actor: OTHER_OWNER actor_subject
       spans (9, 67)  = "contratado por mayor valor con la Alcaldía de Barranquilla"
             (24, 67) = "mayor valor con la Alcaldía de Barranquilla"
```

El span del actor **contiene** el span del comprador; `assemble()` convierte el
solapamiento en `ACTOR_BUYER_SPAN_CONFLICT` y el turno termina en CLARIFY
terminal con cero llamadas. La interpretación de métrica/corte que el resolver
de SUPPLIERS habría producido (`value` / `limit=1`, por «mayor valor» + «quién»)
nunca llega a ejecutarse.

## Clasificación

**PREEXISTING_LIMITATION** — no es regresión del candidato:

* BASE y candidato fallan equivalentemente (mismo reason, mismas llamadas).
* `git diff --stat 795b628..c532f63 -- app/core/accreditation/authorities.py
  app/core/actor_intent.py app/core/entity_resolver.py
  app/core/accreditation/assembly.py` está **vacío**: el candidato no toca
  ninguna de las autoridades que producen el conflicto.

## Tratamiento

Deuda preexistente del par Actor/Comprador, fuera del alcance de SUPPLIERS 1.1.
No se remedia aquí: tocar la autoridad de actor o la de comprador es una
condición de parada explícita del contrato de misión.

Nota de corpus: `s04` conserva su vector de resolución (`value`/`limit=1`) y se
marca `preexisting_veto: ACTOR_BUYER_SPAN_CONFLICT` para que el dogfood lo
separe de los errores semánticos del frente (exige cero llamadas, igual que un
veto determinista).

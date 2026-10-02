# M2A-0 — Revisión H7b (ADR-103 Enm. 4 §4.8) antes del sello del Gold de follow-ups

**Fecha:** 2026-10-01. **Base:** `main` = `8c1f8c54`. Revisión read-only (CASTOR, delegada por HERA).

ADR-103 Enm. 4 exige, antes de sellar el Gold de follow-ups, identificar y revisar los dos solapes BUYER+GEOGRAPHY que
H7b atribuye al «oráculo sintético», y comprobar si son X020 y X059 (homónimos) o anidación real.

## Método

Se re-ejecutó el oráculo de `docs/audits/m2a_advisory_2026-10-01/04_medicion_omision/omission_probe.py` (`oracle()`)
sobre los slices sintéticos del Gold M1 sellado (`docs/audits/m1_search_foundation/gold/{neighbor,minimal_pair,adversarial}.jsonl`;
111 + 191 + 92 = 394 registros), sin modificar nada.

## Resultado

| Registro | Slice | Disposición sellada | BUYER (cita, offsets) | GEOGRAPHY (cita usada por el Gold) | Clase |
|---|---|---|---|---|---|
| X020 | ADVERSARIAL | EXECUTE, CLEAR | negativo, `[28,46)` | ocurrencia final `[50,56)` | homónimo en otra ocurrencia |
| X059 | ADVERSARIAL | EXECUTE, CLEAR | `[16,38)` | ocurrencia final `[42,49)` | homónimo en otra ocurrencia |

- Son exactamente X020 y X059. No hay un tercer caso ni anidación real (del tipo «Soledad» dentro de «Alcaldía de
  Soledad»).
- El solape medido es un artefacto del instrumento: `gold_owner_map` ancla cada cita en su **primera** ocurrencia
  (`find_subseq`), que cae dentro del BUYER. Con los offsets de la ocurrencia que el Gold cita, no hay solape.
- D4(a) (re-anclaje a la única reaparición sin cubrir, regla `reanchored`) produce la disposición sellada. Sin
  contradicción.
- **Independiente del Gold M2a:** ninguno de los dos ids ni sus textos aparece en el árbol asesor ni en
  `M2A_GOLD_AND_FRONTIER_INDEX.json` (los ids M2a son `R…`/`SF…`).

## Errata

H7b (ADR-103 línea 141) y `OMISSION_PROBE.md:222` dicen «2 de 294»; la población medida es 394 (`OMISSION_PROBE.md:16`).
Registrada en ADR-103 Enm. 5 punto 4; el texto de la Enm. 4 no se modifica.

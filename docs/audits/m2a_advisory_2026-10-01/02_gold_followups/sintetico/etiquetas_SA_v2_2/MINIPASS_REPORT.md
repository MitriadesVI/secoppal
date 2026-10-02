# MINIPASS_REPORT SA v2.2 (A8 y A9)

Solo se tocaron los casos de la tabla; los otros 174 registros son identicos a v2.1 (comprobado en `check3.py`). Nota: `adjudicated_by: ["A8"]` va en `notes`.

## Resumen

| id | etiqueta anterior (turn_kind/disp/ambig) | nueva | cambia disposicion | cambia IR resultante | VALUE anterior | VALUE nuevo |
|---|---|---|---|---|---|---|
| SF051 | ANSWER_TO_CLARIFICATION/REASK/CLEAR | ANSWER_TO_CLARIFICATION/REASK/CLEAR | no | no | null | null |
| SF053 | ANSWER_TO_CLARIFICATION/REASK/AMBIGUOUS | ANSWER_TO_CLARIFICATION/REASK/CLEAR | no | no | null | null |
| SF063 | NEW_SEARCH/NEW_SEARCH/AMBIGUOUS | NEW_SEARCH/NEW_SEARCH/AMBIGUOUS | no | no | - | - |
| SF090 | DELTA/EXECUTE/AMBIGUOUS | DELTA/EXECUTE/CLEAR | no | no | less_or_equal 200 millones | less_or_equal 200 millones |
| SF091 | DELTA/EXECUTE/AMBIGUOUS | DELTA/EXECUTE/CLEAR | no | si | less_or_equal 300 millones | greater_or_equal 300 millones; less_or_equal 300 millones |
| SF094 | DELTA/EXECUTE/AMBIGUOUS | DELTA/EXECUTE/CLEAR | no | no | greater_than 400 millones | greater_than 400 millones |
| SF095 | DELTA/EXECUTE/AMBIGUOUS | DELTA/EXECUTE/CLEAR | no | si | greater_or_equal 200 millones | greater_or_equal 200 millones; less_or_equal 200 millones |
| SF099 | DELTA/EXECUTE/AMBIGUOUS | DELTA/EXECUTE/CLEAR | no | no | greater_than 350 millones | greater_than 350 millones |
| SF103 | DELTA/EXECUTE/AMBIGUOUS | DELTA/EXECUTE/CLEAR | no | no | greater_or_equal 350 millones | greater_or_equal 350 millones |
| SF126 | NEW_SEARCH/NEW_SEARCH/AMBIGUOUS | NEW_SEARCH/NEW_SEARCH/CLEAR | no | no | - | - |
| SF132 | DELTA/EXECUTE/CLEAR | DELTA/EXECUTE/CLEAR | no | no | - | - |
| SF134 | NEW_SEARCH/NEW_SEARCH/CLEAR | NEW_SEARCH/NEW_SEARCH/CLEAR | no | no | - | - |
| SF182 | DELTA/EXECUTE/AMBIGUOUS | DELTA/EXECUTE/CLEAR | no | no | less_or_equal 90 millones | less_or_equal 90 millones |
| SF183 | DELTA/EXECUTE/AMBIGUOUS | DELTA/EXECUTE/CLEAR | no | no | less_or_equal 90 millones | less_or_equal 90 millones |
| SF185 | NEW_SEARCH/NEW_SEARCH/AMBIGUOUS | NEW_SEARCH/NEW_SEARCH/CLEAR | no | no | - | - |
| SF187 | NEW_SEARCH/NEW_SEARCH/AMBIGUOUS | NEW_SEARCH/NEW_SEARCH/CLEAR | no | no | - | - |
| SF170 | DELTA/EXECUTE/CLEAR | DELTA/EXECUTE/AMBIGUOUS | no | no | greater_or_equal 200 millones; less_or_equal 900 SMMLV | greater_or_equal 200 millones; less_or_equal 900 SMMLV |

## A8: criterio y calculo por caso

- **SF090**: prior lower >=500 millones (inclusive), new upper <=200 millones (inclusive); same unit MILLONES; lower 500 > upper 200 -> EMPTY -> new bound replaces the whole VALUE restriction (<=200 millones), announced.
- **SF091**: prior lower >=300 millones, new upper <=300 millones; same unit; bounds EQUAL and both operators inclusive -> NOT empty (single point) -> opposite bound kept: >=300 and <=300 millones.
- **SF094**: prior upper <=200 millones, new lower >400 millones (exclusive); same unit; lower 400 > upper 200 -> EMPTY -> replaced whole (>400 millones), announced.
- **SF095**: prior upper <=200 millones, new lower >=200 millones; same unit; EQUAL and both inclusive -> NOT empty (single point) -> opposite kept: >=200 and <=200 millones.
- **SF099**: prior range >=100 and <=300 millones, new lower >350 millones (exclusive); same unit; lower 350 > upper 300 -> EMPTY -> replaced whole (>350 millones), announced.
- **SF103**: prior upper <=300 millones; new lower >=350 (unit inherited MILLONES, F5); lower 350 > upper 300 -> EMPTY -> replaced whole (>=350 millones), announced.
- **SF182**: prior lower >=150 millones, new upper <=90 millones (stated); same unit; lower 150 > upper 90 -> EMPTY -> replaced whole (<=90 millones), announced.
- **SF183**: prior lower >=150 millones, new upper <=90 (unit inherited MILLONES, F5); lower 150 > upper 90 -> EMPTY -> replaced whole (<=90 millones), announced.
- **SF170**: A8 criterion: prior lower >=200 millones (MILLONES), new upper <=900 SMMLV; units differ and A8 names millones vs SMMLV as not comparable -> NOT adjudicated, AMBIGUOUS. Best reading: opposite bound kept (A2 single year 2025: 900 SMMLV converts far above 200 millones, so the range is non-empty) -> >=200 millones and <=900 SMMLV. Alternative IR: new bound replaces the whole restriction (<=900 SMMLV). Previously CLEAR/MEDIUM: now flagged because it meets A8's 'not comparable' clause.

### Barrido de VALUE por lado (SF085-SF108, SF170)

Sin cambio por A8 (ninguno cumple el criterio de vacio):

- SF085: no prior VALUE
- SF086: no prior VALUE
- SF087: no prior VALUE
- SF088: same-side replace (>=100 -> >=250)
- SF089: lower >=100, new upper <=400: 100<400 non-empty
- SF092: same-side replace (<=200 -> <=150)
- SF093: new lower >50, upper <=200: 50<200 non-empty
- SF096: lower >=150, upper <=300 non-empty
- SF097: lower >=100, upper <=250 non-empty
- SF098: both sides new: >=400 and <=600 non-empty
- SF100: explicit change (CLEAR+SET)
- SF101: same-side replace (>=200 -> >=350)
- SF102: CLARIFY UNIT_AMBIGUOUS (F5)
- SF104: same-side replace (>=40.000.000 pesos -> >=650 pesos)
- SF105: upper <=200 millones (2e8 pesos), new lower >900 mil pesos (9e5): 9e5 < 2e8 non-empty, convertible units
- SF106: both sides new: >=450 and <=900 millones non-empty
- SF107: no prior VALUE
- SF108: same-side replace (>=200 millones -> >=2 mil millones)

Casos que cumplian el criterio y yo no habia marcado como tal: SF170 (montos millones frente a SMMLV: no comparables segun A8 -> pasa a AMBIGUOUS; antes CLEAR/MEDIUM). Los demas casos de A8 (SF090, SF091, SF094, SF095, SF099, SF103, SF182, SF183) ya estaban en la lista, pero marcados AMBIGUOUS con RANGE_NO_INTERIOR; SF091 y SF095 (un solo punto inclusivo) NO son vacios: la opuesta se conserva y el IR resultante cambia a dos cotas (>=X y <=X).

## A9: razonamiento por caso

- **SF051** (ANSWER_TO_CLARIFICATION/REASK/CLEAR -> ANSWER_TO_CLARIFICATION/REASK/CLEAR): F25: the answer supplies the year-bound period and also edits VALUE -> REASK, no hybrid. (If applied, <40M against >150M would also have no interior, A1.) A8 note: prior >150 millones vs new <40 millones (same unit) would be an EMPTY interval, so A8 replaces the VALUE restriction (adjudicated; RANGE_NO_INTERIOR gap dropped); it does not change the F25 REASK. A9 check: with history erased 'pero de menos de 40 millones de enero a junio de 2025' starts with the continuation connector 'pero' and has no frame (value + period only) -> DELTA/answer; label unchanged.
- **SF053** (ANSWER_TO_CLARIFICATION/REASK/AMBIGUOUS -> ANSWER_TO_CLARIFICATION/REASK/CLEAR): A9 test: with history erased, 'los de la Alcaldía de Piedecuesta de enero a junio de 2025' has an anaphoric 'los de' with no head noun; it needs the previous interpretation to be read -> NOT self-sufficient -> DELTA (answer to the pending TIME + buyer edit -> F25 REASK ANSWER_EDITS_OTHER_SLOT). Previously AMBIGUOUS (UNIVERSE_FRAGMENT).
- **SF063** (NEW_SEARCH/NEW_SEARCH/AMBIGUOUS -> NEW_SEARCH/NEW_SEARCH/AMBIGUOUS): A7/F23: after 'no, mejor' the content is topic+buyer+year with its own frame: I read it as self-sufficient (A4) -> NEW_SEARCH, universe not said -> UNSPECIFIED, PV1 -> HISTORICAL_PROCESSES. Alternative (A4 middle ground): fragment delta on the pending frame (keeps CONTRACTS and VALUE >150M). O13: ops []. A9 check: with history erased the content ('dotación de la Gobernación del Cauca en 2025') passes the test (UNSPECIFIED + PV1). The only question is whether 'no, mejor' is an explicit continuation marker: it rejects the pending question (A7) but does not continue the frame the way 'y', 'ahora', 'pero' do; two readings remain -> stays AMBIGUOUS (UNIVERSE_FRAGMENT, PENDING_REJECTED); NEW_SEARCH kept, alternative = delta on the pending frame.
- **SF126** (NEW_SEARCH/NEW_SEARCH/AMBIGUOUS -> NEW_SEARCH/NEW_SEARCH/CLEAR): A9 test: with history erased, 'alquiler de maquinaria pesada en 2023' alone yields an admissible SEARCH (universe not said + topic + year -> UNSPECIFIED, PV1 HISTORICAL_PROCESSES). No continuation marker, no anaphora, not a bare topic (F22 n/a: two conditions with a request-like frame) -> self-sufficient -> NEW_SEARCH. Previously AMBIGUOUS. O13: ops [].
- **SF132** (DELTA/EXECUTE/CLEAR -> DELTA/EXECUTE/CLEAR): O6/D3: colloquial 'licitaciones' = PROCESSES noun, not MODALITY; repeats the current universe. A9 check: 'las licitaciones' alone would form a SEARCH (PROCESSES) but 'y' is an explicit continuation marker -> DELTA wins; label unchanged.
- **SF134** (NEW_SEARCH/NEW_SEARCH/CLEAR -> NEW_SEARCH/NEW_SEARCH/CLEAR): Universe noun + topic: self-sufficient; buyer dropped. O13: ops []. A9 check: with history erased 'contratos de vigilancia' yields CONTRACTS + TOPIC; no continuation marker -> NEW_SEARCH; label unchanged.
- **SF185** (NEW_SEARCH/NEW_SEARCH/AMBIGUOUS -> NEW_SEARCH/NEW_SEARCH/CLEAR): A9 test: with history erased, 'mantenimiento de parques de la Alcaldía de Duitama' yields an admissible SEARCH (UNSPECIFIED + PV6 CURRENT_OPPORTUNITIES; no TIME/state/actor/type/order; P1: buyer does not alter the view). No continuation marker -> NEW_SEARCH; twin of SF184 without the universe noun (not decisive). Previously AMBIGUOUS. O13: ops [].
- **SF187** (NEW_SEARCH/NEW_SEARCH/AMBIGUOUS -> NEW_SEARCH/NEW_SEARCH/CLEAR): A9 test: with history erased, 'contratos de 2024' yields an admissible SEARCH (explicit universe CONTRACTS + TIME 2024; M1 does not require content conditions). No continuation marker -> NEW_SEARCH (the 'contenido propio' requirement of old §5.1 was voided by A4/A9). Previously AMBIGUOUS. O13: ops [].

## Cambios de IR/disposicion

- Disposicion: ninguna cambia (todos siguen EXECUTE / NEW_SEARCH / REASK segun el caso); cambian `ambiguity`, `gap_codes` y, en SF091 y SF095, el IR resultante.
- IR resultante: SF091 (>=300 y <=300 millones, antes <=300), SF095 (>=200 y <=200, antes >=200). En los casos vacios (SF090, SF094, SF099, SF103, SF182, SF183) el IR coincide con mi v2.1 (la sustitucion completa), pero ahora esta adjudicada.
- Ambiguity AMBIGUOUS -> CLEAR: SF053, SF090, SF091, SF094, SF095, SF099, SF103, SF126, SF182, SF183, SF185, SF187. CLEAR -> AMBIGUOUS: SF170.

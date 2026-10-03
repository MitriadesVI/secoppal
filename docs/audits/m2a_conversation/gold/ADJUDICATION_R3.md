# M2A_GOLD r3: adjudicación U3 (enmienda R3, 2026-10-02)

**Autor de la decisión:** el usuario (2026-10-02), transmitida por el orquestador. **Registro:** ARES, paso B4 de la
enmienda R3 (contrato, Anexo D). **Mecanismo:** el mismo que U1/U2/D1 (`ADJUDICATION_2026-10-02.md`). La copia sellada
la genera `scripts/audits/m2a_conversation/build_sealed_gold.py` (`u3_additions`) y el sello comprueba que se reproduce
byte a byte. El candidato asesor no cambia: U3 **añade** cuatro casos y no toca ningún registro existente.

## Procedencia

Las etiquetas se conservan literales: no se editan; la adjudicación se aplica al construir.

| Pieza | Ruta | sha256 |
|---|---|---|
| Etiquetas del etiquetador A (Sonnet, ciego) | `docs/audits/m2a_conversation/labeling_r3/labels_A.jsonl` | `cfc932e8c17b5d34beede7537c3fa408ece31d6a9384eba0ebdaa6ef3c70c68e` |
| Etiquetas del etiquetador B (Opus, ciego) | `docs/audits/m2a_conversation/labeling_r3/labels_B.jsonl` | `025d1b3044808449a46bca8516e605bdffca985458de3027da8b4f2217f68299` |
| Contrato contra el que etiquetaron | `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` en `a90d6ad0` | `6eb5ca00c0307f78…` |
| Entrada que recibieron | `labeling_r3/R3_LABELER_INPUT.md` en `a90d6ad0` | `4766d610cd8d1d92…` |
| Esquema | `docs/audits/m2a_advisory_2026-10-01/02_gold_followups/kit_v2_2/SCHEMA_v2_1.md` | `0e96ada47d513a92…` |

**Acuerdo primario.** En los cuatro casos, A y B coinciden en `turn_kind`, `m2a_scope`, `expected_disposition`,
`disposition_reason`, `resulting_ir`, `effects` y `expected_echo`, por lo que `primary_agreement = true`. Ninguno marcó
ambigüedad (`ambiguous_by = []`). Las confianzas se registran tal cual.

## Decisiones

| Caso | Disposición (A = B) | Diferencia A/B | Adjudicación U3 | Digest del registro sellado |
|---|---|---|---|---|
| SF192 | `CLARIFY DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` | `alternatives`: A `[]`, B dos IR (C1 y `SET_ORDER` fecha). Forma de la op `CLEAR(ORDER)` | Se sella **sólo C1** («quitar orden y límite»: el preestado sin ORDER ni CARDINALITY). Los candidatos `SET_ORDER` por criterio alternativo los determina E-ORDER (§19, `DEP-E-ORDER`) y no se sellan. `CLEAR` sin `polarity` ni `value_hint` | `852b7da9da9ce079…` |
| SF193 | `CLARIFY KEEP_ON_EMPTY_SLOT` | `refers_to_result_set` de la op TIME (A `true`, B `false`); `gap_codes` (A `["SET"]`, B `[]`); `value_hint` del KEEP | `refers_to_result_set = true` en «los de 2024» (convención del Gold sellado, matriz T12); `gap_codes = []`; KEEP sin `value_hint` | `68f60fea5dc268b8…` |
| SF194 | `CLARIFY UNRESOLVED_VALUE:TOPIC` | sólo confianza (A MEDIUM, B HIGH) y notas | ninguna; confianzas tal cual | `207f1dc2269e6ef0…` |
| SF195 | `EXECUTE` (CARDINALITY conservada, F10) | orden de `gap_codes` (mismo conjunto); forma de la op `CLEAR(ORDER)` | `gap_codes = ["ORDER_DIRECTION", "SET"]`, el orden canónico del Gold (alfabético: 20 de 33 listas de varios códigos); `CLEAR` sin `polarity` ni `value_hint` | `b767eb83d05e8bac…` |

Cada cambio deja su rastro en las notas de la etiqueta afectada (`| U3 (usuario, 2026-10-02): …`).

## Precisiones del registro (para la revisión)

1. **SF193 `gap_codes`.** La decisión se aplica tal como se dio (`[]`). Pero su motivo declarado («"SET" no es de la
   lista cerrada») no es exacto: `SET` **sí** está en la lista cerrada (`SCHEMA_v2_1.md` §8: «re-consultar o filtrar lo
   mostrado», cerrado por F4), y A y B lo usan en SF192 y SF195. El resultado coincide con la convención dominante del
   Gold sellado: 74 etiquetas con `refers_to_result_set = true` sin `SET`, frente a 2 con `SET`.
2. **Forma de las ops.** «CLEAR/KEEP con `polarity` null y `value_hint` null (forma de B)»:
   - se aplica íntegra a `CLEAR`, que es la forma de B y la del Gold;
   - en `KEEP` se aplica a `value_hint`. Su `polarity` sigue siendo `POSITIVE`, porque A y B coinciden en ella y es la
     convención del Gold (8 de 8 KEEP). Escribirla nula contradiría la «forma de B».
3. **SF195, `polarity` de `SET_ORDER`.** A pone `POSITIVE` y B `null`. No se adjudicó, así que queda tal cual en cada
   etiqueta. La convención del Gold es `POSITIVE` (48 de 48), y ninguna regla del verificador la lee.
4. **SF192 `gap_codes = ["SET"]`.** A y B coinciden; queda tal cual.
5. **SF192, alternativas parciales (R3b, U23).** El registro sellado declara que sus alternativas son **parciales**:
   el campo `partial_alternatives` nombra la dependencia (`DEP-E-ORDER-CANDIDATES`, `COMPOSICION`, §19) y dice qué se
   sella (C1) y qué queda pendiente de E-ORDER (los `SET_ORDER`). Lo provisional no se sella como normativo. Lo escribe
   `build_sealed_gold.py` (`U3_RECORD_FIELDS`), y el extractor de §19 falla si la declaración no nombra una fila
   `COMPOSICION` que contenga el caso. Entra en el sello en el re-sellado único de R3b-2.

## Partición DEV/TEST

Regla fija del sello: estrato = slice + disposición; orden `sha256("m2a-gold-partition/1:" + id)`; posición par →
TEST, impar → DEV; pegajosa. Calculada con los cuatro ids dentro y **0 movimientos** de ids existentes:

| Id | Disposición | Lado |
|---|---|---|
| SF192 | CLARIFY | TEST |
| SF193 | CLARIFY | TEST |
| SF194 | CLARIFY | TEST |
| SF195 | EXECUTE | DEV |

## Totales tras r3

`M2A_GOLD` = 49 REAL + 154 SYNTHETIC = 203. `FOLLOWUP_FRONTIER` = 16 + 41 = 57, sin cambios. Disposiciones del
sintético: EXECUTE 118, CLARIFY 15, NEW_SEARCH 16, REASK 4, INFORM 1.

---

# Sección R3b: adjudicación U25 (enmienda R3b, 2026-10-02)

**Autor de la decisión:** el usuario (2026-10-02), transmitida por el orquestador. **Registro:** ARES, paso R3b-2.
**Mecanismo:** el de U3. `build_sealed_gold.py` (`u25_additions`) construye los registros desde las etiquetas literales
más esta adjudicación, y el sello comprueba que se reproducen byte a byte. U25 **añade** seis casos (SF196–SF201) y
cambia un registro existente (SF195). Respecto del sello r3 cambian dos: SF192, que en R3b-1 declaró sus
alternativas parciales (`partial_alternatives`), y SF195.

## Procedencia

| Pieza | Ruta | sha256 |
|---|---|---|
| Etiquetas del etiquetador A (Sonnet, ciego) | `docs/audits/m2a_conversation/labeling_r3b/labels_A.jsonl` | `cfa473734c7bd6e9d8993a47c4e7926891738c7b198234f72bd1c9e3da409e7c` |
| Etiquetas del etiquetador B (Opus, ciego) | `docs/audits/m2a_conversation/labeling_r3b/labels_B.jsonl` | `67da48076ed01c6e6d31e1d1952587ea55463262abfc468674f027be7240ce37` |
| Contrato contra el que etiquetaron | `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` en `b8372757` | `fe3ff6d6ffdad382…` |
| Entrada que recibieron | `labeling_r3b/R3B_LABELER_INPUT.md` en `b8372757` | `9208876dde678697…` |
| Esquema | `docs/audits/m2a_advisory_2026-10-01/02_gold_followups/kit_v2_2/SCHEMA_v2_1.md` | `0e96ada47d513a92…` |

Las etiquetas cubren SF196–SF202. A y B recibieron la misma entrada; el constructor lo comprueba caso por caso.

**Acuerdo primario.** En SF196, SF197, SF198, SF200, SF201 y SF202, A y B coinciden en `turn_kind`, `m2a_scope`,
`expected_disposition`, `disposition_reason`, `resulting_ir`, `effects` y `expected_echo`. En SF199 discrepan.

## Decisiones

| Caso | Disposición sellada | Diferencia A/B | Adjudicación U25 | Digest del registro sellado |
|---|---|---|---|---|
| SF196 | `INFORM KEEP_ON_EMPTY_SLOT` | forma de las ops | forma de R3 | `9802d3a17535d673…` |
| SF197 | `CLARIFY DELTA_SLOT_CONFLICT` | `quote` del `SET`; `inherited` del `KEEP` (A objeto, B `null`) | forma de R3; lo demás tal cual | `dc211bc4f7ccc682…` |
| SF198 | `INFORM KEEP_ON_EMPTY_SLOT` | `refers_to_result_set` de «los de 2024» (A `true`, B `false`); `gap_codes` (A `["TTL", "SET"]`, B `["TTL"]`) | `refers_to_result_set = true`, la convención adjudicada en SF193; `gap_codes = ["TTL"]`, la convención mayoritaria (`refers` verdadero sin `SET`); forma de R3 | `f52448ecf7d0b52a…` |
| SF199 | `INFORM UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL`, alcance `OUT_UNSUPPORTED_PROPOSAL` | **primaria**: A = `REASK ANSWER_UNRESOLVED` (`IN`); B = `INFORM UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL` (`OUT_UNSUPPORTED_PROPOSAL`) | **se adjudica B** (motivo abajo). A toma la lectura primaria de B; sus notas quedan marcadas `[SUSTITUIDA por U25]`. `primary_agreement = false`; `ambiguous_by = ["A"]`, la ambigüedad que A declaró | `4618a49d2755b76b…` |
| SF200 | `EXECUTE` (selecciona C1: sin ORDER ni CARDINALITY) | `quote` y `value_hint` de la respuesta | forma de R3 | `7a5e705a60b0d0b3…` |
| SF201 | `REASK ANSWER_EDITS_OTHER_SLOT` | `quote` y `value_hint` | forma de R3 | `3460bbac8bacfe2b…` |
| SF202 | — (**se retira**) | ninguna primaria; A y B la marcan `AMBIGUOUS` | no se sella (motivo abajo) | — |
| SF195 (r3) | `EXECUTE`, sin cambios | `polarity` de `SET_ORDER` (A `POSITIVE`, B `null`): el punto abierto de R3 | `POSITIVE`: el tipo prohíbe negar ORDER. Es el único registro existente que cambia U25; SF192 ya había cambiado en R3b-1 (`partial_alternatives`) | `90123057ae4b3122…` |

**Motivo de SF199.** Es una decisión expresa del usuario (U21/P1-2): mientras IR v2 rechace un `FOLLOWUP_DELTA` que
sólo trae CARDINALITY, «dame 20» termina en `INFORM UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL` (§6.7, §7.1 fila 4b). La
restricción del wire es autoridad por §0 y no es una dependencia de §19. Por eso no le aplica la instrucción
«normativo, no provisional» de la entrada.

**Motivo de SF202.** Los dos etiquetadores la marcan `AMBIGUOUS`: la aposición puede leerse como un único `KEEP`. La
ley «`KEEP` + `SET` = conflicto aunque coincidan los valores» es mecánica y ya la cubren el barrido y el verificador.
El usuario pidió doble etiquetado sólo donde hay interpretación humana real. Los controles mínimos siguen cubiertos:
dos rechazos (SF196), `KEEP` + `SET` (SF197) y ancla vencida (SF198). **Nota para M2A-5 (productor):** una aposición
que nombra el valor vigente se emite como un único `KEEP`, no como `KEEP` + `SET`.

**Forma de las ops, como en R3.**
- `CLEAR` y `ANSWER` llevan `polarity` nula, y `CLEAR` además `value_hint` nulo.
- `KEEP` lleva `polarity` `POSITIVE` y `operator` nulo. Sobre una ranura vacía también lleva `value_hint` nulo (SF196,
  SF198).
- En SF197 el `KEEP` nombra una ranura ocupada: su `value_hint`, en el que A y B coinciden, se conserva, como en
  SF024 y SF041.
- `unresolved` queda como lo marcan ambos, que coinciden en todas las ops.

Cada cambio deja su rastro en las notas de la etiqueta afectada (`| U25 (usuario, 2026-10-02; R3b): …`).

## Precisiones del registro (para la revisión)

1. **`ANSWER` con `polarity` nula.** Se aplica como se decidió. Las 18 `ANSWER` del Gold anterior llevan `POSITIVE`,
   pero todas sobre ranuras polarizadas. Las de SF199–SF201 responden sobre CARDINALITY, que no tiene polaridad (§4.2).
   Ninguna regla del verificador lee esa polaridad.
2. **`inherited = null` en un `KEEP` (B).** Es la forma del esquema 2.1 (O5: «un KEEP no hereda nada»), así que queda
   literal. El verificador fallaba con una excepción ante ese `null`. Ahora lo admite sólo en `KEEP` y lo exige objeto
   en lo demás; un mutante lo prueba.
3. **SF199 fuera de `IN*`.** El alcance `OUT_UNSUPPORTED_PROPOSAL` es parte de la adjudicación. Para sellarlo, el
   alcance medible (§1.1) y la regla G8 del verificador admiten ahora `OUT_UNSUPPORTED_PROPOSAL`, sólo como fila 4b:
   `INFORM UNSUPPORTED_PROPOSAL:<motivo>` sobre un ancla o una pendiente, y en ambas direcciones.
4. **Respuesta ejecutada (SF200).** La regla de arrastre G3 atribuye a una respuesta ejecutada las ranuras de la
   pendiente (`candidate_slots`). El candidato seleccionado compone su parche completo (§8.3 «Selección»), y C1 retira
   ORDER aunque la respuesta sólo nombre el límite. Un mutante comprueba que no se atribuye otra ranura (TIME).

## Partición DEV/TEST (r3b)

Misma regla fija y pegajosa. **0 movimientos** de ids existentes (el sello lo comprueba contra el de `4cf1cad3`):

| Id | Disposición | Lado |
|---|---|---|
| SF196 | INFORM | DEV |
| SF197 | CLARIFY | TEST |
| SF198 | INFORM | TEST |
| SF199 | INFORM | DEV |
| SF200 | EXECUTE | TEST |
| SF201 | REASK | TEST |

## Totales tras r3b

`M2A_GOLD` = 49 REAL + 160 SYNTHETIC = 209. `FOLLOWUP_FRONTIER` = 16 + 41 = 57, sin cambios. Disposiciones del
sintético: EXECUTE 119, CLARIFY 16, NEW_SEARCH 16, REASK 5, INFORM 4.

---

# Sección R3c: adjudicación U26 (2026-10-02)

**Autor de la decisión:** el usuario (2026-10-02), tras el NO_GO de ZEUS sobre `7b30b221`. **Registro:** ARES, R3c.
Lo aplica la regla `u25_sf198` de `build_sealed_gold.py`, que deja la marca U26 en las notas. El registro pasa a
`adjudicated_by = ["U25", "U26"]`.

| Caso | Cambio | Motivo |
|---|---|---|
| SF198 | `expected_echo = ["CONTEXT_EXPIRED"]` en A y B; la disposición sigue siendo `INFORM KEEP_ON_EMPTY_SLOT` | vencimiento pegajoso (contrato §10.1 y §11): el turno encuentra el ancla vencida y no la confirma ni la reemplaza, así que lleva el eco y el ancla sigue vencida para los turnos siguientes |

**§19.** SF199 depende de M2A-5 (`DEP-M2A5-WIRE-CARDINALITY`, `LECTURA`). SF199–SF201 dependen de E-ORDER
(`DEP-E-ORDER-CANDIDATES`, `COMPOSICION`), porque su pendiente previa trae la opción `SET_ORDER`. El verificador lo
comprueba (G11) desde la extracción sellada.

Ningún id cambia de lado DEV/TEST. `M2A_GOLD` sigue siendo 209.

---

# Sección R3d: adjudicación U27 (2026-10-02)

**Autor de la decisión:** el usuario (2026-10-02), tras el NO_GO del ZEUS focal sobre `109095fe`. **Registro:** ARES,
R3d. La aplica `build_sealed_gold.py` (`u27_expired_echo`, un parche como U1/U2/D1), que deja la marca U27 en las notas
de la etiqueta A. Los registros pasan a `adjudicated_by = ["U27"]`.

| Caso | Diferencia A/B | Adjudicación | Motivo |
|---|---|---|---|
| SF138 | `expected_echo`: A `[]`, B `["CONTEXT_EXPIRED"]` | la de B: `["CONTEXT_EXPIRED"]` en A | §11 (segunda adición, R3d): todo turno que encuentra el ancla vencida y no la confirma ni la reemplaza emite `CONTEXT_EXPIRED` |
| SF140 | ídem | ídem | ídem |

La disposición y lo demás no cambian. El verificador valida ahora el eco guardado: `CONTEXT_EXPIRED` en todo turno
sobre ancla vencida que no la confirma ni la reemplaza, y nunca en `EXECUTE` ni `NEW_SEARCH`. Tiene mutantes
`U27_*`. Ningún id cambia de lado DEV/TEST. `M2A_GOLD` sigue siendo 209.

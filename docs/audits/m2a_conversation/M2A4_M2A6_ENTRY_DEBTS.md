# Deudas de entrada de M2A-4 y M2A-6 (protocolo de pendientes, vigencia y cancelación)

**Registradas:** 2026-10-02, tras el GO de ZEUS sobre `57530a07` (contrato R3 congelado, sello
`7dd39b4b322990925c5138b6492df909803da3fdc98f7a771e07bc17bd2a03b3`). **Autoridad:** U28 en `M2A_WAVE1_DECISIONS.md`.

**Qué es esto.** §8.3, §10 y §17 del contrato quedan sellados tal como están, pero no los congela M2A-3: son de M2A-4
(pendientes) y M2A-6 (vigencia y runtime). Cada paso los cierra con su propio gate **antes** de implementarse, con la
regla de congelación de §20.4 (sólo por inconsistencia propia o por versionado). Estas son las deudas que ese gate
debe cerrar. **No bloquean M2A-3.**

**Evidencia.**
- `contrato:N` es la línea de `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` en `57530a07`.
- Las sondas de ZEUS están en
  `/private/tmp/claude-501/-Users-rodrigoortiz-Documents-secoppal/427b208d-4550-4c0a-ad09-5659440e95d9/scratchpad/zeus_r3d/`.
  Se corren con `python <sonda> <raíz del worktree>` sobre el modelo simbólico `tests/test_m2a_contract_r3_multiop.py`.
- El directorio es efímero, así que el resultado de cada sonda (corrido el 2026-10-02 sobre `57530a07`) queda copiado
  aquí.

## P1-1 (M2A-4): cancelar `CONTEXT_EXPIRED_CONFIRM` no exige ¬Continuación(P)

- **Hecho.** Con una pendiente `CONTEXT_EXPIRED_CONFIRM`, §8.3 fila 2 (contrato:564) y §17.3 (contrato:873–875) dicen
  que el resto de P es «búsqueda nueva si es admisible sola». Falta ¬Continuación(P). Choca con A9: «Con
  Continuación(P) nunca es búsqueda nueva: gana la marca» (contrato:507; regla a, contrato:499).
- **Evidencia.** Lectura del texto. Ninguna sonda la cubre: el modelo simbólico no modela `CANCEL_PENDING` ni
  NEW_SEARCH.
- **Dueño.** M2A-4.
- **Reparación sugerida.** «Búsqueda nueva si ¬Continuación(P) ∧ Admisible sola(P); si no, `CLARIFY NO_ANCHOR`», en la
  fila 2 y en §17.3.

## P1-2 (M2A-4): «continuar» se alcanza en lenguaje añadiendo un `KEEP`

- **Hecho.** F24 compara la respuesta con el parche que preguntó **antes** de normalizar (contrato:580). La
  normalización quita después los `KEEP` de identidad (contrato:583). Así, «continuar» de `CONTEXT_EXPIRED_CONFIRM`
  se confirma en lenguaje: basta repetir el fragmento con un `KEEP` de relleno. Contradice «sólo se confirma por
  evento (fila 1)» (contrato:581–582, contrato:681).
- **Evidencia.** `probe_expired_keep.py`.
  - Reenviar «y en 2024?»: `REASK ANSWER_UNRESOLVED` (F24).
  - Lo mismo con `KEEP` del comprador, del tema o del universo: `EXECUTE`, igual a componer «continuar» sobre el ancla
    vencida.
  - En el barrido de todas las pendientes H1 que crea el modelo × `KEEP` de relleno: 73.418 respuestas, 55.562
    `EXECUTE` sobre el ancla vencida.
- **Dueño.** M2A-4.
- **Reparación sugerida.**
  - Una cláusula en H1: «Selección en lenguaje sobre `CONTEXT_EXPIRED_CONFIRM` ⇒ fila 7». O bien admitir de forma
    explícita la confirmación con `KEEP`.
  - En cualquier caso, un test que construya pendientes H1 y las responda en lenguaje.

## P1-3 (M2A-4/M2A-6): qué cancelaciones eliminan el ancla vencida

- **Hecho.** Cuatro sitios hablan de qué cancelaciones de `CONTEXT_EXPIRED_CONFIRM` eliminan el ancla vencida
  (`anchor_trace_id = None`), y no dicen lo mismo:
  - §10.1 (contrato:681–682) nombra §17.2 a, c y d, incluido agotar `asks`;
  - §17.3 (contrato:873–875) y §17.4 (contrato:876–878) hablan de «cancelar `CONTEXT_EXPIRED_CONFIRM`» sin enumerar;
  - §10.2 (contrato:697) dice «reset o cancelación».

  Ninguno dice qué pasa con (b), una búsqueda nueva con pendiente vigente, ni con (e), el vencimiento de la propia
  pendiente (§17.2, contrato:870–872).
- **Evidencia.** Lectura del texto.
- **Dueño.** M2A-4 (pendientes) y M2A-6 (vigencia y runtime).
- **Reparación sugerida.** Enumerar en un solo lugar (§17.2) qué causa de cancelación elimina el ancla vencida, y que
  §10.1, §10.2 y §17.3–4 lo citen.

## P2-4 (M2A-4): un `KEEP` en una respuesta sin candidatos acaba en la fila 5

- **Hecho.** La normalización sólo se aplica con candidatos (contrato:583). Sin candidatos, un `KEEP` de identidad de
  otra ranura «toca» una ranura fuera de `answer_slots` en el paso 4 (contrato:585). Resultado: `REASK
  ANSWER_EDITS_OTHER_SLOT`, aunque un `KEEP` no edita nada.
- **Evidencia.** `probe_nocand_keep.py`: en las pendientes sin candidatos (H10, mes sin año), la respuesta `KEEP` de
  otra ranura + `SET TIME` da `REASK ANSWER_EDITS_OTHER_SLOT` en 260 de 260 casos.
- **Dueño.** M2A-4.
- **Reparación sugerida.** Excluir los `KEEP` de identidad (ranura existente en `frame` y que ninguna otra operación de
  P toca) del «toca» del paso 4, también sin candidatos.

## P2-5 (M2A-6): «vigente sólo con continuar» ignora los turnos de protocolo

- **Hecho.** §10.1 (contrato:680–681) dice que el ancla vencida «vuelve a estar vigente sólo con continuar». No dice
  nada de los turnos PROTOCOL: página o evento de vista, orden o filtro. §10.2 los trata como turnos que dejan ancla.
- **Evidencia.** Lectura del texto.
- **Dueño.** M2A-6.
- **Reparación sugerida.** «… sólo con continuar, salvo página o evento de vista», con la regla que M2A-6 fije para
  esos turnos.

## P2-6 (M2A-4): F24 en H5 depende de la etiqueta del productor

- **Hecho.** F24 compara con el parche que preguntó (contrato:580). En H5, ese parche lleva la ranura que el productor
  etiquetó provisionalmente (BUYER o GEOGRAPHY). Qué opción se alcanza en lenguaje depende de esa etiqueta: la opción
  igual al parche que preguntó sólo se alcanza por evento.
- **Evidencia.** `probe_h5_tag.py`.

  | Etiqueta provisional | Respuesta «como comprador» | Respuesta «como lugar» |
  |---|---|---|
  | BUYER | `REASK ANSWER_UNRESOLVED` (F24) | `EXECUTE` |
  | GEOGRAPHY | `EXECUTE` | `REASK ANSWER_UNRESOLVED` (F24) |

- **Dueño.** M2A-4.
- **Reparación sugerida.** Declarar el coste, o limitar F24 a H1.

## Gates ya declarados en el contrato

| Gate | Dueño | Dónde |
|---|---|---|
| Barrer las respuestas a todas las pendientes, con el coste de F24 en H6 (A sólo se alcanza repitiendo sus dos operaciones) | M2A-4 | §8.4 (U26, U27) |
| Una respuesta se compone sobre `frame`, nunca sobre la base; el modelo simbólico no lo distingue (el mutante `answer_on_base_not_frame` sobrevive) | M2A-4 | §8.4 |
| Propiedades de varios turnos: vencimiento pegajoso, plazo de la pendiente, ancla eliminada tras «reset» | M2A-6/8b, por replay | §8.4, §10.1 |

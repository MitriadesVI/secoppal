# Adjudicación del usuario, tercera tanda (2026-10-01) — efectos sobre diseño, esquema y enmienda 2

Texto normativo para etiquetar: `02_gold_followups/kit_v2_2/ADJUDICATED_OVERRIDES.md` (A1–A7). Este archivo registra
las consecuencias fuera del etiquetado. Donde `M2A_DESIGN_v2_1.md`, `GOLD_FOLLOWUP_SCHEMA_v2_1.md` o
`ADR103_AMENDMENT2_INPUTS_v2_1.md` digan otra cosa en estos puntos, manda esta adjudicación.

1. **F26 retirada.** VALUE por lado; la cota opuesta se conserva. Sin «rango sin interior» ni «magnitud absurda» hasta
   tener criterio formal. Unidad heredada sólo con una única unidad activa inequívoca.
2. **SMMLV ejecutable; el Gold M1 NO se re-sella.** Con año único autoritativo se convierte a COP; el follow-up hereda
   y recalcula sobre vigencia única; multianual o año no catalogado aclara o bloquea. **Requisito para E-VALUE**: hospedar
   la autoridad de conversión (catálogo de salario por vigencia, con dueño). Queda sin efecto el punto de los insumos
   v2.1 que pedía re-sellar por F14.
3. **Top-N > 100 = P8 de ADR-103** (ejecutar 100 y anunciar el tope). No es decisión nueva. **Mismatch de C1.2 a
   corregir**: `app/core/foundation_authority_values.py:33` (worktree `secoppal-m1-c12`, rama `mission/m1-c12-ir-v2`)
   dice `MAX_TOP_N = 100  # P8 (ADR-103): «los 500 más caros» exceeds it and is clarified, never clamped`, y la
   validación de `CardinalityValue` (`:529`, `_int(self.n, "CARDINALITY_N", 1, MAX_TOP_N)`) rechaza n > 100 en vez de
   topar y anunciar. El texto de C0 §11 P8 («por encima, aclarar») debe alinearse.
4. **Búsqueda nueva = consulta autosuficiente** (no depende del turno anterior; universo explícito o `UNSPECIFIED` +
   Product View Policy). La guarda de fragmento del diseño no puede apoyarse en «cita universo».
5. **Referente temporal relativo**: único referente activo ⇒ predecesor/sucesor; ambigüedad ⇒ CLARIFY. No sellado como
   regla universal; queda para adjudicación con el Gold. Implica que D7 («relativo al valor previo aclara») se acota a
   los relativos no temporales, y que el host necesita aritmética de periodo sobre un `TimeValue` previo.
6. **Fallo de fuente**: la Accepted IR sigue refinable; no existe `result_set_ref`. El diseño debe separar el referente
   semántico (la interpretación) del referente a resultados (manifiesto de llamada + cursor).
7. **Aclaración pendiente**: respuesta compatible modifica el marco pendiente; consulta autosuficiente lo cancela; si
   sigue ambigua se re-pregunta.

---

# Cuarta tanda (2026-10-01)

8. **Rango contradictorio (A8).** Si conservar la cota opuesta daría un intervalo vacío (inferior > superior, o iguales con
   algún operador exclusivo), la cota nueva sustituye la restricción VALUE completa y se anuncia. Bloqueaba M2a; queda
   adjudicado.
9. **Autosuficiencia estructural (A9).** Con el historial eliminado, ¿el turno forma por sí solo una SEARCH admisible?
   Sí ⇒ búsqueda nueva; no ⇒ delta. No léxica. La guarda de fragmento del diseño debe implementarse como esta prueba
   (el intérprete propone la interpretación del turno sin estado; si el host la admite sola, es búsqueda nueva), no como
   una regex.
10. **Aclaración pendiente con alternativas, sin «doble ranura».** `PendingClarification { ambiguous_span,
    alternatives: [CandidatePatch, …] }`: cada alternativa es un parche candidato completo (p. ej. comprador = X /
    territorio = X); el esquema no conoce pares de dimensiones concretos. Se define en M2a.
11. **Fuera de M2a, con comportamiento provisional y dueño** (ver `02_gold_followups/gold_candidato/FOLLOWUP_GAPS_LEDGER.*`):
    territorio contenido en el comprador (E-GEO/BUYER, CLARIFY); relativo sobre ventana relativa o varios referentes
    (E-TIME, CLARIFY); negación sobre conjuntos (M2b, fail-closed); eco y procedencia de SMMLV (E-VALUE/presentación; M2a
    sólo conserva `unit`, año, cantidad fuente y COP convertido); «no hay página porque la fuente falló» (código de razón
    al implementar el protocolo; `semantic_ref` existe, `result_set_ref` no); `observed_executor = LEGACY_SEARCH` (esquema
    de observación); follow-ups tras PROFILE/COUNT/RANK (Gold de M3).
12. **Sello en dos slices**: `M2A_GOLD` (en alcance y adjudicado) y `FOLLOWUP_FRONTIER` (diferido a M2b/M3/E-*). No se
    exige acuerdo completo fuera de M2a para sellar el Gold M2a.

---

# Quinta tanda (2026-10-01) — cierre semántico del Gold M2a

13. **A8.1.** Un intervalo generado por un follow-up de cota opuesta es contradictorio si queda vacío o si degenera a un
    único punto: la cota nueva sustituye la restricción VALUE completa y se anuncia. Un singleton pedido explícitamente
    sigue siendo válido. Cierra G-FU-01b.
14. **A9.1.** Con SEARCH activa, un turno que sólo aporta condiciones o modificadores y no introduce TOPIC nuevo, ACTOR
    nuevo ni operación explícitamente nueva es DELTA aunque pase la prueba aislada de A9. Se define sobre la forma de la
    IR, no con léxico. Cierra G-FU-02b. Implicación para el diseño: la guarda de fragmento compara las dimensiones de la
    interpretación sin historial con el ancla, no el texto.
15. **G-FU-03.** `PendingClarification { ambiguous_span, candidate_patches[] }`; sin doble ranura. Va al diseño v2.2.
16. Los otros 18 huecos del ledger quedan diferidos con el dueño y el comportamiento provisional declarados. M2a no se
    amplía. Con esta tanda el trabajo semántico del Gold M2a queda cerrado.

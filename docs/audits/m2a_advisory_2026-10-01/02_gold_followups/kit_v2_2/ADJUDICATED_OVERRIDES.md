# Reglas adjudicadas por el usuario (tercera tanda, 2026-10-01) — PREVALECEN

Este archivo prevalece sobre `SCHEMA_v2_1.md` y sobre cualquier versión anterior de la adenda F1–F30 en todo punto
donde difieran. Lo no mencionado aquí sigue como está en `SCHEMA_v2_1.md` y en `CONTRACT_ADDENDUM_FOLLOWUPS_v2.md`
(copiada al lado). Regla de trabajo: **aplica sólo reglas adjudicadas; donde ninguna regla decide, etiqueta tu mejor
lectura por contrato, marca `ambiguity = AMBIGUOUS`, da la alternativa y usa el `gap_code`. No fuerces acuerdo.**

## A1. F26 queda RETIRADA. VALUE por lado, sin cláusulas informales
- F2 vigente: una cota nueva reemplaza la cota de SU MISMO lado; la del lado opuesto SE CONSERVA; una cota del lado que
  faltaba completa el rango. «Cambia el valor a…» (instrucción explícita) sustituye la restricción entera.
- Quedan sin efecto las cláusulas «rango sin interior ⇒ se sustituye entero» (antigua F2) y «magnitud absurda» (F26):
  no hay criterio formal adjudicado. Si tras aplicar la cota nueva la inferior es ≥ la superior, el caso NO está
  adjudicado: etiqueta tu mejor lectura, `AMBIGUOUS`, alternativa, y `gap_codes: ["RANGE_NO_INTERIOR"]` (código nuevo,
  vivo).
- F5 vigente: un monto sin unidad hereda la unidad sólo si existe UNA ÚNICA unidad activa inequívoca en el VALUE previo;
  si no, `CLARIFY` / `UNIT_AMBIGUOUS`. No existe `INHERITED_UNIT_IMPLAUSIBLE`: no lo uses.
- En `SCHEMA_v2_1.md` quedan sin efecto: la fila `INHERITED_UNIT_IMPLAUSIBLE`, la fila F26 de la tabla de comprobaciones,
  `F5/F26` en la precedencia (queda `F5`), el inciso (a) de `VALUE_RANGE_REPLACED` (sólo queda el caso de sustitución
  por instrucción explícita o `CLEAR`), y `UNIT_IMPLAUSIBLE` como gap cerrado.

## A2. SMMLV ES EJECUTABLE (reemplaza a F14). No se re-sella el Gold M1
- Un VALUE con `unit = SMMLV` se ejecuta cuando la interpretación resultante tiene UN ÚNICO AÑO autoritativo (un TIME de
  un solo año): el host convierte a pesos con el salario de esa vigencia y lo anuncia. Disposición `EXECUTE`.
- Un follow-up sobre un previo con SMMLV hereda la unidad (F5 la admite: es la única unidad activa) y, si cambia el año,
  el valor se RECALCULA sobre la nueva vigencia única (eco de la conversión).
- TIME que abarca varios años, o un año fuera del catálogo de salarios: `CLARIFY` / `SMMLV_YEAR_AMBIGUOUS` (código nuevo).
- SMMLV sin ningún TIME en la interpretación resultante: NO adjudicado. Mejor lectura + `AMBIGUOUS` +
  `gap_codes: ["SMMLV"]` (vuelve a estar vivo para este supuesto).
- Un `prior.ir` de búsqueda EJECUTADA puede contener `unit = SMMLV`. Quedan sin efecto `SMMLV_UNSUPPORTED`, la fila F14
  de la tabla de comprobaciones y F14 en la precedencia.

## A3. Top-N por encima de 100
- F20 se mantiene: ejecuta con 100 y lo anuncia (`TOPN_CAPPED`). NO es una decisión nueva: es la P8 de ADR-103. No hay
  tensión X10.

## A4. Búsqueda nueva = consulta AUTOSUFICIENTE (no «cita universo»)
- Una consulta es autosuficiente cuando NO DEPENDE del turno anterior para interpretarse. Puede traer universo explícito
  o no traerlo (universo `UNSPECIFIED` + política de vista por defecto). Citar un sustantivo de universo no es condición
  necesaria ni suficiente.
- F22 se mantiene: un tema desnudo tras una búsqueda es DELTA. En general, un fragmento elíptico o anafórico, o una sola
  condición sin marco de petición, depende del turno previo ⇒ DELTA.
- Entre ambos extremos no hay regla adjudicada: decide por dependencia, y si caben las dos lecturas marca `AMBIGUOUS`
  con `gap_codes: ["UNIVERSE_FRAGMENT"]`.
- Queda sin efecto la frase de `SCHEMA_v2_1.md` §5.1 que define «consulta completa» por universo citado.

## A5. Referente temporal relativo («el año anterior», «el mes siguiente», «un trimestre antes»)
- NO es `CLARIFY` universal. Si existe UN ÚNICO referente temporal activo en la interpretación previa, de granularidad
  compatible, la expresión se resuelve respecto de él (predecesor / sucesor): `SET TIME`, `depends_on_prior_value = true`,
  `year_source = PREVIOUS_ACCEPTED_TIME`, `m2a_scope = IN`, `EXECUTE`, anunciado.
- Si no hay referente activo, hay más de uno, o cabe leerla respecto del reloj con otro resultado: `CLARIFY` /
  `RELATIVE_TO_PREVIOUS_VALUE`.
- No está sellado: marca SIEMPRE `gap_codes: ["RELATIVE_ANCHOR"]` en estos casos y, si dudas, `AMBIGUOUS` con alternativa.
- F15 sigue rigiendo los relativos NO temporales («bájalo», «el doble», «pero no tanto»): `OUT_RELATIVE_TO_PREVIOUS`.

## A6. Turno previo aceptado cuya fuente falló
- La interpretación aceptada sigue siendo REFINABLE: un delta semántico se aplica y se ejecuta (`EXECUTE`).
- No existe referente a resultados: lo que necesita filas o página (ver más, «el segundo», exportar lo mostrado) no tiene
  a qué referirse ⇒ no se ejecuta; disposición `INFORM` (o `CLARIFY` si hay algo que elegir).
- Distingue siempre el referente SEMÁNTICO (la consulta: F4, re-consulta) del referente a RESULTADOS (filas/página).
- `prior.state = NOT_EXECUTED` sin `pending` significa este caso. F29 se aplica sólo cuando hay una pregunta pendiente.

## A7. Aclaración pendiente
- Una respuesta COMPATIBLE con la pregunta modifica el marco pendiente (la base es el marco que estaba pendiente).
- Una consulta nueva AUTOSUFICIENTE (A4) cancela la pendiente y es búsqueda nueva.
- «No, mejor X» con X fragmento: cancela la pregunta y aplica X como delta SOBRE EL MARCO PENDIENTE.
- Si tras aplicar la respuesta el marco sigue ambiguo: `REASK`.
- F24 («cualquiera» ⇒ `REASK`) y F25 (respuesta que además edita otra ranura ⇒ `REASK`) se mantienen. Si dudas de si una
  respuesta con edición es «compatible», etiqueta tu mejor lectura y márcalo `AMBIGUOUS`.

## Tensiones que siguen abiertas (no adjudicadas; decláralas, no las resuelvas)
X7 (F16 con varios positivos o valor contenido), X8 (CLEAR vacío junto a otra operación), X9 (qué cuenta como «sin
condición»), X12 (territorio contenido o comprador nacional en F30), X13 (F12 frente a F30), X15 (contexto vencido con
pendiente), X16 (anuncio de la unidad compartida en un rango), X17 (F27 frente a ACTOR×STATE del contrato).

---

# Cuarta tanda (2026-10-01) — A8 y A9. Prevalecen sobre A1 y A4 donde difieran

## A8. Rango contradictorio (cierra el supuesto que A1 dejó sin adjudicar)
- Regla general (igual que A1): una cota nueva sustituye la cota de su mismo lado y la opuesta se conserva.
- EXCEPCIÓN ADJUDICADA: si conservar la cota opuesta produciría un INTERVALO VACÍO, la cota nueva sustituye la restricción
  VALUE COMPLETA (la opuesta desaparece) y el cambio se ANUNCIA al usuario.
- Criterio formal de «intervalo vacío», con los montos en la misma unidad tras la herencia (F5/F6): la cota inferior es
  MAYOR que la superior; o son IGUALES y al menos uno de los dos operadores es exclusivo (`greater_than` / `less_than`).
  Un rango de un solo punto con ambos operadores inclusivos NO es vacío: ahí la opuesta se conserva.
- Cuando A8 aplica: `EXECUTE`, `ambiguity = CLEAR` (por este motivo), sin `RANGE_NO_INTERIOR` en `gap_codes`, y anota
  `adjudicated_by: ["A8"]` en `notes`. El anuncio corresponde al efecto `VALUE_RANGE_REPLACED`.
- Si los montos no son comparables (unidades distintas no convertibles, p. ej. millones frente a SMMLV): no adjudicado,
  `AMBIGUOUS`.

## A9. Búsqueda nueva frente a delta: prueba ESTRUCTURAL de autosuficiencia (precisa A4)
- Prueba: BORRA EL HISTORIAL. Si el turno, él solo, puede producir una búsqueda (SEARCH) ADMISIBLE por el contrato M1
  —universo explícito, o universo no dicho más la política de vista por defecto—, es AUTOSUFICIENTE ⇒ `NEW_SEARCH`.
  Si necesita material de la interpretación previa para ser ejecutable o para tener sentido (elipsis, anáfora, conector
  de continuación «y…», «ahora…», «sólo…», «pero…», una condición suelta sin marco), es `DELTA`.
- No es una prueba léxica: llevar o no un sustantivo de universo no decide.
- F22 se mantiene como caso adjudicado: un tema desnudo tras una búsqueda es `DELTA`.
- Aplica la prueba al turno tal como está escrito. Si pasa la prueba pero trae una marca explícita de continuación, gana
  la marca (`DELTA`). Si tras aplicar la prueba siguen cabiendo las dos lecturas, `AMBIGUOUS` con
  `gap_codes: ["UNIVERSE_FRAGMENT"]` y la alternativa.
- Con aclaración pendiente (A7): una consulta autosuficiente según esta prueba cancela la pendiente.

---

# Quinta tanda (2026-10-01) — A8.1, A9.1 y G-FU-03. Prevalecen sobre A8 y A9 donde difieran

## A8.1. Rango que degenera a un solo punto
- Un intervalo GENERADO POR UN FOLLOW-UP de cota opuesta cuenta como contradictorio tanto si queda VACÍO (A8) como si
  DEGENERA A UN ÚNICO PUNTO (cota inferior = cota superior, sean los operadores inclusivos o exclusivos).
- En ambos casos la cota nueva sustituye la restricción VALUE COMPLETA y se anuncia (`VALUE_RANGE_REPLACED`).
- Excepción: una igualdad o un valor único PEDIDO EXPLÍCITAMENTE por el usuario («exactamente 500 millones», «de 500
  millones justos», un rango «entre 500 y 500») sigue siendo válido y se ejecuta tal cual.
- Cuando A8.1 aplica: `EXECUTE`, `ambiguity = CLEAR` por este motivo, sin `RANGE_NO_INTERIOR`, y
  `adjudicated_by: ["A8.1"]` en `notes` con el cálculo.

## A9.1. Una sola condición o modificador con búsqueda activa es DELTA
- Con una búsqueda (SEARCH) activa como ancla, un turno cuya interpretación, leída SIN historial, sólo aporta
  condiciones o modificadores (TIME, VALUE, GEOGRAPHY, BUYER, STATE, MODALITY, CONTRACT_TYPE_OR_NATURE, ORDER,
  CARDINALITY o el universo) y NO introduce un TOPIC nuevo, un ACTOR nuevo ni una operación explícitamente nueva, es
  `DELTA`, aunque pase la prueba aislada de autosuficiencia de A9.
- Se decide sobre la FORMA de la interpretación (qué dimensiones trae), no con léxico especial: no hay listas de
  palabras que la activen o la desactiven.
- Si el turno trae un TOPIC nuevo, un ACTOR nuevo o una operación nueva, A9.1 no aplica y rige A9. F22 (tema desnudo tras
  una búsqueda ⇒ DELTA) sigue vigente.
- Cuando A9.1 aplica: `adjudicated_by: ["A9.1"]` en `notes`, indicando qué dimensiones trae el turno sin historial.

## G-FU-03. Aclaración pendiente con alternativas
- `PendingClarification` lleva `candidate_patches[]`: cada alternativa es un parche candidato completo (por ejemplo,
  `SET BUYER = X` / `SET GEOGRAPHY = X`). No existe «doble ranura». Es decisión de forma del diseño; no cambia ninguna
  etiqueta.

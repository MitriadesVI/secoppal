# Mini-pasada SB v2.2 — A8 (rango contradictorio) y A9 (autosuficiencia estructural)

Base: `etiquetas_SB_v2_1/labels_v2_1.jsonl`. Sólo se tocan los 16 casos pedidos más SF170 (hallado en el barrido de VALUE por lado).
Los demás 174 registros son byte-idénticos a v2.1 (lo comprueba `minipass_v2_2.py`). `effects`/`expected_echo` se rederivan con §6/§7.

## Barrido A8 sobre VALUE por lado (SF085–SF108, SF170)

- Cumplen el criterio de vacío y ya estaban marcados: SF090, SF094, SF099, SF103 (más SF182 y SF183 fuera del rango barrido).
- Punto único con ambos operadores inclusivos (no vacío): SF091, SF095.
- SF100: daría vacío (< 90 frente a >= 100), pero ya lo decide la instrucción explícita «cambia el valor a…» (F2/O4). Sin cambio.
- SF105: > 900 MILES frente a <= 200 MILLONES: comparables (900.000 < 200.000.000), tiene interior. Sin cambio.
- **SF170 (no estaba marcado):** >= 200 MILLONES frente a <= 900 SMMLV: A8 los declara no comparables => AMBIGUOUS. Se incluye.
- Ningún otro caso (SF085–SF089, SF092, SF093, SF096–SF098, SF101, SF102, SF104, SF106–SF108) tiene cotas opuestas en conflicto.

## Casos revisados

| id | criterio | ¿cambia la disposición? | ¿cambia la IR? | antes (v2.1) | ahora (v2.2) |
|---|---|---|---|---|---|
| SF051 | A9 + A8 (informativo) | no | no | ANSWER_TO_CLARIFICATION · REASK/ANSWER_EDITS_OTHER_SLOT · CLEAR · IR null | ANSWER_TO_CLARIFICATION · REASK/ANSWER_EDITS_OTHER_SLOT · CLEAR · IR null |
| SF053 | A9 | no | no | ANSWER_TO_CLARIFICATION · REASK/ANSWER_EDITS_OTHER_SLOT · CLEAR · IR null | ANSWER_TO_CLARIFICATION · REASK/ANSWER_EDITS_OTHER_SLOT · CLEAR · IR null |
| SF063 | A9 + A7/F23 | no | no | NEW_SEARCH · NEW_SEARCH · AMBIGUOUS · IR UNSPECIFIED/HISTORICAL_PROCESSES: TOPIC contains dotación; BUYER is Gobernación del Cauca; TIME during_year 2025 | NEW_SEARCH · NEW_SEARCH · CLEAR · IR UNSPECIFIED/HISTORICAL_PROCESSES: TOPIC contains dotación; BUYER is Gobernación del Cauca; TIME during_year 2025 |
| SF090 | A8 | no | no | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE less_or_equal 200 millones | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE less_or_equal 200 millones |
| SF091 | A8 | no | sí | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE less_or_equal 300 millones | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE greater_or_equal 300 millones; VALUE less_or_equal 300 millones |
| SF094 | A8 | no | no | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE greater_than 400 millones | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE greater_than 400 millones |
| SF095 | A8 | no | sí | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE greater_or_equal 200 millones | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE less_or_equal 200 millones; VALUE greater_or_equal 200 millones |
| SF099 | A8 | no | no | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE greater_than 350 millones | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains ambulancias; BUYER is Gobernación de Antioquia; VALUE greater_than 350 millones |
| SF103 | A8 | no | no | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains estudios y diseños; BUYER is Alcaldía de Medellín; VALUE greater_or_equal 350 millones | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains estudios y diseños; BUYER is Alcaldía de Medellín; VALUE greater_or_equal 350 millones |
| SF126 | A9 | sí | sí | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains alquiler de maquinaria pesada; BUYER is Alcaldía de Tunja; TIME during_year 2023 | NEW_SEARCH · NEW_SEARCH · CLEAR · IR UNSPECIFIED/HISTORICAL_PROCESSES: TOPIC contains alquiler de maquinaria pesada; TIME during_year 2023 |
| SF132 | A9 | no | no | DELTA · EXECUTE · AMBIGUOUS · IR PROCESSES: TOPIC contains aseo; BUYER is Alcaldía de Girardot | DELTA · EXECUTE · AMBIGUOUS · IR PROCESSES: TOPIC contains aseo; BUYER is Alcaldía de Girardot |
| SF134 | A9 | no | no | NEW_SEARCH · NEW_SEARCH · AMBIGUOUS · IR CONTRACTS: TOPIC contains vigilancia | NEW_SEARCH · NEW_SEARCH · CLEAR · IR CONTRACTS: TOPIC contains vigilancia |
| SF170 | A8 (barrido) | no | no | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains suministro de papelería; BUYER is Gobernación de Caldas; VALUE greater_or_equal 200 millones; TIME during_year 2025; VALUE less_or_equal 900 SMMLV | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains suministro de papelería; BUYER is Gobernación de Caldas; VALUE greater_or_equal 200 millones; TIME during_year 2025; VALUE less_or_equal 900 SMMLV |
| SF182 | A8 | no | no | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains interventoría de obras; BUYER is Universidad del Valle; VALUE less_or_equal 90 millones | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains interventoría de obras; BUYER is Universidad del Valle; VALUE less_or_equal 90 millones |
| SF183 | A8 | no | no | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains interventoría de obras; BUYER is Universidad del Valle; VALUE less_or_equal 90 millones | DELTA · EXECUTE · CLEAR · IR CONTRACTS: TOPIC contains interventoría de obras; BUYER is Universidad del Valle; VALUE less_or_equal 90 millones |
| SF185 | A9 | no | no | NEW_SEARCH · NEW_SEARCH · AMBIGUOUS · IR UNSPECIFIED/CURRENT_OPPORTUNITIES: TOPIC contains mantenimiento de parques; BUYER is Alcaldía de Duitama | NEW_SEARCH · NEW_SEARCH · CLEAR · IR UNSPECIFIED/CURRENT_OPPORTUNITIES: TOPIC contains mantenimiento de parques; BUYER is Alcaldía de Duitama |
| SF187 | A9 | sí | sí | DELTA · EXECUTE · AMBIGUOUS · IR CONTRACTS: TOPIC contains pavimentación; BUYER is Alcaldía de Montería; TIME during_year 2024 | NEW_SEARCH · NEW_SEARCH · CLEAR · IR CONTRACTS: TIME during_year 2024 |

## Cálculo o razonamiento por caso

- **SF051** (A9 + A8 (informativo)): «pero» es marca de continuación (A9) -> no es autosuficiente; respuesta + edición -> F25 REASK. A8 informativo: < 40 millones (excl.) frente a > 150 millones (excl.) sería vacío, pero el turno no se ejecuta.
- **SF053** (A9): Historial borrado: «los de la Alcaldía de Piedecuesta de enero a junio de 2025» es elíptico («los de» sin núcleo) -> necesita el previo -> DELTA; respuesta + edición de BUYER -> F25 REASK (sin cambio).
- **SF063** (A9 + A7/F23): Historial borrado: X = «dotación de la Gobernación del Cauca en 2025» forma SEARCH admisible (UNSPECIFIED + PV1). «no, mejor» es el rechazo de F23/A7, no una marca de continuación de A9: F23 dice que con consulta completa es búsqueda nueva.
- **SF090** (A8): opuesta conservada: inferior >= 500 millones (incl.); nueva superior <= 200 millones (incl.); unidad MILLONES (dicha); 500 > 200
- **SF091** (A8): opuesta conservada: inferior >= 300 millones (incl.); nueva superior <= 300 millones (incl.); unidad MILLONES; 300 = 300 y ambos inclusivos
- **SF094** (A8): opuesta conservada: superior <= 200 millones (incl.); nueva inferior > 400 millones (excl.); unidad MILLONES; 400 > 200
- **SF095** (A8): opuesta conservada: superior <= 200 millones (incl.); nueva inferior >= 200 millones (incl., «desde» D4); unidad MILLONES; 200 = 200 y ambos inclusivos
- **SF099** (A8): la inferior previa (>= 100) se sustituye por su lado; opuesta conservada: superior <= 300 millones (incl.); nueva inferior > 350 millones (excl.); unidad MILLONES; 350 > 300
- **SF103** (A8): opuesta conservada: superior <= 300 millones (incl.); nueva inferior >= 350 (incl.) con unidad heredada MILLONES (F5: única unidad previa); 350 > 300
- **SF126** (A9): Historial borrado: «alquiler de maquinaria pesada en 2023» = tema + año -> SEARCH admisible (UNSPECIFIED + PV1); sin marca de continuación; no es tema desnudo (F22 no aplica: trae año).
- **SF132** (A9): «y…» es marca explícita de continuación -> DELTA (sin cambio). La ambigüedad que queda no es de A9 sino de significado («licitaciones» = procesos o oportunidades).
- **SF134** (A9): Historial borrado: «contratos de vigilancia» -> SEARCH admisible (CONTRACTS + TOPIC); sin marca de continuación -> NEW_SEARCH.
- **SF170** (A8 (barrido)): inferior >= 200 millones (incl.) frente a nueva superior <= 900 SMMLV (incl.): MILLONES frente a SMMLV, no comparables según A8
- **SF182** (A8): opuesta conservada: inferior >= 150 millones (incl.); nueva superior <= 90 millones (incl.); unidad MILLONES (dicha); 150 > 90
- **SF183** (A8): opuesta conservada: inferior >= 150 millones (incl.); nueva superior <= 90 (incl.) con unidad heredada MILLONES (F5); 150 > 90
- **SF185** (A9): Historial borrado: «mantenimiento de parques de la Alcaldía de Duitama» -> SEARCH admisible (UNSPECIFIED + PV6); sin marca de continuación; no es tema desnudo (trae comprador).
- **SF187** (A9): Historial borrado: «contratos de 2024» -> SEARCH admisible (CONTRACTS + TIME 2024, con una condición, F19 no aplica); sin marca de continuación -> NEW_SEARCH.

## Validador

`self_check_v2_2.py` -> `self_check.json`: los mismos controles de v2.1 más el criterio formal de A8 (inferior > superior, o iguales con algún
operador exclusivo, prohibido en `resulting_ir` y en `alternatives`) y la forma de los casos adjudicados por A8 (EXECUTE, CLEAR, sin RANGE_NO_INTERIOR, con VALUE_RANGE_REPLACED).

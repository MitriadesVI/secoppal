## 5. Slice sintético (separado del REAL)

Archivo propio (`followup_synthetic.jsonl`), en git, con textos **inventados** (ninguno parecido a un texto REAL:
misma comprobación de n-gramas que el Gold M1) y sin datos personales. No se mezcla con el REAL ni en conteos ni en
el sello: son dos entradas del sello.

Cada caso es un par (previo, follow-up) con el previo escrito como IR Gold completa y `prior.selection_rule =
SYNTHETIC_AUTHORED`. Donde tiene sentido, cada caso lleva un **gemelo** que cambia un solo rasgo.

| Clase (ausente o casi en los 65) | Qué cubre | Casos |
|---|---|---|
| Polaridad negativa | `SET` con `NEGATIVE` por dimensión (TOPIC, GEOGRAPHY, BUYER, MODALITY, STATE); negación de un valor ya arrastrado en positivo (debe aclarar); negación en VALUE y TIME | 14 |
| Top-N | cifra explícita, superlativo singular, orden nuevo con top-N arrastrado (P2 del diseño), tope de 100, `CLEAR` de orden con cardinalidad | 10 |
| Tiempo relativo | «este año», «últimos N meses» como `SET TIME`; relativo sobre un TIME arrastrado; mes sin año con previo de varios años y con previo relativo (deben aclarar); mes sin año sin TIME previo (regla del reloj, anunciada) | 12 |
| Relativo al valor previo (D7) | «el mes siguiente», «el año anterior», «bájalo», «el doble», «el mismo comprador»: todos `OUT_RELATIVE_TO_PREVIOUS`, `CLARIFY` | 10 |
| Respuesta a aclaración | las 11 filas de `R5_REPAIR.md` §P2 reexpresadas; opción por evento; respuesta que no resuelve; segundo cambio de marco; pendiente caducado | 18 |
| Referentes ordinales y de fila | «el segundo», «ese contrato», «de esos, los de…»: `OUT_REFERENT_EXTENSIONAL` | 8 |
| Cambio explícito de universo | contratos ↔ procesos, a oportunidades, ancla dual que se queda con uno, universo dicho + condición dependiente de dataset (STATE, MODALITY), vista elegida por botón que **no** se recalcula tras un delta | 12 |
| VALUE por lado | tabla completa: lado previo (ninguno, inferior, superior, ambos) × lado nuevo (inferior, superior, ambos) × relación de cifras (con interior, sin interior, igual); «cambia el valor a…» | 16 |
| Herencia de unidad | unidad única; dos unidades en el previo; previo en SMMLV; previo en pesos sin escala; cifra bajo el piso con unidad propia; cadena de dos herencias | 8 |
| `CLEAR` distinto de VALUE | quitar periodo, lugar, estado, comprador; quitar lo que no existe; quitar lo único que hace segura la consulta | 8 |
| Orden | ascendente por valor y por fecha; orden nuevo sobre orden previo; orden con filtro por token | 6 |
| Búsqueda nueva con historia | gemelos «fragmento / consulta completa» por dimensión (como `P159`–`P164`); fragmento con sustantivo de universo y sin contenido; tema solo tras una búsqueda con comprador | 12 |
| Ancla inválida | no ejecutada, vacía, vencida (a 31 min, 5 h, 13 h), ilegible, de dueño legacy | 10 |
| Protocolo sobre resultado Foundation | página tras búsqueda, tras delta y tras cambio de orden; ancla dual; botón de orden; quitar filtro; guardar radar (mensaje) | 10 |
| Dos o más dimensiones | combinaciones no vistas en los 65, incluida una con `CLEAR` + `SET` | 8 |
| **Total** | | **162** |

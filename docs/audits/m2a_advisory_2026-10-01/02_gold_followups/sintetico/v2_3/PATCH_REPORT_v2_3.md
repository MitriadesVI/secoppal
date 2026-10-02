# PATCH_REPORT_v2_3

Base: `../v2_2/followup_synthetic_cases.jsonl` (164) -> `followup_synthetic_cases.jsonl` (191 = 164 + 27 nuevos, SF165–SF191).
Reglas: `kit_v2_2/ADJUDICATED_OVERRIDES.md` (A1–A7) sobre el esquema 2.1. Reproducible: `herramientas_autor/add_cases_v2_3.py`.

## 1. Los 164 casos existentes
Los 164 registros son byte-idénticos a v2.2 (comparación de líneas, texto y `prior` incluidos) y `AUTHOR_INTENT.jsonl` conserva las 164 líneas
previas sin cambios. Ningún `prior` quedó inválido de forma por A1–A7: lista de corregidos = vacía.
Cambia el VALIDADOR, no los datos: se retiró la comprobación F14 («ningún SMMLV en un previo EXECUTED de Foundation»), porque A2 lo permite.
Efecto de significado (no de forma) sobre casos existentes, sin tocarlos: SF085–SF100 y SF103 (VALUE por lado: los de rango sin interior,
p. ej. SF090, SF091, SF094, SF095, SF099, SF103, ya no tienen «sustitución entera» sino no-adjudicado `RANGE_NO_INTERIOR`); SF050 y SF047–SF049/SF055
(A4: ya no dependen de citar universo); SF038, SF045 (A5: referente temporal; el criterio cambió de CLARIFY a resolver con referente único);
SF135 y SF136 (A6: previo NOT_EXECUTED sin pendiente es refinable); SF015–SF024 fuera de F21 (ahora cubierto por SF171/SF172).

## 2. Casos nuevos por frontera
- **SMMLV (A2)**: SF165, SF166, SF167, SF168, SF169, SF170
- **Orden por fecha sin dirección (F21)**: SF171, SF172
- **Referente temporal relativo (A5)**: SF173, SF174, SF175, SF176, SF177
- **Fuente fallida (A6)**: SF178, SF179, SF180, SF181
- **Cota contradictoria (A1)**: SF182, SF183
- **Autosuficiencia (A4)**: SF184, SF185, SF186, SF187
- **Aclaración pendiente (A7)**: SF188, SF189, SF190, SF191

Detalle (previo -> follow-up -> rasgo); gemelos entre paréntesis:
- SMMLV (A2): SF165 añade condición sobre previo en SMMLV con un año (gemelo SF166, SF168, SF169); SF166 cambia el año (recalcular); SF167
  «mejor mínimo 800» hereda SMMLV; SF168 previo en SMMLV con TIME 2023–2025; SF169 previo en SMMLV sin TIME; SF170 cita SMMLV sobre previo en
  millones (dos unidades). SF165/SF168/SF169 comparten texto y sólo cambia el TIME del previo.
- F21: SF171 sin top-N, SF172 con top-N arrastrado (gemelos).
- A5: SF173 «el año anterior» con un único año (gemelo SF175, SF177 mismo texto), SF174 «el mes siguiente» con un único mes, SF175 TIME plurianual,
  SF176 sin TIME, SF177 ventana relativa previa.
- A6: SF178 delta semántico, SF179 «más resultados», SF180 referente de fila, SF181 exportar; los cuatro comparten previo `NOT_EXECUTED` sin `pending`.
- A1: SF182 cota superior menor que la inferior con unidad dicha, SF183 con unidad heredada (gemelos; previo ≥ 150 millones, nuevo máximo 90).
- A4: SF184 con sustantivo de universo / SF185 sin (gemelos autosuficientes); SF186 «y los contratos de 2024?» depende del previo / SF187 «contratos
  de 2024» (frontera sin regla, `UNIVERSE_FRAGMENT`).
- A7: SF188 respuesta compatible (periodo); SF189 compatible que deja el marco ambiguo (4 Villanuevas, «la del Caribe» deja 2); SF190 consulta
  autosuficiente que cancela; SF191 «no, mejor X» con X fragmento.

Forma: esquema 2.1 como en v2.2 (`input_kind = TEXT` en los 27; ningún evento nuevo), `prior.state = PENDING` con `pending` completo en SF188–SF191,
`NOT_EXECUTED` sin `pending` en SF178–SF181, `unit = SMMLV` en los previos de SF165–SF169, hint de TIME con fechas absolutas en rangos y relativos.

## 3. Comprobaciones
`form_check.json`: 191 casos, 0 errores. `similarity_check.json`: 0 rechazados, máximo Jaccard 0,333 vs REAL y 0,444 vs público (SF178 se
reescribió de «y solo los de más de 100 millones» a «y que valgan más de 100 millones» porque daba 0,667 contra un par público); sin patrones de
cédula/NIT/correo/teléfono.

## 4. Notas de construcción / dudas
- A2 pide un año «dentro del catálogo de salarios»: no hay caso con año fuera del catálogo (los años usados son 2023–2025).
- SF170 («hasta 900 SMMLV» sobre previo en millones) es el único caso con dos unidades activas al terminar; el previo no está en SMMLV.
- SF176/SF177: A5 manda marcar `RELATIVE_ANCHOR` siempre; el etiquetador debe decidir CLARIFY vs resolver contra el reloj.
- SF187 depende de A4: «contratos de 2024» sin marco de petición podría leerse como autosuficiente (AMBIGUOUS).
- SF189: el criterio «compatible que deja el marco ambiguo» exige que «Caribe» cubra exactamente dos de las cuatro opciones (La Guajira y Bolívar).

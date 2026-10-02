# AUTHOR_REPORT — slice sintético de follow-ups (162 casos)

NO mostrar este archivo, `AUTHOR_INTENT.jsonl` ni `author_tools/` a los etiquetadores.

## Conteo por clase (tabla de SYNTHETIC_SPEC §5)

| Clase | Tabla | Escrito | Ids |
|---|---|---|---|
| Polaridad negativa | 14 | 14 | SF001–SF014 |
| Top-N | 10 | 10 | SF015–SF024 |
| Tiempo relativo | 12 | 12 | SF025–SF036 |
| Relativo al valor previo (D7) | 10 | 10 | SF037–SF046 |
| Respuesta a aclaración | 18 | 18 | SF047–SF064 |
| Referentes ordinales y de fila | 8 | 8 | SF065–SF072 |
| Cambio explícito de universo | 12 | 12 | SF073–SF084 |
| VALUE por lado | 16 | 16 | SF085–SF100 |
| Herencia de unidad | 8 | 8 | SF101–SF108 |
| CLEAR distinto de VALUE | 8 | 8 | SF109–SF116 |
| Orden | 6 | 6 | SF117–SF122 |
| Búsqueda nueva con historia | 12 | 12 | SF123–SF134 |
| Ancla inválida | 10 | 10 | SF135–SF144 |
| Protocolo sobre resultado Foundation | 10 | 10 | SF145–SF154 |
| Dos o más dimensiones | 8 | 8 | SF155–SF162 |
| **Total** | **162** | **162** | |

Casos que no pude construir: ninguno. Los que quedan con forma discutible están en "Interpretaciones".

## Verificaciones
- `similarity_check.json`: 0 rechazados. Máximo Jaccard (trigramas de palabras, plegado de tildes/mayúsculas, con relleno
  `<s>`/`</s>` para que los fragmentos de 1–2 palabras también produzcan trigramas) contra los 620 REAL privados: 0,300;
  contra los 394 textos públicos NEIGHBOR/MINIMAL_PAIR/ADVERSARIAL: 0,444. Umbral de rechazo 0,6 aplicado a ambos.
  Primera pasada rechazó SF136 (0,625 vs P163 público) y quedaron SF123/SF126/SF049 en 0,5; se reescribieron.
  Los textos REAL sólo se cargan en memoria; el JSON guarda puntajes y gold_id más cercano, nunca texto.
- Datos personales: sin coincidencias de cédula/NIT (9–10 dígitos, NIT con puntos, marcadores nit/cc/cédula, correo,
  celular). Compradores: sólo entidades públicas. Proveedor: único nombre ficticio, «Inversiones Cóndor del Sur S.A.S.»
  (ACL13/ACL14). Ninguna persona natural.
- Los registros contienen sólo `gold_id, slice, followup_text, followup_sha256, source_kind, prior` (sin campos de etiqueta).
  `prior.gold_id = null`, `selection_rule = SYNTHETIC_AUTHORED`.
- Regenerar: `python3 author_tools/build_cases.py && python3 author_tools/similarity_check.py`.

## Convenciones de forma que tuve que fijar (el esquema no las define)
1. `prior.ir.order` = `{"operator": "value_desc|value_asc|date_desc|date_asc", "polarity": "POSITIVE"}`;
   `prior.ir.cardinality` = `{"operator": "top_n", "value_hint": "10"}` (C0 §5.1.3). Siempre con orden.
2. VALUE en el previo: `value_hint` con la cifra y la unidad como las diría el usuario («200 millones», «500 SMMLV»,
   «40.000.000 pesos»); `unit` ∈ `MILLONES | MIL_MILLONES | SMMLV | PESOS | null`. La lista cerrada de escalas del
   validador no estaba en mis insumos: si usa otros códigos, mapear.
3. `view_source`: `USER` cuando el universo está dicho (CONTRACTS/PROCESSES) o la vista fue elegida por botón
   (universo UNSPECIFIED + `product_view` + `view_source = USER` + `chain = ["PROTOCOL:option"]`); `DERIVED` con
   UNSPECIFIED y `product_view` calculada con PV1–PV6 (UNI09/UNI10/UNI11 son el trío de F9).
4. **Ancla dual** (UNI04, UNI05, PRO04): el esquema sólo tiene un `universe`; usé `universe = "DUAL"` más
   `universes = ["PROCESSES","CONTRACTS"]`. Extensión mía; el validador debe aceptarla o decidir otra codificación.
5. «Opción por evento» (ACL12–ACL14): `followup_text` es la etiqueta del botón («Alcaldía de San Pedro (Valle del Cauca)»,
   «Como comprador», «Como lugar»); el `prior` lleva `state = PENDING` y `pending` con las opciones. No hay campo de evento.
6. `prior.state` en Ancla inválida: no ejecutada = `NOT_EXECUTED`; vencida = `EXPIRED` con `gap_seconds` 1860 / 18000 /
   46800; vacía = `NONE` con IR de cero condiciones, `owner = NONE`, `gap_seconds = null`; ilegible = `state = EXECUTED`
   con `ir = null`; legacy = `owner = LEGACY:COUNT` / `LEGACY:RANK_SUPPLIERS` con `operation` COUNT / RANK_SUPPLIERS.
7. Pendiente caducado (ACL18): `state = EXPIRED` conservando `pending` (gap 2640 s).
8. En Respuesta a aclaración el marco bloqueado es «interventoría + Alcaldía de Floridablanca + más de 150 millones» con
   TIME sin año («primer semestre»); `pending = {dimension: TIME, reason: PERIOD_WITHOUT_YEAR, options: 0}`.
   La pregunta abierta de la que parte R5 §P2 era de periodo; la reexpresé así (ACL01–ACL11 son las 11 filas, con textos
   nuevos: tema/comprador/valor/universo cambiados). Reexpresé «F1/F3» como ACL08 (previo EXECUTED, sin pendiente).
9. «Vista elegida por botón» y «universo dicho» no se distinguen en el IR más que por `view_source` + `chain` (ver 3).

## Interpretaciones y dudas de diseño (para el orquestador; no son etiquetas)
- **Fila «de esos, los de…» vs F4.** SYNTHETIC_SPEC la manda a `OUT_REFERENT_EXTENSIONAL`; F4 dice que «aquellos de…»,
  «los de mayor valor» re-consultan (no filtran lo mostrado). REF04 («de esos, los que sean de Cali») y su gemelo REF05
  («los que sean de Cali») ejercen justo esa frontera; no sé cuál esperaba el diseño, lo dejo para los etiquetadores.
- **D7 «el mismo comprador».** D705 («ahora vigilancia, pero con el mismo comprador») es en el fondo un KEEP citado, no un
  operador relativo; la tabla lo incluye en D7, lo escribí así y lo señalo como el caso menos limpio de la clase.
- **Gemelos de la clase D7.** La spec dice que todos son `CLARIFY`, por eso no hay gemelo con valor explícito; un gemelo
  «abril de 2024» habría tenido otra etiqueta y rompería «todos CLARIFY».
- **Tope de 100 (TOP07/TOP08):** el contrato dado no define qué pasa con N>100 (¿recorte anunciado o aclarar?). Lo dejo al
  etiquetado; TOP08 (N=100 exacto) es el control.
- **Región fuera de la lista (MUL05, «Magdalena Medio»)** y región dentro (POL03/POL04 «Eje Cafetero», NEW01 «Caribe»).
  Lista cerrada tomada de `geo_regions.py`/`colombia_geography.py`: Caribe, Andina, Pacífica, Orinoquía, Amazonía, Insular,
  Eje Cafetero, Santanderes, Tolima Grande («la costa» es ambigua en el producto; no la usé).
- **Herencia:** HER04 («previo en pesos sin escala» + «mínimo 650») y HER05 («cifra bajo el piso con unidad propia») son mi
  lectura de filas poco definidas en la tabla. HER05 no tiene el piso numérico definido en F5; elegí «más de 900 mil pesos»
  sobre un previo de «hasta 200 millones» (con interior).
- **SMMLV (HER03):** F14 declara el default provisional; el caso sólo ejerce la herencia hacia SMMLV.
- **«Con X» tras un ACTOR (F12):** quedó en Respuesta a aclaración (ACL13/ACL14, gemelos comprador/lugar) por ser respuesta a
  la pregunta; no hay un caso de «con X» como follow-up directo sin aclaración previa.
- **Tiempo relativo vs. reloj:** con hoy = 2026-10-01, REL10 («y en abril?») cae en abril de 2026 y REL11 («y en diciembre?»)
  en diciembre de 2025 (más reciente no posterior a hoy). Los casos dependen de esa fecha.
- **Orden con filtro por token (ORD04):** el token «ambulancia» lo escribí como TOPIC en la lectura probable; no lo fijé en el IR.
- **Protocolo:** PRO07/PRO08 son etiquetas de botón de «quitar filtro»; PRO09/PRO10 son mensajes de radar (operación FOLLOW), no
  PROTOCOL en sentido estricto, pero la fila de la tabla («guardar radar (mensaje)») los incluye.
- **Textos idénticos entre casos:** «solo marzo» (REL07/08), «contratos de alquiler de oficinas … en 2024» (ACL03/04), «y solo
  2025» (UNI09/10/11), «y en 2024?» (INV01/03/04/07/09) y «de enero a junio de 2025» (ACL10/18) comparten texto a propósito:
  el rasgo que cambia vive en el `prior`. Si el etiquetador procesa por hash de texto, usar `gold_id`.

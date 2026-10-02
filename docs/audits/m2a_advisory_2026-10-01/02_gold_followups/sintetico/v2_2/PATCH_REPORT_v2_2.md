# PATCH_REPORT_v2_2

Base: `../v2_1/followup_synthetic_cases.jsonl` (164) -> `followup_synthetic_cases.jsonl` (164), esquema `m2a-followup-gold/2.1`.
Reproducible: `herramientas_autor/convert_v2_2.py` (+ `flags.json`), `form_check.py`, `similarity_check.py`.

## 1. Cambios globales de forma (los 164)
- Se añade `schema = "m2a-followup-gold/2.1"` y `input_kind` (`TEXT` en 157; `EVENT` en 7: SF058, SF059, SF060 opciones de aclaración y
  SF149, SF150, SF151, SF152 botones de orden / quitar filtro).
- Toda IR de `prior.ir` no nula gana las claves `aspect` (null; `TOTAL` sólo en SF143, previo COUNT), `logic` (null) y `host_notice` ([]).

## 2. Cambios por id (sólo los que tocan más que lo anterior)
- SF018: ir.order {operator,direction_source}; ir.cardinality {n,n_requested}
- SF019: ir.order {operator,direction_source}; ir.cardinality {n,n_requested}
- SF023: ir.order {operator,direction_source}; ir.cardinality {n,n_requested}
- SF024: ir.order {operator,direction_source}; ir.cardinality {n,n_requested}
- SF029: ir.conditions.value_hint TIME completo (fechas absolutas)
- SF031: ir.conditions.value_hint TIME completo (fechas absolutas)
- SF033: ir.conditions.value_hint TIME completo (fechas absolutas)
- SF036: ir.conditions.value_hint TIME completo (fechas absolutas)
- SF042: ir.conditions.value_hint TIME completo (fechas absolutas)
- SF045: ir.conditions.value_hint TIME completo (fechas absolutas)
- SF047: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF048: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF049: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF050: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF051: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF052: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF053: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF055: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF056: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF057: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF058: prior.ir sin la condición BUYER ambigua (span abierto fuera de la IR); prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks); input_kind=EVENT
- SF059: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks); input_kind=EVENT
- SF060: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks); input_kind=EVENT
- SF061: prior.ir sin la condición BUYER ambigua (span abierto fuera de la IR); prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF062: prior.ir sin la condición BUYER ambigua (span abierto fuera de la IR); prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF063: prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF064: prior.state EXPIRED->PENDING; prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)
- SF076: ir.universe DUAL->BOTH (y se retira ir.universes)
- SF077: ir.universe DUAL->BOTH (y se retira ir.universes)
- SF081: prior.chain option->view
- SF103: followup_text + sha (reescrito); prior.ir VALUE (SMMLV>=500 -> <=300 millones)
- SF119: ir.order {operator,direction_source}
- SF122: ir.order {operator,direction_source}
- SF137: prior.ir [] -> null (state NONE)
- SF138: prior.state EXPIRED->EXECUTED
- SF139: prior.state EXPIRED->EXECUTED
- SF140: prior.state EXPIRED->EXECUTED; ir.order {operator,direction_source}
- SF142: prior.state EXECUTED -> UNREADABLE
- SF143: ir.aspect
- SF147: ir.order {operator,direction_source}
- SF148: ir.universe DUAL->BOTH (y se retira ir.universes)
- SF149: input_kind=EVENT
- SF150: input_kind=EVENT
- SF151: input_kind=EVENT
- SF152: input_kind=EVENT

Resumen por tipo: `prior.pending` a forma 2.1: 17 (SF047–053, 055–064); `order` a `{operator, direction_source}`: 8 (todos `STATED`);
`cardinality` a `{n, n_requested}`: 4; ancla dual `DUAL` -> `BOTH`: 3 (SF076, SF077, SF148); `value_hint` de TIME con fechas absolutas: 6
(SF029, SF031, SF033, SF036, SF042, SF045); `EXPIRED` retirado: 4 (SF138–SF140 -> `EXECUTED` con `gap_seconds` 1860/18000/46800; SF064 -> `PENDING`
con gap 2640); `chain` `option` -> `view`: 1 (SF081); `ir = null`: SF137 (`NONE`, antes IR vacía) y SF142 (`UNREADABLE`, antes `EXECUTED`);
BUYER ambiguo retirado de `prior.ir` (el span abierto no va en las condiciones): SF058, SF061, SF062; reescritura: SF103.

Convenciones que elegí donde el esquema no fija el valor (ver §6):
- `pending.reason`: «periodo sin año» -> `UNRESOLVED_VALUE:TIME`; nombre de comprador ambiguo -> `UNRESOLVED_VALUE:BUYER`; «con X» tras actor ->
  `BUYER_OR_PLACE_AFTER_ACTOR`. `asks` = 1 + nº de `PROTOCOL:reask` en `chain` (SF050 = 2, SF063 = 3).
- Periodo relativo en `value_hint`: «últimos 3 meses (2026-07-01..2026-10-01)» y «últimos 6 meses (2026-04-01..2026-10-01)» (hoy = 2026-10-01).
- Rangos: `AAAA-MM-DD..AAAA-MM-DD` (p. ej. «enero a junio de 2024» -> `2024-01-01..2024-06-30`; «2023 a 2025» -> `2023-01-01..2025-12-31`).
  Año único (`during_year`) y mes (`during_month`) siguen como «2025» y «2025-03».
- «Mil pesos» (HER05, texto «más de 900 mil pesos») no está en un `prior`; el eco `MILES` sería la escala de ese texto, no cambia nada aquí.

## 3. Textos: confirmación
Los 163 `followup_text` y `followup_sha256` restantes son idénticos a v2.1 (comparado campo a campo). Sólo cambió SF103.

### SF103 (único texto reescrito)
- Tenía: previo «contratos de estudios y diseños de la Alcaldía de Medellín de al menos 500 SMMLV» y texto «mínimo 800» (herencia de SMMLV).
- Problema: bajo F14 un `prior.ir` de Foundation `EXECUTED` no puede contener `unit = SMMLV` (el estado no es alcanzable), y F14 manda
  `SMMLV_UNSUPPORTED` en cualquier turno, así que el caso ya no ejercitaba la herencia.
- Quedó: previo «… de hasta 300 millones» (`VALUE.upper`, `MILLONES`) y texto «mejor mínimo 350» (sha nuevo).
  Frontera: unidad única heredada (`PREVIOUS_ACCEPTED_VALUE`) que deja el rango SIN interior -> F26 (`CLARIFY`/`INHERITED_UNIT_IMPLAUSIBLE`),
  no F2 (sustitución). Gemelo de SF101 (mismo texto «mejor mínimo 350», previo con cota inferior ≥200 millones -> hay interior y se hereda).
  El texto se repite a propósito: el rasgo vive en `prior`.

## 4. Textos que creo que deberían cambiar (NO los cambié) y casos marcados por F16–F30
Ninguno queda sin sentido del todo; los marcados están en `AUTHOR_INTENT.jsonl` bajo `v2_2_flag`:
- SF038: X18: «el año anterior»/«un trimestre antes» pueden ser relativos al valor previo o al reloj (RELATIVE_ANCHOR vivo)
- SF041: X18/D705: «el mismo comprador» es un KEEP citado más que un operador relativo (F15)
- SF045: X18: «el año anterior»/«un trimestre antes» pueden ser relativos al valor previo o al reloj (RELATIVE_ANCHOR vivo)
- SF047: Bajo §5.1 el texto nombra universo + contenido propio: es NEW_SEARCH (cancela el pendiente), ya no ejercita F25/respuesta híbrida; redundante con SF052/SF057
- SF048: Bajo §5.1 el texto nombra universo + contenido propio: es NEW_SEARCH (cancela el pendiente), ya no ejercita F25/respuesta híbrida; redundante con SF052/SF057
- SF049: Bajo §5.1 el texto nombra universo + contenido propio: es NEW_SEARCH (cancela el pendiente), ya no ejercita F25/respuesta híbrida; redundante con SF052/SF057
- SF050: Gemelo de SF049 cuyo único rasgo es chain=reask/asks: ninguna regla F16–F30 lo distingue; además NEW_SEARCH (§5.1)
- SF055: Bajo §5.1 el texto nombra universo + contenido propio: es NEW_SEARCH (cancela el pendiente), ya no ejercita F25/respuesta híbrida; redundante con SF052/SF057
- SF068: F30/X12: Cali dentro del Valle del Cauca (comprador Gobernación del Valle): territorio contenido sin regla; el gemelo SF068/SF069 no queda aislado del rasgo «de esos»
- SF069: F30/X12: Cali dentro del Valle del Cauca (comprador Gobernación del Valle): territorio contenido sin regla; el gemelo SF068/SF069 no queda aislado del rasgo «de esos»
- SF123: F30: el previo tiene un comprador de otro territorio (Gobernación de Nariño) y el fragmento fija una región: la etiqueta probable es CLARIFY/TERRITORY_VS_CARRIED_BUYER, no un delta limpio
- SF135: F29/X4: previo NOT_EXECUTED sin pregunta pendiente (la interpretación se aceptó y la fuente falló): sigue vivo PRIOR_NOT_EXECUTED
- SF136: F29/X4: previo NOT_EXECUTED sin pregunta pendiente (la interpretación se aceptó y la fuente falló): sigue vivo PRIOR_NOT_EXECUTED
- SF155: F30: «Cali» con comprador arrastrado de otro territorio (Alcaldía de Bello): domina la etiqueta del caso de dos dimensiones

Marcas adicionales:
- SF047, SF048, SF049, SF055 (y SF052, SF057): empiezan con un sustantivo de universo y traen contenido propio, así que §5.1 los hace `NEW_SEARCH`.
  Antes (R5 §P2) eran híbridos que debían `ASK`; ya no ejercitan F25. Para ejercitar F25 harían falta respuestas sin sustantivo de universo
  (SF051 y SF053 sí lo hacen). Propuesta (no aplicada): reescribir SF048/SF049/SF055 sin «contratos de».
- SF050 (gemelo de SF049 con `chain` reask): ninguna regla F16–F30 distingue la repregunta; rasgo sin efecto.
- Sin caso para F21 (orden por fecha sin dirección -> `DEFAULTED`): ningún texto de ORD/TOP lo ejercita. Propuesta: un caso nuevo.
- SF138/SF139/SF140 ya no son «estado vencido»: son `EXECUTED` con `gap_seconds` > 1800 (F11); SF141 (1740 s) sigue siendo su gemelo de frontera.

## 5. Comprobaciones
- `form_check.json`: 164 casos, 0 errores contra los enums cerrados de §1–§3 del esquema 2.1 (claves exactas de registro/`prior`/IR/condición/
  `pending`, enums de owner/state/chain/universe/view/view_source/unit/level/operator por dimensión/order/direction_source, `product_view` sí y
  sólo sí UNSPECIFIED, `ir` null sólo con UNREADABLE o NONE, `pending` sí y sólo sí PENDING, `n` 1..100, cardinalidad exige orden,
  F14: ningún SMMLV en previo EXECUTED, span abierto fuera de `prior.ir`). Prueba de mutación: con 4 inyecciones (EXPIRED, unit en no-VALUE,
  SMMLV, DUAL) el validador da 4 errores; restaurado, 0.
- `similarity_check.json`: 0 rechazados; máximo Jaccard 0,300 vs REAL y 0,444 vs público; sin patrones de cédula/NIT/correo/teléfono
  (el SF103 nuevo incluido).

## 6. Dónde el esquema 2.1 no alcanzó para expresar un caso
1. **Periodo sin año como pendiente.** No hay código de §5.3 propio (`MONTH_YEAR_AMBIGUOUS` es de mes sin año con TIME previo). Usé
   `UNRESOLVED_VALUE:TIME` y `candidate_value` libre; `options = []`.
2. **Valor de periodo relativo.** El esquema pide «fechas absolutas entre paréntesis» sin decir la convención de la ventana (¿hoy inclusive?, ¿mes
   calendario?). Elegí [hoy - N meses, hoy].
3. **Año único y mes.** No dice si «2025» o «2025-03» basta como lectura completa; los dejé así.
4. **`NOT_EXECUTED` sin pregunta pendiente** (SF135, SF136): el esquema lo permite sólo en sintéticos y deja vivo `PRIOR_NOT_EXECUTED` (X4).
5. **Herencia de SMMLV (X14).** No puede existir un previo en SMMLV, así que ningún caso ejercita «SMMLV no se hereda»; sólo el SMMLV citado en
   el follow-up lo haría (no hay caso así; propuesta: uno nuevo).
6. **Botones como EVENT.** `quote = null` encaja para opciones; para «Mayor valor»/«Quitar filtro…» el esquema no dice si el evento de un botón
   de protocolo cuenta como `EVENT` (lo dejé así: SF149–SF152) o si basta `turn_kind = PROTOCOL` con `input_kind = TEXT`.
7. **Territorio contenido (X12).** SF068/SF069 (Cali dentro del Valle) y SF123/SF155 quedan atados a F30 sin definición de «otro territorio».
8. **Orden `direction_source` en previos.** Todos los previos usan `STATED`; no hay forma de expresar un previo cuyo orden por fecha fue
   `DEFAULTED` salvo escribirlo (no lo hice: ningún previo lo necesita).

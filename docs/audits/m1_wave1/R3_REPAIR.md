# M1 wave 1 — R3 repair after ZEUS (fresh) NO_GO on `d6f7faab` (R2)

Base de la ronda: `d6f7faab` (candidato R2). Reloj de las sondas: 2026-10-01. Escritor único: ARES. Última ronda.
Sin tocar el núcleo Foundation (`foundation_*`, `interpretation_contract.py`, contrato de autoridades C1.1).

**Política de cierre (decisión del orquestador, no cambiada): hardening acotado.** R3 repara (a) lo FINITO y
derivable de un léxico que ya existe en el repo y (b) toda REGRESIÓN que wave 1 introdujo frente a la base
`4ec99bc1`. El residuo de lenguaje abierto que la base ya tenía se CONGELA como regresión conocida
(`xfail(strict=True)`, con la consulta, el comportamiento correcto esperado, la razón y el dueño
`E-TIME (Foundation v5 time authority ACTIVE)`) y no se persigue.

## Corrección a una afirmación de R2

R2 dijo: «Propiedad exhaustiva (P1-A), derivada del léxico (`temporal_semantics._RELATIVE_PATTERNS`…)».
**Era cierto sólo del léxico legado.** `app/core/time_relative.py` declara otras ventanas («el último mes/año»,
«los dos últimos meses», «N últimos días/semanas», «hace dos semanas») que la regla de ancla no conocía: «en el último
mes de 2025» o «en los dos últimos meses de 2025» seguían resolviendo en silencio como el año 2025 completo
(igual que la base y R1). R3 deriva las ventanas de AMBOS léxicos (ver P1-1) y la propiedad se enumera desde los dos;
dos pruebas fijan que ningún patrón relativo nuevo queda sin decidir ni se copia:
`test_every_relative_form_has_a_decision` (cada patrón de `time_relative` es «franja» o «ventana») y
`test_the_authority_windows_are_the_union_of_both_lexicons_not_a_copy`.

## Qué se reparó

| Hallazgo | Clase | Reparación |
|---|---|---|
| P1-1 ventanas de `time_relative` no se aclaran junto a otro periodo | B (fondo, finito) | `time_relative.WINDOW_PATTERNS` (derivado de una tabla `_FORMS` que obliga a clasificar cada patrón) + `temporal_semantics.window_patterns()` (unión lazy de los dos léxicos) usada por `_anchored_window` |
| P1-1b ventana en follow-up con periodo vigente | B | `temporal_period_question.block_for`: la ventana relativa en un follow-up que hereda o reemplaza el periodo previo aclara (fail-closed) |
| P2-3 (bloqueante) la aclaración abría un camino silencioso | B | pregunta sin opciones numeradas ni «completo» + marca de «pregunta de periodo abierta» en el frame bloqueado: toda respuesta que no sea un periodo explícito vuelve a preguntar |
| P2-4 VALUE plural + participio | A (regresión vs base) | `money_suffix_event.py` (clase morfológica cerrada) + una alternativa en la cola de `money_comparators._ADJECTIVE_TAIL` |
| P2-1 CA «hoy» pegado a un verbo de evento / «de hoy» | A (regresión vs R1) | regla por posición en `currentness.py` |
| P2-2 CA «están suspendidos hoy» | A (regresión vs base) | misma regla: «hoy» tras un verbo de estado es vigencia a cualquier distancia |
| P2-5 las citas normativas no son periodos | A (regresión vs R1) | `normative_citation.py` + sus spans entran como protegidos en `_anchored_window` |
| Residuo de lenguaje abierto | congelado | `tests/test_m1_wave1_r3_frozen_residue.py` (12 xfail strict) |

### P1-1 — las ventanas de `time_relative`

`time_relative` importa `temporal_semantics`, así que éste no puede importarlo al cargar: `window_patterns()` lo lee
en la llamada (import local) y devuelve `_WINDOW_PATTERNS + time_relative.WINDOW_PATTERNS`. En `time_relative`
cada forma declara qué es (`_FORMS`): `_FRANJA` («este trimestre/semestre»: franja en curso, no ventana),
`_ULTIMO_UNICO`, `_NUMERO_ANTES` y `_HACE_SEMANAS` (ventanas móviles; las tres resuelven por `_trailing`). Un patrón
sin clasificar rompe `test_every_relative_form_has_a_decision`. La regla de R2 (mismo turno, no adyacencia) no cambia.

Propiedad: `NEW_WINDOWS` = 2 580 frases (5 artículos × 21 cuentas × 4 acentos × 6 unidades, «el/este último mes/año»
y «hace N semana(s)», todas comprobadas contra `WINDOW_PATTERNS`) × 9 clases de ancla × 13 formas de posición; y 9 ventanas representativas × 172 anclas del léxico (años, formas de
mes con 6 separadores de año, deícticos, periodos/rangos/cotas/ISO) × las mismas 13 formas. Todas piden con `relative_window_explicit_anchor` y sin fechas.

Follow-up: el follow-up engine hereda o reemplaza el periodo previo sin saber que el turno habla de una ventana
(la ventana de `time_relative` ni siquiera se resuelve: «ahora solo el último mes» heredaba 2025 en silencio; la
legada, «solo la última semana», reemplazaba por la del reloj). Enganche mínimo en `apply_context` (parsing.py),
después de `detect_and_merge` (para conocer la intención): si el turno trae una ventana (cualquier léxico), la
conversación ya tiene un periodo y la intención no es `new_search`/`reset_context`, el turno devuelve el mismo bloqueo
transaccional que `_temporal_error` (frame previo intacto, 0 llamadas a la fuente). Sobre-aclaración declarada:
una consulta completa con ventana que el motor clasifica como `change_scope`/`contextual_requery` («contratos de aseo
en Barranquilla en los últimos 3 meses» tras «… en 2025») también pregunta; una `new_search` (otro dataset con su
propio alcance) ejecuta con la ventana del reloj. En un follow-up SIN periodo previo la ventana ejecuta como en R1.

### P2-3 — la aclaración no puede crear un camino silencioso

Hallazgo: «2», «la opción 2» y «2025 completo» ejecutaban `LIKE '%opcion%'`/`'%completo%'` (sin fecha, o con el año y
un tema inventado). Dos causas distintas:

1. La pregunta ofrecía frases-respuesta que el parser convertía en tema. Nueva pregunta (`temporal_window_anchor`):
   «Mencionas «X» y también «Y». No sé si la ventana se cuenta hacia atrás desde hoy o dentro de ese periodo, y no voy a
   elegir por ti. Escríbeme el periodo con fechas explícitas, por ejemplo «octubre a diciembre de 2025» o «desde julio
   de 2026».» Sin números, sin «completo». Los dos ejemplos (`EXAMPLES`) son frases que el parser ya resuelve y una
   prueba lo exige.
2. **Aunque la pregunta fuera perfecta, la respuesta se leía como un follow-up nuevo.** Un turno bloqueado persiste el
   alcance que entendió (diseño deliberado de `resolve_entities`: «para que un follow-up como "mantenimiento" no pierda el
   territorio»), pero sin fecha: «sí», «ambas», «la primera» ejecutaban «contratos de Galapa» sin periodo. Esto ya
   pasaba en la base con cualquier aclaración temporal (`invalid_date`), no sólo con ésta. Módulo nuevo
   `temporal_period_question.py` (una responsabilidad): el frame bloqueado de las dos preguntas de ventana lleva la marca
   `_temporal_open_question` (código + pregunta); `apply_context` la toma al entrar (nunca se fusiona al frame) y el
   turno siguiente es una RESPUESTA sólo si trae un periodo explícito (no una ventana relativa) y no trae un tema nuevo
   — salvo que sea una `new_search` completa o un reinicio explícito (`reset_context`). Cualquier otra cosa vuelve a
   preguntar (con la marca otra vez abierta y la salida escrita: «si en realidad quieres otra búsqueda, escríbela
   completa (entidad, tema y periodo)»). Una respuesta con fecha inválida también conserva la pregunta abierta.

Respuestas probadas punta a punta (`run_query`, cliente falso, dos turnos; 3 preguntas × 22 respuestas que no son un
periodo + 3 × 7 que sí lo son): ninguna respuesta que no es un periodo ejecuta (aclara, o cae en el selector
preexistente); las que lo son ejecutan ese periodo con el alcance entendido, sin la marca y sin ninguna palabra de la
respuesta como tema.

| Respuesta | R2 | R3 |
|---|---|---|
| «1», «2» | «Solo tengo 3 sugerencias…» (selector preexistente) | **igual, declarado**: no se tocó el selector |
| «la primera», «la segunda», «la opción 2», «opción 1», «ambas» | `LIKE '%primera%'` etc., sin fecha | vuelve a preguntar |
| «sí», «la 2» | «contratos de Galapa» sin fecha | vuelve a preguntar |
| «2025 completo», «el periodo de 2025 completo», «dentro de 2025» | 2025 + tema `completo`/`dentro` | vuelve a preguntar |
| «los últimos 3 meses» | ventana del reloj | vuelve a preguntar (una ventana no es un periodo explícito) |
| «octubre a diciembre de 2025», «desde julio de 2026», «del 2025-10-01 al 2025-12-31», «de 2025», «todo 2025» | ejecuta | ejecuta el periodo, sin tema nuevo |

Límite declarado (Frame de perfil ON, flag OFF por defecto de código): el turno bloqueado del Frame no persiste alcance ni
marca, así que una respuesta es una búsqueda nueva como cualquier texto suelto; «la primera» y «sí» los atiende el
estado propio del perfil («Este análisis no ofreció opciones numeradas…»), «octubre a diciembre de 2025» pide un filtro,
pero «2025 completo» ejecuta una búsqueda de procesos del 2025 con el tema `completo`. No lo introduce esta pregunta
(es el comportamiento de cualquier texto suelto tras un turno sin alcance), no se arregla aquí.

### P2-4 — VALUE plural + participio

`money_suffix_event.py`: lo que puede seguir a «o/y mayor(es)…» sin modificar un sustantivo es una clase
morfológica CERRADA: participios regulares `-ado/-ada/-ados/-adas/-ido/-ida/-idos/-idas` menos una lista de control de
sustantivos con esa forma (estado, mercado, grado, resultado…), participios irregulares (suscritos, hechos, abiertos…),
pretérito de tercera persona del plural `-aron/-ieron` y auxiliares (se, son, sean, están, hayan…; «esta/este» fuera
a propósito: son demostrativos). Tras el sufijo se admite un espacio o un guion/paréntesis/barra alrededor
(«o mayores - firmados», «(firmados)», «/firmados»); un emoji cuenta como puntuación (era sufijo en la base).
Un sustantivo, un artículo o un adjetivo («vigentes») sigue sin leerse: «y mayor plazo», «y menores plazos», «o mayor
duración» siguen aclarando. Aplica a los dos consumidores (`money_polarity` por regex, `read_comparators` por el mismo
lookahead). Propiedad: 16 sufijos × 88 participios y verbos × 7 separadores × 5 cierres ⇒ la cota de la propia
familia y nunca la opuesta; 16 sufijos × 35 sustantivos × 3 cierres ⇒ UNKNOWN. Polaridad verificada con `val.py`
(tabla de abajo): ninguna consulta invierte.

Límite declarado, **la única regresión VALUE frente a la base que queda**: «300 millones o mayores Galapa» (un nombre
propio tras el sufijo; la base lo leía como cota) sigue aclarando como en R2: es indistinguible de «o mayores plazos»
sin un léxico de sustantivos. Congelado como xfail estricto (`test_a_proper_noun_after_the_plural_suffix_keeps_the_bound`). «y menores plazos» (la base
ejecutaba `valor_max`: defecto de la base) aclara.

### P2-1 / P2-2 — Contract Activity «hoy»

R2 apartaba «hoy» como vigencia cuando tocaba un verbo de estado en cualquier orden, así que «firmados hoy están en
ejecución» (los contratos FIRMADOS hoy que están en ejecución) ejecutaba sin periodo, y la inversa, «están suspendidos
hoy» (la palabra DESPUÉS del estado), aclaraba. Regla por posición en `currentness.py`: manda el último de {palabra de
evento, verbo de estado} antes de «hoy»:

* evento (firmar, celebrar, suscribir, publicar, adjudicar, perfeccionar, registrar, otorgar, aprobar, expedir,
  legalizar, cualquier flexión) ⇒ día de la firma: aclara como R1 («firmados hoy están en ejecución», «celebrados
  hoy», «publicados hoy», «que firmó hoy», «firmados el día de hoy», «están suspendidos y se firmaron hoy»);
* «de hoy» / «del día de hoy» ⇒ el periodo de los contratos: aclara («de hoy están suspendidos»);
* verbo de estado antes (a cualquier distancia) ⇒ vigencia: ejecuta como la base («están suspendidos hoy», «están en
  ejecución hoy», «firmados ayer están hoy en ejecución»... este último aclara por «ayer», que es otro periodo);
* «hoy» seguido de un verbo de estado y sin evento ni «de» antes ⇒ vigencia («hoy están suspendidos»);
* «al día de hoy» ⇒ vigencia. Un límite de oración (`. ; : ? ! ¿ ¡`) reinicia la posición.

### P2-5 — las citas normativas no son periodos

`normative_citation.py`: `<tipo> [adjetivo] [n.º] <número> de|del <año>` con tipo ley, decreto, decreto ley, resolución,
acuerdo, ordenanza, circular (externa/interna/conjunta/única). Sus spans se suman a `protected_spans` sólo dentro de
`_anchored_window` (el mecanismo existente de R2: «un año dentro de un nombre protegido no es ancla»). Efecto:
«contratos por decreto 1082 de 2015 en los últimos 6 meses» vuelve a resolver la ventana (R1), no aclara. Un periodo
real junto a la cita («… en los últimos 6 meses de 2025») sigue aclarando.

**Sobre-disparo aceptado (fail-closed, 0 casos en los 2.778 textos del corpus según ZEUS; R2 → R3 sin cambios en ese corpus):**
«plazo hasta 2027 de los últimos 3 meses», «cierre en 2026 … en los últimos 3 meses» y el MULTI «aseo en Galapa en
los últimos 3 meses y obra en Galapa en 2025» siguen aclarando (están en `test_declared_over_firing_stays_closed`).
También aclaran, y la base resolvía un periodo equivocado en ellos: «contratos del proceso SA-001-2025 en los
últimos 3 meses», «Hospital 7 de Agosto / Colegio 20 de Julio / IE 12 de Octubre en los últimos 3 meses» (el router lee la
fecha del nombre antes de resolver el comprador: límite upstream pre-existente de R2, no tocado).

## Congelado como regresión conocida (12 xfail strict, dueño E-TIME)

`tests/test_m1_wave1_r3_frozen_residue.py` — cada test afirma el comportamiento CORRECTO (aclarar, o no ensanchar) y falla
hoy; si E-TIME arregla una familia, el XPASS estricto obliga a borrar la entrada. Todas fallaban ya en la base, salvo la última, que es la regresión VALUE declarada arriba.

| Test | Consulta | Hoy (= base en lo esencial) |
|---|---|---|
| `test_window_of_the_previous_vigencia_asks` | «… en los últimos 3 meses de la vigencia anterior» | base: sin periodo; R1+: ventana del reloj, sin aclarar |
| `test_window_of_a_year_written_in_words_asks` | «… en los últimos 3 meses de dos mil veinticinco» | ídem |
| `test_window_of_a_year_with_a_thousands_separator_asks` | «… en los últimos 3 meses de 2.025» | ídem |
| `test_window_of_a_two_digit_year_asks` | «… en los últimos 3 meses de 25» | ídem |
| `test_window_of_the_previous_administration_asks` | «… de la administración anterior» | ídem |
| `test_last_bimester_of_a_year_does_not_widen_to_the_year` | «en el último bimestre de 2025» | 2025 completo |
| `test_last_fortnight_of_a_month_does_not_widen_to_the_month` | «en la última quincena de diciembre de 2025» | diciembre completo |
| `test_the_end_of_a_year_does_not_widen_to_the_year` | «a finales de 2025» | 2025 completo |
| `test_months_ago_with_an_explicit_year_asks` | «hace 3 meses de 2025» | 2026-07 |
| `test_a_lone_window_of_time_relative_is_not_dropped` | «en el último mes» (sin ancla) | sin periodo (`time_relative` sólo está cableado a `time_authority`, sin consumidor) |
| `test_a_norm_citation_alone_is_not_a_period` | «contratos por decreto 1082 de 2015» | periodo 2015 |
| `test_a_proper_noun_after_the_plural_suffix_keeps_the_bound` | «de 300 millones o mayores Galapa» | aclara (la base: `valor_min`) — única regresión VALUE frente a la base |

## Tabla de comportamiento (base `4ec99bc1` / R1 `7d9af549` / R2 `d6f7faab` / R3)

Periodos del parse (`resolve_temporal_scope`, reloj 2026-10-01); ASK = `relative_window_explicit_anchor` y sin fechas.
Las sondas son las de ZEUS (82 consultas: `q_ts`, `q_s2`, `q_norm`, `q_new`), reejecutadas sobre los cuatro árboles.

| Consulta (… = «contratos de la Alcaldía de Galapa») | base | R1 | R2 | R3 |
|---|---|---|---|---|
| en los dos últimos meses de 2025 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | ASK |
| en el último mes de 2025 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | ASK |
| en el último mes del año pasado | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | ASK |
| en las dos últimas semanas de marzo de 2025 | 2025-03-01..2025-03-31 | 2025-03-01..2025-03-31 | 2025-03-01..2025-03-31 | ASK |
| en los 10 últimos días de marzo de 2025 | 2025-03-01..2025-03-31 | 2025-03-01..2025-03-31 | 2025-03-01..2025-03-31 | ASK |
| perfil … en los dos últimos meses de 2025 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | ASK |
| en el último año de 2025 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | ASK |
| en los 3 últimos meses de 2025 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | 2025-01-01..2025-12-31 | ASK |
| hace 2 semanas de marzo de 2025 | 2025-03-01..2025-03-31 | 2025-03-01..2025-03-31 | 2025-03-01..2025-03-31 | ASK |
| en los últimos 3 meses de 2025 (legado) | 2025-01-01..2025-12-31 | 2026-07-01..2026-10-01 | ASK | ASK |
| decreto 1082 de 2015 en los últimos 6 meses | 2015-01-01..2015-12-31 | 2026-04-01..2026-10-01 | ASK | 2026-04-01..2026-10-01 |
| circular externa 001 de 2023 en los últimos 6 meses | 2023-01-01..2023-12-31 | 2026-04-01..2026-10-01 | ASK | 2026-04-01..2026-10-01 |
| Ley 80 de 1993 en los últimos 3 meses | sin periodo | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| decreto 1082 de 2015 en los últimos 6 meses de 2025 | 2015-01-01..2015-12-31 | 2026-04-01..2026-10-01 | ASK | ASK |
| plazo hasta 2027 de los últimos 3 meses | None..2027-12-31 | 2026-07-01..2026-10-01 | ASK | ASK |
| cierre en 2026 en los últimos 3 meses | 2026-01-01..2026-12-31 | 2026-07-01..2026-10-01 | ASK | ASK |
| SA-001-2025 en los últimos 3 meses | 2025-01-01..2025-12-31 | 2026-07-01..2026-10-01 | ASK | ASK |
| Hospital 7 de Agosto en los últimos 3 meses | 2026-08-07..2026-08-07 | 2026-08-07..2026-08-07 | ASK | ASK |
| el último mes (sin ancla) | sin periodo | sin periodo | sin periodo | sin periodo |
| en los últimos 3 meses de la vigencia anterior (congelado) | sin periodo | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| en los últimos 3 meses de dos mil veinticinco (congelado) | sin periodo | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| en los últimos 3 meses de 2.025 (congelado) | sin periodo | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| en los últimos 3 meses de 25 (congelado) | sin periodo | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| en los últimos 3 meses de la administración anterior (congelado) | sin periodo | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |

Seguimiento (workflow, dos turnos, 0 llamadas nuevas en ASK):

| Turno 1 → turno 2 | base | R1 | R2 | R3 |
|---|---|---|---|---|
| «… en 2025» → «ahora solo el último mes» | 2025 (ventana descartada) | 2025 | 2025 | ASK |
| «… en 2025» → «y los 3 últimos meses?» | 2025 | 2025 | 2025 | ASK |
| «… en 2025» → «y en los últimos 3 meses» | 2025 | 2026-07-01..10-01 | 2026-07-01..10-01 | ASK |
| «… en marzo de 2025» → «solo la última semana» | marzo 2025 | 2026-09-24..10-01 | 2026-09-24..10-01 | ASK |
| «… » (sin periodo) → «y en los últimos 3 meses de 2025» | 2025 | 2026-07-01..10-01 | ASK | ASK |
| «… en los últimos 3 meses» → «y de 2025?» | 2025 | 2025 | 2025 | 2025 |

VALUE (router, `valor_min`/`valor_max`; `C` = aclara; ninguna invierte la polaridad):

| Consulta («contratos de la Alcaldía de Galapa …») | base | R1 | R2 | R3 |
|---|---|---|---|---|
| de 300 millones o mayores firmados en 2025 | min | min | C | min |
| de 300 millones o superiores adjudicados en 2025 | min | min | C | min |
| de 300 millones o mayores Galapa | min | min | C | C |
| de 300 millones o mayor | C | min | min | min |
| de 300 millones o mayor | C | min | min | min |
| de 300 millones o superior en valor | C | min | min | min |
| de 300 millones y mayor plazo | C | min | C | C |
| de 300 millones o menores firmados en 2025 | max | max | C | max |
| de 300 millones o inferiores suscritos este año | C | max | C | max |
| de 300 millones o menor | C | max | max | max |
| de 300 millones y menor plazo | C | max | C | C |
| de 300 millones o mayores 🙂 | min | min | C | min |
| de más o menos 300 millones | max | max | C | C |
| de 300 millones o superior al promedio | C | min | min | min |
| de 300 millones ó mayores firmados | min | min | C | min |
| de 300 millones o mayores (firmados) | min | min | C | min |
| de 300 millones o mayores - firmados | min | min | C | min |
| de 300 millones o mayores/firmados | min | min | C | min |
| de 300 millones o mayores adjudicados | min | min | C | min |
| de 300 millones o superiores celebrados en 2025 | min | min | C | min |
| de 300 millones y menores plazos | max | max | C | C |
| de 300 millones o mayor duración | C | min | C | C |

Las 25 consultas restantes de las 47 de la sonda (las 44 de ZEUS y 3 de R3) no cambian entre los cuatro árboles. Ninguna invierte polaridad (`val.py`: 0 inversiones).

Contract Activity (`admit_contract_activity`; R = READY, C = aclara):

| Consulta (CA, «¿Cuántos contratos de la Alcaldía de Galapa …») | base | R1 | R2 | R3 |
|---|---|---|---|---|
| Desglosa por estado los contratos … a hoy | R | C | R | R |
| Desglosa por estado los contratos … de 2025 a hoy | R | C | C | C |
| Desglosa por estado los contratos … de hoy | R | C | C | C |
| Desglosa por estado los contratos … firmados a hoy | R | C | R | R |
| Desglosa por estado los contratos … firmados hoy | R | C | C | C |
| Desglosa por estado los contratos … firmados hoy están | R | C | R | C |
| de 2025 … están hoy suspendidos | R | C | R | R |
| de hoy … están suspendidos | R | C | C | C |
| … celebrados hoy están en ejecución | R | C | R | C |
| … de hoy están suspendidos | R | C | R | C |
| … en los últimos 3 meses están hoy suspendidos | R | C | C | C |
| … están en ejecución hoy | R | C | C | R |
| … están suspendidos hoy | R | C | C | R |
| … figuran hoy suspendidos | R | C | R | R |
| … firmados ayer están hoy en ejecución | R | C | C | C |
| … firmados hoy están en ejecución | R | C | R | C |
| … firmados hoy están suspendidos | R | C | R | C |
| … firmados hoy figuran en ejecución | R | C | R | C |
| … firmados hoy figuran hoy suspendidos | R | C | C | C |
| … hoy están suspendidos | R | C | R | R |
| … publicados hoy están en ejecución | R | C | R | C |
| … que firmó hoy están en ejecución | R | C | R | C |
| firmados hoy por la Alcaldía de Galapa están suspendidos | R | C | C | C |
| firmó hoy la Alcaldía de Galapa que estén suspendidos | R | C | C | C |
| firmó la Alcaldía de Galapa hoy | R | C | C | C |

`corp2.py` (2.778 textos de corpus: gold, fixtures, auditorías) en los cuatro árboles: **R2 → R3: 0 cambios**; base → R3: 3
cambios, los mismos tres que ya tenía R1 (no hay uno nuevo de R3). Los tres cambios de la base: «… primer semestre de 2024» (ahora 2024-01-01..06-30, el semestre y no el año), «contratos de aseo de los últimos 6 meses» (ahora la ventana, antes sin periodo) y la consulta larga de la ANI «… de los últimos 3 años de peajes en el Cauca …» (ahora 2023-10-01..2026-10-01, antes sin periodo): correcciones de R1.

## Evidencia

* Focal R1 + R2 + R3: `tests/test_m1_wave1_r1_*.py` + `test_m1_wave1_r2_*.py` + `test_m1_wave1_r3_*.py` (10 archivos) →
  `2090 passed, 12 xfailed` (los 12 xfail son los de `test_m1_wave1_r3_frozen_residue.py`, todos `xfailed` estrictos; 0 XPASS).
  Tests de R1/R2 editados (el contrato cambió a propósito): `test_the_error_names_both_readings…` (ya sin «completo» ni
  «(1)»), el conteo de menciones en `test_the_conflict_reports_window_anchor…` y en `…with_two_windows…`, el texto en
  `test_the_workflow_asks_with_zero_source_calls…`, el tercer turno de `test_a_followup_with_an_anchored_window_asks…`
  (ahora la pregunta sigue abierta y se responde con un periodo explícito, no con otra ventana), «firmados» fuera de la lista de
  sustantivos de `test_a_noun_after_the_adjective…` (−16 casos) y `test_a_participle_after_the_suffix_is_asked_not_guessed`
  → `…_is_a_bound_since_r3`.
* Expandido (no la suite global): MISMA lista de 149 archivos de R2 (`expanded_files.txt` de R2; los que mencionan `temporal_semantics`,
  `resolve_temporal_scope`, `money_comparators`, `money_polarity`, `period_consistency`, `contract_activity`, `temporal_gate`,
  `entity_profile`, `capability_binding`, `supplier_contract`, `geo_authority`, `foundation_value_direction`, `time_authority`,
  `value_authority`, más `test_foundation_*`, `test_geo*`, `test_time*`, `test_temporal*`, `test_money*`, `test_value*`,
  `test_a2*`, `test_supplier*`, `test_capability_binding*`, `test_contract_activity*`, `test_entity_profile*`,
  `test_query_router*`, `test_dialogue*`, `test_query_plan*`, `test_followup*`, `test_m1_*`, `test_analytic_control*`), 5 procesos:
  `671 passed 2 xfailed`, `638 passed`, `4221 passed 1 skipped`, `4866 passed 1 failed`, `11783 passed 1 xfailed`
  = `22179 passed, 1 failed, 1 skipped, 3 xfailed`. El único fallo,
  `test_foundation_runtime.py::test_count_never_credits_less_than_its_recorded_parent_page`, es un `hypothesis.errors.DeadlineExceeded`
  (239 ms > 200 ms) bajo 5 procesos concurrentes más otros agentes en la máquina; ese archivo corrido solo: `56 passed`
  (no toca nada de esta ronda). Descontado ese flake: `22180 passed, 1 skipped, 3 xfailed`, 0 failed
  (R2: `22196 passed, 1 skipped, 3 xfailed`; los 16 de menos son exactamente los 16 casos «firmados» × sufijos que dejaron de ser
  «sustantivo» a propósito). Los 3 xfailed son los preexistentes de R2 (la partición en procesos es distinta de la de R2);
  los 12 xfail nuevos de R3 salen como `xfailed` en el focal (el expandido no los contiene: `test_m1_*` de la lista es la de R2).
  Corrida sobre el árbol final de código de la ronda (después de los mutantes; sólo cambió este informe).
  Archivos ensuciados por la suite: ninguno (`observations.jsonl` y `product_trace.jsonl` sin cambios; `git status` limpio salvo la ronda).
* HCA (6 corpus, `PYTHONHASHSEED=0`) R3 vs R2: `171/195`, `31/32`, `41/42`, `22/24`, `40/40`, `35/36` en ambos; estados por
  caso idénticos; 0 de 442 turnos distintos tras normalizar «consultado el <fecha>», `selection_token`,
  `_opportunity_reference_instant` y marcas de tiempo.
* `ruff check app tests/test_m1_wave1_r{1,2,3}_*.py`: `All checks passed!`.
* `olympus-semgrep-delta --base d6f7faab --config .semgrep/secoppal-laws.yml` sobre los archivos tocados: `TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0` (17 archivos `.py` tocados, con `--base`; no es vacuo: compara contra el árbol de R2).
* No se corrió la suite global (la corre el orquestador).

## Mutantes de la ronda

38 mutantes en una copia del árbol (no en el árbol de trabajo); cada uno corre los 10 archivos R2+R3 con `-x`:

| Grupo | Mutantes | Resultado |
|---|---|---|
| Ventanas de `time_relative` | W1 `window_patterns()` sin `time_relative`; W2 `_HACE_SEMANAS` como franja; W3 `_FRANJA` como ventana; W4 `relative_window_in` siempre `None`; W5 `_anchored_window` con sólo el léxico legado | 5/5 muertos |
| Pregunta abierta / follow-up | P1 sin la excepción `new_search`; P2 sin la condición «hay periodo previo»; P3 una ventana sirve de respuesta; P4 un tema nuevo sirve de respuesta; P5 sin periodo sirve de respuesta; P6 el gate no abre la marca; P7 el re-ask no reescribe la marca; P8 `take` no la quita del frame; P9 la rama de error temporal pierde la marca; P10 la pregunta vuelve a llevar «(1)»; P11 la marca para cualquier código; P12 el follow-up nunca pregunta; P13 el hook de `apply_context` apagado; P14 `reset_context` se re-pregunta | 14/14 muertos |
| VALUE | V1 sin la alternativa de participio; V2 «plazo» en los auxiliares; V3 lista de sustantivos-participio vacía; V5 sin participios irregulares; V6 sin `-aron/-ieron`; V7 sin `-ido`; V8 sin «están» acentuado; V4 separador de ancho cero | 7 muertos; V4 equivalente |
| CA `hoy` | C1 sin la regla del evento; C2 sin «de hoy»; C3 sin «al día de hoy»; C4 vigencia sólo adyacente; C5 sin límite de oración; C6 sin «celebr»; C7 sin «hoy + verbo» | 7/7 muertos (C5 sobrevivió con el test inicial: su texto tenía el verbo de estado DESPUÉS del evento y daba lo mismo con o sin límite; endurecido y muerto) |
| Citas | N1 sin los spans; N2 sin «decreto»; N3 sin el cierre de año; N4 sin «del» | 4/4 muertos |

Equivalente argumentado: **V4** (`_SEPARATOR` con `\s*` en vez de `\s+`): ambos consumidores cierran el sufijo con un límite de
palabra (`\b` en `money_polarity`, `(?![a-z0-9])` en `read_comparators`), así que un sufijo pegado a una letra no llega a la cola, y
pegado a un no-letra la cola exige que siga una palabra del informe. Con 2 254 tests de VALUE (R2+R3+`test_money*`+`test_value*`) sobre
la copia mutada: 2 254 passed. V3 sobrevivió una primera vez porque el test construía su lista de control con la misma tupla del
módulo (autorreferencial); ahora la lista de control vive en el test.

## Riesgos y límites declarados

* **Marca de pregunta abierta en `resolved_params`**: una clave privada (`_temporal_open_question`) viaja en los
  `resolved_params` del turno bloqueado (otras claves `_temporal_*` se retiran en el gate; ésta debe persistir para el
  turno siguiente). Nunca llega al builder (turno bloqueado) ni a un frame ejecutado (se retira al entrar a
  `apply_context`). Sólo la llevan las dos preguntas de ventana; `invalid_date` y los demás errores temporales no (siguen
  como en la base: una respuesta suelta tras ellos es un follow-up normal).
* Otras ramas de `apply_context` que devuelven antes de `detect_and_merge` (negación bloqueante, operación de actor,
  mutación de objeto/departamento, `actor_pending`) no consultan la marca: la pregunta queda sin responder y se
  pierde. Es preguntar de menos sólo si esos turnos arrastran un alcance sin fecha; no se tocó.
* La regla de follow-up pregunta de más ante cualquier intención que hereda contexto (ver P1-1).
* Frame de perfil ON: ver el límite de P2-3.
* Pendiente de verificar: nada que dependa de red o de SECOP (la suite y las sondas usan clientes falsos).

Rondas de reparación usadas: 2 de 2 internas (ronda 1: diseño, la marca de pregunta abierta y los fallos de los tests de R2 cuyo contrato cambia; ronda 2: supervivientes V3 [autorreferencial: la lista de sustantivos la leía el propio test del módulo] y C5, paso de `reset_context`, emoji y el xfail VALUE). Ningún hallazgo C–F.

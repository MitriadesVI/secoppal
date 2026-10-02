# HADES — ¿se puede hacer la llanta de otra manera?

Fecha: 2026-10-01. Sólo lectura sobre todos los repos. Única escritura: este archivo.
Sin LLM, sin red, sin pytest, sin `Settings()`. Las mediciones nuevas (§1) se hicieron con
`/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -B` importando sólo `gap_matrix.py`
(stdlib, `secoppal-m1/scripts/audits/m1_search/`) sobre las capturas ya existentes; los scripts
fueron heredocs por stdin (no quedan en disco). El Gold REAL se cita sólo por id y conteo.

Convenciones: **HECHO** = medido o leído en fuente citada; **INFERENCIA** = deducido, no
ejecutado; **ESPECULACIÓN** = plausible sin evidencia directa.

Abreviaturas de fuentes:
- `OP` = `scratchpad/omission_probe/OMISSION_PROBE.md`; `M3` = `scratchpad/m3_consumers/M3_CONSUMER_MAP.md`;
  `M2A` = `scratchpad/m2a_design/M2A_DESIGN.md`; `LA`/`LB` = informes de etiquetadores; `CMP` = `gold_followups/comparison/summary.json`.
- `PAC` = `secoppal-m1/docs/audits/m1_search_foundation/PHASE_A_CLOSE.md`; `A3` = `A3_GAP_MATRIX.md`;
  `A2H` = `A2_AUTHORITY_HEALTH.md`; `RM` = `secoppal-m1/docs/planes/2026-09-30_FOUNDATION_SINGLE_INTERPRETER_ROADMAP.md`;
  `ADR103` = `secoppal-m1/docs/decisions/2026-09-30_ADR-103_foundation-spine-v5-operation-owners.md`.
- `R5` = `secoppal-m1-int-r5/docs/audits/m1_wave1/R5_REPAIR.md`.
- `MEM:<nombre>` = `~/.claude/projects/-Users-rodrigoortiz-Documents-secoppal/memory/<nombre>.md` (evidencia fechada, no ley).
- `CAP` = `~/Documents/tools/secoppal-diagnostics/m1_search_gold/captures/captures.jsonl` (620 capturas
  legacy + Foundation v4.2, Fase A) con el Gold `sealed_v1/gold_v1.jsonl`.

---

## 0. Resumen

| # | Hipótesis | Veredicto | Relación con el marco |
|---|---|---|---|
| H1 | **Fallo visible por superconjunto anunciado** (en vez de aclarar todo lo no hospedable) | **Viva, la más fuerte**: +58 de 446 búsquedas pasarían a responderse con aviso | Componente dentro del marco (ADR-069 ya dice «se aclara **o se dice**»); cambia la forma del gate de cutover |
| H2 | **Eco de filtros como garantía primaria** (no como adorno) | Viva como complemento obligatorio: el eco expone 20/23 errores silenciosos de Foundation y 138/170 de legacy | Componente; ya planeado (F, INV-M2A-12) pero no como gate |
| H3 | **Invertir el flujo** (autoridades proponen, LLM arbitra) | **Refutada como reemplazo**; sobrevive como pista tipada para el LLM | El error «de dueño» medido es casi todo artefacto de vocabulario |
| H4 | **Interfaz en vez de inferencia** (chips/estado editable) | Viva como complemento menor: 20/65 follow-ups son edición de menú cerrado; las aclaraciones de hoy no tienen menú que ofrecer | Ya absorbida en parte por PV/M2a (eventos de opción y vista) |
| H5 | **Cambiar el problema: eje de recuperación** (traer ancho, ordenar/validar después) | No es alternativa de interpretación (desbloquea 1 registro), pero es un **eje que el roadmap no mide** | Ortogonal; propone un segundo gate (filas), no reemplaza nada |
| H6 | **No hacer el cutover**: dos motores + árbitro | **Muerta**: cualquier árbitro sin Gold importa los errores silenciosos de legacy (70–137 vs 23) | Reemplazo; descartado por la métrica intolerable |
| H7 | **Consenso** (dos lecturas; ejecutar sólo si coinciden) | **Muerta como mecanismo primario** (F∧legacy responde 48/446); F×2 sin poder estadístico | A lo sumo auxiliar monótono (invariante 2) |
| — | Tool-calling directo sobre esquema tipado | Muerta (dominada) | ver §6 |
| — | Caché de interpretaciones adjudicadas por repetición | No verificable con los datos (frecuencias infladas por baterías de replay) | ver §6 |

Probaría primero **H1**: el experimento barato ya está a medio hacer (§1, E3) y su falsación completa
es offline (§3, H1.5).

---

## 1. Mediciones nuevas hechas para este informe (offline, sobre `CAP` + Gold)

Población: Gold `operation == SEARCH`, `expected_disposition == EXECUTE`, `ambiguity != AMBIGUOUS` →
**446** registros (416 REAL + 30 SYN). Veredicto por registro con `gap_matrix.evaluate` +
`entry_verdicts` (el mismo código que produjo A3). «Foundation» = cadena intrínseca v4.2 (sin la
puerta: se simula su retirada); ejecuta ⇔ `final_status == READY`. Legacy ejecuta ⇔
`needs_clarification == False`. CORRECTO = ejecuta y la entrada es OK contra Gold; **SILENCIOSO** =
ejecuta y la entrada no es OK; NO_EJECUTA = aclara/no soporta.

| Id | Medición | Resultado (HECHO) |
|---|---|---|
| E1 | Políticas por motor | Legacy: **178** correctos / **170 silenciosos** / 98 no ejecuta. Foundation v4.2: **122** / **23** / 301 |
| E1b | Coherencia con PAC | legacy L_OK = 178 + 16 (OK pero aclaró) = **194 = 43 %**, igual a la cifra corregida de `PAC:23-26`; Foundation intrínseco OK = 122 (101 con la puerta, `PAC:23`) |
| E2 | Árbitro «F si ejecuta, si no legacy» | 229 / **137** / 80 |
| E2b | Árbitro «F si ejecuta; legacy sólo si F dice UNSUPPORTED/NO_ACTION» | 188 / **70** / 188 |
| E2c | Oráculo «el mejor de los dos» (usa Gold, no implementable) | 241 correctos |
| E3 | **Fallo visible**: registros que Foundation NO ejecuta y cuyas condiciones fallidas son todas *conocidas por el host* (no resuelta por autoridad, proyección no soportada, o propuesta como UNSUPPORTED) y ninguna es ancla dura (BUYER/ACTOR/TOPIC/universo) | **58** (GEO 18 —16 municipio, 3 región—, STATE 15, VALUE 6, ORDER 5, TIME 5, OR/NOT 3, MODALITY 3…); con ancla dura no aplicada: 103 (ACTOR 52, BUYER 42…); con algún fallo que el host **no** sabe (proxy, ausencia): 135 |
| E4 | Visibilidad en un eco de los errores silenciosos | Foundation: 20/23 son sustitución o filtro extra (visibles en un eco); 3 incluyen omisión. Legacy: 138/170 visibles; 32 con omisión |
| E5 | Consenso F∧legacy (acuerdo sin Gold: mismas dimensiones y solape de raíces/dígitos por dimensión) | 48 / 8 / 390 |
| E6 | Autoconsenso F×2 (40 ids repetidos; 34 en la población) | 13 correctos estables; 3 silenciosos, de los cuales 1 cambió entre pasadas; 0 correctos perdidos. n sin poder |
| E7 | Errores de dueño **dentro** del vocabulario v4.2 (condición Gold citada con otra dimensión) | **31 / 839** condiciones in-vocab (3,7 %): CT 8, BUYER 8, STATE 6, MODALITY 3, UNSUPPORTED 3, TOPIC 2, GEO 1. Fuera de vocabulario (proxies forzados): ACTOR 72, TIME 33, ORDER 22, VALUE 19 |
| E8 | Léxico cerrado GEO (DIVIPOLA `municipal_territories_v1.json`, nombres ≥4 letras) sobre los 451 turnos SEARCH/MULTI | Coincidencias dentro de una cita Gold de BUYER 84, de ACTOR 10, propias de GEO 51, fuera de toda condición 124. **93/451 turnos (21 %)** tienen una coincidencia GEO dentro de un comprador/actor |
| E9 | TOPIC como señal blanda: registros cuyo único fallo Foundation es TOPIC | **1**. Los 45 registros con fallo TOPIC fallan por política de universo (`TOPIC_OVERLAPS_REQUEST_UNIVERSE` 22, `TOPIC_ONLY_REQUEST_UNIVERSE` 18) junto con otras dimensiones |
| E10 | Razones de `NEEDS_CLARIFICATION` de Foundation (159) | GEO no resuelta 30, `VALUE_OPERATOR_UNGROUNDED` 29, BUYER no resuelta 28, TOPIC/universo 37, STATE 16, TIME 6… Son huecos de autoridad o de política, no elecciones entre N opciones |
| E11 | Chips sobre los 65 follow-ups (ambos etiquetadores; menú cerrado = año, orden, familia de estado/modalidad, universo, quitar) | **20/65** expresables como edición de un menú cerrado; 38 necesitan valor libre (monto, lugar, comprador, mes/rango) |
| E12 | Acuerdo entre etiquetadores sobre la **IR resultante** (no sobre las operaciones de delta) | **58/65**; de los 9 desacuerdos «de fondo» de `CMP`, **6 coinciden en la IR resultante**; los 7 que difieren tienen 6 con ancla no-SEARCH (R0319, R0320, R0321, R0375, R0441, R0560) y 1 de previo ambiguo (R0306) |

Límites de estas mediciones: el emparejador de `gap_matrix` es heurístico (raíces de 5 letras;
`OP:259`); «acuerdo sin Gold» en E5 es una aproximación mía; E3 clasifica «conocido» por las razones del
host en la captura, no por un diseño; E8 es un léxico simple, no la autoridad GEO real (sólo 51 de 148
condiciones GEO del Gold casaron con él, así que sólo sirve como orden de magnitud).

---

## 2. Correcciones a premisas del encargo

1. **«Legacy acierta 238 (53 %)»** está superado: `PAC:23-26` lo corrigió a **194 (43 %)** tras evaluar
   ACTOR simétricamente; E1b lo reproduce. Debilita toda alternativa que se apoye en «legacy es bueno».
2. **«El error dominante es DUEÑO EQUIVOCADO»** es cierto en conteo bruto (`OP:131`) pero **casi todo
   es artefacto de vocabulario**: ACTOR 72/79 y ORDER 22/23 aparecen «con otra dimensión» porque el
   prompt v4.2 no tiene ACTOR ni ORDER (`gap_matrix.py:68-72`, `IR_VOCAB`); TIME 33 es la convención Y1
   (`PAC:46-48`). Dentro del vocabulario el LLM elige mal el dueño en **3,7 %** (E7). La hipótesis
   «invertir el flujo» pierde con esto su justificación principal (H3).
3. **«126/176 propuestas con TIME tienen BUYER/GEO a ≤4 tokens»** (`OP:228`) mide geometría, no
   error: la adyacencia real es 10/176 y el oráculo Gold da 134 vecinos con 0 solapes
   (`OP:221-233`). Es un coste de la ventana (más contexto sólo retira autoridad: aclaraciones de más),
   no un ensanche.

---

## 3. Fichas

### H1 — Fallo visible: ejecutar el superconjunto más estrecho que el host sí sabe acreditar, y decirlo

**1. La idea.** Cuando una condición de **modificador** (municipio, mes, estado, cota inclusiva, orden,
región) no tiene autoridad o proyección, el host no aclara: ejecuta con el valor envolvente más estrecho
que sí acredita (municipio → departamento; mes → año; `≥` → `>` con aviso de borde; estado no resuelto →
sin filtro de estado) y anuncia en el eco «apliqué X; no pude aplicar Y». Nunca para anclas duras
(comprador, actor, tema, universo) ni para nada que el host **no sepa** que falló. Diferencia de raíz:
el marco persigue «cero ensanche» por aclaración (fail-closed); H1 persigue «cero ensanche **silencioso**»
y acepta ensanche **declarado y acotado**. El problema que el usuario define como intolerable es
«entendí otra cosa, ejecuté y no avisé»; H1 conserva el «avisé».

**2. Evidencia.**
- A favor (HECHO): E3, 58/446 búsquedas (13 %) quedan bloqueadas hoy sólo por modificadores que el host
  sabe que no aplicó. Foundation pasaría de 122 a 180 respuestas útiles con aviso (legacy: 178 correctas
  pero con 170 silenciosas, E1). `RM:51-52` y ADR-069 citado allí: «lo que el host no puede hospedar se
  aclara **o se dice**» — el «se dice» ya está permitido por la ley del propio marco.
- A favor (HECHO): el ensanche que provocó M1-H0 fue Y1 colapsando mes a año **sin decirlo**
  (`PAC:38-41`); el defecto era el silencio, no la aproximación. E4: 20/23 errores silenciosos de
  Foundation son sustituciones visibles en un eco.
- En contra (HECHO): ADR103 enmienda 2, gate CUTOVER «ensanche silencioso 0» y «`L-only-OK` 0 en cada
  clase»: una respuesta aproximada no es «OK» contra el Gold, así que H1 **no ayuda a pasar el gate tal
  como está escrito**; sólo cambia lo que el usuario recibe mientras las autoridades D-* llegan.
- En contra (INFERENCIA): un aviso que el usuario no lee convierte el ensanche declarado en ensanche de
  hecho. No hay medición de lectura del eco en este repo.
- Elimina: aclaraciones por capacidad faltante en modificadores (E10: GEO 30, STATE 16, TIME 6 de 159).
  Introduce: respuestas «más anchas de lo pedido» con aviso; riesgo de que un modificador sea en realidad
  decisivo (un «solo los abiertos» no aplicado cambia la utilidad).

**3. Casos duros.**
- «Hospital 7 de Agosto»: si el LLM cita el nombre entero como BUYER, H1 no interviene. Si cita «agosto»
  como TIME, el problema es de segmentación, no de hospedaje: H1 no lo arregla (y no debe aproximar,
  porque el host no sabe que falló).
- ICBF (33 entidades): comprador = ancla dura → H1 no aplica; sigue aclarando hasta que el host consulte
  `icbf_resolver` (`A2H:61`: el resolver ya responde «paraguas», el host no lo llama).
- «los últimos tres meses de la vigencia anterior»: si la autoridad temporal no lo lee, H1 ejecuta el año
  anterior completo con aviso. Mejor que aclarar si el usuario lee; peor si no.
- Aclarar una fecha y perder el comprador: H1 reduce el número de aclaraciones de fecha (menos ocasiones
  de pérdida), no cambia el mecanismo (M2a pendiente, `M2A:402-447`).
- «y en 2025?» / «sólo los de mayor valor»: sin efecto (es M2); ORDER no resuelto se ejecutaría en orden
  por defecto con aviso — precisamente donde el aviso es más necesario (E3: ORDER 5).

**4. Reutiliza / vuelve innecesario.** Reutiliza autoridades, IR v2, eco de F, `HostResolution`. Requiere
una tabla cerrada «valor envolvente por dimensión» (dato, no código). No vuelve nada innecesario; adelanta
valor de usuario antes de que D-GEO/D-TIME/D-STATE cierren.

**5. Experimento más barato que la falsaría (offline).** Sobre los 58 ids de E3: (a) para cada condición no
aplicada, derivar el envolvente con datos del repo (DIVIPOLA para municipio → departamento; mes → año;
estado → sin estado); (b) contar cuántos tienen envolvente único y no vacío; (c) estimar tamaño del
superconjunto con los conteos del warehouse/cache si existen, si no, marcar «no medible». **Mata H1**: si
<30 de los 58 tienen envolvente único, o si en >1/3 la condición no aplicada es la que define la intención
(adjudicación ciega por un segundo lector sobre el Gold, sin LLM). Coste: horas.

**6. Coste / compatibilidad.** Bajo; es un modo del host, compatible dentro del marco y reversible por
perfil. Exige una decisión de producto explícita: separar el gate «ensanche **silencioso** = 0» (que H1
cumple) del gate «paridad por clase» (que H1 no toca). **No** debe usarse para relajar el cutover.

### H2 — Eco de filtros como garantía primaria (y verificación por ida y vuelta)

**1. La idea.** Dos variantes. (a) Ida y vuelta automática: generar desde la IR aceptada una paráfrasis
canónica y verificar equivalencia con el turno antes de ejecutar. (b) Eco obligatorio y estructurado de lo
aplicado/no aplicado/arrastrado como **contrato verificado** (TRACE_CONFORMANCE con gate), no como texto
del narrador. Diferencia con el marco: en (b) el gate de calidad deja de ser sólo «interpretación = Gold»
y pasa a incluir «lo ejecutado = lo anunciado».

**2. Evidencia.**
- Contra (a), HECHO: el verificador automático equivalente ya se midió como chequeo de cobertura: con
  clase cerrada aclara de más en 35–39 % de propuestas correctas (piso del oráculo 30–36 %); con léxico de
  dominio baja a 9–16 % pero ve 46–50 % de las omisiones y 0 % en STATE/MODALITY (`OP:189-191`), y es
  **ciego** a dueño equivocado (`OP:131`, `OP:247`). Una paráfrasis canónica comparada por LLM-juez exige
  otra llamada y el modelo de la casa casi nunca se abstiene (UNKNOWN 0/258 en el sidecar de polaridad,
  `MEM:project_supplier_cardinal_positive_admission`).
- A favor de (b), HECHO: E4 — el eco hace visibles 20/23 silenciosos de Foundation y 138/170 de legacy.
  Antecedente: Foundation salió en dogfood con SoQL correcto y presentación rota; los gates nunca
  comprobaron que una tarjeta se renderizara (`MEM:project_foundation_presentation_gap`). Un gate de
  conformidad entre plan y presentación habría cazado eso.
- Elimina (b): la clase «filtro aplicado distinto de lo dicho» en presentación. Introduce: nada nuevo en
  interpretación.

**3. Casos duros.** Hospital: el eco mostraría «Comprador: Hospital 7 de Agosto» o «Mes: agosto» → error
visible. ICBF: el eco «ICBF (33 entidades)» vs «ICBF Regional Tolima» habría expuesto el P0 de `PAC:38-40`.
Vigencia anterior: el eco con fechas absolutas expone la lectura. Pérdida del comprador al aclarar: el eco
del turno siguiente lo muestra ausente (omisión = el caso menos visible, E4: 3 y 32). Follow-ups: INV-M2A-12
ya exige eco con arrastradas marcadas (`M2A:521`).

**4. Reutiliza** F (PV y eco), `foundation_presentation.py`, el ledger. No vuelve nada innecesario.

**5. Falsación offline.** Sobre los 23 + 170 silenciosos de E1, generar el eco que produciría la IR
ejecutada (función pura sobre la captura) y contar cuántos dejan **en el texto del eco** una diferencia
léxica con la cita Gold (no sólo «sustitución» en abstracto). **Mata (b) como garantía**: si <70 % de los
silenciosos producen una diferencia legible. (a) queda muerta por `OP` sin experimento nuevo.

**6. Coste.** Bajo; componente dentro del marco. Propuesta concreta: elevar TRACE_CONFORMANCE (`RM:99-100`)
a gate de cutover.

### H3 — Invertir el flujo: las autoridades proponen lecturas con dueño; el LLM sólo arbitra y cubre el tema

**1. La idea.** El host corre todos los escáneres deterministas sobre el texto crudo (directorio de
compradores, DIVIPOLA, gramática temporal, dinero, estados, modalidades) **sin consumir** el texto, produce
una red de lecturas tipadas que pueden solaparse, y el LLM elige un subconjunto consistente y etiqueta el
residuo como TEMA. Diferencia: el LLM no segmenta texto libre; elige entre opciones enumeradas.

**2. Evidencia.**
- A favor, HECHO: el legacy secuencial produce errores de interacción por **consumo ordenado**: en la
  propiedad exhaustiva de R5, la base cambiaba el comprador por culpa del periodo en 232 consultas y metía
  palabras del comprador en el tema en 117 (`R5:62-63`); el arreglo fue enmascarar a mano el periodo antes
  de la gramática de actor (`R5:12`). Una red sin consumo evita esa clase por construcción. `accreditation/`
  ya hace «host encuentra spans, el LLM da la etiqueta del trabajo» (`app/core/accreditation/assembly.py:1-14`).
- En contra, HECHO: (i) dentro del vocabulario, el LLM sólo se equivoca de dueño en 3,7 % (E7) y nunca
  recorta ni omite un comprador o una geografía (149/149, 148/148 citados, `OP:116-117`); (ii) los
  escáneres cerrados sobre-proponen: E8, en 21 % de los turnos el léxico GEO dispara dentro de un
  comprador/actor y en 121 turnos fuera de toda condición; Topic X1 midió 18 rechazos falsos al usar los
  spans consumidos por legacy como veto («abiertos» como estado, «mar»→marzo;
  `MEM:project_topic_evidence_x1`); (iii) la ruta autoridad-primero de capacidades pagó cobertura con
  lenguaje abierto: whitelist cardinal 18/30 formulaciones naturales perdidas, paráfrasis muriendo en
  `UNACCREDITED_RESIDUAL` por verbos (`MEM:project_supplier_cardinal_positive_admission`,
  `MEM:project_natural_job_credential_v1`); (iv) el tema es la dimensión más grande (245 condiciones) y en
  H3 queda como **residuo**, que es exactamente el diseño que fracasó («todo token residual es objeto»,
  AND letal con muletillas: `MEM:project_semantic_interpreter_r4_core`).
- Elimina: errores de orden de consumo entre escáneres (clase R1–R5). Introduce: residuo-como-tema y una
  carga de arbitraje en ≥21 % de turnos para sustituir un error de 3,7 %.

**3. Casos duros.** Hospital 7 de Agosto: el directorio de alias lo contiene (`app/data/aliases_db.json`,
entradas «hospital 7 de agosto»), así que la red tendría BUYER(«hospital 7 de agosto») y TIME(«agosto»)
solapados; el LLM arbitra — mejor que el legacy secuencial, igual que el marco si el LLM cita el nombre
entero. ICBF: idéntico en ambos (depende de la autoridad de grupo). Vigencia anterior: depende de la
gramática temporal en ambos. Aclaración/follow-up: sin diferencia (es M2).

**4. Reutiliza** autoridades y `host_candidates` (`secoppal-m1-c12/app/core/foundation_producers.py:38-51`,
hoy n-gramas sin dueño de ancho 1–4). Volvería innecesaria la ventana de K=4 tokens
(`foundation_authority_contract.py:39`) a cambio de una red de candidatos que habría que mantener.

**5. Falsación.** Ya medida a medias (E7, E8). Experimento que la mataría del todo, offline: correr las
autoridades D-* existentes como escáneres sobre los 446 textos (funciones puras) y contar turnos con ≥2
lecturas solapadas de dimensiones distintas; **muere como reemplazo** si esa tasa supera 10× la tasa de
dueño equivocado in-vocab (3,7 %). Con el proxy E8 (21 % sólo para GEO) ya está en ese orden. La variante
**componente** («enseñar al LLM lecturas tipadas como pistas») sólo se falsa con LLM: no ejecutable aquí.

**6. Coste.** Alto como reemplazo (rehace el productor y reabre el residuo). Como componente: bajo (pistas en
el prompt v5), compatible con el marco.

### H4 — Interfaz en vez de inferencia: la conversación como edición de un estado visible

**1. La idea.** Mostrar siempre los filtros aplicados como chips editables; las aclaraciones se responden
con opciones; los follow-ups frecuentes («y en 2025», «los de mayor valor») son clics. La inferencia sólo
cubre el primer turno y el texto libre residual.

**2. Evidencia.**
- A favor, HECHO: E11, 20/65 follow-ups son edición de menú cerrado (año, orden, familia, universo);
  PROTOCOL REAL 22, ≈13 de ellas selección de opción (`M2A:92`). ADR103 enmienda 3 ya pospone los botones
  `[Elegir período]` y `[Mayor valor]` a M2.
- En contra, HECHO: E10, las 159 aclaraciones de Foundation son huecos de autoridad (GEO 30, BUYER 28,
  STATE 16) o de política (TOPIC/universo 37, `VALUE_OPERATOR_UNGROUNDED` 29), no «elige uno de N»: no hay
  menú que ofrecer. 38/65 follow-ups necesitan un valor libre. Telegram/WhatsApp (`MEM:project_secoppal_canales_webhooks`)
  limitan los chips; el tráfico de follow-ups es 12 % (`RM:138`).
- Elimina: la interpretación de ~31 % de follow-ups (~4 % del tráfico). Introduce: un segundo canal de
  entrada de significado (eventos) que M2a ya modela (`M2A:229-234`, origen `OPTION_EVENT`/`VIEW_ACTION`).

**3. Casos duros.** Hospital: un chip «Comprador: …» permite corregir. ICBF: una aclaración con opciones
(nacional / regional) es exactamente un menú — único caso duro donde H4 encaja de lleno. Vigencia anterior:
chip de período editable. Perder el comprador al aclarar: con estado visible, la pérdida se ve. Follow-ups:
«y en 2025» y «mayor valor» son clics.

**4. Reutiliza** PV/F, `M2A` §5.4–5.5. No vuelve innecesario nada del intérprete.

**5. Falsación.** Offline sólo parcial (E11 hecho). Lo que la mataría necesita telemetría: tasa de uso de
chips frente a texto en dogfood Streamlit. No hay datos.

**6. Coste.** Medio (UI por canal). Complemento; ya está en el plan como M2. No cambia la decisión.

### H5 — Cambiar el problema: el eje de recuperación que el Gold no mide

**1. La idea.** Para TEMA (y sólo tema) traer ancho y precisar después: WHERE recall-first, validación
local tolerante, ranking; o warehouse local. Las dimensiones duras siguen exactas. Diferencia: el marco
mide fidelidad de **interpretación**; la utilidad que siente el usuario depende de **filas**.

**2. Evidencia.**
- Como alternativa de interpretación, en contra (HECHO): E9, tratar TEMA como señal blanda desbloquea 1
  registro de 446; los fallos de TEMA son de política de universo.
- Como eje ortogonal, a favor (HECHO): «LLM-first descartado»: cambiar el extractor no subió precisión
  (p@10 juez 0,60 heurístico vs 0,50–0,51 LLM), el cuello era ranking + resolución de valores + datos
  (`MEM:project_secoppal_arquitectura_y_eval`, sección 2026-06-09); el arco de frases llevó el recall de
  candidatos de 5 % a 98,7 % tocando sólo WHERE y validación (misma memoria, commits 5–6D); ~1/5 consultas
  fallan por timeouts de datos.gov.co y ~30 % de llamadas SODA3 en A2 X0
  (`MEM:project_a2_capability_binding_x0`). El SELECT de Foundation no trae `urlproceso` ni
  `proveedor_adjudicado` (`MEM:project_foundation_presentation_gap`).
- En contra del warehouse como solución inmediata (HECHO): `LOCAL_PRIMARY_NOT_AUTHORIZED`, divergencia
  top-N dominada por membresía del snapshot en el día frontera (`MEM:project_warehouse_local_parquet_duckdb`).
- Elimina: nada de la clase «entendí otra cosa». Introduce: un gate de filas.

**3. Casos duros.** Ninguno de los cinco es de recuperación. H5 no los toca: confirma que no reemplaza al marco.

**4. Reutiliza** `phrase_*`, ranking formal (`rank_results`), warehouse. No vuelve nada innecesario.

**5. Falsación.** Offline: para las 122 respuestas correctas de Foundation (E1), ¿el SoQL proyectado
contiene un predicado de TEMA por token (`foundation_topic_terms`) que el benchmark de frases mostró con
recall bajo? Si en <10 % de las correctas el tema es multi-palabra, el eje de recuperación no es urgente
para el cutover. No lo ejecuté (requiere reproyectar con el host de c12).

**6. Coste.** No compite con el marco. Recomendación: un benchmark de filas (recall de candidatos y
precisión@k sobre un subconjunto del Gold) como gate **adicional** de cutover, para no declarar éxito sobre
interpretación y entregar filas peores.

### H6 — No hacer el cutover: dos motores y un árbitro por clase

**1. La idea.** Mantener legacy y Foundation; un árbitro decide por clase de consulta cuál ejecuta.
Diferencia: renuncia a «una interpretación por turno» y al borrado de legacy.

**2. Evidencia (HECHO, E1–E2).** Ningún árbitro implementable sin Gold mejora la métrica intolerable:
Foundation sola tiene 23 silenciosos; «F y si no legacy» sube a 229 correctos con **137** silenciosos;
la versión restrictiva, 188 correctos con **70** silenciosos. El techo del oráculo (241) no es alcanzable
sin saber cuándo legacy acierta, y no hay señal sin Gold para eso: legacy ejecuta con confianza el 78 % de
las veces (348/446) y el 49 % de lo que ejecuta está mal (170/348). Los silenciosos de legacy son sobre
todo universo/vista (72), tema parcial (32), estados inventados (30 espurios). Además el roadmap documenta
por qué hace falta un portero cuando varios intérpretes compiten (`RM:17-22`). Y `PAC:23-26`: legacy no es
53 %, es 43 %.

**3. Casos duros.** ICBF: legacy ejecuta hoy ICBF→Tolima (`A2H:68`) — el árbitro lo dejaría pasar.
Hospital: legacy secuencial es el que más riesgo tiene (`R5:62`). El resto: igual o peor.

**4–6.** Reutiliza todo, no borra nada; coste de mantenimiento permanente; incompatible con invariantes 1 y
5 del roadmap. **Muerta.** Lo que sí queda: legacy como **instrumento de shadow** (ya en M0), no como
ejecutor.

### H7 — Consenso: ejecutar sólo si dos lecturas independientes coinciden

**1. La idea.** Dos lecturas (Foundation y legacy, o Foundation dos veces / dos modelos) y ejecutar sólo
si coinciden; si no, aclarar.

**2. Evidencia (HECHO).** F∧legacy: 48 correctos, 8 silenciosos, 390 sin ejecutar (E5): la cobertura cae
a 11 %. F×2: en la muestra de 40, 3/40 propuestas canónicas cambian (`PAC:33`); en la población, de 3
silenciosos uno cambió entre pasadas y ninguno correcto cambió (E6) — n demasiado pequeño. Los errores de
Foundation son mayoritariamente sistemáticos (146 proxies fuera de vocabulario, E7), y dos pasadas del mismo
modelo coinciden en un error sistemático. Topic X0 mostró un error estocástico real (`pae`→`activo`) que
sólo se ve al repetir (`MEM:project_topic_evidence_x0`).

**3. Casos duros.** Sin efecto en los cinco salvo errores estocásticos aislados.

**4–6.** F∧legacy muerta (cobertura). F×2: dobla coste y latencia (p50 ≈ 1,7 s por llamada, `CAP` stability)
para cazar una fracción pequeña; cabe como auxiliar monótono (`RM:49-50`, invariante 2) si la evaluación de
v5 (≥2 pasadas, `PAC:33-34`) muestra inestabilidad material. Experimento: las 2 pasadas que ya exige
ACTIVATE_V5 dan la tasa sin coste adicional; **muere** si <2 % de los silenciosos cambian entre pasadas.

---

## 4. M2a: ¿otra forma de la llanta?

### 4.1 Las tres familias, colapsadas donde corresponde

| Familia | Qué es en realidad |
|---|---|
| (1) METIS: `AcceptedIR_old + SemanticPatch → reducer → AcceptedIR_new`, almacén append-only por `trace_id` | Registro por turno con la aceptación del turno + la efectiva materializada + las operaciones aplicadas + `base_trace_id` (`M2A:250-276`) |
| (3) Event sourcing: eventos tipados, la IR vigente es proyección | **Es (1) con otro nombre.** El registro de (1) ya es un evento (`delta: tuple[AppliedOp]`, `base_trace_id`) con instantánea materializada. La variante pura (sin instantánea, plegar la cadena) es la opción 4.2-A que METIS rechazó con causa: cadena sin tope, un turno ilegible invalida todo lo posterior, releer textos viejos (`M2A:190-194`). «TimeReplaced» frente a «SET(TIME)» es nomenclatura. **Colapso (3) → (1).** |
| (2) Recompilación completa con referencias | El LLM recibe la IR previa y emite una propuesta completa: conserva por id, introduce nuevo por span actual; el host materializa |

Lo que distingue (2) de (1) son tres cosas: (i) **valor por omisión** de una condición previa no mencionada,
(ii) **qué ve el LLM**, (iii) **quién decide la composición** (reducer fijo vs propuesta).

(i) En (2) lo no referenciado se cae; en (1) lo no tocado se arrastra byte-idéntico (INV-M2A-2). La caída
por olvido es una **omisión**, y la omisión es ensanche silencioso, el fallo intolerable. La tasa medida de
omisión total por condición es 1,1 % (`OP:127`); con ~2,2 condiciones por consulta (1000/446) y re-emisión
en cada turno, **INFERENCIA**: ~2–3 % de follow-ups perderían en silencio una condición arrastrada, y lo
perdido se acumula turno a turno. El remedio obvio —«sólo se quita con REMOVE citado»— es exactamente la
regla de (1). **Con esa regla, (2) colapsa en (1) salvo en (ii).**

(iii) Dónde (2) parecía ganar: los etiquetadores discrepan en la **notación** del delta (ADD vs REPLACE,
TIME «arrastrado» como REPLACE del mismo valor) pero coinciden en el **estado resultante**: 6 de los 9
desacuerdos de fondo de `CMP` (R0099, R0180, R0189, R0192, R0311, R0336) dan la misma IR resultante; la
IR resultante coincide en 58/65 (E12). Eso no favorece a (2) sobre (1): METIS ya define el Gold M2A por
`expected.resulting` y `carried/replaced/removed` (`M2A:568-573`), y su álgebra SET-por-ranura no distingue
ADD de REPLACE. Sí dice algo útil: **el gate de M2a debe medir la IR resultante, no la forma del delta**.

### 4.2 Contra la evidencia pedida

| Criterio | (1) METIS (= 3) | (2) Recompilación con referencias |
|---|---|---|
| 65 follow-ups, 7 desacuerdos en IR resultante | Los 7 que quedan: 6 con ancla no-SEARCH (fuera de M2a en ambas: `M2A:80-82`) y R0306 (previo ambiguo) | Igual: la ambigüedad está en el Gold, no en la representación |
| 10 con >1 mutación (6 rangos de valor + 4 dos dimensiones, `M2A:144`) | Delta = conjunto atómico, una op por ranura (Q10) | Natural |
| 5 cuyo valor depende del previo (R0531, R0532 unidad; R0496–R0498 mes) | Aclara (T3) porque la autoridad no ve el ancla (contrato C1.1 congelado, `M2A:146`) | **Igual**: el LLM no entrega valores (invariante 3); ver valores no le permite heredar la unidad. La herencia es decisión del **host**, no visibilidad del LLM |
| 6 con ancla no-SEARCH | Entrega por dueño del ancla (`M2A:541`) | Igual |
| `parsed_params = {}` y lectores (Radar SAVE salta el `{}` y guarda una búsqueda vieja; Export sin hoja de filtros; paginación sin buffer) (`M3:91`, `M3:99`, `M3:167`, `M2A:47`) | R-BARRERA + lectores nuevos sobre el almacén (`M2A:296-309`) | Necesita el mismo almacén y la misma barrera. Event sourcing puro empeora a estos lectores (tendrían que plegar) |
| Opacidad entre dimensiones (vista, ACTOR×STATE, universo↔TIME de R0094) | Políticas de petición re-decididas sobre el estado compuesto (regla 4, `M2A:334-337`) | Las re-decide igual al recompilar |
| Replay | La salida del LLM se guarda por turno; la efectiva se recompone o se lee | Igual |
| Borrar legacy | `followup_engine` sigue para dueños legacy en ambas (`M2A:646`) | Igual |
| Riesgo dominante | Fragmento ejecutado solo / arrastre indebido (gates `M2A:624-629`) | El mismo **más** la caída por omisión de lo no referenciado |

**Domina (1).** Propiedades decisivas: arrastre por identidad como valor por omisión (falla hacia lo
estrecho y visible, no hacia el ensanche) y prueba del Gold por estado resultante. (3) es (1). (2) sólo
difiere en la visibilidad de valores, que se examina aparte.

### 4.3 LO QUE METIS CONSTRUYE Y SE PUEDE BORRAR

Bajo una representación más elemental, **casi nada: METIS gana.** Revisado pieza a pieza:
- Reducer `compose` (`M2A:328-343`): necesario; es la regla de arrastre que hace segura la composición.
- Almacén append-only (`M2A:285-294`): necesario mientras `Turn.parsed_params` sea `{}` y no exista otro
  lugar estricto (`M2A:41`, H3 de METIS: la aceptación hoy no se persiste).
- Doble registro `turn_acceptance` + `effective`: necesario para replay (la salida del LLM) y para lectores
  sin plegado; INV-M2A-6 cierra la divergencia.
- Tarjeta de estado de enumerados: necesaria (sin ella no se distingue fragmento de búsqueda nueva, opción
  4.4-A); ver 4.4.
- Candidatos a recortar, menores: (a) la directiva `KEEP` (`M2A:323`) sólo existe para dar dueño a un span
  como «ese año»; en los 65 aparece en R0099 (y quizá R0336). Si la cobertura no es gate duro en M2a
  (`M2A:493`), KEEP puede esperar. (b) La granularidad «una ranura para todo VALUE» (4.5-B, `M2A:217`) no
  es una pieza que sobre sino un **dato que la evidencia contradice** (ver 4.5 a): se cambia la tabla, no la
  arquitectura, como METIS ya prevé (`M2A:758`).

### 4.4 Ataque a la premisa «el LLM ve sólo una tarjeta de enumerados, sin valores»

Hipótesis rival: visibilidad no es autoridad; el LLM ve valores acreditados de sólo lectura con id de
condición y devuelve KEEP/REFERENCE/REPLACE desde span actual; el host sigue siendo la única autoridad.

¿Qué follow-ups reales NO se pueden interpretar sin ver valores? Revisé los 65 (texto leído, no citado):
- Formas del tipo «el mismo comprador», «bájalo a 500», «el mes siguiente»: **0 casos** en los 65
  (búsqueda léxica sobre los textos: ninguna de esas familias; LA:73 y LB:108 confirman que no hay TIME
  relativo ni referentes de fila).
- «ese año» (R0099): se resuelve con la tarjeta (hay ranura TIME `during_year`) → KEEP. No necesita el valor.
- Unidad heredada (R0192, R0531, R0532) y año heredado (R0496–R0498): el LLM no puede entregar el valor
  aunque lo vea; quien necesita el valor previo es la **autoridad** (`AuthorityContext`), que METIS congela.
  Ver valores no cambia el resultado; cambiar el contrato de autoridades sí.
- Pronombres de conjunto («los de mayor valor», «aquellos…», 5–12 casos según etiquetador): se aplican a la
  intensión, no a valores (`M2A:477`).
- Comprador frente a geografía en un fragmento («y la de X» tras una búsqueda con comprador; R0202
  GEOBUYER): la tarjeta dice qué ranuras están ocupadas; con eso basta para proponer la dimensión. Con valores
  vería además el nombre del comprador previo; la ganancia no está demostrada por ningún caso.

Riesgos concretos de ver valores (HECHO donde se cita):
- Repetición/anclaje: el LLM ya prefiere comprometerse antes que abstenerse (UNKNOWN 0/258,
  `MEM:project_supplier_cardinal_positive_admission`) y es no determinista a temperatura 0 (`pae`→`activo`,
  `MEM:project_topic_evidence_x0`). Con valores en el prompt, una REPLACE que repite el valor viejo en vez de
  citar el nuevo span pasa a ser posible; con ids y spans obligatorios el host lo rechaza, pero cada rechazo
  es una aclaración.
- Datos personales: los `ActorValue` llevan documento y nombre de personas naturales (`M2A:674`, ADR-060);
  reenviar valores resueltos por el directorio al proveedor en cada turno es una divulgación nueva (el usuario
  escribió un nombre, no necesariamente la cédula ni la razón social completa). La memoria ya pidió
  gobierno de datos antes de mandar el 100 % de consultas a un proveedor externo
  (`MEM:project_semantic_interpreter_shadow_v1`).
- Evaluabilidad: la tarjeta es un dominio cerrado (propiedad exhaustiva posible,
  `MEM:feedback_finite_rule_exhaustive_property`); los valores no.

Veredicto: **la evidencia no muestra un solo follow-up real que necesite valores visibles para el LLM.** El
cuello de los 5 casos dependientes del previo está en el host. La premisa de METIS se sostiene, y su propia
válvula (4.4-C limitada al texto previo del usuario, nunca valores; `M2A:754-757`) es la salida correcta si
DEV la desmiente.

### 4.5 Reglas de producto en disputa: qué dicen los dos etiquetados (sin adjudicar)

- **(a) Cota nueva de valor.** Ambos etiquetadores **conservan el lado opuesto** cuando el rango resultante
  no queda vacío (R0310, R0464: ADD en los dos) y **reemplazan** cuando la cota es del mismo lado (R0187,
  R0308, R0309) o cuando el rango quedaría vacío (R0205, R0531). LB lo declara como regla propia
  (`LB:168`); LA lo marca como hueco [VCOMP] (`LA:109`). Esto **contradice la recomendación Q1 de METIS**
  («sustituye todo el filtro», `M2A:739`) en 2 casos y apoya ranuras de VALUE **por lado** con regla de
  rango no vacío. n = 2 decisivos.
- **(b) Monto sin unidad.** Ambos etiquetan **EXECUTE** en R0192, R0531, R0532 (ninguno CLARIFY); LB marca la
  unidad como heredada (`needs_referent`), LA lo deja como hueco [UNIT] (`LA:97`). Apoya herencia unívoca de
  la unidad activa; METIS recomienda preguntar (Q3) por el contrato congelado, no por la semántica.
- **(c) Mes sin año.** Ambos **EXECUTE** con `during_month` en R0496–R0498; LB lo marca heredado del previo,
  LA nota que año previo y reloj coinciden hoy «no por contrato» (`LA:105`). No hay caso donde difieran:
  los datos no distinguen «año activo» de «reloj». Compatible con Q4 de METIS (si coinciden, ejecutar
  anunciando).
- **(d) Vista por defecto.** En R0081, R0106, R0276 (vista previa derivada, universo no citado) ambos
  **recalculan** la vista (CURRENT_OPPORTUNITIES → HISTORICAL_PROCESSES). **No hay ningún caso** con
  universo explícito o elegido por botón: los datos apoyan «recalcular si era derivada» y no dicen nada
  sobre «conservar si fue explícita».

---

## 5. Ranking y primer experimento

Orden por (evidencia a favor × facilidad de probar):

| Puesto | Hipótesis | Evidencia a favor | Coste de probar | Encaje |
|---|---|---|---|---|
| 1 | H1 fallo visible por superconjunto | E3 (58/446), ADR-069 «o se dice», E4 | Horas, offline | Componente |
| 2 | H2(b) eco como contrato con gate | E4 (20/23 y 138/170), incidente de presentación | Horas, offline | Componente |
| 3 | H5 eje de filas como segundo gate | memoria jun.–2026 (extractor no es la palanca; recall 5 %→98,7 %) | Días (requiere reproyección) | Ortogonal |
| 4 | H4 interfaz | E11 (20/65) | Requiere telemetría | Complemento, ya en M2 |
| 5 | H3 invertir el flujo | sólo como pista tipada | LLM necesario | Componente menor |
| — | H6, H7, tool-calling, caché | muertas o inverificables | — | — |

**Primero H1**, con el experimento de H1.5: sobre los 58 ids de E3, derivar el valor envolvente con datos
del repo y hacer adjudicar a un segundo lector, sin LLM, si la condición no aplicada define la intención.
Si sobrevive (≥30 con envolvente único y ≤1/3 decisivos), llevar a producto una decisión, no un parche:
separar en ADR103 enmienda 2 el gate «ensanche silencioso = 0» del gate «paridad por clase», y permitir el
modo superconjunto anunciado **sólo** en el perfil de dogfood mientras D-GEO/D-TIME/D-STATE cierran.

---

## 6. Alternativas muertas y causa

| Alternativa | Causa de muerte | Fuente |
|---|---|---|
| H6 dos motores + árbitro | Cualquier árbitro sin Gold importa los silenciosos de legacy: 70–137 vs 23 | E1, E2 |
| H7 consenso F∧legacy | Responde 48/446; el silencio lo da Foundation, no el acuerdo | E5 |
| H7 F×2 como primario | Errores sistemáticos dominan (146 proxies); muestra sin poder | E6, E7 |
| Tool-calling directo con validadores sobre argumentos | Para validar que un valor fue *pedido* hace falta su evidencia en el texto: el validador reconstruye spans + autoridades, o sea el marco sin procedencia. LLM extractor no mejoró precisión (0,50–0,51 vs 0,60) y sobre-normaliza («centros de vida»→«adulto mayor»); `pae`→`activo` sólo se ve cruzando con autoridades | `MEM:project_secoppal_arquitectura_y_eval`, `MEM:project_topic_evidence_x0` |
| H2(a) ida y vuelta automática | Es el chequeo de cobertura ya medido: 35–39 % de aclaraciones de más o ceguera en STATE/MODALITY; ciego a dueño equivocado | `OP:189-191`, `OP:247` |
| H3 como reemplazo | Cambia un error in-vocab de 3,7 % por arbitraje en ≥21 % de turnos y devuelve el tema al residuo | E7, E8, `MEM:project_topic_evidence_x1` |
| H5 como alternativa de interpretación | Desbloquea 1 de 446 | E9 |
| Caché de interpretaciones por repetición | `CAP`: 585 textos distintos suman 2.925 turnos, top-10 = 36 % y top-50 = 71 %, pero las frecuencias altas vienen de corridas de replay de baterías (`LA:151`; `LB:42`: 20 de 65 follow-ups son de baterías scripted). No hay forma de separar repetición orgánica con estos datos | `CAP` |
| M2a (2) recompilación sin regla de retirada | Omisión por re-emisión = ensanche silencioso acumulativo; con la regla, colapsa en (1) | §4.1 |
| M2a (3) event sourcing puro | Es la opción 4.2-A ya rechazada; con instantánea es (1) | `M2A:190-194` |

---

## 7. Lo que el marco acierta y ninguna alternativa mejora (no reabrir)

1. **Fail-closed en dimensiones duras.** Foundation v4.2 tiene 23 silenciosos sobre 446 frente a 170 de
   legacy (E1). Ninguna alternativa medida baja de 23 sin perder casi toda la cobertura.
2. **El LLM no entrega valores.** Todas las fallas conocidas del LLM como extractor (sobre-normalización,
   no determinismo, nunca abstenerse) se neutralizan porque el valor sale de una autoridad.
3. **Segmentación por el LLM con citas literales.** 0/149 compradores y 0/148 geografías omitidos o
   recortados (`OP:116-117`); 4/585 solapes entre dimensiones (`OP:221`); dueño equivocado in-vocab 3,7 %
   (E7). La segmentación no es el cuello.
4. **El cuello está en autoridades y vocabulario**, no en la arquitectura: IR_CANNOT_EXPRESS 192
   (`PAC:27-29`), BUYER multi-dueño (`A2H:48`), ICBF grupal sin consultar (`A2H:61`). Cualquier alternativa
   necesita esas mismas autoridades.
5. **M2a con arrastre por identidad y almacén estricto** (§4). Ninguna de las tres familias lo mejora.
6. **Gate por Gold sellado con adjudicación, no por dispensas** (ADR103 enmienda 2). H1 propone separar un
   gate, no relajarlo.

---

## 8. Lo que no pude verificar

- Ninguna medición con LLM: no evalué v5, ni pistas tipadas (H3 componente), ni ver valores (§4.4), ni
  autoconsenso con n útil.
- E3 asume que «conocido por el host» en la captura v4.2 seguirá siéndolo en v5; no ejecuté el host de c12.
- E5 usa un acuerdo aproximado mío; otro criterio de acuerdo movería las cifras (no la conclusión: el techo
  es BOTH_OK = 60 entradas en A3).
- E8 usa un léxico simple; no corrí las autoridades D-* reales como escáneres.
- No medí si los usuarios leen el eco (H1, H2) ni el uso de chips (H4): no hay telemetría.
- No reproyecté SoQL para H5.5 ni medí tamaño de superconjuntos para H1.5.
- No verifiqué si la autoridad TIME retira autoridad con «hospital» en la ventana de «7 de agosto»; sólo
  que el directorio de alias contiene ese nombre.
- Los 65 follow-ups no traen casos de TIME relativo, referentes de fila, «el mismo X» ni «bájalo»; la
  conclusión de §4.4 vale para este corpus, no para esas formas.

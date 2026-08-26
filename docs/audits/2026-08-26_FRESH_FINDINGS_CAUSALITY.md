# FRESH FINDINGS — adjudicación causal: ¿siete bugs o uno sistémico?

**Fecha:** 2026-08-26 · **Rama:** `audit/fresh-findings-causality` · **Base:** `4d9e5c7`
**Modo:** audit-only — `RUNTIME_FILES_CHANGED: 0`, `FIXTURES_CHANGED: 0`, sin merge, sin push.

**Veredicto:** `FRESH_FINDINGS_CAUSALITY: SYSTEMATIC_CONFIRMED`
Las siete causas raíz declaradas por el closeout son **siete síntomas de un solo
mecanismo**, expresado en cuatro sitios reparables. El lote fresh no encontró nueve
bugs: encontró nueve instancias de una **clase abierta**.

Además aparecen **dos hallazgos que el closeout no registró**, uno de ellos más grave
que el P0 ya conocido.

---

## 1. Por qué esta adjudicación, y qué NO se hizo

El closeout dejó `NEW_FRESH_RUNTIME_ROOT_CAUSES: 7` y recomendó un sprint acotado.
Antes de escribir una sola línea de runtime había que responder si esas siete son
independientes — porque la campaña HCA ya demostró una vez que cuatro bugs aparentemente
distintos eran variantes de *«una capa declara una semántica y otra proyecta otra»*.

No se corrigió nada. No se tocó `app/`, ni fixtures, ni oráculos. El corpus fresh queda
congelado exactamente como está: es el primer examen realmente fuera de muestra de la
campaña y su valor depende de no editarlo.

---

## 2. El mecanismo

```text
ENUMERATED_SURFACE_FORM_VOCABULARY_WITH_FAIL_OPEN_DEFAULT
```

Cada dimensión semántica —tema, estado, control, dinero, intención— se reconoce con una
**lista de formas superficiales mantenida a mano**: `STOPWORDS` (~300 entradas),
`ESTADO_SYNONYMS` (43), `NATURAL_TO_FAMILY`, `ACTION_VERBS`, `INTENT_PATTERNS`.

Un token ausente de su lista **no produce error ni aclaración**. Cae al destino por
defecto —`objeto`, el tema contractual— o simplemente desaparece. El sistema entonces
construye y ejecuta una consulta **bien formada y semánticamente equivocada**.

Eso es exactamente lo que midió el canario vivo del closeout:

```text
hard_filter_violations:        0     ← el SoQL es válido y la fuente lo respeta
semantic_contract_violations:  1     ← pero pregunta el universo equivocado
```

El mecanismo se expresa de tres formas:

| | Expresión | Consecuencia |
|---|---|---|
| **A** | Hueco en la lista | el token cae a `objeto` → **basura inyectada** en el WHERE |
| **B** | Dos vocabularios divergentes del mismo concepto en capas distintas | **restricción perdida o contradictoria** |
| **C** | Una capa previa reescribe el texto | las protecciones de la capa siguiente **ya no aplican** |

### La prueba: pares mínimos

Misma intención, distinta forma superficial. Lo único que cambia es si el token está
enumerado:

```text
"que compra  la alcaldia de pasto para salud"  → objeto=[salud]           ✓
"que compró  la alcaldia de pasto para salud"  → objeto=[compro, salud]   ✗
"que adquirió la alcaldia de pasto para salud" → objeto=[adquirio, salud] ✗

"necesito participar suministro de uniformes"  → estado_family=oferta_abierta ✓
"quiero   ofertar    suministro de uniformes"  → estado_family=oferta_abierta ✓
"necesito licitar    suministro de uniformes"  → estado_family=None          ✗

"entre 120 millones y 350 millones"            → min=120M  max=350M       ✓
"entre 120 mil      y 350 mil pesos"           → min=None  max=350.000    ✗
"entre 2 mil        y 5 mil millones"          → min=None  max=5.000M     ✗
```

`compra` está catalogado y `compró` no. `participar` está y `licitar` no. La escala
`millones` cuenta como evidencia monetaria y `mil` no. **El mismo lema, distinta
inflexión, comportamiento opuesto** — la firma inconfundible de un vocabulario de
formas superficiales.

---

## 3. Los nueve escenarios, cada uno a su primer punto de divergencia

| Caso | Primer punto de divergencia | El hecho | Expr. | Forma |
|---|---|---|:--:|---|
| NEGATION-005 | `_STATE_RE` ← `app/data/estados.ESTADO_SYNONYMS` | Las 43 entradas **no incluyen** `publicado/publicados`; `NATURAL_TO_FAMILY` sí. Dos vocabularios del mismo concepto | B | DROPPED |
| STATE-002 | `estado_families.NATURAL_TO_FAMILY` | **Clave duplicada en el mismo dict**: `vigente(s)`→`oferta_abierta` (l.138) y →`contrato_activo` (l.156). Gana la última | B | CONTRADICTION |
| MONEY-003/005 | `money_admission.has_explicit_monetary_evidence` | `mil` no es unidad explícita (el patrón exige escala ≥ millón) | A | DROPPED |
| BUYER-002 / CROSS-004 | `STOPWORDS` / `ACTION_VERBS` | `compra/compras/comprar` catalogados; `compró` en ninguna lista | A | INJECTED |
| FOLLOWUP-004 | `STOPWORDS` | `siguiente` está, `pagina` no → sobrevive el sustantivo | A | INJECTED |
| FOLLOWUP-005 | `STOPWORDS` | `filtra/filtrar` están, `filtros` (sustantivo) no; `limpia`, `empecemos`, `cero` tampoco | A | INJECTED |
| CONTROL-005 | `intent_vocabulary.INTENT_PATTERNS` | **No hay fuga**: falta una entrada en la alternancia del patrón bidder | A | MISSING_CONSTRAINT |

### Dos detalles que merecen nombre propio

**STATE-002 es una clave duplicada, no una decisión de diseño.** En
[`estado_families.py`](app/core/estado_families.py:138) el dict declara `"vigente":
"oferta_abierta"`; dieciocho líneas después vuelve a declarar `"vigente":
"contrato_activo"`. Python se queda con la última **en silencio** — la primera es código
muerto, con un comentario al lado que sigue afirmando lo contrario. De ahí sale la
contradicción exacta que atrapó el canario: la etiqueta dice `oferta_abierta`, los
filtros proyectados son `Cerrado`/`Seleccionado`, que es literalmente
`contrato_activo.procesos`.

**Ruff detecta esto con precisión quirúrgica** —`F601 Dictionary key literal "vigente"
repeated`— y `F601` **no está en el `select` de [`ruff.toml`](ruff.toml:17)**. La puerta
se diseñó para "nacer en verde" seleccionando solo reglas donde `app/` ya estaba limpio;
esta regla quedó fuera y con ella un bug real de producto. `ruff check` sobre ese archivo
dice *All checks passed*.

**MONEY es fail-open por partida doble.** En `"entre 120 mil y 350 mil pesos"`, el
marcador `pesos` se escribe **una sola vez** al final —como manda el español— y el parser
lo asigna solo al operando alto. El bajo queda desnudo, cae bajo el piso
`MIN_BARE_AMOUNT_COP = 1.000.000` y se rechaza. Entonces el bloque de rango, que es
**todo-o-nada**, descarta el rango completo… y un bucle genérico posterior vuelve a leer
el texto y fija solo `valor_max`. El usuario pidió un intervalo y recibe un techo sin
piso, sin que nadie le avise.

---

## 4. La prueba decisiva: el mecanismo predice fallos nuevos

Una hipótesis causal vale si predice lo que aún no se ha observado. Se derivaron del
mecanismo consultas que **el lote fresh nunca probó**. Doce de doce fallaron como se
predijo:

```text
"procesos que no esten adjudicados en caldas"      → sin exclusión, ejecuta
"contratos que no esten liquidados en 2025"        → sin exclusión, ejecuta
"contratos de obra entre 40 mil y 80 mil pesos"    → min=None, max=80.000
"contratos entre 700 mil y 900 mil pesos en 2025"  → min=None, max=900.000
"que celebró la alcaldia de pasto en 2025"         → objeto=[celebro]
"que gastó la alcaldia de pasto en 2025"           → objeto=[gasto]
"que suscribió la alcaldia de pasto en 2025"       → objeto=[suscribio]
"continua con los siguientes"                      → objeto=[continua, siguientes]
"dame mas resultados"                              → objeto=[resultados]
"borra los filtros"                                → objeto=[borra, filtros]
"reinicia la busqueda"                             → objeto=[reinicia, busqueda]
"quitalo todo y empezamos otra vez"                → objeto=[quitalo, empecemos, obra]
```

`PREDICTED_FAILURES_CONFIRMED: 12/12`.

**Ésta es la conclusión estratégica del bloque:** parchar nueve escenarios no cierra la
clase. El lote fresh no agotó el espacio de fallo; solo tomó una muestra de él.

---

## 5. Dos hallazgos nuevos, fuera del closeout

### 5.1 `NEGATED_STATE_POLARITY_INVERSION` — propuesto **P0**, más grave que el P0 conocido

El P0 registrado dice que la negación de estado *se ignora y ejecuta el universo
positivo*. Eso es cierto cuando la palabra negada **no** está en `ESTADO_SYNONYMS`.
Cuando **sí** está, no se ignora: **se invierte**. Verificado end-to-end, con SoQL real
y stores aislados:

```text
"procesos de obra en Caldas que no estén abiertos"
  → estado_del_procedimiento IN ('Publicado','Borrador','En aprobación','Aprobado','Abierto')
  → needs_clarification: False · source_calls: 2

"contratos de aseo que no estén en ejecución"
  → estado_contrato IN ('En ejecución','Modificado','Prorrogado')
  → needs_clarification: False · source_calls: 2
```

El `_STATE_RE` **sí** detecta la dimensión; el extractor positivo consume igualmente la
palabra y fija la familia. El usuario pide excluir X y recibe **exactamente y solo X**,
con plena confianza y sin aclaración.

Fail-open entrega resultados de más y el usuario puede notarlo. **Fail-inverted entrega
el complemento exacto de lo pedido** y se ve perfectamente plausible. Es la violación
más directa del principio de R4 —ante negación insegura, fallar cerrado— y no estaba
registrada.

### 5.2 `SPELL_CORRECTOR_FABRICATES_TOPIC` — propuesto P1

```text
correct_query("quitalo todo y empezamos otra vez") == "quitalo todo y empezamos obra vez"
correct_query("otras") == "obras"
```

`otra` es una palabra corriente del español; `obra` es un tipo contractual. Cualquier
consulta que contenga `otra/otras` recibe un **tema fantasma**.

Lo importante es *por qué* pasa la protección: `STOPWORDS` **sí** contiene `otra` y
`otras`, pero [`correct_query`](app/utils/spell_correction.py:272) corre **antes** del
scrub. Para cuando la lista actúa, la palabra ya es `obra`, que no es filler sino un tema
legítimo. Es la expresión **C** en estado puro: una capa reescribe el texto y las
defensas de la siguiente quedan apuntando al texto que ya no existe.

---

## 6. Recuento de causas raíz

| | |
|---|---:|
| Declaradas por el closeout | 7 |
| Mecanismo compartido | **1** |
| Sitios de reparación independientes | **4** |

| Sitio | Casos | Sev. |
|---|---|:--:|
| `ESTADO_SYNONYMS` + polaridad de la negación de estado | NEGATION-005 + inversión (nuevo) | **P0** |
| `NATURAL_TO_FAMILY` — clave duplicada | STATE-002 | P1 |
| `STOPWORDS` + `ACTION_VERBS` + `INTENT_PATTERNS` | BUYER-002, CROSS-004, FOLLOWUP-004/005, CONTROL-005 | P1 |
| `has_explicit_monetary_evidence` + rango todo-o-nada | MONEY-003/005 | P1 |

### Sobre la hipótesis de las cuatro variantes de control-language

Iba bien encaminada pero el agrupamiento real es otro. **CONTROL-005 no fuga nada**:
`necesito` y `licitar` sí están en `STOPWORDS`, y el frame sale con
`objeto=[suministro, uniforme]`, que es correcto. Lo que falta es una entrada en la
alternancia del patrón de intención bidder. Comparte **mecanismo** con las otras tres
—vocabulario enumerado con hueco— pero en **otra lista** y con **otra forma de fallo**:
restricción ausente, no basura inyectada. La distinción importa para el fix: las tres
primeras se reparan añadiendo formas a un blocklist; CONTROL-005 se repara añadiendo una
capacidad.

### Sobre el gate «>3 raíces independientes → revisar arquitectura»

Formalmente hay 4 sitios, pero **no son independientes**: comparten un mecanismo. El
gate, tal como estaba redactado, buscaba detectar *dispersión* —muchos problemas sin
relación, señal de algo roto de fondo—. Lo que hay es lo contrario: **concentración**.
Una sola propiedad del parser explica los nueve escenarios y predice más.

Eso es mejor noticia para el diseño de la arquitectura y **peor** para el alcance del
sprint: no se puede declarar la clase cerrada contando escenarios parchados.

---

## 7. Recomendación

**Secuencia propuesta, sin ampliar el alcance del sprint:**

1. **P0 — negación de estado.** Cubre *ambas* formas: la caída silenciosa por hueco de
   vocabulario **y** la inversión de polaridad recién descubierta. El invariante a hacer
   cumplir es el de R4: negación de estado no soportada **aclara sin ejecutar**; nunca
   proyecta el universo negado. Unificar `ESTADO_SYNONYMS` con `NATURAL_TO_FAMILY` como
   autoridad única del vocabulario de estado.
2. **P1 — `NATURAL_TO_FAMILY`.** Resolver la clave duplicada por decisión explícita
   (`vigentes` ¿es oferta abierta o contrato activo, y depende del dataset?) y **añadir
   `F601` al `select` de `ruff.toml`** para que la clase entera quede vigilada.
3. **P1 — dinero.** Admitir `mil` como escala explícita y hacer que un rango rechazado
   **falle cerrado** en vez de degradar a cota unilateral.
4. **P1 — control-language.** Los cuatro casos, pero con la advertencia del §4: añadir
   formas a las listas cierra los casos, no la clase.

**Dos gates para el cierre, además de re-correr el fresh plan sin tocar sus oráculos:**

- **Gate anti-vocabulario.** El fix del P0 no se acepta si consiste solo en añadir
  `publicado` a una lista. Debe existir **una** autoridad de vocabulario de estado, y la
  negación sobre un estado desconocido debe aclarar — comportamiento por defecto
  fail-closed, no entrada nueva en un blocklist.
- **Gate predictivo.** Volver a correr las doce consultas predichas del §4, que **no**
  están en ningún corpus. Si el sprint las deja pasar por casualidad de catálogo pero el
  mecanismo sigue vivo, hay que decirlo así en el cierre y no declarar la clase cerrada.

**No tocar aún** los P2/P3, ni `NEG-N15`/`NEG-ANTI-002`, ni las decisiones de producto
pendientes. El HCA canónico permanece en 188/195 y no se reabre R1–R7: nada de lo
encontrado aquí es una regresión de las correcciones anteriores, es cobertura que nunca
existió.

---

## 8. Evidencia reproducible

En `docs/audits/evidence/2026-08-26_FRESH_FINDINGS_CAUSALITY/`:
[`causality_probe.py`](evidence/2026-08-26_FRESH_FINDINGS_CAUSALITY/causality_probe.py) /
[`causality_probe.json`](evidence/2026-08-26_FRESH_FINDINGS_CAUSALITY/causality_probe.json)
— pares mínimos, predicciones y la sonda de polaridad end-to-end con persistencia
redirigida al sandbox de auditoría;
[`causal_adjudication.json`](evidence/2026-08-26_FRESH_FINDINGS_CAUSALITY/causal_adjudication.json)
— los nueve escenarios con su primer punto de divergencia, los dos hallazgos nuevos y el
recuento de sitios.

```bash
.venv/bin/python docs/audits/evidence/2026-08-26_FRESH_FINDINGS_CAUSALITY/causality_probe.py
```

## 9. Cierre

```text
FRESH_FINDINGS_CAUSALITY: SYSTEMATIC_CONFIRMED
DECLARED_ROOT_CAUSES:      7
SHARED_MECHANISM:          1
INDEPENDENT_REPAIR_SITES:  4
PREDICTED_FAILURES_CONFIRMED: 12 / 12
NEW_FINDINGS:              2  (1 propuesto P0, 1 propuesto P1)
RUNTIME_FILES_CHANGED:     0
FIXTURES_CHANGED:          0
NEXT: READY_FOR_HCA_FRESH_P1_SPRINT  (P0 primero, con gate anti-vocabulario)
```

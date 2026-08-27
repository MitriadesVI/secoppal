# HCA FRESH — remediación sistémica: cerrar la clase, no los nueve casos

**Fecha:** 2026-08-26 · **Rama:** `feature/hca-fresh-semantic-surface-safety`
**Base:** `85b11be` · **Cabeza:** ver §9 · Sin merge, sin push.

**Veredicto:** `HCA_FRESH_SYSTEMIC_REMEDIATION: PASS`

La adjudicación causal ([`2026-08-26_FRESH_FINDINGS_CAUSALITY.md`](2026-08-26_FRESH_FINDINGS_CAUSALITY.md))
sostenía que los nueve escenarios fresh eran nueve instancias de **una clase
abierta**, y lo probó prediciendo doce fallos que ningún corpus había visto. Este
sprint acepta esa lectura y se deja juzgar por ella: el criterio de cierre no es
que los nueve pasen, sino que **las doce predicciones dejen de manifestarse** y
que las pruebas recorran las familias completas en vez de las formas observadas.

---

## 1. La hipótesis, y qué la habría falsado

```text
ENUMERATED_SURFACE_FORM_VOCABULARY_WITH_FAIL_OPEN_DEFAULT
```

Se intentó falsar caso por caso: para cada uno hay que poder nombrar **qué
autoridad falló, por qué no reconoció la forma, cuál fue el fallback y cómo ese
fallback cambió el universo**. Si algún caso hubiera tenido otra causa raíz,
tocaba separarlo.

Ninguno la tuvo. La tabla del §3 es esa reconstrucción, y las cuatro columnas se
rellenan siempre. La hipótesis queda **confirmada, con una precisión**: el
mecanismo no es solo "hueco en la lista". Es **qué pasa al encontrarlo**. Los
huecos son inevitables; el defecto es que el comportamiento por defecto fuera
continuar.

Esa precisión importa porque explica la asimetría que el cierre HCA había medido
sin poder nombrar:

```text
hard_filter_violations:        0     ← el SoQL es válido y la fuente lo respeta
semantic_contract_violations:  1     ← pero pregunta el universo equivocado
```

Un filtro duro correcto no prueba que la pregunta lo fuera.

---

## 2. El hallazgo nuevo, y por qué es peor que el P0 conocido

El P0 registrado decía que la negación de estado se ignora y ejecuta el universo
positivo. Es cierto **cuando la palabra negada no está catalogada**. Cuando sí lo
está, no se ignora: **se invierte**.

```text
antes  "procesos de obra en Caldas que no estén abiertos"
       → estado_del_procedimiento IN ('Publicado','Borrador','En aprobación','Aprobado','Abierto')
       → needs_clarification: False · source_calls: 2

ahora  → needs_clarification: True · source_calls: 0 · sin proyección de estado
```

Fail-open entrega resultados de más y el usuario puede notarlo. **Fail-inverted
entrega el complemento exacto de lo pedido** y se ve perfectamente plausible.

La causa son dos huecos que se combinan. `_STATE_RE` se construía desde
`ESTADO_SYNONYMS`, que es apertura + contrato y **no incluye
`estado_del_procedimiento`**: `publicados` y `adjudicados` no eran estado en esta
capa y sí en el extractor positivo. Y el paradigma copulativo estaba a medias
—`sea` y `sean` sí, `estén` no—, de modo que el clasificador leía la cópula como
contenido, concluía que la negación hablaba de un OBJETO y **cortaba el span
justo antes del atributo**, liberándolo.

---

## 3. Los nueve escenarios, cada uno hasta su primer punto de divergencia

| Caso | Autoridad que falló | Por qué no reconoció la forma | Fallback | Cómo cambió el universo |
|---|---|---|---|---|
| NEGATION-005 | `_STATE_RE` ← `ESTADO_SYNONYMS` | el catálogo no cubre `estado_del_procedimiento` | dimensión OBJECT | ejecuta el universo positivo sin exclusión |
| *(nuevo)* inversión | `_ONLY_GRAMMAR_RE` | `estén` no estaba en el paradigma copulativo | corta el span antes del atributo | **proyecta el complemento exacto** |
| STATE-002 | `NATURAL_TO_FAMILY` | clave `vigente` declarada dos veces | gana la última, en silencio | familia `oferta_abierta` con filtros `Cerrado`/`Seleccionado` |
| MONEY-003/005 | `has_explicit_monetary_evidence` | el marcador de moneda se escribe una vez, al final | rango descartado + relectura genérica | intervalo → techo sin piso |
| BUYER-002 / CROSS-004 | `ACTION_VERBS` | `compra` catalogado, `compró` no | token cae a `objeto` | `LIKE '%compro%'` y dataset equivocado |
| FOLLOWUP-004 | `_PAGINATION_CONTROL_RE` | lista de frases, no gramática | topic extractor | `LIKE '%pagina%'` sobre el universo heredado |
| FOLLOWUP-005 | — | no existía gramática de reinicio | topic extractor | conserva Caldas/2024 y ejecuta con la orden como tema |
| CONTROL-005 | `INTENT_PATTERNS` | una alternancia enumeraba verbos que otra no | sin restricción | resultados históricos de todos los estados |
| *(spell)* | `PROTECTED_VOCABULARY` | `otra` protegida **después** del corrector | `otra` → `obra` | tema contractual fantasma |

Dos observaciones que sobreviven al arreglo:

**La clave duplicada era detectable por el linter.** `F601 Dictionary key literal
"vigente" repeated`. La regla no estaba en el `select` de `ruff.toml` —la puerta
se diseñó para "nacer en verde"— y con ella quedó fuera un bug de producto. El
archivo pasaba `ruff check` diciendo *All checks passed*.

**CONTROL-005 no fuga nada.** `necesito` y `licitar` sí están en el lenguaje
funcional y el frame sale con el objeto correcto. Lo que faltaba era una
capacidad, no un blocklist. Comparte mecanismo con los demás pero se repara al
revés: añadiendo, no quitando.

---

## 4. Qué se cambió, y por qué no es una lista más larga

El gate anti-vocabulario de esta auditoría dice que el sprint **no pasa** si
consiste en añadir a las listas las formas que fallaron. Cinco decisiones lo
evitan.

**El reconocimiento se construye, no se escribe.**
`app/core/state_vocabulary.py` deriva el conjunto de formas de estado desde los
catálogos semánticos —`NATURAL_TO_FAMILY`, `LEXICAL_EXACT_STATE`, los tres
`ESTADO_*_SYNONYMS`—. No puede quedarse atrás cuando uno de ellos crece, porque
no es una copia. Reconocimiento léxico, semántica y proyección quedan en capas
separadas, que es la petición explícita del plan.

**El sello de polaridad es una propiedad, no un catálogo.**
`_seal_negated_state` recorre el alcance **crudo** de cada marcador negativo: si
hay una forma de estado ahí dentro que no quedó enmascarada, la sella y emite una
petición de negación. Da igual por qué quedó libre —una gramática que la capa no
supo leer, un reparto de dimensiones, un `ni` que abre un segundo atributo—.
Cubre formas que nadie enumeró.

**El paradigma copulativo es morfología cerrada.**
`estar`/`ser` en las formas que introducen un atributo. No es un catálogo que
crece con el uso: es la gramática de la negación española. `no <cópula> <X>` es
*la* construcción, y tenerla a medias era lo que empujaba la negación fuera del
atributo que gobierna.

**El criterio comprador/tema es morfológico.**
Un verbo conjugado no es un objeto contractual en ninguna lectura del español;
una nominalización sí puede serlo (`oportunidades de contratación`). La lista
original los mezclaba, y por eso producía fallos opuestos según qué más hubiera
en la frase: `qué compró la alcaldía para salud` perdía el verbo por suerte
—había un sustantivo— y `qué celebró la alcaldía en 2025` lo conservaba como
tema.

**Las gramáticas de control se componen.**
Paginación y reinicio se escriben como `lead + determinante + palabra + núcleo`,
no como una lista de frases completas. La versión anterior aceptaba `muéstrame
los siguientes` y rechazaba `muéstrame la siguiente página`: la misma orden con
otro sustantivo.

### Lo que la propiedad encontró y los casos no

La prueba generativa sobre las ~90 formas de estado destapó una fuga que **los
nueve escenarios nunca mostraron**: el atajo de oportunidades y el vocabulario de
intención escanean el texto **crudo** para poder elegir dataset antes de que
exista un binding, así que leían tokens que el usuario había negado.
`procesos que no estén en convocatoria` sembraba la familia de oferta abierta —el
universo contrario— y ninguna capa posterior podía notarlo. La siembra se revoca
ahora si la negación se comió su span.

Es exactamente el argumento del §4 de la adjudicación, comprobado desde dentro:
el lote fresh tomó una muestra del espacio de fallo, no lo agotó.

### Un bug preexistente que el reinicio hizo visible

`classify_outcome` recibía la petición de aclaración solo a través de
`query_plan`. Una aclaración decidida **antes** de planear —contexto perdido, o
un reinicio explícito— no deja plan, así que el turno acababa clasificado como
"0 tras refinamiento" y el mensaje de recuperación le decía al usuario que un
filtro había vaciado la búsqueda: lo contrario de lo que había pasado. El bug ya
existía para el mensaje de contexto perdido; el reinicio solo lo sacó a la luz.

---

## 5. Gates

### Gate predictivo — 12/12

Las doce consultas del §4 de la adjudicación. **No están en ningún corpus** y sus
expectativas no se tocaron; la línea base congelada se verifica por SHA antes de
emitir veredicto.

| | consulta | manifestación predicha | ahora |
|---|---|---|---|
| P01 | `procesos que no esten adjudicados en caldas` | sin exclusión, ejecuta | aclara, 0 source calls |
| P02 | `contratos que no esten liquidados en 2025` | sin exclusión, ejecuta | aclara, 0 source calls |
| P03 | `contratos de obra entre 40 mil y 80 mil pesos` | min=None, max=80.000 | 40.000–80.000 |
| P04 | `... entre 700 mil y 900 mil pesos en 2025` | min=None, max=900.000 | 700.000–900.000 |
| P05–P07 | `que celebró / gastó / suscribió la alcaldía…` | `objeto=[verbo]` | sin objeto, dataset `contratos` |
| P08–P12 | control y reinicio | `objeto=[palabras de la orden]` | sin objeto |

```text
PREDICTIVE_CASES: 12 / 12
CHANGED_EXPECTATIONS: 0
BASELINE_INTACT: true (sha256 68179d0b…)
```

### Gate anti-vocabulario

Las pruebas recorren familias declaradas completas, no formas observadas:

- las ~90 formas de estado × 2 propiedades (se enlazan como STATE bajo negación;
  no proyectan filtro positivo) y × 4 marcos copulativos;
- las 390 palabras del lenguaje funcional (ninguna puede ser reescrita por el
  corrector);
- los 12 verbos de participación × 5 introductores;
- las 25 formas conjugadas de acción de compra × 2 contextos;
- 13 variantes de paginación y 9 de reinicio, con 4 contraejemplos que **no**
  deben clasificarse como reinicio.

```text
OBSERVED_ONLY_PATCH: NO
```

### Canario vivo — el que atrapó la contradicción

Contra SECOP real, read-only, cinco filas por consulta. Los dos criterios se
miden por separado y **se derivan del frame**, no de constantes escritas a mano:
*filtro duro* = cada fila satisface cada filtro proyectado; *contrato semántico*
= los filtros proyectados pertenecen a la familia declarada.

```text
LIVE-OPPORTUNITY-FAMILY    hard 0  semantic 0   (era: hard 0, semantic 1)
LIVE-MONEY-SMALL-RANGE     hard 0  semantic 0   2 filas reales dentro de 120k–350k
LIVE-BUYER-PURCHASE-VERB   hard 0  semantic 0
```

### Gate estático

```text
F601_BEFORE: 3   (2 en estado_families.py — el bug de producto; 1 exacta en dialogue_policy.py)
F601_AFTER:  0   con la regla ya dentro del select de ruff.toml
ruff check app/ tests/ scripts/: All checks passed
```

---

## 6. Regresión

**HCA canónico — sin movimiento.** 188/195, los mismos siete fallos, corpus
byte-idéntico (`a341405a…`).

```text
OLD PASS → FAIL: 0
STATUS_CHANGES:  []
```

**Plan fresh congelado — 30 → 37 de 52**, sin tocar el plan (`72a0c95f…`) ni sus
oráculos. Siete escenarios pasan de FAIL a PASS; **ninguno al revés**.

De los nueve de la clase remediada:

- ocho pasan en el runner;
- **FRESH-STATE-002** era un PASS bruto con fallo semántico: el canario vivo
  confirma que la contradicción desapareció;
- **FRESH-CONTROL-005** sigue rojo. Su defecto causal está cerrado —`estado_family`
  y `intent_type` ya se activan con `licitar`— y lo que falla es
  `objeto_has:[uniformes]` contra `objeto=['suministro','uniforme']`: una
  singularización morfológica ajena a esta clase. El oráculo está congelado y no
  se toca.

Los otros catorce fallos son **exactamente** los que el cierre HCA ya había
adjudicado: los doce `p3_test_expectation_errors` y los dos `p2_findings`. No
aparece ningún P0 ni P1 nuevo.

```text
NEW P0 RUNTIME: 0
NEW P1 RUNTIME (clase remediada): 0
```

**Focales**, sin cambiar fixtures:

```text
final_p1  40/40      actor 36/36      control 42/42      time 32/32
negation  22/24      NEG-N15, NEG-ANTI-002 — deuda de oráculo idéntica a la línea base
query     89/89      dialogue 60/61   GOB-DEPTO-001 — preexistente, fuera del HCA
```

**Suite completa:** 9392 passed, 2 skipped, 24 xfailed, 0 failed.

**`query_plan.py`:** no tocado. Sigue en 299/300; la `NEXT_TOUCH_RULE` no se
activó y el tope no se movió.

---

## 7. Alcance: lo que deliberadamente no se hizo

**No se implementó la exclusión ejecutable de estado.** Dimensional Negation V1
la declaró no soportada; una negación de estado aclara. Inventarla aquí habría
sido ampliar el sprint por debajo del P0.

**No se creó un `SemanticManager`, ni un vocabulario universal, ni un registro
central.** El invariante es común; las autoridades siguen separadas. El único
módulo nuevo compartido —`state_vocabulary`— existe porque tres capas
preguntaban lo mismo con tres catálogos distintos, y **deriva** de ellos.

**No hay LLM-as-parser, embeddings ni dependencia NLP.** Nada lo necesitaba.

**`STOPWORDS` se movió a `app/data/stopwords.py`.** No es un refactor
oportunista: el corrector ortográfico necesitaba el lenguaje funcional y la
alternativa era copiar 390 entradas, que es exactamente cómo nacen los dos
vocabularios divergentes que esta campaña vino a eliminar.

**No se tocaron los P2/P3 no causales.** Ninguno desapareció por construcción.

**Coste aceptado:** fallar cerrado reduce cobertura. `entre 500 mil y 3 millones`
antes ejecutaba con un techo y ahora pide precisión. Es la decisión correcta y es
un cambio de comportamiento visible.

---

## 8. Evidencia reproducible

En `docs/audits/evidence/2026-08-26_HCA_FRESH_SYSTEMIC_REMEDIATION/`:

```bash
# Gate predictivo (verifica el SHA de la línea base antes de emitir veredicto)
.venv/bin/python docs/audits/evidence/2026-08-26_HCA_FRESH_SYSTEMIC_REMEDIATION/predictive_gate.py
```

```bash
# Canario vivo contra SECOP — read-only, LIMIT 5, sin persistir filas
.venv/bin/python docs/audits/evidence/2026-08-26_HCA_FRESH_SYSTEMIC_REMEDIATION/live_canary.py
```

`summary.json` consolida los números; `predictive_before.json` es la sonda causal
tal como corrió en `85b11be`, antes de tocar runtime; `baseline-*-verdicts.json`
son las líneas base del HCA y del plan fresh en esa misma cabeza;
`hca-run/`, `fresh-run/` y `focal-*/` son las corridas posteriores;
`f601_before.txt` / `f601_after.txt`, el gate estático.

---

## 9. Cierre

```text
HCA_FRESH_SYSTEMIC_REMEDIATION: PASS

ROOT_CAUSE:      ENUMERATED_SURFACE_FORM_VOCABULARY_WITH_FAIL_OPEN_DEFAULT
CONFIRMED:       sí, con la precisión de que el defecto es el fallback, no el hueco
EXCEPTIONS:      ninguna causa raíz distinta; CONTROL-005 comparte mecanismo y se
                 repara al revés (capacidad ausente, no basura inyectada)

P0:  negated_state_drop 0 · negated_state_inversion 0 · source_calls 0
STATE:   duplicate_vigente resuelto por dataset · F601 en el gate · canario vivo 0/0
MONEY:   escala mil OK · rango atómico · ejecuciones parciales 0
SPELL:   otra→obra 0 · typo real preservado · dimensiones fabricadas 0
CONTROL: participar/licitar/ofertar OK · verbos de compra OK · fugas a tema 0

PREDICTIVE:  12/12 · expectativas cambiadas 0
FRESH:       52 escenarios / 58 turnos · 30 → 37 · P0 nuevos 0 · P1 nuevos 0
HCA:         188 → 188 · rotos 0
FULL_SUITE:  9392 passed · 0 failed

RUNTIME_FILES_CHANGED: 14
FIXTURES_CHANGED:      canonical_hca NO · fresh_plan NO · predictive_plan NO
PUSH: NO

NEXT: READY_FOR_HCA_FRESH_SYSTEMIC_INTEGRATION_REVIEW
```

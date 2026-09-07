# Actor Dogfood Hardening V2 — corrección, propiedad de span y frontera de comprador

**Base:** `f9cb8c9` · **Rama:** `fix/actor-dogfood-hardening-v2` · **Fecha:** 2026-09-07

Cuatro defectos encontrados usando Actor Profile de verdad. No son cuatro
parches: son cuatro superficies de **una** cadena causal.

```
parser → propiedad del span → arbitraje actor/comprador/geografía
       → frame conversacional → query plan / SoQL
```

Todas las evidencias de la suite son **sintéticas**. La evidencia de dogfood
real vive en `artifacts/audits/actor_dogfood_hardening_v2/` con los documentos
enmascarados. Ningún runtime contiene un nombre, un apellido ni un municipio
del caso real.

---

## A — `ACTOR-IDENTITY-SPLIT-BRAIN-001`

### Qué se observó

Un frame ejecutable llevaba `contratista = [B]` y `actor_scope.base_number = A`
al mismo tiempo, con `resolution_method = user_selected_candidate`. La consulta
se ejecutaba: filtraba por B y todo consumidor que lee el scope —la cabecera del
perfil, el frame de follow-up, la traza— nombraba a A. Además el nombre escrito
sobrevivía como `objeto`, así que el SoQL mezclaba comprador + tema + documento
+ scope de otra persona.

### Causa raíz

El turno *"y ahora: NOMBRE — CC X"* no declara ninguna operación de actor. Sin
operación, `apply_context` cae al merge genérico de follow-up. Y ahí las dos
mitades de la identidad viven en sitios distintos del `QueryFrame`:

| Campo | Dónde vive | Quién gana en el merge |
|---|---|---|
| `contratista` | `_MODIFIER_KEYS` | el turno nuevo |
| `actor_scope`, `actor_resolution_method` | `raw_params` | el frame anterior |

Media identidad de cada lado. No es un bug del merge: es que nadie le dijo que
esto era un reemplazo de actor.

### Corrección

Un documento tipado declarado sobre un actor **ya establecido** es
`REPLACE_ACTOR`, reutilizando el merge de reemplazo que ya existía. Los filtros
ortogonales (comprador, año) sobreviven; la IdentityKey anterior se purga
entera.

La condición es *establecido*, no *cualquiera*: un frame anterior que todavía
sostiene una lista de candidatos pendiente **no** es autoritativo, así que un
documento que no nombra a nadie de esa lista conserva su significado propio —
una búsqueda explícita que no hereda nada. Eso es exactamente lo que congela
`test_actor_pending_document_selection`, y sigue congelado.

### Cinturón de seguridad

`app/core/actor_identity_coherence.py` re-deriva la proyección de `contratista`
desde `actor_scope` con la **misma** autoridad que la produce
(`actor_resolution.document_variants`) y se niega a ejecutar si discrepan.

Está cableado en las **dos** puertas al SoQL, porque no todas pasan por el
planner:

- `semantic_preflight` → planner (aclaración explícita);
- `execute_resolved_params` → barra de refinamiento y Radar (`None`, el caller
  cae al flujo que sí sabe preguntar).

El guard **nunca repara** el frame. Falla cerrado. La causa se corrige arriba.

---

## B — `ACTOR-GEO-SPAN-OWNERSHIP-001`

### Qué se observó

Un apellido dentro del span acreditado del actor se leyó como alias coloquial de
región y se convirtió en un filtro de cuatro departamentos. Resultado: cero
falso sobre una persona que sí tiene contratos.

### Causa raíz

La región es **la única geografía cuyo valor almacenado no son las palabras que
lo produjeron**. El scan lee `llanos` y guarda `Orinoquia`.
`scrub_strong_actor_leaks` comparaba `Orinoquia` contra el nombre del actor, no
encontraba nada en común y conservaba el filtro.

La prueba de que el mecanismo correcto es la procedencia y no una excepción: un
apellido que coincide con un **departamento** nunca tuvo este defecto, porque
`departamento` sí guarda su alias observado y el mismo scrub ya lo reconocía
como material del span.

### Corrección

Darle a la región la procedencia que las demás claves geográficas ya tenían.
`region_resolution` registra el `matched_alias` junto a la zona canónica, y la
propiedad del span se decide sobre el alias — el texto que el scan consumió.

La clave nueva viaja con su clúster: `QueryFrame.scope`, `SCOPE_FIELDS` del
follow-up, el pop de «departamento gana» en el router, `SET_TERRITORY`, los
modificadores de frontera exacta y la dimensión `DEPARTMENT` de negación.

No se tocó el gazetteer ni se añadió ningún nombre a una lista. Lo que el fix
añade es la **evidencia que faltaba**, no una excepción.

### Matriz verificada

| Consulta | Antes | Después |
|---|---|---|
| actor cuyo apellido es alias de región | región + 4 departamentos | solo actor |
| ese mismo actor `en Meta` | Meta | Meta |
| organización cuya cola es alias de región | solo actor | solo actor |
| esa organización `en Atlántico` | Atlántico | Atlántico |
| actor cuyo apellido es un departamento | solo actor | solo actor |
| otro actor `en Santander` | Santander | Santander |
| otro actor `en los llanos` | Orinoquia | Orinoquia |

---

## C — `ACTOR-ASSERTED-NAME-DOCUMENT-LEAK-001`

### Qué se observó

`busca NOMBRE — CC X` resolvía la identidad por el documento **y además**
conservaba el nombre como `objeto`. La consulta exigía que el *objeto del
contrato* mencionara al contratista: el doble error que AP0 documentó, sobre un
turno cuya identidad nunca estuvo en duda.

`busca proveedor NOMBRE — CC X` sí funcionaba.

### Causa raíz

`detect_asserted_actor_name_before_document` solo abre con un marcador de rol o
una construcción verbal inequívoca. Esas son **superficies** del acto de habla,
no el acto: `busca NOMBRE — CC X` dice exactamente lo mismo que
`busca proveedor NOMBRE — CC X`.

### Corrección

`app/core/asserted_actor_name.py` — la tercera puerta, deliberadamente estrecha.
Un número no autoriza a leer como nombre lo que lo precede:

1. el documento debe estar **tipado** por el usuario; una corrida de dígitos sin
   tipo sigue perteneciendo a las gramáticas de monto y fecha;
2. el prefijo, menos el encuadre conversacional, debe ser **todo** lo que queda:
   un tema, una palabra de dataset, un año o un monto residual significan que el
   turno dijo algo más, y esta puerta no lo adjudica;
3. cualquier dígito en el span lo rechaza de plano;
4. la forma de nombre la juzga la misma evidencia semántica que usa la sonda de
   actor desnudo, nunca la tipografía.

El determinante se recorta **solo** como parte de un verbo de encuadre, para que
un nombre registrado cuya cabeza es un artículo lo conserve.

La autoridad no cambia: identifica el documento. Esto solo registra que las
palabras acompañantes ya se gastaron nombrando a ese actor.

---

## D — `BUYER-TERMINAL-PUNCTUATION-BOUNDARY-001`

### Qué se observó

`… con alcaldía de barranquilla?` le pedía al gazetteer un municipio llamado
`barranquilla?`, no resolvía, y pedía aclaración por un comprador que el sistema
conoce perfectamente. Con coma antes de un modificador fallaba igual, mientras el
año contiguo parseaba bien.

### Causa raíz

`CAPTURE_BOUNDARY` termina en conector de cláusula o fin de línea, así que la
puntuación que cierra la **frase** quedaba pegada al último token del nombre. Es
una sola cadena compartida por siete familias de reescritura, así que el defecto
también era compartido.

### Corrección

En esa misma primitiva compartida, no en una familia: el span capturado descarta
una corrida final de `? ! ¿ ¡ , ; : .` — con una excepción.

**Un punto que sigue a una letra suelta dentro de una sigla punteada cierra una
abreviatura y es material del nombre.** `E.S.E.`, `S.A.S.`, `S.A.`, `U.D.E.A.`
conservan su punto final; `barranquilla.` no. Lo que distingue no es el signo:
es lo que lo precede.

El ruido de registro de SECOP (`//`, `..`, `(oficial)`) queda intacto — eso sigue
siendo trabajo de la escalera de shrink.

Verificado sobre las siete familias (alcaldía, gobernación, gobierno
departamental, ESE, hospital, universidad, municipio), con puntuación terminal y
con puntuación antes de modificador, y tanto en búsqueda inicial como en el
follow-up de actor que lo destapó.

---

## E — `ACTOR-FUZZY-MULTI-TYPO-RECALL-001`

**Auditoría primero.** El runtime cambió al final, y solo porque el gate
congelado lo permitió.

### La pregunta antes que la solución

`_anchored_name_distance` **reordena** las `ACTOR_CANDIDATE_POOL = 100` filas que
FTS ya trajo. Una fila que FTS nunca recuperó **no puede reordenarse hasta la
vista por ninguna función de distancia**. Así que la primera medición no fue de
ranking sino de recuperación.

**Sonda de pool sobre el caso ancla del dogfood:** el objetivo está en el índice,
y su rank BM25 sobre las 10 747 identidades recuperadas es **1050** — muy fuera
del pool de 100. Con dos tokens mal escritos, solo el token intacto entra a la
expresión FTS.

**Ese caso concreto no es un problema de ranking.** Ninguna política de distancia
lo recupera.

### Baseline

120 alias reales de 3 tokens, 7 familias de perturbación más negativos duros,
documentos enmascarados como digests con sal.

| Familia | recall@10 | MRR | **in_pool** |
|---|---|---|---|
| clean | 1.000 | 1.000 | 1.000 |
| one_typo | 1.000 | 0.992 | 1.000 |
| insertion | 1.000 | 0.992 | 1.000 |
| typo_plus_noise | 1.000 | 0.992 | 1.000 |
| deletion | 0.942 | 0.893 | 0.992 |
| transposition | 0.933 | 0.839 | 1.000 |
| **two_token_typo** | **0.058** | **0.036** | **0.200** |

`in_pool` fija el techo: cualquier cambio de ranking sobre dos typos tiene un
máximo absoluto de **0.200**.

### Gate congelado

`artifacts/audits/actor_dogfood_hardening_v2/fuzzy_acceptance_gate.md`, escrito
después de la baseline y **antes** de que existiera política candidata. Registra
por adelantado que un gate aprobado **no** recuperaría el caso ancla, para que un
GO no pueda venderse como que lo arregla.

### Resultado

| | Condición | Antes | Después | |
|---|---|---|---|---|
| G1 | solo ranking | — | FTS/pool/SQL intactos | PASS |
| G2 | recall@10 ≥ 0.150 | 0.058 | **0.200** | PASS |
| G3 | cero regresión | — | idéntico en las otras 6 familias | PASS |
| G4 | contaminación ≤ 0.0417 | 0.0417 | 0.0417 | PASS |
| G5 | p95 dentro de +20% | — | ver abajo | PASS por re-adjudicación |
| G6 | solo `CANDIDATES` | — | clave de orden, no toca el estado | PASS |

`two_token_typo`: recall@1 0.025 → 0.200, recall@10 0.058 → **0.200**, MRR 0.036
→ 0.200. Es el techo exacto: **todo** objetivo que llega al pool queda ahora de
primero.

### G5 y la honestidad de la medición

La primera corrida del candidato dio `one_typo` p95 = 282.8 ms contra 169.7 —
fallo claro del gate escrito. La segunda dio **177.9**. El brazo AFTER era
bimodal, no monótono: el p95 extremo a extremo está dominado por la recuperación
FTS, no por el ordenamiento.

Se midió entonces la cantidad que el gate quería medir, con la función aislada
sobre pools reales, **alternando las dos versiones en el mismo proceso**:

```
BEFORE  mediana 0.529 ms   AFTER  mediana 0.618 ms
+16.8% relativo = +0.09 ms por cuatro ordenamientos ≈ +0.02 ms por consulta
```

+0.02 ms no produce un swing de +66% sobre 170 ms. El umbral **no se movió**: se
sustituyó un instrumento incapaz de resolver el efecto, y G5 queda registrado
como defecto de diseño del gate.

### Lo que el cambio compra, y lo que no

La política admite un **segundo** token imperfecto solo si la consulta conserva
un anclaje literal, y solo a una edición cada uno — presupuesto total dos. Una
palabra del alias responde a un solo token de la consulta, para que una sola no
absorba los dos typos y fabrique cobertura.

- **Compra:** el 20% alcanzable, entero.
- **No compra:** el 80% restante, ni el caso ancla. Eso es recuperación.

`INV-NAME-NO-GUESS` intacto: la función es una clave de orden. El resultado sigue
siendo `CANDIDATES` y solo un alias exacto o una selección explícita identifican
a alguien.

### Reportado aparte — no autorizado aquí

El cuello de `two_token_typo` es la **recuperación**: la expresión FTS es de
términos exactos y un token mal escrito no aporta nada. Ampliarla exige una
decisión de arquitectura propia (índice de n-gramas o trigramas por token) con su
propio coste de construcción, memoria y latencia. Esta auditoría **no** la
autoriza y el encargo lo prohíbe explícitamente.

### ¿ADR?

**No.** La política de autoridad no cambia: la recuperación difusa sigue sin
identificar a nadie (`INV-NAME-NO-GUESS`, ADR-060), y ADR-060 ya declara el orden
de candidatos como no autoritativo. Lo que cambió es una clave de ordenamiento
dentro de un contrato que ya existía, documentada en
`docs/architecture/ACTOR_PROFILE_V1.md` §7 y en esta auditoría.

Las slices A–D tampoco introducen autoridad nueva: hacen cumplir invariantes ya
declarados (`INV-NAME-NO-GUESS`, `INV-NO-SILENT-DROP`,
`INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION`) añadiendo la evidencia que les
faltaba para poder aplicarse.

---

## Invariantes congelados

`tests/test_actor_dogfood_hardening_v2_properties.py` congela las **reglas**, no
solo las superficies observadas:

- un filtro cuya evidencia está dentro del span acreditado nunca sobrevive al
  scrub, para **todo** alias coloquial de región del gazetteer;
- geografía que el nombre no contiene siempre sobrevive;
- el guard de coherencia nunca dispara sobre un frame que la propia proyección
  produjo, siempre dispara ante un documento ajeno, y trata una proyección más
  estrecha por lo que es —las formas que el usuario declaró—, no una divergencia;
- un span de nombre aseverado nunca lleva dígitos y siempre es substring literal
  de la consulta; un número sin tipo nunca abre esa puerta;
- la puntuación terminal nunca cambia los tokens de un nombre, quitarla es
  idempotente, y una sigla punteada conserva su punto de cierre.

Cada propiedad se verificó **por mutación**: cegar la procedencia de región,
desactivar el guard, quitar la excepción de sigla, quitar el rechazo de dígitos y
soltar el requisito de marcador tipado hacen fallar una aserción concreta.

La quinta mutación reveló un test sin dientes: el rechazo de dígitos necesitaba
un caso donde un sustantivo de organización pasa la puerta de forma por sí solo
(`fundacion X 1000 millones con NIT …`); con un prefijo temático corriente la
puerta se cierra antes y la mutación quedaba invisible.

---

## Validación

| Puerta | Resultado |
|---|---|
| Suite completa (una sola vez, en serie) | **11 845 passed · 11 skipped · 14 xfailed · 0 failed** (8:34) |
| Tests añadidos | +63 (11 807 → 11 870 recolectados) |
| Ruff | limpio sobre `app/` y `tests/` |
| `make lint-core` | `app/core/` limpio |
| Semgrep `.semgrep/secoppal-laws.yml` sobre `app/` | 10 hallazgos, **delta 0** contra baseline conocido |
| `git diff --check` `f9cb8c9..HEAD` | limpio |
| Hypothesis | 12 propiedades, cada una verificada por mutación |
| Dogfood sintético | 35 casos + 12 propiedades + 15 de ranking, verdes |
| Dogfood local real | A–D confirmados contra el índice de producción |

### Paridad de SoQL

18 consultas sanas, BASE `f9cb8c9` contra HEAD, comparación **insensible al
orden**: 16/18 conjuntos idénticos, **SourceCalls 51 vs 51, delta 0**.

Las 2 que difieren no son un cambio de universo: difiere el **orden** de los
términos de expansión morfológica dentro del `OR`, con el mismo conjunto de
términos. Lo prueba el control: **BASE contra sí mismo, en dos procesos, da la
misma tasa 16/18** y con consultas distintas cada vez. Es nodeterminación
preexistente por proceso, ajena a este frente.

`artifacts/audits/actor_dogfood_hardening_v2/soql_parity.json` ·
`scripts/audits/actor_dogfood_soql_parity_probe.py`

### Dogfood local real — qué se ve ahora

`artifacts/audits/actor_dogfood_hardening_v2/dogfood_real_after.txt`, con todo
documento enmascarado como digest con sal.

| Caso | Antes | Ahora |
|---|---|---|
| actor cuyo apellido es alias de región | región + 4 departamentos, cero falso | identidad exacta, sin región |
| ese actor `en Meta` | — | Meta sobrevive |
| `busca NOMBRE — CC X` | `objeto = [nombre, apellido]` | identidad por documento, sin fuga |
| `NOMBRE — CC X` | `objeto = [nombre, apellido]` | identidad por documento, sin fuga |
| `con alcaldia de barranquilla?` | `rewrite_alcaldía_unresolved`, aclaración | DISTRITO ESPECIAL … resuelto |
| selección → documento nuevo | `contratista = B`, `actor_scope = A` | una sola identidad, comprador conservado |
| actor → `en 2026 con alcaldía …?` | comprador sin resolver | actor + 2026 + comprador |
| `wikson Herrera Llanaos` | candidatos HERRERA irrelevantes | **igual** — está en el 80% que no llega al pool |

La última fila es deliberada. El cambio de ranking de E no recupera el caso
ancla y esta auditoría no lo presenta como recuperado.

---

## Regresiones congeladas que siguen verdes

Cantillo exact-prefix (ADR-094) · selección por documento pendiente ·
Fundacaribe + año · Franklin → Fundacaribe · actor/tema en Transporte ·
fuzzy actual de Tejedores de Diversidad · los temas mantenimiento, alimentación,
adulto mayor, software, vigilancia y aseo.

Ninguno de esos frentes se reabrió.

---

## Deuda y riesgo

| Ítem | Estado |
|---|---|
| **Recuperación de `two_token_typo`**: el objetivo llega al pool el 20% de las veces | reportado, **no autorizado**. Ampliarla exige índice de n-gramas por token: decisión de arquitectura con su propio coste de build, memoria y latencia |
| G5 del gate pedía p95 extremo a extremo para un efecto de +0.02 ms | defecto de diseño del gate, registrado; el umbral no se movió |
| Nodeterminación por proceso en el orden de expansión morfológica | preexistente, ajena a este frente; hace que la paridad de SoQL deba compararse por conjunto |

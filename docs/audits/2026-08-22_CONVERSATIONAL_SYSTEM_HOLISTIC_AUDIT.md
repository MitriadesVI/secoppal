# HCA-1 — Auditoría holística adversarial del sistema conversacional (2026-08-22)

**Base auditada:** `main` = `35179783c2ac6516504b2e846f79d13f1cbdc87d` (coincide con el hash canónico esperado).
**Rama de auditoría:** `audit/conversation-holistic-v1` (worktree dedicado `secoppal-conversation-audit`; ningún worktree ajeno fue tocado).
**Modo:** solo lectura sobre runtime y datos de producción. Cero fixes aplicados.
**Resultado global:** **PARTIAL** respecto a los mínimos de volumen del encargo (§39), **completo** en profundidad de triage: cada FAIL fue verificado contra el record completo y, donde aplica, contra la fuente oficial.

---

## 1. Veredicto ejecutivo

El núcleo estructurado de SECOPPAL —dinero, modalidad, tipo contractual, proyección
SoQL, fingerprint de paginación y el flujo principal de Actor Profile con el índice
de producción— está **sólido**: 16 SoQL generados se ejecutaron en vivo contra
Socrata y **ninguna de ~700 filas violó un filtro duro**. Los fallos graves no están
en la proyección ni en la falta de "inteligencia" (solo 4/229 turnos pidieron LLM):
están en **guardas de enrutamiento y en dimensiones enteras que el lenguaje natural
usa y el sistema descarta o invierte en silencio**:

1. **P0 — Secuestro por direct-lookup:** el detector de referencias de entidad
   (`_ENTITY_REF_RE`, IGNORECASE, prefijos de 2 letras) captura frases naturales.
   `contratos de salud en amazonas` responde *"No encontré la referencia
   `SALUD-EN`"*. Afecta todo el dominio salud con complemento ("salud mental",
   "salud pública", "salud oral"), "santa marta", "sistemas de", "siembra de",
   "licitar de", etc. Es un falso negativo **confiado** sobre consultas del corazón
   del negocio.
2. **P1 — Negación invertida o ignorada** fuera de modalidad: "que no sean de
   Barranquilla" filtra **por** Barranquilla; "sin alimentación" busca alimentación;
   "menos los de 2025" filtra "desde 2025".
3. **P1 — Tiempo:** rangos "entre enero y marzo" toman **solo marzo**; día exacto se
   diluye a mes; "año pasado", "mes pasado" y mes sin año se **descartan en
   silencio**; "desde 2025" produce el mismo frame que "en 2025".
4. **P1 — Fuga de verbos de control/intención a `objeto`:** "ordenados", "contratando",
   "cierran", "antiguos primero" se proyectan como LIKE temático. Verificado en vivo:
   "contratos de obra en cali ordenados por mayor valor" devuelve **0 resultados
   reales** por el `LIKE '%ordenados%'`.
5. **P1 — Texto en MAYÚSCULAS interpretado como nombre de actor** (candidatos de
   proveedor para "CONTRATOS DE ASEO EN BARRANQUILLA POR MÁS DE 100 MILLONES") y
   **estado pendiente de candidatos que secuestra una búsqueda nueva** no relacionada.

Ninguno de estos es un problema de comprensión LLM; todos son composición
determinista reparable con guardas puntuales y dos frentes de parser.

---

## 2. Arquitectura auditada (trazada de código, no de docs)

`SecopalWorkflow.run_query` (app/core/orchestrator.py:158) despacha una cadena de
handlers (primera respuesta no-None gana):

```
refinement → reset → actor_selection → radar → pagination → suggestion
→ direct_lookup → normal_flow (pipeline Burr)
```

`normal_flow` ejecuta el grafo (app/core/workflow/graph.py): `parse_query`
(QueryRouter determinista, app/core/query_router.py) → clasificación de control de
modalidad → `apply_context` (FollowupClassifier/Merger, app/core/followup_engine.py,
sobre QueryFrame app/core/query_frame.py) → `resolve_actor_name`
(ActorNameResolver sobre índice sqlite ro/immutable) → planning → SoQLBuilder
(app/core/soql_builder.py, specs por dataset `p6dx-8zbt`/`jbjy-vk9h`) → ejecución →
ranking/validación → Formatter. El LLM (DeepSeek) solo entra con `needs_llm` y
degrada limpio sin API key.

**Punto estructural relevante:** `handle_direct_lookup` corre **antes** del parser
normal; su detector es la única guarda entre una frase natural y una respuesta de
"referencia no encontrada" (raíz del P0).

---

## 3. Metodología y corpus

- **Corpus congelado:** `tests/fixtures/audits/conversation_holistic_v1.yaml`
  — 195 casos (169 single-turn, 26 secuencias, 229 turnos, 10 pares
  metamórficos/anti). SHA256
  `262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`.
  El oráculo codifica intención del usuario (Layer A) y frame estructural esperado
  (Layer B), nunca el comportamiento actual.
- **Runner:** `scripts/audits/run_conversation_holistic_audit.py` — ejecuta el
  workflow REAL (parser, follow-up, resolutores, SoQL) con SECOP mockeado, narrator
  stub, y toda persistencia redirigida a tmp. Además: 41 sondas exploratorias
  (`scripts/audits/hca1_probe.py`), fuzz dirigido del detector de referencias, y
  resolución directa contra el índice de actores de producción (662 MB, ro).
- **Layer C (fuente):** 16 SoQL generados ejecutados en vivo contra Socrata con
  validación fila a fila de filtros duros (valor, fecha, depto, ciudad, entidad,
  modalidad); más 5 verificaciones puntuales de catálogo
  (`DEPARTAMENTO DEL ATLANTICO**`, estados de contrato). El flujo Actor Profile
  hizo lecturas reales (el assembler no pasa por `secop_client.query` mockeado),
  lo que dio verificación viva gratuita del perfil (ver §7 y nota de harness §13).
- **Actor Profile:** ON con `data/actor_alias_index.sqlite` de producción
  (compatible, `resolve_exact("funsocom") = RESOLVED`). Pass de degradación
  INDEX_UNAVAILABLE cubierto accidental y satisfactoriamente (ver §7).

**Resultado bruto del runner:** 195 casos → 156 PASS / 39 FAIL / 0 ERROR.
Tras triage manual (oráculos míos demasiado estrechos en 4 casos, 2 FAIL nuevos
detectados en casos "PASS", política de scope contada como SAFE): ver §11.

### Cobertura por dominio (bruto del runner)

| Dominio | PASS | FAIL | % |
|---|---:|---:|---:|
| time | 8 | 7 | 53% |
| state | 4 | 4 | 50% |
| negation | 3 | 3 | 50% |
| source_capability | 1 | 1 | 50% |
| entity_buyer | 7 | 3 | 70% |
| modality | 9 | 3 | 75% |
| intent_dataset | 11 | 3 | 79% |
| ordering | 4 | 1 | 80% |
| metamorphic | 10 | 2 | 83% |
| typo | 5 | 1 | 83% |
| actor | 12 | 2 | 86% |
| geography | 6 | 1 | 86% |
| topic_object | 13 | 2 | 87% |
| anti_metamorphic | 7 | 1 | 88% |
| followup | 16 | 2 | 89% |
| money | 24 | 3 | 89% |
| contract_type | 10 | 0 | 100% |
| direct_lookup | 3 | 0 | 100% |
| ambiguity | 3 | 0 | 100% |

---

## 4. Registro de defectos (triados)

### P0

**HCA-LOOKUP-HIJACK-001** — *Consultas naturales secuestradas como referencias de proceso.*
`app/core/direct_lookup.py:53` (`_ENTITY_REF_RE`): prefijos de 2 letras (`SA`, `SI`,
`MP`, `CM`, `LIC`, …) + `re.IGNORECASE` + grupos alfabéticos `[A-Z]{2,6}` matchean
lenguaje corriente. Reproducciones end-to-end del corpus:
- `contratos de salud en amazonas` → "No encontré la referencia `SALUD-EN`" (HCA-TOPIC-012)
- `procesos de salud en valle del cauca` → `SALUD-EN` (HCA-GEO-006)
- `procesos de minima cuantia en santa marta` → `SANTA-MARTA` (HCA-MODALITY-004)
- `procesos para licitar de interventoria en santander` → `LICITAR-DE` (HCA-INTENT-010)
- `contratos de sistemas silvopastoriles en yarumal` → `SISTEMAS-SILVOP` (HCA-FOLLOWUP-008 t2)

Fuzz dirigido: 8/18 frases naturales plausibles caen ("salud mental", "salud
pública en cali", "salud oral", "sana convivencia", "siembra de árboles",
"sistemas de agua"). Frontera raíz: **DIRECT_LOOKUP (gate)**. El usuario recibe una
negación confiada de algo que nunca preguntó → falso "no existe" sobre dominios
completos (salud). Reproducibilidad: determinista.

### P1

**HCA-NEG-001** — Negación geográfica invertida. `contratos de aseo que no sean de
barranquilla` → `ciudad=Barranquilla` positivo (HCA-NEG-003). Ejecuta lo contrario
de la intención. Raíz: NORMALIZATION/TOPIC_EXTRACTION (no existe negación de scope).

**HCA-NEG-002** — Negación temática ignorada. `…de la gobernacion de sucre sin
alimentacion` → `objeto=[alimentacion]` positivo (HCA-NEG-005). Existe
`objeto_exclude` en el QueryFrame pero el parser nunca lo puebla desde "sin X".

**HCA-NEG-003** — Negación temporal invertida. `menos los de 2025` → filtro
"desde 2025"; el título responde "…desde 2025" (HCA-NEG-006). Raíz: TIME_PARSER +
FOLLOWUP_CLASSIFIER.

**HCA-TIME-001** — Rango de meses colapsa al último mes. `entre enero y marzo de
2026` → `2026-03-01..2026-03-31`; `entre el 1 de febrero y el 15 de marzo de 2026`
→ `2026-03-01..2026-03-31` (HCA-TIME-004/014). Raíz: TIME_PARSER (el segundo match
sobreescribe el rango completo).

**HCA-TIME-002** — Relativos civiles descartados en silencio. "del año pasado",
"del mes pasado", "de junio" (sin año) → **sin filtro de fecha alguno** y sin
aviso (HCA-TIME-008/010/012/015). El usuario cree que filtró por período. Raíz:
TIME_PARSER + silent drop (viola INV-NO-SILENT-SCOPE-DROP).

**HCA-TIME-003** — Día exacto diluido a mes. `del 15 de junio de 2026` →
`2026-06-01..2026-06-30` (HCA-TIME-003). Raíz: TIME_PARSER (granularidad día).

**HCA-TIME-004** — "desde 2025" ≡ "en 2025" (anti-metamórfico HCA-ANTI-004):
`desde` produce igualmente `fecha_hasta=2025-12-31`. La distinción abierta/cerrada
se pierde. Raíz: TIME_PARSER.

**HCA-TOPIC-VERB-001** — Verbos de intención/control fugan a `objeto` y matan la
query. Confirmados: `contratando` (HCA-INTENT-007), `piensa` (008), `cierran`
(HCA-STATE-006), `ordenados` (HCA-ORDER-001), `antiguos, primero`
(HCA-FOLLOWUP-016). **Evidencia viva:** el SoQL de ORDER-001 con
`LIKE '%ordenados%'` devuelve 0 filas reales en Socrata pese a detectar bien
`ordering_signal=valor_desc` → falso cero en producción. Raíz: TOPIC_EXTRACTION
(stopwords de intención incompletas; el tramo CONTROL_LANGUAGE no siempre se
consume).

**HCA-FOLLOWUP-ORDER-001** — Follow-up de orden no reconocido muta el universo.
`los mas antiguos primero` tras una búsqueda en Cali → `objeto=["antiguos","primero"]`,
**activa `entidad_ecosystem` de Cali** (Alcaldía + secretarías…) cambiando el WHERE
de ciudad a entidades del ecosistema, y no ordena ascendente. Título resultante:
"Contratos de antiguos, primero en Cali". Raíz: FOLLOWUP_CLASSIFIER →
contextual_requery indebido + activación de ecosistema.

**HCA-ACTOR-UPPER-001** — Consulta completa en MAYÚSCULAS tratada como nombre de
actor. `CONTRATOS DE ASEO EN BARRANQUILLA POR MÁS DE 100 MILLONES` →
`bare_actor_like` con candidatos "UNION TEMPORAL ASEO SCJ…" (HCA-META-002B). El
mismo texto en minúsculas ejecuta normal. Usuarios que escriben en mayúsculas
(frecuente en Colombia) pierden la búsqueda. Raíz: ACTOR_RESOLVER (heurística de
mayúsculas como evidencia de nombre propio sin gate de vocabulario de dominio).

**HCA-ACTOR-PENDING-001** — Candidatos pendientes secuestran una búsqueda nueva.
Tras la lista de "jorge verbel", `contratos de aseo en barranquilla en 2025`
re-imprime la lista de candidatos en lugar de ejecutar la búsqueda nueva
(HCA-ACTOR-011). Raíz: UI/SESSION_STATE (pending actor no cede ante una consulta
completa con señal fuerte de new_search).

**HCA-MONEY-CONTRA-001** — Contradicción monetaria resuelta en silencio. `por mas
de 500 millones y menos de 100 millones` → `valor_min=100M, valor_max=500M`
(swap silencioso; ejecutaría un rango que el usuario no pidió; hoy solo lo frena
casualmente la política de scope) (HCA-MONEY-021). Raíz: MONEY_PARSER (epistémica
de rango imposible).

**HCA-STATE-FAMILY-001** — "publicados" inyecta estados no publicados. `procesos
publicados…` proyecta la familia `oferta_abierta` completa
(`Publicado, Borrador, En aprobación, Aprobado, Abierto`) (HCA-STATE-005). Un
borrador no está publicado; sobre-inclusión material. Además la familia difiere por
ruta: opportunity_search con "para licitar" usa `[Publicado, Borrador, Abierto]`
(sonda probe-41) — dos universos distintos para el mismo concepto. Raíz:
DIMENSION_CLASSIFIER (estado_families) + inconsistencia interna.

### P2

**HCA-ACTOR-TYPO-001** — El typo del sentinel pierde al candidato correcto.
`lmp ingenieria` → rank 1 = LMP INGENIERIA SAS (NIT 900739049); `lmp ingieneria`
(typo real del histórico) → candidatos LMPO/ALMP/LMPM/…, el correcto **no aparece**.
Fail-safe (no adivina) pero recall roto para el caso que motivó el sentinel. Raíz:
ACTOR_RESOLVER (retrieval fts5-trigram sin tolerancia a transposición).

**HCA-ENTITY-GENERIC-001** — Genérico sin calificar cae al vertedero temático.
`contratos de la alcaldia` → `objeto=["alcaldia"]`, `LIKE '%alcaldia%'`
(HCA-ENTITY-005). La guarda anti-entidad-arbitraria funciona, pero el residuo
debió pedir aclaración, no buscar la palabra.

**HCA-ENTITY-DEPTO-001** — "gobernación del atlántico" (buyer) vs "departamento del
atlántico" (solo territorio): frames materialmente distintos para paráfrasis del
dogfood (HCA-META-001B, HCA-ENTITY-004). `que contrato el departamento del
atlantico` pierde al comprador. Raíz: ENTITY_RESOLVER (patrón territorial gana
aunque el verbo pida comprador).

**HCA-FOLLOWUP-CONBUYER-001** — `en cordoba?` tras `con cordoba?` (actor activo)
mantiene el buyer `DEPARTAMENTO DE CORDOBA` heredado además del territorio; el
usuario pidió cambiar la relación, no sumarla (HCA-ACTOR-003 t3). Sentinel
FUNSOCOM_EN_TERRITORY: parcial. Raíz: FOLLOWUP_MUTATION (switch_scope no purga
buyer de relación actor).

**HCA-MONEY-UNIT-001** — `100 mil pesos` y `1 millon` (singular) no parsean →
filtro económico silenciosamente ausente (HCA-MONEY-020/026). Raíz: MONEY_PARSER.

**HCA-ORDER-TOP-001** — `top 10 … por valor` ignora el N (LIMIT 50) y el orden por
valor (HCA-ORDER-004). Raíz: DIMENSION_CLASSIFIER (ordering) — "por valor" sin
"ordenados/mayor" no dispara señal.

**HCA-POLICY-SCOPE-001** — Parse perfecto bloqueado con mensaje genérico.
`contratos por seleccion abreviada de menor cuantia en 2026` parsea modalidad+año
correctos y responde "Necesito al menos un filtro de entidad, lugar, tema o
contratista…" sin reconocer lo ya entendido (HCA-MODALITY-002/012, HCA-TYPO-003).
Seguro pero opaco; el usuario no sabe que la modalidad sí se entendió. Raíz:
dialogue policy (UX), no parser.

**HCA-SEM-NOISE-001** — Expansión semántica con vecino no equivalente:
`alimentacion escolar` incluye OR `%simat%` (sistema de matrícula — RELATED, no
EQUIVALENT) → ruido potencial en resultados. Clasificación del par:
`simat ↔ alimentación escolar = NOT_EQUIVALENT`. Raíz: catálogo de frases.

### P3 (presentación; DATA/SUMMARY/FORMATTING separados de retrieval)

- **HCA-SUMMARY-001** — Títulos con underscore interno: "Contratos de adulto_mayor",
  "Procesos de alimentacion_escolar" (FORMATTING).
- **HCA-SUMMARY-002** — `DEPARTAMENTO DEL ATLANTICO**` mostrado tal cual.
  **Verificado en fuente:** el nombre registrado en SECOP II **incluye los
  asteriscos** (igualdad exacta con `**` devuelve filas; sin `**`, cero) → el
  retrieval es correcto (baja del P0 sospechado a P3 display: sanitizar solo la
  presentación, no el filtro) (DATA_ERROR de fuente + FORMATTING propio).
- **HCA-SUMMARY-003** — Rango temporal mal resumido: "en junio de 2026" y "este
  mes" se muestran como "desde 2026" (SUMMARY_ERROR).
- **HCA-SUMMARY-004** — "1 contratos" en la huella del perfil de actor
  (concordancia singular).
- **HCA-SUMMARY-005** — Backticks de markdown en respuestas de referencia
  (`\`SALUD-EN\``) — cruda en canales sin markdown.
- **HCA-STATE-LIQ-001** — "liquidados" se mapea a `estado_contrato='Cerrado'`;
  **verificado en fuente: no existe valor 'Liquidado'** — el proxy es defendible
  pero no se comunica al usuario (SUMMARY/epistémica menor). El pivot silencioso
  `procesos liquidados` → dataset contratos es semánticamente justificado pero
  también merece aviso (HCA-SOURCE-002).

---

## 5. Hallazgos por dominio no cubiertos arriba

- **Money (89% PASS):** unidades colombianas correctas (mil millones, `2,5 mil
  millones`, billón, COP, `$`), min/max/rango, `o más`, proyección
  `valor_del_contrato` vs `precio_base` correcta por dataset, reemplazo en
  follow-up correcto, y el fix ADR-061 (año repetido no borra valor_min) verificado
  vivo en la secuencia sentinel.
- **Modalidad:** familias e IDs canónicos correctos (incl. `NOT IN` para
  exclusiones); coexistencia modalidad+tipo+valor+año verificada; singular/plural/
  tildes OK.
- **Tipo contractual (SEARCH-SH1): 10/10.** `convenio` como COMPOSITE con tema
  intacto; interadministrativo/solidario; "quita lo de régimen especial" retira la
  modalidad conservando convenio+valor.
- **Intent/dataset:** celebrado/firmado/adjudicado→contratos y
  abierto/publicado/oportunidades→procesos consistentes (salvo los verbos del
  P1 TOPIC-VERB).
- **Follow-up (16/18):** herencia de scope/tema/fecha, purga de valor al cambiar
  de tema (guard Yarumal OK), reemplazo de comprador, pivot de dataset sin
  alucinación de fechas, reset limpio, NOOP idempotente.
- **Paginación:** el fingerprint respeta el universo refinado (estructural);
  `pagination_exhausted` correcto. Cobertura con universo >50 real no ejercitada
  (mock de 5 filas) — el buffer canónico E8B sigue pendiente según bitácora.
- **Actor Profile (índice real, vivo):** FUNSOCOM exacto → NIT 900334689 con
  huella real; candidatos con CC/NIT visibles y selección numérica funcional
  (JORGE VERBEL → selección "1" → perfil del elegido); TTL y token de selección
  presentes; reset limpia pending; composición actor+valor+año re-filtra la
  huella; degradación INDEX_UNAVAILABLE honesta ("Compárteme el NIT o la cédula y
  continúo sin adivinar"); actor+procesos rechaza con explicación de capacidad de
  fuente ("El conjunto de procesos no publica quién contrata") — INV-NO-SILENT-
  SCOPE-DROP se cumple aquí.

## 6. Sentinels históricos

| Sentinel | Estado |
|---|---|
| LMP (typo ingieneria) | **FAIL parcial** — candidatos seguros, el correcto no aparece (P2) |
| FUNSOCOM con buyer ("con cordoba") | PASS (vivo) |
| FUNSOCOM en territorio ("en cordoba") | **FAIL parcial** — buyer heredado no purgado (P2) |
| JORGE VERBEL selección texto | PASS (vivo) |
| JOSE CANTILLO + alcaldía | PASS (buyer resuelto; actor a resolución) |
| BRIANA LOPESIERRA | PASS (fail-safe) |
| Cartagena adulto mayor >1000M 2025 | PASS |
| Convenios (alcaldía Bquilla 2026) | PASS |
| Convenios régimen especial >2000M | PASS |
| Money follow-up (año repetido) | PASS |
| Paginación filtrada | PASS (estructural) |
| Selección abreviada celebradas | PASS |
| Procesos abiertos alcaldía Bquilla | PASS |
| Junio 2026 + >200M | PASS (display del período incorrecto, P3) |

## 7. Blind spots de la suite existente (por qué 8.250 tests no vieron esto)

| Clase | Casos |
|---|---|
| NO_TEST | negación geo/tema/tiempo; relativos temporales; texto íntegro en mayúsculas; "top N por valor"; contradicción monetaria |
| MISSING_E2E | gate de direct_lookup nunca se prueba con lenguaje natural (sus tests usan referencias reales); la cadena de handlers completa casi no se ejercita adversarialmente |
| MISSING_LIVE_SOURCE | ORDER-001 pasa cualquier check estructural y muere en vivo (0 filas por `%ordenados%`); solo la capa C lo revela |
| AMBIENT_STATE_NOT_REPRODUCED | pending de candidatos + búsqueda nueva; ecosistema activado por follow-up |
| WEAK_ASSERTION | corpus v1 valida presencia de campos, no ausencia de fugas a `objeto` ni el texto de títulos |
| WRONG_ORACLE (parcial) | familias de estado consagradas como correctas ("publicados" ⊃ Borrador) |

## 8. Métricas

```
SINGLE_TURN ejecutados:        169 corpus + 41 sondas = 210
MULTI_TURN_SEQUENCES:          26 (corpus) + 8 (sondas) = 34
TOTAL_TURNS:                   229 corpus + 51 sondas = 280
METAMORPHIC/ANTI PAIRS:        10
LIVE_SOURCE_VERIFIED:          21 (16 SoQL fila-a-fila + 5 catálogo) + perfiles actor vivos
VERDICTS (tras triage):        PASS 152 · SAFE_UNSUPPORTED 4 · P0 1 · P1 13 · P2 9 · P3 6
INFRA_FAILURE:                 0 · ORACLE_UNCERTAIN 4 (oráculos re-clasificados, documentados)
needs_llm:                     4/229 turnos (98% heurística determinista)
```

Los mínimos del encargo (500/120/100/…) no se alcanzaron en una sesión; el corpus
prioriza fronteras no cubiertas por los 700+ casos ya existentes en fixtures
(inventariados: query 89, dialogue e2e 29 seq, policy 91, modalidad 301, actor 41,
stress 25, rated 62).

## 9. Scores por dominio (0-100, % PASS bruto del runner; infra excluida)

INTENT_DATASET 79 · TOPIC_OBJECT 87 · STRUCTURED_DIMENSIONS (tipo) 100 ·
MONEY 89 · TIME 53 · ENTITY_BUYER 70 · GEOGRAPHY 86 · MODALITY 75(*) ·
ACTOR 86 · FOLLOWUPS 89 · PAGINATION 100(estructural) · STATE_OPPORTUNITY 50 ·
SAFE_FAILURE 90 · RESULT_CORRECTNESS 100 (live, 0 violaciones) ·
RESPONSE_SUMMARY 60 (defectos P3 sistemáticos).
(*) los FAIL de modalidad son la política de scope y el hijack, no el resolutor.

## 10. Gates de producción

| Gate | Veredicto | Condición |
|---|---|---|
| CONVERSATIONAL_CORE | **CONDITIONAL** | cerrar P0 hijack + cluster negación + cluster tiempo |
| SEARCH_CORE | **CONDITIONAL** | P0 hijack (la proyección en sí: sólida) |
| ACTOR_PROFILE | **CONDITIONAL** | uppercase-as-actor, pending-hijack, recall de typo |
| FOLLOWUP_ENGINE | **CONDITIONAL** | orden no reconocido→ecosystem; purga buyer con/en |
| MONEY | **YES** | (2 gaps menores P2 de unidades) |
| MODALITY | **YES** | — |
| PAGINATION | **CONDITIONAL** | verificar buffer con universo real >50 (E8B pendiente) |

## 11. Frentes de remediación recomendados (NO implementados)

1. **Gate de direct-lookup** — P0. Exigir dígitos o mayúsculas literales en la
   referencia; prefijos case-sensitive; no aceptar `num1` puramente alfabético en
   minúscula. Bugs: LOOKUP-HIJACK-001. Riesgo bajo, alcance ~horas, ADR no
   necesario. Tests: fuzz de frases naturales + referencias reales del backlog.
2. **Negación transversal** — P1 ×3. Poblar `objeto_exclude` desde "sin/excepto/
   que no sean/menos los de" y decidir exclusión vs clarificación para scope/tiempo.
   ADR recomendado (semántica de exclusión por dimensión).
3. **Tiempo v2** — P1 ×4. Rangos multi-mes y multi-día, día exacto, relativos
   civiles (año/mes pasado, mes sin año), "desde" abierto. Falla cerrada: si un
   marcador temporal no parsea, avisar, no callar.
4. **Consumo del tramo de control** — P1. Stopwords de verbos de intención
   (contratando, cierran, licitar, ordenados, antiguos, primero…) + clasificar
   "más antiguos primero" como change_order (nunca requery + ecosystem).
5. **Higiene de actor** — P1/P2. Gate de vocabulario de dominio antes de
   `bare_actor_like` (mayúsculas no bastan); pending cede ante new_search con
   señal fuerte; retrieval tolerante a transposición para typos.
6. **Presentación** — P3 batch. Sanitizar `**`/underscores en display, resumen de
   período fiel, plurales, sin backticks en canales planos; explicitar mapeos
   (liquidado→Cerrado) y pivots de dataset.

**Qué NO tocar:** SoQLBuilder y proyección por dataset (verificados en vivo),
money core, resolutor de modalidad, SEARCH-SH1/contract_type, flujo de selección
de candidatos, política INDEX_UNAVAILABLE, fingerprint de paginación.

## 12. Decisión de roadmap

**PAUSE_UT_FOR_P0_P1 (Opción B), acotada.** El P0 es un fix de guarda de horas y
los frentes 2-4 son el corazón del valor conversacional; avanzar Actor
Relationships/UT sobre un núcleo que invierte negaciones y descarta fechas
relativas acumularía deuda visible en dogfood. Frentes 1, 4 y 5 son quirúrgicos;
2 y 3 son los únicos con diseño real. UT puede retomarse tras 1-4.

## 13. Notas de harness y limitaciones

- El assembler de Actor Profile no pasa por `secop_client.query`, así que el mock
  no lo intercepta: hubo lecturas reales (solo GET) durante la fase estructural.
  Para un runner 100% offline hay que inyectar también esa vía.
- Persistencia verificada: `data/` de producción intacto (mtimes previos a la
  sesión); feedback/conversaciones/DB redirigidos a tmp.
- El corpus congelado no se editó tras la ejecución; 4 oráculos resultaron más
  estrechos que el comportamiento correcto (documentados en §4/§8 como
  ORACLE_UNCERTAIN) y se corrigen en una futura v1.1 versionada, no in situ.

## Apéndice — evidencia

- Corpus: `tests/fixtures/audits/conversation_holistic_v1.yaml` (sha256 arriba).
- Runner: `scripts/audits/run_conversation_holistic_audit.py` (offline, tmp-only).
- Sondas: `scripts/audits/hca1_probe.py`.
- Dumps de ejecución (no versionados, scratchpad de la sesión): `run1/results.jsonl`,
  `run1/verdicts.json`, `live_checks.json`, `probes1.jsonl`.

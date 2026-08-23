# RS0 — Semantic Authority Audit

**Fecha:** 2026-08-22

**Rama:** `feature/semantic-registry-v0`

**HEAD basal:** `35179783c2ac6516504b2e846f79d13f1cbdc87d`

**Alcance:** conocimiento semántico temático ya programado; sin cambios de runtime.

## Veredicto RS0

`RUNTIME_BUG_CONFIRMED` para la clase de diseño que mezcla morfología,
equivalencia, alias, jerarquía y asociación dentro de
`MORPHOLOGICAL_VARIANTS`. El caso literal `nutricion -> alimentacion` reproduce
esa mezcla. La frase `recuperación nutricional`, sin embargo, **no** reproduce
esa transformación en el HEAD auditado: `nutricional` no está catalogado.

El riesgo no es una sustitución simple en el SoQL final. El router pierde la
identidad explícita al convertir `nutricion` en el root `alimentacion`; luego el
builder invierte todo el grupo y abre un OR que contiene tanto nutrición como
alimentación, alimentos y comedor. Por tanto, la superficie reaparece en
retrieval, pero ya no sobrevive como núcleo explícito distinguible y la relación
asociativa adquiere autoridad de equivalencia automática.

## Preflight y baseline

El árbol inicial estaba en `main`, en el HEAD indicado arriba, con archivos no
rastreados preexistentes bajo `data/`, `docs/` y `tests/fixtures/`, además de
`Icon\r` y artefactos `.fuse_hidden*`. Se conservaron fuera del alcance.

| Gate | Comando solicitado | Resultado basal |
| --- | --- | --- |
| Rama/HEAD | `git branch --show-current`; `git rev-parse HEAD` | `main`; `35179783...` |
| Working tree | `git status --short` | dirty solo por archivos no rastreados preexistentes |
| lint | `make lint-core` | PASS — `app/core/ limpio` |
| pytest solicitado | `pytest -q` | no ejecutable en `PATH` (`command not found`) |
| pytest equivalente vigente | `.venv/bin/python -m pytest -q` | PASS — `8250 passed, 2 skipped, 24 xfailed` |
| query corpus solicitado | `python ...` | `python` no existe en `PATH` |
| query corpus equivalente | `.venv/bin/python scripts/run_query_corpus.py --mode all` | PASS — `89/89` |
| dialogue corpus equivalente | `.venv/bin/python scripts/run_dialogue_corpus.py --mode all` | deuda basal — `60/61`; falla `GOB-DEPTO-001` |

La falla basal espera `GOBERNACIÓN DE BOYACÁ` y obtiene `None`. Es ajena a este
bloque y no se corrige aquí.

## Caso semilla: causa exacta

### Traza que sí ocurre: `contratos de nutrición`

1. `QueryRouter.parse()` normaliza a texto sin acentos mediante
   `normalize_text()` (`app/core/query_router.py:401-403`).
2. `QueryRouter._extract_object_terms()` consulta `VARIANT_TO_ROOT`
   (`app/core/query_router.py:1830-1876`).
3. `app/data/morphological_variants.py:206-207` declara
   `nutricion -> alimentacion`.
4. El `ParsedQuery`/futuro frame recibe `objeto=["alimentacion"]`; la identidad
   explícita `nutricion` ya no está representada.
5. `SoQLBuilder._object_term_conditions()` consulta el índice inverso
   `ROOT_TO_VARIANTS` (`app/core/soql_builder.py:709-728`).
6. El root abre un OR, en nombre/objeto y descripción, para `comedor`,
   `nutricion`, `nutrición`, `alimentos`, `alimentacion` y `alimentación`.
7. La superficie explícita reaparece como patrón, pero mezclada con asociados;
   no queda evidencia de cuál término escribió el usuario.

### Control obligatorio: `recuperación nutricional`

La ejecución real produce `objeto=["recuperacion", "nutricional"]` y dos
predicados AND literales. No llega a `alimentacion`, porque el catálogo contiene
`nutricion`/`nutrición`, no `nutricional`. Esta distinción evita atribuir al
runtime actual una transformación inexistente.

### RS1.1 — Estado empírico congelado

```text
EMPIRICAL_CASE_STATUS: NOT_REPRODUCED_AT_25986d9

INPUT:
recuperación nutricional

CURRENT_PARSE:
objeto = ["recuperacion", "nutricional"]

LLM:
needs_llm = false
route = heuristic_only

KNOWN_RELATED_SEMANTIC_DEBT:
nutricion -> alimentacion

CAUSAL_LINK_TO_REPORTED_CASE:
NOT_ESTABLISHED
```

En el workflow canónico, `llm_parse` solo es alcanzable cuando
`needs_llm=true`; por tanto el fallback LLM queda expresamente descartado como
causa para este input en `25986d9`. El caso reportado pudo pertenecer a otra
versión, wording o ruta, pero no existe evidencia versionada para elegir una.

### Caminos runtime que consumen la relación

| Consumer | Efecto |
| --- | --- |
| `QueryRouter._extract_object_terms` | reemplaza superficies por root y reconoce frases multi-token vía `KNOWN_BIGRAMS` |
| `SoQLBuilder._object_term_conditions` | expande el root a todas las superficies como OR de `LIKE` |
| `RelevanceScorer._load_synonyms/_expand_synonyms` | trata todo el grupo como sinónimos para BM25 |
| `actor_arbitration._KNOWN_TOPIC_TERMS` | usa keys y roots como evidencia de que una mención es tema, no actor |
| `actor_conversation._KNOWN_TOPICS` | usa el catálogo, más una lista manual parcialmente duplicada, para arbitraje conversacional |

### Tests que hoy protegen el comportamiento

No existe un test que nombre `nutricion -> alimentacion` ni que congele la
traza del caso semilla. La conducta queda cubierta solo indirectamente por los
tests genéricos de parser/expansión (`tests/test_e2e_1_2.py` y
`tests/test_morpho_expansion_consistency.py`). Sí hay protecciones específicas
para PAE/alimentación escolar, cultura y adulto mayor en
`tests/test_query_router.py` y `tests/test_soql_builder.py`. Las apariciones de
`nutricional` en tests de Radar/notify son datos de fila, no tests del parser.

## A. Inventario de fuentes semánticas

| Fuente | Responsabilidad actual | Tipo de conocimiento | Runtime consumer | Riesgo |
| --- | --- | --- | --- | --- |
| `app/data/morphological_variants.py` | root de entrada, frase greedy e índice inverso de expansión | morfología + equivalencia + aliases + asociaciones + jerarquías implícitas | router, builder, BM25, arbitraje de actor | **alto**: una sola relación habilita reemplazo y expansión bidireccional |
| `app/core/query_router.py` | consume superficies y construye `objeto` | mecanismo de canonicalización temática | query pipeline | alto por pérdida del término explícito |
| `app/core/soql_builder.py` | convierte roots a predicados de retrieval | política de expansión | toda consulta SECOP | alto: cualquier miembro recupera todo el grupo |
| `app/core/relevance.py` | expansión llamada “synonyms” para BM25 | equivalencia implícita heredada | ranking sin frase | medio/alto: replica la fuerza del catálogo |
| `app/data/polysemic_topics.json` + `app/core/polysemic.py` | sentidos, marcadores y variantes; aclara o auto-resuelve con dos marcadores | polisemia y alternativas de retrieval por sentido | router/workflow donde se invoque | medio: `object_variants` mezcla evidencia de sentido y expansión |
| `app/core/phrase_catalog.py` + `phrase_spec.py` | preserva colocaciones con anclas/variantes | identidad léxica de frases | matching, recall-first y ranking | bajo; autoridad separada correcta |
| `app/core/phrase_match.py`, `phrase_validator.py`, `phrase_ranking.py` | matching, evidencia de fila, BM25/orden | mecanismo, no relación conceptual | retrieval post-query | bajo; no migrar |
| `app/data/estados.py` + `app/core/estado_families.py` | lenguaje natural a valores/familias SECOP | semántica jurídica/lifecycle y proyección de fuente | router y builder | alto si se mezcla; mantener autoridad estructurada separada |
| `app/core/contract_type_resolver.py` + `contract_type_projection.py` | naturaleza contractual canónica y proyección por dataset | clases contractuales con evidencia estructurada/composite | router y builder | bajo/medio; autoridad especializada vigente |
| `app/data/modality_catalog_v1.yaml`, `modality_lexicon_v1.yaml` | modalidad legal, aliases y calificadores | catálogo jurídico tipado | resolver/composición/proyección | bajo; autoridad especializada vigente |
| `app/core/llm_handler.py` | fallback y promoción heurística de dos frases a `objeto_or` | “sinonimia” no catalogada inferida por solape | fallback LLM | alto: relación dinámica no auditable; REVIEW_REQUIRED |
| `app/core/actor_conversation.py`, `actor_arbitration.py` | diferenciar tema de identidad de actor | lista de indicios temáticos | Actor Profile | medio: consumidor secundario y duplicación parcial, no autoridad conceptual |
| `app/data/aliases_db.json`, actor alias index y resolvers | nombres observados a identidad institucional/actor | entity resolution e identidad | buyer/actor pipelines | alto si se mezcla; explícitamente fuera del registro temático |
| `app/core/semantic_sanity.py` | flags pasivos sin mutar params | juicio de riesgo pre-ejecución | QueryPlan P14 | bajo; no es catálogo semántico |
| `app/core/semantic_dimensions.py` | nombres de dimensiones consumidas | vocabulario estructural, no ontología | resolvers y follow-ups | bajo; no migrar |
| `app/core/intent_vocabulary.py` | acciones/control e intención del usuario | gramática operativa | router | bajo/medio; no es relación de conceptos |
| `app/data/departamentos.py`, geography y entity type rewrites | aliases geográficos/de entidad | scope e identidad | entity resolver/router | separado; no migrar |

## B. Relaciones encontradas

La tabla agrupa únicamente entradas homogéneas del catálogo existente. Las
formas separadas por comas son relaciones individuales con la misma
clasificación. Los roots autorreferentes son identidad técnica y se omiten salvo
que aporten evidencia; acentos idénticos se consideran normalización léxica.

| Source | Surface | Current canonical/root | Tipo actual implícito | Tipo recomendado | Riesgo | Acción |
| --- | --- | --- | --- | --- | --- | --- |
| morph | plurales/flexiones de mantenimiento, dotación, construcción, adecuación, suministro, consultoría, interventoría, rehabilitación, reparación, señalización, vigilancia, acueducto, alcantarillado, capacitación, transporte, carpa | singular correspondiente | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | `mantenimiento correctivo`, `mantenimiento preventivo` y coordinaciones | `mantenimiento` | equivalencia total | NARROWER | medio | MOVE_TO_REGISTRY_LATER |
| morph | `parque(s) automotor(es)` | `parque_automotor` | equivalencia/slug | EXACT_EQUIVALENT + MORPHOLOGICAL | bajo | KEEP |
| morph | `vía(s)` | `vial` | equivalencia total | UNKNOWN | medio | REVIEW_REQUIRED |
| morph | `malla vial`, `infraestructura vial` | `vial` | equivalencia total | ASSOCIATED_WITH | medio | MOVE_TO_REGISTRY_LATER |
| morph | `pavimento(s)` | `pavimentacion` | equivalencia total | ASSOCIATED_WITH | medio | MOVE_TO_REGISTRY_LATER |
| morph | `limpiar` | `limpieza` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | `aseo urbano`, `aseos` | `aseo` | equivalencia total | MORPHOLOGICAL / NARROWER | bajo/medio | REVIEW_REQUIRED |
| morph | `PAE`, `programa de alimentacion escolar` | `alimentacion_escolar` | equivalencia total | PROGRAM_ALIAS | bajo | MOVE_TO_REGISTRY_LATER |
| morph | `alimentacion escolar` | `alimentacion_escolar` | equivalencia/slug | EXACT_EQUIVALENT | bajo | MOVE_TO_REGISTRY_LATER |
| morph | `complemento alimentario`, `restaurante/comedor escolar`, `refrigerios escolares` | `alimentacion_escolar` | equivalencia total | ASSOCIATED_WITH | medio | MOVE_TO_REGISTRY_LATER |
| morph | `SIMAT` | `alimentacion_escolar` | equivalencia total | UNKNOWN | **alto** | RUNTIME_BUG_CONFIRMED |
| morph | `PAEF`, `ICBF`, `SGP`, `OCAD`, `PDET`, `SAT` y sus nombres desarrollados | root acrónimo | equivalencia total | PROGRAM_ALIAS / ALIAS | bajo/medio | MOVE_TO_REGISTRY_LATER |
| morph | flexiones `cultural(es)` | `cultura` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | flexiones `artistico/a/os/as` | `cultura` | equivalencia total | ASSOCIATED_WITH | alto | RUNTIME_BUG_CONFIRMED |
| morph | `patrimonio/eventos/actividades culturales`, `actividades artisticas` | `cultura` | equivalencia total | NARROWER / ASSOCIATED_WITH | medio | MOVE_TO_REGISTRY_LATER |
| morph | `ferias`, `fiestas`, `festivales` | `cultura` | equivalencia total | ASSOCIATED_WITH | alto | REVIEW_REQUIRED |
| morph | `centro de vida` | `centros de vida` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | flexiones de `trampa(s) de grasa(s)` | `trampas de grasas` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | `adulto(s) mayor(es)` | `adulto_mayor` | equivalencia/slug | MORPHOLOGICAL | bajo | KEEP |
| morph | `persona(s) mayor(es)` | `adulto_mayor` | equivalencia total | EXACT_EQUIVALENT | medio | MOVE_TO_REGISTRY_LATER |
| morph | `PTAR`, `STAR`, nombre completo singular/plural | `tratamiento_aguas_residuales` | equivalencia total | ALIAS / EXACT_EQUIVALENT | bajo/medio | MOVE_TO_REGISTRY_LATER |
| morph | `aguas residuales` | `tratamiento_aguas_residuales` | equivalencia total | UNKNOWN | alto | REVIEW_REQUIRED |
| morph | flexiones `turistico/a/os/as` | `turismo` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | `hoteleria`, `promocion turistica` | `turismo` | equivalencia total | ASSOCIATED_WITH | medio | MOVE_TO_REGISTRY_LATER |
| morph | `equipamiento(s)` | `equipo` | equivalencia total | EXACT_EQUIVALENT | bajo | KEEP |
| morph | `dotacion de equipos` | `equipo` | equivalencia total | COMPOSED_WITH | medio | MOVE_TO_REGISTRY_LATER |
| morph | `medico(s)`, `hospitalario(s)`, `hospital`, `sanidad` | `salud` | equivalencia total | ASSOCIATED_WITH / UNKNOWN | **alto** | RUNTIME_BUG_CONFIRMED |
| morph | flexiones `educativo(s)` | `educacion` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | `colegio(s)`, `escuela(s)` | `educacion` | equivalencia total | ASSOCIATED_WITH | alto | REVIEW_REQUIRED |
| morph | `alimentos`, `comedor` | `alimentacion` | equivalencia total | ASSOCIATED_WITH | medio/alto | MOVE_TO_REGISTRY_LATER |
| morph | `nutricion` | `alimentacion` | equivalencia total | ASSOCIATED_WITH | **alto** | RUNTIME_BUG_CONFIRMED |
| morph | `vigilante(s)`, `guardia(s)`, `proteccion` | `seguridad` | equivalencia total | ASSOCIATED_WITH / CONTRACT_FUNCTION | medio | REVIEW_REQUIRED |
| morph | flexiones `tecnologico(s)` | `tecnologia` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | `informatica` | `tecnologia` | equivalencia total | EXACT_EQUIVALENT | medio | MOVE_TO_REGISTRY_LATER |
| morph | `sistemas`, `computo` | `tecnologia` | equivalencia total | ASSOCIATED_WITH | alto | REVIEW_REQUIRED |
| morph | flexiones `agricola(s)` | `agricultura` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | `agro`, `agropecuario`, `cultivo(s)` | `agricultura` | equivalencia total | ASSOCIATED_WITH / UNKNOWN | medio | REVIEW_REQUIRED |
| morph | `medio ambiente` | `ambiente` | equivalencia total | EXACT_EQUIVALENT | bajo | KEEP |
| morph | `ambiental(es)` | `ambiente` | equivalencia total | MORPHOLOGICAL | bajo | KEEP |
| morph | `ecologico`, `sostenible` | `ambiente` | equivalencia total | ASSOCIATED_WITH / QUALIFIER | alto | REVIEW_REQUIRED |
| morph | `habitacional` | `vivienda` | equivalencia total | QUALIFIER | medio | MOVE_TO_REGISTRY_LATER |
| morph | `residencial`, `hogar(es)`, `apartamento(s)`, `casa(s)` | `vivienda` | equivalencia total | ASSOCIATED_WITH / NARROWER / UNKNOWN | **alto** | REVIEW_REQUIRED |
| morph | `infraestructura/planta fisica`, `edificio(s)` | `sede` | equivalencia total | ASSOCIATED_WITH / UNKNOWN | **alto** | RUNTIME_BUG_CONFIRMED |
| morph | `dotacion textil` | `uniforme` | equivalencia total | COMPOSED_WITH / ASSOCIATED_WITH | medio | REVIEW_REQUIRED |
| morph | `materia(s) prima(s)` | `insumo` | equivalencia total | NARROWER | medio | MOVE_TO_REGISTRY_LATER |
| morph | `farmaco(s)` | `medicamento` | equivalencia total | EXACT_EQUIVALENT | bajo | KEEP |
| morph | `analisis`, `examenes` | `laboratorio` | equivalencia total | CONTRACT_FUNCTION / ASSOCIATED_WITH | alto | REVIEW_REQUIRED |
| morph | frase plural/singular de planta de tratamiento | `tratamiento_aguas_residuales` | equivalencia total | NARROWER | medio | MOVE_TO_REGISTRY_LATER |
| morph | `dotacion textil`, `dotacion de equipos` | `uniforme`, `equipo` | equivalencia total | COMPOSED_WITH | medio | MOVE_TO_REGISTRY_LATER |
| polysemic | marcadores y `object_variants` de alojamiento, transporte, servicios, soporte y dotación | sense ID local | asociación contextual | ASSOCIATED_WITH / QUALIFIER | medio | REVIEW_REQUIRED |
| phrase catalog | plurales de `manual de contratacion`, `aire acondicionado` | canonical de frase | variante de superficie | MORPHOLOGICAL | bajo | NOT_SEMANTIC |
| states/families | lenguaje de estado a valor/familia SECOP | estado/familia estructurada | alias de fuente/lifecycle | ALIAS | medio | NOT_SEMANTIC |
| contract type resolver | superficies contractuales a canonical IDs | naturaleza contractual | clase estructurada | ALIAS / NARROWER | bajo/medio | NOT_SEMANTIC |
| modality catalogs | superficies y calificadores a procedure IDs | modalidad jurídica | alias/qualifier legal | ALIAS / QUALIFIER | bajo/medio | NOT_SEMANTIC |
| LLM handler | dos frases multi-token con un token común | `objeto_or` | equivalencia inferida | UNKNOWN | alto | REVIEW_REQUIRED |

`interventoria -> obra` **no existe** en los catálogos auditados. El catálogo
solo contiene la flexión `interventorias -> interventoria`; por tanto no se
inventó una relación `CONTRACT_FUNCTION` con obra.

## C. Casos peligrosos

| Caso | Evidencia runtime | Clasificación conservadora | Riesgo |
| --- | --- | --- | --- |
| `nutricion -> alimentacion` | router emite `alimentacion`; builder abre ambos grupos | ASSOCIATED_WITH | pierde identidad en params y amplía a alimento/comedor |
| `hospital -> salud` | router emite `salud`; builder incluye salud, médico, sanidad y hospital | UNKNOWN/ASSOCIATED_WITH | “hospital” puede ser instalación, entidad o ámbito; no equivale a todo salud |
| `hogar -> vivienda` | router emite `vivienda`; builder incluye casa, apartamento, residencial | UNKNOWN | “hogar” también aparece en servicios/programas y nombres de entidades |
| `edificio -> sede` | router emite `sede`; builder incluye planta/infraestructura física | UNKNOWN | un edificio no siempre es sede y una sede no siempre es edificio |
| `artistico -> cultura` | router emite `cultura`; builder incluye ferias, fiestas y festivales | ASSOCIATED_WITH | abre un dominio mucho mayor que la expresión artística explícita |
| `SIMAT -> alimentacion_escolar` | router emite el root; builder abre PAE/comedor/refrigerio | UNKNOWN, REVIEW_REQUIRED | SIMAT es una señal contextual, no evidencia suficiente de alimentación escolar |

Todos sobreviven como algún patrón en el SoQL por la inversión del grupo, pero
ninguno conserva autoridad separada como término explícito. Ese comportamiento
es precisamente la equivalencia implícita que debe desagregarse en un bloque
posterior de wiring.

## D. Qué NO debe migrarse al registro semántico

| Responsabilidad | Decisión | Autoridad que permanece |
| --- | --- | --- |
| `PhraseSpec` | no migrar | contrato de colocación, anchors, gaps y variantes de superficie |
| phrase matching/validation | no migrar | `phrase_match.py` / `phrase_validator.py` |
| phrase ranking | no migrar | `phrase_ranking.py` |
| BM25 | no migrar | `relevance.py`; un bloque futuro podrá consumir decisiones ya tipadas, no poseerlas |
| order semantics | no migrar | `order_semantics.py` y pipeline de ordering |
| query semantic sanity | no migrar | flags pasivos de `semantic_sanity.py` |
| follow-up semantics | no migrar | álgebra de `followup_*` y dimensiones estructuradas |
| money | no migrar | `money_resolution.py`, catálogo SMMLV y ADR-059 |
| entity resolution | no migrar | gazetteers y `entity_resolver.py` |
| actor identity | no migrar | actor alias index/resolver y `IdentityKey` |
| estados/lifecycle | no migrar | `estados.py`, `estado_families.py` y contratos jurídicos vigentes |
| modalidad | no migrar | catálogos/resolver/proyección de modalidad |
| naturaleza contractual | no migrar | resolver/proyección por dataset |
| geography | no migrar | catálogos de departamento/ciudad/región |

## Cierre RS0

El problema confirmado no exige cambiar hoy el parser ni el builder. Exige que
el conocimiento futuro deje de entrar como una bolsa de “variantes” que, por
diseño, equivale, expande y rankea en ambas direcciones. RS1 puede fijar el
contrato de datos y sus prohibiciones en modo dormante. La migración y el
wiring quedan explícitamente como `RUNTIME_WIRING_REQUIRED_LATER`.

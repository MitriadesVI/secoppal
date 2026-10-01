# Roadmap — de la puerta de entrada a un solo intérprete (Foundation V3)

Fecha: 2026-09-30. Origen: misión `FOUNDATION_ENTRY_ADMISSION_V1`, Paso 0 medido sobre 585
consultas reales, y discusión con el asesor. Estado de ejecución al final del documento.

## 1. Por qué existe este plan

La pregunta inicial era «¿la puerta de entrada decide bien?». La medición respondió otra cosa:

| Hallazgo (Paso 0, diagnóstico, no Gold) | Cifra |
|---|---|
| Búsquedas vetadas por el dueño ACTOR | 2/318 (<1 %), causadas por un bug de vocabulario legacy |
| Búsquedas rechazadas por la regex léxica | 88/318 (52 legítimas: sin sustantivo de universo) |
| Historial | `eligible_first_search` exige cero historial y no hay TTL: en Telegram/WhatsApp Foundation sólo ve el primer mensaje |
| Admisión sin capacidad | 14/14 «más caros» y ~55/228 búsquedas admitidas llevan ciudad, mes/rango o región que el host Foundation no resuelve; Foundation no devuelve el turno |

Conclusión: el problema no es que el portero sea poco inteligente; es que **hace falta un portero
porque varios intérpretes compiten por el mismo texto** (QueryRouter, detectores especializados,
Natural Alpha, Foundation, y el sidecar de polaridad de Suppliers). La v0.3 de la fundación de
interpretación ya lo había contado: 4–8 escritores por dimensión y ≥7 capas que releen el texto.

**El premio no es un portero excelente: es poder despedirlo.**

## 2. Arquitectura destino

```
 texto ──────────────┐                      evento UI (botón, opción, token)
                     ▼                                   │
 1 PROTOCOLO    reset · paginación · ID SECOP · selección  (0 LLM, determinista)
 2 INTÉRPRETE   Foundation: texto + estado → propuesta
                (operación · objetivo · aspecto · condiciones · spans)
 3 HOST         autoridades por dimensión → ACCEPTED IR ◄─┘ (el evento UI entra aquí)
 4 CAPACIDAD    registro de manifiestos: ¿puedo ejecutar OP con esta IR? → plan común
 5 EJECUCIÓN    guard → recibos → SECOP
 6 PRESENTACIÓN tarjetas · eco de filtros aplicados · acciones disponibles
 7 ESTADO       Conversation IR: IR aceptada + referentes (resultado, perfil, lista pendiente)
 ════ LEDGER: una fila por etapa · replay por trace_id · invariantes · shadow legacy ════
```

Es un **compilador de lenguaje natural con type-checking**, no «LLM → SQL»:
Foundation interpreta intención; las autoridades deterministas deciden valores; las capacidades
ejecutan operaciones.

### Invariantes

1. **Una interpretación semántica autoritativa por turno.** No es «una llamada LLM»: puede haber
   cero (protocolo) o inferencias auxiliares, pero nunca dos interpretaciones que gobiernen lo
   ejecutable.
2. **Monotonía de lo auxiliar:** toda inferencia auxiliar sólo puede retirar autoridad (llevar a
   aclarar), nunca concederla ni cambiar el significado. El sidecar de polaridad de Suppliers cabe.
3. **El LLM nunca entrega valores ejecutables:** propone estructura citando spans; el host resuelve.
4. **Ningún fallback silencioso** (ADR-069): lo que el host no puede hospedar se aclara o se dice.
5. **Un dueño por capacidad en cada momento; la transferencia legacy → Foundation es de un solo
   sentido y se acompaña de borrado**, nunca de fallback.
6. **Nadie relee el texto crudo después del intérprete.**

### Qué muere y qué sobrevive del legado

| Muere (interpretación) | Sobrevive (autoridades de dominio / ejecutores) |
|---|---|
| `QueryRouter.parse` y sus regex | SoQLBuilder, guard, recibos, transporte |
| Detectores de dueño (`detect_entity_profile_turn`, `REGISTRY.detect`, lectura de texto en CA) | EntityResolver, directorio de actores, DIVIPOLA, catálogos de modalidad/tipo |
| `followup_engine` (fusiones), reglas de texto de `compute_query_plan`, `degrade_query` | Autoridad temporal (ADR-065), money, política de universo (O1B, H7, ADR-100) |
| Puerta `eligible_first_search` / `decline_reason` | Ejecutores: Actor Profile, Entity Profile, Suppliers (A2), Contract Activity, Radar, export |

### Capacidades como manifiesto

Las capacidades actuales (Contract Activity, Suppliers) ya siguen el patrón propone → acredita →
contrato tipado → ejecuta; migrarlas cambia quién propone y de dónde salen los insumos, no el
ejecutor. Cada capacidad declara un manifiesto:

```yaml
capability: entity_profile.suppliers
operation: ANALYZE
aspect: SUPPLIERS
target: [BUYER, REF(profile)]
dimensions: {required: [BUYER], allowed: [TIME, CARDINALITY, VALUE], forbidden: [ACTOR]}
preconditions: [buyer_resolved, single_buyer]
ui_actions: [suppliers]
executor: entity_profile.aspects.suppliers
maturity: DOGFOOD            # DECLARED → SHADOW → DOGFOOD → GA (en GA se borra lo legacy)
legacy_to_delete: [...]
gold: tests/gold/capabilities/suppliers.yaml
```

Del manifiesto se derivan: sección de operaciones del prompt (con SHA congelado y revalidación del
held-out), validación de dimensiones por operación en el host, fila de la matriz de paridad,
andamiaje de tests, catálogo de botones y estado de madurez (reemplaza los 17 flags `SECOPPAL_*`).
Acciones de Entity Profile: **dos puertas, una IR** — «¿y sus proveedores?» (intérprete →
`ANALYZE·REF(perfil)·SUPPLIERS`) y el botón «Proveedores» (evento → la misma IR) llegan al mismo
ejecutor.

### Rastreo de bugs y flujos

- **Ledger de turno** con etapas fijas y códigos cerrados (v1 entregado en M0).
- **Replay determinista**: la salida del LLM es lo único no determinista y queda guardada;
  `replay <trace_id>` reproduce el resto con instante de referencia y fuente congelada.
- **Triage por etapa**: la primera etapa que diverge del Gold es la culpable.
- **Invariantes de frontera**: cobertura de spans, conformidad IR→plan (TRACE_CONFORMANCE), eco de
  filtros en la respuesta.
- **Shadow diferencial legacy** (entregado en M0): instrumento de paridad que se borra en M4.
- GLiNER queda fuera del runtime; a lo sumo, instrumento de desacuerdo en shadow.

## 3. Roadmap paso a paso

### Paso 0 — Integrar la publicación en vivo de Foundation V3

- Qué: ZEUS fresco sobre `fix/foundation-v3-live-publication-r1` (tarjetas, Excel, traza en
  Streamlit; hasta `8e0b9ca3`) y merge ff-only a `main`. La promoción R1 ya está en `main`
  (`efb0dfc2`).
- Salida: Foundation muestra tarjetas reales en dogfood.

### M0 — Instrumentación y desbloqueo mínimo (candidato listo, ver §5)

- L1: corpus y harness de diagnóstico (corpus con datos personales fuera de git).
- L2: `_NON_ACTOR_SEARCH_LEADS` con singular/plural/artículo; corrige también la respuesta legacy.
- L3: ledger de turno v1 con etapa `ADMISSION` (por qué Foundation tomó o no cada turno).
- L4: shadow diferencial legacy, integrado como etapa `SHADOW`.
- Prohibido en M0: ampliar regex o historial, fallback a legacy, subsistema semántico de admisión.
- Salida: ZEUS fresco (L2 cambia la propiedad de turnos en legacy) y merge.

### M1 — Paridad de SEARCH (el bloque grande)

1. **Gold**: segundo etiquetador independiente sobre el corpus real + slice no-dogfood + adversarial;
   acuerdo medido; paquete sellado. El corpus del Paso 0 es diagnóstico, no Gold.
2. **Matriz de tres columnas GOLD / LEGACY / FOUNDATION** por dimensión y clase, generada desde el
   corpus (no a mano). «LEGACY ✓» significa «tiene código», no «acierta»: paridad = cero
   regresiones exclusivas frente a Gold; donde legacy falla, Foundation acierta o aclara.
3. **Filas a cerrar** (cada una, misión pequeña con mutantes): municipio (DIVIPOLA), mes/rango
   (autoridad temporal), orden por valor, dimensión ACTOR (`resolve_actor_params`), resolución de
   compradores (gobernaciones, ministerios, universidades, SENA), operadores inclusivos, OR/NOT,
   regiones, sustantivos de universo (convocatoria, convenio, selección abreviada).
4. **Diferencias deliberadas** listadas como decisiones de producto (p. ej. aclarar «al menos»).
5. Al cerrar la matriz: **liberar el historial** (búsqueda nueva completa con historial → Foundation,
   usando la política de follow-up existente), **borrar la puerta** y **borrar la interpretación
   SEARCH de legacy**. Sin fallback.

Prioridad por tráfico real (585): búsqueda 58 %, actor 19 %, follow-ups 12 %, protocolo 3 % (se
queda), proveedores 2 %, perfil de entidad 2 %, conteos <1 %.

### M2 — Conversation IR (el frente duro; METIS)

- `AcceptedInterpretation anterior + delta semántico → AcceptedInterpretation nueva`.
  «y en 2025?» es `REFINE · REPLACE TIME 2026→2025`, no una búsqueda.
- Referentes en estado: resultado, perfil, lista pendiente (habilita «sus proveedores», «ver más»,
  exportar sobre resultados Foundation).
- **Debe absorber el Frame de Entity Profile** (`app/core/entity_profile/`, 56 archivos con su
  propio estado y follow-ups); un segundo modelo de estado reproduce el problema de autoridad.
- Gold de follow-ups antes del código. Muere `followup_engine`.

### M3 — Operaciones y registro por manifiestos

- Natural Alpha se funde en el intérprete (sus jobs = vocabulario de operaciones).
- `PROFILE_BUYER`, `PROFILE_ACTOR`, `RANK_SUPPLIERS`, `COUNT/GROUP`, `FOLLOW`, `EXPORT`, `PAGINATE`.
- Las capacidades dejan de preguntar «¿el texto parece mío?» y responden «¿puedo ejecutar
  Operation.X con esta IR?». Registro indexado por operación admitida; flags → madurez.
- Mueren los detectores de dueño y las gramáticas locales de enrutamiento.

### M4 — Funeral de `QueryRouter`

Sólo cuando M1–M3 cierren. Decidir antes el modo ante caída del proveedor.

Cada paso cierra con: corpus Gold sin regresiones exclusivas por clase, suite global pareada por
nodeid, semgrep delta con `--base`, candidate freeze y ZEUS fresco (implementador ≠ auditor).

## 4. Decisiones pendientes (producto)

1. Modo ante caída del proveedor sin legacy: «no puedo interpretar ahora» vs modo degradado
   rotulado mínimo.
2. Tema desnudo sin sustantivo de universo («pavimentaciones en Córdoba»): ¿universo por defecto?
3. Legacy convierte «licitaciones» en modalidad licitación pública, contra DA-2bis («licitación»
   coloquial no es modalidad); Foundation ya sigue O6. Resolver en la matriz de M1.
4. Operadores inclusivos: aclarar (Foundation hoy) vs proyectar `>=`/`<=`.

## 5. Estado de ejecución

| Paso | Estado | Rama / commit |
|---|---|---|
| Paso 0 publicación en vivo | Esperando ZEUS | `fix/foundation-v3-live-publication-r1` @ `8e0b9ca3` |
| M0 | Candidato congelado, global pareado limpio; **pendiente ZEUS fresco** | `mission/foundation-entry-admission-m0` · código `e69f0c1f` · cierre documental: este commit (ver `git log`) · evidencia `docs/audits/foundation_entry_admission_m0/CANDIDATE.md` |
| M1 Fase A | **Cerrada** (Gold 999 sellado, autoridades, brechas, fronteras, failover spec); plan de olas en `docs/audits/m1_search_foundation/PHASE_A_CLOSE.md` | `mission/m1-search-foundation-phase-a` |
| M1-H0 | Fail-closed BUYER multi-dueño + TIME, en curso | `fix/m1-h0-dogfood-fail-closed` |
| M2–M4 | No iniciados | — |

Deudas encontradas en el camino (fuera de M0):

- `EntityResolver.resolve_entidad("INVIAS")` devuelve `ANI` con método `exact` (comprador
  equivocado en silencio).
- Léxicos duplicados de sustantivos de universo en ≥4 módulos (`foundation_basic_fallback`,
  `foundation_universe_policy`, `query_router`, `source_occurrence`) + el de `actor_conversation`.
- Rojo preexistente `tests/test_actor_profile_render.py::test_a_specific_aggregate_question_still_gets_its_specific_answer`
  (fixture sin `date_extract_y`).
- Las 9 consultas de actor sin marcador («contratos de codetec en 2026») son un punto ciego
  compartido por legacy y Foundation: se cierra con la dimensión ACTOR de M1.

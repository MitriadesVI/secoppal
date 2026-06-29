# ADR-042: Perfil integral de proponente/contratista (dossier transversal)

**Estado:** Proposed
**Fecha:** 2026-06-29
**Deciders:** rodrigo (owner) + pipeline SECOPPAL
**Alcance:** feature transversal — WhatsApp + Streamlit, sobre el pipeline channel-agnostic existente.

---

## Contexto

El usuario quiere preguntar, en lenguaje natural o por identificador, por un
**proponente/contratista** y recibir una **revisión integral**: sus contratos (con
links), si tiene **responsabilidad fiscal**, **inhabilidades/antecedentes
disciplinarios**, su estado en **registros** (RUP/RUES), y la **composición de las
uniones temporales/consorcios** en que participa.

Variantes de consulta a soportar (no exhaustivo):
- "¿qué hay de la entidad FUNSOCOM?" (nombre → identidad → dossier)
- "¿qué ha celebrado la persona 1140835135 en el 2025?" (cédula → contratos filtrados por año, con links)
- "¿la UT X quién la conforma?" (grupo → integrantes)
- "¿el representante legal de <empresa> tiene inhabilidades?" (cruce NIT empresa → cédula RL → SIRI)

### Insumos de datos confirmados (5 fuentes, todas Socrata / datos.gov.co)

| Fuente | Rol | Llave | Formato real de la llave | Frescura |
|---|---|---|---|---|
| RUP Proveedores registrados | identidad + contacto + **representante legal** | `nit` | `900264685` limpio, **sin DV** | — |
| RUES (entidades / ESAL) | identidad registral + vigencia | `nit` + `digito_verificacion` (separados) | NIT y DV en columnas distintas | — |
| Responsabilidad fiscal (SIREF / Boletín RF) | sanciones fiscales | `n_mero_de_identificaci_n` | `8,600,345,941` — **comas Y dígito de verificación** | — |
| Antecedentes disciplinarios SIRI (Procuraduría) | inhabilidades / sanciones disciplinarias | `numero_identificacion` (cédula) | `7534386` limpio; trae `DURACION_*` + `FECHA_EFECTOS_JURIDICOS` | — |
| Grupos de Proveedores - SECOP II | UT / consorcios + **integrantes** | `nit_grupo` / `nit_participante` | **una fila por (grupo, participante)**: trae `nit_participante`, `participacion` (%), `es_lider_del_grupo` | diaria |

`SecopClient.query(dataset_id, soql)` ya es **genérico por dataset** — lo único
atado a SECOP es `_normalize_row` (reparación de URL + campos de dinero). Las 5
fuentes son consultables en vivo con el mismo cliente apuntando a su dominio.

---

## Hallazgo de datos (manda el dato, no la expectativa)

La pregunta original ("¿vectores/parquet o fuzzy?") era la pregunta equivocada
para el grueso del trabajo. El problema dominante **no** es encontrar nombres —
es **conciliar la misma identidad escrita distinto entre fuentes**:

- fiscal: `8,600,345,941` (comas + DV) → NIT canónico `860034594`
- RUP: `900264685` (limpio, sin DV)
- RUES: NIT y DV en columnas separadas
- SIRI: cédula limpia `7534386`

Un JOIN ingenuo **silenciosamente** omite sanciones (falso "limpio") o señala a la
persona equivocada (falso "inhabilitado"). En este dominio eso no es un bug: es un
riesgo legal (aseveración difamatoria). Por eso **la normalización de identidad es
la columna vertebral de la feature**, no un detalle de preprocesamiento.

Segundo hallazgo: `tipo_empresa` en RUP viene sucio (se observó una persona
natural con cédula marcada `UNIÓN TEMPORAL`). No es fuente de verdad para
clasificar grupos; eso lo da el dataset **Grupos de Proveedores**.

---

## Decisión

Construir un **perfil/dossier de contratista** como agregador transversal sobre el
pipeline existente, organizado en **1 spine + 3 capas**:

- **Spine — `app/core/identity.py`**: normaliza cualquier identificador a forma
  canónica (quita comas/puntos, separa/alinea DV, distingue cédula vs NIT,
  conserva el crudo para mostrar). Todo lo demás se cuelga de aquí.
- **Capa A — Resolución por nombre** (`nombre → nit/cédula`): **SQLite FTS5 local**
  sobre RUP + RUES. Aquí (y solo aquí) vive el fuzzy. Detección de identificador
  desnudo (cédula/NIT) extiende el patrón de `direct_lookup`.
- **Capa B — Enriquecimiento** (por identidad exacta, **en vivo**): fiscal, SIRI,
  RUES, Grupos. Un módulo pequeño por fuente (política "no monolitos").
- **Capa C — Contratos** (pipeline SECOP existente): filtra por
  `contractor_document`/`contractor`, con links y filtro temporal.

El **agregador `dossier`** orquesta A → B + C y renderiza por el `formatter`
existente, con lo que WhatsApp y Streamlit salen sin trabajo extra de canal.

### Estrategia de origen (frescura vs latencia)

- **Local (índice):** SOLO el índice de nombres (RUP + RUES) en FTS5, reconstruido
  periódicamente. Es el único componente que justifica copia local.
- **Vivo (Socrata):** enriquecimiento fiscal/SIRI/RUES/Grupos/contratos por
  identidad exacta. La responsabilidad legal exige el dato más fresco; el lookup
  exacto por NIT/cédula es barato y acotable por el `deadline` existente.

---

## Opciones consideradas (resolución por nombre)

### Opción A — Embeddings / búsqueda vectorial
| Dimensión | Evaluación |
|---|---|
| Complejidad | Alta (infra de índice vectorial nueva) |
| Costo | Alto (cómputo de embeddings + almacenamiento) |
| Encaje | Malo — la consulta NO es semántica; es match de nombre con tolerancia a tipos/orden |

**Contras:** sobreingeniería; introduce un subsistema ajeno al stack actual.

### Opción B — LIKE en vivo sobre Socrata por nombre
| Dimensión | Evaluación |
|---|---|
| Complejidad | Baja |
| Latencia | Mala — LIKE sin índice sobre millones de proveedores |
| Calidad | Mala — sin tolerancia a tipos, ranking pobre, ambigüedad alta |

**Contras:** lento e impreciso; choca con el presupuesto de latencia (Etapa 6).

### Opción C — FTS5 local para nombre→identidad + enriquecimiento vivo (ELEGIDA)
| Dimensión | Evaluación |
|---|---|
| Complejidad | Media — FTS5 encaja con stores SQLite ya presentes (conversation_store, feedback_sqlite) |
| Latencia | Buena — nombre resuelve local; enriquecimiento es lookup exacto |
| Frescura | Buena — sanciones/inhabilidades siempre en vivo |

**Pros:** separa cleanly el fuzzy (nombres, tolerante) del exacto (identidad, legal);
trigram/FTS cubre tipos y orden de palabras a fracción del costo de vectores.
**Contras:** índice local requiere rebuild periódico (aceptable: los nombres/NIT
cambian poco; las sanciones —que sí cambian— van en vivo).

---

## Plan por etapas

> Estilo Sprint: cada etapa cierra con tests e invariantes; no se declara la
> feature cerrada hasta E7.

- **E1 — Spine de identidad** (`identity.py`)
  Normalización canónica con los formatos reales de las 5 fuentes. Cédula vs NIT;
  comas/puntos; DV (separar y alinear). **INV-ID:** la misma identidad real produce
  la misma forma canónica desde cualquiera de las 5 fuentes (test con los crudos de
  este ADR).

- **E2 — Resolución de identidad**
  (a) Índice FTS5 sobre RUP + RUES (`nombre`, `razon_social`, `sigla`). Script de
  build en `scripts/`. (b) Detección de identificador desnudo (cédula/NIT) que
  extiende `direct_lookup`. **INV-AMB:** nunca aseverar sanción/inhabilidad sin
  documento exacto; nombre ambiguo → desambiguar antes.

- **E3 — Enriquecimiento por identidad** (vivo, un módulo por fuente)
  `fiscal.py` (por NIT), `inhabilidades.py` (SIRI por cédula), `registro.py` (RUES
  vigencia). **INV-VIG:** SIRI reporta **vigencia calculada** (ventana activa desde
  `FECHA_EFECTOS_JURIDICOS` + `DURACION_*`), no mera presencia de fila. **INV-RL:**
  el dossier de una empresa cruza el documento del **representante legal** (de RUP)
  contra SIRI.

- **E4 — Contratos del proveedor**
  Reúso del pipeline SECOP por `contractor_document` + filtro temporal ("en 2025"),
  con links (reúso de la jerarquía de reparación de URL existente).

- **E5 — UT / consorcios** (Grupos de Proveedores — estructura confirmada: 1 fila
  por (grupo, participante))
  (a) **Directo:** `nit_grupo` → integrantes con `participacion` (%) y
  `es_lider_del_grupo`. (b) **Inverso:** `nit_participante` → grupos en que participa
  una identidad. (c) **Recursivo:** cada `nit_participante` es un proveedor →
  enriquecer cada miembro vía Capa B (E3). **INV-UT:** el dossier de una UT marca
  qué integrante(s) tienen fiscal/inhabilidad vigente, no solo lista nombres.

- **E6 — Agregador dossier + render**
  Orquesta A → B + C + UT; `formatter` para ambos canales; **disclaimer legal +
  fecha de corte** visibles. **INV-CORTE:** todo veredicto de sanción/inhabilidad
  muestra su fuente y fecha de consulta.

- **E7 — Routing de intent**
  Engancha en `query_router`/`orchestrator` el intent "perfil de contratista" y lo
  separa de la búsqueda normal de procesos.

---

## Riesgos (estos *son* el diseño)

- **A. Normalización de identidad.** Si falla, todo lo demás miente con confianza.
  Mitigación: E1 como spine testeado contra los formatos crudos reales.
- **B. Vigencia, no presencia.** "Inhabilidad general 20 años desde 2006" sigue
  viva; "multa 2006" ya no inhabilita. Mitigación: INV-VIG en E3.
- **C. Homónimos + cruce con representante legal.** El nombre devuelve muchas
  personas; solo el documento desambigua. El chequeo legalmente relevante (empresa
  cuyo RL aparece en SIRI) no es obvio en ninguna tabla sola. Mitigación: INV-AMB +
  INV-RL.
- **D. Composición de UT — RESUELTA (2026-06-29).** "Grupos de Proveedores" trae los
  integrantes en formato largo (1 fila por grupo×participante) con `nit_participante`,
  `participacion` (%) y `es_lider_del_grupo`. E5 entra en V1 con dirección directa,
  inversa y enriquecimiento recursivo de miembros. Riesgo cerrado.

---

## Consecuencias

- **Más fácil:** responder consultas de debida diligencia sobre un tercero en un
  solo turno; reutilizar el dossier en cualquier canal sin código de canal.
- **Más difícil / nuevo:** mantener el índice de nombres (rebuild); gobernar el
  riesgo legal de aseverar sanciones (disclaimer, vigencia, fuente, fecha).
- **A revisitar:** cadencia de rebuild del índice; si el volumen de proveedores
  exige paginar el build; resolución del Riesgo D.

## Action items

1. [x] Confirmar columnas de "Grupos de Proveedores" — RESUELTO: trae integrantes (`nit_participante`, `participacion`, `es_lider_del_grupo`). E5 desbloqueada.
2. [ ] Confirmar dominios Socrata y `app_token`/límites de fiscal, SIRI, RUES, Grupos.
3. [ ] E1: `identity.py` + tests con crudos de las 5 fuentes.
4. [ ] E2: build FTS5 (script) + detección de identificador desnudo.
5. [ ] E3–E7 según plan.

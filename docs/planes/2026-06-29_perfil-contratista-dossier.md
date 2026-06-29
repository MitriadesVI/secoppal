# ADR-042: Perfil integral de proponente/contratista (dossier transversal)

**Estado:** Proposed
**Fecha:** 2026-06-29 · rev. 2026-06-29 (feedback de revisión incorporado)
**Deciders:** rodrigo (owner) + pipeline SECOPPAL
**Alcance:** feature transversal — WhatsApp + Streamlit, sobre el pipeline channel-agnostic existente.

> **Nota de numeración:** el ADR más alto comprometido en el repo es ADR-041. Hay
> worktrees paralelos que pueden estar asignando 042+ a la vez; **reconciliar el
> número al merge** (no está garantizado que 042 quede libre).

---

## Contexto

El usuario quiere preguntar, en lenguaje natural o por identificador, por un
**proponente/contratista** y recibir una **revisión integral**: sus contratos (con
links), si tiene **responsabilidad fiscal**, **inhabilidades/antecedentes
disciplinarios**, su estado en **registros** (RUP/RUES), y la **composición de las
uniones temporales/consorcios** en que participa.

Variantes de consulta a soportar (no exhaustivo):
- "¿qué hay de la entidad FUNSOCOM?" (nombre → identidad → dossier)
- "¿qué ha celebrado la persona 1140835135 en el 2025?" (cédula → contratos por año, con links)
- "¿la UT X quién la conforma?" (grupo → integrantes)
- "¿el representante legal de <empresa> tiene inhabilidades?" (cruce NIT empresa → cédula RL → SIRI)

### Insumos de datos confirmados (5 fuentes, todas Socrata / datos.gov.co)

| Fuente | Rol | Llave | Formato real de la llave | Frescura |
|---|---|---|---|---|
| RUP Proveedores registrados | identidad + contacto + **representante legal** | `nit` | `900264685` limpio, **sin DV** | — |
| RUES (entidades / ESAL) | identidad registral + vigencia | `nit` + `digito_verificacion` (separados) | NIT y DV en columnas distintas | — |
| Responsabilidad fiscal (SIREF / Boletín RF) | sanciones fiscales | `n_mero_de_identificaci_n` | `8,600,345,941` — **comas Y dígito de verificación** | — |
| Antecedentes disciplinarios SIRI (Procuraduría) | inhabilidades / sanciones disciplinarias | `numero_identificacion` (cédula) | `7534386` limpio; trae `DURACION_*` + `FECHA_EFECTOS_JURIDICOS` | — |
| Grupos de Proveedores - SECOP II | UT / consorcios + **integrantes** | `nit_grupo` / `nit_participante` | **1 fila por (grupo, participante)**: `nit_participante`, `participacion` (%), `es_lider_del_grupo` | diaria |

---

## Hallazgo de datos (manda el dato, no la expectativa)

La pregunta original ("¿vectores/parquet o fuzzy?") era la pregunta equivocada para
el grueso del trabajo. El problema dominante **no** es encontrar nombres — es
**conciliar la misma identidad escrita distinto entre fuentes**:

- fiscal: `8,600,345,941` (comas + DV) → NIT base `860034594`, DV `1`
- RUP: `900264685` (limpio, sin DV)
- RUES: NIT y DV en columnas separadas
- SIRI: cédula limpia `7534386`

Un JOIN ingenuo **silenciosamente** omite sanciones (falso "limpio") o señala a la
persona equivocada (falso "inhabilitado"). En este dominio eso no es un bug: es un
riesgo legal. Por eso **la normalización de identidad es la columna vertebral de la
feature**, no un detalle de preprocesamiento.

Segundo hallazgo: `tipo_empresa` en RUP viene sucio (se observó una persona natural
con cédula marcada `UNIÓN TEMPORAL`). No es fuente de verdad para clasificar grupos;
eso lo da el dataset **Grupos de Proveedores**.

---

## Decisión

Construir un **perfil/dossier de contratista** como agregador transversal sobre el
pipeline existente, organizado en **1 spine + 3 capas**:

- **Spine — `app/core/identity.py`**: identidad **tipada** (ver abajo). Todo se cuelga de aquí.
- **Capa A — Resolución por nombre** (`nombre → identidad`): **SQLite FTS5 local**
  sobre RUP + RUES para generar candidatos + **rerank fuzzy**. Único componente con copia local.
- **Capa B — Enriquecimiento** (por identidad exacta, **en vivo**): fiscal, SIRI,
  RUES, Grupos. Un **adapter pequeño por fuente** sobre transporte Socrata genérico.
- **Capa C — Contratos** (pipeline SECOP existente): filtra por `contractor_document`, con links y año.

El **agregador `dossier`** orquesta A → B + C con **fan-out concurrente acotado**
(ver más abajo) y renderiza por el `formatter` existente → WhatsApp y Streamlit sin
código de canal.

### Identidad tipada (corrección al spine: no es solo un número)

La llave **canónica de join** es `(document_type, base_number)`. La **procedencia**
(`raw_value`, `source`, `confidence`) va en el registro de match, **no** en la llave
— si la llave llevara `source`, dos registros de la misma entidad desde fuentes
distintas no comparan iguales y el JOIN falla.

```
# llave canónica (lo que une fuentes)
IdentityKey(document_type="NIT", base_number="860034594")

# match (lo que se muestra / audita)
IdentityMatch(key=IdentityKey(...), check_digit="1",
              raw_value="8,600,345,941", source="responsabilidad_fiscal",
              confidence="exact")
```

El **tipo documental** se decide por, en orden: (1) columna explícita de la fuente;
(2) contexto del dataset; (3) validación de DV **cuando aplica**; (4) inferencia
solo como último recurso, marcada incierta. **Nunca por longitud de dígitos**
(cédula y NIT se solapan). Matiz: el NIT tiene DV (módulo 11), la **cédula no** → el
DV es señal *positiva* de "es NIT", no un discriminador sobre cédulas; ausencia de
DV NO es evidencia de cédula.

**INV-ID-TYPE:** dos números iguales con tipos documentales distintos **no** se
fusionan automáticamente.

### Estrategia de origen (frescura vs latencia)

- **Local (índice):** SOLO el índice de nombres (RUP + RUES) en FTS5, reconstruido
  periódicamente. Es el único componente que justifica copia local.
- **Vivo (Socrata):** enriquecimiento fiscal/SIRI/RUES/Grupos/contratos por
  identidad exacta. La responsabilidad legal exige el dato más fresco.

### Transporte vs. normalización (corrección a "reúso de SecopClient")

`SecopClient.query()` aplica `_normalize_row()` **incondicionalmente** (reparación
de URL, campos de dinero, columnas contractuales): es lógica SECOP, no genérica.
Meter RUES/SIRI/fiscal por ahí corrompe datos. Separar:

```
SocrataTransport   → get(dataset_id, soql): deadline, reintentos, errores, telemetría
SecopAdapter       → normalización SECOP (la actual)
RupAdapter / RuesAdapter / FiscalAdapter / SiriAdapter / GroupsAdapter
                   → normalización por fuente (pequeños, una responsabilidad)
```

No es refactor grande: extraer el transporte del `SecopClient` actual y dejar la
normalización SECOP como su adapter.

### Estados de resultado por fuente ("no encontré" ≠ "está limpio")

**La regla más importante del producto.** El dossier NUNCA convierte ausencia en
ausencia-de-antecedentes. Cada fuente devuelve un `SourceResult` con estado:

```
coincidencia_exacta            — hay match por documento exacto
sin_coincidencia_exacta        — la fuente respondió, no hay match (≠ "limpio")
fuente_no_disponible           — la fuente no respondió / error
consulta_agotó_tiempo          — venció el presupuesto de esa fuente
identidad_ambigua              — el nombre no se resolvió a un documento único
vigencia_no_determinable       — hay registro pero no se pudo calcular si está vigente
```

Redacción correcta: *"No se encontró coincidencia exacta para este documento en la
fuente X consultada a las HH:MM del DD/MM/AAAA."* — **nunca** "No tiene antecedentes".

```
SourceResult(source="SIRI", status="coincidencia_exacta",
             matched_document="7534386", queried_at="...",
             records=[...], elapsed_ms=820)
```

### Fan-out concurrente acotado (la decisión de concurrencia SÍ cambia aquí)

En Sprint A, H3 (paralelo procesos/contratos) se difirió por no justificar el costo.
Un dossier es distinto: consulta 5–6 fuentes; secuencial revienta el deadline de
15s. Se justifica **fan-out concurrente con presupuesto por fuente + ensamblaje
parcial**. Debe **integrarse con `deadline.py` existente**, no ser un mecanismo
paralelo (respetar la thread-safety ya documentada en `SecopClient.derived_for_timeout`).
Si una fuente falla/expira → su `SourceResult` lo refleja y el dossier continúa.

---

## Opciones consideradas (resolución por nombre)

### Opción A — Embeddings / búsqueda vectorial
Descartada. La consulta no es semántica; decidir si `860034594-1`, `8,600,345,941`
y `860034594` son la misma persona jurídica con embeddings es matar mosca con
satélite. Infra nueva, costo alto, mal encaje.

### Opción B — LIKE en vivo sobre Socrata por nombre
Descartada. LIKE sin índice sobre millones de proveedores: lento, ranking pobre,
choca con el presupuesto de latencia (Etapa 6).

### Opción C — FTS5 local + rerank fuzzy para nombre→identidad; enriquecimiento vivo (ELEGIDA)
FTS5 genera candidatos (tokens, orden); un **rerank fuzzy** afina tipos/siglas que
FTS5 no cubre. **Nota honesta:** RapidFuzz **no está** hoy en el proyecto (se
verificó) → o se añade como dependencia nueva, o se rerankea con la maquinaria
existente (`normalize_retrieval_text` + `spell_correction`). Decisión de E2.
Campos indexados: `razon_social`, `razon_social_normalizada`, `sigla`,
`nombre_comercial`, `nit`, `source`. El candidato muestra **por qué coincidió**.

---

## Plan por etapas

> Estilo Sprint: cada etapa cierra con tests e invariantes. No se declara cerrada
> hasta E7. Todo detrás de **feature flag** para el alfa.

- **E0 — Contrato de fuentes** (antes de tocar código de identidad)
  Por cada una de las 5 fuentes: dataset ID, dominio, columnas reales, tipos, tasa
  de nulos, formatos de documento, volumen, límite de consultas/`app_token`,
  **muestras congeladas** y fecha máxima de actualización. Subsume el viejo Action
  Item de "confirmar dominios/tokens". Sin esto, E3 se escribe a ciegas.

- **E1 — Identidad tipada** (`identity.py`)
  `IdentityKey` (join) + `IdentityMatch` (procedencia). Normalización con los crudos
  reales de las 5 fuentes. **INV-ID:** misma identidad real → misma `IdentityKey`
  desde cualquier fuente. **INV-ID-TYPE** (arriba).

- **E2 — Resolución por ID y por nombre**
  (a) Índice FTS5 (RUP+RUES) + rerank fuzzy; script de build en `scripts/`.
  (b) Detección de identificador desnudo (cédula/NIT) extendiendo `direct_lookup`.
  **INV-AMB:** nombre ambiguo → desambiguar antes de aseverar nada.

- **E3 — Adapters de fuentes + estados de resultado** (vivo)
  `SocrataTransport` genérico + `Fiscal/Siri/Rues/Groups Adapter`. Cada uno devuelve
  `SourceResult` con su estado. **INV-VIG:** SIRI reporta vigencia **calculada**
  (ventana desde `FECHA_EFECTOS_JURIDICOS` + `DURACION_*`), conservando clase de
  sanción, fecha de efectos, duración, fecha-fin calculada, **regla usada**, fuente
  y fecha de consulta; si no se puede calcular → `vigencia_no_determinable` (no se
  esconde el razonamiento). **INV-RL:** dossier de empresa cruza el documento del
  **representante legal** (RUP) contra SIRI.

- **E4 — Contratos del proveedor**
  Reúso del pipeline SECOP por `contractor_document` + filtro temporal, con links.

- **E5 — UT / consorcios** (Grupos de Proveedores; estructura confirmada)
  (a) **Directo:** `nit_grupo` → integrantes con `participacion` (%) y
  `es_lider_del_grupo`. (b) **Inverso:** `nit_participante` → grupos.
  (c) **Recursivo ACOTADO:** NO se generan N dossiers completos. Composición completa
  + **señales básicas** por integrante; dossier profundo **bajo demanda**.
  **INV-UT:** "8 integrantes, señales en 2 — ver detalle de 3 y 7", no 8×4 consultas
  automáticas.

- **E6 — Agregador dossier + render**
  Orquesta A → B + C + UT con fan-out acotado; `formatter` para ambos canales;
  disclaimer legal + fecha de corte. **INV-CORTE:** todo veredicto muestra fuente,
  documento y fecha de consulta.

- **E7 — Routing de intent** (no secuestrar `entity_query`)
  ```
  NIT/cédula explícita              → dossier
  "proveedor/contratista/proponente/empresa" → dossier
  "quién conforma la UT"            → dossier de grupo
  "ha celebrado la persona…"        → contratos del proveedor
  "entidad X" (genérico)            → buscar en comprador (gazetteer) + índice de
                                      proveedores; si ambos existen → preguntar cuál
  ```
  Misma familia que ADR-040 (territorio vs comprador): el nuevo intent **no** debe
  capturar la ruta de comprador existente.

---

## Riesgos (estos *son* el diseño)

- **A. Normalización de identidad.** Si falla, todo miente con confianza. Mitiga: E1.
- **B. Vigencia, no presencia.** Inhabilidad de 20 años (2006) sigue viva; multa de
  2006 ya no inhabilita. Mitiga: INV-VIG + estado `vigencia_no_determinable`.
- **C. Homónimos + cruce con representante legal.** Solo el documento desambigua;
  el chequeo empresa→RL→SIRI no es obvio en ninguna tabla sola. Mitiga: INV-AMB + INV-RL.
- **D. Composición de UT — RESUELTA.** "Grupos de Proveedores" trae integrantes en
  formato largo (`nit_participante`, `participacion`, `es_lider_del_grupo`). E5 en V1.
- **E. Falso "limpio".** El bug más peligroso. Mitiga: máquina de estados de resultado.
- **F. Habeas data (Ley 1581).** El RUP trae correos, teléfonos y **domicilios** de
  personas naturales. Exponer el domicilio de un ciudadano es problema de privacidad
  **independiente de la exactitud**. **INV-PII:** el dossier muestra NIT/nombre/
  ubicación municipal; **no** domicilio, correo ni teléfono personal. Propósito limitado.

---

## Qué entra en el alfa

**Dentro:** búsqueda por NIT/cédula; búsqueda por nombre con desambiguación;
identidad RUP/RUES; contratos con links y año; fiscal exacto; SIRI exacto + vigencia
conservadora; representante legal cuando hay documento exacto; composición directa
de UT; estados parciales + fecha de corte; INV-PII; WhatsApp y Streamlit por
formatter compartido. **Detrás de feature flag.**

**Fuera (post-alfa):** dossiers recursivos completos de todos los integrantes; grafo
profundo de participaciones; historial de cambios registrales; scoring de riesgo;
conclusiones automáticas tipo "proveedor riesgoso" (mina legal); cache persistente
de datos jurídicos.

---

## Criterios de aceptación del alfa (gates)

```
1. Cero cruces entre documentos distintos.
2. Cero "sin antecedentes" derivados de timeout/no-match.
3. Toda alerta muestra fuente, documento y fecha de consulta.
4. Una fuente caída no tumba el dossier completo.
5. Nombre ambiguo nunca produce una sanción atribuida sin desambiguar.
6. Ningún dato personal sensible (domicilio/correo/teléfono) en el render. [INV-PII]
```

**Corpus mínimo:** 10 personas jurídicas · 10 personas naturales · 5 UT/consorcios ·
5 homónimos o nombres similares · 5 casos con formatos sucios de identificación.

---

## Consecuencias

- **Más fácil:** debida diligencia sobre un tercero en un turno; dossier reutilizable
  en cualquier canal.
- **Más difícil / nuevo:** mantener índice de nombres; gobernar riesgo legal
  (disclaimer, vigencia, fuente, fecha) y de privacidad (INV-PII); transporte Socrata
  desacoplado de la normalización SECOP.
- **A revisitar:** cadencia de rebuild; paginar el build si el volumen lo exige;
  decisión RapidFuzz vs. maquinaria existente (E2); reconciliación del número de ADR.

## Action items

1. [x] Composición de UT — RESUELTO (integrantes en el dataset de Grupos).
2. [ ] **E0:** contrato de fuentes (dominios, `app_token`, columnas, límites, muestras congeladas) de las 5 fuentes.
3. [ ] E1: `identity.py` (IdentityKey/IdentityMatch) + tests con crudos de las 5 fuentes.
4. [ ] E2: build FTS5 + rerank fuzzy + detección de identificador desnudo.
5. [ ] E3–E7 según plan, todo detrás de feature flag.
6. [ ] Reconciliar número de ADR al merge (042 no garantizado libre entre worktrees).

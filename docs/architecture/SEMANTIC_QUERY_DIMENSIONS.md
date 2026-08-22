# Dimensiones semánticas de consulta y composición de refinamientos

**Estado:** entregado y vigente en `main` (`8acb21a`) · decisión canónica en
[`ADR-061`](../decisions_log.md)  
**Última actualización:** 2026-08-22

> Guía **técnica y de mantenimiento**. La decisión, sus alternativas descartadas
> y la evidencia de dogfood viven una sola vez, en `docs/decisions_log.md`
> (ADR-061). La evidencia de fuente detallada vive en
> `docs/audits/2026-08-22_SEARCH_SH1_SEMANTIC_MONEY_HARDENING.md`.

---

## 1. Por qué existen las dimensiones semánticas

El extractor de objeto era el **vertedero por defecto**: todo tramo no-stopword
que ningún resolutor estructurado reclamaba terminaba como término temático y se
proyectaba como búsqueda literal de texto sobre el objeto del contrato.

Eso produjo dos fallas de dogfood con la misma raíz:

```text
convenios de alcaldia de barranquilla en 2026
```

`convenios` no es modalidad reconocida ni tema real, así que se buscó la cadena
`convenio` dentro del objeto contractual — trayendo prestación de servicios, obra
e interventoría que solo **referencian** un convenio ajeno.

```text
base:      contratos adulto mayor gobernacion de atlantico en 2026
follow-up: por mas de 100 millones en 2026
```

El año repetido clasificó el turno como *cambio de año*, y ese conjunto permitido
descartó `valor_min`. El campo desapareció **antes** de serializar el frame, así
que el fingerprint no representaba un universo nuevo y la paginación siguió
recorriendo resultados sin filtrar.

**Conclusión de arquitectura:** el lenguaje del usuario se reparte en dimensiones
estructurales distintas, y esas dimensiones tienen que (a) reclamar su tramo y
(b) poder cambiar de forma independiente en un mismo turno.

---

## 2. Taxonomía de dimensiones

`app/core/semantic_dimensions.py` — **un vocabulario, no una ontología**:

```python
class SemanticDimension(str, Enum):
    TOPIC = "TOPIC"
    MODALITY = "MODALITY"
    CONTRACT_TYPE_OR_NATURE = "CONTRACT_TYPE_OR_NATURE"
    ACTOR = "ACTOR"
    BUYER = "BUYER"
    GEOGRAPHY = "GEOGRAPHY"
    TIME = "TIME"
    VALUE = "VALUE"
    STATE = "STATE"
    CONTROL_LANGUAGE = "CONTROL_LANGUAGE"
```

Su única función es permitir que un resolutor estructurado **declare qué
consumió**, para que ese tramo no vuelva a inferirse como término temático.

Que sea un `str, Enum` compartido y no una jerarquía es deliberado: una ontología
invitaría a modelar relaciones entre dimensiones, y aquí lo valioso es
exactamente lo contrario —que sean **independientes**.

---

## 3. Principio del tramo consumido

**`INV-STRUCTURED-DIMENSION-NOT-TOPIC`**

> Una vez que un tramo se consume con confianza como dimensión estructurada, no
> puede además filtrarse a `objeto`/tema, salvo que la semántica exija
> explícitamente el uso dual.

El orden en el router es: **modalidad** enmascara su tramo → **tipo/naturaleza
contractual** reclama el suyo → el extractor de objeto recibe solo el **residual**.

La excepción legítima es la clase `COMPOSITE` (§4): ahí el texto controlado se usa
*además* del campo estructurado, porque el valor de fuente por sí solo confunde
contratos y convenios. Es uso dual **declarado**, no fuga.

---

## 4. Tema vs. naturaleza contractual

`app/core/contract_type_resolver.py` clasifica en tres niveles:

| Clasificación | Significado |
|---|---|
| `STRUCTURED_EXACT` | un valor exacto de `tipo_de_contrato` |
| `STRUCTURED_FAMILY` | un conjunto cerrado de valores de fuente |
| `COMPOSITE` | requiere evidencia estructurada **+** texto controlado |

`app/core/contract_type_projection.py` proyecta cada clase **por dataset**,
porque los nombres del campo de justificación difieren
(`justificaci_n_modalidad_de` en procesos, `justificacion_modalidad_de` en
contratos).

### El caso `convenio`

La auditoría de fuente en vivo (2026-08-22) encontró que **no existe** un valor
`tipo_de_contrato = 'Convenio'`. La familia interadministrativa —135.932 filas de
procesos, 123.823 de contratos— está mayoritariamente almacenada como tipo
`Otro`. Familia solidaria: 2.208 / 8.900. Tipo exacto `Acuerdo de cooperación`:
27 / 22.

Es decir: **`tipo_de_contrato` por sí solo no puede representar el lenguaje
natural `convenio`**. Por eso `convenio` es `COMPOSITE`, y la política de
proyección es:

```text
familia de justificación solidaria
  OR (familia interadministrativa combinada + texto controlado "convenio")
  OR (Decreto 092 + texto controlado "convenio")
  OR tipo exacto "Acuerdo de cooperación"
```

El `%convenio%` crudo **no** es equivalente a naturaleza contractual. Falsos
positivos verificados: un contrato de prestación de servicios que *apoya la
ejecución* de un convenio, una obra *bajo* un convenio, una interventoría *sobre*
obras derivadas de convenios.

Las construcciones específicas se resuelven **antes** que la genérica:
`convenio interadministrativo`, `convenio de asociación` y `convenio solidario`
tienen sus propios patrones y ganan el texto antes de que `convenio` amplio lo
reclame.

---

## 5. Naturaleza contractual vs. modalidad

**Son ejes independientes.** `convenio` puede coexistir con `régimen especial` sin
que uno sea alias del otro.

```text
convenios de régimen especial de alcaldía de barranquilla en 2026
  → naturaleza contractual = convenio        (CONTRACT_TYPE_OR_NATURE)
  → modalidad              = régimen especial (MODALITY)
  → comprador              = Barranquilla     (BUYER)
  → año                    = 2026             (TIME)
  → objeto                 = []
```

**Regla dura:** los IDs de procedimiento de modalidad **nunca** se usan como IDs
de tipo contractual, y viceversa. Documentar `convenio == régimen especial` sería
arquitectura falsa.

---

## 6. Autoridad de modalidad

`ModalityResolver`, su catálogo y su léxico **no se modificaron en SEARCH-SH1**
(`M2 cambiado: NO`). Siguen siendo la única autoridad sobre la modalidad de
contratación.

Familias verificadas en la auditoría, todas consumidas sin residual y sin fuga a
`objeto`, en ambos datasets:

| Lenguaje del usuario | IDs de procedimiento |
|---|---|
| licitación / licitación pública | `licitacion_publica`, `licitacion_publica_obra` |
| selección abreviada | `seleccion_abreviada_menor_cuantia`, `..._sin_manifestacion`, `..._subasta_inversa` |
| subasta inversa | `seleccion_abreviada_subasta_inversa` |
| mínima cuantía | `minima_cuantia` |
| concurso de méritos | `concurso_meritos_abierto`, `concurso_meritos_precalificacion` |
| contratación directa | `contratacion_directa`, `contratacion_directa_con_ofertas` |
| régimen especial | `regimen_especial`, `regimen_especial_con_ofertas` |
| acuerdo marco / AMP | `acuerdo_marco_precio_secop` |
| enajenación de bienes | `enajenacion_sobre_cerrado`, `enajenacion_subasta` |

El control congelado de *selección abreviada* sigue expandiendo a exactamente
tres hijos y excluyendo acuerdo marco.

---

## 7. Composición en el `QueryFrame`

Un turno de refinamiento produce un `QueryFrame` con deltas por dimensión. El
merge conserva del frame anterior lo que el turno no toca, y aplica lo que sí.

Familias de campos en `app/core/followup_mutations.py`:

```python
TOPIC_FIELDS          = {objeto, objeto_or, objeto_phrases, objeto_exclude, topic_facets}
MONEY_FIELDS          = {valor_min, valor_max, money_metadata}
MODALITY_FIELDS       = {modalidad_include_procedure_ids, modalidad_exclude_procedure_ids}
CONTRACT_TYPE_FIELDS  = {contract_type_include}
SCOPE_FIELDS          = {entidad*, departamento*, ciudad*, region, ...}
PAGING_FIELDS         = {offset, _display_offset, _shown_total}
```

`contract_type_include` está además en `SCOPE_TOPIC_KEYS`
(`app/core/_followup_constants.py`), de modo que una consulta con tipo
contractual cuenta como consulta con alcance real y no como consulta desnuda.

---

## 8. Deltas ortogonales

**`INV-ORTHOGONAL-DELTA-NOT-LOST-BY-NOOP`**

> Un no-op o una repetición en una dimensión no puede borrar un cambio explícito
> en otra.

Dos cambios lo sostienen:

**Clasificación** (`app/core/followup_engine.py`) — un turno deja de clasificarse
como `change_year` o `change_order` si además trae un filtro ortogonal:

```python
has_orthogonal_filter = any(
    key not in {"fecha_desde", "fecha_hasta", "ordering_signal"}
    and value not in (None, "", [], {}, (), False)
    for key, value in current_frame.modifiers.items()
)
```

**Compuerta** (`app/core/followup_mutations.py`) — `MONEY_FIELDS` entra en los
conjuntos permitidos de `change_order`, `change_year`, `change_scope` y
`change_dataset`.

Traza resultante del caso base:

```text
intent           = refine_filter
requested_fields = [fecha_desde, fecha_hasta, valor_min]
accepted_fields  = [fecha_desde, fecha_hasta, valor_min]
rejected_fields  = []
merged dataset   = contratos
merged topic     = [adulto_mayor]
merged buyer     = DEPARTAMENTO DEL ATLANTICO**
merged dates     = 2026-01-01 .. 2026-12-31
merged valor_min = 100000000
```

---

## 9. Filtros monetarios

Reglas de límites, implementadas en `_transition_clears`:

- **Rango completo nuevo** (`valor_min` + `valor_max`) → reemplaza **ambos**
  extremos previos.
- **Mínimo nuevo** → limpia un `valor_max` heredado **solo si los límites son
  incompatibles**.
- **Máximo nuevo** → regla simétrica.

Todas las frases de mínimo, máximo y rango sobreviven la cadena completa: parser
→ frame → compuerta de intención → merge → params → SoQL → contexto almacenado.

---

## 10. Proyección por dataset

La restricción monetaria del usuario es **una sola** a nivel de producto; el
campo de almacenamiento depende del dataset:

| Dataset | Campo monetario |
|---|---|
| Contratos (`jbjy-vk9h`) | `valor_del_contrato` |
| Procesos (`p6dx-8zbt`) | `precio_base` |

**Fuera del contrato de producto vigente esos campos no se declaran
semánticamente equivalentes.** La proyección de tipo contractual sigue el mismo
principio: explícita por dataset, porque los nombres de campo y la evidencia
disponible difieren.

---

## 11. Invalidación de paginación

**`INV-FILTER-MUTATION-INVALIDATES-PAGINATION`**

> Si cambia un filtro semántico, cambia el universo de resultados; el estado de
> paginación deja de ser válido.

Implementación:

- `ConversationStore.invalidate_pagination_buffers(chat_id)` elimina el sidecar
  **completo**;
- `handle_normal_flow` lo invoca **antes** de parsear y ejecutar cualquier turno
  que no sea de paginación;
- toda mutación aceptada limpia además `offset`, `_display_offset` y
  `_shown_total`;
- los nuevos parámetros/fingerprint filtrados se guardan para la paginación
  posterior.

Dimensiones que disparan la invalidación: **valor, tiempo, tema, comprador,
territorio, modalidad, tipo/naturaleza contractual y actor**.

El sidecar es **solo** una caché reproducible de paginación: el historial de
conversación queda intacto y sigue siendo autoritativo. Borrarlo entero también
impide que un grupo hermano rancio de una consulta dual-dataset sobreviva a un
refinamiento posterior de un solo dataset, y cubre los caminos de cero
resultados, aclaración y timeout.

Tras un refinamiento, *"más resultados"* pagina el universo **refinado**.

---

## 12. Ejemplos resueltos

| Lenguaje | Dimensión | Notas |
|---|---|---|
| `adulto mayor` | `TOPIC` | tema real; sigue expandiéndose morfológicamente |
| `selección abreviada` | `MODALITY` | familia exacta de tres hijos |
| `régimen especial` | `MODALITY` | dos IDs de procedimiento |
| `convenio` | `CONTRACT_TYPE_OR_NATURE` | `COMPOSITE`: estructura + texto controlado |
| `prestación de servicios` | `CONTRACT_TYPE_OR_NATURE` | `STRUCTURED_EXACT` |
| `arrendamiento` | `CONTRACT_TYPE_OR_NATURE` | `STRUCTURED_FAMILY` (inmuebles + muebles) |
| `Alcaldía de Barranquilla` | `BUYER` | vía `EntityResolver` |
| `Cartagena` | `GEOGRAPHY` | territorio, cuando el contexto lo sostiene |
| `2026` | `TIME` | |
| `más de 100 millones` | `VALUE` | `valor_min` |

Caso completo:

```text
convenio + régimen especial + Barranquilla + >2B + 2026   →   0 filas
```

**Cero es un resultado válido.** La intersección está respaldada por la fuente y
correctamente formada; ambas dimensiones, comprador, tiempo y dinero están
presentes y no hay fuga a `objeto`. No toda consulta debe devolver filas.

---

## 13. Invariantes

| Invariante | Enunciado |
|---|---|
| `INV-STRUCTURED-DIMENSION-NOT-TOPIC` | un tramo consumido como dimensión estructurada no se filtra además a `objeto`, salvo uso dual declarado |
| `INV-ORTHOGONAL-DELTA-NOT-LOST-BY-NOOP` | un no-op en una dimensión no borra un cambio explícito en otra |
| `INV-FILTER-MUTATION-INVALIDATES-PAGINATION` | toda mutación de filtro semántico rebasa el estado de paginación |

---

## 14. Estrategia de pruebas

| Archivo | Cubre |
|---|---|
| `tests/test_search_sh1_dogfood_regressions.py` | las cuatro regresiones congeladas antes de tocar runtime |
| `tests/test_contract_type_resolver.py` | clasificación y consumo de tramos |
| `tests/test_contractual_dimension_pipeline.py` | pipeline de punta a punta, sin fuga a `objeto` |
| `tests/test_search_sh1_modality_audit.py` | que la autoridad de modalidad no se movió |
| `tests/test_search_sh1_money_followups.py` | composición de deltas monetarios |
| `tests/test_pagination_buffer_lifecycle.py` | invalidación del sidecar |
| `scripts/run_search_sh1_mutations.py` | campaña de mutación acotada: **15/15 detectadas**, fuentes restauradas byte a byte |

Disciplina del frente: **congelar la regresión antes de tocar runtime**, y validar
el fix por mutación —no por "los tests pasan".

---

## 15. Reglas de extensión

1. **Una clase contractual nueva exige evidencia de fuente para su proyección**,
   no una entrada de léxico. Auditar el dataset primero.
2. **No promover naturaleza contractual a modalidad** ni al revés. Los espacios de
   IDs son disjuntos.
3. **Un tramo consumido no vuelve a `objeto`**; si necesita uso dual, declararlo
   como `COMPOSITE` con evidencia textual controlada.
4. **Toda dimensión nueva que filtre debe invalidar paginación.**
5. **Todo delta nuevo debe componer** con los existentes; si la clasificación de
   intención lo descarta, el bug está en la clasificación, no en la compuerta.
6. **La proyección se declara por dataset.** Dos campos de fuente distintos no son
   sinónimos solo porque el producto los exponga igual.
7. **Cero filas no es señal de fallo.** No añadir fallbacks que aflojen filtros
   para "encontrar algo".

---

## Referencias

- Decisión canónica: `docs/decisions_log.md` → **ADR-061**
- Evidencia de fuente, proyección y dogfood:
  `docs/audits/2026-08-22_SEARCH_SH1_SEMANTIC_MONEY_HARDENING.md`
- Estado técnico canónico:
  `docs/documento_tecnico_secoppal_arquitectura_bitacora.md`
- Dimensión `ACTOR`: `docs/architecture/ACTOR_PROFILE_V1.md`

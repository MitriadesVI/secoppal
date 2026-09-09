# F1–F5: matriz de cierre

Oráculos propios en
[`tests/test_cu_v11_audit_f1_f5.py`](../../../tests/test_cu_v11_audit_f1_f5.py)
(48 casos, verdes) y
[`tests/test_cu_v11_audit_findings.py`](../../../tests/test_cu_v11_audit_findings.py)
(8 casos, **rojos a propósito**: el incumplimiento reproducido). Ninguna
expectativa se derivó del helper de runtime.

| Hallazgo | V1 (1ª auditoría) | V1.1 medido aquí | Estado |
|---|---|---|---|
| **F1** palabra de estado en objeto o Actor | ejecutaba `estado_contrato='Borrador'`, estampaba `source_records`, borraba `borradores` del tema | universo `celebrated`, tema preservado, identidad exacta, cero `'BORRADOR'` en el SoQL, OFF/ON | **CERRADO** |
| **F2** composición de dos estados | ejecutaba sólo la primera mitad según el orden | dos excluidos: aclara con cero SQL en ambos órdenes. **Admitido + excluido: sigue ejecutando media composición cuando el admitido va primero** | **NO CERRADO** (P1) |
| **F3** contenido publicado | `record_terms()` reescribía la descripción de la fuente | `record_terms` eliminado del runtime; cadena fuente idéntica en Search y perfiles OFF/ON | **CERRADO** |
| **F4** params materializados = consentimiento | tres entradas ampliaban el universo | ninguna amplía; conflicto antes de SQL; sólo autoridad versionada o lookup directo admiten registros | **CERRADO** |
| **F5** dual pierde la parte de registros | ejecutaba sólo procesos y respondía cero | aclara sin ejecutar ningún hijo, ambos órdenes, OFF/ON; el dual ordinario ejecuta ambos | **CERRADO** |

## F1 — detalle

| Caso | Exigido | Observado |
|---|---|---|
| A `contratos de elaboración de borradores de documentos de la Alcaldía de Cartagena` | celebrated, «borradores» en el objeto, sin estado Borrador, sin `source_records` | `contract_universe='celebrated'`; `objeto` contiene `borrador`; `estado_contrato` ausente; ningún SoQL con `'BORRADOR'`. OFF y ON |
| B `qué ha contratado FUNDACION BORRADORES` | Actor resuelto por la infraestructura normal, celebrated | `actor_scope.base_number='71000001'`; universo `celebrated`; SoQL limpio. OFF y ON |
| C `registros en borrador de la Alcaldía de Cartagena` | acredita `source_records`, estado Borrador, ejecuta | `source_records`, `estado_contrato=['Borrador']`, 10 filas, todas `Borrador`, sin aclaración |
| Contrapruebas Actor: `CONSTRUCTORA CANCELADOS`, `FUNDACION REGISTROS`, `COOPERATIVA BORRADORES`, `UNION APROBACION` | identidad, nunca estado | los cuatro resuelven su NIT/CC y conservan `celebrated`; ningún estado excluido en el SoQL |
| Contrapruebas de objeto: `custodia de registros clínicos`, `elaboración de borradores de documentos`, `gestión de cancelados bancarios`, `aprobacion de estudios previos` | celebrated | los cuatro `celebrated`, sin filas de estados excluidos |

Propiedad Hypothesis adicional: ninguna de las cuatro palabras dentro de un
objeto de cuatro temas distintos, ni dentro de un nombre de Actor protegido,
produce un universo distinto de `celebrated`.

## F2 — detalle, incluido el incumplimiento

| Caso | Exigido | Observado | |
|---|---|---|---|
| `registros cancelados y borradores…` | aclarar, cero SQL, ambos estados como razón | `needs_clarification=True`, 0 SQL, `intent.state is None`, el span cubre `cancelados` y `borradores` | ✅ |
| `registros borradores y cancelados…` | idem | idem | ✅ |
| un solo `cancelados` | ejecuta | 10 filas `Cancelado` | ✅ |
| un solo `en borrador` | ejecuta | 10 filas `Borrador` | ✅ |
| `contratos celebrados y cancelados…` | aclarar | aclara, 0 SQL | ✅ |
| `registros cancelados y celebrados…` | aclarar | aclara, 0 SQL | ✅ |
| **`registros celebrados y cancelados…`** | aclarar | **`needs_clarification=False`, `contract_universe='source_records'`, `estado_contrato=['Cancelado']`, 1 consulta SQL ejecutada** | ❌ |
| **`registros suspendidos y cancelados…`** | aclarar | **igual: ejecuta sólo `Cancelado`** | ❌ |
| **`registros celebrados y borradores…`**, **`registros firmados y cancelados…`**, **`registros suscritos y cancelados…`**, **`registros aprobados y cancelados…`** | aclarar | **igual** | ❌ |

Es exactamente el `first-match-wins` que el contrato prohíbe, y la asimetría de
orden que el encargo exige descartar: invertir los dos estados cambia si el
sistema aclara o ejecuta.

**Causa.** En `app/core/contract_universe_intent.py:69-74`, el marcador de
celebración sólo se busca (a) tras una *ocurrencia de fuente* reconocida y (b)
en la **cola** posterior al match. `registros` no es palabra de fuente para
`source_request_occurrences` (que sólo reconoce `procesos|licitaciones|
convocatorias|procedimientos|contratos`), y el prefijo de la cláusula nunca se
examina. Con `contratos` el caso sí aclara, porque ahí sí hay ocurrencia.

**Regresión, no sólo residuo.** En V1 (`067b3977`) el conflicto se calculaba
con una búsqueda del marcador sobre **todo** el texto:

```python
conflict = bool(state and re.search(r'\b(?:firmados?|celebrados?|suscritos?)\b', normalized.text))
```

`registros celebrados y cancelados` daba `conflict=True` en V1 y da
`conflict=False` en V1.1. La remediación de F2 sustituyó una comprobación
global por dos locales y perdió cobertura en la misma superficie que remediaba.
`registros suspendidos y cancelados` ya fallaba en V1 y sigue fallando: ahí es
residuo, no regresión.

## F3 — detalle

Cadena fuente literal: `elaboración de borradores de documentos y contratos de
consultoría`, en `objeto_del_contrato` y `descripcion_del_proceso`.

| Superficie | Observado |
|---|---|
| Search (`contratos … por mas de 500 millones`) | cadena idéntica en la respuesta y en cada fila |
| Perfil Frame OFF (`analiza registros cancelados …`, ruta `entity_profile_overview`, universo `source_records`) | cadena idéntica; no aparece `y registros de consultoría` |
| Perfil Frame ON | idéntico |
| Respuesta dual (`procesos y contratos …`) | idéntica |

`record_terms` no existe en el runtime (`grep -rn record_terms app/` vacío).
Las dos sustituciones textuales que quedan —`frame_response.py:127` y
`render.py:271`— operan sobre las constantes propias `_NEXT_STEPS` y
`_SCOPE_NOUN`, literales sin interpolación de datos de la fuente; no son una
transformación equivalente a `record_terms` global.

Nota de presentación, fuera del alcance de P1/P2: la remediación también
eliminó `_short()`, con lo que los títulos de la fuente dejan de recortarse a
90 caracteres y de colapsar espacios. Está declarado en POLICY_DELTA y es
coherente con la inmutabilidad del contenido; se señala por su efecto visible.

## F4 — detalle

`apply_request(params, request(raw_query))` sobre las fronteras públicas:

| Entrada | Observado |
|---|---|
| `estado=['Cancelado'], estado_field='estado_del_procedimiento'` | `celebrated` + error de aclaración |
| `estado_contrato=['Cancelado'], raw_query='sin cancelados'` | `celebrated` + error |
| `estado=['Cancelado'], estado_family='cancelled', raw_query='contratos suspendidos'` | `celebrated` + error |
| `estado_family='no_procede'` (familia real, Cancelado+Suspendido) | `celebrated` + error |
| `estado_family='en_evaluacion'` (familia real, cuatro preparatorios) | `celebrated` + error |
| `estado_contrato=['Borrador']` | `celebrated` + error |
| `contract_universe='source_records'` sin versión | degradado a `celebrated` + «requiere revisión» |
| `contract_universe='source_records'` + versión V1 | `ValueError` de revisión antes de cualquier SQL |
| `contract_universe='source_records'` + versión `contract-universe-1.1` | admitido, sin error |
| `intent_type='direct_lookup'` | admitido |

Productores reales, no sólo la función pura: `parser → plan → persistencia →
reload` conserva `celebrated` y la versión 1.1 en el turno siguiente; la
paginación con `todos los registros del dataset` conserva `source_records` y su
versión a través del buffer y del export.

## F5 — detalle

| Caso | Exigido | Observado |
|---|---|---|
| A `analiza procesos y todos los registros del dataset de alcaldia de cartagena por mas de 500 millones` | aclarar, cero SQL en ambos hijos | `needs_clarification=True`, 0 SQL, 0 filas. OFF y ON |
| A, orden inverso (`… todos los registros del dataset y procesos …`) | idem | idem |
| B `analiza procesos y contratos de alcaldia de cartagena por mas de 500 millones` | ambos hijos; procesos sin estado contractual; comprador/monto preservados | ambos datasets consultados; `p6dx-8zbt` sin `estado_contrato` ni universo; `jbjy-vk9h` con el predicado; comprador y monto en cada WHERE. OFF y ON |
| B, orden inverso (`contratos y procesos …`) | idem | idem |
| C `procesos y registros cancelados …` y su inverso | aclarar o ejecutar ambos, nunca uno solo | aclara con cero SQL en ambos órdenes |

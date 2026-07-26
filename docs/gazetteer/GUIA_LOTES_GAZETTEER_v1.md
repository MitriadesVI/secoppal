# Guía para crear lotes del Gazetteer SECOPPAL

**Versión:** v1.0  
**Fecha:** 2026-07-08  
**Uso:** guía operativa para ampliar, reconciliar y mantener el `gazetteer` de entidades SECOPPAL sin degradar precisión.

---

## 0. Principio rector

El `gazetteer` no es una lista bonita de aliases. Es una **capa de identidad institucional verificable**.

Una entidad no entra solo porque “suena correcta”. Entra porque tiene evidencia suficiente:

```text
Registro SECOP / NIT / nombre registrado / Socrata / gates / tests / documentación
```

El objetivo no es crecer rápido a cualquier costo. El objetivo es que el sistema pueda responder mejor preguntas reales sin contaminar el parser, sin crear falsos positivos y sin romper búsquedas territoriales.

---

## 1. Qué es un lote

Un lote es una unidad controlada de actualización del `gazetteer`.

Puede ser de tres tipos:

| Tipo | Objetivo | Ejemplo |
|---|---|---|
| `expansion` | agregar entidades nuevas verificadas | alcaldías Caribe, hospitales, CARs |
| `reconciliation` | corregir o completar entidades existentes | NIT, conteos, official_name real |
| `disambiguation` | resolver colisiones de identidad | Gobernación vs Departamento, CRA vs CORPONOR |

Desde Lote 02, la prioridad debe ser:

```text
registry-first verified expansion
```

Es decir: primero identidad, luego aliases.

---

## 2. Regla de oro: no tocar query_router.py salvo necesidad extrema

El `query_router.py` debe seguir siendo parser de intención. No debe volverse un órgano de identidad institucional.

La lógica de entidades debe vivir preferentemente en:

```text
app/core/entity_resolver.py
app/core/entity_ecosystem_router.py
scripts/reconcile_*.py
tests/test_*gazetteer*.py
docs/gazetteer/*.md
```

Si un lote necesita tocar `query_router.py`, se debe justificar por escrito.

Checklist antes de tocarlo:

```text
[ ] ¿Esto es realmente parsing de intención?
[ ] ¿No puede vivir en entity_resolver.py?
[ ] ¿No puede vivir en entity_ecosystem_router.py?
[ ] ¿No puede resolverse con aliases/gates/tests?
[ ] ¿Hay test de no-regresión?
```

---

## 3. Fuentes de verificación

### 3.1 Registro SECOP

Sirve para confirmar identidad institucional:

```text
NIT
Nombre registrado
Usuarios registrados
```

El campo `Usuarios` no equivale a procesos ni contratos. Sirve como evidencia de presencia institucional, no como métrica contractual.

### 3.2 Socrata procesos / contratos

Sirve para confirmar actividad contractual:

```text
total_procesos
total_contratos
source_datasets
```

Datasets usados en el proyecto:

```text
procesos:  p6dx-8zbt
contratos: jbjy-vk9h
```

### 3.3 NIT

El NIT es una llave fuerte, pero no infalible.

Sirve para unir variantes de nombre, por ejemplo:

```text
GOBERNACIÓN DE SANTANDER
DEPARTAMENTO DE SANTANDER
NIT 890201235
```

Pero hay instituciones donde el NIT puede representar una familia institucional o varias unidades operativas, especialmente:

```text
SENA
ICBF
```

En esos casos no se debe forzar una entidad simple; se usa modelo de ecosistema.

---

## 4. Taxonomía de decisiones

Todo reconciliador debe clasificar candidatos con estados explícitos.

| Estado | Significado | Acción |
|---|---|---|
| `accepted_auto_verified` | pasa gates, tiene NIT y conteos Socrata suficientes | aplicar a `aliases_db.json` |
| `accepted_registry_verified` | identidad registral fuerte, pero Socrata no dio conteos | aplicar solo si el lote permite registry-only |
| `candidate_manual_review` | candidato plausible, pero hay ambigüedad | no aplicar |
| `manual_review_blocked_parent_only` | existe entidad madre, pero no subentidad independiente | no aplicar como canónica |
| `reject_false_positive` | falla gate duro | descartar |
| `needs_review_no_candidate` | no hay candidato útil | dejar pendiente |

Regla práctica:

```text
Solo accepted_auto_verified entra automáticamente.
Todo lo demás se documenta.
```

---

## 5. Gates obligatorios

Los gates son más importantes que el score. Un NIT con muchos contratos confirma que **ese candidato existe**, no que sea **la entidad buscada**.

### 5.1 Gate de tipo institucional

Para `alcaldia_municipal`, aceptar candidatos que contengan:

```text
ALCALDIA
ALCALDÍA
MUNICIPIO
DISTRITO
```

Rechazar candidatos que contengan:

```text
INSTITUTO
CONCEJO
PERSONERIA
PERSONERÍA
CAMARA
CÁMARA
HOSPITAL
ESE
INSTITUCION EDUCATIVA
INSTITUCIÓN EDUCATIVA
ESTABLECIMIENTO PENITENCIARIO
SENA
ICBF
```

Ejemplo:

```text
ALCALDIA MUNICIPAL DE APARTADÓ
→ NO aceptar: INSTITUTO MUNICIPAL DE DEPORTES Y RECREACIÓN DE APARTADÓ
```

### 5.2 Gate de token

El token territorial principal debe aparecer en el candidato normalizado.

```text
MAGANGUÉ → MAGANGUE
MOMPÓS → MOMPOS
SAN JUAN NEPOMUCENO → SAN JUAN NEPOMUCENO
```

Rechazo típico:

```text
SAN MARCOS SUCRE
→ NO aceptar: ALCALDIA LOCAL DE SAN CRISTOBAL
```

### 5.3 Gate de departamento

Si el candidato contiene un departamento explícito distinto, se rechaza.

```text
CANDELARIA Atlántico
→ NO aceptar: ALCALDÍA MUNICIPAL DE CANDELARIA VALLE
```

### 5.4 Gate de ambigüedad

Si hay varios municipios con el mismo nombre, no aplicar sin evidencia adicional.

Ejemplo:

```text
RIONEGRO
```

Puede referirse a varias entidades. En Lote 02B se aceptó Rionegro Antioquia solo cuando se confirmó:

```text
MUNICIPIO DE RIONEGRO
NIT 890907317
```

### 5.5 Gate NIT / Socrata

Cuando sea posible:

```text
[ ] NIT confirmado
[ ] procesos por NIT o nombre confirmado
[ ] contratos por NIT o nombre confirmado
[ ] source_datasets real
```

---

## 6. Territorio vs entidad

Separar siempre:

```text
Departamento como territorio
Gobernación / Departamento como entidad contratante
```

### 6.1 Consulta territorial

```text
contratos más caros del departamento de Santander en 2026
```

Debe interpretarse como:

```json
{
  "departamento_resolved": "Santander",
  "entidad_resolved": null,
  "year": 2026,
  "ordering_signal": "mayor_valor"
}
```

No debe limitarse a la Gobernación.

### 6.2 Consulta de entidad

```text
contratos de la Gobernación de Santander en 2026
```

Debe interpretarse como:

```json
{
  "entidad_resolved": "GOBERNACIÓN DE SANTANDER",
  "nit": "890201235",
  "year": 2026
}
```

### 6.3 Alias institucional vs filtro territorial

`DEPARTAMENTO DE SANTANDER` puede ser alias institucional de la Gobernación si el contexto dice entidad.

Pero no debe activarse automáticamente en consultas territoriales.

---

## 7. Entidades ecosistema: SENA, ICBF y similares

No todo cabe bien en el gazetteer simple.

SENA e ICBF pueden tener:

```text
sede nacional
regionales
centros
intercentros
grupo administrativo
mismo NIT en varias unidades
```

Regla:

```text
SENA/ICBF territorial → entity_ecosystem_router.py
No intentar resolverlo solo con más aliases.
```

Ejemplo:

```text
sena atlántico
```

No debería caer por orden del JSON en SENA nacional si hay contexto regional.

---

## 8. Selección de candidatos para un lote

Un lote no debe ser una lista al azar. Debe tener una tesis.

Buenas tesis:

```text
Demo/producto: entidades que usuarios piden en vivo
Caribe municipal: alcaldías relevantes para demos regionales
Manual review closure: cerrar deuda pequeña
Top volumen: entidades con muchos procesos/contratos
Colisiones: resolver xfails vivos
Equidad territorial: departamentos subrepresentados
```

Mala tesis:

```text
“Metamos 50 entidades porque sí.”
```

---

## 9. Tamaño recomendado del lote

| Tipo de lote | Tamaño sugerido |
|---|---:|
| expansión verificada | 10–25 entidades |
| reconciliación/manual review | 4–15 casos |
| disambiguation | 1–5 bugs semánticos |
| lote experimental | máximo 10 |

Lotes grandes solo si el pipeline está estable y con pruebas suficientes.

---

## 10. Pipeline estándar de lote

### Paso 0 — baseline

```bash
git status --short
python -m pytest tests/test_entity_resolver.py -q
python scripts/run_query_corpus.py --mode all
python scripts/gazetteer_manager.py validate
make lint-core
```

No iniciar lote con baseline roto.

### Paso 1 — documento de intención

Crear o actualizar:

```text
docs/gazetteer/LOTE_XX_PLAN.md
```

Debe responder:

```text
¿Qué problema resuelve?
¿Por qué ahora?
¿Qué entidades/casos entran?
¿Qué queda fuera?
¿Qué criterio de aceptación tendrá?
```

### Paso 2 — candidatos

Crear archivo de entrada:

```text
scripts/lote_XX_candidates.json
```

Campos mínimos:

```json
{
  "proposed_name": "ALCALDIA MUNICIPAL DE X",
  "departamento": "...",
  "entity_type": "alcaldia_municipal",
  "source_reason": "demo | manual_review | top_gap | product"
}
```

### Paso 3 — registry-first reconciliation

Crear script:

```text
scripts/reconcile_lote_XX_registry_first.py
```

Debe producir:

```text
scripts/reconcile_lote_XX_results.json
docs/gazetteer/LOTE_XX_REGISTRY_FIRST.md
```

### Paso 4 — aplicar solo aceptados

Solo aplicar:

```text
accepted_auto_verified
```

No aplicar:

```text
candidate_manual_review
reject_false_positive
needs_review_no_candidate
manual_review_blocked_parent_only
```

### Paso 5 — tests

Crear:

```text
tests/test_gazetteer_lote_XX.py
```

Tests mínimos:

```text
[ ] aceptados resuelven
[ ] aceptados tienen NIT
[ ] falsos positivos no resuelven
[ ] casos territoriales no se vuelven entidad
[ ] regresiones de lotes anteriores siguen pasando
```

### Paso 6 — deuda

Crear o actualizar:

```text
docs/gazetteer/lote_XX_debt.md
```

La deuda debe distinguir:

```text
manual_review
blocked_parent_only
reject_false_positive
needs_review_no_candidate
legacy debt
```

### Paso 7 — validación final

```bash
python -m pytest tests/test_entity_resolver.py tests/test_gazetteer_lote_XX.py -q
python scripts/run_query_corpus.py --mode all
python scripts/gazetteer_manager.py validate
make lint-core
```

Para lotes que tocan territorio/entidad:

```bash
python -m pytest tests/test_territorial_vs_entity.py -q
```

### Paso 8 — commit

Mensaje recomendado:

```bash
git commit -m "feat(gazetteer): add lote XX registry-first verified updates"
```

Si es bugfix de resolver:

```bash
git commit -m "fix(entity): resolve <ISSUE-ID>"
```

### Paso 9 — ADR si hay arquitectura nueva

Crear ADR si el lote introduce una regla que otros lotes deben respetar.

Ejemplos:

```text
Entity Identity Reconciliation
Territory vs Entity Disambiguation
SENA/ICBF Ecosystem Routing
```

---

## 11. Reglas de aliases

### 11.1 No usar aliases genéricos puros

Prohibidos como aliases únicos:

```text
alcaldia
municipio
gobernacion
secretaria
hospital
corporacion
instituto
fondo
empresa
entidad
```

### 11.2 Alias mínimo

Cada entidad nueva debe tener al menos 3 aliases útiles, salvo excepción documentada.

Ejemplo para alcaldía:

```text
alcaldia de magangue
alcaldia municipal de magangue
municipio de magangue
magangue bolivar
```

### 11.3 Cuidado con aliases demasiado amplios

Evitar:

```text
salud atlantico
educacion bolivar
autoridad ambiental del atlantico
corporacion autonoma regional
```

Preferir:

```text
secretaria de salud del atlantico
gobernacion del atlantico secretaria de salud
corporacion autonoma regional del atlantico
cra atlantico
```

### 11.4 Alias con departamento

El departamento puede ayudar, pero no debe volver falso el nombre canónico.

Ejemplo:

```text
official_name: ALCALDIA DE EL CARMEN DE BOLIVAR
alias: alcaldia municipal de el carmen de bolivar
alias: el carmen de bolivar
```

---

## 12. Manual review

Manual review no es fracaso. Es una defensa contra falsos positivos.

Un caso debe quedar en manual review cuando:

```text
[ ] hay varios candidatos plausibles
[ ] hay homónimos territoriales
[ ] el candidato tiene NIT pero no coincide bien en tipo/departamento
[ ] existe entidad madre pero no subentidad independiente
[ ] el nombre SECOP no contiene token suficiente
```

### 12.1 Estados de manual review

```text
candidate_manual_review
manual_review_blocked_parent_only
manual_review_blocked_ambiguous_department
manual_review_blocked_no_independent_registry
```

### 12.2 Ejemplo: Secretaría de Educación Bolívar

Si el registro solo muestra:

```text
GOBERNACIÓN DE BOLÍVAR
```

pero no:

```text
SECRETARIA DE EDUCACION DE BOLIVAR
```

entonces no se acepta como entidad independiente.

Estado:

```text
manual_review_blocked_parent_only
```

---

## 13. Métricas de aceptación del lote

Un lote bueno debe reportar:

```text
entidades procesadas
accepted_auto_verified
candidate_manual_review
reject_false_positive
needs_review_no_candidate
entidades aplicadas a DB
NITs completados
conteos completados
xfails antes/después
corpus
lint
validate warnings
```

Ejemplo de tabla:

```markdown
| Métrica | Antes | Después |
|---|---:|---:|
| Applied to DB | 11 | 16 |
| Manual review | 7 | 4 |
| XFAIL | 0 | 0 |
| Corpus | 89/89 | 89/89 |
```

---

## 14. Criterios mínimos para cerrar un lote

No cerrar si falta alguno:

```text
[ ] baseline verde
[ ] documento de lote creado
[ ] deuda documentada
[ ] tests del lote creados
[ ] accepted_auto_verified aplicados
[ ] manual_review separado
[ ] falsos positivos documentados
[ ] 0 regresiones corpus
[ ] lint limpio
[ ] validate revisado
[ ] query_router.py intacto o justificado
```

---

## 15. Red flags

Detener el lote si aparece cualquiera:

```text
[ ] el script quiere aceptar candidatos de otro departamento
[ ] el score alto contradice el tipo institucional
[ ] NIT confirma entidad distinta a la buscada
[ ] aumentan aliases genéricos
[ ] aumentan xfails
[ ] se empieza a meter lógica de identidad en query_router.py
[ ] el lote acepta más por cobertura que por evidencia
[ ] no se puede explicar por qué una entidad fue aceptada
```

---

## 16. Modelos/agentes recomendados

### Hermes + DeepSeek Pro

Útil para:

```text
lotes de datos
JSON
scripts de reconciliación
tests
documentación
limpieza de aliases
```

### Sonnet High / Opus

Usar cuando:

```text
hay que tocar arquitectura del resolver
hay ambigüedad SENA/ICBF compleja
hay cambios en Burr/workflow
hay riesgo de contaminar parser/ranking/renderer
```

### Haiku / modelos baratos

Útil para:

```text
revisión de listas
normalización simple
borradores de docs
```

Regla práctica:

```text
Datos + tests: Hermes/DeepSeek.
Arquitectura delicada: Sonnet High u Opus.
```

---

## 17. Prompt base para un lote

```text
TAREA: SECOPPAL — Lote XX Registry-first Gazetteer Update

Objetivo:
Actualizar el gazetteer con un lote pequeño y verificable usando Registro SECOP + NIT + Socrata + hard gates.

No tocar query_router.py.
No agregar entidades sin verificación.
No introducir aliases genéricos.
No aumentar xfails.
No aumentar entidades con <3 aliases.

Contexto:
- Gazetteer opera como capa de identidad institucional verificable.
- Usar taxonomía: accepted_auto_verified, candidate_manual_review, reject_false_positive, needs_review_no_candidate, manual_review_blocked_parent_only.
- Separar territorio vs entidad.
- SENA/ICBF regionales deben tratarse como ecosistema, no como aliases simples.

Pasos:
1. Confirmar baseline:
   git status --short
   pytest relevante
   python scripts/run_query_corpus.py --mode all
   python scripts/gazetteer_manager.py validate
   make lint-core

2. Crear candidates JSON.
3. Ejecutar registry-first reconciliation.
4. Aplicar solo accepted_auto_verified.
5. Crear tests.
6. Crear docs del lote y deuda.
7. Validar suite/corpus/lint.
8. Preparar commit.

Criterios:
- 0 regresiones.
- 0 nuevos xfails.
- corpus PASS.
- lint limpio.
- manual review documentado.
- query_router.py intacto.
```

---

## 18. Estructura de archivos por lote

```text
scripts/lote_XX_candidates.json
scripts/reconcile_lote_XX_registry_first.py
scripts/reconcile_lote_XX_results.json
tests/test_gazetteer_lote_XX.py
docs/gazetteer/LOTE_XX_REGISTRY_FIRST.md
docs/gazetteer/lote_XX_debt.md
```

Si hubo revisión manual:

```text
docs/gazetteer/LOTE_XX_MANUAL_REVIEW.md
tests/test_lote_XX_manual_review.py
```

Si hubo decisión arquitectónica:

```text
docs/decisions/YYYY-MM-DD_ADR-XXX_<tema>.md
docs/decisions_log.md
```

---

## 19. Cierre recomendado de lote

Resumen final esperado:

```text
Lote XX completado.

Resultado:
- N processed
- A accepted_auto_verified
- B candidate_manual_review
- C reject_false_positive
- D needs_review_no_candidate

Validación:
- tests: X passed, Y xfailed
- corpus: PASS
- lint: clean
- validate: warnings conocidos

Archivos:
- ...

Commit sugerido:
feat(gazetteer): add lote XX registry-first verified updates
```

---

## 20. Filosofía final

Un lote exitoso no es el que más entidades mete. Es el que mejora el mapa institucional sin aumentar ruido.

```text
Cobertura sin identidad = deuda.
Identidad sin tests = riesgo.
Tests sin documentación = memoria frágil.
Documentación sin criterio = burocracia.
```

La meta es construir un `gazetteer` que pueda sostener:

```text
búsqueda conversacional
radar
perfiles de entidad
alertas
warehouse
análisis territorial
análisis por NIT
```

Por eso cada lote debe dejar el sistema un poco más inteligente, no solo un poco más grande.

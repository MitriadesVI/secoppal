# Plan: A1 — Búsqueda directa por ID, URL o referencia de entidad

**Fecha:** 2026-05-25
**Feature:** A1 del Bloque A (Proponentes)
**Riesgo:** Mínimo (~3h → ~4.5h con entity_ref)
**Slug:** `direct-process-lookup`

---

## 1. Goal

Permitir al usuario buscar un proceso específico por su ID de SECOP II (ej: `CO1.NTC.2597221`) o por su URL completa (ej: `https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.2597221`).

El resultado debe ser la vista detallada del proceso, sin pasar por el pipeline normal de búsqueda (query_router → entity_resolver → soql_builder fuzzy).

---

## 2. Formato de IDs y URLs

**IDs de proceso SECOP II** (dataset p6dx-8zbt):
```
CO1.NTC.2597221      # notificación
CO1.REQ.2577563      # requisición
CO1.BDOS.848814      # borrador
CO1.PCCNTR.1000001   # contrato (en dataset jbjy-vk9h usa id_contrato)
```
Regex: `CO1\.[A-Z]{2,7}\.\d+`

**URLs de SECOP II:**
```
https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.2597221&isFromPublicArea=True
https://community.secop.gov.co/Public/Tendering/ContractNoticeManagement/Index?noticeUID=CO1.PCCNTR.1000001
```
Extraer parámetro `noticeUID`.

---

## 3. Approach

### 3.1 Detección (detección directa, sin LLM)

Agregar un módulo nuevo `app/core/direct_lookup.py` con una función:

```python
def detect_direct_lookup(raw_query: str) -> str | None:
    """Si raw_query es un ID o URL de SECOP II, devuelve el ID extraído.
    Si no, devuelve None (pipeline normal)."""
```

**Reglas de detección:**
1. Si el texto contiene `CO1.` y matchea el regex → extraer ID
2. Si es una URL de `community.secop.gov.co` → extraer `noticeUID`
3. Si no matchea → `None`

### 3.2 Ejecución de lookup (bypass del pipeline)

En el orchestrator (`app/core/orchestrator.py`), agregar un paso temprano en la acción `route_query` que:
1. Llama a `detect_direct_lookup()`
2. Si encuentra un ID, construye un SoQL directo y consulta
3. Si no, sigue el pipeline normal

**SoQL para lookup directo:**

```sql
-- Procesos (p6dx-8zbt)
SELECT id_del_proceso, referencia_del_proceso, entidad, departamento_entidad,
       ciudad_entidad, nombre_del_procedimiento, descripci_n_del_procedimiento,
       tipo_de_contrato, modalidad_de_contratacion, precio_base,
       estado_de_apertura_del_proceso, estado_del_procedimiento,
       fecha_de_publicacion_del, urlproceso
WHERE id_del_proceso = '<id>'

-- Contratos (jbjy-vk9h)
SELECT id_contrato, referencia_del_contrato, nombre_entidad, departamento,
       ciudad, objeto_del_contrato, descripcion_del_proceso,
       tipo_de_contrato, modalidad_de_contratacion, valor_del_contrato,
       estado_contrato, fecha_de_firma, proveedor_adjudicado, documento_proveedor,
       urlproceso
WHERE id_contrato = '<id>'
```

**Estrategia de búsqueda:**
- Intentar primero en procesos (p6dx-8zbt) por `id_del_proceso`
- Si 0 resultados, intentar en contratos (jbjy-vk9h) por `id_contrato`
- Si 0 resultados en ambos → "Proceso no encontrado" con mensaje amigable

### 3.3 Integración en Streamlit

En `streamlit_app.py`, la barra de búsqueda acepta el ID/URL directamente. El flujo es:
1. Usuario pega `CO1.NTC.2597221` o la URL completa
2. El orchestrator detecta el lookup directo
3. Se devuelve el resultado como una card individual (o cards si se encontró en ambos datasets)
4. El formateador actual (`_render_cards`) ya funciona con los campos que devuelve el lookup

---

## 4. Files to Change

| Archivo | Cambio | Líneas |
|---------|--------|--------|
| `app/core/direct_lookup.py` | **NUEVO** — detección + extracción de ID | ~50 |
| `app/core/orchestrator.py` | Agregar early-return en `route_query` si es lookup directo | ~20 |
| `tests/test_direct_lookup.py` | **NUEVO** — tests unitarios de detección | ~60 |
| `tests/test_sprint2a_canonical_smoke.py` | Agregar 1-2 casos de smoke test | ~10 |

**Archivos que NO se tocan:**
- `query_router.py` (no participa en el lookup directo)
- `entity_resolver.py` (no participa)
- `soql_builder.py` (la query es determinística, no pasa por `build()`)
- `secop_client.py` (se usa el método `query()` existente)
- `formatter.py` (los campos devueltos son los mismos que ya conoce)

---

## 5. Tests

### Unitarios (`tests/test_direct_lookup.py`)
```python
def test_detect_id_co1_ntc():
    assert detect_direct_lookup("CO1.NTC.2597221") == "CO1.NTC.2597221"

def test_detect_id_co1_req():
    assert detect_direct_lookup("CO1.REQ.2577563") == "CO1.REQ.2577563"

def test_detect_id_co1_bdos():
    assert detect_direct_lookup("CO1.BDOS.848814") == "CO1.BDOS.848814"

def test_detect_id_co1_pccntr():
    assert detect_direct_lookup("CO1.PCCNTR.1000001") == "CO1.PCCNTR.1000001"

def test_detect_url_noticeuid():
    url = "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.2597221"
    assert detect_direct_lookup(url) == "CO1.NTC.2597221"

def test_detect_url_with_extra_params():
    url = "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.2597221&isFromPublicArea=True&isModal=true"
    assert detect_direct_lookup(url) == "CO1.NTC.2597221"

def test_no_detect_normal_query():
    assert detect_direct_lookup("mantenimiento vial en Bolívar") is None

def test_no_detect_partial_id():
    assert detect_direct_lookup("CO1") is None
    assert detect_direct_lookup("NTC.2597221") is None

def test_no_detect_other_url():
    assert detect_direct_lookup("https://google.com") is None
```

### Smoke test (end-to-end)
Agregar al corpus de smoke tests 2 casos con IDs reales conocidos (ej: `CO1.NTC.2597221`, `CO1.BDOS.848814`) que se sepa existen en Socrata.

---

## 6. Edge Cases

| Caso | Comportamiento |
|------|---------------|
| ID existe en procesos | Devuelve 1 card de proceso |
| ID existe solo en contratos | Devuelve 1 card de contrato |
| ID no existe en ningún dataset | Mensaje: "No se encontró el proceso `{id}` en SECOP II" |
| URL sin `noticeUID` | Pasa al pipeline normal |
| ID + texto adicional | Pasa al pipeline normal (detección estricta: solo ID o URL) |
| `CO1.PCCNTR.XXXXX` | Buscar en ambos datasets (es contrato en jbjy-vk9h) |

---

## 7. Rollout

1. Implementar `direct_lookup.py` + tests unitarios → verificar verde
2. Integrar en `orchestrator.py` → verificar smoke tests
3. Probar manualmente en Streamlit con IDs reales
4. Smoke test a las 4 AM (martes) para validar contra API real

---

## 8. Risks

- **Ninguno funcional:** El bypass solo se activa cuando la detección matchea. Si no matchea, el pipeline normal sigue intacto.
- **Riesgo de falsos positivos:** Mínimo. El regex `CO1\.[A-Z]{2,7}\.\d+` es muy específico. Solo IDs reales de SECOP II lo matchean.
- **Riesgo de regresión:** Cero. No modifica ninguna función existente, solo agrega un early-return.

---

## 9. No incluido (out of scope)

- Búsqueda por `referencia_del_proceso` (ej: "LP-2026-001") ~~— eso requiere fuzzy matching → feature separada~~ → **INCLUIDO en A1-COMPLETION**
- Historial de consultas por ID
- Búsqueda por NIT de contratista

---

## 10. Decisions (post-review adjustments 2026-05-25)

1. **Detección NO estricta** — usar `re.search()` en lugar de `re.match()`.
   Si el texto contiene exactamente UN ID o URL, hacer bypass.
   Si contiene 0 → pipeline normal.
   Si contiene 2+ → pipeline normal con mensaje "múltiples IDs detectados, búscalos uno a la vez".

2. **Mapping prefijo → dataset:**
   ```
   NTC, REQ, BDOS, CONS, PPROC, AAPPM → p6dx-8zbt (procesos)
   PCCNTR → jbjy-vk9h (contratos)
   ```
   Single round-trip cuando prefijo es conocido. Fallback secuencial solo si prefijo desconocido.

3. **`route_reason` value:**
   - `"direct_lookup_id"` cuando vino de ID puro.
   - `"direct_lookup_url"` cuando vino de URL parseada.
   - `"direct_lookup_entity_ref"` cuando vino de referencia interna de entidad (match único).
   - `"entity_ref_disambiguation"` cuando hay múltiples matches para una referencia.
   Permite analytics post-cohort sobre uso de lookup directo vs búsqueda natural.

4. **UX rich detail en Streamlit:**
   - Cuando `route_reason` starts with `"direct_lookup"`, render con todos los campos sin truncar.
   - Sección "Información completa" expandida por default.
   - Link "Ver en SECOP" prominente.
   - Si proceso adjudicado, sugerir lookup del contrato relacionado.

5. **Bypass es estricto:** solo IDs SECOP determinísticos (CO1.XXX.YYY), URLs `community.secop.gov.co`, y referencias internas con prefijos conocidos (LP, LIC, SAMC, MC, CMC, CD, IP, etc.).

6. **Ambos formatos son ciudadanos de primera clase.** En SECOP II, cada proceso tiene DOS identificadores visibles: número electrónico (CO1.XXX.YYY) y referencia interna (CD-XX-YYYY). El usuario típico copia indistintamente cualquiera de los dos. Reconocer solo uno era diseño incompleto. Feature A1 se cierra a su alcance real con soporte de referencias internas vía LIKE (no equality, porque el dataset no garantiza formato exacto).

7. **Validación de referencias:** mínimo 2 segmentos significativos (prefijo + número/código). `CD` solo o `5534` solo no califican. Esto evita falsos positivos con texto natural.

8. **Normalización de referencias:** uppercase + espacios→guiones para el LIKE, pero el dataset puede tener variaciones de formato. El LIKE captura todas.

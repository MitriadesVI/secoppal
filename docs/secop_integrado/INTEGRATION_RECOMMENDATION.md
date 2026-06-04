# INTEGRATION_RECOMMENDATION — SECOP Integrado (rpmr-utcd)

**Fecha:** 2026-05-28
**Versión:** 1.1 (reclasificación C0.2 — active legacy source)
**Estado:** AUDIT ONLY — NO INTEGRAR TODAVÍA

---

## Veredicto

**rpmr-utcd es el dataset consolidado de contratos SECOP I + II, con valor
tanto histórico como operativo para contratos SECOP I que siguen cargándose
en 2026.** Debe integrarse como dataset complementario a `jbjy-vk9h`
(contratos SECOP II en vivo), no como reemplazo.

---

## ¿Qué resuelve?

| Problema actual | Cómo lo resuelve rpmr-utcd |
|----------------|---------------------------|
| Sin contratos de SECOP I en búsquedas | 74,933 contratos SECOP I en 2026 (COP 3.155.203.479.424, ~3,2 billones) |
| Búsqueda de contratos solo en SECOP II | Datos unificados de ambas plataformas |
| Sin trazabilidad histórica pre-2023 | Datos desde 2021 con ~1.5M contratos/año |
| SECOP I legacy todavía produce contratos | Filtro `origen=SECOPI` + `fecha_de_firma_del_contrato >= 2026` |

## ¿Qué NO resuelve?

- **Oportunidades abiertas**: Usar `p6dx-8zbt` para SECOP II, `f789-7hwg` para SECOP I.
- **Procesos precontractuales ricos**: rpmr-utcd tiene estados precontractuales
  pero sin fecha de publicación ni detalle UNSPSC. Para trazabilidad SECOP I
  usar `f789-7hwg`.

---

## Estrategia de Integración Recomendada

### Rol: Dataset Consolidado Histórico + Operativo Legacy

### Fase 1: Búsqueda unificada de contratos (cuando se active)

```python
# Pseudocódigo — NO IMPLEMENTAR AHORA
def search_contracts(query: SearchQuery) -> dict:
    """Búsqueda unificada de contratos SECOP I + II."""
    results = {"secop_ii": [], "secop_i": []}

    # 1. Contratos SECOP II recientes (jbjy-vk9h)
    results["secop_ii"] = secop_client.search(DATASET_CONTRATOS, query)

    # 2. Contratos SECOP I + II consolidados (rpmr-utcd)
    #    con filtro de estados post-adjudicación
    integrado = secop_client.search(
        DATASET_INTEGRADO,
        query,
        extra_where="estado_del_proceso IN ('Celebrado','Liquidado','En ejecución','Cerrado')"
    )
    # Separar por origen para mostrarlos distintamente
    results["secop_i"] = [r for r in integrado if r["origen"] == "SECOPI"]
    results["secop_ii_hist"] = [r for r in integrado if r["origen"] == "SECOPII"]

    # 3. Merge y deduplicar jbjy-vk9h ↔ rpmr-utcd SECOP II por numero_de_proceso
    return merge_and_dedup(results)
```

### Fase 2: Contratos SECOP I recientes (operativo)

Para contratos SECOP I que siguen cargándose en 2026:

```python
# Consultar rpmr-utcd con filtros explícitos
params = {
    "origen": "SECOPI",
    "fecha_desde": "2026-01-01",
    "estado": ["Celebrado", "Liquidado", "En ejecución"],
}
```

### Fase 3: Filtros por plataforma

Permitir al usuario acotar:
- "contratos del SENA en SECOP I" → `origen = 'SECOPI'` en rpmr-utcd
- "contratos de Bogotá en SECOP II" → `jbjy-vk9h` o `origen = 'SECOPII'` en rpmr-utcd

---

## Precauciones Técnicas

1. **Timeout**: rpmr-utcd requiere ≥60s. No usar el mismo cliente que para p6dx-8zbt/jbjy-vk9h (15s).
2. **Volumen**: 21.9M filas. Siempre filtrar por estado y/o fecha antes de consultar.
3. **Estados precontractuales**: No asumir que todo registro tiene contrato. La descripción oficial miente.
4. **"NO DEFINIDO"**: Mapear a `None`, no mostrar al usuario.
5. **Años inválidos**: Filtrar `WHERE year < 2030` para excluir los ~100 registros con fechas erróneas (2044-2099).
6. **URLs**: Las URLs apuntan a `contratos.gov.co` (formato SECOP I), incluso para registros SECOP II.
7. **SECOP I activo**: Para contratos SECOP I 2026, usar `origen=SECOPI` + `fecha_de_firma_del_contrato >= '2026-01-01'`.

---

## Decisión

**APROBADO para uso futuro como dataset consolidado de contratos SECOP I + II
(histórico + operativo legacy).**

**No activar hasta que:**
- [ ] Exista un mecanismo de merge/dedup con jbjy-vk9h
- [ ] Los timeouts estén calibrados (instancia Socrata separada, 60s+)
- [ ] El formateador distinga contratos SECOP I legacy de contratos SECOP II
- [ ] Se defina cómo mostrar URLs de SECOP I al usuario (contratos.gov.co es legacy)
- [ ] Se implemente separación por plataforma en resultados (SECOP II / SECOP I legacy)

**Mientras tanto:** Solo documentado. Cero cambios en `query_router.py`, `soql_builder.py`, `orchestrator.py`.

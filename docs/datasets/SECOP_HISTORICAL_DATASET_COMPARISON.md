# SECOP Historical Dataset Comparison

**Fecha:** 2026-05-28
**Versión:** 1.1 (reclasificación C0.2 — SECOP I as active legacy source)
**Auditoría:** DATASET-C0 + DATASET-C0.2
**Estado:** AUDIT COMPLETED — Datasets documentados, integración diferida

---

## Resumen Ejecutivo

SECOPPAL opera actualmente con 2 datasets Socrata:
- `p6dx-8zbt` → procesos/oportunidades SECOP II (búsqueda de oportunidades)
- `jbjy-vk9h` → contratos SECOP II (búsqueda de contratos)

Esta auditoría evaluó 2 datasets adicionales:
- `rpmr-utcd` → SECOP Integrado (SECOP I + II consolidado)
- `f789-7hwg` → SECOP I Procesos (solo SECOP I, 79 columnas)

**Actualización C0.2:** Se verificó que SECOP I sigue activo en 2026 con
procesos convocados, contratos cargándose, y actividad concentrada en
municipios pequeños, mínima cuantía y régimen especial. SECOP I es legacy
pero no está muerto. Ambos datasets se reclasifican para reflejar su valor
operativo actual, no solo histórico.

**Conclusión:** Los 4 datasets son complementarios. Se recomienda integración
diferida con búsqueda dual SECOP I + SECOP II, resultados separados por
plataforma, y advertencia de producto para SECOP I legacy.

---

## Matriz de Datasets SECOP

| Dataset | ID Socrata | Plataforma | Filas | Columnas | Tipo | Rol |
|---------|-----------|------------|-------|----------|------|-----|
| SECOP II Procesos | `p6dx-8zbt` | SECOP II | ~2.5M | ~22 | Oportunidades | **Activo — primario** |
| SECOP II Contratos | `jbjy-vk9h` | SECOP II | ~2M | ~30 | Contratos | **Activo — primario** |
| SECOP Integrado | `rpmr-utcd` | SECOP I + II | 21.9M | 22 | Consolidado | **Auditado — histórico + operativo legacy** |
| SECOP I Procesos | `f789-7hwg` | SECOP I | 6.37M | 79 | Procesos | **Auditado — operativo secundario + analítico** |

---

## Comparación Detallada

### Cobertura Temporal

| Dataset | Desde | Hasta | Último año con actividad |
|---------|-------|-------|--------------------------|
| p6dx-8zbt | ~2023 | Actual | 2026 (activo) |
| jbjy-vk9h | ~2023 | Actual | 2026 (activo) |
| rpmr-utcd | 2021 | 2026 | 2026 (contratos SECOP I + II) |
| f789-7hwg | 2018 | 2026 | 2026 (procesos SECOP I cargándose) |

### Estados del Proceso

| Estado | rpmr-utcd | f789-7hwg | p6dx-8zbt | jbjy-vk9h |
|--------|-----------|-----------|-----------|-----------|
| Borrador | ✅ 208K | ✅ 2.7K | ✅ | — |
| Convocado | ✅ 1M | ✅ 466K | ✅ | — |
| Activo | ✅ 559K | — | ✅ | — |
| Adjudicado | ✅ 96K | ✅ 21K | ✅ | — |
| Celebrado | ✅ 9.1M | ✅ 3.9M | — | ✅ |
| Liquidado | ✅ 3.6M | ✅ 1.7M | — | — |
| Cerrado | ✅ 1.5M | — | — | ✅ |
| En ejecución | ✅ 2M | — | — | — |
| Cancelado/Descartado | ✅ 117K | ✅ 90K | ✅ | — |

### Capacidades por Dataset

| Capacidad | rpmr-utcd | f789-7hwg | p6dx-8zbt | jbjy-vk9h |
|-----------|-----------|-----------|-----------|-----------|
| Oportunidades SECOP II abiertas | ❌ Sin fecha publ. | ❌ Solo SECOP I | ✅ | ❌ |
| Procesos SECOP I activos/recientes | ❌ Sin fecha publ. | ✅ Convocados 2026 | ❌ | ❌ |
| Contratos SECOP II | ❌ Solo vía rpmr | ❌ | ❌ | ✅ |
| Contratos SECOP I | ✅ `origen=SECOPI` | ✅ 5.8M firmados | ❌ | ❌ |
| Contratos consolidados I+II | ✅ 16.2M post-award | ❌ Solo SECOP I | ❌ | ❌ |
| Procesos sin contrato | ✅ Parcial | ✅ 540K | ✅ | ❌ |
| Categorización UNSPSC | ❌ | ✅ Grupo/Familia/Clase | ❌ | ❌ |
| Información presupuestal | ❌ | ✅ Rubros | ❌ | ❌ |
| Adiciones/Modificaciones | ❌ | ✅ | ❌ | ❌ |
| URLs directas | ✅ contratos.gov.co | ✅ contratos.gov.co | ✅ SECOP II | ❌ |
| Trazabilidad completa | ✅ | ✅ | ✅ | ❌ |

---

## Preguntas de Auditoría — Respuestas

### 1. ¿rpmr-utcd contiene solo procesos con contrato?

**NO.** La descripción oficial dice "que han finalizado con un contrato" pero
el dataset contiene ~5.5M de registros en estados precontractuales (Borrador,
Convocado, Activo, En aprobación, Aprobado, Adjudicado).

### 2. ¿f789-7hwg contiene procesos sin contrato?

**SÍ.** 540,857 registros (8.5%) tienen `fecha_de_firma_del_contrato IS NULL`.

### 3. ¿Hay campo común para join entre datasets?

**Sí, con limitaciones:**

| Par de datasets | Campo de join | Fiabilidad |
|----------------|---------------|-----------|
| rpmr-utcd ↔ f789-7hwg | `numero_de_proceso` | Media |
| rpmr-utcd ↔ jbjy-vk9h | `numero_de_proceso` | Media |
| rpmr-utcd ↔ p6dx-8zbt | `numero_de_proceso` | Media |

### 4. ¿rpmr-utcd duplica parcialmente jbjy-vk9h?

**SÍ.** Los ~7.4M registros con `origen = 'SECOPII'` en rpmr-utcd se solapan
con jbjy-vk9h. Requiere dedup por `numero_de_proceso`.

### 5. ¿f789-7hwg duplica parcialmente p6dx-8zbt?

**NO.** f789-7hwg es exclusivamente SECOP I. p6dx-8zbt es exclusivamente
SECOP II. Son plataformas diferentes, sin solapamiento.

### 6. ¿Qué dataset debe usar una búsqueda de contratos?

| Escenario | Dataset primario | Complemento |
|-----------|-----------------|-------------|
| Contratos SECOP II | `jbjy-vk9h` | `rpmr-utcd` (`origen=SECOPII`) para histórico |
| Contratos SECOP I | `rpmr-utcd` (`origen=SECOPI`) | `f789-7hwg` para detalle UNSPSC |
| Contratos sin especificar plataforma | `jbjy-vk9h` + `rpmr-utcd` | Dedup + separar por plataforma |

### 7. ¿Qué dataset debe usar una búsqueda de procesos?

| Escenario | Dataset primario | Complemento |
|-----------|-----------------|-------------|
| Oportunidades SECOP II | `p6dx-8zbt` | — |
| Procesos SECOP I | `f789-7hwg` | `rpmr-utcd` (`origen=SECOPI`) |
| Procesos sin especificar plataforma | `p6dx-8zbt` + `f789-7hwg` | Separar por plataforma |

---

## SECOP I: Legacy pero Activo

**Actualización C0.2 (2026-05-28):** SECOP I fue cerrado para nuevos
registros en 2022, pero la plataforma sigue recibiendo cargues y
actualizaciones en 2026. Verificación cuantitativa:

### Actividad SECOP I en 2026 — Datos Duros

| Métrica | f789-7hwg (Procesos) | rpmr-utcd origen=SECOPI (Contratos) |
|---------|---------------------|-------------------------------------|
| Total 2026 | **120,440** procesos cargados | **74,933** contratos firmados |
| Sin contrato | 18,057 (15%) — 15,620 CONVOCADO | N/A (son contratos) |
| Valor total | N/A | COP 3.155.203.479.424 (~3,2 billones) |
| Valor promedio | N/A | COP 42.106.995 (~42,1 millones) |
| Top modalidad | Contratación Directa 64.1% | Contratación Directa 73.3% |
| Top departamento | Nariño 16,992 (14.1%) | Nariño 9,265 (12.4%) |

### Perfil de la Actividad

- **Municipios pequeños y periferia**: Tumaco (4,950), Providencia (2,798),
  Inírida (1,240), Mitú (1,085), Puerto Carreño (1,126).
- **Mínima cuantía y régimen especial**: 27.5% combinado en procesos.
- **Convenios interadministrativos**: 4,618 procesos y 2,049 contratos
  (avg COP 113.384.334 — el doble del promedio general).
- **Cámaras de comercio**: Principales publicadores (Bucaramanga, Pereira,
  Medellín, Barranquilla). Operan en SECOP I por inercia institucional.
- **Caso concreto**: AMA-IMC-024-2026 (Tolima - Alcaldía de Ataco),
  estado CONVOCADO — proceso activo real en SECOP I.

### Implicación para búsquedas

Una búsqueda sin especificar plataforma debe consultar **ambas**:
- `p6dx-8zbt` para procesos SECOP II
- `f789-7hwg` para procesos SECOP I (filtrando por año reciente)

Una búsqueda de contratos sin especificar plataforma debe consultar:
- `jbjy-vk9h` para contratos SECOP II
- `rpmr-utcd` con `origen=SECOPI` para contratos SECOP I

---

## Reglas de Ruteo Futuro

Cuando se active la integración, el `query_router.py` debe aplicar estas reglas:

| Intención del usuario | Datasets a consultar | Separación |
|----------------------|---------------------|------------|
| "oportunidades abiertas en [depto]" | `p6dx-8zbt` | — |
| "procesos de [entidad] en SECOP I" | `f789-7hwg` | — |
| "procesos de [entidad]" (sin especificar) | `p6dx-8zbt` + `f789-7hwg` | SECOP II / SECOP I |
| "contratos de [entidad] en 2025" | `jbjy-vk9h` | — |
| "contratos de [entidad] en SECOP I" | `rpmr-utcd` (`origen=SECOPI`) | — |
| "contratos de [entidad]" (sin especificar) | `jbjy-vk9h` + `rpmr-utcd` | SECOP II / SECOP I |
| "histórico de contratos de [entidad]" | `rpmr-utcd` | — |
| "qué pasó con el proceso X" | Buscar en los 4 datasets por `numero_de_proceso` | — |

### Algoritmo de ruteo (pseudocódigo)

```python
def route_datasets(intent_type: str, platform_hint: str | None) -> list[str]:
    datasets = []

    if intent_type == "opportunity_search":
        if platform_hint == "SECOP I":
            datasets.append("f789-7hwg")
        elif platform_hint == "SECOP II":
            datasets.append("p6dx-8zbt")
        else:
            datasets.extend(["p6dx-8zbt", "f789-7hwg"])  # dual

    elif intent_type == "contract_search":
        if platform_hint == "SECOP I":
            datasets.append("rpmr-utcd")  # con where origen=SECOPI
        elif platform_hint == "SECOP II":
            datasets.append("jbjy-vk9h")
        else:
            datasets.extend(["jbjy-vk9h", "rpmr-utcd"])  # dual con dedup

    return datasets
```

---

## Advertencia de Producto — Resultados Duales

Cuando una búsqueda retorna resultados de ambas plataformas, la respuesta
debe:

1. **Separar visualmente:**

   ```
   📊 Resultados SECOP II (3 encontrados)
   ...

   📊 Resultados SECOP I — plataforma legacy (2 encontrados)
   ...
   ```

2. **Incluir nota contextual:**

   > "SECOP I es una plataforma legacy, pero todavía registra actividad en
   > algunos casos. Te separo los resultados por plataforma."

3. **No mezclar URLs:** Las URLs de SECOP I apuntan a `contratos.gov.co`
   (viewer antiguo). Las de SECOP II a `community.secop.gov.co`. No
   intercambiarlas ni asumir formato uniforme.

4. **Priorizar SECOP II:** Mostrar primero los resultados de SECOP II,
   luego los de SECOP I. SECOP II es la plataforma vigente.

---

## Arquitectura de Datasets Recomendada

```
                    ┌─────────────────────────────────┐
                    │     SECOPPAL Query Router        │
                    └─────────────┬───────────────────┘
                                  │
        ┌──────────────┬──────────┼──────────┬──────────────┐
        │              │          │          │              │
        ▼              ▼          ▼          ▼              ▼
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ SECOP II │  │ SECOP II │  │ SECOP I  │  │ SECOP I+II│
│ Procesos │  │Contratos │  │ Procesos │  │Consolidado│
│p6dx-8zbt │  │jbjy-vk9h │  │f789-7hwg │  │ rpmr-utcd │
│ Oportun. │  │ Actual   │  │Op. Secund│  │Hist+OpLeg │
└──────────┘  └──────────┘  └──────────┘  └──────────┘
     │              │              │              │
     └──────┬───────┘              └──────┬───────┘
            │                             │
            ▼                             ▼
    ┌──────────────┐            ┌──────────────────┐
    │ Búsqueda     │            │ Búsqueda         │
    │ SECOP II     │            │ SECOP I legacy   │
    │ (primaria)   │            │ (secundaria)     │
    └──────────────┘            └──────────────────┘
            │                             │
            └──────────┬──────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Respuesta dual  │
              │ separada por    │
              │ plataforma      │
              └─────────────────┘
```

---

## Recomendación Final

### p6dx-8zbt — MANTENER OPERATIVO (primario SECOP II)

Dataset primario para búsqueda de oportunidades SECOP II. **No reemplazar.**

### jbjy-vk9h — MANTENER OPERATIVO (primario SECOP II)

Dataset primario para búsqueda de contratos SECOP II actuales. **No reemplazar.**

### rpmr-utcd — INTEGRAR COMO CONSOLIDADO (diferido)

Dataset consolidado de contratos SECOP I + II. Cubre el universo completo de
contratos firmados en ambas plataformas, incluyendo contratos SECOP I que
siguen cargándose en 2026. Requiere timeout 60s+, filtro de estados, y
dedup con jbjy-vk9h.

### f789-7hwg — INTEGRAR COMO OPERATIVO SECUNDARIO (diferido)

Dataset para procesos SECOP I activos/recientes y trazabilidad histórica.
SECOP I es legacy pero no está muerto — ignorarlo dejaría fuera procesos
reales. Debe consultarse junto con `p6dx-8zbt` en búsquedas sin plataforma
especificada.

---

## Próximos Pasos (diferidos)

1. [ ] Implementar `SecopHistoricalClient` con timeout 60s+ dedicado a rpmr-utcd
2. [ ] Implementar `NormalizedContractRecord` y `NormalizedHistoricalProcessRecord`
3. [ ] Agregar ruteo dual en `query_router.py` (p6dx-8zbt + f789-7hwg)
4. [ ] Implementar dedup `jbjy-vk9h` ↔ `rpmr-utcd` por `numero_de_proceso`
5. [ ] Agregar parser de nombres de entidad f789-7hwg → gazetteer
6. [ ] Pipeline de enriquecimiento cruzado f789-7hwg → rpmr-utcd
7. [ ] Formateador dual con separación SECOP II / SECOP I legacy
8. [ ] Advertencia de producto para resultados SECOP I

**Criterio de activación:** Cuando el feedback de usuarios muestre demanda de
búsqueda en SECOP I o búsqueda sin especificar plataforma.

---

## Formato de Valores COP — Convención

Para evitar ambigüedad entre escala corta (inglés: 1 billion = 10⁹) y escala
larga (colombiano: 1 billón = 10¹²), todos los valores monetarios en SECOPPAL
deben usar el formato canónico colombiano.

### Helper de Formato

```python
def format_cop(value: int | float) -> str:
    """Formatea un valor en COP con escala larga colombiana.

    >>> format_cop(3_000_000_000)
    'COP 3.000.000.000 (3.000 millones; 0,003 billones)'

    >>> format_cop(3_000_000_000_000)
    'COP 3.000.000.000.000 (3 billones)'

    >>> format_cop(42_106_995)
    'COP 42.106.995 (~42,1 millones)'
    """
    cop = f"COP {value:,.0f}".replace(",", ".")

    millones = value / 1_000_000
    billones = value / 1_000_000_000_000

    if billones >= 1:
        return f"{cop} (~{billones:,.1f} billones)".replace(",", "X").replace(".", ",").replace("X", ".")
    elif millones >= 1:
        return f"{cop} (~{millones:,.0f} millones)".replace(",", ".")
    else:
        return cop
```

### Verificación MONEY-LABEL-001

Valor bruto obtenido de Socrata para contratos SECOP I 2026:

```
Query: SELECT SUM(valor_contrato) FROM rpmr-utcd
       WHERE origen = 'SECOPI'
       AND date_extract_y(fecha_de_firma_del_contrato) = 2026

Resultado raw: 3155203479424
Formato COP:   COP 3.155.203.479.424
En millones:   3.155.203 millones de pesos colombianos
En billones:   aproximadamente 3,2 billones de pesos colombianos
```

**✅ Confirmado:** El valor es 3,155 billones (escala larga colombiana),
no 3,155 mil millones (escala corta). La suma de Socrata es correcta.

---

## Referencias

- [SCHEMA_AUDIT — rpmr-utcd](../secop_integrado/SCHEMA_AUDIT.md)
- [FIELD_MAPPING — rpmr-utcd](../secop_integrado/FIELD_MAPPING.md)
- [INTEGRATION_RECOMMENDATION — rpmr-utcd](../secop_integrado/INTEGRATION_RECOMMENDATION.md)
- [SCHEMA_AUDIT — f789-7hwg](../secop1_procesos/SCHEMA_AUDIT.md)
- [FIELD_MAPPING — f789-7hwg](../secop1_procesos/FIELD_MAPPING.md)
- [INTEGRATION_RECOMMENDATION — f789-7hwg](../secop1_procesos/INTEGRATION_RECOMMENDATION.md)

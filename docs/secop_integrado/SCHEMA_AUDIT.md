# SCHEMA_AUDIT — SECOP Integrado (rpmr-utcd)

**Dataset ID:** `rpmr-utcd`
**Nombre oficial:** SECOP Integrado
**URL humana:** https://www.datos.gov.co/Estad-sticas-Nacionales/SECOP-Integrado/rpmr-utcd/about_data
**API base:** https://www.datos.gov.co/resource/rpmr-utcd.json
**Última actualización (timestamp):** 1779978085
**Fecha de auditoría:** 2026-05-28

---

## Descripción Oficial

> Información integrada de los procesos de compra pública que se han registrado
> en las plataformas SECOP I y II, que han finalizado con un contrato.

**⚠️ La descripción oficial es incorrecta.** El dataset contiene procesos en
estados precontractuales (Borrador, Convocado, En aprobación, Activo) con
contratista "NO DEFINIDO". NO está limitado a procesos con contrato firmado.

---

## Estadísticas

| Métrica | Valor |
|---------|-------|
| Filas totales | 21,922,905 |
| SECOP I | 14,516,629 (66.2%) |
| SECOP II | 7,406,276 (33.8%) |
| Columnas | 22 |
| Rango de años (firma) | 2021–2026 (+ datos erróneos 2044–2099) |
| Pico anual | 2025: ~1,992,499 |

---

## Columnas

| # | Campo | Tipo Socrata | Descripción |
|---|-------|-------------|-------------|
| 1 | `nivel_entidad` | text | Nivel: NACIONAL / TERRITORIAL |
| 2 | `codigo_entidad_en_secop` | text | Código de entidad en SECOP |
| 3 | `nombre_de_la_entidad` | text | Nombre de la entidad compradora |
| 4 | `nit_de_la_entidad` | text | NIT de la entidad |
| 5 | `departamento_entidad` | text | Departamento de la entidad |
| 6 | `municipio_entidad` | text | Municipio de la entidad |
| 7 | `estado_del_proceso` | text | Estado actual del proceso |
| 8 | `modalidad_de_contrataci_n` | text | Modalidad de contratación |
| 9 | `objeto_a_contratar` | text | Objeto (catálogo UNSPSC) |
| 10 | `objeto_del_proceso` | text | Descripción detallada del objeto |
| 11 | `tipo_de_contrato` | text | Tipo de contrato |
| 12 | `fecha_de_firma_del_contrato` | calendar_date | Fecha de firma del contrato |
| 13 | `fecha_inicio_ejecuci_n` | calendar_date | Inicio de ejecución |
| 14 | `fecha_fin_ejecuci_n` | calendar_date | Fin de ejecución |
| 15 | `numero_del_contrato` | text | ID del contrato |
| 16 | `numero_de_proceso` | text | ID del proceso |
| 17 | `valor_contrato` | number | Valor del contrato |
| 18 | `nom_raz_social_contratista` | text | Nombre del contratista |
| 19 | `url_contrato` | text | URL en contratos.gov.co |
| 20 | `origen` | text | Plataforma: SECOPI o SECOPII |
| 21 | `tipo_documento_proveedor` | text | Tipo de documento del proveedor |
| 22 | `documento_proveedor` | text | Número de documento del proveedor |

---

## Distribución por Estado del Proceso

| Estado | Conteo | % | Tipo |
|--------|--------|---|------|
| Celebrado | 9,089,950 | 41.5% | Post-adjudicación |
| Liquidado | 3,601,666 | 16.4% | Post-adjudicación |
| En ejecución | 1,965,382 | 9.0% | Post-adjudicación |
| Cerrado | 1,543,911 | 7.0% | Post-adjudicación |
| Convocado | 1,014,968 | 4.6% | Pre-adjudicación |
| Activo | 559,454 | 2.6% | Pre-adjudicación |
| Aprobado | 453,399 | 2.1% | Pre-adjudicación |
| Borrador | 208,354 | 1.0% | Pre-adjudicación |
| Adjudicado | 95,639 | 0.4% | Transición |
| Cancelado | 85,079 | 0.4% | Terminado |
| cedido | 44,174 | 0.2% | Post-adjudicación |
| enviado Proveedor | 34,642 | 0.2% | Pre-adjudicación |
| En aprobación | 19,155 | 0.1% | Pre-adjudicación |
| Descartado | 31,795 | 0.1% | Terminado |

---

## Limitaciones y Riesgos

1. **Descripción oficial incorrecta**: No filtrar por defecto asumiendo "solo contratos".
2. **Datos erróneos en fechas**: ~100 registros con años 2044-2099 en `fecha_de_firma_del_contrato`. Requiere filtro `WHERE year < 2030`.
3. **"NO DEFINIDO" como valor centinela**: `objeto_a_contratar`, `nom_raz_social_contratista`, `tipo_documento_proveedor`, `documento_proveedor` usan "NO DEFINIDO" cuando el dato no existe.
4. **Volumen**: 21.9M filas. Las queries agregadas (COUNT, GROUP BY) son lentas (timeout frecuente a 15s). Requiere timeout ≥60s.
5. **URL formato SECOP I**: Las URLs de `origen=SECOPI` apuntan a `contratos.gov.co`, no a `community.secop.gov.co`. Las URLs de `origen=SECOPII` también usan formato SECOP I.
6. **Sin campo `codigo_entidad`**: Usa `codigo_entidad_en_secop` y `nit_de_la_entidad` como identificadores.
7. **Sin fecha de publicación/apertura**: Solo tiene fechas relacionadas al contrato (firma, inicio, fin ejecución). No tiene fecha de publicación del proceso.
8. **Origen usa "SECOPI" / "SECOPII"**: Nótese la ausencia de espacio — "SECOPI" no "SECOP I".
9. **Valor de contrato como texto en algunas filas**: `valor_contrato` es `number` en el esquema pero puede contener valores 0 para procesos sin contrato.

---

## ¿Sirve para oportunidades abiertas?

**NO.** Aunque contiene estados precontractuales, el dataset no tiene fecha de
publicación/apertura y mezcla SECOP I (plataforma legacy cerrada) con SECOP II.
Para búsqueda de oportunidades abiertas, usar `p6dx-8zbt` (SECOP II procesos).

## ¿Sirve para búsqueda histórica de contratos?

**SÍ, con filtros.** Filtrando por `estado_del_proceso IN ('Celebrado', 'Liquidado',
'En ejecución', 'Cerrado')` se obtiene el universo de contratos firmados en ambas
plataformas (16.2M registros). Complementa `jbjy-vk9h` (SECOP II contratos actual)
con datos históricos y SECOP I.

---

## Addendum C0.2 — SECOP I Activo en 2026 (2026-05-28)

La auditoría inicial (C0) trató los datos SECOP I como puramente históricos.
La verificación C0.2 corrige con datos duros:

| Métrica SECOP I 2026 | Valor |
|---------------------|-------|
| Contratos firmados | **74,933** |
| Valor total | **COP 3.155.203.479.424 (~3,2 billones)** |
| Valor promedio | COP 42.106.995 (~42,1 millones) |
| Contratación Directa | 73.3% (54,892 contratos) |
| Régimen Especial | 14.7% (11,043) |
| Top departamento | Nariño (9,265 contratos, COP 287.900.928.389) |

Para contratos SECOP I recientes, consultar rpmr-utcd con:
- `origen = 'SECOPI'`
- `date_extract_y(fecha_de_firma_del_contrato) = 2026`
- `estado_del_proceso IN ('Celebrado', 'Liquidado', 'En ejecución', 'Cerrado')`

**Reclasificación:** rpmr-utcd pasa de "dataset histórico de contratos" a
**"dataset consolidado de contratos SECOP I+II, con valor histórico y
operativo para contratos SECOP I que siguen cargándose en 2026."**

Para procesos precontractuales SECOP I (convocados, borrador), usar
`f789-7hwg` que sí tiene fecha de cargue/publicación.

Ver [INTEGRATION_RECOMMENDATION.md](INTEGRATION_RECOMMENDATION.md) v1.1.

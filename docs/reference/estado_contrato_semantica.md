# Semántica de `estado_contrato` en SECOP II (dataset `jbjy-vk9h`)

**Validado contra datos reales de datos.gov.co el 2026-06-05** (queries `GROUP BY`
sobre el universo completo de contratos). Este doc fija el sentido jurídico de
cada estado para que las queries no lo malinterpreten.

## 1. La línea de la firma (`fecha_de_firma`)

Un registro existe en el dataset de **contratos** porque se firmó o está en el
trámite previo. El hecho jurídico decisivo es `fecha_de_firma`:

| `estado_contrato` | total | % con `fecha_de_firma` | lado de la firma |
|---|--:|--:|---|
| En ejecución | 1,680,032 | 100.0% | **firmado** |
| Cerrado | 1,567,172 | 100.0% | **firmado** |
| Modificado | 1,031,720 | 100.0% | **firmado** |
| terminado | 730,535 | 100.0% | **firmado** |
| Aprobado | 184,033 | 100.0% | **firmado** |
| cedido | 28,044 | 100.0% | **firmado** |
| Suspendido | 6,428 | 100.0% | **firmado** |
| Prorrogado | 121 | 100.0% | **firmado** |
| Borrador | 234,448 | 0.2% | pre-firma |
| Cancelado | 107,313 | 0.1% | pre-firma |
| enviado Proveedor | 41,562 | 0.3% | pre-firma |
| En aprobación | 21,659 | 0.4% | pre-firma |

> Valores exactos en SECOP (respetar mayúsculas/acentos): `terminado` y `cedido`
> van en minúscula; `enviado Proveedor` con esa capitalización rara.

**"Firmado" NO es un estado** — es `fecha_de_firma IS NOT NULL`. Equivale
exactamente a los 8 estados de arriba con ~100%. (Ver fix `a745ac8`: el mapeo
legacy `firmado→Aprobado` tiraba el ~97% del recall.)

## 2. "Aprobado" ≠ "En aprobación" (nombres casi iguales, sentido OPUESTO)

- **`Aprobado`** — 184,033 · **100% firmado**. Contrato **perfeccionado/legalizado**.
  El inicio de ejecución es mixto: ~29% aún **sin** `fecha_de_inicio_del_contrato`
  (firmado pero no iniciado), ~71% ya con inicio. NO confundir con "en ejecución".
- **`En aprobación`** — 21,659 · **0.4% firmado**. Aún en **trámite de aprobación**,
  **pre-firma**. Muchos registros son viejos/estancados (fechas fin 2018, 2021).

Están en **lados opuestos de la firma**. Agruparlos (como hacía la familia
`en_evaluacion` para contratos) mezcla contratos firmados con pre-firma → error
jurídico. **Pendiente de corregir** (issue "aprobado"): `"aprobado/aprobados"` en
contratos debe resolver a `estado_contrato = 'Aprobado'` exacto, no al set
`{Borrador, enviado Proveedor, En aprobación, Aprobado}`.

## 3. Borrador-PROCESO ≠ Borrador-CONTRATO

Dos cosas distintas con el mismo nombre:

- **`estado_del_procedimiento = 'Borrador'` (PROCESOS)** — proceso aún sin
  publicar. **Es oportunidad temprana valiosa** para proponentes (señal pre-
  publicación). Está incluido a propósito en el funnel `oferta_abierta`
  ([estado_families.py](../../app/core/estado_families.py)). **No filtrar fuera.**
- **`estado_contrato = 'Borrador'` (CONTRATOS)** — contrato en borrador, **sin
  firmar** (0.2% con firma; objeto a veces "No definido"). Otra cosa.

## 4. Implicaciones para queries

| Frase del usuario | Comportamiento correcto |
|---|---|
| "firmado / firmados / se han firmado" | `dataset=contratos` + filtrar `fecha_de_firma` (si hay rango). **Sin** filtro de estado. |
| "aprobado / aprobados" (contratos) | `estado_contrato = 'Aprobado'` exacto. NO agrupar con En aprobación/Borrador. |
| "en aprobación" | `estado_contrato = 'En aprobación'` (pre-firma) — distinto de "aprobado". |
| "oportunidades / procesos abiertos" | funnel `oferta_abierta` incl. Borrador-proceso (no excluir). |

## Fuente de validación

`SELECT estado_contrato, count(*) [WHERE fecha_de_firma IS NOT NULL] GROUP BY
estado_contrato` contra `jbjy-vk9h` en datos.gov.co, 2026-06-05. Reproducible
con `SecopClient` (`app/core/secop_client.py`).

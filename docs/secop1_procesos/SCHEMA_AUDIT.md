# SCHEMA_AUDIT — SECOP I Procesos (f789-7hwg)

**Dataset ID:** `f789-7hwg`
**Nombre oficial:** SECOP I - Procesos de Compra Pública
**URL humana:** https://www.datos.gov.co/Estad-sticas-Nacionales/SECOP-I-Procesos-de-Compra-P-blica/f789-7hwg/about_data
**API base:** https://www.datos.gov.co/resource/f789-7hwg.json
**Última actualización (timestamp):** 1780001151
**Fecha de auditoría:** 2026-05-28

---

## Descripción Oficial

> Procesos de Compra pública registrados en el SECOP I desde el 01 de enero
> de 2018, contiene la información del proceso, fase de selección y la
> adjudicación.

---

## Estadísticas

| Métrica | Valor |
|---------|-------|
| Filas totales | 6,371,069 |
| Columnas | 79 |
| Rango de años (cargue) | 2018–2026 |
| Sin fecha de firma | 540,857 (8.5%) |
| Sin fecha de cargue | 0 |

---

## Distribución por Estado del Proceso

| Estado | Conteo | % |
|--------|--------|---|
| Celebrado | 3,884,516 | 61.0% |
| Liquidado | 1,715,691 | 26.9% |
| Convocado | 465,901 | 7.3% |
| Terminado sin liquidar | 190,786 | 3.0% |
| Terminado anormalmente después de convocado | 82,579 | 1.3% |
| Adjudicado | 21,082 | 0.3% |
| Descartado | 7,711 | 0.1% |
| Borrador | 2,667 | 0.04% |

---

## Distribución por Modalidad

| Modalidad | Conteo | % |
|-----------|--------|---|
| Contratación Directa (Ley 1150 de 2007) | 3,017,255 | 47.4% |
| Régimen Especial | 2,398,804 | 37.7% |
| Contratación Mínima Cuantía | 638,197 | 10.0% |
| Selección Abreviada de Menor Cuantía | 99,624 | 1.6% |
| Contratos y Convenios con Más de Dos Partes | 90,522 | 1.4% |
| Subasta | 46,036 | 0.7% |
| Concurso de Méritos Abierto | 32,095 | 0.5% |
| Licitación Pública | 22,970 | 0.4% |
| Licitación Obra Pública | 21,500 | 0.3% |

---

## Top 10 Entidades

| Entidad | Conteo |
|---------|--------|
| SENA | 81,112 |
| Antioquia - Hospital General de Medellín | 72,522 |
| Antioquia - Universidad de Antioquia | 54,074 |
| Valle del Cauca - Alcaldía de Cali | 42,893 |
| Antioquia - E.S.E. Hospital La María | 38,602 |
| Santander - Cámara de Comercio de Bucaramanga | 29,011 |
| Antioquia - Institución Universitaria Colegio Mayor | 28,822 |
| Antioquia - E.S.E. Metrosalud | 28,377 |
| ICBF | 27,903 |
| Atlántico - E.S.E. Hospital Materno Infantil de Soledad | 25,324 |

---

## Top 10 Departamentos

| Departamento | Conteo |
|-------------|--------|
| Antioquia | 925,382 |
| Bogotá D.C. | 528,060 |
| Nariño | 469,639 |
| Cundinamarca | 458,719 |
| Valle del Cauca | 378,147 |
| Santander | 297,679 |
| Boyacá | 284,614 |
| Magdalena | 248,323 |
| Tolima | 234,755 |
| Norte de Santander | 234,444 |

---

## Columnas Relevantes (79 total)

Se listan las más relevantes para mapping. Las 79 columnas completas están
disponibles en el metadata de Socrata.

### Identificación
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `uid` | text | ID único de la relación proceso-adjudicación |
| `numero_de_constancia` | text | ID del proceso generado por SECOP I |
| `numero_de_proceso` | text | ID del proceso según nomenclatura de entidad |
| `numero_de_contrato` | text | ID del contrato según nomenclatura de entidad |
| `id_adjudicacion` | text | ID de la adjudicación |
| `ruta_proceso_en_secop_i` | url | URL directa al proceso en SECOP I |

### Entidad
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `nombre_entidad` | text | Nombre de la entidad compradora |
| `nit_de_la_entidad` | text | NIT de la entidad |
| `c_digo_de_la_entidad` | number | Código único en SECOP I |
| `nivel_entidad` | text | NACIONAL / TERRITORIAL |
| `departamento_entidad` | text | Departamento de la entidad |
| `municipio_entidad` | text | Municipio de la entidad |

### Proceso
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `estado_del_proceso` | text | Estado del proceso |
| `modalidad_de_contratacion` | text | Modalidad de selección |
| `tipo_de_contrato` | text | Tipo de contrato |
| `objeto_a_contratar` | text | Objeto (catálogo UNSPSC) |
| `detalle_del_objeto_a_contratar` | text | Detalle adicional del objeto |
| `cuantia_proceso` | number | Valor de lanzamiento del proceso |
| `fecha_de_cargue_en_el_secop` | calendar_date | Fecha de registro en SECOP I |

### Contrato (si existe)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `cuantia_contrato` | number | Valor firmado |
| `valor_total_de_adiciones` | number | Suma de adiciones |
| `valor_contrato_con_adiciones` | number | Valor total con adiciones |
| `fecha_de_firma_del_contrato` | calendar_date | Fecha de firma |
| `fecha_ini_ejec_contrato` | calendar_date | Inicio de ejecución |
| `fecha_fin_ejec_contrato` | calendar_date | Fin de ejecución |
| `fecha_liquidacion` | calendar_date | Fecha de liquidación |
| `objeto_del_contrato_a_la` | text | Objeto del contrato al momento de firma |

### Contratista (si existe)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `nom_razon_social_contratista` | text | Nombre o razón social |
| `identificacion_del_contratista` | text | Identificación |
| `tipo_identifi_del_contratista` | text | Tipo de documento |
| `dpto_y_muni_contratista` | text | Departamento y municipio |

### Categorización
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_grupo` / `nombre_grupo` | text | Categoría UNSPSC nivel 1 |
| `id_familia` / `nombre_familia` | text | Categoría UNSPSC nivel 2 |
| `id_clase` / `nombre_clase` | text | Categoría UNSPSC nivel 3 |
| `id_objeto_a_contratar` | number | ID UNSPSC del objeto |

### Presupuesto
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `compromiso_presupuestal` | text | Código de registro presupuestal |
| `posicion_rubro` / `nombre_rubro` | text | Rubro presupuestal |
| `valor_rubro` | number | Valor del rubro |
| `destino_gasto` | text | Destino del gasto |

### Temporal y Metadata
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `anno_cargue_secop` | number | Año de cargue |
| `anno_firma_contrato` | text | Año de firma o "Sin Firma" |
| `ultima_actualizacion` | calendar_date | Última modificación |
| `moneda` | text | Moneda (COP) |
| `es_postconflicto` | number | Marcador postconflicto |
| `es_mipyme` | text | Si la entidad es Mipyme |

---

## Limitaciones y Riesgos

1. **Solo SECOP I**: No contiene procesos de SECOP II. No es un dataset unificado.
2. **Sin ID canónico único**: `numero_de_proceso` sigue nomenclatura de cada entidad,
   no es un identificador global. `uid` es el campo más confiable.
3. **540K registros sin contrato** (8.5%): `fecha_de_firma_del_contrato IS NULL`.
   Estos representan procesos que no llegaron a adjudicación o que están en curso.
4. **URL directa**: `ruta_proceso_en_secop_i` apunta a `contratos.gov.co` (SECOP I
   legacy). Las URLs requieren el viewer antiguo, no son accesibles vía API de
   SECOP II.
5. **Datos erróneos en año de firma**: ~20 registros con años 2075-2099.
6. **Valores en texto**: `anno_firma_contrato` es `text`, no `number`. Usa "Sin Firma"
   como valor centinela para procesos sin contrato.
7. **Normalización de nombres**: Los nombres de entidades usan formato
   "DEPARTAMENTO - NOMBRE ENTIDAD - CIUDAD" que requiere parsing.

---

## ¿Sirve para búsqueda histórica SECOP I?

**SÍ.** Es el dataset más completo para procesos de SECOP I. Incluye:
- Procesos con y sin contrato
- Categorización UNSPSC detallada
- Información presupuestal (rubros)
- URLs directas al proceso original

## ¿Sirve para búsqueda de contratos?

**Parcialmente.** El 91.5% tiene contrato firmado, pero `jbjy-vk9h` y `rpmr-utcd`
cubren mejor la búsqueda de contratos. f789-7hwg es superior para trazabilidad
precontractual y análisis de procesos que no llegaron a contrato.

---

## Addendum C0.2 — SECOP I: Legacy Activo (2026-05-28)

La auditoría inicial (C0) clasificó SECOP I como plataforma puramente
histórica. La verificación C0.2 corrige esta clasificación con datos duros:

| Métrica 2026 | Valor |
|-------------|-------|
| Procesos cargados | **120,440** |
| Sin contrato (CONVOCADO) | 15,620 |
| Contratos firmados (rpmr-utcd) | 74,933 — COP 3.155.203.479.424 (~3,2 billones) |
| Avg contrato | COP 42.106.995 (~42,1 millones) |
| Top departamento | Nariño (16,992 procesos) |
| Top modalidad | Contratación Directa (64% procesos, 73% contratos) |

**SECOP I es legacy pero no está muerto.** La actividad se concentra en
municipios pequeños (Tumaco, Providencia, Inírida, Mitú), mínima cuantía,
régimen especial y cámaras de comercio con inercia institucional.

**Reclasificación:** f789-7hwg pasa de "dataset analítico complementario
histórico" a **"dataset operativo secundario para procesos SECOP I
activos/recientes + dataset analítico de trazabilidad histórica."**

Ver [INTEGRATION_RECOMMENDATION.md](INTEGRATION_RECOMMENDATION.md) v1.1
para detalles completos.

# INTEGRATION_RECOMMENDATION — SECOP I Procesos (f789-7hwg)

**Fecha:** 2026-05-28
**Versión:** 1.1 (reclasificación C0.2 — active legacy source)
**Estado:** AUDIT ONLY — NO INTEGRAR TODAVÍA

---

## Veredicto

**f789-7hwg es el dataset operativo secundario para procesos SECOP I
activos/recientes y el dataset analítico de trazabilidad histórica de
SECOP I.**

SECOP I es una plataforma legacy, pero **no está muerta**. Se verificó
actividad en 2026: procesos convocados, contratos cargándose, y actividad
concentrada en municipios pequeños, mínima cuantía, régimen especial y
convenios. Ignorar SECOP I en búsquedas dejaría fuera procesos reales
que todavía ocurren en esta plataforma.

---

## ¿Qué resuelve?

| Problema actual | Cómo lo resuelve f789-7hwg |
|----------------|---------------------------|
| Sin visibilidad de procesos SECOP I activos en 2026 | Datos frescos con fecha de cargue reciente |
| Sin visibilidad de procesos que no llegaron a contrato | 540K registros sin firma |
| Sin categorización UNSPSC en búsquedas | Grupo, familia, clase para 6.37M registros |
| Sin trazabilidad presupuestal | Rubros, compromisos, valores con adiciones |
| Sin URLs directas a procesos SECOP I | `ruta_proceso_en_secop_i` |

## ¿Qué NO resuelve?

- **Procesos de SECOP II**: Solo SECOP I. Para SECOP II usar `p6dx-8zbt`.
- **Contratos de SECOP II**: Usar `jbjy-vk9h` o `rpmr-utcd` con `origen=SECOPII`.

---

## SECOP I: Legacy pero Activo

SECOP I fue cerrado para nuevos registros en 2022, pero la plataforma
sigue recibiendo cargues y actualizaciones en 2026.

### Actividad 2026 verificada (C0.2)

| Métrica | Valor |
|---------|-------|
| Procesos cargados en 2026 | **120,440** |
| Procesos sin contrato | 18,057 (15%) — 15,620 CONVOCADO |
| Contratos firmados (vía rpmr-utcd) | 74,933 — COP 3.155.203.479.424 (~3,2 billones) |
| Contratación Directa | 64% de procesos, 73% de contratos |
| Top departamento | Nariño (16,992 procesos, 9,265 contratos) |

La actividad se concentra en:

- **Municipios pequeños** que nunca migraron completamente a SECOP II
- **Mínima cuantía** y **régimen especial** que usan SECOP I por inercia
- **Convenios y contratos interadministrativos** con ciclos largos
- **Registros precontractuales** que se publican en SECOP I por req. legal

**Implicación para SECOPPAL:** Una búsqueda de "procesos de dotación en
Nariño" debería considerar tanto `p6dx-8zbt` (SECOP II) como `f789-7hwg`
(SECOP I), mostrando resultados separados por plataforma.

---

## Estrategia de Integración Recomendada

### Rol: Dataset Operativo Secundario + Analítico

f789-7hwg debe usarse para:

1. **Búsqueda de procesos SECOP I activos/recientes**:
   - Filtrar por `date_extract_y(fecha_de_cargue_en_el_secop) = 2026`
   - Filtrar por `estado_del_proceso = 'CONVOCADO'` para oportunidades
   - Mostrar junto a resultados de `p6dx-8zbt`, separados por plataforma

2. **Consultas de trazabilidad**: "¿Qué pasó con el proceso X de SECOP I?"
   - Búsqueda por `numero_de_constancia` o `numero_de_proceso`
   - Muestra todo el ciclo: publicación → adjudicación → contrato → liquidación

3. **Análisis de procesos sin contrato**: "¿Cuántos procesos de modalidad Y no
   llegaron a contrato en el departamento Z?"
   - Filtrar por `fecha_de_firma_del_contrato IS NULL`
   - Agrupar por modalidad, departamento, año

4. **Enriquecimiento de resultados**: Cuando un resultado de `rpmr-utcd` tiene
   `origen = 'SECOPI'`, enriquecer con datos de f789-7hwg (UNSPSC,
   rubros, detalle del objeto).

### NO usar para:

- Procesos/contratos de SECOP II (usar `p6dx-8zbt` / `jbjy-vk9h`)
- Búsqueda principal de contratos (usar `rpmr-utcd` o `jbjy-vk9h`)

---

## Comparativa de Riqueza por Dataset

| Dimensión | f789-7hwg | rpmr-utcd | jbjy-vk9h | p6dx-8zbt |
|-----------|-----------|-----------|-----------|-----------|
| Plataforma | SECOP I | SECOP I+II | SECOP II | SECOP II |
| Registros | 6.37M | 21.9M | ~2M | ~2.5M |
| Columnas | 79 | 22 | ~30 | ~22 |
| Categorización UNSPSC | ✅ Grupo/Familia/Clase | ❌ | ❌ | ❌ |
| Rubros presupuestales | ✅ | ❌ | ❌ | ❌ |
| Procesos sin contrato | ✅ 540K | Parcial* | ❌ | ✅ |
| URLs directas | ✅ SECOP I | ✅ contratos.gov.co | ❌ | ✅ SECOP II |
| Adiciones/Modificaciones | ✅ | ❌ | ❌ | ❌ |

---

## Precauciones Técnicas

1. **79 columnas**: Para búsqueda, seleccionar solo las necesarias (~20). El payload completo es innecesario.
2. **URLs legacy**: `ruta_proceso_en_secop_i` apunta a `contratos.gov.co`. El usuario necesita saber que es el viewer antiguo.
3. **Nombres de entidad**: Formato "DEPARTAMENTO - ENTIDAD - CIUDAD". Puede requerir parsing para matching con el gazetteer.
4. **Datos personales**: Contiene `identific_representante_legal`, `nombre_del_represen_legal`. No exponer en resultados de búsqueda.
5. **Timeout**: 30s suficiente. Dataset más eficiente que rpmr-utcd.
6. **Separación por plataforma**: Cuando se combina con `p6dx-8zbt`, los resultados deben mostrarse en secciones separadas (SECOP II / SECOP I legacy).

---

## Decisión

**APROBADO para uso futuro como dataset operativo secundario de SECOP I
(procesos activos/recientes) + dataset analítico de trazabilidad histórica.**

**No activar hasta que:**
- [ ] Se implemente búsqueda dual p6dx-8zbt + f789-7hwg con separación por plataforma
- [ ] Exista un parser de nombres de entidad compatible con el gazetteer
- [ ] Se filtren campos con datos personales antes de mostrar resultados
- [ ] Se implemente advertencia de producto: "SECOP I es una plataforma legacy"
- [ ] Se implemente el enriquecimiento cruzado con rpmr-utcd (join por numero_de_proceso)

**Mientras tanto:** Solo documentado. Cero cambios en el hot path.

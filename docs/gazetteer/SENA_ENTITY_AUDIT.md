# SENA Entity Audit — 2026-05-26

**Estado:** Read-only audit contra SECOP II live
**Fuentes:** Procesos (p6dx-8zbt) + Contratos (jbjy-vk9h)

---

## Resumen

| Metrica | Valor |
|---------|-------|
| Total entidades SENA unicas | 89 |
| Volumen total (procesos + contratos) | 616,210 |
| Departamentos cubiertos | 33 |
| Clasificaciones | 7 tipos con datos |

## Clasificacion por Tipo

| Tipo | Cantidad | Volumen total | Descripcion |
|------|----------|---------------|-------------|
| national_secretaria_general | 1 | 21,545 | SENA Secretaria General (nivel nacional) |
| national_direccion_general | 2 | 8,817 | SENA Direccion General (nivel nacional) |
| regional_parent | 28 | 179,712 | SENA Regional (entidad principal por departamento) |
| regional_admin_group | 54 | 395,848 | Grupo Administrativo Regional |
| training_center | 1 | 4,491 | Centro de Formacion SENA |
| intercentros | 2 | 5,793 | Grupo Intercentros |
| needs_review | 1 | 4 | Requiere revision (nombre atipico o sin departamento) |

## Top 20 por Volumen

| # | Volumen | Nombre Oficial | Departamento | Clasificacion |
|---|---------|---------------|-------------|---------------|
| 1 |   22885 | SENA REGIONAL VALLE Grupo de Apoyo Administrativo Mixto | Valle del Cauca | regional_admin_group |
| 2 |   21545 | SENA SECRETARIA GENERAL | Distrito Capital de Bogot | national_secretaria_general |
| 3 |   20993 | CONGRESO DE LA REPÚBLICA - HONORABLE SENADO DE LA REPÚBLICA | Distrito Capital de Bogot | regional_parent |
| 4 |   19827 | SENA REGIONAL ANTIOQUIA Grupo de Apoyo Administrativo Mixto | Antioquia | regional_admin_group |
| 5 |   17649 | SENA REGIONAL SANTANDER Grupo de Apoyo Administrativo Mixto | Santander | regional_admin_group |
| 6 |   17353 | SENA REGIONAL TOLIMA 1 | Tolima | regional_parent |
| 7 |   15616 | SENA REGIONAL CESAR Grupo de Apoyo Administrativo Mixto | Cesar | regional_admin_group |
| 8 |   15246 | SENA REGIONAL BOLIVAR Grupo de Apoyo Administrativo Mixto | Bolívar | regional_admin_group |
| 9 |   15205 | SENA REGIONAL NORTE DE SANTANDER Grupo de Apoyo Administrativo Mixto | Norte de Santander | regional_admin_group |
| 10 |   14830 | SENA REGIONAL ATLANTICO Intercentros | Atlántico | regional_parent |
| 11 |   14073 | SENA REGIONAL RISARALDA Grupo de Apoyo Administrativo Mixto// | Risaralda | regional_admin_group |
| 12 |   13779 | SENA REGIONAL DISTRITO CAPITAL Grupo de Apoyo Administrativo Mixto CSF | Distrito Capital de Bogot | regional_admin_group |
| 13 |   13436 | SENA REGIONAL DISTRITO CAPITAL Grupo Administrativo Intercentros CEET | Distrito Capital de Bogot | regional_admin_group |
| 14 |   11943 | SENA REGIONAL ATLANTICO | Atlántico | regional_parent |
| 15 |   11916 | SENA REGIONAL QUINDIO Grupo de Apoyo Administrativo Mixto | Quindío | regional_admin_group |
| 16 |   11388 | SENA REGIONAL ANTIOQUIA Grupo Administrativo Intercentros Medellin | Antioquia | regional_admin_group |
| 17 |   11092 | SERVICIO NACIONAL DE APRENDIZAJE - SENA MAGDALENA | Magdalena | regional_parent |
| 18 |   10970 | SENA DISTRITO CAPITAL GRUPO DE APOYO ADMINISTRATIVO - CGI | Distrito Capital de Bogot | regional_parent |
| 19 |   10896 | SERVICIO NACIONAL DE APRENDIZAJE SENA REGIONAL CORDOBA | Córdoba | regional_parent |
| 20 |   10420 | SENA REGIONAL SUCRE | Sucre | regional_parent |

## Regionales por Departamento

| Departamento | Cantidad | Volumen |
|--------------|----------|---------|
| Amazonas | 1 | 3,121 |
| Antioquia | 1 | 3,920 |
| Atlántico | 3 | 30,498 |
| Bolívar | 1 | 3,332 |
| Boyacá | 1 | 851 |
| Caldas | 4 | 12,388 |
| Caquetá | 1 | 4,904 |
| Cauca | 3 | 12,902 |
| Córdoba | 1 | 10,896 |
| Distrito Capital de Bogotá | 3 | 35,471 |
| Guaviare | 1 | 3,957 |
| Magdalena | 1 | 11,092 |
| Nariño | 1 | 3,792 |
| San Andrés, Providencia y Santa Catalina | 1 | 6,869 |
| Sucre | 1 | 10,420 |
| Tolima | 1 | 17,353 |
| Valle del Cauca | 1 | 3,398 |
| Vaupés | 1 | 1,854 |
| Vichada | 1 | 2,694 |

## Needs Review

| # | Volumen | Nombre Oficial | Departamento |
|---|---------|---------------|-------------|
| 1 |       4 | SENA - REGIONAL SANTANDER | Santander |

## Observaciones

- SENA tiene entidades regionales que pueden tener nombre distinto en procesos vs contratos
- Algunas regionales aparecen como `SENA REGIONAL {DEPT}` y otras como `SENA {DEPT}//`
- Los grupos administrativos (Grupo Administrativo CBA, CDA, etc.) son variantes internas
- Centros de formacion (Centro Agropecuario, Centro Industrial, etc.) existen por regional
- Intercentros son grupos administrativos multi-centro
- NIT base frecuente: 899999034, pero varía por regional y centro
- `needs_review` incluye entidades sin departamento o con nombres atípicos

## Recomendaciones

1. Agrupar SENA + departamento como ecosistema, no como entidad unica
2. No fusionar los nombres oficiales — cada resultado conserva su official_name real
3. Para busqueda regional, usar LIKE con contains_all (SENA + DEPARTAMENTO)
4. Para macro-region, expandir a lista de departamentos con LIKE compuesto
5. Secretaria General y Direccion General son entidades nacionales aparte

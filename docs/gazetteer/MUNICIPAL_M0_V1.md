# Municipal M0 — cobertura acreditada y deuda de afiliación

**Estado: PARTIAL.** Extracción bulk, conciliación y propuestas automáticas: PASS. El porcentaje exacto de todo el universo municipal de BuyerUnits todavía no es identificable con estas fuentes sin adjudicar los casos pendientes. Se publican denominadores, cotas y perfiles pendientes; ninguno se convierte en una unidad por compartir NIT o ubicación.

## Fuentes y alcance

- [DIVIPOLA DANE 2025](https://portalgis.dane.gov.co/mparcgis/rest/services/Divipola/Serv_DIVIPOLA_MGN_2025/MapServer/317): 1.122 territorios; 1.103 municipios/distritos, 18 áreas no municipalizadas y una isla. Sólo los 1.103 entran al denominador municipal.
- [Directorio SECOP II](https://www.datos.gov.co/resource/4ex9-j3n8.json): 27.297 estratos de registros con `es_entidad=Si`. Un código de cuenta aporta procedencia, no identidad lógica.
- [SIGEP, reporte oficial 61](https://www.funcionpublica.gov.co/VisualSIE/faces/reporte/publico.xhtml?externo=true&idConsulta=Pr&idReporte=61): exportación completa de 6.300 entidades activas; 5.543 de suborden Municipal y 182 Distrital. El suborden acredita adscripción; el NIT sólo genera candidatos para contrastar nombre, cuenta y territorio.
- [Procesos SECOP II](https://www.datos.gov.co/resource/p6dx-8zbt.json) y [contratos SECOP II](https://www.datos.gov.co/resource/jbjy-vk9h.json): ventana `[2025-09-03, 2026-09-03)`. 1.885.602 filas de procesos / 1.819.018 procesos únicos; 1.095.013 filas y contratos únicos.

Los conteos antes/después coinciden con la suma de estratos. La suma de procesos únicos por estrato coincide con el DISTINCT global; en contratos existe igualdad global entre filas e IDs distintos. Esa prueba habilita sumar las métricas lógicas por cohorte sin deduplicar por NIT. El primer agregado de contratos agotó 55 segundos; el reintento redujo dimensiones y mantuvo conciliación completa. Se preserva la evidencia de ambas llamadas.

La vía de afiliación SIGEP exige directorio por cuenta + nombre institucional + evidencia fiscal + DIVIPOLA + SIGEP de jurisdicción única. Para ejecutivos se reutiliza la familia institucional existente y se exige el municipio completo en los dos nombres. Un nombre territorial aislado no acredita pertenencia. La vía alternativa de Bogotá usa su registro ejecutivo declarado y exige coincidencia exacta con nombre, cuenta y evidencia fiscal del lote acreditado de G; no genera membresía desde geografía o NIT.

## Cobertura medida

Hay **607 municipios con actividad municipal acreditada** y **70 reconocidos con alcance comprobado** (11,53% de esa cohorte; 6,35% de DIVIPOLA). Se certificaron 119 BuyerUnits existentes sin reclamaciones físicas compartidas. Los demás territorios no se declaran inactivos.

| Dataset / métrica | Cohorte municipal acreditada | Reconocido comprobado | Cobertura de la cohorte | Afiliación pendiente |
|---|---:|---:|---:|---:|
| Procesos: filas físicas | 357.631 | 205.409 | 57,44% | 978.390 |
| Procesos: IDs únicos | 339.745 | 197.352 | 58,09% | 961.133 |
| Contratos: filas físicas | 331.185 | 199.963 | 60,38% | 429.291 |
| Contratos: IDs únicos | 331.185 | 199.963 | 60,38% | 429.291 |

La afiliación pendiente mezcla posibles compradores municipales con otros territoriales y ruido de registro. No se llama municipal a todo ese residuo. El denominador municipal de procesos únicos queda entre 339.745 y 1.300.878; el de contratos únicos entre 331.185 y 760.476. Las cotas se refieren al volumen; no convierten cuentas o strings en BuyerUnits.

## Incorporación derivable

**503 ejecutivos municipales** cumplen el predicado conservador: 498 unidades nuevas y 5 ampliaciones de alias. Representan **501 territorios adicionales**, porque dos ya tienen otra unidad municipal reconocida.

El predicado requiere nombre físico byte-idéntico en ambos datasets, una única cuenta observada, evidencia fiscal consistente, afiliación acreditada por SIGEP/directorio/DIVIPOLA, cero conflictos de alias o proyección, y comprobación histórica de propietarios por nombre exacto. Las 58 consultas por lotes verifican los propietarios sin hacer 1.103×N consultas. Los pares de grafías, múltiples cuentas y evidencia fiscal divergente quedan fuera; no se fusionan.

Los **1888 alias propuestos** resolvieron exactamente y con propietario único en un catálogo temporal: cero fallos. Aportarían 126.608 procesos únicos y 118.941 contratos únicos de la ventana. Es capacidad comprobada sobre estas capturas, no garantía ante cambios futuros de la fuente. **No se importó ninguna propuesta en producción.**

Baranoa (08078) entra por la misma regla: `ALCALDIA MUNICIPAL DE BARANOA`, cuenta `701706467`, 381 filas / 380 procesos únicos y 374 contratos únicos. No hay excepción por municipio en el código.

## Principales brechas acreditadas por actividad

| Municipio | Departamento | Procesos únicos sin cubrir | Contratos únicos sin cubrir |
|---|---|---:|---:|
| BUCARAMANGA | SANTANDER | 6.425 | 6.300 |
| SABANETA | ANTIOQUIA | 2.598 | 2.531 |
| MANIZALES | CALDAS | 2.045 | 1.811 |
| PUERTO GAITÁN | META | 1.760 | 1.720 |
| MAGANGUÉ | BOLÍVAR | 1.206 | 1.851 |
| LOS PATIOS | NORTE DE SANTANDER | 1.248 | 1.155 |
| TOCANCIPÁ | CUNDINAMARCA | 1.028 | 991 |
| BOGOTÁ, D.C. | BOGOTÁ, D.C. | 1.027 | 932 |
| AGUAZUL | CASANARE | 1.022 | 911 |
| CARTAGENA DE INDIAS | BOLÍVAR | 902 | 968 |
| IPIALES | NARIÑO | 973 | 896 |
| EL CERRITO | VALLE DEL CAUCA | 922 | 879 |
| SANTANDER DE QUILICHAO | CAUCA | 925 | 817 |
| FLORIDA | VALLE DEL CAUCA | 865 | 860 |
| TENJO | CUNDINAMARCA | 816 | 809 |

## Deudas y reproducción

Permanece abierta la adjudicación del residuo territorial y la cardinalidad completa de BuyerUnits municipales. Los perfiles pendientes conservan cuenta, nombre, dataset y evidencia fiscal en el artefacto; no se presentan como compradores lógicos distintos ni se consolidan automáticamente.

El censo detecta 25 canonicals sin aparición byte-for-byte en la ventana y 55 registros con NIT declarado. Son colas de revisión: ausencia reciente no prueba un canonical fantasma. El NIT transpuesto de Ibagué documentado en P0.1 no aparece en el catálogo de runtime auditado. No se corrigieron NIT ni se crearon merges para mejorar métricas.

Evidencia íntegra: `artifacts/audits/municipal_m0_v1/`, con SHA-256, SoQL literal, estados de extracción, matriz de afiliación, métricas y propuestas. Las capturas están comprimidas; SIGEP conserva únicamente columnas necesarias para identidad y afiliación.

Reproducir sin red:

```bash
.venv/bin/python3 scripts/audits/audit_municipal_m0.py
.venv/bin/python3 scripts/audits/audit_municipal_m0_proposals.py
.venv/bin/python3 scripts/audits/summarize_municipal_m0.py
```

No hay mutación de runtime en M0. Los gates y el commit de cierre se registran en `INSTITUTIONAL_BUYER_INTEGRITY_GROUPS_V1.md`.

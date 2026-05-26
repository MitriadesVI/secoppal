# Lote 01 — Deuda Tecnica

Entidades nuevas con <3 aliases. Requieren enriquecimiento manual o via LLM.

Total: 13 entidades de 50 agregadas (26%).

| Nombre Oficial | Tipo | Departamento | Aliases | Motivo | Accion Recomendada |
|---------------|------|-------------|---------|--------|--------------------|
| SECRETARIA GENERAL- GOBERNACION DEL VALLE | secretaria | Valle del Cauca | 2 | Nombre compuesto con guion no parseable por heuristica | manual_aliases |
| GOBERNACIÓN DEL VALLE DEL CAUCA - SECRETARÍA DE DESARROLLO RURAL, AGRI | secretaria | Valle del Cauca | 1 | Nombre compuesto con guion no parseable por heuristica | manual_aliases |
| INSTITUTO COLOMBIANO DE BIENESTAR FAMILIAR REGIONAL BOYACA | instituto | Boyacá | 1 | Instituto no tiene patron heuristico completo | llm_aliases |
| SERVICIO NACIONAL DE APRENDIZAJE SENA REGIONAL SAN ANDRES | sena_regional | San Andrés, Providencia y Santa Catalina | 1 | Prefijo atipico SERVICIO NACIONAL DE APRENDIZAJE | manual_aliases |
| DEPARTAMENTO DE CUNDINAMARCA - SECRETARIA DE EDUCACION | secretaria | Cundinamarca | 1 | Nombre compuesto con guion no parseable por heuristica | manual_aliases |
| INSTITUTO DEPARTAMENTAL DE DEPORTES DE ANTIOQUIA | instituto | Antioquia | 1 | Instituto no tiene patron heuristico completo | llm_aliases |
| ALCALDIA LOCAL DE PUENTE ARANDA-FONDO DE DESARROLLO LOCAL | fondo | Distrito Capital de Bogotá | 1 | Nombre compuesto con guion no parseable por heuristica | manual_aliases |
| EMPRESA SOCIAL DEL ESTADO HOSPITAL EMIRO QUINTERO CAÑIZARES 12 | ese_hospital | Norte de Santander | 1 | Prefijo ESE con nombre completo no parseable | llm_aliases |
| DEPARTAMENTO DE CUNDINAMARCA -SECRETARIA DE GOBIERNO | secretaria | Distrito Capital de Bogotá | 1 | Nombre compuesto con guion no parseable por heuristica | manual_aliases |
| Instituto Departamental de Recreación y Deporte INDERSANTANDER SITIO O | instituto | Santander | 1 | Instituto no tiene patron heuristico completo | llm_aliases |
| EMPRESA SOCIAL DEL ESTADO HOSPITAL BENJAMIN BARNEY GASCA | ese_hospital | Valle del Cauca | 1 | Prefijo ESE con nombre completo no parseable | llm_aliases |
| EMPRESA SOCIAL DEL ESTADO HOSPITAL REGIONAL NOROCCIDENTAL | ese_hospital | Norte de Santander | 1 | Prefijo ESE con nombre completo no parseable | llm_aliases |
| NUEVA EMPRESA SOCIAL DEL ESTADO HOSPITAL DEPARTAMENTAL SAN FRANCISCO D | ese_hospital | Chocó | 1 | Prefijo ESE con nombre completo no parseable | llm_aliases |

## Acciones

| Accion | Descripcion | When |
|--------|-------------|------|
| manual_aliases | Escribir aliases a mano en lote JSON | Lote 02 o antes |
| llm_aliases | Generar con DeepSeek, tipo "otro" | Lote 02 o antes |
| acceptable_official_name_only | Aceptar que solo tenga nombre oficial como alias | Backlog |

## Regla para Lote 02

El numero de entidades con <3 aliases NO debe aumentar respecto a Lote 01 (13 actual).
Si se agregan 50 entidades, maximo 13 pueden tener <3 aliases.

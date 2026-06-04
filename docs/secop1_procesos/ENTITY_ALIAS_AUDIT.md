# ENTITY_ALIAS_AUDIT — SECOP I Entities 2026

**Fecha:** 2026-05-29
**Task:** C1A — SECOP I Entity Crosswalk / source-aware aliases
**Dataset:** f789-7hwg (procesos) + rpmr-utcd origen=SECOPI (contratos)
**Muestra:** 85 entidades con actividad SECOP I en 2026

---

## Resumen

Se auditaron las 85 entidades con mayor actividad en SECOP I durante 2026,
provenientes de tres fuentes:

| Fuente | Entidades | Criterio |
|--------|-----------|----------|
| f789-7hwg top 50 | 50 | Mayor volumen de procesos cargados en 2026 |
| rpmr-utcd SECOPI top 50 | 50 | Mayor volumen de contratos firmados en 2026 |
| CONVOCADO 2026 top 25 | 25 | Procesos activos sin contrato |

Tras deduplicación entre fuentes: **85 entidades únicas**.

---

## Clasificación

| Clasificación | Cantidad | % | Descripción |
|--------------|----------|---|-------------|
| `matched_existing` | 27 | 31.8% | Entidad ya existe en el gazetteer SECOPPAL |
| `secop1_only` | 41 | 48.2% | Entidad solo existe en SECOP I — nueva para el gazetteer |
| `needs_review` | 14 | 16.5% | Requiere verificación manual (ESEs, nombres ambiguos) |
| `bad_name` | 3 | 3.5% | Nombre truncado o malformado en la fuente |

---

## Hallazgos Clave

### 1. Cámaras de Comercio — matched_existing

Las cámaras de comercio dominan ambas plataformas. Nombres casi idénticos:

| SECOP I (f789-7hwg) | SECOP II (gazetteer esperado) |
|---------------------|------------------------------|
| SANTANDER - CAMARA DE COMERCIO DE BUCARAMANGA | CÁMARA DE COMERCIO DE BUCARAMANGA |
| ANTIOQUIA - CAMARA DE COMERCIO DE MEDELLIN | CÁMARA DE COMERCIO DE MEDELLÍN |
| VALLE DEL CAUCA - CAMARA DE COMERCIO DE CALI | CÁMARA DE COMERCIO DE CALI |

**Alias necesarios**: Agregar variante con prefijo "DEPARTAMENTO - " y sin tildes.

### 2. Municipios de periferia — secop1_only

41 entidades solo existen en SECOP I. Perfil típico:

- **Alcaldías de municipios pequeños**: Tumaco, Barbacoas, El Charco,
  Francisco Pizarro, La Tola, Ricaurte (Nariño); Cantagallo, San Pablo
  (Bolívar); Novita, Condoto, Lloró, Bojayá (Chocó).
- **Gobernaciones de periferia**: Guainía, Guaviare, Vaupés, Vichada.
- **ESEs rurales**: Centro Hospital Divino Niño (Tumaco), Hospital San
  José del Guaviare, Red de Servicios de Salud de Primer Nivel (Guaviare).

**Implicación**: ~48% de las entidades activas en SECOP I no existen en
el gazetteer actual. Cualquier búsqueda dual necesitará agregarlas.

### 3. Nombres con errores — bad_name

3 entidades tienen nombres truncados en Socrata:
- `ANTIOQUIA - CORPORACIoN HOSPITAL INFANTIL CONCEJO DE ME` (truncado)
- `SAN ANDReS; PROVIDENCIA Y SANTA CATALINA - ALCALDiA MUN` (truncado)
- `MAGDALENA - CAMARA DE COMERCIO DE SANTA MARTA ¿ SANTA M` (carácter basura "¿")

### 4. Caso concreto — Ataco, Tolima

`TOLIMA - ALCALDIA MUNICIPIO DE ATACO` aparece en f789-7hwg con el proceso
**AMA-IMC-024-2026** (CONVOCADO, Mínima Cuantía, COP 49.000.000, cargado
2026-05-20). El número AMA-IMC-027-2026 no existe — es AMA-IMC-024-2026.

Clasificación: `matched_existing` (alcaldías de Tolima ya están en el
gazetteer vía SECOP II).

---

## Principio de Diseño

> No crear dos gazetteers separados.
> Una entidad canónica + aliases por fuente (SECOP I, SECOP II).

Cada entidad del crosswalk tiene:
- `secop1_name`: nombre como aparece en f789-7hwg / rpmr-utcd
- `canonical_name`: nombre normalizado único
- `aliases`: lista de variantes por fuente
- `classification`: matched_existing | secop1_only | needs_review | bad_name

---

## Datos

- **JSON completo**: `app/data/entity_source_aliases/secop1_entities_sample.json`
- **Crosswalk model**: `app/data/entity_source_aliases/entity_crosswalk_sample.json`
- **Modelo de diseño**: [SECOP1_ENTITY_CROSSWALK_MODEL.md](SECOP1_ENTITY_CROSSWALK_MODEL.md)

---

## Próximos Pasos

1. [ ] Revisar manualmente las 14 `needs_review` (ESEs y hospitales)
2. [ ] Completar nombres truncados (`bad_name`) consultando el registro completo
3. [ ] Agregar al gazetteer las 41 `secop1_only` con aliases fuente-específicos
4. [ ] Generar aliases automáticos: sin tildes, sin "DEPARTAMENTO - " prefijo, etc.
5. [ ] Integrar en `entity_resolver.py` con discriminación por fuente

---

## Addendum C1A.1 — Hardening (2026-05-29)

### canonical_id

Cada entidad ahora tiene un `canonical_id` estable derivado de su nombre
y departamento (no de un contador). Ejemplos:

| Entidad | canonical_id |
|---------|-------------|
| TOLIMA - ALCALDIA MUNICIPIO DE ATACO | `municipio_ataco_tolima` |
| SANTANDER - CAMARA DE COMERCIO DE BUCARAMANGA | `camara_comercio_bucaramanga_santander` |
| VALLE DEL CAUCA - CAMARA DE COMERCIO DE CALI | `camara_comercio_cali_valle_del_cauca` |
| GUAINIA - GOBERNACION | `gobernacion_guainia` |

### source_aliases vs user_aliases

- **source_aliases**: Nombres tal cual aparecen en los datasets Socrata.
- **user_aliases**: 249 variantes generadas automáticamente para búsqueda
  (sin prefijo departamental, formas cortas, abreviaciones).

### integration_ready

De 86 entidades, 69 son `integration_ready`. Las 17 excluidas:
- 3 `bad_name` (nombres truncados)
- 14 `needs_review` (ESEs y hospitales requieren verificación manual)

Ver [SECOP1_ENTITY_CROSSWALK_MODEL.md](SECOP1_ENTITY_CROSSWALK_MODEL.md) v0.2.0.

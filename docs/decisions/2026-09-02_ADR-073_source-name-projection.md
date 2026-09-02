# ADR-073 — La proyección de comprador por fuente es una autoridad independiente

**Fecha:** 2026-09-02  
**Estado:** Accepted for implementation — `SOURCE_NAME_PROJECTION_V1`

**Evidencia:**
[`SOURCE_NAME_PROJECTION_V1.md`](../gazetteer/SOURCE_NAME_PROJECTION_V1.md) y
[`source_name_projection_v1.json`](../../artifacts/audits/source_name_projection_v1.json).

## Contexto

`EntityResolver` devuelve un `official_name` que cumple simultáneamente dos
papeles: identidad/display humano y string proyectado contra `entidad` o
`nombre_entidad`. DAPRE demuestra que esos papeles no son equivalentes:

```text
canonical/display  DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA
procesos           DAPRE  (17.128)
contratos          DAPRE  (4.351)
```

El string largo devuelve cero filas en ambos datasets. Cuatro identidades más
demuestran nombres distintos por dataset. En cambio, CGR, Cundinamarca y Valle
demuestran otro problema: varias unidades físicas bajo una raíz, que exige
Buyer Groups y no puede entrar por la misma abstracción.

## Decisión

La proyección de nombres físicos será una autoridad tipada e independiente del
gazetteer de aliases y de la identidad canónica.

La representación persistente será un sidecar dedicado; un loader tipado debe
validar:

- `identity_key` y `canonical_name` únicos;
- datasets conocidos;
- 1..N nombres no vacíos por binding;
- nombres sin duplicados;
- fecha y procedencia de la evidencia.

La API será plural:

```python
project_buyer_names(canonical_identity, dataset) -> tuple[str, ...] | None
```

Un binding produce comparaciones exactas unidas por `OR`. `None` conserva el
camino actual para compatibilidad. La lista vacía es un error de carga. El
proyector no hace fuzzy, no descubre nombres en red y no cambia el display.

V1 liga sólo DAPRE. El NIT es la llave de identidad y evidencia de auditoría,
no el predicado runtime por defecto. Un filtro por NIT requiere un binding
posterior que pruebe formatos por dataset y ausencia de unidades hijas.

## Consecuencias

- La identidad permanece estable aunque cambien strings físicos.
- Los aliases siguen significando input humano, no transporte Socrata.
- Una identidad puede tener 1..N nombres por dataset sin relajar la consulta.
- Los compradores sin binding conservan exactamente el comportamiento actual.
- El drift queda fechado y revisable en datos.
- `QueryFrame`/`resolved_params` necesitarán transportar identidad o aplicar la
  proyección después de resolver y antes de construir SoQL.
- La admisión de “Presidencia de la República” en `QueryRouter` sigue siendo
  una brecha separada; este ADR no autoriza arreglarla con aliases nuevos.

## Límites

Esta decisión no autoriza Buyer Groups, agregación por NIT, CGR, Cundinamarca,
Valle, migración masiva del gazetteer, universidades completas, fuzzy, cambios
de geografía, Radar, Actor ni transporte SODA3.

## Alternativas descartadas

1. **Campos en `aliases_db.json`:** vuelve a mezclar ownership de entrada,
   display e infraestructura de fuente.
2. **Sidecar sin contrato tipado:** separa archivos, pero no impone cardinalidad,
   datasets ni procedencia.
3. **Derivación dinámica por NIT:** introduce red y drift en runtime y convierte
   sin revisión un NIT común en Buyer Group.
4. **Cambiar el canonical a `DAPRE`:** arregla el WHERE destruyendo la identidad
   humana y no generaliza a splits por dataset.


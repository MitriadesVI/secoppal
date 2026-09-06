# Modelo consultable de las seis capas — V1

Implementación sobre `d608336` / ADR-092, en `codex/procurement-layer-model`.
La orden de esta entrega es implementar primero el modelo y las conexiones
existentes. `CARRILES.md` describe el experimento anterior, no limita esta entrega.

El contrato V0 ya enumeraba tipos y relaciones. Esta extensión construye sus
instancias: nodos separados por autoridad, relaciones con evidencia por fila,
observaciones que conservan discrepancias y una consulta de vecinos acotada.
Es un modelo en memoria y un comando offline; no activa una capacidad en la app.
No importa el nuevo modelo ningún módulo de `app/`. Conserva el estado SHADOW
de ADR-092, sin modificar el registro ni repinear su auditoría histórica.

| Capa | Objetos | Conexión implementada |
|---|---|---|
| 1. Dónde | Territory | Afiliación acreditada de Institution; no sede ni lugar de ejecución |
| 2. Quién compra | Institution con BuyerUnit productiva; BuyerGroup separado | Publicación mediante nombres físicos acreditados; membresía declarada |
| 3. Qué tramita y contrata | Process, Contract | Asociación por portafolio único, 0..n contratos por proceso |
| 4. Con qué registro participa | SECOP Supplier, Supplier Group | Contratista reportado, participación observada y membresía registrada |
| 5. Identidad documental | Actor | Adaptador de IdentityMatch existente; no conecta cuentas con actores |
| 6. Qué planea comprar | PAA Item reservado | DEFERRED; todavía sin identidad materializable |

## Uso

Desde el worktree de esta entrega:

```bash
.venv/bin/python scripts/explore_procurement_layers.py --cohort baranoa --output /tmp/baranoa-layers.json
.venv/bin/python scripts/explore_procurement_layers.py --cohort baranoa --node territory:08078 --hops 1
```

`--output` exige un archivo nuevo, nunca sobrescribe uno existente. Sin esa
opción, el JSON sale por stdout. El comando no hace red ni escribe en `data/`.
Las cohortes disponibles son Baranoa, Barranquilla, Bucaramanga, Yumbo y Atlántico,
exclusivamente con las capturas existentes. Una cohorte o un ID desconocido falla;
no vuelve silenciosamente a Baranoa. Los saltos admitidos son de 0 a 3.

API Python offline:

```python
from scripts.procurement_layers.build import build

graph, scope = build("baranoa")
snapshot = graph.export()
view = graph.neighborhood("territory:08078", hops=1)
```

El adaptador `scripts.procurement_layers.actors.add_actor(graph, match, evidence)`
acepta un `IdentityMatch` existente con recibo de la misma fuente. Conserva tipo,
número base, DV, crudo, confianza y motivos. UNKNOWN sigue siendo UNKNOWN. Un
match sin clave registra una incidencia. No toma `codigo_proveedor` como documento
ni consulta Actor Profile. Las capturas de las cinco cohortes no acreditan ese
puente: por eso los snapshots de ejemplo no contienen actores.

## Evidencia y límites

Cada observación conserva fuente, archivo, SHA-256, localizador de fila y fecha
de captura. Antes de leer un recibo se verifica su hash contra el manifiesto
congelado, el hash del payload, el estado MATERIALIZED y el dataset. Las consultas
de padres y de comprobación de IDs deben ser no truncadas. Se conserva el hash
del manifiesto y el de cada entrada utilizada; esto prueba integridad respecto de
esa versión local, no autenticidad criptográfica independiente de SECOP.

El modelo usa los adaptadores existentes `result_identity`, proyección de nombres
físicos, Territorial Knowledge, BuyerGroups y las uniones auditadas. No materializa
una institución cuando falta un único ID lógico PRODUCT_RECOGNIZED. Atlántico
conserva sus registros transaccionales y declara esa falta de identidad institucional.

`jbjy.proceso_de_compra = p6dx.id_del_portafolio` sólo conecta un contrato cuando
el padre es único. Ambigüedades, huérfanos e identidades ausentes aparecen en
`issues`. Los contratos sin enlace permanecen como nodos; ausencia de relación
no significa ausencia del contrato. Los códigos de cuenta conservan ceros iniciales
y rechazan espacios, flotantes, signos, dígitos no ASCII y códigos formados sólo por
ceros. Nombres, NIT y DIVIPOLA no sustituyen el ID lógico de una institución.

La participación acredita una observación proponente→proceso. No inventa un ID de
oferta ni de lote, ni declara ganador o competencia. HAS_CONTRACTOR conserva el
estado y fecha de firma reportados; no afirma por sí solo que exista contrato firmado.

Los grupos se seleccionan cuando su código o el de un integrante aparece entre
las cuentas observadas de la cohorte. Se incluyen sólo membresías capturadas. No
se deriva vigencia de la fecha de creación del grupo. El tipo jurídico de grupo
queda desconocido: estas capturas no acreditan distinguir UT de consorcio.
Porcentaje y liderazgo pertenecen a la membresía, con crudos y desconocidos intactos.
Un código de grupo coincidente con una cuenta produce `supplier_ref` con testigos
de ambas fuentes en las observaciones del grupo; no fusiona los dos nodos ni crea
una relación primaria adicional. La navegación sigue únicamente los ocho tipos
de relación aceptados, no salta por esa referencia auxiliar.

Todas las relaciones de los snapshots son SHADOW con cobertura PARTIAL. Incluso
la selección de un BuyerGroup en una cohorte es sólo un subconjunto del registro
declarado completo. El intervalo de procesos y el muestreo de contratos difieren
y se explican en `scope`. `absence_means_zero=false` rige también vistas vacías.
En una vista, los conteos por capa corresponden sólo a sus nodos; las incidencias
y la procedencia de scope siguen describiendo el snapshot de la cohorte completa.

Duplicados exactos no multiplican relaciones; se conservan todos sus recibos.
Valores discrepantes permanecen en observaciones separadas. Ningún valor se suma
ni se atribuye automáticamente a integrantes. La consulta de vecinos devuelve
relaciones observadas y nunca fabrica relaciones transitivas o identidad legal.

## Verificación

Los logs literales y el resumen de cohortes están en
`artifacts/audits/procurement_layers_v1/`. Las pruebas focales incluyen Hypothesis
determinista, integridad de recibos, las cinco cohortes, el CLI y regresión del
contrato V0. Codegraph CLI se sincronizó en este worktree; `affected` no encontró
tests, así que se verificaron imports con `query` y se seleccionaron los focales
manualmente. No se ejecuta la suite completa ni se realiza una consulta SECOP.

Pendientes: auditoría de identidad cuenta→Actor, identidad PAA y vínculos PAA→proceso,
prueba de vigencia y tipología jurídica de grupos, y una futura integración de la
vista en la app con gates propios. Esta entrega no hace merge ni push.

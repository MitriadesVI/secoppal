# Procurement Relationship Foundation V0

Territorio → institución → proceso → contrato → proveedor SECOP → actor legal.

Esta secuencia explica el producto, pero sus flechas tienen autoridades distintas.
Hoy existe afiliación territorial acreditada en conocimiento shadow; la institución
productiva se vincula a filas de procesos y contratos mediante bindings exactos.
La cuenta proveedora aparece en contratos y participaciones observadas. **Proveedor
SECOP → Actor no está acreditado en V0**. Actor Profile conserva su propia identidad
tipada; no certifica por sí solo todas las características jurídicas de una persona.
PAA queda declarado, sin identidad materializable ni conexiones a procesos.

**Estado: SHADOW_FOUNDATION.** Contrato de relaciones, no base de grafos. No contiene
un censo de edges de Colombia. No hay warehouse, graph DB, runtime wiring, merge ni push.
Base congelada: `c79f8460c84ab26631c2534af106405cb16803f2`.
Auditoría previa leída: `b936d0f91974e40114c777cf041ea55fe3849955`, hipótesis,
sin cherry-pick. Las conclusiones aceptadas utilizan 71 recibos propios materializados.

## Identidades

| Tipo | ID graph-ready | Autoridad / límite |
|---|---|---|
| TERRITORY | `territory:<divipola>` | 1.103 territorios de ADR-080; departamento/región son metadata |
| INSTITUTION | `institution:<logical_id>` | `Institution.institution_id`; conserva estado, BuyerUnit opcional, territorio y familia |
| BUYER_GROUP | `buyer_group:<group_id>` | grupo declarado versionado; no institución jurídica, NIT ni territorio |
| PROCESS | `process:p6dx-8zbt:<stable_key>` | result_identity, ID primario o referencia estable, verbatim |
| CONTRACT | `contract:jbjy-vk9h:<stable_key>` | result_identity, ID primario o referencia estable, verbatim |
| SECOP_SUPPLIER | `secop_supplier:<platform_code>` | cuenta SECOP; códigos exactos no nulos, no ceros; nunca NIT |
| SUPPLIER_GROUP | `supplier_group:<group_code>` | ceth.codigo_grupo; tipo distinto de cuenta y Actor |
| ACTOR | `actor:<document_type>:<base_number>` | IdentityKey; NIT, CC, UNKNOWN. UNKNOWN no acredita tipo conocido |
| PAA_ITEM | `paa_item:<future_authority>` | tipo reservado; identidad y materialización pendientes |

El fallback de referencias conserva exactamente las garantías de result_identity:
no se declara unicidad global adicional ni se normalizan espacios. La Gobernación
se sondea como `DEPARTAMENTO DEL ATLANTICO**`, canonical exacto existente; al no tener
ID lógico en Territorial Knowledge, no recibe un nodo INSTITUTION inventado.

## Estados y decisiones de vocabulario

ASSERTION responde si una fila válida acredita la relación que se publica.
COVERAGE responde qué universo cubre esa evidencia. RUNTIME responde si este edge
graph-ready puede consumirse actualmente. **Todos los nuevos edges V0 son SHADOW**.
Una fuente o mecanismo productivo preexistente no hace productivo este contrato.
En particular, BuyerGroup ya tiene selección productiva, pero este loader no participa
en ella. Se conserva COMPLETE sólo respecto de la membresía declarada versionada.

La orden proponía PRODUCTIVE_METADATA_ONLY para IN_TERRITORY. Se conserva el
vocabulario cerrado PRODUCTIVE/SHADOW/DEFERRED y prevalece ADR-080 SHADOW_NO_RUNTIME.
No se amplía la autoridad territorial ni se autoriza metadata runtime en esta entrega.

| EDGE | WHAT IT MEANS | SOURCE | ASSERTION | COVERAGE | RUNTIME |
|---|---|---|---|---|---|
| BID_ON | cuenta observada como proponente del proceso | hgi6 + p6dx | SUPPORTED | PARTIAL | SHADOW |
| HAS_CONTRACTOR | cuenta proveedora consignada en el registro contractual | jbjy | SUPPORTED | PARTIAL | SHADOW |
| HAS_RECORDED_CONTRACT | contrato referencia portafolio de un proceso único | jbjy + p6dx | SUPPORTED | PARTIAL | SHADOW |
| IN_TERRITORY | afiliación acreditada; no sede ni ejecución | Territorial Knowledge | SUPPORTED | PARTIAL | SHADOW |
| MEMBER_OF_SUPPLIER_GROUP | membresía registrada de cuenta en grupo | ceth | SUPPORTED | PARTIAL | SHADOW |
| PLANNED_AS | PAA → proceso, prohibido en V0 | pendiente | UNSUPPORTED | UNKNOWN | DEFERRED |
| PUBLISHED | institución publica registro de proceso | p6dx + binding | SUPPORTED | PARTIAL | SHADOW |
| PUBLISHED_CONTRACT | institución publica registro de contrato | jbjy + binding | SUPPORTED | PARTIAL | SHADOW |
| REPRESENTS_ACTOR | identidad cuenta → Actor sin autoridad aceptada | pendiente | UNSUPPORTED | UNKNOWN | DEFERRED |
| SELECTED_IN_BUYER_GROUP | selección declarada exacta | buyer_groups.json | SUPPORTED | COMPLETE | SHADOW |

IN_TERRITORY y SELECTED_IN_BUYER_GROUP proyectan autoridades locales acreditadas.
PUBLISHED, PUBLISHED_CONTRACT, HAS_CONTRACTOR y MEMBER_OF_SUPPLIER_GROUP afirman
registros de fuente con endpoints acreditados. BID_ON y HAS_RECORDED_CONTRACT
requieren joins exactos entre fuentes; no son inferencias por nombre, NIT o similitud.
Las afirmaciones son sobre lo registrado; no existe estimación independiente de
accuracy de la UI, validez de oferta, personalidad jurídica o cumplimiento contractual.

## Proceso → contrato: hipótesis refutada y puente aceptado

`jbjy.proceso_de_compra = p6dx.id_del_proceso` dio 0/1.250: BDOS y REQ son claves
diferentes. El diccionario publica `id_del_portafolio` como identificador del
portafolio al que corresponde el proceso. La relación aceptada utiliza:

`jbjy.proceso_de_compra = p6dx.id_del_portafolio → result_identity(p6dx)`.

Sólo se emite cuando el portafolio resuelve a **un único ID estable de proceso**.
Un portafolio con múltiples REQ no elige el primero. No se reemplazan prefijos ni
se usa el nombre o la referencia humana para unir. El nombre HAS_RECORDED_CONTRACT
expresa asociación registrada; GENERATED_CONTRACT introduciría una afirmación causal
más fuerte. PUBLISHED_CONTRACT sustituye a SIGNED/HAS_CONTRACT de institución porque
el diccionario de nombre_entidad acredita publicación, y hay borradores/cancelados.

Muestra: primeros 250 registros por id_contrato de cada comprador, todas las fechas.
Sesgo deliberado hacia el orden de ID; no representa prevalencia temporal ni nacional.
Consultas de padres sin filtro temporal para no fabricar huérfanos por la ventana.

| Comprador | Filas | Join unívoco | Huérfanos | Ambiguas | Procesos con varios contratos |
|---|---:|---:|---:|---:|---:|
| atlantico | 250 | 250 (100.0%) | 0 | 0 | 19 |
| baranoa | 250 | 248 (99.2%) | 2 | 0 | 36 |
| barranquilla | 250 | 250 (100.0%) | 0 | 0 | 5 |
| bucaramanga | 250 | 237 (94.8%) | 0 | 13 | 4 |
| yumbo | 250 | 223 (89.2%) | 21 | 6 | 19 |

Total: 1.208 relaciones unívocas, 23 huérfanas y 19 ambiguas. Doce claves de
portafolio corresponden a varios procesos en las capturas (siete Bucaramanga,
cinco Yumbo). Las filas ambiguas no crean edges. La cardinalidad proceso→contratos
es 0..n; la inversa aceptada es 0..1, condicionada al gate de unicidad.
Los 73 códigos de proveedor nulos/no acreditados/cero en los 1.250 registros se
conservan en evidencia y se excluyen del endpoint; no se inventan desde documentos.
HAS_CONTRACTOR significa proveedor consignado en esa fila; consultar obligaciones
firmadas requiere además estado/fecha. No es prueba automática de firma.

## Participación observada y cobertura

Ventana: 2025-01-01 inclusive a 2026-09-07 exclusive, bindings exactos del comprador.
Join rate = procesos hgi6 enlazados / procesos hgi6 con código válido.
Coverage = procesos hgi6 dentro del universo / IDs p6dx únicos del corte.
Duplicados = filas válidas excedentes sobre pares proceso/código distintos.
Las búsquedas exactas fuera del corte se distinguen de cobertura dentro del corte.

| Comprador | Filas hgi6 | Procesos hgi6 | Join rate | Universo p6dx | Coverage | Exceso duplicados |
|---|---:|---:|---:|---:|---:|---:|
| atlantico | 1375 | 437 | 99.313501% | 3091 | 14.040764% | 2 |
| baranoa | 102 | 27 | 100.0% | 401 | 6.733167% | 0 |
| barranquilla | 939 | 265 | 96.226415% | 3864 | 6.599379% | 1 |
| bucaramanga | 1236 | 398 | 99.748744% | 9888 | 4.014968% | 2 |
| yumbo | 334 | 225 | 100.0% | 7298 | 3.083036% | 0 |

Bucaramanga necesitó una segunda página de hasta 10.000 filas; la captura terminó
antes del límite de 20.000. La paginación no es un snapshot transaccional de Socrata.
Cada recibo conserva hora, consulta y hash; las cifras describen las capturas.
No se permiten competencia por lote ni interpretar ausencia como cero proponentes.
Bogotá ejecutivo: 38 miembros exactos acreditados; control transaccional de agosto
2026 sobre los nombres proyectados de todos los miembros, sin escoger uno solo.

## Grupos e identidad de plataforma

Conteo propio ceth: 701.182 filas; 9.105 sin codigo_participante; cero sin codigo_grupo.
Coincide con el conteo previo, sin utilizarlo como respuesta de la sonda nueva.
Cero claves grupo/participante duplicadas en agregado del servidor; cero duplicados
de la proyección de códigos y atributos seleccionados. No se afirma comparación de
todas las columnas físicas del dataset. Se capturan 10 grupos con sus miembros
completos y ejemplos de activo/inactivo, líder y participación.

Coincidencias exactas: 455 ceth.codigo_grupo↔hgi6.codigo_proveedor;
149 ceth.codigo_participante↔hgi6.codigo_proveedor;
42 hgi6.codigo_proveedor↔jbjy.codigo_proveedor;
6 ceth.codigo_participante↔jbjy.codigo_proveedor.
Son testigos de compatibilidad acotada y coherente con los diccionarios oficiales;
no prueban universalmente igualdad de todos los namespaces históricos.

SupplierGroup conserva su ID y PLATFORM_SUPPLIER_CODE con supplier_ref explícito,
respaldado por el testigo exacto de ambas fuentes. No se colapsa con SECOP_SUPPLIER.
El código se conserva como texto entero ASCII; no se convierte float/int, no se
recortan ceros iniciales ni se usa NIT. Los códigos compuestos sólo por ceros se
rechazan como endpoints no acreditados. Participación y liderazgo son atributos del
edge; activo y creación pertenecen al grupo. Desconocido no se convierte en falso.
Porcentajes fuera de [0,100] quedan nulos con el valor original conservado.
No se deriva fecha efectiva de membresía de la fecha de creación del grupo.

## Deudas y relaciones derivadas

SUPPLIER_TO_ACTOR_IDENTITY_AUTHORITY_REQUIRED permanece DEFERRED: no hay fuente
canónica acreditada para conectar cuenta con IdentityKey. NIT/nombre/código parecido
no resuelven la deuda. PAA_ITEM y PLANNED_AS permanecen DEFERRED_TO_PAA_AUDIT.

COMPETES_WITH, PARTNERED_WITH, BUYER_CONTRACTED_WITH, WON_AGAINST,
FREQUENT_PARTNER, SAME_MARKET y RELATED_TO nunca son edges primarios. Coocurrencia
en un proceso/grupo es un soporte estructural; para interpretarla como competencia,
asociación o mercado se requieren ventana, métrica, cobertura y alcance. No se
atribuye a cada integrante el valor completo del contrato de un grupo.

## Reproducción y validación

`app/data/procurement_relationship_registry_v0.json` contiene sólo el contrato.
El loader lee únicamente ese JSON, valida y devuelve copias; no hace red ni consulta
otros archivos. supported_edges filtra ASSERTION, **no autoriza runtime**.
Scripts separados realizan identidad, sondeo, joins, membresías, gates y reporte.

```bash
.venv/bin/python scripts/audits/audit_relationship_foundation_v0.py
.venv/bin/python scripts/audits/audit_relationship_foundation_v0.py --check
.venv/bin/python -m pytest tests/test_procurement_relationships.py tests/test_module_size_limits.py -q
```

Offline por defecto; --check no escribe ni hace red. --capture sólo llena recibos
faltantes y no sobrescribe capturas anteriores. Un recibo fallido, cambiado o corrupto
falla cerrado. Presupuesto: 120 requests por ejecución, 10.000 filas por request,
8 MB por respuesta, timeout 35 s. Agregados nacionales no materializan Colombia.
La evidencia anterior de refutación se conserva aunque deje de ser necesaria para
el join final. El manifiesto congela todos los recibos; replay reconstruye los
artefactos y el registro byte por byte sin red.

Los `source_authorities.local_digests` describen los blobs Git de
`FOUNDATION_HEAD = d608336a32553ac11a69924d663491ce852e1446`, no los bytes de
un checkout futuro. Las referencias y fuentes permitidas viven en
`scripts/audits/relationship_foundation/frozen_delivery.py`; si falta Git, el
commit o un blob, el replay falla cerrado sin recurrir al archivo actual.
El aislamiento sigue inspeccionando los imports del checkout actual y los
efectos de la entrega en `git diff BASE FOUNDATION_HEAD`. Son contratos distintos.

Ruff, Semgrep y pytest tienen logs literales en validation.json y archivos anexos.
Semgrep debe separar sus diez hallazgos baseline del delta propio. No se ejecuta la
suite completa: el módulo es nuevo y shadow; no se alteró código productivo existente.
Hypothesis comprueba conservación de identidad, separación de tipos, abstención por
endpoint ausente y rangos de participación. Se falsifican portafolios ambiguos,
duplicados y promoción accidental. Codegraph se consultó sobre la base congelada;
AST/inspección de imports y diff comprueban aislamiento de los archivos nuevos.

## Decisión y siguiente fase

ADR-092 acepta sólo este contrato shadow, tras el barrido global cuyo máximo fue
ADR-091 en otro worktree. No reutiliza los rangos 081–090 de otros carriles.
NEXT: ENTITY_ANALYSIS_V1_INTEGRATION, **requiere decisión y gates propios**; este PASS
no autoriza consumo runtime, promoción de identidad, merge ni push.

# Contract Universe V1 — política operativa

Fecha: 2026-09-08. Base: `c52d7eda355f44278164d4cb9803a199eec83486`.
Esta política sustituye exclusivamente la afirmación de INV-005 según la cual
cada fila de `jbjy-vk9h` acredita celebración. `Firmado` y `Celebrado` siguen
sin convertirse en literales de `estado_contrato`.

## Adjudicación de estados

| Estado de Socrata | Universo celebrado | Criterio |
|---|---|---|
| En ejecución | Incluido | Estado posterior admitido; no exige fecha |
| Cerrado | Incluido | Histórico admitido |
| Modificado | Incluido | Estado posterior admitido |
| terminado | Incluido | Histórico admitido |
| cedido | Incluido | Estado posterior admitido |
| Suspendido | Incluido | No equivale a Cancelado |
| Prorrogado | Incluido | Estado posterior admitido |
| Aprobado | Incluido operativamente | Adjudicación descrita abajo; no produce fecha ni año |
| Cancelado | Excluido | Incluso si tiene fecha de firma publicada |
| Borrador | Excluido | Incluso si tiene fecha de firma publicada |
| enviado Proveedor | Excluido | Incluso si tiene fecha de firma publicada |
| En aprobación | Excluido | No equivale a Aprobado; otra decisión necesita evidencia adicional |
| NULL, vacío, estado desconocido o centinela textual | Indeterminado; no se incluye | Una fecha aislada no acredita un estado admitido |

`Aprobado`: el catálogo versionado `app/data/estados.py` documentaba ocho
estados con señal de firma, incluido Aprobado, mientras `contract_signature.py`
lo clasificaba junto a Borrador y En aprobación. Esas dos interpretaciones
no podían conservarse como una misma autoridad.

La documentación oficial describe la aprobación interna previa al envío y la
aprobación/firma de las partes como pasos distintos. Las etiquetas nativas
«Pendiente de firma», «Firmado y enviado a la Entidad Estatal» y «Firmado» no
se trasladan como literales a Socrata. Fuentes consultadas:

- [Revisión y aprobación del contrato, CCE](https://operaciones.colombiacompra.gov.co/secop-ii/3247/23710/Revisi%C3%B3n%20y%20aprobaci%C3%B3n%20del%20Contrato).
- [Manual de régimen especial, CCE](https://www.colombiacompra.gov.co/sites/cce_public/files/cce_documentos/manual_modalidad_regimenespecial.pdf).
- [Cambio del gestor contractual en 2020, CCE](https://www.colombiacompra.gov.co/wp-content/uploads/2024/10/20200420infografiarealese20.2v3_0.pdf).

El contraste público de `source.json` hizo cinco solicitudes Socrata, timeout
15 segundos, sin credenciales: dos agregados por estado de los compradores
reportados vencieron; dos muestras ordenadas por ID devolvieron cinco registros
Aprobado cada una, todos con fecha de firma; una consulta puntual al primer ID
confirmó que Socrata acepta `upper(unaccent(trim(estado_contrato)))`.
Los timeouts no acreditan conteos ni ausencia. Las diez muestras son históricas
(2020), seleccionadas por ID ascendente: no son representativas del país ni de
la distribución temporal actual. Los resultados contienen URL, consulta,
fecha de observación y selección; no se hizo un barrido nacional.

**Decisión operativa, por inferencia:** admitir Aprobado como señal de inclusión,
con apoyo combinado del catálogo versionado y el contraste de fuente, y retirar
su clasificación automática como pre-firma. La documentación nativa ayuda a
separar los conceptos, pero no es un diccionario de correspondencias Socrata.
No afirmamos universalidad de fechas ni certificación jurídica de cada fila.
La regla admite también Aprobado sin fecha por estado; este extremo es una
inferencia de política, no una observación demostrada por las diez muestras.
La revisión puede cuestionar esta adjudicación sin alterar identidades ni años.

## Contrato técnico y de presentación

`ContractUniverse` tiene dos valores: `celebrated` y `source_records`.
`contract_universe.py` es la única definición de elegibilidad y predicado SQL.
La inclusión usa una lista positiva: un estado nuevo no entra automáticamente.
La fecha no rescata estados excluidos y no es obligatoria para estados admitidos.
El clasificador conserva la fila original. La comprobación de fechas reutiliza
`parse_iso_text`; el catálogo de sentinelas no tiene política autorizada para
`jbjy-vk9h.fecha_de_firma`, por lo que no se inventan umbrales como 1900 o 1970.
NULL, vacío, texto inválido y fecha utilizable no se usan indistintamente como
prueba de celebración. Una falta de fecha se presenta como fecha de firma no
publicada; una fecha de inicio conserva la etiqueta Inicio.

Search y sus COUNT/SUM/GROUP BY reciben el predicado antes de LIMIT/OFFSET.
Frame lo recibe desde `SourcePredicates`, antes de congelar cada plan y job;
`_FrozenWhereBuilder` consume ese WHERE, sin volver a interpretar raw_query.
El universo por defecto no satisface la guarda de alcance global.

El acceso explícito a cancelados, borradores, enviados al proveedor y todos los
registros conserva esas filas con etiqueta de registros. «Sin cancelados» no
amplía el universo. Suspendido se proyecta sin Cancelado. Una incompatibilidad
entre universo celebrado y un estado preparatorio/cancelado requiere aclaración.
Las consultas por ID/URL acceden al registro, aunque no sea elegible; si aparece
una fecha de firma en una fila no admitida, se muestra una nota de verificación.
El índice de proveedores conserva su función de identidad.

## Continuación y consumidores

| Consumidor | Autoridad efectiva y control |
|---|---|
| Search / count / observer | `_build_where`; fixture SQL y presupuestos existentes |
| Sumas y rankings | Builders existentes consumen el mismo WHERE; pruebas de agregados reales |
| Actor | Ocho aspectos existentes, incluidos clientes derivados; ON con Frame OFF/ON y control Actor OFF |
| Entity sin Frame | `project_source` estampa filtros antes de congelarlos; secciones usan SoQL común |
| Frame | Predicado propio, esquema 2 y catálogo versionado; publicación y recarga verificadas |
| Consulta dual | Hijo contractual estampado; hijo procesos sin claves contractuales |
| Páginas / exportación | Universo en QueryFrame, buffer y metadatos XLSX; cursor contractual versionado |
| Radar | Nuevo guardado conserva universo; replay lo verifica antes de llamar a la fuente |
| Consulta directa | Universo explícito de registros, estado crudo y traza |

Las continuaciones contractuales antiguas y los planes Frame incompatibles
requieren una nueva consulta. No se eliminan conversaciones ni archivos.
Los cursores exclusivos de procesos conservan su versión y sus oráculos.
Los radares anteriores no migran silenciosamente: requieren revisión.
La conversión de un perfil legacy que ya acredita esta política conserva su
universo explícito; el control Frame OFF → ON mantiene 35 registros de 2026.
El descubrimiento del Radar excluye los estados no admitidos antes del límite;
su seguimiento por ID conserva cambios de registros ya vigilados que salieron
del universo, lo declara y no los suma al conteo contractual.

No se modifican comprador, proveedor, monto, modalidad, tema, periodo, unidad de
conteo ni deduplicación por URL. No se añaden llamadas de relleno, conteos de
excluidos, flags, preferencias ni activación del Frame. El orden existente se
conserva y no se inventan fechas para sostener recencia.

## Trazabilidad para integración

Registrar esta política y sus controles en `docs/decisions_log.md` al integrar.
No se reservó ADR ni se editó la bitácora compartida. `AGENTS.md` y
`docs/orquestacion/CARRILES.md` fueron leídos. Las rutas `docs/INVARIANTS.md` y
`docs/SEMANTIC_CONTRACT.md`, comprobadas durante la búsqueda acotada, no existen;
no se reconstruyeron. INV-005 se encontró y actualizó en el documento técnico
existente. La evidencia histórica y los snapshots originales se conservan.

# Contract Universe V1.1 — adjudicación operativa

La orden del usuario adjudica Aprobado. Esta política no afirma una equivalencia
jurídica universal ni infiere fecha o año.

| Señal publicada | V1 | V1.1 |
|---|---|---|
| Aprobado con fecha ISO utilizable | Incluido | Incluido |
| Aprobado sin fecha, inválida, centinela textual o no observable | Incluido por estado | Indeterminado, no incluido |
| En ejecución, Cerrado, Modificado, terminado, cedido, Suspendido, Prorrogado | Incluido independientemente de fecha | Sin cambio |
| Cancelado, Borrador, enviado Proveedor, En aprobación | Excluido incluso con fecha | Sin cambio |
| Estado desconocido o ausente | No incluido | Sin cambio |

`eligible()` conserva la fila y exige `parse_iso_text` utilizable únicamente
para Aprobado. No se inventan umbrales de año: el catálogo vigente no declara
1900/1970 como centinelas del campo fecha_de_firma. Los centinelas textuales,
fechas imposibles, NULL y campo no observado no acreditan una fecha utilizable.
`contract_signature.py` retira únicamente Aprobado de los estados que permiten
inferir firma sin fecha; conserva las demás reglas de ese eje.

El predicado parentizado es la unión de los siete estados posteriores y
`APROBADO AND date_extract_y(fecha_de_firma) IS NOT NULL`, antes de LIMIT,
COUNT, SUM y GROUP BY. La función se usa para comprobar presencia de un valor
temporal utilizable, no para asignar un año al expediente. Está documentada
para WHERE y Floating Timestamp en [Socrata](https://dev.socrata.com/docs/functions/date_extract_y).
La columna tipada no admite texto arbitrario como fecha: ver
[Floating Timestamp](https://dev.socrata.com/docs/datatypes/floating_timestamp).
No se hizo SECOP live. La paridad se ejecuta en SQLite con una emulación
explícita del tipo temporal; no certifica otros dialectos ni datos nacionales.

## Autoridad de admisión

- `celebrated` es el default contractual.
- `source_records` requiere la construcción explícita completa del turno,
  una autoridad estructurada versionada o el lookup directo existente.
- La admisión `apply_request(params, None)` no deduce consentimiento de
  estado, familia o campo materializado. Un estado incompatible conserva un
  error de aclaración antes de SQL. Un universo de registros sin versión
  también requiere revisión en esta admisión.
- Se conserva la construcción nominal histórica `borradores de Cartagena`:
  núcleo de petición al inicio de cláusula y complemento, nunca una palabra
  aislada ni una palabra dentro del objeto o Actor.
- Los nombres exactos acreditados del comprador se protegen como superficies;
  la sintaxis de rol Actor tiene precedencia. No hay excepciones por nombre,
  stopwords nuevas, inferencia de identidad ni recompilación desde raw_query.
- Dos estados explícitos excluidos, en ambos órdenes, requieren aclaración.
  El adaptador UniverseRequest vigente transporta un estado singular; no se
  fabrica una unión a partir del primer match ni se ejecuta media composición.
- La combinación de procesos y registros explícitos requiere aclaración sin
  ninguno de los jobs: el adaptador de registros no acredita el segundo hijo
  de fuente en los dos caminos vigentes. Se usa source_request_occurrences y
  el canal existente de issues/claims, no el veto global por palabra procesos.
  Procesos y contratos ordinarios siguen ejecutando ambos hijos.

## Persistencia

`POLICY_VERSION = contract-universe-1.1` y
`CATALOG_VERSION = entity-profile-frame-2-contract-universe-1.1`.
El Frame conserva `schema_version = 2`: sus campos y tipos no cambiaron.
El formato del buffer tampoco necesita cambiar: la revisión se exige por sus
campos de universo y versión, que son distintos del formato contenedor.

Se rechazan para continuación los contratos de universo V1 y anteriores.
No se actualizan silenciosamente buffers, Frames, planes, registros de ejecución,
radares ni exports guardados. Las búsquedas completas nuevas siguen disponibles.
`v1_serialized.json` fue emitido ejecutando los serializadores reales de
067b39770592640b5aef3898cbc99d8f82c59e0f en una copia desechable. Los cinco
controles de replay verifican conversación, buffer, Frame/plan/registro, Radar
y export; conservan los bytes o el round-trip del artefacto original.
También se mantienen los cuatro controles independientes de BASE main.

## Presentación

Se elimina record_terms y toda sustitución sobre la respuesta compuesta.
Las etiquetas de métricas y navegación se eligen desde el plan ejecutado antes
de interpolar títulos, nombres y valores. Los títulos fuente del perfil no se
normalizan ni recortan. Los nombres oficiales de los datasets se conservan.
La cadena `elaboración de borradores de documentos y contratos de consultoría`
se comprueba idéntica en Search y perfiles con Frame OFF/ON.

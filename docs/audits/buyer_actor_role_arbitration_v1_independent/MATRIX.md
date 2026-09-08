# Matriz focal

Todas las expectativas canónicas de las semillas se fijaron antes de consultar
la salida del parser. Monto mínimo conserva `>=`, semántica vigente.

| Semilla | Índice | Frame OFF | Frame ON | Continuidad |
|---|---|---|---|---|
| Cartagena, 500 millones | Sin homónimo | PASS | PASS | Año + página disjunta |
| Cartagena, 500 millones | Un exacto | PASS | PASS | Año + página disjunta |
| Cartagena, 500 millones | Cuatro exactos | PASS | PASS | Año + página disjunta |
| Cartagena, 500 millones | Ausente | PASS | PASS | Año + página disjunta |
| Cartagena, 500 millones | Incompatible | PASS | PASS | Año + página disjunta |
| Barranquilla, 2000 millones | Sin homónimo | PASS | PASS | Año + página disjunta |
| Barranquilla, 2000 millones | Un exacto | PASS | PASS | Año + página disjunta |
| Barranquilla, 2000 millones | Cuatro exactos | PASS | PASS | Año + página disjunta |
| Barranquilla, 2000 millones | Ausente | PASS | PASS | Año + página disjunta |
| Barranquilla, 2000 millones | Incompatible | PASS | PASS | Año + página disjunta |

Cada PASS exige comprador correcto, contratos, monto, ausencia de Actor espurio,
WHERE efectivamente enviado, filas del universo correcto, respuesta no aclaratoria
y recuperación desde disco. Con página en buffer puede no haber una llamada
nueva: los IDs mostrados pertenecen al universo previamente consultado.

| Contraste | OFF/ON | Expectativa acreditada |
|---|---|---|
| FUNDACARIBE bare y verbal | PASS | Único → Actor y SQL documental; ambiguo → selección, sin SQL |
| Proveedor explícito nombre/documento | PASS | Actor/documento conservado; documento decide incluso con alias ambiguo |
| FUNDACARIBE con Cartagena | PASS | WHERE conserva ambos filtros independientes |
| Cartagena / UIS explícitamente proveedoras | PASS | Actor, sin comprador filtrado por el propio nombre |
| Proveedor largo con alias parcial | PASS | Actor completo explícito; bare conserva aclaración |
| UNIVERSIDAD SOLANO fuzzy | PASS | No autoriza veto global; conserva aclaración/Actor |
| Bogotá / Barranquilla geográficas | PASS | Sin Actor ni comprador pedido; city_to_entity se retira antes de resolver Actor |
| Barranquilla verbal | PASS | Actor explícito conserva su documento pese al topónimo |
| Software homónimo | PASS | Aclaración de intención, sin ejecutar |
| Comprador/proveedor negados, formas soportadas | PASS | Aclaración sin ejecución |
| Nueva semilla tras Actor confirmado o pendiente | PASS | Sin documento heredado; año y página conservan comprador |
| Refinamientos dependientes | PASS | Mantienen documento anterior y añaden año/comprador solicitado |
| Perfil institucional después de Actor | PASS | Ruta y respuesta de perfil institucional, dueño ENTITY_PROFILE con Frame ON |
| Artículos/tildes/mayúsculas/año/tema/orden/Galapa/Baranoa | PASS | Filtros e instituciones explícitas conservados |
| Negación corta «sin proveedor FUNDACARIBE» | INCORRECTA HEREDADA | BASE=candidato: excluye tema y devuelve contratos del proveedor |

`matrix.json` incluye respuestas, SQL, flags leídos, documentos sintéticos,
filas y trazas históricas/mutantes. Los archivos completos están en
`traces/independent-final.tar.gz`, `traces/independent-local.tar.gz`,
`traces/base-seeds.tar.gz`, `traces/pre-frame-seeds.tar.gz` y los archivos
`traces/mutation-*.tar.gz`. Los candidatos personales del índice real no se
exportan: sólo método, estado, mención autorizada y cardinalidad devuelta.

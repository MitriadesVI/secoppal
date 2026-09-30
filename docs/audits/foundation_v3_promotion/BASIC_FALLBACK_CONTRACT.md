# Contrato — búsqueda básica rotulada ante caída del proveedor (ADR-102 §12)

**Estado:** escrito y commiteado antes de implementar. Si la implementación no
puede cumplir cada cláusula sin crear una segunda autoridad semántica, el veredicto
es `BASIC_PROVIDER_FALLBACK = DEFERRED_UNSAFE` y el core Foundation no depende de él.

## Disparo (todas las condiciones)

1. El productor Foundation terminó en `PROVIDER_ERROR` (el proveedor lanzó una
   excepción: timeout, no disponible, error HTTP). Una respuesta recibida y
   malformada es `INVALID_PROPOSAL` y **nunca** dispara el fallback.
2. El turno ya fue reclamado por Foundation, es decir: SEARCH de primer turno que
   todos los dueños existentes declinaron (`ROUTE_OWNERSHIP_MATRIX.md`). No hay
   continuación ni job especializado.
3. Nunca por `INVALID_PROPOSAL`, `NEEDS_CLARIFICATION`, `UNSUPPORTED`, conflicto
   H7, error de admisión ni multi-job.

## Autoridad

No se crea una autoridad nueva. El fallback usa el parser determinista vigente del
producto (`QueryRouter.parse`, el mismo de `admit()` y del camino legado), **sin**
el nodo `llm_parse` ni ningún otro proveedor: `second_llm_calls = 0` por
construcción y por test. Sólo acepta un subconjunto cerrado de lo que ese parser
produce; todo lo demás bloquea.

## Seguridad (lista blanca cerrada)

Se ejecuta sólo si **todas** se cumplen sobre la salida del parser:

- claves materiales ⊆ {`dataset`, `objeto`, `entidad_resolved`,
  `departamento_resolved`, `fecha_desde`, `fecha_hasta`, `contract_universe`,
  `contract_universe_version`}; claves de metadatos toleradas: `dataset_explicit`,
  `entidad`, `entidad_resolution`, `departamento`, `departamento_resolution`,
  `_temporal_replace_scope`. Cualquier otra clave no vacía (valor, modalidad,
  estado, intención de oportunidad, `dataset_scope` dual, exclusiones, `objeto_or`,
  frases, contratista, actor, ciudad, …) → no seguro;
- `needs_llm` falso, sin `negation_requests`, sin controles analíticos (COUNT/SUM);
- ancla no temporal: al menos uno de `objeto`, `entidad_resolved`,
  `departamento_resolved` (nunca una consulta global ni sólo-fecha);
- `entidad`/`departamento` sólo con su forma `_resolved` (sin LIKE de respaldo);
- ningún término de `objeto` es un sustantivo de universo (`contrato(s)`,
  `proceso(s)`, `licitacion(es)`, `oportunidad(es)`, `convocatoria(s)`): la
  contaminación estructural conocida bloquea;
- el residuo material del parser está contenido en `objeto` (ninguna palabra
  material queda fuera de la consulta ejecutada);
- el builder legado no lanza `UnsafeGlobalQueryError`.

Si no es seguro: cero llamadas a la fuente, mensaje humano con opción de reintento,
`BASIC_FALLBACK_NOT_SAFE` en la telemetría.

## Ejecución

Una consulta y, si la página se llena, un conteo acotado, por
`_query_first_then_count` (mismos presupuestos del legado), con recibos
`foundation_basic_fallback:query|count`. No hay H4, relajación, sugerencias,
ranking ni observador.

## Rótulo visible (obligatorio)

La respuesta empieza con «Búsqueda básica — modo degradado», explica que el
intérprete avanzado no respondió, lista los filtros ejecutados (dataset, entidad,
departamento, tema, fechas), advierte que puede no conservar relaciones,
exclusiones o matices, e invita a reintentar.

## Telemetría

`degraded = true`, `degradation_source = "foundation_provider_error"`,
`fallback_mode = "legacy_deterministic_basic"`, `fallback_filters`,
`fallback_source_calls`, `advanced_provider_calls = 1`, `second_llm_calls = 0`,
`fallback_safe`, `fallback_label_shown`.

## Pruebas obligatorias

timeout; proveedor no disponible; error HTTP; payload malformado (→
`INVALID_PROPOSAL`, sin fallback); consulta simple segura; global; con negación;
dual; con valor; analítica; «contratos de primera infancia de la alcaldía de
barranquilla»; cero segunda llamada LLM; rótulo obligatorio; cero fallback
silencioso; actor y Suppliers nunca llegan aquí (dueños previos, ver matriz).

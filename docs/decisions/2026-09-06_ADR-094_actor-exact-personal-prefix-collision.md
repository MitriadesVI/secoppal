# ADR-094 — Actor Exact Personal Prefix Collision V1

Fecha: 2026-09-06. Estado: implementado en rama; promoción **NO_GO** por gate de
rendimiento. Base: `345804cf55becaf728d3ec9c20dc9e2f76a3a5b9`.
Rama: `feature/actor-exact-personal-prefix-collision-v1`.
El coordinador verificó numeración GLOBAL en todos los worktrees: máximo 093,
reserva exclusiva 094. La orden FINAL del usuario autoriza esta nueva política.

## Problema y decisión

Un alias N2 exacto que identifica una sola CC puede ser el inicio exacto de un
nombre más largo observado bajo otra CC. Por ejemplo, `ana maria lopez` y
`ana maria lopez perez` con documentos distintos. El exacto sigue existiendo;
la nueva decisión retira su suficiencia para autoatribuir contratos.

Se aplica únicamente después de obtener una sola IdentityKey exacta **entre
todos los tipos documentales**, y únicamente si esa identidad es CC. Se buscan
otras CC cuyos aliases N2 cumplan `alias.startswith(query + " ")`, con al menos
un token adicional. Se cubren todos los tamaños, incluyendo un token. Tipo CC
no certifica que el alias publicado sea un nombre personal limpio.

Se conserva N2 sin cambios, incluidos acentos, puntuación, mayúsculas, Unicode y
espacios. No se usa similitud, distancia, FTS, actividad contractual, compradores,
territorio, LLM ni ocurrencias como autoridad. Otra variante de la misma
IdentityKey no desafía el exacto. Exactos ya ambiguos, NIT y entradas sin exacto
conservan sus puertas anteriores. Documento explícito y selección explícita
continúan siendo autoridad sin volver a consultar el challenger.

## Tres contratos separados

1. **Existencia:** excluye la CC original antes de `LIMIT 1`. Devuelve
   `ABSENT_CONFIRMED`, `FOUND` o `INCONCLUSIVE`. FOUND conserva un testigo con
   IdentityKey, alias representante y evidencia presentable.
2. **Enumeración:** únicamente tras FOUND, recorre aliases en orden nominal y
   deduplica por IdentityKey antes de detenerse en el décimo challenger. El
   primer alias encontrado de cada identidad es su menor alias N2 en el rango.
3. **Presentación:** exacto original primero, máximo nueve challengers visibles,
   con ranks consecutivos. Orden por alias representante, tipo y número base.
   `alias_id`, inserción, BM25 y ocurrencias no deciden el orden. Se reutiliza
   la construcción existente de nombres observados desde el alias representante.

ABSENT_CONFIRMED permite `RESOLVED/exact_unique_alias`. FOUND produce
`AMBIGUOUS/exact_prefix_ambiguous`. El estado externo nuevo
`INCONCLUSIVE/exact_prefix_check_inconclusive` no contiene identidad ni
candidatos y explica por qué no terminó el chequeo. Bloquea otras sondas, FTS,
perfil y fallback temático; llega a `actor_scope_policy` también desde la sonda
conversacional. El documento declarado en el siguiente turno sustituye esa
incertidumbre mediante el merge de reemplazo de actor existente.

`candidates_has_more=true` prueba una décima identidad challenger no visible;
false acredita enumeración completa con hasta nueve; null significa
exhaustividad desconocida. `enumeration_incomplete` describe el fallo, no prueba
más opciones. Un fallo o contradicción en enumeración conserva AMBIGUOUS con
exacto y testigo, has_more=null, incomplete=true. No se calcula total exhaustivo.
Metadata vive en el resultado y pendiente; selección la elimina con el pendiente.
La traza reutiliza `candidates_count` y agrega esos dos campos cuando aplican.

La intención actor/tema precede a la selección de personas: cuando una colisión
prefix nueva coincide con un conflicto temático previo, no se crea pendiente ni
controles de rank hasta que el usuario pida explícitamente el proveedor. Se
conservan texto y cero controles del baseline de TRANSPORTE. Este guard no cambia
la presentación histórica de `exact_ambiguous_alias`.

## Acceso y presupuesto

Esquema 1 sin migración ni índice nuevo. Las dos SQL usan el autoindex UNIQUE
existente de `(normalized_alias, document_type, base_number)` con rango nominal
obligatorio. La collation real es BINARY y la codificación UTF-8; su orden coincide
con el de escalares Unicode. `lexicographic_successor(query + " ")` calcula el
límite superior, incluyendo acarreo y salto de surrogates. La frontera final es
siempre un espacio ASCII debido a N2. No se sustituye con LIKE.

EXPLAIN acredita `normalized_alias>? AND normalized_alias<?` en ambas consultas.
El plan incluye un orden parcial temporal; se mide la SQL entera, deduplicación,
elección del representante y materialización, no un rango preliminar.

El coordinador midió BEFORE antes del runtime y fijó 5 ms existencia y 20 ms
enumeración en `budget_contract.json`. Interrupción cooperativa mediante reloj
monotónico, progress_handler SQLite cada 100 instrucciones VM, y controles entre
lecturas y al materializar. Se limpia el handler incluso al interrumpir. No se
incrementa timeout SQLite. El SO puede desprogramar el proceso o bloquear una
lectura; la siguiente comprobación rechaza acreditar ausencia fuera del plazo.

El primer AFTER incumplió el umbral por consulta de p95 BEFORE+0,5 ms en una de
160 queries. Se conserva **NO_GO**. Una medición diagnóstica posterior no reprodujo
el pico, pero no identifica su causa ni reemplaza la puerta fallida. Detalles,
impacto, resultados y límites en el [informe](../audits/ACTOR_EXACT_PERSONAL_PREFIX_COLLISION_V1.md).

## Consecuencias y límites

El oráculo completo acredita 20.788 aliases afectados de 1.066.953 elegibles
(1,948 %). El chequeo runtime encontró 20.787, confirmó ausencia en 1.046.165 y
conservó una comprobación INCONCLUSIVE por presupuesto; ningún desacuerdo en
chequeos completos. La distribución mide impacto, no precisión humana. No se
promueve esta rama ni se reconstruye el índice productivo.

Las fixtures deterministas son sintéticas y bloquean sockets. El dogfood usa
índice local real en solo lectura, fuente doble y persistencia temporal. Los
artefactos enmascaran documentos; los controles de selección del producto
conservan su política explícita de exhibir el documento al usuario.

Validación funcional final, única suite serial del coordinador: **11738 passed,
11 skipped, 14 xfailed in 505.61s (0:08:25)**. No reemplaza el NO_GO de rendimiento.

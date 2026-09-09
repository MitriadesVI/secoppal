# CONTRACT_UNIVERSE_V1_1_F2_2_READY_FOR_FINAL_DELTA_CHECK

La composición se evalúa en dos niveles: su denotación debe caber en la
representación vigente, y el productor existente debe acreditar un filtro
contractual efectivo. Si falta esa prueba, se aclara antes de SQL. Las
ocurrencias repetidas se deduplican por estado normalizado, no por número de
matches. No se cambió el vocabulario de estados ni se creó una unión multiestado.

**SHA probado:** `4a22efc330101c3511c034a53cd02d4790bb192f`.
**Base:** `b2fb22e276e17d812cb6545a453f92c95b83b0a0`.
**SHA probado anterior:** `c671cceec605975d5c09b91bed8f26b8dfc17ac7`.
**Revisión final anterior:** `8dfd5740d60950c760edf622cacfbbbecb6d9d19`.
**Rama:** `fix/contract-universe-v1-1-f2-2`.
El HEAD de entrega añade documentación/evidencia al SHA probado; no modifica su
runtime ni sus tests.

## A — Representable exige un productor efectivo

`ConditionComposition.representable` conserva la pregunta estructural: un
estado concreto dentro de un universo. La nueva comprobación `materializable`
consulta `resolve_estado(surface, 'contratos')`, la misma autoridad usada por
el productor ordinario. Una condición admitida necesita que ese resultado
contenga un filtro `estado_contrato` efectivo que acredite su denotación.
Ni el catálogo de elegibilidad ni el predicado del universo bastan.

Una familia predefinida puede transportar varios valores desde una única
expresión: eso no es una unión nueva de dos condiciones naturales.
El adaptador escalar de condiciones contiguas exige adicionalmente un único
valor ya resuelto; no toma el primero de una familia amplia ni construye filtros
con `_DENOTES`. Los eventos temporales y sus familias actuales conservan sus
productores previos.

El gate sólo añade esta comprobación cuando hay más de una ocurrencia, y no
se apropia de las peticiones exclusivamente de procesos. Los estados excluidos
siguen bajo la autoridad del adaptador explícito de registros.

`celebrados + prorrogados` y `celebrados + aprobados`, en ambos órdenes,
aclaran con cero SQL, cero filas y cero objeto residual. No se añadió ninguna
de esas palabras al resolvedor.

Para los casos admitidos materializables, el adaptador transporta el escalar
que el resolvedor ya produjo y consume el control completo: así una variante
repetida o `suscritos` no queda como objeto. Sólo se consume como un tramo
único lo formado por condiciones, sustantivos de petición y lenguaje de unión;
no se borra contenido intermedio de objeto, fecha o actualidad.

Controles ejecutados en ambos órdenes:

| Composición | Filtro específico | Resultado |
|---|---|---|
| celebrados + suspendidos | Suspendido | ejecuta, cero residuo |
| celebrados + cerrados | Cerrado | ejecuta, cero residuo |
| celebrados + terminados | terminado | ejecuta, cero residuo |
| celebrados + cedidos | cedido | ejecuta, cero residuo |

Los tests aíslan el fragmento específico de filtro de estado, excluyen el
predicado de Contract Universe y comprueban las filas devueltas. No usan la
aparición del literal en la lista general como oráculo.

## B — Repetición semántica

El adaptador normaliza primero con su `_state` existente y deduplica los
literales resultantes antes de decidir `len(states) > 1`. Dos Cancelado
producen `source_records + Cancelado`, exactamente el mismo SQL y las mismas
filas que el singular, con una ejecución. Se mantienen escalar `UniverseRequest.state`
y first-match/single family `detect_estado_family`.

Se comprueba igualdad con el singular para `cancelados`, `borradores`,
`suspendidos` y `terminados`. Las variantes reconocidas por el productor
(`borradores/en borrador`, `cancelados/cancelado`, `suspendidos/suspendida`,
`terminados/terminado`) conservan filtro y filas en los dos órdenes.
Cancelado + Borrador sigue aclarando; los estados distintos no se deduplican.

## Paridad y documentación

El test de paridad compara los valores concretos de `_DENOTES`, quitando sólo
`CELEBRATED_UNIVERSE`, contra `CELEBRATED_STATES ∪ EXCLUDED_STATES` importados
de la política. Compara literales exactos: altas, bajas y cambios de
capitalización/ortografía fallan. Los dos lados no salen de la misma tabla.
El test original del auditor también permanece intacto y pasa.

Se corrigieron las dos afirmaciones obsoletas de `F2.1/REMEDIATION.md`:
compartir universo no acredita representabilidad, dos estados distintos no se
ejecutan sin autoridad multiestado, y universo + estado exige productor efectivo.

## Verificación focal

Los 19 tests finales del auditor se ejecutaron antes de modificar runtime:
`4 failed, 15 passed`. Después: `19 passed`, con el mismo SHA-256 de `8dfd574`.

Resultados finales, con comandos, salida íntegra y códigos en `validation.json`:

```text
ruff: All checks passed!
semgrep: Ran 3 rules on 331 files: 10 findings.
symmetry: 46 passed
completeness: 248 passed
findings anteriores: 8 passed
auditor restantes: 390 passed
remediation: 85 passed
author targets: 1217 passed
frozen literal: 10 failed, 33 passed
shim V1: 43 passed
corpus: Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
auditor final intacto: 19 passed
F2.2 producto + Hypothesis: 34 passed
lint-core: app/core/ limpio
size: 2 passed
diff-check: exit 0, sin salida
```

Se omite únicamente la duración de algunos resúmenes en esta tabla; los logs
conservan la salida literal y el JSON también preserva sus espacios originales.
Los diez nodeids frozen coinciden exactamente con la revisión anterior; no hay
rojos nuevos. `frozen_comparison.json` registra la comparación y el shim V1
sigue acreditando causalidad de Aprobado sin modificar sus oráculos.

Semgrep sale con código 0, pero **no es un scan limpio**: diez hallazgos en la
base, diez aquí, cero nuevos y cero resueltos por regla/ruta/fragmento con
multiplicidad. No se tocó ese backlog.

Instrumentos: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Las propiedades
nuevas ejercitan workflow, SQL, objeto y filas, con generación determinista e
instancias aisladas por ejemplo. Codegraph CLI está presente, pero versión y
`status` abortan por Node 26.8.1; no se acredita índice ni MCP operativo. Se
usó el fallback declarado `rg` y lectura directa de productores/consumidores.

Los controles añadidos durante la implementación también detectaron interferencia
con spans temporales. Se preservó su camino original sin editar QueryRouter ni
Frame. El bloque conjunto de los 53 nuevos/intactos y los tres archivos afectados
por esa interferencia da **376 passed**; la corrida final de 1217 confirma el
cierre. Las salidas anteriores se conservan y no se presentan como validación final.

Corrí estos targets focales, **no la suite**. No hubo SECOP live, LLM ni app.

## Mutaciones

**8/8 muertas por aserciones de producto, cero import errors y cero timeouts.**
Baseline antes: `403 passed in 58.68s`; baseline restaurado:
`403 passed in 55.87s`. Los dos archivos runtime volvieron byte por byte al
candidato. Se inspeccionaron también los logs completos de las aserciones.

| Mutación | Salida literal |
|---|---|
| M1_tail_only_detection | 101 failed, 249 passed in 43.74s |
| M2_first_match_wins | 157 failed, 193 passed in 44.43s |
| M3_composition_ignored_only_two_excluded_counted | 135 failed, 215 passed in 43.20s |
| M6_two_admitted_states_are_representable | 14 failed, 336 passed in 41.95s |
| M4_representable_ignores_universe_frontier | 45 failed, 305 passed in 41.89s |
| M5_reader_drops_clause_ownership | 6 failed, 344 passed in 41.84s |
| M7_occurrences_are_not_deduplicated | 7 failed, 46 passed in 16.97s |
| M8_materializability_is_not_required | 12 failed, 41 passed in 15.63s |

M1–M6 conservan los 350 killers originales. M7/M8 usan 53 tests (19 intactos
más 34 de F2.2); no se usaron skip, xfail ni deselect. El baseline de 403 cubre
ambos grupos. `mutations.json` y `mutation_logs` conservan resultados y trazas.
La corrida interrumpida de una iteración previa no cuenta como evidencia final.

## Preservación y alcance

Runtime modificado exclusivamente en `app/core/contract_conditions.py` y
`app/core/contract_universe_intent.py`. No cambian los bytes de
`estado_families.py`, `contract_universe.py`, QueryRouter ni Frame. Las pruebas
históricas se conservan por hash, incluidos los 19 tests importados del auditor.
El delta posterior al SHA probado es sólo documental/de evidencia.

F1/F3/F4/F5/Aprobado conservan sus controles adjudicados; no se alteraron sus
oráculos ni se reabrió la política de elegibilidad. No hubo flags, merge, push,
despliegue ni escrituras en persistencia del usuario.

## Deuda separada y mejora recomendada

`CONTRACT_STATE_LEXICON_GAPS_001`: `contratos prorrogados` y
`contratos aprobados` aislados siguen siendo deuda léxica preexistente. F2.2
no los arregla; protege la composición sin inventar vocabulario.

`SYSTEM_IMPROVEMENTS.md` propone que el productor entregue juntos filtro y
spans consumidos como una única evidencia. Se registra para el coordinador;
no se introduce ese refactor en este carril.

Pendiente externo: final delta check del coordinador y, sólo si se autoriza,
el gate posterior. La entrega no autoriza integración ni activación.

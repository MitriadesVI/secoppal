# Actor dogfood batch V2: diagnóstico y ajustes del plan

Fecha: 2026-09-06. Base examinada: `76b78c35e8505a092bcd5b3e82d88c8ab15992d2`.
Rama: `fix/actor-cantillo-scope-pending-v1`.
Alcance: investigación local y revisión del plan aportado; ningún cambio de runtime,
índice productivo, reglas de autoridad o tests. Sin merge, push ni arranque de app.

## Resultado

El índice acredita la colisión estructural propuesta, con DOS extensiones de
identidades diferentes. El resolver actual cumple su contrato vigente: cada
alias exacto único resuelve. Convertir esa resolución en aclaración sería una
decisión nueva de producto/autoridad, no reparar una búsqueda exacta ausente.

La omisión visual del scope sí se reproduce. Los fallos de alcance del nombre
completo y de reemplazo del pendiente no se reproducen en el workflow local de
esta base. No se justifica modificar esas puertas con la evidencia actual.

| Frente | Evidencia actual | Tratamiento propuesto |
|---|---|---|
| A: nombre corto | Exacto único CC terminada en 3153; ARCON y SOLANO corresponden a otras CC | Evaluar nueva política de aclaración, con ADR y pruebas específicas |
| B: nombre completo ARCON | `resolve_exact` y `resolve` devuelven `RESOLVED / exact_unique_alias`; tras reset el workflow llega a `actor_profile` | No modificar runtime; conservar regresión y buscar procedencia del dogfood original |
| C: scope | Buyer y fechas llegan a las ocho consultas de perfil, pero no se explican en la respuesta | Corregir representación/render del scope aplicado |
| D: Franklin → Fundacaribe | Pending `CANDIDATES` seguido de `set_actor`, `RESOLVED` y `actor_profile`, sin pending anterior | Ya funciona en la base local; regresión, sin nuevo fix |

Para D existe además `639c8d999696e01b2b976144acb701a403506608`,
`fix(actor): let strong new actor search reach resolution after context`,
anterior a la base. `can_resolve_new_actor` protege la admisión antes del guard.

## Evidencia del índice

Artefacto examinado: `/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite`.
SQLite abierto con `mode=ro&immutable=1` y `PRAGMA query_only=ON`.
Metadatos: N2, esquema 1, build `2026-08-20T18:10:58+00:00`, fuente
`jbjy-vk9h`, máximo de fecha de fuente `2026-08-18`, 1.217.203 aliases y
1.214.351 identidades. El manifest guarda SHA-256 y comprobación de stat.
No se reconstruyó el índice ni se consultó SECOP.

| Alias N2 | CC enmascarada | Ocurrencias del alias |
|---|---|---:|
| jose david cantillo | ******3153 | 3 |
| jose david cantillo arcon | ******2711 | 8 |
| jose david cantillo solano | ******5396 | 3 |

Las ocurrencias son evidencia del índice, no conteos contractuales actuales.
Las cuatro sondas solicitadas (dos nombres por dos entradas) resolvieron por
exactitud. No hay `FULL_FUZZY_MATCHED_ALIAS`: FTS no intervino en esas llamadas.
El resultado `RESOLVED` no transporta `matched_alias`/ocurrencias; se acreditan
con la lectura directa de `actor_aliases`, no se inventan campos del resolver.

El plan de una consulta `LIKE 'jose david cantillo %'` fue:

```text
SCAN actor_aliases USING COVERING INDEX actor_aliases_exact_idx
```

El rango `normalized_alias >= 'jose david cantillo '` y
`normalized_alias < 'jose david cantillo!'`, con orden por alias/identidad y
`LIMIT 101`, produjo:

```text
SEARCH actor_aliases USING INDEX sqlite_autoindex_actor_aliases_1 (normalized_alias>? AND normalized_alias<?)
```

Es una sonda de viabilidad, no la implementación final del challenger. En una
muestra determinista de 20 nombres exactos CC de tres o más tokens, más Cantillo
corto/completo, 30 repeticiones por nombre y caché caliente, el máximo p95 por
nombre de la consulta adicional fue 0,0055 ms. Para Cantillo corto fue 0,002917 ms.
La consulta adicional se midió en conexión abierta; no incluye apertura ni
validación. La muestra no estima prevalencia, no cubre el caso de solo aliases
de una misma identidad y no constituye un BEFORE/AFTER de runtime.

## Reproducción del workflow y límites

Se ejecutaron el parser, resolución real sobre el índice local, grafo Burr,
selección y turnos persistidos en un directorio temporal. Un doble de fuente
respondió vacío y registró las consultas. Las conexiones de socket estaban
bloqueadas; LLM desactivado. No se utilizaron conversaciones reales del usuario.

Esto acredita routing, preservación de parámetros, consultas emitidas y texto.
NO acredita los conteos reales ni identifica cuál era la CC del dogfood original.
La selección `2` fue una selección explícita de prueba, no una adjudicación por
actividad contractual.

- ARCON: reset → frase exacta del plan → `actor_profile`, autoridad exacta.
- Franklin: reset → candidatos → Fundacaribe → `set_actor` → `actor_profile`.
- José Cantillo + Barranquilla: tres candidatos; selección explícita preserva
  buyer e identidad. Las ocho consultas incluyen el buyer resuelto; la respuesta
  muestra `- 0 contratos` y no contiene Barranquilla.
- Fundacaribe → `ahora muestrame 2026`: conserva actor y aplica
  `2026-01-01`/`2026-12-31` en las ocho consultas. El filtro temporal no se declara.
  El año que aparece en la fecha de consulta NO cuenta como scope visible.

## Cambios recomendados al plan

1. **Congelar también la procedencia del proceso que produjo el dogfood.**
   Además del HEAD del checkout, registrar revisión cargada por el servidor,
   flag efectivo, ruta/hash del índice, N2/build, canal, fecha y conversación
   desde reset. B y D pasan localmente; una revisión anterior es una hipótesis,
   no una causa demostrada. No reiniciar la app antes de recoger esa evidencia.

2. **Definir exhaustividad antes de TOP_K.** El cap visible debe limitar
   identidades distintas después de agrupar aliases. Limitar filas antes de
   excluir/agrupar la identidad principal puede ocultar una CC diferente tras
   muchos aliases de la misma persona. Separar detección de existencia,
   enumeración y presentación; detectar truncamiento. Agotar un presupuesto
   sin establecer ausencia no puede significar `RESOLVED`. Probar el challenger
   más allá del cap y permutaciones de inserción/ocurrencias.

3. **Completar el gate de coste y la política personal.** Exigir `SEARCH` por
   rango; un `SCAN ... USING INDEX` también recorre el índice. `LIMIT` no limita
   necesariamente trabajo anterior de agrupación/ordenamiento. Medir la SQL
   final, incluidos deduplicación, exclusión y orden, no solo el rango preliminar.
   Especificar que el challenger también sea CC y qué ocurre con prefijos muy
   cortos, tokens numéricos o aliases observados que no parecen nombres personales.
   CC es tipo documental; no certifica que el alias sea un nombre personal limpio.
   El prefijo acredita necesidad de aclaración, no que dos documentos sean la
   misma persona. Mantener selección/documento explícitos fuera de un bucle de
   nueva aclaración y congelar la normalización N2.

4. **Congelar el baseline temático REAL.** Mantenimiento, alimentación, adulto
   mayor, software, vigilancia y aseo conservan ruta temática en las sondas.
   `transporte` es hoy un alias exacto CC del índice productivo: la consulta
   produce `actor_or_topic_ambiguous`, no búsqueda temática directa. Un test que
   exija ruta temática incondicional para ese caso cambiaría la política actual.
   Añadir fixtures con colisión exacta tema/alias, además de aliases parciales.
   No atribuir esta colisión al futuro challenger.

5. **Derivar el scope de predicados aplicados y comprobar equivalencia de SQL.**
   Buyer/fechas/territorio/modalidad son insuficientes si la consulta también
   filtra valor, estado, objeto o exclusiones: `_build_where` ya los admite.
   Inventariar cuáles llegan efectivamente al perfil y representarlos sin
   cambiar su semántica ni abrir los frentes de Money u otros fuera de alcance.
   Distinguir buyer exacto, grupo o alcance aproximado; un raw label no es
   autoridad acreditada. Preservar EMPTY frente a ERROR/desconocido.
   Para probar `0 SourceCalls adicionales`, comparar consultas y parámetros
   por aspecto, además del número de llamadas. Igual cantidad no prueba igual
   universo. No añadir una consulta global para adornar un cero filtrado.

6. **Separar decisiones y entregas.** C tiene defecto visible reproducido;
   puede avanzar independientemente. A requiere una nueva decisión de autoridad
   y su ADR. B/D quedan como regresiones salvo una nueva reproducción fallida.
   No crear commits de fixes vacíos. Mantener la suite completa como validación
   posterior del orquestador, una sola vez y en serie; “background” por sí solo
   no coordina recursos.

## Instrumentación y puertas

Codegraph MCP cargado: status/context/explore/impact para resolver, admisión,
selección y perfil. CLI `codegraph 0.9.6`, `codegraph status`:
`✓ Index is up to date`, 813 archivos. Lectura directa complementaria para ramas
truncadas y predicados. `git log`/`git show` acreditan la corrección de pending.
Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10.

Salida literal Ruff (`ruff check app/ tests/ scripts/`):

```text
All checks passed!
```

Salida literal de resumen Semgrep
(`semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off`):

```text
Ran 3 rules on 264 files: 10 findings.
```

Baseline: 10 hallazgos, todos `except-exception-que-silencia`; delta cero porque
no se modificó código. El comando retorna 0, pero eso NO vuelve verde la puerta
global. stdout/stderr íntegros están en `semgrep_gate.json`.

Corrí `test_actor_name_resolver.py`, `test_actor_resolution_orchestration.py`,
`test_actor_conversation.py` y `test_actor_profile_render.py`, no la suite.
Salida literal:

```text
........................................................................ [ 33%]
........................................................................ [ 67%]
.....................................................................    [100%]
213 passed in 29.82s
```

Hypothesis se verificó como disponible; estos cuatro archivos no contienen
propiedades Hypothesis. No se ejecutó una campaña de propiedades ni se añadieron
pruebas ornamentales a una revisión documental. No se ejecutaron la suite
completa, corpora completos, campaña adversarial nueva ni dogfood de red.

Salida literal de `make lint-core` (no sustituye Ruff):

```text
app/core/ limpio
```

## Fuera del cambio y pendientes

Los diez hallazgos Semgrep preexistentes se conservaron. Se documentó la colisión
temática con TRANSPORTE; no se cambió esa autoridad ni el índice. No se tocaron
otros carriles, `CARRILES.md`, bitácora compartida ni ADR.

Quedan pendientes la procedencia del dogfood original, el oráculo real de la CC
y sus conteos filtrados/globales, las nuevas regresiones propuestas, el diseño y
decisión del challenger, sus gates completos y la implementación de scope. Esta
entrega es el diagnóstico solicitado para ajustar el plan, no un cierre del batch
de implementación ni un permiso de promoción.

## Artefactos

En `artifacts/audits/actor_cantillo_batch_v1/`:

- `manifest.json`: revisión, hash/metadata del índice, procedencia y privacidad.
- `resolver_probe.json`, `alias_probe.json`: resultados y planes SQLite.
- `routing_probe.json`: parser, bare probe, resolución y plan para 13 consultas.
- `workflow_probe.json`: resultados Burr y SQL emitida con fuente simulada vacía.
- `scope_checks.json`: comprobación de filtros presentes en 8/8 consultas.
- `prefix_feasibility.json`: muestra, SQL, iteraciones y p50/p95 preliminares.
- `ruff_gate.json`, `semgrep_gate.json`, `pytest_gate.json`: salidas de puertas.

Los documentos personales se enmascaran en diagnósticos. Los probes de aliases
preservan igualdad mediante digest tipado. La SQL redacted del workflow sirve
para inspección; no es un replay con documentos reales. No se publican registros
de límites de páginas que contengan documentos ajenos al caso.

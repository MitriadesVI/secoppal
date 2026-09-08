# Evidencia R3d: fuente, tema, modalidad, SQL y alcance visible

## Proveniencia

```text
BASE_A2=a952805de050ec28d571b7a637e9240f88c71a45
BASE_A2_PARENT_TESTED_CODE=2ec5033edb5bea1a1394df427a9cdf966155da25
TESTED_CODE_HEAD=e4b85db855d50f7e5671a515bfe1b26c882c2867
BRANCH=fix/entity-profile-r3d-postposed-source
WORKTREE=/Users/rodrigoortiz/Documents/secoppal-entity-profile-r3d-postposed-source
```

`2ec5033edb5bea1a1394df427a9cdf966155da25` es padre directo de `BASE_A2`;
`BASE_A2` agrega la evidencia
documental A2. El worktree de referencia A2 no fue modificado.

## Matriz antes/despues

| Entrada | BASE_A2 | R3d |
|---|---|---|
| `contratacion directa de alimentacion en procesos` | Search PROCESSES por fallback; tema `alimentacion + procesos`; sin claim explicito | una ocurrencia POSTPOSED; PROCESSES explicito; tema `alimentacion` |
| `alimentacion en contratos` | Search CONTRACTS por fallback; `contratos` queda en tema | una ocurrencia POSTPOSED; CONTRACTS explicito; tema `alimentacion` |
| `contratos de gestion de procesos y contratos en 2026` | Search CONTRACTS y tema correctos, pero dos claims SOURCE | un claim HEAD CONTRACTS; tema intacto; ano ortogonal |
| `contratos excepto procesos industriales` | BOTH positivo falso | CONTRACTS explicito; exclusion no crea fuente positiva |
| `procesos publicados en julio y contratos firmados en agosto` | BOTH | BOTH, sin cambio |

## Semilla de punta a punta

Entrada:

```text
contratacion directa de alimentacion en procesos de la ALCALDIA DE BARANOA en 2026
```

Observacion offline `QueryRouter -> claims -> Frame -> plan -> SQL -> scope`:

```text
ready=True
sources={PROCESSES}
dataset=procesos
dataset_scope=<absent>
topic=TERM(alimentacion)
modality={contratacion_directa, contratacion_directa_con_ofertas}
period=2026-01-01..2026-12-31
source_claim_surface=procesos
source_claim_role=POSTPOSED
source_provenance=EXPLICIT
jobs=solo procesos
```

Invariantes SQL observadas:

```text
LIKE '%alimentacion%' = presente
LIKE '%procesos%' = ausente
modalidad_de_contratacion IN = presente
fecha_de_publicacion_del >= '2026-01-01' = presente
fecha_de_publicacion_del <= '2026-12-31' = presente
```

Alcance visible:

```text
Fuentes: procesos.
2026, según fecha de publicación.
Tema: alimentacion.
Modalidad: Contratación directa, Contratación Directa (con ofertas).
```

## Negativos y coordinacion ortogonal

Los cinco negativos exigidos mantienen las ocurrencias interiores disponibles
para tema. Una propiedad Hypothesis los prueba con y sin `en 2026`, de modo que
el periodo no cambia su clasificacion. La propiedad positiva genera cortesia,
articulos, tildes, espacios, mayusculas y Title Case. Tambien se cubren:

- `excepto`, `sin` y `excluyendo`, sin fuente positiva secundaria;
- comprador acreditado cuyo nombre contiene `contratos`;
- ano y comprador aplicados a
  `contratos de gestion de procesos y contratos`; y
- el contraste positivo con eventos distintos por fuente.

La frase tematica completa llega a Frame como:

```text
source=CONTRACTS
topic=ALL(TERM(gestion),TERM(procesos),TERM(contratos))
scope=Fuentes: contratos. / Tema: gestion y procesos y contratos.
```

## Paridad Search A2/R3d

La comparacion ejecuto procesos Python aislados por worktree, con `cwd`,
`PYTHONPATH` y fecha fija independientes:

```text
PARITY_UNAFFECTED=15/15
UNEXPECTED_DIVERGENCES=[]
TARGETED_DIVERGENCES=["contratacion directa de alimentacion en procesos", "alimentacion en contratos", "contratos excepto procesos industriales"]
TARGETED_UNCHANGED=["contratos de gestion de procesos y contratos en 2026"]
```

Las tres divergencias son las correcciones autorizadas. El control ortogonal
conserva exactamente los params de QueryRouter y cambia solo la propiedad del
claim. El corpus sin cambios cubre eventos temporales, dinero, exclusiones no
relacionadas, frase de catalogo, busqueda sin fuente, software, administracion
de contratos, seleccion `procesos o contratos` y los cinco negativos.

## Semgrep: baseline y delta

La comparacion normalizo `(regla, archivo, linea, fragmento)` en BASE_A2 y R3d:

```text
SEMGREP_IDENTITY_DELTA_EXIT=0
FINDINGS=10
semgrep.except-exception-que-silencia app/core/formatter.py:171
semgrep.except-exception-que-silencia app/core/formatter.py:699
semgrep.except-exception-que-silencia app/core/response_policy.py:508
semgrep.except-exception-que-silencia app/core/response_policy.py:577
semgrep.except-exception-que-silencia app/core/workflow/actions/execution.py:365
semgrep.except-exception-que-silencia app/core/workflow/handlers/direct_lookup.py:366
semgrep.except-exception-que-silencia app/core/workflow/handlers/normal_flow.py:171
semgrep.except-exception-que-silencia app/core/workflow/handlers/pagination.py:177
semgrep.except-exception-que-silencia app/core/workflow/handlers/radar.py:109
semgrep.except-exception-que-silencia app/core/workflow/handlers/refinement.py:93
```

Baseline heredado: 10. Hallazgos nuevos: 0. Archivos propios en hallazgos: 0.

## Codegraph e impacto

Preflight inicial: CLI disponible, worktree no inicializado. Se inicializo y
sincronizo con Node 22. El estado final es:

```text
Files: 895
Nodes: 21,162
Edges: 51,248
python: 873
yaml: 22
Index is up to date
```

Antes del cambio, `callers source_head_occurrences` encontro siete consumidores
y `impact` devolvio un universo demasiado amplio de 750 simbolos. La primera
sincronizacion posterior encontro 10 callers y 19 simbolos de impacto; `affected`
no encontro tests, una limitacion ya observada del indice.

Tras el ultimo refactor local, la busqueda de simbolo final si indexa los imports
de `query_router.py`, `source_request.py`, `claim_adapters.py` y el test nuevo,
pero `callers` solo reporta los dos wrappers locales. Como esa salida contradice
los imports que el propio indice muestra, se declara `CODEGRAPH_CALLERS_PARTIAL`
y se usa el fallback requerido `rg` mas revision manual.

El fallback encontro consumidores runtime exactamente en:

```text
app/core/query_router.py:274
app/core/query_router.py:1072
app/core/entity_profile/source_request.py:119
app/core/entity_profile/claim_adapters.py:238
```

Los wrappers de compatibilidad estan en `source_occurrence.py`; los usos
restantes son tests. Todos los consumidores runtime enumerados quedaron
cubiertos por el target A2 de 1.709 pruebas y por los tests end-to-end R3d.

La politica escalar historica de `procesos de administracion de contratos`
permanece como deuda A2. No se promueve a autoridad del Frame y no se modifica
en este micro-lote.

## Frame productivo y limites

La busqueda fuera de `app/core/entity_profile/` de `EntityProfileFrame`,
`ProfilePlan`, `compile_profile`, `collect_claims` y `migrate_legacy_context` no
produjo salida. El diff tampoco toca workflow, UI, configuracion u orquestador.

```text
PRODUCTIVE_FRAME_ACTIVATION=OFF
NEW_PRODUCTIVE_FRAME_CONSUMERS=NONE
NETWORK_CALLS=0
GLOBAL_PYTEST=NOT_RUN
MUTATION_CAMPAIGN=NOT_RUN
MERGE=NOT_DONE
PUSH=NOT_DONE
```

## Veredicto

R3d cierra la familia pendiente: la semilla y su rollback pasan, el adjunto A2
y R1/R2/R4 permanecen verdes dentro del target completo, no hay divergencias
inesperadas de Search y las puertas del carril no tienen delta. Resultado:
`CORE_GO_FOR_V1_2_B` dentro del alcance autorizado de esta rama.

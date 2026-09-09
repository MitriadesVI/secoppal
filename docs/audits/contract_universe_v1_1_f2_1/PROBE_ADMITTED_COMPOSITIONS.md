# PROBE_FOUND_PARTIAL_ADMITTED_COMPOSITION

La infraestructura **no** representa exactamente dos condiciones admitidas
distintas. `ConditionComposition.representable` era incorrecto al considerar
representables todas las combinaciones `admitted + admitted`.

## Evidencia del probe

Seis consultas, por workflow completo, antes de tocar nada.

| Consulta | `needs_clarification` | `estado_contrato` | objeto residual | SQL | estados devueltos | total |
|---|---|---|---|---|---|---|
| `contratos suspendidos y aprobados …` | **False** | `['Suspendido']` | `['aprobados']` | 2 | — | **0** |
| `contratos aprobados y suspendidos …` | **False** | `['Suspendido']` | `['aprobados']` | 2 | — | **0** |
| `contratos terminados y cedidos …` | **False** | `['terminado']` | `['cedidos']` | 2 | — | **0** |
| `contratos cedidos y terminados …` | **False** | `['cedido']` | — | 1 | `['cedido']` | **3** |
| `contratos cerrados y prorrogados …` | **False** | `['Cerrado']` | `['prorrogados']` | 2 | — | **0** |
| `contratos prorrogados y cerrados …` | **False** | `['Cerrado']` | `['prorrogados']` | 2 | — | **0** |

Tres lecturas de esa tabla:

1. **Ejecución parcial.** En los seis casos el filtro lleva un solo estado. La
   segunda condición nunca entra en `estado_contrato`.
2. **La condición perdida se degrada a tema.** En cinco de los seis, la palabra
   sobrante acaba en `objeto`, de modo que la consulta filtra
   `objeto_del_contrato LIKE '%aprobados%'` y devuelve **cero**. El usuario pide
   dos estados y recibe «no encontré nada»: una respuesta falsa, peor que perder
   la condición en silencio.
3. **Asimetría de orden, otra vez.** `terminados y cedidos` devuelve 0 filas y
   `cedidos y terminados` devuelve 3. Los dos órdenes dan respuestas distintas.

### El falso verde que había que evitar

Una comprobación ingenua de «¿están ambos estados en el SoQL?» da **true** en
los seis casos. No prueba nada: el predicado del universo celebrado ya contiene

```sql
upper(unaccent(trim(estado_contrato))) IN ('EN EJECUCION', 'CERRADO',
  'MODIFICADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO')
```

Los siete estados posteriores aparecen ahí **siempre**, por el universo y no por
lo que el usuario pidió. Pertenecer al mismo universo no es estar en el filtro.
La propiedad que se añadió aísla el fragmento de filtro `estado_contrato` y
descarta el del universo antes de comprobar nada.

## ¿Existe una representación multiestado autoritativa real?

**Transporte: sí. Autoridad: no.**

El builder sabe emitir la unión si alguien se la entrega:

```text
['Suspendido']              -> estado_contrato = 'Suspendido'
['Suspendido', 'Aprobado']  -> estado_contrato IN ('Suspendido', 'Aprobado')
['terminado', 'cedido']     -> estado_contrato IN ('terminado', 'cedido')
```

Pero ningún productor construye esa unión a partir de dos condiciones sueltas
del texto, y los dos que podrían hacerlo declaran lo contrario:

- `estado_families.detect_estado_family` — *«Returns the first match found.
  Multiple matches are not allowed — a query can only belong to one estado
  family.»* Es first-match-wins por diseño documentado.
- `UniverseRequest.state` es `str | None`, escalar, y `apply_request` escribe
  `clean['estado_contrato'] = [intent.state]`: una lista de un elemento.

Fabricar esa autoridad sería la gramática multiestado nueva que el encargo
prohíbe. Por tanto se adopta la salida conservadora.

## Corrección aplicada

`representable` deja de razonar sobre superficies y pasa a razonar sobre lo que
cada condición **denota**: un literal de estado, o el universo celebrado.

- Dos estados denotados distintos —admitidos, excluidos o mezclados— no caben en
  el filtro singular que producen los adaptadores vigentes: se aclara.
- El universo celebrado no cuenta como estado, porque ya viaja en el predicado:
  `contratos celebrados y suspendidos` conserva ambas condiciones y sigue
  ejecutando.
- Lo que no puede convivir con el universo celebrado es un estado excluido: eso
  cruza la frontera, y se aclara (comportamiento ya cerrado en F2.1).

La tabla de denotación se comprueba contra el catálogo de política: sus doce
literales son exactamente `CELEBRATED_STATES ∪ EXCLUDED_STATES`, de modo que no
pueden divergir en silencio.

### Después de la corrección

Los seis casos, en ambos órdenes: `needs_clarification=True`, **cero SQL**, cero
filas, `estado_contrato` vacío y **sin objeto residual** — la composición
completa se consume como control.

`contratos celebrados y suspendidos` y su inverso siguen ejecutando, con el
predicado de universo en el WHERE y `estado_contrato = 'Suspendido'` en el
filtro: **ambas condiciones conservadas**.

## La propiedad

[`tests/test_contract_universe_f2_1_completeness.py`](../../../tests/test_contract_universe_f2_1_completeness.py),
**248 casos verdes**, sobre las 240 permutaciones de dieciséis superficies:

- **184 pares no representables** → aclaran con cero SQL. Ninguno ejecuta.
- **56 pares representables** → o aclaran con cero SQL, o el filtro ejecutado
  conserva **cada** condición. El literal debe estar en un fragmento
  `estado_contrato` propio; el predicado de universo se descarta antes de mirar.
- Un control comprueba que la referencia no es hueca (al menos ocho condiciones
  se conservan cuando se piden solas) y otro deja constancia de que el predicado
  de universo contiene los cinco literales que harían falso el verde ingenuo.

La referencia de cada condición es esa misma condición ejecutada sola. Así la
propiedad exige que **componer no degrade lo que el caso simple conservaba**,
sin culpar a esta remediación de un hueco que no le pertenece.

## Hueco preexistente, fuera de alcance

`contratos prorrogados` y `contratos aprobados`, **sin composición alguna**,
ya pierden su filtro y mandan la palabra al objeto:

```text
{"q": "contratos prorrogados", "estado": null, "family": null,
 "objeto": ["prorrogados"], "filter": "", "total": 0}
{"q": "contratos aprobados",   "estado": null, "family": null,
 "objeto": ["aprobados"],   "filter": "", "total": 0}
```

Se reprodujo idéntico en la base `2e564d0`, y su causa es léxica:
`resolve_estado('prorrogados', 'contratos')` y `resolve_estado('aprobados',
'contratos')` devuelven `None`. Es un hueco de `estado_families` anterior a esta
microremediación, no una ejecución parcial de una composición. Queda **fuera del
alcance** de F2.1, que tiene prohibido tocar otras superficies; se deja
registrado para el coordinador.

## Mutación

`M6_two_admitted_states_are_representable` restaura el `representable` anterior
—el que consideraba representable cualquier `admitted + admitted`— y **mata 34
casos** por aserciones de producto, sin un solo error de importación.

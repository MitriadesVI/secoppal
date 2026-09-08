# Remediacion R3d: fuente pospuesta y propiedad de ocurrencias

## Tesis

El fallo no estaba en la seleccion escalar de Search ni en la modalidad. La
frase `contratacion directa de alimentacion en procesos` no acreditaba
`procesos` como fuente porque la unica autoridad compartida reconocia fuentes
en cabeza de clausula. Como consecuencia, el router elegia procesos por su
fallback escalar, pero dejaba la misma ocurrencia disponible como tema.

R3d extiende esa autoridad unica con una gramatica pospuesta estrecha. Una
ocurrencia `en [articulo] fuente` se acredita solamente si:

- hay texto material anterior;
- no existe ya una fuente de cabeza que sea dueña de la consulta;
- existe exactamente una candidata pospuesta; y
- despues de la fuente solo quedan palabras funcionales o un periodo temporal
  ya resuelto.

La ocurrencia acreditada se consume una vez del texto tematico y se reutiliza
para Search, claims y Frame. No se cambio `_select_dataset`, no se agrego un
segundo parser y no se activo Frame en runtime productivo.

## Causa reproducida

En `BASE_A2=a952805de050ec28d571b7a637e9240f88c71a45`, el caso pendiente terminaba
con:

```text
E AssertionError: assert ['alimentacion', 'procesos'] == ['alimentacion']
1 failed in 0.98s
```

La inspeccion antes del cambio mostro:

| Capa | Antes |
|---|---|
| Search | `dataset=procesos`, sin ocurrencia explicita acreditada |
| Tema | `alimentacion`, `procesos` |
| Modalidad | contratacion directa correctamente resuelta |
| Claim de fuente | ausente para la ocurrencia pospuesta |
| Frame/SQL | el tema generaba `LIKE '%procesos%'` |

El test adjunto A2 seguia verde antes de editar: `13 passed in 0.25s`.

## Contrato de gramatica

| Familia | Clasificacion | Resultado |
|---|---|---|
| `contratacion directa de alimentacion en procesos` | admitida | PROCESSES explicito; tema `alimentacion` |
| `alimentacion en contratos` | admitida | CONTRACTS explicito; tema `alimentacion` |
| articulo, cortesia, mayusculas, tildes y espacios | admitida | misma propiedad de fuente y consumo |
| periodo o comprador despues de la fuente acreditada | ortogonal | no devuelve la fuente al tema |
| `capacitacion en procesos industriales` | tematica | `procesos industriales` queda en tema |
| `apoyo en procesos judiciales` | tematica | `procesos judiciales` queda en tema |
| `software para gestion de procesos` | tematica | la ocurrencia interior queda en tema |
| `asesoria en contratos de arrendamiento` | tematica | la ocurrencia interior queda en tema |
| `gestion de procesos y contratos` | tematica | ambas ocurrencias interiores quedan en tema |
| nombre acreditado de comprador con `contratos` | identidad | no crea otra fuente |
| segunda fuente con evento propio | admitida | conserva contraste BOTH existente |
| ocurrencia gobernada por `excepto`, `sin` o `excluyendo` | exclusion | no crea fuente positiva ni BOTH |
| mas de una candidata pospuesta sin cabeza | ambigua | no se acredita por esta gramatica |
| cola material despues de la candidata | ambigua/tematica | no se acredita por esta gramatica |

El control preventivo `contratos de gestion de procesos y contratos en 2026`
confirmo un riesgo adicional: el ano, por si solo, podia convertir la segunda
ocurrencia tematica en una nueva fuente. Ahora solamente un evento o estado
propio reabre una clausula de la misma fuente; un periodo desnudo permanece
ortogonal. El control historico `procesos publicados en julio y contratos
firmados en agosto` conserva las dos fuentes.

## Diseno aplicado

`source_request_occurrences()` es la autoridad unica. Sus consumidores son:

- `QueryRouter.parse`, para `dataset_scope` explicito;
- el enmascarado tematico del router;
- `resolve_requested_sources`, para fuente y proveniencia;
- `collect_claims`, para el claim SOURCE y su span; y
- los nombres de compatibilidad `source_head_occurrences` y
  `mask_source_heads`, que delegan sin bifurcar semantica.

Cada `SourceOccurrence` expone `HEAD` o `POSTPOSED`. El rol solo describe la
estructura; ambos roles tienen la misma autoridad una vez acreditados.

La deteccion de exclusiones se hace antes de acreditar una cabeza. Esto elimina
el BOTH falso de `contratos excepto procesos industriales` sin ampliar el
alcance a la semantica general de negacion.

## Despues

Para la semilla completa con comprador y ano:

```text
Fuentes: procesos.
2026, según fecha de publicación.
Tema: alimentacion.
Modalidad: Contratación directa, Contratación Directa (con ofertas).
```

El plan produce solo jobs de procesos. El SQL contiene `LIKE '%alimentacion%'`,
el filtro de modalidades y los limites de fecha de publicacion; no contiene
`LIKE '%procesos%'`.

Para `contratos de gestion de procesos y contratos ... en 2026`, Frame conserva
CONTRACTS como unica fuente y el tema
`ALL(TERM(gestion),TERM(procesos),TERM(contratos))`; los tres terminos llegan al
SQL y al alcance visible.

## Limites deliberados

- No se modifico la politica escalar de Search.
- No se implemento ninguna parte de V1.2-B.
- No se tocaron workflow, UI, configuracion, transporte ni red.
- No se construyo `Settings()` fuera de pytest.
- No se corrio la suite global.
- No se hizo merge ni push.

El HEAD de codigo probado es
`57ea478031ed16f9b1df4c9f92754edd2038de9b`. El commit posterior a ese SHA
contiene exclusivamente esta evidencia documental; el `FINAL_HEAD` se registra
en la entrega, porque un commit no puede contener de forma no autorreferencial
su propio SHA.

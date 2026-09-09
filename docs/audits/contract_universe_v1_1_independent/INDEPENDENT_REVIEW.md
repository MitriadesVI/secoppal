# REMEDIATION_REQUIRED

Cuatro de los cinco hallazgos quedaron cerrados y la adjudicación de Aprobado
quedó acreditada, incluida la comprobación del dialecto contra la fuente real.
**F2 no está cerrado**: una composición de dos estados sigue ejecutando media
petición sin aclarar cuando el estado admitido va primero, y ese caso concreto
*sí aclaraba en V1* — la remediación de F2 introdujo una regresión en la misma
superficie que remediaba. Los diez rojos congelados sí son, exclusivamente,
expectativas obsoletas y sus ports pasan. No se concede
`CONTRACT_UNIVERSE_V1_1_GO_FOR_COORDINATOR_GATE`.

SHA auditado: `8fc5643651d6a603d3bc3849369d9773ab55ea11`.
HEAD final de la remediación: `2e564d06a4f18a8ef4d3b5a1fa462000e21a00da`.
Candidato V1: `067b39770592640b5aef3898cbc99d8f82c59e0f`.
Primera auditoría: `2fb9c6e2974803a8ae46b3fcc27257e200e4609d`.
BASE main: `c52d7eda355f44278164d4cb9803a199eec83486`.
Rama de entrega: `audit/contract-universe-v1-1-independent`. El veredicto llega
en `fa9a6c81bfc1d3bbf147bfe075a952998f52ef82`; el HEAD final de la entrega se
comunica completo en la respuesta de cierre y se obtiene con `git rev-parse HEAD`
en este worktree. Añade sólo documentación, evidencia y pruebas propias, y
permanece distinto del SHA auditado.

## Incumplimiento reproducido

### P1 — F2: el primer estado sigue ganando, y ahora en un caso que V1 sí aclaraba

`registros celebrados y cancelados de la Alcaldía de Cartagena` devuelve
`needs_clarification=False`, estampa `contract_universe='source_records'`,
fija `estado_contrato=['Cancelado']` y **ejecuta una consulta SQL**. La mitad
«celebrados» desaparece sin decírselo al usuario; además `registros` queda como
tema, así que la consulta que sí se ejecuta tampoco es la que se pidió.

El orden inverso, `registros cancelados y celebrados …`, aclara con cero SQL.
La misma frase con `contratos` en vez de `registros` también aclara. La
asimetría de orden es exactamente lo que el contrato prohíbe.

Reproducido en seis formas: `celebrados y cancelados`, `celebrados y
borradores`, `firmados y cancelados`, `suscritos y cancelados`, `suspendidos y
cancelados`, `aprobados y cancelados`.

Autoridad causal: `app/core/contract_universe_intent.py:69-74`. El marcador de
celebración sólo se busca tras una *ocurrencia de fuente* acreditada y en la
**cola** posterior al match; el prefijo de la cláusula no se examina nunca, y
`registros` no es palabra de fuente para `source_request_occurrences`.

Contraste con V1, que calculaba el conflicto sobre todo el texto:

```python
# 067b3977, contract_universe_intent.py:36
conflict = bool(state and re.search(r'\b(?:firmados?|celebrados?|suscritos?)\b', normalized.text))
```

`registros celebrados y cancelados` → `conflict=True` en V1, `conflict=False`
en V1.1. `registros suspendidos y cancelados` ya fallaba en V1: ahí es residuo
de F2, no regresión.

Pruebas: `tests/test_cu_v11_audit_findings.py`, ocho casos rojos, con la
consulta realmente evaluada y el conteo de llamadas SQL en el mensaje de fallo.
Detalle y tabla completa en [F1_F5_MATRIX.md](F1_F5_MATRIX.md).

## Lo que sí quedó cerrado

**F1.** La palabra de estado dentro del objeto o del nombre del Actor ya no
cambia el universo. Los tres casos del encargo pasan con Frame OFF y ON, más
ocho contrapruebas (cuatro nombres de Actor con `CANCELADOS`, `REGISTROS`,
`BORRADORES`, `APROBACION`, y cuatro objetos contractuales con esas palabras) y
dos propiedades Hypothesis. `registros en borrador …` conserva su autoridad y
ejecuta. La superficie protegida se pasa desde el resolvedor de nombres propios
y desde los compradores acreditados del Frame.

**F3.** `record_terms` fue eliminado del runtime; `grep -rn record_terms app/`
no devuelve nada. La cadena
`elaboración de borradores de documentos y contratos de consultoría` sobrevive
idéntica en Search, en el perfil con Frame OFF, con Frame ON y en la respuesta
dual. Las dos sustituciones que quedan operan sobre constantes propias
(`_NEXT_STEPS`, `_SCOPE_NOUN`) sin interpolación de datos de la fuente: no son
un `record_terms` global.

**F4.** Diez entradas por la frontera pública, incluidas familias de estado
reales (`no_procede`, `en_evaluacion`) que la primera auditoría no había
probado: ninguna amplía a `source_records`. Un estado incompatible produce
aclaración antes de SQL. Sólo la autoridad versionada `contract-universe-1.1` o
el lookup directo admiten registros; un universo sin versión se degrada con
aviso y uno con versión V1 lanza revisión. Comprobado también en los
productores reales —parser, plan, persistencia y recarga—, no sólo en la
función pura.

**F5.** La composición de procesos con registros explícitos aclara sin ejecutar
ningún hijo, en ambos órdenes de fuente y con Frame OFF y ON. El dual ordinario
`procesos y contratos …` ejecuta ambos hijos: `p6dx-8zbt` sin `estado_contrato`
ni universo, `jbjy-vk9h` con el predicado, y comprador y monto presentes en cada
WHERE. `procesos y registros cancelados …` aclara con cero SQL en ambos órdenes;
nunca ejecuta uno solo.

**Aprobado.** Acreditado; ver [APROBADO_V1_1.md](APROBADO_V1_1.md). Matriz
propia de 302 casos: quince estados por nueve formas de fecha, con paridad
exacta entre `eligible()` y el predicado SQL ejecutado, sin una sola
divergencia. La fuente real acepta el predicado completo de producción sobre
`jbjy-vk9h` y admite una fila Aprobado con fecha; `fecha_de_firma` es
`calendar_date`. La lectura del caso sin fecha venció por timeout y no se
convierte en ausencia ni en conteo.

## Los diez rojos congelados

`10 failed, 33 passed`, exactamente los declarados. La adjudicación no descansa
en una tabla: sobre el runtime V1.1 completo se restauró **sólo** la regla V1
de Aprobado mediante un plugin que no toca ninguna otra superficie, y los 43
quedaron **verdes**. Ninguna otra parte de la remediación contribuye a esos
fallos.

Para cada uno se verificó que las aserciones anteriores pasan y que las
posteriores pasan con el conteo nuevo. Los dos tests del Actor son
especialmente relevantes: su contrato F1 —identidad exacta y universo
celebrado— ya pasa; sólo falla el `26`. Los ports independientes conservan
todas las aserciones originales y añaden la composición que el conteo no podía
expresar (`A-7-dated` dentro, `A-7-absent` y `A-7-invalid` fuera): 11 verdes.
Tabla completa en [FROZEN_TEST_ADJUDICATION.md](FROZEN_TEST_ADJUDICATION.md).

**Se autoriza** actualizar esos diez oráculos en el gate de coordinación, con
los targets y las líneas exactas en [TARGETS.md](TARGETS.md). No se editaron
aquí, y no se usó xfail, skip ni deselect en ningún punto.

## Preservación

El SHA probado es ancestro del HEAD final; el delta posterior son 32 archivos,
**todos** de documentación y evidencia, sin tocar `app/`, `tests/` ni
`scripts/`. El runtime cambiado entre V1 y V1.1 son exactamente los nueve
archivos declarados, sin uno más. Las seis fuentes Python de los tests
independientes conservan un SHA-256 idéntico al de la primera auditoría.
[PRESERVATION.json](PRESERVATION.json).

Dos ficheros del autor sí cambiaron y merecen constar: `contract_universe_support.py`
y `test_contract_universe_properties.py` reciben dos líneas cada uno para
registrar las funciones temporales en su conexión SQLite. Son helpers del autor,
no controles independientes, y el plugin que registran sólo extiende el
dialecto: `datetime.fromisoformat`, sin elegibilidad ni allowlist. La
equivalencia se comprobó por la matriz de paridad, no por confianza.

## Persistencia y versiones

`POLICY_VERSION = contract-universe-1.1` y
`CATALOG_VERSION = entity-profile-frame-2-contract-universe-1.1`.

El artefacto V1 de contraste se generó **ejecutando los serializadores reales
del candidato V1** en un worktree desechable de `067b3977`, no quitando claves a
un dict nuevo. Reproducido contra V1.1: los params V1 son incompatibles y
`universe_from_params` exige revisión; el Frame V1 ni siquiera se deserializa
—el rechazo ocurre en la construcción, antes de cualquier migración—; el plan y
el registro de ejecución piden revisión de catálogo conservando sus bytes; el
buffer conserva su formato (`to_dict()` idéntico) y bloquea la continuación; el
radar conserva su `query_frame_json` intacto y devuelve
`contract_universe_review_required` con cero SQL; el export se bloquea. Una
búsqueda nueva completa sigue funcionando y estampa la versión 1.1, y la
conversación histórica conserva sus bytes.

Que el Frame siga en schema 2 se comprobó por separado: repineando **sólo** la
versión de universo del artefacto V1, el mismo dict carga, hace round-trip
idéntico y produce fingerprint. La única incompatibilidad es la versión de
universo, no los tipos. Cambiar el universo mueve el fingerprint.

## Mutaciones

Once mutaciones —las seis nuevas, con la de Aprobado desdoblada en su rama
Python y su rama SQL, y las cuatro previas— con baseline verde de 372 casos.
**11/11 muertas por aserciones de producto propias, cero errores de importación,
montaje o timeout**, y el worktree de mutación queda limpio tras revertir cada
una. [mutations.json](mutations.json).

## Puertas y regresiones

```text
ruff: exit 0            All checks passed!
semgrep: exit 0         Ran 3 rules on 330 files: 10 findings.
frozen 43: exit 1       10 failed, 33 passed
frozen 43 + shim V1: 0  43 passed
audit propios: exit 0   390 passed
hallazgos: exit 1       8 failed
author targets: exit 0  1217 passed
corpus: exit 0          Summary: 89/89 PASS, 0 FAIL
lint-core: exit 0       app/core/ limpio
diff-check: exit 0      (sin salida)
```

Semgrep es un baseline rojo estable, no un scan limpio: **10 hallazgos en BASE
main, 10 en V1 y 10 en V1.1, con cero nuevos y cero resueltos** comparados por
regla, ruta y fragmento con multiplicidad. Los diez hallazgos históricos no se
tocaron.

Regresiones propias, trece casos verdes: las dos semillas Buyer–Actor con Frame
OFF y ON conservan comprador, monto y la composición completa de filas; el
Actor ordinario resuelve; Search, COUNT y SUM comparten un solo universo; la
paginación no solapa y el export conserva el universo y su versión en ambos
modos; el radar sintético hace round-trip; el perfil con Frame OFF y ON emite
el mismo predicado. **Los procesos ordinarios no reciben predicado contractual
alguno**: ni `estado_contrato`, ni `APROBADO`, ni `contract_universe`.

Ninguna de las regresiones que reproduje pertenece a los frentes excluidos. No
abrí parsing temporal, fuzzy de Actor, montos anómalos, creación de Radar en
lenguaje natural, el toggle del Frame ni P2.

## Aislamiento y límites

Sin LLM, sin app, sin despliegue, sin activación de flags, sin merge ni push,
sin escritura en almacenes del usuario. El ledger offline estuvo activo en cada
pytest y no registró intentos. La única red fueron las cuatro lecturas públicas
anónimas del §8, documentadas una a una.

No se ejecutó la suite global. No se modificó el runtime ni ningún otro
worktree: los dos worktrees desechables se crearon en el scratchpad de la
sesión y se eliminaron. Los conteos son de fixtures controladas y no estiman
SECOP real; las dos filas leídas en vivo no sostienen ninguna afirmación
nacional.

## Pendiente

Cerrar el P1 de F2 en el carril de remediación, incluida la regresión respecto
de V1, y volver a revisión. La actualización de los diez oráculos congelados
queda autorizada y mapeada, pero el gate global, la integración y cualquier
activación corresponden al coordinador después de esa nueva revisión.

# CONTRACT_UNIVERSE_V1_1_F2_1_READY_FOR_FINAL_REVIEW

Se cerró exclusivamente el P1 restante de la segunda auditoría. Ninguna otra
superficie acreditada cambió: F1, F3, F4, F5, Aprobado, persistencia, Radar,
Actor, Frame y Buyer–Actor conservan su comportamiento y sus controles.

**SHA probado:** `da7d1703e4ccc4527612712bee12c5ec609985a3`.
El HEAD final añade sólo esta línea de documentación al SHA probado; se comunica
completo en la entrega y se obtiene con `git rev-parse HEAD` en este worktree.
**Base de remediación:** `2e564d06a4f18a8ef4d3b5a1fa462000e21a00da`.
**SHA probado anterior:** `8fc5643651d6a603d3bc3849369d9773ab55ea11`.
**Segunda auditoría:** `298c79271e1a57ff57c1a841c05dd35d2a0a3b10`.
**Rama:** `fix/contract-universe-v1-1-f2-1`. Sin merge, push, despliegue ni flags.

## La causa, más concreta de lo que parecía

La auditoría situó la causa en que el conflicto sólo se buscaba en la cola
posterior al match. El diagnóstico se confirmó y tiene un mecanismo exacto:
`split_clauses` corta por `y`, de modo que en

    registros celebrados y cancelados de la Alcaldía de Cartagena

`celebrados` queda en la cláusula 1 (`0-20`) y el patrón que selecciona el
estado trabaja en la cláusula 2 (`23-61`), cuyo prefijo está vacío. Ningún
escáner leía la primera mitad: ni el patrón, ni la cola, ni la regla de
«marcador de celebración tras una palabra de fuente», porque `registros` no es
palabra de fuente para `source_request_occurrences`. Con el orden invertido la
segunda condición sí caía en la cola, y por eso aclaraba.

## El arreglo

Un módulo nuevo, [`app/core/contract_conditions.py`](../../../app/core/contract_conditions.py),
con una sola responsabilidad: **enumerar todas las condiciones contractuales que
la petición declara**, antes de que ninguna sea seleccionada. No elige universo,
no construye SQL, no interpreta tema ni comprador.

Cada condición se acredita dentro de **su** cláusula y sólo si lo que la precede
allí es lenguaje funcional, una vez consumidos la palabra de fuente, el
sustantivo de la petición y las condiciones ya acreditadas. Es la misma
propiedad de ocurrencias que ya usa el resto del adaptador, no una excepción por
frase: por eso una palabra dentro de un objeto o de un nombre propio no cuenta
como segunda condición, sin necesidad de listarla.

`ConditionComposition.representable` responde una única pregunta: ¿cabe esto en
un `estado_contrato` singular dentro de un solo universo? No caben dos estados
excluidos distintos, ni un estado admitido junto a uno excluido —eso cruza la
frontera del universo—. Dos condiciones admitidas sí caben: ambas viven en el
celebrado, y seguir ejecutándolas no pierde nada.

`contract_universe_intent` consume esa lectura y **sustituye** con ella las tres
reglas locales que tenía; no se les añadió una cuarta. El span del control pasa
a cubrir la composición entera, incluido el sustantivo que la introduce, así que
`registros` deja de sobrevivir como tema residual de una petición que ni
siquiera se ejecuta. Un camino de seguridad cubre el caso en que ningún patrón
de registros explícitos sea dueño de la petición pero la composición siga siendo
incompatible: se aclara, sin ampliar el universo.

Runtime tocado, y nada más:

- `app/core/contract_conditions.py` (nuevo, 167 LoC)
- `app/core/contract_universe_intent.py` (132 → 143 LoC)

## Matriz antes/después

Medida a través del adaptador real en los dos árboles, por
[`scripts/audits/contract_universe_f2_1_matrix.py`](../../../scripts/audits/contract_universe_f2_1_matrix.py);
datos completos en [matrix.json](matrix.json).

### Composiciones que ejecutaban media petición

| Consulta | Antes | Después |
|---|---|---|
| `registros celebrados y cancelados …` | `source_records` + `Cancelado`, **ejecuta** | aclara, cero SQL |
| `registros celebrados y borradores …` | `source_records` + `Borrador`, **ejecuta** | aclara, cero SQL |
| `registros firmados y cancelados …` | `source_records` + `Cancelado`, **ejecuta** | aclara, cero SQL |
| `registros suscritos y cancelados …` | `source_records` + `Cancelado`, **ejecuta** | aclara, cero SQL |
| `registros suspendidos y cancelados …` | `source_records` + `Cancelado`, **ejecuta** | aclara, cero SQL |
| `registros aprobados y cancelados …` | `source_records` + `Cancelado`, **ejecuta** | aclara, cero SQL |
| `registros cancelados y suscritos …` | `source_records` + `Cancelado`, **ejecuta** | aclara, cero SQL |

La séptima fila no estaba en la lista del encargo: apareció al recorrer el
producto de condiciones en ambos órdenes y fallaba igual que las otras seis.

### Composiciones que ya aclaraban: se corrige el control consumido

| Consulta | Control antes | Control después |
|---|---|---|
| `registros cancelados y celebrados …` | `registros cancelados` | `registros cancelados y celebrados` |
| `registros borradores y celebrados …` | `registros borradores` | `registros borradores y celebrados` |
| `registros cancelados y firmados …` | `registros cancelados` | `registros cancelados y firmados` |
| `registros cancelados y suspendidos …` | `registros cancelados` | `registros cancelados y suspendidos` |
| `registros cancelados y aprobados …` | `registros cancelados` | `registros cancelados y aprobados` |

Antes, la mitad no consumida podía sobrevivir como tema. Ahora el control cubre
la composición completa en los dos órdenes.

### Lo que no cambió

Idénticos antes y después, sin una sola diferencia: `registros cancelados`,
`registros en borrador`, `registros enviados al proveedor`, `contratos
celebrados`, `contratos suspendidos`, `todos los contratos`, `todos los
registros del dataset`, `registros cancelados y borradores` y su inverso, y los
dos controles de F1 (`contratos de elaboración de borradores de documentos …`,
`contratos de custodia de borradores cancelados de archivo …`).

## Simetría

[`tests/test_contract_universe_f2_1_symmetry.py`](../../../tests/test_contract_universe_f2_1_symmetry.py),
**46 casos verdes**. Nueve pares A/B se ejecutan en los dos órdenes por el
workflow completo y se compara el resultado **observable**, no los params: si
aclara, cuántas consultas SQL se emitieron, cuántas filas volvieron, qué
universo se estampó, qué estado quedó filtrado y qué estados devolvió la fuente.
Los dos órdenes deben producir el mismo diccionario, y ese diccionario debe ser
aclaración con cero SQL y cero filas.

Dos propiedades Hypothesis completan el cuadro sobre el producto cartesiano de
diez condiciones admitidas por cinco excluidas: leer «A y B» y «B y A» da la
misma composición, y cruzar la frontera del universo siempre aclara, en ambos
órdenes.

Los pares incluyen dos que el encargo no pedía —`prorrogados`/`borradores` y
`terminados`/`cancelados`— porque la regla es por frontera de universo, no por
lista de frases.

## Pruebas de la auditoría

Ejecutadas intactas, con sus hashes verificados contra `298c792`:

```text
tests/test_cu_v11_audit_findings.py        8 passed   (eran los 8 rojos)
resto de los tests del auditor           390 passed
```

Los 43 congelados literales siguen dando `10 failed, 33 passed`: exactamente los
mismos diez de la adjudicación de Aprobado, ninguno nuevo. El control de
causalidad del auditor se repitió sobre este árbol —restaurar sólo la regla V1
de Aprobado— y devuelve `43 passed`, así que F2.1 no introdujo ninguna otra
causa. Esos diez no pertenecen a esta microremediación; para la política V1.1
sirven los ports adjudicados, que pasan.

## Regresión focal

```text
ruff: exit 0                      All checks passed!
semgrep: exit 0                   Ran 3 rules on 331 files: 10 findings.
f2_1_symmetry: exit 0             46 passed
auditor_findings_now_green: 0     8 passed
auditor_rest: exit 0              390 passed
remediation_85: exit 0            85 passed
author_targets: exit 0            1217 passed
frozen_43_literal: exit 1         10 failed, 33 passed
frozen_43_with_v1_shim: exit 0    43 passed
corpus: exit 0                    Summary: 89/89 PASS, 0 FAIL
lint-core: exit 0                 app/core/ limpio
diff-check: exit 0                (sin salida)
```

Semgrep sigue siendo un baseline rojo estable: **10 hallazgos en BASE main y 10
aquí, cero nuevos y cero resueltos**, comparados por regla, ruta y fragmento con
multiplicidad ([semgrep_delta.json](semgrep_delta.json)). Los diez históricos no
se tocaron. Los topes de tamaño pasan; ningún módulo con tope se modificó.

Comandos completos, códigos de salida y colas de salida en
[validation.json](validation.json), producidos por
[`scripts/audits/contract_universe_f2_1_validate.py`](../../../scripts/audits/contract_universe_f2_1_validate.py).

## Mutaciones

Cinco mutaciones sobre una copia desechable, con baseline verde de 102 casos.
**5/5 muertas por aserciones de producto, cero errores de importación, montaje o
timeout**, y cada archivo mutado vuelve byte por byte a su contenido. Detalle en
[mutations.json](mutations.json).

| Mutación | Qué restaura | Resultado |
|---|---|---|
| M1 detección sólo en la cola | el cálculo de conflicto V1.1 exacto | 26 fallos |
| M2 first-match-wins | ejecutar el primer estado seleccionable | 29 fallos |
| M3 sólo dos excluidos adyacentes cuentan | ignorar la composición leída | 25 fallos |
| M4 la frontera de universo deja de ser conflicto | admitido + excluido se ejecuta | 25 fallos |
| M5 el lector pierde la propiedad de cláusula | una palabra del objeto o del nombre cuenta como condición | 6 fallos |

M5 es la que acredita que la propiedad de ocurrencias es load-bearing y no
decorativa: sin ella, `FUNDACION BORRADORES CANCELADOS` y un objeto con dos
palabras de estado producirían aclaraciones espurias. Ese caso no lo cubrían ni
la remediación anterior ni la auditoría; se añadió aquí.

## Límites

No se implementó la unión multi-estado: la aclaración es la salida adoptada, tal
como el encargo la admite. Dos condiciones **admitidas** siguen ejecutándose,
porque ambas caben en el universo celebrado y no hay nada que perder; sólo se
aclara cuando la composición no es representable.

Los conteos son de fixtures controladas. No se ejecutó la suite global, no hubo
SECOP live, LLM, app, despliegue, activación, merge ni push. Los worktrees
desechables usados para la matriz y las mutaciones se crearon en el scratchpad
de la sesión y se eliminaron; ningún otro worktree fue tocado.

Pendiente: la revisión final y cualquier gate o integración del coordinador,
incluida la actualización de los diez oráculos congelados por Aprobado, que
sigue siendo ajena a esta microremediación.

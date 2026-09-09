# BUYER_ACTOR_HARNESS_COMPATIBILITY_ADJUDICATED

Base runtime/remediation: `387267d2156f96e03d69b2a8b4039c218ab56638`.
Evidencia causal R2 consultada: commit documental `9e264f6`.
Rama/worktree propio: `codex/contract-universe-v1-1-r2b-buyer-harness` en
`/Users/rodrigoortiz/Documents/secoppal-contract-universe-v1-1-r2b-buyer-harness`.
CARRILES.md leído; asignación expresa R2B: fixture/arnés Buyer-Actor y evidencia.

## Cambio adjudicado

El fixture contractual usa En ejecución y una emulación NFD real de unaccent.
Se sustituye el guard posicional del primer AND por lectura de comparaciones
campo/literal: sólo objeto_del_contrato y descripcion_del_proceso pueden
acreditar contaminación de objeto. La identidad legítima
ALCALDIA MUNICIPAL DE BARANOA permanece válida como nombre_entidad.

El lector pequeño vive en tests/buyer_actor_object_predicates.py. Reconoce
LIKE/NOT LIKE/igualdad y wrappers unaccent/upper/lower/trim del builder; las
cadenas SQL con apóstrofes escapados son tokens indivisibles. No es un parser
SQL general ni política productiva. El builder actual emite comparaciones
unaccent(UPPER(campo)) LIKE unaccent(UPPER(literal)); las contrapruebas incluyen
SQL generado por ese builder, negación/coalesce y comparaciones directas.
Ninguna posición de AND/OR participa en la adjudicación del campo.

La emulación de texto acreditada existía como lambdas inline en
`tests/test_contract_universe_properties.py::database` y
`tests/buyer_actor_independent_support.py::attach_source`; no había helper de
unaccent reutilizable. El nuevo helper conserva exactamente NFD + eliminación
de combining marks (con None preservado), sin alterar esos tests. El plugin
existente tests.contract_universe_sql_dates sigue registrando date_extract_y.

Los 349 archivos de app/ son byte-idénticos a la base (runtime_identity.json).
El único archivo existente editado es test_buyer_actor_role_arbitration_v1.py:
importaciones, registro unaccent, estado/comentario y guard dentro de source_for.
El AST de todas sus demás funciones y sus assertions es idéntico. El bucle de
62 filas es idéntico salvo el literal de estado. Controls y goldens no se tocaron.

## Resultados medidos

| Grupo | Resultado |
|---|---|
| C05 | 20/20 |
| C06 | 4/4 |
| C07 residuales R1 | 8/8 |
| C08 | 12/12 |
| Integridad/contrapruebas nuevas | 18 passed |
| Replay exacto de 98 | 44 passed / 54 failed |

Los nodeids se derivaron de previous_116_after_fix.json de R1 y se verificó
igualdad de conjuntos contra JUnit. Los 44 que pasan en el replay son exactamente
C05–C08; los 54 fallos pertenecen a los otros 22 clusters. No hubo errores ni skips
en los 44 obligatorios, en las mutaciones, ni en el replay de 98.

El test de integridad usa eligible/predicate productivos, sin duplicar estados:
las 62 filas son celebradas, ninguna usa Firmado; SYNTH-060 sólo se elimina por
monto y SYNTH-061 sólo por buyer. La propiedad Hypothesis determinista inserta
hasta 12 condiciones previas/posteriores, permuta AND/OR y conserva el resultado
del guard en 100 ejemplos. El acceso a fuentes de estos controles es sintético.
physical_rows_observed.json verifica 32 turnos de seeds/continuidad con filas
no vacías, sin los dos decoys ni Actor.

## Mutaciones aisladas

| Mutación | Passed | Failed | Kill |
|---|---:|---:|---|
| M1 restaurar Firmado | 0 | 44 | assertions del arnés |
| M2 restaurar unaccent identidad | 0 | 44 | assertions del arnés |
| M3 restaurar split del primer AND | 40 | 4 | Baranoa / request language as topic |

Cada mutación parte de una copia desechable de la base más los cuatro archivos
del candidato y cambia sólo el mecanismo indicado. Se ejecutan los mismos 44
nodeids en cada copia. El candidato nunca se mutó. M1/M2 dejan la fuente vacía;
la recuperación intenta una consulta sin monto y dispara la assertion existente.
M3 produce exactamente el falso positivo de Baranoa demostrado en R2.
Los kills son AssertionError; no errores de importación ni timeouts.

## Puertas: salidas literales

```text
All checks passed!
Ran 3 rules on 331 files: 10 findings.
44 passed in 9.92s
18 passed in 1.00s
19 passed in 3.82s
34 passed in 11.91s
43 passed in 5.80s
Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
54 failed, 44 passed in 12.72s
app/core/ limpio
2 passed in 0.12s
```

Semgrep: 10 heredados, 0 nuevos; exit 0 no equivale a scan limpio. Omite un
archivo mayor de 1 MB. Sus stdout/stderr íntegros están en semgrep.json;
los .txt sólo eliminan espacios finales para cumplir diff-check.
residual_98.xml también normaliza esos espacios; su original íntegro está en
residual_98_junit_raw.json.
Los 43 frozen incluyen los dos tests size; no sumar suites solapadas.
Diff-check final en diff_check.json: salida vacía y código 0.

Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph 0.9.6 con Node 22
reporta Not initialized; se usó rg + revisión directa como fallback. No se
atribuye cobertura a un índice inexistente. Disponibilidad no es validación.

La primera colección del test nuevo falló por la capitalización SoqlBuilder,
corregida a SoQLBuilder. Se conserva como harness_integrity_initial_import_error;
no se contabiliza como kill ni como gate final.

## Fuera del carril y límites

No se adjudicaron ni repararon los otros 22 clusters; el archivo
RESIDUAL_98_AFTER_R2B.json conserva sus 54 nodeids y conteos observados.
La comprobación adicional de consumers dio 14 passed / 4 skipped: cuatro casos
locales son opt-in mediante BUYER_ACTOR_LOCAL_INDEX, no configurado. Se conserva
su skip; no forman parte de C05–C08. Incluye ocho variantes solapadas con C07,
dos variantes adicionales y cuatro casos de reemplazo de Actor por Buyer.

Corrí los 44 nodeids, las contrapruebas nuevas, las muestras focales, los consumers
indicados y los 98 residuales, no la suite. No se ejecutó suite global, índice
local opt-in, merge, push, deploy ni restart. El replay parcial de 98 conserva
54 fallos: esta adjudicación no acredita una puerta global de integración.

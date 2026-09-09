# Contract Universe V1.1 — Integration R3

**SQL_ORACLE_COMPATIBILITY_ADJUDICATED**

Los 34 nodeids de C09 y C14–C23 eran oráculos obsoletos. Se preservan las
propiedades originales y se exige el universo celebrated como conjunción
independiente en contratos. Ningún cambio productivo fue necesario.

- Base solicitada: `cb02e086c42686792def9e16037dc1e31e33489b`.
- Main histórico de R1, usado como control: `c52d7eda355f44278164d4cb9803a199eec83486`; no se utiliza un main móvil.
- Rama: `codex/contract-universe-v1-1-r3-sql-oracles`.
- Worktree: `/Users/rodrigoortiz/Documents/secoppal-contract-universe-v1-1-r3-sql-oracles`.
- Alcance: siete módulos de tests, nueve hashes contractuales, un helper y ocho
  pruebas adicionales. Los nombres de funciones/nodeids se conservan para el replay exacto.
- Cero cambios en `app/`, `scripts/`, fuentes físicas, aliases, configuración o
  evidencia histórica. `runtime_identity.json` acredita 541 archivos por SHA-256.
- Los siete archivos de tests originales son idénticos entre main histórico y
  cb02e08: `original_test_identity.json`. Main original: 34 passed; candidato
  original: 34 failed; candidato adjudicado: 34 passed.

## Tesis de adjudicación

C09/C18/C19 comparan únicamente el predicado de identidad con las igualdades y
variantes físicas esperadas. Las cláusulas se separan sólo por AND de nivel
superior, respetando strings y paréntesis; un OR exterior hace fallar el oráculo.
El universo se verifica contra una especificación congelada independiente de la
función productiva, para no volver tautológica la prueba.

C14/C15/C21 exigen 1=0 como conjunct efectivo: ninguna asignación, ni NULL, puede
rescatar filas. Hypothesis permuta su posición y verifica que retirarlo falle;
se rechazan falsos dentro de strings y disyunciones. SQLite ejecuta el SQL
original y la contraprueba: 0 filas frente a 9 al retirar el falso.

C20 acredita que allow_global evita el anti-global guard pero deja exactamente
el universo celebrado, sin buyer, geografía o topic artificiales.
**allow_global no equivale a source_records.**

C16: los 9 inputs canónicos sólo agregan `contract_universe=celebrated` y
`contract_universe_version=contract-universe-1.1`. No hay cambios adicionales
que necesiten una adjudicación heredada. `FINGERPRINT_DIFFS.json` conserva params
completos, inputs canónicos, hashes viejos/nuevos y SQL de los 12 controles:
9 contractuales y 3 no cambiados. Los marcadores de topic, buyer, territorio,
dinero, fechas, estado y proveedor permanecen. Los tres hashes restantes no
se modifican. Las pruebas nuevas requieren separación entre universos y
versiones e invariancia al orden de keys.

C17 conserva el NOT completo sobre los dos coalesce/LIKE de objeto y descripción.
SQLite ejecuta las nueve combinaciones NULL/non-match/match: conserva 4 filas;
las variantes sin NOT o sin null safety dan un resultado distinto y son rechazadas.

C22/C23 preservan el predicado source original y procesos no reciben universo.
**C22 ya usaba LIKE legítimamente** en las ocho identidades sin binding. Por eso
M1b altera ese operador a igualdad y los ocho tests lo detectan; imponerles
exactitud violaría la instrucción de preservar el operador que correspondía.
M1 exacto→LIKE sí es aplicable a C09/C18/C19/C23 y sus 12 nodeids lo detectan.

`SQL_PROPERTY_MATRIX.json` contiene las capturas main/candidato por nodeid y
llamada. Al separar la cláusula contractual independiente, todo el SQL restante
es idéntico; procesos es idéntico sin quitar nada. La captura controlada fija
PYTHONHASHSEED=0 en ambos procesos. Las primeras capturas sin seed conservadas
como `unseeded_*` mostraron permutaciones de alternativas OR morfológicas por
orden de sets; no se alteró runtime para ordenarlas. Los fingerprints canónicos
ya eran independientes de ese orden.

## Puertas ejecutadas

Corrí los 34 nodeids adjudicados, las pruebas focales indicadas y el replay exacto
de 54; **no la suite**. Los totales de abajo se solapan y no se suman.

| Comprobación | Resultado observado |
|---|---|
| Main histórico, tests originales | 34 passed |
| Antes, cb02e08 | 34 failed |
| Después, mismos nodeids | 34 passed |
| Auditor final | 19 passed |
| F2.2 | 34 passed |
| Frozen, incluye size | 43 passed |
| Corpus | 89/89 PASS |
| Buyer–Actor C05–C08 | 44 passed |
| Módulos de tests modificados, incluidos controles procesos y pruebas nuevas | 271 passed |
| Nuevas propiedades R3, incluidas en 271 | 8 passed |
| Size, también incluido en frozen | 2 passed |
| Replay exacto 54 | 34 passed / 20 failed / 0 errors / 0 skipped |
| Mutaciones desechables | 7/7 rechazadas por assertions |

Salidas literales de las tres puertas, extractadas sin reformular:

```text
All checks passed!
Ran 3 rules on 331 files: 10 findings.
34 passed in 1.69s
```

Salidas completas y comandos: `ruff.txt/json`, `semgrep.txt/json`,
`after_34.txt/json`. Semgrep devuelve exit code 0, pero su baseline continúa
**rojo con 10 hallazgos**; no se presenta como scan limpio. Delta 0: runtime y
reglas son idénticos a cb02e08, cuyo scan R2B también tiene 10. lint-core:
`app/core/ limpio`. Size: `2 passed in 0.11s`. diff-check: exit code 0, sin salida.

Instrumentación: Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10, ejecutado con
propiedades deterministas sin I/O externo. Codegraph CLI 0.9.6: status
`Not initialized`. No se usó un índice no acreditado ni un MCP de Codegraph;
fallback a rg y revisión directa de SoQLBuilder, query_fingerprint,
contract_universe y consumidores focales.

## Mutaciones

Copias desechables de cb02e08 con sólo los tests de este carril superpuestos;
se eliminan después. No se mutó runtime entregado. Cada mutación conserva
comando, salida y JUnit. `MUTATIONS.json` acredita failures por assertions,
sin import errors ni skips. Pytest renderiza algunas assertions como `E assert`
y otras como `E AssertionError`; ambas se contabilizan desde su evidencia XML.

| Mutación | Nodeids rechazados |
|---|---:|
| M1 exacto→LIKE | 12 |
| M1b LIKE legítimo→exacto | 8 |
| M2 retirar falso | 3 |
| M3 retirar universo | 1 |
| M4 colapsar fingerprints de universo/versión | 10 |
| M5 retirar NOT | 1 |
| M5b retirar coalesce | 1 |

## Fuera del carril y trabajo pendiente

El replay conserva exactamente los clusters **C01–C04, C10–C13, C24–C26**:
20 fallos. No se adjudican ni corrigen aquí; los nodeids y resultados están en
`RESIDUAL_54_AFTER_R3.json`. Este veredicto sólo cierra los oráculos SQL de R3;
no declara verde la integración global.

No quedan pendientes dentro de R3. Suite global, resolución de los 20 restantes,
merge, push, deploy y restart no se ejecutaron y permanecen fuera de esta orden.
Los tests heredados con salida histórica fija se ejecutaron conservando sus bytes
previos; sus nuevas observaciones se copiaron a esta carpeta.

### Formato de evidencia

El diff-check de toda la entrega encontró espacios finales en logs crudos de
pytest al añadirlos a Git. Se conserva cada original afectado en
`*.txt.raw.json` / `*.xml.raw.json`, con su SHA-256; las representaciones TXT/XML
sólo eliminan espacios finales y líneas vacías terminales. Los JSON de comandos
conservan stdout/stderr literales. `log_presentation.json` lista los archivos;
`diff_check_before_log_format.json` conserva el diagnóstico anterior. El gate
final compara la entrega completa contra cb02e08, incluidos los archivos añadidos.

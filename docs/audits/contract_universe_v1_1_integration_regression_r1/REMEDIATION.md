# CONTRACT_UNIVERSE_V1_1_INTEGRATION_REGRESSION_R1_MULTIPLE_CAUSES

La selección documental pendiente queda restaurada al distinguir defaults derivados de intención expresa. Cambio runtime único: actor_pending_document.py; pruebas nuevas: test_actor_pending_system_metadata.py. F1–F5/Aprobado, stamping y workflow permanecen intactos.

## Puertas ejecutadas

Corrí el archivo completo Actor, las propiedades nuevas y los grupos focales indicados abajo, no la suite global. Las salidas literales completas están en los .txt/.json de esta carpeta.

- actor_after: 61 passed in 36.18s
- frozen_after: ============================== 43 passed in 5.56s ==============================
- auditor_final_19: 19 passed in 3.48s
- f2_2_product_properties: 34 passed in 11.98s
- f2_1_symmetry: 46 passed in 4.84s
- f2_1_completeness: 248 passed in 30.19s
- auditor_findings_now_green: 8 passed in 1.30s
- auditor_rest: 390 passed in 9.40s
- remediation_85: 85 passed in 8.52s
- corpus: Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
- ruff: All checks passed!
- lint_core: app/core/ limpio
- size: 2 passed in 0.11s
- Semgrep: `Ran 3 rules on 331 files: 10 findings.` Baseline 10, delta 0; no es verde global.
- diff-check: exit 0, stdout vacío.

## Fuera del carril

18 de los 116 fallos anteriores pasan; 98 persisten. FAILURE_CLUSTERS.md registra 26 clusters con un representante idéntico main/R1, todos A. Son superficies distintas y varios requieren adjudicación de fixtures/goldens. No se considera deuda previa un fallo que pasa en main. No se tocaron esos tests ni runtime independiente.

## Pendiente y decisión requerida

Global no ejecutada: §11 ordena detener y §12 no autoriza aún la global. Se solicita al coordinador decidir carriles/adjudicación para los clusters residuales antes del retry. No se emite READY. No merge, push, deploy, restart ni cambios de flags. Las deudas expresamente excluidas siguen fuera de alcance.

Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10 con propiedades deterministas sin I/O. Codegraph 0.9.6 disponible con Node22, índice no inicializado; consumidores inspeccionados con rg y lectura directa. No se utilizó MCP Codegraph ni se declaró índice activo.

Los JSON conservan stdout/stderr literales. Las vistas TXT, XML y Markdown eliminan espacios finales para cumplir diff-check; no cambian assertions ni resultados. Los comandos de MUTATIONS.json son la autoridad de las mutaciones finales.

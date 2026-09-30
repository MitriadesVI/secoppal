# R1.2 — gates locales antes del freeze

Destino: candidate funcional local para ZEUS fresco. No se emite GO_GLOBAL_GATE ni merge readiness; no se ejecutó global.

BASE_CODE 8e0b9ca3314f986bc8bfc396603996e10264e05b. Auditoría de origen f3b250b9842dbca9faf9f36d84d853c8470d7e4f. Scope producto: sólo app/core/foundation_presentation.py y tests/test_foundation_publication_adapter.py. Ningún otro runtime cambió.

RED previo: dos nodeids nuevos públicos Telegram/WhatsApp fallan por AssertionError 50!=25 antes de tocar presentación. Los errores de construcción de fixture previos se guardaron y excluyeron. ARES expanded: cinco archivos,132 passed. HERA canales/receipts/trace/Excel: ocho archivos disjuntos,73 passed. Total205 passed, no suite global. Un test Hypothesis parametriza capacidades0..40, hasta8 grupos, budget0..350 y tres canales; verifica conservación/prefijos/identidad/no mutación/fairness/tie/representación, no copia el bucle del allocator.

HERA transporte propio:21 escenarios públicos (30/30,2/30,0/30,10/10,30/2,30/0,error+30 por Telegram/WhatsApp/Streamlit). CODE retorna25=13+12 para30/30,2+23 para2/30,25+0 para30/0,10+10 para10/10. Identidad exacta R1.1/R1.2 de results/groups/rows/tipos/llamadas en todos. Streamlit no incorpora tarjetas al texto y conserva60 en rows. Grupo sin cuota no se presenta como source vacío. Encabezados no vacíos anuncian count asignado. MatrizPUBLICATION_MATRIX y ambos snapshots completos en evidence/.

Z1: strings9007199254740993/1500000000 y URLNone preservados; fuente original intacta y rows tarjetas, sin volcadoIDs. Z2: fallback single grupo30 anuncia25 y publica25 en ambos canales texto, web30;60 casos de canales sin superarlimit. Contexto raw/guard/count permanece intacto por source hash y oracle de aislamiento/retry/turnos/threadpool.

M01–M19:19 controles fresh passed antes de cada mutante,19 muertos por AssertionError de propiedad. In-memory source overlay, no bytes runtime escritos. Diff candidate idéntico antes/después por mutante; diffs exactos y salidas en evidence. M17_GLOBAL_RESPONSE_BUDGET (50!=25),M18_REDISTRIBUTE_UNUSED_QUOTA (2/12!=2/23),M19_FAIR_SHARE_AND_REPRESENTATION (25/0!=13/12). M16 aislado en proceso nuevo.

Flag OFF:30 pares R1.1/R1.2 (15legacy,15default), cero diferencias materiales;response/rows/results/tipos/route/state/outcome/error/SoQL/dataset/params/calls/orden, sin normalización. Reloj fijo en origen y persistencia/tmp. Defaultlegacy no cambió.

Ruff runtime/tests/instrumentosPASS; diff-checkPASS; Semgrep baseline runtime0/candidate0/new delta0, instrumentos0, errors0. Config repo excluye tests del scanner; no se declara test escaneado. Ver COMMANDS.jsonl y ARES_*.argv.json. TOOLCHAIN: Python3.14.7,pytest9.0.2,Hypothesis6.165.10,Ruff0.16.6,Semgrep1.176.0,macOS26.6.1arm64;dependencias preexistentes, sandbox-execdeny network.

HERA adjudica Z3LOCAL_DEFECT/B contract application. Sin nueva arquitectura, DESIGN_REOPEN=0. Un evento bruto Olympus repairB de ARES correspondió a corrección de fixture durante implementación; adjudicadoIMPLEMENTATION_CORRECTION/TOOLING_PROBLEM, no remediación de candidate congelado, no se borró historial. LOCALIZED_REMEDIATION_USED=0. Olympus run r-20260930-184140-647b conserva baselineCOMPLETE/ownership/gates/scorecard fuera producto.

Modelo: ARES solicitado gpt-6.1-sol/medium fresh; resolución efectiva UNKNOWN. HERA modelo/esfuerzo efectivo UNKNOWN. Un solo writer producto;HERA docs/instrumentos. ZEUS todavía no ejecutado: se entregará prompt sin narrativa persuasiva. Sin main writes, merge/push/deploy/restart/reactivación/proveedor/red/global.

# Preservación semántica

`preservation.json`: comparación byte a byte de 1029 archivos app/scripts/tests
contra `4a22efc330101c3511c034a53cd02d4790bb192f`. Solo difieren los tres archivos
de oráculos autorizados. Runtime no modificado. `app/config.py` es idéntico a main;
lectura AST acredita `secoppal_entity_profile_frame_enabled` default=False.
No se construyó Settings fuera de pytest ni se cambió ninguna flag.

Gates focales repetidos y verdes (salida literal en `focal_static_literal.json`):
- F1/F3/F4/F5: auditor_rest (incluye test_cu_v11_audit_f1_f5.py) y remediation_85.
- Aprobado V1.1: test_cu_v11_audit_aprobado.py y ports independientes, dentro de auditor_rest;
  frozen_after 43/43 con los oráculos adjudicados y sin shim de política.
- Buyer–Actor Barranquilla/Cartagena: test_cu_v11_audit_regressions.py,
  test_buyer_actor_independent_product.py y test_buyer_actor_independent_boundaries.py.
- Procesos ordinarios: test_ordinary_processes_never_receive_a_contract_predicate
  y controles duales preservados de frozen_after.
- Política `contract-universe-1.1`: gates de persistencia y regresiones del auditor.
- Artefactos V1: test_cu_v11_audit_persistence.py y frozen legacy;
  rechazo explícito/revisión sin reinterpretación silenciosa.

Estos son controles focales; su verde no reemplaza el gate global.

# Adjudicación expresa de una expectativa activa

Consulta: `que ha contratado UNIVERSIDAD INDUSTRIAL DE SANTANDER`.
Test: `test_all_expanded_strong_families_produce_clean_actor_frames`.
Original: `52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386:tests/test_actor_conversation_corpus.py`.
SHA256 original: `14db9a2aec764eb6f7436efbca904550c27bea0a59cbd00a85969e620861df1e`.

Contrato viejo: todas las familias verbales cruzadas con real_actors deben producir SET_ACTOR.
Contrato nuevo: esta mención institucional completa, acreditada por exact, es comprador ante «qué ha contratado»; el verbo solo no decide contractor.
Autorización del prompt: «Qué ha contratado es evidencia verbal de consulta contractual, NO prueba universal de que su sujeto sea contratista» y §7 permite ajustar únicamente la expectativa contradictoria conservando su fuente.

Cambio: solo ese par familia/mención verifica contratos + comprador y ausencia de Actor/tema. Los demás pares mantienen sus expectativas originales, incluidos los proveedores explícitos UIS.
No se modifican filas/documentos del corpus ni los fixtures de identidad, tokens, TTL, selección o permisos. No se introduce xfail, skip ni deselect para este caso.
Dos xfails estrictos ya presentes en test_actor_hardening_v2_1_remediation.py:341 (actor + región + comprador) se ejecutan y se reportan como tales; no fueron introducidos ni modificados.

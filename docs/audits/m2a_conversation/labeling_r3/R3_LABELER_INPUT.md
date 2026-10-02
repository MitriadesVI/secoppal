# M2A_GOLD r3: entrada para el etiquetado ciego

Etiquetan dos etiquetadores (A y B), cada uno por separado y sin ver el trabajo del otro.

## Instrucciones

1. **Autoridad.** Hay dos, y sólo dos:
   - el contrato `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` (en esta rama, con la enmienda R3);
   - el esquema `docs/audits/m2a_advisory_2026-10-01/02_gold_followups/kit_v2_2/SCHEMA_v2_1.md`.

   Donde difieran, manda el contrato (§0; el Anexo B dice cómo se lee el esquema). No consultes ningún otro documento,
   test ni archivo del repositorio, ni etiquetas de otros casos.
2. **Etiqueta.** Para cada caso de abajo, produce una etiqueta con la forma de `label_A` / `label_B` del esquema 2.1.
   - Copia tal cual: `gold_id`, `schema`, `slice`, `source_kind`, `input_kind`, `followup_text`, `followup_sha256` y
     `prior`.
   - Completa: `labeler` (`A` o `B`), `turn_kind`, `m2a_scope`, `ops`, `expected_disposition`, `disposition_reason`,
     `resulting_ir`, `effects`, `expected_echo`, `alternatives`, `ambiguity`, `confidence`, `gap_codes` y `notes`.
3. **Varias lecturas.** Si un caso admite más de una lectura bajo el contrato, no la fuerces: márcala en `ambiguity` y
   `confidence` y explica en `notes` qué regla lleva a cada una.
4. **Entrega.** Un JSONL, una etiqueta por línea, en la ruta que indique el orquestador. No modifiques el repositorio.

## Casos

```jsonl
{"gold_id": "SF192", "schema": "m2a-followup-gold/2.1", "slice": "SYNTHETIC_FOLLOWUP", "source_kind": "SYNTHETIC", "input_kind": "TEXT", "followup_text": "ya no los ordenes por valor", "followup_sha256": "252601648ad05238c0f56c28540c9e8acb5953e3abc7273d734d8534311e56db", "prior": {"chain": [], "gap_seconds": 50, "gold_id": null, "owner": "FOUNDATION_SEARCH", "pending": null, "selection_rule": "SYNTHETIC_AUTHORED", "state": "EXECUTED", "ir": {"aspect": null, "cardinality": {"n": 10, "n_requested": 10, "operator": "top_n"}, "conditions": [{"dimension": "TOPIC", "id": "c1", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "vigilancia"}, {"dimension": "BUYER", "id": "c2", "level": null, "operator": "is", "polarity": "POSITIVE", "unit": null, "value_hint": "Gobernación del Meta"}, {"dimension": "TIME", "id": "c3", "level": null, "operator": "during_year", "polarity": "POSITIVE", "unit": null, "value_hint": "2025"}], "host_notice": [], "logic": null, "operation": "SEARCH", "order": {"direction_source": "STATED", "operator": "value_desc"}, "product_view": null, "universe": "CONTRACTS", "view": "RECORDS", "view_source": "USER"}}}
{"gold_id": "SF193", "schema": "m2a-followup-gold/2.1", "slice": "SYNTHETIC_FOLLOWUP", "source_kind": "SYNTHETIC", "input_kind": "TEXT", "followup_text": "y los de 2024 con la misma entidad contratante", "followup_sha256": "45ad7a07b3dc7d039f230db53699884c8533100027c767bc5e9db4ae84298bfa", "prior": {"chain": [], "gap_seconds": 35, "gold_id": null, "owner": "FOUNDATION_SEARCH", "pending": null, "selection_rule": "SYNTHETIC_AUTHORED", "state": "EXECUTED", "ir": {"aspect": null, "cardinality": null, "conditions": [{"dimension": "TOPIC", "id": "c1", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "mantenimiento de vías"}, {"dimension": "TIME", "id": "c2", "level": null, "operator": "during_year", "polarity": "POSITIVE", "unit": null, "value_hint": "2025"}], "host_notice": [], "logic": null, "operation": "SEARCH", "order": null, "product_view": null, "universe": "CONTRACTS", "view": "RECORDS", "view_source": "USER"}}}
{"gold_id": "SF194", "schema": "m2a-followup-gold/2.1", "slice": "SYNTHETIC_FOLLOWUP", "source_kind": "SYNTHETIC", "input_kind": "TEXT", "followup_text": "pero que no sean de cafetería", "followup_sha256": "a889ce1303b771cc80d3f41f25e39e567bbfd8d07d456ac9c14d3e445d8b9399", "prior": {"chain": [], "gap_seconds": 40, "gold_id": null, "owner": "FOUNDATION_SEARCH", "pending": null, "selection_rule": "SYNTHETIC_AUTHORED", "state": "EXECUTED", "ir": {"aspect": null, "cardinality": null, "conditions": [{"dimension": "TOPIC", "id": "c1", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "aseo"}, {"dimension": "TOPIC", "id": "c2", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "cafetería"}, {"dimension": "BUYER", "id": "c3", "level": null, "operator": "is", "polarity": "POSITIVE", "unit": null, "value_hint": "Alcaldía de Pereira"}], "host_notice": [], "logic": null, "operation": "SEARCH", "order": null, "product_view": null, "universe": "CONTRACTS", "view": "RECORDS", "view_source": "USER"}}}
{"gold_id": "SF195", "schema": "m2a-followup-gold/2.1", "slice": "SYNTHETIC_FOLLOWUP", "source_kind": "SYNTHETIC", "input_kind": "TEXT", "followup_text": "quítales el orden por valor y ordénalos por fecha", "followup_sha256": "4917df8b6294041d34380b3e3ab999b0be81b5d7798530eac400a97437467503", "prior": {"chain": [], "gap_seconds": 45, "gold_id": null, "owner": "FOUNDATION_SEARCH", "pending": null, "selection_rule": "SYNTHETIC_AUTHORED", "state": "EXECUTED", "ir": {"aspect": null, "cardinality": {"n": 10, "n_requested": 10, "operator": "top_n"}, "conditions": [{"dimension": "TOPIC", "id": "c1", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "vigilancia"}, {"dimension": "BUYER", "id": "c2", "level": null, "operator": "is", "polarity": "POSITIVE", "unit": null, "value_hint": "Gobernación del Meta"}], "host_notice": [], "logic": null, "operation": "SEARCH", "order": {"direction_source": "STATED", "operator": "value_desc"}, "product_view": null, "universe": "CONTRACTS", "view": "RECORDS", "view_source": "USER"}}}
```

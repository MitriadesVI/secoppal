# Approved Executive UX V1 plan

Base `56f689df112bac60e9e384818db447af8969b921`. Planner: gpt-5.6-sol / high.
PLAN_CONFIDENCE: HIGH. IMPLEMENTATION_COMPLEXITY: MEDIUM.

## Decision

Choose B: Streamlit structured view, with a small pure shared projection from the accredited EntityProfile. A (Markdown-only) cannot provide components without forbidden text parsing and affects other channels. C (general shared presentation model) adds abstraction for consumers that do not currently need structured rendering. B introduces an optional presentation payload through existing response plumbing, not a new semantic authority or persistence model. The task's architecture-larger-than-expected PLAN_READY condition does not apply after this bounded plan is established.

## Packages — one primary writer, Sol Medium

1. Pure versioned JSON-safe overview projector in entity_profile. Include identity, executed period/territory, four available/unavailable KPI slots, modalities, independently accredited supplier leaders, recent title excerpt and full text, sources, executed universe labels, warnings and scope. Formatting/truncation allowed; no parsing final text, queries, metric/share recalculation or scope inference.
2. OFF projection after build_entity_profile; carry through entity_profile_state, execute_query's Burr writes, graph default, normal_flow explicit result extraction and results.CANONICAL_KEYS. ON use the same build_profile_sections instance for text and projection; carry through ProfileTurnOutcome and profile_route._result. Clear/omit view on failures, clarification and unrelated routes. Do not persist the view in Turn/envelope or alter publish_turn.
3. Dedicated Streamlit helper with minimal app wiring. Preserve view in assistant session history. Overview renders header, four metrics, modalities, suppliers, recent activity and accessible full titles, deduplicated sources and secondary scope/universe. Preserve warnings. Unknown/malformed version or focus uses canonical text. Escape dollar signs only at profile UI Markdown boundaries; do not change canonical channel text.

## Scope

V1 overview visual view. Focused replies keep textual rendering. V1 preserves existing next-step text; real drilldown buttons deferred to V1.1, requiring existing follow-up submission and published continuation references without client-side filters. No misleading inactive buttons.

No QueryRouter, Contract Universe policy, aggregates, rankings, identity, Frame default, canonical conversation persistence, query params or deployment changes. Session history reruns/JSON roundtrip are included; browser-refresh history restoration is not added.

## Acceptance and verification

- Four visible KPI slots; missing/invalid never becomes zero, confirmed zero stays zero.
- Same facts for equivalent OFF/ON input; count/value ranking accreditation independent.
- Executed sources/universe explicit; source_records labels remain registros.
- Long title excerpt with full title accessible; source links preserved without sentence-level repetition.
- Existing canonical text remains unchanged across other channels.
- JSON-safe history/rerun stability, unknown schema fallback, no stale view on unrelated turns.
- Paired currency regression at Streamlit boundary for overview and focused fallback.
- Meaningful Hypothesis properties for optional/invalid fact combinations.
- Ruff app/tests/scripts; Semgrep actual baseline vs owned-file delta; affected focal pytest only, not global suite; diff-check. Freeze code candidate for fresh read-only review.

## Stop conditions

Stop/replan if any metric/scope must be inferred from text/params, if OFF/ON require competing semantic projectors, if the payload must enter persisted Turn/envelope/continuation, if unavailable executed facts require recomputation, or if interactive controls require new routing authority. Rollback removes optional transport/UI projection while preserving text and persistence.

## Routing

| Step | Model | Reason | Result |
|---|---|---|---|
| Scout | Terra Medium | Bounded source-flow evidence at lower cost | Structured facts exist but are dropped before UI |
| Adjudication | Sol High | Task §9: coordinated response/workflow/Frame transport | B approved, high confidence, medium complexity |
| Implementation | Sol Medium | Fully specified bounded plan, one writer | Pending gates |
| Orchestration | Current Astra session | Scope, decisions and delivery verification | No model-setting change claimed |

Maximum concurrent agents including root: 3. No competing design reviewers needed: evidence established one bounded architecture. No Extra High escalation.

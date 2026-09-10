# Entity Profile Executive UX V1 — evidence

Base: `56f689df112bac60e9e384818db447af8969b921`.
Scout: gpt-5.6-terra / medium, read-only. Root verified history/Markdown path and tool availability.

## Current path

Frame OFF: execute_query → run_entity_profile_turn → build_entity_profile → render_entity_profile → analytical_response → format_response → response.
Frame ON: admission → run_profile_route → run_profile_turn → Frame/plan/evidence → render_frame_profile → response and envelope publication.

Accredited EntityProfile sections exist upstream. OFF loses the temporary profile through entity_profile_state; ON reduces ProfileTurnOutcome in profile_route._result. Neither final result exposes a KPI-safe UI view. `_assistant_history_item` retains response, rows, debug and continuation references, but no profile. Streamlit renders `item["content"]` via st.markdown.

## Dollar defect

`app/core/entity_profile/render.py:28` emits raw `$`; line 87 joins total and average into one sentence. `streamlit_app.py:1758` forwards content to Markdown. Installed Streamlit 1.56.0 MarkdownMixin.markdown documents `$` pairs as LaTeX delimiters. This establishes the causal source path; no running-browser visual reproduction was performed. Preserve currency signs; use UI components and UI-only escaping rather than changing shared channel text.

## Authorities and scope

- Independent accreditation for supplier count and value must survive projection.
- Missing or invalid source evidence cannot become zero.
- Labels derive from executed scope: source_records may mean registros, not contratos.
- No semantic parsing of rendered text; no metric calculation in Streamlit.
- Textual responses remain available to other channels.
- Frame default OFF, persistence and continuation authority remain separate from presentation.
- Interactive drilldowns require evidence of existing follow-up authority; do not synthesize query filters.

## Source pointers

- app/core/entity_profile/render.py:26–129,227–258
- app/core/entity_profile/frame_response.py:58–129
- app/core/entity_profile/frame_turn.py:221–250
- app/core/workflow/actions/entity_profile.py:17–122
- app/core/workflow/profile_route.py:38–85
- app/core/workflow/turn_coordination.py:100–160
- streamlit_app.py:445–475,1751–1812,1894–1905
- app/core/conversation_store.py:75–127,374–404
- docs/architecture/ENTITY_ANALYSIS_V1.md:185
- docs/decisions/2026-09-07_ADR-095_entity-profile-semantic-plan-v1.md:63

## Instrumentation preflight

Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10.
Codegraph 0.9.6 requires the available Node 22 PATH override; default Node 26.8.1 rejected. `codegraph status` reports index up to date: 1,064 files, 23,550 nodes, 52,974 edges. CLI availability is not MCP availability or test validation.

Read docs/orquestacion/CARRILES.md and root MULTI_AGENT_POLICY.md. The AGENTS-referenced docs/orquestacion/MULTI_AGENT_POLICY.md does not exist. Main has pre-existing modified AGENTS.md and untracked files; these were left untouched. No app start, Settings construction, network SECOP query, merge, push or deployment.

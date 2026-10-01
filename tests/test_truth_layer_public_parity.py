"""Exact public oracles captured from main before dormant M1 integration.

M1 froze these because its contract was ``VISIBLE_PRODUCT_CHANGE = NO``. M2
(PROCESS-OP-STATE-001) authorizes exactly ONE visible divergence — the process
state presentation — so three oracles move on purpose. Re-pinning them alone
would throw away the guard, so the divergence is pinned AND bounded: the tests
below prove that stripping the new fields restores the pre-M2 bytes exactly, and
that WHERE/ORDER/LIMIT, ranking, ordering, counts and pagination did not move.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from app.core.formatter import Formatter
from app.core.pagination_buffer import PaginationBuffer, pagination_fingerprint
from app.core.phrase_ranking import prepare_results_for_display
from app.core.response_policy import build_advisor_response
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.results import empty_result, tabular_result


# Contract Universe V1: eight explicit file repins are adjudicated individually
# in docs/audits/contract_universe_v1/repins.json. All process output oracles remain frozen.
# Files M2 is authorized to touch, re-pinned. Their divergence is not taken on
# trust: it is bounded by the structural tests at the end of this module.
M2_MODIFIED_FILE_SHA256 = {
    # HCA-R6 replaces the inlined `ordering_signal == "valor_desc"` conditional
    # with a call to order_semantics.ordering_label, so the two ascending
    # directions R6 adds are not announced as "de mayor valor"/"más recientes".
    # The label for every pre-R6 signal is unchanged and the oracles below stay
    # byte-identical, which bounds the change to the new signals.
    # RADAR-SECOP-ACCESS-V2 re-pins it for a COMMENT-ONLY edit: the docstring of
    # _safe_url attributed the CO1.REQ guard to "pre-publication", a cause the
    # 2026-08-31 census falsified (100% of p6dx-8zbt ids carry that prefix and
    # 98.97% do have a public view). The guard and every returned value are
    # unchanged — test_m2_card_divergence_is_purely_additive below is what
    # bounds that claim, and the Search oracles stay byte-identical.
    # SECOP-ACCESS-V2 channel parity re-pins it once more, this time for a real
    # text change: format_telegram, format_whatsapp and _grouped_card_lines stop
    # reading row["url"] themselves and delegate to app/core/secop_access.py via
    # one helper (_access_lines). Where there is no direct link they now offer
    # the official public search plus the observed reference, instead of a note
    # that named a lifecycle stage the data does not support (ADR-027's parity
    # principle is kept; what is signalled changed). Nothing else moves: the
    # helper adds no field to to_rows — test_m2_card_divergence_is_purely_
    # additive still holds — and touches no WHERE/ORDER/LIMIT, ranking, cursor
    # or fingerprint, which test_m2_does_not_move_ranking_ordering_counts_or_
    # pagination bounds. tests/test_url_fallback_channel_parity.py pins the new
    # copy and proves the three channels take the SAME decision per row.
    # Contract Universe V1.1 final closeout re-pins it for a boundary change,
    # not a behaviour one: _date_and_label asked whether a contractual date is
    # really a date by importing a private Truth Layer resolver, which
    # test_temporal_modules_have_no_runtime_consumers forbids. The question now
    # goes through app/core/published_temporal_value.py, whose answer was
    # measured identical to the previous one over civil dates, naive and
    # offset-bearing timestamps, free text, blanks and impossible calendars
    # (tests/test_published_temporal_value.py). Nothing else in the file moves:
    # to_rows gains no field, no WHERE/ORDER/LIMIT, ranking, cursor or
    # fingerprint is touched, and the import stays deferred so a normal
    # application import still leaves the layer dormant.
    # FOUNDATION-PRESENTATION re-pins it for a refactor, not a behaviour change:
    # the per-row card mapping of to_rows moves verbatim into _card so Foundation
    # can project rows it already published (display_rows: same order and count,
    # no dedupe, no cut, source rows untouched). to_rows keeps its dedupe, cut,
    # mojibake repair and single clock read; tests/test_formatter_display_rows.py
    # proves both paths emit identical cards for the same row.
    "app/core/formatter.py": "2dcbca5e0f773b36657b1defbee67c94ad4a136e509646b291d82ba0b479d170",
    # Money V2 / M7 adds a section gated strictly by valor_min/valor_max. The
    # no-money oracle below remains byte-identical and bounds that condition;
    # tests/test_money_presentation.py pins the authorized monetary divergence.
    # HCA-R5 replaces the interpretation's "desde <año>" collapse with a scope
    # formatter that preserves day/month/range/open granularity. It reads only
    # fecha_desde/fecha_hasta, so the dateless ordinary-query oracle below stays
    # byte-identical; tests/test_time_semantics_v2.py
    # (test_interpretation_preserves_temporal_granularity) pins the new phrasing.
    # HCA-R6: same delegation to order_semantics.ordering_label as formatter.
    "app/core/response_policy.py": "940ab9fb9c08a1fd31339570aa23028f6610088ec46aad2d689af78cc4438ba4",
    # O1B/O1B.1 add a predicate gated by opportunity metadata; the
    # ordinary-query oracle below remains byte-identical and bounds that
    # authorization, including the calendar-date boundary correction.
    # AP2-C adds two builders (territories, observed names) that nothing else
    # calls, plus one DatasetSpec field. AP2-E adds one WHERE predicate gated
    # strictly by params["actor_scope"], a key only the actor route emits. The
    # oracle below is unchanged, which is what bounds both claims: the ordinary
    # query path emits the same SoQL byte for byte.
    # SEARCH-SH1 adds dataset-explicit contractual predicates. The ordinary
    # oracle below remains byte-identical and therefore bounds the new paths to
    # queries that actually carry contract_type_include.
    # HCA-R4-S2 adds a null-safe predicate gated strictly by objeto_exclude.
    # The ordinary-query oracle below remains byte-identical; the six-valued
    # null specification lives in tests/test_object_negation_soql.py.
    # HCA-R6 adds two ORDER BY branches (valor_asc, fecha_asc) and makes the
    # id tie-break follow the requested direction. Both are gated strictly by
    # ordering_signal values the parser could not emit before R6; the ordinary
    # oracle below is byte-identical, which bounds the divergence to them.
    # SOURCE-NAME-PROJECTION-V1 (ADR-073) hace que el filtro de comprador
    # consulte un registro de nombres físicos por dataset y, cuando hay
    # binding, compare con igualdad exacta en vez de LIKE. La divergencia
    # está acotada por el sidecar, no por una condición de código: sólo
    # diverge una identidad con binding, y V1 liga únicamente a DAPRE, que
    # ninguna consulta ordinaria menciona. Por eso el oráculo de abajo sigue
    # byte-idéntico. tests/test_source_name_projection_v1.py prueba además
    # que ocho identidades SIN binding emiten el mismo SoQL byte por byte,
    # que entidad_like nunca se proyecta y que los guards de
    # entidad_ecosystem e entidad_in_candidates no se movieron.
    # BUYER-CANDIDATE-FAIL-CLOSED repinea el builder para una divergencia
    # estrictamente acotada a entidad_in_candidates con más de 15 nombres: en
    # vez de sustituir el grupo por candidates[0], lanza un bloqueo tipado.
    # Los OR de 1..15 y las consultas ordinarias conservan bytes y semántica;
    # tests/test_buyer_candidate_execution_limit.py fija ambos bordes.
    # ADR-074 / D extiende el bloqueo >15 a entidad_ecosystem. Sólo cambia
    # el caso antes truncado: test_entity_candidates_no_silent_collapse lo
    # exige; el oráculo ordinario de WHERE/ORDER/LIMIT permanece intacto.
    # G: registered exact BuyerGroup projection and physical row tie-break.
    # M1A.1: exact municipal city+department and fail-closed ambiguity are
    # gated by municipal_territory; the ordinary public oracle stays unchanged.
    # M1A.2 rejects unresolved complete municipal buyer identities, even if
    # topic/date filters exist. The ordinary-query oracles below remain
    # byte-identical; test_compound_identity_guard pins the new rejection.
    # VALUE-SANITY-REACHABILITY-V1 restores ADR-005 by adding valor_facturado
    # to the contracts SELECT only. test_value_sanity_reachability freezes the
    # previous 15 fields and proves that removing this one column restores the
    # query byte for byte, with identical count, WHERE, ORDER BY and LIMIT.
    # The process/public oracles below and all other pins remain unchanged.
    # Foundation V3 adds optional traced clauses, exclusive money/time bounds,
    # exact Foundation topic predicates and a validated explicit SELECT catalog.
    # O1B's real predicates satisfy the nonempty guard only under Foundation's
    # explicit ``allow_opportunity_only`` opt-in (ADR-102); every other caller
    # keeps main's guard (tests/test_foundation_runtime.py pins both sides).
    # The default legacy builder path is bounded by the unchanged public SoQL,
    # count/ranking/pagination oracles below; Foundation guards and projection
    # controls independently cover the opt-in execution surface.
    "app/core/soql_builder.py": "c76209e1843b77627f0097911ab79b194bfbe76be588b1cbf0c8abc38e88aa08",
}

# M2.1 corrigió dos consumidores visibles. AP3-G autoriza además controles de
# candidato dentro del consumidor Streamlit; el nuevo pin se acota con
# tests/test_actor_candidate_ux.py y no modifica el consumidor direct_lookup.
# EXPORT-XLSX-001 V1 re-pinea el consumidor Streamlit una vez más: añade un
# download_button que serializa a XLSX exactamente las filas ya presentes en
# item["rows"] del turno renderizado. No consulta SECOP, no reordena, no toca
# cursor, buffer, fingerprint ni el bloque de paginación — se inserta antes de
# él. Lo acotan tests/test_export_xlsx.py (contenido y alcance del libro) y
# test_m2_does_not_move_ranking_ordering_counts_or_pagination más abajo, que
# sigue verde y es lo que sostiene la afirmación de no-movimiento.
# RADAR-R0 repinea el mismo consumidor para corregir únicamente verdad horaria,
# baseline, cobertura y evidencia del scheduler en la pestaña Radar. Las rutas
# de Search continúan acotadas por el oracle de paridad de este archivo.
# RADAR-R1 lo repinea una vez más para añadir el detalle de una oportunidad
# detectada: una vista que se abre desde el feed del Radar y que solo LEE el
# evento ya persistido (app/core/radar/detail.py). No consulta SECOP, no
# reordena, no toca cursor, buffer, fingerprint ni el bloque de paginación, y
# no construye URLs. Todo el diff vive dentro de la superficie "# Radar tab"
# salvo tres símbolos importados de app.core.radar.detail; los oracles de
# Search de este archivo y test_m2_does_not_move_ranking_ordering_counts_and_
# pagination siguen verdes, y tests/test_radar_detail.py acota lo que la vista
# puede afirmar.
# RADAR-R1.1 lo repinea por dos ediciones de copy dentro de esa misma vista:
# la fecha de recepción de ofertas deja de mostrarse con hora (fecha_de_
# recepcion_de es un calendar_date, auditoría O1B.1) y se retira un aviso de
# roadmap. Ninguna toca lógica; el diff son dos hunks contiguos dentro de
# "# Radar tab".
# RADAR-R2 lo repinea para que Guardar tenga consecuencia: la vista de detalle
# de R1 gana una acción que ESCRIBE en ``workspace_items`` —la autoridad
# persistente que ya existía, sin tabla paralela ni cambio de schema— y la app
# gana una tercera pestaña ("Mi escritorio") que LEE esa misma tabla. Ninguna
# de las dos consulta SECOP: guardar copia el snapshot que el radar ya observó,
# y el escritorio se pinta con ese snapshot. No se toca cursor, buffer,
# fingerprint, ranking ni el bloque de paginación, y no se construyen URLs. El
# diff vive en la superficie Radar, en un bloque nuevo "# Mi escritorio tab",
# en cuatro helpers de identidad/aviso y en la línea de ``st.tabs``. Lo acotan
# tests/test_workspace_store.py, tests/test_radar_workspace.py y los casos R2 de
# tests/test_radar_ui_truth.py; los oracles de Search de este archivo y
# test_m2_does_not_move_ranking_ordering_counts_or_pagination siguen verdes.
# RADAR-R5A lo repinea para proyectar el feed raw como Inbox, registrar ``viewed``
# al abrir el detalle y ofrecer Descartar/No es relevante/Restaurar sobre la
# autoridad append-only de receipts. El cambio sigue acotado a la superficie
# Radar: no toca Search, ranking, cursor de paginación ni sus fingerprints. Lo
# protegen tests/test_radar_receipts.py, tests/test_radar_inbox.py y los casos
# R5A de tests/test_radar_ui_truth.py.
# BETA-P0-ISOLATION lo repinea por la frontera de sesión: el logout vacía todo el
# session_state en vez de cinco claves, el ``chat_id`` pasa a ser identidad de
# sesión (``st_<uuid>``) en vez de derivarse del email, y las rutas Radar de la
# web leen y mutan con dueño (``*_for_owner``). El diff vive en tres helpers de
# sesión, en el botón de cerrar sesión y en las cinco llamadas al RadarStore de
# la superficie Radar: no toca Search, ranking, cursor de paginación ni sus
# fingerprints —los oráculos de Search de este archivo siguen byte-idénticos—.
# Lo acotan tests/test_auth.py (frontera de sesión), tests/test_web_session.py,
# tests/test_radar_detail.py (dueño en el enrutado) y tests/test_radar_store.py.
M2_1_MODIFIED_FILE_SHA256 = {
    "app/core/workflow/handlers/direct_lookup.py": "fe552becbb2f8c8daeda58398f411e0018f5912a8c85b5f3a4ddfd0371bdd982",
    # 078fb1b was a documentation-only microfix after the isolation hotfix.
    # RADAR-SECOP-ACCESS-V2 lo repinea por la vía a SECOP. Dos cambios y ambos
    # de presentación: (a) la app deja de INFERIR etapa de la ausencia de enlace
    # ("proceso pre-publicación"), que ADR-032 #4 ya prohibía y el censo del
    # 2026-08-31 falsificó; (b) donde no hay enlace directo se ofrece el
    # buscador público oficial con la referencia observada, en vez de un hueco.
    # La decisión vive en una autoridad pura nueva (app/core/secop_access.py) y
    # la superficie solo la consume: no construye URLs, no consulta SECOP, no
    # toca Search, ranking, cursor de paginación ni sus fingerprints —los
    # oráculos de Search de este archivo siguen byte-idénticos—. Lo acotan
    # tests/test_secop_access.py, tests/test_radar_detail.py y los casos de
    # superficie de tests/test_radar_ui_truth.py.
    # Frame B/B.1 (20aea784, ff2a5844): transport result/revision/chat refs.
    # test_truth_layer_frame_guard_controls exercises actual UI helpers/chips
    # and valid/invalid reloads; Search/M2.1 oracles below remain unchanged.
    # Executive UX V1.1 re-pins this consumer for Frame-ON profile actions;
    # the executive UX boundary and profile-action UI boundary tests bound it.
    # Natural Alpha A0-A1 (70cab9c) adds only its projection import/call,
    # optional history metadata and render import/call. The inverse-byte guard
    # in test_entity_profile_executive_ux_boundary removes those exact blocks
    # and restores the complete pre-Alpha 84ce0fbc file before the older guard.
    # test_natural_alpha_presentation bounds eligibility and host-only display;
    # test_natural_alpha_runtime keeps OFF/legacy execution parity explicit.
    # Alpha R3 seals only the current authenticated account identity. The same
    # inverse-byte guard removes the exact issuer seam and restores 84ce0fbc;
    # test_natural_alpha_identity proves /chat spoofing and stale accounts fail.
    # DOGFOOD-001 C lo repinea por HONESTIDAD DE LA TRAZA, no por función: el
    # encabezado decía "LLM: No" en un turno que Natural Alpha decidió, y no
    # decía en qué estado terminó cada job del perfil, de modo que un 2/2
    # timeout y un universo vacío dejaban la misma huella en pantalla. El diff
    # son dos proyecciones de solo lectura sobre campos que el result YA lleva
    # (`natural_alpha_status`, `profile_job_receipts`) y su render dentro del
    # expander de trace: no toca Search, ranking, cursor de paginación ni sus
    # fingerprints —los oráculos de Search de este archivo siguen
    # byte-idénticos—. Lo acota el guard inverso de
    # tests/test_entity_profile_executive_ux_boundary.py, que retira esas
    # adiciones exactas y restaura 84ce0fbc, y
    # tests/test_dogfood_001_execution_budget.py.
    # FOUNDATION-PRESENTATION lo repinea para que un turno Foundation exporte bajo
    # su sello de universo (no trae params legacy), bloquee con aviso explícito
    # el Excel de un turno dual y no ofrezca "Ver más", una paginación legacy que
    # Foundation no puede continuar. Sólo adiciones de presentación; no toca
    # Search, ranking, cursor ni fingerprints. Las retira el guard inverso de
    # tests/test_entity_profile_executive_ux_boundary.py.
    # FOUNDATION-TRACE lo repinea otra vez para proyectar, dentro del expander de
    # trace, los campos Foundation que el resultado ya lleva (terminal, productor,
    # requests/conditions, resoluciones, H7, grupos, SoQL, recibos, fallback).
    # Lectura pura; la respuesta pública no cambia.
    "streamlit_app.py": "f65bebef80490de28d8dcdc1a9a80fafe0011d172cb837c64f4638bc2092ca3e",
}

# HCA-R6 re-pins ONE previously byte-frozen file. The pagination header carried
# a third copy of the ordering label; leaving it behind would have announced
# "más recientes" over an oldest-first page — a false claim about the rows on
# screen. The edit is the same delegation applied to the other two consumers and
# touches no cursor, buffer or fingerprint logic; test_m2_does_not_move_ranking_
# ordering_counts_or_pagination below is what bounds that claim.
CONTROL_LANGUAGE_REPINNED_SHA256 = {
    # Frame B.1 F6 (ff2a5844): consume the admitted buffer and its params.
    # test_truth_layer_frame_guard_controls reuses legacy order/universe
    # controls with Frame ON; test_pagination_buffer covers legacy OFF.
    #
    # M1 wave 1 R4 re-pins it (declared in R5, ZEUS4 gate): one early return at the top of
    # ``handle_pagination`` — a «ver más» after a turn that a TEMPORAL error left unexecuted
    # (period question, invalid date) is refused (``route_reason="pagination_blocked_turn"``, 0
    # SECOP calls, the refusal is not persisted) instead of re-executing the blocked scope without its
    # period. The decision lives in ``app/core/pagination_blocked.py`` (reads the persisted marker of
    # ``temporal_period_question``; R5 narrowed it to the marker only, so a non-temporal clarification
    # paginates as in the base). No cursor, buffer, fingerprint, ranking or ordering logic changed: bounded by
    # tests/test_m1_wave1_r4_pagination_blocked.py (refusal, executed search still pages, the cases
    # that must NOT be refused) and by this module's oracles, which stay byte-identical.
    "app/core/workflow/handlers/pagination.py": "3fc9512d1b8cb248efe21857d2ef298e42d07b54e3cbe0a1f4a9641e9517de72",
}

# Still byte-frozen: M2/SEARCH-SH1 touch neither ranking nor the surrounding
# public result contracts.
#
# ENTITY-PROFILE-SEMANTIC-V1.1 re-pins dual_dataset.py for the first time since
# pre-M1. Its independent review proved that `build_dataset_groups` executed
# SourceCalls and REPLACED the response after Entity Profile had already run its
# own plan — a second executor with no owner. The file gains exactly one
# early return, gated on `owns_execution_and_presentation(result, state)`, which
# is true only when `analytical_intent == "entity_profile"`.
#
# The divergence is not taken on trust. It is bounded by
# test_entity_profile_ownership_gate_is_inert_for_tabular_turns below, which
# proves the gate answers False for every non-profile intent and that an
# ordinary dual query still expands; the oracles in this module stay
# byte-identical because none of them is a profile turn.
#
# PROCESS_STATE_SINGLE_REQUEST_V1 re-pins it for one more early return: when
# the turn needs clarification (a process state it cannot represent, raised
# after planning), the secondary child does not run. It used to execute the
# half the primary refused. Bounded by
# tests/test_process_state_request_integrity_v1.py::test_dual_child_does_not_
# run_after_a_clarification and test_dual_multi_state_reaches_neither_child;
# test_entity_profile_ownership_gate_is_inert_for_tabular_turns still proves
# an ordinary dual query expands, and every oracle here stays byte-identical.
BASELINE_FILE_SHA256 = {
    "app/core/pagination_buffer.py": "802e763ace630368eada377319a715d3ca1bcdca00cb90dd4187236988e2f25c",
    "app/core/phrase_ranking.py": "0a99c8f4ebc10dd71556f1796a40c9192404475831a424964871954bab598c5a",
    # Foundation native receipts annotate the existing child query/count call
    # with dual_child:<dataset>. This adds correlation metadata; predicates,
    # child outputs and ordinary dual expansion remain pinned below.
    "app/core/workflow/dual_dataset.py": "fbfd211e0829cc78e7ec77c3866edc84dacb6c01be45712d6f389778bf7ff25f",
    "app/core/workflow/handlers/refinement.py": "fe1a08d26550d5a39fb4fdddbcb40fd623b89e7ab09c634d0bba7a95db00e076",
    # Executive UX V1: optional presentation transport/UI hook only.
    # test_entity_profile_executive_ux_boundary restores the prior exact bytes.
    "app/core/workflow/results.py": "5adc74aac2cf9da68597b67b88520733f273407bf291383b2dc97787b5683cfe",
    "app/main.py": "bad1f025e142301e036fd40d6dbde78077719850bfda45e45ff1ff0bf4dda282",
}

# SEARCH-SH1 intentionally invalidates cached result universes at the normal
# semantic-turn boundary. HCA-R4-S3 preserves that behavior except for a
# fail-closed unsupported negation, whose confirmed frame and buffer are
# transactional. tests/test_pagination_buffer_lifecycle.py and
# tests/test_safe_negation_policies.py bound the authorized handler changes.
# HCA-R5 adds exactly one keyword argument (today=ctx.today) to the existing
# app.run() invocation, so the workflow reads the injected civil clock instead
# of Date.today(). No predicate, projection or ordering moves: the oracles below
# stay byte-identical, and tests/test_time_semantics_v2.py pins the determinism
# this argument buys.
# HCA-FRESH widens exactly one argument of classify_outcome: a clarification
# decided BEFORE planning (lost context, or an explicit context reset) leaves no
# query_plan, so it was invisible here and the turn got classified as "zero after
# refinement" — whose recovery message then told the user a filter had emptied
# the search, the opposite of what happened, and overwrote the question with it.
# No predicate, projection or ordering moves; the oracles below stay
# byte-identical and tests/test_linguistic_family_coverage.py pins the reset
# behaviour this argument protects.
# ENTITY-PROFILE-SEMANTIC-V1.1 adds the upstream half of the same guard: the
# dual expansion and the tabular pagination bootstrap are skipped when the turn
# already has a terminal owner. Seeding a tabular buffer for a profile turn made
# a later "más" paginate rows its plan never showed. No predicate, projection,
# ranking or ordering moves for tabular turns — the same bounding test applies,
# and the 461 dual/grouped/pagination/normal_flow tests stay green unchanged.
SEARCH_SH1_MODIFIED_FILE_SHA256 = {
    # ADR-074 / D retira la inyección geográfica posterior al fetch. El nuevo
    # oráculo instrumentado test_buyer_execution_truth compara ejecución y
    # contexto asesor; pagination_buffer exige ausencia de la inyección en
    # ambos turnos. No cambian los oráculos de ranking/conteo/paginación.
    # Executive UX V1 optional view transport; inverse-byte guard in
    # test_entity_profile_executive_ux_boundary preserves the old contract.
    # ANALYTIC_CONTROL_UPSTREAM_V1 prepends ``analytic_limitation`` — empty
    # unless the turn carried a count/group instruction nobody executed — so
    # every oracle below stays byte-identical; the same inverse-byte guard
    # strips exactly that block.
    "app/core/workflow/handlers/normal_flow.py": (
        "b8affdff94dc19fee03f3209d1604a37239a0ed0b2d512749ec4fd303b15b4fd"
    ),
}

BASELINE_ORACLE_SHA256 = {
    # Moved by M2 — SELECT projection only; WHERE/ORDER/LIMIT asserted unchanged.
    "soql": "7f02ccbe3544cc7cc07e0acdecd5e967baa379a2f3cfb3f01e7e923787d6a94d",
    "ranked": "132fc4c6a549c9150ced9adf715e5b6afe8760bda4728e3dc54da86eeea36a76",
    "response": "55fcb0abf29f2abf7f59acdc8185fa7cf133041ed3465ed93887b6c5b4a0cf21",
    # Moved by M2 — additive process-state fields only.
    "cards": "61c8c20daa142efab8463e17fa7ec2423a7c5f1744f7ecf73883b8639d350b56",
    "empty": "c9cc48f43781f66cc7753f02e31ddc3a5f97d9d8959a68b84062bbbea367fde2",
    # Moved by M2 — carries `cards` inside.
    "tabular": "2c274be3ca019a18374419981d5f99130c463fe389dd546b3434f53d6e86468e",
    "buffer": "24b3d80a010d8d46e145ed74206d43343566d670830110e19ea61530ff805f00",
    "page": "dd5a788979d1ec2ec6d550390326887ed71982d3d11c7219e7d8b1e54865ead8",
    # M2 cambia únicamente el fragmento de estado dentro de la respuesta asesora.
    "advisor_response": "4208f28a762febd7efa59827242e22b5b5dba40e1a4668b9a1e2dbd46fe667f1",
}

# Pre-M2 oracles. Restoring them by removing the new fields is the proof that
# M2 added and did not rewrite.
PRE_M2_ORACLE_SHA256 = {
    "soql": "1df7633eeede2783ac4d0321fdb26f5c3bac948d6366840e149ef81912d31a60",
    "cards": "e6864547ebf7717e98ecf6f361586abbe5b0a5bd78b4f29da972342862f854e3",
    "tabular": "6faf58c7f8241804978114e5628093d408479403fe205df4136ce692c013388a",
    "advisor_response": "4f51370fbf0498e77be464976001fda1dc462a18b9680930e1847c19d14da3cc",
}

# The only field M2 adds to the process SELECT, and the only keys it adds to a
# process card. Any other addition must fail the tests below.
M2_ADDED_SELECT_FIELD = "estado_resumen"
M2_ADDED_CARD_KEYS = frozenset({
    "estado_op", "estado_op_label", "estado_op_icono",
    "estado_op_raw", "estado_op_mostrar_raw",
})


def _strip_m2_card_fields(cards: list[dict]) -> list[dict]:
    return [{k: v for k, v in card.items() if k not in M2_ADDED_CARD_KEYS}
            for card in cards]


def _advisor_context(cards: list[dict]) -> dict:
    return {
        "user_query": "aseo en Antioquia",
        "resolved_params": {
            "dataset": "procesos", "objeto": ["aseo"],
            "departamento_resolved": "ANTIOQUIA",
        },
        "total_count": 3,
        "total_count_approximate": False,
        "rows": cards,
        "universe_insights": None,
        "suggestions": [
            {"label": "Filtrar por entidad"},
            {"label": "Ordenar por mayor valor"},
        ],
        "dataset_id": "p6dx-8zbt",
        "channel": "telegram",
        "needs_clarification": False,
        "clarification_reason": "",
        "degraded": False,
        "degraded_hint": "",
        "query_error": "",
        "timeout_suggestions": [],
        "result_order_guarantee": "exact_order",
    }


def _advisor_non_state_structure(response: str) -> list[str]:
    """Normaliza exclusivamente label/icon y nota raw del estado de procesos."""
    normalized: list[str] = []
    for line in response.splitlines():
        if line.startswith("   SECOP reporta apertura:"):
            continue
        if line.startswith("   $") and line.count(" | ") == 2:
            value, _state, date = line.split(" | ")
            line = f"{value} | <PROCESS_STATE_PRESENTATION> | {date}"
        normalized.append(line)
    return normalized


def _json_hash(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _oracle_values() -> dict[str, object]:
    dataset_id = "p6dx-8zbt"
    rows = [
        {
            "id_del_proceso": "P-2",
            "referencia_del_proceso": "R-2",
            "nombre_del_procedimiento": "Aseo 2",
            "entidad": "ALCALDIA TEST",
            "precio_base": "2000000",
            "estado_de_apertura_del_proceso": "Publicado",
            "fecha_de_publicacion_del": "2026-08-02T00:00:00.000",
            "urlproceso": "https://example.test/p2",
        },
        {
            "id_del_proceso": "P-1",
            "referencia_del_proceso": "R-1",
            "nombre_del_procedimiento": "Aseo 1",
            "entidad": "ALCALDIA TEST",
            "precio_base": "1000000",
            "estado_de_apertura_del_proceso": "Publicado",
            "fecha_de_publicacion_del": "2026-08-01T00:00:00.000",
            "urlproceso": "https://example.test/p1",
        },
        {
            "id_del_proceso": "P-1",
            "referencia_del_proceso": "R-1",
            "nombre_del_procedimiento": "Aseo duplicado",
            "entidad": "ALCALDIA TEST",
            "precio_base": "999999",
            "estado_de_apertura_del_proceso": "Publicado",
            "fecha_de_publicacion_del": "2026-07-31T00:00:00.000",
            "urlproceso": "https://example.test/p1b",
        },
    ]
    params = {
        "dataset": "procesos",
        "objeto": ["aseo"],
        "departamento": "Antioquia",
        "limit": 50,
        "offset": 0,
    }
    ranked = prepare_results_for_display(rows, dataset_id, params)
    response, cards = Formatter(max_results=25).format_for_channel(
        ranked,
        dataset_id,
        "streamlit",
        params=params,
        total_count=3,
    )
    fingerprint = pagination_fingerprint(params)
    buffer = PaginationBuffer.create(
        query_fingerprint=fingerprint,
        group="procesos",
        dataset_id=dataset_id,
        query_params=params,
        candidate_buffer=rows,
        validated_ranked_buffer=ranked,
        display_cursor=0,
        source_cursor=50,
        source_candidate_total=3,
        now=datetime(2026, 8, 13, tzinfo=timezone.utc),
    )
    page, advanced = buffer.take(1)
    empty = empty_result(response="Sin resultados", route_reason="oracle")
    tabular = tabular_result(
        response="R",
        rows=cards,
        results=ranked,
        dataset_id=dataset_id,
        soql_query="Q",
        parsed_params=params,
        resolved_params=params,
        route_reason="oracle",
        total_count=3,
    )
    assert fingerprint == "3e69c27c8cf7914b54161c47dafd30d402c4397210f5f7afeda946f99e2fe62f"
    assert advanced.displayed_so_far == 1
    advisor_response = build_advisor_response(_advisor_context(cards))
    return {
        "soql": SoQLBuilder().build(dataset_id, params),
        "ranked": ranked,
        "response": response,
        "cards": cards,
        "empty": empty,
        "tabular": tabular,
        "buffer": buffer.to_dict(),
        "page": page,
        "advisor_response": advisor_response,
    }


def test_control_language_repinned_files_match_their_pinned_state():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in CONTROL_LANGUAGE_REPINNED_SHA256
    }
    assert actual == CONTROL_LANGUAGE_REPINNED_SHA256


def test_public_product_files_are_byte_identical_to_pre_m1_main():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in BASELINE_FILE_SHA256
    }
    assert actual == BASELINE_FILE_SHA256


def test_m2_modified_files_match_their_pinned_state():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in M2_MODIFIED_FILE_SHA256
    }
    assert actual == M2_MODIFIED_FILE_SHA256


def test_m2_1_modified_consumers_match_their_pinned_state():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in M2_1_MODIFIED_FILE_SHA256
    }
    assert actual == M2_1_MODIFIED_FILE_SHA256


def test_search_sh1_modified_handler_matches_its_pinned_state():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in SEARCH_SH1_MODIFIED_FILE_SHA256
    }
    assert actual == SEARCH_SH1_MODIFIED_FILE_SHA256


def test_m2_response_policy_is_covered_by_parity_guard():
    assert "app/core/response_policy.py" in M2_MODIFIED_FILE_SHA256


def test_public_keys_values_types_ordering_and_serialization_match_baseline():
    values = _oracle_values()
    actual = {
        name: (
            hashlib.sha256(value.encode()).hexdigest()
            if isinstance(value, str)
            else _json_hash(value)
        )
        for name, value in values.items()
    }
    assert actual == BASELINE_ORACLE_SHA256


def test_m2_soql_divergence_is_confined_to_the_select_projection():
    """WHERE, ORDER BY y LIMIT deben ser byte-idénticos al oráculo pre-M2."""
    soql = _oracle_values()["soql"]
    select, where = soql.split(" WHERE ", 1)
    fields = select[len("SELECT "):].split(", ")
    assert M2_ADDED_SELECT_FIELD in fields
    # Reconstruir el SELECT pre-M2 quitando SOLO el campo añadido devuelve el
    # oráculo original: ni un campo más, ni un reordenamiento.
    pre = "SELECT " + ", ".join(f for f in fields if f != M2_ADDED_SELECT_FIELD)
    restored = f"{pre} WHERE {where}"
    assert hashlib.sha256(restored.encode()).hexdigest() == PRE_M2_ORACLE_SHA256["soql"]


def test_m2_card_divergence_is_purely_additive():
    """Quitar los campos nuevos restaura las tarjetas pre-M2 byte a byte."""
    values = _oracle_values()
    cards = values["cards"]
    added = set().union(*(set(c) & M2_ADDED_CARD_KEYS for c in cards))
    assert added == M2_ADDED_CARD_KEYS
    stripped = _strip_m2_card_fields(cards)
    assert _json_hash(stripped) == PRE_M2_ORACLE_SHA256["cards"]
    # `estado` conserva su significado crudo: el radar y la persistencia de
    # membresías lo consumen y M2 no cambia dato persistido.
    assert [c["estado"] for c in cards] == ["Publicado", "Publicado"]

    tabular = dict(values["tabular"])
    tabular["rows"] = _strip_m2_card_fields(tabular["rows"])
    assert _json_hash(tabular) == PRE_M2_ORACLE_SHA256["tabular"]


def test_m2_response_policy_divergence_is_only_process_state_presentation():
    cards = _oracle_values()["cards"]
    current = build_advisor_response(_advisor_context(cards))
    restored = build_advisor_response(
        _advisor_context(_strip_m2_card_fields(cards))
    )

    assert hashlib.sha256(current.encode()).hexdigest() == BASELINE_ORACLE_SHA256["advisor_response"]
    assert hashlib.sha256(restored.encode()).hexdigest() == PRE_M2_ORACLE_SHA256["advisor_response"]
    assert _advisor_non_state_structure(current) == _advisor_non_state_structure(restored)

    # Los hechos públicos no relacionados siguen presentes y exactos.
    for stable in (
        "Procesos de aseo en ANTIOQUIA",
        "Encontre 3 resultados. Te muestro los 2 más recientes; hay 1 mas.",
        "1. Filtrar por entidad",
        "2. Ordenar por mayor valor",
    ):
        assert stable in current


def test_m2_does_not_move_ranking_ordering_counts_or_pagination():
    """Los oráculos de recuperación y paginación no pueden moverse en M2."""
    values = _oracle_values()
    for name in ("ranked", "response", "empty", "buffer", "page"):
        value = values[name]
        actual = (hashlib.sha256(value.encode()).hexdigest()
                  if isinstance(value, str) else _json_hash(value))
        assert actual == BASELINE_ORACLE_SHA256[name], f"{name} moved"


def test_normal_application_import_does_not_activate_truth_layer():
    code = "import sys; import app.main; print(any(n.startswith('app.core.truth_layer') for n in sys.modules))"
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "False"


def test_entity_profile_ownership_gate_is_inert_for_tabular_turns():
    """Acota la divergencia de V1.1 en dual_dataset.py y normal_flow.py.

    Los dos archivos ganan una sola condición: ``owns_execution_and_presentation``.
    Que sea inerte para todo lo que no sea un turno de perfil es lo que permite
    re-pinearlos sin tirar el guard — el mismo criterio que M2 y SEARCH-SH1
    aplicaron a los suyos.
    """
    from app.core.workflow.result_ownership import (
        ResultOwner,
        owns_execution_and_presentation,
        result_owner,
    )

    # Ningún intent tabular ni analítico reclama el turno.
    for intent in (
        "", None, "aggregate_sum", "actor_profile", "count", "ranking",
        "unknown_future_intent",
    ):
        for source in ({"analytical_intent": intent}, {}):
            assert owns_execution_and_presentation(source, {}) is False
            assert result_owner(source, {}) is ResultOwner.TABULAR

    # Y sólo el perfil lo reclama, en sus tres finales (respuesta, aclaración,
    # error honesto comparten el mismo analytical_intent).
    profile = {"analytical_intent": "entity_profile"}
    assert owns_execution_and_presentation(profile, {}) is True
    assert owns_execution_and_presentation({}, profile) is True
    assert result_owner(profile, {}) is ResultOwner.ENTITY_PROFILE


def test_dual_expansion_still_runs_for_an_ordinary_dual_query():
    """La otra mitad de la cota: la búsqueda normal conserva su expansión."""
    from app.core.workflow.dual_dataset import build_dataset_groups

    class _State(dict):
        pass

    result = {
        "resolved_params": {"dataset_scope": ["procesos", "contratos"]},
        "query_plan": {"should_execute": True},
    }
    state = _State({"dataset_id": "p6dx-8zbt", "resolved_params": result["resolved_params"]})

    class _Deps:
        class soql_builder:  # noqa: N801 - stub mínimo
            @staticmethod
            def build(*_args, **_kwargs):
                raise AssertionError("el hijo secundario SÍ se intentó ejecutar")

    # Sin dueño terminal la expansión llega hasta el hijo secundario: el stub
    # falla al construirlo, y `execute_dataset_child` lo captura como error.
    groups = build_dataset_groups(_Deps(), result, state)
    assert groups is not None
    assert set(groups) == {"procesos", "contratos"}

    # Con dueño terminal ni siquiera se intenta.
    owned = {**result, "analytical_intent": "entity_profile"}
    assert build_dataset_groups(_Deps(), owned, state) is None

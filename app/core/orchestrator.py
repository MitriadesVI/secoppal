from __future__ import annotations

from burr.core import ApplicationBuilder, State, action, default, expr

from app.config import Settings
from app.core.entity_resolver import EntityResolver
from app.core.feedback import FeedbackStore
from app.core.formatter import Formatter
from app.core.llm_handler import LLMHandler
from app.core.query_router import QueryRouter
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder


@action(reads=["user_query"], writes=["parsed_params", "needs_llm", "route_reason"])
def parse_query(state: State, query_router: QueryRouter) -> State:
    result = query_router.parse(state["user_query"])
    return state.update(
        parsed_params=result.params,
        needs_llm=result.needs_llm,
        route_reason=result.route_reason,
    )


@action(reads=["user_query", "needs_llm", "parsed_params"], writes=["parsed_params"])
def llm_parse(state: State, llm_handler: LLMHandler) -> State:
    if not state["needs_llm"]:
        return state
    parsed = llm_handler.parse(state["user_query"], existing_params=state["parsed_params"])
    return state.update(parsed_params=parsed)


@action(
    reads=["parsed_params"],
    writes=["resolved_params", "dataset_id", "needs_clarification", "clarification_reason"],
)
def resolve_entities(state: State, entity_resolver: EntityResolver) -> State:
    """
    Resolve entities to SECOP-compatible values.
    If gazetteer scan already resolved in parse_query, just pass through.
    Only does fuzzy/LIKE for entities the scan missed (e.g. from LLM).
    """
    params = dict(state["parsed_params"])
    resolved = dict(params)
    needs_clarification = False
    clarification_reason = ""

    # Department: only resolve if not already resolved by scan
    if params.get("departamento") and not params.get("departamento_resolved"):
        resolution = entity_resolver.resolve_departamento(str(params["departamento"]))
        resolved["departamento_resolution"] = resolution.to_dict()
        if resolution.value:
            resolved["departamento_resolved"] = resolution.value
        else:
            needs_clarification = True
            clarification_reason = f"No pude ubicar el departamento '{params['departamento']}'."

    # Entity: only resolve if not already resolved by scan
    if params.get("entidad") and not params.get("entidad_resolved"):
        depto_value = resolved.get("departamento_resolved")
        resolution = entity_resolver.resolve_entidad(str(params["entidad"]), departamento=depto_value)
        resolved["entidad_resolution"] = resolution.to_dict()
        if resolution.value:
            resolved["entidad_resolved"] = resolution.value
        elif resolution.like_value:
            resolved["entidad_like"] = resolution.like_value

    dataset_id = SoQLBuilder.dataset_id_for(str(params.get("dataset")))
    return state.update(
        resolved_params=resolved,
        dataset_id=dataset_id,
        needs_clarification=needs_clarification,
        clarification_reason=clarification_reason,
    )


@action(reads=["resolved_params", "dataset_id"], writes=["soql_query"])
def build_query(state: State, soql_builder: SoQLBuilder) -> State:
    soql = soql_builder.build(state["dataset_id"], state["resolved_params"])
    return state.update(soql_query=soql)


@action(reads=["dataset_id", "soql_query"], writes=["results", "query_error"])
def execute_query(state: State, secop_client: SecopClient) -> State:
    """Execute SECOP query with graceful error handling."""
    try:
        results = secop_client.query(state["dataset_id"], state["soql_query"])
        return state.update(results=results, query_error="")
    except Exception as exc:
        return state.update(results=[], query_error=str(exc))


@action(reads=["results", "dataset_id", "channel", "query_error"], writes=["formatted_response", "formatted_rows"])
def format_response(state: State, formatter: Formatter) -> State:
    if state.get("query_error"):
        return state.update(
            formatted_response="SECOP no respondio a tiempo. Intenta de nuevo en unos segundos.",
            formatted_rows=[],
        )
    response, rows = formatter.format_for_channel(state["results"], state["dataset_id"], state["channel"])
    return state.update(formatted_response=response, formatted_rows=rows)


@action(reads=["clarification_reason"], writes=["formatted_response", "formatted_rows"])
def clarify_query(state: State) -> State:
    return state.update(
        formatted_response=state["clarification_reason"] or "Necesito un poco mas de contexto para buscar en SECOP.",
        formatted_rows=[],
    )


class SecopalWorkflow:
    """High-level workflow facade around Burr."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.entity_resolver = EntityResolver(settings.secop_alias_db_path)
        self.query_router = QueryRouter(entity_resolver=self.entity_resolver)
        self.llm_handler = LLMHandler(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
        self.soql_builder = SoQLBuilder()
        self.secop_client = SecopClient(
            domain=settings.datos_gov_domain,
            app_token=settings.secop_app_token,
            timeout=settings.secop_timeout_seconds,
        )
        self.formatter = Formatter(max_results=min(settings.secop_results_limit, 10))
        self.feedback = FeedbackStore()

    def run_query(self, user_query: str, channel: str = "whatsapp") -> dict:
        app = (
            ApplicationBuilder()
            .with_actions(
                parse_query=parse_query.bind(query_router=self.query_router),
                llm_parse=llm_parse.bind(llm_handler=self.llm_handler),
                resolve_entities=resolve_entities.bind(entity_resolver=self.entity_resolver),
                build_query=build_query.bind(soql_builder=self.soql_builder),
                execute_query=execute_query.bind(secop_client=self.secop_client),
                format_response=format_response.bind(formatter=self.formatter),
                clarify_query=clarify_query,
            )
            .with_transitions(
                ("parse_query", "llm_parse", expr("needs_llm")),
                ("parse_query", "resolve_entities", expr("not needs_llm")),
                ("llm_parse", "resolve_entities", default),
                ("resolve_entities", "clarify_query", expr("needs_clarification")),
                ("resolve_entities", "build_query", expr("not needs_clarification")),
                ("build_query", "execute_query", default),
                ("execute_query", "format_response", default),
            )
            .with_state(
                user_query=user_query,
                channel=channel,
                parsed_params={},
                resolved_params={},
                results=[],
                formatted_rows=[],
                formatted_response="",
                soql_query="",
                dataset_id="",
                route_reason="",
                query_error="",
                needs_llm=False,
                needs_clarification=False,
                clarification_reason="",
            )
            .with_entrypoint("parse_query")
            .build()
        )

        *_, state = app.run(halt_after=["format_response", "clarify_query"])
        result = {
            "response": state["formatted_response"],
            "rows": state["formatted_rows"],
            "results": state["results"],
            "dataset_id": state["dataset_id"],
            "soql_query": state["soql_query"],
            "parsed_params": state["parsed_params"],
            "resolved_params": state["resolved_params"],
            "needs_llm": state["needs_llm"],
            "needs_clarification": state["needs_clarification"],
            "route_reason": state["route_reason"],
        }

        trace_id = self.feedback.log_trace(user_query, channel, result)
        result["trace_id"] = trace_id

        return result

    def rate_query(self, trace_id: str, rating: int, comment: str | None = None) -> bool:
        return self.feedback.rate(trace_id, rating, comment)

    def get_feedback_stats(self) -> dict:
        return self.feedback.get_stats()

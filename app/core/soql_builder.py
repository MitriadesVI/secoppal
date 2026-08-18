from __future__ import annotations

import logging
from dataclasses import dataclass

from app.core.modality_dataset_projection import (
    DatasetModalityProjection,
    project_modality_filter,
)
from app.core.modality_filter import ModalityFilter
from app.core.opportunity_retrieval_policy import (
    REFERENCE_INSTANT_PARAM,
    evaluate_current_opportunity_policy,
    format_socrata_calendar_date,
    parse_reference_instant,
)

logger = logging.getLogger(__name__)


class UnsafeGlobalQueryError(RuntimeError):
    """Raised when SoQLBuilder would emit a query with no WHERE predicate.

    This guard is intentionally broader than the execution guard: estado and
    modalidad are enough to avoid WHERE 1=1, but they are not enough to run a
    useful user search across SECOP. Execution authority stays with
    _followup_constants.SCOPE_TOPIC_KEYS.

    Callers that genuinely need a global query (admin/diagnostic tools)
    must opt in explicitly with ``allow_global=True``.
    """


# Filtros que hacen que el WHERE no sea tautologico. Esto NO responde
# "hay base util para ejecutar?" (autoridad: SCOPE_TOPIC_KEYS en execution).
# Estado/modalidad pueden producir millones de filas y aun requerir aclaracion
# aguas abajo, pero si evitan que el builder emita WHERE 1=1.
NONEMPTY_WHERE_KEYS: frozenset[str] = frozenset({
    # scope
    "departamento_resolved", "departamentos_in", "ciudad",
    "entidad_resolved", "entidad_like",
    "contratista",
    # topic
    "objeto", "objeto_or", "objeto_phrases",
    # estado
    "estado", "estado_de_apertura_del_proceso", "estado_del_procedimiento",
    "estado_contrato", "estado_contrato_adicional",
    # entity ecosystem (G7C)
    "entidad_contains_all", "entidad_in_candidates",
})


def _has_nonempty_where_filter(
    params: dict, *, modality_effective: bool = False,
) -> bool:
    if modality_effective:
        return True
    for key in NONEMPTY_WHERE_KEYS:
        value = params.get(key)
        if value in (None, "", [], {}, False):
            continue
        return True
    return False

try:
    from app.data.morphological_variants import ROOT_TO_VARIANTS, dedup_like_variants
except ImportError as exc:
    logger.warning(
        "Could not import ROOT_TO_VARIANTS; morphological expansion disabled: %s",
        exc,
    )
    ROOT_TO_VARIANTS: dict[str, frozenset[str]] = {}
    dedup_like_variants = lambda v: sorted(v, key=len)  # type: ignore

try:
    from app.core.intent_vocabulary import COMMON_TOKENS
except ImportError as exc:
    logger.warning("Could not import COMMON_TOKENS; object demotion disabled: %s", exc)
    COMMON_TOKENS: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    dataset_id: str
    entity: str
    object_name: str
    description: str | None
    department: str
    value: str
    state: str
    date: str
    url: str
    modality: str = "modalidad_de_contratacion"
    # Proyección de evento del radar (R2B-1B1). Declarados por dataset y no
    # derivados de `date`: en procesos `date` es la fecha de PUBLICACIÓN, que no
    # es ni cierre ni firma. Reusarla proyectaría publicación bajo la etiqueta
    # equivocada. None = el dataset no expone ese hecho.
    closing_date: str | None = None
    signature_date: str | None = None
    reference: str | None = None
    contractor: str | None = None
    contractor_document: str | None = None
    city: str | None = None
    select_fields: tuple[str, ...] = ()


class SoQLBuilder:
    PROCESOS_DATASET = "p6dx-8zbt"
    CONTRATOS_DATASET = "jbjy-vk9h"

    SPECS = {
        PROCESOS_DATASET: DatasetSpec(
            dataset_id=PROCESOS_DATASET,
            entity="entidad",
            object_name="nombre_del_procedimiento",
            description="descripci_n_del_procedimiento",
            department="departamento_entidad",
            value="precio_base",
            state="estado_de_apertura_del_proceso",
            date="fecha_de_publicacion_del",
            url="urlproceso",
            # Única columna de recepción de ofertas que el repo ya reconoce
            # (formatter._best_date_for_row, DOGFOOD-DATE-002). Auditoría previa
            # (opportunity_stage.py): ~99% NULL en el dataset, así que el radar
            # la proyecta como dato opcional, no como garantía de vigencia.
            closing_date="fecha_de_recepcion_de",
            reference="referencia_del_proceso",
            city="ciudad_entidad",
            select_fields=(
                "id_del_proceso",
                "referencia_del_proceso",
                "entidad",
                "departamento_entidad",
                "ciudad_entidad",
                "nombre_del_procedimiento",
                "descripci_n_del_procedimiento",
                "tipo_de_contrato",
                "modalidad_de_contratacion",
                "precio_base",
                "estado_de_apertura_del_proceso",
                "estado_del_procedimiento",
                "fase",
                # M2/PROCESS-OP-STATE-001: no arbitra la etapa (eso lo deciden
                # estado_del_procedimiento + fase), pero el Truth Layer lo tiene
                # catalogado con política de sentinelas. Omitirlo lo hacía llegar
                # como "not_observed", que NO penaliza la calidad: la lectura se
                # declaraba "complete" ignorando un campo con 100% de cobertura.
                # Se transporta para que la calidad observada sea la real.
                "estado_resumen",
                "fecha_de_publicacion_del",
                "fecha_de_recepcion_de",
                "urlproceso",
            ),
        ),
        CONTRATOS_DATASET: DatasetSpec(
            dataset_id=CONTRATOS_DATASET,
            entity="nombre_entidad",
            object_name="objeto_del_contrato",
            description="descripcion_del_proceso",
            department="departamento",
            value="valor_del_contrato",
            state="estado_contrato",
            date="fecha_de_firma",
            url="urlproceso",
            # Contratos no publican cierre de ofertas (el proceso ya terminó);
            # el hecho fechado equivalente es la firma. Coincide con `date`, pero
            # se declara aparte: son dos contratos distintos que hoy convergen.
            signature_date="fecha_de_firma",
            contractor="proveedor_adjudicado",
            contractor_document="documento_proveedor",
            reference="referencia_del_contrato",
            city="ciudad",
            select_fields=(
                "id_contrato",
                "referencia_del_contrato",
                "nombre_entidad",
                "departamento",
                "ciudad",
                "objeto_del_contrato",
                "descripcion_del_proceso",
                "tipo_de_contrato",
                "modalidad_de_contratacion",
                "valor_del_contrato",
                "estado_contrato",
                "fecha_de_firma",
                "proveedor_adjudicado",
                "documento_proveedor",
                "urlproceso",
            ),
        ),
    }

    @classmethod
    def dataset_id_for(cls, dataset_name: str | None) -> str:
        return cls.CONTRATOS_DATASET if dataset_name == "contratos" else cls.PROCESOS_DATASET

    def build(self, dataset_id: str, params: dict, *, allow_global: bool = False,
              include_order: bool = True) -> str:
        spec = self.SPECS[dataset_id]
        where_clause = self._build_where(spec, params, allow_global=allow_global)

        select_clause = ", ".join(spec.select_fields)

        # Default: date DESC (most recent first).
        # valor_desc: value DESC, date DESC
        # fecha_desc: date DESC, value DESC (same as default, explicit intent)
        ordering = params.get("ordering_signal", "")
        has_estado = bool(
            params.get("estado")
            or params.get("estado_family")
            or params.get("estado_de_apertura_del_proceso")
            or params.get("estado_del_procedimiento")
            or params.get("estado_contrato")
        )
        intent = params.get("intent_type", "")
        is_historical = "historical" in intent.lower() or "analysis" in intent.lower()

        # Default ordering: solo fecha DESC + valor DESC.
        # DOGFOOD-SOQL-006: el CASE WHEN ... IN (...) que usábamos para poner
        # "activos primero" rompe Socrata para jbjy-vk9h: el parser interno
        # transforma `CASE WHEN x IN ('En ejecución', ...) THEN ... END` a
        # `#CASE(x IN (...), 0, TRUE, 1)` y revienta con strings acentuados
        # (400 Bad Request "Unexpected character"). El boost "activos primero"
        # ahora se aplica en post-procesamiento (formatter._reorder_active_first).
        if (
            not has_estado
            and not is_historical
            and intent != "opportunity_search"
            and not ordering
        ):
            order_expr = f"{spec.date} DESC, {spec.value} DESC"
        elif params.get("intent_type") == "opportunity_search" and not ordering:
            order_expr = f"{spec.date} DESC, {spec.value} DESC"  # freshness + value
        elif ordering == "valor_desc":
            order_expr = f"{spec.value} DESC, {spec.date} DESC"
        else:
            order_expr = f"{spec.date} DESC, {spec.value} DESC"
        # Etapa 8A: desempate ESTABLE por identificador. Sin esto, filas con misma fecha
        # y mismo valor quedan en orden no determinista entre llamadas → páginas que se
        # solapan/saltan al cruzar la ventana SECOP. El id es único y la columna final
        # rompe todos los empates de forma reproducible entre procesos.
        id_field = "id_contrato" if dataset_id == self.CONTRATOS_DATASET else "id_del_proceso"
        order_expr = f"{order_expr}, {id_field} DESC"
        # Commit 5: con frase recall-first ampliamos el POOL de candidatos (no el
        # display, que recorta aguas abajo). Permite que el validador (Commit 6) vea
        # los positivos hoy perdidos sin que LIMIT los trunque.
        candidate_limit = 200 if (params.get("objeto_phrases")) else 50
        limit = int(params.get("limit", candidate_limit))
        offset = int(params.get("offset", 0))
        limit_clause = f"LIMIT {limit}"
        if offset:
            limit_clause += f" OFFSET {offset}"

        # H4: el fallback degradado retira el ORDER BY costoso (sort server-side sobre el
        # universo filtrado es el cuello). Solo permitido cuando el orden es presentacional;
        # el caller reordena la página localmente y marca result_order_guarantee.
        if not include_order:
            return f"SELECT {select_clause} WHERE {where_clause} {limit_clause}"

        return f"SELECT {select_clause} WHERE {where_clause} ORDER BY {order_expr} {limit_clause}"

    def build_count(self, dataset_id: str, params: dict, *, allow_global: bool = False) -> str:
        """Build a SELECT count(*) query with the same filters as build(), no LIMIT/ORDER."""
        spec = self.SPECS[dataset_id]
        where_clause = self._build_where(spec, params, allow_global=allow_global)
        return f"SELECT count(*) WHERE {where_clause}"

    def _build_where(self, spec: DatasetSpec, params: dict, *, allow_global: bool = False) -> str:
        """Build the WHERE clause string shared by build() and build_count().

        Raises UnsafeGlobalQueryError if the resulting clause would collapse to
        a tautology (``1=1``) and the caller has not opted in via
        ``allow_global=True``. Date/value/order alone create no WHERE predicate.
        """
        legacy_key = "modalidad"
        if (
            legacy_key in params
            and params.get(legacy_key) not in (None, "", [], {}, (), False)
        ):
            raise ValueError("legacy modality field 'modalidad' is not accepted")

        modality_filter = ModalityFilter.from_params(params)
        dataset_name = (
            "contratos"
            if spec.dataset_id == self.CONTRATOS_DATASET
            else "procesos"
        )
        modality_projection = project_modality_filter(
            modality_filter, dataset_name
        )
        modality_effective = bool(
            modality_projection.include_raw_values
            or modality_projection.include_unsatisfiable
            or modality_projection.exclude_raw_values
        )
        if not allow_global and not _has_nonempty_where_filter(
            params, modality_effective=modality_effective,
        ):
            raise UnsafeGlobalQueryError(
                "SoQL build refused: no WHERE predicate (scope/topic/estado/modalidad). "
                "Pass allow_global=True only if a global query is intentional."
            )
        where_clauses: list[str] = []
        opportunity_policy = evaluate_current_opportunity_policy(
            params,
            spec.dataset_id,
            reference_instant=parse_reference_instant(
                params.get(REFERENCE_INSTANT_PARAM)
            ),
        )

        if params.get("departamento_resolved"):
            where_clauses.append(f"{spec.department} = '{self._escape(params['departamento_resolved'])}'")
        elif params.get("departamentos_in"):
            # Region search: filter across every department of a region.
            # _add_estado_clause emits `field IN ('a','b',...)` for lists.
            self._add_estado_clause(where_clauses, spec.department, params["departamentos_in"])

        if params.get("ciudad") and spec.city:
            where_clauses.append(
                self._accent_fold_like(spec.city, self._escape(str(params['ciudad'])))
            )
        entidad_value = params.get("entidad_like") or params.get("entidad_resolved")
        entidad_ecosystem = params.get("entidad_ecosystem")
        if entidad_ecosystem and isinstance(entidad_ecosystem, list) and not entidad_value:
            # ENTITY-ECOSYSTEM-001: expand to OR across all ecosystem entities
            entity_likes = " OR ".join(
                self._accent_fold_like(spec.entity, self._escape(str(e)))
                for e in entidad_ecosystem[:15]  # max 15 to avoid query bloat
            )
            where_clauses.append(f"({entity_likes})")
        elif entidad_value:
            where_clauses.append(
                self._accent_fold_like(spec.entity, self._escape(str(entidad_value)))
            )

        # G7C: entity ecosystem contains_all — AND of LIKEs (e.g. SENA + MAGDALENA)
        entidad_contains_all = params.get("entidad_contains_all")
        if entidad_contains_all and isinstance(entidad_contains_all, list):
            for token in entidad_contains_all:
                where_clauses.append(
                    self._accent_fold_like(spec.entity, self._escape(str(token)))
                )

        # G7C: entity ecosystem in_candidates — OR of official name LIKEs
        entidad_in_candidates = params.get("entidad_in_candidates")
        if entidad_in_candidates and isinstance(entidad_in_candidates, list) and not entidad_value:
            if len(entidad_in_candidates) <= 15:
                entity_likes = " OR ".join(
                    self._accent_fold_like(spec.entity, self._escape(str(e)))
                    for e in entidad_in_candidates
                )
                where_clauses.append(f"({entity_likes})")
            else:
                # Too many — fallback to contains_all tokens if available, else first 15
                where_clauses.append(
                    self._accent_fold_like(spec.entity, self._escape(str(entidad_in_candidates[0])))
                )

        objeto_terms = params.get("objeto", []) or []
        if self._should_demote_objeto(objeto_terms):
            rare_terms, common_terms = self._split_objeto_by_rarity(objeto_terms)
            # With 3+ rare terms, AND between them is too strict
            # (e.g., "control de calidad de agua para consumo humano" → 5 terms).
            # Merge into a single OR group — BM25 re-ranks later for precision.
            if len(rare_terms) >= 3:
                all_conditions: list[str] = []
                for term in rare_terms:
                    all_conditions.extend(self._object_term_conditions(spec, term))
                where_clauses.append(f"({' OR '.join(all_conditions)})")
            else:
                for term in rare_terms:
                    where_clauses.append(
                        f"({' OR '.join(self._object_term_conditions(spec, term))})"
                    )
            if common_terms:
                common_conditions: list[str] = []
                for term in common_terms:
                    common_conditions.extend(self._object_term_conditions(spec, term))
                where_clauses.append(f"({' OR '.join(common_conditions)})")
        else:
            for term in objeto_terms:
                # str = required term (AND with other object entries)
                # list[str] = OR group created by parser for "X o Y"
                terms = term if isinstance(term, list) else [term]
                term_conditions: list[str] = []
                for sub_term in terms:
                    term_conditions.extend(
                        self._object_term_conditions(spec, str(sub_term))
                    )
                where_clauses.append(f"({' OR '.join(term_conditions)})")

        objeto_or = params.get("objeto_or") or []
        if objeto_or:
            term_conditions = []
            for term in objeto_or:
                term_conditions.extend(self._object_term_conditions(spec, str(term)))
            where_clauses.append(f"({' OR '.join(term_conditions)})")

        # Exclusiones temáticas son predicados negativos independientes. Cada
        # término se excluye tanto de objeto como de descripción, de forma
        # simétrica a las condiciones positivas.
        for term in params.get("objeto_exclude") or []:
            conditions = self._object_term_conditions(spec, str(term))
            if conditions:
                where_clauses.append(f"NOT ({' OR '.join(conditions)})")

        # Phrase recall-first (Commit 5): cada PhraseSpec en objeto_phrases aporta una
        # cláusula amplia = anclas co-ocurrentes en el MISMO campo, OR entre campos.
        # Subsume la frase exacta (substring) sin rama redundante. NO cruza objeto con
        # descripcion. La clasificación direct/probable y el ranking son Commit 6.
        from app.core.phrase_catalog import spec_for_canonical

        for canonical in params.get("objeto_phrases") or []:
            phrase_spec = spec_for_canonical(str(canonical))
            if phrase_spec:
                clause = self._phrase_recall_conditions(spec, phrase_spec)
                if clause:
                    where_clauses.append(clause)

        if params.get("valor_min") is not None:
            where_clauses.append(f"{spec.value} >= {int(params['valor_min'])}")
        if params.get("valor_max") is not None:
            where_clauses.append(f"{spec.value} <= {int(params['valor_max'])}")

        if params.get("estado"):
            # Use the specific field identified by QueryRouter, or fall back to spec default
            estado_field = params.get("estado_field", spec.state)
            estado_val = params["estado"]
            # Skip if a more specific estado field already covers this
            # (estado_family resolved fields take priority over legacy estado)
            if not any(
                params.get(f) for f in (
                    "estado_de_apertura_del_proceso",
                    "estado_del_procedimiento",
                    "estado_contrato",
                    "estado_contrato_adicional",
                )
            ):
                self._add_estado_clause(where_clauses, estado_field, estado_val)

        # NUEVO: estado_families filters — puede ser lista o simple
        for field in ("estado_de_apertura_del_proceso", "estado_del_procedimiento",
                      "estado_contrato", "estado_contrato_adicional"):
            if opportunity_policy.applies and field == "estado_del_procedimiento":
                continue  # O1B replaces only the automatically-derived bundle.
            if field in params and params[field]:
                self._add_estado_clause(where_clauses, field, params[field])

        if opportunity_policy.applies:
            self._add_estado_clause(
                where_clauses,
                "estado_del_procedimiento",
                list(opportunity_policy.allowed_states),
            )

        modality_clause = self._modality_clause(modality_projection)
        if modality_clause:
            where_clauses.append(modality_clause)

        # STATE-PRIORITY-001: oportunidades en procesos requieren apertura=Abierto
        if (
            params.get("intent_type") == "opportunity_search"
            and spec.dataset_id == self.PROCESOS_DATASET
            and not params.get("estado_de_apertura_del_proceso")
        ):
            where_clauses.append("estado_de_apertura_del_proceso = 'Abierto'")

        if opportunity_policy.applies:
            current_colombia_date = format_socrata_calendar_date(
                opportunity_policy.current_colombia_date  # type: ignore[arg-type]
            )
            publication_since = format_socrata_calendar_date(
                opportunity_policy.unknown_deadline_publication_since  # type: ignore[arg-type]
            )
            # Equivalent to the policy's OR, expressed as one Socrata case()
            # predicate. On p6dx-8zbt this avoids the severe planner penalty of
            # OR across two calendar-date columns while preserving NULL/future
            # semantics exactly.
            where_clauses.append(
                "case(fecha_de_recepcion_de IS NULL, "
                f"fecha_de_publicacion_del >= '{publication_since}', "
                f"true, fecha_de_recepcion_de >= '{current_colombia_date}')"
            )

        if params.get("fecha_desde"):
            where_clauses.append(f"{spec.date} >= '{self._escape(params['fecha_desde'])}'")
        if params.get("fecha_hasta"):
            where_clauses.append(f"{spec.date} <= '{self._escape(params['fecha_hasta'])}'")

        if params.get("contratista") and spec.contractor:
            contratista = params["contratista"]
            if isinstance(contratista, list):
                # NIT detectado por forma: lista de variantes → OR sobre documento_proveedor
                if spec.contractor_document:
                    variants = " OR ".join(
                        f"{spec.contractor_document} = '{self._escape(v)}'"
                        for v in contratista
                    )
                    where_clauses.append(f"({variants})")
                else:
                    # dataset sin documento_proveedor — LIKE sobre nombre
                    variants = " OR ".join(
                        f"UPPER({spec.contractor}) LIKE UPPER('%{self._escape(v)}%')"
                        for v in contratista
                    )
                    where_clauses.append(f"({variants})")
            else:
                contractor = str(contratista).strip()
                if contractor.isdigit() and spec.contractor_document:
                    where_clauses.append(f"{spec.contractor_document} = '{self._escape(contractor)}'")
                else:
                    where_clauses.append(
                        f"UPPER({spec.contractor}) LIKE UPPER('%{self._escape(contractor)}%')"
                    )

        return " AND ".join(where_clauses) if where_clauses else "1=1"

    def build_top_entities(self, dataset_id: str, params: dict, limit: int = 5, *, allow_global: bool = False) -> str:
        spec = self.SPECS[dataset_id]
        where = self._build_where(spec, params, allow_global=allow_global)
        return (
            f"SELECT {spec.entity}, count(*) AS cnt WHERE {where} "
            f"GROUP BY {spec.entity} ORDER BY cnt DESC LIMIT {limit}"
        )

    def build_value_stats(self, dataset_id: str, params: dict, *, allow_global: bool = False) -> str:
        spec = self.SPECS[dataset_id]
        where = self._build_where(spec, params, allow_global=allow_global)
        v = spec.value
        return (
            f"SELECT avg({v}) AS mean, min({v}) AS min_val, max({v}) AS max_val "
            f"WHERE {where} AND {v} IS NOT NULL"
        )

    def build_top_modalities(self, dataset_id: str, params: dict, limit: int = 3, *, allow_global: bool = False) -> str:
        spec = self.SPECS[dataset_id]
        where = self._build_where(spec, params, allow_global=allow_global)
        m = spec.modality
        return (
            f"SELECT {m}, count(*) AS cnt WHERE {where} "
            f"GROUP BY {m} ORDER BY cnt DESC LIMIT {limit}"
        )

    def build_date_range(self, dataset_id: str, params: dict, *, allow_global: bool = False) -> str:
        spec = self.SPECS[dataset_id]
        where = self._build_where(spec, params, allow_global=allow_global)
        d = spec.date
        return (
            f"SELECT min({d}) AS date_min, max({d}) AS date_max "
            f"WHERE {where} AND {d} IS NOT NULL"
        )

    def build_temporal_dist(self, dataset_id: str, params: dict, *, allow_global: bool = False) -> str:
        spec = self.SPECS[dataset_id]
        where = self._build_where(spec, params, allow_global=allow_global)
        d = spec.date
        return (
            f"SELECT date_trunc_y({d}) AS yr, count(*) AS cnt "
            f"WHERE {where} AND {d} IS NOT NULL "
            f"GROUP BY yr ORDER BY yr DESC"
        )

    @staticmethod
    def _should_demote_objeto(objeto: list) -> bool:
        return len(objeto) >= 4 and all(isinstance(term, str) for term in objeto)

    @staticmethod
    def _split_objeto_by_rarity(objeto: list[str]) -> tuple[list[str], list[str]]:
        rare_terms = [term for term in objeto if term.lower() not in COMMON_TOKENS]
        common_terms = [term for term in objeto if term.lower() in COMMON_TOKENS]
        return rare_terms, common_terms

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("'", "''")

    @classmethod
    def _modality_clause(
        cls, projection: DatasetModalityProjection,
    ) -> str:
        if projection.include_unsatisfiable:
            return "1=0"

        includes = projection.include_raw_values
        excludes = projection.exclude_raw_values
        include_clause = ""
        if len(includes) == 1:
            include_clause = (
                "modalidad_de_contratacion = "
                f"'{cls._escape(includes[0])}'"
            )
        elif includes:
            values = ", ".join(f"'{cls._escape(raw)}'" for raw in includes)
            include_clause = f"modalidad_de_contratacion IN ({values})"

        exclude_clause = ""
        if excludes:
            values = ", ".join(f"'{cls._escape(raw)}'" for raw in excludes)
            exclude_clause = f"modalidad_de_contratacion NOT IN ({values})"

        if include_clause and exclude_clause:
            return f"({include_clause}) AND ({exclude_clause})"
        return include_clause or exclude_clause

    @classmethod
    def _accent_fold_like(cls, field: str, escaped_pattern: str) -> str:
        field_expr = cls._accent_fold_expr(f"UPPER({field})")
        pattern_expr = cls._accent_fold_expr(f"UPPER('%{escaped_pattern}%')")
        return f"{field_expr} LIKE {pattern_expr}"

    def _object_term_conditions(self, spec: DatasetSpec, term: str) -> list[str]:
        # Expand to all morphological variants (OR clauses).
        # ROOT_TO_VARIANTS maps root to all surface forms; unknown terms are used as-is.
        # BOTH routing paths (heuristic_only and heuristic_plus_llm) converge here —
        # so morphological expansion is always applied. Confirmed by test_morpho_expansion_consistency.py.
        # MORPHO-EXPANSION-INCONSISTENT-001 was a false alarm.
        term_variants = ROOT_TO_VARIANTS.get(term, frozenset([term]))
        # Dedup: '%mantenimiento%' already matches 'mantenimiento correctivo'
        # so we only need the shortest variant. Fixes VARIANT-EXPLOSION-001.
        deduped = dedup_like_variants(term_variants)
        conditions: list[str] = []
        for variant in deduped:
            esc = self._escape(variant)
            if spec.description:
                conditions.append(
                    f"{self._accent_fold_like(spec.object_name, esc)} OR "
                    f"{self._accent_fold_like(spec.description, esc)}"
                )
            else:
                conditions.append(self._accent_fold_like(spec.object_name, esc))
        return conditions

    def _phrase_recall_conditions(self, spec: DatasetSpec, phrase_spec) -> str:
        """Cláusula recall-first de una frase: anclas co-ocurrentes en el MISMO campo.

        ( (objeto LIKE %a1% AND objeto LIKE %a2%) OR (desc LIKE %a1% AND desc LIKE %a2%) )
        Substring → cubre singular y plural y subsume la frase exacta. Misión: traer
        candidatos; la precisión fina (gap/orden/validador) es Commit 6.
        """
        anchors = [self._escape(a) for a in phrase_spec.ordered_anchors]
        if not anchors:
            return ""
        fields = [spec.object_name] + ([spec.description] if spec.description else [])
        field_clauses = [
            "(" + " AND ".join(self._accent_fold_like(field, a) for a in anchors) + ")"
            for field in fields
        ]
        return "(" + " OR ".join(field_clauses) + ")"

    @staticmethod
    def _accent_fold_expr(expr: str) -> str:
        """Strip accents using unaccent() — supported by Socrata SoQL (PostgreSQL backend).

        Socrata's SoQL does NOT support regexp_replace with 3 arguments.
        unaccent() is the correct and working approach for accent-insensitive LIKE matching.
        """
        return f"unaccent({expr})"

    @staticmethod
    def _add_estado_clause(where_clauses: list[str], field: str, value) -> None:
        """Add estado filter — handles both scalar and list values.

        Scalar: field = 'value'
        List: field IN ('v1', 'v2', ...)
        """
        if isinstance(value, list):
            if len(value) == 1:
                where_clauses.append(f"{field} = '{SoQLBuilder._escape(value[0])}'")
            else:
                escaped = ", ".join(f"'{SoQLBuilder._escape(v)}'" for v in value)
                where_clauses.append(f"{field} IN ({escaped})")
        else:
            where_clauses.append(f"{field} = '{SoQLBuilder._escape(str(value))}'")

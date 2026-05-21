from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class UnsafeGlobalQueryError(RuntimeError):
    """Raised when SoQLBuilder would emit a query with no substantial filter.

    A "substantial" filter is one of: scope (departamento, ciudad, entidad,
    contratista), topic (objeto), estado-family fields, or modalidad. Date
    ranges, value ranges and ordering signals alone are NOT substantial —
    they narrow nothing without a scope or topic and would otherwise produce
    a tautological WHERE 1=1 across the full SECOP universe.

    Callers that genuinely need a global query (admin/diagnostic tools)
    must opt in explicitly with ``allow_global=True``.
    """


# Filtros que cuentan como "base suficiente" para una consulta a SECOP.
# fecha/valor/orden/dataset/intent_type por sí solos NO cuentan.
_SUBSTANTIAL_FILTER_KEYS: frozenset[str] = frozenset({
    # scope
    "departamento_resolved", "ciudad",
    "entidad_resolved", "entidad_like",
    "contratista",
    # topic
    "objeto",
    # estado
    "estado", "estado_de_apertura_del_proceso", "estado_del_procedimiento",
    "estado_contrato", "estado_contrato_adicional",
    # modality
    "modalidad",
})


def _has_substantial_filter(params: dict) -> bool:
    for key in _SUBSTANTIAL_FILTER_KEYS:
        value = params.get(key)
        if value in (None, "", [], {}, False):
            continue
        return True
    return False

try:
    from app.data.morphological_variants import ROOT_TO_VARIANTS
except ImportError as exc:
    logger.warning(
        "Could not import ROOT_TO_VARIANTS; morphological expansion disabled: %s",
        exc,
    )
    ROOT_TO_VARIANTS: dict[str, frozenset[str]] = {}


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
                "fecha_de_publicacion_del",
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

    def build(self, dataset_id: str, params: dict, *, allow_global: bool = False) -> str:
        spec = self.SPECS[dataset_id]
        where_clause = self._build_where(spec, params, allow_global=allow_global)

        select_clause = ", ".join(spec.select_fields)

        # Default: date DESC (most recent first).
        # valor_desc: value DESC, date DESC
        # fecha_desc: date DESC, value DESC (same as default, explicit intent)
        ordering = params.get("ordering_signal", "")
        if params.get("intent_type") == "opportunity_search" and not ordering:
            order_expr = f"{spec.date} DESC"  # only freshness for opportunities
        elif ordering == "valor_desc":
            order_expr = f"{spec.value} DESC, {spec.date} DESC"
        else:
            order_expr = f"{spec.date} DESC, {spec.value} DESC"
        limit = int(params.get("limit", 50))
        offset = int(params.get("offset", 0))
        limit_clause = f"LIMIT {limit}"
        if offset:
            limit_clause += f" OFFSET {offset}"

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
        ``allow_global=True``. Date/value/order alone are not substantial
        filters — they narrow nothing without a scope or topic.
        """
        if not allow_global and not _has_substantial_filter(params):
            raise UnsafeGlobalQueryError(
                "SoQL build refused: no substantial filter (scope/topic/estado/modalidad). "
                "Pass allow_global=True only if a global query is intentional."
            )
        where_clauses: list[str] = []

        if params.get("departamento_resolved"):
            where_clauses.append(f"{spec.department} = '{self._escape(params['departamento_resolved'])}'")

        if params.get("ciudad") and spec.city:
            where_clauses.append(
                f"UPPER({spec.city}) LIKE UPPER('%{self._escape(str(params['ciudad']))}%')"
            )
        entidad_value = params.get("entidad_resolved") or params.get("entidad_like")
        if entidad_value:
            where_clauses.append(
                f"UPPER({spec.entity}) LIKE UPPER('%{self._escape(str(entidad_value))}%')"
            )

        for term in params.get("objeto", []):
            # str = required term (AND with other object entries)
            # list[str] = OR group created by parser for "X o Y"
            terms = term if isinstance(term, list) else [term]
            term_conditions: list[str] = []
            for sub_term in terms:
                # Expand to all morphological variants (OR clauses).
                # ROOT_TO_VARIANTS maps root→frozenset of all surface forms.
                # If the term is not in the index, it's used as-is (single LIKE).
                term_variants = ROOT_TO_VARIANTS.get(sub_term, frozenset([sub_term]))
                for variant in sorted(term_variants):  # sorted for deterministic SoQL
                    esc = self._escape(variant)
                    if spec.description:
                        term_conditions.append(
                            f"{self._accent_fold_like(spec.object_name, esc)} OR "
                            f"{self._accent_fold_like(spec.description, esc)}"
                        )
                    else:
                        term_conditions.append(self._accent_fold_like(spec.object_name, esc))
            where_clauses.append(f"({' OR '.join(term_conditions)})")

        if params.get("valor_min") is not None:
            where_clauses.append(f"{spec.value} >= {int(params['valor_min'])}")
        if params.get("valor_max") is not None:
            where_clauses.append(f"{spec.value} <= {int(params['valor_max'])}")

        if params.get("estado"):
            # Use the specific field identified by QueryRouter, or fall back to spec default
            estado_field = params.get("estado_field", spec.state)
            estado_val = params["estado"]
            # If a more specific list exists for this field, skip scalar (list handles below)
            if not (estado_field in params and isinstance(params.get(estado_field), list)):
                self._add_estado_clause(where_clauses, estado_field, estado_val)

        # NUEVO: estado_families filters — puede ser lista o simple
        legacy_field = params.get("estado_field")
        for field in ("estado_de_apertura_del_proceso", "estado_del_procedimiento",
                      "estado_contrato", "estado_contrato_adicional"):
            if field in params and params[field]:
                # Skip if legacy estado/estado_field already covers this
                if field == legacy_field:
                    continue
                self._add_estado_clause(where_clauses, field, params[field])

        if params.get("modalidad"):
            where_clauses.append(f"modalidad_de_contratacion = '{self._escape(params['modalidad'])}'")

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
    def _escape(value: str) -> str:
        return value.replace("'", "''")

    @classmethod
    def _accent_fold_like(cls, field: str, escaped_pattern: str) -> str:
        field_expr = cls._accent_fold_expr(f"UPPER({field})")
        pattern_expr = cls._accent_fold_expr(f"UPPER('%{escaped_pattern}%')")
        return f"{field_expr} LIKE {pattern_expr}"

    @staticmethod
    def _accent_fold_expr(expr: str) -> str:
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

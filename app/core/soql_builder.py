from __future__ import annotations

from dataclasses import dataclass


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
    reference: str | None = None
    contractor: str | None = None
    contractor_document: str | None = None
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

    def build(self, dataset_id: str, params: dict) -> str:
        spec = self.SPECS[dataset_id]
        where_clauses: list[str] = []

        if params.get("departamento_resolved"):
            where_clauses.append(f"{spec.department} = '{self._escape(params['departamento_resolved'])}'")

        entidad_value = params.get("entidad_resolved") or params.get("entidad_like")
        if entidad_value:
            where_clauses.append(
                f"UPPER({spec.entity}) LIKE UPPER('%{self._escape(str(entidad_value))}%')"
            )

        for term in params.get("objeto", []):
            escaped_term = self._escape(term)
            if spec.description:
                where_clauses.append(
                    f"(UPPER({spec.object_name}) LIKE UPPER('%{escaped_term}%') OR "
                    f"UPPER({spec.description}) LIKE UPPER('%{escaped_term}%'))"
                )
            else:
                where_clauses.append(f"UPPER({spec.object_name}) LIKE UPPER('%{escaped_term}%')")

        if params.get("valor_min") is not None:
            where_clauses.append(f"{spec.value} >= {int(params['valor_min'])}")
        if params.get("valor_max") is not None:
            where_clauses.append(f"{spec.value} <= {int(params['valor_max'])}")

        if params.get("estado"):
            # Use the specific field identified by QueryRouter, or fall back to spec default
            estado_field = params.get("estado_field", spec.state)
            where_clauses.append(f"{estado_field} = '{self._escape(params['estado'])}'")

        if params.get("modalidad"):
            where_clauses.append(f"modalidad_de_contratacion = '{self._escape(params['modalidad'])}'")

        if params.get("fecha_desde"):
            where_clauses.append(f"{spec.date} >= '{self._escape(params['fecha_desde'])}'")
        if params.get("fecha_hasta"):
            where_clauses.append(f"{spec.date} <= '{self._escape(params['fecha_hasta'])}'")

        if params.get("contratista") and spec.contractor:
            contractor = str(params["contratista"]).strip()
            if contractor.isdigit() and spec.contractor_document:
                where_clauses.append(f"{spec.contractor_document} = '{self._escape(contractor)}'")
            else:
                where_clauses.append(
                    f"UPPER({spec.contractor}) LIKE UPPER('%{self._escape(contractor)}%')"
                )

        select_clause = ", ".join(spec.select_fields)
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Default: date DESC (most recent first). Override to value DESC
        # only when user explicitly asks for ordering by price/amount.
        if params.get("ordering_signal") == "valor_desc":
            order_column = spec.value
        else:
            order_column = spec.date
        limit = int(params.get("limit", 50))

        return f"SELECT {select_clause} WHERE {where_clause} ORDER BY {order_column} DESC LIMIT {limit}"

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("'", "''")

from __future__ import annotations

from hypothesis import given, settings, strategies as st

from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.models import AdmissionStatus
from app.core.analytics.contract_activity.planning import build_plan
from app.core.analytics.contract_activity.runtime import execute_plan
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS, CorpusClient


@settings(max_examples=30, deadline=None)
@given(
    amount=st.integers(min_value=1, max_value=999_999),
    phrase=st.sampled_from((
        ("superiores a", ">"),
        ("mayores a", ">"),
        ("por encima de", ">"),
        ("al menos", ">="),
        ("inferiores a", "<"),
        ("menores o iguales a", "<="),
    )),
)
def test_money_comparator_survives_raw_request_and_sql(amount, phrase) -> None:
    surface, operator = phrase
    raw = f"¿Cuántos contratos {surface} {amount} pesos firmó Galapa en 2026?"
    admission = admit_contract_activity(raw, BUYER_PARAMS)
    assert admission.status is AdmissionStatus.READY and admission.request is not None
    assert admission.request.money is not None
    assert admission.request.money.operator == operator
    assert f"valor_del_contrato {operator} {amount}" in build_plan(admission.request).where


@settings(max_examples=30, deadline=None)
@given(parts=st.lists(st.integers(min_value=1, max_value=25), min_size=1, max_size=8))
def test_any_complete_group_partition_reconciles(parts) -> None:
    admission = admit_contract_activity(
        "Desglosa los contratos de Galapa por modalidad.", BUYER_PARAMS
    )
    assert admission.request is not None
    plan = build_plan(admission.request)

    class PartitionClient(CorpusClient):
        def aggregate(self, dataset: str, sql: str) -> list[dict]:
            self.calls.append((dataset, sql))
            total = sum(parts)
            if "HAVING" in sql:
                return []
            if "GROUP BY modalidad_de_contratacion" in sql:
                return [
                    {
                        "group_label": f"M-{index}",
                        "row_count": str(count),
                        "id_count": str(count),
                        "distinct_contract_count": str(count),
                    }
                    for index, count in enumerate(parts)
                ]
            return [{
                "row_count": str(total),
                "id_count": str(total),
                "distinct_contract_count": str(total),
                "group_non_null_count": str(total),
                "distinct_group_count": str(len(parts)),
            }]

    result = execute_plan(plan, PartitionClient())
    assert result.total == sum(parts)
    assert sum(group.contract_count for group in result.groups) == result.total
    assert abs(sum(group.share for group in result.groups) - 1.0) < 1e-12


@given(suffix=st.text(alphabet=st.characters(whitelist_categories=("Ll",)), min_size=1, max_size=12))
def test_unknown_modality_is_absorbing_and_makes_no_plan(suffix) -> None:
    admission = admit_contract_activity(
        f"¿Cuántos contratos de modalidad zzz{suffix} firmó Galapa en 2026?",
        BUYER_PARAMS,
    )
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.request is None

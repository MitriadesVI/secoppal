#!/usr/bin/env python3
"""Run the bounded SEARCH-SH1 semantic/money mutation campaign."""
from __future__ import annotations

import hashlib
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class Mutation:
    identifier: str
    invariant: str
    path: str
    original: str
    mutated: str
    tests: tuple[str, ...]


MUTATIONS = (
    Mutation(
        "M01", "broad convenio is no longer classified",
        "app/core/contract_type_resolver.py",
        '    _Pattern("convenio", _compile(r"\\bconvenios?\\b")),\n',
        '    _Pattern("convenio", _compile(r"\\b__disabled_convenio__\\b")),\n',
        ("tests/test_search_sh1_dogfood_regressions.py::test_r1_convenios_is_not_inferred_as_topic",),
    ),
    Mutation(
        "M02", "consumed contractual span leaks back into topic",
        "app/core/contract_type_resolver.py",
        "        residual_text=residual,\n",
        '        residual_text=str(text or ""),\n',
        ("tests/test_contractual_dimension_pipeline.py::test_natural_contractual_class_corpus",),
    ),
    Mutation(
        "M03", "special-regime modality resolution is discarded",
        "app/core/query_router.py",
        "        if modality_result.executable_params:\n            params.update(modality_result.executable_params)\n",
        "        if False and modality_result.executable_params:\n            params.update(modality_result.executable_params)\n",
        ("tests/test_search_sh1_dogfood_regressions.py::test_r2_convenio_and_regimen_especial_are_independent_dimensions",),
    ),
    Mutation(
        "M04", "abbreviated-selection parent loses one ordinary child",
        "app/data/modality_catalog_v1.yaml",
        "  - procedure_id: seleccion_abreviada_menor_cuantia_sin_manifestacion\n    display_name: Seleccion Abreviada Menor Cuantia Sin Manifestacion Interes\n    legal_selection_family: seleccion_abreviada\n    legal_classification_status: verified\n    retrieval_family: seleccion_abreviada\n",
        "  - procedure_id: seleccion_abreviada_menor_cuantia_sin_manifestacion\n    display_name: Seleccion Abreviada Menor Cuantia Sin Manifestacion Interes\n    legal_selection_family: seleccion_abreviada\n    legal_classification_status: verified\n    retrieval_family: disabled_search_sh1_mutant\n",
        ("tests/test_modality_resolver.py::test_family_child_and_amp_contract_is_frozen",),
    ),
    Mutation(
        "M05", "contractual type is incorrectly written into modality",
        "app/core/query_router.py",
        '            params["contract_type_include"] = list(\n                contract_type_result.include_ids\n            )\n',
        '            params["modalidad_include_procedure_ids"] = list(\n                contract_type_result.include_ids\n            )\n',
        ("tests/test_contractual_dimension_pipeline.py::test_convenio_modality_money_buyer_and_time_all_compose",),
    ),
    Mutation(
        "M06", "minimum monetary delta is rejected by the intent gate",
        "app/core/followup_mutations.py",
        'MONEY_FIELDS = frozenset({"valor_min", "valor_max", "money_metadata"})\n',
        'MONEY_FIELDS = frozenset({"valor_max", "money_metadata"})\n',
        ("tests/test_search_sh1_money_followups.py::test_minimum_families_survive_followup",),
    ),
    Mutation(
        "M07", "maximum monetary delta is rejected by the intent gate",
        "app/core/followup_mutations.py",
        'MONEY_FIELDS = frozenset({"valor_min", "valor_max", "money_metadata"})\n',
        'MONEY_FIELDS = frozenset({"valor_min", "money_metadata"})  # M07\n',
        ("tests/test_search_sh1_money_followups.py::test_maximum_families_survive_followup",),
    ),
    Mutation(
        "M08", "range parsing drops its upper monetary bound",
        "app/core/query_router.py",
        '                params["valor_min"] = min(amount_lo, amount_hi)\n                params["valor_max"] = max(amount_lo, amount_hi)\n                # Remove matched span so generic loop below doesn\'t re-parse it\n',
        '                params["valor_min"] = min(amount_lo, amount_hi)\n                params.pop("valor_max", None)\n                # Remove matched span so generic loop below doesn\'t re-parse it\n',
        ("tests/test_search_sh1_money_followups.py::test_range_families_keep_both_bounds",),
    ),
    Mutation(
        "M09", "same-year classification suppresses a concurrent money delta",
        "app/core/followup_engine.py",
        "        if (\n            has_year\n            and not current_frame.topic\n            and not has_scope_signal\n            and not has_order\n            and not has_orthogonal_filter\n        ):\n            return \"change_year\"\n",
        "        if (\n            has_year\n            and not current_frame.topic\n            and not has_scope_signal\n            and not has_order\n        ):\n            return \"change_year\"\n",
        ("tests/test_search_sh1_money_followups.py::test_same_year_noop_does_not_suppress_money",),
    ),
    Mutation(
        "M10", "money refinement erases the inherited topic",
        "app/core/followup_engine.py",
        "    topic_inherited = FollowupClassifier._is_weak_topic(curr.topic)\n    inherited_topic = list(prev.topic) if topic_inherited else list(curr.topic)\n    return QueryFrame(\n        dataset=prev.dataset,\n        dataset_explicit=prev.dataset_explicit,\n        scope=dict(prev.scope),\n",
        "    topic_inherited = FollowupClassifier._is_weak_topic(curr.topic)\n    inherited_topic = []\n    return QueryFrame(\n        dataset=prev.dataset,\n        dataset_explicit=prev.dataset_explicit,\n        scope=dict(prev.scope),\n",
        ("tests/test_search_sh1_dogfood_regressions.py::test_r3_same_year_money_delta_survives_every_deterministic_boundary",),
    ),
    Mutation(
        "M11", "money refinement erases the inherited buyer",
        "app/core/followup_engine.py",
        "        scope=dict(prev.scope),\n        topic=inherited_topic,\n        objeto_exclude=list(dict.fromkeys([*prev.objeto_exclude, *curr.objeto_exclude])),\n",
        "        scope={},\n        topic=inherited_topic,\n        objeto_exclude=list(dict.fromkeys([*prev.objeto_exclude, *curr.objeto_exclude])),\n",
        ("tests/test_search_sh1_dogfood_regressions.py::test_r3_same_year_money_delta_survives_every_deterministic_boundary",),
    ),
    Mutation(
        "M12", "money refinement erases the inherited modality",
        "app/core/followup_engine.py",
        "        objeto_exclude=list(dict.fromkeys([*prev.objeto_exclude, *curr.objeto_exclude])),\n        modifiers=_merge_modifiers(prev.modifiers, curr.modifiers),\n        raw_params=_merge_raw(prev.raw_params, curr.raw_params, topic_inherited=topic_inherited),\n        intent_type=\"refine_filter\",\n",
        "        objeto_exclude=list(dict.fromkeys([*prev.objeto_exclude, *curr.objeto_exclude])),\n        modifiers=dict(curr.modifiers),\n        raw_params=_merge_raw(prev.raw_params, curr.raw_params, topic_inherited=topic_inherited),\n        intent_type=\"refine_filter\",\n",
        ("tests/test_search_sh1_money_followups.py::test_money_followup_preserves_previous_modality",),
    ),
    Mutation(
        "M13", "contracts project money onto the process value field",
        "app/core/soql_builder.py",
        '            value="valor_del_contrato",\n',
        '            value="precio_base",\n',
        ("tests/test_search_sh1_money_followups.py::test_followup_money_uses_dataset_value_projection",),
    ),
    Mutation(
        "M14", "processes project money onto the contract value field",
        "app/core/soql_builder.py",
        '            value="precio_base",\n',
        '            value="valor_del_contrato",\n',
        ("tests/test_search_sh1_money_followups.py::test_followup_money_uses_dataset_value_projection",),
    ),
    Mutation(
        "M15", "semantic refinement leaves stale pagination sidecars",
        "app/core/conversation_store.py",
        "        with self._pagination_lock:\n            path = self._pagination_path_for(chat_id)\n            if path.exists():\n                path.unlink()\n\n    # ------------------------------------------------------------------\n",
        "        with self._pagination_lock:\n            path = self._pagination_path_for(chat_id)\n            if False and path.exists():\n                path.unlink()\n\n    # ------------------------------------------------------------------\n",
        ("tests/test_pagination_buffer_lifecycle.py::test_semantic_turn_invalidation_drops_all_buffer_groups",),
    ),
)


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    initial = {mutation.path: _hash(ROOT / mutation.path) for mutation in MUTATIONS}
    detected = 0
    for mutation in MUTATIONS:
        path = ROOT / mutation.path
        before = path.read_bytes()
        source = before.decode("utf-8")
        if source.count(mutation.original) != 1:
            print(f"{mutation.identifier} SETUP_ERROR: {mutation.invariant}")
            continue
        try:
            path.write_text(
                source.replace(mutation.original, mutation.mutated, 1),
                encoding="utf-8",
            )
            process = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", *mutation.tests],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        finally:
            path.write_bytes(before)
        caught = process.returncode != 0
        detected += int(caught)
        print(
            f"{mutation.identifier} {'DETECTED' if caught else 'SURVIVED'}: "
            f"{mutation.invariant}"
        )
        if not caught:
            print(process.stdout.strip())

    restored = all(_hash(ROOT / path) == digest for path, digest in initial.items())
    print(f"MUTATIONS: TOTAL {len(MUTATIONS)} · DETECTED {detected}")
    print(f"RESTORED_BYTE_FOR_BYTE: {'YES' if restored else 'NO'}")
    return 0 if detected == len(MUTATIONS) and restored else 1


if __name__ == "__main__":
    raise SystemExit(main())

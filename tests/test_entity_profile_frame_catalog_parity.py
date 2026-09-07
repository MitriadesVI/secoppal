"""Mientras convivan los dos catálogos, deben decir lo mismo.

El core V1.2-A declara sus propias tablas de capacidad, jobs y secciones porque
no puede leer `execution_plan` como coautoridad: ese módulo interpreta un
universo legacy y se retira en V1.2-B. Pero dos tablas que describen el mismo
hecho pueden divergir sin que nada falle, y entonces habría dos verdades.

Este guard existe SÓLO durante la convivencia. Cuando el lector de V1.1
desaparezca en el cutover, este archivo desaparece con él.
"""
from __future__ import annotations

from app.core.entity_profile import execution_plan as legacy
from app.core.entity_profile.frame_compiler import (
    FOCUS_CAPABILITY,
    FOCUS_SECTIONS,
    SECTION_CALLS,
    _job_catalog,
)
from app.core.entity_profile.frame_values import SourceKind
from app.core.entity_profile.query_runner import jobs_for_focus


def test_focus_capability_matches_v1_1():
    projected = {
        focus: frozenset(source.dataset_id for source in sources)
        for focus, sources in FOCUS_CAPABILITY.items()
    }
    assert projected == dict(legacy.FOCUS_CAPABILITY)


def test_section_catalogs_match_v1_1():
    assert dict(SECTION_CALLS) == dict(legacy.SECTION_CALLS)
    assert dict(FOCUS_SECTIONS) == dict(legacy.FOCUS_SECTIONS)


def test_job_catalog_matches_v1_1_call_ids_and_datasets():
    catalog = _job_catalog()
    assert set(catalog) == set(FOCUS_SECTIONS)
    for focus, specs in catalog.items():
        old = jobs_for_focus(focus)
        assert [spec.call_id for spec in specs] == [job.call_id for job in old]
        assert [spec.operation for spec in specs] == [job.operation for job in old]
        assert [spec.source.dataset_id for spec in specs] == [
            job.dataset_id for job in old
        ]


def test_dataset_ids_match_v1_1():
    assert SourceKind.PROCESSES.dataset_id == legacy.PROCESS_DATASET
    assert SourceKind.CONTRACTS.dataset_id == legacy.CONTRACT_DATASET

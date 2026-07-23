"""MOD-M1 — tests del catálogo canónico de modalidad V1.

Invariantes mínimas + batería fail-closed (tests negativos que mutan una copia
del YAML). Los conteos son metadata observacional: NUNCA se comparan contra
cifras exactas (drift diario). El snapshot JSON es la fuente independiente de
verdad para cobertura de raws.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml

from app.core.modality_models import (
    CatalogValidationError,
    LEGAL_SELECTION_FAMILIES,
    load_catalog,
)

ROOT = Path(__file__).parent.parent
CATALOG_PATH = ROOT / "app" / "data" / "modality_catalog_v1.yaml"
SNAPSHOT_PATH = ROOT / "tests" / "fixtures" / "modality_raw_snapshot_v1.json"
MODELS_SRC = (ROOT / "app" / "core" / "modality_models.py").read_text(encoding="utf-8")
# Solo las líneas de import cuentan para el guard de acoplamiento; la prosa del
# docstring (que nombra los módulos fuera de alcance para explicar qué NO toca)
# no debe disparar falsos positivos.
MODELS_IMPORT_LINES = "\n".join(
    ln for ln in MODELS_SRC.splitlines()
    if ln.strip().startswith(("import ", "from "))
).lower()


@pytest.fixture(scope="module")
def catalog():
    return load_catalog()


@pytest.fixture(scope="module")
def snapshot():
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def base_data():
    return yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))


def _snapshot_raws(snapshot, dataset):
    return set(snapshot["datasets"][dataset]["raw_values"].keys())


def _catalog_raws(catalog, dataset):
    raws = []
    for p in catalog.procedures:
        raws.extend(p.dataset_values.get(dataset, ()))
    return raws


# ═══════════════════════════════════════════════════════════════════════════
# Invariantes positivas (catálogo real)
# ═══════════════════════════════════════════════════════════════════════════

# 1 — Todos los raw values del snapshot aparecen EXACTAMENTE UNA VEZ en dataset_values.
@pytest.mark.parametrize("dataset", ["procesos", "contratos"])
def test_1_every_raw_appears_exactly_once(catalog, snapshot, dataset):
    catalog_raws = _catalog_raws(catalog, dataset)
    snap_raws = _snapshot_raws(snapshot, dataset)
    for raw in snap_raws:
        assert catalog_raws.count(raw) == 1, f"{raw!r} no aparece exactamente una vez en {dataset}"
    assert set(catalog_raws) == snap_raws  # cero invenciones


# 2 — Ningún raw value desaparece durante la clasificación (cobertura total).
@pytest.mark.parametrize("dataset,expected", [("procesos", 17), ("contratos", 15)])
def test_2_no_raw_value_lost(catalog, snapshot, dataset, expected):
    catalog_raws = _catalog_raws(catalog, dataset)
    snap_raws = _snapshot_raws(snapshot, dataset)
    assert len(snap_raws) == expected            # snapshot: 17 procesos / 15 contratos
    assert set(catalog_raws) == snap_raws         # cero omisiones, cero invenciones
    assert len(catalog_raws) == len(set(catalog_raws))  # cero duplicados por dataset


# 3 — procedure_id no puede estar duplicado.
def test_3_no_duplicate_procedure_id(catalog):
    ids = [p.procedure_id for p in catalog.procedures]
    assert len(ids) == len(set(ids))


# 4 — platform_test no es user_searchable.
def test_4_platform_test_not_searchable(catalog):
    tests = [p for p in catalog.procedures if p.record_kind == "platform_test"]
    assert tests
    assert all(p.user_searchable is False for p in tests)


# 5 — planning_rfi no tiene legal_selection_family.
def test_5_planning_rfi_has_no_legal_family(catalog):
    rfis = [p for p in catalog.procedures if p.record_kind == "planning_rfi"]
    assert rfis
    assert all(p.legal_selection_family is None for p in rfis)


# 6 — state_asset_sale requiere intención explícita.
def test_6_state_asset_sale_requires_explicit_intent(catalog):
    sales = [p for p in catalog.procedures if p.record_kind == "state_asset_sale"]
    assert len(sales) == 2
    assert all(p.explicit_intent_required is True for p in sales)
    assert all(p.parent_expansion_policy == "explicit_only" for p in sales)


# 7 — acuerdo_marco NO pertenece a los padres licitacion_publica ni seleccion_abreviada.
def test_7_acuerdo_marco_not_in_parents(catalog):
    amp = catalog.by_id("acuerdo_marco_precio_secop")
    assert amp is not None
    assert amp.retrieval_family == "acuerdo_marco"
    assert amp.parent_expansion_policy == "explicit_only"
    lic = {p.procedure_id for p in catalog.retrieval_family("licitacion_publica")}
    sa = {p.procedure_id for p in catalog.retrieval_family("seleccion_abreviada")}
    assert "acuerdo_marco_precio_secop" not in lic
    assert "acuerdo_marco_precio_secop" not in sa


# 8 — contratación directa (padre) contiene AMBOS raws aplicables (D3).
def test_8_contratacion_directa_parent_both_modes(catalog):
    family = catalog.retrieval_family("contratacion_directa")
    assert {p.submission_mode for p in family} == {"standard", "con_ofertas"}
    raws = set()
    for p in family:
        raws |= set(p.dataset_values.get("procesos", ()))
    assert {"Contratación directa", "Contratación Directa (con ofertas)"} <= raws


# 9 — régimen especial (padre) contiene AMBOS raws aplicables (D3).
def test_9_regimen_especial_parent_both_modes(catalog):
    family = catalog.retrieval_family("regimen_especial")
    assert {p.submission_mode for p in family} == {"standard", "con_ofertas"}
    assert all(p.legal_regime == "especial" for p in family)
    assert all(p.legal_selection_family is None for p in family)
    raws = set()
    for p in family:
        raws |= set(p.dataset_values.get("procesos", ()))
    assert {"Contratación régimen especial",
            "Contratación régimen especial (con ofertas)"} <= raws


# 10 — los conteos son metadata observacional; NO se comparan cifras exactas.
@pytest.mark.parametrize("dataset", ["procesos", "contratos"])
def test_10_counts_are_observational_metadata(snapshot, dataset):
    counts = snapshot["datasets"][dataset]["raw_values"]
    assert counts
    assert all(isinstance(n, int) and n >= 0 for n in counts.values())
    assert snapshot["_meta"]["counts_are"].startswith("OBSERVATIONAL")


# 11-13 — el modelo NO se acopla a parser/builder/frame.
@pytest.mark.parametrize("module", ["query_router", "soql_builder", "query_frame"])
def test_11_13_models_decoupled_from_pipeline(module):
    assert module not in MODELS_IMPORT_LINES


# 14 — el modelo NO se acopla a Radar / warehouse / Telegram / follow-up.
@pytest.mark.parametrize("token", ["radar", "warehouse", "telegram", "followup", "follow_up"])
def test_14_models_decoupled_from_out_of_scope(token):
    assert token not in MODELS_IMPORT_LINES


# 15 — sentinel eliminado; estado de clasificación en campo propio.
def test_15_sentinel_removed_status_field(catalog):
    assert "null_pending_review" not in LEGAL_SELECTION_FAMILIES
    assert "null_pending_review" not in CATALOG_PATH.read_text(encoding="utf-8")
    amp = catalog.by_id("acuerdo_marco_precio_secop")
    assert amp.legal_selection_family is None
    assert amp.legal_classification_status == "pending_review"
    # Asignaciones por familia de record.
    for p in catalog.procedures:
        if p.record_kind in ("planning_rfi", "platform_test") or p.retrieval_family == "regimen_especial":
            assert p.legal_classification_status == "not_applicable", p.procedure_id
        elif p.procedure_id == "acuerdo_marco_precio_secop":
            assert p.legal_classification_status == "pending_review"
        else:
            assert p.legal_classification_status == "verified", p.procedure_id


def test_catalog_loads_and_validates(catalog):
    assert catalog.version == 1
    assert len(catalog.procedures) == 17


# ═══════════════════════════════════════════════════════════════════════════
# Batería fail-closed — mutar una copia y verificar que load_catalog rechaza.
# ═══════════════════════════════════════════════════════════════════════════

def _load_mutated(tmp_path, data):
    p = tmp_path / "mutated.yaml"
    p.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return load_catalog(p)


def _rec(data, pid):
    return next(r for r in data["procedures"] if r["procedure_id"] == pid)


# ── Estructura / tipos ──────────────────────────────────────────────────────

def test_neg_root_not_mapping(tmp_path):
    p = tmp_path / "x.yaml"
    p.write_text("- just\n- a\n- list\n", encoding="utf-8")
    with pytest.raises(CatalogValidationError, match="raíz"):
        load_catalog(p)


def test_neg_version_missing(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    del data["version"]
    with pytest.raises(CatalogValidationError, match="version"):
        _load_mutated(tmp_path, data)


def test_neg_version_not_int(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    data["version"] = "1"
    with pytest.raises(CatalogValidationError, match="version"):
        _load_mutated(tmp_path, data)


def test_neg_version_non_positive(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    data["version"] = 0
    with pytest.raises(CatalogValidationError, match="version"):
        _load_mutated(tmp_path, data)


def test_neg_procedures_missing(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    del data["procedures"]
    with pytest.raises(CatalogValidationError, match="procedures"):
        _load_mutated(tmp_path, data)


def test_neg_procedures_empty(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    data["procedures"] = []
    with pytest.raises(CatalogValidationError, match="vacía"):
        _load_mutated(tmp_path, data)


def test_neg_procedures_not_list(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    data["procedures"] = {"foo": "bar"}
    with pytest.raises(CatalogValidationError, match="lista"):
        _load_mutated(tmp_path, data)


def test_neg_record_not_mapping(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    data["procedures"].append("no soy un mapping")
    with pytest.raises(CatalogValidationError, match="mapping"):
        _load_mutated(tmp_path, data)


def test_neg_missing_required_field(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    del _rec(data, "minima_cuantia")["record_kind"]
    with pytest.raises(CatalogValidationError, match="obligatorio"):
        _load_mutated(tmp_path, data)


@pytest.mark.parametrize("field", ["procedure_id", "display_name", "retrieval_family"])
def test_neg_empty_key_string(tmp_path, base_data, field):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")[field] = "   "
    with pytest.raises(CatalogValidationError, match="no vacío|no vac"):
        _load_mutated(tmp_path, data)


@pytest.mark.parametrize("value", [1, "true", "false", 0, None])
def test_neg_bool_not_real_bool(tmp_path, base_data, value):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["user_searchable"] = value
    with pytest.raises(CatalogValidationError, match="booleano"):
        _load_mutated(tmp_path, data)


def test_neg_dataset_values_missing(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    del _rec(data, "minima_cuantia")["dataset_values"]
    with pytest.raises(CatalogValidationError, match="obligatorio"):
        _load_mutated(tmp_path, data)


def test_neg_dataset_values_empty(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["dataset_values"] = {}
    with pytest.raises(CatalogValidationError, match="ausente o vacío"):
        _load_mutated(tmp_path, data)


def test_neg_unknown_dataset(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["dataset_values"]["secop3"] = ["X"]
    with pytest.raises(CatalogValidationError, match="dataset desconocido"):
        _load_mutated(tmp_path, data)


def test_neg_values_not_list(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["dataset_values"]["procesos"] = "Mínima cuantía"
    with pytest.raises(CatalogValidationError, match="debe ser una lista"):
        _load_mutated(tmp_path, data)


def test_neg_values_empty_list(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["dataset_values"]["procesos"] = []
    with pytest.raises(CatalogValidationError, match="lista vacía"):
        _load_mutated(tmp_path, data)


@pytest.mark.parametrize("bad", ["   ", 123, None])
def test_neg_raw_empty_or_nonstring(tmp_path, base_data, bad):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["dataset_values"]["procesos"] = [bad]
    with pytest.raises(CatalogValidationError, match="raw value inválido"):
        _load_mutated(tmp_path, data)


def test_neg_duplicate_procedure_id(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    dup = copy.deepcopy(_rec(data, "minima_cuantia"))
    dup["dataset_values"] = {"procesos": ["Otro raw único"]}
    data["procedures"].append(dup)
    with pytest.raises(CatalogValidationError, match="procedure_id duplicado"):
        _load_mutated(tmp_path, data)


def test_neg_duplicate_raw_per_dataset(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    # asignar el raw de mínima cuantía también a otro procedure en el mismo dataset
    _rec(data, "licitacion_publica")["dataset_values"]["procesos"].append("Mínima cuantía")
    with pytest.raises(CatalogValidationError, match="más de un procedure"):
        _load_mutated(tmp_path, data)


@pytest.mark.parametrize("field,value,pid", [
    ("legal_selection_family", "no_existe", "minima_cuantia"),
    ("legal_classification_status", "maybe", "minima_cuantia"),
    ("record_kind", "weird", "minima_cuantia"),
    ("submission_mode", "raro", "minima_cuantia"),
    ("legal_regime", "otro", "minima_cuantia"),
    ("parent_expansion_policy", "sometimes", "minima_cuantia"),
    ("evidence_confidence", "altisima", "minima_cuantia"),
])
def test_neg_invalid_vocabulary(tmp_path, base_data, field, value, pid):
    data = copy.deepcopy(base_data)
    _rec(data, pid)[field] = value
    with pytest.raises(CatalogValidationError, match="inválid"):
        _load_mutated(tmp_path, data)


# ── Invariantes semánticas ──────────────────────────────────────────────────

# Coherencia general familia jurídica ↔ estado de clasificación.
def test_neg_family_present_with_not_applicable(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["legal_classification_status"] = "not_applicable"
    with pytest.raises(CatalogValidationError, match="not_applicable"):
        _load_mutated(tmp_path, data)


def test_neg_family_present_with_pending_review(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["legal_classification_status"] = "pending_review"
    with pytest.raises(CatalogValidationError, match="pending_review"):
        _load_mutated(tmp_path, data)


def test_neg_family_null_with_verified(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    _rec(data, "minima_cuantia")["legal_selection_family"] = None
    with pytest.raises(CatalogValidationError, match="verified"):
        _load_mutated(tmp_path, data)


# planning_rfi: matriz cerrada y solo dataset procesos.
@pytest.mark.parametrize("field,value", [
    ("legal_selection_family", "seleccion_abreviada"),
    ("legal_classification_status", "verified"),
    ("legal_regime", "especial"),
    ("submission_mode", "standard"),
    ("retrieval_family", "platform_test"),
    ("user_searchable", False),
    ("explicit_intent_required", False),
    ("not_searchable_as_modality", False),
    ("parent_expansion_policy", "family_default"),
])
def test_neg_planning_rfi_matrix(tmp_path, base_data, field, value):
    data = copy.deepcopy(base_data)
    _rec(data, "solicitud_informacion_proveedores")[field] = value
    with pytest.raises(CatalogValidationError, match="planning_rfi"):
        _load_mutated(tmp_path, data)


def test_neg_planning_rfi_present_in_contracts(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    rec = _rec(data, "solicitud_informacion_proveedores")
    rec["dataset_values"]["contratos"] = ["Solicitud RFI inválida en contratos"]
    with pytest.raises(CatalogValidationError, match="planning_rfi"):
        _load_mutated(tmp_path, data)


# platform_test: matriz cerrada y solo dataset procesos.
@pytest.mark.parametrize("field,value", [
    ("legal_selection_family", "seleccion_abreviada"),
    ("legal_classification_status", "verified"),
    ("legal_regime", "general"),
    ("submission_mode", "standard"),
    ("retrieval_family", "solicitud_informacion_proveedores"),
    ("user_searchable", True),
    ("explicit_intent_required", True),
    ("parent_expansion_policy", "family_default"),
])
def test_neg_platform_test_matrix(tmp_path, base_data, field, value):
    data = copy.deepcopy(base_data)
    _rec(data, "subasta_de_prueba")[field] = value
    with pytest.raises(CatalogValidationError, match="platform_test"):
        _load_mutated(tmp_path, data)


def test_neg_platform_test_present_in_contracts(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    rec = _rec(data, "subasta_de_prueba")
    rec["dataset_values"]["contratos"] = ["Subasta de prueba inválida en contratos"]
    with pytest.raises(CatalogValidationError, match="platform_test"):
        _load_mutated(tmp_path, data)


# state_asset_sale: clasificación jurídica y recuperación cerradas.
@pytest.mark.parametrize("field,value", [
    ("legal_selection_family", None),
    ("legal_classification_status", "not_applicable"),
    ("legal_regime", "especial"),
    ("retrieval_family", "seleccion_abreviada"),
    ("user_searchable", False),
    ("explicit_intent_required", False),
    ("parent_expansion_policy", "family_default"),
])
def test_neg_state_asset_sale_matrix(tmp_path, base_data, field, value):
    data = copy.deepcopy(base_data)
    _rec(data, "enajenacion_subasta")[field] = value
    with pytest.raises(CatalogValidationError, match="state_asset_sale"):
        _load_mutated(tmp_path, data)


# retrieval_family=regimen_especial: régimen, familia y forma cerrados.
@pytest.mark.parametrize("field,value", [
    ("legal_selection_family", "contratacion_directa"),
    ("legal_classification_status", "verified"),
    ("legal_regime", "general"),
    ("record_kind", "procurement_instrument"),
    ("submission_mode", "no_aplica"),
    ("parent_expansion_policy", "explicit_only"),
])
def test_neg_regimen_especial_matrix(tmp_path, base_data, field, value):
    data = copy.deepcopy(base_data)
    _rec(data, "regimen_especial")[field] = value
    with pytest.raises(CatalogValidationError, match="regimen_especial"):
        _load_mutated(tmp_path, data)


def test_neg_unsearchable_but_requires_explicit_intent(tmp_path, base_data):
    data = copy.deepcopy(base_data)
    rec = _rec(data, "contratacion_directa")
    rec["user_searchable"] = False
    rec["explicit_intent_required"] = True
    with pytest.raises(CatalogValidationError, match="incompatible"):
        _load_mutated(tmp_path, data)


# yaml.safe_load conserva silenciosamente la última clave; el loader canónico no.
def test_neg_duplicate_yaml_key(tmp_path):
    lines = CATALOG_PATH.read_text(encoding="utf-8").splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.lstrip().startswith("legal_regime: general"):
            indent = line[:len(line) - len(line.lstrip())]
            lines.insert(index, f"{indent}legal_regime: especial\n")
            break
    else:
        pytest.fail("el catálogo no contiene legal_regime: general")

    path = tmp_path / "duplicate-key.yaml"
    path.write_text("".join(lines), encoding="utf-8")
    with pytest.raises(CatalogValidationError, match="clave YAML duplicada.*legal_regime"):
        load_catalog(path)

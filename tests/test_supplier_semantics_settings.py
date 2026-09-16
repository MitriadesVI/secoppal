from __future__ import annotations

import pytest
from pydantic import ValidationError

from app import config
from app.config import SupplierPolaritySidecarState, SupplierSemanticsSettings


ALIASES = {
    "SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED",
    "DEEPSEEK_API_KEY",
    "DEEPSEEK_MODEL",
    "SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS",
}
GENERIC_NAMES = {"ENABLED", "API_KEY", "MODEL", "TIMEOUT_SECONDS"}


@pytest.fixture(autouse=True)
def _isolated_cache(monkeypatch):
    for name in ALIASES | GENERIC_NAMES:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.lower(), raising=False)
    getter = config.get_supplier_semantics_settings
    getter.cache_clear()
    yield
    getter.cache_clear()


def _env_file(tmp_path, **values):
    path = tmp_path / "sidecar.env"
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()))
    return path


def test_settings_have_exactly_the_four_authorized_aliases():
    fields = SupplierSemanticsSettings.model_fields.values()
    assert {field.alias for field in fields} == ALIASES
    assert len(SupplierSemanticsSettings.model_fields) == 4
    assert SupplierSemanticsSettings.model_config["populate_by_name"] is False
    assert all(field.validation_alias == field.alias for field in fields)


def test_generic_environment_names_cannot_activate_or_change_prerequisites(monkeypatch):
    monkeypatch.setenv("ENABLED", "true")
    monkeypatch.setenv("API_KEY", "foreign-key")
    monkeypatch.setenv("MODEL", "foreign-model")
    monkeypatch.setenv("TIMEOUT_SECONDS", "2")
    settings = SupplierSemanticsSettings(_env_file=None)
    assert settings.enabled is False
    assert settings.api_key is None
    assert settings.model == "deepseek-chat"
    assert settings.timeout_seconds == 8.0


def test_generic_dotenv_names_cannot_activate_or_change_prerequisites(tmp_path):
    env_file = _env_file(
        tmp_path,
        ENABLED="true",
        API_KEY="foreign-key",
        MODEL="foreign-model",
        TIMEOUT_SECONDS="2",
    )
    settings = SupplierSemanticsSettings(_env_file=env_file)
    assert settings.enabled is False
    assert settings.api_key is None
    assert settings.model == "deepseek-chat"
    assert settings.timeout_seconds == 8.0


def test_generic_environment_names_cannot_block_or_replace_canonical_dotenv(
    tmp_path, monkeypatch,
):
    env_file = _env_file(
        tmp_path,
        SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED="true",
        DEEPSEEK_API_KEY="file-key",
        DEEPSEEK_MODEL="file-model",
        SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS="3.5",
    )
    monkeypatch.setenv("ENABLED", "false")
    monkeypatch.setenv("API_KEY", "foreign-key")
    monkeypatch.setenv("MODEL", "foreign-model")
    monkeypatch.setenv("TIMEOUT_SECONDS", "2")
    settings = SupplierSemanticsSettings(_env_file=env_file)
    assert (settings.enabled, settings.api_key, settings.model, settings.timeout_seconds) == (
        True, "file-key", "file-model", 3.5,
    )


def test_mixed_case_canonical_environment_aliases_override_dotenv(tmp_path, monkeypatch):
    env_file = _env_file(
        tmp_path,
        SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED="false",
        DEEPSEEK_API_KEY="file-key",
        DEEPSEEK_MODEL="file-model",
        SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS="8",
    )
    monkeypatch.setenv("SeCoPpAl_A2_SuPpLiEr_PoLaRiTy_SiDeCaR_EnAbLeD", "true")
    monkeypatch.setenv("dEePsEeK_aPi_KeY", "environment-key")
    monkeypatch.setenv("DeEpSeEk_MoDeL", "environment-model")
    monkeypatch.setenv("sEcOpPaL_A2_sUpPlIeR_pOlArItY_tImEoUt_SeCoNdS", "2.5")
    settings = SupplierSemanticsSettings(_env_file=env_file)
    assert (settings.enabled, settings.api_key, settings.model, settings.timeout_seconds) == (
        True, "environment-key", "environment-model", 2.5,
    )


def test_omitted_flag_is_off_without_validating_inactive_prerequisites(tmp_path, monkeypatch):
    env_file = _env_file(
        tmp_path,
        DEEPSEEK_MODEL="",
        SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS="not-a-number",
    )
    settings = SupplierSemanticsSettings(_env_file=env_file)
    assert settings.enabled is False
    monkeypatch.setattr(config, "get_supplier_semantics_settings", lambda: settings)
    assert config.supplier_polarity_sidecar_config().state is SupplierPolaritySidecarState.DISABLED


def test_explicit_off_survives_broken_inactive_prerequisites(tmp_path, monkeypatch):
    env_file = _env_file(
        tmp_path,
        SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED="false",
        DEEPSEEK_MODEL="",
        SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS="not-a-number",
    )
    settings = SupplierSemanticsSettings(_env_file=env_file)
    assert settings.enabled is False
    monkeypatch.setattr(config, "get_supplier_semantics_settings", lambda: settings)
    assert config.supplier_polarity_sidecar_config().state is SupplierPolaritySidecarState.DISABLED


def test_environment_lowercase_on_overrides_dotenv_off(tmp_path, monkeypatch):
    env_file = _env_file(
        tmp_path,
        SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED="false",
        DEEPSEEK_API_KEY="file-key",
    )
    monkeypatch.setenv("secoppal_a2_supplier_polarity_sidecar_enabled", "true")
    settings = SupplierSemanticsSettings(_env_file=env_file)
    assert settings.enabled is True
    assert settings.api_key == "file-key"


def test_environment_off_overrides_dotenv_on(tmp_path, monkeypatch):
    env_file = _env_file(
        tmp_path,
        SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED="true",
        DEEPSEEK_API_KEY="file-key",
    )
    monkeypatch.setenv("SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED", "false")
    assert SupplierSemanticsSettings(_env_file=env_file).enabled is False


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({}, "missing sidecar credential"),
        ({"DEEPSEEK_API_KEY": "key", "DEEPSEEK_MODEL": ""}, "empty sidecar model"),
        (
            {
                "DEEPSEEK_API_KEY": "key",
                "SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS": "invalid",
            },
            "valid number",
        ),
    ],
)
def test_on_rejects_each_unavailable_prerequisite(tmp_path, values, message):
    env_file = _env_file(
        tmp_path,
        SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED="true",
        **values,
    )
    with pytest.raises(ValidationError, match=message):
        SupplierSemanticsSettings(_env_file=env_file)


def test_dotenv_on_ignores_unrelated_invalid_global_setting(tmp_path, monkeypatch):
    env_file = _env_file(
        tmp_path,
        SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED="true",
        DEEPSEEK_API_KEY="sidecar-key",
        DEEPSEEK_MODEL="sidecar-model",
        SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS="3.5",
        SECOP_TIMEOUT_SECONDS="not-an-integer",
    )
    settings = SupplierSemanticsSettings(_env_file=env_file)
    monkeypatch.setattr(config, "get_supplier_semantics_settings", lambda: settings)
    monkeypatch.setattr(config, "get_settings", lambda: (_ for _ in ()).throw(RuntimeError()))
    snapshot = config.supplier_polarity_sidecar_config()
    assert snapshot.state is SupplierPolaritySidecarState.ENABLED
    assert (snapshot.api_key, snapshot.model, snapshot.timeout_seconds) == (
        "sidecar-key", "sidecar-model", 3.5,
    )


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"DEEPSEEK_API_KEY": "key", "DEEPSEEK_MODEL": ""},
        {
            "DEEPSEEK_API_KEY": "key",
            "SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS": "invalid",
        },
    ],
)
def test_on_with_invalid_prerequisite_snapshots_as_unavailable(tmp_path, monkeypatch, values):
    env_file = _env_file(
        tmp_path,
        SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED="true",
        **values,
    )
    monkeypatch.setattr(
        config,
        "get_supplier_semantics_settings",
        lambda: SupplierSemanticsSettings(_env_file=env_file),
    )
    snapshot = config.supplier_polarity_sidecar_config()
    assert snapshot.state is SupplierPolaritySidecarState.ENABLED_BUT_UNAVAILABLE


def test_own_construction_error_is_unavailable(monkeypatch):
    def broken():
        raise RuntimeError("sidecar settings failed")

    monkeypatch.setattr(config, "get_supplier_semantics_settings", broken)
    snapshot = config.supplier_polarity_sidecar_config()
    assert snapshot.state is SupplierPolaritySidecarState.ENABLED_BUT_UNAVAILABLE
    assert snapshot.detail == "CONFIGURATION_UNAVAILABLE"


def test_dedicated_cache_is_isolated_and_can_be_cleared(monkeypatch):
    monkeypatch.setenv("SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED", "false")
    first = config.get_supplier_semantics_settings(_env_file=None)
    monkeypatch.setenv("SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED", "true")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "key")
    assert config.get_supplier_semantics_settings(_env_file=None) is first
    config.get_supplier_semantics_settings.cache_clear()
    second = config.get_supplier_semantics_settings(_env_file=None)
    assert second is not first
    assert second.enabled is True

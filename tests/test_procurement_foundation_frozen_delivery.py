"""Historical provenance is independent of later checkout bytes and Git failures."""
import hashlib
from pathlib import Path
import subprocess

from hypothesis import given, settings, strategies as st
import pytest

from app.core.procurement_relationships import load
from scripts.audits.relationship_foundation import build_registry, frozen_delivery, gates


@pytest.mark.parametrize("path", build_registry.LOCAL_SOURCES)
def test_local_digest_matches_frozen_git_blob_and_registry(path):
    blob = subprocess.check_output(
        ["git", "show", f"{gates.FOUNDATION_HEAD}:{path}"], cwd=gates.ROOT,
    )
    expected = load()["source_authorities"]["local_digests"][path]
    assert hashlib.sha256(blob).hexdigest() == expected
    assert build_registry.sources()["local_digests"][path] == expected


@settings(max_examples=12, derandomize=True, deadline=None)
@given(later_bytes=st.binary(max_size=256))
def test_later_source_bytes_cannot_change_historical_provenance(later_bytes):
    expected = load()["source_authorities"]
    current_sources = {gates.ROOT / p for p in build_registry.LOCAL_SOURCES}
    read_bytes = Path.read_bytes
    current_reads = []

    def later_checkout(path):
        if path in current_sources:
            current_reads.append(path)
            return later_bytes
        return read_bytes(path)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Path, "read_bytes", later_checkout)
        actual = build_registry.sources()
    assert actual == expected
    assert current_reads == []


def test_source_reads_use_only_exact_foundation_ref(monkeypatch):
    check_output = subprocess.check_output
    commands = []

    def record(command, **kwargs):
        commands.append((command, kwargs))
        return check_output(command, **kwargs)

    monkeypatch.setattr(subprocess, "check_output", record)
    build_registry.sources()
    assert commands == [
        (["git", "show", f"{gates.FOUNDATION_HEAD}:{path}"], {"cwd": gates.ROOT})
        for path in build_registry.LOCAL_SOURCES
    ]


@pytest.mark.parametrize("missing_git", [False, True])
def test_historical_git_failure_never_falls_back_to_checkout(monkeypatch, missing_git):
    commands = []

    def fail(command, **kwargs):
        commands.append(command)
        if missing_git:
            raise FileNotFoundError("git unavailable")
        raise subprocess.CalledProcessError(128, command, stderr=b"missing historical blob")

    monkeypatch.setattr(subprocess, "check_output", fail)
    with pytest.raises(FileNotFoundError if missing_git else subprocess.CalledProcessError):
        build_registry.sources()
    assert commands == [["git", "show", f"{gates.FOUNDATION_HEAD}:{build_registry.LOCAL_SOURCES[0]}"]]


@pytest.mark.parametrize("missing", ["ref", "blob"])
def test_real_missing_historical_object_fails_despite_existing_checkout(monkeypatch, missing):
    path = "app/core/actor_identity.py"
    if missing == "ref":
        monkeypatch.setattr(frozen_delivery, "FOUNDATION_HEAD", "0" * 40)
    else:
        # This file exists now but did not exist at the frozen delivery.
        path = "app/core/actor_identity_soql.py"
        monkeypatch.setattr(frozen_delivery, "LOCAL_SOURCES", (path,))
    assert (gates.ROOT / path).is_file()
    with pytest.raises(subprocess.CalledProcessError) as error:
        frozen_delivery.read_source_blob(path)
    assert error.value.returncode == 128


def test_unaccredited_path_is_rejected_before_git(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("unaccredited path reached Git")

    monkeypatch.setattr(subprocess, "check_output", forbidden)
    with pytest.raises(ValueError, match="not a Foundation local source"):
        frozen_delivery.read_source_blob("app/core/actor_identity_soql.py")

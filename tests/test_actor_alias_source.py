"""La adquisición AP3-B es acotada, por lotes y agregada en servidor."""
from __future__ import annotations

import pytest

from app.data.actor_alias_source import acquire_actor_alias_source


class _FakeSocrata:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def get(self, dataset_id: str, **kwargs):
        self.calls.append((dataset_id, kwargs))
        if kwargs.get("order") == ":id":
            if "Cédula" in kwargs["where"]:
                return [
                    {"tipodocproveedor": "Cédula de Ciudadanía", "documento_proveedor": "1140835135"},
                    {"tipodocproveedor": "Cédula de Ciudadanía", "documento_proveedor": "1140835135"},
                ]
            return [
                {"tipodocproveedor": "NIT", "documento_proveedor": "900111222"},
                {"tipodocproveedor": "NIT", "documento_proveedor": "900111222"},
                {"tipodocproveedor": "NIT", "documento_proveedor": "900333444"},
            ]
        return []


def test_source_policy_never_requests_an_unbounded_or_raw_contract_scan():
    client = _FakeSocrata()
    snapshot = acquire_actor_alias_source(
        client,
        anchor_per_type=3,
        selected_per_type=1,
        batch_size=2,
        seed=7,
    )

    anchor_calls = [kwargs for _, kwargs in client.calls if kwargs.get("order") == ":id"]
    aggregate_calls = [kwargs for _, kwargs in client.calls if kwargs.get("group")]
    assert len(anchor_calls) == 2
    assert all(call["limit"] == 3 for call in anchor_calls)
    assert all(call["select"].startswith(":id,") for call in anchor_calls)
    assert aggregate_calls
    assert all(" IN (" in call["where"] for call in aggregate_calls)
    assert all("count(*) AS occurrences" in call["select"] for call in aggregate_calls)
    assert all(call["limit"] <= 10_000 for call in aggregate_calls)
    assert snapshot.acquisition["global_group_scan"] is False
    assert snapshot.acquisition["raw_contract_download"] is False
    assert snapshot.acquisition["targeted_batches"] == len(aggregate_calls)


@pytest.mark.parametrize(
    ("kwargs", "name"),
    [
        ({"anchor_per_type": 100_001}, "anchor_per_type"),
        ({"selected_per_type": 10_001}, "selected_per_type"),
        ({"batch_size": 101}, "batch_size"),
        ({"batch_size": 0}, "batch_size"),
    ],
)
def test_source_policy_enforces_hard_bounds_before_network(kwargs, name):
    client = _FakeSocrata()
    with pytest.raises(ValueError, match=name):
        acquire_actor_alias_source(client, **kwargs)
    assert client.calls == []


# ---------------------------------------------------------------------------
# AP3-E — recorrido completo de la fuente.
# ---------------------------------------------------------------------------


class _DirectoryClient:
    """Fuente falsa que agrupa y pagina como lo hace Socrata.

    Reproduce el contrato que importa: ``count(*)`` bajo el mismo predicado,
    orden total por (documento, tipo, nombre) y páginas por ``offset``.
    """

    def __init__(self, groups, *, count_rows=None, drop_offsets=(), repeat_offsets=()):
        self.groups = list(groups)
        self._count_rows = (
            count_rows if count_rows is not None
            else sum(int(g["occurrences"]) for g in groups)
        )
        self.drop_offsets = set(drop_offsets)
        self.repeat_offsets = set(repeat_offsets)
        self.calls = []

    def get(self, dataset_id, **kwargs):
        self.calls.append(kwargs)
        if kwargs.get("select", "").startswith("count(*)"):
            return [{"n": str(self._count_rows)}]
        ordered = sorted(
            self.groups,
            key=lambda g: (
                g["documento_proveedor"], g["tipodocproveedor"], g["proveedor_adjudicado"],
            ),
        )
        offset = int(kwargs.get("offset", 0) or 0)
        limit = int(kwargs["limit"])
        page = ordered[offset:offset + limit]
        if offset in self.drop_offsets and page:
            page = page[:-1]          # simula un hueco
        if offset in self.repeat_offsets and page:
            page = page + [page[0]]   # simula un grupo repetido
        return page


def _group(document, name, *, source_type="NIT", occurrences=1):
    return {
        "tipodocproveedor": source_type,
        "documento_proveedor": document,
        "proveedor_adjudicado": name,
        "occurrences": str(occurrences),
        "first_seen": "2024-01-01T00:00:00.000",
        "last_seen": "2026-01-01T00:00:00.000",
    }


def _sample_groups(n=25):
    return [
        _group(f"9001000{index:02d}", f"PROVEEDOR {index}", occurrences=index % 4 + 1)
        for index in range(n)
    ]


def test_directory_traversal_visits_every_group_across_pages():
    from app.data.actor_alias_source import acquire_actor_alias_directory

    groups = _sample_groups(25)
    client = _DirectoryClient(groups)
    snapshot = acquire_actor_alias_directory(client, page_size=10)

    assert len(snapshot.rows) == len(groups)
    assert snapshot.acquisition["complete"] is True
    assert snapshot.acquisition["reconciled"] is True
    assert snapshot.acquisition["pages"] >= 3


def test_directory_traversal_reconciles_against_the_source_count():
    """La suma de ocurrencias debe igualar el count(*) de la fuente.

    Es la única comprobación que detecta un HUECO: un grupo omitido no
    aparece en ninguna página, así que contar duplicados no lo vería.
    """
    from app.data.actor_alias_source import acquire_actor_alias_directory

    groups = _sample_groups(25)
    client = _DirectoryClient(groups)
    snapshot = acquire_actor_alias_directory(client, page_size=10)
    assert snapshot.acquisition["observed_contract_rows"] == sum(
        int(g["occurrences"]) for g in groups
    )


def test_a_gap_between_pages_fails_instead_of_returning_a_partial_directory():
    from app.data.actor_alias_source import (
        DirectoryTraversalError,
        acquire_actor_alias_directory,
    )

    client = _DirectoryClient(_sample_groups(25), drop_offsets={10})
    with pytest.raises(DirectoryTraversalError):
        acquire_actor_alias_directory(client, page_size=10)


def test_a_repeated_group_is_counted_once_and_reported():
    """Un grupo repetido entre páginas no puede inflar las ocurrencias."""
    from app.data.actor_alias_source import acquire_actor_alias_directory

    groups = _sample_groups(25)
    client = _DirectoryClient(groups, repeat_offsets={10})
    snapshot = acquire_actor_alias_directory(client, page_size=10)

    assert snapshot.acquisition["duplicate_group_keys"] >= 1
    assert len(snapshot.rows) == len(groups)
    assert snapshot.acquisition["reconciled"] is True


def test_traversal_uses_a_total_deterministic_order():
    from app.data.actor_alias_source import acquire_actor_alias_directory

    client = _DirectoryClient(_sample_groups(12))
    acquire_actor_alias_directory(client, page_size=5)
    page_calls = [c for c in client.calls if not c.get("select", "").startswith("count(*)")]
    assert page_calls
    for call in page_calls:
        assert call["order"] == (
            "documento_proveedor,tipodocproveedor,proveedor_adjudicado"
        )


def test_traversal_never_downloads_raw_contracts():
    from app.data.actor_alias_source import acquire_actor_alias_directory

    client = _DirectoryClient(_sample_groups(12))
    snapshot = acquire_actor_alias_directory(client, page_size=5)
    assert snapshot.acquisition["raw_contract_download"] is False
    for call in client.calls:
        if call.get("select", "").startswith("count(*)"):
            continue
        assert "group" in call and "count(*)" in call["select"]


def test_traversal_without_a_source_count_declares_itself_unreconciled():
    """Sin denominador el recorrido sigue, pero no afirma completitud."""
    from app.data.actor_alias_source import acquire_actor_alias_directory

    class _NoCount(_DirectoryClient):
        def get(self, dataset_id, **kwargs):
            if kwargs.get("select", "").startswith("count(*)"):
                raise RuntimeError("count unsupported")
            return super().get(dataset_id, **kwargs)

    snapshot = acquire_actor_alias_directory(_NoCount(_sample_groups(8)), page_size=4)
    assert snapshot.acquisition["reconciled"] is False
    assert snapshot.acquisition["expected_contract_rows"] is None


def test_a_transient_page_failure_is_retried_not_fatal():
    """Un corte de red no puede costar el recorrido entero.

    El primer build real murió en la página 25, con 1,2 M de grupos ya
    recorridos, por un read timeout.
    """
    from app.data.actor_alias_source import acquire_actor_alias_directory

    class _Flaky(_DirectoryClient):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.failures = 0

        def get(self, dataset_id, **kwargs):
            if kwargs.get("offset") == 10 and self.failures < 2:
                self.failures += 1
                raise TimeoutError("read timed out")
            return super().get(dataset_id, **kwargs)

    client = _Flaky(_sample_groups(25))
    snapshot = acquire_actor_alias_directory(
        client, page_size=10, sleep=lambda _seconds: None,
    )
    assert client.failures == 2
    assert len(snapshot.rows) == 25
    assert snapshot.acquisition["reconciled"] is True


def test_a_page_that_never_succeeds_fails_the_whole_build():
    """Agotados los reintentos se falla: un hueco silencioso borra actores."""
    from app.data.actor_alias_source import (
        DirectoryTraversalError,
        acquire_actor_alias_directory,
    )

    class _Broken(_DirectoryClient):
        def get(self, dataset_id, **kwargs):
            if kwargs.get("offset") == 10:
                raise TimeoutError("read timed out")
            return super().get(dataset_id, **kwargs)

    with pytest.raises(DirectoryTraversalError):
        acquire_actor_alias_directory(
            _Broken(_sample_groups(25)), page_size=10, sleep=lambda _seconds: None,
        )

from app.core.estado_families import ESTADO_FAMILIES, detect_estado_family


def test_open_process_state_catalog_matches_b0_audit() -> None:
    values = ESTADO_FAMILIES["oferta_abierta"]["procesos"][
        "estado_del_procedimiento"
    ]

    assert values == ["Publicado", "Borrador", "Abierto"]


def test_open_process_state_catalog_excludes_non_open_real_states() -> None:
    values = set(
        ESTADO_FAMILIES["oferta_abierta"]["procesos"][
            "estado_del_procedimiento"
        ]
    )

    assert not {
        "Seleccionado",
        "Evaluación",
        "Cancelado",
        "Aprobado",
        "En aprobación",
        "Suspendido",
    } & values


def test_publicados_maps_to_open_process_family() -> None:
    resolved = detect_estado_family(
        "procesos publicados de mantenimiento",
        "procesos",
    )

    assert resolved is not None
    assert resolved["family"] == "oferta_abierta"
    assert resolved["filters"]["estado_del_procedimiento"] == [
        "Publicado",
        "Borrador",
        "Abierto",
    ]

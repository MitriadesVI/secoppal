"""Bloque G — contrato de la agrupación neutral de resultados.

Estas pruebas son el detector de que agrupar NO cambia lo que el usuario ve.
La afirmación central es la equivalencia con el dedupe heredado: si un cambio
en ``result_grouping`` alterara los representantes o su orden, la paridad del
modo ``off`` se rompería sin que ninguna otra suite lo notara.
"""
from __future__ import annotations

import pytest

from app.core.phrase_ranking import (
    _dedupe_results,
)
from app.core.result_grouping import (
    Occurrence,
    ResultGroup,
    WindowOrigin,
    group_from_dict,
    group_results,
    group_to_dict,
    grouping_metrics,
    groups_from_dicts,
    groups_to_dicts,
    merge_group_sequences,
    representatives,
)

PROC = "p6dx-8zbt"
CONTR = "jbjy-vk9h"


def _proc(pid: str, **extra) -> dict:
    return {"id_del_proceso": pid, **extra}


def _contr(cid: str, **extra) -> dict:
    return {"id_contrato": cid, **extra}


# ── Reglas fundacionales ────────────────────────────────────────────────────


def test_una_fila_produce_un_grupo_singleton():
    (group,) = group_results([_proc("A")], PROC)
    assert group.stable_key == "A"
    assert group.identity_status == "stable"
    assert group.physical_row_count == 1
    assert group.fanout_status == "single"
    assert group.member_rows == (group.representative_row,)


def test_filas_duplicadas_forman_un_grupo_con_fanout():
    rows = [_proc("A", n=1), _proc("A", n=2), _proc("A", n=3)]
    (group,) = group_results(rows, PROC)
    assert group.physical_row_count == 3
    assert group.fanout_status == "fanout"
    # El representante es el PRIMERO del orden canónico, no uno cualquiera.
    assert group.representative_row is rows[0]
    assert list(group.member_rows) == rows


def test_el_orden_de_grupos_es_el_orden_de_sus_representantes():
    rows = [_proc("A"), _proc("B"), _proc("A"), _proc("C"), _proc("B")]
    groups = group_results(rows, PROC)
    assert [g.stable_key for g in groups] == ["A", "B", "C"]


def test_fila_sin_identidad_forma_grupo_singleton():
    rows = [_proc("A"), {"sin": "identidad"}]
    groups = group_results(rows, PROC)
    assert len(groups) == 2
    assert groups[1].stable_key is None
    assert groups[1].identity_status == "unstable"
    assert groups[1].physical_row_count == 1


def test_dos_filas_sin_identidad_nunca_se_fusionan():
    """Regla 5 — el invariante más importante del bloque.

    Ni siquiera dos filas idénticas campo por campo se agrupan si no tienen
    identidad: sin clave no hay evidencia de que sean el mismo hecho.
    """
    rows = [{"objeto": "aseo"}, {"objeto": "aseo"}]
    groups = group_results(rows, PROC)
    assert len(groups) == 2
    assert all(g.stable_key is None for g in groups)
    assert all(g.physical_row_count == 1 for g in groups)


def test_una_fila_sin_identidad_conserva_su_posicion():
    rows = [_proc("A"), {"sin": "id"}, _proc("A"), _proc("B")]
    groups = group_results(rows, PROC)
    assert [g.stable_key for g in groups] == ["A", None, "B"]
    assert groups[0].physical_row_count == 2


def test_dataset_desconocido_no_fusiona_nada():
    """Falla cerrado: sin autoridad de identidad, cada fila es su propio grupo."""
    rows = [_proc("A"), _proc("A")]
    groups = group_results(rows, "dataset-inventado")
    assert len(groups) == 2
    assert all(g.identity_status == "unstable" for g in groups)


def test_procesos_y_contratos_no_comparten_espacio_de_nombres():
    """La misma cadena en dos datasets son dos identidades distintas."""
    proc_groups = group_results([_proc("X")], PROC)
    contr_groups = group_results([_contr("X")], CONTR)
    assert proc_groups[0].stable_key == contr_groups[0].stable_key == "X"
    assert proc_groups[0].dataset_id != contr_groups[0].dataset_id


def test_agrupar_no_muta_las_filas_fuente():
    rows = [_proc("A"), _proc("A")]
    antes = [dict(r) for r in rows]
    group_results(rows, PROC)
    assert rows == antes


def test_entrada_vacia_produce_cero_grupos():
    assert group_results([], PROC) == ()
    assert group_results(None, PROC) == ()


def test_fallback_a_referencia_cuando_falta_el_id_primario():
    rows = [
        {"referencia_del_proceso": "REF-1"},
        {"referencia_del_proceso": "REF-1"},
    ]
    (group,) = group_results(rows, PROC)
    assert group.stable_key == "REF-1"
    assert group.identity_field == "referencia_del_proceso"
    assert group.physical_row_count == 2


# ── Equivalencia con el dedupe heredado (paridad off) ───────────────────────


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [_proc("A")],
        [_proc("A"), _proc("A")],
        [_proc("A"), _proc("B"), _proc("A")],
        [_proc("A"), {"sin": "id"}, _proc("A")],
        [{"sin": "id"}, {"sin": "id"}],
        [_proc(""), _proc("A")],
    ],
)
def test_representantes_coinciden_con_el_dedupe_heredado(rows):
    """La librería dormida conserva paridad para filas válidas soportadas."""
    esperado = _dedupe_results(list(rows), PROC)
    obtenido = representatives(group_results(rows, PROC))
    assert obtenido == esperado


def test_ninguna_fila_fisica_se_pierde_al_agrupar():
    rows = [_proc("A"), _proc("A"), _proc("B"), {"sin": "id"}]
    groups = group_results(rows, PROC)
    total = sum(g.physical_row_count for g in groups)
    assert total == len(rows)


# ── Métricas ────────────────────────────────────────────────────────────────


def test_metricas_de_grouping():
    rows = [_proc("A"), _proc("A"), _proc("A"), _proc("B"), {"sin": "id"}]
    metrics = grouping_metrics(group_results(rows, PROC))
    assert metrics == {
        "physical_row_count": 5,
        "unique_group_count": 3,
        "duplicate_physical_rows": 2,
        "unstable_identity_count": 1,
        "max_physical_fanout": 3,
    }


def test_metricas_de_secuencia_vacia():
    assert grouping_metrics(())["max_physical_fanout"] == 0


# ── Serialización (contrato del buffer) ─────────────────────────────────────


def test_roundtrip_conserva_fanout_e_identidad():
    rows = [_proc("A", n=1), _proc("A", n=2)]
    (original,) = group_results(rows, PROC)
    recuperado = group_from_dict(group_to_dict(original))
    assert recuperado.stable_key == original.stable_key
    assert recuperado.physical_row_count == original.physical_row_count
    assert recuperado.fanout_status == "fanout"
    assert recuperado.identity_reason_codes == original.identity_reason_codes
    assert list(recuperado.member_rows) == list(original.member_rows)


def test_roundtrip_de_grupo_inestable():
    (original,) = group_results([{"sin": "id"}], PROC)
    recuperado = group_from_dict(group_to_dict(original))
    assert recuperado.stable_key is None
    assert recuperado.identity_status == "unstable"


def test_grupo_serializado_sin_miembros_se_rechaza():
    payload = group_to_dict(group_results([_proc("A")], PROC)[0])
    payload["members"] = []
    with pytest.raises(ValueError):
        group_from_dict(payload)


def test_grupo_serializado_con_occurrence_key_repetida_se_rechaza():
    """Dos ocurrencias con la misma clave harían indistinguibles dos hechos."""
    payload = group_to_dict(group_results([_proc("A"), _proc("A")], PROC)[0])
    payload["members"][1]["occurrence_key"] = payload["members"][0]["occurrence_key"]
    with pytest.raises(ValueError, match="occurrence_key"):
        group_from_dict(payload)


def test_grupo_estable_sin_clave_se_rechaza():
    payload = group_to_dict(group_results([_proc("A")], PROC)[0])
    payload["stable_key"] = None
    with pytest.raises(ValueError):
        group_from_dict(payload)


def test_grupo_inestable_con_clave_se_rechaza():
    payload = group_to_dict(group_results([{"sin": "id"}], PROC)[0])
    payload["stable_key"] = "inventada"
    with pytest.raises(ValueError):
        group_from_dict(payload)


def test_estado_de_identidad_invalido_se_rechaza():
    payload = group_to_dict(group_results([_proc("A")], PROC)[0])
    payload["identity_status"] = "quizas"
    with pytest.raises(ValueError):
        group_from_dict(payload)


def test_el_conteo_persistido_no_manda_sobre_los_miembros():
    """Un conteo que no cuadra con los miembros se ignora, no se cree."""
    payload = group_to_dict(group_results([_proc("A")], PROC)[0])
    payload["physical_row_count"] = 99
    assert group_from_dict(payload).physical_row_count == 1


def test_roundtrip_de_secuencia_completa():
    rows = [_proc("A"), _proc("A"), _proc("B"), {"sin": "id"}]
    groups = group_results(rows, PROC)
    recuperados = groups_from_dicts(groups_to_dicts(groups))
    assert [g.stable_key for g in recuperados] == [g.stable_key for g in groups]
    assert [g.physical_row_count for g in recuperados] == [
        g.physical_row_count for g in groups
    ]


# ── Ocurrencias: multiplicidad física ───────────────────────────────────────


def test_dos_filas_identicas_en_la_misma_ventana_son_dos_ocurrencias():
    """G.1 — el defecto que motivó esta revisión.

    Dos filas idénticas campo por campo bajo la misma identidad son dos hechos
    físicos distintos que SECOP devolvió. Colapsarlas perdía una ocurrencia
    real y falseaba el fan-out hacia abajo.
    """
    rows = [_proc("A", n=1), _proc("A", n=1)]
    (group,) = group_results(rows, PROC)
    assert group.physical_row_count == 2
    assert group.fanout_status == "fanout"
    assert len(set(group.occurrence_keys)) == 2


def test_cada_ocurrencia_tiene_clave_distinta_dentro_de_la_ventana():
    rows = [_proc("A"), _proc("A"), _proc("B"), {"sin": "id"}, {"sin": "id"}]
    groups = group_results(rows, PROC)
    todas = [k for g in groups for k in g.occurrence_keys]
    assert len(todas) == len(rows)
    assert len(set(todas)) == len(rows)


def test_la_misma_ventana_produce_siempre_las_mismas_claves():
    rows = [_proc("A"), _proc("A"), _proc("B")]
    origin = WindowOrigin("fp", PROC, 50)
    primera = group_results(rows, PROC, origin=origin)
    segunda = group_results(rows, PROC, origin=origin)
    assert [g.occurrence_keys for g in primera] == [g.occurrence_keys for g in segunda]


def test_ventanas_distintas_producen_claves_distintas():
    rows = [_proc("A")]
    v1 = group_results(rows, PROC, origin=WindowOrigin("fp", PROC, 0))
    v2 = group_results(rows, PROC, origin=WindowOrigin("fp", PROC, 50))
    assert v1[0].occurrence_keys != v2[0].occurrence_keys


def test_el_representante_es_la_primera_ocurrencia_por_construccion():
    rows = [_proc("A", n=1), _proc("A", n=2)]
    (group,) = group_results(rows, PROC)
    assert group.representative_row is group.members[0].row
    assert group.representative_row == {"id_del_proceso": "A", "n": 1}


# ── Merge entre ventanas ────────────────────────────────────────────────────


def _ventana(rows, offset):
    return group_results(rows, PROC, origin=WindowOrigin("fp", PROC, offset))


def test_merge_suma_miembros_de_una_ventana_posterior():
    """El fan-out que llega en la ventana siguiente engorda su grupo."""
    merged = merge_group_sequences(
        _ventana([_proc("A", n=1)], 0), _ventana([_proc("A", n=2)], 50), limit=100,
    )
    assert len(merged) == 1
    assert merged[0].physical_row_count == 2
    # El representante NO cambia: la primera página ya lo mostró.
    assert merged[0].representative_row == {"id_del_proceso": "A", "n": 1}


def test_repetir_la_misma_ventana_no_infla_el_conteo():
    """Un reintento sobre la misma ventana no puede inventar ocurrencias."""
    ventana = _ventana([_proc("A", n=1), _proc("A", n=1), _proc("B")], 50)
    merged = merge_group_sequences(ventana, ventana, limit=100)
    assert [g.physical_row_count for g in merged] == [2, 1]
    assert len(merged) == 2


def test_una_ocurrencia_identica_de_otra_ventana_si_suma():
    """Misma fila, otro offset: es otra ocurrencia física y debe contarse."""
    merged = merge_group_sequences(
        _ventana([_proc("A", n=1)], 0), _ventana([_proc("A", n=1)], 50), limit=100,
    )
    assert merged[0].physical_row_count == 2


def test_merge_detecta_miembros_repetidos_tras_serializar():
    """Tras el roundtrip los dicts son objetos nuevos, pero la clave persiste."""
    ventana = _ventana([_proc("A", n=1)], 50)
    revividos = groups_from_dicts(groups_to_dicts(ventana))
    merged = merge_group_sequences(ventana, revividos, limit=100)
    assert merged[0].physical_row_count == 1


def test_merge_anexa_grupos_nuevos_al_final():
    merged = merge_group_sequences(
        _ventana([_proc("A")], 0), _ventana([_proc("B")], 50), limit=100,
    )
    assert [g.stable_key for g in merged] == ["A", "B"]


def test_merge_respeta_el_limite_de_grupos():
    merged = merge_group_sequences(
        _ventana([_proc("A")], 0), _ventana([_proc("B"), _proc("C")], 50), limit=2,
    )
    assert [g.stable_key for g in merged] == ["A", "B"]


def test_un_grupo_ya_admitido_crece_aunque_el_limite_este_lleno():
    """El límite acota GRUPOS, no ocurrencias: negarle una fila a un grupo ya
    presente falsearía su conteo físico."""
    merged = merge_group_sequences(
        _ventana([_proc("A", n=1)], 0),
        _ventana([_proc("A", n=2), _proc("B")], 50),
        limit=1,
    )
    assert len(merged) == 1
    assert merged[0].physical_row_count == 2


def test_merge_no_fusiona_inestables_entre_ventanas():
    merged = merge_group_sequences(
        _ventana([{"sin": "id"}], 0), _ventana([{"sin": "id"}], 50), limit=100,
    )
    assert len(merged) == 2
    assert all(g.stable_key is None for g in merged)


def test_repetir_una_ventana_no_duplica_los_grupos_inestables():
    """Sin clave estable, solo la ocurrencia distingue un grupo inestable de su
    reaparición. Reconocerla no es fusionarla."""
    ventana = _ventana([{"sin": "id"}, {"sin": "id"}], 50)
    merged = merge_group_sequences(ventana, ventana, limit=100)
    assert len(merged) == 2


def test_merge_no_fusiona_entre_datasets():
    merged = merge_group_sequences(
        _ventana([_proc("X")], 0),
        group_results([_contr("X")], CONTR, origin=WindowOrigin("fp", CONTR, 0)),
        limit=100,
    )
    assert len(merged) == 2


def test_procesos_y_contratos_no_comparten_occurrence_keys():
    proc = group_results([_proc("X")], PROC, origin=WindowOrigin("fp", PROC, 0))
    contr = group_results([_contr("X")], CONTR, origin=WindowOrigin("fp", CONTR, 0))
    assert set(proc[0].occurrence_keys).isdisjoint(contr[0].occurrence_keys)




def test_result_group_es_inmutable():
    (group,) = group_results([_proc("A")], PROC)
    with pytest.raises(Exception):
        group.stable_key = "otra"  # type: ignore[misc]


def test_member_rows_es_una_tupla():
    (group,) = group_results([_proc("A")], PROC)
    assert isinstance(group.member_rows, tuple)
    assert isinstance(group, ResultGroup)


def test_occurrence_es_inmutable_y_no_es_identidad():
    """La clave representa una posición observada dentro de ventana/TTL.

    No es identidad física inmutable ni jurídica. Dos posiciones observadas del
    mismo proceso tienen claves distintas, y la misma fila en otra consulta
    tendría otra clave. La identidad del resultado vive en ``stable_key``.
    """
    (group,) = group_results([_proc("A"), _proc("A")], PROC)
    primera, segunda = group.members
    assert isinstance(primera, Occurrence)
    assert primera.occurrence_key != segunda.occurrence_key
    assert group.stable_key == "A"  # una sola identidad para dos ocurrencias

    otra_consulta = group_results(
        [_proc("A")], PROC, origin=WindowOrigin("otro-fp", PROC, 0),
    )
    assert otra_consulta[0].occurrence_keys[0] != primera.occurrence_key

    with pytest.raises(Exception):
        primera.occurrence_key = "otra"  # type: ignore[misc]

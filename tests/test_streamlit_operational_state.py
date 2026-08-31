"""M2.1: el renderer Streamlit consume el resumen operativo ya formateado.

El módulo Streamlit ejecuta la aplicación al importarse. Para probar las funciones
reales sin iniciar sesión, red ni workflow, cargamos del AST únicamente los helpers
de render relevantes y los ejecutamos contra un ``st`` capturador.
"""
from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from app.core.formatter import Formatter, _estado_raw_nota
from app.core.secop_access import resolve_secop_access

# Host real de SECOP: cero filas de p6dx-8zbt y de jbjy-vk9h traen un
# `urlproceso` fuera de community.secop.gov.co (medición 2026-08-31), y
# `secop_access` solo confía en ese host para un enlace directo. Un host
# inventado haría que estos oráculos —que son sobre el ESTADO operativo—
# midieran la política de URL en vez del estado.
_URL_PROCESO = (
    "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/"
    "Index?noticeUID=CO1.NTC.1"
)
_URL_CONTRATO = (
    "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/"
    "Index?noticeUID=CO1.NTC.2"
)


REF = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
STREAMLIT_APP = Path(__file__).parents[1] / "streamlit_app.py"
RENDER_NAMES = {
    "_state_presentation",
    "_render_cards",
    "_render_detail_card",
}


class _Capture:
    def __init__(self):
        self.output: list[tuple[str, str]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def markdown(self, value, **_kwargs):
        self.output.append(("markdown", str(value)))

    def caption(self, value, **_kwargs):
        self.output.append(("caption", str(value)))

    def info(self, value, **_kwargs):
        self.output.append(("info", str(value)))

    def button(self, *_args, **_kwargs):
        return False

    def container(self, **_kwargs):
        return self

    def expander(self, *_args, **_kwargs):
        return self

    def columns(self, spec):
        count = spec if isinstance(spec, int) else len(spec)
        return [self for _ in range(count)]


def _load_renderers():
    tree = ast.parse(STREAMLIT_APP.read_text(), filename=str(STREAMLIT_APP))
    functions = [
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in RENDER_NAMES
    ]
    namespace = {
        "st": _Capture(),
        "workflow": SimpleNamespace(radar_store=None),
        "detect_test_or_duplicate_from_row": lambda _row: None,
        "_estado_raw_nota": _estado_raw_nota,
        # La autoridad real, no un doble: la vía a SECOP que pinta la tarjeta
        # tiene que ser la que corre en producción.
        "resolve_secop_access": resolve_secop_access,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(STREAMLIT_APP), "exec"), namespace)
    return namespace, namespace["st"]


def _process_card(procedure_state: str, *, phase: str | None = None) -> dict:
    row = {
        "id_del_proceso": f"CO1.NTC.{procedure_state}",
        "referencia_del_proceso": f"REF-{procedure_state}",
        "nombre_del_procedimiento": f"PROCESO {procedure_state}",
        "entidad": "ENTIDAD PRUEBA",
        "precio_base": "1000000",
        "estado_de_apertura_del_proceso": "Abierto",
        "estado_del_procedimiento": procedure_state,
        "fecha_de_publicacion_del": "2026-08-01T00:00:00.000",
        "urlproceso": {"url": _URL_PROCESO},
    }
    if phase is not None:
        row["fase"] = phase
    return Formatter().to_rows(
        [row], "p6dx-8zbt", {}, reference_instant=REF,
    )[0]


def _contract_card() -> dict:
    return Formatter().to_rows([{
        "id_contrato": "CO1.PCCNTR.1",
        "referencia_del_contrato": "C-1",
        "objeto_del_contrato": "CONTRATO BASELINE",
        "nombre_entidad": "ENTIDAD CONTRATO",
        "valor_del_contrato": "2000000",
        "estado_contrato": "En ejecucion",
        "fecha_de_firma": "2026-08-01T00:00:00.000",
        "urlproceso": _URL_CONTRATO,
    }], "jbjy-vk9h", {}, reference_instant=REF)[0]


def _render_card(row: dict) -> str:
    ns, capture = _load_renderers()
    ns["_render_cards"]([row])
    return "\n".join(value for _kind, value in capture.output)


def _render_detail(row: dict, dataset_id: str = "p6dx-8zbt") -> str:
    ns, capture = _load_renderers()
    ns["_render_detail_card"](row, dataset_id=dataset_id)
    return "\n".join(value for _kind, value in capture.output)


def test_streamlit_card_shows_selected_and_raw_note_not_green_open():
    rendered = _render_card(_process_card("Seleccionado"))
    assert "⚫ Seleccionado" in rendered
    assert "SECOP reporta apertura: Abierto" in rendered
    assert "🟢 Abierto" not in rendered


def test_streamlit_card_shows_evaluation_and_raw_note():
    rendered = _render_card(_process_card("Evaluación"))
    assert "🟠 En evaluación" in rendered
    assert "SECOP reporta apertura: Abierto" in rendered


def test_streamlit_card_shows_published_without_raw_noise():
    rendered = _render_card(_process_card("Publicado"))
    assert "🔵 Publicado" in rendered
    assert "SECOP reporta apertura" not in rendered


def test_streamlit_contract_card_keeps_raw_baseline_render():
    rendered = _render_card(_contract_card())
    assert rendered == (
        "**1.** CONTRATO BASELINE  \n"
        "🏛️ ENTIDAD CONTRATO  \n"
        "💰 $2,000,000 🔵 En ejecucion  \n"
        "📅 2026-08-01 _(Firma)_  \n"
        f"[🔗 Ver en SECOP]({_URL_CONTRATO})"
    )


def test_streamlit_legacy_fallback_keeps_raw_baseline_render():
    legacy = {
        "titulo": "LEGACY", "entidad": "E", "valor": "$1",
        "estado": "Abierto", "fecha": "", "fecha_label": "",
        "url": "", "dataset_id": "p6dx-8zbt",
    }
    rendered = _render_card(legacy)
    assert "💰 $1 🟢 Abierto" in rendered
    assert "SECOP reporta apertura" not in rendered


def test_streamlit_direct_lookup_detail_uses_operational_state():
    rendered = _render_detail(_process_card("Seleccionado"))
    assert "**Estado:** ⚫ Seleccionado" in rendered
    assert "SECOP reporta apertura: Abierto" in rendered
    assert "**Estado:** 🟢 Abierto" not in rendered


def test_streamlit_contract_detail_keeps_raw_baseline_render():
    rendered = _render_detail(_contract_card(), dataset_id="jbjy-vk9h")
    assert rendered == (
        "**Objeto:** CONTRATO BASELINE\n"
        "**ID:** `CO1.PCCNTR.1`\n"
        "**Referencia:** `C-1`\n"
        "**Estado:** 🔵 En ejecucion\n"
        "**Entidad:** ENTIDAD CONTRATO\n"
        "**Valor:** $2,000,000\n"
        "**Departamento:** \n"
        "**Ciudad:** \n"
        "**Modalidad:** \n"
        "**Fecha:** 2026-08-01 _(Firma)_\n"
        f"### 🔗 [Ver proceso completo en SECOP II]({_URL_CONTRATO})\n"
        "💡 Esto es un contrato. Busca el proceso original con su ID si lo tienes a mano."
    )

"""Independent offline source/channel/context oracles; no runtime files edited."""
from __future__ import annotations

import argparse
import copy
import difflib
import importlib.abc
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from datetime import datetime

NOW = datetime.fromisoformat("2026-09-30T12:00:00-05:00")
P, C = "p6dx-8zbt", "jbjy-vk9h"
SOURCE = {
    C: {"id_contrato": "CO1.PCCNTR.9", "objeto_del_contrato": "aseo",
        "nombre_entidad": "MUNICIPIO DE SOLEDAD", "valor_del_contrato": "9007199254740993",
        "urlproceso": None, "estado_contrato": "En ejecución", "fecha_de_firma": "2026-08-01"},
    P: {"id_del_proceso": "CO1.REQ.7", "nombre_del_procedimiento": "aseo",
        "entidad": "MUNICIPIO DE SOLEDAD", "precio_base": "1500000000", "urlproceso": None,
        "estado_del_procedimiento": "Publicado", "fecha_de_publicacion_del": "2026-09-01T00:00:00.000"},
}


def typed(v):
    if isinstance(v, dict):
        return {"type": type(v).__name__, "fields": {k: typed(x) for k, x in v.items()}}
    if isinstance(v, (list, tuple)):
        return {"type": type(v).__name__, "items": [typed(x) for x in v]}
    return {"type": type(v).__name__, "value": v}


def exact(a, b):
    return a == b and typed(a) == typed(b)


def rows(ds, n):
    key = "id_del_proceso" if ds == P else "id_contrato"
    return [dict(SOURCE[ds], **{key: f"CO1.REQ.{i}" if ds == P else f"CO1.PCCNTR.{i}"}) for i in range(n)]


class OfflineTransport:
    name = "independent_offline"
    def __init__(self, data, *, errors=(), count_value=None):
        self.data = copy.deepcopy(data)
        self.before = copy.deepcopy(self.data)
        self.errors = list(errors)
        self.count_value = count_value
        self.calls = []

    def query(self, dataset, soql, **kwargs):
        from app.core import foundation_guard as g
        self.calls.append({"operation": "query", "dataset": dataset, "soql": soql,
                           "kwargs": {k: str(v) if k == "ordering_specifier" else v for k, v in kwargs.items()},
                           "raw": getattr(g, "source_rows_raw", g.foundation_source_active)(),
                           "grant": g.foundation_source_active()})
        if self.errors:
            exc = self.errors.pop(0)
            if exc:
                raise exc
        return self.data.get(dataset, [])  # Deliberately shared: source mutation must be observable.

    def count(self, dataset, soql, **kwargs):
        from app.core import foundation_guard as g
        self.calls.append({"operation": "count", "dataset": dataset, "soql": soql,
                           "kwargs": {k: str(v) if k == "ordering_specifier" else v for k, v in kwargs.items()},
                           "raw": getattr(g, "source_rows_raw", g.foundation_source_active)(),
                           "grant": g.foundation_source_active()})
        if isinstance(self.count_value, Exception):
            raise self.count_value
        return [{"count": str(len(self.data.get(dataset, [])) if self.count_value is None else self.count_value)}]


def proposal(raw, subjects):
    from app.core.foundation_producers import decode_proposal
    requests, conditions = [], []
    for i, subject in enumerate(subjects, 1):
        noun = "procesos" if subject == "processes" else "contratos"
        rid = f"r{i}"
        start, topic = raw.index(noun), raw.index("aseo")
        requests.append({"request_id": rid, "scope_id": rid, "operation": "SEARCH", "subject": subject,
                         "universe_kind": "records",
                         "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                         "universe_evidence": {"start": start, "end": start + len(noun), "quote": noun},
                         "condition_expression": {"operator": "ATOM", "condition_id": rid + ".c"}})
        conditions.append({"condition_id": rid + ".c", "request_ids": [rid], "dimension": "TOPIC",
                           "operator": "contains", "polarity": "POSITIVE",
                           "evidence": [{"start": topic, "end": topic + 4, "quote": "aseo"}]})
    body = {"requests": requests, "conditions": conditions}
    if len(requests) == 2:
        for request in requests:
            request["scope_id"] = "shared"
        body["relations"] = [{"kind": "SHARES_SCOPE", "members": [r["request_id"] for r in requests]}]
    return decode_proposal(body, raw, arm="C")


def workflow(mp, data, *, fallback=False, mode="foundation_v3", errors=(), count_value=None, subjects=("processes",)):
    from tests.product_contract.harness import ProductHarness
    from app.core.foundation_producers import LinguisticProducer
    from app.core.secop_client import SecopClient
    from app.config import get_settings
    import app.core.formatter as fm

    def down(*args):
        raise TimeoutError("independent local provider-outage control")

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)

    mp.setattr(fm, "datetime", FixedDateTime)
    mp.setenv("SECOPPAL_NATURAL_ALPHA_MODE", "off")
    mp.setenv("SECOPPAL_SEMANTIC_INTERPRETER_MODE", "off")
    mp.setenv("SECOPPAL_OBSERVER_ENABLED", "false")
    if mode == "default":
        mp.delenv("SECOPPAL_FOUNDATION_MODE", raising=False)
    else:
        mp.setenv("SECOPPAL_FOUNDATION_MODE", mode)
    get_settings.cache_clear()
    settings = {} if mode == "default" else {"secoppal_foundation_mode": mode}
    producer = LinguisticProducer(down) if fallback else lambda raw: proposal(raw, subjects)
    h = ProductHarness(Path(tempfile.mkdtemp(prefix="r11-probe-")), mp, reference_instant=NOW,
                       today=NOW.date(), foundation_producer=producer, settings_overrides=settings)
    t = OfflineTransport(data, errors=errors, count_value=count_value)
    h.flow.secop_client = SecopClient(domain="offline.invalid", app_token=None, transport=t, timeout=15, max_retries=1)
    h.flow.formatter.max_results = 25
    return h.flow, t


def run_turn(flow, text, channel="telegram", chat="fresh"):
    return flow.run_query(text, channel=channel, chat_id=chat, today=NOW.date())


def source_probe(ds):
    import pytest
    with pytest.MonkeyPatch.context() as mp:
        flow, t = workflow(mp, {ds: [SOURCE[ds]]}, fallback=True)
        result = run_turn(flow, "procesos de aseo" if ds == P else "contratos de aseo")
        assert result["route_reason"] == "foundation_basic_fallback", "Z1 real fallback path required"
        return {"dataset": ds, "source": t.before[ds], "published": result["results"],
                "typed_source": typed(t.before[ds]), "typed_published": typed(result["results"]),
                "exact": exact(result["results"], t.before[ds]),
                "source_unchanged": exact(t.data, t.before), "cards": result["rows"],
                "card_shape": all("titulo" in c and "valor" in c for c in result["rows"]),
                "no_id_dump": "Referencia: CO1" not in result["response"], "response": result["response"],
                "calls": t.calls}


def channel_probe(fallback, channel, sizes, *, errors=(), count_value=None):
    import pytest
    subjects = ("processes", "contracts") if len(sizes) > 1 else ("processes",)
    raw = "procesos y contratos de aseo" if len(sizes) > 1 else "procesos de aseo"
    data = {ds: rows(ds, n) for ds, n in zip((P, C), sizes)}
    with pytest.MonkeyPatch.context() as mp:
        flow, t = workflow(mp, data, fallback=fallback, errors=errors, count_value=count_value, subjects=subjects)
        result = run_turn(flow, raw, channel)
        response = result["response"]
        announced = [int(x) for x in re.findall(r"[Tt]e muestro (?:los )?(\d+)", response)]
        numbered = re.findall(r"^\d+\. ", response, re.M)
        return {"fallback": fallback, "channel": channel, "sizes": sizes, "limit": 25,
                "available": sum(sizes), "cards_in_rows": len(result["rows"]),
                "numbered": len(numbered), "announced": announced,
                "response": response, "state": result.get("state"), "total": result.get("total_count"),
                "approximate": result.get("total_count_approximate"), "calls": t.calls}


def context_probe():
    from app.core import foundation_guard as g
    from app.core.secop_client import SecopClient
    import pytest
    assert not g.source_rows_raw(), "M16 fresh process entry contaminated"
    records = []
    for case in ("normal", "exception", "nested", "consecutive"):
        try:
            with g.raw_source_rows():
                assert g.source_rows_raw() and not g.foundation_source_active(), "raw context is not guard"
                if case == "exception":
                    raise ValueError("isolated context exception")
                if case == "nested":
                    with g.raw_source_rows():
                        assert g.source_rows_raw(), "nested raw active"
                    assert g.source_rows_raw(), "nested restored outer state"
        except ValueError:
            pass
        assert not g.source_rows_raw(), f"M16 reset failed after {case}"
        records.append({"case": case, "restored": True})
    t = OfflineTransport({C: [SOURCE[C]]}, count_value=-1)
    client = SecopClient(domain="offline.invalid", app_token=None, transport=t, max_retries=1)
    legacy_before = client.query(C, "SELECT * LIMIT 50")
    with g.raw_source_rows():
        raw = client.query(C, "SELECT * LIMIT 50")
        assert exact(raw, t.before[C]), "raw query exactness"
        try:
            count = client.count(C, "SELECT count(*)")
            count_error = None
        except RuntimeError as exc:
            count, count_error = None, str(exc.__cause__)
        assert count_error is None and count == -1, "raw must not enable Foundation count validation"
    legacy_after = client.query(C, "SELECT * LIMIT 50")
    assert exact(legacy_before, legacy_after), "legacy normalized after success"
    assert type(legacy_after[0]["valor_del_contrato"]) is float, "legacy money normalization"
    assert isinstance(legacy_after[0]["urlproceso"], str), "legacy URL normalization"
    try:
        with g.raw_source_rows():
            t.errors = [TimeoutError("query timeout")]
            client.query(C, "SELECT * LIMIT 50")
    except RuntimeError:
        pass
    assert not g.source_rows_raw(), "raw restored after query exception"
    assert exact(client.query(C, "SELECT * LIMIT 50"), legacy_before), "legacy after exception"
    # Existing consumers use ThreadPoolExecutor without copy_context: test that actual model.
    from concurrent.futures import ThreadPoolExecutor
    with g.raw_source_rows(), ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(g.source_rows_raw).result() is False, "thread must not inherit raw"
    import asyncio
    from fastapi.concurrency import run_in_threadpool

    def worker_turn():
        assert not g.source_rows_raw(), "real threadpool entry state"
        with g.raw_source_rows():
            assert g.source_rows_raw(), "real threadpool raw block"
        assert not g.source_rows_raw(), "real threadpool restored state"

    async def request_model():
        await run_in_threadpool(worker_turn)
        assert not g.source_rows_raw(), "async caller restored state"
        await run_in_threadpool(worker_turn)

    asyncio.run(request_model())
    # Retry succeeds under raw and releases it; suppress only the local retry sleep.
    with pytest.MonkeyPatch.context() as mp:
        import app.core.secop_client as sc
        mp.setattr(sc.time, "sleep", lambda seconds: None)
        retry_t = OfflineTransport({C: [SOURCE[C]]}, errors=[TimeoutError("retry first attempt"), None])
        retry_client = SecopClient(domain="offline.invalid", app_token=None, transport=retry_t, max_retries=2)
        with g.raw_source_rows():
            assert exact(retry_client.query(C, "SELECT * LIMIT 50"), retry_t.before[C]), "raw retry exact source"
        assert not g.source_rows_raw(), "retry restores raw"
    for i in range(2):
        with pytest.MonkeyPatch.context() as mp:
            f, transport = workflow(mp, {P: rows(P, 1)}, fallback=True,
                                    errors=(TimeoutError("fallback source failure"),) if i else ())
            run_turn(f, "procesos de aseo", chat=f"turn-{i}")
            assert not g.source_rows_raw(), "fallback turn restored raw"
            f.secop_client.transport.errors = []
            q = f.secop_client.query(P, "SELECT * LIMIT 50")
            assert type(q[0]["precio_base"]) is float, "legacy after fallback turn"
    return {"records": records, "legacy_before": typed(legacy_before), "legacy_after": typed(legacy_after),
            "count_without_grant": -1, "thread": "EXERCISED_NO_COPY_CONTEXT",
            "async": "EXERCISED_FASTAPI_RUN_IN_THREADPOOL_TWO_REQUESTS", "retry_calls": retry_t.calls,
            "restored": True, "calls": t.calls}


def parity_probe(mode):
    import pytest
    cases = []
    for channel in ("streamlit", "telegram", "whatsapp"):
        for label, ds, raw in [("process", P, "procesos de aseo"), ("contract", C, "contratos de aseo"),
                               ("money", C, "contratos de aseo por más de 100 millones"),
                               ("null_url", P, "procesos de vigilancia"), ("source_error", C, "contratos de aseo")]:
            with pytest.MonkeyPatch.context() as mp:
                f, t = workflow(mp, {ds: [SOURCE[ds]]}, mode=mode,
                                errors=[ValueError("offline source error")] * 5 if label == "source_error" else ())
                result = run_turn(f, raw, channel)
                keys = ["route_reason", "response", "rows", "results", "total_count", "soql_query", "dataset_id",
                        "state", "outcome", "outcome_reason", "query_error", "technical_reason", "result_row_count",
                        "total_count_approximate", "resolved_params", "parsed_params"]
                public = {k: result.get(k) for k in keys}
                cases.append({"case": label, "channel": channel, "mode_resolved": f.settings.secoppal_foundation_mode,
                              "public": public, "typed_public": typed(public), "calls": t.calls})
    return cases


# Source mutations are applied by an import loader, never by writing app/.
MUTATIONS = {
    "M01": ("foundation_runtime", "rows=[card for cards in cards_by_group for card in cards]", "rows=all_rows"),
    "M02": ("foundation_runtime", "results=all_rows,", "results=[card for cards in cards_by_group for card in cards],"),
    "M03": ("foundation_presentation", "reference_instant=reference_instant)", "reference_instant=None)"),
    "M04": ("formatter", "for item in results]", "for item in _dedupe_results(results, dataset_id)]"),
    "M05": ("formatter", "for item in results]", "for item in results[:self.max_results]]"),
    "M06": ("formatter", "instant = reference_instant\n        if instant is None and dataset_id", "instant = reference_instant\n        if results:\n            results[0]['zeus_mutation'] = True\n        if instant is None and dataset_id"),
    "M07": ("formatter", "for item in results]", "for item in reversed(results)]"),
    "M08": ("foundation_basic_fallback", "rows=cards, results=rows", "rows=rows, results=rows"),
    "M09": ("foundation_basic_fallback", "reference_instant=instant)", "reference_instant=None)"),
    "M10": ("foundation_basic_fallback", "lines += text_card_lines(shown, channel=ctx.channel, limit=workflow.formatter.max_results)", "lines += [f'Referencia: {r.get(\"id_del_proceso\")}' for r in rows]"),
    "M11": ("foundation_presentation", "if len(groups) > 1:", "if False:"),
    "M12": ("foundation_runtime", "result[\"response\"] = response_text(groups, cards_by_group, channel=ctx.channel,\n                                       limit=workflow.formatter.max_results)", "result[\"response\"] = '\\n'.join('Referencia: ' + str(r.get('id_del_proceso')) for r in all_rows)"),
    "M13": ("foundation_basic_fallback", "with raw_source_rows():", "with __import__('contextlib').nullcontext():"),
    "M14": ("foundation_guard", "return _GUARD.get() is not None or _RAW_ROWS.get()", "return _GUARD.get() is not None"),
    "M15": ("foundation_basic_fallback", "te muestro {len(shown)}.", "te muestro {len(cards)}."),
    "M16": ("foundation_guard", "_RAW_ROWS.reset(token)", "pass  # mutant: omit reset"),
    "A01": ("foundation_guard", "return _GUARD.get() is not None or _RAW_ROWS.get()", "return _GUARD.get() is not None and _RAW_ROWS.get()"),
    "A02": ("foundation_guard", "finally:\n        _RAW_ROWS.reset(token)", "except BaseException:\n        raise\n    _RAW_ROWS.reset(token)"),
    "A03": ("foundation_presentation", "return cards if channel in _CARD_CHANNELS else cards[:limit]", "return cards"),
    "A04": ("secop_client", "if foundation_source_active():", "if __import__('app.core.foundation_guard', fromlist=['source_rows_raw']).source_rows_raw():"),
    "A05": ("foundation_guard", "token = _RAW_ROWS.set(True)\n    try:", "_RAW_ROWS.set(True)\n    raise ValueError('mutant exception before token assignment')\n    token = _RAW_ROWS.set(True)\n    try:"),
    "A06": ("foundation_basic_fallback", "with raw_source_rows():\n            total, rows", "with __import__('contextlib').nullcontext():\n            with raw_source_rows():\n                pass\n            total, rows"),
    "A07": ("foundation_guard", "_RAW_ROWS.reset(token)", "_RAW_ROWS.set(False)"),
}


class MutationLoader(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, root, mutation, diff_path):
        module, old, new = MUTATIONS[mutation]
        self.name = "app.core." + module
        self.path = root / "app/core" / (module + ".py")
        source = self.path.read_text()
        assert old in source, f"mutation anchor absent: {mutation}"
        self.source = source.replace(old, new)
        Path(diff_path).write_text("".join(difflib.unified_diff(source.splitlines(True), self.source.splitlines(True),
                                                           fromfile=str(self.path), tofile=str(self.path) + " [in-memory]")))

    def find_spec(self, fullname, path=None, target=None):
        return importlib.util.spec_from_loader(fullname, self, origin=str(self.path)) if fullname == self.name else None

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        module.__file__ = str(self.path)
        exec(compile(self.source, str(self.path), "exec"), module.__dict__)  # noqa: S102 - frozen source overlay


def killer(mutation):
    if mutation in ("M16", "A02", "A05", "A07"):
        context_probe()
        return
    if mutation in ("M13", "M14", "A01", "A06"):
        z = source_probe(C)
        assert z["exact"], "SOURCE_EXACTNESS: fallback must preserve source values and types"
        return
    if mutation in ("M15", "A03"):
        z = channel_probe(True, "telegram", [30])
        assert z["announced"] == [z["numbered"]], "CHANNEL_TRUTH: announced equals visible cards"
        return
    if mutation == "A04":
        context_probe()
        return
    if mutation == "M11":
        from app.core.foundation_presentation import export_fields, DUAL_EXPORT_UNSUPPORTED
        assert export_fields([{"universe": "processes"}, {"universe": "contracts"}]) == {"export_notice": DUAL_EXPORT_UNSUPPORTED}, "DUAL_EXPORT: must be blocked"
        return
    import pytest
    import app.core.formatter as fm
    is_fallback = mutation in ("M08", "M09", "M10")
    with pytest.MonkeyPatch.context() as mp:
        data = {P: rows(P, 30)}
        data[P].append(copy.deepcopy(data[P][0]))
        f, t = workflow(mp, data, fallback=is_fallback)
        original_card = fm.Formatter._card
        instants = []

        def observed_card(self, item, ds, instant):
            instants.append(instant)
            return original_card(self, item, ds, instant)

        mp.setattr(fm.Formatter, "_card", observed_card)
        original_display = fm.Formatter.display_rows

        def observed_display(self, result_rows, ds, *, reference_instant=None):
            assert reference_instant == NOW, "TURN_REFERENCE_INSTANT_EXPLICIT"
            return original_display(self, result_rows, ds, reference_instant=reference_instant)

        mp.setattr(fm.Formatter, "display_rows", observed_display)
        z = run_turn(f, "procesos de aseo")
        assert z["route_reason"] == ("foundation_basic_fallback" if is_fallback else "foundation_search"), "designated runtime path"
        assert exact(z["results"], t.before[P]), "RESULTS_EXACT_SOURCE"
        assert exact(t.data, t.before), "SOURCE_IMMUTABLE"
        assert len(z["rows"]) == len(data[P]), "CARD_MULTIPLICITY_NO_LIMIT_NO_DEDUPE"
        assert all("titulo" in card for card in z["rows"]), "CARD_SHAPE"
        assert [c["id_del_proceso"] for c in z["rows"]] == [r["id_del_proceso"] for r in data[P]], "CARD_SOURCE_ORDER"
        assert instants and all(instant == NOW for instant in instants), "TURN_REFERENCE_INSTANT"
        assert "Referencia: CO1" not in z["response"] and re.search(r"^1\. ", z["response"], re.M), "VISIBLE_CARDS_NOT_ID_DUMP"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--action", choices=["source", "channels", "context", "parity", "killer"], required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--mode", default="legacy")
    parser.add_argument("--mutation")
    parser.add_argument("--control-for")
    args = parser.parse_args()
    os.chdir(args.root)
    sys.path.insert(0, str(args.root))
    if args.mutation:
        sys.meta_path.insert(0, MutationLoader(args.root, args.mutation, str(args.out) + ".diff"))
    if args.action == "source":
        output = [source_probe(ds) for ds in (C, P)]
    elif args.action == "channels":
        output = [channel_probe(fb, ch, [n]) for fb in (False, True)
                  for ch in ("streamlit", "telegram", "whatsapp") for n in (0, 1, 24, 25, 26, 30)]
        output += [channel_probe(False, ch, sizes) for ch in ("streamlit", "telegram", "whatsapp")
                   for sizes in ([30, 30], [0, 30])]
        output += [channel_probe(fb, ch, [50], count_value=value) for fb in (False, True)
                   for ch in ("streamlit", "telegram", "whatsapp") for value in (75, TimeoutError("count down"))]
        output += [channel_probe(fb, ch, [1], errors=[ValueError("offline source unavailable")])
                   for fb in (False, True) for ch in ("streamlit", "telegram", "whatsapp")]
    elif args.action == "context":
        output = context_probe()
    elif args.action == "parity":
        output = parity_probe(args.mode)
    else:
        killer(args.mutation or args.control_for)
        output = {"property_passed": True, "mutation": args.mutation, "control_for": args.control_for}
    args.out.write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps({"action": args.action, "records": len(output) if isinstance(output, list) else 1,
                      "out": str(args.out)}))


if __name__ == "__main__":
    main()

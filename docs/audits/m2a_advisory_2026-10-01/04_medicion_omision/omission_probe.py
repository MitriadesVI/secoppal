"""OFFLINE, read-only measurement: span-omission / coverage check over captured Foundation v4.2 proposals.

Run:  cd /Users/rodrigoortiz/Documents/secoppal-m1 && PYTHONDONTWRITEBYTECODE=1 \
      /Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -B <this file>

No network, no LLM, no Settings()/SecopalWorkflow(). Imports only pure functions of the m1 worktree
(host_candidates, decode_proposal, normalize_text, residual_of/material_tokens, gap_matrix.match_request).
Writes only next to this script. Real query text is never written out: aggregates, ids, token frequencies >= 3.
"""
from __future__ import annotations

import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
M1 = Path("/Users/rodrigoortiz/Documents/secoppal-m1")
DIAG = Path("/Users/rodrigoortiz/Documents/tools/secoppal-diagnostics/m1_search_gold")
HELD = Path("/Users/rodrigoortiz/Documents/secoppal-evidence/bc_h7_heldout")
sys.path.insert(0, str(M1))
sys.path.insert(0, str(M1 / "scripts/audits/m1_search"))
sys.path.insert(0, str(HERE))

import gap_matrix as gm  # noqa: E402  (stdlib-only, pure)
import lexicons  # noqa: E402
from app.core.accreditation.assembly import material_tokens as existing_material_tokens  # noqa: E402
from app.core.accreditation.assembly import residual_of  # noqa: E402
from app.core.foundation_producers import decode_proposal, host_candidates  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

import os
RES = os.environ.get("RES", "res_cond")  # res (A: all spans) | res_B (A minus whole-text spans) | res_cond (C)
MIN_FREQ = 2  # privacy floor for token frequencies taken from REAL queries


def jl(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


# ------------------------------------------------------------------------------------------ lexicons
PROTECTED = frozenset("""no ni sin excepto salvo menos mas desde hasta entre mayor menor superior inferior minimo maximo
antes despues""".split())

L1_CLOSED_MIN = frozenset("""el la los las un una unos unas lo de del al a ante bajo con contra en para por segun sobre tras
durante mediante y e o u que""".split())
L1B_CLOSED_FULL = L1_CLOSED_MIN | frozenset("""me te se le les nos mi mis tu tus su sus es son ser sea sean ha han hay esta
estan este esto esa ese cual cuales quien quienes como cuando donde todo todos toda todas otro otra otros otras cada muy
tambien ya""".split())


def fold_set(items):
    return frozenset(normalize_text(x).strip() for x in items if x and " " not in x.strip())


def build_lexicons():
    loaded, failed = {}, []
    for label, path, var in lexicons.SOURCES:
        try:
            loaded[label] = fold_set(lexicons.load(path, var))
        except Exception as exc:  # noqa: BLE001
            failed.append((label, str(exc)[:80]))
    # STOPWORDS / RESIDUAL_FUNCTION_WORDS also as the real objects (ground truth for the existing machinery)
    from app.core.accreditation.residual_language import RESIDUAL_FUNCTION_WORDS
    from app.data.stopwords import STOPWORDS
    assert loaded["STOPWORDS"] == fold_set(STOPWORDS), "AST-loaded STOPWORDS differ from the real object"
    assert loaded["RESIDUAL_FUNCTION_WORDS"] == fold_set(RESIDUAL_FUNCTION_WORDS)
    union_all = frozenset().union(*loaded.values())
    sets = {
        "L0_empty": frozenset(),
        "L1_closed_min(art/prep/conj)": L1_CLOSED_MIN,
        "L1b_closed_full(+clit/aux/interrog/quant)": L1B_CLOSED_FULL,
        "L2_STOPWORDS": loaded["STOPWORDS"],
        "L3_STOPWORDS+RESIDUAL_FW": loaded["STOPWORDS"] | loaded["RESIDUAL_FUNCTION_WORDS"],
        "L4_union_all_found": union_all | L1B_CLOSED_FULL,
    }
    sets = {k: frozenset(v - PROTECTED) for k, v in sets.items()}
    # the existing machinery exactly as shipped (comparators/negators in STOPWORDS stay ignorable): reference only
    sets["L3x_existing_as_is(comparators_ignorable)"] = loaded["STOPWORDS"] | loaded["RESIDUAL_FUNCTION_WORDS"]
    return loaded, failed, sets


# ------------------------------------------------------------------------------------------ tokens
WORD = re.compile(r"\w+", re.UNICODE)
PIECE = re.compile(r"[a-z0-9]+")


def pieces_of(raw):
    """[(start, end, piece)] over [a-z0-9]+ of the folded text; offsets are those of the raw word."""
    out = []
    for m in WORD.finditer(raw):
        for p in PIECE.findall(normalize_text(m.group())):
            out.append((m.start(), m.end(), p))
    return out


def piece_idx_for_span(pieces, start, end):
    """Piece indices overlapping >= 50% of the piece's word with [start, end)."""
    res = set()
    for i, (s, e, _) in enumerate(pieces):
        ov = min(e, end) - max(s, start)
        if ov > 0 and ov * 2 >= (e - s):
            res.add(i)
    return res


def find_subseq(hay, needle):
    n = len(needle)
    if n == 0:
        return None
    for i in range(len(hay) - n + 1):
        if hay[i:i + n] == needle:
            return (i, i + n)
    return None


# ------------------------------------------------------------------------------------------ categories (heuristic, declared)
REQ_VERB = re.compile(r"^(busc\w*|muestr\w*|mostr\w*|quier\w*|quis\w*|necesit\w*|requier\w*|dam\w*|dim\w*|list\w*|"
                      r"encuentr\w*|encontr\w*|trae\w*|traem\w*|consult\w*|revis\w*|averigu\w*|genera\w*|ver|veo|mira\w*|"
                      r"pued\w*|podri\w*|conocer|saber|sabes|ayud\w*|indic\w*|dar|deme|presenta\w*|extrae\w*|obten\w*|"
                      r"obtener|interes\w*|ofrec\w*|vend\w*|participar|postul\w*|aplicar|licitar|ofertar)$")
QUESTION = re.compile(r"^(cuant\w*|cual\w*|quien\w*|donde|como|cuando|que|porque|por)$")
UNIVERSE_NOUN = re.compile(r"^(contrat\w*|proces\w*|licitaci\w*|convocatoria\w*|oportunidad\w*|procedimiento\w*|ofertas?)$")
COURTESY = frozenset("favor porfa porfavor gracias hola holaa buenas buenos dias tardes noches amable cordial saludos".split())
FILLER = frozenset("""pues entonces ademas ey oye ok okay eh bueno asi nomas algun alguno alguna algunos algunas cualquier
cualquiera similares similar relacionado relacionados relacionada relacionadas referente respecto acerca tema temas asunto
cuyo cuya cuyos cuyas tipo todo todos toda todas otro otra otros otras""".split())
AUX = frozenset("sea sean tenga tengan tiene tienen haya hayan esten este esta estan estar ser fue fueron han ha hay existe existen".split())


def category(piece, owner):
    """Heuristic class of an uncovered piece. `owner` = gold dimension owning it, or None."""
    if piece in PROTECTED:
        return "PROTECTED_neg/comparator"
    if owner == "TIME":
        return "GOLD_CLIP:TIME(Y1)"
    if owner is not None:
        return "GOLD_CLIP:other(span mal recortado)"
    if piece.isdigit():
        return "NUMBER"
    if REQ_VERB.match(piece):
        return "REQUEST_VERB"
    if QUESTION.match(piece):
        return "QUESTION_WORD"
    if UNIVERSE_NOUN.match(piece):
        return "UNIVERSE_NOUN"
    if piece in COURTESY:
        return "COURTESY"
    if piece in AUX:
        return "AUX_CONNECTOR"
    if piece in ("y", "e", "o", "u"):
        return "COORDINATOR(y/o between cited spans)"
    if piece in FILLER:
        return "FILLER/connector-noun"
    return "OTHER"


CATEGORIES = ["PROTECTED_neg/comparator", "GOLD_CLIP:TIME(Y1)", "GOLD_CLIP:other(span mal recortado)", "NUMBER",
              "REQUEST_VERB", "QUESTION_WORD", "UNIVERSE_NOUN", "COURTESY", "AUX_CONNECTOR", "COORDINATOR(y/o between cited spans)",
              "FILLER/connector-noun", "OTHER"]


# ------------------------------------------------------------------------------------------ proposals
def decode(raw, content):
    prop = decode_proposal(json.loads(content), raw, arm="B", candidates=host_candidates(raw))
    return prop


def span_records(prop):
    """[(kind, dim, start, end)] of every span a proposal cites (kind: COND | UNIVERSE | SOURCE | REQUEST)."""
    out = []
    for r in prop.requests:
        for s in r.evidence:
            out.append(("REQUEST", "REQUEST", s.start, s.end))
        if r.universe_evidence is not None:
            s = r.universe_evidence
            out.append(("UNIVERSE", "UNIVERSE", s.start, s.end))
        if r.source_evidence is not None:
            s = r.source_evidence
            out.append(("SOURCE", "SOURCE", s.start, s.end))
    for c in prop.conditions:
        if c.role.value == "REQUESTED_UNIVERSE":  # derived by the host from universe_kind, never cited by the LLM
            continue
        for s in c.evidence:
            out.append(("COND", c.dimension.value, s.start, s.end))
    return out


def fnd_from_prop(prop):
    """Minimal `foundation` view for gap_matrix.match_request (same fields the capture holds)."""
    reqs = [{"request_id": r.request_id, "subject": r.subject} for r in prop.requests]
    conds = [{"id": c.condition_id, "request_ids": list(c.request_ids), "dimension": c.dimension.value,
              "operator": c.operator, "polarity": c.polarity.value, "role": c.role.value,
              "quotes": [s.quote for s in c.evidence], "unsupported_label": c.unsupported_label}
             for c in prop.conditions if c.role.value != "REQUESTED_UNIVERSE"]
    return {"producer_status": "PROPOSAL", "requests": reqs, "conditions": conds}


# ------------------------------------------------------------------------------------------ gold access
def gold_conditions(gold):
    """[(request, cond)] for SEARCH requests (and SEARCH jobs of MULTI)."""
    return [(req, c) for _, req in gm.gold_requests(gold) for c in req.get("conditions", [])]


def gold_owner_map(gold, pieces):
    """piece index -> gold dimension owning it (condition quote or 'UNIVERSE'); also per (req,cond) piece sets."""
    owner, per_cond = {}, {}
    ptxt = [p for _, _, p in pieces]
    for _, req in gm.gold_requests(gold):
        uq = req.get("universe_quote")
        if uq:
            span = find_subseq(ptxt, PIECE.findall(normalize_text(uq)))
            if span:
                for i in range(*span):
                    owner.setdefault(i, "UNIVERSE")
        for c in req.get("conditions", []):
            q = c.get("quote")
            span = find_subseq(ptxt, PIECE.findall(normalize_text(q or ""))) if q else None
            idxs = set(range(*span)) if span else set()
            per_cond[(req["request_id"], c["id"])] = idxs
            for i in idxs:
                owner[i] = c["dimension"]
    return owner, per_cond


def classify_rows(gold, prop_fnd):
    """Per gold SEARCH condition: status in {MATCH, OMITTED, IR_PROXY, MISPROPOSED:*}; + record flags."""
    used, rows, spurious = set(), [], 0
    subject_bad = False
    universe_uncited = False
    for _, req in gm.gold_requests(gold):
        f_req = gm.foundation_request_for(req, prop_fnd, used)
        if f_req is not None:
            used.add(f_req["request_id"])
        f_conds = [c for c in prop_fnd["conditions"] if f_req and f_req["request_id"] in c["request_ids"]]
        mapping, left = gm.match_request(req.get("conditions", []), f_conds)
        spurious += len(left)
        if req["universe"] == "UNSPECIFIED":
            universe_uncited = True
        elif f_req is None or f_req["subject"] != {"PROCESSES": "processes", "CONTRACTS": "contracts"}[req["universe"]]:
            subject_bad = True
        for g in req.get("conditions", []):
            fm, detail = mapping[g["id"]]
            invocab = gm.in_vocab(g)
            if fm is None:
                status = "OMITTED"
            elif detail:
                status = "MISPROPOSED:" + detail.split(":")[0]
            elif not invocab:
                status = "IR_PROXY"
            else:
                status = "MATCH"
            rows.append({"req": req["request_id"], "id": g["id"], "dimension": g["dimension"], "key": gm.row_key(g),
                         "invocab": invocab, "status": status})
    return rows, {"spurious": spurious, "subject_bad": subject_bad, "universe_uncited": universe_uncited}


# ------------------------------------------------------------------------------------------ per-record analysis
def analyze(gid, slice_, raw, prop, gold, lex_sets):
    pieces = pieces_of(raw)
    n = len(pieces)
    recs = span_records(prop)
    cov_all, cov_cond, cov_B = set(), set(), set()
    kinds_big, req_exact_whole = set(), False
    span_dim_idx = []  # (dim, kind, set idx)
    whole = False
    for kind, dim, s, e in recs:
        idx = piece_idx_for_span(pieces, s, e)
        span_dim_idx.append((dim, kind, idx))
        cov_all |= idx
        if kind != "REQUEST":
            cov_cond |= idx
        big = (e - s) >= 0.9 * len(raw) and len(raw) > 0
        if big:
            whole = True
            kinds_big.add(kind)
        if kind == "REQUEST" and s <= 0 and e >= len(raw.rstrip(" ?¿!.,;")):
            req_exact_whole = True
        if not big:
            cov_B |= idx
    unc_all = [i for i in range(n) if i not in cov_all]
    unc_cond = [i for i in range(n) if i not in cov_cond]
    unc_B = [i for i in range(n) if i not in cov_B]
    owner, per_cond = ({}, {}) if gold is None else gold_owner_map(gold, pieces)
    out = {"gid": gid, "slice": slice_, "n_tokens": n, "cov_all": len(cov_all) / n if n else 1.0,
           "cov_cond": len(cov_cond) / n if n else 1.0, "whole_text_span": whole, "big_span_kinds": sorted(kinds_big), "req_exact_whole": req_exact_whole,
           "n_omissions_declared": len(prop.omissions)}
    # residual by lexicon variant, for 'all spans' and 'cond+universe+source only'
    out["res"], out["res_cond"], out["res_B"] = {}, {}, {}
    for name, lex in lex_sets.items():
        out["res"][name] = [i for i in unc_all if pieces[i][2] not in lex]
        out["res_cond"][name] = [i for i in unc_cond if pieces[i][2] not in lex]
        out["res_B"][name] = [i for i in unc_B if pieces[i][2] not in lex]
    out["pieces"] = [p for _, _, p in pieces]
    out["owner"] = owner
    out["per_cond"] = per_cond
    out["unc_all"] = unc_all
    # existing machinery (assembly.residual_of + material_tokens) on the same spans
    ex = existing_material_tokens(residual_of(raw, tuple(SimpleNamespace(start=s, end=e) for _, _, s, e in recs)))
    out["existing_nonempty"] = bool(ex)
    # overlaps between spans of distinct dimensions (request-level REQUEST spans excluded by design)
    cited = [(d, idx) for d, k, idx in span_dim_idx if k != "REQUEST"]
    pairs = set()
    for a in range(len(cited)):
        for b in range(a + 1, len(cited)):
            if cited[a][0] != cited[b][0] and cited[a][1] & cited[b][1]:
                pairs.add(tuple(sorted((cited[a][0], cited[b][0]))))
    out["overlap_pairs"] = sorted(pairs)
    out["blind"] = blind_window(span_dim_idx, n)
    return out


def blind_window(span_dim_idx, n, dims=("BUYER", "ACTOR", "GEOGRAPHY")):
    """TIME cites whose neighbours (<=4 tokens each side) fall inside a BUYER/ACTOR/GEOGRAPHY span."""
    host = set()
    for d, k, idx in span_dim_idx:
        if k == "COND" and d in dims:
            host |= idx
    res = {"time_cites": 0, "within4": 0, "adjacent1": 0}
    for d, k, idx in span_dim_idx:
        if k == "COND" and d == "TIME" and idx:
            res["time_cites"] += 1
            a, b = min(idx), max(idx)
            nb4 = set(range(max(0, a - 4), a)) | set(range(b + 1, min(n, b + 5)))
            nb1 = {x for x in (a - 1, b + 1) if 0 <= x < n}
            res["within4"] += bool(nb4 & host)
            res["adjacent1"] += bool(nb1 & host)
    return res


# ------------------------------------------------------------------------------------------ oracle (Gold -> perfect proposal)
def oracle(gold, lex_sets):
    raw = gold["text"]
    pieces = pieces_of(raw)
    owner, per_cond = gold_owner_map(gold, pieces)
    cov = set(owner)
    # a request with no gold universe_quote or a condition whose quote was not found simply covers nothing
    n = len(pieces)
    unc = [i for i in range(n) if i not in cov]
    res = {name: [i for i in unc if pieces[i][2] not in lex] for name, lex in lex_sets.items()}
    # blind window on the oracle: TIME quote neighbours inside BUYER/ACTOR/GEOGRAPHY quotes
    dims_idx = []
    for (rid, cid), idx in per_cond.items():
        dim = next(c["dimension"] for _, r in gm.gold_requests(gold) if r["request_id"] == rid
                   for c in r["conditions"] if c["id"] == cid)
        dims_idx.append((dim, "COND", idx))
    cited = [(d, idx) for d, _, idx in dims_idx]
    pairs = {tuple(sorted((cited[a][0], cited[b][0]))) for a in range(len(cited)) for b in range(a + 1, len(cited))
             if cited[a][0] != cited[b][0] and cited[a][1] & cited[b][1]}
    return {"gid": gold["gold_id"], "slice": gold["slice"], "n_tokens": n, "cov_all": len(cov) / n if n else 1.0,
            "res": res, "res_cond": res, "res_B": res, "pieces": [p for _, _, p in pieces], "owner": owner, "overlap_pairs": sorted(pairs),
            "blind": blind_window(dims_idx, n)}


# ------------------------------------------------------------------------------------------ aggregation helpers
def pct(a, b):
    return round(100.0 * a / b, 1) if b else None


def dist(values):
    values = sorted(values)
    if not values:
        return {}
    q = lambda p: values[min(len(values) - 1, int(p * (len(values) - 1) + 0.5))]  # noqa: E731
    return {"n": len(values), "mean": round(statistics.mean(values), 3), "median": round(q(0.5), 3), "p10": round(q(0.1), 3),
            "p90": round(q(0.9), 3), "share_full(=1.0)": pct(sum(v >= 0.999999 for v in values), len(values))}


def token_top(counter, k=40, floor=MIN_FREQ):
    """Privacy: singletons of the open class (OTHER / gold-clip tokens may be names) are suppressed; closed classes
    (verbs, courtesy, connectors...) are shown at any frequency."""
    def ok(t, c):
        return c >= floor or category(t, None) not in ("OTHER", "GOLD_CLIP:other(span mal recortado)")
    items = [(t, c) for t, c in counter.most_common() if ok(t, c)][:k]
    return [{"token": t, "n": c} for t, c in items], sum(c for t, c in counter.items() if not ok(t, c))


def main():
    loaded, failed, lex_sets = build_lexicons()
    L_NAMES = list(lex_sets)

    # ---- lexicon table ----
    all_labels = list(loaded)
    lex_table = {"sizes": {k: len(v) for k, v in loaded.items()}, "failed_to_load": failed,
                 "pairwise_intersection": {}, "variants": {k: len(v) for k, v in lex_sets.items()},
                 "protected_never_ignorable": sorted(PROTECTED)}
    for a in all_labels:
        lex_table["pairwise_intersection"][a] = {b: len(loaded[a] & loaded[b]) for b in all_labels}
    lex_table["only_in_one_lexicon"] = {a: len(loaded[a] - frozenset().union(*[loaded[b] for b in all_labels if b != a]))
                                        for a in all_labels}
    lex_table["stopwords_minus_residualFW_examples_sample"] = sorted(loaded["RESIDUAL_FUNCTION_WORDS"] - loaded["STOPWORDS"])
    lex_table["closed_min_vs_STOPWORDS"] = {"closed_min_not_in_STOPWORDS": sorted(L1_CLOSED_MIN - loaded["STOPWORDS"]),
                                            "n_in_both": len(L1_CLOSED_MIN & loaded["STOPWORDS"])}
    lex_table["protected_inside_lexicons"] = {a: sorted(loaded[a] & PROTECTED) for a in all_labels if loaded[a] & PROTECTED}

    sw = loaded["STOPWORDS"]
    dom = {
        "months": set("enero febrero marzo abril mayo junio julio agosto septiembre octubre noviembre diciembre".split()),
        "time_words": set("ano mes meses semana semestre trimestre bimestre dia dias periodo anual mensual hoy ayer ahora actualmente pasado pasada proximo ultimo ultima ultimos ultimas anterior siguiente reciente recientes recientemente momento adelante".split()),
        "state_words": set("abierta abiertas abierto abiertos cerrado cerrados celebrado celebrados firmado firmada firmados firmadas liquidado liquidados adjudicado adjudicados vigente vigentes disponible disponibles publicadas publicados convocadas convocado convocados publica publico".split()),
        "comparator_negator_words": set(PROTECTED | set("mayores menores superiores superen supere exceda excedan rebase rebasen minimos solo solos sola solas solamente unicamente sino".split())),
        "money_value_words": set("millon millones pesos smmlv smlmv smlv salario salarios cuantia valor total totales suma suman sumaron sumo gastado costosa costosas costoso costosos barata baratas barato baratos cara caras caro caros".split()),
        "universe_nouns": set("contrato contratos proceso procesos licitacion licitaciones convocatoria convocatorias oportunidad oportunidades oferta ofertas concurso".split()),
        "actor_geo_sector_words": set("proveedor proveedores departamento departamentos provincia area sector oficial linea".split()),
    }
    sw_dom, taken = {}, set()
    for k, v in dom.items():
        hit = sorted(sw & v)
        sw_dom[k] = {"n": len(hit), "examples": hit[:14]}
        taken |= set(hit)
    verbs = sorted(t for t in sw - taken if REQ_VERB.match(t))
    taken |= set(verbs)
    sw_dom["request_verbs(regex)"] = {"n": len(verbs), "examples": verbs[:14]}
    rest = sorted(sw - taken)
    sw_dom["rest(closed class / other)"] = {"n": len(rest), "examples": rest}
    lex_table["STOPWORDS_content_split"] = {"total": len(sw), **sw_dom}

    # ---- load data ----
    gold_priv = {r["gold_id"]: r for r in jl(DIAG / "sealed_v1/gold_v1.jsonl")}
    prod1 = {r["gold_id"]: r for r in jl(DIAG / "captures/producer_raw.jsonl")}
    prod2 = {r["gold_id"]: r for r in jl(DIAG / "captures/producer_raw_rerun.jsonl")}
    caps = {r["gold_id"]: r for r in jl(DIAG / "captures/captures.jsonl")}
    gold_syn = []
    for name in ("neighbor", "minimal_pair", "adversarial"):
        gold_syn += jl(M1 / f"docs/audits/m1_search_foundation/gold/{name}.jsonl")

    notes = {"gold_private_records": len(gold_priv), "producer_pass1": len(prod1), "producer_pass2(rerun)": len(prod2),
             "synthetic_slices_with_gold_but_NO_producer_capture": len(gold_syn),
             "gold_slices_in_captured_set": dict(Counter(r["slice"] for r in gold_priv.values()))}

    def run_pass(prods, label):
        rs, errs = {}, []
        for gid, p in prods.items():
            g = gold_priv[gid]
            try:
                prop = decode(g["text"], p["raw_content"])
            except Exception as exc:  # noqa: BLE001
                errs.append((gid, type(exc).__name__))
                continue
            rs[gid] = (analyze(gid, g["slice"], g["text"], prop, g, lex_sets), prop)
        notes[f"decode_errors_{label}"] = errs
        return rs

    pass1 = run_pass(prod1, "pass1")
    pass2 = run_pass(prod2, "pass2")

    # sanity: decoded condition quotes == the capture's quotes (pass 1)
    mismatch = 0
    for gid, (_, prop) in pass1.items():
        cq = sorted(tuple(c["quotes"]) for c in caps[gid]["foundation"]["conditions"])
        pq = sorted(tuple(s.quote for s in c.evidence) for c in prop.conditions if c.role.value != "REQUESTED_UNIVERSE")
        mismatch += cq != pq
    notes["decoded_vs_capture_quote_mismatch(pass1)"] = mismatch

    # ---- classification of each record against Gold ----
    def classify(rs):
        out = {}
        for gid, (a, prop) in rs.items():
            g = gold_priv[gid]
            rows, flags = classify_rows(g, fnd_from_prop(prop))
            out[gid] = {"rows": rows, **flags}
        return out

    cls1 = classify(pass1)
    cls2 = classify(pass2)

    def population(gid):
        g = gold_priv[gid]
        return g["operation"] == "SEARCH" and g["ambiguity"] != "AMBIGUOUS"

    def record_kind(gid, cls):
        g = gold_priv[gid]
        c = cls[gid]
        if g["expected_disposition"] != "EXECUTE":
            return "NOT_EXECUTE"
        ir_limit = any(not r["invocab"] for r in c["rows"])
        if ir_limit:
            return "IR_LIMIT"
        wrong = (c["spurious"] > 0 or c["subject_bad"] or any(r["status"] != "MATCH" for r in c["rows"]))
        if wrong:
            return "PROPOSAL_WRONG"
        if c["universe_uncited"]:
            return "CORRECT_UNIVERSE_UNCITED"
        return "CLEAN_CORRECT"

    # ================= coverage (item 1) =================
    cov = {"denominators": {}, "by_slice": {}}
    for sl in ("REAL", "SYNTHETIC", "ALL"):
        ids = [g for g in pass1 if sl == "ALL" or gold_priv[g]["slice"] == sl]
        cov["by_slice"][sl] = {"n": len(ids), "cov_all_spans": dist([pass1[g][0]["cov_all"] for g in ids]),
                               "cov_cond_universe_source_only": dist([pass1[g][0]["cov_cond"] for g in ids]),
                               "cov_B_all_spans_minus_whole_text": dist([1 - len(pass1[g][0]["res_B"]["L0_empty"]) / max(1, pass1[g][0]["n_tokens"]) for g in ids]),
                               "any_span_covering_ge90pct_of_text": sum(pass1[g][0]["whole_text_span"] for g in ids),
                               "request_level_span_covering_ge90pct": sum("REQUEST" in pass1[g][0]["big_span_kinds"] for g in ids),
                               "cond_or_universe_span_covering_ge90pct": sum(bool(set(pass1[g][0]["big_span_kinds"]) - {"REQUEST"}) for g in ids),
                               "request_level_span_equal_to_whole_raw": sum(pass1[g][0]["req_exact_whole"] for g in ids),
                               "n_tokens_le_4": sum(pass1[g][0]["n_tokens"] <= 4 for g in ids),
                               "proposals_declaring_omissions": sum(pass1[g][0]["n_omissions_declared"] > 0 for g in ids),
                               "existing_machinery_vs_mine_agree_L3x(as-is)": sum(
                                   pass1[g][0]["existing_nonempty"] == bool(pass1[g][0]["res"]["L3x_existing_as_is(comparators_ignorable)"])
                                   for g in ids),
                               "existing_machinery_vs_mine_agree_L3": sum(
                                   pass1[g][0]["existing_nonempty"] == bool(pass1[g][0]["res"]["L3_STOPWORDS+RESIDUAL_FW"])
                                   for g in ids)}
    cov["request_level_spans_effect"] = {
        "records_where_cov_all_gt_cov_cond": sum(pass1[g][0]["cov_all"] > pass1[g][0]["cov_cond"] + 1e-9 for g in pass1),
        "records_where_residual_L3_differs_all_vs_cond": sum(
            bool(pass1[g][0]["res"]["L3_STOPWORDS+RESIDUAL_FW"]) != bool(pass1[g][0]["res_cond"]["L3_STOPWORDS+RESIDUAL_FW"])
            for g in pass1)}

    # ================= record kinds =================
    kinds1 = {g: record_kind(g, cls1) for g in pass1}
    kinds_count = {sl: dict(Counter(kinds1[g] for g in pass1 if sl == "ALL" or gold_priv[g]["slice"] == sl))
                   for sl in ("REAL", "SYNTHETIC", "ALL")}
    op_count = dict(Counter((gold_priv[g]["operation"], gold_priv[g]["expected_disposition"]) .__repr__() for g in pass1))

    pop_counts = defaultdict(Counter)
    for g in pass1:
        if population(g):
            pop_counts[gold_priv[g]["slice"]][kinds1[g]] += 1
            pop_counts["ALL"][kinds1[g]] += 1
    pop_counts = {k: dict(v) for k, v in pop_counts.items()}

    # ================= detection (item 3a) =================
    def detection(rs, cls, kinds_ok=None):
        """Per omitted gold condition (SEARCH, non-ambiguous): did the residual flag it?"""
        table = defaultdict(lambda: {"n": 0, **{L: {"any": 0, "attrib": 0, "attrib_ignoring_protected": 0} for L in L_NAMES}})
        tables_key = defaultdict(lambda: {"n": 0, **{L: {"any": 0, "attrib": 0} for L in L_NAMES}})
        records_with_omission = set()
        for gid in rs:
            if not population(gid):
                continue
            a = rs[gid][0]
            for r in cls[gid]["rows"]:
                if r["status"] != "OMITTED":
                    continue
                records_with_omission.add(gid)
                idxs = a["per_cond"].get((r["req"], r["id"]), set())
                for tab, key in ((table, r["dimension"]), (tables_key, r["key"])):
                    tab[key]["n"] += 1
                    tab["__ALL__"]["n"] += 1
                    for L in L_NAMES:
                        res = set(a[RES][L])
                        any_ = bool(res)
                        attrib = bool(res & idxs)
                        for k in (key, "__ALL__"):
                            tab[k][L]["any"] += any_
                            tab[k][L]["attrib"] += attrib
                        if tab is table:
                            ign = bool({i for i in res if a["pieces"][i] not in PROTECTED})
                            for k in (key, "__ALL__"):
                                tab[k][L]["attrib_ignoring_protected"] += ign
        return {"by_dimension": {k: v for k, v in table.items()}, "by_key": {k: v for k, v in tables_key.items()},
                "records_with_omitted_condition": len(records_with_omission)}

    det1 = detection(pass1, cls1)

    def geometric(rs):
        """Gold-quote geometry (independent of the stem matcher): is each gold condition's quote cited, clipped or absent?"""
        byk = defaultdict(lambda: {"n": 0, "cited": 0, "partial_clip": 0, "full_uncited": 0, "not_located": 0,
                                   "flag_partial": {L: 0 for L in L_NAMES}, "flag_full": {L: 0 for L in L_NAMES}})
        for gid in rs:
            g = gold_priv[gid]
            if not population(gid) or g["expected_disposition"] != "EXECUTE":
                continue
            a = rs[gid][0]
            unc0 = set(a[RES]["L0_empty"])
            for _, req in gm.gold_requests(g):
                for c in req.get("conditions", []):
                    idxs = a["per_cond"].get((req["request_id"], c["id"]), set())
                    for key in (gm.row_key(c), "__DIM__" + c["dimension"], "__ALL__"):
                        t = byk[key]
                        t["n"] += 1
                        if not idxs:
                            t["not_located"] += 1
                            continue
                        unc = idxs & unc0
                        if not unc:
                            t["cited"] += 1
                            continue
                        full = len(unc) == len(idxs)
                        t["full_uncited" if full else "partial_clip"] += 1
                        for L in L_NAMES:
                            if any(a["pieces"][i] not in lex_sets[L] for i in unc):
                                (t["flag_full"] if full else t["flag_partial"])[L] += 1
        return dict(byk)

    geo1 = geometric(pass1)

    # invisible-to-coverage errors: gold condition proposed but wrong
    mis = Counter()
    for gid in pass1:
        if population(gid):
            for r in cls1[gid]["rows"]:
                if r["status"].startswith("MISPROPOSED") or r["status"] == "IR_PROXY":
                    mis[(r["status"], r["dimension"])] += 1
    detection_extra = {"gold_conditions_proposed_but_wrong_or_proxy(invisible to a span-coverage check)":
                       {f"{k[0]}|{k[1]}": v for k, v in sorted(mis.items())}}
    # omitted conditions that the record-level residual flags anyway for an unrelated reason (non-attributable)
    # ================= cost (item 3b) =================
    def cost(rs, cls, kind_name, only_ids=None):
        ids = [g for g in rs if population(g) and record_kind(g, cls) == kind_name and (only_ids is None or g in only_ids)]
        n = len(ids)
        res = {}
        for L in L_NAMES:
            fp = [g for g in ids if rs[g][0][RES][L]]
            fp_prot = [g for g in fp if any(rs[g][0]["pieces"][i] in PROTECTED for i in rs[g][0][RES][L])]
            fp_noprot = [g for g in fp if any(rs[g][0]["pieces"][i] not in PROTECTED for i in rs[g][0][RES][L])]
            fp_cond = [g for g in ids if rs[g][0]["res_cond"][L]]
            # Y1: residual that disappears if tokens inside gold TIME quotes are ignored
            y1_only = [g for g in fp if not [i for i in rs[g][0][RES][L] if rs[g][0]["owner"].get(i) != "TIME"]]
            clip_only = [g for g in fp if not [i for i in rs[g][0][RES][L] if rs[g][0]["owner"].get(i) is None]]
            res[L] = {"n": n, "false_positive_records": len(fp), "fp_rate_pct": pct(len(fp), n),
                      "fp_only_protected_tokens": len(fp) - len(fp_noprot), "fp_with_protected_token": len(fp_prot),
                      "fp_rate_pct_variantC": pct(len(fp_cond), n),
                      "fp_solely_due_to_Y1(TIME clip)": len(y1_only),
                      "fp_solely_due_to_any_gold_quoted_clip": len(clip_only)}
        return {"n": n, "by_L": res, "ids_sample": ids[:0]}

    cost_clean1 = cost(pass1, cls1, "CLEAN_CORRECT")
    cost_uncited1 = cost(pass1, cls1, "CORRECT_UNIVERSE_UNCITED")
    cost_ir1 = cost(pass1, cls1, "IR_LIMIT")

    # ================= residual tokens in false positives + categories =================
    def fp_tokens(rs, ids, L):
        cnt, cat_tokens = Counter(), defaultdict(Counter)
        cat_rec = Counter()
        for g in ids:
            a = rs[g][0]
            seen_cats = set()
            for i in a[RES][L]:
                p = a["pieces"][i]
                c = category(p, a["owner"].get(i))
                cnt[p] += 1
                cat_tokens[c][p] += 1
                seen_cats.add(c)
            for c in seen_cats:
                cat_rec[c] += 1
        return cnt, cat_tokens, cat_rec

    clean_ids = [g for g in pass1 if population(g) and kinds1[g] == "CLEAN_CORRECT"]
    fp_analysis = {}
    for L in ("L1b_closed_full(+clit/aux/interrog/quant)", "L3_STOPWORDS+RESIDUAL_FW", "L4_union_all_found"):
        ids = [g for g in clean_ids if pass1[g][0][RES][L]]
        cnt, cat_tokens, cat_rec = fp_tokens(pass1, ids, L)
        top, suppressed = token_top(cnt)
        cat_of = {t: category(t, None) for t in cnt}
        cat_top = {c: token_top(cat_tokens[c], 12)[0] for c in CATEGORIES if c in cat_tokens}
        fp_analysis[L] = {"fp_records": len(ids), "n_distinct_tokens": len(cnt),
                          "token_occurrences_total": sum(cnt.values()),
                          "top40_tokens": [dict(t, category=(category(t["token"], None))) for t in top],
                          "tokens_below_freq_floor_suppressed_occurrences": suppressed,
                          "records_per_category": dict(cat_rec),
                          "occurrences_per_category": {c: sum(v.values()) for c, v in cat_tokens.items()},
                          "top_tokens_by_category": cat_top}
    # top-40 with category taking gold-owner into account (clip) is in occurrences_per_category; the top40 list uses
    # token-only category (clips show as the token's lexical class).

    # ================= sensitivity: category uplift (item 4) =================
    def uplift(base_L, ids, rs):
        cats_per = {}
        for g in ids:
            a = rs[g][0]
            cats_per[g] = {category(a["pieces"][i], a["owner"].get(i)) for i in a[RES][base_L]}
        base_fp = sum(1 for g in ids if cats_per[g])
        out = [{"ignored_categories": [], "fp_records": base_fp, "fp_rate_pct": pct(base_fp, len(ids))}]
        ignored, remaining = [], set(CATEGORIES) - {"PROTECTED_neg/comparator"}
        while remaining:
            best = None
            for c in sorted(remaining):
                trial = set(ignored) | {c}
                fp = sum(1 for g in ids if cats_per[g] - trial)
                if best is None or fp < best[1]:
                    best = (c, fp)
            ignored.append(best[0])
            remaining.discard(best[0])
            out.append({"ignored_categories": list(ignored), "fp_records": best[1], "fp_rate_pct": pct(best[1], len(ids))})
        return out

    uplift_tbl = {L: uplift(L, clean_ids, pass1) for L in ("L1_closed_min(art/prep/conj)",
                                                          "L1b_closed_full(+clit/aux/interrog/quant)",
                                                          "L3_STOPWORDS+RESIDUAL_FW")}
    # per-category alone (ignore one category on top of the base L)
    single = {}
    for L in uplift_tbl:
        single[L] = {}
        cp = {g: {category(pass1[g][0]["pieces"][i], pass1[g][0]["owner"].get(i)) for i in pass1[g][0][RES][L]}
              for g in clean_ids}
        for c in CATEGORIES:
            if c == "PROTECTED_neg/comparator":
                continue
            single[L][c] = {"records_whose_FP_depends_on_it": sum(1 for g in clean_ids if cp[g] == {c}),
                            "records_containing_it": sum(1 for g in clean_ids if c in cp[g])}

    # ================= sensitivity curve: recall vs FP over L (item 4) =================
    curve = []
    for L in L_NAMES:
        omitted_total = det1["by_dimension"].get("__ALL__", {}).get("n", 0)
        any_ = det1["by_dimension"].get("__ALL__", {}).get(L, {}).get("any", 0)
        att = det1["by_dimension"].get("__ALL__", {}).get(L, {}).get("attrib", 0)
        curve.append({"L": L, "lexicon_size": len(lex_sets[L]), "omitted_conditions": omitted_total,
                      "recall_any_residual_pct": pct(any_, omitted_total), "recall_attributable_pct": pct(att, omitted_total),
                      "fp_clean_pct": cost_clean1["by_L"][L]["fp_rate_pct"],
                      "fp_ir_limit_pct": cost_ir1["by_L"][L]["fp_rate_pct"]})

    # ================= IR_CANNOT_EXPRESS split (item 3c) =================
    ir_rows = defaultdict(lambda: {"n_records": 0, "res_nonempty_L3": 0, "res_nonempty_L1b": 0, "res_nonempty_L4": 0,
                                   "attrib_any_gold_ir_cond_in_residual_L3": 0})
    ir_oracle = defaultdict(lambda: {"n_records": 0, "oracle_res_nonempty_L3": 0})
    ir_ids = [g for g in pass1 if population(g) and kinds1[g] == "IR_LIMIT"]
    for g in ir_ids:
        a = pass1[g][0]
        keys = sorted({r["key"] for r in cls1[g]["rows"] if not r["invocab"]})
        og = oracle(gold_priv[g], lex_sets)
        for k in keys + ["__ANY__"]:
            ir_rows[k]["n_records"] += 1
            ir_rows[k]["res_nonempty_L3"] += bool(a[RES]["L3_STOPWORDS+RESIDUAL_FW"])
            ir_rows[k]["res_nonempty_L1b"] += bool(a[RES]["L1b_closed_full(+clit/aux/interrog/quant)"])
            ir_rows[k]["res_nonempty_L4"] += bool(a[RES]["L4_union_all_found"])
            ir_oracle[k]["n_records"] += 1
            ir_oracle[k]["oracle_res_nonempty_L3"] += bool(og[RES]["L3_STOPWORDS+RESIDUAL_FW"])
            ir_oracle[k]["oracle_res_nonempty_L1b"] = ir_oracle[k].get("oracle_res_nonempty_L1b", 0) + bool(og[RES]["L1b_closed_full(+clit/aux/interrog/quant)"])
            ir_oracle[k]["oracle_res_nonempty_L4"] = ir_oracle[k].get("oracle_res_nonempty_L4", 0) + bool(og[RES]["L4_union_all_found"])
            ir_rows[k]["attrib_any_gold_ir_cond_in_residual_L3"] += any(
                set(a[RES]["L3_STOPWORDS+RESIDUAL_FW"]) & a["per_cond"].get((r["req"], r["id"]), set())
                for r in cls1[g]["rows"] if not r["invocab"] and r["key"] in ((k,) if k != "__ANY__" else [x["key"] for x in cls1[g]["rows"]]))
    ir_status = Counter()
    for g in ir_ids:
        for r in cls1[g]["rows"]:
            if not r["invocab"]:
                ir_status[(r["key"], r["status"])] += 1

    # ================= oracle (perfect proposal) =================
    def oracle_cost(golds, label):
        ids = [g for g in golds if g["operation"] == "SEARCH" and g["expected_disposition"] == "EXECUTE"
               and g["ambiguity"] != "AMBIGUOUS"]
        orc = {g["gold_id"]: oracle(g, lex_sets) for g in ids}
        res = {}
        for L in L_NAMES:
            fp = [k for k, o in orc.items() if o[RES][L]]
            res[L] = {"n": len(ids), "fp_records": len(fp), "fp_rate_pct": pct(len(fp), len(ids))}
        cnt = Counter()
        for k, o in orc.items():
            for i in o[RES]["L3_STOPWORDS+RESIDUAL_FW"]:
                cnt[o["pieces"][i]] += 1
        top, supp = token_top(cnt, 40, MIN_FREQ if label == "REAL+SYNTHETIC(35)" else 1)
        return {"label": label, "n": len(ids), "by_L": res, "top40_L3_residual_tokens": top,
                "suppressed_occurrences": supp,
                "cov_all": dist([o["cov_all"] for o in orc.values()])}, orc

    oracle_priv, orc_priv = oracle_cost(list(gold_priv.values()), "REAL+SYNTHETIC(35)")
    syn_by_slice = {}
    orc_syn_all = {}
    for sl in ("NEIGHBOR", "MINIMAL_PAIR", "ADVERSARIAL"):
        r, orc = oracle_cost([g for g in gold_syn if g["slice"] == sl], sl)
        syn_by_slice[sl] = r
        orc_syn_all.update(orc)

    # oracle restricted to clean ids (same population as the real-proposal cost)
    oracle_on_clean = {L: {"n": len(clean_ids), "fp_records": sum(1 for g in clean_ids if orc_priv[g][RES][L]),
                           "fp_rate_pct": pct(sum(1 for g in clean_ids if orc_priv[g][RES][L]), len(clean_ids))}
                       for L in L_NAMES}

    # ================= overlaps / blind window (item 5) =================
    def overlap_stats(items, label):
        n = len(items)
        pairs = Counter()
        for a in items:
            for p in a["overlap_pairs"]:
                pairs["+".join(p)] += 1
        with_overlap = sum(1 for a in items if a["overlap_pairs"])
        with_overlap_noU = sum(1 for a in items if any("UNIVERSE" not in p for p in a["overlap_pairs"]))
        t = sum(a["blind"]["time_cites"] for a in items)
        with_time = sum(1 for a in items if a["blind"]["time_cites"])
        return {"label": label, "n_proposals": n, "proposals_with_cross_dimension_overlap": with_overlap,
                "proposals_with_cross_dimension_overlap_excluding_UNIVERSE": with_overlap_noU,
                "overlap_pair_counts_(proposals)": dict(pairs),
                "proposals_with_TIME_cite": with_time, "TIME_cites_total": t,
                "TIME_cites_with_neighbour_in_BUYER/ACTOR/GEO_within4": sum(a["blind"]["within4"] for a in items),
                "TIME_cites_with_neighbour_in_BUYER/ACTOR/GEO_adjacent1": sum(a["blind"]["adjacent1"] for a in items),
                "proposals_with_blind_window_within4": sum(1 for a in items if a["blind"]["within4"]),
                "proposals_with_blind_window_adjacent1": sum(1 for a in items if a["blind"]["adjacent1"])}

    overlaps = {"REAL_proposals_pass1": overlap_stats([pass1[g][0] for g in pass1 if gold_priv[g]["slice"] == "REAL"], "REAL pass1"),
                "SYNTH35_proposals_pass1": overlap_stats([pass1[g][0] for g in pass1 if gold_priv[g]["slice"] == "SYNTHETIC"], "SYNTHETIC(35) pass1"),
                "ORACLE_gold_quotes_REAL+35": overlap_stats(list(orc_priv.values()), "oracle gold quotes (SEARCH/EXECUTE)"),
                "ORACLE_gold_quotes_NEIGHBOR+MP+ADV": overlap_stats(list(orc_syn_all.values()), "oracle gold quotes synthetic slices")}

    # ================= second pass (item 6) =================
    both = sorted(set(pass1) & set(pass2))
    p2 = {}
    if both:
        kinds2 = {g: record_kind(g, cls2) for g in both}
        pop = [g for g in both if population(g)]
        stability = {"n_in_both_passes": len(both),
                     "records_SEARCH_nonambiguous": len(pop),
                     "kind_pass1": dict(Counter(kinds1[g] for g in pop)),
                     "kind_pass2": dict(Counter(kinds2[g] for g in pop)),
                     "kind_changed": sum(kinds1[g] != kinds2[g] for g in pop)}
        worst = {}
        for L in L_NAMES:
            clean_both = [g for g in pop if kinds1[g] == "CLEAN_CORRECT" or kinds2[g] == "CLEAN_CORRECT"]
            row = {}
            for tag, ids, rsx, kd in (("pass1", [g for g in pop if kinds1[g] == "CLEAN_CORRECT"], pass1, kinds1),
                                      ("pass2", [g for g in pop if kinds2[g] == "CLEAN_CORRECT"], pass2, kinds2)):
                row[tag] = {"n_clean": len(ids), "fp": sum(1 for g in ids if rsx[g][0][RES][L]),
                            "fp_rate_pct": pct(sum(1 for g in ids if rsx[g][0][RES][L]), len(ids))}
            both_clean = [g for g in pop if kinds1[g] == "CLEAN_CORRECT" and kinds2[g] == "CLEAN_CORRECT"]
            row["worst_case_per_record(clean in both)"] = {
                "n": len(both_clean), "fp": sum(1 for g in both_clean if pass1[g][0][RES][L] or pass2[g][0][RES][L]),
                "fp_rate_pct": pct(sum(1 for g in both_clean if pass1[g][0][RES][L] or pass2[g][0][RES][L]), len(both_clean)),
                "records_whose_FP_status_flips_between_passes": sum(
                    bool(pass1[g][0][RES][L]) != bool(pass2[g][0][RES][L]) for g in both_clean)}
            worst[L] = row
        # detection over both passes
        det2 = detection(pass2, cls2)
        det_both = {}
        for L in L_NAMES:
            om1 = {(g, r["id"], r["req"]) for g in both if population(g) for r in cls1[g]["rows"] if r["status"] == "OMITTED"}
            om2 = {(g, r["id"], r["req"]) for g in both if population(g) for r in cls2[g]["rows"] if r["status"] == "OMITTED"}
            det_both[L] = {"omitted_pass1": len(om1), "omitted_pass2": len(om2),
                           "detected_any_pass1": sum(1 for (g, c, q) in om1 if pass1[g][0][RES][L]),
                           "detected_any_pass2": sum(1 for (g, c, q) in om2 if pass2[g][0][RES][L])}
        p2 = {"stability": stability, "cost_by_pass_and_worst": worst, "detection_on_the_40": det_both}

    # ================= held-out (DA-6, v4.2, 3 passes, invented cases) =================
    held = {}
    try:
        rep = {}
        for tag, d, capdir in (("s1", "v42_s1", "v42_s1"), ("s2", "v42_s2", "v42_s2"), ("s3", "v42_s3", "v42_s3_merged")):
            cases = {c["case_id"]: c for c in json.load(open(HELD / d / "bc_report.json"))["cases"] if c["arm"] == "B"}
            capt = {c["case_id"]: c for c in jl(HELD / capdir / "captures.jsonl") if c.get("arm") == "B"}
            rep[tag] = (cases, capt)
        lex_L = lex_sets
        for tag, (cases, capt) in rep.items():
            clean, items = [], {}
            for cid, c in cases.items():
                cp = capt.get(cid)
                if not cp or c["producer"] != "PROPOSAL":
                    continue
                prop = decode(cp["raw"], cp["content"])
                a = analyze(cid, "HELDOUT", cp["raw"], prop, None, lex_L)
                items[cid] = (a, c)
            ok_strict = [k for k, (a, c) in items.items() if c["expected"] == "READY" and c["linguistic"]["verdict"] == "MATCH"]
            ok_loose = [k for k, (a, c) in items.items() if c["expected"] == "READY"
                        and c["linguistic"]["verdict"] in ("MATCH", "MATCH_EXCEPT_BOUNDARY")]
            held[tag] = {"cases": len(cases), "decoded": len(items),
                         "n_expected_READY_and_MATCH": len(ok_strict),
                         "n_expected_READY_and_MATCH_or_BOUNDARY": len(ok_loose),
                         "fp_strict": {L: sum(1 for k in ok_strict if items[k][0][RES][L]) for L in L_NAMES},
                         "fp_loose": {L: sum(1 for k in ok_loose if items[k][0][RES][L]) for L in L_NAMES},
                         "cov_all": dist([a["cov_all"] for a, _ in items.values()]),
                         "residual_tokens_L3_strict": [
                             {"token": t, "n": n} for t, n in Counter(
                                 items[k][0]["pieces"][i] for k in ok_strict
                                 for i in items[k][0][RES]["L3_STOPWORDS+RESIDUAL_FW"]).most_common(25)],
                         "overlaps": overlap_stats([a for a, _ in items.values()], "heldout " + tag)}
            held[tag]["_items"] = items
        # worst case over the three passes (strict)
        ids = set.intersection(*[{k for k, (a, c) in held[t]["_items"].items() if c["expected"] == "READY"
                                  and c["linguistic"]["verdict"] == "MATCH"} for t in held])
        held["worst_case_3_passes_strict(clean in all 3)"] = {
            "n": len(ids), "fp_any_pass": {L: sum(1 for k in ids if any(held[t]["_items"][k][0][RES][L] for t in ("s1", "s2", "s3")))
                                         for L in L_NAMES}}
        for t in ("s1", "s2", "s3"):
            held[t].pop("_items")
    except Exception as exc:  # noqa: BLE001
        held = {"error": f"{type(exc).__name__}: {exc}"}

    # ================= Y1 (explicit) =================
    y1 = {}
    for L in ("L1b_closed_full(+clit/aux/interrog/quant)", "L3_STOPWORDS+RESIDUAL_FW", "L4_union_all_found"):
        pop_ids = [g for g in pass1 if population(g) and kinds1[g] in ("CLEAN_CORRECT", "IR_LIMIT")]
        tokens_total = tokens_y1 = 0
        recs_nonempty = recs_y1_only = recs_clean_nonempty = recs_clean_y1_only = 0
        month_range_tokens = Counter()
        for g in pop_ids:
            a = pass1[g][0]
            res = a[RES][L]
            if not res:
                continue
            ytok = [i for i in res if a["owner"].get(i) == "TIME"]
            tokens_total += len(res)
            tokens_y1 += len(ytok)
            recs_nonempty += 1
            only = len(ytok) == len(res)
            recs_y1_only += only
            if kinds1[g] == "CLEAN_CORRECT":
                recs_clean_nonempty += 1
                recs_clean_y1_only += only
            for i in ytok:
                month_range_tokens[a["pieces"][i]] += 1
        y1[L] = {"records_with_residual(CLEAN+IR_LIMIT)": recs_nonempty, "of_which_ONLY_gold-TIME-quote_tokens": recs_y1_only,
                 "CLEAN_records_with_residual": recs_clean_nonempty, "CLEAN_of_which_ONLY_TIME_clip": recs_clean_y1_only,
                 "residual_tokens_total": tokens_total, "residual_tokens_inside_gold_TIME_quote": tokens_y1,
                 "top_TIME_clip_tokens": token_top(month_range_tokens, 15)[0]}

    # ================= assemble =================
    results = {
        "notes": notes, "lexicons": lex_table, "coverage": cov, "record_kinds": kinds_count,
        "operations_in_captured_set": op_count, "population_SEARCH_by_slice_kind": pop_counts, "geometric_gold_quote_coverage": geo1, "detection": det1, "detection_invisible": detection_extra,
        "cost_CLEAN_CORRECT": cost_clean1, "cost_CORRECT_UNIVERSE_UNCITED": cost_uncited1, "cost_IR_LIMIT": cost_ir1,
        "fp_analysis_clean": fp_analysis, "sensitivity_curve": curve, "category_uplift_greedy": uplift_tbl,
        "category_single": single, "IR_limit_split": {k: v for k, v in ir_rows.items()},
        "IR_limit_oracle_comparison": {k: v for k, v in ir_oracle.items()},
        "IR_limit_gold_condition_status": {f"{k[0]}|{k[1]}": v for k, v in sorted(ir_status.items())},
        "oracle_private_REAL+35": oracle_priv, "oracle_on_clean_ids": oracle_on_clean,
        "oracle_synthetic_slices": syn_by_slice, "overlaps_blind_window": overlaps,
        "second_pass": p2, "heldout_DA6_v42": held, "Y1": y1,
    }
    (HERE / f"omission_probe_results__{RES}.json").write_text(json.dumps(results, ensure_ascii=False, indent=1, default=str), encoding="utf-8")

    # per-record (ids + counts, no tokens)
    per = []
    for g, (a, _) in pass1.items():
        per.append({"gold_id": g, "slice": a["slice"], "op": gold_priv[g]["operation"], "kind": kinds1[g],
                    "n_tokens": a["n_tokens"], "cov_all": round(a["cov_all"], 3),
                    **{f"res_n[{L.split('_')[0]}]": len(a[RES][L]) for L in L_NAMES}})
    (HERE / f"per_record_counts__{RES}.json").write_text(json.dumps(per, ensure_ascii=False), encoding="utf-8")
    print("done", len(pass1), "records pass1;", len(pass2), "pass2")
    return results


if __name__ == "__main__":
    main()

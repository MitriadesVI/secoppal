"""Membership projections; missing endpoints never receive fiscal/name substitutes."""
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation

from scripts.audits.relationship_foundation.edge_evidence import in_values
from scripts.audits.relationship_foundation.identity_spine import platform_code

G = "ceth-n4bn"
FIELDS = "codigo_grupo,codigo_participante,participacion,es_lider_del_grupo,esta_activo,fecha_creaci_n_grupo,es_proveedor"


def membership(row):
    group, participant = platform_code(row.get("codigo_grupo")), platform_code(row.get("codigo_participante"))
    if not group or not participant:
        return None
    raw = row.get("participacion")
    try:
        value = Decimal(raw) if isinstance(raw, str) else None
    except InvalidOperation:
        value = None
    pct = str(value) if value is not None and value.is_finite() and 0 <= value <= 100 else None
    return {"from": "secop_supplier:" + participant, "to": "supplier_group:" + group,
            "participation_pct": pct, "participation_raw": raw,
            "is_leader": {"Verdadero": True, "Falso": False}.get(row.get("es_lider_del_grupo")),
            "leader_raw": row.get("es_lider_del_grupo")}


def audit(probe, bidder_codes):
    totals = probe.get("group_totals", G, {"$select": "count(*) as rows,count(codigo_participante) as participants,count(codigo_grupo) as groups", "$limit": 1})[0]
    duplicates = probe.get("group_duplicate_keys", G, {
        "$select": "codigo_grupo,codigo_participante,count(*) as rows",
        "$group": "codigo_grupo,codigo_participante", "$having": "count(*) > 1",
        "$order": "codigo_grupo,codigo_participante", "$limit": 1000})
    exact_duplicates = probe.get("group_duplicate_projection", G, {
        "$select": FIELDS + ",count(*) as rows", "$group": FIELDS,
        "$having": "count(*) > 1", "$order": "codigo_grupo,codigo_participante", "$limit": 1000})
    rows = []
    codes = sorted(bidder_codes)[:500]
    for start in range(0, len(codes), 100):
        rows.extend(probe.get(f"bid_groups_{start}", G, {
            "$select": FIELDS, "$where": in_values("codigo_grupo", codes[start:start + 100]),
            "$order": "codigo_grupo,codigo_participante", "$limit": 10000}))
    tail = sorted(bidder_codes)[-500:]
    for start in range(0, len(tail), 100):
        rows.extend(probe.get(f"bid_groups_tail_{start}", G, {
            "$select": FIELDS, "$where": in_values("codigo_grupo", tail[start:start + 100]),
            "$order": "codigo_grupo,codigo_participante", "$limit": 10000}))
    for active in ("Si", "No"):
        rows.extend(probe.get("groups_active_" + active, G, {
            "$select": FIELDS, "$where": "esta_activo = '" + active + "'",
            "$order": "codigo_grupo,codigo_participante", "$limit": 100}))
    # Deduplicate overlapping probes before calculating sample membership statistics.
    unique = {tuple((f, r.get(f)) for f in FIELDS.split(",")): r for r in rows}
    rows = list(unique.values())
    by_group = defaultdict(list)
    for row in rows:
        if membership(row):
            by_group[row["codigo_grupo"]].append(row)
    selected = sorted(by_group)[:10]
    # Re-fetch complete membership for selected groups; no sliced group claimed complete.
    complete = probe.get("group_canaries_complete", G, {
        "$select": FIELDS, "$where": in_values("codigo_grupo", selected),
        "$order": "codigo_grupo,codigo_participante", "$limit": 10000}) if selected else []
    groups = defaultdict(list)
    for row in complete:
        if (edge := membership(row)):
            groups[row["codigo_grupo"]].append(edge)
    summary = {"rows_total": int(totals["rows"]),
               "null_participant_codes": int(totals["rows"]) - int(totals["participants"]),
               "null_group_codes": int(totals["rows"]) - int(totals["groups"]),
               "duplicate_key_groups": duplicates, "duplicate_key_scan_capped": len(duplicates) == 1000,
               "duplicate_exact_projected_memberships": exact_duplicates,
               "duplicate_projection_scan_capped": len(exact_duplicates) == 1000,
               "duplicate_definition": "same endpoint codes and all selected edge/group attributes; not full source-row equality",
               "canaries": dict(groups), "canary_capture_capped": len(complete) == 10000,
               "group_attribute_canaries": [{"group": "supplier_group:" + r["codigo_grupo"],
                                              "active": r.get("esta_activo"),
                                              "creation_date": r.get("fecha_creaci_n_grupo")}
                                             for r in rows if r.get("codigo_grupo")][:20]
                                           + [{"group": "supplier_group:" + r["codigo_grupo"],
                                               "active": r.get("esta_activo"),
                                               "creation_date": r.get("fecha_creaci_n_grupo")}
                                              for r in rows if r.get("esta_activo") == "No"][:10],
               "active_values_observed": dict(Counter(r.get("esta_activo") for r in rows)),
               "leader_values_observed": dict(Counter(r.get("es_lider_del_grupo") for r in rows)),
               "previous_audit": {"rows_total": 701182, "null_participant_codes": 9105, "null_group_codes": 0},
               "coverage": "PARTIAL: omitted endpoint codes and unverified national universe"}
    return summary, rows

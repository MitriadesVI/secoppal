"""Evaluate independent health hypotheses without extending ADR-076 implicitly."""
from collections import Counter

from app.core.entity_types import name_tokens
from scripts.audits.m2b1.io import summary
from scripts.audits.m2b1.rules import RULES


def build(ctx, rules):
    profiles = [p for p in ctx['target_profiles'] if p['institutional_family'] == 'ESE_HEALTH' or
                'hospital' in name_tokens(p['physical_source_name'])]
    rows = []
    for p in profiles:
        d = rules['by_id'][p['profile_id']]
        official = ctx['sigep'][d['selected_sigep_index']] if d['selected_sigep_index'] is not None else None
        pt = name_tokens(p['physical_source_name'])
        ot = name_tokens(official['NOMBRE']) if official else []
        rows.append(dict(profile_id=p['profile_id'], name=p['physical_source_name'],
                         hospital='hospital' in pt, hospital_local='hospital' in pt and 'local' in pt,
                         care_level=any(token == 'nivel' for token in pt + ot),
                         official_name_longer=bool(official and len(ot) > len(pt)),
                         printed_acronym_rule=RULES[4] in d['rules_matched'],
                         location_qualifier_rule=RULES[1] in d['rules_matched'],
                         exact_adr076_existing_canonical=RULES[0] in d['rules_matched'],
                         status=d['status'], accepted_rule=d['identity_authority'],
                         contradictions=d['contradictions']))
    pattern_defs = {
        'HOSPITAL': lambda r: r['hospital'], 'HOSPITAL_LOCAL': lambda r: r['hospital_local'],
        'CARE_LEVEL': lambda r: r['care_level'], 'OFFICIAL_LONGER_NAME': lambda r: r['official_name_longer'],
        'OFFICIALLY_PRINTED_ACRONYM': lambda r: r['printed_acronym_rule'],
        'OFFICIAL_LOCATION_QUALIFIER': lambda r: r['location_qualifier_rule'],
        'ADR076_EXISTING_CANONICAL': lambda r: r['exact_adr076_existing_canonical'],
    }
    patterns = {}
    for name, predicate in pattern_defs.items():
        selected = [r for r in rows if predicate(r)]
        ps = [ctx['by_id'][r['profile_id']] for r in selected]
        patterns[name] = dict(summary(ps), accepted=sum(r['status'] == 'ADJUDICATED_DELTA' for r in selected),
                              rejected=sum(r['status'] != 'ADJUDICATED_DELTA' for r in selected),
                              contradictions=dict(Counter(x for r in selected for x in r['contradictions'])))
    return dict(input=summary(profiles), patterns=patterns, profiles=rows,
                canary='Hospital/family/territory never merges profiles. Each accepted profile has its own unique official code and account.',
                combined_new_relaxations=0)

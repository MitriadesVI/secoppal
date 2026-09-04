"""Describe the frozen F class without treating a lexical label as an expansion."""
from collections import Counter
import re

from scripts.audits.m2b.identity_vias import ese_core
from scripts.audits.m2b1.io import summary


def pattern(name, family):
    lowered = name.lower()
    core = ese_core(name)[0]
    if re.search(r'\be\s*\.\s*s\s*\.\s*e\b|\bese\b', lowered):
        return 'HOSPITAL_OR_ESE_LEGAL_ABBREVIATION' if family in ('ESE_HEALTH', 'UNKNOWN') else 'ESE_SIGNAL_OUTSIDE_HEALTH'
    if re.search(r'\be\s*\.\s*s\s*\.\s*p\b|\besp\b|\bs\s*\.\s*a(?:\s*\.\s*s)?\b|\bltda\b', lowered):
        return 'PUBLIC_COMPANY_LEGAL_FORM_ABBREVIATION'
    if len(core) <= 3 and all(len(token) <= 12 for token in core):
        return 'OPAQUE_OR_LOCAL_SHORT_FORM'
    if any(char.isdigit() for char in name) or re.search(r'[*/+]{2}|\b\w+\d+\b', name):
        return 'TRUNCATION_OR_SOURCE_NOISE_SIGNAL'
    if family == 'INSTITUTE':
        return 'INSTITUTE_WITH_ACRONYM_OR_SHORT_NAME'
    return 'OTHER_ABBREVIATION_SIGNAL'


def build(ctx, rules):
    rows = []
    for p in ctx['target_all']['F']:
        d = rules['by_id'].get(p['profile_id'])
        value = pattern(p['physical_source_name'], p['institutional_family'])
        rows.append(dict(profile_id=p['profile_id'], pattern=value, name=p['physical_source_name'],
                         m2b_accepted=p['profile_id'] in ctx['accepted_ids'],
                         official_expansion_available=bool(d and d['rules_matched']),
                         delta_status=d['status'] if d else 'M2B_ACCEPTED_BASELINE',
                         accepted_rule=d['identity_authority'] if d else None,
                         contradictions=d['contradictions'] if d else []))
    patterns = {}
    for value in sorted({r['pattern'] for r in rows}):
        selected = [r for r in rows if r['pattern'] == value]
        profiles = [ctx['by_id'][r['profile_id']] for r in selected]
        ranked = sorted(profiles, key=lambda p: (-p['volumes']['unique_processes'], p['profile_id']))
        patterns[value] = dict(summary(profiles), examples=[p['physical_source_name'] for p in ranked[:8]],
                               official_expansion_available=sum(r['official_expansion_available'] for r in selected),
                               contradictions=dict(Counter(reason for r in selected for reason in r['contradictions'])),
                               m2b_accepted=sum(r['m2b_accepted'] for r in selected),
                               m2b1_delta_accepted=sum(r['delta_status'] == 'ADJUDICATED_DELTA' for r in selected))
    return dict(input=summary(ctx['target_all']['F']), patterns=patterns, profiles=rows,
                rule='Lexical patterns describe surfaces only. No pattern, abbreviation or source noise is identity authority.')

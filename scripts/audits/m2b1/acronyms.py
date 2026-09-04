"""Inventory opaque short forms; only a literally printed official acronym can expand."""
import re
from collections import Counter, defaultdict

from app.core.entity_types import name_tokens
from scripts.audits.m2b.identity_vias import ese_core
from scripts.audits.m2b1.io import summary
from scripts.audits.m2b1.rules import geographic_tokens, printed_acronyms, RULES

LEGAL = {'ese', 'esp', 'sa', 'sas', 'ltda', 'e', 's', 'p', 'c', 'comprador'}


def candidates(name, municipality_names, printed):
    core = tuple(t for t in ese_core(name)[0] if t not in LEGAL)
    found = set()
    if (len(core) == 1 and 2 <= len(core[0]) <= 12 and core[0] not in municipality_names
            and (len(core[0]) <= 6 or core[0] in printed)):
        found.add(core[0])
    for match in re.finditer(r'(?:^|[-(/])\s*([A-ZÁÉÍÓÚÑ]{2,12})\s*(?:$|[-/)])', name):
        token = ''.join(name_tokens(match.group(1)))
        if token not in LEGAL:
            found.add(token)
    return sorted(found)


def build(ctx, rules):
    official = defaultdict(list)
    for i, row in enumerate(ctx['sigep']):
        for acronym in printed_acronyms(row['NOMBRE']):
            if acronym in geographic_tokens(row['divipola'], ctx['towns']):
                continue
            official[acronym].append(dict(index=i, code=row['CÓDIGO SIGEP'], territory=row['divipola'],
                                          name=row['NOMBRE'], nature=row['NATURALEZA JURÍDICA']))
    municipality_names = {''.join(name_tokens(t['municipality'])) for t in ctx['towns'].values()}
    municipality_names.update(token for t in ctx['towns'].values()
                              for token in name_tokens(t['municipality']) if len(token) >= 3)
    rows = []
    unresolved = [p for p in ctx['pending'] if p['profile_id'] not in ctx['accepted_ids']]
    for p in unresolved:
        for acronym in candidates(p['physical_source_name'], municipality_names, official):
            if acronym in geographic_tokens(p['territory_divipola'], ctx['towns']):
                continue
            matches = [m for m in official[acronym] if m['territory'] == p['territory_divipola']]
            d = rules['by_id'].get(p['profile_id'])
            accepted = bool(d and d['identity_authority'] == RULES[4])
            if accepted:
                status = 'OFFICIALLY_EXPANDED'
            elif len({m['code'] for m in matches}) > 1:
                status = 'MULTIPLE_EXPANSIONS'
            elif matches and d and any('CONFLICT' in x for x in d['contradictions']):
                status = 'CONFLICTING_EXPANSION'
            else:
                status = 'UNRESOLVED_ACRONYM'
            rows.append(dict(profile_id=p['profile_id'], acronym=acronym, status=status,
                             official_matches=matches, source_class=ctx['primary'][p['profile_id']],
                             contradictions=d['contradictions'] if d else ['OUTSIDE_TARGETED_CLASSES_NO_ADJUDICATION']))
    counts = Counter(r['status'] for r in rows)
    classifications = {status: counts[status] for status in (
        'OFFICIALLY_EXPANDED', 'MULTIPLE_EXPANSIONS', 'UNRESOLVED_ACRONYM',
        'CONFLICTING_EXPANSION')}
    return dict(profiles_examined=len(unresolved), acronym_occurrences=len(rows),
                classifications=classifications, rows=rows,
                affected_profile_summary=summary([ctx['by_id'][pid] for pid in sorted({r['profile_id'] for r in rows})]),
                official_printed_acronym_index={k: v for k, v in sorted(official.items())},
                guard='Initial-letter derivation is prohibited. Only a literal official acronym can enter the candidate rule.')

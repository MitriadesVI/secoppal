"""Health subaudit: source labels are signals, only own institutions confer nature."""
from collections import Counter, defaultdict

from app.core.entity_types import name_tokens
from scripts.audits.m2b.identity_vias import ese_core
from scripts.audits.m2b.io import summary


def is_health(p):
    tokens = name_tokens(p['physical_source_name'])
    return p['institutional_family'] == 'ESE_HEALTH' or bool(ese_core(p['physical_source_name'])[1]) or 'hospital' in tokens


def audit(ctx, via):
    rows = []
    for p in ctx['pending']:
        if not is_health(p):
            continue
        d = via['by_id'][p['profile_id']]
        tokens = name_tokens(p['physical_source_name'])
        town_tokens = tuple(name_tokens(ctx['towns'][p['territory_divipola']]['municipality']))
        core, label = ese_core(p['physical_source_name'])
        rows.append(dict(profile_id=p['profile_id'], name=p['physical_source_name'],
                         m2a_family=p['institutional_family'], territory=p['territory_divipola'],
                         source_legal_label=label,
                         ese_plus_town_only=bool(label) and core == town_tokens,
                         hospital_plus_town_only=core in (('hospital',) + town_tokens, ('hospital', 'de') + town_tokens),
                         own_sigep_longer_name=any(len(ctx['sigep'][m['sigep_index']]['NOMBRE']) > len(p['physical_source_name'])
                                                   for m in d['matched_sigep'] if m['territory'] == p['territory_divipola']),
                         hospital_signal='hospital' in tokens, centre_or_post_signal=bool({'centro', 'puesto'} & set(tokens)),
                         institutional_authority_accredited=d['status'] == 'ADJUDICATED_CANDIDATE',
                         official_matches=d['matched_sigep'], identity_authority=d['identity_authority'],
                         status=d['status'], reasons=d['reasons'],
                         same_account_other_surfaces=d['same_account_profiles'],
                         competing_accounts=d['competing_observed_accounts']))
    bytown = defaultdict(list)
    for r in rows:
        bytown[r['territory']].append(r)
    controls = []
    for code, rs in sorted(bytown.items()):
        ids = sorted({via['by_id'][r['profile_id']]['candidate_logical_unit_id'] for r in rs
                      if via['by_id'][r['profile_id']]['candidate_logical_unit_id']})
        if len(rs) > 1:
            controls.append(dict(territory=code, observed_health_profile_ids=sorted(r['profile_id'] for r in rs),
                                 current_logical_health_units=sorted(u['logical_unit_id'] for u in ctx['units']
                                     if u['territory_divipola'] == code and u['institutional_family'] == 'ESE_HEALTH'),
                                 newly_accredited_logical_ids=ids, automatic_territorial_merges=0,
                                 unresolved_profile_ids=sorted(r['profile_id'] for r in rs if r['status'] == 'REVIEW')))
    return dict(summary=summary([ctx['by_id'][r['profile_id']] for r in rows]), profiles=rows,
                patterns=dict(legal_label=Counter(r['source_legal_label'] or 'NONE' for r in rows),
                              hospital=sum(r['hospital_signal'] for r in rows),
                              centre_or_post=sum(r['centre_or_post_signal'] for r in rows),
                              ese_plus_town_only=sum(r['ese_plus_town_only'] for r in rows),
                              hospital_plus_town_only=sum(r['hospital_plus_town_only'] for r in rows),
                              own_sigep_longer_name=sum(r['own_sigep_longer_name'] for r in rows),
                              same_account_multiple_names=sum(bool(r['same_account_other_surfaces']) for r in rows),
                              statuses=Counter(r['status'] for r in rows)),
                same_territory_controls=controls,
                canary='Territory plus family never identifies a hospital. Distinct official entities retain distinct IDs.',
                scope='Includes ESE signals outside frozen ESE_HEALTH; centre/post alone never grants health institution status.')

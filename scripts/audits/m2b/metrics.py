"""Account for profiles and volume at their frozen grains, with no production claim."""
from scripts.audits.m2b.io import summary


def rank(p):
    return (-p['volumes']['unique_processes'], -p['volumes']['contract_physical_rows'],
            p['territory_divipola'], p['profile_id'])


def impact(ctx, via):
    safe = sorted([ctx['by_id'][d['profile_id']] for d in via['accepted']], key=rank)
    before = [p for p in ctx['verified'] if p['logical_unit_id']]
    baseline, total = summary(before), summary(ctx['verified'])
    scenarios = {}
    for name, limit in (('TOP_SAFE_50', 50), ('TOP_SAFE_100', 100), ('ALL_RULE_BASED', len(safe))):
        ps = safe[:limit]
        sm = summary(ps)
        scenarios[name] = dict(profiles_resolved=len(ps), logical_units_created=len({via['by_id'][p['profile_id']]['candidate_logical_unit_id'] for p in ps}),
                               processes_resolved=sm['unique_processes'], contract_rows_resolved=sm['contract_rows'],
                               profile_ids=sm['profile_ids'], requested_limit=limit,
                               safe_pool_exhausted=limit >= len(safe),
                               resulting_logical_profiles=baseline['profiles'] + len(ps),
                               resulting_logical_units=len(ctx['units']) + len(ps),
                               coverage_verified_territorial_profiles_pct=100 * (baseline['profiles'] + len(ps)) / total['profiles'],
                               coverage_verified_territorial_processes_pct=100 * (baseline['unique_processes'] + sm['unique_processes']) / total['unique_processes'],
                               coverage_verified_territorial_contract_rows_pct=100 * (baseline['contract_rows'] + sm['contract_rows']) / total['contract_rows'],
                               pct_pending_process_volume_resolved=100 * sm['unique_processes'] / 347163,
                               pct_pending_contract_rows_resolved=100 * sm['contract_rows'] / 117277)
    return dict(baseline_logical=baseline, verified_territorial_denominator=total,
                baseline_logical_units=len(ctx['units']), scenarios=scenarios,
                remaining=summary([p for p in ctx['pending'] if via['by_id'][p['profile_id']]['status'] == 'REVIEW']),
                production_coverage_change=0,
                metric_scope='Hypothetical institutional knowledge inside frozen verified territorial cohort; not SECOPPAL productive coverage.',
                process_provenance='MEASURED unique processes in frozen strata; M2A global identity reconciliation inherited.',
                contract_provenance='MEASURED physical rows; do not relabel as unique contracts.')


def territory_delta(ctx, via):
    rows = []
    for t in ctx['territories']:
        code = t['divipola']
        pending = [p for p in ctx['pending'] if p['territory_divipola'] == code]
        new = [via['by_id'][p['profile_id']] for p in pending if via['by_id'][p['profile_id']]['status'] == 'ADJUDICATED_CANDIDATE']
        current = sorted(u['logical_unit_id'] for u in ctx['units'] if u['territory_divipola'] == code)
        rows.append(dict(divipola=code, municipality=t['municipality'], current_logical_buyer_units=current,
                         newly_adjudicated_candidate_buyer_units=[dict(logical_unit_id=d['candidate_logical_unit_id'],
                                                                      profile_id=d['profile_id'], family=d['institutional_family']) for d in new],
                         identity_unresolved=sorted(p['profile_id'] for p in pending if via['by_id'][p['profile_id']]['status'] == 'REVIEW'),
                         baseline_unresolved_context_reference=f'artifacts/audits/municipal_m2a/territory_profiles.json#divipola={code}',
                         projected_logical_units=len(current) + len(new)))
    # These are reporting controls, never matching exceptions or identity rules.
    controls = {'BARRANQUILLA', 'BUCARAMANGA', 'YUMBO', 'BARRANCABERMEJA', 'BARANOA', 'BOGOTÁ, D.C.'}
    selected = [r for r in rows if r['municipality'] in controls]
    small = sorted([r for r in rows if 0 < len(r['current_logical_buyer_units']) <= 2 and
                    len(r['identity_unresolved']) + len(r['newly_adjudicated_candidate_buyer_units']) <= 2], key=lambda r: r['divipola'])[:5]
    return dict(territories=rows, controls=selected, small_territory_controls=small,
                small_control_definition='At most two current logical units and at most two pending M2B profiles; not population data.',
                hypothetical=True, original_territory_profiles_mutated=False)

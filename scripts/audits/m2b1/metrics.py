"""Volume concentration, delta impact and cumulative M2B accounting."""
from scripts.audits.m2b1.io import summary


def ranking_key(p):
    return (-p['volumes']['unique_processes'], -p['volumes']['contract_physical_rows'],
            p['territory_divipola'], p['profile_id'])


def top_volume(ctx, rules):
    ordered = sorted(ctx['target_profiles'], key=ranking_key)
    target = summary(ordered)
    remaining = [p for p in ctx['pending'] if p['profile_id'] not in ctx['accepted_ids']]
    global_remaining = summary(remaining)
    scenarios = {}
    for name, limit in (('TOP_25', 25), ('TOP_50', 50), ('TOP_100', 100)):
        selected = ordered[:limit]
        total = summary(selected)
        accepted = [p for p in selected if rules['by_id'][p['profile_id']]['status'] == 'ADJUDICATED_DELTA']
        scenarios[name] = dict(**total,
            target_process_volume_share_pct=100 * total['unique_processes'] / target['unique_processes'],
            remaining_process_volume_share_pct=100 * total['unique_processes'] / global_remaining['unique_processes'],
            target_contract_volume_share_pct=100 * total['contract_rows'] / target['contract_rows'],
            accepted=summary(accepted),
            audit=[dict(rank=i + 1, profile_id=p['profile_id'], source_class=ctx['primary'][p['profile_id']],
                        name=p['physical_source_name'], territory=p['territory_divipola'],
                        unique_processes=p['volumes']['unique_processes'],
                        contract_rows=p['volumes']['contract_physical_rows'],
                        decision=rules['by_id'][p['profile_id']]['status'],
                        rule=rules['by_id'][p['profile_id']]['identity_authority'],
                        contradictions=rules['by_id'][p['profile_id']]['contradictions'])
                   for i, p in enumerate(selected)])
    return dict(targeted_remaining=target, global_remaining_after_m2b=global_remaining,
                scenarios=scenarios, sort='unique_processes DESC, contract_rows DESC, territory, profile_id')


def impact(ctx, rules):
    delta_profiles = [ctx['by_id'][d['profile_id']] for d in rules['accepted']]
    delta = summary(delta_profiles)
    existing_profiles = [ctx['by_id'][pid] for pid in ctx['accepted_ids']]
    existing = summary(existing_profiles)
    unresolved = [p for p in ctx['pending'] if p['profile_id'] not in ctx['accepted_ids'] and
                  rules['by_id'].get(p['profile_id'], {}).get('status') != 'ADJUDICATED_DELTA']
    unresolved_summary = summary(unresolved)
    total = summary(ctx['pending'])
    new_units = sum(d['creates_new_logical_unit'] for d in rules['accepted'])
    existing_links = len(rules['accepted']) - new_units
    threshold_volume = total['unique_processes'] * 0.10
    diminishing = not (len(delta_profiles) >= 50 or delta['unique_processes'] >= threshold_volume)
    return dict(m2b_baseline=existing,
                m2b1_delta=dict(**delta, new_logical_units=new_units,
                                 existing_canonical_profiles_linked=existing_links),
                cumulative_m2b=dict(total_profiles_resolved=existing['profiles'] + delta['profiles'],
                    total_logical_candidates=55 + new_units,
                    existing_canonical_profiles_linked=existing_links,
                    remaining_identity_pending=unresolved_summary['profiles'],
                    process_gain=existing['unique_processes'] + delta['unique_processes'],
                    contract_gain=existing['contract_rows'] + delta['contract_rows'],
                    pct_process_volume_resolved=100 * (existing['unique_processes'] + delta['unique_processes']) / total['unique_processes'],
                    pct_contract_volume_resolved=100 * (existing['contract_rows'] + delta['contract_rows']) / total['contract_rows']),
                remaining=unresolved_summary,
                stop_condition=dict(min_profiles=50, min_additional_process_pct=10,
                    actual_profiles=delta['profiles'], actual_process_gain=delta['unique_processes'],
                    actual_additional_process_pct=100 * delta['unique_processes'] / total['unique_processes'],
                    M2B_IDENTITY_DIMINISHING_RETURNS='YES' if diminishing else 'NO',
                    interpretation='OR threshold. Exactly 50 profiles satisfies the profile branch.'),
                production_coverage_change=0,
                contract_metric='PHYSICAL_ROWS; unique-contract wording is not inferred.')

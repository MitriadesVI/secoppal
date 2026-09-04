"""Build the frozen municipal institutional universe; no live calls or promotion."""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
import socket
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.audits.m2a.affiliation import indexes, investigate
from scripts.audits.m2a.gates import evaluate
from scripts.audits.m2a.identity import reconcile
from scripts.audits.m2a.io import (M0, M1B, M1A2A, FINAL, DS, byte_identity, read, save,
                                  physical_profiles, protected_hashes, reconciliation)
from scripts.audits.m2a.metrics import coverage, rankings, territories, via_metrics
from scripts.audits.m2a.report import render
from scripts.audits.gate_divipola_buyer_matrix import discovery


def deny_network(*args, **kwargs):
    raise RuntimeError('M2A_OFFLINE_NETWORK_PROHIBITED')


def build(out, before=None):
    before = before or protected_hashes()
    analysis = read(FINAL / 'm0_post_analysis.json.gz')
    ledger = reconciliation(analysis)  # Required stop before continuing if totals differ.
    profiles = physical_profiles(analysis)
    if len({(r['dataset'], r['account'], r['name']) for r in analysis['strata']}) != len(analysis['strata']):
        raise ValueError('Duplicate physical stratum triple')
    source_processes = read(M0 / 'sources/processes.json.gz')
    source_contracts = read(M0 / 'sources/contracts.json.gz')
    global_processes = int(read(M0 / 'sources/process_distinct.json.gz')['rows'][0]['unique_processes'])
    assert global_processes == sum(r['unique_ids'] for r in analysis['strata'] if r['dataset'] == DS[0])
    assert source_contracts['counts_after'][0]['physical_rows'] == source_contracts['counts_after'][0]['unique_contracts']
    assert sum(int(r['physical_rows']) for r in source_contracts['rows']) == int(source_contracts['counts_after'][0]['physical_rows'])
    assert len(source_processes['rows']) + len(source_contracts['rows']) == len(analysis['strata'])
    m1b = read(M1B / 'buyer_profiles.json')
    m1b_classification = read(M1B / 'classification.json')
    m1b_candidates = read(M1B / 'promotion_candidates.json')
    sigep, directory = read(M0 / 'sources/sigep.json.gz'), read(M0 / 'sources/directory.json.gz')['rows']
    towns = {t['divipola']: t for t in read(ROOT / 'app/data/municipal_territories_v1.json')['territories']}
    records = read(ROOT / 'app/data/aliases_db.json')
    # Source-binding bytes are protected and the frozen recognition authority references their post-promotion state.
    sidecar = read(ROOT / 'app/data/buyer_source_names.json')
    ix = indexes(analysis, sigep, directory, m1b, read(M1A2A / 'adjudication.json'))
    affiliations = [investigate(p, [analysis['strata'][i] for i in p['stratum_indices']], ix, towns) for p in profiles]
    logical, delta = reconcile(profiles, affiliations, analysis, ix, records, m1b, m1b_candidates, sidecar)
    cov = coverage(profiles, logical['units'], affiliations, analysis, ledger)
    tp = territories(profiles, affiliations, logical['units'], towns, analysis)
    vias = via_metrics(profiles, affiliations, analysis, ix)
    counts = read(FINAL / 'post_gates.json')['compound_identity']['counts']
    if discovery()['counts'] != counts:
        raise ValueError('STOP: derived compound universe differs from M1A FINAL')
    proof = byte_identity(before)
    gates = evaluate(profiles, affiliations, logical, cov, delta, proof, counts)
    gates['baseline_head'] = '715362e1f91bcc581aaeca45e7e278108ef9ccdd'
    gates['territories'] = dict(WITH_EXECUTIVE=sum(bool(t['buyer_units']['executive']) for t in tp),
                               WITH_MULTIPLE_BUYER_FAMILIES=sum(len(t['institutional_families']) > 1 for t in tp),
                               WITH_UNRESOLVED_UNITS=sum(bool(t['unresolved_count']) for t in tp))
    for action in ('READY_FOR_REGISTRY', 'READY_FOR_BUYER_PROMOTION', 'MANUAL_REVIEW',
                   'KEEP_SEPARATE', 'SOURCE_BINDING_ONLY', 'DO_NOT_ADD'):
        gates['candidates'].setdefault(action, 0)
    artifacts = {
        'physical_profiles': dict(grain=['account', 'physical_source_name'],
            counts_by_dataset=dict(Counter(r['dataset'] for r in analysis['strata'])), profiles=profiles,
            stratum_reference='artifacts/audits/municipal_m1a_final/m0_post_analysis.json.gz#/strata'),
        'logical_units': logical,
        'territorial_affiliation': dict(affiliations=affiliations,
            source_order_authority='NONE: ordenentidad/orden are registry noise, never used for affiliation'),
        'affiliation_vias': vias,
        'institutional_families': dict(rows=[dict(profile_id=a['profile_id'], family=a['institutional_family'],
            basis=a['family_basis'], status=a['family_status'], sigep_entity_codes=a['entity_sigep_codes']) for a in affiliations]),
        'family_mapping_m1b': dict(references=dict(profiles='artifacts/audits/municipal_m1b/buyer_profiles.json',
            classification='artifacts/audits/municipal_m1b/classification.json',
            candidates='artifacts/audits/municipal_m1b/promotion_candidates.json'),
            consumed_profiles=len(m1b['profiles']), consumed_classifications=len(m1b_classification['rows']),
            registry_only_signals_referenced=len(m1b['registry_only_signals']),
            mapping=[dict(m1b=f, m2a=['OTHER_DECENTRALIZED', 'EDUCATION'] if f == 'OTHER_DECENTRALIZED'
                    else ['UNKNOWN'] if f in ('UNKNOWN_REVIEW', 'REGISTRY_NOISE') else [f])
                    for f in sorted({r['family'] for r in m1b_classification['rows']})],
            delta=delta),
        'territory_profiles': dict(territories=tp),
        'coverage': cov,
        'unresolved': dict(profile_ids=[p['profile_id'] for p in profiles
            if p['affiliation'] not in ('VERIFIED', 'NON_MUNICIPAL') or (p['affiliation'] == 'VERIFIED' and not p['logical_unit_id'])],
            registry_only_reference='artifacts/audits/municipal_m1b/buyer_profiles.json#/registry_only_signals',
            no_identity_inferred_from_fiscal_peers=True),
        'rankings': rankings(logical['units'], tp), 'byte_identity': proof, 'gates': gates,
    }
    for name, data in artifacts.items():
        save(out / f'{name}.json', data)
    render(out, cov, vias, gates, tp, delta)
    return gates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'artifacts/audits/municipal_m2a')
    parser.add_argument('--validate', action='store_true', help='Run focal tests, corpus, lint and deterministic replay')
    parser.add_argument('--baseline-hashes', type=Path, help='Optional pre-session SHA-256 snapshot')
    args = parser.parse_args()
    socket.socket.connect = deny_network
    socket.socket.connect_ex = deny_network
    socket.create_connection = deny_network
    gates = build(args.output, read(args.baseline_hashes) if args.baseline_hashes else None)
    if args.validate:
        from scripts.audits.m2a.validation import validate
        validate(args.output)
    print(gates['MUNICIPAL_M2A'], gates['territories'], gates['candidates'])
    if gates['MUNICIPAL_M2A'] != 'PASS':
        raise SystemExit(1)


if __name__ == '__main__':
    main()

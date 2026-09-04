"""Manual, evidence-specific Turbaco correction. Default: dry run, no promotion.

The task authorizes adjudicating this synthetic duplicate independently of the
ADR-074 five-condition automatic predicate, which DOES NOT apply here.
"""
from copy import deepcopy
from pathlib import Path
import argparse
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.entity_resolver import EntityResolver
from app.utils.money import normalize_text
from scripts.audits.promote_municipal_m1a import (
    DATASETS, debt, encode, ownership, read, scan_conflicts, scan_surfaces,
)

OUT = ROOT / 'artifacts/audits/municipal_m1a_2a'
A = 'ALCALDIA DE TURBACO BOLIVAR'
B = 'ALCALDIA MUNICIPAL DE TURBACO BOLIVAR'
ACCOUNT = '702768342'
EVIDENCE = 'artifacts/audits/municipal_m1a_2a/evidence/turbaco_manual_identity.json'


def verified_history():
    by_dataset = {}
    for ds, file in zip(DATASETS, ('turbaco_history_p6dx-8zbt.json', 'turbaco_history_contracts_retry.json')):
        report = read(OUT / 'evidence' / file)
        if report.get('status_code') != 200 or report.get('error') or len(report['rows']) >= 10000:
            raise ValueError('Incomplete historical ownership evidence')
        rows = report['rows']
        related = [r for r in rows if r.get('entidad', r.get('nombre_entidad')) in (A, B)
                   or r['codigo_entidad'] in (ACCOUNT, '706283421') or r['nit_entidad'] == '890481149']
        if len(related) != 1:
            raise ValueError('Manual identity needs re-adjudication')
        row = related[0]
        assert (row['codigo_entidad'], row.get('entidad', row.get('nombre_entidad')), row['nit_entidad']) == (
            ACCOUNT, B, '890481149')
        by_dataset[ds] = int(row['physical_rows'])
    return by_dataset


def consolidate(records, sidecar):
    """Apply exactly the manually adjudicated A -> B, never similarity/NIT rules."""
    evidence = read(ROOT / EVIDENCE)
    assert evidence['decision'] == 'DUPLICATE_BUYER_UNIT' and evidence['manual_authorization']
    volumes = verified_history()
    records, sidecar = deepcopy(records), deepcopy(sidecar)
    names = {r['official_name']: r for r in records}
    if A not in names:
        assert B in names and any(b['canonical_name'] == B and b['evidence'] == EVIDENCE
                                  for b in sidecar['bindings'])
        return records, sidecar
    old, target = names[A], names[B]
    aliases = list(dict.fromkeys([*target['aliases'], *old['aliases'], A]))
    owners = ownership(records)
    assert all(owners.get(normalize_text(alias), set()) <= {A, B} for alias in [B, *aliases])
    assert not any(b['canonical_name'] in (A, B) or any(B in ns or A in ns for ns in b['sources'].values())
                   for b in sidecar['bindings'])
    target['aliases'] = aliases
    proposal = dict(code='13836', canonical=B, aliases=aliases, account=ACCOUNT,
                    nit_evidence='890481149', source_names={ds: [B] for ds in DATASETS})
    analysis = read(ROOT / 'artifacts/audits/municipal_m0_v1/analysis.json.gz')
    conflicts = scan_conflicts([proposal], scan_surfaces(analysis))['13836']
    disabled = {*old.get('scan_disabled_aliases', []), *target.get('scan_disabled_aliases', []),
                *(r['alias'] for r in conflicts)}
    if disabled:
        target['scan_disabled_aliases'] = sorted(disabled)
    assert not old.get('resolution_blocked_aliases') and not target.get('resolution_blocked_aliases')
    records = [r for r in records if r['official_name'] != A]
    sidecar['bindings'].append(dict(canonical_name=B, identity_key=dict(type='BUYER_UNIT', value=B),
                                   observed_at='2026-09-04', evidence=EVIDENCE,
                                   sources={ds: [B] for ds in DATASETS}, observed_rows=volumes))
    return records, sidecar


def check(before, before_sources, after, sources):
    analysis = read(ROOT / 'artifacts/audits/municipal_m0_v1/analysis.json.gz')
    target = next(r for r in after if r['official_name'] == B)
    assert [r for r in before if r['official_name'] not in (A, B)] == [r for r in after if r['official_name'] != B]
    assert all(b in sources['bindings'] for b in before_sources['bindings'])
    assert {k: v for k, v in target.items() if k not in ('aliases', 'scan_disabled_aliases')} == {
        k: v for r in before if r['official_name'] == B for k, v in r.items()
        if k not in ('aliases', 'scan_disabled_aliases')}
    pre, post = debt(before, before_sources), debt(after, sources)
    new_multi = {k: v for k, v in post['multi_owner_alias_keys'].items()
                 if k not in pre['multi_owner_alias_keys'] or set(v) - set(pre['multi_owner_alias_keys'][k])}
    assert not new_multi
    proposal = dict(code='13836', canonical=B, aliases=target['aliases'], account=ACCOUNT,
                    nit_evidence='890481149', source_names={ds: [B] for ds in DATASETS})
    conflicts = scan_conflicts([proposal], scan_surfaces(analysis))['13836']
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / 'aliases.json'
        path.write_bytes(encode(after))
        resolver = EntityResolver(path)
        failures = [alias for alias in [B, *target['aliases']]
                    if resolver.resolve_entidad(alias).value != B
                    or resolver.owners_by_alias[normalize_text(alias)] != {B}]
        edges = {(a, c) for entries in resolver._scan_index.values() for a, c in entries}
        scan_failures = [r for r in conflicts if (normalize_text(r['alias']), B) in edges]
    assert not failures and not scan_failures
    assert consolidate(after, sources) == (after, sources)
    return dict(status='PASS', exact_alias_checks=len(target['aliases']) + 1,
                new_wrong_owners=failures, new_multi_owner=new_multi, new_scan_cross_owner=scan_failures,
                scan_surfaces=len(scan_surfaces(analysis)), excluded_scan_evidence=conflicts,
                no_nit_merge=True, unrelated_units_preserved=True, old_bindings_preserved=True,
                production_promoted=0, manual_duplicate_units_removed=1, idempotent=True,
                multi_owner_keys_removed=len(pre['multi_owner_alias_keys']) - len(post['multi_owner_alias_keys']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    before = read(OUT / 'evidence/before_aliases_db.json.gz')
    before_sources = read(OUT / 'evidence/before_buyer_source_names.json.gz')
    after, sources = consolidate(before, before_sources)
    result = check(before, before_sources, after, sources)
    (OUT / 'turbaco_correction_gates.json').write_bytes(encode(result))
    if args.apply:
        current = read(ROOT / 'app/data/aliases_db.json'), read(ROOT / 'app/data/buyer_source_names.json')
        assert current in ((before, before_sources), (after, sources)), 'Unexpected production change'
        (ROOT / 'app/data/aliases_db.json').write_bytes(encode(after))
        (ROOT / 'app/data/buyer_source_names.json').write_bytes(encode(sources))
    print(result['status'], 'APPLIED' if args.apply else 'DRY_RUN', flush=True)


if __name__ == '__main__':
    main()

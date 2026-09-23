"""Deterministic, offline M0 -> ADR-074 BuyerUnits; never merge by NIT.

Default is a dry run. --apply writes only after validation and idempotence.
The frozen before files are audit inputs, not a replacement legacy gazetteer.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.audits.adr073_historical_projection import parse_historical_projection
from app.core.entity_resolver import EntityResolver
from app.utils.money import normalize_text
from scripts.audits.audit_gazetteer_alias_ownership import Entity

M0 = ROOT / 'artifacts/audits/municipal_m0_v1'
OUT = ROOT / 'artifacts/audits/municipal_m1a'
EVIDENCE = 'artifacts/audits/municipal_m1a/promotion_manifest.json'
DATASETS = ('p6dx-8zbt', 'jbjy-vk9h')
EXPECTED = {'ADD_NEW_ACCREDITED_UNIT': 498, 'ADD_ALIASES_TO_EXISTING_UNIT': 5}
HEAD_BEFORE = '9fb6375a3d23158b44ca9ad14bf1624c2495d1fc'


def encode(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode()


def read(path):
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == '.gz' else raw)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def load_inputs():
    report = read(M0 / 'proposals.json')
    proposals = report['proposals']
    if (
        Counter(p['kind'] for p in proposals) != EXPECTED
        or sum(len(p['aliases']) for p in proposals) != 1888
    ):
        raise ValueError('STOP: M0 proposal counts changed')
    if report['summary']['status'] != 'PASS' or report['summary']['failures']:
        raise ValueError('STOP: M0 eligibility is not PASS')
    if len({p['code'] for p in proposals}) != 503:
        raise ValueError('STOP: duplicate M0 proposal identifiers')
    manifest = read(M0 / 'sources/manifest.json')
    for snapshot in manifest['snapshots']:
        raw = gzip.decompress((M0 / 'sources' / snapshot['file']).read_bytes())
        if sha(raw) != snapshot['uncompressed_sha256']:
            raise ValueError('STOP: changed M0 evidence: ' + snapshot['file'])
    return proposals, read(M0 / 'analysis.json.gz'), manifest


def ownership(records):
    owners = defaultdict(set)
    for record in records:
        for alias in [record['official_name'], *record.get('aliases', [])]:
            owners[normalize_text(alias)].add(record['official_name'])
    return owners


def debt(records, sidecar):
    groups = defaultdict(set)
    for record in records:
        groups[Entity(record).identity_key].add(record['official_name'])
    sources = defaultdict(set)
    bindings = {b['canonical_name']: b for b in sidecar['bindings']}
    for record in records:
        canonical = record['official_name']
        projection = bindings.get(canonical, {}).get(
            'sources', {ds: [canonical] for ds in DATASETS}
        )
        for ds, names in projection.items():
            for name in names:
                sources[ds + ':' + name].add(canonical)
    return {
        'duplicate_canonical_identity_keys': {
            k: sorted(v) for k, v in groups.items() if len(v) > 1
        },
        'multi_owner_alias_keys': {
            k: sorted(v) for k, v in ownership(records).items() if len(v) > 1
        },
        'source_name_identity_splits': {k: sorted(v) for k, v in sources.items() if len(v) > 1},
    }


def evidence_indexes(analysis):
    recent = defaultdict(set)
    strata = defaultdict(list)
    for r in analysis['strata']:
        recent[r['dataset'], r['name']].add((r['account'], r['nit_evidence']))
        strata[r['dataset'], r['name']].append(r)
    historical = defaultdict(set)
    audited = set()
    for batch in read(M0 / 'sources/ownership.json.gz'):
        if batch.get('error') or 'rows' not in batch or len(batch['rows']) >= 10000:
            continue
        audited.update((batch['dataset'], n) for n in batch['source_names'])
        for r in batch['rows']:
            historical[batch['dataset'], r.get('entidad', r.get('nombre_entidad'))].add(
                (str(r['codigo_entidad']), str(r['nit_entidad']))
            )
    return recent, strata, historical, audited


def scan_surfaces(analysis):
    """Include inactive directory accounts and all activity, not just executives."""
    surfaces = set()
    for r in read(M0 / 'sources/directory.json.gz')['rows']:
        if r.get('nombre_entidad'):
            surfaces.add(
                (
                    r['nombre_entidad'],
                    str(r.get('codigo_entidad', '')),
                    str(r.get('nit_entidad', '')),
                    'directory',
                )
            )
    for r in analysis['strata']:
        surfaces.add((r['name'], r['account'], r['nit_evidence'], r['dataset']))
    return sorted(surfaces)


def scan_conflicts(proposals, surfaces):
    index = defaultdict(list)
    for surface in surfaces:
        norm = normalize_text(surface[0])
        for word in set(norm.split()):
            index[word].append((norm, surface))
    conflicts = defaultdict(list)
    for p in proposals:
        for alias in dict.fromkeys([p['canonical'], *p['aliases']]):
            norm = normalize_text(alias)
            pattern = re.compile(r'\b' + re.escape(norm) + r'\b')
            for text, (name, account, nit, dataset) in index[norm.split()[0]]:
                if not pattern.search(text):
                    continue
                # Exact physical name + observed account/fiscal tuple is the
                # only exemption. No inference from NIT, tokens or location.
                if name in p['source_names'].get(
                    dataset, next(iter(p['source_names'].values()), [])
                ) and (account, nit) == (p['account'], p['nit_evidence']):
                    continue
                conflicts[p['code']].append(
                    dict(
                        alias=alias,
                        foreign_name=name,
                        account=account,
                        nit_evidence=nit,
                        dataset=dataset,
                    )
                )
    return conflicts


def transform(records, sidecar, proposals, analysis, observed_at):
    """Pure transform; supports repeated application and per-proposal REVIEW."""
    records, sidecar = deepcopy(records), deepcopy(sidecar)
    by_name = {r['official_name']: r for r in records}
    owners = ownership(records)
    identity_owners = defaultdict(set)
    for r in records:
        identity_owners[Entity(r).identity_key].add(r['official_name'])
    bindings = {b['canonical_name']: b for b in sidecar['bindings']}
    source_owners = defaultdict(set)
    for b in sidecar['bindings']:
        for ds, names in b['sources'].items():
            for name in names:
                source_owners[ds, name].add(b['canonical_name'])
    recent, strata, historical, audited = evidence_indexes(analysis)
    conflicts = scan_conflicts(proposals, scan_surfaces(analysis))
    accepted, rejected = [], []
    for p in sorted(proposals, key=lambda p: p['code']):
        canonical = p['canonical']
        record = by_name.get(canonical)
        binding = bindings.get(canonical)
        reason = None
        if not EntityResolver._official_matches_institutional_family(canonical, 'municipality'):
            reason = 'INSTITUTIONAL_FAMILY_NOT_MUNICIPAL_EXECUTIVE'
        elif p['source_names'].keys() != set(DATASETS) or any(
            len(p['source_names'][ds]) != 1 for ds in DATASETS
        ):
            reason = 'MISSING_DATASET_EVIDENCE'
        elif p['source_names'][DATASETS[0]] != p['source_names'][DATASETS[1]]:
            reason = 'SOURCE_NAME_CHANGED'
        elif p['kind'] == 'ADD_ALIASES_TO_EXISTING_UNIT' and record is None:
            reason = 'EXISTING_UNIT_MISSING'
        elif (
            p['kind'] == 'ADD_NEW_ACCREDITED_UNIT'
            and record is not None
            and (not binding or binding['evidence'] != EVIDENCE)
        ):
            reason = 'NEW_EXACT_OWNER'
        elif (
            record
            and record.get('nit')
            and re.sub(r'\D', '', str(record['nit'])) != re.sub(r'\D', '', p['nit_evidence'])
        ):
            reason = 'FISCAL_EVIDENCE_CONTRADICTION'
        elif any(owners[normalize_text(a)] - {canonical} for a in [canonical, *p['aliases']]):
            reason = 'NEW_MULTI_OWNER_ALIAS'
        elif identity_owners[Entity({'official_name': canonical}).identity_key] - {canonical}:
            reason = 'NEW_DUPLICATE_CANONICAL_IDENTITY'
        elif binding and binding['sources'] != p['source_names']:
            reason = 'EXISTING_SOURCE_PROJECTION_CHANGED'
        else:
            for ds, names in p['source_names'].items():
                name = names[0]
                key = (ds, name)
                expected = {(p['account'], p['nit_evidence'])}
                obs = strata[key]
                if key not in audited or historical[key] != expected or recent[key] != expected:
                    reason = 'SOURCE_ACCOUNT_OR_FISCAL_EVIDENCE_CHANGED'
                elif not obs or any(
                    r['status'] != 'ACCREDITED_MUNICIPAL_AFFILIATION'
                    or r['municipality'] != p['code']
                    for r in obs
                ):
                    reason = 'AFFILIATION_OR_DATASET_CHANGED'
                elif any(
                    sum(r[m] for r in obs) != p['volumes'][ds][m]
                    for m in ('physical_rows', 'unique_ids')
                ):
                    reason = 'SOURCE_VOLUME_CHANGED'
                elif source_owners[key] - {canonical}:
                    reason = 'NEW_SOURCE_NAME_IDENTITY_SPLIT'
        if reason:
            rejected.append(
                dict(proposal_code=p['code'], canonical=canonical, status='REVIEW', reason=reason)
            )
            continue
        if record is None:
            record = dict(
                official_name=canonical,
                departamento=p['department'],
                municipio=p['town'],
                source_datasets=['procesos', 'contratos'],
                aliases=[],
            )
            records.append(record)
            by_name[canonical] = record
        record['aliases'] = list(dict.fromkeys([*record.get('aliases', []), *p['aliases']]))
        disabled = {r['alias'] for r in conflicts[p['code']]}
        if disabled:
            record['scan_disabled_aliases'] = sorted(
                set(record.get('scan_disabled_aliases', [])) | disabled
            )
        if binding is None:
            binding = dict(
                canonical_name=canonical,
                identity_key={'type': 'BUYER_UNIT', 'value': 'municipal_m0:' + p['code']},
                observed_at=observed_at,
                evidence=EVIDENCE,
                sources=deepcopy(p['source_names']),
                observed_rows={ds: p['volumes'][ds]['physical_rows'] for ds in DATASETS},
            )
            sidecar['bindings'].append(binding)
            bindings[canonical] = binding
        for alias in [canonical, *p['aliases']]:
            owners[normalize_text(alias)].add(canonical)
        identity_owners[Entity(record).identity_key].add(canonical)
        for ds, names in binding['sources'].items():
            for name in names:
                source_owners[ds, name].add(canonical)
        affiliations = [
            dict(
                dataset=r['dataset'],
                account=r['account'],
                source_name=r['name'],
                affiliation_basis=r['affiliation_basis'],
                sigep_codes=r['sigep_codes'],
            )
            for ds in DATASETS
            for r in strata[ds, p['source_names'][ds][0]]
        ]
        accepted.append(
            dict(
                **deepcopy(p),
                proposal_id='municipal_m0:' + p['code'],
                accredited_at=observed_at,
                institutional_family='MUNICIPAL_EXECUTIVE',
                authorization_rule='M0_AUTO_SAFE_SINGLE_ACCOUNT_SINGLE_FISCAL_SAME_PHYSICAL_NAME_BOTH_DATASETS_UNIQUE_EXACT_OWNER',
                fiscal_identity={'evidence': p['nit_evidence'], 'is_identity_key': False},
                affiliation_evidence=affiliations,
                scan_disabled=sorted(disabled),
                scan_evidence=conflicts[p['code']],
            )
        )
    parse_historical_projection(sidecar)
    return records, sidecar, accepted, rejected


def validate(before_records, before_sidecar, records, sidecar, accepted, analysis):
    before, after = debt(before_records, before_sidecar), debt(records, sidecar)
    new_debt = {
        key: {
            k: v
            for k, v in after[key].items()
            if k not in before[key] or set(v) - set(before[key][k])
        }
        for key in before
    }
    failures = []
    with tempfile.TemporaryDirectory(prefix='m1a-gates-') as temp:
        path = Path(temp) / 'catalog.json'
        path.write_bytes(encode(records))
        resolver = EntityResolver(path)
        path_before = Path(temp) / 'before.json'
        path_before.write_bytes(encode(before_records))
        resolver_before = EntityResolver(path_before)
        for p in accepted:
            for alias in p['aliases']:
                r = resolver.resolve_entidad(alias)
                if (
                    r.value != p['canonical']
                    or r.method != 'exact'
                    or resolver.owners_by_alias.get(normalize_text(alias)) != {p['canonical']}
                ):
                    failures.append(dict(code=p['code'], alias=alias, result=r.to_dict()))
        # Audit every newly enabled scan edge (including automatic canonical
        # names), independently of which longer alias happens to win today.
        edges_before = {
            (a, c) for entries in resolver_before._scan_index.values() for a, c in entries
        }
        edges_after = {(a, c) for entries in resolver._scan_index.values() for a, c in entries}
        pmap = {p['canonical']: p for p in accepted}
        new_conflicts = []
        conflicts = scan_conflicts(accepted, scan_surfaces(analysis))
        for alias, canonical in sorted(edges_after - edges_before):
            p = pmap.get(canonical)
            if p:
                new_conflicts.extend(
                    dict(canonical=canonical, **c)
                    for c in conflicts[p['code']]
                    if normalize_text(c['alias']) == alias
                )
    # All pre-existing units and bindings survive; only M0 aliases and scan
    # exclusions can change on the five existing units. No NIT is merged.
    after_by_name = {r['official_name']: r for r in records}
    preserved = all(
        {k: v for k, v in r.items() if k not in ('aliases', 'scan_disabled_aliases')}
        == {
            k: v
            for k, v in after_by_name[r['official_name']].items()
            if k not in ('aliases', 'scan_disabled_aliases')
        }
        and set(r.get('aliases', [])) <= set(after_by_name[r['official_name']]['aliases'])
        for r in before_records
    )
    bindings_preserved = all(b in sidecar['bindings'] for b in before_sidecar['bindings'])
    gates = dict(
        new_debt=new_debt,
        exact_wrong_owners=failures,
        new_scan_cross_owner=new_conflicts,
        existing_units_preserved=preserved,
        existing_bindings_preserved=bindings_preserved,
        scan_surface_count=len(scan_surfaces(analysis)),
        before_debt=before,
        after_debt=after,
    )
    gates['status'] = (
        'PASS'
        if not any(new_debt.values())
        and not failures
        and not new_conflicts
        and preserved
        and bindings_preserved
        else 'BLOCKED'
    )
    return gates


def pipeline_gate(before_records, records, sidecar, proposals):
    """Full M0 alias replay through the real parser, plan and exact SoQL.

    This must precede --apply: lookup accreditation alone is insufficient.
    The temporary projector is the candidate registry; no production write
    or provider call is needed to observe the query actually built.
    """
    from unittest.mock import patch

    from app.core.query_plan import compute_query_plan
    from app.core.query_router import QueryRouter
    from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError

    registry = parse_historical_projection(sidecar)
    failures = []
    checks = 0
    with tempfile.TemporaryDirectory(prefix='m1a-pipeline-') as temp:
        paths = [Path(temp) / 'before.json', Path(temp) / 'after.json']
        for path, catalog in zip(paths, (before_records, records)):
            path.write_bytes(encode(catalog))
        before, after = [QueryRouter(EntityResolver(path)) for path in paths]
        builder = SoQLBuilder()
        with patch('app.core.soql_builder.project_buyer_names', registry.project):
            for p in proposals:
                for alias in p['aliases']:
                    # Both dataset heads are exercised for every proposal.
                    is_mayoralty = alias.startswith('alcaldia')
                    query = f'contratos de la {alias}' if is_mayoralty else f'procesos del {alias}'
                    ds = DATASETS[1] if is_mayoralty else DATASETS[0]
                    field = 'nombre_entidad' if is_mayoralty else 'entidad'
                    params = after.parse(query).params
                    plan = compute_query_plan(params, query)
                    try:
                        soql = builder.build_count(ds, dict(plan.params))
                    except UnsafeGlobalQueryError:
                        soql = 'BLOCKED: UnsafeGlobalQueryError (no WHERE predicate)'
                    source = p['source_names'][ds][0].replace("'", "''")
                    checks += 1
                    if (
                        params.get('entidad_resolved') != p['canonical']
                        or not plan.should_execute
                        or plan.params.get('entidad_resolved') != p['canonical']
                        or f"{field} = '{source}'" not in soql
                    ):
                        baseline = before.parse(query).params
                        failures.append(
                            dict(
                                code=p['code'],
                                query=query,
                                expected=p['canonical'],
                                before=baseline,
                                after=params,
                                should_execute=plan.should_execute,
                                candidate_soql=soql,
                                new_wrong_owner=bool(
                                    params.get('entidad_resolved')
                                    and params.get('entidad_resolved') != p['canonical']
                                    and params.get('entidad_resolved')
                                    != baseline.get('entidad_resolved')
                                ),
                            )
                        )
    return dict(
        status='BLOCKED' if failures else 'PASS',
        checks=checks,
        failed_queries=len(failures),
        affected_municipalities=len({r['code'] for r in failures}),
        new_wrong_owner_queries=sum(r['new_wrong_owner'] for r in failures),
        failures=failures,
        llm_execution_mode='DETERMINISTIC_OFFLINE_NO_LLM',
        llm_provider_live_coverage='NOT_VERIFIED',
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    proposals, analysis, source_manifest = load_inputs()
    before_records = read(OUT / 'before_aliases_db.json.gz')
    before_sidecar = read(OUT / 'before_buyer_source_names.json.gz')
    paths = [ROOT / 'app/data/aliases_db.json', ROOT / 'app/data/buyer_source_names.json']
    current = [read(p) for p in paths]
    records, sidecar, accepted, rejected = transform(
        *current, proposals, analysis, source_manifest['as_of']
    )
    repeat = transform(records, sidecar, proposals, analysis, source_manifest['as_of'])
    if repeat != (records, sidecar, accepted, rejected):
        raise ValueError('STOP: non-idempotent promotion')
    gates = validate(before_records, before_sidecar, records, sidecar, accepted, analysis)
    pipeline = pipeline_gate(before_records, records, sidecar, accepted)
    blocked = gates['status'] != 'PASS' or pipeline['status'] != 'PASS'
    manifest = dict(
        stage='MUNICIPAL_M1A',
        head_before=HEAD_BEFORE,
        production_applied=args.apply and not blocked,
        input_m0='artifacts/audits/municipal_m0_v1/proposals.json',
        input_sha256=sha((M0 / 'proposals.json').read_bytes()),
        source_manifest_sha256=sha((M0 / 'sources/manifest.json').read_bytes()),
        proposals=accepted,
    )
    metrics = dict(
        status='PARTIAL' if rejected else 'PASS',
        input=EXPECTED,
        new_buyer_units=len(records) - len(before_records),
        existing_extended=sum(p['kind'] == 'ADD_ALIASES_TO_EXISTING_UNIT' for p in accepted),
        rejected_at_promotion=len(rejected),
        aliases_added=sum(len(r.get('aliases', [])) for r in records)
        - sum(len(r.get('aliases', [])) for r in before_records),
        source_bindings_added=len(sidecar['bindings']) - len(before_sidecar['bindings']),
        gazetteer_before=len(before_records),
        gazetteer_after=len(records),
        aliases_before=sum(len(r.get('aliases', [])) for r in before_records),
        aliases_after=sum(len(r.get('aliases', [])) for r in records),
        exact_only_surface_count=sum(len(p['scan_disabled']) for p in accepted),
        idempotent=True,
        production_sha256={
            str(p.relative_to(ROOT)): sha(encode(value))
            for p, value in zip(paths, (records, sidecar))
        },
    )
    metrics['data_eligibility_status'] = metrics['status']
    metrics['pipeline_status'] = pipeline['status']
    metrics['status'] = 'BLOCKED' if blocked else metrics['status']
    metrics['production_applied'] = args.apply and not blocked
    # Dry-run artifacts are separate, so a preview cannot claim production.
    output = OUT if args.apply and not blocked else OUT / 'preview'
    output.mkdir(parents=True, exist_ok=True)
    for name, value in [
        ('promotion_manifest', manifest),
        ('before_after', metrics),
        ('rejected', rejected),
        ('safety_gates', gates),
        ('pipeline_gate', pipeline),
    ]:
        (output / (name + '.json')).write_bytes(encode(value))
    if args.apply and not blocked:
        for path, value in zip(paths, (records, sidecar)):
            raw = encode(value)
            if path.read_bytes() != raw:
                path.write_bytes(raw)
    print(json.dumps(metrics, indent=2))
    if blocked:
        (OUT / 'promotion_manifest.json').write_bytes(
            encode(
                dict(
                    stage='MUNICIPAL_M1A',
                    status='BLOCKED',
                    head_before=HEAD_BEFORE,
                    production_applied=False,
                    proposals_promoted=[],
                    proposals_deferred=[p['proposal_id'] for p in accepted],
                    input_m0=manifest['input_m0'],
                    input_sha256=manifest['input_sha256'],
                    candidate_manifest='preview/promotion_manifest.json',
                    reason='STOP_GLOBAL_SYSTEMIC_QUERY_CAPTURE_AND_NEW_WRONG_OWNER',
                )
            )
        )
        (OUT / 'before_after.json').write_bytes(
            encode(
                dict(
                    status='BLOCKED',
                    production_applied=False,
                    new_buyer_units=0,
                    existing_extended=0,
                    aliases_added=0,
                    source_bindings_added=0,
                    rejected_at_promotion=len(rejected),
                    globally_deferred=len(accepted),
                    gazetteer_before=len(before_records),
                    gazetteer_after=len(current[0]),
                    aliases_before=metrics['aliases_before'],
                    aliases_after=sum(len(r.get('aliases', [])) for r in current[0]),
                    production_sha256={
                        str(p.relative_to(ROOT)): sha(p.read_bytes()) for p in paths
                    },
                    candidate_metrics='preview/before_after.json',
                )
            )
        )
        (OUT / 'rejected.json').write_bytes(
            encode(
                dict(
                    per_proposal_data_rejections=rejected,
                    pipeline_affected_proposals=sorted({r['code'] for r in pipeline['failures']}),
                    global_stop=True,
                    deferred_count=len(accepted),
                )
            )
        )
        print(
            f'STOP GLOBAL: candidate pipeline={pipeline["status"]}; '
            f'{pipeline["failed_queries"]}/{pipeline["checks"]} failing queries; '
            'production data were not written.'
        )
        raise SystemExit(1)


if __name__ == '__main__':
    main()
